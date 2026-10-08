/**
 * Runtime composition layer: store + adapters + engine + service + inspection loop.
 * Shared by the extension and the CLI/service; importing the module has no side effects (design 2.5).
 */

import * as path from 'node:path';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import * as os from 'node:os';
import type { JudgePort, SourceAdapter, ManagedDeliveryPort } from './contracts/interfaces.js';
import { WatchStore } from './storage/store.js';
import { TaskStatusV1Adapter } from './source/task-status-v1/adapter.js';
import { AgentCheckAdapter } from './source/agent-check/adapter.js';
import { AgentFileAdapter } from './source/agent-file/adapter.js';
import { WatchEngine, DEFAULT_SEMANTIC_CONFIG, type AttentionNotice, type JudgmentNotice } from './engine/engine.js';
import { WatchService } from './engine/service.js';
import { negotiateRelay, negotiationFromRelaySource, type RelayNegotiation, type ProbeResult } from './relay/negotiate.js';
import { WatcherRelaySource, deriveSourceId } from './relay/managed.js';
import { SystemClock } from './util/clock.js';
import type { Clock } from './util/clock.js';
import { JevHttpClient } from './jev/index.js';
import { PiRegistryJudge, type PiClassifierRegistry } from './jev/pi-registry.js';
import { jevConsentState } from './jev/consent.js';
import type { SemanticEngineConfig } from './engine/engine.js';
import type { SemanticThresholds } from './engine/semantic.js';

export interface WatcherRuntimeOptions {
  /** watcher state root (location of SQLite/lock/evidence/results) */
  rootDir: string;
  /** sourceId -> task-status-v1 data root directory */
  sourceRoots: ReadonlyMap<string, string>;
  mode?: 'embedded' | 'service';
  /** Source scope approved by the owner profile */
  allowedSourceIds?: readonly string[];
  /** sourceId -> required check ids (completion contract) */
  requiredCheckIds?: ReadonlyMap<string, readonly string[]>;
  probeRelay?: () => Promise<ProbeResult>;
  /** Inspection interval (default 5s; tests may lower it) */
  pollTickMs?: number;
  /** Pi's model registry (`ctx.modelRegistry`), read live: lets Jev use credentials configured in Pi. */
  piRegistry?: () => PiClassifierRegistry | undefined;
  /** Explicitly injected judge (tests); default: JEV_API_KEY direct client, else Jev via piRegistry, else none */
  judge?: JudgePort;
  /** Consent for outbound data (required for live Jev judgment); default JEV_CONSENT=1 */
  semanticConsent?: boolean;
  /** Relay integration: true or {relayHome} enables the embedded managed source (default relayHome=~/.pi/relay) */
  relay?: boolean | { relayHome?: string; realm?: string; consumerProfileId?: string; sourceId?: string };
  /** Attention notification hook (display layer: session toast/widget; does not block the engine) */
  onAttention?: (notice: AttentionNotice) => void;
  /** Judgment-complete notice (reported once to the display layer after an accepted judgment) */
  onJudgment?: (notice: JudgmentNotice) => void;
  /** Injectable clock (tests); default SystemClock */
  clock?: Clock;
}

export interface WatcherRuntime {
  store: WatchStore;
  engine: WatchEngine;
  service: WatchService;
  negotiation: RelayNegotiation;
  /** Embedded relay managed source (null when not enabled) */
  relay: WatcherRelaySource | null;
  refreshNegotiation(): RelayNegotiation;
  /** Try to finalize on demand (takes effect right after a mid-session bind; idempotent) */
  ensureRelayReady(): Promise<RelayNegotiation>;
  /** Consume host responses -> applyResponse -> confirmApplied (called automatically inside the loop) */
  pumpRelayResponses(): Promise<void>;
  startLoop(signal?: AbortSignal, onAfterSweep?: () => void): void;
  stopLoop(): void;
  close(): void;
}

export async function startRuntime(options: WatcherRuntimeOptions): Promise<WatcherRuntime> {
  const rootDir = path.resolve(options.rootDir);
  const store = await WatchStore.open(rootDir, { mode: options.mode ?? 'embedded' });
  const adapters = new Map<string, SourceAdapter>();
  for (const [sourceId, sourceRoot] of options.sourceRoots) {
    adapters.set(sourceId, new TaskStatusV1Adapter(path.resolve(sourceRoot)));
  }
  // Generic monitor mode (decision-delta 2026-09-21): only agent-declared evidence sources.
  // unified-exec was removed: the agent holds the log_path from the exec result and watch-file pins
  // the exact path, so no external-library naming heuristics are needed; exit detection = declared pattern matching content.
  adapters.set('agent-file', new AgentFileAdapter());
  adapters.set('agent-check', new AgentCheckAdapter());
  const clock = options.clock ?? new SystemClock();

  // V2 semantic layer: judge selection + consent + policy-defaults thresholds (design 5.5/5.6)
  const policyDefaults = loadPolicyDefaults();
  const semanticConfig: SemanticEngineConfig = policyDefaults
    ? {
        thresholds: policyDefaults.thresholds as SemanticThresholds,
        maxJudgeRequestsPerRootDay: policyDefaults.scheduler.maxJudgeRequestsPerRootDay,
        maxProbesPerInspection: policyDefaults.capacity.maxProbesPerInspection,
        maxProbesPerEpisode: policyDefaults.capacity.maxProbesPerEpisode,
        maxEvidenceEvents: policyDefaults.capacity.maxEvidenceEvents,
        stateMaxBytes: policyDefaults.capacity.stateMaxBytes
      }
    : DEFAULT_SEMANTIC_CONFIG;
  const envKey = typeof process !== 'undefined' ? process.env?.JEV_API_KEY : undefined;
  // Egress consent is read live (env JEV_CONSENT, or the stored /watcher jev consent setting).
  const consent: boolean | (() => boolean) = options.semanticConsent ?? (() => jevConsentState().granted);
  // Judge: an explicit JEV_API_KEY uses the pinned direct client; otherwise Jev through Pi's
  // model registry (TYPESAFE_API_KEY or a provider added via /login); otherwise none.
  const judge: JudgePort | undefined = options.judge
    ?? (envKey ? new JevHttpClient({ apiKey: envKey })
      : options.piRegistry ? new PiRegistryJudge(options.piRegistry) : undefined);

  // V1 closed loop: embedded relay managed source (PI_WATCHER_RELAY=1 or options.relay)
  const relayEnabled = options.relay === true || (typeof options.relay === 'object' && options.relay !== undefined)
    || (typeof process !== 'undefined' && process.env?.PI_WATCHER_RELAY === '1');
  let relay: WatcherRelaySource | null = null;
  let relayError: string | undefined;
  if (relayEnabled) {
    const relayOpts = typeof options.relay === 'object' && options.relay !== undefined ? options.relay : {};
    try {
      relay = await WatcherRelaySource.open(path.join(rootDir, 'relay'), {
        relayHome: relayOpts.relayHome ?? process.env?.PI_RELAY_HOME ?? path.join(os.homedir(), '.pi', 'relay'),
        realm: relayOpts.realm,
        consumerProfileId: relayOpts.consumerProfileId,
        // One source store per root: sharing a global sourceId across roots would let an unrelated session's owner lock
        // block roots that really need relay (2026-09-21); an explicitly passed value is respected
        sourceId: relayOpts.sourceId ?? deriveSourceId(rootDir)
      });
      // Binding exists but not configured -> configure automatically (owner pre-authorized via relay-setup; design 9.4 audience)
      if (!relay.ready() && relay.hasActiveMembership()) {
        try {
          relay.ensureConsumerDeclaration();
          relay.provision();
        } catch {
          /* left for the owner to handle with relay-setup --finalize; inspection continues in local-display */
        }
      }
    } catch (e) {
      relayError = e instanceof Error ? e.message : String(e);
      relay = null;
    }
  }
  // When open throws, relay=null but relayError holds the real cause (authority mismatch / owner_conflict / ...);
  // do not fall back to the default probe message (it would mask the real failure as "relay not enabled")
  const negotiation0 = relayError
    ? negotiationFromRelaySource(null, relayError)
    : relay
      ? negotiationFromRelaySource(relay)
      : await negotiateRelay(options.probeRelay ? { probeRelay: options.probeRelay } : {});
  let negotiation = negotiation0;
  const delivery: () => ManagedDeliveryPort | undefined = () => (relay && relay.ready() ? relay : undefined);

  const engine = new WatchEngine({
    clock,
    store,
    adapters,
    requiredCheckIds: options.requiredCheckIds ?? new Map(),
    judge,
    semantic: semanticConfig,
    semanticConsent: consent,
    judgeRequiresConsent: !options.judge && !!judge,
    delivery,
    onAttention: options.onAttention,
    onJudgment: options.onJudgment
  });
  const service = new WatchService({
    clock,
    store,
    engine,
    negotiation: () => negotiation,
    allowedSourceIds: options.allowedSourceIds ?? [...options.sourceRoots.keys(), 'agent-file', 'agent-check'],
    requiredCheckIds: options.requiredCheckIds ?? new Map(),
    delivery: () => (relay && relay.ready() ? relay : undefined)
  });

  let timer: NodeJS.Timeout | null = null;
  const RESPONSE_STREAM = 'managed-responses';
  const refreshNegotiation = (): RelayNegotiation => {
    if (relay) negotiation = negotiationFromRelaySource(relay);
    return negotiation;
  };

  const ensureRelayReady = async (): Promise<RelayNegotiation> => {
    if (relay) {
      // resume self-heal: local setup claims ready but relay truth is closed (resume scenario)
      // -> reconfigure with a fresh audienceRef/scopeId (needs an active membership; with no binding, retry after auto-bind)
      if (relay.ready()) {
        try {
          const healed = relay.ensureHealed();
          if (healed.healed) refreshNegotiation();
        } catch { /* diagnosis/reconfigure failed: keep the status quo and retry next round (delivery failures have a separate make-loud fallback) */ }
      } else if (relay.hasActiveMembership()) {
        try {
          relay.ensureConsumerDeclaration();
          relay.provision();
          refreshNegotiation();
        } catch { /* retry on the next loop */ }
      }
    }
    return negotiation;
  };
  const runtime: WatcherRuntime = {
    store,
    engine,
    service,
    // Live value rather than a startup snapshot: once standing binding/provision completes, negotiation flips from unavailable to
    // managed-1-2; a snapshot would pin agent-registered watches to local-display forever (observed 2026-09-21)
    get negotiation() { return negotiation; },
    relay,
    refreshNegotiation,
    ensureRelayReady,
    async pumpRelayResponses() {
      if (!relay || !relay.ready()) return;
      try {
        await pumpResponses(relay, service, store, RESPONSE_STREAM);
      } catch {
        /* Relay unreachable or a response failed: local observation continues (design 9.5);
           the cursor was not advanced, so the next sweep re-reads the same batch. */
      }
    },
    startLoop(signal, onAfterSweep) {
      if (timer) return;
      const tick = options.pollTickMs ?? 5000;
      timer = setInterval(() => {
        if (signal?.aborted) {
          runtime.stopLoop();
          return;
        }
        // Auto-finalize after a mid-session bind (owner pre-authorized audience/scope via relay-setup)
        void ensureRelayReady();
        void engine.runDue().catch(() => {
          /* an inspection failure does not interrupt the loop; health is degraded internally by the engine */
        });
        void runtime.pumpRelayResponses().catch(() => { /* same as above */ });
        // Widget refresh (design 11 s33): re-pull the status-bar projection after every tick; it does not enter the LLM context
        try {
          onAfterSweep?.();
        } catch {
          /* display-layer failure does not affect the engine */
        }
      }, tick);
      timer.unref?.();
    },
    stopLoop() {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
    },
    close() {
      runtime.stopLoop();
      for (const adapter of adapters.values()) {
        void adapter.close();
      }
      store.close();
      if (relay) void relay.close();
    }
  };
  return runtime;
}

interface PolicyDefaultsFile {
  thresholds: SemanticThresholds;
  scheduler: { maxJudgeRequestsPerRootDay: number };
  capacity: {
    maxProbesPerInspection: number; maxProbesPerEpisode: number;
    maxEvidenceEvents: number; stateMaxBytes: number;
  };
}

function loadPolicyDefaults(): PolicyDefaultsFile | null {
  // Resolved next to this module only (src/ under tsx, dist/ when installed). Never from the
  // working directory: a user project must not be able to silently override the thresholds.
  try {
    return JSON.parse(readFileSync(fileURLToPath(new URL('./contracts/policy-defaults.json', import.meta.url)), 'utf8')) as PolicyDefaultsFile;
  } catch {
    return null;
  }
}

/**
 * Host response pump (T5, design 6.3/7.3): readResponses -> applyResponse -> confirmApplied.
 * The cursor advances only after the whole batch is applied and confirmed; the first failure
 * stops the batch and keeps the cursor, so the next sweep re-reads from the same position.
 * Re-reading is safe: applyResponse dedupes by responseId and confirmApplied is idempotent
 * by its stable operationId (conf-<responseId>). "Forgot it succeeded" must never become
 * "response lost forever" (I16). Failures propagate to the caller instead of being swallowed.
 */
export async function pumpResponses(
  port: Pick<ManagedDeliveryPort, 'readResponses' | 'confirmApplied'>,
  service: Pick<WatchService, 'applyResponse'>,
  store: WatchStore,
  stream: string
): Promise<{ applied: number; cursor: number }> {
  let after = store.transaction(tx => (tx as unknown as { getRelayCursor(stream: string): number }).getRelayCursor?.(stream)) ?? 0;
  if (typeof after !== 'number') after = 0;
  const batch = await port.readResponses(after);
  let applied = 0;
  for (const response of batch.responses) {
    const result = await service.applyResponse(response);
    await port.confirmApplied(`conf-${response.responseId}`, response.responseId, result);
    applied += 1;
  }
  if (batch.cursor > after) {
    store.transaction(tx => {
      (tx as unknown as { putRelayCursor(stream: string, cursor: number): void }).putRelayCursor?.(stream, batch.cursor);
    });
  }
  return { applied, cursor: Math.max(after, batch.cursor) };
}
