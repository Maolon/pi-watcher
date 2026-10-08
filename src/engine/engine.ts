/**
 * WatchEngine: inspection pipeline (deterministic V1 subset of the design 5.2 processing order).
 * 1) validate active/validity -> 2) read events/state (T2 ingest) -> 3) hard rules
 * -> 4) episode supersede + result card (durable outbox + local display copy).
 * The Jev semantic layer is wired in at V2 (JudgePort is ready); V1 has deterministic facts only.
 */

import type { Json, Observation, SourceAdapter, WatchSpec, RunTarget, Probe, JudgePort, Basis, QuestionKey, ManagedDeliveryPort, AttentionEnvelope } from '../contracts/interfaces.js';
import type { WatchStore, WatchRow, WatchSnapshot } from '../storage/store.js';
import { emptySnapshot } from '../storage/store.js';
import type { Clock } from '../util/clock.js';
import { newId, digestJson } from '../util/ids.js';
import { statusPayloadToTaskState, isTerminalTaskState, isTerminalFact, deadlineFact, silenceFact, completionLevel } from './hard-rules.js';
import { buildResultCard, buildGroupResultCard } from './cards.js';
import {
  windowFingerprint, buildSemanticState, applyThresholds, dayPeriodKey,
  type SemanticThresholds, type RecentObservation, type SemanticCandidate
} from './semantic.js';

export interface WatchEngineOptions {
  clock: Clock;
  store: WatchStore;
  adapters: Map<string, SourceAdapter>;
  requiredCheckIds?: ReadonlyMap<string, readonly string[]>;
  /** V2 hook: shadow/active semantic judgment; may be omitted in V1 */
  judge?: JudgePort;
  /** Note for the local display copy of the result card */
  displayNote?: string;
  /** V2 semantic-layer config (defaults to policy-defaults) */
  semantic?: SemanticEngineConfig;
  /** Data egress consent (required for live Jev; Mock does not export locally, tests may set judgeRequiresConsent=false) */
  semanticConsent?: boolean | (() => boolean);
  /** Whether the judge is egress-type (needs consent); pass false for Mock local judgment */
  judgeRequiresConsent?: boolean;
  /** V1 closed loop: relay managed delivery port (static or live reference; upgraded after mid-session bind) */
  delivery?: ManagedDeliveryPort | (() => ManagedDeliveryPort | undefined);
  /** Notification hook after attention lands in the outbox (for the display layer; does not block the engine) */
  onAttention?: (notice: AttentionNotice) => void;
  /** Judgment-complete notice (display layer; fires after an accepted judgment, must not block the engine) */
  onJudgment?: (notice: JudgmentNotice) => void;
}

/** Attention notification published by the engine (passed across layers to the display layer / session toast). */
export interface AttentionNotice {
  watchId: string;
  ownerSession?: string;
  episodeId: string;
  reasonCode: string;
  summary: string;
  transport: 'relay-managed' | 'local-display' | 'relay-failed';
  /** Failure reason when transport=relay-failed (same source as outbox admission_json.error) */
  relayError?: string;
  /** Deadline until which the envelope is still valid (ms epoch) */
  validUntil: number;
}

/** Accepted-judgment-complete notification (display-layer notice; judge review must be visible) */
export interface JudgmentNotice {
  watchId: string;
  ownerSession?: string;
  objective: string;
  /** shadow = record only; active = may create episode/attention */
  mode: 'shadow' | 'active';
  /** Candidates that hit the threshold (none -> everything normal, no action needed) */
  candidates: Array<{ reason: string; probability: number; note: string | null }>;
  probabilities: Record<string, number>;
}

export interface SemanticEngineConfig {
  thresholds: SemanticThresholds;
  maxJudgeRequestsPerRootDay: number;
  maxProbesPerInspection: number;
  maxProbesPerEpisode: number;
  maxEvidenceEvents: number;
  stateMaxBytes: number;
}

export const DEFAULT_SEMANTIC_CONFIG: SemanticEngineConfig = {
  thresholds: {
    contextSufficient: 0.75,
    needsHostDecision: 0.85,
    unresolvedBlocker: 0.85,
    repeating: 0.9,
    claimConflict: 0.85
  },
  maxJudgeRequestsPerRootDay: 2000,
  maxProbesPerInspection: 1,
  maxProbesPerEpisode: 2,
  maxEvidenceEvents: 16,
  stateMaxBytes: 24576
};

/** Default attention envelope validity: real wake latency includes safety-net ticks / long turns,
 *  300s is not enough; also bounds the fast-close line. */
export const DEFAULT_ATTENTION_TTL_MS = 1_800_000;

const SEMANTIC_UNAVAILABLE = 'semantic review unavailable; hard rules continue';
const NO_JEV_CREDENTIALS = 'no Jev credentials (set JEV_API_KEY or TYPESAFE_API_KEY, or add a Jev provider via /login)';

/** Relay error codes that will not change on retry: record the control op as rejected. */
const DETERMINISTIC_RELAY_CODES = new Set(['stale_scope_revision', 'invalid_state', 'invalid_payload', 'not_found', 'unauthorized', 'id_conflict']);

export interface InspectionOutcome {
  inspectionId: string;
  watchId: string;
  ingested: number;
  produced: {
    episodes: string[];
    resultCards: string[];
  };
  health: WatchRow['health'];
  lifecycleAfter: WatchRow['lifecycle'];
}

export class WatchEngine {
  private readonly clock: Clock;
  private readonly store: WatchStore;
  private readonly adapters: Map<string, SourceAdapter>;
  private readonly requiredCheckIds: ReadonlyMap<string, readonly string[]>;
  private readonly judge?: JudgePort;
  private readonly displayNote: string;
  private readonly semanticConfig: SemanticEngineConfig;
  private readonly semanticConsentRef: () => boolean;
  private readonly judgeRequiresConsent: boolean;
  private readonly deliveryRef: () => ManagedDeliveryPort | undefined;
  private readonly onAttention?: (notice: AttentionNotice) => void;
  private readonly onJudgment?: (notice: JudgmentNotice) => void;
  /** Single-flight per Watch (design 5.6) */
  private readonly semanticInFlight = new Set<string>();
  /** After 401/403, egress is disabled within this process (design 5.6) */
  private judgeDisabled = false;

  constructor(options: WatchEngineOptions) {
    this.clock = options.clock;
    this.store = options.store;
    this.adapters = options.adapters;
    this.requiredCheckIds = options.requiredCheckIds ?? new Map();
    this.judge = options.judge;
    this.displayNote = options.displayNote ?? 'local-display projection; not a relay delivery fact';
    this.semanticConfig = options.semantic ?? DEFAULT_SEMANTIC_CONFIG;
    const consent = options.semanticConsent ?? false;
    this.semanticConsentRef = typeof consent === 'function' ? consent : () => consent;
    this.judgeRequiresConsent = options.judgeRequiresConsent ?? true;
    const d = options.delivery;
    this.deliveryRef = typeof d === 'function' ? d : () => d;
    this.onAttention = options.onAttention;
    this.onJudgment = options.onJudgment;
  }

  /** Consent is read live: the user may grant or revoke it mid-session (/watcher jev consent). */
  private get semanticConsent(): boolean {
    return this.semanticConsentRef();
  }

  private get delivery(): ManagedDeliveryPort | undefined {
    return this.deliveryRef();
  }

  /** Run due inspections: returns the list of executed inspectionIds. */
  async runDue(limit = 32): Promise<string[]> {
    // Durable control outbox first (design 7.7: control precedes ordinary publish).
    try {
      await this.drainControl();
    } catch {
      /* intents stay durable; retried next sweep */
    }
    const now = this.clock.wallNow();
    // Sweep: archive expired attention; supersede open episodes left on disabled watches (honest state, never leave pending forever)
    this.store.transaction(tx => {
      tx.expireStaleAttentions(now);
      tx.supersedeEpisodesOfInactiveWatches(now);
    });
    const due = this.store.transaction(tx => tx.listAllWatches(1000))
      .filter(w => w.lifecycle === 'active')
      .filter(w => w.nextDueAt === null || w.nextDueAt <= now)
      .slice(0, limit);
    const done: string[] = [];
    for (const row of due) {
      const outcome = await this.inspectWatch(row.watchId);
      if (outcome) done.push(outcome.inspectionId);
    }
    return done;
  }

  async inspectWatch(watchId: string, signal?: AbortSignal): Promise<InspectionOutcome | null> {
    const inspectionId = newId('insp');
    const row = this.store.transaction(tx => tx.getWatchRow(watchId));
    if (!row) return null;
    if (row.lifecycle !== 'active') {
      return {
        inspectionId, watchId, ingested: 0,
        produced: { episodes: [], resultCards: [] },
        health: row.health, lifecycleAfter: row.lifecycle
      };
    }

    // limits.expiresAt: observation authorization expired -> lifecycle expired (does not imply business failure)
    const now = this.clock.wallNow();
    if (row.spec.limits.expiresAt && now > Date.parse(row.spec.limits.expiresAt)) {
      this.store.transaction(tx => {
        tx.updateWatch(watchId, { lifecycle: 'expired' }, now);
      });
      return {
        inspectionId, watchId, ingested: 0,
        produced: { episodes: [], resultCards: [] },
        health: row.health, lifecycleAfter: 'expired'
      };
    }

    const target = row.spec.target;
    if (target.kind === 'group') {
      return this.inspectGroup(watchId, row, inspectionId, now);
    }
    if (target.kind === 'obligation') {
      return this.inspectObligation(watchId, row, inspectionId, now);
    }
    const adapter = this.adapters.get(target.sourceId);
    if (!adapter) {
      // Source unreachable: keep last known, health=degraded (design 4.5)
      this.degrade(watchId, `source adapter unavailable: ${target.sourceId}`, now);
      this.ensureMonitorEpisode(watchId, row, 'monitor.degraded', `source adapter unavailable: ${target.sourceId}`, now);
      return {
        inspectionId, watchId, ingested: 0,
        produced: { episodes: [], resultCards: [] },
        health: 'degraded', lifecycleAfter: row.lifecycle
      };
    }

    let readResult: Awaited<ReturnType<SourceAdapter['read']>>;
    try {
      const cursor = this.store.transaction(tx => tx.getCursor(watchId, target.sourceId, row.generation));
      readResult = await adapter.read(target, cursor, signal ?? new AbortController().signal);
    } catch (e) {
      this.degrade(watchId, `source read failed: ${e instanceof Error ? e.message : String(e)}`, now);
      this.ensureMonitorEpisode(watchId, row, 'monitor.degraded', `source read failed: ${e instanceof Error ? e.message : String(e)}`, now);
      return {
        inspectionId, watchId, ingested: 0,
        produced: { episodes: [], resultCards: [] },
        health: 'degraded', lifecycleAfter: row.lifecycle
      };
    }

    // T2 ingest: save observation, update snapshot/cursor, advance next_due (cursor advances only after the whole batch commits)
    const ingest = this.store.transaction(tx => {
      let inserted = 0;
      let snapshot: WatchSnapshot = row.snapshot ?? emptySnapshot();
      let newestStatusAtMs = snapshot.lastObservedAtMs ?? 0;
      for (const obs of readResult.observations) {
        const localSeq = tx.bumpObservationSeq(watchId);
        const did = tx.insertObservation({
          observationId: obs.observationId,
          watchId,
          generation: row.generation,
          sourceId: target.sourceId,
          attemptId: obs.target.attemptId,
          sourceSeq: obs.sourceSeq,
          localSeq,
          observedAt: Date.parse(obs.observedAt) || now,
          digest: digestJson(obs.payload),
          payload: obs.payload
        });
        if (did) inserted += 1;
        // Identity check: evidence from different attempts is not merged (I01)
        if (obs.target.attemptId === target.attemptId && obs.target.runId === target.runId) {
          const applied = applyObservationToSnapshot(snapshot, obs, now);
          snapshot = applied.snapshot;
          if (applied.activityAtMs !== null && applied.activityAtMs > newestStatusAtMs) newestStatusAtMs = applied.activityAtMs;
        }
      }
      snapshot.lastObservedAtMs = Math.max(newestStatusAtMs, snapshot.lastObservedAtMs ?? 0) || snapshot.lastObservedAtMs;
      snapshot.coverage = {
        truncated: false,
        sourceGap: readResult.gap || (snapshot.coverage?.sourceGap ?? false)
      };

      // Backoff: new facts reset to pollMin; without new facts grow by x2 up to pollMax
      const { pollMinMs, pollMaxMs } = row.spec.limits;
      let backoffMs: number;
      if (inserted > 0) {
        backoffMs = pollMinMs;
      } else {
        const prev = snapshot.backoffMs || pollMinMs;
        backoffMs = Math.min(Math.max(prev * 2, pollMinMs), pollMaxMs);
      }
      // After terminal facts the source no longer changes: inspection backs off to a slow liveness cadence (wait for auto-close / explicit control, no idle burn)
      if (isTerminalFact(snapshot)) {
        backoffMs = Math.max(backoffMs, 60_000);
      }
      snapshot.backoffMs = backoffMs;

      let health: WatchRow['health'] = readResult.gap ? 'degraded' : 'healthy';
      if (readResult.gap) {
        snapshot.degradedReason = 'source gap (journal rotation/truncation or unparseable line)';
      } else if (health === 'healthy') {
        snapshot.degradedReason = null;
      }

      tx.updateWatch(watchId, { snapshot, nextDueAt: now + backoffMs, health }, now);
      tx.putCursor(watchId, target.sourceId, row.generation, readResult.nextCursor);
      return { inserted, snapshot, health };
    });

    // Hard rules (deterministic facts first)
    const produced = { episodes: [] as string[], resultCards: [] as string[] };
    const spec = row.spec;
    const snapshot = ingest.snapshot;
    const requiredChecks = this.requiredCheckIds.get(target.sourceId) ?? [];

    // 3a. Terminal facts: explicit terminal state, or "exited but result unknown" (exited evidence -> task.exited-unknown)
    if (isTerminalFact(snapshot) && !snapshot.terminalAt) {
      const completion = completionLevel(spec, snapshot, requiredChecks);
      const kind = snapshot.taskState === 'failed'
        ? 'task.failed'
        : snapshot.taskState === 'unknown'
          ? 'task.exited-unknown'
          : 'task.terminal';
      const episodeId = this.openEpisode(watchId, row, kind, {
        reasonCode: kind,
        taskState: snapshot.taskState,
        exitCode: snapshot.exitCode ?? null,
        summary: snapshot.summary ?? null,
        overall: completion.overall,
        firstSeenAt: new Date(now).toISOString()
      }, now);
      if (episodeId) {
        produced.episodes.push(episodeId);
        await this.publishEpisodeAttention(watchId, row, episodeId, kind, `${snapshot.taskState}${snapshot.exitCode !== undefined && snapshot.exitCode !== null ? ` exitCode=${snapshot.exitCode}` : ''}: ${snapshot.summary ?? 'no summary'}`, now);
      }

      const resultId = `result-${watchId}-${row.generation}-${snapshot.lastSourceSeq ?? 0}`;
      const card = buildResultCard({
        resultId,
        spec: { ...spec, watchId },
        snapshot: { ...snapshot, terminalAt: now },
        requiredCheckIds: requiredChecks,
        health: ingest.health,
        observedAtMs: now,
        deliveryNote: this.displayNote
      });
      this.commitResultCard(watchId, row, episodeId, resultId, card, now);
      produced.resultCards.push(resultId);
      this.store.transaction(tx => {
        const fresh = tx.getWatchRow(watchId);
        if (fresh) {
          tx.updateWatch(watchId, {
            snapshot: { ...fresh.snapshot, terminalAt: now, lastResultId: resultId }
          }, now);
        }
      });
    }

    // 3b. Business deadline crossed (a new fact even when observations are unchanged).
    //     Crossing is an edge: at most one deadline episode per slot (design 3.6). Once the
    //     host has closed it, it is not reopened/re-sent on every inspection (I18: no
    //     self-wake loop without new facts).
    const dl = deadlineFact(spec.mission, now);
    const deadlineAlreadyRaised = dl.crossed && this.store.transaction(tx =>
      tx.nextEpisodeOrdinal(watchId, row.generation, 'deadline.exceeded', spec.mission.checkpointId) > 1);
    if (dl.crossed && !deadlineAlreadyRaised && !isTerminalFact(snapshot)) {
      const episodeId = this.openEpisode(watchId, row, 'deadline.exceeded', {
        reasonCode: 'deadline.exceeded',
        deadlineAt: spec.mission.deadlineAt ?? null,
        taskState: snapshot.taskState,
        firstSeenAt: new Date(now).toISOString()
      }, now);
      if (episodeId) {
        produced.episodes.push(episodeId);
        await this.publishEpisodeAttention(watchId, row, episodeId, 'deadline.exceeded', `deadline ${spec.mission.deadlineAt ?? ''} crossed; taskState=${snapshot.taskState}`, now);
      }
    }

    // 3c. Silence over limit -> monitoring health degraded (not task failure); after terminal facts the source no longer produces output, so the silence rule does not apply
    if (!isTerminalFact(snapshot)) {
      const silence = silenceFact(snapshot, spec.limits.maxSilenceMs, now);
      if (silence.exceeded) {
        this.store.transaction(tx => {
          const fresh = tx.getWatchRow(watchId);
          if (fresh && fresh.health === 'healthy') {
            tx.updateWatch(watchId, { health: 'degraded' }, now);
          }
        });
        this.ensureMonitorEpisode(watchId, row, 'monitor.degraded', `silence exceeded maxSilenceMs=${spec.limits.maxSilenceMs}`, now);
      }
    }

    // 3d. Terminal-state auto-close (a watch the host already responded to should not keep occupying the TTL window):
    //   a) all episodes resolved/superseded (host responded) -> close after a short grace once terminal
    //   b) otherwise, if still unanswered after attention TTL + grace -> supersede episodes and shut down (clean up after watch completes)
    //   Delivery guard: when the wake is unconfirmed (publish not done / still in flight in relay)
    //   do not discard on TTL cleanup -- undelivered != host has seen; the watch stays until limits.expiresAt, visible in the widget.
    const attentionTtlMs = spec.policy.attentionTtlMs || DEFAULT_ATTENTION_TTL_MS;
    if (isTerminalFact(snapshot) && snapshot.terminalAt) {
      const ttlElapsed = now > snapshot.terminalAt + attentionTtlMs + 60_000;
      let hostResponded = false;
      if (!ttlElapsed) {
        const episodes = this.store.transaction(tx => tx.listEpisodes(watchId, 50));
        hostResponded = episodes.length > 0
          && episodes.every(e => e.state === 'resolved' || e.state === 'superseded');
      }
      if ((ttlElapsed && !this.hasUndeliveredWake(watchId, now)) || (hostResponded && now > snapshot.terminalAt + 5_000)) {
        this.autoCloseTerminal(watchId, now);
      }
    }

    // 3e. Delivery-layer health: attention still unknown and episode undecided -> make loud + bounded retry within envelope TTL
    await this.checkDeliveryHealth(watchId, now);

    // 5.2 steps 3-5: semantic layer (call Jev only if the window changed and egress is permitted; shadow only records candidates)
    if (spec.policy.semanticMode !== 'off' && !isTerminalFact(snapshot)) {
      // A judge may be configured but not usable yet (credentials resolved lazily from Pi).
      const readiness = !this.judge
        ? { ready: false, reason: NO_JEV_CREDENTIALS as string | undefined }
        : this.judge.ready ? await this.judge.ready().catch(() => ({ ready: false, reason: 'judge readiness check failed' })) : { ready: true };
      if (readiness.ready) {
        await this.runSemanticPass(watchId, now, signal);
      } else {
        // Say so instead of silently tracking facts only. A mock judge is never substituted
        // outside tests; its scores are not model output.
        const reason = `${SEMANTIC_UNAVAILABLE}${'reason' in readiness && readiness.reason ? `: ${readiness.reason}` : ''}`;
        this.store.transaction(tx => {
          const fresh = tx.getWatchRow(watchId);
          if (!fresh || fresh.snapshot.semantic?.error === reason) return;
          tx.updateWatch(watchId, {
            snapshot: { ...fresh.snapshot, semantic: { ...(fresh.snapshot.semantic ?? { mode: spec.policy.semanticMode === 'active' ? 'active' : 'shadow', repeatingStreak: 0 }), error: reason } }
          }, now);
        });
      }
    }

    return {
      inspectionId,
      watchId,
      ingested: ingest.inserted,
      produced,
      health: ingest.health,
      lifecycleAfter: 'active'
    };
  }

  /**
   * V2 semantic inspection (design 5.2 steps 3-5 / 5.5 / 5.6).
   * Network calls do not hold a SQL write transaction; single-flight per Watch; late results are discarded after version check.
   */
  private async runSemanticPass(watchId: string, now: number, signal?: AbortSignal): Promise<void> {
    if (this.judgeDisabled || this.semanticInFlight.has(watchId)) return;
    this.semanticInFlight.add(watchId);
    try {
      const row0 = this.store.transaction(tx => tx.getWatchRow(watchId));
      if (!row0 || row0.lifecycle !== 'active' || row0.spec.target.kind !== 'run') return;
      const observations = this.store.transaction(tx => tx.listObservations(watchId, 32));
      const fp = windowFingerprint(row0, now);
      const sem0 = row0.snapshot.semantic;
      const model = 'jev-1.13.0';
      const questionSet = 'watcher-q1';

      // Window unchanged and an accepted judgment exists -> cache hit (cache key includes model/question set/policy version, 5.2 step 3)
      const cached = this.store.transaction(tx =>
        tx.getJudgmentByWindow(watchId, row0.generation, fp, model, questionSet));
      if (sem0?.lastWindowDigest === fp && cached?.status === 'accepted') return;

      const isLiveJudge = this.judgeRequiresConsent;
      if (isLiveJudge && !this.semanticConsent) {
        this.store.transaction(tx => {
          const fresh = tx.getWatchRow(watchId);
          if (fresh) tx.updateWatch(watchId, {
            snapshot: { ...fresh.snapshot, semantic: { ...(fresh.snapshot.semantic ?? { mode: 'shadow', repeatingStreak: 0 }), consentMissing: true, budgetExhausted: false } }
          }, now);
        });
        return;
      }

      // Budget (5.6): hard limit by request count; per-watch daily limit + per-root daily limit
      const day = dayPeriodKey(now);
      const watchUsed = this.store.transaction(tx => tx.countBudgetReservations('judge', `watch:${watchId}:${day}`, watchId));
      const rootUsed = this.store.transaction(tx => tx.countBudgetReservations('judge', `root:${day}`, undefined));
      const watchCap = row0.spec.limits.maxJudgeRequestsPerDay;
      if (watchUsed >= watchCap || rootUsed >= this.semanticConfig.maxJudgeRequestsPerRootDay) {
        this.store.transaction(tx => {
          const fresh = tx.getWatchRow(watchId);
          if (!fresh) return;
          tx.updateWatch(watchId, {
            health: 'degraded',
            snapshot: { ...fresh.snapshot, degradedReason: 'semantic budget exhausted; hard rules continue (design 5.6)', semantic: { ...(fresh.snapshot.semantic ?? { mode: 'shadow', repeatingStreak: 0 }), budgetExhausted: true } }
          }, now);
        });
        return;
      }
      const reservationId = newId('bud');
      this.store.transaction(tx => {
        tx.insertBudgetReservation({ reservationId, watchId, category: 'judge', periodKey: `watch:${watchId}:${day}`, units: 1, state: 'reserved', createdAt: now });
        tx.insertBudgetReservation({ reservationId: reservationId + '-root', watchId, category: 'judge', periodKey: `root:${day}`, units: 1, state: 'reserved', createdAt: now });
      });

      const basis: Basis = {
        watchId,
        generation: row0.generation,
        missionRevision: row0.missionRevision,
        controlRevision: row0.controlRevision,
        observationSeq: row0.observationSeq,
        windowDigest: fp
      };
      const state = buildSemanticState(row0, observations, this.semanticConfig);

      let judgment;
      try {
        judgment = await this.judge!.evaluate(basis, state, signal ?? new AbortController().signal);
        this.store.transaction(tx => tx.setBudgetReservationState(reservationId, 'spent'));
        this.store.transaction(tx => tx.setBudgetReservationState(reservationId + '-root', 'spent'));
      } catch (e) {
        // Timeout is treated as possibly billed; conservatively consume the reservation (5.6)
        this.store.transaction(tx => tx.setBudgetReservationState(reservationId, 'unknown-cost'));
        this.store.transaction(tx => tx.setBudgetReservationState(reservationId + '-root', 'unknown-cost'));
        const msg = e instanceof Error ? e.message : String(e);
        const auth = e instanceof Error && (e.name === 'JevAuthenticationError' || /401|403|authentication/i.test(msg));
        if (auth && !this.judge?.ready) this.judgeDisabled = true; // host-resolved judges re-check credentials each pass
        this.store.transaction(tx => {
          const fresh = tx.getWatchRow(watchId);
          if (!fresh) return;
          tx.insertJudgment({
            judgmentId: newId('jd'), watchId, generation: row0.generation, missionRevision: row0.missionRevision,
            controlRevision: row0.controlRevision, windowDigest: fp, model, questionSet, status: 'failed',
            data: { error: msg, authDisabled: auth }, createdAt: now
          });
          tx.updateWatch(watchId, {
            health: 'degraded',
            snapshot: { ...fresh.snapshot, degradedReason: `jev failed: ${msg}`, semantic: { ...(fresh.snapshot.semantic ?? { mode: 'shadow', repeatingStreak: 0 }), error: msg } }
          }, now);
        });
        return;
      }

      // Validate response completeness and probability range (5.2 step 4); late results are discarded after version check (5.6)
      const keys: QuestionKey[] = ['meaningful_progress', 'unresolved_blocker', 'needs_host_decision', 'repeating_without_new_information', 'claim_conflicts_with_evidence', 'context_sufficient'];
      const valid = keys.every(k => typeof judgment.probabilities[k] === 'number' && judgment.probabilities[k] >= 0 && judgment.probabilities[k] <= 1);
      const stillCurrent = this.store.transaction(tx => {
        const fresh = tx.getWatchRow(watchId);
        return !!fresh && fresh.missionRevision === row0.missionRevision && fresh.controlRevision === row0.controlRevision;
      });
      if (!valid || !stillCurrent) {
        this.store.transaction(tx => tx.insertJudgment({
          judgmentId: newId('jd'), watchId, generation: row0.generation, missionRevision: row0.missionRevision,
          controlRevision: row0.controlRevision, windowDigest: fp, model: judgment.model, questionSet, status: 'discarded',
          data: { reason: !valid ? 'invalid probabilities' : 'stale basis', probabilities: judgment.probabilities as unknown as Record<string, Json> }, createdAt: now
        }));
        return;
      }

      // "Window with new observations" (5.5) is judged by the local monotonic observationSeq,
      // counting every observation kind from every source. lastSourceSeq only advances on
      // status, so log-only sources would otherwise never count as new.
      const hadNewObservations = sem0?.lastJudgedObservationSeq === undefined || sem0.lastJudgedObservationSeq === null
        ? (row0.snapshot.lastSourceSeq ?? -1) !== (sem0?.lastJudgedSourceSeq ?? -1)
        : row0.observationSeq !== sem0.lastJudgedObservationSeq;
      const prevStreak = row0.snapshot.semantic?.repeatingStreak ?? 0;
      const applied = applyThresholds(judgment.probabilities, this.semanticConfig.thresholds, hadNewObservations, prevStreak);

      let followUpJudged = false;
      let candidates = applied.candidates;
      let probabilities = judgment.probabilities;

      // Context insufficient and budget available: pick one bounded probe; after updating evidence, judge at most once more (5.2 step 5)
      if (applied.contextLow) {
        const probes = await this.listProbes(watchId);
        const probeBudgetOk = this.store.transaction(tx =>
          tx.countBudgetReservations('probe', `watch:${watchId}:${day}`, watchId) < row0.spec.limits.maxProbesPerEpisode);
        if (probes.length > 0 && probeBudgetOk) {
          const probeRunId = newId('prb');
          const chosen = await this.judge!.chooseProbe(basis, state, probes, signal ?? new AbortController().signal);
          this.store.transaction(tx => tx.insertProbeRun({
            probeRunId, watchId, episodeId: null, generation: row0.generation, controlRevision: row0.controlRevision,
            probeId: chosen === 'none' ? 'none' : chosen, status: 'inflight',
            data: { reason: 'context_sufficient below threshold', windowDigest: fp }, createdAt: now
          }));
          if (chosen !== 'none') {
            const probe = probes.find(p => p.probeId === chosen);
            if (probe) {
              const budgetId = newId('bud');
              this.store.transaction(tx => tx.insertBudgetReservation({ reservationId: budgetId, watchId, category: 'probe', periodKey: `watch:${watchId}:${day}`, units: 1, state: 'spent', createdAt: now }));
              const got = await this.executeProbe(watchId, chosen, signal);
              // Record probe observation (dedup relies on UNIQUE constraint)
              const ingested2 = this.store.transaction(tx => {
                const fresh = tx.getWatchRow(watchId);
                if (!fresh) return 0;
                let n = 0;
                let snap: WatchSnapshot = { ...fresh.snapshot };
                const t = fresh.spec.target as RunTarget;
                for (const obs of got) {
                  const localSeq = tx.bumpObservationSeq(watchId);
                  const did = tx.insertObservation({
                    observationId: obs.observationId, watchId, generation: fresh.generation,
                    sourceId: (fresh.spec.target as RunTarget).sourceId, attemptId: obs.target.attemptId,
                    sourceSeq: obs.sourceSeq, localSeq, observedAt: Date.parse(obs.observedAt) || now,
                    digest: digestJson(obs.payload), payload: obs.payload
                  });
                  if (did) n += 1;
                  // Same projection and identity check as ingest (I01): probe evidence also updates tail/state
                  if (obs.target.attemptId === t.attemptId && obs.target.runId === t.runId) {
                    snap = applyObservationToSnapshot(snap, obs, now).snapshot;
                  }
                }
                if (n > 0) snap.lastSourceSeq = Math.max(snap.lastSourceSeq ?? 0, ...got.map(o => o.sourceSeq));
                tx.updateWatch(watchId, { snapshot: snap }, now);
                return n;
              });
              this.store.transaction(tx => tx.setProbeRunStatus(probeRunId, 'done', { ingested: ingested2, probeId: chosen }));
              // Judge once more (new window)
              const row1 = this.store.transaction(tx => tx.getWatchRow(watchId));
              if (row1) {
                const fp2 = windowFingerprint(row1, now);
                if (fp2 !== fp) {
                  const basis2 = { ...basis, windowDigest: fp2, observationSeq: row1.observationSeq };
                  const state2 = buildSemanticState(row1, this.store.transaction(tx => tx.listObservations(watchId, 32)), this.semanticConfig);
                  try {
                    const j2 = await this.judge!.evaluate(basis2, state2, signal ?? new AbortController().signal);
                    const valid2 = keys.every(k => typeof j2.probabilities[k] === 'number' && j2.probabilities[k] >= 0 && j2.probabilities[k] <= 1);
                    if (valid2) {
                      const applied2 = applyThresholds(j2.probabilities, this.semanticConfig.thresholds, true, applied.nextRepeatingStreak);
                      candidates = applied2.candidates.length > 0 ? applied2.candidates : (applied2.contextLow ? [{ reason: 'evidence.insufficient', probability: j2.probabilities.context_sufficient, note: 'context still insufficient (after probe)' }] : []);
                      probabilities = j2.probabilities;
                    }
                    followUpJudged = true;
                  } catch {
                    followUpJudged = true;
                  }
                }
              }
            } else {
              this.store.transaction(tx => tx.setProbeRunStatus(probeRunId, 'discarded', { reason: 'chosen probe not found' }));
            }
          } else {
            this.store.transaction(tx => tx.setProbeRunStatus(probeRunId, 'discarded', { reason: 'judge chose none' }));
          }
        }
        if (candidates.length === 0 && !followUpJudged) {
          candidates = [{ reason: 'evidence.insufficient', probability: judgment.probabilities.context_sufficient, note: 'context insufficient and no probe available or budget exhausted' }];
        }
      }

      // Record judgment (accepted) + candidate projection (shadow only records, no episode/outbox, design 5.5)
      const mode = row0.spec.policy.semanticMode === 'active' ? 'active' : 'shadow';
      const candidateMap: Record<string, Json> = {};
      for (const c of candidates) candidateMap[c.reason] = { probability: c.probability, note: c.note ?? null } as unknown as Json;
      this.store.transaction(tx => {
        tx.insertJudgment({
          judgmentId: newId('jd'), watchId, generation: row0.generation, missionRevision: row0.missionRevision,
          controlRevision: row0.controlRevision, windowDigest: fp, model: judgment.model, questionSet, status: 'accepted',
          data: {
            probabilities: probabilities as unknown as Record<string, Json>,
            inputTokens: judgment.inputTokens,
            mode,
            candidates: candidateMap,
            followUpJudged
          }, createdAt: now
        });
        const fresh = tx.getWatchRow(watchId);
        if (fresh) {
          tx.updateWatch(watchId, {
            snapshot: {
              ...fresh.snapshot,
              semantic: {
                mode,
                lastWindowDigest: fp,
                lastJudgedAtMs: now,
                lastJudgedSourceSeq: row0.snapshot.lastSourceSeq ?? null,
                lastJudgedObservationSeq: row0.observationSeq,
                repeatingStreak: applied.nextRepeatingStreak,
                meaningfulProgress: probabilities.meaningful_progress,
                candidates: candidateMap,
                consentMissing: false,
                budgetExhausted: false,
                error: null
              }
            }
          }, now);
        }
      });

      // Judgment-complete notice (must be visible during judge review); display failure does not block the engine
      try {
        this.onJudgment?.({
          watchId,
          ownerSession: row0.ownerSession,
          objective: row0.spec.mission.objective,
          mode,
          candidates: candidates.map(c => ({ reason: c.reason, probability: c.probability, note: c.note ?? null })),
          probabilities: probabilities as unknown as Record<string, number>
        });
      } catch { /* display-layer failure does not affect the engine */ }

      // Only in active mode create episode + attention (and only with egress consent); relay not ready -> local outbox (design 5.5/9.3)
      if (mode === 'active' && this.semanticConsent) {
        for (const c of candidates) {
          const episodeId = this.openEpisode(watchId, row0, c.reason, {
            reasonCode: c.reason,
            probability: c.probability,
            note: c.note ?? null,
            windowDigest: fp,
            source: 'jev',
            firstSeenAt: new Date(now).toISOString()
          }, now);
          if (episodeId) {
            // Without delivery, likewise land in the local outbox (handled inside publishEpisodeAttention)
            await this.publishEpisodeAttention(watchId, row0, episodeId, c.reason, `jev ${c.reason}${c.note ? ` (${c.note})` : ''}`, now, c.probability);
          }
        }
      }
    } finally {
      this.semanticInFlight.delete(watchId);
    }
  }

  /** V3: group (design 3.x watch-group) -- reads committed child Watch state. */
  private inspectGroup(watchId: string, row: WatchRow, inspectionId: string, now: number): InspectionOutcome {
    const target = row.spec.target as { kind: 'group'; members: Array<{ watchId: string; generation: number }>; readyWhen: 'all_terminal' | 'all_succeeded' };
    const produced = { episodes: [] as string[], resultCards: [] as string[] };
    const children = target.members.map(m => ({ ref: m, row: this.store.transaction(tx => tx.getWatchRow(m.watchId)) }));
    const missing = children.filter(c => !c.row || c.row.generation !== c.ref.generation);
    if (missing.length > 0) {
      this.degrade(watchId, `group member missing or generation mismatch: ${missing.map(m => m.ref.watchId).join(', ')}`, now);
      this.ensureMonitorEpisode(watchId, row, 'monitor.degraded', `group member missing or generation mismatch: ${missing.map(m => m.ref.watchId).join(', ')}`, now);
    }
    const present = children.filter(c => c.row);
    const states = present.map(c => ({ watchId: c.ref.watchId, taskState: c.row!.snapshot.taskState, lifecycle: c.row!.lifecycle }));
    const allTerminal = present.length > 0 && present.every(c => isTerminalTaskState(c.row!.snapshot.taskState));
    const allSucceeded = present.length > 0 && present.every(c => c.row!.snapshot.taskState === 'succeeded');
    const ready = target.readyWhen === 'all_succeeded' ? allSucceeded : allTerminal;

    const { pollMinMs, pollMaxMs } = row.spec.limits;
    const backoffMs = ready ? pollMaxMs : Math.min((row.snapshot.backoffMs || pollMinMs) * 2, pollMaxMs);
    const snapshot: WatchSnapshot = { ...row.snapshot, backoffMs, members: states, groupReadyAt: ready ? (row.snapshot.groupReadyAt ?? now) : row.snapshot.groupReadyAt ?? null };
    this.store.transaction(tx => tx.updateWatch(watchId, { snapshot, nextDueAt: now + backoffMs, health: missing.length > 0 ? 'degraded' : 'healthy' }, now));

    if (ready && !row.snapshot.groupReadyAt) {
      const episodeId = this.openEpisode(watchId, row, 'dependency.ready', {
        reasonCode: 'dependency.ready',
        readyWhen: target.readyWhen,
        members: states as unknown as never,
        firstSeenAt: new Date(now).toISOString()
      }, now);
      if (episodeId) produced.episodes.push(episodeId);
      const resultId = `result-${watchId}-${row.generation}-group`;
      const card = buildGroupResultCard({
        resultId,
        spec: { ...row.spec, watchId },
        members: states,
        readyWhen: target.readyWhen,
        health: missing.length > 0 ? 'degraded' : 'healthy',
        observedAtMs: now,
        deliveryNote: this.displayNote
      });
      this.commitResultCard(watchId, row, episodeId, resultId, card, now);
      produced.resultCards.push(resultId);
    }
    return { inspectionId, watchId, ingested: 0, produced, health: missing.length > 0 ? 'degraded' : 'healthy', lifecycleAfter: 'active' };
  }

  /** V3: explicit follow-up obligation (pi-obligation-v1 semantics; pure time reminder = obligation without dependencies). */
  private async inspectObligation(watchId: string, row: WatchRow, inspectionId: string, now: number): Promise<InspectionOutcome> {
    const target = row.spec.target as { kind: 'obligation'; dependencies: Array<{ watchId: string; generation: number }>; readyWhen: 'all_terminal' | 'all_succeeded'; hostAction: string };
    const deps = target.dependencies.map(d => ({ ref: d, row: this.store.transaction(tx => tx.getWatchRow(d.watchId)) }));
    const missing = deps.filter(d => !d.row || d.row.generation !== d.ref.generation);
    const present = deps.filter(d => d.row);
    const depsSatisfied =
      target.dependencies.length === 0
        ? true // pure time obligation: no dependencies, fires when due
        : present.length === target.dependencies.length &&
          (target.readyWhen === 'all_succeeded'
            ? present.every(d => d.row!.snapshot.taskState === 'succeeded')
            : present.every(d => isTerminalTaskState(d.row!.snapshot.taskState)));

    const dl = deadlineFact(row.spec.mission, now);
    const due = depsSatisfied && dl.crossed;
    const open = this.store.transaction(tx => tx.getActiveEpisodeBySlot(watchId, row.generation, 'dependency.ready', row.spec.mission.checkpointId));
    const snoozedUntil = open?.snoozeUntil ?? null;
    const recheckDue = snoozedUntil !== null && snoozedUntil <= now;

    const produced = { episodes: [] as string[], resultCards: [] as string[] };
    if (missing.length > 0) {
      this.degrade(watchId, `obligation dependency missing: ${missing.map(m => m.ref.watchId).join(', ')}`, now);
    }
    // If the same slot already has a terminal episode (resolved/superseded), do not reopen -- no permanent looping alarm (design 6.4)
    const priorForSlot = this.store.transaction(tx => tx.listEpisodes(watchId, 100))
      .some(e => e.kind === 'dependency.ready' && e.checkpointId === row.spec.mission.checkpointId);
    if (due && !open && !priorForSlot) {
      // Due and no open/snoozed episode -> open the dependency-ready question (needs explicit ACK; expiry does not auto-swap ID to extend, design 6.4)
      const episodeId = this.openEpisode(watchId, row, 'dependency.ready', {
        reasonCode: 'dependency.ready',
        hostAction: target.hostAction,
        dueAt: row.spec.mission.deadlineAt ?? null,
        dependenciesSatisfied: depsSatisfied,
        firstSeenAt: new Date(now).toISOString()
      }, now);
      if (episodeId) {
        produced.episodes.push(episodeId);
        await this.publishEpisodeAttention(watchId, row, episodeId, 'dependency.ready', `obligation due: ${target.hostAction}`, now);
      }
    } else if (open && open.state === 'snoozed' && recheckDue) {
      // On defer expiry, recheck first: reopen (no permanent looping alarm, design 6.4)
      this.store.transaction(tx => {
        tx.updateEpisode(open.episodeId, open.revision, { state: 'open' }, now);
      });
      produced.episodes.push(open.episodeId);
    }

    // Next due: snooze expiry, deadline, or poll cap
    const { pollMinMs, pollMaxMs } = row.spec.limits;
    let nextDue: number;
    if (open?.state === 'snoozed' && snoozedUntil) nextDue = snoozedUntil;
    else if (!due && row.spec.mission.deadlineAt) nextDue = Math.min(Date.parse(row.spec.mission.deadlineAt), now + pollMaxMs);
    else nextDue = now + pollMinMs;
    const snapshot: WatchSnapshot = { ...row.snapshot, backoffMs: pollMaxMs };
    this.store.transaction(tx => tx.updateWatch(watchId, { snapshot, nextDueAt: nextDue, health: missing.length > 0 ? 'degraded' : 'healthy' }, now));
    // Delivery-layer health check too (an obligation's dependency.ready attention may likewise hang as unknown)
    await this.checkDeliveryHealth(watchId, now);
    return { inspectionId, watchId, ingested: 0, produced, health: missing.length > 0 ? 'degraded' : 'healthy', lifecycleAfter: 'active' };
  }

  /** Unified attention publishing for hard facts / semantic candidates: truth lands in the outbox first, then publishes via relay managed delivery (I6). */
  private async publishEpisodeAttention(
    watchId: string,
    row: WatchRow,
    episodeId: string,
    reasonCode: string,
    summary: string,
    now: number,
    probability?: number
  ): Promise<void> {
    const envelopeId = newId('att');
    const envelope = {
      schemaVersion: 1 as const,
      envelopeId,
      episodeId,
      episodeRevision: 1,
      watchId,
      generation: row.generation,
      missionRevision: row.missionRevision,
      controlRevision: row.controlRevision,
      ownerBindingEpoch: row.ownerBindingEpoch,
      target: row.spec.target,
      reasonCode: reasonCode as never,
      summary: probability !== undefined ? `${summary} (jev p=${probability.toFixed(2)})` : summary,
      evidenceRefs: [],
      requiredNextStep: 'inspect-current-watch-before-acting' as const,
      occurredAt: new Date(now).toISOString(),
      validUntil: new Date(now + (row.spec.policy.attentionTtlMs || DEFAULT_ATTENTION_TTL_MS)).toISOString()
    };
    const bytes = Buffer.from(JSON.stringify(envelope), 'utf8');
    const validUntilMs = now + (row.spec.policy.attentionTtlMs || DEFAULT_ATTENTION_TTL_MS);
    const relayReady = !!this.delivery;
    let transport: AttentionNotice['transport'] = relayReady ? 'relay-managed' : 'local-display';
    let relayError: string | undefined;
    this.store.transaction(tx => {
      if (tx.getOutbox(envelopeId)) return;
      tx.insertOutbox({
        eventId: envelopeId, watchId, episodeId, generation: row.generation, eventType: 'watcher.attention.v1',
        eventBytes: bytes, eventDigest: digestJson(envelope),
        validUntil: validUntilMs,
        admission: relayReady ? 'pending' : 'empty-audience',
        admissionJson: relayReady
          ? { transport: 'relay-managed', pending: true }
          : { transport: 'local-display', note: 'relay managed path not ready; displayed locally (widget/notify); no auto wake (I05/I21)' },
        now
      });
    });
    if (relayReady) {
      // Per-watch scope (design 9.6). Publishing is blocked while a scope advance for this
      // watch is unconfirmed (design 7.7): the old scope must not publish, the new one
      // needs a relay receipt first.
      const scope = await this.ensureRelayScope(watchId, now);
      const r = scope.ok
        ? await this.publishAttention(envelopeId, bytes, now, scope.scope)
        : this.deferAttention(envelopeId, scope.reason, now);
      if (!r.ok) {
        // Delivery failure made loud : truth already landed in outbox (admission=unknown),
        // but a broken channel must be visible -- otherwise the toast still fires while the wake leg is dead (truth layer and delivery layer silently split-brain)
        transport = 'relay-failed';
        relayError = r.error;
        this.degrade(watchId, `relay publish failed: ${r.error}`, now);
      }
    }
    // Notification hook: regardless of transport, broadcast to the display layer when a fact needs host attention (toast/widget; relay wake takes a separate managed path)
    try {
      this.onAttention?.({
        watchId,
        ownerSession: row.ownerSession,
        episodeId,
        reasonCode,
        summary,
        transport,
        relayError,
        validUntil: validUntilMs
      });
    } catch { /* display-layer failure does not affect the engine */ }
  }

  /**
   * Publishes an attention through the relay managed path; only advances admission, never the
   * frozen truth (I06). The scope used is recorded in admission_json so a retry re-sends the
   * identical (event, options) pair. Never throws: returns status for loud failure handling.
   */
  private async publishAttention(
    eventId: string,
    bytes: Buffer,
    now: number,
    scope?: { scopeId: string; revision: number }
  ): Promise<{ ok: true } | { ok: false; error: string }> {
    if (scope) {
      this.store.transaction(tx => tx.mergeOutboxAdmissionJson(eventId, {
        scope: { scopeId: scope.scopeId, revision: scope.revision }, deferred: null
      }, now));
    }
    try {
      const receipt = await this.delivery!.publish(new Uint8Array(bytes), scope);
      const admission =
        receipt.sourceState === 'captured'
          ? (receipt.routes.some(r => (r as { admission?: string }).admission === 'accepted') ? 'source-staged' : 'empty-audience')
          : receipt.sourceState === 'empty_audience' ? 'empty-audience'
            : receipt.sourceState === 'rejected' ? 'rejected'
              : 'unknown';
      this.store.transaction(tx => {
        const prev = tx.getOutbox(eventId)?.admissionJson ?? {};
        tx.setOutboxAdmission(eventId, admission, { ...prev, receipt: receipt as unknown as Json, error: null }, now);
      });
      return { ok: true };
    } catch (e) {
      const error = e instanceof Error ? e.message : String(e);
      const code = (e as { code?: string }).code;
      this.store.transaction(tx => {
        const prev = tx.getOutbox(eventId)?.admissionJson ?? {};
        // 'cancelled' = the event id was tombstoned by a withdraw before capture (I20):
        // that is an authoritative negative, not an unknown admission.
        tx.setOutboxAdmission(eventId, code === 'cancelled' ? 'rejected' : 'unknown', { ...prev, error, errorCode: code ?? null }, now);
      });
      return { ok: false, error };
    }
  }

  /** Records that an attention could not be published yet (scope control unconfirmed). */
  private deferAttention(eventId: string, reason: string, now: number): { ok: false; error: string } {
    this.store.transaction(tx => tx.mergeOutboxAdmissionJson(eventId, { deferred: reason }, now));
    return { ok: false, error: reason };
  }

  /**
   * Returns the watch's relay scope, creating it on first use (expectedRevision=0 ->
   * nextRevision=1, design 9.6). Unconfirmed control ops for the watch are drained first;
   * if any remain open, or the scope is not active, publishing is blocked (design 7.7).
   * scopeId = scope-<watchId>-g<generation>-e<bindingEpoch>: a generation or owner-binding
   * change yields a new scope, and the old one is never reused.
   */
  private async ensureRelayScope(
    watchId: string,
    now: number
  ): Promise<{ ok: true; scope: { scopeId: string; revision: number } } | { ok: false; reason: string }> {
    await this.drainControl(watchId);
    const open = this.store.transaction(tx => tx.listOpenControlOutbox(watchId));
    if (open.length > 0) {
      return { ok: false, reason: `scope control ${open[0].operationId} unconfirmed (${open[0].status}); publish deferred` };
    }
    const row = this.store.transaction(tx => tx.getWatchRow(watchId));
    if (!row) return { ok: false, reason: 'watch not found' };
    const scopeId = `scope-${watchId}-g${row.generation}-e${row.ownerBindingEpoch}`;
    const current = row.snapshot.relayScope;
    if (current && current.scopeId === scopeId) {
      return current.state === 'active'
        ? { ok: true, scope: { scopeId, revision: current.revision } }
        : { ok: false, reason: `relay scope ${current.state}` };
    }
    // Create: durable intent first, then the relay call.
    const operationId = newId('ctl');
    this.store.transaction(tx => {
      tx.insertControlOutbox({ operationId, watchId, scopeId, expectedRevision: 0, nextRevision: 1, requestedState: 'active', now });
      const fresh = tx.getWatchRow(watchId);
      if (fresh) {
        tx.updateWatch(watchId, {
          snapshot: { ...fresh.snapshot, relayScope: { scopeId, revision: 1, state: 'active' } }
        }, now);
      }
    });
    await this.drainControl(watchId);
    const stillOpen = this.store.transaction(tx => tx.listOpenControlOutbox(watchId));
    if (stillOpen.length > 0) {
      return { ok: false, reason: `scope create ${operationId} unconfirmed; publish deferred` };
    }
    return { ok: true, scope: { scopeId, revision: 1 } };
  }

  /**
   * Drains the durable control outbox (design 7.3 T6 / 7.7): scope advances FIFO per watch,
   * then pending event withdraws. Each op is retried under its original operationId; the
   * relay dedupes by operationId + request digest. Deterministic relay refusals are recorded
   * as rejected (no retry); anything else stays unknown and is retried on the next sweep.
   */
  async drainControl(watchId?: string): Promise<void> {
    const delivery = this.delivery;
    if (!delivery) return;
    const ops = this.store.transaction(tx => tx.listOpenControlOutbox(watchId));
    const blocked = new Set<string>();
    for (const op of ops) {
      if (blocked.has(op.scopeId)) continue; // FIFO per scope
      try {
        const result = await delivery.advanceScope(op.operationId, op.scopeId, op.expectedRevision, op.nextRevision, op.requestedState);
        this.store.transaction(tx => tx.completeControlOutbox(op.operationId, 'source-applied', { relay: result }));
      } catch (e) {
        const code = (e as { code?: string }).code ?? null;
        const error = e instanceof Error ? e.message : String(e);
        const deterministic = code !== null && DETERMINISTIC_RELAY_CODES.has(code);
        this.store.transaction(tx => tx.completeControlOutbox(op.operationId, deterministic ? 'rejected' : 'unknown', { error, code }));
        blocked.add(op.scopeId);
      }
    }
    const withdraws = this.store.transaction(tx => tx.listOutboxAwaitingWithdraw(64))
      .filter(o => watchId === undefined || o.watchId === watchId);
    for (const o of withdraws) {
      const w = o.admissionJson.withdraw as { operationId: string; reason: string } | undefined;
      if (!w) continue;
      try {
        const result = await delivery.withdraw(w.operationId, o.eventId, w.reason);
        this.store.transaction(tx => tx.mergeOutboxAdmissionJson(o.eventId, {
          withdraw: { ...w, result: result as unknown as Json, error: null }
        }, this.clock.wallNow()));
      } catch (e) {
        const error = e instanceof Error ? e.message : String(e);
        this.store.transaction(tx => tx.mergeOutboxAdmissionJson(o.eventId, {
          withdraw: { ...w, result: null, error }
        }, this.clock.wallNow()));
      }
    }
  }

  /**
   * Delivery health: for attentions that are unknown (publish failed)
   * or deferred (scope control unconfirmed) while the episode is still open, each sweep
   * (a) retries once if the envelope is still valid, re-sending the same (event, options);
   * (b) otherwise only reports loudly (the host must inspect).
   * Withdrawn events are never re-published; an event whose recorded scope has since
   * advanced is fenced and marked rejected instead of being re-sent under a new scope.
   */
  private async checkDeliveryHealth(watchId: string, now: number): Promise<void> {
    if (!this.delivery) return;
    const pending = this.store.transaction(tx => tx.listOutboxByWatch(watchId, 20))
      .filter(o => o.eventType === 'watcher.attention.v1'
        && (o.admission === 'unknown' || (o.admission === 'pending' && typeof o.admissionJson.deferred === 'string'))
        && !o.admissionJson.withdraw);
    for (const o of pending) {
      if (o.createdAt >= now) continue; // attempted this cycle already; retry from the next one
      const epId = o.episodeId;
      if (!epId) continue;
      const ep = this.store.transaction(tx => tx.getEpisode(epId));
      if (!ep || (ep.state !== 'open' && ep.state !== 'snoozed' && ep.state !== 'acknowledged')) continue;
      if (o.validUntil <= now) {
        this.degrade(watchId, `relay publish failed (envelope expired ${new Date(o.validUntil).toISOString()}); host must inspect`, now);
        continue;
      }
      const recorded = o.admissionJson.scope as { scopeId: string; revision: number } | undefined;
      let r: { ok: true } | { ok: false; error: string };
      if (recorded) {
        const live = this.store.transaction(tx => tx.getWatchRow(watchId))?.snapshot.relayScope;
        if (!live || live.scopeId !== recorded.scopeId || live.revision !== recorded.revision) {
          this.store.transaction(tx => {
            const prev = tx.getOutbox(o.eventId)?.admissionJson ?? {};
            tx.setOutboxAdmission(o.eventId, 'rejected', { ...prev, note: 'fenced: scope advanced after first publish attempt; not re-sent' }, now);
          });
          continue;
        }
        r = await this.publishAttention(o.eventId, o.eventBytes, now, recorded);
      } else if (typeof o.admissionJson.deferred === 'string') {
        const scope = await this.ensureRelayScope(watchId, now);
        r = scope.ok ? await this.publishAttention(o.eventId, o.eventBytes, now, scope.scope) : this.deferAttention(o.eventId, scope.reason, now);
      } else {
        // Legacy row first published under the source-wide scope: same options on retry.
        r = await this.publishAttention(o.eventId, o.eventBytes, now);
      }
      if (!r.ok) this.degrade(watchId, `relay publish failed: ${r.error}`, now);
    }
  }

  /** Terminal-state watch auto-close: supersede open episodes -> superseded, lifecycle -> closed (does not touch controlRevision CAS semantics). */
  private autoCloseTerminal(watchId: string, now: number): void {
    this.store.transaction(tx => {
      const fresh = tx.getWatchRow(watchId);
      if (!fresh || fresh.lifecycle !== 'active') return;
      if (!isTerminalTaskState(fresh.snapshot.taskState)) return;
      tx.supersedeEpisodesOfWatch(watchId, now);
      tx.updateWatch(watchId, { lifecycle: 'closed' }, now);
    });
  }

  private degrade(watchId: string, reason: string, now: number): void {
    this.store.transaction(tx => {
      const fresh = tx.getWatchRow(watchId);
      if (!fresh) return;
      tx.updateWatch(watchId, {
        health: 'degraded',
        snapshot: { ...fresh.snapshot, degradedReason: reason }
      }, now);
    });
  }

  /**
   * Delivery guard: there is a wake the host never confirmed -- episode still open and its
   * attention admission is in {unknown, pending, publishing} (publish unconfirmed) or =source-staged
   * and the envelope has not expired (relay delivery in flight) -> returns true, blocking ttlElapsed auto-close.
   */
  private hasUndeliveredWake(watchId: string, now: number): boolean {
    const rows = this.store.transaction(tx => tx.listOutboxByWatch(watchId, 50))
      .filter(o => o.eventType === 'watcher.attention.v1'
        && (o.admission === 'unknown' || o.admission === 'pending' || o.admission === 'publishing'
          || (o.admission === 'source-staged' && o.validUntil > now)));
    if (rows.length === 0) return false;
    const openEpisodes = new Set(this.store.transaction(tx => tx.listEpisodes(watchId, 100))
      .filter(e => e.state === 'open').map(e => e.episodeId));
    return rows.some(o => o.episodeId !== null && openEpisodes.has(o.episodeId));
  }

  /** slot = (watchId, generation, kind, checkpointId); at most one active episode per slot (design 3.3). */
  private openEpisode(
    watchId: string,
    row: WatchRow,
    kind: string,
    body: Record<string, Json>,
    now: number
  ): string | null {
    const checkpointId = row.spec.mission.checkpointId;
    return this.store.transaction(tx => {
      const existing = tx.getActiveEpisodeBySlot(watchId, row.generation, kind, checkpointId);
      if (existing) {
        // Repeated evidence for the same episode only updates lastSeen, does not open a new issue
        const merged = { ...existing.body, lastSeenAt: new Date(now).toISOString() };
        tx.updateEpisode(existing.episodeId, existing.revision, { body: merged }, now);
        return null;
      }
      const episodeId = newId('ep');
      const ordinal = tx.nextEpisodeOrdinal(watchId, row.generation, kind, checkpointId);
      tx.insertEpisode({
        episodeId, watchId, generation: row.generation, kind, checkpointId, ordinal,
        body: { ...body, lastSeenAt: new Date(now).toISOString() }, now
      });
      return episodeId;
    });
  }

  private ensureMonitorEpisode(watchId: string, row: WatchRow, kind: string, detail: string, now: number): void {
    const opened = this.openEpisode(watchId, row, kind, {
      reasonCode: kind,
      detail,
      firstSeenAt: new Date(now).toISOString()
    }, now);
    void opened;
  }

  /** Result card: truth = durable outbox (I6: event bytes/ID/expiry frozen); the display copy is persisted separately (I16). */
  private commitResultCard(
    watchId: string,
    row: WatchRow,
    episodeId: string | null,
    resultId: string,
    card: ReturnType<typeof buildResultCard> | ReturnType<typeof buildGroupResultCard>,
    now: number
  ): void {
    const bytes = Buffer.from(JSON.stringify(card), 'utf8');
    const validUntil = now + 24 * 3600 * 1000; // initial result display retention 24h (design 9.3)
    this.store.transaction(tx => {
      const existing = tx.getOutbox(resultId);
      if (existing) return;
      tx.insertOutbox({
        eventId: resultId,
        watchId,
        episodeId,
        generation: row.generation,
        eventType: 'watcher.result.v1',
        eventBytes: bytes,
        eventDigest: digestJson(card),
        validUntil,
        admissionJson: {
          transport: 'local-display',
          note: 'relay managed path not negotiated; no auto-resume (I05/I21)'
        },
        now
      });
    });
    this.store.writeResultCard(watchId, resultId, card as unknown as Json);
  }

  /** probes delegation (trusted candidates, design 4.4) */
  async listProbes(watchId: string): Promise<readonly Probe[]> {
    const row = this.store.transaction(tx => tx.getWatchRow(watchId));
    if (!row || row.spec.target.kind !== 'run') return [];
    const adapter = this.adapters.get(row.spec.target.sourceId);
    if (!adapter) return [];
    const basis = {
      watchId: row.watchId,
      generation: row.generation,
      missionRevision: row.missionRevision,
      controlRevision: row.controlRevision,
      observationSeq: row.observationSeq,
      windowDigest: digestJson(row.snapshot)
    };
    return adapter.probes(row.spec.target, basis);
  }

  async executeProbe(watchId: string, probeId: string, signal?: AbortSignal): Promise<Observation[]> {
    const probes = await this.listProbes(watchId);
    const probe = probes.find(p => p.probeId === probeId);
    if (!probe) return [];
    const row = this.store.transaction(tx => tx.getWatchRow(watchId));
    if (!row) return [];
    const adapter = this.adapters.get((row.spec.target as RunTarget).sourceId);
    if (!adapter) return [];
    // Re-check revision/expiry before execution (design 4.4)
    if (Date.parse(probe.expiresAt) < this.clock.wallNow()) return [];
    return adapter.execute(probe, signal ?? new AbortController().signal);
  }
}

/**
 * Projects one identity-checked observation onto the snapshot. Shared by ingest and probe
 * ingestion so both paths derive the same state from the same evidence. activityAtMs is set
 * when the observation counts as task activity (status/heartbeat/log_delta) for the silence
 * rule; validation/artifact observations do not.
 */
export function applyObservationToSnapshot(
  snapshot: WatchSnapshot,
  obs: Observation,
  now: number
): { snapshot: WatchSnapshot; activityAtMs: number | null } {
  const atMs = Date.parse(obs.observedAt) || now;
  let next: WatchSnapshot = snapshot;
  let activityAtMs: number | null = null;
  if (obs.kind === 'status') {
    const st = statusPayloadToTaskState(obs.payload);
    next = { ...next, ...st, lastSourceSeq: obs.sourceSeq };
    activityAtMs = atMs;
  } else if (obs.kind === 'validation') {
    const p = (obs.payload ?? {}) as Record<string, unknown>;
    const checkId = typeof p.checkId === 'string' ? p.checkId : null;
    if (checkId) {
      const checks = [...(next.checks ?? [])];
      const idx = checks.findIndex(c => c.checkId === checkId);
      const entry = {
        checkId,
        outcome: String(p.outcome ?? 'unknown'),
        artifactDigest: typeof p.artifactDigest === 'string' ? p.artifactDigest : null
      };
      if (idx >= 0) checks[idx] = entry;
      else checks.push(entry);
      next = { ...next, checks };
    }
  } else if (obs.kind === 'artifact_manifest') {
    const p = (obs.payload ?? {}) as Record<string, unknown>;
    const arts = Array.isArray(p.artifacts) ? p.artifacts : [];
    const ids = arts
      .filter((a): a is Record<string, unknown> => !!a && typeof a === 'object')
      .map(a => String(a.artifactId ?? ''))
      .filter(x => x !== '');
    next = { ...next, artifactIds: ids };
  } else if (obs.kind === 'heartbeat') {
    activityAtMs = atMs;
  } else if (obs.kind === 'log_delta') {
    const p = (obs.payload ?? {}) as Record<string, unknown>;
    const deltaLines = Array.isArray(p.lines)
      ? (p.lines as unknown[]).filter((l): l is string => typeof l === 'string')
      : [];
    if (deltaLines.length > 0) {
      next = { ...next, tailLines: [...(next.tailLines ?? []), ...deltaLines].slice(-20) };
    }
    activityAtMs = atMs;
  }
  if (atMs > (next.lastObservedAtMs ?? 0)) {
    next = { ...next, lastObservedAtMs: atMs };
  }
  return { snapshot: next, activityAtMs };
}
