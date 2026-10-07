/**
 * Watcher-side relay integration (design 09 / relay-next 02 s2.2, 4-stage managed delivery).
 *
 * Division of responsibilities:
 * - This module (watcher repo): embedded managed SourceHost, consumer declaration file, invite production,
 *   audience/scope configuration (owner surface), and the ManagedDeliveryPort implementation (publish/withdraw/receipt/
 *   host-response consumption/applied confirmation).
 * - relay repo (done): target-side admit/guard/pump/respond, Pi session pump wiring
 *   (stage 4), declarative consumer registration (<relay-home>/consumers).
 *
 * Discipline: wake goes only through the relay managed path (I05/I21); this module never touches pi sendMessage.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
import { createSource, resetSource, SourceAuthorityMismatch, type SourceHost } from '@maolon/pi-relay/source';
import { writeConsumerDeclaration, declarationFile } from '@maolon/pi-relay/consumer';
import { secret, newId as relayNewId } from '@maolon/pi-relay/protocol';
import type {
  Id, Json, ManagedDeliveryPort, VerifiedConsumerResponse, HostAck, AttentionEnvelope
} from '../contracts/interfaces.js';
import type {
  ManagedReceipt, WithdrawResult, ApplicationResult
} from '../contracts/relay-next.interfaces.js';

/** Structurally identical to the relay protocol's managed-types (the protocol entry does not re-export the types, so they are used structurally) */
interface ManagedEvent {
  kind: 'event'; id: string; type: string; schemaVersion: number;
  occurredAt: string; validUntil: string; data: Record<string, unknown>;
}
interface RouteReceiptShape {
  routeRef: string; targetRevision: number; freshness: string; admission: string; delivery: string; withdrawal: string;
}
interface ConsumerResponseShape {
  responseId: string; deliveryRef: string; responseType: string; data: Record<string, unknown>;
}
interface RelayManagedReceiptShape {
  eventId: string; sourceState: string; sourceCursor: number;
  scope: { id: string; revision: number };
  routes: RouteReceiptShape[];
  responses?: ConsumerResponseShape[];
}

export interface WatcherRelayOptions {
  /** relay home (must match the host session's relay target; default ~/.pi/relay) */
  relayHome: string;
  realm?: string;
  sourceId?: string;
  channelId?: string;
  consumerProfileId?: string;
}

interface RelaySecrets { ownerToken: string; publisherToken: string }
interface RelaySetup {
  consumerProfileId: string;
  audienceRef: string;
  scopeId: string;
  scopeRevision: number;
  inviteFile?: string;
  provisioned: boolean;
}

/** watcher application event TypeManifest (design 9.3: watcher.attention/result.v1 is the application-layer manifest) */
interface TypeManifestShape {
  type: string;
  schemaVersion: number;
  kind: 'event';
  dataSchema: Record<string, unknown>;
}

const envelopeProps = {
  schemaVersion: { type: 'integer', minimum: 1, maximum: 1 },
  envelopeId: { type: 'string', minLength: 1, maxLength: 160 },
  episodeId: { type: 'string', minLength: 1, maxLength: 160 },
  episodeRevision: { type: 'integer', minimum: 1 },
  watchId: { type: 'string', minLength: 1, maxLength: 160 },
  generation: { type: 'integer', minimum: 1 },
  missionRevision: { type: 'integer', minimum: 1 },
  controlRevision: { type: 'integer', minimum: 1 },
  ownerBindingEpoch: { type: 'integer', minimum: 1 },
  target: { type: 'object' },
  reasonCode: { type: 'string', minLength: 1, maxLength: 64 },
  summary: { type: 'string', maxLength: 2048 },
  evidenceRefs: { type: 'array', maxItems: 64, items: { type: 'object' } },
  requiredNextStep: { type: 'string', maxLength: 64 },
  occurredAt: { type: 'string', maxLength: 64 },
  validUntil: { type: 'string', maxLength: 64 }
};

export const WATCHER_TYPE_MANIFESTS: readonly TypeManifestShape[] = [
  {
    type: 'watcher.attention.v1',
    schemaVersion: 1,
    kind: 'event',
    dataSchema: {
      type: 'object',
      additionalProperties: false,
      properties: envelopeProps,
      required: ['schemaVersion', 'envelopeId', 'episodeId', 'episodeRevision', 'watchId', 'generation', 'missionRevision', 'controlRevision', 'ownerBindingEpoch', 'target', 'reasonCode', 'summary', 'occurredAt', 'validUntil']
    }
  },
  {
    type: 'watcher.result.v1',
    schemaVersion: 1,
    kind: 'event',
    dataSchema: {
      type: 'object',
      additionalProperties: false,
      properties: envelopeProps,
      required: ['schemaVersion', 'envelopeId', 'episodeId', 'watchId', 'generation', 'reasonCode', 'summary', 'occurredAt', 'validUntil']
    }
  }
];

export const WATCHER_EVENT_TYPES = ['watcher.attention.v1', 'watcher.result.v1'] as const;

const OWNER = { kind: 'owner' } as const;

function readJson<T>(p: string): T | undefined {
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8')) as T;
  } catch {
    return undefined;
  }
}

function writePrivate(p: string, value: unknown): void {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(value, null, 2), { mode: 0o600 });
}

/** Per-root sourceId: with a global extension, every pi session starts a watcher runtime for its own cwd root.
 * If all roots shared the sourceId 'pi-watcher', the same source store in one relay home would be
 * held exclusively (owner lock) by the first process that opens it until it exits, so an idle session of an unrelated project could
 * permanently block roots that really need relay.
 * Root primary election ensures only one runtime per root opens the store -> one sourceId per root structurally eliminates cross-root contention.
 * The charset must satisfy the relay bind-request validation /^[A-Za-z0-9][A-Za-z0-9._:-]*$/. */
export function deriveSourceId(rootDir: string): string {
  let slug = path.basename(path.resolve(rootDir))
    .toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 24);
  if (!slug) slug = 'root';
  // A brand-new root directory may not exist yet (realpath fails): fall back to the resolved path to stay stable
  let real = path.resolve(rootDir);
  try { real = fs.realpathSync(real); } catch { /* not created -> use the resolved value */ }
  const h = createHash('sha256').update(real).digest('hex').slice(0, 8);
  return `pi-watcher:${slug}-${h}`;
}

/**
 * Unlimited validity for the local-domain managed audience sentinel.
 * Value = pi-relay FOREVER / STANDING_EXPIRES_AT = MAX_SAFE_INTEGER (accepted on localTrust
 * channels in current pi-relay, stored as 9007199254740991, aligned with standing membership).
 * Monitoring channel lifetime must be >= the task horizon: a 24h wall silently kills the wake chain in long sessions.
 * This source's channel is always localTrust:true (same-uid standing on this machine); if a non-localTrust
 * channel is introduced in the future, this choice must be tied to the channel trust level. Remote/invite domains are still guarded by the relay-side 24h cap.
 */
export const LOCAL_AUDIENCE_VALID_UNTIL_MS = 9007199254740991;

export class WatcherRelaySource implements ManagedDeliveryPort {
  readonly host: SourceHost;
  private readonly opts: Required<WatcherRelayOptions>;
  private readonly secretsPath: string;
  private readonly setupPath: string;
  private setup: RelaySetup;

  private constructor(host: SourceHost, opts: Required<WatcherRelayOptions>, secretsPath: string, setupPath: string, setup: RelaySetup) {
    this.host = host;
    this.opts = opts;
    this.secretsPath = secretsPath;
    this.setupPath = setupPath;
    this.setup = setup;
  }

  static async open(watcherRoot: string, options: WatcherRelayOptions): Promise<WatcherRelaySource> {
    const realPath = (p: string): string => {
      try {
        return fs.realpathSync(p);
      } catch {
        return p;
      }
    };
    const opts: Required<WatcherRelayOptions> = {
      // The relay private-path policy requires a stable realpath (macOS /tmp -> /private/tmp); create the directory first, then take the real path
      relayHome: (() => {
        const p = path.resolve(options.relayHome);
        fs.mkdirSync(p, { recursive: true, mode: 0o700 });
        // An existing directory (e.g. pre-created externally) does not inherit 0700; fill in the private-boundary requirement
        try {
          fs.chmodSync(p, 0o700);
        } catch { /* best effort; the boundary check will report an error as a fallback */ }
        return realPath(p);
      })(),
      realm: options.realm ?? 'local',
      sourceId: options.sourceId ?? 'pi-watcher',
      channelId: options.channelId ?? 'W',
      consumerProfileId: options.consumerProfileId ?? 'pi-watcher-host'
    };
    fs.mkdirSync(watcherRoot, { recursive: true });
    watcherRoot = realPath(path.resolve(watcherRoot));
    const secretsPath = path.join(watcherRoot, 'relay-secrets.json');
    const setupPath = path.join(watcherRoot, 'relay-setup.json');
    const existingSecrets = readJson<RelaySecrets>(secretsPath);
    const secrets: RelaySecrets = existingSecrets ?? {
      ownerToken: secret(),
      publisherToken: secret()
    };
    if (!existingSecrets) writePrivate(secretsPath, secrets);
    const setup: RelaySetup = readJson<RelaySetup>(setupPath) ?? {
      consumerProfileId: opts.consumerProfileId,
      audienceRef: 'watcher-main',
      scopeId: 'watcher-scope-1',
      scopeRevision: 0,
      provisioned: false
    };
    const sourceConfig = {
      version: 1,
      sourceId: opts.sourceId,
      realm: opts.realm,
      home: opts.relayHome,
      ownerToken: secrets.ownerToken,
      publisherTokens: { [opts.channelId]: secrets.publisherToken },
      channels: [{
        id: opts.channelId,
        types: WATCHER_TYPE_MANIFESTS as never[],
        allowedModes: ['display', 'resume'],
        maxAutoTargets: 4,
        // Local standing binding: same-uid sessions on this machine can skip
        // the invite and bind standing directly; remote/non-opted-in channels still go through the invite ceremony
        localTrust: true
      }]
    } as never;
    let host: SourceHost;
    try {
      host = await createSource(sourceConfig, {});
    } catch (e) {
      // Channel config drift (an existing store predates localTrust, rejected by relay's channel digest invariant):
      // this source is owned by us and the owner/publisher tokens match (authority mismatch is still thrown as-is).
      // Automatic recovery: quarantine the old store (auditable, into .quarantine) -> rebuild with the new config; the standing path
      // needs no old invite/membership and is fully rebuilt automatically.
      const isChannelDrift = e instanceof SourceAuthorityMismatch === false
        && (e as { code?: string }).code === 'invalid_state';
      if (!isChannelDrift) throw e;
      const reset = resetSource(opts.sourceId, opts.relayHome);
      if (!reset.reset) throw e;
      host = await createSource(sourceConfig, {});
    }
    return new WatcherRelaySource(host, opts, secretsPath, setupPath, setup);
  }

  // --- owner configuration surface (not a model action; called by CLI relay-setup)---

  /** Declarative consumer registration (relay stage 4 generic config surface: <relay-home>/consumers); idempotent: returns if it already exists. */
  ensureConsumerDeclaration(force = false): { file: string; existed: boolean } {
    const file = declarationFile(this.opts.relayHome, this.setup.consumerProfileId);
    const existed = fs.existsSync(file);
    if (existed && !force) return { file, existed: true };
    writeConsumerDeclaration(this.opts.relayHome, {
      profileId: this.setup.consumerProfileId,
      displayName: 'pi-watcher host session',
      description: 'Pi host session: consumes watcher attention/result events, responds watcher.response.v1',
      eventTypes: [...WATCHER_EVENT_TYPES],
      responseTypes: ['watcher.response.v1'],
      requestedMode: 'resume',
      policy: {
        admission: 'auto',
        // Local-domain transition policy (2026-09-21): bindLocal's standing registration does not yet issue a target->source
        // proof credential (pi-relay internal.source.proof only recognizes the invite principal);
        // requireCurrentScope:true would make the pump defer forever (observed in production). Same-uid in the local domain
        // can already read the source store, so a wider staleness window is accepted; revert to true once the official standing proof credential lands.
        requireCurrentScope: false,
        timeoutMs: 1000
      }
    }, { force: true });
    return { file, existed };
  }

  /** Produce a one-time invite (the file is placed in capabilities/ by the relay source; hand it to the session model's relay_bindings bind). */
  createInvite(): { inviteFile: string } {
    const invite = this.host.core.createInvite({
      operationId: relayNewId('op'),
      channelId: this.opts.channelId,
      ttlMs: 600_000,
      bindingTtlMs: 86_400_000,
      allowResume: true
    });
    const sourceStoreDir = path.join(this.opts.relayHome, 'sources', sha256Hex(this.opts.sourceId));
    const inviteFile = path.join(sourceStoreDir, 'capabilities', `${invite.inviteId}.json`);
    this.setup.inviteFile = inviteFile;
    this.persistSetup();
    return { inviteFile };
  }

  /** audience + scope configuration (idempotent; requires the session target to be bound, since the route set is frozen from the active membership). */
  provision(): { audienceRef: string; scopeId: string; scopeRevision: number } {
    this.host.managed.provisionAudience(OWNER, {
      audienceRef: this.setup.audienceRef,
      channelId: this.opts.channelId,
      consumerProfileId: this.setup.consumerProfileId,
      requestedMode: 'resume',
      // Plan A (2026-09-23): the local-domain sentinel has unlimited validity and no longer hits the 24h wall; see the LOCAL_AUDIENCE_VALID_UNTIL_MS comment
      validUntilMs: LOCAL_AUDIENCE_VALID_UNTIL_MS
    });
    if (this.setup.scopeRevision === 0) {
      this.host.managed.advanceScope(OWNER, {
        operationId: relayNewId('op'),
        scopeId: this.setup.scopeId,
        expectedRevision: 0,
        nextRevision: 1,
        state: 'active'
      });
      this.setup.scopeRevision = 1;
    }
    this.setup.provisioned = true;
    this.persistSetup();
    return { audienceRef: this.setup.audienceRef, scopeId: this.setup.scopeId, scopeRevision: this.setup.scopeRevision };
  }

  /** Whether a binding membership already exists (precondition for provision; the route set is frozen from the active membership). */
  hasActiveMembership(): boolean {
    const status = this.host.core.status() as { memberships?: { n?: number } };
    return (status?.memberships?.n ?? 0) > 0;
  }

  /**
   * Diagnose relay truth: read the source store only and report the actual state of the audience/scope
   * the setup points at, plus the active membership route set. After resume the local setup may be stale (closed).
   */
  diagnose(): {
    audienceRef: string | null;
    audienceState: string | null;
    routeSet: string[];
    scopeId: string | null;
    scopeState: string | null;
    activeMemberships: number;
  } {
    const storePath = path.join(this.opts.relayHome, 'sources', sha256Hex(this.opts.sourceId), 'source.sqlite');
    if (!fs.existsSync(storePath)) {
      return { audienceRef: null, audienceState: null, routeSet: [], scopeId: null, scopeState: null, activeMemberships: 0 };
    }
    const db = new Database(storePath, { readonly: true });
    try {
      const audience = db.prepare('SELECT state, route_set_json FROM managed_audiences WHERE audience_ref=?')
        .get(this.setup.audienceRef) as { state: string; route_set_json: string } | undefined;
      const scope = db.prepare('SELECT state FROM managed_scopes WHERE scope_id=?')
        .get(this.setup.scopeId) as { state: string } | undefined;
      const memberships = db.prepare("SELECT COUNT(*) AS n FROM memberships WHERE state='active'")
        .get() as { n: number };
      return {
        audienceRef: audience ? this.setup.audienceRef : null,
        audienceState: audience?.state ?? null,
        routeSet: audience ? (JSON.parse(audience.route_set_json) as string[]) : [],
        scopeId: scope ? this.setup.scopeId : null,
        scopeState: scope?.state ?? null,
        activeMemberships: memberships.n
      };
    } finally {
      db.close();
    }
  }

  /**
   * resume/reopen self-heal: local setup claims provisioned
   * but relay truth is closed/missing (explicitly closed on last exit, or the store evolved) -> switch to a fresh
   * audienceRef + scopeId and reconfigure (closed never reopens / routeSet is frozen and immutable -> healing = new identity).
   * Precondition: an active membership is required (routeSet is frozen from the current membership; with no binding, auto-bind first).
   * Idempotent: no-op when healthy. Returns whether a reconfiguration was performed.
   */
  ensureHealed(): { healed: boolean; reason?: string } {
    if (!this.setup.provisioned) return { healed: false, reason: 'not provisioned' };
    const d = this.diagnose();
    const needsHeal = d.audienceState !== 'open' || d.scopeState !== 'active';
    if (!needsHeal) return { healed: false, reason: 'healthy' };
    if (d.activeMemberships === 0) return { healed: false, reason: 'no active membership; re-bind first' };
    this.reprovision();
    return { healed: true };
  }

  /** Reconfigure with a fresh audienceRef + scopeId (healing primitive; idempotency is guaranteed by ensureHealed's health check). */
  private reprovision(): { audienceRef: string; scopeId: string; scopeRevision: number } {
    const bump = (base: string): string => {
      const m = /^(.*?)(?:-(\d+))?$/.exec(base);
      const n = m?.[2] ? parseInt(m[2], 10) + 1 : 2;
      return `${m?.[1]}-${n}`;
    };
    this.setup.audienceRef = bump(this.setup.audienceRef);
    this.setup.scopeId = bump(this.setup.scopeId);
    this.setup.scopeRevision = 0;
    this.host.managed.provisionAudience(OWNER, {
      audienceRef: this.setup.audienceRef,
      channelId: this.opts.channelId,
      consumerProfileId: this.setup.consumerProfileId,
      requestedMode: 'resume',
      validUntilMs: LOCAL_AUDIENCE_VALID_UNTIL_MS
    });
    this.host.managed.advanceScope(OWNER, {
      operationId: relayNewId('op'),
      scopeId: this.setup.scopeId,
      expectedRevision: 0,
      nextRevision: 1,
      state: 'active'
    });
    this.setup.scopeRevision = 1;
    this.setup.provisioned = true;
    this.persistSetup();
    return { audienceRef: this.setup.audienceRef, scopeId: this.setup.scopeId, scopeRevision: this.setup.scopeRevision };
  }

  ready(): boolean {
    return this.setup.provisioned && this.setup.scopeRevision > 0;
  }

  statusJson(): Json {
    return {
      relayHome: this.opts.relayHome,
      realm: this.opts.realm,
      sourceId: this.opts.sourceId,
      consumerProfileId: this.setup.consumerProfileId,
      audienceRef: this.setup.audienceRef,
      scopeId: this.setup.scopeId,
      scopeRevision: this.setup.scopeRevision,
      provisioned: this.setup.provisioned,
      inviteFile: this.setup.inviteFile ?? null,
      activeMembership: this.hasActiveMembership()
    } as unknown as Json;
  }

  private persistSetup(): void {
    writePrivate(this.setupPath, this.setup);
  }

  // --- ManagedDeliveryPort (watcher contract)---

  private options(scope?: { scopeId: Id; revision: number }) {
    return {
      audienceRef: this.setup.audienceRef,
      scope: scope
        ? { id: scope.scopeId, revision: scope.revision }
        : { id: this.setup.scopeId, revision: this.setup.scopeRevision },
      consumerProfileId: this.setup.consumerProfileId,
      requestedMode: 'resume' as const
    };
  }

  private mapReceipt(r: RelayManagedReceiptShape): ManagedReceipt {
    return {
      eventId: r.eventId,
      sourceState: r.sourceState as ManagedReceipt['sourceState'],
      sourceCursor: r.sourceCursor,
      scope: r.scope as ManagedReceipt['scope'],
      routes: (r.routes ?? []) as never,
      responses: (r.responses ?? []) as never
    };
  }

  async publish(frozenRequestBytes: Uint8Array, scope?: { scopeId: Id; revision: number }): Promise<ManagedReceipt> {
    const envelope = JSON.parse(Buffer.from(frozenRequestBytes).toString('utf8')) as AttentionEnvelope;
    const event: ManagedEvent = {
      kind: 'event',
      id: envelope.envelopeId,
      type: 'watcher.attention.v1',
      schemaVersion: 1,
      occurredAt: envelope.occurredAt,
      validUntil: envelope.validUntil,
      data: envelope as unknown as Record<string, never>
    };
    const receipt = await this.host.managed.publishManaged(OWNER, { event, options: this.options(scope) });
    return this.mapReceipt(receipt);
  }

  async reconcile(eventId: Id): Promise<ManagedReceipt> {
    return this.mapReceipt(this.host.managed.managedReceipt(OWNER, eventId));
  }

  async advanceScope(operationId: Id, scopeId: Id, expectedRevision: number, nextRevision: number, state: 'active' | 'paused' | 'closed'): Promise<Json> {
    const r = this.host.managed.advanceScope(OWNER, {
      operationId, scopeId, expectedRevision, nextRevision, state
    });
    if (scopeId === this.setup.scopeId && nextRevision > this.setup.scopeRevision) {
      this.setup.scopeRevision = nextRevision;
      this.persistSetup();
    }
    return r as unknown as Json;
  }

  async withdraw(operationId: Id, eventId: Id, reason: string): Promise<WithdrawResult> {
    return this.host.managed.withdraw(OWNER, { operationId, eventId, reason }) as unknown as WithdrawResult;
  }

  /** Consume host responses: managed watch update stream -> VerifiedConsumerResponse mapping. */
  async readResponses(after: number): Promise<{ cursor: number; responses: VerifiedConsumerResponse[]; resyncRequired: boolean }> {
    const updates = this.host.managed.managedWatch(OWNER, after, 128);
    const responses: VerifiedConsumerResponse[] = [];
    for (const u of updates.updates) {
      // Update shape of the target-side appendManaged: {cursor,eventId,routeRef,targetRevision,at,fact:{authentication,fact}}
      const outer = u as {
        kind?: string;
        fact?: { kind?: string; fact?: { kind?: string } & Record<string, unknown> } & Record<string, unknown>;
        responses?: ConsumerResponseShape[];
      };
      const candidates: ConsumerResponseShape[] = [];
      if (outer.fact && typeof outer.fact === 'object' && 'fact' in outer.fact && outer.fact.fact) {
        const inner = outer.fact.fact as { kind?: string } & Record<string, unknown>;
        if (inner.kind === 'consumer-response') candidates.push(inner as unknown as ConsumerResponseShape);
      } else if (outer.kind === 'consumer-response' && outer.fact) {
        candidates.push(outer.fact as unknown as ConsumerResponseShape);
      } else if (Array.isArray(outer.responses)) {
        candidates.push(...outer.responses);
      }
      for (const c of candidates) {
        const body = (c.data ?? {}) as unknown as HostAck;
        if (!body?.episodeId || !body?.action) continue;
        responses.push({
          responseId: c.responseId,
          deliveryRef: c.deliveryRef,
          ownerBindingEpoch: Number((c.data as { ownerBindingEpoch?: number }).ownerBindingEpoch ?? 1),
          digest: digestOf(c),
          body
        });
      }
    }
    return { cursor: updates.cursor, responses, resyncRequired: updates.resyncRequired };
  }

  async confirmApplied(operationId: Id, responseId: Id, result: ApplicationResult): Promise<void> {
    this.host.managed.confirmApplied(OWNER, {
      operationId, responseId,
      result: result as never
    });
  }

  async close(): Promise<void> {
    // Explicit lifecycle wrap-up: once the sentinel has no TTL, closing is the only way to invalidate it.
    // scope closed + audience closed, both idempotent best-effort; a failure does not block local wrap-up.
    try {
      if (this.setup.provisioned && this.setup.scopeRevision > 0) {
        await this.advanceScope(relayNewId('op'), this.setup.scopeId, this.setup.scopeRevision, this.setup.scopeRevision + 1, 'closed');
      }
    } catch { /* scope already closed / relay unreachable: do not block */ }
    try {
      if (this.setup.provisioned) {
        this.host.managed.closeAudience(OWNER, this.setup.audienceRef);
      }
    } catch { /* audience not provisioned / already closed: do not block */ }
    await this.host.close();
  }
}

function sha256Hex(input: string): string {
  // Consistent with the relay's source storage directory derivation (<home>/sources/<sha256(sourceId)>)
  return createHash('sha256').update(input, 'utf8').digest('hex');
}

function digestOf(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value), 'utf8').digest('hex');
}
