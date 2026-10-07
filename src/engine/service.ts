/**
 * WatchService (design 11.1 / contracts interfaces.ts):
 * register / list / inspect / check / control / applyResponse.
 * The same business-state logic is reused by the Pi tool, slash command, service RPC and tests.
 * Idempotency: same requestId+digest returns the old result; different digest -> REQUEST_CONFLICT (11.4).
 */

import type {
  Id, Json, WatchSpec, RegisterCandidate, ActorContext,
  VerifiedConsumerResponse, HostAck
} from '../contracts/interfaces.js';
import type { WatchStore, WatchRow, EpisodeRow } from '../storage/store.js';
import { emptySnapshot } from '../storage/store.js';
import { WatchEngine } from './engine.js';
import type { Clock } from '../util/clock.js';
import { newId, stableStringify, digestJson } from '../util/ids.js';
import { WatcherError, type ServiceResult, ok, err } from '../util/result.js';
import type { RelayNegotiation } from '../relay/negotiate.js';
import type { ManagedDeliveryPort } from '../contracts/interfaces.js';

export interface WatchServiceOptions {
  clock: Clock;
  store: WatchStore;
  engine: WatchEngine;
  /** Negotiated result or its live reference (upgraded to transport=relay after relay binds) */
  negotiation: RelayNegotiation | (() => RelayNegotiation);
  /** Source scope allowed by the profile (owner-approved source profile) */
  allowedSourceIds?: readonly string[];
  requiredCheckIds?: ReadonlyMap<string, readonly string[]>;
  /** V1 closed loop: relay managed delivery port (static or live reference; advances scope on pause/close) */
  delivery?: ManagedDeliveryPort | (() => ManagedDeliveryPort | undefined);
}

interface InFlightCheck {
  inspectionId: string;
  promise: Promise<{ inspectionId: string }>;
}

export class WatchService {
  private readonly clock: Clock;
  private readonly store: WatchStore;
  private readonly engine: WatchEngine;
  private readonly negotiationRef: () => RelayNegotiation;
  private readonly deliveryRef: () => ManagedDeliveryPort | undefined;
  private readonly allowedSourceIds: readonly string[];
  private readonly requiredCheckIds: ReadonlyMap<string, readonly string[]>;
  private readonly inFlightChecks = new Map<string, InFlightCheck>();

  constructor(options: WatchServiceOptions) {
    this.clock = options.clock;
    this.store = options.store;
    this.engine = options.engine;
    const neg = options.negotiation;
    this.negotiationRef = typeof neg === 'function' ? neg : () => neg;
    const d = options.delivery;
    this.deliveryRef = typeof d === 'function' ? d : () => d;
    this.allowedSourceIds = options.allowedSourceIds ?? [];
    this.requiredCheckIds = options.requiredCheckIds ?? new Map();
  }

  get negotiation(): RelayNegotiation {
    return this.negotiationRef();
  }

  private requireOwner(row: WatchRow, actor: ActorContext): void {
    if (row.ownerSession !== actor.owner.sessionId) {
      throw new WatcherError('CAPABILITY_DENIED', 'watch belongs to a different owner session');
    }
  }

  // --- register (T1) ---

  async register(requestId: Id, candidate: RegisterCandidate, actor: ActorContext): Promise<WatchSpec> {
    validateRegisterCandidate(candidate);
    // V3: group/obligation target validation (members exist, same owner, cap); run uses the original path
    if (candidate.target.kind !== 'run') {
      validateCompositeTarget(candidate, actor, this.store, this.allowedSourceIds);
    }
    if (candidate.target.kind === 'run' && !this.allowedSourceIds.includes(candidate.target.sourceId)) {
      throw new WatcherError('CAPABILITY_DENIED', `sourceId ${candidate.target.sourceId} not approved in owner profile`);
    }
    if (candidate.policy.transport === 'relay' && this.negotiation.transport !== 'relay') {
      throw new WatcherError(
        'DEPENDENCY_UNQUALIFIED',
        `transport=relay requires relay protocol 1.2 managed features; negotiation status=${this.negotiation.status}. Register with transport=local-display.`
      );
    }
    const digest = stableStringify({ op: 'register', requestId, candidate });
    const now = this.clock.wallNow();
    return this.store.transaction(tx => {
      const watchId = newId('w');
      const spec: WatchSpec = {
        ...candidate,
        watchId,
        generation: 1,
        missionRevision: 1,
        controlRevision: 1,
        owner: actor.owner
      };
      const response = { spec } as unknown as Json;
      const cmd = tx.persistCommandResult(actor.actorId, requestId, digest, response);
      if (cmd.duplicate && cmd.previous) {
        const prev = cmd.previous as Record<string, unknown>;
        return prev.spec as unknown as WatchSpec;
      }
      tx.insertWatch({
        watchId,
        generation: 1,
        missionRevision: 1,
        controlRevision: 1,
        lifecycle: 'active',
        health: 'healthy',
        ownerSession: actor.owner.sessionId,
        ownerBindingEpoch: actor.owner.bindingEpoch,
        spec,
        snapshot: emptySnapshot(),
        nextDueAt: now,
        now
      });
      return spec;
    });
  }

  // --- list ---

  async list(cursor: string | undefined, limit: number, actor: ActorContext, lifecycles?: Array<WatchRow['lifecycle']>): Promise<Json> {
    const boundedLimit = Math.max(1, Math.min(limit, 50));
    const rows = this.store.transaction(tx => tx.listWatches(actor.owner.sessionId, cursor, boundedLimit, lifecycles));
    const items = rows.map(row => ({
      watchId: row.watchId,
      lifecycle: row.lifecycle,
      health: row.health,
      taskState: row.snapshot.taskState,
      controlRevision: row.spec.controlRevision,
      checkpointId: row.spec.mission.checkpointId,
      target: row.spec.target,
      nextDueAt: row.nextDueAt,
      openEpisodes: this.store.transaction(tx => tx.countOpenEpisodes(row.watchId)),
      lastResultId: row.snapshot.lastResultId ?? null
    }));
    return {
      items,
      nextCursor: rows.length === boundedLimit ? rows[rows.length - 1].watchId : null,
      // Terminal-state summary (to cut noise in the list projection): closed/expired are no longer echoed row by row to the model, only counted
      terminalCount: this.store.transaction(tx => tx.countWatches(actor.owner.sessionId, ['closed', 'expired'])),
      relay: { status: this.negotiation.status, transport: this.negotiation.transport }
    };
  }

  // --- inspect ---

  async inspect(watchId: Id, actor: ActorContext): Promise<Json> {
    const row = this.store.transaction(tx => tx.getWatchRow(watchId));
    if (!row) throw new WatcherError('UNKNOWN_TARGET', `watch ${watchId} not found`);
    this.requireOwner(row, actor);
    const episodes = this.store.transaction(tx => tx.listEpisodes(watchId, 20));
    const outbox = this.store.transaction(tx => tx.listOutboxByWatch(watchId, 20));
    const observations = this.store.transaction(tx => tx.listObservations(watchId, 16));
    const judgments = this.store.transaction(tx => tx.listJudgments(watchId, 10));
    const cards = this.store.listResultCards(watchId);
    return {
      watch: row.spec,
      lifecycle: row.lifecycle,
      health: row.health,
      snapshot: row.snapshot,
      nextDueAt: row.nextDueAt,
      episodes: episodes.map(episodeView),
      outbox: outbox.map(o => ({
        eventId: o.eventId,
        eventType: o.eventType,
        admission: o.admission,
        admissionJson: o.admissionJson,
        validUntil: o.validUntil,
        digest: o.eventDigest
      })),
      observations: observations.map(o => ({
        observationId: o.observationId,
        sourceSeq: o.sourceSeq,
        observedAt: new Date(o.observedAt).toISOString(),
        kind: 'auto' as const,
        digest: o.digest
      })),
      judgments,
      resultCards: cards,
      relay: { status: this.negotiation.status, transport: this.negotiation.transport }
    } as unknown as Json;
  }

  // --- check: bounded read-only refresh (merges in-flight requests of the same version) ---

  async check(requestId: Id, watchId: Id, expectedControlRevision: number, actor: ActorContext): Promise<{ inspectionId: Id }> {
    const row = this.store.transaction(tx => tx.getWatchRow(watchId));
    if (!row) throw new WatcherError('UNKNOWN_TARGET', `watch ${watchId} not found`);
    this.requireOwner(row, actor);
    if (row.controlRevision !== expectedControlRevision) {
      throw new WatcherError('STALE_REVISION', `expected controlRevision ${expectedControlRevision}, current ${row.controlRevision}`);
    }
    const key = `${watchId}:${row.controlRevision}`;
    const existing = this.inFlightChecks.get(key);
    if (existing) {
      return existing.promise;
    }
    const inspectionId = newId('insp');
    const promise = (async () => {
      try {
        await this.engine.inspectWatch(watchId);
        return { inspectionId };
      } finally {
        this.inFlightChecks.delete(key);
      }
    })();
    this.inFlightChecks.set(key, { inspectionId, promise });
    return promise;
  }

  // --- control: pause/resume/close (T6) ---

  async control(
    requestId: Id,
    watchId: Id,
    expectedControlRevision: number,
    action: 'pause' | 'close' | 'resume',
    reason: string,
    actor: ActorContext
  ): Promise<Json> {
    const digest = stableStringify({ op: 'control', requestId, watchId, expectedControlRevision, action, reason });
    const now = this.clock.wallNow();
    const result = this.store.transaction(tx => {
      // Idempotent replay first: same requestId+digest returns the saved result directly, without re-validating the current revision
      const replay = tx.getCommand(actor.actorId, requestId);
      if (replay) {
        if (replay.digest === digest) return replay.response;
        throw new WatcherError('REQUEST_CONFLICT', `requestId ${requestId} replayed with different digest`);
      }
      const row0 = tx.getWatchRow(watchId);
      if (!row0) throw new WatcherError('UNKNOWN_TARGET', `watch ${watchId} not found`);
      this.requireOwner(row0, actor);
      if (row0.controlRevision !== expectedControlRevision) {
        throw new WatcherError('STALE_REVISION', `expected controlRevision ${expectedControlRevision}, current ${row0.controlRevision}`);
      }
      const row = tx.getWatchRow(watchId);
      if (!row) throw new WatcherError('UNKNOWN_TARGET', `watch ${watchId} not found`);
      this.requireOwner(row, actor);
      if (row.controlRevision !== expectedControlRevision) {
        throw new WatcherError('STALE_REVISION', `expected controlRevision ${expectedControlRevision}, current ${row.controlRevision}`);
      }
      const nextLifecycle: WatchRow['lifecycle'] =
        action === 'pause' ? 'paused' : action === 'close' ? 'closed' : 'active';
      if (action === 'resume' && row.lifecycle !== 'paused') {
        throw new WatcherError('INVALID_SPEC', `resume requires paused watch, current lifecycle=${row.lifecycle}`);
      }
      if (action === 'pause' && row.lifecycle !== 'active') {
        throw new WatcherError('INVALID_SPEC', `pause requires active watch, current lifecycle=${row.lifecycle}`);
      }
      if (action === 'close' && row.lifecycle === 'closed') {
        throw new WatcherError('INVALID_SPEC', 'watch already closed');
      }
      const nextControlRevision = row.controlRevision + 1;
      const requestedState = action === 'pause' ? 'paused' : action === 'close' ? 'closed' : 'active';
      // Scope advance intent (design 9.6): only for a relay scope that exists. A watch that
      // never published a managed attention has nothing captured under its scope to fence.
      // resume re-activates the scope but does not re-arm old events (they stay fenced).
      const relayScope = row.snapshot.relayScope;
      let scopeOperationId: string | null = null;
      let nextRelayScope = relayScope;
      if (relayScope && relayScope.state !== 'closed') {
        scopeOperationId = newId('ctl');
        tx.insertControlOutbox({
          operationId: scopeOperationId,
          watchId,
          scopeId: relayScope.scopeId,
          expectedRevision: relayScope.revision,
          nextRevision: relayScope.revision + 1,
          requestedState,
          now
        });
        nextRelayScope = { scopeId: relayScope.scopeId, revision: relayScope.revision + 1, state: requestedState };
      }
      // Withdraw intents (design 6.5, I15, I20): every attention of this watch that may
      // still wake the host. Durable in the same transaction; the relay call happens after
      // commit and is retried by the control drain until it returns per-route results.
      const withdrawEventIds: string[] = [];
      if (action !== 'resume') {
        for (const o of tx.listOutboxByWatch(watchId, 200)) {
          if (o.eventType !== 'watcher.attention.v1' || o.validUntil <= now || o.admissionJson.withdraw) continue;
          if (!['pending', 'publishing', 'unknown', 'source-staged'].includes(o.admission)) continue;
          tx.mergeOutboxAdmissionJson(o.eventId, {
            withdraw: { operationId: newId('wd'), reason: `watch ${action}: ${reason}`.slice(0, 200), requestedAt: now, result: null }
          }, now);
          withdrawEventIds.push(o.eventId);
        }
      }
      tx.updateWatch(watchId, {
        lifecycle: nextLifecycle,
        controlRevision: nextControlRevision,
        snapshot: { ...row.snapshot, relayScope: nextRelayScope }
      }, now);
      // On close, supersede unresolved episodes (the issue is superseded as the watch shuts down; not disguised as responded)
      if (action === 'close') {
        tx.supersedeEpisodesOfWatch(watchId, now);
      }
      // The persisted command result is the local cut only (design 7.3 T6). Relay-side
      // layers are derived from durable state on every read, so a replay never reports a
      // stale snapshot of them.
      const response: Record<string, Json> = {
        localPaused: action === 'pause' || action === 'close',
        lifecycle: nextLifecycle,
        controlRevision: nextControlRevision,
        scopeOperationId,
        withdrawEventIds,
        reason
      };
      updateCommandFinal(tx, actor.actorId, requestId, digest, response as Json);
      return response as Json;
    });
    // External I/O strictly after commit (a sync throw inside the outer transaction once
    // opened a nested transaction).
    if (this.deliveryRef()) {
      try {
        await this.engine.drainControl(watchId);
      } catch {
        /* intents are durable; the loop drain retries */
      }
    }
    return { ...(result as Record<string, Json>), ...this.relayCutReport(result as Record<string, Json>) };
  }

  /**
   * Layered withdrawal report (design 6.5): sourceFence for the scope advance and one entry
   * per affected route. "withdrawn" is claimed only when every route reports prevented
   * (I15); missing evidence stays pending/unknown, never success.
   */
  private relayCutReport(local: Record<string, Json>): Record<string, Json> {
    const scopeOperationId = typeof local.scopeOperationId === 'string' ? local.scopeOperationId : null;
    const withdrawEventIds = Array.isArray(local.withdrawEventIds) ? (local.withdrawEventIds as string[]) : [];
    const relayReady = this.negotiationRef().transport === 'relay' && !!this.deliveryRef();
    const status = scopeOperationId
      ? this.store.transaction(tx => tx.getControlOutboxStatus(scopeOperationId))
      : null;
    let sourceFence: Record<string, Json>;
    if (!scopeOperationId) {
      sourceFence = withdrawEventIds.length > 0
        ? { status: 'not-needed', detail: 'no per-watch relay scope; legacy events are cut by withdraw' }
        : relayReady
          ? { status: 'not-needed', detail: 'nothing was captured under a relay scope for this watch' }
          : { status: 'not-configured', detail: 'no relay managed path; nothing in flight to withdraw (local-display only)' };
    } else if (status === 'source-applied' || status === 'complete') {
      sourceFence = { status: 'applied', operationId: scopeOperationId };
    } else if (status === 'rejected') {
      sourceFence = { status: 'rejected', operationId: scopeOperationId };
    } else if (status === 'unknown') {
      sourceFence = { status: 'unknown', operationId: scopeOperationId };
    } else {
      sourceFence = { status: 'pending', operationId: scopeOperationId };
    }
    const routes: Json[] = [];
    let allPrevented = withdrawEventIds.length > 0;
    for (const eventId of withdrawEventIds) {
      const w = this.store.transaction(tx => tx.getOutbox(eventId))?.admissionJson.withdraw as
        { result?: { routes?: Array<{ routeRef: string; disposition: string }> } | null } | undefined;
      const rs = w?.result?.routes;
      if (!rs) {
        routes.push({ eventId, routeRef: null, disposition: 'pending' });
        allPrevented = false;
        continue;
      }
      if (rs.length === 0) {
        routes.push({ eventId, routeRef: null, disposition: 'unknown' });
        allPrevented = false;
      }
      for (const r of rs) {
        routes.push({ eventId, routeRef: r.routeRef, disposition: r.disposition });
        if (r.disposition !== 'prevented') allPrevented = false;
      }
    }
    return { sourceFence, routes, withdrawn: allPrevented };
  }

  // --- applyResponse: internal response pump (T5), no independent network ACK entry ---

  async applyResponse(response: VerifiedConsumerResponse): Promise<{
    outcome: 'applied' | 'stale' | 'rejected';
    applicationRevision: number;
    code: 'APPLIED' | 'STALE_EPISODE' | 'OWNER_MISMATCH' | 'ALREADY_CLOSED' | 'INVALID_RESPONSE';
  }> {
    const now = this.clock.wallNow();
    return this.store.transaction(tx => {
      const existing = tx.getAppliedResponse(response.responseId);
      if (existing) {
        const r = existing.result as Record<string, unknown>;
        return {
          outcome: (r.outcome as 'applied' | 'stale' | 'rejected') ?? 'applied',
          applicationRevision: (r.applicationRevision as number) ?? 0,
          code: (r.code as 'APPLIED') ?? 'APPLIED'
        };
      }
      const episode = tx.getEpisode(response.body.episodeId);
      if (!episode) {
        return { outcome: 'rejected', applicationRevision: 0, code: 'INVALID_RESPONSE' };
      }
      const watch = tx.getWatchRow(episode.watchId);
      if (!watch || watch.ownerBindingEpoch !== response.ownerBindingEpoch) {
        return { outcome: 'rejected', applicationRevision: 0, code: 'OWNER_MISMATCH' };
      }
      if (episode.revision !== response.body.expectedEpisodeRevision) {
        return { outcome: 'stale', applicationRevision: episode.revision, code: 'STALE_EPISODE' };
      }
      if (episode.state === 'resolved' || episode.state === 'superseded') {
        // A closed issue is not reopened by a late ACK (design 3.4)
        return { outcome: 'stale', applicationRevision: episode.revision, code: 'ALREADY_CLOSED' };
      }
      const action = response.body.action;
      const body = { ...episode.body, lastAck: { action, reason: response.body.reason, at: new Date(now).toISOString() } };
      let nextState: EpisodeRow['state'];
      let snoozeUntil: number | null = null;
      switch (action) {
        case 'received':
        case 'investigating':
          nextState = 'acknowledged';
          break;
        case 'defer':
          nextState = 'snoozed';
          snoozeUntil = response.body.until ? Date.parse(response.body.until) : now + 3600_000;
          break;
        case 'resolved':
        case 'dismiss':
          nextState = 'resolved';
          break;
      }
      const newRevision = tx.updateEpisode(episode.episodeId, episode.revision, {
        state: nextState,
        body,
        snoozeUntil
      }, now);
      if (newRevision === null) {
        return { outcome: 'stale', applicationRevision: episode.revision, code: 'STALE_EPISODE' };
      }
      const result: { outcome: 'applied'; applicationRevision: number; code: 'APPLIED' } = {
        outcome: 'applied', applicationRevision: newRevision, code: 'APPLIED'
      };
      tx.insertAppliedResponse({
        responseId: response.responseId,
        watchId: episode.watchId,
        episodeId: episode.episodeId,
        deliveryRef: response.deliveryRef,
        digest: response.digest,
        result: result as unknown as Record<string, Json>,
        now
      });
      tx.insertAppliedConfirmOutbox(newId('conf'), response.responseId);
      // Respond-then-close: when a relay response flows back and sets resolved, bring nextDue forward so the fast-close gate finishes immediately (5s grace)
      if (nextState === 'resolved' && watch.spec.target.kind === 'run') {
        const episodes = tx.listEpisodes(watch.watchId, 50);
        if (episodes.length > 0 && episodes.every(e => e.state === 'resolved' || e.state === 'superseded')) {
          const targetDue = Math.max(now, (watch.snapshot.terminalAt ?? now) + 5_000);
          if (watch.nextDueAt === null || watch.nextDueAt > targetDue) {
            tx.updateWatch(watch.watchId, { nextDueAt: targetDue }, now);
          }
        }
      }
      return result;
    });
  }

  /** V3: local owner ACK (for the /watcher panel only; the model's tool ack still needs relay delivery, design 11.7). */
  async ackEpisode(
    requestId: Id,
    episodeId: Id,
    action: 'received' | 'investigating' | 'defer' | 'resolved' | 'dismiss',
    reason: string,
    untilIso: string | undefined,
    actor: ActorContext
  ): Promise<Json> {
    const digest = stableStringify({ op: 'ack', requestId, episodeId, action, reason, untilIso });
    const now = this.clock.wallNow();
    return this.store.transaction(tx => {
      const replay = tx.getCommand(actor.actorId, requestId);
      if (replay) {
        if (replay.digest === digest) return replay.response;
        throw new WatcherError('REQUEST_CONFLICT', `requestId ${requestId} replayed with different digest`);
      }
      const episode = tx.getEpisode(episodeId);
      if (!episode) throw new WatcherError('UNKNOWN_TARGET', `episode ${episodeId} not found`);
      const watch = tx.getWatchRow(episode.watchId);
      if (!watch) throw new WatcherError('UNKNOWN_TARGET', `watch ${episode.watchId} not found`);
      this.requireOwner(watch, actor);
      if (episode.state === 'resolved' || episode.state === 'superseded') {
        throw new WatcherError('STALE_REVISION', `episode already ${episode.state}`);
      }
      const nextState: EpisodeRow['state'] =
        action === 'received' || action === 'investigating' ? 'acknowledged'
          : action === 'defer' ? 'snoozed' : 'resolved';
      const snoozeUntil = action === 'defer'
        ? (untilIso ? Date.parse(untilIso) : now + 3600_000)
        : null;
      if (Number.isNaN(snoozeUntil)) throw new WatcherError('INVALID_SPEC', 'until must be ISO datetime');
      const body = { ...episode.body, lastAck: { action, reason, at: new Date(now).toISOString(), via: 'local-owner-panel' } };
      const newRevision = tx.updateEpisode(episodeId, episode.revision, { state: nextState, body, snoozeUntil }, now);
      if (newRevision === null) throw new WatcherError('STALE_REVISION', 'episode revision moved');
      // Respond-then-close: when all episodes of a terminal-state watch are already resolved/superseded,
      // inspection is still on the 60s terminal backoff cadence -- bring nextDue forward to after the 5s grace so the fast-close gate evaluates promptly
      if (nextState === 'resolved' && watch.spec.target.kind === 'run') {
        const episodes = tx.listEpisodes(watch.watchId, 50);
        if (episodes.length > 0 && episodes.every(e => e.state === 'resolved' || e.state === 'superseded')) {
          const targetDue = Math.max(now, (watch.snapshot.terminalAt ?? now) + 5_000);
          if (watch.nextDueAt === null || watch.nextDueAt > targetDue) {
            tx.updateWatch(watch.watchId, { nextDueAt: targetDue }, now);
          }
        }
      }
      const response = {
        episodeId,
        state: nextState,
        revision: newRevision,
        snoozeUntil,
        note: 'local owner panel ack; no relay delivery claimed (I05/I21)'
      } as unknown as Json;
      updateCommandFinal(tx, actor.actorId, requestId, digest, response);
      return response;
    });
  }

  /** Minimal projection of the panel's six questions (design 11.3). */
  panel(actor: ActorContext): Json {
    const meta = this.store.transaction(tx => tx.meta());
    const watches = this.store.transaction(tx => tx.listWatches(actor.owner.sessionId, undefined, 50));
    return {
      runtimeEpoch: meta.runtimeEpoch,
      mode: meta.mode,
      recoveryAttentionHold: meta.recoveryAttentionHold,
      relay: this.negotiation,
      watches: watches.map(w => ({
        watchId: w.watchId,
        lifecycle: w.lifecycle,
        health: w.health,
        taskState: w.snapshot.taskState,
        nextDueAt: w.nextDueAt,
        semantic: w.snapshot.semantic
          ? {
              mode: w.snapshot.semantic.mode,
              candidates: w.snapshot.semantic.candidates ?? {},
              meaningfulProgress: w.snapshot.semantic.meaningfulProgress ?? null,
              consentMissing: w.snapshot.semantic.consentMissing ?? false,
              budgetExhausted: w.snapshot.semantic.budgetExhausted ?? false,
              error: w.snapshot.semantic.error ?? null
            }
          : null,
        whyNoWake: noWakeReason(w, this.negotiation)
      }))
    } as unknown as Json;
  }
}

function noWakeReason(row: WatchRow, negotiation: RelayNegotiation): string {
  if (negotiation.transport !== 'relay') return 'display-only: relay managed path not negotiated';
  if (row.lifecycle === 'paused') return 'paused';
  if (row.lifecycle === 'closed') return 'closed';
  if (row.lifecycle === 'expired') return 'expired';
  return 'none';
}

function episodeView(e: EpisodeRow): Json {
  return {
    episodeId: e.episodeId,
    kind: e.kind,
    checkpointId: e.checkpointId,
    ordinal: e.ordinal,
    revision: e.revision,
    state: e.state,
    body: e.body,
    snoozeUntil: e.snoozeUntil,
    updatedAt: e.updatedAt
  };
}

function updateCommandFinal(
  tx: import('../storage/store.js').StoreTx,
  actorId: string,
  requestId: string,
  digest: string,
  response: Json
): void {
  const r = tx.persistCommandResult(actorId, requestId, digest, response);
  void r;
}

function validateRegisterCandidate(candidate: RegisterCandidate): void {
  const errors: string[] = [];
  if (!candidate.mission?.objective) errors.push('mission.objective required');
  if (!candidate.mission?.checkpointId) errors.push('mission.checkpointId required');
  if (!candidate.limits?.expiresAt || Number.isNaN(Date.parse(candidate.limits.expiresAt))) errors.push('limits.expiresAt (ISO) required');
  if (!candidate.policy) errors.push('policy required');
  if (candidate.policy && !['local-display', 'relay'].includes(candidate.policy.transport)) errors.push('policy.transport must be local-display|relay');
  if (candidate.policy && !['off', 'shadow', 'active'].includes(candidate.policy.semanticMode)) errors.push('policy.semanticMode invalid');
  if (candidate.limits?.pollMinMs !== undefined && candidate.limits.pollMinMs < 1000) errors.push('limits.pollMinMs >= 1000 required');
  if (errors.length > 0) {
    throw new WatcherError('INVALID_SPEC', errors.join('; '));
  }
}

export type { HostAck };
export function serviceResult<T>(requestId: string | undefined, fn: () => T): ServiceResult<T> {
  try {
    return ok(fn(), requestId);
  } catch (e) {
    if (e instanceof WatcherError) return err(e.code, e.message, requestId);
    return err('STORE_UNAVAILABLE', e instanceof Error ? e.message : String(e), requestId);
  }
}

/** V3 composite target validation: members exist, same owner, generation matches, maxGroupChildren cap. */
function validateCompositeTarget(
  candidate: RegisterCandidate,
  actor: ActorContext,
  store: WatchStore,
  allowedSourceIds: readonly string[]
): void {
  const t = candidate.target;
  if (t.kind === 'group' || t.kind === 'obligation') {
    const refs = t.kind === 'group' ? t.members : t.dependencies;
    if (t.kind === 'group' && refs.length === 0) {
      throw new WatcherError('INVALID_SPEC', 'group target requires at least one member');
    }
    if (refs.length > 16) {
      throw new WatcherError('INVALID_SPEC', 'group/obligation references exceed maxGroupChildren=16');
    }
    for (const ref of refs) {
      const row = store.transaction(tx => tx.getWatchRow(ref.watchId));
      if (!row) throw new WatcherError('UNKNOWN_TARGET', `member watch ${ref.watchId} not found`);
      if (row.generation !== ref.generation) {
        throw new WatcherError('STALE_REVISION', `member ${ref.watchId} generation ${ref.generation} != current ${row.generation}`);
      }
      if (row.ownerSession !== actor.owner.sessionId) {
        throw new WatcherError('CAPABILITY_DENIED', `member watch ${ref.watchId} belongs to another owner session`);
      }
    }
    if (t.kind === 'obligation') {
      if (!t.hostAction || t.hostAction.trim() === '') {
        throw new WatcherError('INVALID_SPEC', 'obligation target requires hostAction (the decision/action owed)');
      }
    }
  }
}
