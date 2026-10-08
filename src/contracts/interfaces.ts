/** pi-watcher core ports and domain types (engine, store, sources, judge, managed delivery). */
export type Id = string;
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type SemanticMode = 'off' | 'shadow' | 'active';
export type RunTarget = { kind: 'run'; sourceId: Id; taskId: Id; runId: Id; attemptId: Id; /**
 * decision-delta 2026-09-21 (universal monitor mode, not in design package v0.2.0):
 * agent-declared
 * generic evidence sources. The typed source enum is removed as a product surface (unified-exec deleted);
 * agent-file / agent-check are the only two agent registration surfaces.
 *
 * file: tail a declared file path; an okPattern/failPattern hit in the tail window is the only
 * content terminal-state evidence (fail takes precedence over ok); growth means progress; file missing/rotated -> degraded, non-terminal;
 * having no fd holder is not exit evidence (a writer may open the file per write).
 */ file?: { path: string; okPattern?: string; failPattern?: string }; /**
 * check: an agent-declared bounded check command. The watcher runs cmd at a fixed cadence (within timeoutMs,
 * SIGTERM->SIGKILL); a non-zero exit code has three meanings, mapped by the declaration: exit 0 / okPattern ->
 * succeeded; failCodes / failPattern -> failed; 126/127/timeout/spawn error ->
 * unknown+degraded (the check is broken; do not sentence the task to death); any other non-zero -> running (pending).
 * The original cmd text is stored with the spec as evidence -- it must not contain secrets.
 */ check?: { cmd: string; cwd?: string; timeoutMs?: number; okPattern?: string; failPattern?: string; failCodes?: number[] } };
export type ChildRef = { watchId: Id; generation: number };
export type Target = RunTarget
  | { kind: 'group'; members: ChildRef[]; readyWhen: 'all_terminal' | 'all_succeeded' }
  | { kind: 'obligation'; dependencies: ChildRef[]; readyWhen: 'all_terminal' | 'all_succeeded'; hostAction: string };
export type Owner = { sessionId: Id; originAnchor: string | null; bindingEpoch: number; profileId: Id };
export interface WatchSpec {
  schemaVersion: 1; watchId: Id; generation: number; missionRevision: number; controlRevision: number;
  mode: 'embedded' | 'service'; owner: Owner; target: Target;
  mission: { objective: string; scope: string; checkpointId: Id; requiredArtifacts: Id[]; requiresChecks: boolean; businessAcceptance: 'host' | 'not_required'; deadlineAt?: string };
  policy: { transport: 'local-display' | 'relay'; semanticMode: SemanticMode; notificationOwner: 'watcher' | 'external'; requestKinds: string[]; maxRequestsPerEpisode: number; episodeCooldownMs: number; attentionTtlMs: number };
  limits: { pollMinMs: number; pollMaxMs: number; maxSilenceMs: number; maxProbesPerEpisode: number; maxJudgeRequestsPerDay: number; expiresAt: string };
}
export type RegisterCandidate = Omit<WatchSpec, 'watchId' | 'generation' | 'missionRevision' | 'controlRevision' | 'owner'>;
export interface ActorContext { actorId: Id; owner: Owner; profileRevision: number; permitted: ReadonlySet<string>; }
export interface Basis { watchId: Id; generation: number; missionRevision: number; controlRevision: number; observationSeq: number; windowDigest: string; }
export interface EvidenceRef { evidenceId: Id; sourceId: Id; capturedAt: string; sha256: string; bytes: number; sampled: boolean; }
export interface Observation { schemaVersion: 1; observationId: Id; target: RunTarget; sourceSeq: number; observedAt: string; kind: 'status' | 'heartbeat' | 'log_delta' | 'validation' | 'artifact_manifest' | 'decision_request'; payload: Json; evidenceRefs: EvidenceRef[]; }
export interface Probe { probeId: Id; revision: number; target: RunTarget; kind: 'read-status' | 'read-log-delta' | 'read-artifact-manifest' | 'read-validation-result'; scopeId: Id; expiresAt: string; timeoutMs: number; maxBytes: number; }
export interface SourceCapabilities { stableAttempt: boolean; terminalEvents: boolean; incrementalCursor: boolean; exclusiveNotificationOwner: boolean; }
export interface SourceAdapter {
  readonly adapterId: string;
  readonly capabilities: SourceCapabilities;
  read(target: RunTarget, cursor: Json | undefined, signal: AbortSignal): Promise<{ observations: Observation[]; nextCursor: Json; gap: boolean }>;
  probes(target: RunTarget, basis: Basis): Promise<readonly Probe[]>;
  execute(probe: Probe, signal: AbortSignal): Promise<Observation[]>;
  close(): Promise<void>;
}
export type QuestionKey = 'meaningful_progress' | 'unresolved_blocker' | 'needs_host_decision'
 | 'repeating_without_new_information' | 'claim_conflicts_with_evidence' | 'context_sufficient';
export interface Judgment { basis: Basis; model: string; questionSet: 'watcher-q1'; probabilities: Record<QuestionKey, number>; receivedAt: string; inputTokens: number; discarded: boolean; }
export interface JudgePort {
  evaluate(basis: Basis, state: Json, signal: AbortSignal): Promise<Judgment>;
  chooseProbe(basis: Basis, state: Json, candidates: readonly Probe[], signal: AbortSignal): Promise<Id | 'none'>;
  /** Optional availability check (e.g. credentials resolved lazily from the host); absent means always ready. */
  ready?(): Promise<{ ready: boolean; reason?: string; model?: string }>;
}
export type AttentionReason = 'task.failed' | 'task.terminal' | 'deadline.exceeded' | 'decision.required'
 | 'blocker.unresolved' | 'progress.repeating' | 'claim.conflict' | 'evidence.insufficient' | 'dependency.ready' | 'monitor.degraded' | 'target.changed';
export interface AttentionEnvelope {
  schemaVersion: 1; envelopeId: Id; episodeId: Id; episodeRevision: number; watchId: Id;
  generation: number; missionRevision: number; controlRevision: number; ownerBindingEpoch: number;
  target: Target; reasonCode: AttentionReason; summary: string; evidenceRefs: EvidenceRef[];
  requiredNextStep: 'inspect-current-watch-before-acting'; occurredAt: string; validUntil: string;
}
export interface HostAck {
  schemaVersion: 1; requestId: Id; watchId: Id; generation: number; episodeId: Id;
  expectedEpisodeRevision: number; action: 'received' | 'investigating' | 'defer' | 'resolved' | 'dismiss';
  reason: string; until?: string; evidenceIds: Id[];
}
export interface VerifiedConsumerResponse {
  responseId: Id; deliveryRef: Id; ownerBindingEpoch: number; digest: string; body: HostAck;
}
export interface ManagedDeliveryPort {
  /** New managed relay adapter. Never fallback to direct Pi injection. */
  /**
   * decision-delta 2026-10-07 (per-watch scope, design 9.6): optional scope the event is
   * captured under. Omitted -> the adapter's source-wide default scope (legacy behaviour).
   */
  publish(frozenRequestBytes: Uint8Array, scope?: { scopeId: Id; revision: number }): Promise<import('./relay-next.interfaces').ManagedReceipt>;
  reconcile(eventId: Id): Promise<import('./relay-next.interfaces').ManagedReceipt>;
  advanceScope(operationId: Id, scopeId: Id, expectedRevision: number, nextRevision: number, state: 'active' | 'paused' | 'closed'): Promise<Json>;
  withdraw(operationId: Id, eventId: Id, reason: string): Promise<import('./relay-next.interfaces').WithdrawResult>;
  readResponses(after: number): Promise<{ cursor: number; responses: VerifiedConsumerResponse[]; resyncRequired: boolean }>;
  confirmApplied(operationId: Id, responseId: Id, result: import('./relay-next.interfaces').ApplicationResult): Promise<void>;
  close(): Promise<void>;
}
export interface Clock { wallNow(): number; monotonicNow(): number; }
export interface Transaction {
  getWatch(watchId: Id): WatchSpec | undefined;
  compareRevision(basis: Basis): boolean;
  persistCommandResult(requestId: Id, digest: string, result: Json): void;
}
export interface Store {
  /** Synchronous, bounded transaction body: external I/O is forbidden. */
  transaction<T>(runtimeEpoch: number, body: (tx: Transaction) => T): T;
  close(): void;
}
export interface WatchService {
  register(requestId: Id, candidate: RegisterCandidate, actor: ActorContext): Promise<WatchSpec>;
  list(cursor: string | undefined, limit: number, actor: ActorContext): Promise<Json>;
  inspect(watchId: Id, actor: ActorContext): Promise<Json>;
  check(requestId: Id, watchId: Id, expectedControlRevision: number, actor: ActorContext): Promise<{ inspectionId: Id }>;
  /** Internal response pump only. No independent network ACK endpoint. */
  applyResponse(response: VerifiedConsumerResponse): Promise<import('./relay-next.interfaces').ApplicationResult>;
  control(requestId: Id, watchId: Id, expectedControlRevision: number, action: 'pause' | 'close' | 'resume', reason: string, actor: ActorContext): Promise<Json>;
}
