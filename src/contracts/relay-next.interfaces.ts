/**
 * Watcher-side view of the pi-relay managed-delivery shapes (receipts, withdraw results,
 * consumer responses). pi-relay does not re-export these types from its public entry points,
 * so they are declared here; keep them in sync with the @maolon/pi-relay version in package.json.
 */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export interface Event { kind: 'event'; id: string; type: string; schemaVersion: number; subject?: string; occurredAt: string; validUntil: string; data: { [key: string]: Json }; }
export interface Scope { id: string; revision: number; }
export interface ManagedOptions { audienceRef: string; scope: Scope; consumerProfileId: string; requestedMode: 'display' | 'resume'; }
export interface RouteReceipt { routeRef: string; targetRevision: number; targetUpdatedAt?: string; freshness: 'fresh' | 'stale' | 'offline'; admission: 'accepted' | 'staged' | 'rejected' | 'unknown'; delivery: 'pending' | 'held' | 'intent' | 'submitted' | 'recorded' | 'unknown' | 'suppressed' | 'expired' | 'withdrawn'; withdrawal: 'none' | 'pending' | 'prevented' | 'too_late' | 'unknown'; }
export interface ApplicationResult { outcome: 'applied' | 'stale' | 'rejected'; applicationRevision: number; code: 'APPLIED' | 'STALE_EPISODE' | 'OWNER_MISMATCH' | 'ALREADY_CLOSED' | 'INVALID_RESPONSE'; }
export interface ConsumerResponse { responseId: string; deliveryRef: string; responseType: string; schemaVersion: number; data: { [key: string]: Json }; createdAt: string; state: 'target_staged' | 'source_recorded' | 'application_applied'; applicationResult?: ApplicationResult; }
export interface ManagedReceipt { eventId: string; sourceState: 'captured' | 'empty_audience' | 'rejected' | 'unknown'; sourceCursor: number; scope: Scope; routes: RouteReceipt[]; responses: ConsumerResponse[]; }
export interface WithdrawResult { operationId: string; eventId: string; sourceApplied: boolean; routes: Array<{ routeRef: string; disposition: 'prevented' | 'too_late' | 'pending' | 'unknown' }>; }
export interface ScopeAdvance { operationId: string; scopeId: string; expectedRevision: number; nextRevision: number; state: 'active' | 'paused' | 'closed'; }
export interface ManagedSource {
  publishManaged(event: Event, options: ManagedOptions): Promise<ManagedReceipt>;
  getManagedReceipt(eventId: string): Promise<ManagedReceipt>;
  watchManagedUpdates(after: number, limit: number): Promise<{ cursor: number; resyncRequired: boolean; updates: ManagedReceipt[] }>;
  withdraw(operationId: string, eventId: string, reason: string): Promise<WithdrawResult>;
  advanceScope(input: ScopeAdvance): Promise<{ revision: number; state: 'active' | 'paused' | 'closed'; pendingRouteControls: number }>;
  confirmApplied(operationId: string, responseId: string, result: ApplicationResult): Promise<void>;
  dispose(): void;
}
export interface GateResult { decision: 'allow' | 'defer' | 'drop'; reasonCode: 'CURRENT' | 'BUSY' | 'SOURCE_UNAVAILABLE' | 'GUARD_UNAVAILABLE' | 'STALE_REQUEST' | 'CANCELLED' | 'WRONG_OWNER' | 'EXPIRED'; guardEpoch: number; validUntil?: string; }
export interface ConsumerContext { readonly deliveryRef: string; readonly bindingEpoch: number; readonly profileId: string; }
export interface ConsumerRegistration { profileId: string; eventManifestDigest: string; responseManifestDigest: string; guardImplementationId: string; timeoutMs: number; }
export interface ConsumerAPI {
  registerConsumer(profile: ConsumerRegistration, assess: (event: Event, context: ConsumerContext, signal: AbortSignal) => Promise<GateResult>): () => void;
  respond(input: { operationId: string; deliveryRef: string; responseType: string; schemaVersion: number; data: { [key: string]: Json } }): Promise<{ responseId: string; state: 'target_staged' | 'source_recorded' | 'application_applied'; applicationResult?: ApplicationResult }>;
}
export interface SetupPlan { sourceRef: string; channelId: string; consumerProfileId: string; audienceIntent: 'single-current-owner'; requestedMode: 'display' | 'resume'; grant: { maxClaims: number; ttlMs: number; eventTypes: string[] }; }
export interface OwnerAPI {
  prepareSetup(plan: SetupPlan): Promise<{ operationId: string; planDigest: string; ownerPrompt: string }>;
  /** consentReceiptRef comes from trusted owner UI, never from an LLM boolean. */
  commitSetup(input: { operationId: string; planDigest: string; consentReceiptRef: string }): Promise<{ state: 'complete' | 'partial'; audienceRef?: string; receipts: Json[] }>;
}
