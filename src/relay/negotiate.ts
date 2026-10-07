/**
 * Relay 1.2 managed-delivery feature negotiation (design 09 / V0).
 *
 * The watcher owns task semantics; automatic wake goes only through the relay managed path.
 * relay protocol 1.2 (managed receipts / withdraw / scope advance / consumer gate /
 * durable response / applied confirm / audience handle / owner setup) is currently a
 * PROPOSED proposal and is not yet implemented.
 *
 * When negotiation is unavailable: the watcher only provides local observation and display (local-display),
 * and does not silently degrade to a direct wake path without a guard (I05/I21).
 */

export type RelayNegotiationStatus = 'unavailable' | 'legacy-1-1' | 'managed-1-2';

export interface RelayNegotiation {
  status: RelayNegotiationStatus;
  protocolMinor: number | null;
  features: string[];
  requiredFeatures: string[];
  detail: string;
  checkedAt: string;
  /** transport usable on the watcher side, derived from this */
  transport: 'local-display' | 'relay';
}

export const MANAGED_REQUIRED_FEATURES = [
  'source.managed-receipts',
  'event.withdraw',
  'scope.advance',
  'consumer.gate',
  'durable-response',
  'applied-confirm',
  'audience.handle',
  'owner.setup'
] as const;

export type ProbeResult = { present: boolean; protocolMinor?: number; features?: string[]; detail?: string };

export interface NegotiateOptions {
  /** Injectable probe function (for tests) */
  probeRelay?: () => Promise<ProbeResult>;
}

export async function negotiateRelay(options: NegotiateOptions = {}): Promise<RelayNegotiation> {
  const checkedAt = new Date().toISOString();
  const probe = options.probeRelay ?? defaultProbe;
  let probed: ProbeResult;
  try {
    probed = await probe();
  } catch (e) {
    probed = { present: false, detail: `probe failed: ${e instanceof Error ? e.message : String(e)}` };
  }
  if (!probed.present) {
    return {
      status: 'unavailable',
      protocolMinor: null,
      features: [],
      requiredFeatures: [...MANAGED_REQUIRED_FEATURES],
      detail: probed.detail ?? 'pi-relay not detected',
      checkedAt,
      transport: 'local-display'
    };
  }
  const minor = probed.protocolMinor ?? 1;
  const features = probed.features ?? [];
  if (minor >= 2 && MANAGED_REQUIRED_FEATURES.every(f => features.includes(f))) {
    return {
      status: 'managed-1-2',
      protocolMinor: minor,
      features,
      requiredFeatures: [...MANAGED_REQUIRED_FEATURES],
      detail: 'relay managed path negotiated',
      checkedAt,
      transport: 'relay'
    };
  }
  return {
    status: 'legacy-1-1',
    protocolMinor: minor,
    features,
    requiredFeatures: [...MANAGED_REQUIRED_FEATURES],
    detail: 'relay present without managed-delivery features; legacy display only; auto-resume path disabled (I21)',
    checkedAt,
    transport: 'local-display'
  };
}

/** V1 closed loop: derive the negotiation result from the real state of the watcher's embedded managed source (design 9.5 compatibility mode). */
export function negotiationFromRelaySource(
  relay: { ready(): boolean; hasActiveMembership(): boolean } | null,
  error?: string
): RelayNegotiation {
  const checkedAt = new Date().toISOString();
  if (!relay) {
    return {
      status: 'unavailable',
      protocolMinor: null,
      features: [],
      requiredFeatures: [...MANAGED_REQUIRED_FEATURES],
      detail: error ?? 'relay source not configured (PI_WATCHER_RELAY=1 to enable)',
      checkedAt,
      transport: 'local-display'
    };
  }
  if (relay.ready()) {
    return {
      status: 'managed-1-2',
      protocolMinor: 2,
      features: [...MANAGED_REQUIRED_FEATURES],
      requiredFeatures: [...MANAGED_REQUIRED_FEATURES],
      detail: 'watcher managed source provisioned (audience + active scope); transport=relay allowed',
      checkedAt,
      transport: 'relay'
    };
  }
  return {
    status: 'unavailable',
    protocolMinor: 2,
    features: [...MANAGED_REQUIRED_FEATURES],
    requiredFeatures: [...MANAGED_REQUIRED_FEATURES],
    detail: relay.hasActiveMembership()
      ? 'relay source open with membership; audience/scope provisioning pending (run relay-setup --finalize)'
      : 'relay source open; session target not bound yet (relay_bindings bind with invite file)',
    checkedAt,
    transport: 'local-display'
  };
}

/** Default probe: module presence no longer implies relay availability; only a watcher-side managed source configuration upgrades the transport. */
async function defaultProbe(): Promise<ProbeResult> {
  // The pi-relay dependency ships with the watcher; the only path to transport=relay is the embedded managed source
  // being ready (PI_WATCHER_RELAY=1 -> negotiationFromRelaySource). When not enabled, honestly report
  // unavailable instead of guessing from module presence (design 9.5; no silent downgrade or silent upgrade).
  return {
    present: false,
    detail:
      'managed delivery source not enabled in this runtime (PI_WATCHER_RELAY=1 or options.relay); watcher stays local-display'
  };
}
