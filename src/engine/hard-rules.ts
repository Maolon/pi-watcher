/**
 * Deterministic hard rules (design 5.2 step 2, 16.4 completion contract).
 * Hard events are never offset by a low model score (I03/I12); terminal comes only from explicit state.
 */

import type { WatchSpec, Json } from '../contracts/interfaces.js';
import type { WatchSnapshot } from '../storage/store.js';

export const TERMINAL_TASK_STATES = ['succeeded', 'failed', 'cancelled'] as const;
export type TerminalTaskState = (typeof TERMINAL_TASK_STATES)[number];

export function isTerminalTaskState(state: string): state is TerminalTaskState {
  return (TERMINAL_TASK_STATES as readonly string[]).includes(state);
}

/** Extract task state from the status observation payload. */
export function statusPayloadToTaskState(payload: Json): {
  taskState: WatchSnapshot['taskState'];
  exitCode: number | null;
  stage: string | null;
  summary: string | null;
  /** Source evidence: process has exited (even if the exit result is unknown) -- outcome-unknown terminal signal */
  exited: boolean;
  checks: Array<{ checkId: string; outcome: string; artifactDigest: string | null }>;
  artifactIds: string[];
} {
  const p = (payload ?? {}) as Record<string, unknown>;
  const state = typeof p.state === 'string' ? p.state : 'unknown';
  const checksRaw = Array.isArray(p.checks) ? p.checks : [];
  const checks = checksRaw
    .filter((c): c is Record<string, unknown> => !!c && typeof c === 'object')
    .map(c => ({
      checkId: String(c.checkId ?? ''),
      outcome: String(c.outcome ?? 'unknown'),
      artifactDigest: typeof c.artifactDigest === 'string' ? c.artifactDigest : null
    }))
    .filter(c => c.checkId !== '');
  const artifactsRaw = Array.isArray(p.artifacts) ? p.artifacts : [];
  const artifactIds = artifactsRaw
    .filter((a): a is Record<string, unknown> => !!a && typeof a === 'object')
    .map(a => String(a.artifactId ?? ''))
    .filter(a => a !== '');
  return {
    taskState: (['unknown', 'queued', 'running', 'blocked', 'succeeded', 'failed', 'cancelled'] as const).includes(
      state as WatchSnapshot['taskState']
    )
      ? (state as WatchSnapshot['taskState'])
      : 'unknown',
    exitCode: typeof p.exitCode === 'number' ? p.exitCode : null,
    stage: typeof p.stage === 'string' ? p.stage : null,
    summary: typeof p.summary === 'string' ? p.summary : null,
    exited: p.exited === true,
    checks,
    artifactIds
  };
}

/** Terminal fact: explicit terminal state, or "process exited but result unknown" (exited evidence). Both open an episode, stop the silence timer, and are eligible for auto-close. */
export function isTerminalFact(snapshot: { taskState: string; exited?: boolean }): boolean {
  return isTerminalTaskState(snapshot.taskState) || (snapshot.taskState === 'unknown' && snapshot.exited === true);
}

export interface DeadlineFact {
  crossed: boolean;
  deadlineAtMs: number | null;
}

export function deadlineFact(mission: WatchSpec['mission'], nowMs: number): DeadlineFact {
  if (!mission.deadlineAt) return { crossed: false, deadlineAtMs: null };
  const t = Date.parse(mission.deadlineAt);
  if (Number.isNaN(t)) return { crossed: false, deadlineAtMs: null };
  return { crossed: nowMs > t, deadlineAtMs: t };
}

export interface SilenceFact {
  exceeded: boolean;
  lastObservedAtMs: number | null;
}

export function silenceFact(snapshot: WatchSnapshot, maxSilenceMs: number, nowMs: number): SilenceFact {
  if (!snapshot.lastObservedAtMs) return { exceeded: false, lastObservedAtMs: null };
  return { exceeded: nowMs - snapshot.lastObservedAtMs > maxSilenceMs, lastObservedAtMs: snapshot.lastObservedAtMs };
}

/** Completion contract (design 16.4): report only the verified level. */
export interface CompletionLevel {
  executorState: WatchSnapshot['taskState'];
  checks: Array<{ checkId: string; outcome: 'passed' | 'failed' | 'skipped' | 'unknown'; artifactDigest: string | null }>;
  artifactsPresent: boolean;
  /** succeeded-evidence: exit ok + required checks passed + required artifacts present */
  overall: 'succeeded-evidence' | 'failed' | 'cancelled' | 'incomplete-evidence' | 'in-progress';
  businessAcceptance: 'pending_host' | 'not_required' | 'unknown';
}

export function completionLevel(
  spec: WatchSpec,
  snapshot: WatchSnapshot,
  requiredCheckIds: readonly string[]
): CompletionLevel {
  const mission = spec.mission;
  const observedChecks = snapshot.checks ?? [];
  const checks = requiredCheckIds.map(req => {
    const found = observedChecks.find(c => c.checkId === req);
    if (!found) {
      return { checkId: req, outcome: 'unknown' as const, artifactDigest: null };
    }
    const outcome =
      found.outcome === 'passed' ? 'passed' as const
      : found.outcome === 'failed' ? 'failed' as const
      : found.outcome === 'skipped' ? 'skipped' as const
      : 'unknown' as const;
    return { checkId: req, outcome, artifactDigest: found.artifactDigest ?? null };
  });
  const artifactsPresent = mission.requiredArtifacts.every(id => (snapshot.artifactIds ?? []).includes(id));
  const businessAcceptance: CompletionLevel['businessAcceptance'] =
    mission.businessAcceptance === 'host'
      ? 'pending_host'
      : mission.businessAcceptance === 'not_required'
        ? 'not_required'
        : 'unknown';

  let overall: CompletionLevel['overall'];
  if (snapshot.taskState === 'failed') {
    overall = 'failed';
  } else if (snapshot.taskState === 'cancelled') {
    overall = 'cancelled';
  } else if (snapshot.taskState === 'succeeded') {
    const checksOk = mission.requiresChecks
      ? checks.length > 0 && checks.every(c => c.outcome === 'passed')
      : true;
    overall = checksOk && artifactsPresent ? 'succeeded-evidence' : 'incomplete-evidence';
  } else if (isTerminalTaskState(snapshot.taskState)) {
    overall = 'incomplete-evidence';
  } else {
    overall = 'in-progress';
  }
  return { executorState: snapshot.taskState, checks, artifactsPresent, overall, businessAcceptance };
}
