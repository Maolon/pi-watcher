/**
 * Result card (design 6.6): deterministic template built from verified structured facts.
 * Unknown fields are explicitly unknown; a zero exit code does not mean tests/business passed (I14).
 */

import type { WatchSpec } from '../contracts/interfaces.js';
import type { WatchSnapshot } from '../storage/store.js';
import { completionLevel } from './hard-rules.js';

export interface ResultCard {
  schemaVersion: 1;
  resultId: string;
  watchId: string;
  generation: number;
  target: WatchSpec['target'];
  executorState: string;
  checks: Array<{ checkId: string; outcome: string; artifactDigest: string | null }>;
  artifactIds: string[];
  businessAcceptance: string;
  health: string;
  observedAt: string;
  summary: string;
  /** display-only declaration: local display copy, not a relay delivery fact */
  deliveryNote?: string;
}

export function buildResultCard(input: {
  resultId: string;
  spec: WatchSpec;
  snapshot: WatchSnapshot;
  requiredCheckIds: readonly string[];
  health: string;
  observedAtMs: number;
  deliveryNote?: string;
}): ResultCard {
  const { resultId, spec, snapshot, requiredCheckIds, health, observedAtMs, deliveryNote } = input;
  const completion = completionLevel(spec, snapshot, requiredCheckIds);
  const summaryLines: string[] = [];
  switch (completion.overall) {
    case 'succeeded-evidence':
      summaryLines.push('Succeeded; required checks passed and required artifacts present (verified tier).');
      break;
    case 'failed':
      summaryLines.push(`Failed (exitCode=${snapshot.exitCode ?? 'unknown'}); root cause may still be unknown.`);
      break;
    case 'cancelled':
      summaryLines.push('Execution was cancelled.');
      break;
    case 'incomplete-evidence':
      summaryLines.push('Reported terminal but completion evidence is incomplete (missing checks or artifacts).');
      break;
    case 'in-progress':
      summaryLines.push(`Still running (state=${snapshot.taskState}).`);
      break;
  }
  if (completion.businessAcceptance === 'pending_host') {
    summaryLines.push('Business acceptance pending host (pending_host).');
  }
  const card: ResultCard = {
    schemaVersion: 1,
    resultId,
    watchId: spec.watchId,
    generation: spec.generation,
    target: spec.target,
    executorState: completion.executorState,
    checks: completion.checks,
    artifactIds: snapshot.artifactIds ?? [],
    businessAcceptance: completion.businessAcceptance,
    health,
    observedAt: new Date(observedAtMs).toISOString(),
    summary: summaryLines.join(' ')
  };
  if (deliveryNote) {
    card.deliveryNote = deliveryNote;
  }
  return card;
}

export interface GroupResultCard {
  schemaVersion: 1;
  resultId: string;
  watchId: string;
  generation: number;
  target: WatchSpec['target'];
  kind: 'group-summary';
  readyWhen: string;
  members: Array<{ watchId: string; taskState: string; lifecycle: string }>;
  overall: 'all-succeeded' | 'all-terminal-with-failures' | 'partial';
  health: string;
  observedAt: string;
  summary: string;
  deliveryNote?: string;
}

/** V3 group result card: member projection summary; no member-level acceptance judgment (each member's own card is responsible). */
export function buildGroupResultCard(input: {
  resultId: string;
  spec: WatchSpec;
  members: Array<{ watchId: string; taskState: string; lifecycle: string }>;
  readyWhen: 'all_terminal' | 'all_succeeded';
  health: string;
  observedAtMs: number;
  deliveryNote?: string;
}): GroupResultCard {
  const { resultId, spec, members, readyWhen, health, observedAtMs, deliveryNote } = input;
  const succeeded = members.filter(m => m.taskState === 'succeeded').length;
  const terminal = members.filter(m => ['succeeded', 'failed', 'cancelled'].includes(m.taskState)).length;
  const overall =
    members.length > 0 && succeeded === members.length
      ? 'all-succeeded'
      : members.length > 0 && terminal === members.length
        ? 'all-terminal-with-failures'
        : 'partial';
  const card: GroupResultCard = {
    schemaVersion: 1,
    resultId,
    watchId: spec.watchId,
    generation: spec.generation,
    target: spec.target,
    kind: 'group-summary',
    readyWhen,
    members,
    overall,
    health,
    observedAt: new Date(observedAtMs).toISOString(),
    summary: `group ${readyWhen}: ${terminal}/${members.length} terminal, ${succeeded}/${members.length} succeeded`
  };
  if (deliveryNote) card.deliveryNote = deliveryNote;
  return card;
}
