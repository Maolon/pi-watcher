/**
 * V2 semantic layer tests: fingerprint stability, cache, shadow candidates, probe follow-up, budget, consent gate,
 * active-mode episode + attention outbox, repeating two windows (design 5.1-5.6).
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { WatchStore } from '../../src/storage/store.js';
import { TaskStatusV1Adapter } from '../../src/source/task-status-v1/adapter.js';
import { WatchEngine, DEFAULT_SEMANTIC_CONFIG } from '../../src/engine/engine.js';
import { MockJudge } from '../../src/jev/index.js';
import { SystemClock } from '../../src/util/clock.js';
import { windowFingerprint, applyThresholds } from '../../src/engine/semantic.js';
import type { WatchRow } from '../../src/storage/store.js';
import { makeFixture, makeActor, makeCandidate } from './fixture.js';
import { WatchService } from '../../src/engine/service.js';

function writeStatus(sourceRoot: string, runId: string, attemptId: string, state: string, extra: Record<string, unknown> = {}): void {
  const dir = path.join(sourceRoot, 'executor-local', 'build', runId, attemptId);
  fs.mkdirSync(dir, { recursive: true });
  const snapshotSeq = Date.now();
  fs.writeFileSync(path.join(dir, 'snapshot.json'), JSON.stringify({
    schemaVersion: 1,
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId, attemptId },
    state,
    snapshotSeq,
    updatedAt: new Date().toISOString(),
    ...extra
  }));
  fs.writeFileSync(path.join(dir, 'journal.jsonl'), '');
}

async function makeEngine(tmpRoot: string, sourceRoot: string, judge: MockJudge, opts: {
  consent?: boolean; mode?: 'shadow' | 'active'; requiresConsent?: boolean; config?: typeof DEFAULT_SEMANTIC_CONFIG;
  onJudgment?: import('../../src/engine/engine.js').JudgmentNotice extends never ? never : (n: import('../../src/engine/engine.js').JudgmentNotice) => void;
} = {}) {
  const store = await WatchStore.open(path.join(tmpRoot, 'state'), { mode: 'embedded' });
  const clock = new SystemClock();
  const engine = new WatchEngine({
    clock,
    store,
    adapters: new Map([['executor-local', new TaskStatusV1Adapter(sourceRoot)]]),
    judge,
    semantic: opts.config ?? DEFAULT_SEMANTIC_CONFIG,
    semanticConsent: opts.consent ?? true,
    judgeRequiresConsent: opts.requiresConsent ?? false,
    onJudgment: opts.onJudgment
  });
  const service = new WatchService({
    clock, store, engine,
    negotiation: { status: 'unavailable', transport: 'local-display' as const, detail: 'test' } as never,
    allowedSourceIds: ['executor-local']
  });
  return { store, engine, service, clock };
}

test('windowFingerprint: stable as time passes, flips when deadline is crossed (design 5.1)', () => {
  const row = {
    watchId: 'w1', generation: 1, missionRevision: 1, controlRevision: 1,
    lifecycle: 'active', health: 'healthy', ownerSession: 's', ownerBindingEpoch: 1,
    spec: { ...makeCandidate(), mission: { ...makeCandidate().mission, deadlineAt: new Date(Date.now() + 1000).toISOString() } },
    snapshot: { taskState: 'running', coverage: { truncated: false, sourceGap: false }, scopeRevision: 0, backoffMs: 1000 },
    nextDueAt: 0, updatedAt: 0, observationSeq: 0
  } as unknown as WatchRow;
  const before = windowFingerprint(row, Date.now());
  const later = windowFingerprint(row, Date.now() + 500);
  assert.equal(before, later, 'same fact boundary -> fingerprint is stable');
  const afterDeadline = windowFingerprint(row, Date.now() + 5000);
  assert.notEqual(before, afterDeadline, 'deadline boolean boundary crossed -> fingerprint flips');
});

test('shadow mode: judgment persisted, same-window cache (one evaluate), no episode/outbox', async () => {
  const fx = makeFixture('pw-sem-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running', { stage: 'compile' });
    const judge = new MockJudge({ evaluateFn: () => ({ needs_host_decision: 0.9, context_sufficient: 0.9 }) });
    const { store, engine, service } = await makeEngine(fx.rootDir, fx.sourceRoot, judge, { mode: 'shadow' });
    const spec = await service.register('req-1', makeCandidate({ policy: { ...makeCandidate().policy, semanticMode: 'shadow' } }) as never, makeActor());
    await engine.inspectWatch(spec.watchId);
    assert.equal(judge.evaluateCallCount, 1, 'first window calls once');
    const judgments = store.transaction(tx => tx.listJudgments(spec.watchId, 10));
    assert.equal(judgments.length, 1);
    assert.equal(judgments[0].status, 'accepted');
    const row1 = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    assert.ok(row1.snapshot.semantic?.candidates?.['decision.required'], 'shadow records the candidate');
    const episodes = store.transaction(tx => tx.listEpisodes(spec.watchId, 10));
    assert.equal(episodes.filter(e => e.kind === 'decision.required').length, 0, 'shadow does not create an episode');
    // Inspect the same window again -> cache hit
    await engine.inspectWatch(spec.watchId);
    assert.equal(judge.evaluateCallCount, 1, 'same window does not egress again (cache)');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('new observation -> new window -> second judgment; repeating needs two windows with new observations', async () => {
  const fx = makeFixture('pw-sem-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running');
    const judge = new MockJudge({ evaluateFn: () => ({ repeating_without_new_information: 0.95, context_sufficient: 0.9 }) });
    const { store, engine, service } = await makeEngine(fx.rootDir, fx.sourceRoot, judge);
    const spec = await service.register('req-1', makeCandidate({ policy: { ...makeCandidate().policy, semanticMode: 'shadow' } }) as never, makeActor());
    await engine.inspectWatch(spec.watchId);
    let row = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    assert.ok(!row.snapshot.semantic?.candidates?.['progress.repeating'], 'a single window is not judged as spinning');
    await new Promise(r => setTimeout(r, 30));
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running', { stage: 'link' });
    await engine.inspectWatch(spec.watchId);
    row = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    assert.equal(judge.evaluateCallCount, 2);
    assert.ok(row.snapshot.semantic?.candidates?.['progress.repeating'], 'two windows with new observations that stay high-scoring -> spinning candidate');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('context insufficient -> one probe follow-up (evidence.insufficient if no probe is available)', async () => {
  const fx = makeFixture('pw-sem-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running');
    const judge = new MockJudge({ evaluateFn: () => ({ context_sufficient: 0.3, needs_host_decision: 0.95 }) });
    const { store, engine, service } = await makeEngine(fx.rootDir, fx.sourceRoot, judge);
    const spec = await service.register('req-1', makeCandidate({ policy: { ...makeCandidate().policy, semanticMode: 'shadow' } }) as never, makeActor());
    await engine.inspectWatch(spec.watchId);
    const row = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    // task-status-v1 adapter has no probe candidate -> evidence.insufficient directly; a high score does not decide business failure (5.5)
    assert.ok(row.snapshot.semantic?.candidates?.['evidence.insufficient'], 'context insufficient -> insufficient-evidence candidate');
    assert.ok(!row.snapshot.semantic?.candidates?.['decision.required'], 'when context is insufficient a high semantic score does not directly decide business failure');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('budget exhausted: hard rules continue, semantics stop sending, health degraded (design 5.6)', async () => {
  const fx = makeFixture('pw-sem-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running');
    const judge = new MockJudge({ evaluateFn: () => ({ context_sufficient: 0.9 }) });
    const { store, engine, service } = await makeEngine(fx.rootDir, fx.sourceRoot, judge);
    const spec = await service.register('req-1', makeCandidate({
      limits: { ...makeCandidate().limits, maxJudgeRequestsPerDay: 1 },
      policy: { ...makeCandidate().policy, semanticMode: 'shadow' }
    }) as never, makeActor());
    await engine.inspectWatch(spec.watchId);
    assert.equal(judge.evaluateCallCount, 1);
    await new Promise(r => setTimeout(r, 30));
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running', { stage: 'link' });
    await engine.inspectWatch(spec.watchId);
    assert.equal(judge.evaluateCallCount, 1, 'watch daily limit 1 -> second window stops sending');
    const row = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    assert.ok(row.snapshot.semantic?.budgetExhausted);
    assert.equal(row.health, 'degraded');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('egress consent missing -> Jev not called, only consentMissing recorded (design 5.5)', async () => {
  const fx = makeFixture('pw-sem-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running');
    const judge = new MockJudge({});
    const { store, engine, service } = await makeEngine(fx.rootDir, fx.sourceRoot, judge, { consent: false, requiresConsent: true });
    const spec = await service.register('req-1', makeCandidate({ policy: { ...makeCandidate().policy, semanticMode: 'shadow' } }) as never, makeActor());
    await engine.inspectWatch(spec.watchId);
    assert.equal(judge.evaluateCallCount, 0, 'no consent, no egress');
    const row = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    assert.ok(row.snapshot.semantic?.consentMissing);
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('active mode (with consent): candidate creates episode + attention outbox (local admission honestly annotated)', async () => {
  const fx = makeFixture('pw-sem-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running');
    const judge = new MockJudge({ evaluateFn: () => ({ needs_host_decision: 0.95, context_sufficient: 0.9 }) });
    const { store, engine, service } = await makeEngine(fx.rootDir, fx.sourceRoot, judge, { consent: true, mode: 'active' });
    const spec = await service.register('req-1', makeCandidate({ policy: { ...makeCandidate().policy, semanticMode: 'active' } }) as never, makeActor());
    await engine.inspectWatch(spec.watchId);
    const episodes = store.transaction(tx => tx.listEpisodes(spec.watchId, 10));
    assert.ok(episodes.some(e => e.kind === 'decision.required'), 'active creates a decision.required episode');
    const outbox = store.transaction(tx => tx.listOutboxByWatch(spec.watchId, 10));
    const att = outbox.find(o => o.eventType === 'watcher.attention.v1');
    assert.ok(att, 'attention envelope lands in the outbox');
    assert.equal(att!.admission, 'empty-audience');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('applyThresholds pure function: claim.conflict wording does not claim deception', () => {
  const r = applyThresholds(
    { meaningful_progress: 0.2, unresolved_blocker: 0.1, needs_host_decision: 0.1, repeating_without_new_information: 0.1, claim_conflicts_with_evidence: 0.95, context_sufficient: 0.9 },
    DEFAULT_SEMANTIC_CONFIG.thresholds, true, 0
  );
  const cc = r.candidates.find(c => c.reason === 'claim.conflict')!;
  assert.ok(cc.note?.includes('appear to conflict'));
});

test('onJudgment notice hook: fires after an accepted judgment, carries mode/objective/candidate summary (judge review visible)', async () => {
  const fx = makeFixture('pw-jnotice-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running');
    const judge = new MockJudge({ evaluateFn: () => ({ needs_host_decision: 0.95, context_sufficient: 0.9 }) });
    const notices: Array<{ mode: string; objective: string; candidates: string[] }> = [];
    const { store, engine, service } = await makeEngine(fx.rootDir, fx.sourceRoot, judge, {
      consent: true, mode: 'active',
      onJudgment: n => notices.push({ mode: n.mode, objective: n.objective, candidates: n.candidates.map(c => c.reason) })
    });
    const spec = await service.register('req-jn-1', makeCandidate({ policy: { ...makeCandidate().policy, semanticMode: 'active' } }) as never, makeActor());
    await engine.inspectWatch(spec.watchId);
    assert.ok(notices.length >= 1, `onJudgment must fire for the accepted judgment: ${JSON.stringify(notices)}`);
    assert.equal(notices[0]!.mode, 'active');
    assert.ok(notices[0]!.objective.length > 0, 'objective carried for the hint');
    assert.ok(notices[0]!.candidates.includes('decision.required'), 'top candidate carried');
    store.close();
  } finally {
    fx.cleanup();
  }
});
