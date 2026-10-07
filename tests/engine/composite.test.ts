/**
 * V3 composite target tests: group readiness, obligation due / defer recheck / owner ACK (replaces the scheduler surface).
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { WatchStore } from '../../src/storage/store.js';
import { TaskStatusV1Adapter } from '../../src/source/task-status-v1/adapter.js';
import { WatchEngine } from '../../src/engine/engine.js';
import { SystemClock } from '../../src/util/clock.js';
import { makeFixture, makeActor, makeCandidate } from './fixture.js';
import { WatchService } from '../../src/engine/service.js';
import { WatcherError } from '../../src/util/result.js';

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

async function setup(fx: { rootDir: string; sourceRoot: string }) {
  const store = await WatchStore.open(path.join(fx.rootDir, 'state'), { mode: 'embedded' });
  const clock = new SystemClock();
  const engine = new WatchEngine({
    clock,
    store,
    adapters: new Map([['executor-local', new TaskStatusV1Adapter(fx.sourceRoot)]])
  });
  const service = new WatchService({
    clock, store, engine,
    negotiation: { status: 'unavailable', transport: 'local-display' as const, detail: 'test' } as never,
    allowedSourceIds: ['executor-local']
  });
  return { store, engine, service, clock };
}

test('group: all_terminal ready -> dependency.ready episode + group result card', async () => {
  const fx = makeFixture('pw-grp-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'succeeded');
    writeStatus(fx.sourceRoot, 'run-2', 'attempt-1', 'failed', { exitCode: 2 });
    const { store, engine, service } = await setup(fx);
    const actor = makeActor();
    const a = await service.register('r1', makeCandidate({ target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' } }), actor);
    const b = await service.register('r2', makeCandidate({ target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-2', attemptId: 'attempt-1' } }), actor);
    const g = await service.register('r3', makeCandidate({
      target: { kind: 'group', members: [{ watchId: a.watchId, generation: 1 }, { watchId: b.watchId, generation: 1 }], readyWhen: 'all_terminal' }
    }), actor);
    // First let each member watch read its terminal state (the group reads committed child state)
    await engine.inspectWatch(a.watchId);
    await engine.inspectWatch(b.watchId);
    await engine.inspectWatch(g.watchId);
    const episodes = store.transaction(tx => tx.listEpisodes(g.watchId, 10));
    assert.ok(episodes.some(e => e.kind === 'dependency.ready'), 'group-ready episode');
    const cards = store.listResultCards(g.watchId);
    assert.equal(cards.length, 1, 'group result card');
    assert.equal((cards[0] as { overall?: string }).overall, 'all-terminal-with-failures');
    // readyWhen=all_succeeded that is not ready does not fire
    const g2 = await service.register('r4', makeCandidate({
      target: { kind: 'group', members: [{ watchId: a.watchId, generation: 1 }, { watchId: b.watchId, generation: 1 }], readyWhen: 'all_succeeded' }
    }), actor);
    await engine.inspectWatch(g2.watchId);
    assert.equal(store.transaction(tx => tx.listEpisodes(g2.watchId, 10)).length, 0, 'all_succeeded not met -> no episode');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('obligation (pure time obligation): due -> episode; defer recheck; owner ACK resolve', async () => {
  const fx = makeFixture('pw-ob-');
  try {
    const { store, engine, service, clock } = await setup(fx);
    const actor = makeActor();
    const o = await service.register('r1', makeCandidate({
      target: { kind: 'obligation', dependencies: [], readyWhen: 'all_terminal', hostAction: 'Review the run-1 result card and decide whether to rerun' },
      mission: { ...makeCandidate().mission, deadlineAt: new Date(clock.wallNow() + 50).toISOString() }
    }), actor);
    await engine.inspectWatch(o.watchId);
    assert.equal(store.transaction(tx => tx.listEpisodes(o.watchId, 10)).length, 0, 'does not fire before due');
    await new Promise(r => setTimeout(r, 80));
    await engine.inspectWatch(o.watchId);
    let episodes = store.transaction(tx => tx.listEpisodes(o.watchId, 10));
    assert.equal(episodes.length, 1, 'due -> dependency.ready (needs ACK)');
    assert.equal(episodes[0].kind, 'dependency.ready');
    const epId = episodes[0].episodeId;
    // defer (local owner panel path)
    const defer = await service.ackEpisode('ack-1', epId, 'defer', 'handle later', new Date(clock.wallNow() + 60).toISOString(), actor);
    assert.equal((defer as { state?: string }).state, 'snoozed');
    // no reopen during snooze recheck
    await engine.inspectWatch(o.watchId);
    episodes = store.transaction(tx => tx.listEpisodes(o.watchId, 10));
    assert.equal(episodes[0].state, 'snoozed');
    // recheck after snooze expiry reopens (no permanent alarm: reopen rather than a new episode)
    await new Promise(r => setTimeout(r, 80));
    await engine.inspectWatch(o.watchId);
    episodes = store.transaction(tx => tx.listEpisodes(o.watchId, 10));
    assert.equal(episodes[0].state, 'open', 'defer expiry rechecks first -> same episode reopened');
    // owner ACK resolve
    const res = await service.ackEpisode('ack-2', epId, 'resolved', 'handled', undefined, actor);
    assert.equal((res as { state?: string }).state, 'resolved');
    // Idempotent replay
    const res2 = await service.ackEpisode('ack-2', epId, 'resolved', 'handled', undefined, actor);
    assert.deepEqual(res2, res, 'same requestId+digest idempotent replay');
    // no reopen after resolved
    await engine.inspectWatch(o.watchId);
    episodes = store.transaction(tx => tx.listEpisodes(o.watchId, 10));
    assert.equal(episodes[0].state, 'resolved');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('obligation dependencies: no fire while dependencies are non-terminal; fires only when terminal and due', async () => {
  const fx = makeFixture('pw-obd-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running');
    const { store, engine, service, clock } = await setup(fx);
    const actor = makeActor();
    const dep = await service.register('r1', makeCandidate({ target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' } }), actor);
    const o = await service.register('r2', makeCandidate({
      target: { kind: 'obligation', dependencies: [{ watchId: dep.watchId, generation: 1 }], readyWhen: 'all_terminal', hostAction: 'Decide the next step' },
      mission: { ...makeCandidate().mission, deadlineAt: new Date(clock.wallNow() - 1000).toISOString() }
    }), actor);
    await engine.inspectWatch(o.watchId);
    assert.equal(store.transaction(tx => tx.listEpisodes(o.watchId, 10)).length, 0, 'dependency running -> does not fire even when due');
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'succeeded');
    await engine.inspectWatch(dep.watchId);
    await engine.inspectWatch(o.watchId);
    assert.equal(store.transaction(tx => tx.listEpisodes(o.watchId, 10)).length, 1, 'dependency terminal + due -> fires');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('registration validation: missing members / cross-owner / empty group / missing hostAction rejected', async () => {
  const fx = makeFixture('pw-val-');
  try {
    writeStatus(fx.sourceRoot, 'run-1', 'attempt-1', 'running');
    const { store, service } = await setup(fx);
    const actor = makeActor();
    await assert.rejects(
      () => service.register('v1', makeCandidate({ target: { kind: 'group', members: [{ watchId: 'w-missing', generation: 1 }], readyWhen: 'all_terminal' } }), actor),
      (e: unknown) => e instanceof WatcherError && e.code === 'UNKNOWN_TARGET'
    );
    const run = await service.register('v2', makeCandidate(), actor);
    await assert.rejects(
      () => service.register('v3', makeCandidate({ target: { kind: 'group', members: [{ watchId: run.watchId, generation: 99 }], readyWhen: 'all_terminal' } }), actor),
      (e: unknown) => e instanceof WatcherError && e.code === 'STALE_REVISION'
    );
    await assert.rejects(
      () => service.register('v4', makeCandidate({ target: { kind: 'group', members: [], readyWhen: 'all_terminal' } }), actor),
      (e: unknown) => e instanceof WatcherError && e.code === 'INVALID_SPEC'
    );
    await assert.rejects(
      () => service.register('v5', makeCandidate({ target: { kind: 'obligation', dependencies: [], readyWhen: 'all_terminal', hostAction: '' } }), actor),
      (e: unknown) => e instanceof WatcherError && e.code === 'INVALID_SPEC'
    );
    const other = await service.register('v6', makeCandidate(), actor);
    const actor2 = makeActor('sess-2');
    await assert.rejects(
      () => service.register('v7', makeCandidate({ target: { kind: 'group', members: [{ watchId: other.watchId, generation: 1 }], readyWhen: 'all_terminal' } }), actor2),
      (e: unknown) => e instanceof WatcherError && e.code === 'CAPABILITY_DENIED'
    );
    store.close();
  } finally {
    fx.cleanup();
  }
});
