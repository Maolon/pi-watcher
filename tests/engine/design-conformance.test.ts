/**
 * Design-conformance regressions (code review 2026-10-07):
 *  - deadline crossing is an edge: one episode/attention per slot (design 3.6, I18)
 *  - new watch-file log lines change the semantic window; "new observation" uses the
 *    local observationSeq (design 5.1/5.5)
 *  - response pump never advances its cursor past a failed apply/confirm (T5, I16)
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { WatchStore } from '../../src/storage/store.js';
import { TaskStatusV1Adapter } from '../../src/source/task-status-v1/adapter.js';
import { AgentFileAdapter, fileRunId } from '../../src/source/agent-file/adapter.js';
import { WatchEngine, DEFAULT_SEMANTIC_CONFIG } from '../../src/engine/engine.js';
import { WatchService } from '../../src/engine/service.js';
import { MockJudge } from '../../src/jev/index.js';
import { SystemClock } from '../../src/util/clock.js';
import type { SourceAdapter } from '../../src/contracts/interfaces.js';
import { makeFixture, makeActor, makeCandidate } from './fixture.js';

function writeStatus(sourceRoot: string, state: string): void {
  const dir = path.join(sourceRoot, 'executor-local', 'build', 'run-1', 'attempt-1');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'snapshot.json'), JSON.stringify({
    schemaVersion: 1,
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' },
    state, snapshotSeq: Date.now(), updatedAt: new Date().toISOString()
  }));
  if (!fs.existsSync(path.join(dir, 'journal.jsonl'))) fs.writeFileSync(path.join(dir, 'journal.jsonl'), '');
}

async function setup(fx: ReturnType<typeof makeFixture>, judge?: MockJudge) {
  const store = await WatchStore.open(fx.rootDir, { mode: 'embedded' });
  const clock = new SystemClock();
  const adapters = new Map<string, SourceAdapter>([
    ['executor-local', new TaskStatusV1Adapter(fx.sourceRoot)],
    ['agent-file', new AgentFileAdapter()]
  ]);
  const engine = new WatchEngine({
    clock, store, adapters, judge,
    semantic: DEFAULT_SEMANTIC_CONFIG, semanticConsent: true, judgeRequiresConsent: false
  });
  const service = new WatchService({
    clock, store, engine,
    negotiation: { status: 'unavailable', transport: 'local-display' as const, detail: 'test' } as never,
    allowedSourceIds: ['executor-local', 'agent-file']
  });
  return { store, engine, service, clock };
}

test('deadline crossing raises one episode/attention per slot; not reopened after host resolve (I18)', async () => {
  const fx = makeFixture('pw-dl-once-');
  try {
    writeStatus(fx.sourceRoot, 'running');
    const { store, engine, service, clock } = await setup(fx);
    const actor = makeActor();
    const w = await service.register('r1', makeCandidate({
      mission: { ...makeCandidate().mission, deadlineAt: new Date(clock.wallNow() - 1000).toISOString() }
    }), actor);
    for (let i = 0; i < 3; i++) {
      await engine.inspectWatch(w.watchId);
      const open = store.transaction(tx => tx.listEpisodes(w.watchId, 50))
        .filter(e => e.kind === 'deadline.exceeded' && e.state === 'open');
      for (const e of open) await service.ackEpisode(`ack-${i}-${e.episodeId}`, e.episodeId, 'resolved', 'handled', undefined, actor);
    }
    const episodes = store.transaction(tx => tx.listEpisodes(w.watchId, 50)).filter(e => e.kind === 'deadline.exceeded');
    const attentions = store.transaction(tx => tx.listOutboxByWatch(w.watchId, 50)).filter(o => o.eventType === 'watcher.attention.v1');
    assert.equal(episodes.length, 1, 'one deadline episode per slot');
    assert.equal(attentions.length, 1, 'one deadline attention per slot');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('watch-file: new log lines open a new semantic window; repeating counts log-only windows (design 5.1/5.5)', async () => {
  const fx = makeFixture('pw-file-sem-');
  try {
    fs.mkdirSync(fx.sourceRoot, { recursive: true });
    const logPath = path.join(fx.sourceRoot, 'job.log');
    fs.writeFileSync(logPath, 'start\n');
    const judge = new MockJudge({ evaluateFn: () => ({ repeating_without_new_information: 0.95, context_sufficient: 0.9 }) });
    const { store, engine, service } = await setup(fx, judge);
    const patterns = { okPattern: 'ALL DONE' };
    const w = await service.register('f1', makeCandidate({
      target: {
        kind: 'run', sourceId: 'agent-file', taskId: 'file',
        runId: fileRunId(logPath, patterns), attemptId: 'attempt-1', file: { path: logPath, ...patterns }
      },
      mission: { ...makeCandidate().mission, requiredArtifacts: [], requiresChecks: false },
      policy: { ...makeCandidate().policy, semanticMode: 'shadow' }
    }), makeActor());

    await engine.inspectWatch(w.watchId);
    assert.equal(judge.evaluateCallCount, 1);
    // No new log lines: same window stays cached, no extra call
    await engine.inspectWatch(w.watchId);
    assert.equal(judge.evaluateCallCount, 1, 'unchanged window stays cached');

    for (let i = 0; i < 2; i++) {
      fs.appendFileSync(logPath, `ERROR: connection refused (try ${i})\nretrying same command\n`);
      await engine.inspectWatch(w.watchId);
    }
    assert.equal(judge.evaluateCallCount, 3, 'each new log delta opens a new window');
    const sem = store.transaction(tx => tx.getWatchRow(w.watchId))!.snapshot.semantic!;
    assert.ok(sem.repeatingStreak >= 2, `repeating streak counts log-only windows (got ${sem.repeatingStreak})`);
    assert.ok(sem.candidates && 'progress.repeating' in sem.candidates, 'progress.repeating candidate recorded in shadow');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('response pump: a failed apply/confirm holds the cursor; the re-read is idempotent (T5, I16)', async () => {
  const fx = makeFixture('pw-pump-');
  try {
    const store = await WatchStore.open(fx.rootDir, { mode: 'embedded' });
    const mk = (id: string) => ({
      responseId: id, deliveryRef: `d-${id}`, ownerBindingEpoch: 1, digest: `dg-${id}`,
      body: { episodeId: `ep-${id}`, action: 'received' } as never
    });
    let relayCursor = 0;
    const responses = [mk('r1'), mk('r2')];
    const confirmed: string[] = [];
    let failConfirmOnce = true;
    const port = {
      async readResponses(after: number) {
        relayCursor = 2;
        return { cursor: relayCursor, responses: after < 2 ? responses : [], resyncRequired: false };
      },
      async confirmApplied(operationId: string) {
        if (operationId === 'conf-r2' && failConfirmOnce) {
          failConfirmOnce = false;
          throw new Error('relay transient');
        }
        confirmed.push(operationId);
      }
    };
    const appliedIds = new Set<string>();
    let applyCalls = 0;
    const service = {
      async applyResponse(r: { responseId: string }) {
        applyCalls += 1;
        appliedIds.add(r.responseId); // the real service dedupes by responseId; here we only record the set
        return { outcome: 'applied' as const, applicationRevision: 1, code: 'APPLIED' as const };
      }
    };
    const { pumpResponses } = await import('../../src/runtime.js');
    await assert.rejects(() => pumpResponses(port, service as never, store, 'managed-responses'), /relay transient/);
    assert.equal(store.transaction(tx => tx.getRelayCursor('managed-responses')) ?? 0, 0, 'cursor held after failure');
    const second = await pumpResponses(port, service as never, store, 'managed-responses');
    assert.equal(second.cursor, 2);
    assert.deepEqual(confirmed.sort(), ['conf-r1', 'conf-r1', 'conf-r2'].sort(), 'r2 confirmed on retry; r1 re-confirm is idempotent by operationId');
    assert.deepEqual([...appliedIds].sort(), ['r1', 'r2']);
    assert.equal(applyCalls, 4);
    assert.equal(store.transaction(tx => tx.getRelayCursor('managed-responses')), 2, 'cursor advanced after full batch');
    store.close();
  } finally {
    fx.cleanup();
  }
});

test('no judge configured: semanticMode is reported unavailable instead of silently using a mock (design 5.5)', async () => {
  const fx = makeFixture('pw-nojudge-');
  try {
    writeStatus(fx.sourceRoot, 'running');
    const { store, engine, service } = await setup(fx);
    const w = await service.register('nj1', makeCandidate({ policy: { ...makeCandidate().policy, semanticMode: 'shadow' } }), makeActor());
    await engine.inspectWatch(w.watchId);
    const sem = store.transaction(tx => tx.getWatchRow(w.watchId))!.snapshot.semantic;
    assert.match(String(sem?.error), /semantic review unavailable/);
    assert.equal(store.transaction(tx => tx.listObservations(w.watchId, 10)).length > 0, true, 'hard rules still ingest');
    store.close();
  } finally {
    fx.cleanup();
  }
});
