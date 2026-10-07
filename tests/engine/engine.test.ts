import test from 'node:test';
import assert from 'node:assert/strict';
import { startRuntime, type WatcherRuntime } from '../../src/runtime.js';
import { WatchStore } from '../../src/storage/store.js';
import { runScenario, scenarioSteps, TaskStatusProducer } from '../../src/source/task-status-v1/producer.js';
import { makeActor, makeCandidate, makeFixture } from './fixture.js';
import { ManualClock } from '../../src/util/clock.js';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
const signal = () => new AbortController().signal;

const target = { sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' };

interface Insp {
  episodes: Array<{ episodeId: string; kind: string; state: string; body: Record<string, unknown> }>;
  resultCards: Array<Record<string, unknown>>;
  snapshot: Record<string, unknown>;
  lifecycle: string;
  health: string;
}

async function inspectJson(rt: WatcherRuntime, watchId: string): Promise<Insp> {
  return (await rt.service.inspect(watchId, makeActor())) as unknown as Insp;
}

test('engine: explicit terminal → task.failed episode + durable result card (no model needed)', async () => {
  const fx = makeFixture('pw-eng-term-');
  await runScenario(fx.sourceRoot, target, scenarioSteps('build-failure', 10));
  const rt = await startRuntime({
    rootDir: fx.rootDir,
    sourceRoots: new Map([['executor-local', fx.sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]])
  });
  const actor = makeActor();
  const spec = await rt.service.register('req-1', makeCandidate() as never, actor);
  const outcome = await rt.engine.inspectWatch(spec.watchId);
  assert.ok(outcome);
  assert.ok(outcome!.ingested >= 3, 'journal + snapshot observations ingested');
  assert.equal(outcome!.produced.episodes.length, 1);
  assert.equal(outcome!.produced.resultCards.length, 1);

  const insp = await inspectJson(rt, spec.watchId);
  const episode = insp.episodes.find(e => e.kind === 'task.failed');
  assert.ok(episode, 'task.failed episode exists');
  assert.equal(episode!.state, 'open');
  const card = insp.resultCards[0];
  assert.equal(card.executorState, 'failed');
  assert.deepEqual(card.checks, [{ checkId: 'unit-tests', outcome: 'unknown', artifactDigest: null }]);
  assert.equal(card.businessAcceptance, 'pending_host');
  assert.ok(String(card.summary).includes('Failed'));

  // truth = outbox row (immutable), the display copy exists separately; terminal state also produces an attention envelope (local annotation, no egress without relay)
  const outbox = rt.store.transaction(tx => tx.listOutboxByWatch(spec.watchId, 10));
  assert.equal(outbox.length, 2);
  const resultRow = outbox.find(o => o.eventType === 'watcher.result.v1')!;
  assert.equal(resultRow.admission, 'pending');
  assert.equal((resultRow.admissionJson as Record<string, unknown>).transport, 'local-display');
  const attentionRow = outbox.find(o => o.eventType === 'watcher.attention.v1')!;
  assert.equal((attentionRow.admissionJson as Record<string, unknown>).transport, 'local-display');

  // Inspect again: no new facts -> no duplicate episode/card
  await rt.engine.inspectWatch(spec.watchId);
  const insp2 = await inspectJson(rt, spec.watchId);
  assert.equal(insp2.episodes.filter(e => e.kind === 'task.failed').length, 1);
  assert.equal(insp2.resultCards.length, 1);
  rt.close();
  fx.cleanup();
});

test('engine: succeeded with passing checks and artifacts → succeeded-evidence card', async () => {
  const fx = makeFixture('pw-eng-succ-');
  await runScenario(fx.sourceRoot, target, scenarioSteps('build-success', 10));
  const rt = await startRuntime({
    rootDir: fx.rootDir,
    sourceRoots: new Map([['executor-local', fx.sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]])
  });
  const actor = makeActor();
  const spec = await rt.service.register('req-2', makeCandidate() as never, actor);
  await rt.engine.inspectWatch(spec.watchId);
  const insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.snapshot.taskState, 'succeeded');
  assert.equal(insp.episodes.filter(e => e.kind === 'task.terminal').length, 1);
  const card = insp.resultCards[0];
  assert.equal(card.executorState, 'succeeded');
  assert.deepEqual(card.checks, [{ checkId: 'unit-tests', outcome: 'passed', artifactDigest: 'deadbeef01' }]);
  assert.ok(String(card.summary).includes('Succeeded'));
  rt.close();
  fx.cleanup();
});

test('engine: deadline crossing creates exactly one deadline.exceeded episode while not terminal', async () => {
  const fx = makeFixture('pw-eng-dl-');
  const producer = new TaskStatusProducer(fx.sourceRoot, target);
  await producer.publishSnapshot({ state: 'running', stage: 'compile' });
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();
  const spec = await rt.service.register('req-3', makeCandidate({
    mission: {
      objective: 'o', scope: 's', checkpointId: 'compile-and-test',
      requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required',
      deadlineAt: new Date(Date.now() - 1000).toISOString() // already expired
    }
  }) as never, actor);
  await rt.engine.inspectWatch(spec.watchId);
  let insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.episodes.filter(e => e.kind === 'deadline.exceeded').length, 1);
  // Repeated inspection does not open a new episode (same slot supersede)
  await rt.engine.inspectWatch(spec.watchId);
  insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.episodes.filter(e => e.kind === 'deadline.exceeded').length, 1);
  rt.close();
  fx.cleanup();
});

test('engine: silence beyond maxSilenceMs degrades health and opens monitor.degraded episode (not task failure)', async () => {
  const fx = makeFixture('pw-eng-sil-');
  const producer = new TaskStatusProducer(fx.sourceRoot, target);
  await producer.publishSnapshot({ state: 'running', stage: 'compile' });
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();
  const spec = await rt.service.register('req-4', makeCandidate({
    limits: {
      pollMinMs: 1000, pollMaxMs: 5000,
      maxSilenceMs: 1, // silence limit exceeded immediately
      maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10,
      expiresAt: new Date(Date.now() + 3600_000).toISOString()
    }
  }) as never, actor);
  await rt.engine.inspectWatch(spec.watchId);
  await new Promise(r => setTimeout(r, 30));
  await rt.engine.inspectWatch(spec.watchId);
  const insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.health, 'degraded');
  assert.equal(insp.episodes.filter(e => e.kind === 'monitor.degraded').length, 1);
  assert.equal(insp.snapshot.taskState, 'running', 'monitoring unreachable is not task failure');
  rt.close();
  fx.cleanup();
});

test('engine: restart recovery — committed facts survive, epoch advances, no duplicate episodes', async () => {
  const fx = makeFixture('pw-eng-restart-');
  await runScenario(fx.sourceRoot, target, scenarioSteps('build-failure', 10));
  const rt1 = await startRuntime({
    rootDir: fx.rootDir,
    sourceRoots: new Map([['executor-local', fx.sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]])
  });
  const actor = makeActor();
  const spec = await rt1.service.register('req-5', makeCandidate() as never, actor);
  await rt1.engine.inspectWatch(spec.watchId);
  const epoch1 = rt1.store.runtimeEpoch;
  rt1.close();

  // "Restart": reopen the same root
  const rt2 = await startRuntime({
    rootDir: fx.rootDir,
    sourceRoots: new Map([['executor-local', fx.sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]])
  });
  assert.ok(rt2.store.runtimeEpoch > epoch1, 'runtime epoch must advance after restart');
  const meta = rt2.store.transaction(tx => tx.meta());
  assert.equal(meta.recoveryAttentionHold, true, 'attention hold takes effect after recovery (design 7.5)');

  const insp = await inspectJson(rt2, spec.watchId);
  assert.equal(insp.snapshot.taskState, 'failed', 'committed facts recovered');
  assert.equal(insp.episodes.filter(e => e.kind === 'task.failed').length, 1, 'episode is not duplicated by restart');

  // Continue inspecting after recovery: resume reading from the last cursor, no duplicate observations
  await rt2.engine.inspectWatch(rt2.store.transaction(tx => tx.getWatchRow(spec.watchId))!.watchId);
  const insp2 = await inspectJson(rt2, spec.watchId);
  assert.equal(insp2.resultCards.length, 1);
  rt2.close();
  fx.cleanup();
});

test('engine: limits.expiresAt reached → lifecycle expired (not business failure)', async () => {
  const fx = makeFixture('pw-eng-exp-');
  const producer = new TaskStatusProducer(fx.sourceRoot, target);
  await producer.publishSnapshot({ state: 'running' });
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();
  const spec = await rt.service.register('req-6', makeCandidate({
    limits: {
      pollMinMs: 1000, pollMaxMs: 5000, maxSilenceMs: 60000,
      maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10,
      expiresAt: new Date(Date.now() - 1000).toISOString()
    }
  }) as never, actor);
  await rt.engine.inspectWatch(spec.watchId);
  const insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.lifecycle, 'expired');
  rt.close();
  fx.cleanup();
});

test('engine: paused watch does not inspect or publish (R08 isolation)', async () => {
  const fx = makeFixture('pw-eng-pause-');
  await runScenario(fx.sourceRoot, target, scenarioSteps('build-failure', 10));
  const rt = await startRuntime({
    rootDir: fx.rootDir,
    sourceRoots: new Map([['executor-local', fx.sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]])
  });
  const actor = makeActor();
  const spec = await rt.service.register('req-7', makeCandidate() as never, actor);
  await rt.service.control('req-7-p', spec.watchId, 1, 'pause', 'test pause', actor);
  const outcome = await rt.engine.inspectWatch(spec.watchId);
  assert.equal(outcome!.ingested, 0);
  const insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.resultCards.length, 0, 'paused watch must not produce cards');
  assert.equal(insp.episodes.length, 0);
  rt.close();
  fx.cleanup();
});

test('store: second instance on same root reports ROOT_LOCK_HELD (singleton)', async () => {
  const fx = makeFixture('pw-eng-lock-');
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  await assert.rejects(
    WatchStore.open(fx.rootDir, {}),
    (e: unknown) => e instanceof Error && /already locked/.test(e.message)
  );
  rt.close();
  fx.cleanup();
});

test('engine: terminal watch — no post-terminal silence degradation, attention expires, auto-close after TTL', async () => {
  const fx = makeFixture('pw-eng-autoclose-');
  const producer = new TaskStatusProducer(fx.sourceRoot, target);
  await producer.publishSnapshot({ state: 'running', stage: 'compile' });
  const clock = new ManualClock(Date.now());
  const notices: Array<{ reasonCode: string; transport: string }> = [];
  const rt = await startRuntime({
    rootDir: fx.rootDir,
    sourceRoots: new Map([['executor-local', fx.sourceRoot]]),
    clock,
    onAttention: n => notices.push({ reasonCode: n.reasonCode, transport: n.transport })
  });
  const actor = makeActor();
  const spec = await rt.service.register('req-ac-1', makeCandidate({
    limits: {
      pollMinMs: 1000, pollMaxMs: 5000,
      maxSilenceMs: 5_000, // far below TTL: if the silence rule still applied after terminal state it would certainly fire
      maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10,
      expiresAt: new Date(clock.wallNow() + 3600_000).toISOString()
    },
    policy: { ...makeCandidate().policy, attentionTtlMs: 120_000 }
  }) as never, actor);
  await rt.engine.inspectWatch(spec.watchId);
  let insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.snapshot.taskState, 'running');

  // Terminal state -> task.terminal episode + attention (local-display -> empty-audience, not pending)
  await producer.publishSnapshot({ state: 'succeeded' });
  await rt.engine.inspectWatch(spec.watchId);
  insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.snapshot.taskState, 'succeeded');
  assert.equal(insp.lifecycle, 'active');
  assert.equal(notices.length, 1);
  assert.equal(notices[0]!.reasonCode, 'task.terminal');
  assert.equal(notices[0]!.transport, 'local-display');
  const attention = rt.store.transaction(tx => tx.listOutboxByWatch(spec.watchId, 10))
    .find(o => o.eventType === 'watcher.attention.v1')!;
  assert.equal(attention.admission, 'empty-audience', 'local-display attention must not sit pending');

  // Past maxSilenceMs + attentionTtl + grace: silence after terminal state does not trigger degraded; auto-close after TTL
  clock.advanceWall(120_000 + 60_000 + 10_000);
  await rt.engine.runDue();
  insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.lifecycle, 'closed', 'terminal watch auto-closes after attention TTL + grace');
  assert.equal(insp.health, 'healthy', 'post-terminal silence must not degrade health');
  assert.ok(!insp.episodes.some(e => e.kind === 'monitor.degraded'), 'no spurious monitor.degraded after terminal');
  assert.ok(insp.episodes.every(e => e.state === 'superseded'), 'open episodes superseded on auto-close');
  const attentionAfter = rt.store.transaction(tx => tx.listOutboxByWatch(spec.watchId, 10))
    .find(o => o.eventType === 'watcher.attention.v1')!;
  assert.equal(attentionAfter.admission, 'empty-audience', 'local-display admission is terminal (displayed), never pending forever');
  assert.equal(rt.store.transaction(tx => tx.countPendingAttentions(clock.wallNow())), 0, 'widget pending count drops to 0');
  rt.close();
  fx.cleanup();
});

test('service: close() supersedes open episodes (no zombie open episodes on closed watches)', async () => {
  const fx = makeFixture('pw-eng-close-eps-');
  const producer = new TaskStatusProducer(fx.sourceRoot, target);
  await producer.publishSnapshot({ state: 'running' });
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();
  const spec = await rt.service.register('req-ce-1', makeCandidate({
    mission: {
      objective: 'o', scope: 's', checkpointId: 'compile-and-test',
      requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required',
      deadlineAt: new Date(Date.now() - 1000).toISOString()
    }
  }) as never, actor);
  await rt.engine.inspectWatch(spec.watchId);
  let insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.episodes.filter(e => e.kind === 'deadline.exceeded' && e.state === 'open').length, 1);

  await rt.service.control('req-ce-2', spec.watchId, 1, 'close', 'owner done', actor);
  insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.lifecycle, 'closed');
  assert.ok(insp.episodes.every(e => e.state === 'superseded'), 'close must supersede remaining open episodes');
  rt.close();
  fx.cleanup();
});

test('engine: no declared pattern → no fabricated terminal (agent-file honesty), exited-unknown projection stays available for sources with real exit evidence', async () => {
  const fx = makeFixture('pw-eng-exu-');
  const producer = new TaskStatusProducer(fx.sourceRoot, target);
  // hard-rules projection: real exit evidence (exited) still maps to an honest outcome-unknown terminal state
  const { isTerminalFact, statusPayloadToTaskState } = await import('../../src/engine/hard-rules.js');
  const st = statusPayloadToTaskState({ state: 'unknown', exited: true, summary: 'process exited; outcome unknown' } as never);
  assert.equal(st.taskState, 'unknown');
  assert.equal(st.exited, true);
  assert.ok(isTerminalFact({ taskState: st.taskState, exited: st.exited }), 'exited-unknown is a terminal fact');
  assert.ok(!isTerminalFact({ taskState: 'unknown', exited: false }), 'plain unknown stays non-terminal');
  assert.ok(!isTerminalFact({ taskState: 'running', exited: false }));
  // Engine path: agent-file without a pattern does not fabricate a terminal state -- a handle closed right after writing is still running
  // (a generic file with no fd holder is not exit evidence, unlike the semantics of the removed exec adapter)
  const { AgentFileAdapter } = await import('../../src/source/agent-file/adapter.js');
  const tmpLog = fs.mkdtempSync(path.join(os.tmpdir(), 'pw-exu-'));
  try {
    const log = path.join(tmpLog, 'app.log');
    fs.writeFileSync(log, 'partial output\n');
    const fh = fs.openSync(log, 'r+');
    const adapter = new AgentFileAdapter();
    const t = { kind: 'run' as const, sourceId: 'agent-file', taskId: 'file', runId: 'file-x', attemptId: 'attempt-1', file: { path: log } };
    const r0 = await adapter.read(t, undefined, signal());
    const st0 = statusPayloadToTaskState(r0.observations.find(o => o.kind === 'status')!.payload);
    assert.equal(st0.taskState, 'running');
    fs.closeSync(fh);
    const r1 = await adapter.read(t, r0.nextCursor, signal());
    assert.equal(r1.observations.filter(o => o.kind === 'status').length, 0, 'state unchanged → no status observation');
    assert.ok(!isTerminalFact({ taskState: 'running' }), 'no pattern hit → never terminal');
  } finally {
    fs.rmSync(tmpLog, { recursive: true, force: true });
  }
  fx.cleanup();
});

test('terminal + host responded (all episodes resolved) → fast close, not held for attention TTL', async () => {
  const fx = makeFixture('pw-fastclose-');
  const producer = new TaskStatusProducer(fx.sourceRoot, target);
  await producer.publishSnapshot({ state: 'running', stage: 'compile' });
  const clock = new ManualClock(Date.now());
  const rt = await startRuntime({
    rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]), clock
  });
  try {
    const actor = makeActor();
    const spec = await rt.service.register('req-fc-1', makeCandidate({
      limits: {
        pollMinMs: 1000, pollMaxMs: 5000, maxSilenceMs: 60_000,
        maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10,
        expiresAt: new Date(clock.wallNow() + 3600_000).toISOString()
      },
      policy: { ...makeCandidate().policy, attentionTtlMs: 120_000 }
    }) as never, actor);
    await rt.engine.inspectWatch(spec.watchId);
    await producer.publishSnapshot({ state: 'succeeded' });
    await rt.engine.inspectWatch(spec.watchId);
    // Host response: episode -> resolved (simulates the state after relay_respond flows back)
    const eps = rt.store.transaction(tx => tx.listEpisodes(spec.watchId, 10));
    assert.ok(eps.length > 0, 'terminal episode exists');
    for (const e of eps) {
      await rt.service.ackEpisode(`req-fc-a-${e.episodeId}`, e.episodeId, 'resolved', 'host responded', undefined, actor);
    }
    // Only the 5s grace passes (far below the 120s TTL): respond-then-close
    clock.advanceWall(6_000);
    await rt.engine.runDue();
    const insp = await inspectJson(rt, spec.watchId);
    assert.equal(insp.lifecycle, 'closed', 'host-responded terminal watch must close fast, not hold the TTL window');
  } finally {
    rt.stopLoop();
    fx.cleanup();
  }
});
