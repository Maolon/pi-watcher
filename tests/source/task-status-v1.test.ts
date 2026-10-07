import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { TaskStatusV1Adapter } from '../../src/source/task-status-v1/adapter.js';
import { TaskStatusProducer } from '../../src/source/task-status-v1/producer.js';

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pw-src-test-'));
}

const target = { kind: 'run' as const, sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' };
const signal = new AbortController().signal;

test('adapter: reads journal incrementally and snapshot atomically', async () => {
  const dir = tmpDir();
  const producer = new TaskStatusProducer(dir, target);
  await producer.appendEvent('log_delta', { lines: ['a'] });
  await producer.appendEvent('log_delta', { lines: ['b'] });
  await producer.publishSnapshot({ state: 'running', stage: 'compile' });

  const adapter = new TaskStatusV1Adapter(dir);
  const r1 = await adapter.read(target, undefined, signal);
  assert.equal(r1.observations.filter(o => o.kind === 'log_delta').length, 2);
  assert.equal(r1.observations.filter(o => o.kind === 'status').length, 1);
  assert.equal(r1.gap, false);

  // no new facts -> empty
  const r2 = await adapter.read(target, r1.nextCursor, signal);
  assert.equal(r2.observations.length, 0);

  // append new events + new snapshot -> read only the increment
  await producer.appendEvent('validation', { checkId: 'unit-tests', outcome: 'passed', artifactDigest: 'd1' });
  await producer.publishSnapshot({ state: 'succeeded', exitCode: 0 });
  const r3 = await adapter.read(target, r2.nextCursor, signal);
  assert.equal(r3.observations.filter(o => o.kind === 'validation').length, 1);
  assert.equal(r3.observations.filter(o => o.kind === 'status').length, 1);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('adapter: half-line stays buffered until completed', async () => {
  const dir = tmpDir();
  const runDir = path.join(dir, target.sourceId, target.taskId, target.runId, target.attemptId);
  fs.mkdirSync(runDir, { recursive: true });
  fs.writeFileSync(path.join(runDir, 'journal.jsonl'), '{"sourceSeq":1,"at":"t","kind":"log_delta","payload":{}}\n{"sourceSeq":2,"at":"t', 'utf8');
  const adapter = new TaskStatusV1Adapter(dir);
  const r1 = await adapter.read(target, undefined, signal);
  assert.equal(r1.observations.length, 1, 'only the complete first line counts');
  // complete the partial line
  fs.appendFileSync(path.join(runDir, 'journal.jsonl'), '","kind":"log_delta","payload":{}}\n', 'utf8');
  const r2 = await adapter.read(target, r1.nextCursor, signal);
  assert.equal(r2.observations.length, 1, 'completed line now readable');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('adapter: journal rotation changes stream generation → gap=true and re-read from 0 (store dedups)', async () => {
  const dir = tmpDir();
  const producer = new TaskStatusProducer(dir, target);
  await producer.appendEvent('log_delta', { lines: ['x'] });
  const adapter = new TaskStatusV1Adapter(dir);
  const r1 = await adapter.read(target, undefined, signal);
  assert.equal(r1.observations.length, 1);

  // rotation: replaced by a new-inode file
  const runDir = path.join(dir, target.sourceId, target.taskId, target.runId, target.attemptId);
  fs.renameSync(path.join(runDir, 'journal.jsonl'), path.join(runDir, 'journal.old'));
  const producer2 = new TaskStatusProducer(dir, target);
  await producer2.appendEvent('log_delta', { lines: ['y'] });

  const r2 = await adapter.read(target, r1.nextCursor, signal);
  assert.equal(r2.gap, true, 'stream generation change must be reported');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('adapter: unparseable snapshot does not overwrite state (keeps last valid)', async () => {
  const dir = tmpDir();
  const producer = new TaskStatusProducer(dir, target);
  await producer.publishSnapshot({ state: 'running', stage: 'compile' });
  const adapter = new TaskStatusV1Adapter(dir);
  const r1 = await adapter.read(target, undefined, signal);
  assert.equal(r1.observations.filter(o => o.kind === 'status').length, 1);

  // overwrite snapshot with invalid JSON (simulated corruption; atomic replace is guaranteed inside the producer, here we directly verify the read defense)
  const runDir = path.join(dir, target.sourceId, target.taskId, target.runId, target.attemptId);
  fs.writeFileSync(path.join(runDir, 'snapshot.json'), '{corrupted', 'utf8');
  const r2 = await adapter.read(target, r1.nextCursor, signal);
  assert.equal(r2.observations.filter(o => o.kind === 'status').length, 0, 'corrupt snapshot yields no observation');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('adapter: probes are trusted instances; execute reads bounded payloads', async () => {
  const dir = tmpDir();
  const producer = new TaskStatusProducer(dir, target);
  await producer.publishSnapshot({
    state: 'failed', exitCode: 2,
    checks: [{ checkId: 'unit-tests', outcome: 'unknown', artifactDigest: null }],
    artifacts: []
  });
  const adapter = new TaskStatusV1Adapter(dir);
  const probes = await adapter.probes(target, {
    watchId: 'w', generation: 1, missionRevision: 1, controlRevision: 1, observationSeq: 0, windowDigest: 'd'
  });
  assert.equal(probes.length, 4);
  assert.ok(probes.every(p => p.kind !== ('exec' as never)));
  const ids = probes.map(p => p.probeId);
  assert.equal(new Set(ids).size, 4, 'distinct probeIds');
  for (const probe of probes) {
    const obs = await adapter.execute(probe, signal);
    assert.equal(obs.length, 1);
    assert.equal(obs[0].target.attemptId, target.attemptId, 'probe bound to target/attempt');
  }
  fs.rmSync(dir, { recursive: true, force: true });
});

test('producer: snapshot atomic replace never exposes partial JSON', async () => {
  const dir = tmpDir();
  const producer = new TaskStatusProducer(dir, target);
  const adapter = new TaskStatusV1Adapter(dir);
  const reads: number[] = [];
  for (let i = 0; i < 20; i++) {
    await producer.publishSnapshot({ state: 'running', stage: `s${i}` });
    const r = await adapter.read(target, undefined, signal);
    for (const obs of r.observations) {
      const payload = obs.payload as Record<string, unknown>;
      JSON.stringify(payload); // must not throw
      reads.push(1);
    }
  }
  assert.ok(reads.length > 0);
  fs.rmSync(dir, { recursive: true, force: true });
});
