import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { AgentFileAdapter, fileRunId } from '../../src/source/agent-file/adapter.js';
import type { RunTarget } from '../../src/contracts/interfaces.js';

const signal = new AbortController().signal;

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pw-agent-file-'));
}

function fileTarget(filePath: string, patterns: { okPattern?: string; failPattern?: string } = {}): RunTarget {
  return {
    kind: 'run', sourceId: 'agent-file', taskId: 'file',
    runId: fileRunId(filePath, patterns), attemptId: 'attempt-1',
    file: { path: filePath, ...patterns }
  };
}

test('agent-file: missing file → unknown + gap (degraded), never terminal', async () => {
  const dir = tmpDir();
  const t = fileTarget(path.join(dir, 'not-yet.log'), { okPattern: 'DONE' });
  const adapter = new AgentFileAdapter();
  const r = await adapter.read(t, undefined, signal);
  assert.equal(r.gap, true);
  const status = r.observations.find(o => o.kind === 'status')!;
  assert.equal((status.payload as { state: string }).state, 'unknown');
  assert.equal((r.nextCursor as { done: boolean }).done, false);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('agent-file: growth → log_delta incremental; state-change-only status discipline', async () => {
  const dir = tmpDir();
  const filePath = path.join(dir, 'build.log');
  fs.writeFileSync(filePath, 'line-1\n');
  const t = fileTarget(filePath, { okPattern: '__EXEC_EXIT__:0' });
  const adapter = new AgentFileAdapter();

  const r1 = await adapter.read(t, undefined, signal);
  assert.equal(r1.observations.filter(o => o.kind === 'log_delta').length, 1);
  const st1 = r1.observations.find(o => o.kind === 'status')!;
  assert.equal((st1.payload as { state: string }).state, 'running');

  // no growth and no state change -> zero observations (feeds silence semantics)
  const r2 = await adapter.read(t, r1.nextCursor, signal);
  assert.equal(r2.observations.length, 0);

  fs.appendFileSync(filePath, 'line-2\nline-3\n');
  const r3 = await adapter.read(t, r2.nextCursor, signal);
  const delta = r3.observations.find(o => o.kind === 'log_delta')!;
  assert.deepEqual((delta.payload as { lines: string[] }).lines, ['line-2', 'line-3']);
  assert.equal(r3.observations.filter(o => o.kind === 'status').length, 0, 'state unchanged → no status observation');

  fs.rmSync(dir, { recursive: true, force: true });
});

test('agent-file: declared patterns on tail are the only content-terminal; fail ordered before ok', async () => {
  const dir = tmpDir();
  const adapter = new AgentFileAdapter();

  // okPattern hit -> succeeded; cursor.done short-circuits subsequent reads
  const okPath = path.join(dir, 'ok.log');
  fs.writeFileSync(okPath, 'working\n__EXEC_EXIT__:0\n');
  const tOk = fileTarget(okPath, { okPattern: '__EXEC_EXIT__:0', failPattern: '__EXEC_EXIT__:-?[1-9]' });
  const rOk = await adapter.read(tOk, undefined, signal);
  assert.equal((rOk.observations.find(o => o.kind === 'status')!.payload as { state: string }).state, 'succeeded');
  assert.equal((rOk.nextCursor as { done: boolean }).done, true);
  const rOk2 = await adapter.read(tOk, rOk.nextCursor, signal);
  assert.equal(rOk2.observations.length, 0, 'terminal already reported → no further reads');

  // failPattern hit (even if the same file also has ok content) -> failed
  const failPath = path.join(dir, 'fail.log');
  fs.writeFileSync(failPath, 'ok: __EXEC_EXIT__:0\nretry\n__EXEC_EXIT__:2\n');
  const tFail = fileTarget(failPath, { okPattern: '__EXEC_EXIT__:0', failPattern: '__EXEC_EXIT__:-?[1-9]' });
  const rFail = await adapter.read(tFail, undefined, signal);
  const stFail = rFail.observations.find(o => o.kind === 'status')!;
  assert.equal((stFail.payload as { state: string }).state, 'failed', 'fail pattern must win over earlier ok content');

  fs.rmSync(dir, { recursive: true, force: true });
});

test('agent-file: rotation (size < cursor) → cursor reset + gap', async () => {
  const dir = tmpDir();
  const filePath = path.join(dir, 'rotated.log');
  fs.writeFileSync(filePath, 'aaaa\n');
  const t = fileTarget(filePath);
  const adapter = new AgentFileAdapter();
  const r1 = await adapter.read(t, undefined, signal);
  assert.equal((r1.nextCursor as { offset: number }).offset, 5);

  fs.writeFileSync(filePath, 'bb\n'); // rotation/truncation
  const r2 = await adapter.read(t, r1.nextCursor, signal);
  assert.equal(r2.gap, true, 'rotation must surface as source gap (health degraded)');
  const delta = r2.observations.find(o => o.kind === 'log_delta');
  assert.ok(delta, 're-read from zero after rotation');

  fs.rmSync(dir, { recursive: true, force: true });
});

test('agent-file: no pattern declared → progress/silence facts only, no fabricated terminal', async () => {
  const dir = tmpDir();
  const filePath = path.join(dir, 'plain.log');
  fs.writeFileSync(filePath, 'whatever\n__EXEC_EXIT__:0\n');
  const t = fileTarget(filePath); // no pattern
  const adapter = new AgentFileAdapter();
  const r = await adapter.read(t, undefined, signal);
  const st = r.observations.find(o => o.kind === 'status')!;
  assert.equal((st.payload as { state: string }).state, 'running');
  assert.equal((r.nextCursor as { done: boolean }).done, false);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('agent-file: tail probe reads bounded last lines', async () => {
  const dir = tmpDir();
  const filePath = path.join(dir, 'probe.log');
  fs.writeFileSync(filePath, Array.from({ length: 60 }, (_, i) => `l${i}`).join('\n') + '\n');
  const t = fileTarget(filePath, { okPattern: 'l5[0-9]' });
  const adapter = new AgentFileAdapter();
  const probes = await adapter.probes(t, {} as never);
  assert.equal(probes.length, 1);
  const obs = await adapter.execute(probes[0]!, signal);
  const lines = (obs[0]!.payload as { lines: string[] }).lines;
  assert.ok(lines.length <= 41, 'probe output bounded');
  assert.ok(lines.includes('l59'), 'tail includes the last line');
  fs.rmSync(dir, { recursive: true, force: true });
});
