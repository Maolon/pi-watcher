import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { AgentCheckAdapter, checkRunId } from '../../src/source/agent-check/adapter.js';
import type { RunTarget } from '../../src/contracts/interfaces.js';

const signal = new AbortController().signal;

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pw-agent-check-'));
}

function checkTarget(cmd: string, extra: { okPattern?: string; failPattern?: string; failCodes?: number[]; timeoutMs?: number } = {}): RunTarget {
  return {
    kind: 'run', sourceId: 'agent-check', taskId: 'check',
    runId: checkRunId(cmd, extra), attemptId: 'attempt-1',
    check: { cmd, ...extra }
  };
}

const statusOf = (r: { observations: Array<{ kind: string; payload: unknown }> }): { state: string; summary: string } | null => {
  const st = r.observations.find(o => o.kind === 'status');
  return st ? st.payload as { state: string; summary: string } : null;
};

test('agent-check: exit 0 → succeeded terminal (marker philosophy generalizes)', async () => {
  const adapter = new AgentCheckAdapter();
  const t = checkTarget('echo ready');
  const r = await adapter.read(t, undefined, signal);
  const st = statusOf(r)!;
  assert.equal(st.state, 'succeeded');
  assert.equal((r.nextCursor as { done: boolean }).done, true);
  // output changed -> log_delta recorded
  assert.ok(r.observations.some(o => o.kind === 'log_delta'));
});

test('agent-check: unmapped nonzero → pending (running), not failure', async () => {
  const adapter = new AgentCheckAdapter();
  const t = checkTarget('exit 3');
  const r = await adapter.read(t, undefined, signal);
  const st = statusOf(r)!;
  assert.equal(st.state, 'running');
  assert.match(st.summary, /pending: check exit 3/);
  assert.equal((r.nextCursor as { done: boolean }).done, false);
});

test('agent-check: declared failCodes → failed terminal', async () => {
  const adapter = new AgentCheckAdapter();
  const t = checkTarget('exit 7', { failCodes: [7, 8] });
  const r = await adapter.read(t, undefined, signal);
  assert.equal(statusOf(r)!.state, 'failed');
  assert.equal((r.nextCursor as { done: boolean }).done, true);
});

test('agent-check: okPattern/failPattern on stdout decide when CLI always exits 0', async () => {
  const adapter = new AgentCheckAdapter();
  // gh run view style: exit is always 0, stdout carries the status
  const pending = checkTarget("echo 'completed/null'", { okPattern: 'completed/success', failPattern: 'completed/failure' });
  const r1 = await adapter.read(pending, undefined, signal);
  assert.equal(statusOf(r1)!.state, 'running', 'pattern not hit yet → pending');

  const ok = checkTarget("echo 'completed/success'", { okPattern: 'completed/success', failPattern: 'completed/failure' });
  const r2 = await adapter.read(ok, undefined, signal);
  assert.equal(statusOf(r2)!.state, 'succeeded');

  const fail = checkTarget("echo 'completed/failure'", { okPattern: 'completed/success', failPattern: 'completed/failure' });
  const r3 = await adapter.read(fail, undefined, signal);
  assert.equal(statusOf(r3)!.state, 'failed', 'failPattern must win even with exit 0');
});

test('agent-check: broken check (127/timeout) → unknown + gap degraded, never a task verdict', async () => {
  const adapter = new AgentCheckAdapter();
  // 127: command does not exist
  const t = checkTarget('definitely-not-a-command-xyz-1');
  const r = await adapter.read(t, undefined, signal);
  const st = statusOf(r)!;
  assert.equal(st.state, 'unknown');
  assert.match(st.summary, /check broken/);
  assert.equal(r.gap, true, 'broken check must degrade health, not fabricate a task verdict');
  assert.equal((r.nextCursor as { done: boolean }).done, false);

  // timeout: sleep exceeds timeoutMs -> broken
  const t2 = checkTarget('sleep 5', { timeoutMs: 1000 });
  const r2 = await adapter.read(t2, undefined, signal);
  assert.equal(statusOf(r2)!.state, 'unknown');
  assert.match(statusOf(r2)!.summary, /timed out/);
  assert.equal(r2.gap, true);
});

test('agent-check: unchanged output → no new observations (silence semantics for stuck-pending)', async () => {
  const adapter = new AgentCheckAdapter();
  const cmd = 'echo pending-state';
  const t = checkTarget(cmd, { okPattern: 'READY' });
  const r1 = await adapter.read(t, undefined, signal);
  assert.equal(statusOf(r1)!.state, 'running');
  // neither output nor status changed -> zero observations: no new information is not an evidence line
  const r2 = await adapter.read(t, r1.nextCursor, signal);
  assert.equal(r2.observations.length, 0);
});

test('agent-check: condition flips pending → ok (readiness flow)', async () => {
  const dir = tmpDir();
  const flag = path.join(dir, 'ready.flag');
  const adapter = new AgentCheckAdapter();
  const t = checkTarget(`test -f ${JSON.stringify(flag)} && echo READY`);
  const r1 = await adapter.read(t, undefined, signal);
  assert.equal(statusOf(r1)!.state, 'running', 'flag absent → pending');

  fs.writeFileSync(flag, '1');
  const r2 = await adapter.read(t, r1.nextCursor, signal);
  const st2 = statusOf(r2)!;
  assert.equal(st2.state, 'succeeded', 'flag present → exit 0 → terminal success');
  assert.equal((r2.nextCursor as { done: boolean }).done, true);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('agent-check: checkRunId stable for same cmd+verdict, distinct across verdicts', () => {
  const a = checkRunId('gh run view 1', { okPattern: 'success' });
  const b = checkRunId('gh run view 1', { okPattern: 'success' });
  const c = checkRunId('gh run view 1', { okPattern: 'success', failPattern: 'failure' });
  assert.equal(a, b);
  assert.notEqual(a, c);
});
