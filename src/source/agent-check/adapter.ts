/**
 * agent-check SourceAdapter — agent-declared bounded check command
 * (agent-declared check source).
 *
 * Target: { kind:'run', sourceId:'agent-check', taskId:'check',
 *           runId:'chk-<digest16>', attemptId:'attempt-1', check:{...} }
 *
 * The adapter RUNS the declared command at the watch's poll cadence (engine
 * schedules read(); read executes). This is bounded probing, never a business
 * action: timeoutMs hard kill (SIGTERM→SIGKILL 2s), byte-capped capture.
 *
 * Verdict mapping (nonzero exit is three-valued; agent declares the mapping):
 * - broken  : spawn error / exit 126|127 / timeout → state 'unknown' + gap
 *             (health degraded) — the CHECK is broken; never a task verdict.
 * - failed  : exitCode ∈ failCodes OR failPattern matches stdout — explicit
 *             declared failure evidence.
 * - succeeded: okPattern declared → match required; otherwise exit 0
 *             (marker philosophy: exit code is explicit evidence).
 * - pending : any other outcome → 'running' (condition not yet met).
 * Ordering: broken → declared-fail → declared-ok → pending.
 *
 * Evidence economy: log_delta only when output digest changes ("no new
 * information" is the Jev signal, not a journal row); status only on state
 * change. Silence therefore means "output and state unchanged for
 * maxSilenceMs" — stuck-pending surfaces via the silence episode.
 *
 * Single-flight per target: overlapping sweeps skip (never double-run).
 */

import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import type {
  Json, Observation, Probe, RunTarget,
  SourceAdapter, SourceCapabilities
} from '../../contracts/interfaces.js';

export interface AgentCheckCursor {
  lastSeq: number;
  lastState: string;
  lastOutputDigest: string;
  /** terminal already reported (succeeded/failed) → stop running the command */
  done: boolean;
}

const MAX_STDOUT_BYTES = 16 * 1024;
const MAX_STDERR_BYTES = 2 * 1024;
const MAX_DELTA_LINES = 40;
const KILL_GRACE_MS = 2000;

interface CheckRun {
  exitCode: number | null;   // null: killed/spawn error
  timedOut: boolean;
  spawnError: string | null;
  stdout: string;
  stderr: string;
  durationMs: number;
}

interface CheckVerdict {
  taskState: 'unknown' | 'running' | 'succeeded' | 'failed';
  exitCode: number | null;
  broken: boolean;
  summary: string;
}

function normalizeCursor(cursor: Json | undefined): AgentCheckCursor {
  const c = cursor as Partial<AgentCheckCursor> | undefined;
  return {
    lastSeq: typeof c?.lastSeq === 'number' ? c.lastSeq : 0,
    lastState: typeof c?.lastState === 'string' ? c.lastState : '',
    lastOutputDigest: typeof c?.lastOutputDigest === 'string' ? c.lastOutputDigest : '',
    done: c?.done === true
  };
}

const digestOf = (s: string): string => createHash('sha256').update(s).digest('hex').slice(0, 16);

export class AgentCheckAdapter implements SourceAdapter {
  readonly adapterId = 'agent-check';
  readonly capabilities: SourceCapabilities = {
    stableAttempt: true,
    terminalEvents: true,
    incrementalCursor: true,
    exclusiveNotificationOwner: true
  };

  private readonly inflight = new Set<string>();

  private runCheck(cmd: string, cwd: string | undefined, timeoutMs: number): Promise<CheckRun> {
    return new Promise(resolve => {
      const startedAt = Date.now();
      let stdout = '';
      let stderr = '';
      let timedOut = false;
      let settled = false;
      const child = spawn(cmd, {
        shell: true,
        cwd: cwd && cwd.trim() !== '' ? cwd : undefined,
        env: process.env,
        windowsHide: true
      });
      const finish = (exitCode: number | null, spawnError: string | null): void => {
        if (settled) return;
        settled = true;
        clearTimeout(killTimer);
        clearTimeout(termTimer);
        resolve({
          exitCode,
          timedOut,
          spawnError,
          stdout: stdout.slice(-MAX_STDOUT_BYTES),
          stderr: stderr.slice(-MAX_STDERR_BYTES),
          durationMs: Date.now() - startedAt
        });
      };
      child.stdout?.on('data', (d: Buffer) => { if (stdout.length < MAX_STDOUT_BYTES * 2) stdout += d.toString('utf8'); });
      child.stderr?.on('data', (d: Buffer) => { if (stderr.length < MAX_STDERR_BYTES * 2) stderr += d.toString('utf8'); });
      child.on('error', e => finish(null, e.message));
      child.on('close', code => finish(code, null));
      // Timeout: SIGTERM, then SIGKILL after 2s; close always arrives after kill (fallback resolve)
      const termTimer = setTimeout(() => {
        timedOut = true;
        try { child.kill('SIGTERM'); } catch { /* already dead */ }
      }, timeoutMs);
      const killTimer = setTimeout(() => {
        try { child.kill('SIGKILL'); } catch { /* already dead */ }
      }, timeoutMs + KILL_GRACE_MS);
    });
  }

  private verdictOf(run: CheckRun, check: NonNullable<RunTarget['check']>): CheckVerdict {
    // 1. broken -- the check itself is unavailable: never sentence the task to death
    if (run.spawnError !== null) {
      return { taskState: 'unknown', exitCode: null, broken: true, summary: `check broken: spawn error — ${run.spawnError}` };
    }
    if (run.timedOut) {
      return { taskState: 'unknown', exitCode: null, broken: true, summary: `check broken: timed out after ${run.durationMs}ms` };
    }
    if (run.exitCode === 126 || run.exitCode === 127) {
      return { taskState: 'unknown', exitCode: run.exitCode, broken: true, summary: `check broken: exit ${run.exitCode} (command not found / not executable)` };
    }
    // 2. declared-fail: explicitly declared failure evidence (the pattern also applies to exit 0)
    if (check.failCodes && run.exitCode !== null && check.failCodes.includes(run.exitCode)) {
      return { taskState: 'failed', exitCode: run.exitCode, broken: false, summary: `check failed: exit ${run.exitCode} (declared failCode)` };
    }
    if (check.failPattern && new RegExp(check.failPattern).test(run.stdout)) {
      return { taskState: 'failed', exitCode: run.exitCode, broken: false, summary: `check failed: failPattern matched (${summarizeOutput(run)})` };
    }
    // 3. declared-ok / exit 0
    if (check.okPattern) {
      if (new RegExp(check.okPattern).test(run.stdout)) {
        return { taskState: 'succeeded', exitCode: run.exitCode, broken: false, summary: `check ok: okPattern matched (${summarizeOutput(run)})` };
      }
    } else if (run.exitCode === 0) {
      return { taskState: 'succeeded', exitCode: 0, broken: false, summary: 'check ok: exit 0' };
    }
    // 4. pending
    return { taskState: 'running', exitCode: run.exitCode, broken: false, summary: `pending: check exit ${run.exitCode ?? 'null'} (${summarizeOutput(run)})` };
  }

  async read(
    target: RunTarget,
    cursor: Json | undefined,
    _signal: AbortSignal
  ): Promise<{ observations: Observation[]; nextCursor: Json; gap: boolean }> {
    const check = target.check;
    const cur = normalizeCursor(cursor);
    const next: AgentCheckCursor = { ...cur };
    if (!check || typeof check.cmd !== 'string' || check.cmd === '') {
      // no command to execute: do not fabricate facts
      return { observations: [], nextCursor: next as unknown as Json, gap: true };
    }
    if (cur.done) {
      return { observations: [], nextCursor: next as unknown as Json, gap: false }; // terminal already reported
    }

    const key = `${target.sourceId}:${target.taskId}:${target.runId}:${target.attemptId}`;
    if (this.inflight.has(key)) {
      return { observations: [], nextCursor: next as unknown as Json, gap: false }; // single-flight: skip this round
    }
    this.inflight.add(key);
    let run: CheckRun;
    try {
      run = await this.runCheck(check.cmd, check.cwd, Math.min(Math.max(check.timeoutMs ?? 10_000, 1_000), 30_000));
    } finally {
      this.inflight.delete(key);
    }

    const verdict = this.verdictOf(run, check);
    const observations: Observation[] = [];
    const now = new Date().toISOString();

    // log_delta only when output changes (no new information is not an evidence line; feeds Jev's repeating signal)
    const outputDigest = digestOf(run.stdout + '\u0000' + run.stderr);
    if (outputDigest !== cur.lastOutputDigest) {
      const lines = [...run.stdout.split('\n'), ...(run.stderr.trim() !== '' ? [run.stderr.trim()] : [])]
        .filter(l => l !== '')
        .slice(-MAX_DELTA_LINES);
      if (lines.length > 0) {
        next.lastSeq += 1;
        observations.push({
          schemaVersion: 1,
          observationId: `obs-check-${randomUUID()}`,
          target,
          sourceSeq: next.lastSeq,
          observedAt: now,
          kind: 'log_delta',
          payload: { lines, exitCode: run.exitCode, durationMs: run.durationMs } as unknown as Json,
          evidenceRefs: []
        });
      }
    }
    next.lastOutputDigest = outputDigest;

    // status only when the state changes
    if (verdict.taskState !== cur.lastState) {
      next.lastSeq += 1;
      next.lastState = verdict.taskState;
      observations.push({
        schemaVersion: 1,
        observationId: `obs-check-${randomUUID()}`,
        target,
        sourceSeq: next.lastSeq,
        observedAt: now,
        kind: 'status',
        payload: {
          state: verdict.taskState,
          stage: 'check',
          exitCode: verdict.exitCode,
          summary: verdict.summary
        } as unknown as Json,
        evidenceRefs: []
      });
    }

    if (verdict.taskState === 'succeeded' || verdict.taskState === 'failed') {
      next.done = true;
    }
    // broken -> source degraded (health degraded); not a terminal state, not a task verdict
    return { observations, nextCursor: next as unknown as Json, gap: verdict.broken };
  }

  async probes(_target: RunTarget, _basis: unknown): Promise<readonly Probe[]> {
    // A check command has no passive artifact to read: the evidence window comes from read()'s log_delta journal;
    // an active probe would run the command again (double cost), and read() already provides new output at the polling cadence.
    return [];
  }

  async execute(_probe: Probe, _signal: AbortSignal): Promise<Observation[]> {
    return [];
  }

  async close(): Promise<void> { /* no resident resources; in-flight is covered by the timeout */ }
}

function summarizeOutput(run: CheckRun): string {
  const last = run.stdout.trim().split('\n').filter(l => l !== '').slice(-1)[0] ?? '';
  return last !== '' ? `last: ${last.slice(0, 120)}` : 'no output';
}

/** runId of the watch-check tool action: stable digest of the command + verdict mapping (for dedup) */
export function checkRunId(cmd: string, verdict: { okPattern?: string; failPattern?: string; failCodes?: number[] }): string {
  return `chk-${digestOf(cmd + '\u0000' + (verdict.okPattern ?? '') + '\u0000' + (verdict.failPattern ?? '') + '\u0000' + (verdict.failCodes ?? []).join(','))}`;
}
