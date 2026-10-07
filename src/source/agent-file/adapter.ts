/**
 * agent-file SourceAdapter — agent-declared file evidence (universal monitor
 * mode).
 *
 * Target: { kind:'run', sourceId:'agent-file', taskId:'file',
 *           runId:'file-<digest16>', attemptId:'attempt-1',
 *           file:{ path, okPattern?, failPattern? } }
 *
 * Replaces the removed unified-exec adapter: the agent pins the exact file
 * path (it holds log_path in the exec tool result), so no foreign-library
 * naming heuristics, no runId namespace scans, no cross-process collisions.
 * Exit detection is declared patterns over content — e.g. watching an exec
 * session log with okPattern='__EXEC_EXIT__:0', failPattern='__EXEC_EXIT__:-?[1-9]'
 * (the marker itself is a pi-watcher convention injected by the extension).
 *
 * Facts (honest, conservative):
 * - file missing → 'unknown' + gap (degraded) — NOT terminal; may appear later
 * - rotation/truncation (size < cursor offset) → cursor reset + gap
 * - growth → bounded incremental log_delta (progress evidence; feeds silence)
 * - failPattern match on tail window → failed; okPattern match → succeeded
 *   (fail ordered first: explicit failure evidence outranks ok)
 * - no pattern hit → 'running' (liveness via lsof fd holders is informational
 *   only — a generic file's "no holder" is NOT exit evidence, writers may
 *   write per-open; the removed exec adapter's exited-unknown fabrication
 *   does not generalize and is gone)
 * - terminal (pattern) → cursor.done; source stops reading (engine anneals
 *   and auto-closes)
 */

import * as fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import type {
  Json, Observation, Probe, RunTarget,
  SourceAdapter, SourceCapabilities
} from '../../contracts/interfaces.js';

export interface AgentFileCursor {
  offset: number;
  lastSeq: number;
  lastState: string;
  /** terminal already reported → stop reading */
  done: boolean;
}

const TAIL_BYTES = 4096;
const MAX_DELTA_BYTES = 16 * 1024;
const MAX_DELTA_LINES = 200;

interface FileVerdict {
  taskState: 'unknown' | 'running' | 'succeeded' | 'failed';
  summary: string;
  gap: boolean;
}

function normalizeCursor(cursor: Json | undefined): AgentFileCursor {
  const c = cursor as Partial<AgentFileCursor> | undefined;
  return {
    offset: typeof c?.offset === 'number' ? c.offset : 0,
    lastSeq: typeof c?.lastSeq === 'number' ? c.lastSeq : 0,
    lastState: typeof c?.lastState === 'string' ? c.lastState : '',
    done: c?.done === true
  };
}

const digestOf = (s: string): string => createHash('sha256').update(s).digest('hex').slice(0, 16);

function readTail(logPath: string): string {
  try {
    const size = fs.statSync(logPath).size;
    const len = Math.min(size, TAIL_BYTES);
    const fh = fs.openSync(logPath, 'r');
    try {
      const buf = Buffer.alloc(len);
      const n = fs.readSync(fh, buf, 0, len, size - len);
      return buf.subarray(0, n).toString('utf8');
    } finally {
      fs.closeSync(fh);
    }
  } catch {
    return '';
  }
}

export class AgentFileAdapter implements SourceAdapter {
  readonly adapterId = 'agent-file';
  readonly capabilities: SourceCapabilities = {
    stableAttempt: true,
    terminalEvents: true,
    incrementalCursor: true,
    exclusiveNotificationOwner: true
  };

  /** fd holders (lsof): only running summary info; unavailable -> null (no inference) */
  private holders(filePath: string): string | null {
    const r = spawnSync('lsof', ['-t', '--', filePath], { timeout: 2000, encoding: 'utf8' });
    if (r.error) return null;
    if (r.status !== 0 && r.status !== 1) return null;
    return (r.stdout ?? '').trim();
  }

  private verdictOf(filePath: string, file: NonNullable<RunTarget['file']>, grew: boolean): FileVerdict {
    const tail = readTail(filePath);
    const lastLine = (tail.split('\n').filter(l => l !== '').slice(-1)[0] ?? '').slice(0, 120);
    if (file.failPattern && new RegExp(file.failPattern).test(tail)) {
      return { taskState: 'failed', summary: `failPattern matched in tail (${lastLine})`, gap: false };
    }
    if (file.okPattern && new RegExp(file.okPattern).test(tail)) {
      return { taskState: 'succeeded', summary: `okPattern matched in tail (${lastLine})`, gap: false };
    }
    const holder = this.holders(filePath);
    const liveness = holder === null ? 'liveness unknown (lsof unavailable)' : holder !== '' ? 'writer holds the file' : 'no fd holder (writer may write per-open)';
    return { taskState: 'running', summary: grew ? `file growing; ${liveness}` : `no new content; ${liveness}`, gap: false };
  }

  async read(
    target: RunTarget,
    cursor: Json | undefined,
    _signal: AbortSignal
  ): Promise<{ observations: Observation[]; nextCursor: Json; gap: boolean }> {
    const file = target.file;
    const cur = normalizeCursor(cursor);
    const next: AgentFileCursor = { ...cur };
    if (!file || typeof file.path !== 'string' || file.path === '') {
      return { observations: [], nextCursor: next as unknown as Json, gap: true };
    }
    if (cur.done) {
      return { observations: [], nextCursor: next as unknown as Json, gap: false };
    }

    let size: number;
    try {
      size = fs.statSync(file.path).size;
    } catch {
      // File missing: degraded, non-terminal (may not be created yet)
      const observations: Observation[] = [];
      if (cur.lastState !== 'unknown') {
        next.lastState = 'unknown';
        next.lastSeq += 1;
        observations.push(this.statusObservation(target, next.lastSeq, 'unknown', `file not found: ${file.path}`));
      }
      return { observations, nextCursor: next as unknown as Json, gap: true };
    }

    // Rotation/truncation: cursor reset, gap notice
    let gap = false;
    if (size < cur.offset) {
      next.offset = 0;
      gap = true;
    }

    const observations: Observation[] = [];
    let grew = false;
    if (size > next.offset) {
      grew = true;
      const len = Math.min(size - next.offset, MAX_DELTA_BYTES);
      let text = '';
      try {
        const fh = fs.openSync(file.path, 'r');
        try {
          const buf = Buffer.alloc(len);
          const n = fs.readSync(fh, buf, 0, len, next.offset);
          text = buf.subarray(0, n).toString('utf8');
        } finally {
          fs.closeSync(fh);
        }
      } catch {
        return { observations, nextCursor: next as unknown as Json, gap: true }; // raced away; retry next sweep
      }
      let lines = text.split('\n');
      lines.pop(); // trailing partial line (writer mid-write) — next sweep re-reads from offset
      if (lines.length > MAX_DELTA_LINES) {
        lines = [`[... ${lines.length - MAX_DELTA_LINES} earlier lines truncated ...]`, ...lines.slice(-MAX_DELTA_LINES)];
      }
      if (lines.length > 0) {
        next.lastSeq += 1;
        observations.push({
          schemaVersion: 1,
          observationId: `obs-file-${randomUUID()}`,
          target,
          sourceSeq: next.lastSeq,
          observedAt: new Date().toISOString(),
          kind: 'log_delta',
          payload: { lines } as unknown as Json,
          evidenceRefs: []
        });
      }
      next.offset = size;
    }

    const verdict = this.verdictOf(file.path, file, grew);
    if (verdict.taskState !== cur.lastState) {
      next.lastSeq += 1;
      next.lastState = verdict.taskState;
      observations.push(this.statusObservation(target, next.lastSeq, verdict.taskState, verdict.summary));
    }
    if (verdict.taskState === 'succeeded' || verdict.taskState === 'failed') {
      next.done = true;
    }
    return { observations, nextCursor: next as unknown as Json, gap: gap || verdict.gap };
  }

  private statusObservation(target: RunTarget, seq: number, state: string, summary: string): Observation {
    return {
      schemaVersion: 1,
      observationId: `obs-file-${randomUUID()}`,
      target,
      sourceSeq: seq,
      observedAt: new Date().toISOString(),
      kind: 'status',
      payload: { state, stage: 'file', summary } as unknown as Json,
      evidenceRefs: []
    };
  }

  async probes(target: RunTarget, _basis: unknown): Promise<readonly Probe[]> {
    return [{
      probeId: `probe-file-tail-${randomUUID()}`,
      revision: 1,
      target,
      kind: 'read-log-delta',
      scopeId: 'file-tail',
      expiresAt: new Date(Date.now() + 30_000).toISOString(),
      timeoutMs: 2000,
      maxBytes: TAIL_BYTES
    }];
  }

  async execute(probe: Probe, _signal: AbortSignal): Promise<Observation[]> {
    const path = probe.target.file?.path;
    const tail = path ? readTail(path) : '';
    return [{
      schemaVersion: 1,
      observationId: `obs-file-${randomUUID()}`,
      target: probe.target,
      sourceSeq: 0,
      observedAt: new Date().toISOString(),
      kind: 'log_delta',
      payload: { probe: 'file-tail', lines: tail.split('\n').slice(-40) } as unknown as Json,
      evidenceRefs: []
    }];
  }

  async close(): Promise<void> { /* nothing held */ }
}

/** runId of the watch-file tool action: stable digest of path + patterns (for dedup) */
export function fileRunId(path: string, patterns: { okPattern?: string; failPattern?: string }): string {
  return `file-${digestOf(path + '\u0000' + (patterns.okPattern ?? '') + '\u0000' + (patterns.failPattern ?? ''))}`;
}
