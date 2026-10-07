/**
 * task-status-v1 read-only SourceAdapter (design 4.1/4.2).
 * - snapshot is published via atomic replace; journal is JSONL with stable sequence numbers
 * - cursor stores file identity + byte offset + lastSeq + snapshotSeq
 * - only complete JSON lines are accepted; a partial line waits for the next round
 * - truncation/rotation changes the stream generation -> gap=true; the old cursor is not applied directly
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { createHash } from 'node:crypto';
import type {
  Json, Basis, Observation, Probe, RunTarget,
  SourceAdapter, SourceCapabilities, EvidenceRef
} from '../../contracts/interfaces.js';
import type { TaskStatusSnapshot, TaskStatusJournalEvent } from './producer.js';

export interface TaskStatusV1Cursor {
  fileId: string | null;
  offset: number;
  lastSeq: number;
  snapshotSeq: number;
}

export interface TaskStatusV1AdapterOptions {
  adapterId?: string;
  defaultTimeoutMs?: number;
  defaultMaxBytes?: number;
}

function fileIdentity(absPath: string): string | null {
  try {
    const st = fs.statSync(absPath);
    return `${st.dev}:${st.ino}`;
  } catch {
    return null;
  }
}

export class TaskStatusV1Adapter implements SourceAdapter {
  readonly adapterId: string;
  readonly capabilities: SourceCapabilities = {
    stableAttempt: true,
    terminalEvents: true,
    incrementalCursor: true,
    exclusiveNotificationOwner: true
  };

  private readonly timeoutMs: number;
  private readonly maxBytes: number;

  constructor(
    private readonly rootDir: string,
    options: TaskStatusV1AdapterOptions = {}
  ) {
    this.adapterId = options.adapterId ?? 'task-status-v1';
    this.timeoutMs = options.defaultTimeoutMs ?? 5000;
    this.maxBytes = options.defaultMaxBytes ?? 65536;
  }

  private targetDir(target: RunTarget): string {
    return path.join(this.rootDir, target.sourceId, target.taskId, target.runId, target.attemptId);
  }

  async read(
    target: RunTarget,
    cursor: Json | undefined,
    signal: AbortSignal
  ): Promise<{ observations: Observation[]; nextCursor: Json; gap: boolean }> {
    if (signal.aborted) throw signal.reason ?? new Error('aborted');
    const cur = normalizeCursor(cursor);
    const dir = this.targetDir(target);
    const journalPath = path.join(dir, 'journal.jsonl');
    const snapshotPath = path.join(dir, 'snapshot.json');

    let gap = false;
    let offset = cur.offset;
    let lastSeq = cur.lastSeq;
    const nextCursor: TaskStatusV1Cursor = { fileId: cur.fileId, offset, lastSeq, snapshotSeq: cur.snapshotSeq };
    const observations: Observation[] = [];

    // --- journal (JSONL with stable sequence numbers) ---
    const journalId = fileIdentity(journalPath);
    if (journalId !== null) {
      if (cur.fileId !== null && cur.fileId !== journalId) {
        // stream generation change (rotation/replacement): gap, re-read from the start, dedup by store
        gap = true;
        offset = 0;
      }
      const size = fs.statSync(journalPath).size;
      if (offset > size) {
        // in-place truncation
        gap = true;
        offset = 0;
      }
      if (size > offset) {
        const fh = fs.openSync(journalPath, 'r');
        try {
          const readLen = Math.min(size - offset, this.maxBytes);
          const buf = Buffer.alloc(readLen);
          const bytesRead = fs.readSync(fh, buf, 0, readLen, offset);
          const text = buf.subarray(0, bytesRead).toString('utf8');
          const lines = text.split('\n');
          // last element is '' (complete line) or a partial line: a partial line does not advance offset and waits for the next round
          lines.pop();
          let advanced = 0;
          const events: TaskStatusJournalEvent[] = [];
          for (const line of lines) {
            const trimmed = line.trim();
            const lineLen = Buffer.byteLength(line, 'utf8') + 1;
            if (trimmed === '') {
              advanced += lineLen;
              continue;
            }
            try {
              events.push(JSON.parse(trimmed) as TaskStatusJournalEvent);
            } catch {
              // complete line but invalid JSON: audit gap, does not advance state conclusions
              gap = true;
            }
            advanced += lineLen;
          }
          offset += advanced;
          for (const ev of events) {
            if (typeof ev.sourceSeq !== 'number' || ev.sourceSeq <= lastSeq) continue;
            lastSeq = ev.sourceSeq;
            observations.push(journalObservation(target, ev));
          }
        } finally {
          fs.closeSync(fh);
        }
      }
      nextCursor.offset = offset;
      nextCursor.lastSeq = lastSeq;
      nextCursor.fileId = journalId;
    } else if (cur.fileId !== null) {
      // journal disappeared -> source gap
      gap = true;
      nextCursor.fileId = null;
    }

    // --- snapshot (atomic replace; a parse failure does not overwrite the last valid state) ---
    if (fs.existsSync(snapshotPath)) {
      let snapshot: TaskStatusSnapshot | null = null;
      try {
        snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8')) as TaskStatusSnapshot;
      } catch {
        snapshot = null;
      }
      if (snapshot && typeof snapshot.snapshotSeq === 'number' && snapshot.snapshotSeq > cur.snapshotSeq) {
        nextCursor.snapshotSeq = snapshot.snapshotSeq;
        observations.push(snapshotObservation(target, snapshot));
      }
    }

    observations.sort((a, b) => a.sourceSeq - b.sourceSeq);
    return { observations, nextCursor: nextCursor as unknown as Json, gap };
  }

  async probes(target: RunTarget, _basis: Basis): Promise<readonly Probe[]> {
    const dir = this.targetDir(target);
    const expiresAt = new Date(Date.now() + 60_000).toISOString();
    const base = { target, scopeId: dir, expiresAt, timeoutMs: this.timeoutMs };
    return [
      { probeId: 'task-status-v1:read-status', revision: 1, kind: 'read-status', maxBytes: 64 * 1024, ...base },
      { probeId: 'task-status-v1:read-log-delta', revision: 1, kind: 'read-log-delta', maxBytes: 128 * 1024, ...base },
      { probeId: 'task-status-v1:read-artifact-manifest', revision: 1, kind: 'read-artifact-manifest', maxBytes: 64 * 1024, ...base },
      { probeId: 'task-status-v1:read-validation-result', revision: 1, kind: 'read-validation-result', maxBytes: 64 * 1024, ...base }
    ];
  }

  async execute(probe: Probe, signal: AbortSignal): Promise<Observation[]> {
    if (signal.aborted) throw signal.reason ?? new Error('aborted');
    const dir = probe.scopeId;
    const nowIso = new Date().toISOString();
    const mk = (kind: Observation['kind'], payload: Json): Observation => ({
      schemaVersion: 1,
      observationId: `obs-probe-${probe.probeId}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`,
      target: probe.target as RunTarget,
      sourceSeq: 0,
      observedAt: nowIso,
      kind,
      payload,
      evidenceRefs: []
    });
    const readSnapshot = (): TaskStatusSnapshot | null => {
      const p = path.join(dir, 'snapshot.json');
      if (!fs.existsSync(p)) return null;
      try {
        return JSON.parse(fs.readFileSync(p, 'utf8')) as TaskStatusSnapshot;
      } catch {
        return null;
      }
    };
    switch (probe.kind) {
      case 'read-status': {
        const snap = readSnapshot();
        return [mk('status', snap ? (snap as unknown as Json) : { state: 'unknown', note: 'no snapshot' })];
      }
      case 'read-log-delta': {
        const p = path.join(dir, 'journal.jsonl');
        if (!fs.existsSync(p)) return [mk('log_delta', { lines: [], note: 'no journal' })];
        const lines = fs.readFileSync(p, 'utf8').split('\n').filter(l => l.trim() !== '').slice(-32);
        return [mk('log_delta', { lines })];
      }
      case 'read-artifact-manifest': {
        const snap = readSnapshot();
        const payload: Record<string, unknown> = { artifacts: snap?.artifacts ?? [] };
        if (!snap) payload.note = 'no snapshot';
        return [mk('artifact_manifest', payload as unknown as Json)];
      }
      case 'read-validation-result': {
        const snap = readSnapshot();
        const payload: Record<string, unknown> = { checks: snap?.checks ?? [] };
        if (!snap) payload.note = 'no snapshot';
        return [mk('validation', payload as unknown as Json)];
      }
      default:
        return [];
    }
  }

  async close(): Promise<void> {
    /* nothing to close */
  }
}

function normalizeCursor(cursor: Json | undefined): TaskStatusV1Cursor {
  if (!cursor || typeof cursor !== 'object') return { fileId: null, offset: 0, lastSeq: 0, snapshotSeq: 0 };
  const c = cursor as Record<string, unknown>;
  return {
    fileId: typeof c.fileId === 'string' ? c.fileId : null,
    offset: typeof c.offset === 'number' ? c.offset : 0,
    lastSeq: typeof c.lastSeq === 'number' ? c.lastSeq : 0,
    snapshotSeq: typeof c.snapshotSeq === 'number' ? c.snapshotSeq : 0
  };
}

function evidenceRefsFor(target: RunTarget, ev: { sourceSeq: number; at: string; kind: string; payload: unknown }): EvidenceRef[] {
  const text = JSON.stringify({ target, seq: ev.sourceSeq, at: ev.at, kind: ev.kind, payload: ev.payload });
  const sha = createHash('sha256').update(text).digest('hex');
  return [
    {
      evidenceId: `ev-${sha.slice(0, 16)}`,
      sourceId: target.sourceId,
      capturedAt: ev.at,
      sha256: sha,
      bytes: Buffer.byteLength(text, 'utf8'),
      sampled: false
    }
  ];
}

function journalObservation(target: RunTarget, ev: TaskStatusJournalEvent): Observation {
  return {
    schemaVersion: 1,
    observationId: `obs-${target.sourceId}-${target.runId}-${ev.sourceSeq}`,
    target,
    sourceSeq: ev.sourceSeq,
    observedAt: ev.at,
    kind: ev.kind,
    payload: ev.payload as Json,
    evidenceRefs: evidenceRefsFor(target, ev)
  };
}

function snapshotObservation(target: RunTarget, snap: TaskStatusSnapshot): Observation {
  return {
    schemaVersion: 1,
    observationId: `obs-${target.sourceId}-${target.runId}-snap-${snap.snapshotSeq}`,
    target,
    sourceSeq: snap.snapshotSeq,
    observedAt: snap.updatedAt,
    kind: 'status',
    payload: {
      state: snap.state,
      stage: snap.stage,
      exitCode: snap.exitCode,
      summary: snap.summary,
      checks: snap.checks,
      artifacts: snap.artifacts
    } as unknown as Json,
    evidenceRefs: evidenceRefsFor(target, {
      sourceSeq: snap.snapshotSeq,
      at: snap.updatedAt,
      kind: 'status',
      payload: { state: snap.state }
    })
  };
}
