/**
 * task-status-v1 fixture producer (design 4.1).
 * Real async process: publishes the snapshot via a same-directory temp file + atomic replace; the journal is JSONL with stable sequence numbers.
 * The watcher only reads these files and is not responsible for task execution.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { randomUUID } from 'node:crypto';

export type TaskState = 'unknown' | 'queued' | 'running' | 'blocked' | 'succeeded' | 'failed' | 'cancelled';

export interface TaskStatusSnapshot {
  schemaVersion: 1;
  target: { kind: 'run'; sourceId: string; taskId: string; runId: string; attemptId: string };
  state: TaskState;
  stage: string | null;
  exitCode: number | null;
  summary: string | null;
  checks: Array<{ checkId: string; outcome: 'passed' | 'failed' | 'skipped' | 'unknown'; artifactDigest: string | null }>;
  artifacts: Array<{ artifactId: string; sha256: string; sourceRevision: number }>;
  snapshotSeq: number;
  updatedAt: string;
}

export interface TaskStatusJournalEvent {
  schemaVersion: 1;
  sourceSeq: number;
  at: string;
  kind: 'status' | 'heartbeat' | 'log_delta' | 'validation' | 'artifact_manifest' | 'decision_request';
  payload: Record<string, unknown>;
}

export function runDir(rootDir: string, target: { sourceId: string; taskId: string; runId: string; attemptId: string }): string {
  return path.join(rootDir, target.sourceId, target.taskId, target.runId, target.attemptId);
}

export class TaskStatusProducer {
  private seq = 0;
  private readonly dir: string;
  private readonly target: { kind: 'run'; sourceId: string; taskId: string; runId: string; attemptId: string };
  private snapshot: TaskStatusSnapshot | null = null;

  constructor(
    rootDir: string,
    target: { sourceId: string; taskId: string; runId: string; attemptId: string },
    private readonly journalId = `journal-${randomUUID().slice(0, 8)}`
  ) {
    this.dir = runDir(rootDir, target);
    this.target = { kind: 'run', ...target };
    fs.mkdirSync(this.dir, { recursive: true });
  }

  private nextSeq(): number {
    this.seq += 1;
    return this.seq;
  }

  private nowIso(): string {
    return new Date().toISOString();
  }

  async appendEvent(kind: TaskStatusJournalEvent['kind'], payload: Record<string, unknown>): Promise<number> {
    const sourceSeq = this.nextSeq();
    const event: TaskStatusJournalEvent = {
      schemaVersion: 1,
      sourceSeq,
      at: this.nowIso(),
      kind,
      payload
    };
    await fs.promises.appendFile(path.join(this.dir, 'journal.jsonl'), JSON.stringify(event) + '\n', 'utf8');
    return sourceSeq;
  }

  /** Atomic snapshot replace: tmp file + rename in the same directory. */
  async publishSnapshot(patch: Partial<Omit<TaskStatusSnapshot, 'schemaVersion' | 'target' | 'snapshotSeq' | 'updatedAt'>>): Promise<number> {
    const snapshotSeq = this.nextSeq();
    const prev = this.snapshot;
    this.snapshot = {
      schemaVersion: 1,
      target: this.target,
      state: patch.state ?? prev?.state ?? 'unknown',
      stage: patch.stage !== undefined ? patch.stage : (prev?.stage ?? null),
      exitCode: patch.exitCode !== undefined ? patch.exitCode : (prev?.exitCode ?? null),
      summary: patch.summary !== undefined ? patch.summary : (prev?.summary ?? null),
      checks: patch.checks !== undefined ? patch.checks : (prev?.checks ?? []),
      artifacts: patch.artifacts !== undefined ? patch.artifacts : (prev?.artifacts ?? []),
      snapshotSeq,
      updatedAt: this.nowIso()
    };
    const tmp = path.join(this.dir, `.snapshot.${snapshotSeq}.tmp`);
    await fs.promises.writeFile(tmp, JSON.stringify(this.snapshot, null, 2), 'utf8');
    await fs.promises.rename(tmp, path.join(this.dir, 'snapshot.json'));
    return snapshotSeq;
  }

  get lastSnapshot(): TaskStatusSnapshot | null {
    return this.snapshot;
  }
}

export interface ProducerStep {
  waitMs: number;
  event?: { kind: TaskStatusJournalEvent['kind']; payload: Record<string, unknown> };
  snapshot?: Partial<Omit<TaskStatusSnapshot, 'schemaVersion' | 'target' | 'snapshotSeq' | 'updatedAt'>>;
}

/** Real async fixture scenarios. */
export async function runScenario(
  rootDir: string,
  target: { sourceId: string; taskId: string; runId: string; attemptId: string },
  steps: ProducerStep[]
): Promise<TaskStatusProducer> {
  const producer = new TaskStatusProducer(rootDir, target);
  for (const step of steps) {
    if (step.waitMs > 0) {
      await new Promise(resolve => setTimeout(resolve, step.waitMs));
    }
    if (step.event) {
      await producer.appendEvent(step.event.kind, step.event.payload);
    }
    if (step.snapshot) {
      await producer.publishSnapshot(step.snapshot);
    }
  }
  return producer;
}

/** Standard demo scenarios: build-success / build-failure. */
export function scenarioSteps(kind: 'build-success' | 'build-failure', intervalMs = 200): ProducerStep[] {
  if (kind === 'build-success') {
    return [
      { waitMs: intervalMs, snapshot: { state: 'running', stage: 'compile', summary: 'compiling' } },
      { waitMs: intervalMs, event: { kind: 'log_delta', payload: { lines: ['cc -c main.c', 'cc -c util.c'] } } },
      { waitMs: intervalMs, snapshot: { state: 'running', stage: 'test', summary: 'compile finished, running tests' } },
      { waitMs: intervalMs, event: { kind: 'validation', payload: { checkId: 'unit-tests', outcome: 'passed', artifactDigest: 'deadbeef01' } } },
      {
        waitMs: intervalMs,
        snapshot: {
          state: 'succeeded',
          stage: 'done',
          exitCode: 0,
          summary: 'build and tests completed',
          checks: [{ checkId: 'unit-tests', outcome: 'passed', artifactDigest: 'deadbeef01' }],
          artifacts: [{ artifactId: 'artifact-app', sha256: 'a'.repeat(64), sourceRevision: 1 }]
        }
      }
    ];
  }
  return [
    { waitMs: intervalMs, snapshot: { state: 'running', stage: 'compile', summary: 'compiling' } },
    { waitMs: intervalMs, event: { kind: 'log_delta', payload: { lines: ['cc -c main.c'] } } },
    {
      waitMs: intervalMs,
      event: {
        kind: 'decision_request',
        payload: { text: 'Compiler reports that the requested API does not exist. Need the host to choose which API contract to target.' }
      }
    },
    {
      waitMs: intervalMs,
      snapshot: {
        state: 'failed',
        stage: 'compile',
        exitCode: 2,
        summary: 'executor reports build failure; not yet attributed.',
        checks: [{ checkId: 'unit-tests', outcome: 'unknown', artifactDigest: null }],
        artifacts: []
      }
    }
  ];
}
