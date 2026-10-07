import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { WatcherError } from '../../src/util/result.js';
import type { RegisterCandidate } from '../../src/contracts/interfaces.js';

/**
 * Shared test fixture: real store + adapter + engine + service (no mock storage).
 * service is verified together with inspection in engine.test.ts; this file focuses on API semantics.
 */

export interface Fixture {
  rootDir: string;
  sourceRoot: string;
  cleanup: () => void;
}

export function makeFixture(prefix = 'pw-fx-'): Fixture {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  return {
    rootDir: path.join(tmp, 'state'),
    sourceRoot: path.join(tmp, 'sources'),
    cleanup: () => fs.rmSync(tmp, { recursive: true, force: true })
  };
}

export function makeActor(sessionId = 'sess-1') {
  return {
    actorId: `actor:${sessionId}`,
    owner: { sessionId, originAnchor: null, bindingEpoch: 1, profileId: 'profile-1' },
    profileRevision: 1,
    permitted: new Set(['watcher.register', 'watcher.list', 'watcher.inspect', 'watcher.check', 'watcher.control'])
  };
}

export function makeCandidate(overrides: Record<string, unknown> = {}): RegisterCandidate {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return {
    schemaVersion: 1,
    mode: 'embedded',
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' },
    mission: {
      objective: 'Run the existing async build task, observe its status, and report on failure or when the result is deliverable.',
      scope: 'Observe run-1 only; do not modify, retry or terminate the build.',
      checkpointId: 'compile-and-test',
      requiredArtifacts: ['artifact-app'],
      requiresChecks: true,
      businessAcceptance: 'host'
    },
    policy: {
      transport: 'local-display',
      semanticMode: 'off',
      notificationOwner: 'watcher',
      requestKinds: ['task.failed', 'task.terminal'],
      maxRequestsPerEpisode: 1,
      episodeCooldownMs: 60000,
      attentionTtlMs: 120000
    },
    limits: {
      pollMinMs: 1000,
      pollMaxMs: 5000,
      maxSilenceMs: 60000,
      maxProbesPerEpisode: 2,
      maxJudgeRequestsPerDay: 10,
      expiresAt: new Date(Date.now() + 3600_000).toISOString()
    },
    ...overrides
  } as RegisterCandidate;
}

export { WatcherError };
