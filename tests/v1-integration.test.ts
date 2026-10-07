import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn, type ChildProcess } from 'node:child_process';
import { startRuntime, type WatcherRuntime } from '../src/runtime.js';
import { makeActor, makeCandidate } from './engine/fixture.js';

/**
 * V1 display-only integration test:
 * real child-process fixture producer (async file writes) -> watcher engine polling -> hard terminal state ->
 * durable episode + result card -> pause isolation -> restart recovery.
 * Does not include the relay managed path (depends on relay 1.2, not yet implemented; see relay/negotiate.ts).
 */

const REPO_ROOT = path.resolve(import.meta.dirname, '..');

function tsxArgs(script: string, args: string[]): string[] {
  return [path.join(REPO_ROOT, 'node_modules', 'tsx', 'dist', 'cli.mjs'), path.join(REPO_ROOT, script), ...args];
}

function runProducer(sourceRoot: string, scenario: string, intervalMs: number, runId: string): ChildProcess {
  return spawn(
    process.execPath,
    tsxArgs('src/cli.ts', ['producer', '--dir', sourceRoot, '--scenario', scenario, '--interval', String(intervalMs), '--run-id', runId]),
    { cwd: REPO_ROOT, stdio: ['ignore', 'pipe', 'pipe'] }
  );
}

interface Insp {
  episodes: Array<{ episodeId: string; kind: string; state: string }>;
  resultCards: Array<Record<string, unknown>>;
  snapshot: Record<string, unknown>;
  lifecycle: string;
  health: string;
}

async function inspectJson(rt: WatcherRuntime, watchId: string): Promise<Insp> {
  return (await rt.service.inspect(watchId, makeActor())) as unknown as Insp;
}

test('V1 integration: real child-process producer -> engine loop -> terminal result card -> pause -> restart', { timeout: 60000 }, async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'pw-v1-int-'));
  const sourceRoot = path.join(tmp, 'sources');
  const rootDir = path.join(tmp, 'state');

  const rt = await startRuntime({
    rootDir,
    sourceRoots: new Map([['executor-local', sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]]),
    pollTickMs: 50
  });
  rt.startLoop();

  const actor = makeActor();
  // Register first (the task has not produced any facts yet)
  const spec = await rt.service.register('v1-req-1', makeCandidate({
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-v1-a', attemptId: 'attempt-1' },
    limits: {
      pollMinMs: 1000, pollMaxMs: 2000, maxSilenceMs: 60000,
      maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10,
      expiresAt: new Date(Date.now() + 3600_000).toISOString()
    }
  }) as never, actor);

  // real async child-process producer
  const child = runProducer(sourceRoot, 'build-failure', 120, 'run-v1-a');
  await new Promise<void>((resolve, reject) => {
    let out = '';
    child.stdout!.on('data', c => {
      out += String(c);
      if (out.includes('"done": true')) resolve();
    });
    child.on('exit', code => {
      if (code === 0 && out.includes('"done": true')) resolve();
      else reject(new Error(`producer exited ${code}: ${out}`));
    });
    setTimeout(() => reject(new Error(`producer timeout: ${out}`)), 30000);
  });

  // the polling loop should discover the terminal state within pollMin
  const deadline = Date.now() + 10_000;
  let insp = await inspectJson(rt, spec.watchId);
  while (Date.now() < deadline && insp.resultCards.length === 0) {
    await new Promise(r => setTimeout(r, 100));
    insp = await inspectJson(rt, spec.watchId);
  }
  assert.equal(insp.snapshot.taskState, 'failed');
  assert.equal(insp.resultCards.length, 1);
  assert.equal(insp.resultCards[0].executorState, 'failed');
  assert.ok(insp.episodes.some(e => e.kind === 'task.failed'));

  // pause: CAS + control idempotency
  const paused = await rt.service.control('v1-req-2', spec.watchId, 1, 'pause', 'integration pause', actor);
  assert.equal((paused as Record<string, unknown>).lifecycle, 'paused');

  // restart recovery: close -> reopen the same root, facts still present
  rt.close();
  const rt2 = await startRuntime({
    rootDir,
    sourceRoots: new Map([['executor-local', sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]]),
    pollTickMs: 50
  });
  const insp2 = await inspectJson(rt2, spec.watchId);
  assert.equal(insp2.lifecycle, 'paused', 'pause does not vanish across restart');
  assert.equal(insp2.resultCards.length, 1, 'result card persisted');
  assert.equal(insp2.snapshot.taskState, 'failed');
  rt2.close();
  fs.rmSync(tmp, { recursive: true, force: true });
});

test('V1 integration: succeeded scenario produces succeeded-evidence card with checks bound', { timeout: 60000 }, async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'pw-v1-ok-'));
  const sourceRoot = path.join(tmp, 'sources');
  const rt = await startRuntime({
    rootDir: path.join(tmp, 'state'),
    sourceRoots: new Map([['executor-local', sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]])
  });
  const child = runProducer(sourceRoot, 'build-success', 60, 'run-v1-b');
  await new Promise<void>((resolve, reject) => {
    let out = '';
    child.stdout!.on('data', c => {
      out += String(c);
      if (out.includes('"done": true')) resolve();
    });
    child.on('exit', code => (code === 0 ? resolve() : reject(new Error(`producer exited ${code}`))));
    setTimeout(() => reject(new Error('producer timeout')), 30000);
  });
  const actor = makeActor();
  const spec = await rt.service.register('v1-req-3', makeCandidate({
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-v1-b', attemptId: 'attempt-1' }
  }) as never, actor);
  await rt.engine.inspectWatch(spec.watchId);
  const insp = await inspectJson(rt, spec.watchId);
  assert.equal(insp.resultCards[0].executorState, 'succeeded');
  assert.deepEqual(insp.resultCards[0].checks, [{ checkId: 'unit-tests', outcome: 'passed', artifactDigest: 'deadbeef01' }]);
  rt.close();
  fs.rmSync(tmp, { recursive: true, force: true });
});
