import test from 'node:test';
import assert from 'node:assert/strict';
import { startRuntime } from '../../src/runtime.js';
import { runToolAction } from '../../src/engine/tool-actions.js';
import { makeActor, makeCandidate, makeFixture } from './fixture.js';

/**
 * tool projection layer (design 11.4): list returns only active watches + a terminalCount summary by default.
 * Background: a single session's exec->watch loop accumulated 35 watches (33 already terminal); echoing them all
 * burned ~1.5KB of model context each time -- terminal rows are history, not decision input.
 */

interface ListShape {
  watches: Array<{ watchId?: string; runId?: string; lifecycle?: string; taskState?: string }>;
  terminalCount?: number;
  relay?: string;
}

test('tool list: closed watches hidden by default, terminalCount summarizes them; includeClosed returns full history', async () => {
  const fx = makeFixture('pw-ta-list-');
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();

  const s1 = await rt.service.register('r-list-1', makeCandidate() as never, actor);
  const s2 = await rt.service.register('r-list-2', makeCandidate({
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-2', attemptId: 'attempt-1' }
  }) as never, actor);
  await rt.service.control('r-list-3', s1.watchId, 1, 'close', 'test close', actor);

  const out = (await runToolAction(rt, { action: 'list' }, actor)) as unknown as ListShape;
  assert.equal(out.watches.length, 1, 'closed watch must not be echoed to the model by default');
  assert.equal(out.watches[0]?.watchId, s2.watchId, 'only the active watch remains visible');
  assert.equal(out.terminalCount, 1, 'terminal history summarized as a count');
  assert.equal(typeof out.relay, 'string', 'relay negotiation info preserved for extension wiring');

  const full = (await runToolAction(rt, { action: 'list', includeClosed: true }, actor)) as unknown as ListShape;
  assert.equal(full.watches.length, 2, 'includeClosed=true returns full history');
  assert.equal(full.terminalCount, 1);

  rt.close();
  fx.cleanup();
});

test('tool watch-file: path dedup stays correct with terminal watches present', async () => {
  const fx = makeFixture('pw-ta-dedup-');
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();

  const first = (await runToolAction(rt, { action: 'watch-file', requestId: 'w-dedup-1', path: '/tmp/dedup-42.log' }, actor)) as {
    watchId: string; runId: string;
  };
  // Noise: the same owner accumulated two closed terminal watches (which once filled the dedup limit-50 window)
  for (const runId of ['run-a', 'run-b']) {
    const spec = await rt.service.register(`w-noise-${runId}`, makeCandidate({
      target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId, attemptId: 'attempt-1' }
    }) as never, actor);
    await rt.service.control(`w-noise-ctl-${runId}`, spec.watchId, 1, 'close', 'noise close', actor);
  }

  const second = (await runToolAction(rt, { action: 'watch-file', requestId: 'w-dedup-2', path: '/tmp/dedup-42.log' }, actor)) as {
    watchId: string; note?: string;
  };
  assert.equal(second.watchId, first.watchId, 'repeat watch on same path must dedup to the existing watch');
  assert.match(second.note ?? '', /Already watching/, 'dedup note must say already watching');

  const listed = (await rt.service.list(undefined, 50, actor, ['active', 'paused'])) as unknown as {
    items: Array<{ target?: { runId?: string } }>;
  };
  assert.equal(
    listed.items.filter(it => it.target?.runId === first.runId).length,
    1,
    'exactly one active watch for the declared path'
  );

  rt.close();
  fx.cleanup();
});

test('tool inspect: compact projection removes duplicate tailLines, outbox plumbing, and bounds latestOutput', async () => {
  const fx = makeFixture('pw-ta-inspect-');
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();

  // Simulate long log lines and an over-long objective
  const longLine = 'A'.repeat(500);
  const spec = await rt.service.register('r-insp-1', makeCandidate({
    mission: {
      objective: 'B'.repeat(300),
      scope: 'test',
      checkpointId: 'chk',
      requiredArtifacts: [],
      requiresChecks: false,
      businessAcceptance: 'not_required'
    }
  }) as never, actor);

  // Write tailLines containing long and multi-line entries into the snapshot
  rt.store.transaction(tx => {
    const row = tx.getWatchRow(spec.watchId)!;
    const tailLines = [
      'line 1',
      'line 2',
      longLine,
      ...Array.from({ length: 15 }, (_, i) => `tail entry ${i + 1}`)
    ];
    tx.updateWatch(spec.watchId, { snapshot: { ...row.snapshot, tailLines, taskState: 'running' } }, Date.now());
  });

  const insp = (await runToolAction(rt, { action: 'inspect', watchId: spec.watchId }, actor)) as Record<string, unknown>;

  // 1. no duplicate tailLines field
  assert.equal(insp.tailLines, undefined, 'tailLines must not be present (no duplicate of latestOutput)');
  // 2. no internal outbox / TTL bookkeeping
  assert.equal(insp.recentEvents, undefined, 'recentEvents must be stripped');
  assert.equal(insp.expiresAt, undefined, 'expiresAt must be stripped');
  assert.equal(insp.deadlineAt, undefined, 'deadlineAt must be stripped');
  // 3. objective hard-truncated to <= 100 characters
  assert.ok(typeof insp.objective === 'string');
  assert.ok((insp.objective as string).length <= 100, 'objective must be capped at 100 chars');
  assert.ok((insp.objective as string).endsWith('...'));
  // 4. latestOutput exists with total volume <= 1000 characters, no single line over 300 characters
  assert.ok(typeof insp.latestOutput === 'string');
  const outStr = insp.latestOutput as string;
  assert.ok(outStr.length <= 1000, `latestOutput must be <= 1000 chars, got ${outStr.length}`);
  for (const line of outStr.split('\n')) {
    assert.ok(line.length <= 300, `each line in latestOutput must be <= 300 chars, got ${line.length}`);
  }
  // 5. no redundant empty episodes array when there are no open episodes
  assert.equal(insp.episodes, undefined, 'episodes omitted when no open episodes');

  rt.close();
  fx.cleanup();
});
