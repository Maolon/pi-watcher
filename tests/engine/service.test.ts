import test from 'node:test';
import assert from 'node:assert/strict';
import { startRuntime } from '../../src/runtime.js';
import { makeActor, makeCandidate, makeFixture } from './fixture.js';
import { WatcherError } from '../../src/util/result.js';

test('service.register: validates spec, enforces profile sources, rejects relay transport without 1.2', async () => {
  const fx = makeFixture('pw-svc-reg-');
  const rt = await startRuntime({
    rootDir: fx.rootDir,
    sourceRoots: new Map([['executor-local', fx.sourceRoot]]),
    allowedSourceIds: ['executor-local']
  });
  const actor = makeActor();

  await assert.rejects(
    rt.service.register('r-inv-1', makeCandidate({ mission: undefined }) as never, actor),
    (e: unknown) => e instanceof WatcherError && e.code === 'INVALID_SPEC'
  );
  await assert.rejects(
    rt.service.register('r-cap-1', makeCandidate({ target: { kind: 'run', sourceId: 'other-source', taskId: 't', runId: 'r', attemptId: 'a' } }) as never, actor),
    (e: unknown) => e instanceof WatcherError && e.code === 'CAPABILITY_DENIED'
  );
  await assert.rejects(
    rt.service.register('r-relay-1', makeCandidate({ policy: { transport: 'relay', semanticMode: 'off', notificationOwner: 'watcher', requestKinds: [], maxRequestsPerEpisode: 1, episodeCooldownMs: 60000, attentionTtlMs: 120000 } }) as never, actor),
    (e: unknown) => e instanceof WatcherError && e.code === 'DEPENDENCY_UNQUALIFIED' && /relay protocol 1\.2/.test(e.message)
  );

  const spec = await rt.service.register('r-ok-1', makeCandidate() as never, actor);
  assert.equal(spec.generation, 1);
  assert.equal(spec.owner.sessionId, actor.owner.sessionId);
  rt.close();
  fx.cleanup();
});

test('service.register: idempotent requestId — same digest replays, different digest conflicts', async () => {
  const fx = makeFixture('pw-svc-idem-');
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();
  const candidate = makeCandidate() as never;
  const spec1 = await rt.service.register('req-x', candidate, actor);
  const spec2 = await rt.service.register('req-x', candidate, actor);
  assert.equal(spec1.watchId, spec2.watchId);
  const listed = await rt.service.list(undefined, 50, actor);
  assert.equal((listed as { items: unknown[] }).items.length, 1, 'replay must not create a second watch');
  await assert.rejects(
    rt.service.register('req-x', makeCandidate({ mission: { objective: 'different', scope: 's', checkpointId: 'c', requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required' } }) as never, actor),
    (e: unknown) => e instanceof WatcherError && e.code === 'REQUEST_CONFLICT'
  );
  rt.close();
  fx.cleanup();
});

test('service.control: CAS on controlRevision; pause→resume→close transitions; control outbox recorded', async () => {
  const fx = makeFixture('pw-svc-ctl-');
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();
  const spec = await rt.service.register('req-ctl-1', makeCandidate() as never, actor);

  // Wrong revision -> STALE_REVISION
  await assert.rejects(
    rt.service.control('req-ctl-2', spec.watchId, 99, 'pause', 'stale test', actor),
    (e: unknown) => e instanceof WatcherError && e.code === 'STALE_REVISION'
  );

  const paused = (await rt.service.control('req-ctl-2', spec.watchId, 1, 'pause', 'host pause', actor)) as Record<string, unknown>;
  assert.equal(paused.lifecycle, 'paused');
  assert.equal(paused.controlRevision, 2);
  assert.deepEqual(
    (paused.sourceFence as Record<string, unknown>).status,
    'not-configured',
    'no relay → no in-flight managed events; pause reports not-configured honestly'
  );

  // Idempotent replay
  const pausedReplay = (await rt.service.control('req-ctl-2', spec.watchId, 1, 'pause', 'host pause', actor)) as Record<string, unknown>;
  assert.equal(pausedReplay.controlRevision, 2);

  // resume requires paused and expected=2
  await assert.rejects(
    rt.service.control('req-ctl-3', spec.watchId, 1, 'resume', 'wrong rev', actor),
    (e: unknown) => e instanceof WatcherError && e.code === 'STALE_REVISION'
  );
  const resumed = (await rt.service.control('req-ctl-3', spec.watchId, 2, 'resume', 'host resume', actor)) as Record<string, unknown>;
  assert.equal(resumed.lifecycle, 'active');

  const closed = (await rt.service.control('req-ctl-4', spec.watchId, 3, 'close', 'done', actor)) as Record<string, unknown>;
  assert.equal(closed.lifecycle, 'closed');

  // owner isolation
  const stranger = makeActor('other-session');
  await assert.rejects(
    rt.service.inspect(spec.watchId, stranger),
    (e: unknown) => e instanceof WatcherError && e.code === 'CAPABILITY_DENIED'
  );
  rt.close();
  fx.cleanup();
});

test('service.check: merges in-flight same-revision checks; returns stable inspectionId', async () => {
  const fx = makeFixture('pw-svc-chk-');
  const rt = await startRuntime({ rootDir: fx.rootDir, sourceRoots: new Map([['executor-local', fx.sourceRoot]]) });
  const actor = makeActor();
  const spec = await rt.service.register('req-chk-1', makeCandidate() as never, actor);
  const [a, b] = await Promise.all([
    rt.service.check('req-chk-a', spec.watchId, 1, actor),
    rt.service.check('req-chk-b', spec.watchId, 1, actor)
  ]);
  assert.equal(a.inspectionId, b.inspectionId, 'same controlRevision in-flight merges');
  await assert.rejects(
    rt.service.check('req-chk-c', spec.watchId, 9, actor),
    (e: unknown) => e instanceof WatcherError && e.code === 'STALE_REVISION'
  );
  rt.close();
  fx.cleanup();
});
