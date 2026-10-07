/**
 * Make delivery failure loud: relay publish fails ->
 * 1) admission=unknown is persisted (truth retained);
 * 2) watch health=degraded + degradedReason=relay publish failed (widget [!] visible);
 * 3) onAttention transport=relay-failed + relayError (toast states relay wake FAILED);
 * 4) bounded idempotent retry each cycle within the envelope TTL; self-heal after recovery (health back to healthy, admission advances);
 * 5) no more retries after the envelope expires; the symptom is annotated host must inspect.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as path from 'node:path';
import { WatchStore } from '../../src/storage/store.js';
import { TaskStatusV1Adapter } from '../../src/source/task-status-v1/adapter.js';
import { WatchEngine, type AttentionNotice } from '../../src/engine/engine.js';
import { WatchService } from '../../src/engine/service.js';
import { runScenario, scenarioSteps, TaskStatusProducer } from '../../src/source/task-status-v1/producer.js';
import { ManualClock } from '../../src/util/clock.js';
import { makeActor, makeCandidate, makeFixture } from './fixture.js';
import type { ManagedDeliveryPort } from '../../src/contracts/interfaces.js';

type FakeReceipt = Awaited<ReturnType<ManagedDeliveryPort['publish']>>;

const target = { sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' };
const negotiation = { status: 'managed-1-2', transport: 'relay' } as never;

/** Fake delivery with switchable success/failure: publish throws to simulate binding_expired-style rejection (implements only the publish used by the engine path). */
function fakeDelivery(shouldFail: () => boolean) {
  let calls = 0;
  const port = {
    publish: async (): Promise<FakeReceipt> => {
      calls += 1;
      if (shouldFail()) throw new Error('Binding authority has expired');
      return {
        eventId: 'ev', sourceState: 'captured', sourceCursor: 1,
        scope: { id: 'watcher-scope-1', revision: 1 },
        routes: [{ admission: 'accepted' }], responses: []
      } as unknown as FakeReceipt;
    },
    // Per-watch scope creation succeeds; only publish is flaky in these scenarios.
    advanceScope: async (_op: string, _scopeId: string, _expected: number, next: number, state: string) => ({ revision: next, state }),
    withdraw: async (operationId: string, eventId: string) => ({ operationId, eventId, sourceApplied: true, routes: [] })
  } as unknown as ManagedDeliveryPort;
  return { port, calls: () => calls };
}

interface Harness {
  engine: WatchEngine;
  store: WatchStore;
  service: WatchService;
  spec: { watchId: string };
  notices: AttentionNotice[];
  clock: ManualClock;
  sourceRoot: string;
  cleanup: () => void;
}

async function harness(prefix: string, delivery: ManagedDeliveryPort, candidateOverrides: Record<string, unknown> = {}): Promise<Harness> {
  const fx = makeFixture(prefix);
  const store = await WatchStore.open(path.join(fx.rootDir, 'state'), { mode: 'embedded' });
  const clock = new ManualClock(Date.parse('2026-09-23T03:54:00Z'));
  const notices: AttentionNotice[] = [];
  const engine = new WatchEngine({
    clock, store,
    adapters: new Map([['executor-local', new TaskStatusV1Adapter(fx.sourceRoot)]]),
    delivery,
    onAttention: n => notices.push(n)
  });
  const service = new WatchService({
    clock, store, engine, negotiation,
    allowedSourceIds: ['executor-local'], delivery
  });
  const spec = await service.register('req-loud-1', makeCandidate({
    policy: { ...makeCandidate().policy, transport: 'relay' },
    ...candidateOverrides
  }), makeActor());
  return { engine, store, service, spec, notices, clock, sourceRoot: fx.sourceRoot, cleanup: fx.cleanup };
}

function outboxAttention(store: WatchStore, watchId: string) {
  return store.transaction(tx => tx.listOutboxByWatch(watchId, 10))
    .find(o => o.eventType === 'watcher.attention.v1')!;
}

function watchRow(store: WatchStore, watchId: string) {
  return store.transaction(tx => tx.getWatchRow(watchId))!;
}

test('delivery loud: publish failure -> unknown persisted + degraded + relay-failed notice; self-heal after retry recovers', async () => {
  let failing = true;
  const d = fakeDelivery(() => failing);
  const h = await harness('pw-dl-loud-', d.port);
  try {
    await runScenario(h.sourceRoot, target, scenarioSteps('build-failure', 10));

    // Cycle 1: task.failed attention publish is rejected
    await h.engine.inspectWatch(h.spec.watchId);
    let att = outboxAttention(h.store, h.spec.watchId);
    assert.equal(att.admission, 'unknown', 'truth persisted as unknown');
    assert.match(String((att.admissionJson as Record<string, unknown>).error), /Binding authority has expired/);
    let row = watchRow(h.store, h.spec.watchId);
    assert.equal(row.health, 'degraded', 'delivery failure must lower health (widget [!] visible)');
    assert.match(String(row.snapshot.degradedReason), /^relay publish failed/);
    assert.equal(h.notices.length, 1);
    assert.equal(h.notices[0].transport, 'relay-failed');
    assert.equal(h.notices[0].reasonCode, 'task.failed');
    assert.match(String(h.notices[0].relayError), /Binding authority has expired/);
    assert.equal(d.calls(), 1);

    // Cycle 2 (+5s, still failing): episode supersede does not reopen; one bounded retry within the TTL; symptom persists
    h.clock.advanceWall(5_000);
    await h.engine.inspectWatch(h.spec.watchId);
    assert.equal(d.calls(), 2, 'idempotent retry each cycle within the envelope TTL');
    row = watchRow(h.store, h.spec.watchId);
    assert.equal(row.health, 'degraded');
    assert.equal(h.notices.length, 1, 'retry does not duplicate the toast');

    // Cycle 3 (+5s, delivery recovered): retry succeeds -> admission advances + health self-heals
    failing = false;
    h.clock.advanceWall(5_000);
    await h.engine.inspectWatch(h.spec.watchId);
    att = outboxAttention(h.store, h.spec.watchId);
    assert.equal(att.admission, 'source-staged', 'admission advances after a successful retry');
    row = watchRow(h.store, h.spec.watchId);
    assert.equal(row.health, 'healthy', 'health self-heals after delivery recovers');
    assert.equal(row.snapshot.degradedReason, null);
    assert.equal(d.calls(), 3);
  } finally {
    h.cleanup();
  }
});

test('delivery guard: while wake is unconfirmed (admission=unknown) fast-close does not swallow the episode; explicit close still finishes', async () => {
  const d = fakeDelivery(() => true);
  const h = await harness('pw-dl-guard-', d.port);
  try {
    await runScenario(h.sourceRoot, target, scenarioSteps('build-failure', 10));
    await h.engine.inspectWatch(h.spec.watchId); // task.failed → attention admission=unknown
    // Past the old close line: terminalAt + attentionTtl (fixture 120s) + 60s grace
    h.clock.advanceWall(200_000);
    await h.engine.inspectWatch(h.spec.watchId);
    let row = watchRow(h.store, h.spec.watchId);
    assert.equal(row.lifecycle, 'active', 'wake unconfirmed -> no auto-close (undelivered != host has seen)');
    const eps = h.store.transaction(tx => tx.listEpisodes(h.spec.watchId, 10));
    assert.ok(eps.some(e => e.kind === 'task.failed' && e.state === 'open'),
      'episode stays open -- the host has not seen it, so TTL cleanup must not drop it');
    assert.equal(row.health, 'degraded', 'expired-but-undelivered wake stays loud (widget [!])');
    assert.match(String(row.snapshot.degradedReason), /relay publish failed/, 'symptom points at delivery failure');
    // owner explicit close can still finish (control plane is not blocked by the guard)
    await h.service.control('req-guard-close', h.spec.watchId, 1, 'close', 'owner done', makeActor());
    row = watchRow(h.store, h.spec.watchId);
    assert.equal(row.lifecycle, 'closed');
  } finally {
    h.cleanup();
  }
});

test('delivery loud: no retry after the envelope expires; symptom annotated host must inspect', async () => {
  const d = fakeDelivery(() => true);
  const h = await harness('pw-dl-exp-', d.port, {
    mission: {
      objective: 'o', scope: 's', checkpointId: 'compile-and-test',
      requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required',
      deadlineAt: '2026-09-23T03:00:00Z' // earlier than the ManualClock start -> exceeded immediately
    }
  });
  try {
    const producer = new TaskStatusProducer(h.sourceRoot, target);
    await producer.publishSnapshot({ state: 'running', stage: 'compile' });

    // Cycle 1: deadline.exceeded attention publish is rejected
    await h.engine.inspectWatch(h.spec.watchId);
    const att = outboxAttention(h.store, h.spec.watchId);
    assert.equal(att.admission, 'unknown');
    assert.equal(h.notices[0].transport, 'relay-failed');
    assert.equal(h.notices[0].reasonCode, 'deadline.exceeded');
    assert.equal(d.calls(), 1);

    // Cycle 2 (+200s > attentionTtlMs 120s): envelope has expired -> no retry, only make loud
    h.clock.advanceWall(200_000);
    await h.engine.inspectWatch(h.spec.watchId);
    assert.equal(d.calls(), 1, 'expired envelope is not retried');
    const row = watchRow(h.store, h.spec.watchId);
    assert.equal(row.health, 'degraded');
    assert.match(String(row.snapshot.degradedReason), /envelope expired.*host must inspect/);
    // episode still undecided (nobody was woken), symptom persists
    assert.equal(att.admission, 'unknown');
  } finally {
    h.cleanup();
  }
});
