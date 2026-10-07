/**
 * pause/close cut against the real pi-relay libraries (design 6.5, 9.6; I15, I20, I23):
 * 1) the first managed attention creates the watch's own relay scope and is captured under it;
 * 2) pause, before the host claims the wake, advances that scope and withdraws the event;
 *    the result is layered (sourceFence + per-route dispositions), never a blanket "withdrawn";
 * 3) the target pump, using the source scope proof, fences the delivery: no wake is invoked;
 * 4) resume re-activates the scope but does not re-arm the withdrawn event.
 *
 * Clock note: the relay checks occurredAt skew against the real clock, so the ManualClock is
 * anchored at realNow-55s and publishes happen within that window.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createHash } from 'node:crypto';
import { WatcherRelaySource } from '../../src/relay/managed.js';
import { negotiationFromRelaySource } from '../../src/relay/negotiate.js';
import { WatchStore } from '../../src/storage/store.js';
import { TaskStatusV1Adapter } from '../../src/source/task-status-v1/adapter.js';
import { runScenario, scenarioSteps } from '../../src/source/task-status-v1/producer.js';
import { WatchEngine } from '../../src/engine/engine.js';
import { WatchService } from '../../src/engine/service.js';
import { ManualClock } from '../../src/util/clock.js';
import { makeActor, makeCandidate } from '../engine/fixture.js';
import { createTarget } from '@maolon/pi-relay/target';
import { newId } from '@maolon/pi-relay/protocol';

const idle = { idle: true, pending: false, knownWait: false, strictNoAutoResume: false };
const target = { sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' };

test('pause withdraws an unclaimed managed attention; the target pump fences it; resume does not re-arm', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-pause-withdraw-')));
  const relayHome = path.join(tmp, 'relay-home');
  let relay: WatcherRelaySource | null = null;
  let targetHost: Awaited<ReturnType<typeof createTarget>> | null = null;
  let store: WatchStore | null = null;
  try {
    relay = await WatcherRelaySource.open(path.join(tmp, 'watcher-relay'), { relayHome, realm: 'local' });
    relay.ensureConsumerDeclaration();
    const { inviteFile } = relay.createInvite();
    targetHost = await createTarget({ home: relayHome, realm: 'local', fingerprint: createHash('sha256').update('pw-pause-withdraw').digest('hex') });
    const invite = JSON.parse(fs.readFileSync(inviteFile, 'utf8'));
    const bindingId = await targetHost.bind(invite, { resume: true, operationId: newId('op') });
    targetHost.core.control(bindingId, {
      operationId: newId('op'),
      expectedRevision: targetHost.core.binding(bindingId).revision,
      action: 'arm',
      grant: { eventTypes: ['watcher.attention.v1', 'watcher.result.v1'], maxClaims: 4, ttlMs: 30 * 60 * 1000 }
    });
    targetHost.managed.registerConsumer({
      profileId: 'pi-watcher-host',
      eventManifestDigest: 'decl-manifest',
      responseManifestDigest: 'resp-manifest',
      guardImplementationId: 'declared-guard',
      timeoutMs: 1000
    }, async () => ({
      decision: 'allow', reasonCode: 'CURRENT', guardEpoch: 1,
      validUntil: new Date(Date.now() + 3_600_000).toISOString()
    }));
    relay.provision();

    const sourceRoot = path.join(tmp, 'sources');
    await runScenario(sourceRoot, target, scenarioSteps('build-failure', 10));
    store = await WatchStore.open(path.join(tmp, 'watcher-state'), { mode: 'embedded' });
    const clock = new ManualClock(Date.now() - 55_000);
    const engine = new WatchEngine({
      clock, store,
      adapters: new Map([['executor-local', new TaskStatusV1Adapter(sourceRoot)]]),
      delivery: relay
    });
    const service = new WatchService({
      clock, store, engine,
      negotiation: negotiationFromRelaySource(relay),
      allowedSourceIds: ['executor-local'],
      delivery: relay
    });
    const actor = makeActor();
    const spec = await service.register('pause-withdraw-1', makeCandidate({
      policy: { ...makeCandidate().policy, transport: 'relay', semanticMode: 'off', attentionTtlMs: 120_000 }
    }), actor);

    // 1) task.failed -> attention captured under the watch's own scope
    await engine.inspectWatch(spec.watchId);
    const att = store.transaction(tx => tx.listOutboxByWatch(spec.watchId, 10)).find(o => o.eventType === 'watcher.attention.v1')!;
    assert.equal(att.admission, 'source-staged');
    const scope = (att.admissionJson as { scope?: { scopeId: string; revision: number } }).scope;
    assert.ok(scope && scope.scopeId.startsWith(`scope-${spec.watchId}-g1-e1`), 'per-watch scope recorded with the event');
    assert.equal(scope!.revision, 1);

    // 2) pause before the host claims the wake
    const row = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    const paused = await service.control('pause-1', spec.watchId, row.controlRevision, 'pause', 'user paused', actor) as Record<string, unknown>;
    assert.equal(paused.localPaused, true);
    assert.equal((paused.sourceFence as { status: string }).status, 'applied', 'scope advance confirmed by relay');
    const routes = paused.routes as Array<{ eventId: string; disposition: string }>;
    assert.ok(routes.length >= 1, 'one entry per affected route');
    for (const r of routes) {
      assert.equal(r.eventId, att.eventId);
      assert.ok(['prevented', 'too_late', 'pending', 'unknown'].includes(r.disposition), r.disposition);
    }
    assert.equal(paused.withdrawn, routes.every(r => r.disposition === 'prevented'), '"withdrawn" only when every route is prevented (I15)');
    const afterPause = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    assert.deepEqual(afterPause.snapshot.relayScope, { scopeId: scope!.scopeId, revision: 2, state: 'paused' });

    // 3) target pump with the source scope proof: the delivery is fenced, no wake invoked
    const sourceHost = (relay as unknown as { host: { managed: { scopeProof(b: string, ids: string[]): unknown } } }).host;
    const proof = async (b: string, ids: string[]) => sourceHost.managed.scopeProof(b, ids) as never;
    let woke = false;
    await targetHost.managed.pumpManaged(idle, async () => {
      woke = true;
      return { evidence: 'runtime-entry' };
    }, proof);
    assert.equal(woke, false, 'paused watch must not wake the host');
    const deliveries = (targetHost.managed as unknown as {
      managedDeliveries(): Array<{ deliveryRef: string; eventId: string; state: string }>;
    }).managedDeliveries().filter(d => d.eventId === att.eventId);
    assert.ok(deliveries.length >= 1 && deliveries.every(d => d.state === 'withdrawn'), `delivery fenced at target (${deliveries.map(d => d.state).join(',')})`);

    // 4) resume: scope active again, old event stays fenced (no re-arm)
    const resumed = await service.control('resume-1', spec.watchId, afterPause.controlRevision, 'resume', 'user resumed', actor) as Record<string, unknown>;
    assert.equal((resumed.sourceFence as { status: string }).status, 'applied');
    assert.deepEqual(store.transaction(tx => tx.getWatchRow(spec.watchId))!.snapshot.relayScope, { scopeId: scope!.scopeId, revision: 3, state: 'active' });
    await targetHost.managed.pumpManaged(idle, async () => {
      woke = true;
      return { evidence: 'runtime-entry' };
    }, proof);
    assert.equal(woke, false, 'resume does not re-arm a withdrawn attention');

    // replay of the same pause request returns the same local cut plus the durable relay layers
    const replay = await service.control('pause-1', spec.watchId, row.controlRevision, 'pause', 'user paused', actor) as Record<string, unknown>;
    assert.equal(replay.controlRevision, paused.controlRevision);
    assert.equal((replay.sourceFence as { status: string }).status, 'applied');
  } finally {
    try { await targetHost?.close(); } catch { /* best effort */ }
    try { await relay?.close(); } catch { /* best effort */ }
    try { store?.close(); } catch { /* best effort */ }
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
