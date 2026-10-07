/**
 * Joint acceptance, end to end against the real pi-relay library:
 * 1) Local-domain sentinel audience (provision -> valid_until=9007199254740991, >24h wall removed)
 * 2) First delivery fails (unknown) -> periodic retry succeeds (source-staged) - delivery retry leg
 * 3) fast-close delivery guard: no auto-close while a wake is in flight (source-staged + envelope alive) - delivery guard leg
 * 4) idle pump claim (quiet-session external-arrival semantics) -> wake callback
 * 5) host respond(resolved) -> readResponses -> applyResponse -> episode resolved -> hostResponded fast-close
 * 6) relay.close() -> audience closed (explicit lifecycle)
 *
 * Clock note: relay-side hard validation uses the real system clock (occurredAt offset <=60s, validUntil > now),
 * so ManualClock is anchored at realNow-55s and publish only happens at moments with offset <60s.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
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

const SENTINEL = 9007199254740991;
const idle = { idle: true, pending: false, knownWait: false, strictNoAutoResume: false };
const target = { sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' };

function audienceRow(relayHome: string, sourceId: string): { valid_until: number; state: string } {
  const dir = path.join(relayHome, 'sources', createHash('sha256').update(sourceId).digest('hex'));
  const db = new Database(path.join(dir, 'source.sqlite'), { readonly: true });
  try {
    return db.prepare('SELECT valid_until, state FROM managed_audiences WHERE audience_ref=?').get('watcher-main') as { valid_until: number; state: string };
  } finally {
    db.close();
  }
}

test('joint e2e: sentinel audience + retry recovery + delivery guard + idle claim + respond->applied->fast-close + explicit close', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-joint-e2e-')));
  const relayHome = path.join(tmp, 'relay-home');
  let relay: WatcherRelaySource | null = null;
  let targetHost: Awaited<ReturnType<typeof createTarget>> | null = null;
  let store: WatchStore | null = null;
  try {
    // 0) Real closed-loop prerequisites: source + declaration + invite + target binding + consumer registration (allow guard)
    relay = await WatcherRelaySource.open(path.join(tmp, 'watcher-relay'), { relayHome, realm: 'local' });
    relay.ensureConsumerDeclaration();
    const { inviteFile } = relay.createInvite();
    targetHost = await createTarget({ home: relayHome, realm: 'local', fingerprint: createHash('sha256').update('pw-joint-e2e').digest('hex') });
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

    // 1) sentinel audience (>24h wall removed)
    relay.provision();
    const prov = audienceRow(relayHome, 'pi-watcher');
    assert.equal(prov.valid_until, SENTINEL, 'local-domain audience sentinel validity');
    assert.equal(prov.state, 'open');

    // 2) watcher runtime: delivery that fails on first publish and succeeds on the second (retry leg + guard timing)
    let failFirst = true;
    const realRelay: WatcherRelaySource = relay;
    const flakyDelivery = {
      publish: async (bytes: Uint8Array, scope?: { scopeId: string; revision: number }) => {
        if (failFirst) { failFirst = false; throw new Error('simulated transient outage'); }
        return realRelay.publish(bytes, scope);
      },
      advanceScope: (...args: Parameters<WatcherRelaySource['advanceScope']>) => realRelay.advanceScope(...args),
      withdraw: (...args: Parameters<WatcherRelaySource['withdraw']>) => realRelay.withdraw(...args)
    } as unknown as typeof relay;
    const sourceRoot = path.join(tmp, 'sources');
    await runScenario(sourceRoot, target, scenarioSteps('build-failure', 10));
    store = await WatchStore.open(path.join(tmp, 'watcher-state'), { mode: 'embedded' });
    const clock = new ManualClock(Date.now() - 55_000); // anchored at real clock -55s: offset at publish time <60s
    const engine = new WatchEngine({
      clock, store,
      adapters: new Map([['executor-local', new TaskStatusV1Adapter(sourceRoot)]]),
      delivery: flakyDelivery
    });
    const service = new WatchService({
      clock, store, engine,
      negotiation: negotiationFromRelaySource(relay),
      allowedSourceIds: ['executor-local'],
      delivery: flakyDelivery
    });
    const spec = await service.register('joint-e2e-1', makeCandidate({
      policy: { ...makeCandidate().policy, transport: 'relay', semanticMode: 'off', attentionTtlMs: 120_000 }
    }), makeActor());

    // Cycle 1 (t0): task.failed -> first publish fails -> admission=unknown
    await engine.inspectWatch(spec.watchId);
    let att = store.transaction(tx => tx.listOutboxByWatch(spec.watchId, 10)).find(o => o.eventType === 'watcher.attention.v1')!;
    assert.equal(att.admission, 'unknown', 'first-publish failure honestly recorded as unknown');
    assert.match(String((att.admissionJson as Record<string, unknown>).error), /simulated transient outage/);

    // Cycle 2 (t0+70s): retry succeeds -> source-staged (real relay store captured)
    clock.advanceWall(70_000);
    await engine.inspectWatch(spec.watchId);
    att = store.transaction(tx => tx.listOutboxByWatch(spec.watchId, 10)).find(o => o.eventType === 'watcher.attention.v1')!;
    assert.equal(att.admission, 'source-staged', 'retry success advances admission');
    const episode = store.transaction(tx => tx.listEpisodes(spec.watchId, 10)).find(e => e.kind === 'task.failed')!;
    assert.equal(episode.state, 'open', 'episode stays open before the response');

    // 4) idle pump claim (quiet-session external-arrival semantics, target behavior after the delivery-guard fix)
    let wake: { event?: { id?: string } } | null = null;
    const pumped = await targetHost.managed.pumpManaged(idle, async packet => {
      wake = packet as { event?: { id?: string } };
      return { evidence: 'runtime-entry' };
    });
    assert.ok(pumped.claimed >= 1, 'idle session claims the external arrival');
    assert.equal((wake as { event?: { id?: string } } | null)?.event?.id, att.eventId, 'wake is that attention');

    // 5) host respond(resolved) -> source sync -> watcher applyResponse -> episode resolved
    const deliveries = (targetHost.managed as unknown as {
      managedDeliveries(): Array<{ deliveryRef: string; eventId: string; state: string }>;
    }).managedDeliveries();
    const dv = deliveries.find(d => d.eventId === att.eventId && (d.state === 'recorded' || d.state === 'submitted'))!;
    assert.ok(dv, 'target side recorded');
    targetHost.managed.respond({
      operationId: newId('op'),
      deliveryRef: dv.deliveryRef,
      responseType: 'watcher.response.v1',
      schemaVersion: 1,
      data: {
        schemaVersion: 1, requestId: 'joint-e2e-resp-1', watchId: spec.watchId, generation: 1,
        episodeId: episode.episodeId, expectedEpisodeRevision: episode.revision,
        action: 'resolved', reason: 'joint e2e acceptance', evidenceIds: [],
        ownerBindingEpoch: 1
      }
    });
    await new Promise(r => setTimeout(r, 1600)); // source-side managed sync pump pulls consumer-response
    const batch = await relay.readResponses(0);
    assert.ok(batch.responses.length >= 1, 'host response flows back to source');
    const applied = await service.applyResponse(batch.responses[0]);
    assert.equal(applied.outcome, 'applied', 'watcher applies the response');
    await relay.confirmApplied(`conf-${batch.responses[0].responseId}`, batch.responses[0].responseId, applied);
    assert.equal(store.transaction(tx => tx.getEpisode(episode.episodeId))!.state, 'resolved', 'episode resolved');

    // Cycle 3 (t0+185s, past the close line): host has responded -> hostResponded fast-close.
    // Note: the envelope is frozen in cycle 1 (immutable outbox), and validUntil is always 60s before the close line -
    // the source-staged in-flight guard is a defensive clause; guard behavior for the unconfirmed unknown leg is covered by delivery-loud #3.
    clock.advanceWall(115_000);
    await engine.inspectWatch(spec.watchId);
    const row = store.transaction(tx => tx.getWatchRow(spec.watchId))!;
    assert.equal(row.lifecycle, 'closed', 'hostResponded fast-close works');

    // 7) explicit lifecycle: relay.close() -> audience closed
    await relay.close();
    const closedRow = audienceRow(relayHome, 'pi-watcher');
    assert.equal(closedRow.state, 'closed', 'audience explicitly closed (the only way to invalidate the sentinel)');
  } finally {
    try { await targetHost?.close(); } catch { /* best effort */ }
    try { await relay?.close(); } catch { /* best effort */ }
    try { store?.close(); } catch { /* best effort */ }
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
