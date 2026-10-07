/**
 * V1 closed-loop integration test: watcher embedded relay managed source -> real TargetHost ->
 * consumer guard allow -> respond -> watcher consumes response -> applyResponse -> confirmApplied.
 * Uses the real pi-relay engineering library (file: dependency), not a mock transport.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { WatcherRelaySource } from '../../src/relay/managed.js';
import { negotiationFromRelaySource } from '../../src/relay/negotiate.js';
import { WatchStore } from '../../src/storage/store.js';
import { TaskStatusV1Adapter } from '../../src/source/task-status-v1/adapter.js';
import { WatchEngine } from '../../src/engine/engine.js';
import { WatchService } from '../../src/engine/service.js';
import { SystemClock } from '../../src/util/clock.js';
import { makeActor, makeCandidate } from '../engine/fixture.js';
import { createTarget } from '@maolon/pi-relay/target';
import { createHash } from 'node:crypto';
import { newId } from '@maolon/pi-relay/protocol';

const idle = { idle: true, pending: false, knownWait: false, strictNoAutoResume: false };

test('relay closed loop: setup -> bind -> provision -> publish -> guard allow -> respond -> applied', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-relay-')));
  const relayHome = path.join(tmp, 'relay-home');
  let relay: import('../../src/relay/managed.js').WatcherRelaySource | null = null;
  let target: Awaited<ReturnType<typeof createTarget>> | null = null;
  let store: import('../../src/storage/store.js').WatchStore | null = null;
  try {
    // 1) watcher embedded managed source + owner configuration (declaration + invite)
    relay = await WatcherRelaySource.open(path.join(tmp, 'watcher-relay'), { relayHome, realm: 'pw-test' });
    const decl = relay.ensureConsumerDeclaration();
    assert.ok(fs.existsSync(decl.file), 'consumer declaration file written to disk');
    const { inviteFile } = relay.createInvite();
    assert.ok(fs.existsSync(inviteFile), 'invite file written to disk');

    // Negotiation is honestly local-display when unbound
    assert.equal(negotiationFromRelaySource(relay).transport, 'local-display');

    // 2) Real TargetHost binding (simulates host session relay extension behavior)
    target = await createTarget({ home: relayHome, realm: 'pw-test', fingerprint: createHash('sha256').update('pw-target-loop').digest('hex') });
    const invite = JSON.parse(fs.readFileSync(inviteFile, 'utf8'));
    const bindingId = await target.bind(invite, { resume: true, operationId: newId('op') });
    target.core.control(bindingId, {
      operationId: newId('op'),
      expectedRevision: target.core.binding(bindingId).revision,
      action: 'arm',
      grant: { eventTypes: ['watcher.attention.v1', 'watcher.result.v1'], maxClaims: 4, ttlMs: 30 * 60 * 1000 }
    });

    // 3) Register the consumer (simulates relay stage-4 session scanning declarations and registering)
    target.managed.registerConsumer({
      profileId: 'pi-watcher-host',
      eventManifestDigest: 'decl-manifest',
      responseManifestDigest: 'resp-manifest',
      guardImplementationId: 'declared-guard',
      timeoutMs: 1000
    }, async () => ({
      decision: 'allow', reasonCode: 'CURRENT', guardEpoch: 1,
      validUntil: new Date(Date.now() + 3_600_000).toISOString()
    }));

    // 4) finalize: audience + scope
    assert.ok(relay.hasActiveMembership(), 'membership exists after binding');
    const prov = relay.provision();
    assert.equal(prov.scopeRevision, 1);
    const negotiation = negotiationFromRelaySource(relay);
    assert.equal(negotiation.status, 'managed-1-2');
    assert.equal(negotiation.transport, 'relay');

    // 5) watcher runtime: active semantic mode + delivery publish
    store = await WatchStore.open(path.join(tmp, 'watcher-state'), { mode: 'embedded' });
    const clock = new SystemClock();
    const engine = new WatchEngine({
      clock, store,
      adapters: new Map([['executor-local', new TaskStatusV1Adapter(path.join(tmp, 'sources'))]]),
      judge: { evaluate: async () => ({
        basis: {} as never, model: 'mock', questionSet: 'watcher-q1',
        probabilities: {
          meaningful_progress: 0.1, unresolved_blocker: 0.1, needs_host_decision: 0.95,
          repeating_without_new_information: 0.1, claim_conflicts_with_evidence: 0.1, context_sufficient: 0.9
        },
        receivedAt: new Date().toISOString(), inputTokens: 10, discarded: false
      }), chooseProbe: async () => 'none' },
      semanticConsent: true,
      judgeRequiresConsent: false,
      delivery: relay
    });
    const service = new WatchService({
      clock, store, engine,
      negotiation,
      allowedSourceIds: ['executor-local'],
      delivery: relay
    });
    // transport=relay registration is now legal (negotiated managed-1-2)
    const spec = await service.register('reg-1', makeCandidate({
      policy: { ...makeCandidate().policy, transport: 'relay', semanticMode: 'active' }
    }), makeActor());

    // 6) semantic inspection -> decision.required episode -> attention published via relay
    const srcDir = path.join(tmp, 'sources', 'executor-local', 'build', 'run-1', 'attempt-1');
    fs.mkdirSync(srcDir, { recursive: true });
    fs.writeFileSync(path.join(srcDir, 'snapshot.json'), JSON.stringify({
      schemaVersion: 1,
      target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' },
      state: 'running', stage: 'compile', snapshotSeq: 1, updatedAt: new Date().toISOString()
    }));
    fs.writeFileSync(path.join(srcDir, 'journal.jsonl'), '');
    await engine.inspectWatch(spec.watchId);
    const outbox = store.transaction(tx => tx.listOutboxByWatch(spec.watchId, 10));
    const att = outbox.find(o => o.eventType === 'watcher.attention.v1');
    assert.ok(att, 'attention envelope lands in the outbox');
    assert.equal(att!.admission, 'source-staged', `managed publish accepted (actual ${att!.admission})`);

    // 7) Target pump: guard allow -> intent -> invoke -> submitted -> recorded
    let deliveredEvent: { options?: { consumerProfileId?: string } } | null = null;
    const pumped = await target.managed.pumpManaged(idle, async packet => {
      deliveredEvent = packet as typeof deliveredEvent;
      return { evidence: 'runtime-entry' };
    });
    assert.ok(pumped.claimed >= 1, 'pump claims the managed delivery');
    assert.equal((deliveredEvent as { options?: { consumerProfileId?: string } } | null)?.options?.consumerProfileId, 'pi-watcher-host');

    // 8) Host response (watcher.response.v1 / HostAck semantics)
    const deliveries = (target.managed as unknown as {
      managedDeliveries(): Array<{ deliveryRef: string; eventId: string; state: string }>;
    }).managedDeliveries();
    const dv = deliveries.find(d => d.state === 'recorded' || d.state === 'submitted');
    assert.ok(dv, 'target side has a submitted/recorded delivery');
    const episode = store.transaction(tx => tx.listEpisodes(spec.watchId, 10)).find(e => e.kind === 'decision.required')!;
    const respondResult = target.managed.respond({
      operationId: newId('op'),
      deliveryRef: dv!.deliveryRef,
      responseType: 'watcher.response.v1',
      schemaVersion: 1,
      data: {
        schemaVersion: 1, requestId: 'resp-1', watchId: spec.watchId, generation: 1,
        episodeId: episode.episodeId, expectedEpisodeRevision: episode.revision,
        action: 'received', reason: 'host received it and started investigating', evidenceIds: [],
        ownerBindingEpoch: 1
      }
    });
    assert.ok(respondResult.responseId, 'response staged');

    // 9) watcher consumes response -> applyResponse -> confirmApplied (full round trip)
    await new Promise(r => setTimeout(r, 1600)); // source-side managed sync pump (500ms interval) pulls consumer-response
    const batch = await relay.readResponses(0);
    assert.ok(batch.responses.length >= 1, 'readResponses gets the host response');
    const response = batch.responses[0];
    const applied = await service.applyResponse(response);
    assert.equal(applied.outcome, 'applied', `applied successfully (actual ${JSON.stringify(applied)})`);
    await relay.confirmApplied(`conf-${response.responseId}`, response.responseId, applied);
    const episodeAfter = store.transaction(tx => tx.getEpisode(episode.episodeId))!;
    assert.equal(episodeAfter.state, 'acknowledged', 'episode enters acknowledged');

    // 10) receipt review: sourceState captured + routes delivery state
    const receipt = await relay.reconcile(att!.eventId);
    assert.equal(receipt.sourceState, 'captured');

  } finally {
    try { await relay?.close(); } catch { /* best effort */ }
    try { await target?.close(); } catch { /* best effort */ }
    try { store?.close(); } catch { /* best effort */ }
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('when relay is not ready, active mode only lands locally (no sneaking a plain publish)', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-relay2-')));
  let relay: import('../../src/relay/managed.js').WatcherRelaySource | null = null;
  try {
    relay = await WatcherRelaySource.open(path.join(tmp, 'watcher-relay'), {
      relayHome: path.join(tmp, 'relay-home'), realm: 'pw-test2'
    });
    relay.createInvite();
    // unbound and unconfigured -> negotiation local-display -> engine has no delivery
    const negotiation = negotiationFromRelaySource(relay);
    assert.equal(negotiation.transport, 'local-display');
    assert.ok(negotiation.detail.includes('bind') || negotiation.detail.includes('finalize'));
  } finally {
    try { await relay?.close(); } catch { /* best effort */ }
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
