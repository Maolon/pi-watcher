/**
 * resume self-heal:
 * After reopening the same watcher root (= resume reusing the source store with the same sourceId), the
 * closed audience/scope left by the last exit must be diagnosed and healed - reconfigure with a fresh audienceRef/scopeId so publish revives.
 * No self-heal without an active membership (wait for rebind); idempotent no-op when healthy.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
import { WatcherRelaySource } from '../../src/relay/managed.js';
import { createTarget } from '@maolon/pi-relay/target';
import { newId } from '@maolon/pi-relay/protocol';

function rows(relayHome: string, sourceId: string) {
  const db = new Database(path.join(relayHome, 'sources', createHash('sha256').update(sourceId).digest('hex'), 'source.sqlite'), { readonly: true });
  try {
    return {
      audiences: db.prepare('SELECT audience_ref, state FROM managed_audiences ORDER BY audience_ref').all() as Array<{ audience_ref: string; state: string }>,
      scopes: db.prepare('SELECT scope_id, state FROM managed_scopes ORDER BY scope_id').all() as Array<{ scope_id: string; state: string }>
    };
  } finally {
    db.close();
  }
}

function envelope(id: string) {
  return Buffer.from(JSON.stringify({
    schemaVersion: 1, envelopeId: id, episodeId: 'ep-1', episodeRevision: 1,
    watchId: 'w-heal', generation: 1, missionRevision: 1, controlRevision: 1, ownerBindingEpoch: 1,
    target: { kind: 'run', sourceId: 'x', taskId: 't', runId: 'r', attemptId: 'a' },
    reasonCode: 'task.failed', summary: 'heal probe', evidenceRefs: [],
    requiredNextStep: 'inspect-current-watch-before-acting',
    occurredAt: new Date().toISOString(),
    validUntil: new Date(Date.now() + 60_000).toISOString()
  }), 'utf8');
}

test('resume heal: reopen the same root after close -> diagnose closed -> new audienceRef/scopeId -> publish revives', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-resume-heal-')));
  const relayHome = path.join(tmp, 'relay-home');
  const watcherRoot = path.join(tmp, 'watcher-relay');
  let first: WatcherRelaySource | null = null;
  let second: WatcherRelaySource | null = null;
  let target: Awaited<ReturnType<typeof createTarget>> | null = null;
  try {
    // 1) First process: provision healthy, then exit (explicitly closes audience+scope - the precursor of the resume failure)
    first = await WatcherRelaySource.open(watcherRoot, { relayHome, realm: 'local' });
    first.ensureConsumerDeclaration();
    const { inviteFile } = first.createInvite();
    target = await createTarget({ home: relayHome, realm: 'local', fingerprint: createHash('sha256').update('pw-heal').digest('hex') });
    const invite = JSON.parse(fs.readFileSync(inviteFile, 'utf8'));
    const bindingId = await target.bind(invite, { resume: true, operationId: newId('op') });
    target.core.control(bindingId, {
      operationId: newId('op'),
      expectedRevision: target.core.binding(bindingId).revision,
      action: 'arm',
      grant: { eventTypes: ['watcher.attention.v1', 'watcher.result.v1'], maxClaims: 4, ttlMs: 30 * 60 * 1000 }
    });
    first.provision();
    assert.ok(first.ready());
    await first.close();
    const afterClose = rows(relayHome, 'pi-watcher');
    assert.ok(afterClose.audiences.some(a => a.audience_ref === 'watcher-main' && a.state === 'closed'), 'old audience closed after exit');
    assert.ok(afterClose.scopes.some(s => s.scope_id === 'watcher-scope-1' && s.state === 'closed'), 'old scope closed after exit');

    // 2) resume: reopen the same root (same sourceId reuses the source store; setup.json still shows the stale provisioned=true view)
    second = await WatcherRelaySource.open(watcherRoot, { relayHome, realm: 'local' });
    assert.ok(second.ready(), 'local view falsely reports ready (resume symptom)');
    const d0 = second.diagnose();
    assert.equal(d0.audienceState, 'closed', 'diagnosis reads relay truth closed');

    // 3) self-heal: new audienceRef + new scopeId, idempotent once healthy
    const h1 = second.ensureHealed();
    assert.equal(h1.healed, true, 'reconfiguration performed');
    const h2 = second.ensureHealed();
    assert.equal(h2.healed, false, 'idempotent no-op once healthy');
    const afterHeal = rows(relayHome, 'pi-watcher');
    assert.ok(afterHeal.audiences.some(a => a.audience_ref === 'watcher-main-2' && a.state === 'open'), 'new audienceRef open');
    assert.ok(afterHeal.audiences.some(a => a.audience_ref === 'watcher-main' && a.state === 'closed'), 'old audience kept on record');
    assert.ok(afterHeal.scopes.some(s => s.scope_id === 'watcher-scope-2' && s.state === 'active'), 'new scopeId active');

    // 4) publish revives (same target still alive -> captured + accepted)
    const receipt = await second.publish(envelope('att-heal-probe'));
    assert.equal(receipt.sourceState, 'captured', 'publish passes after healing');
    assert.ok(receipt.routes.some(r => (r as { admission?: string }).admission === 'accepted'), 'route accepted');
  } finally {
    try { await second?.close(); } catch { /* best effort */ }
    try { await target?.close(); } catch { /* best effort */ }
    try { await first?.close(); } catch { /* best effort */ }
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('resume heal: no self-heal without an active membership (wait for rebind); no-op when not provisioned', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-resume-heal2-')));
  const relayHome = path.join(tmp, 'relay-home');
  let relay: WatcherRelaySource | null = null;
  try {
    relay = await WatcherRelaySource.open(path.join(tmp, 'watcher-relay'), { relayHome, realm: 'local' });
    const r1 = relay.ensureHealed();
    assert.equal(r1.healed, false, 'not provisioned -> no-op');
    assert.equal(relay.diagnose().activeMemberships, 0, 'no membership');
  } finally {
    try { await relay?.close(); } catch { /* best effort */ }
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
