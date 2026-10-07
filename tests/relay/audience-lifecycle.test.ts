/**
 * Local-domain audience lifecycle:
 * 1) provision sentinel -> source store persists valid_until = 9007199254740991 (24h wall removed);
 * 2) close() -> scope closed + audience closed (idempotent); after closing, publish is rejected by relay (terminal state, no more delivery).
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
import { WatcherRelaySource, LOCAL_AUDIENCE_VALID_UNTIL_MS } from '../../src/relay/managed.js';
import { createTarget } from '@maolon/pi-relay/target';
import { newId } from '@maolon/pi-relay/protocol';

const SENTINEL = 9007199254740991;

function audienceRow(relayHome: string, sourceId: string): { valid_until: number; state: string } {
  const dir = path.join(relayHome, 'sources', createHash('sha256').update(sourceId).digest('hex'));
  const db = new Database(path.join(dir, 'source.sqlite'), { readonly: true });
  try {
    return db.prepare('SELECT valid_until, state FROM managed_audiences WHERE audience_ref=?').get('watcher-main') as { valid_until: number; state: string };
  } finally {
    db.close();
  }
}

test('audience lifecycle: local-domain provision sentinel -> valid_until=9007199254740991; close idempotently closes scope+audience', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-aud-life-')));
  const relayHome = path.join(tmp, 'relay-home');
  let relay: WatcherRelaySource | null = null;
  let target: Awaited<ReturnType<typeof createTarget>> | null = null;
  try {
    // Real closed-loop prerequisites: source + declaration + invite + target binding (provision freezing the route set needs an active membership)
    relay = await WatcherRelaySource.open(path.join(tmp, 'watcher-relay'), { relayHome, realm: 'local' });
    relay.ensureConsumerDeclaration();
    const { inviteFile } = relay.createInvite();
    target = await createTarget({ home: relayHome, realm: 'local', fingerprint: createHash('sha256').update('pw-aud-life').digest('hex') });
    const invite = JSON.parse(fs.readFileSync(inviteFile, 'utf8'));
    const bindingId = await target.bind(invite, { resume: true, operationId: newId('op') });
    assert.ok(relay.hasActiveMembership());

    // 1) provision: sentinel persisted (no longer 24h)
    assert.ok(relay.ready() === false, 'not ready before provision');
    relay.provision();
    assert.ok(relay.ready(), 'ready after provision');
    const row = audienceRow(relayHome, 'pi-watcher');
    assert.equal(row.valid_until, SENTINEL, `local-domain audience sentinel validity (actual ${row.valid_until})`);
    assert.equal(LOCAL_AUDIENCE_VALID_UNTIL_MS, SENTINEL, 'watcher constant aligned with pi-relay FOREVER');
    assert.equal(row.state, 'open');

    // 2) Protocol-level terminal rejection (store stays open): after audience closed, publish must be invalid_state, not silent
    (relay.host.managed as { closeAudience: (p: unknown, ref: string) => unknown }).closeAudience({ kind: 'owner' }, 'watcher-main');
    const envelope = {
      schemaVersion: 1, envelopeId: 'att-life-test', episodeId: 'ep-1', episodeRevision: 1,
      watchId: 'w-life', generation: 1, missionRevision: 1, controlRevision: 1, ownerBindingEpoch: 1,
      target: { kind: 'run', sourceId: 'x', taskId: 't', runId: 'r', attemptId: 'a' },
      reasonCode: 'task.failed', summary: 'closed-channel probe', evidenceRefs: [],
      requiredNextStep: 'inspect-current-watch-before-acting',
      occurredAt: new Date().toISOString(),
      validUntil: new Date(Date.now() + 60_000).toISOString()
    };
    await assert.rejects(
      () => relay!.publish(Buffer.from(JSON.stringify(envelope), 'utf8')),
      (e: Error & { code?: string }) => e.code === 'invalid_state' || /not valid in this state/i.test(e.message),
      'publish on a closed audience must be explicitly rejected (invalid_state)'
    );

    // 3) close(): scope closed + idempotent; audience stays closed
    await relay.close();
    const closed1 = audienceRow(relayHome, 'pi-watcher');
    assert.equal(closed1.state, 'closed', 'audience explicitly closed');
    await assert.doesNotReject(() => relay!.close(), 'repeated close is idempotent');
  } finally {
    try { await target?.close(); } catch { /* best effort */ }
    try { await relay?.close(); } catch { /* best effort */ }
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
