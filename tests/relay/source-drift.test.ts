/**
 * Regression: an existing source store's channel config predates localTrust,
 * and relay's channel digest invariant makes opening with the new config report invalid_state -> relay=null -> local-display forever.
 * WatcherRelaySource.open now auto-recovers from a "channel-drift invalid_state" (not an authority mismatch):
 * resetSource quarantines the old store (.quarantine, auditable) -> rebuild with the new config (localTrust).
 * An authority mismatch (different owner/publisher token) is a security signal: thrown as-is, never auto-reset.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createSource } from '@maolon/pi-relay/source';
import { secret as relaySecret } from '@maolon/pi-relay/protocol';
import { WatcherRelaySource, WATCHER_TYPE_MANIFESTS } from '../../src/relay/managed.js';

const legacyChannel = {
  id: 'W',
  types: WATCHER_TYPE_MANIFESTS as never[],
  allowedModes: ['display', 'resume'],
  maxAutoTargets: 4
  // no localTrust - the legacy config
};

test('relay source open: channel drift (same authority, new localTrust) -> auto quarantine + recreate; authority mismatch -> rethrow', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-drift-')));
  const relayHome = path.join(tmp, 'rh');
  const watcherRoot = path.join(tmp, 'wr');

  const ot1 = relaySecret(), pt1 = relaySecret();
  const ot2 = relaySecret(), pt2 = relaySecret();
  try {
    // 1. Mint an "old world" store (same authority: token = ot1/pt1; no localTrust)
    const legacy = await createSource({
      version: 1, sourceId: 'pi-watcher', realm: 'local', home: relayHome,
      ownerToken: ot1, publisherTokens: { W: pt1 },
      channels: [legacyChannel]
    } as never, {});
    await (legacy as unknown as { close: () => Promise<void> }).close();

    // 2. Reproduce the drift error in place: same token + localTrust -> SDK reports invalid_state directly (channel digest invariant)
    await assert.rejects(
      createSource({
        version: 1, sourceId: 'pi-watcher', realm: 'local', home: relayHome,
        ownerToken: ot1, publisherTokens: { W: pt1 },
        channels: [{ ...legacyChannel, localTrust: true }]
      } as never, {}),
      (e: unknown) => (e as { code?: string }).code === 'invalid_state'
    );

    // 3. WatcherRelaySource.open (secrets share the old store's authority) -> automatic recovery:
    //    quarantine the old store -> rebuild succeeds with the new config (including localTrust)
    fs.mkdirSync(watcherRoot, { recursive: true });
    fs.writeFileSync(path.join(watcherRoot, 'relay-secrets.json'), JSON.stringify({
      ownerToken: ot1, publisherToken: pt1
    }));
    const recovered = await WatcherRelaySource.open(watcherRoot, { relayHome });
    assert.ok(recovered, 'open must succeed after channel-drift auto-recovery');
    const quarantine = path.join(relayHome, 'sources', '.quarantine');
    assert.ok(fs.existsSync(quarantine), 'legacy store must be quarantined (auditable), not deleted');
    await recovered.close();

    // 4. authority mismatch (different token) -> thrown as-is, never auto-reset (the store now belongs to ot1; the ot2 key must not be able to rebuild it)
    const watcherRoot2 = path.join(tmp, 'wr2');
    fs.mkdirSync(watcherRoot2, { recursive: true });
    fs.writeFileSync(path.join(watcherRoot2, 'relay-secrets.json'), JSON.stringify({
      ownerToken: ot2, publisherToken: pt2
    }));
    await assert.rejects(
      WatcherRelaySource.open(watcherRoot2, { relayHome }),
      (e: unknown) => (e as { name?: string }).name === 'SourceAuthorityMismatch'
    );
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('deriveSourceId: per-root, stable, relay-charset safe, cross-root distinct', async () => {
  const { deriveSourceId } = await import('../../src/relay/managed.js');
  const a = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-sid-')));
  const b = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-sid-')));
  const a1 = deriveSourceId(a), a2 = deriveSourceId(a);
  assert.equal(a1, a2, 'stable for same root');
  assert.notEqual(a1, deriveSourceId(b), 'distinct across roots');
  assert.match(a1, /^pi-watcher:[a-z0-9][a-z0-9-]*-[0-9a-f]{8}$/, 'slug-hash format');
  assert.match(a1, /^[A-Za-z0-9][A-Za-z0-9._:-]*$/, 'satisfies relay bind-request sourceId charset');
  // same-named directory at a different path -> distinguished by hash
  const c = path.join(b, 'dupname'); const d = path.join(a, 'dupname');
  fs.mkdirSync(c, { recursive: true }); fs.mkdirSync(d, { recursive: true });
  assert.notEqual(deriveSourceId(c), deriveSourceId(d), 'same basename different path must not collide');
  fs.rmSync(a, { recursive: true, force: true }); fs.rmSync(b, { recursive: true, force: true });
});
