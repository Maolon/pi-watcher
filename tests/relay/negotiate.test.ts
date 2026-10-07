import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { negotiateRelay, MANAGED_REQUIRED_FEATURES } from '../../src/relay/negotiate.js';

test('negotiate: default probe → unavailable → local-display (no silent fallback to direct wake)', async () => {
  const n = await negotiateRelay();
  assert.equal(n.status, 'unavailable');
  assert.equal(n.transport, 'local-display');
  assert.deepEqual(n.requiredFeatures, [...MANAGED_REQUIRED_FEATURES]);
  assert.ok(n.detail.includes('not enabled'), 'detail names the enable path, not a module-presence guess');
});

test('negotiate: full 1.2 feature set → managed path enabled', async () => {
  const n = await negotiateRelay({
    probeRelay: async () => ({
      present: true,
      protocolMinor: 2,
      features: [...MANAGED_REQUIRED_FEATURES]
    })
  });
  assert.equal(n.status, 'managed-1-2');
  assert.equal(n.transport, 'relay');
});

test('negotiate: relay present without managed features → legacy display only, auto-resume disabled', async () => {
  const n = await negotiateRelay({
    probeRelay: async () => ({ present: true, protocolMinor: 1, features: ['source.managed-receipts'] })
  });
  assert.equal(n.status, 'legacy-1-1');
  assert.equal(n.transport, 'local-display');
  assert.ok(n.detail.includes('auto-resume path disabled'));
});

test('negotiate: probe throw → unavailable (never guesses relay presence)', async () => {
  const n = await negotiateRelay({
    probeRelay: async () => {
      throw new Error('boom');
    }
  });
  assert.equal(n.status, 'unavailable');
  assert.ok(n.detail.includes('boom'));
});
