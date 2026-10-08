import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { PiRegistryJudge, type PiClassifierRegistry, type PiClassifierModel } from '../../src/jev/pi-registry.js';
import { jevConsentState, setStoredJevConsent } from '../../src/jev/consent.js';
import type { Basis, Probe } from '../../src/contracts/interfaces.js';

const basis: Basis = { watchId: 'w-1', generation: 1, missionRevision: 1, controlRevision: 1, observationSeq: 1, windowDigest: 'd' };
const signal = new AbortController().signal;

function fakeRegistry(available: PiClassifierModel[], answer: (questions: Record<string, unknown>) => Record<string, unknown>, opts: { error?: string } = {}) {
  const calls: Array<{ model: PiClassifierModel; state: unknown; questions: Record<string, unknown> }> = [];
  const registry: PiClassifierRegistry = {
    async getAvailableOfType() { return available; },
    async classify(model, context) {
      calls.push({ model, state: context.state, questions: context.questions });
      if (opts.error) return { provider: model.provider, model: model.id, answers: {}, stopReason: 'error', errorMessage: opts.error };
      return { provider: model.provider, model: model.id, answers: answer(context.questions) as never, usage: { input: 321 }, stopReason: 'stop' };
    }
  };
  return { registry, calls };
}

const allBool = (questions: Record<string, unknown>) =>
  Object.fromEntries(Object.keys(questions).map(k => [k, { type: 'bool', probability: k === 'unresolved_blocker' ? 0.9 : 0.2 }]));

test('pi-registry: prefers TypeSafe direct, maps bool probabilities, sanitizes state, records provider/model', async () => {
  const { registry, calls } = fakeRegistry([
    { provider: 'openrouter', id: 'typesafe/jev-1.13' },
    { provider: 'typesafe', id: 'jev-latest' }
  ], allBool);
  const judge = new PiRegistryJudge(() => registry, undefined);
  assert.deepEqual(await judge.ready(), { ready: true, model: 'typesafe/jev-latest' });
  const j = await judge.evaluate(basis, { log: 'token=apikey_0000fake0000fake0000fake0000fake000 failed' }, signal);
  assert.equal(j.model, 'typesafe/jev-latest');
  assert.equal(j.probabilities.unresolved_blocker, 0.9);
  assert.equal(j.inputTokens, 321);
  assert.equal(Object.keys(calls[0].questions).length, 6);
  assert.ok(Object.values(calls[0].questions).every(q => (q as { type: string }).type === 'bool'));
  assert.doesNotMatch(JSON.stringify(calls[0].state), /apikey_0000fake/);
});

test('pi-registry: falls back to a gateway provider; PI_WATCHER_JEV_MODEL override is honored', async () => {
  const { registry } = fakeRegistry([{ provider: 'openrouter', id: 'typesafe/jev-1.13' }, { provider: 'opencode', id: 'jev-1.13' }], allBool);
  assert.equal((await new PiRegistryJudge(() => registry, undefined).ready()).model, 'openrouter/typesafe/jev-1.13');
  assert.equal((await new PiRegistryJudge(() => registry, 'opencode/jev-1.13').ready()).model, 'opencode/jev-1.13');
});

test('pi-registry: no credentials -> not ready with an actionable reason; picks up credentials added later', async () => {
  let available: PiClassifierModel[] = [];
  let t = 0;
  const registry: PiClassifierRegistry = {
    async getAvailableOfType() { return available; },
    async classify() { throw new Error('unused'); }
  };
  const judge = new PiRegistryJudge(() => registry, undefined, () => t);
  const r = await judge.ready();
  assert.equal(r.ready, false);
  assert.match(String(r.reason), /\/login/);
  available = [{ provider: 'typesafe', id: 'jev-latest' }]; // user ran /login mid-session
  t += 61_000;
  assert.equal((await judge.ready()).ready, true);
});

test('pi-registry: classifier error result surfaces as a failed evaluation (never a fake score)', async () => {
  const { registry } = fakeRegistry([{ provider: 'typesafe', id: 'jev-latest' }], allBool, { error: '401 Unauthorized' });
  const judge = new PiRegistryJudge(() => registry, undefined);
  await assert.rejects(() => judge.evaluate(basis, {}, signal), /401 Unauthorized/);
});

test('pi-registry: chooseProbe only returns a listed probe id, else none', async () => {
  const probes = [{ probeId: 'p-1', kind: 'read-status', scopeId: 's', timeoutMs: 1000 }] as unknown as Probe[];
  const ok = fakeRegistry([{ provider: 'typesafe', id: 'jev-latest' }], () => ({ probe_choice: { type: 'choice', choice: 'p-1' } }));
  assert.equal(await new PiRegistryJudge(() => ok.registry, undefined).chooseProbe(basis, {}, probes, signal), 'p-1');
  const bogus = fakeRegistry([{ provider: 'typesafe', id: 'jev-latest' }], () => ({ probe_choice: { type: 'choice', choice: 'rm -rf' } }));
  assert.equal(await new PiRegistryJudge(() => bogus.registry, undefined).chooseProbe(basis, {}, probes, signal), 'none');
});

test('consent: stored setting via /watcher jev consent; JEV_CONSENT env overrides it', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pw-consent-'));
  const saved = { cfg: process.env.PI_WATCHER_CONFIG, env: process.env.JEV_CONSENT };
  try {
    process.env.PI_WATCHER_CONFIG = path.join(dir, 'pi-watcher.json');
    delete process.env.JEV_CONSENT;
    assert.deepEqual(jevConsentState(), { granted: false, source: 'none' });
    setStoredJevConsent(true);
    assert.deepEqual(jevConsentState(), { granted: true, source: 'stored' });
    assert.equal(fs.statSync(process.env.PI_WATCHER_CONFIG).mode & 0o777, 0o600);
    process.env.JEV_CONSENT = '0';
    assert.deepEqual(jevConsentState(), { granted: false, source: 'env' });
    delete process.env.JEV_CONSENT;
    setStoredJevConsent(false);
    assert.deepEqual(jevConsentState(), { granted: false, source: 'stored' });
  } finally {
    if (saved.cfg === undefined) delete process.env.PI_WATCHER_CONFIG; else process.env.PI_WATCHER_CONFIG = saved.cfg;
    if (saved.env === undefined) delete process.env.JEV_CONSENT; else process.env.JEV_CONSENT = saved.env;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
