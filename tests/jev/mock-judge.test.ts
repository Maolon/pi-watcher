import test from 'node:test';
import assert from 'node:assert/strict';
import { MockJudge } from '../../src/jev/mock.js';
import type { Basis, Probe } from '../../src/contracts/interfaces.js';

const mockBasis: Basis = {
  watchId: 'watch-1',
  generation: 1,
  missionRevision: 1,
  controlRevision: 1,
  observationSeq: 1,
  windowDigest: 'digest-1'
};

test('MockJudge: returns default probabilities when not overridden', async () => {
  const judge = new MockJudge();
  const judgment = await judge.evaluate(mockBasis, { state: 'test' });

  assert.equal(judgment.model, 'jev-1.13.0');
  assert.equal(judgment.questionSet, 'watcher-q1');
  assert.equal(judgment.probabilities.meaningful_progress, 0.5);
  assert.equal(judgment.probabilities.context_sufficient, 0.95);
  assert.equal(judgment.discarded, false);
  assert.equal(judge.evaluateCallCount, 1);
});

test('MockJudge: supports overriding probabilities dynamically', async () => {
  const judge = new MockJudge({
    evaluateFn: (_basis, state: any) => {
      if (state.hasBlocker) {
        return { unresolved_blocker: 0.95, meaningful_progress: 0.05 };
      }
      return {};
    }
  });

  const judgment = await judge.evaluate(mockBasis, { hasBlocker: true });
  assert.equal(judgment.probabilities.unresolved_blocker, 0.95);
  assert.equal(judgment.probabilities.meaningful_progress, 0.05);
});

test('MockJudge: selects candidate probe or none', async () => {
  const probes: Probe[] = [
    {
      probeId: 'probe-read-logs',
      revision: 1,
      target: { kind: 'run', sourceId: 'src-1', taskId: 't-1', runId: 'r-1', attemptId: 'a-1' },
      kind: 'read-log-delta',
      scopeId: 'scope-1',
      expiresAt: new Date(Date.now() + 60000).toISOString(),
      timeoutMs: 5000,
      maxBytes: 1024
    }
  ];

  const judge = new MockJudge({ probeChoice: 'probe-read-logs' });
  const choice = await judge.chooseProbe(mockBasis, {}, probes);
  assert.equal(choice, 'probe-read-logs');

  judge.setProbeChoice('none');
  const fallback = await judge.chooseProbe(mockBasis, {}, probes);
  assert.equal(fallback, 'none');
});
