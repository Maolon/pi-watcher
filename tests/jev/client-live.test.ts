import test from 'node:test';
import assert from 'node:assert/strict';
import { JevHttpClient } from '../../src/jev/client.js';
import { JevAuthenticationError } from '../../src/jev/types.js';
import type { Basis, Probe } from '../../src/contracts/interfaces.js';

const liveApiKey = process.env.JEV_API_KEY || process.env.jev_api_key;

const liveBasis: Basis = {
  watchId: 'watch-live-001',
  generation: 1,
  missionRevision: 1,
  controlRevision: 1,
  observationSeq: 1,
  windowDigest: 'digest-live-001'
};

test('Live Jev: verifies real API connectivity and BERT-based classification (not an LLM)', { skip: !liveApiKey }, async () => {
  assert.ok(liveApiKey, 'JEV_API_KEY environment variable is present');

  const client = new JevHttpClient();

  const state = {
    mission: {
      objective: 'Run build and report on failure',
      scope: 'run-42 only',
      checkpointId: 'compile-and-test',
      requiredArtifacts: ['artifact-app'],
      requiresChecks: true,
      businessAcceptance: 'host'
    },
    current_target: {
      kind: 'run',
      sourceId: 'executor-local',
      taskId: 'build',
      runId: 'run-42',
      attemptId: 'attempt-1'
    },
    trusted_facts: {
      is_current_attempt: true,
      deadline_exceeded: false,
      consecutive_failed_attempts: 1
    },
    executor_delegation: 'May inspect and fix build scripts. May not change the API contract.',
    prior_effective_state: {
      stage: 'compile',
      status: 'running'
    },
    untrusted_evidence: [
      {
        evidenceId: 'ev-build-42-final',
        text: 'Compiler reports that the requested API does not exist. Need the host to choose which API contract to target.'
      }
    ],
    coverage: {
      truncated: false,
      source_gap: false
    }
  };

  const startTime = Date.now();
  const judgment = await client.evaluate(liveBasis, state);
  const elapsedMs = Date.now() - startTime;

  // 1. Model metadata verification
  assert.equal(judgment.model, 'jev-1.13.0');
  assert.equal(judgment.questionSet, 'watcher-q1');
  assert.equal(judgment.discarded, false);
  assert.ok(judgment.inputTokens > 0, `Expected inputTokens > 0, got ${judgment.inputTokens}`);

  // 2. All 6 probabilities must be normalized numbers between 0 and 1
  const keys = [
    'meaningful_progress',
    'unresolved_blocker',
    'needs_host_decision',
    'repeating_without_new_information',
    'claim_conflicts_with_evidence',
    'context_sufficient'
  ] as const;

  for (const key of keys) {
    const p = judgment.probabilities[key];
    assert.equal(typeof p, 'number', `Probability for ${key} must be a number`);
    assert.ok(p >= 0 && p <= 1, `Probability for ${key} (${p}) must be in [0, 1]`);
  }

  // 3. Semantic quality check on this explicit blocker scenario:
  // "Compiler reports that the requested API does not exist. Need the host to choose..."
  // Both unresolved_blocker and needs_host_decision should be high.
  assert.ok(
    judgment.probabilities.unresolved_blocker > 0.7,
    `Expected unresolved_blocker > 0.7, got ${judgment.probabilities.unresolved_blocker}`
  );
  assert.ok(
    judgment.probabilities.needs_host_decision > 0.7,
    `Expected needs_host_decision > 0.7, got ${judgment.probabilities.needs_host_decision}`
  );

  // 4. Verification that Jev behaves as a fast System 1 discriminator (BERT-style classification)
  // rather than a slow autoregressive LLM generating hundreds of reasoning tokens.
  console.log(`[Live Test] Jev evaluation roundtrip: ${elapsedMs}ms, tokens: ${judgment.inputTokens}`);
  console.log(`[Live Test] Probabilities:`, judgment.probabilities);

  assert.ok(elapsedMs < 5000, `Expected BERT roundtrip under 5s, got ${elapsedMs}ms`);
});

test('Live Jev: chooseProbe candidate selection with real service', { skip: !liveApiKey }, async () => {
  const client = new JevHttpClient();

  const probes: Probe[] = [
    {
      probeId: 'probe-read-compiler-log',
      revision: 1,
      target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-42', attemptId: 'attempt-1' },
      kind: 'read-log-delta',
      scopeId: 'scope-build-logs',
      expiresAt: new Date(Date.now() + 60000).toISOString(),
      timeoutMs: 5000,
      maxBytes: 8192
    },
    {
      probeId: 'probe-check-artifacts',
      revision: 1,
      target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-42', attemptId: 'attempt-1' },
      kind: 'read-artifact-manifest',
      scopeId: 'scope-artifacts',
      expiresAt: new Date(Date.now() + 60000).toISOString(),
      timeoutMs: 3000,
      maxBytes: 4096
    }
  ];

  const state = {
    gap: 'Build log truncated near compile error',
    current_status: 'failed'
  };

  const choice = await client.chooseProbe(liveBasis, state, probes);
  console.log(`[Live Test] Jev chooseProbe result:`, choice);

  // Must either choose one of the valid probe IDs or 'none'
  const validChoices = new Set(['probe-read-compiler-log', 'probe-check-artifacts', 'none']);
  assert.ok(validChoices.has(choice), `Choice '${choice}' must be one of the candidates or 'none'`);
});

test('Live Jev: real authentication rejection with invalid key', async () => {
  const badClient = new JevHttpClient({
    apiKey: 'apikey_invalid_fake_key_99999999999999999999'
  });

  await assert.rejects(
    async () => {
      await badClient.evaluate(liveBasis, { ping: 'pong' });
    },
    (err: any) => {
      assert.ok(err instanceof JevAuthenticationError);
      assert.ok(err.status === 401 || err.status === 403);
      return true;
    }
  );
});
