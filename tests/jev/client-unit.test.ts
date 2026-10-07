import test from 'node:test';
import assert from 'node:assert/strict';
import { JevHttpClient } from '../../src/jev/client.js';
import {
  JevAuthenticationError,
  JevUnprocessableError,
  JevServerError,
  JevTimeoutError,
  type JevSystemOneResponse
} from '../../src/jev/types.js';
import type { Basis, Probe } from '../../src/contracts/interfaces.js';

const mockBasis: Basis = {
  watchId: 'watch-unit-1',
  generation: 1,
  missionRevision: 1,
  controlRevision: 1,
  observationSeq: 1,
  windowDigest: 'digest-unit-1'
};

test('JevHttpClient: throws JevAuthenticationError if no API key is provided', async () => {
  const client = new JevHttpClient({ apiKey: '' });
  await assert.rejects(
    async () => {
      await client.evaluate(mockBasis, {});
    },
    (err: any) => {
      assert.ok(err instanceof JevAuthenticationError);
      assert.equal(err.status, 401);
      return true;
    }
  );
});

test('JevHttpClient: parses standard answers into probabilities and records tokens', async () => {
  let capturedHeaders: HeadersInit | undefined;
  let capturedBody: any;

  const mockResponse: JevSystemOneResponse = {
    model: 'jev-1.13.0',
    answers: {
      meaningful_progress: { type: 'noul', noul: 0.15 },
      unresolved_blocker: { type: 'noul', noul: 0.88 },
      needs_host_decision: { type: 'noul', noul: 0.85 },
      repeating_without_new_information: { type: 'noul', noul: 0.1 },
      claim_conflicts_with_evidence: { type: 'noul', noul: 0.05 },
      context_sufficient: { type: 'noul', noul: 0.9 }
    },
    usage: {
      input_tokens: 1100,
      output_tokens: 120
    }
  };

  const fetchMock: typeof fetch = async (_url, init) => {
    capturedHeaders = init?.headers;
    capturedBody = JSON.parse(init?.body as string);
    return new Response(JSON.stringify(mockResponse), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  };

  const client = new JevHttpClient({
    apiKey: 'apikey_test_unit_123',
    fetchFn: fetchMock
  });

  const judgment = await client.evaluate(mockBasis, { someKey: 'someValue' });

  assert.equal(judgment.model, 'jev-1.13.0');
  assert.equal(judgment.questionSet, 'watcher-q1');
  assert.equal(judgment.probabilities.meaningful_progress, 0.15);
  assert.equal(judgment.probabilities.unresolved_blocker, 0.88);
  assert.equal(judgment.probabilities.context_sufficient, 0.9);
  assert.equal(judgment.inputTokens, 1100);

  // Check stats
  assert.equal(client.stats.totalRequests, 1);
  assert.equal(client.stats.totalInputTokens, 1100);
  assert.equal(client.stats.totalOutputTokens, 120);

  // Check headers
  assert.equal((capturedHeaders as Record<string, string>)['Authorization'], 'Bearer apikey_test_unit_123');

  // Check model in body
  assert.equal(capturedBody.model, 'jev-1.13.0');
});

test('JevHttpClient: does not retry on 401 or 403', async () => {
  let callCount = 0;
  const fetchMock: typeof fetch = async () => {
    callCount++;
    return new Response('Unauthorized', { status: 401 });
  };

  const client = new JevHttpClient({
    apiKey: 'apikey_bad_key',
    fetchFn: fetchMock,
    maxRetries: 2
  });

  await assert.rejects(
    async () => {
      await client.evaluate(mockBasis, {});
    },
    (err: any) => {
      assert.ok(err instanceof JevAuthenticationError);
      return true;
    }
  );

  // Must only call once (no retries)
  assert.equal(callCount, 1);
});

test('JevHttpClient: does not retry on 422 unprocessable entity', async () => {
  let callCount = 0;
  const fetchMock: typeof fetch = async () => {
    callCount++;
    return new Response(JSON.stringify({ error: 'invalid schema' }), { status: 422 });
  };

  const client = new JevHttpClient({
    apiKey: 'apikey_test',
    fetchFn: fetchMock,
    maxRetries: 2
  });

  await assert.rejects(
    async () => {
      await client.evaluate(mockBasis, {});
    },
    (err: any) => {
      assert.ok(err instanceof JevUnprocessableError);
      return true;
    }
  );

  assert.equal(callCount, 1);
});

test('JevHttpClient: does not retry on 400/404 client errors (design 5.6)', async () => {
  for (const status of [400, 404]) {
    let callCount = 0;
    const fetchMock: typeof fetch = async () => {
      callCount++;
      return new Response('bad request', { status });
    };
    const client = new JevHttpClient({ apiKey: 'apikey_test', fetchFn: fetchMock, maxRetries: 2 });
    await assert.rejects(() => client.evaluate(mockBasis, {}), new RegExp(`status ${status}`));
    assert.equal(callCount, 1, `status ${status} must not be retried`);
  }
});

test('JevHttpClient: retries on 500 up to maxRetries then throws JevServerError', async () => {
  let callCount = 0;
  const fetchMock: typeof fetch = async () => {
    callCount++;
    return new Response('Internal Server Error', { status: 500 });
  };

  const client = new JevHttpClient({
    apiKey: 'apikey_test',
    fetchFn: fetchMock,
    maxRetries: 2
  });

  await assert.rejects(
    async () => {
      await client.evaluate(mockBasis, {});
    },
    (err: any) => {
      assert.ok(err instanceof JevServerError);
      assert.equal(err.status, 500);
      return true;
    }
  );

  // Initial + 2 retries = 3 calls
  assert.equal(callCount, 3);
});

test('JevHttpClient: times out when request exceeds timeoutMs', async () => {
  const fetchMock: typeof fetch = async (_url, init) => {
    return new Promise((_resolve, reject) => {
      init?.signal?.addEventListener('abort', () => {
        reject(new Error('Aborted by signal'));
      });
    });
  };

  const client = new JevHttpClient({
    apiKey: 'apikey_test',
    fetchFn: fetchMock,
    timeoutMs: 50,
    maxRetries: 0
  });

  await assert.rejects(
    async () => {
      await client.evaluate(mockBasis, {});
    },
    (err: any) => {
      assert.ok(err instanceof JevTimeoutError);
      return true;
    }
  );
});

test('JevHttpClient: chooseProbe selects valid candidate or none', async () => {
  const probes: Probe[] = [
    {
      probeId: 'probe-alpha',
      revision: 1,
      target: { kind: 'run', sourceId: 'src', taskId: 'tsk', runId: 'run', attemptId: 'att' },
      kind: 'read-status',
      scopeId: 'scope-1',
      expiresAt: new Date(Date.now() + 60000).toISOString(),
      timeoutMs: 3000,
      maxBytes: 2048
    }
  ];

  const fetchMock: typeof fetch = async () => {
    return new Response(JSON.stringify({
      model: 'jev-1.13.0',
      answers: {
        probe_choice: { type: 'choice', choice: 'probe-alpha' }
      },
      usage: { input_tokens: 500, output_tokens: 10 }
    }), { status: 200 });
  };

  const client = new JevHttpClient({
    apiKey: 'apikey_test',
    fetchFn: fetchMock
  });

  const selected = await client.chooseProbe(mockBasis, {}, probes);
  assert.equal(selected, 'probe-alpha');
});
