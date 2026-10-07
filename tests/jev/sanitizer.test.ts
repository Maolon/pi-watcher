import test from 'node:test';
import assert from 'node:assert/strict';
import { sanitizeText, sanitizeJson } from '../../src/jev/sanitizer.js';

test('sanitizer: removes credentials from text strings', () => {
  const text = 'Log: failed with apikey_0000fake0000fake0000fake0000fake000 and sk-abcdef1234567890abcdef and Bearer eyJhbGciOiJIUzI1NiJ9';
  const sanitized = sanitizeText(text);

  assert.doesNotMatch(sanitized, /apikey_/i);
  assert.doesNotMatch(sanitized, /sk-abcdef/i);
  assert.doesNotMatch(sanitized, /Bearer eyJ/i);
  assert.match(sanitized, /\[REDACTED_CREDENTIAL\]/);
});

test('sanitizer: removes publisher and binding handles', () => {
  const text = 'Routing event to pub-998877665544 and bnd-112233445566';
  const sanitized = sanitizeText(text);

  assert.doesNotMatch(sanitized, /pub-998877665544/);
  assert.doesNotMatch(sanitized, /bnd-112233445566/);
  assert.match(sanitized, /\[REDACTED_CREDENTIAL\]/);
});

test('sanitizer: sanitizes deeply nested JSON structures', () => {
  const payload = {
    task: 'build',
    config: {
      apiKey: 'apikey_secret123456',
      token: 'some-bearer-token',
      publisherHandle: 'pub-test123456',
      normalField: 'all good'
    },
    logs: [
      'Normal log line',
      'Leak: token=secret-token-value-here'
    ]
  };

  const sanitized = sanitizeJson(payload) as Record<string, any>;

  assert.equal(sanitized.config.apiKey, '[REDACTED_CREDENTIAL]');
  assert.equal(sanitized.config.token, '[REDACTED_CREDENTIAL]');
  assert.equal(sanitized.config.publisherHandle, '[REDACTED_CREDENTIAL]');
  assert.equal(sanitized.config.normalField, 'all good');
  assert.doesNotMatch(sanitized.logs[1], /secret-token-value/);
});
