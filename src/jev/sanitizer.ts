/**
 * Credential sanitizer for Jev requests.
 *
 * Enforces Invariant I11 & AT-065:
 * Ensures credentials (API keys, tokens, publisher handles, secrets) are NEVER
 * sent outbound to Jev or any external API.
 */

import type { Json } from '../contracts/interfaces.js';

const CREDENTIAL_PATTERNS = [
  /apikey_[a-zA-Z0-9_]+/gi,
  /sk-[a-zA-Z0-9_\-]{16,}/gi,
  /bearer\s+[a-zA-Z0-9_\-\.]+/gi,
  /pub-[a-zA-Z0-9_\-]{8,}/gi,
  /bnd-[a-zA-Z0-9_\-]{8,}/gi,
  /(?:password|secret|token|api[_-]?key)\s*[:=]\s*["']?[^\s"']+["']?/gi
];

const SENSITIVE_KEY_NAMES = new Set([
  'apikey',
  'api_key',
  'secret',
  'password',
  'token',
  'authorization',
  'publisherhandle',
  'publisher_handle',
  'privatekey',
  'private_key'
]);

export function sanitizeText(text: string): string {
  let sanitized = text;
  for (const pattern of CREDENTIAL_PATTERNS) {
    sanitized = sanitized.replace(pattern, '[REDACTED_CREDENTIAL]');
  }
  return sanitized;
}

export function sanitizeJson(value: Json): Json {
  if (value === null || typeof value === 'boolean' || typeof value === 'number') {
    return value;
  }
  if (typeof value === 'string') {
    return sanitizeText(value);
  }
  if (Array.isArray(value)) {
    return value.map(item => sanitizeJson(item));
  }
  if (typeof value === 'object') {
    const result: Record<string, Json> = {};
    for (const [k, v] of Object.entries(value)) {
      const normalizedKey = k.toLowerCase().replace(/[-_]/g, '');
      if (SENSITIVE_KEY_NAMES.has(normalizedKey)) {
        result[k] = '[REDACTED_CREDENTIAL]';
      } else {
        result[k] = sanitizeJson(v);
      }
    }
    return result;
  }
  return value;
}
