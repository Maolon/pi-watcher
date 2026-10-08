/**
 * User consent for sending sanitized evidence windows to Jev (design 10.2: a consent separate
 * from the watch profile and from relay grants). Granted only by the user: `JEV_CONSENT=1`, or
 * the `/watcher jev consent on` command, which persists it here. Never settable from the tool.
 */

import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

export function consentFilePath(): string {
  return process.env?.PI_WATCHER_CONFIG ?? path.join(os.homedir(), '.pi', 'agent', 'pi-watcher.json');
}

interface ConfigFile { jevConsent?: boolean; jevConsentUpdatedAt?: string }

function readConfig(): ConfigFile {
  try {
    return JSON.parse(fs.readFileSync(consentFilePath(), 'utf8')) as ConfigFile;
  } catch {
    return {};
  }
}

/** Where the effective consent comes from. The environment wins over the stored setting. */
export function jevConsentState(): { granted: boolean; source: 'env' | 'stored' | 'none' } {
  const env = process.env?.JEV_CONSENT;
  if (env === '1') return { granted: true, source: 'env' };
  if (env === '0') return { granted: false, source: 'env' };
  const stored = readConfig().jevConsent;
  return stored === true ? { granted: true, source: 'stored' } : { granted: false, source: stored === false ? 'stored' : 'none' };
}

export function setStoredJevConsent(granted: boolean): void {
  const file = consentFilePath();
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const next: ConfigFile = { ...readConfig(), jevConsent: granted, jevConsentUpdatedAt: new Date().toISOString() };
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(next, null, 2) + '\n', { mode: 0o600 });
  fs.renameSync(tmp, file);
}
