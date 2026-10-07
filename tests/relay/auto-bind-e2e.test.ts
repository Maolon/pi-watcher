/**
 * Joint acceptance:
 *
 * s4.1 Standing without invite: channel W declares localTrust -> watcher session_start sends v2
 *      kind:'local' -> relay registerAutoBindListener bindLocal -> standing armed ->
 *      primary ensureRelayReady provision -> relay projection flips to 'relay'.
 * s4.4 Multiple concurrent standing sessions (relay ruling Q1: standing is exempt from cross-target overlap):
 *      a second session (new target fingerprint) runs the handshake again -> each holds its own standing binding, without shadowing each other.
 * The relay home is isolated throughout (never touches ~/.pi/relay).
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import watcherExtension, { type PiExtensionAPI, type PiTool, type PiToolCallContext, type PiToolResultEvent } from '../../src/pi-extension.js';

const require_ = createRequire(import.meta.url);
// The pi-relay exports map does not list dist subpaths; derive the package root from the exported './target' entry and load via file-URL directly (tests only)
const relayPkgDir = path.resolve(path.dirname(require_.resolve('@maolon/pi-relay/target')), '..', '..');
const { registerAutoBindListener } = await import(pathToFileURL(path.join(relayPkgDir, 'dist', 'pi', 'auto-bind.js')).href) as {
  registerAutoBindListener: (
    bus: { on(event: string, l: (d: unknown) => void): () => void; emit(event: string, d: unknown): void } | undefined,
    getHost: () => unknown,
    getSessionFile: () => string | undefined
  ) => () => void;
};
const { createTarget } = await import('@maolon/pi-relay/target') as {
  createTarget: (o: { home: string; realm: string; fingerprint: string }) => Promise<TargetStub>;
};
interface TargetStub {
  bind: (invite: unknown, o: { resume: boolean; operationId: string }) => Promise<string>;
  bindLocal: (o: { sourceId: string; channelId: string }) => Promise<{ bindingId: string; armed: boolean }>;
  core: {
    binding: (id: string) => { proposal: { policy: Record<string, { model: string }> }; revision: number };
    control: (id: string, o: unknown) => void;
    status: () => { bindings: Array<{ bindingId: string; state: string; grants: unknown[] }> };
  };
  close: () => Promise<void>;
}

/** Minimal event bus (corresponds to pi.events). */
class Bus {
  private listeners = new Map<string, Array<(d: unknown) => void>>();
  on(event: string, l: (d: unknown) => void): () => void {
    const arr = this.listeners.get(event) ?? [];
    arr.push(l); this.listeners.set(event, arr);
    return () => { this.listeners.set(event, (this.listeners.get(event) ?? []).filter(x => x !== l)); };
  }
  emit(event: string, d: unknown): void {
    for (const l of [...(this.listeners.get(event) ?? [])]) l(d);
  }
}

class FakePi implements PiExtensionAPI {
  tools: PiTool[] = [];
  commands: Array<{ name: string; def: { description: string; handler: (args: string, ctx: PiToolCallContext) => Promise<void> | void } }> = [];
  startHandlers: Array<(event: unknown, ctx: PiToolCallContext) => Promise<void> | void> = [];
  shutdownHandlers: Array<(event: unknown, ctx: PiToolCallContext) => Promise<void> | void> = [];
  toolResultHandlers: Array<(event: PiToolResultEvent, ctx: PiToolCallContext) => Promise<void> | void> = [];
  constructor(public events?: { on(event: string, l: (d: unknown) => void): (() => void) | void; emit(event: string, d: unknown): void }) {}
  registerTool(tool: PiTool): void { this.tools.push(tool); }
  registerCommand(name: string, def: { description: string; handler: (args: string, ctx: PiToolCallContext) => Promise<void> | void }): void { this.commands.push({ name, def }); }
  on(event: 'session_start' | 'session_shutdown', handler: (event: unknown, ctx: PiToolCallContext) => Promise<void> | void): void;
  on(event: 'tool_result', handler: (event: PiToolResultEvent, ctx: PiToolCallContext) => Promise<void> | void): void;
  on(event: 'tool_call', handler: (event: { toolName: string; toolCallId: string; input?: { cmd?: string } }, ctx: PiToolCallContext) => Promise<void> | void): void;
  on(event: string, handler: (event: never, ctx: PiToolCallContext) => Promise<void> | void): void {
    if (event === 'tool_result') this.toolResultHandlers.push(handler as unknown as (event: PiToolResultEvent, ctx: PiToolCallContext) => Promise<void> | void);
    else if (event === 'session_start') this.startHandlers.push(handler as (event: unknown, ctx: PiToolCallContext) => Promise<void> | void);
    else this.shutdownHandlers.push(handler as (event: unknown, ctx: PiToolCallContext) => Promise<void> | void);
  }
}

async function startWatcherSession(
  bus: Bus, watcherRoot: string, relayHome: string, sessionId: string
): Promise<{ pi: FakePi; ctx: PiToolCallContext; toasts: string[] }> {
  const toasts: string[] = [];
  const pi = new FakePi(bus);
  watcherExtension(pi, { rootDir: watcherRoot, pollTickMs: 50, relay: { relayHome } });
  const ctx: PiToolCallContext = {
    sessionId, cwd: path.dirname(watcherRoot), signal: new AbortController().signal,
    ui: { notify: (m: string) => { toasts.push(m); } }
  };
  for (const h of pi.startHandlers) await h({}, ctx);
  return { pi, ctx, toasts };
}

async function relayTransportOf(pi: FakePi, ctx: PiToolCallContext): Promise<string> {
  const tool = pi.tools.find(t => t.name === 'watcher')!;
  const res = await tool.execute('tc', { action: 'list' }, ctx.signal ?? new AbortController().signal, undefined, ctx);
  const parsed = JSON.parse(res.content[0]!.text) as { value?: { relay?: string } };
  return parsed.value?.relay ?? '';
}

test('standing binding joint E2E: no invite -> v2 local -> standing armed -> negotiation flips; multi-session coexists', async () => {
  const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-sb-')));
  const relayHome = path.join(tmp, 'rh');
  const watcherRoot = tmp;  // short path (macOS unix socket path length limit)

  let removeL1: (() => void) | undefined;
  let removeL2: (() => void) | undefined;
  let t1: TargetStub | undefined;
  let t2: TargetStub | undefined;

  try {
    // -- Setup: relay side of session 1 (real TargetHost + auto-bind listener) --
    const bus = new Bus();
    const sessionFile = path.join(tmp, 'session1.jsonl');
    fs.writeFileSync(sessionFile, '{}\n');
    t1 = await createTarget({ home: relayHome, realm: 'local', fingerprint: createHash('sha256').update('pw-sb-t1').digest('hex') });
    removeL1 = registerAutoBindListener(bus, () => t1, () => sessionFile);

    // s4.1 Without invite: the source is opened by the watcher runtime (channel W already declares localTrust) -> v2 direct connect
    const w1 = await startWatcherSession(bus, watcherRoot, relayHome, 'sess-sb-1');
    const toastDeadline = Date.now() + 8000;
    while (Date.now() < toastDeadline && !w1.toasts.some(t => t.includes('standing binding active'))) {
      await new Promise(r => setTimeout(r, 50));
    }
    assert.ok(w1.toasts.some(t => t.includes('standing binding active')), `standing toast expected: ${JSON.stringify(w1.toasts)}`);
    const st1 = (t1.core.status() as { bindings: Array<{ state: string; grants: unknown[] }> }).bindings;
    assert.equal(st1.length, 1, 'target1 must hold exactly one standing binding');
    assert.ok(st1[0]!.grants.length > 0, 'standing binding must carry a grant (sessionScoped wake)');

    // negotiation flip: primary loop ensureRelayReady -> provision -> relay='relay'
    let transport = '';
    const negotiateDeadline = Date.now() + 8000;
    while (Date.now() < negotiateDeadline) {
      transport = await relayTransportOf(w1.pi, w1.ctx);
      if (transport === 'relay') break;
      await new Promise(r => setTimeout(r, 100));
    }
    assert.equal(transport, 'relay', `relay projection must flip to 'relay' via standing binding (got '${transport}')`);

    // s4.4 Concurrent sessions: session 2 (new target fingerprint + its own listener) -> each standing, no shadowing
    const sessionFile2 = path.join(tmp, 'session2.jsonl');
    fs.writeFileSync(sessionFile2, '{}\n');
    t2 = await createTarget({ home: relayHome, realm: 'local', fingerprint: createHash('sha256').update('pw-sb-t2').digest('hex') });
    removeL2 = registerAutoBindListener(bus, () => t2, () => sessionFile2);
    const w2 = await startWatcherSession(bus, watcherRoot, relayHome, 'sess-sb-2');
    const toast2Deadline = Date.now() + 8000;
    while (Date.now() < toast2Deadline && !w2.toasts.some(t => t.includes('standing binding active'))) {
      await new Promise(r => setTimeout(r, 50));
    }
    assert.ok(w2.toasts.some(t => t.includes('standing binding active')), `session2 standing toast expected: ${JSON.stringify(w2.toasts)}`);
    const st2 = (t2.core.status() as { bindings: Array<{ state: string; grants: unknown[] }> }).bindings;
    assert.equal(st2.length, 1, 'target2 must hold its own standing binding (standing exempt from cross-target overlap)');
    assert.ok(st2[0]!.grants.length > 0, 'session2 standing binding must carry a grant');

    // -- Teardown --
    for (const h of w1.pi.shutdownHandlers) await h({}, w1.ctx);
    for (const h of w2.pi.shutdownHandlers) await h({}, w2.ctx);
  } finally {
    removeL1?.();
    removeL2?.();
    await t1?.close();
    await t2?.close();
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
