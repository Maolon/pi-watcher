import test from 'node:test';
import assert from 'node:assert/strict';
import * as fsSync from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { flockSync } from 'fs-ext';
import watcherExtension, { attentionNotifyLevel, type PiExtensionAPI, type PiTool, type PiToolCallContext, type PiToolResultEvent, type PiToolCallEvent } from '../../src/pi-extension.js';

/** Captures the fake Pi runtime that gets registered. */
class FakePi implements PiExtensionAPI {
  tools: PiTool[] = [];
  commands: Array<{ name: string; def: { description: string; handler: (args: string, ctx: PiToolCallContext) => Promise<void> | void } }> = [];
  startHandlers: Array<(event: unknown, ctx: PiToolCallContext) => Promise<void> | void> = [];
  shutdownHandlers: Array<(event: unknown, ctx: PiToolCallContext) => Promise<void> | void> = [];
  toolResultHandlers: Array<(event: PiToolResultEvent, ctx: PiToolCallContext) => Promise<void> | void> = [];
  toolCallHandlers: Array<(event: PiToolCallEvent, ctx: PiToolCallContext) => Promise<void> | void> = [];
  registerTool(tool: PiTool): void {
    this.tools.push(tool);
  }
  registerCommand(name: string, def: { description: string; handler: (args: string, ctx: PiToolCallContext) => Promise<void> | void }): void {
    this.commands.push({ name, def });
  }
  on(event: 'session_start' | 'session_shutdown', handler: (event: unknown, ctx: PiToolCallContext) => Promise<void> | void): void;
  on(event: 'tool_result', handler: (event: PiToolResultEvent, ctx: PiToolCallContext) => Promise<void> | void): void;
  on(event: 'tool_call', handler: (event: PiToolCallEvent, ctx: PiToolCallContext) => Promise<void> | void): void;
  on(event: 'session_start' | 'session_shutdown' | 'tool_result' | 'tool_call', handler: (event: unknown & PiToolResultEvent & PiToolCallEvent, ctx: PiToolCallContext) => Promise<void> | void): void {
    if (event === 'tool_result') this.toolResultHandlers.push(handler as (event: PiToolResultEvent, ctx: PiToolCallContext) => Promise<void> | void);
    else if (event === 'tool_call') this.toolCallHandlers.push(handler as (event: PiToolCallEvent, ctx: PiToolCallContext) => Promise<void> | void);
    else if (event === 'session_start') this.startHandlers.push(handler as (event: unknown, ctx: PiToolCallContext) => Promise<void> | void);
    else this.shutdownHandlers.push(handler as (event: unknown, ctx: PiToolCallContext) => Promise<void> | void);
  }
}

function makeCtx(cwd: string, sessionId = 'sess-ext'): PiToolCallContext {
  const controller = new AbortController();
  return {
    sessionId,
    cwd,
    signal: controller.signal,
    ui: { notify: () => {} }
  };
}

async function startSession(pi: FakePi, cwd: string, sessionId = 'sess-ext'): Promise<PiToolCallContext> {
  const ctx = makeCtx(cwd, sessionId);
  for (const h of pi.startHandlers) await h({}, ctx);
  return ctx;
}

async function callTool(pi: FakePi, ctx: PiToolCallContext, params: Record<string, unknown>): Promise<{ ok: boolean; error?: { code: string; message: string }; value?: unknown }> {
  const tool = pi.tools.find(t => t.name === 'watcher');
  assert.ok(tool, 'watcher tool registered');
  const res = await tool.execute('call-1', params, ctx.signal ?? new AbortController().signal, undefined, ctx);
  return JSON.parse(res.content[0].text);
}

test('extension: registers watcher tool + /watcher command; no import side effects beyond registration', async (t) => {
  const tmp = await import('node:fs/promises').then(fs => fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-reg-')));
  const pi = new FakePi();
  watcherExtension(pi, {
    rootDir: `${tmp}/state`,
    sourceRoots: new Map([['executor-local', `${tmp}/sources`]]),
    pollTickMs: 100,
    relay: false
  });
  assert.equal(pi.tools.length, 1);
  assert.equal(pi.tools[0].name, 'watcher');
  assert.equal(pi.commands.length, 1);
  assert.equal(pi.commands[0].name, 'watcher');
  await import('node:fs/promises').then(fs => fs.rm(tmp, { recursive: true, force: true }));
  void t;
});

test('extension: session lifecycle — register/list/inspect/pause via tool; ack returns NO_DELIVERY; shutdown releases lock', async () => {
  const fs = await import('node:fs/promises');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-life-'));
  const pi = new FakePi();
  watcherExtension(pi, {
    rootDir: `${tmp}/state`,
    sourceRoots: new Map([['executor-local', `${tmp}/sources`]]),
    pollTickMs: 100,
    relay: false
  });
  const ctx = await startSession(pi, tmp);

  // register (fixed candidate object: idempotent digest is stable)
  const candidate = {
    schemaVersion: 1,
    mode: 'embedded',
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-1', attemptId: 'attempt-1' },
    mission: { objective: 'o', scope: 's', checkpointId: 'c', requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required' },
    policy: { transport: 'local-display', semanticMode: 'off', notificationOwner: 'watcher', requestKinds: [], maxRequestsPerEpisode: 1, episodeCooldownMs: 60000, attentionTtlMs: 120000 },
    limits: { pollMinMs: 1000, pollMaxMs: 5000, maxSilenceMs: 60000, maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10, expiresAt: new Date(Date.now() + 3600_000).toISOString() }
  };
  const reg = await callTool(pi, ctx, { action: 'register', requestId: 'ext-req-1', candidate });
  assert.equal(reg.ok, true, JSON.stringify(reg));
  const regValue = reg.value as { watchId: string; monitoringActive?: boolean; note?: string; controlRevision?: number; transport?: string };
  assert.equal(regValue.monitoringActive, true, 'register result must signal monitoring is active');
  assert.match(regValue.note ?? '', /Do NOT poll/, 'register note must tell the agent to stop polling');
  assert.equal(regValue.controlRevision, 1);
  assert.equal(regValue.transport, 'local-display');
  assert.ok(regValue.watchId.startsWith('w-'), 'register result must expose top-level watchId (no spec echo)');
  const watchId = regValue.watchId;

  // register replay is idempotent
  const reg2 = await callTool(pi, ctx, { action: 'register', requestId: 'ext-req-1', candidate });
  assert.equal((reg2.value as { watchId: string }).watchId, watchId);

  // list
  const list = await callTool(pi, ctx, { action: 'list' });
  assert.equal((list.value as { watches: unknown[] }).watches.length, 1);

  // inspect (projection: top-level watchId / taskState, no spec echo)
  const insp = await callTool(pi, ctx, { action: 'inspect', watchId });
  const inspValue = insp.value as { watchId: string; taskState: string; episodes: unknown[] };
  assert.equal(inspValue.watchId, watchId);
  assert.ok(typeof inspValue.taskState === 'string');

  // check (CAS, actively queries progress + returns the live inspection projection)
  const chk = await callTool(pi, ctx, { action: 'check', requestId: 'ext-req-chk-1', watchId, expectedControlRevision: 1 });
  assert.equal(chk.ok, true);
  const chkValue = chk.value as { inspectionId: string; watchId: string; taskState: string; note: string };
  assert.ok(chkValue.inspectionId);
  assert.equal(chkValue.watchId, watchId);
  assert.ok(typeof chkValue.taskState === 'string');
  assert.match(chkValue.note, /Live progress refreshed/);

  // pause (CAS)
  const pause = await callTool(pi, ctx, { action: 'pause', requestId: 'ext-req-2', watchId, expectedControlRevision: 1, reason: 'ext test' });
  assert.equal((pause.value as { lifecycle: string }).lifecycle, 'paused');

  // ack: local mode has no relay delivery -> NO_DELIVERY, never fabricates a receipt
  const ack = await callTool(pi, ctx, { action: 'ack', requestId: 'ext-req-3', watchId });
  assert.equal(ack.ok, false);
  assert.equal(ack.error!.code, 'NO_DELIVERY');

  // shutdown releases the lock -> a new session can take over again
  for (const h of pi.shutdownHandlers) await h({}, ctx);
  const ctx2 = await startSession(pi, tmp, 'sess-ext-2');
  const list2 = await callTool(pi, ctx2, { action: 'list' });
  // owner isolation: the new session is not the owner -> empty list
  assert.equal((list2.value as { watches: unknown[] }).watches.length, 0);
  for (const h of pi.shutdownHandlers) await h({}, ctx2);
  await fs.rm(tmp, { recursive: true, force: true });
});

test('extension: ROOT_LOCK_HELD without a serving socket (raw flock holder, e.g. transient CLI) — honest error, lazy retry recovers', async () => {
  const fs = await import('node:fs/promises');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-rawlock-'));
  const state = `${tmp}/state`;
  const pi = new FakePi();
  watcherExtension(pi, {
    rootDir: state,
    sourceRoots: new Map([['executor-local', `${tmp}/sources`]]),
    pollTickMs: 100,
    relay: false
  });
  // An external bare flock holds the lock but does not serve (simulates a duplicate session process / abnormal holder); the lock is on this session's per-session root
  const sessionRoot = `${state}/sessions/sess-raw`;
  fsSync.mkdirSync(sessionRoot, { recursive: true });
  const fd = fsSync.openSync(`${sessionRoot}/watcher.lock`, 'a');
  flockSync(fd, 'exnb');
  const ctx = await startSession(pi, tmp, 'sess-raw');
  const blocked = await callTool(pi, ctx, { action: 'list' });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.error!.code, 'ROOT_LOCK_HELD');
  assert.match(blocked.error!.message, /retried on every watcher call/);
  // Holder releases -> the next call within the same session self-heals into primary
  flockSync(fd, 'un');
  fsSync.closeSync(fd);
  const recovered = await callTool(pi, ctx, { action: 'list' });
  assert.equal(recovered.ok, true);
  for (const h of pi.shutdownHandlers) await h({}, ctx);
  await fs.rm(tmp, { recursive: true, force: true });
});

test('extension: per-session roots — independent runtimes per session, structural owner isolation, survival across peer exit', async () => {
  const fs = await import('node:fs/promises');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-shared-'));
  const shared = `${tmp}/shared-state`;
  const opts = {
    rootDir: shared,
    sourceRoots: new Map([['executor-local', `${tmp}/sources`]]),
    pollTickMs: 100,
    relay: false
  };
  const candidateFor = (run: string) => ({
    schemaVersion: 1,
    mode: 'embedded',
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: run, attemptId: 'attempt-1' },
    mission: { objective: 'o', scope: 's', checkpointId: 'c', requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required' },
    policy: { transport: 'local-display', semanticMode: 'off', notificationOwner: 'watcher', requestKinds: [], maxRequestsPerEpisode: 1, episodeCooldownMs: 60000, attentionTtlMs: 120000 },
    limits: { pollMinMs: 1000, pollMaxMs: 5000, maxSilenceMs: 60000, maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10, expiresAt: new Date(Date.now() + 3600_000).toISOString() }
  });

  // A: primary
  const piA = new FakePi();
  watcherExtension(piA, opts);
  const ctxA = await startSession(piA, tmp, 'sess-a');

  // B/C: same project root -> each gets its own independent per-session root (decision 2026-09-21: no attach, no version skew)
  const piB = new FakePi();
  watcherExtension(piB, opts);
  const widgetLines: string[][] = [];
  const ctxB: PiToolCallContext = {
    sessionId: 'sess-b', cwd: tmp, signal: new AbortController().signal,
    ui: { notify: () => {}, setWidget: (_n, lines) => widgetLines.push(lines) }
  };
  for (const h of piB.startHandlers) await h({}, ctxB);
  const piC = new FakePi();
  watcherExtension(piC, opts);
  const ctxC = await startSession(piC, tmp, 'sess-c');

  // B registers; fully functional in its own root (no IPC forwarding)
  const regB = await callTool(piB, ctxB, { action: 'register', requestId: 'shared-req-b', candidate: candidateFor('run-b') });
  assert.equal(regB.ok, true, `attached register should work: ${JSON.stringify(regB)}`);
  const listB = await callTool(piB, ctxB, { action: 'list' });
  assert.equal((listB.value as { watches: unknown[] }).watches.length, 1);

  // owner isolation: A's and C's lists each see only their own
  const regA = await callTool(piA, ctxA, { action: 'register', requestId: 'shared-req-a', candidate: candidateFor('run-a') });
  assert.equal(regA.ok, true);
  const regC = await callTool(piC, ctxC, { action: 'register', requestId: 'shared-req-c', candidate: candidateFor('run-c') });
  assert.equal(regC.ok, true);
  const listA = await callTool(piA, ctxA, { action: 'list' });
  assert.equal((listA.value as { watches: unknown[] }).watches.length, 1);
  const listC = await callTool(piC, ctxC, { action: 'list' });
  assert.equal((listC.value as { watches: unknown[] }).watches.length, 1);

  // attached session's widget polling takes effect (including rows for registered watches)
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline && !widgetLines.some(l => l.some(x => x.includes('[watcher]')))) {
    await new Promise(r => setTimeout(r, 50));
  }
  assert.ok(widgetLines.some(l => l.some(x => x.includes('[watcher]'))), 'attached widget poll should render [watcher] lines');

  // failover: primary exits -> B is promoted, data still present
  for (const h of piA.shutdownHandlers) await h({}, ctxA);
  const listB2 = await callTool(piB, ctxB, { action: 'list' });
  assert.equal(listB2.ok, true, `B should survive primary exit: ${JSON.stringify(listB2)}`);
  assert.equal((listB2.value as { watches: unknown[] }).watches.length, 1);
  const listC2 = await callTool(piC, ctxC, { action: 'list' });
  assert.equal(listC2.ok, true);

  for (const h of piB.shutdownHandlers) await h({}, ctxB);
  for (const h of piC.shutdownHandlers) await h({}, ctxC);
  await fs.rm(tmp, { recursive: true, force: true });
});

test('extension: agent-file progress query via check and inspect returns latestOutput and advances lastObservedAt', async () => {
  const fs = await import('node:fs/promises');
  const os = await import('node:os');
  const path = await import('node:path');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-prog-'));
  const logDir = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-exec-log-'));
  const runId = 'exec-prog-99';
  const logFile = path.join(logDir, `app-${runId}.log`);
  await fs.writeFile(logFile, 'Step 1: starting build...\n');
  const fh = fsSync.openSync(logFile, 'r+');

  const pi = new FakePi();
  watcherExtension(pi, {
    pollTickMs: 50,
    relay: false
  });
  const ctx = await startSession(pi, tmp, 'sess-prog');

  try {
    const candidate = {
      mode: 'embedded',
      target: { kind: 'run', sourceId: 'agent-file', taskId: 'file', runId, attemptId: 'attempt-1', file: { path: logFile, okPattern: '__EXEC_EXIT__:0' } },
      mission: { objective: 'build test suite', scope: 's', checkpointId: 'c', requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required' },
      policy: { transport: 'local-display', semanticMode: 'off', notificationOwner: 'watcher', requestKinds: [], maxRequestsPerEpisode: 1, episodeCooldownMs: 60000, attentionTtlMs: 120000 },
      limits: { pollMinMs: 1000, pollMaxMs: 5000, maxSilenceMs: 60000, maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10, expiresAt: new Date(Date.now() + 3600_000).toISOString() }
    };
    const reg = await callTool(pi, ctx, { action: 'register', requestId: 'prog-req-1', candidate });
    assert.equal(reg.ok, true, JSON.stringify(reg));
    const watchId = (reg.value as { watchId: string }).watchId;

    // Active check: first active probe, returns the live inspection projection
    const chk1 = await callTool(pi, ctx, { action: 'check', requestId: 'prog-req-chk-1', watchId, expectedControlRevision: 1 });
    assert.equal(chk1.ok, true, JSON.stringify(chk1));
    const chk1Val = chk1.value as { taskState: string; latestOutput?: string; lastObservedAt?: string; note: string };
    assert.equal(chk1Val.taskState, 'running');
    assert.match(chk1Val.latestOutput ?? '', /Step 1: starting build/);
    assert.ok(chk1Val.lastObservedAt);
    assert.match(chk1Val.note, /Live progress refreshed/);

    // A following inspect should also see the refreshed state
    const insp1 = await callTool(pi, ctx, { action: 'inspect', watchId });
    const insp1Val = insp1.value as { taskState: string; latestOutput?: string; lastObservedAt?: string };
    assert.equal(insp1Val.taskState, 'running');
    assert.match(insp1Val.latestOutput ?? '', /Step 1: starting build/);

    // The process produces new log output
    fsSync.appendFileSync(logFile, 'Step 2: compiling files...\nStep 3: running tests...\n');

    // Active check again (simulates the agent calling the check entry to view live progress)
    const chk2 = await callTool(pi, ctx, { action: 'check', requestId: 'prog-req-chk-2', watchId, expectedControlRevision: 1 });
    assert.equal(chk2.ok, true);
    const chk2Val = chk2.value as { inspectionId: string; taskState: string; latestOutput?: string; note: string };
    assert.ok(chk2Val.inspectionId);
    assert.equal(chk2Val.taskState, 'running');
    assert.match(chk2Val.latestOutput ?? '', /Step 3: running tests/);
    assert.match(chk2Val.note, /Live progress refreshed/);

    // Simulate task completion writing the exit marker
    fsSync.appendFileSync(logFile, 'Build finished successfully.\n__EXEC_EXIT__:0\n');
    fsSync.closeSync(fh);

    // Check again: declarative pattern hit -> terminal state and final log
    const chk3 = await callTool(pi, ctx, { action: 'check', requestId: 'prog-req-chk-3', watchId, expectedControlRevision: 1 });
    assert.equal(chk3.ok, true);
    const chk3Val = chk3.value as { taskState: string; latestOutput?: string };
    assert.equal(chk3Val.taskState, 'succeeded');
    assert.match(chk3Val.latestOutput ?? '', /Build finished successfully/);
  } finally {
    for (const h of pi.shutdownHandlers) await h({}, ctx);
    await fs.rm(tmp, { recursive: true, force: true });
    await fs.rm(logDir, { recursive: true, force: true });
  }
});

test('extension: attention toast level — success is info, everything else warning', () => {
  assert.equal(attentionNotifyLevel({ reasonCode: 'task.terminal', taskState: 'succeeded', transport: 'local-display' }), 'info');
  assert.equal(attentionNotifyLevel({ reasonCode: 'task.terminal', taskState: 'succeeded', transport: 'relay-managed' }), 'info');
  assert.equal(attentionNotifyLevel({ reasonCode: 'task.terminal', taskState: 'succeeded', transport: 'relay-failed' }), 'warning');
  assert.equal(attentionNotifyLevel({ reasonCode: 'task.terminal', taskState: 'cancelled', transport: 'local-display' }), 'warning');
  assert.equal(attentionNotifyLevel({ reasonCode: 'task.failed', taskState: 'failed', transport: 'local-display' }), 'warning');
  assert.equal(attentionNotifyLevel({ reasonCode: 'task.exited-unknown', taskState: 'unknown', transport: 'local-display' }), 'warning');
  assert.equal(attentionNotifyLevel({ reasonCode: 'deadline.exceeded', transport: 'local-display' }), 'warning');
  assert.equal(attentionNotifyLevel({ reasonCode: 'task.terminal', transport: 'local-display' }), 'warning');
});

test('extension: attention reaches sessions — strict session isolation (primary vs attached)', async () => {
  const fs = await import('node:fs/promises');
  const os = await import('node:os');
  const path = await import('node:path');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-att-'));
  const logDir = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-exec-log-'));
  const runIdA = 'exec-att-a';
  const logFileA = path.join(logDir, `pi-unified-exec-${runIdA}-abcd0001.log`);
  await fs.writeFile(logFileA, 'work\n__EXEC_EXIT__:0\n');

  const notifyA: string[] = [];
  const levelsA = new Map<string, string | undefined>();
  const piA = new FakePi();
  watcherExtension(piA, { pollTickMs: 50, relay: false });
  const ctxA: PiToolCallContext = {
    sessionId: 'sess-att-a', cwd: tmp, signal: new AbortController().signal,
    ui: { notify: (m: string, level?: string) => { notifyA.push(m); levelsA.set(m, level); } }
  };
  for (const h of piA.startHandlers) await h({}, ctxA);

  try {
    const candidateA = {
      mode: 'embedded',
      target: { kind: 'run', sourceId: 'agent-file', taskId: 'file', runId: runIdA, attemptId: 'attempt-1', file: { path: logFileA, okPattern: '__EXEC_EXIT__:0' } },
      mission: { objective: 'session A watch', scope: 's', checkpointId: 'c', requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required' },
      policy: { transport: 'local-display', semanticMode: 'off', notificationOwner: 'watcher', requestKinds: [], maxRequestsPerEpisode: 1, episodeCooldownMs: 60000, attentionTtlMs: 120000 },
      limits: { pollMinMs: 1000, pollMaxMs: 5000, maxSilenceMs: 600000, maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10, expiresAt: new Date(Date.now() + 3600_000).toISOString() }
    };
    const regA = await callTool(piA, ctxA, { action: 'register', requestId: 'att-req-1', candidate: candidateA });
    assert.equal(regA.ok, true, JSON.stringify(regA));
    const watchIdA = (regA.value as { watchId: string }).watchId;

    // The sweep (50ms tick) should quickly find the terminal state -> task.terminal attention -> primary session toast
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline && !notifyA.some(m => m.includes('pi-watcher attention') && m.includes('task.terminal'))) {
      await new Promise(r => setTimeout(r, 50));
    }
    assert.ok(notifyA.some(m => m.includes('task.terminal')), `primary session must be notified of terminal attention: ${JSON.stringify(notifyA)}`);
    assert.ok(notifyA.some(m => m.includes(watchIdA)), 'notice must reference the watchId');
    const terminalA = notifyA.find(m => m.includes('task.terminal') && m.includes(watchIdA));
    assert.equal(levelsA.get(terminalA!), 'info', 'a succeeded terminal notice must toast at info, not warning');

    // A second session attaches to the same root: it should not receive Session A's attention notification (strict isolation)
    const notifyB: string[] = [];
    const piB = new FakePi();
    watcherExtension(piB, { pollTickMs: 50, relay: false });
    const ctxB: PiToolCallContext = {
      sessionId: 'sess-att-b', cwd: tmp, signal: new AbortController().signal,
      ui: { notify: (m: string) => { notifyB.push(m); } }
    };
    for (const h of piB.startHandlers) await h({}, ctxB);

    // Wait two widget polling periods and confirm it was not notified
    await new Promise(r => setTimeout(r, 200));
    assert.equal(notifyB.some(m => m.includes('pi-watcher attention')), false, 'attached session B must NOT receive session A attention notices');

    // Now let Session B register a task of its own, to verify it normally receives its own attention notification
    const runIdB = 'exec-att-b';
    const logFileB = path.join(logDir, `pi-unified-exec-${runIdB}-abcd0002.log`);
    await fs.writeFile(logFileB, 'work-b\n__EXEC_EXIT__:0\n');
    const candidateB = {
      mode: 'embedded',
      target: { kind: 'run', sourceId: 'agent-file', taskId: 'file', runId: runIdB, attemptId: 'attempt-1', file: { path: logFileB, okPattern: '__EXEC_EXIT__:0' } },
      mission: { objective: 'session B watch', scope: 's', checkpointId: 'c', requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required' },
      policy: { transport: 'local-display', semanticMode: 'off', notificationOwner: 'watcher', requestKinds: [], maxRequestsPerEpisode: 1, episodeCooldownMs: 60000, attentionTtlMs: 120000 },
      limits: { pollMinMs: 1000, pollMaxMs: 5000, maxSilenceMs: 600000, maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10, expiresAt: new Date(Date.now() + 3600_000).toISOString() }
    };
    const regB = await callTool(piB, ctxB, { action: 'register', requestId: 'att-req-2', candidate: candidateB });
    assert.equal(regB.ok, true, JSON.stringify(regB));
    const watchIdB = (regB.value as { watchId: string }).watchId;

    const countA0 = notifyA.filter(m => m.includes('task.terminal')).length;
    const deadlineB = Date.now() + 5000;
    while (Date.now() < deadlineB && !notifyB.some(m => m.includes('pi-watcher attention') && m.includes(watchIdB))) {
      await new Promise(r => setTimeout(r, 50));
    }
    assert.ok(notifyB.some(m => m.includes(watchIdB)), `attached session B must receive its own attention notices: ${JSON.stringify(notifyB)}`);
    // Session A should not receive Session B's new attention
    const countA1 = notifyA.filter(m => m.includes('task.terminal')).length;
    assert.equal(countA1, countA0, 'primary session A must NOT receive session B attention notices');

    for (const h of piB.shutdownHandlers) await h({}, ctxB);
  } finally {
    for (const h of piA.shutdownHandlers) await h({}, ctxA);
    await fs.rm(tmp, { recursive: true, force: true });
    await fs.rm(logDir, { recursive: true, force: true });
  }
});

test('extension: auto-registers a watch for still-running exec sessions observed via tool_result (dedup + exit skip + opt-out)', async () => {
  const fs = await import('node:fs/promises');
  const os = await import('node:os');
  const path = await import('node:path');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-auto-'));
  const toasts: string[] = [];
  const pi = new FakePi();
  watcherExtension(pi, { pollTickMs: 50, relay: false, autoExecWatch: true });  // off by default since 2026-09-21, explicit opt-in
  const ctx: PiToolCallContext = {
    sessionId: 'sess-auto', cwd: tmp, signal: new AbortController().signal,
    ui: { notify: (m: string) => { toasts.push(m); } }
  };
  for (const h of pi.startHandlers) await h({}, ctx);

  const fireToolResult = async (toolName: string, text: string): Promise<void> => {
    const ev: PiToolResultEvent = { toolName, toolCallId: 'tc-1', content: [{ type: 'text', text }], isError: false };
    for (const h of pi.toolResultHandlers) await h(ev, ctx);
    // auto-watch is fire-and-forget: give the side-path promise one microtask cycle
    await new Promise(r => setImmediate(r));
  };

  try {
    // 1) exec_command result still running -> autonomous registration (file watch pinning log_path)
    await fireToolResult('exec_command', '[still running]\nsession_id: 45\nlog_path: /var/folders/xx/T/pi-unified-exec-45-1234abcd.log\nyield_time_ms: 10000\n');
    let list = await callTool(pi, ctx, { action: 'list' });
    let watches = (list.value as { watches: Array<{ runId?: string; lifecycle?: string }> }).watches;
    assert.equal(watches.length, 1, `auto-register should create exactly one watch: ${JSON.stringify(watches)}`);
    assert.equal(watches[0]!.runId, 'auto-exec-45');
    assert.equal(watches[0]!.lifecycle, 'active');
    assert.ok(toasts.some(t => t.includes('auto-watching') && t.includes('45')), `toast should announce auto-watch: ${JSON.stringify(toasts)}`);

    // Pin verification: target must carry the full log_path from the exec result + marker pattern (universal monitor mode)
    {
      const Database = (await import('node:sqlite')).DatabaseSync;
      const db = new Database(path.join(tmp, '.pi-watcher', 'sessions', 'sess-auto', 'watcher.db'), { readOnly: true });
      const spec = JSON.parse(db.prepare('SELECT spec_json FROM watches WHERE lifecycle = ?').get('active')?.spec_json as string);
      db.close();
      assert.equal(spec.target.sourceId, 'agent-file');
      assert.equal(spec.target.file.path, '/var/folders/xx/T/pi-unified-exec-45-1234abcd.log', 'target must pin the exact log_path from the exec result');
      assert.equal(spec.target.file.okPattern, '__EXEC_EXIT__:0');
      assert.equal(spec.target.file.failPattern, '__EXEC_EXIT__:-?[1-9]');
    }

    // 2) duplicate event (write_stdin polling the same session) -> dedup, do not create again
    await fireToolResult('write_stdin', '[still running]\nsession_id: 45\nlog_path: /var/folders/xx/T/pi-unified-exec-45-1234abcd.log\nwait_status: relative_deadline_reached\n');
    list = await callTool(pi, ctx, { action: 'list' });
    watches = (list.value as { watches: Array<{ runId?: string; lifecycle?: string }> }).watches;
    assert.equal(watches.length, 1, 'duplicate tool_result for same runId must not create a second watch');

    // 3) exited session -> skipped
    await fireToolResult('exec_command', '[exited]\nexit_code: 0\nlog_path: /tmp/y.log\n');
    list = await callTool(pi, ctx, { action: 'list' });
    watches = (list.value as { watches: Array<{ runId?: string; lifecycle?: string }> }).watches;
    assert.equal(watches.length, 1, 'exited exec result must not auto-register');

    // 4) unrelated tool -> ignored
    await fireToolResult('read', 'file content here');
    list = await callTool(pi, ctx, { action: 'list' });
    watches = (list.value as { watches: Array<{ runId?: string; lifecycle?: string }> }).watches;
    assert.equal(watches.length, 1);

    for (const h of pi.shutdownHandlers) await h({}, ctx);
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
  }
});

test('extension: auto-exec-watch can be disabled via options (PI_WATCHER_AUTO_EXEC=0 equivalent)', async () => {
  const fs = await import('node:fs/promises');
  const os = await import('node:os');
  const path = await import('node:path');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-autooff-'));
  const pi = new FakePi();
  watcherExtension(pi, { pollTickMs: 50, relay: false, autoExecWatch: false });
  const ctx: PiToolCallContext = {
    sessionId: 'sess-autooff', cwd: tmp, signal: new AbortController().signal,
    ui: { notify: () => {} }
  };
  for (const h of pi.startHandlers) await h({}, ctx);
  try {
    const ev: PiToolResultEvent = { toolName: 'exec_command', toolCallId: 'tc-1', content: [{ type: 'text', text: '[still running]\nsession_id: 77\nlog_path: /tmp/anything/pi-unified-exec-77-deadbeef.log\n' }], isError: false };
    for (const h of pi.toolResultHandlers) await h(ev, ctx);
    await new Promise(r => setImmediate(r));
    const list = await callTool(pi, ctx, { action: 'list' });
    assert.equal((list.value as { watches: unknown[] }).watches.length, 0, 'autoExecWatch=false must not register');
    // Default (autoExecWatch not passed) also does not auto-register: the choice belongs to the agent
    const piDefault = new FakePi();
    watcherExtension(piDefault, { pollTickMs: 50, relay: false });
    const ctxDefault: PiToolCallContext = { sessionId: 'sess-auto-def', cwd: tmp, signal: new AbortController().signal, ui: { notify: () => {} } };
    for (const h of piDefault.startHandlers) await h({}, ctxDefault);
    for (const h of piDefault.toolResultHandlers) await h(ev, ctxDefault);
    await new Promise(r => setImmediate(r));
    const listDefault = await callTool(piDefault, ctxDefault, { action: 'list' });
    assert.equal((listDefault.value as { watches: unknown[] }).watches.length, 0, 'default must be agent-driven (no auto-watch)');
    for (const h of piDefault.shutdownHandlers) await h({}, ctxDefault);
    for (const h of pi.shutdownHandlers) await h({}, ctx);
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
  }
});

test('extension: auto-bind v2 — standing-first, invite fallback on local_trust_disabled, silent relay retries', async () => {
  const fs = await import('node:fs/promises');
  const os = await import('node:os');
  const path = await import('node:path');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-bind-'));
  const inviteDir = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-invite-'));
  const invitePath = path.join(inviteDir, 'invite-test0001.json');
  await fs.writeFile(invitePath, JSON.stringify({ inviteId: 'invite-test0001' }));

  class Bus {
    listeners = new Map<string, Array<(d: unknown) => void>>();
    on(event: string, l: (d: unknown) => void): () => void {
      const arr = this.listeners.get(event) ?? [];
      arr.push(l); this.listeners.set(event, arr);
      return () => { this.listeners.set(event, (this.listeners.get(event) ?? []).filter(x => x !== l)); };
    }
    emit(event: string, d: unknown): void {
      for (const l of [...(this.listeners.get(event) ?? [])]) l(d);
    }
  }

  type Req = { requestId: string; kind?: string; source: string; invitePath?: string; sourceId?: string; channelId?: string; realm?: string };
  const run = async (mode: 'standing-ok' | 'fallback' | 'silent') => {
    const bus = new Bus();
    const toasts: string[] = [];
    const requests: Req[] = [];
    bus.on('pi-relay:bind-request', (d) => {
      const r = d as Req;
      requests.push(r);
      if (mode === 'silent') return;
      if (r.kind === 'local') {
        bus.emit('pi-relay:bind-result', mode === 'standing-ok'
          ? { requestId: r.requestId, ok: true, bindingId: 'bnd-standing', armed: true, standing: true }
          : { requestId: r.requestId, ok: false, error: { code: 'local_trust_disabled', message: 'channel not opted in' } });
      } else {
        bus.emit('pi-relay:bind-result', { requestId: r.requestId, ok: true, bindingId: 'bnd-invite', armed: true });
      }
    });
    const pi = new FakePi();
    (pi as unknown as { events: unknown }).events = bus;
    await fs.mkdir(path.join(tmp, 'root-bind'), { recursive: true }).catch(() => {});
    await fs.writeFile(path.join(tmp, 'root-bind', 'relay-setup.json'), JSON.stringify({ inviteFile: invitePath, provisioned: false }));
    watcherExtension(pi, {
      rootDir: path.join(tmp, 'root-bind'),
      pollTickMs: 50,
      relay: { relayHome: path.join(tmp, 'relay-home') }
    });
    const ctx: PiToolCallContext = {
      sessionId: 'sess-bind', cwd: tmp, signal: new AbortController().signal,
      ui: { notify: (m: string) => { toasts.push(m); } }
    };
    for (const h of pi.startHandlers) await h({}, ctx);
    return { pi, ctx, toasts, requests };
  };

  try {
    // 1) standing first: v2 kind:'local' connects directly, invite is not touched
    const a = await run('standing-ok');
    await new Promise(r => setTimeout(r, 150));
    assert.equal(a.requests.length, 1, `standing path should emit exactly one request: ${JSON.stringify(a.requests)}`);
    assert.equal(a.requests[0]!.kind, 'local');
    // per-session sourceId (2026-09-21 per-session root: derived from the session root)
    const { deriveSourceId } = await import('../../src/relay/managed.js');
    assert.equal(a.requests[0]!.sourceId, deriveSourceId(path.join(tmp, 'root-bind', 'sessions', 'sess-bind')));
    assert.match(a.requests[0]!.sourceId, /^pi-watcher:/);
    assert.equal(a.requests[0]!.channelId, 'W');
    assert.equal(a.requests[0]!.invitePath, undefined, 'v2 local must not carry invitePath');
    assert.ok(a.toasts.some(t => t.includes('standing binding active')), `standing toast expected: ${JSON.stringify(a.toasts)}`);
    for (const h of a.pi.shutdownHandlers) await h({}, a.ctx);

    // 2) local_trust_disabled -> falls back to the v1 invite path, which succeeds
    const b = await run('fallback');
    await new Promise(r => setTimeout(r, 150));
    assert.equal(b.requests.length, 2, `fallback should emit v2 then v1: ${JSON.stringify(b.requests)}`);
    assert.equal(b.requests[0]!.kind, 'local');
    assert.equal(b.requests[1]!.kind, 'invite');
    assert.equal(b.requests[1]!.invitePath, invitePath);
    assert.ok(b.toasts.some(t => t.includes('relay wake bound')), `fallback success toast expected: ${JSON.stringify(b.toasts)}`);
    for (const h of b.pi.shutdownHandlers) await h({}, b.ctx);

    // 3) relay does not reply -> retry a few times then give up, no crash
    const c = await run('silent');
    await new Promise(r => setTimeout(r, 300));
    assert.ok(c.requests.length >= 1 && c.requests.every(r => r.kind === 'local'), 'silent relay: only v2 attempts, no invite fallback');
    assert.ok(!c.toasts.some(t => t.includes('binding active') || t.includes('relay wake bound')), 'no success toast when relay silent');
    for (const h of c.pi.shutdownHandlers) await h({}, c.ctx);
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
    await fs.rm(inviteDir, { recursive: true, force: true });
  }
});


test('extension: tool_call injects __EXEC_EXIT__ marker into exec_command inputs (idempotent, opt-out respected)', async () => {
  const fs = await import('node:fs/promises');
  const os = await import('node:os');
  const path = await import('node:path');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-marker-'));

  const run = async (off: 'marker' | 'none') => {
    const pi = new FakePi();
    watcherExtension(pi, { pollTickMs: 50, relay: false, ...(off === 'marker' ? { execMarkerInjection: false } : {}) });
    const ctx: PiToolCallContext = { sessionId: 'sess-marker', cwd: tmp, signal: new AbortController().signal, ui: { notify: () => {} } };
    for (const h of pi.startHandlers) await h({}, ctx);
    const fire = async (cmd: string): Promise<string> => {
      const input = { cmd };
      const ev = { toolName: 'exec_command', toolCallId: 'tc', input } as never;
      for (const h of pi.toolCallHandlers) await h(ev, ctx);
      return input.cmd;
    };
    return { pi, ctx, fire };
  };

  try {
    // 1) no marker -> appended automatically (on by default; decoupled from autoExecWatch: passive helper creates no watch)
    const a = await run('none');
    assert.match(await a.fire('sleep 2'), /\necho __EXEC_EXIT__:\$\?$/);
    // 2) marker already present -> not duplicated
    assert.equal(await a.fire('sleep 2; echo __EXEC_EXIT__:$?'), 'sleep 2; echo __EXEC_EXIT__:$?');
    // 3) trailing whitespace is trimmed
    assert.match(await a.fire('sleep 2\n\n'), /\necho __EXEC_EXIT__:\$\?$/);
    // 4) execMarkerInjection=false -> not injected
    const b = await run('marker');
    assert.equal(await b.fire('sleep 2'), 'sleep 2');
    for (const h of a.pi.shutdownHandlers) await h({}, a.ctx);
    for (const h of b.pi.shutdownHandlers) await h({}, b.ctx);
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
  }
});

test('extension: action=watch-file — agent-declared file registration, INVALID_SPEC without path, pattern + semanticMode passthrough, idempotent replay', async () => {
  const fs = await import('node:fs/promises');
  const os = await import('node:os');
  const path = await import('node:path');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-watchact-'));
  const pi = new FakePi();
  watcherExtension(pi, { pollTickMs: 50, relay: false });
  const ctx = await startSession(pi, tmp, 'sess-watchact');
  const logPath = path.join(tmp, 'build-output.log');
  try {
    // missing path -> INVALID_SPEC
    const bad = await callTool(pi, ctx, { action: 'watch-file', requestId: 'w-bad-1' });
    assert.equal(bad.ok, false);
    assert.equal((bad.error as { code: string }).code, 'INVALID_SPEC');

    // path + pattern given -> registration succeeds, returns a compact projection
    const ok = await callTool(pi, ctx, { action: 'watch-file', requestId: 'w-ok-1', path: logPath, okPattern: '__EXEC_EXIT__:0', failPattern: '__EXEC_EXIT__:-?[1-9]' });
    assert.equal(ok.ok, true, JSON.stringify(ok));
    const v = ok.value as { watchId: string; runId: string; path: string; controlRevision: number; transport: string; note: string };
    assert.ok(v.watchId.startsWith('w-'));
    assert.ok(v.runId.startsWith('file-'));
    assert.equal(v.path, logPath, 'relative-free absolute path must pass through');
    assert.equal(v.controlRevision, 1);
    assert.equal(v.transport, 'local-display');
    assert.match(v.note, /Watching file/);

    // spec persisted: target.file carries the full path and declarative pattern
    {
      const { DatabaseSync } = await import('node:sqlite');
      const db = new DatabaseSync(path.join(tmp, '.pi-watcher', 'sessions', 'sess-watchact', 'watcher.db'), { readOnly: true });
      const row = db.prepare('SELECT spec_json FROM watches WHERE json_extract(spec_json, ?) = ?').get('$.target.runId', v.runId) as { spec_json: string };
      db.close();
      const spec = JSON.parse(row.spec_json);
      assert.equal(spec.target.sourceId, 'agent-file');
      assert.equal(spec.target.file.path, logPath);
      assert.equal(spec.target.file.okPattern, '__EXEC_EXIT__:0');
      assert.equal(spec.target.file.failPattern, '__EXEC_EXIT__:-?[1-9]');
    }

    // semanticMode is optionally passed through (shadow/active lets Jev take part in judgment)
    const sem = await callTool(pi, ctx, { action: 'watch-file', requestId: 'w-sem-1', path: path.join(tmp, 'other.log') });
    assert.equal(sem.ok, true, JSON.stringify(sem));
    {
      const { DatabaseSync } = await import('node:sqlite');
      const db = new DatabaseSync(path.join(tmp, '.pi-watcher', 'sessions', 'sess-watchact', 'watcher.db'), { readOnly: true });
      const row = db.prepare('SELECT spec_json FROM watches WHERE json_extract(spec_json, ?) = ?').get('$.mission.objective', 'file watch: other.log') as { spec_json: string };
      db.close();
      const spec = JSON.parse(row.spec_json);
      assert.equal(spec.policy.semanticMode, 'off', 'semanticMode defaults to off without explicit param');
    }
    const sem2 = await callTool(pi, ctx, { action: 'watch-file', requestId: 'w-sem-2', path: path.join(tmp, 'third.log'), semanticMode: 'shadow' });
    assert.equal(sem2.ok, true, JSON.stringify(sem2));
    {
      const { DatabaseSync } = await import('node:sqlite');
      const db = new DatabaseSync(path.join(tmp, '.pi-watcher', 'sessions', 'sess-watchact', 'watcher.db'), { readOnly: true });
      const row = db.prepare('SELECT spec_json FROM watches WHERE json_extract(spec_json, ?) = ?').get('$.mission.objective', 'file watch: third.log') as { spec_json: string };
      db.close();
      const spec = JSON.parse(row.spec_json);
      assert.equal(spec.policy.semanticMode, 'shadow', 'semanticMode must be stored in policy');
    }

    // Idempotent: same requestId + same params (digest stable within the hour bucket) replay -> same watchId
    const ok2 = await callTool(pi, ctx, { action: 'watch-file', requestId: 'w-ok-1', path: logPath, okPattern: '__EXEC_EXIT__:0', failPattern: '__EXEC_EXIT__:-?[1-9]' });
    assert.equal((ok2.value as { watchId: string }).watchId, v.watchId);

    // same path + same pattern, different requestId -> dedup returns the existing watch (dedup identity = path + patterns: same file with different verdict declarations are different watches)
    const ok3 = await callTool(pi, ctx, { action: 'watch-file', requestId: 'w-dedup-1', path: logPath, okPattern: '__EXEC_EXIT__:0', failPattern: '__EXEC_EXIT__:-?[1-9]' });
    assert.equal((ok3.value as { watchId: string }).watchId, v.watchId);

    // visible in list
    const list = await callTool(pi, ctx, { action: 'list' });
    assert.equal((list.value as { watches: Array<{ runId?: string }> }).watches.filter(w => w.runId === v.runId).length, 1);
  } finally {
    for (const h of pi.shutdownHandlers) await h({}, ctx);
    await fs.rm(tmp, { recursive: true, force: true });
  }
});

test('extension: /watcher cancel closes every active/paused watch of this session only, no watchId needed', async () => {
  const fs = await import('node:fs/promises');
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'pw-ext-cancel-'));
  const pi = new FakePi();
  watcherExtension(pi, { pollTickMs: 50, relay: false });
  const notes: Array<{ text: string; level?: string }> = [];
  const ctx = await startSession(pi, tmp, 'sess-cancel');
  ctx.ui = { notify: (text, level) => { notes.push({ text, level }); } };
  const peer = await startSession(pi, tmp, 'sess-cancel-peer');
  const cmd = pi.commands.find(c => c.name === 'watcher');
  assert.ok(cmd);
  try {
    const a = await callTool(pi, ctx, { action: 'watch-file', requestId: 'c-a', path: path.join(tmp, 'a.log') });
    const b = await callTool(pi, ctx, { action: 'watch-file', requestId: 'c-b', path: path.join(tmp, 'b.log') });
    const p = await callTool(pi, peer, { action: 'watch-file', requestId: 'c-p', path: path.join(tmp, 'p.log') });
    assert.equal(a.ok && b.ok && p.ok, true);
    const bId = (b.value as { watchId: string }).watchId;
    const paused = await callTool(pi, ctx, { action: 'pause', requestId: 'c-pause', watchId: bId, expectedControlRevision: 1, reason: 'test' });
    assert.equal(paused.ok, true, JSON.stringify(paused));

    await cmd.def.handler('cancel done with these', ctx);
    assert.match(notes.at(-1)!.text, /closed 2 watch\(es\) in this session/);
    assert.equal(notes.at(-1)!.level, 'info');

    const after = await callTool(pi, ctx, { action: 'list' });
    assert.equal((after.value as { watches: unknown[] }).watches.length, 0, JSON.stringify(after));
    const peerAfter = await callTool(pi, peer, { action: 'list' });
    assert.equal((peerAfter.value as { watches: unknown[] }).watches.length, 1, 'peer session watch untouched');

    await cmd.def.handler('cancel', ctx);
    assert.match(notes.at(-1)!.text, /no active or paused watch in this session/);
  } finally {
    for (const h of pi.shutdownHandlers) await h({}, ctx);
    for (const h of pi.shutdownHandlers) await h({}, peer);
    await fs.rm(tmp, { recursive: true, force: true });
  }
});
