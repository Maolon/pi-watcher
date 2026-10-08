/**
 * Pi extension entry point (design 08 / 11 + decision 2026-09-21 per-session root).
 * - The factory only registers tools/commands; no import side effects
 * - per-session root: each session owns <root>/sessions/<sessionId>/ (store + engine + sweep loop)
 *   and is the only process on its own root. Version skew is structurally impossible: after a plugin upgrade,
 *   old and new sessions each run on their own root without interfering (2026-09-21 measurement: with a shared root, primary/attached mode
 *   split the action surface at random by the primary's code version, since npm updates disk code but old sessions still hold old code in memory).
 *   Under strict session isolation attention is only sent to the owner session anyway, so all that sharing left was one loop
 *   and a global panel, which is not worth this failure mode. Cost (honestly recorded): N sessions run N loops (cheap);
 *   the /watcher panel is session-scoped; the Jev budget is per session.
 * - The IPC/primary/attached machinery stays in src/ipc/ (seed of the V4 standalone service: atomic standalone-process
 *   upgrade + protocol version negotiation); the embedded path no longer uses it.
 * - session_shutdown: abort session-scoped I/O, clear timers, close the runtime (release this session's lock)
 * - The only LLM tool: `watcher` (watch-file/watch-check/register/list/inspect/check/ack/pause/close/update)
 *   source/profile/owner capability are injected from the trusted context; the model cannot specify sessionId
 * - ack requires relay delivery (deliveryRef); when relay is not negotiated it returns NO_DELIVERY and never fabricates a receipt
 */

import { Type } from 'typebox';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as pathMod from 'node:path';
import { startRuntime, type WatcherRuntime } from './runtime.js';
import { deriveSourceId } from './relay/managed.js';
import { toServiceResultAsync } from './util/result.js';
import { renderWatcherWidget } from './engine/widget.js';
import { runToolAction, type ToolParams } from './engine/tool-actions.js';
import { DEFAULT_ATTENTION_TTL_MS, type AttentionNotice } from './engine/engine.js';
import type { Json, RegisterCandidate, ActorContext } from './contracts/interfaces.js';
import { PiRegistryJudge, type PiClassifierRegistry } from './jev/pi-registry.js';
import { jevConsentState, setStoredJevConsent, consentFilePath } from './jev/consent.js';

/** Minimal structural types: avoid a hard dependency on the pi runtime (the loader is Pi itself). */
export interface PiToolCallContext {
  sessionId?: string;
  cwd: string;
  signal?: AbortSignal;
  ui?: {
    /** The pi 1.0.0 type union is "info" | "warning" | "error"(Extensions types L80); this plugin historically passed 'warn', which is not in the union, so it is now unified to 'warning' (2026-10-02 pi 1.0.0 compatibility check) */
    notify(message: string, level?: 'info' | 'warning' | 'error'): void;
    /** SDK setWidget first overload (string[]); degrades silently when absent (no status bar environment) */
    setWidget?: (name: string, lines: string[]) => void;
  };
  sessionManager?: { getSessionId?: () => string; getSessionFile?: () => string };
  /** Pi's model registry (Pi >= 1.1): classifier models such as Jev, with Pi-managed credentials */
  modelRegistry?: PiClassifierRegistry;
}

export interface PiTool {
  name: string;
  label: string;
  description: string;
  promptSnippet: string;
  promptGuidelines: string[];
  parameters: unknown;
  execute(toolCallId: string, params: unknown, signal: AbortSignal, onUpdate: ((v: unknown) => void) | undefined, ctx: PiToolCallContext): Promise<{ content: Array<{ type: string; text: string }> }>;
}

export interface PiToolResultEvent {
  toolName: string;
  toolCallId: string;
  input?: unknown;
  content: unknown;
  isError?: boolean;
}

export interface PiToolCallEvent {
  toolName: string;
  toolCallId: string;
  input?: { cmd?: string } & Record<string, unknown>;
}

export interface PiExtensionAPI {
  registerTool(tool: PiTool): void;
  registerCommand(name: string, def: { description: string; handler: (args: string, ctx: PiToolCallContext) => Promise<void> | void }): void;
  on(event: 'session_start' | 'session_shutdown', handler: (event: unknown, ctx: PiToolCallContext) => Promise<void> | void): void;
  on(event: 'tool_result', handler: (event: PiToolResultEvent, ctx: PiToolCallContext) => Promise<void> | void): void;
  on(event: 'tool_call', handler: (event: PiToolCallEvent, ctx: PiToolCallContext) => Promise<void> | void): void;
  /** Pi cross-extension event bus (other extensions in the same process, such as pi-relay, can interoperate) */
  events?: PiEventsBus;
}

export interface WatcherExtensionOptions {
  rootDir?: string;
  sourceRoots?: Map<string, string>;
  allowedSourceIds?: readonly string[];
  pollTickMs?: number;
  /** relay integration: auto-detected by default (enabled when the pi-relay home exists); false disables it explicitly */
  relay?: boolean | { relayHome?: string; realm?: string; consumerProfileId?: string; sourceId?: string };
  /** exec_command exit-marker injection (on by default; passive helper, creates no watch; PI_WATCHER_EXEC_MARKER=0 disables it); the injected __EXEC_EXIT__ lets the exec log be covered by a declarative pattern watch */
  execMarkerInjection?: boolean;
  /** automatically watch this session's long-running exec sessions (off by default; the choice belongs to the agent, which pins log_path via watcher action=watch-file; PI_WATCHER_AUTO_EXEC=1 enables it, converting automatically to a file watch on the exec log) */
  autoExecWatch?: boolean;
}

type AckAction = 'received' | 'investigating' | 'defer' | 'resolved' | 'dismiss';


const errorText = (e: unknown): string => (e instanceof Error ? e.message : String(e));

const isRootLockHeld = (e: unknown): boolean =>
  typeof e === 'object' && e !== null && (e as { code?: unknown }).code === 'ROOT_LOCK_HELD';

/** tool_result event content to plain text (handles both the array-of-blocks and string forms). */
const contentText = (content: unknown): string => {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content
      .map((p): string => {
        if (typeof p === 'string') return p;
        if (p && typeof p === 'object' && typeof (p as { text?: unknown }).text === 'string') return (p as { text: string }).text;
        return '';
      })
      .join('\n');
  }
  return '';
};

/** Minimal structural type of the Pi cross-extension event bus (pi.events). */
export interface PiEventsBus {
  on(event: string, listener: (data: unknown) => void): (() => void) | void;
  emit(event: string, data: unknown): void;
}

/** watcher -> relay auto-bind request (event contract v2). */
export interface RelayBindRequest {
  requestId: string;
  source: 'pi-watcher';
  projectRoot: string;
  /** v2: invite-free local standing binding (localTrust channel) */
  kind?: 'invite' | 'local';
  sourceId?: string;
  channelId?: string;
  realm?: string;
  /** v1 (and v2 fallback): invite path */
  invitePath?: string;
}

/** relay -> watcher bind result. */
export interface RelayBindResult {
  requestId: string;
  ok: boolean;
  bindingId?: string;
  armed?: boolean;
  error?: { code?: string; message?: string };
}

/** Primary-side widget snapshot (supports per-session isolation; pure rendering). */
const widgetSnapshotOf = (rt: WatcherRuntime, ownerSession?: string): string[] => {
  const snap = rt.store.transaction(tx => {
    const rawWatches = ownerSession
      ? tx.listWatches(ownerSession, undefined, 200)
      : tx.listAllWatches(200);
    return {
      watches: rawWatches.map(w => ({
        lifecycle: w.lifecycle,
        health: w.health,
        deadlineAt: w.spec.mission.deadlineAt ?? null
      })),
      unresolvedEpisodes: tx.countUnresolvedEpisodes(ownerSession),
      pendingAttentions: tx.countPendingAttentions(Date.now(), ownerSession)
    };
  });
  return renderWatcherWidget({ ...snap, now: Date.now() });
};

interface WatcherBackend {
  /** Returns ServiceResult JSON text (same shape as local tool output). */
  toolCall(params: ToolParams, actor: ActorContext): Promise<string>;
  panel(actor: ActorContext): Promise<Json>;
  localAck(req: { requestId: string; episodeId: string; action: AckAction; note: string; until?: string }, actor: ActorContext): Promise<string>;
  widgetLines(): string[];
  requestWidgetRefresh(): void;
  /** Diagnose and, if needed, self-heal the relay channel (closed after resume -> new audienceRef/scopeId) */
  healRelay(): Promise<unknown>;
  close(): void;
}

// A real Pi ExtensionContext exposes session identity through sessionManager. Measured on pi 1.0.0: there is no direct sessionId
// field (all ExtensionContext fields in extensions/types.d.ts checked); the first branch is a defensive forward probe;
// identity derivation is handled by sessionManager.getSessionId() (the ReadonlySessionManager Pick set includes this method).
// Identity is derived from the trusted context; model parameters cannot specify it (I11).
const sessionIdOf = (ctx: PiToolCallContext): string =>
  ctx.sessionId
  ?? ctx.sessionManager?.getSessionId?.()
  ?? 'pi-unknown-session';

/** sessionId -> filesystem-safe segment (per-session root directory name). */
const sessionDirOf = (sid: string): string =>
  sid.replace(/[^A-Za-z0-9._-]+/g, '_').slice(0, 64) || 'session';

/**
 * Session backend: local runtime, no IPC (per-session root, decision 2026-09-21).
 * This session is the only process on its own root, so the single-writer invariant holds naturally; no primary/attached,
 * no failover; version skew (an old primary serving an old action surface) is structurally impossible.
 */
/**
 * Toast level for an attention notice. A clean success is informational; anything that needs a
 * closer look (failure, cancellation, unknown exit, deadline, semantic candidate) or a broken
 * relay wake leg stays a warning.
 */
export function attentionNotifyLevel(notice: Pick<AttentionNotice, 'reasonCode' | 'transport' | 'taskState'>): 'info' | 'warning' {
  if (notice.transport === 'relay-failed') return 'warning';
  return notice.reasonCode === 'task.terminal' && notice.taskState === 'succeeded' ? 'info' : 'warning';
}

class SessionBackend implements WatcherBackend {
  private closed = false;

  constructor(
    private readonly rt: WatcherRuntime,
    private readonly refreshWidget: () => void,
    private readonly notifyUser: (text: string, level: 'info' | 'warning') => void,
    private readonly sessionId: string
  ) {}

  /** Engine attention hook: toast to this session (under a per-session root the owner is always itself; the filter is kept defensively). */
  handleAttention(notice: AttentionNotice): void {
    const tail = notice.transport === 'local-display'
      ? ' (local-display: call watcher inspect; no auto wake)'
      : notice.transport === 'relay-failed'
        ? ` (relay wake FAILED: ${notice.relayError ?? 'unknown error'} — call watcher inspect; no auto wake)`
        : '';
    const text = `pi-watcher attention [${notice.reasonCode}] watch ${notice.watchId}: ${notice.summary}` + tail;
    if (!notice.ownerSession || notice.ownerSession === this.sessionId) {
      try { this.notifyUser(text, attentionNotifyLevel(notice)); } catch { /* display failure does not block */ }
    }
  }

  widgetLines(): string[] {
    return widgetSnapshotOf(this.rt, this.sessionId);
  }

  async healRelay(): Promise<unknown> {
    return this.rt.ensureRelayReady();
  }

  async toolCall(params: ToolParams, actor: ActorContext): Promise<string> {
    return JSON.stringify(await toServiceResultAsync(params.requestId, () => runToolAction(this.rt, params, actor)));
  }

  async panel(actor: ActorContext): Promise<Json> {
    return this.rt.service.panel(actor);
  }

  async localAck(req: { requestId: string; episodeId: string; action: AckAction; note: string; until?: string }, actor: ActorContext): Promise<string> {
    return JSON.stringify(await toServiceResultAsync(req.requestId, () =>
      this.rt.service.ackEpisode(req.requestId, req.episodeId, req.action, req.note, req.until, actor)));
  }

  requestWidgetRefresh(): void {
    this.refreshWidget();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.rt.close();
  }
}

export default function watcherExtension(pi: PiExtensionAPI, options: WatcherExtensionOptions = {}): void {
  let backend: WatcherBackend | null = null;
  // Latest Pi model registry seen on any context: Jev resolves credentials from it lazily.
  let latestRegistry: PiClassifierRegistry | undefined;
  const noteRegistry = (ctx: PiToolCallContext | undefined): void => {
    if (ctx?.modelRegistry && typeof ctx.modelRegistry.classify === 'function') latestRegistry = ctx.modelRegistry;
  };
  let backendError: unknown = null;
  let sessionClosed = false;
  let widgetCtx: PiToolCallContext | null = null;

  const resolveOptions = (ctx: PiToolCallContext): Required<Pick<WatcherExtensionOptions, 'rootDir'>> & WatcherExtensionOptions => {
    // per-session root (decision 2026-09-21): each session owns sessions/<sessionId>/ exclusively,
    // and is the only process on it; no cross-process attach, version skew is structurally impossible.
    // options.rootDir now means the parent root (default <cwd>/.pi-watcher).
    const parent = options.rootDir ?? `${ctx.cwd}/.pi-watcher`;
    const rootDir = pathMod.join(parent, 'sessions', sessionDirOf(sessionIdOf(ctx)));
    const sourceRoots = options.sourceRoots ?? new Map<string, string>([
      ['executor-local', `${ctx.cwd}/.pi-watcher-sources`]
    ]);
    return { ...options, rootDir, sourceRoots };
  };

  // Status bar widget (design 11 §33 / 08 §35): ordinary progress goes to the widget, not into the LLM context.
  // Rendered immediately after each sweep (each session's own runtime).
  const refreshWidget = (): void => {
    const ctx = widgetCtx;
    if (!backend || !ctx?.ui?.setWidget) return;
    try {
      ctx.ui.setWidget('watcher', backend.widgetLines());
    } catch {
      /* display failure does not affect the engine */
    }
  };

  /**
   * relay auto-enable: on when pi-relay is installed (default home exists), otherwise stay local-display.
   * Without relay the watcher can only display, not wake, so the agent is forced into babysitting polling.
   * PI_WATCHER_RELAY=0 disables it explicitly; options.relay takes precedence.
   */
  const resolveRelay = (): boolean | { relayHome?: string; realm?: string; consumerProfileId?: string; sourceId?: string } => {
    if (options.relay !== undefined) return options.relay;
    if (process.env?.PI_WATCHER_RELAY === '0') return false;
    const relayHome = process.env?.PI_RELAY_HOME ?? pathMod.join(os.homedir(), '.pi', 'relay');
    return fs.existsSync(relayHome) ? { relayHome } : false;
  };

  const actorOf = (ctx: PiToolCallContext): ActorContext => ({
    actorId: `pi-session:${sessionIdOf(ctx)}`,
    owner: {
      sessionId: sessionIdOf(ctx),
      originAnchor: ctx.sessionManager?.getSessionFile?.() ?? null,
      bindingEpoch: 1,
      profileId: 'default-local'
    },
    profileRevision: 1,
    permitted: new Set(['watcher.register', 'watcher.list', 'watcher.inspect', 'watcher.check', 'watcher.control'])
  });

  /**
   * Lazily ensure the backend (single-flight serialization): start this session's own runtime (per-session root).
   * No lock contention, no attach, no failover; this session is the only process on its own root.
   * ROOT_LOCK_HELD only appears when another process holds the same sessionId (e.g. the same session opened twice),
   * in which case we report it honestly and keep retrying lazily.
   */
  const runEnsure = async (ctx: PiToolCallContext): Promise<void> => {
    noteRegistry(ctx);
    if (backend || sessionClosed) return;
    const opts = resolveOptions(ctx);
    try {
      const primarySid = sessionIdOf(ctx);
      let attentionSink: ((n: AttentionNotice) => void) | null = null;
      const rt = await startRuntime({
        rootDir: opts.rootDir,
        sourceRoots: opts.sourceRoots ?? new Map(),
        allowedSourceIds: opts.allowedSourceIds,
        pollTickMs: opts.pollTickMs,
        relay: resolveRelay(),
        piRegistry: () => latestRegistry,
        onAttention: n => { try { attentionSink?.(n); } catch { /* display failure does not block the engine */ } },
        onJudgment: j => {
          if (j.ownerSession && primarySid && j.ownerSession !== primarySid) return;
          try {
            const top = j.candidates.length
              ? j.candidates.map(c => `${c.reason} p=${c.probability.toFixed(2)}${c.note ? ` (${c.note})` : ''}`).join('; ')
              : 'no action needed';
            ctx.ui?.notify?.(`pi-watcher: jev reviewed (${j.mode}) "${j.objective}" — ${top}`, 'info');
          } catch { /* display failure does not block the engine */ }
        }
      });
      const be = new SessionBackend(rt, refreshWidget, (text, level) => { ctx.ui?.notify?.(text, level); }, primarySid);
      attentionSink = n => be.handleAttention(n);
      if (sessionClosed) { be.close(); return; }
      backend = be;
      backendError = null;
      rt.startLoop(ctx.signal, refreshWidget);
      refreshWidget();
    } catch (e) {
      if (!sessionClosed) backendError = e;
    }
  };

  /** Serialize ensure: a chained promise, no shared mutable in-flight variable. */
  let ensureChain: Promise<void> = Promise.resolve();
  const ensureBackend = (ctx: PiToolCallContext): Promise<void> => {
    widgetCtx ??= ctx;
    const next = ensureChain.then(() => runEnsure(ctx));
    ensureChain = next;
    return next;
  };

  pi.on('session_start', async (_event, ctx) => {
    sessionClosed = false;
    await ensureBackend(ctx);
    if (!backend && backendError) {
      const msg = isRootLockHeld(backendError)
        ? `pi-watcher: this session's watcher root is locked by another process with the same session id (duplicate session process? ${errorText(backendError)}); every watcher call retries`
        : `pi-watcher: embedded startup failed (${errorText(backendError)}); will retry on next watcher call`;
      ctx.ui?.notify?.(msg, 'warning');
    }
    // Last link of the autonomous loop: relay not negotiated but owner already ran relay-setup (invite present) -> request relay binding for this session
    void autoBindRelay(ctx).catch(() => { /* bind side-path failure does not affect the session */ });
  });

  /** relay auto-bind: owner pre-authorized (relay-setup produced an invite) but the session is unbound -> request this session's relay binding via pi.events. */
  const autoBindPending = new Map<string, (r: RelayBindResult) => void>();
  let autoBindResultOff: (() => void) | undefined;
  const ensureAutoBindListener = (): void => {
    if (autoBindResultOff || !pi.events) return;
    const off = pi.events.on('pi-relay:bind-result', (data) => {
      const r = data as RelayBindResult;
      if (!r || typeof r.requestId !== 'string') return;
      const resolve = autoBindPending.get(r.requestId);
      if (resolve) { autoBindPending.delete(r.requestId); resolve(r); }
    });
    autoBindResultOff = typeof off === 'function' ? off : undefined;
  };

  /** Send one bind-request and wait for the reply (timeout -> null). kind selects the v2 local / v1 invite payload. */
  const emitBindRequest = (
    bus: PiEventsBus, requestId: string,
    kind: 'local' | 'invite', payload: { realm: string; sourceId: string; channelId: string; projectRoot: string; invitePath?: string }
  ): Promise<RelayBindResult | null> => new Promise(resolve => {
    autoBindPending.set(requestId, resolve);
    const timer = setTimeout(() => {
      if (autoBindPending.delete(requestId)) resolve(null);
    }, 3000);
    timer.unref?.();
    try {
      const req: RelayBindRequest = kind === 'local'
        ? { kind: 'local', requestId, source: 'pi-watcher', sourceId: payload.sourceId, channelId: payload.channelId, realm: payload.realm, projectRoot: payload.projectRoot }
        : { kind: 'invite', requestId, source: 'pi-watcher', invitePath: payload.invitePath!, projectRoot: payload.projectRoot };
      bus.emit('pi-relay:bind-request', req);
    } catch {
      clearTimeout(timer);
      if (autoBindPending.delete(requestId)) resolve(null);
    }
  });

  const autoBindRelay = async (ctx: PiToolCallContext): Promise<void> => {
    const bus = pi.events;
    if (!bus) return;                                   // host has no event bus (old pi)
    if (options.relay === false) return;                // relay explicitly disabled
    if (process.env?.PI_WATCHER_AUTO_BIND === '0') return; // auto-bind explicitly disabled
    if (!backend) return;                               // runtime unavailable: retry on the next session event
    const opts = resolveOptions(ctx);
    const relayConf = resolveRelay();
    const realm = typeof relayConf === 'object' && relayConf.realm ? relayConf.realm : 'local';
    // Exactly the same derivation as runtime.ts uses when opening the source store (shared helper), otherwise we would bind to a nonexistent source
    const relaySourceId = typeof relayConf === 'object' && relayConf.sourceId ? relayConf.sourceId : deriveSourceId(opts.rootDir);
    ensureAutoBindListener();
    const sleep = (ms: number): Promise<void> => new Promise(r => { const t = setTimeout(r, ms); t.unref?.(); });
    const delays = [0, 1500, 4000];
    const sid = sessionIdOf(ctx);

    // -- Phase 1: v2 local standing binding (invite-free, idempotent, held by each session separately) --
    // Not gated on negotiation state: when a shared runtime is already on relay, a new session still needs its own standing
    // binding (otherwise this session has no wake channel after the host session exits; standing has no TTL/overlap limit, so the cost is zero)

    // -- Phase 1: v2 local standing binding (invite-free; capability probe = send v2 directly) --
    for (let attempt = 0; attempt < delays.length; attempt++) {
      if (sessionClosed) return;
      if (delays[attempt]! > 0) await sleep(delays[attempt]!);
      if (sessionClosed || !backend) return;
      const result = await emitBindRequest(bus, `auto-bind-${sid}-l${attempt}`, 'local', {
        realm, sourceId: relaySourceId, channelId: 'W', projectRoot: opts.rootDir
      });
      if (!result) continue; // no reply (relay not loaded / not attached) -> back off and retry
      if (result.ok) {
        ctx.ui?.notify?.(`pi-watcher: relay standing binding active for this session${result.bindingId ? ` (${result.bindingId})` : ''}${result.armed === false ? ' [not armed]' : ' [sessionScoped wake]'} — watcher attention will now wake this session`, 'info');
        // resume self-heal: diagnose/reconfigure right after a successful bind (ensures the frozen routeSet contains the new binding's
        // active membership; pid-GC already cleared dead rows at enroll)
        void backend?.healRelay().catch(() => { /* retry on the next session event */ });
        return;
      }
      const code = result.error?.code ?? '';
      // capability/authorization unsupported -> fall back to invite (v1 path)
      if (/local_trust_disabled|unsupported_feature|source_not_found|invalid_payload/i.test(code)) break;
      if (/not.?attached|no.?host|attach/i.test(`${code} ${result.error?.message ?? ''}`)) continue; // relay not ready yet -> retry
      ctx.ui?.notify?.(`pi-watcher: relay standing auto-bind failed (${code || 'unknown'}: ${result.error?.message ?? ''}) — falling back to invite path`, 'warning');
      break;
    }

    // -- Phase 2: v1 invite fallback (only when relay is not yet negotiated and owner pre-authorized: relay-setup has an unconsumed invite) --
    try {
      const listRaw = JSON.parse(await backend.toolCall({ action: 'list' }, actorOf(ctx))) as {
        ok?: boolean; value?: { relay?: string };
      };
      if (listRaw?.value?.relay === 'relay') return; // already negotiated: invite path not needed
    } catch { return; }
    let invitePath: string | undefined;
    try {
      const setupCandidates = [
        pathMod.join(opts.rootDir, 'relay', 'relay-setup.json'),
        pathMod.join(opts.rootDir, 'relay-setup.json'),
        // per-session root (decision 2026-09-21): owner pre-authorization is a project-level artifact ->
        // the parent root (two levels above <parent>/sessions/<sid>)
        pathMod.join(pathMod.dirname(pathMod.dirname(opts.rootDir)), 'relay-setup.json')
      ];
      const setupPath = setupCandidates.find(p => fs.existsSync(p));
      if (setupPath) {
        const setup = JSON.parse(fs.readFileSync(setupPath, 'utf8')) as { inviteFile?: string; invitePath?: string };
        const candidate = setup.inviteFile ?? setup.invitePath;
        if (candidate && fs.existsSync(candidate)) {
          try {
            const parsed = JSON.parse(fs.readFileSync(candidate, 'utf8')) as { expiresAt?: number };
            // Expired stale invite (e.g. an old store invite left over from yesterday): do not send an invalid bind, to avoid a store_unavailable error
            if (typeof parsed.expiresAt !== 'number' || parsed.expiresAt > Date.now()) {
              invitePath = candidate;
            }
          } catch {
            invitePath = candidate;
          }
        }
      }
    } catch { /* no setup -> no fallback */ }
    if (!invitePath || !fs.existsSync(invitePath)) return;
    for (let attempt = 0; attempt < delays.length; attempt++) {
      if (sessionClosed) return;
      if (delays[attempt]! > 0) await sleep(delays[attempt]!);
      if (sessionClosed || !backend) return;
      const result = await emitBindRequest(bus, `auto-bind-${sid}-i${attempt}`, 'invite', {
        realm, sourceId: relaySourceId, channelId: 'W', projectRoot: opts.rootDir, invitePath
      });
      if (!result) continue;
      if (result.ok) {
        ctx.ui?.notify?.(`pi-watcher: relay wake bound to this session${result.bindingId ? ` (${result.bindingId})` : ''}${result.armed === false ? ' [not armed]' : ' [armed]'} — watcher attention will now wake this session`, 'info');
        return;
      }
      // A single-use invite already consumed by another session -> treat as "bound elsewhere", no retry and no error
      const code = result.error?.code ?? '';
      if (/invite|consumed|used|already/i.test(`${code} ${result.error?.message ?? ''}`)) {
        ctx.ui?.notify?.(`pi-watcher: relay invite already used by another session — wakes go there; relay_bindings list shows bindings`, 'info');
        return;
      }
      if (/not.?attached|no.?host|attach/i.test(`${code} ${result.error?.message ?? ''}`)) continue; // relay not ready yet -> retry
      ctx.ui?.notify?.(`pi-watcher: relay auto-bind failed (${code || 'unknown'}: ${result.error?.message ?? ''}) — use relay_bindings bind manually`, 'warning');
      return;
    }
  };

  pi.on('session_shutdown', async () => {
    await ensureChain.catch(() => { /* serialization: do not wait for intermediate results */ });
    sessionClosed = true;
    const be = backend;
    backend = null;
    backendError = null;
    widgetCtx = null;
    be?.close();
  });

  // -- Autonomous registration: long-running exec sessions automatically get a watch (no reliance on the model remembering to call watcher register) --
  // Trigger signal: this session's exec_command / write_stdin result contains a session_id that is still running.
  // Attribution is exact: the tool_result event fires in the extension instance of the session that made the call.
  const autoExecEnabled = options.autoExecWatch
    ?? (typeof process !== 'undefined' ? process.env?.PI_WATCHER_AUTO_EXEC === '1' : false);

  // Exit-marker injection: a watched exec session without a marker has an undecidable outcome (the marker is not a pi standard; pi-unified-exec
  // does not write the exit code to the log). tool_call patches in `echo __EXEC_EXIT__:$?` in place: passive helper, creates no watch, zero noise.
  const markerInjectionEnabled = options.execMarkerInjection
    ?? (typeof process !== 'undefined' ? process.env?.PI_WATCHER_EXEC_MARKER !== '0' : true);
  pi.on('tool_call', (event) => {
    if (!markerInjectionEnabled || sessionClosed) return;
    if (event.toolName !== 'exec_command') return;
    const input = event.input as { cmd?: unknown } | undefined;
    if (!input || typeof input.cmd !== 'string') return;
    if (input.cmd.includes('__EXEC_EXIT__')) return;  // model already included it -> do not duplicate
    const trimmed = input.cmd.replace(/\s+$/, '');
    input.cmd = `${trimmed}\necho __EXEC_EXIT__:$?`;
  });

  pi.on('tool_result', (event, ctx) => {
    if (!autoExecEnabled || sessionClosed) return;
    if (event.toolName !== 'exec_command' && event.toolName !== 'write_stdin') return;
    // async side-path: does not block tool result delivery, fails silently (the next event / manual register is the fallback)
    void autoWatchExec(event, ctx).catch(() => { /* autonomous behavior must not interfere with the tool chain */ });
  });

  const autoWatchExec = async (event: PiToolResultEvent, ctx: PiToolCallContext): Promise<void> => {
    const text = contentText(event.content);
    if (/\[exited\]/.test(text)) return; // already finished: nothing to observe
    const sid = /session_id:\s*(\d+)/.exec(text)?.[1];
    if (!sid) return;
    // universal monitor mode: pin the log_path from the exec result directly (the exact artifact the agent already holds),
    // using a hit on the injected marker pattern as the declarative terminal state (the marker is a pi-watcher convention, not a fact about external libraries)
    const logPath = /log_path:\s*(\S+\.log)/.exec(text)?.[1];
    if (!logPath) return; // no path to pin: do not act on our own
    await ensureBackend(ctx);
    const be = backend;
    if (!be) return;
    const actor = actorOf(ctx);
    // Dedup: if this owner already has an active watch on the same log path, do not create another (list projection runId = path + pattern digest)
    const listRaw = JSON.parse(await be.toolCall({ action: 'list' }, actor)) as {
      ok?: boolean; value?: { watches?: Array<{ runId?: string; lifecycle?: string }> ; relay?: string };
    };
    const watches = listRaw?.value?.watches ?? [];
    const transport = listRaw?.value?.relay === 'relay' ? 'relay' : 'local-display';
    const candidate = {
      mode: 'embedded',
      target: {
        kind: 'run', sourceId: 'agent-file', taskId: 'file',
        runId: `auto-exec-${sid}`, attemptId: 'attempt-1',
        file: {
          path: logPath,
          okPattern: '__EXEC_EXIT__:0',
          failPattern: '__EXEC_EXIT__:-?[1-9]'
        }
      },
      mission: {
        objective: `auto: shell session ${sid} (exec log)`,
        scope: `tail exec log ${logPath}; read-only`,
        checkpointId: 'auto-exec',
        requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required'
      },
      policy: {
        transport,
        semanticMode: 'off', // autonomous watch never sends egress to Jev (three-consent model)
        notificationOwner: 'watcher',
        requestKinds: [],
        maxRequestsPerEpisode: 1,
        episodeCooldownMs: 60000,
        attentionTtlMs: DEFAULT_ATTENTION_TTL_MS
      },
      limits: {
        pollMinMs: 1000, pollMaxMs: 5000,
        maxSilenceMs: 600000,
        maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10,
        expiresAt: new Date(Date.now() + 12 * 3600_000).toISOString()
      }
    };
    // Idempotent: requestId is stable; skip if an active watch with the same runId exists
    const digestRunId = candidate.target.runId;
    if (watches.some(w => w.runId === digestRunId && (w.lifecycle === 'active' || w.lifecycle === 'paused'))) return;
    const regRaw = JSON.parse(await be.toolCall({
      action: 'register', requestId: `auto-exec-${sid}`, candidate
    }, actor)) as { ok?: boolean; value?: { watchId?: string } };
    if (regRaw?.ok && regRaw.value?.watchId) {
      ctx.ui?.notify?.(`pi-watcher: auto-watching shell session ${sid} log (${regRaw.value.watchId}, ${transport}) — progress via watcher inspect/check`, 'info');
    }
  };

  /** /watcher jev: which Jev source is active, and whether egress consent is granted. */
  const jevStatusText = async (ctx: PiToolCallContext): Promise<string> => {
    noteRegistry(ctx);
    const consent = jevConsentState();
    let source: string;
    if (process.env?.JEV_API_KEY) {
      source = 'JEV_API_KEY (direct TypeSafe API, jev-1.13.0)';
    } else {
      const status = latestRegistry ? await new PiRegistryJudge(() => latestRegistry).ready() : { ready: false, reason: 'this Pi version exposes no model registry to extensions' };
      source = status.ready ? `Pi model registry: ${status.model}` : `none: ${status.reason ?? 'unavailable'}`;
    }
    return [
      'pi-watcher semantic review (Jev, optional)',
      `  model source: ${source}`,
      `  egress consent: ${consent.granted ? 'granted' : 'not granted'} (${consent.source === 'env' ? 'JEV_CONSENT env' : consent.source === 'stored' ? consentFilePath() : 'default'})`,
      '  enable: /login a Jev provider (or set TYPESAFE_API_KEY / JEV_API_KEY), then /watcher jev consent on;',
      '  watches opt in per watch with semanticMode=shadow|active.'
    ].join('\n');
  };

  pi.registerCommand('watcher', {
    description: 'pi-watcher panel: watches, health, open issues, and why no attention was sent. /watcher ack <episodeId> <received|investigating|defer|resolved|dismiss> [until ISO] — local owner response. /watcher jev [status] | /watcher jev consent <on|off> — optional Jev semantic review',
    handler: async (args: string, ctx: PiToolCallContext) => {
      await ensureBackend(ctx);
      const be = backend;
      if (!be) {
        ctx.ui?.notify?.(`pi-watcher unavailable: ${errorText(backendError) || 'not started'}`, 'warning');
        return;
      }
      const argv = (args ?? '').trim().split(/\s+/).filter(Boolean);
      if (argv[0] === 'jev') {
        // User-only: egress consent is never settable from the model-facing tool (I24, design 10.2).
        if (argv[1] === 'consent' && (argv[2] === 'on' || argv[2] === 'off')) {
          setStoredJevConsent(argv[2] === 'on');
          const after = jevConsentState();
          ctx.ui?.notify?.(
            `pi-watcher: Jev egress consent ${argv[2] === 'on' ? 'granted' : 'revoked'} (stored in ${consentFilePath()})` +
            (after.source === 'env' ? `; note: JEV_CONSENT in the environment overrides it (effective: ${after.granted ? 'on' : 'off'})` : ''),
            'info'
          );
          return;
        }
        if (argv[1] && argv[1] !== 'status') {
          ctx.ui?.notify?.('Usage: /watcher jev [status] | /watcher jev consent <on|off>', 'warning');
          return;
        }
        ctx.ui?.notify?.(await jevStatusText(ctx), 'info');
        return;
      }
      if (argv[0] === 'ack') {
        // Local owner panel ACK (non-model path; does not claim relay delivery, design 11.7 + I05/I21)
        const [, episodeId, action, until, ...rest] = argv;
        const allowed = ['received', 'investigating', 'defer', 'resolved', 'dismiss'];
        if (!episodeId || !action || !allowed.includes(action)) {
          ctx.ui?.notify?.('Usage: /watcher ack <episodeId> <received|investigating|defer|resolved|dismiss> [until ISO] [reason...]', 'warning');
          return;
        }
        const result = await be.localAck(
          {
            requestId: `ack-${episodeId}-${Date.now()}`,
            episodeId,
            action: action as AckAction,
            note: rest.join(' ') || `local owner ${action}`,
            until: until && !Number.isNaN(Date.parse(until)) ? until : undefined
          },
          actorOf(ctx)
        );
        const parsed = JSON.parse(result) as { ok?: boolean };
        ctx.ui?.notify?.(result, parsed.ok === false ? 'error' : 'info');
        return;
      }
      const panel = await be.panel(actorOf(ctx));
      const text = JSON.stringify(panel, null, 2);
      ctx.ui?.notify?.(text.length > 4000 ? `${text.slice(0, 4000)}…` : text, 'info');
    }
  });

  pi.registerTool({
    name: 'watcher',
    label: 'Watcher',
    description:
      'Fact-driven monitoring for long-running tasks — the replacement for polling loops, sleep waits, and scheduled checks. ' +
      'Actions: watch-file/watch-check/register/list/inspect/check/pause/close/ack/update. watch-file tails an agent-declared file ' +
      '(e.g. the log_path of a running exec session, a build log, a report) with declared okPattern/failPattern as terminal evidence; ' +
      'watch-check runs an agent-declared bounded command (CI/cloud/remote state) on a fixed interval with a declared verdict mapping. ' +
      'Watches observe task facts (run state, deadlines, silence, ' +
      'obligations, dependencies) and optionally invoke Jev (a fast, lightweight discriminator model) for semantic progress monitoring. ' +
      'list returns active watches by default with a terminalCount summary of finished (closed/expired) history; pass includeClosed=true to list full history. ' +
      'Wakes this session via pi-relay only when a fact requires a decision. ' +
      'Each session runs its own watcher runtime under .pi-watcher/sessions/<id>/ — no cross-session coupling, plugin upgrades never skew. ' +
      'Never executes business actions, never arms, binds, or force-wakes. ack requires a relay delivery credential ' +
      '(returns NO_DELIVERY while the relay managed path is not negotiated).',
    promptSnippet: 'watcher: fact-driven monitoring for long-running tasks — replaces polling loops, sleep waits, and scheduled checks; wakes this session via pi-relay when facts change',
    promptGuidelines: [
      'Prefer watcher over any polling pattern: whenever you would run a watch/sleep loop, retry with delays, schedule a future check ("check back in 10 minutes"), or repeatedly poll a running unified-exec session with write_stdin, register a watch instead — it observes the task, tracks deadlines and silence, and wakes this session via pi-relay only when a fact requires a decision. To check progress on a watched task at any time, call watcher(action: "inspect") or watcher(action: "check") — it returns the live status, latest output tail, and health without blocking. Never busy-poll the underlying process with write_stdin, sleep loops, or ad-hoc terminal reads.',
      'You decide what to watch — point the watcher at durable evidence. To watch a long-running shell command, first run it with exec_command (it yields session_id + log_path once it outlives the attach window), then call watcher action=watch-file with path=<that log_path>, okPattern="__EXEC_EXIT__:0", failPattern="__EXEC_EXIT__:-?[1-9]" (the exit marker is auto-injected into your command). Growth is tracked as progress, the declared patterns fire terminal facts, silence/deadline surface stuck tasks, and terminal facts auto-close after the attention TTL. By default semanticMode is "off" (pure fact tracking); pass semanticMode="shadow" to have Jev review progress/loops/blockers in the background without waking, or "active" to have Jev wake this session via relay when an impasse or decision-required blocker occurs. Check progress anytime with watcher inspect/check (live status, output tail, health) — never write_stdin-poll a watched session. Use the full action=register ONLY for custom policy (group targets, specific deadlines/silence budgets beyond the simple overrides).',
      'For multi-step pipelines (long sequences of short commands), do NOT shepherd each step with attached waits — run the whole sequence as ONE long-running command or script (append `; echo __EXEC_EXIT__:$?`) and watch its log file instead.',
      'When the user asks to monitor, watch, or keep an eye on a background task (builds, tests, training runs, long jobs), call watcher with action=watch-file on its log/output file, or action=watch-check when there is no local file — never spawn while/sleep loops or detached pollers.',
      'Use watcher action=watch-file for any state that materializes as a growing file: exec session logs (path from the exec result), build/test output redirected to a file, generated reports and artifacts. Declare the verdict honestly: okPattern (success) and failPattern (failure, checked first) on the file tail are the ONLY content-terminal facts — without patterns you get progress/silence facts only, and a missing or rotated file degrades health honestly (never a fabricated verdict).',
      'Use Jev lightweight semantic progress monitoring (semanticMode="shadow"|"active") when task status cannot be judged by exit code or pattern alone — e.g. long builds, model training, migrations, multi-step pipelines, or flaky test suites. Jev is a fast, lightweight System 1 model (~300ms latency, zero autoregressive LLM overhead) that semantically inspects log windows for forward progress, repeating loops without new info, and unresolved blockers. Use "shadow" for quiet background review (status visible in inspect/widget) or "active" to wake this session via relay when an impasse or blocker occurs.',
      'Use watcher action=watch-check to watch any remote or file-less state you cannot tail locally — CI build conclusion (gh run view), cloud resource readiness (kubectl/gcloud), service health (curl). Declare the verdict honestly: okPattern/failCodes/failPattern map outcomes to terminal facts; unmapped nonzero exits are pending (not failure); exit 126/127/timeout mean the CHECK is broken (degraded, never a task verdict). The watcher runs the command on a fixed interval — do NOT run it yourself in a loop. For blocking waits (kubectl wait) prefer exec_command + a log file + action=watch-file.',
      'Call watcher with action=inspect to query or report the status of any registered watch — it holds the authoritative fact base (observations, episodes, health); do not guess the state and do not probe with ad-hoc bash when a watch exists.',
      'When a pi-relay managed delivery wakes this session: call watcher inspect for that watch first, report or decide using the fresh facts, then call relay_respond (received/investigating/defer/resolved/dismiss) with the deliveryRef to close the loop.',
      'When the user cancels, abandons, or changes a monitored task, call watcher with action=pause or action=close so stale watches stop generating attention.',
      'Use watches for obligations and dependencies too: kind=obligation watches fire on facts (dependencies terminal/succeeded, readyWhen) instead of timers, and defer responses resurface the same episode at the snooze deadline without re-waking.',
    ],
    parameters: Type.Object({
      action: Type.Union([
        Type.Literal('watch-file'),
        Type.Literal('watch-check'),
        Type.Literal('register'),
        Type.Literal('list'),
        Type.Literal('inspect'),
        Type.Literal('check'),
        Type.Literal('ack'),
        Type.Literal('pause'),
        Type.Literal('close'),
        Type.Literal('update')
      ]),
      path: Type.Optional(Type.String({ description: 'For action=watch-file: absolute path of the file to tail (e.g. the log_path from an exec_command result, a build log, a report). Relative paths resolve against the session cwd' })),
      okPattern: Type.Optional(Type.String({ description: 'RegExp meaning terminal success. watch-file: matched against the file tail (for an exec log: __EXEC_EXIT__:0). watch-check: matched against command stdout (for CLIs that always exit 0; without it, exit 0 = succeeded)' })),
      failPattern: Type.Optional(Type.String({ description: 'RegExp meaning terminal failure, checked before okPattern. watch-file: file tail (for an exec log: __EXEC_EXIT__:-?[1-9]). watch-check: command stdout, even when exit 0' })),
      cmd: Type.Optional(Type.String({ description: 'For action=watch-check: the bounded check command to run periodically (max 2000 chars; NO secrets — it is persisted as evidence). Exit code/stdout carries the verdict per okPattern/failPattern/failCodes; non-mapped nonzero exits mean pending, not failure' })),
      intervalMs: Type.Optional(Type.Integer({ description: 'For action=watch-check: fixed run interval (default 30000, clamped 1000-600000)' })),
      timeoutMs: Type.Optional(Type.Integer({ description: 'For action=watch-check: per-run timeout with kill (default 10000, clamped 1000-30000)' })),
      failCodes: Type.Optional(Type.Array(Type.Integer(), { description: 'For action=watch-check: exit codes meaning explicit failure (default: only patterns decide failure)' })),
      objective: Type.Optional(Type.String({ description: 'For action=watch-file/watch-check: one-line purpose of the watched target' })),
      deadlineAt: Type.Optional(Type.String({ description: 'For action=watch-file/watch-check: optional business deadline (ISO)' })),
      maxSilenceMs: Type.Optional(Type.Integer({ description: 'For action=watch-file/watch-check: optional silence budget (default 600000 for files, 1800000 for checks)' })),
      includeClosed: Type.Optional(Type.Boolean({
        description:
          'For action=list: include terminal (closed/expired) watches in the response. ' +
          'Default returns only active/paused watches plus a terminalCount summary — finished watches are history, not decision inputs.'
      })),
      semanticMode: Type.Optional(Type.Union([Type.Literal('off'), Type.Literal('shadow'), Type.Literal('active')], {
        description:
          'For action=watch-file/watch-check: semantic progress monitoring via Jev (a fast, lightweight discriminator model, not an LLM). ' +
          'off (default): pure fact tracking. ' +
          'shadow: lightweight model semantically monitors output logs in background for progress, repeating loops, and blockers without waking. ' +
          'active: lightweight model monitors semantic progress and wakes this session via pi-relay when an impasse, repeating loop, or host decision is needed.'
      })),
      requestId: Type.Optional(Type.String({ description: 'Idempotency key (required for register/pause/close)' })),
      watchId: Type.Optional(Type.String({ description: 'Watch id starting with w- (required for inspect/check/pause/close)' })),
      expectedControlRevision: Type.Optional(Type.Integer({ description: 'Current controlRevision of the watch (required for check/pause/close)' })),
      reason: Type.Optional(Type.String({ description: 'Human-readable reason (required for pause/close)' })),
      candidate: Type.Optional(Type.Record(Type.String(), Type.Unknown(), { description: 'RegisterCandidate spec (required for register): target, mission, policy, limits' }))
    }),
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      await ensureBackend(ctx);
      let be = backend;
      if (!be) {
        const lockHeld = isRootLockHeld(backendError);
        const base = `pi-watcher runtime unavailable: ${errorText(backendError) || 'not started'}`;
        const message = lockHeld
          ? `${base} — another process holds this session's watcher root (duplicate session process?); acquisition is retried on every watcher call`
          : base;
        return {
          content: [{ type: 'text', text: JSON.stringify({ ok: false, error: { code: lockHeld ? 'ROOT_LOCK_HELD' : 'STORE_UNAVAILABLE', message } }) }]
        };
      }
      const actor = actorOf(ctx);
      // cwd injection: watch-file relative path resolution / watch-check command working directory (decision record 2026-09-21)
      const injectable = params as { action?: string; cwd?: string };
      if ((injectable.action === 'watch-file' || injectable.action === 'watch-check') && injectable.cwd === undefined) {
        injectable.cwd = ctx.cwd;
      }
      const runTool = (b: WatcherBackend): Promise<string> => b.toolCall(params as unknown as ToolParams, actor);
      // per-session root: no cross-process failover path; tool failures are thrown up honestly
      const text = await runTool(be);
      return { content: [{ type: 'text', text }] };
    }
  });
}
