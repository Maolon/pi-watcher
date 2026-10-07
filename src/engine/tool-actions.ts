/**
 * Action dispatch + LLM projection layer for the watcher tool (design 11.4 service surface).
 * Reused by two backends: the local primary calls it directly; attached sessions run it in the primary over IPC.
 *
 * Projection principle: the model only needs the minimum facts required for decisions -- do not echo the spec it submitted,
 * and do not expose internal bookkeeping fields (owner path / digest / generation / receipt JSON).
 * Full facts remain in SQLite and the /watcher panel (human debugging view).
 */

import type { Json, RegisterCandidate, ActorContext } from '../contracts/interfaces.js';
import type { WatcherRuntime } from '../runtime.js';
import { checkRunId } from '../source/agent-check/adapter.js';
import { fileRunId } from '../source/agent-file/adapter.js';
import { DEFAULT_ATTENTION_TTL_MS } from './engine.js';
import * as pathMod from 'node:path';

export interface ToolParams {
  action: string;
  requestId?: string;
  watchId?: string;
  expectedControlRevision?: number;
  reason?: string;
  candidate?: Record<string, unknown>;
  /** action=watch-file / watch-check (generic evidence source declared by the agent, decision-delta 2026-09-21) */
  path?: string;
  cwd?: string;
  okPattern?: string;
  failPattern?: string;
  /** action=watch-check */
  cmd?: string;
  intervalMs?: number;
  timeoutMs?: number;
  failCodes?: number[];
  objective?: string;
  deadlineAt?: string;
  maxSilenceMs?: number;
  semanticMode?: 'off' | 'shadow' | 'active';
  /** action=list: explicitly include terminal-state (closed/expired) watches; by default only active watches + a terminalCount summary are returned */
  includeClosed?: boolean;
}

/** Active lifecycle filter set shared by list/watch dedup */
const ACTIVE_LIFECYCLES: Array<'active' | 'paused'> = ['active', 'paused'];

// ---- Defensive read helpers ----
type Rec = Record<string, unknown>;
const rec = (v: unknown): Rec => (v !== null && typeof v === 'object' ? v as Rec : {});
const str = (v: unknown): string | undefined => (typeof v === 'string' && v !== '' ? v : undefined);
const num = (v: unknown): number | undefined => (typeof v === 'number' ? v : undefined);
const arr = (v: unknown): unknown[] => (Array.isArray(v) ? v : []);
const compact = (v: Rec): Json => JSON.parse(JSON.stringify(v)) as Json;  // drop undefined

function projectList(v: unknown): Json {
  const r = rec(v);
  const watches = arr(r.items).map(it => {
    const w = rec(it); const t = rec(w.target);
    return {
      watchId: str(w.watchId),
      runId: str(t.runId),
      taskState: str(w.taskState) ?? 'unknown',
      lifecycle: str(w.lifecycle),
      health: str(w.health),
      openEpisodes: num(w.openEpisodes) ?? 0
    };
  });
  const out: Rec = {
    watches,
    terminalCount: num(r.terminalCount) ?? 0,
    relay: str(rec(r.relay).transport) ?? 'local-display'
  };
  const next = str(r.nextCursor);
  if (next) out.nextCursor = next;
  return out as Json;
}

/**
 * Log tail formatting and hard limits (design 11.4 evidence economy):
 * 1. Filter out non-string lines; take at most the last 8 lines.
 * 2. Truncate each line to 300 characters (prevents oversized JSON / ARGV / stack traces from blowing up context).
 * 3. Hard-truncate total volume to 1000 characters (if over, keep the newest tail and add a truncation marker).
 * 4. Fully replaces the old tailLines array to avoid echoing twice.
 */
function formatLatestOutput(rawLines: unknown[]): string | undefined {
  const lines = rawLines.filter((l): l is string => typeof l === 'string');
  if (lines.length === 0) return undefined;
  const sliced = lines.slice(-8).map(l => (l.length > 300 ? l.slice(0, 297) + '...' : l));
  const joined = sliced.join('\n');
  if (joined.length <= 1000) return joined;
  return '[... output truncated ...]\n' + joined.slice(-900);
}

function projectInspect(v: unknown): Json {
  const r = rec(v);
  const spec = rec(r.watch);
  const mission = rec(spec.mission);
  const snap = rec(r.snapshot);

  const openEpisodes = arr(r.episodes)
    .filter(e0 => rec(e0).state === 'open')
    .map(e0 => {
      const e = rec(e0); const b = rec(e.body);
      return compact({
        kind: str(e.kind),
        reason: str(b.reasonCode) ?? str(b.summary) ?? str(b.taskState)
      });
    });

  const latestOutput = formatLatestOutput(arr(snap.tailLines));
  const obj = str(mission.objective);
  const objective = obj && obj.length > 100 ? obj.slice(0, 97) + '...' : obj;

  return compact({
    watchId: str(spec.watchId),
    objective,
    taskState: str(snap.taskState) ?? 'unknown',
    exitCode: num(snap.exitCode),
    stage: str(snap.stage),
    summary: str(snap.summary),
    latestOutput,
    lifecycle: str(r.lifecycle),
    health: str(r.health),
    degradedReason: str(snap.degradedReason),
    terminalAt: num(snap.terminalAt) !== undefined ? new Date(num(snap.terminalAt)!).toISOString() : undefined,
    lastObservedAt: num(snap.lastObservedAtMs) !== undefined ? new Date(num(snap.lastObservedAtMs)!).toISOString() : undefined,
    episodes: openEpisodes.length > 0 ? openEpisodes : undefined,
    controlRevision: num(spec.controlRevision)
  });
}

function projectControl(v: unknown): Json {
  const r = rec(v);
  return compact({
    watchId: str(r.watchId),
    lifecycle: str(r.lifecycle),
    controlRevision: num(r.controlRevision)
  });
}

export async function runToolAction(rt: WatcherRuntime, params: ToolParams, actor: ActorContext): Promise<Json> {
  switch (params.action) {
    case 'register': {
      if (!params.requestId || !params.candidate) {
        throw Object.assign(new Error('register requires requestId and candidate'), { code: 'INVALID_SPEC' });
      }
      // Finalize on demand before transport=relay registration (mid-session bind scenario)
      if ((params.candidate as { policy?: { transport?: string } })?.policy?.transport === 'relay') {
        await rt.ensureRelayReady();
      }
      const spec = await rt.service.register(
        params.requestId as string,
        params.candidate as unknown as RegisterCandidate,
        actor
      );
      const isRelay = (spec.policy as { transport?: string }).transport === 'relay';
      return {
        watchId: spec.watchId,
        controlRevision: spec.controlRevision,
        monitoringActive: true,
        transport: isRelay ? 'relay' : 'local-display',
        note: isRelay
          ? 'Watch active (relay wake enabled). Do NOT poll with write_stdin.'
          : 'Watch active (local-display). Do NOT poll with write_stdin.'
      } as unknown as Json;
    }
    case 'list': {
      // Owner decision 2026-09-21: in-session exec->watch loops accumulate many closed/failed terminal rows; echoing them all
      // just burns model context (33 of 35 watches were converged history). By default only active (active/paused)
      // watches + a terminalCount summary are returned; the model sees the full history only with explicit includeClosed=true.
      const result = await rt.service.list(undefined, 50, actor, params.includeClosed ? undefined : ACTIVE_LIFECYCLES);
      return projectList(result);
    }
    case 'watch-file': {
      // Agent-declared file evidence source (universal monitor mode, decision-delta 2026-09-21):
      // the agent pins an exact path (log_path from an exec result, a build artifact, a report file);
      // the declarative pattern is the only content terminal-state evidence; growth means progress; silence is a stuck signal.
      // Replaces the removed unified-exec adapter: no external-library naming heuristics, no runId namespace collisions.
      if (!params.requestId || typeof params.path !== 'string' || params.path.trim() === '') {
        throw Object.assign(new Error('watch-file requires requestId and path (absolute file path, e.g. the log_path from an exec_command result)'), { code: 'INVALID_SPEC' });
      }
      for (const [p, name] of [[params.okPattern, 'okPattern'], [params.failPattern, 'failPattern']] as Array<[string | undefined, string]>) {
        if (p !== undefined) {
          try { new RegExp(p); } catch (e) {
            throw Object.assign(new Error(`${name} is not a valid RegExp: ${e instanceof Error ? e.message : String(e)}`), { code: 'INVALID_SPEC' });
          }
        }
      }
      // Relative paths resolve against the declaring session's cwd (injected by the extension layer)
      const filePath = pathMod.isAbsolute(params.path)
        ? params.path
        : (typeof params.cwd === 'string' && params.cwd !== '' ? pathMod.resolve(params.cwd, params.path) : pathMod.resolve(params.path));
      const runId = fileRunId(filePath, { okPattern: params.okPattern, failPattern: params.failPattern });
      // Dedup: an active watch with the same path + same pattern is returned directly (idempotent; keeps terminal history from crowding out the limit window)
      const existing = await rt.service.list(undefined, 50, actor, ACTIVE_LIFECYCLES);
      const prior = (existing as unknown as { items?: Array<{ target?: { runId?: string }; watchId?: string; lifecycle?: string; controlRevision?: number }> })
        .items?.find(w => w.target?.runId === runId && (w.lifecycle === 'active' || w.lifecycle === 'paused'));
      if (prior?.watchId) {
        return {
          watchId: prior.watchId,
          runId,
          controlRevision: prior.controlRevision ?? 1,
          transport: rt.negotiation.transport === 'relay' ? 'relay' : 'local-display',
          note: 'Already watching this file — progress via watcher inspect/check.'
        } as unknown as Json;
      }
      const isRelay = rt.negotiation.transport === 'relay';
      const candidate = {
        mode: 'embedded',
        target: {
          kind: 'run', sourceId: 'agent-file', taskId: 'file', runId, attemptId: 'attempt-1',
          file: {
            path: filePath,
            ...(params.okPattern !== undefined ? { okPattern: params.okPattern } : {}),
            ...(params.failPattern !== undefined ? { failPattern: params.failPattern } : {})
          }
        },
        mission: {
          objective: typeof params.objective === 'string' && params.objective
            ? params.objective
            : `file watch: ${pathMod.basename(filePath)}`,
          scope: `tail declared file ${filePath}; read-only`,
          checkpointId: 'file-watch',
          requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required',
          ...(typeof params.deadlineAt === 'string' ? { deadlineAt: params.deadlineAt } : {})
        },
        policy: {
          transport: isRelay ? 'relay' : 'local-display',
          semanticMode: params.semanticMode === 'active' || params.semanticMode === 'shadow' ? params.semanticMode : 'off',
          notificationOwner: 'watcher',
          requestKinds: [],
          maxRequestsPerEpisode: 1,
          episodeCooldownMs: 60000,
          attentionTtlMs: DEFAULT_ATTENTION_TTL_MS
        },
        limits: {
          // Local stat+read is cheap: fast sampling while active, backoff while quiet (same band as the old exec watch)
          pollMinMs: 1000, pollMaxMs: 5000,
          maxSilenceMs: typeof params.maxSilenceMs === 'number' ? params.maxSilenceMs : 600000,
          maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10,
          // Round to the hour: the candidate digest stays stable when the same requestId is replayed (idempotent)
          expiresAt: new Date(Math.ceil(Date.now() / 3600_000) * 3600_000 + 12 * 3600_000).toISOString()
        }
      } as unknown as RegisterCandidate;
      const spec = await rt.service.register(params.requestId as string, candidate, actor);
      const relay = isRelay && spec.policy.transport === 'relay';
      return {
        watchId: spec.watchId,
        runId,
        path: filePath,
        controlRevision: spec.controlRevision,
        transport: relay ? 'relay' : 'local-display',
        note: relay
          ? 'Watching file via relay wake. Call watcher inspect/check anytime.'
          : 'Watching file. Call watcher inspect/check anytime.'
      } as unknown as Json;
    }
    case 'watch-check': {
      // Bounded check command declared by the agent (decision-delta 2026-09-21): covers scenarios with no local artifact
      // (CI build, cloud readiness, remote state). The watcher runs cmd periodically and maps results to facts via the declarative verdict mapping;
      // a non-zero exit code is three-way ambiguous (failed/pending/broken), so the agent declares the mapping and the watcher does not guess.
      if (!params.requestId || typeof params.cmd !== 'string' || params.cmd.trim() === '') {
        throw Object.assign(new Error('watch-check requires requestId and cmd'), { code: 'INVALID_SPEC' });
      }
      if (params.cmd.length > 2000) {
        throw Object.assign(new Error('cmd too long (max 2000 chars); put longer logic in a script file and check it'), { code: 'INVALID_SPEC' });
      }
      const patterns: Array<[string | undefined, string]> = [
        [params.okPattern, 'okPattern'],
        [params.failPattern, 'failPattern']
      ];
      for (const [p, name] of patterns) {
        if (p !== undefined) {
          try { new RegExp(p); } catch (e) {
            throw Object.assign(new Error(`${name} is not a valid RegExp: ${e instanceof Error ? e.message : String(e)}`), { code: 'INVALID_SPEC' });
          }
        }
      }
      const intervalMs = Math.min(Math.max(Math.trunc(params.intervalMs ?? 30_000), 1_000), 600_000);
      const timeoutMs = Math.min(Math.max(Math.trunc(params.timeoutMs ?? 10_000), 1_000), 30_000);
      const cmd = params.cmd;
      const runId = checkRunId(cmd, { okPattern: params.okPattern, failPattern: params.failPattern, failCodes: params.failCodes });
      // Dedup: an active watch with the same command + same verdict mapping is returned directly (idempotent)
      const existing = await rt.service.list(undefined, 50, actor, ACTIVE_LIFECYCLES);
      const prior = (existing as unknown as { items?: Array<{ target?: { runId?: string }; watchId?: string; lifecycle?: string; controlRevision?: number }> })
        .items?.find(w => w.target?.runId === runId && (w.lifecycle === 'active' || w.lifecycle === 'paused'));
      if (prior?.watchId) {
        return {
          watchId: prior.watchId,
          runId,
          controlRevision: prior.controlRevision ?? 1,
          transport: rt.negotiation.transport === 'relay' ? 'relay' : 'local-display',
          note: 'Already watching this check command — progress via watcher inspect/check.'
        } as unknown as Json;
      }
      const isRelay = rt.negotiation.transport === 'relay';
      const candidate = {
        mode: 'embedded',
        target: {
          kind: 'run', sourceId: 'agent-check', taskId: 'check', runId, attemptId: 'attempt-1',
          check: {
            cmd,
            ...(typeof params.cwd === 'string' && params.cwd !== '' ? { cwd: params.cwd } : {}),
            timeoutMs,
            ...(params.okPattern !== undefined ? { okPattern: params.okPattern } : {}),
            ...(params.failPattern !== undefined ? { failPattern: params.failPattern } : {}),
            ...(Array.isArray(params.failCodes) ? { failCodes: params.failCodes } : {})
          }
        },
        mission: {
          objective: typeof params.objective === 'string' && params.objective ? params.objective : `agent check: ${cmd.slice(0, 100)}`,
          scope: 'run agent-declared bounded check command; read-only intent',
          checkpointId: 'check-watch',
          requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required',
          ...(typeof params.deadlineAt === 'string' ? { deadlineAt: params.deadlineAt } : {})
        },
        policy: {
          transport: isRelay ? 'relay' : 'local-display',
          semanticMode: params.semanticMode === 'active' || params.semanticMode === 'shadow' ? params.semanticMode : 'off',
          notificationOwner: 'watcher',
          requestKinds: [],
          maxRequestsPerEpisode: 1,
          episodeCooldownMs: 60_000,
          attentionTtlMs: DEFAULT_ATTENTION_TTL_MS
        },
        limits: {
          // Fixed cadence (pollMin=pollMax=interval): no new information is not a backoff signal, so the cadence stays predictable
          pollMinMs: intervalMs, pollMaxMs: intervalMs,
          // Long pending is the normal shape of a readiness check: default silence is 30min (overridable)
          maxSilenceMs: typeof params.maxSilenceMs === 'number' ? params.maxSilenceMs : 1_800_000,
          maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10,
          expiresAt: new Date(Math.ceil(Date.now() / 3600_000) * 3600_000 + 12 * 3600_000).toISOString()
        }
      } as unknown as RegisterCandidate;
      const spec = await rt.service.register(params.requestId as string, candidate, actor);
      const relay = isRelay && spec.policy.transport === 'relay';
      return {
        watchId: spec.watchId,
        runId,
        controlRevision: spec.controlRevision,
        transport: relay ? 'relay' : 'local-display',
        intervalMs,
        timeoutMs,
        note: relay
          ? 'Watching check command via relay wake. Call watcher inspect/check anytime.'
          : 'Watching check command. Call watcher inspect/check anytime.'
      } as unknown as Json;
    }
    case 'inspect': {
      if (!params.watchId) throw Object.assign(new Error('inspect requires watchId'), { code: 'INVALID_SPEC' });
      return projectInspect(await rt.service.inspect(params.watchId as string, actor));
    }
    case 'check': {
      if (!params.requestId || !params.watchId || typeof params.expectedControlRevision !== 'number') {
        throw Object.assign(new Error('check requires requestId, watchId, expectedControlRevision'), { code: 'INVALID_SPEC' });
      }
      const r = await rt.service.check(
        params.requestId as string,
        params.watchId as string,
        params.expectedControlRevision as number,
        actor
      ) as { inspectionId: string };
      const inspection = projectInspect(await rt.service.inspect(params.watchId as string, actor));
      return {
        inspectionId: r.inspectionId,
        ...(inspection as Record<string, unknown>),
        note: 'Live progress refreshed from source.'
      } as unknown as Json;
    }
    case 'pause':
    case 'close': {
      if (!params.requestId || !params.watchId || typeof params.expectedControlRevision !== 'number') {
        throw Object.assign(new Error(`${params.action} requires requestId, watchId, expectedControlRevision, reason`), { code: 'INVALID_SPEC' });
      }
      return projectControl(await rt.service.control(
        params.requestId as string,
        params.watchId as string,
        params.expectedControlRevision as number,
        params.action as 'pause' | 'close',
        (params.reason as string) ?? 'via watcher tool',
        actor
      ));
    }
    case 'ack': {
      // No relay delivery: do not fabricate a receipt out of thin air (design 11.7)
      throw Object.assign(
        new Error('ack requires a relay consumer delivery (deliveryRef); relay managed path not negotiated — use local owner panel instead'),
        { code: 'NO_DELIVERY' }
      );
    }
    case 'update': {
      throw Object.assign(new Error('update is owner-scoped and not yet implemented (V1)'), { code: 'INVALID_SPEC' });
    }
    default:
      throw Object.assign(new Error(`unknown action ${String(params.action)}`), { code: 'INVALID_SPEC' });
  }
}
