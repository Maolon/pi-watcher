/**
 * WatchStore: local truth store (design 07).
 * - SQLite WAL / synchronous=FULL / foreign_keys=ON
 * - Each process holds the root flock (fixed inode); runtimeEpoch increments on every open
 * - transaction(): synchronous, bounded transaction body (external I/O is forbidden inside)
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import type { Json, WatchSpec, Basis } from '../contracts/interfaces.js';
import { RootLock } from './lock.js';
import { WatcherError } from '../util/result.js';

const SCHEMA_SQL_PATH = fileURLToPath(new URL('./schema.sql', import.meta.url));

export interface WatchSnapshot {
  taskState: 'unknown' | 'queued' | 'running' | 'blocked' | 'succeeded' | 'failed' | 'cancelled';
  stage?: string | null;
  exitCode?: number | null;
  summary?: string | null;
  checks?: Array<{ checkId: string; outcome: string; artifactDigest: string | null }>;
  artifactIds?: string[];
  lastObservedAtMs?: number;
  lastSourceSeq?: number;
  lastResultId?: string | null;
  coverage: { truncated: boolean; sourceGap: boolean };
  degradedReason?: string | null;
  /** Legacy local counter (pre-2026-10-07); superseded by relayScope. */
  scopeRevision: number;
  /**
   * Per-watch relay scope (design 9.6): one scope per watch generation + owner bindingEpoch,
   * created lazily on the first managed attention. revision is the latest intended revision;
   * an open control_outbox row means the relay has not confirmed it yet (design 7.7).
   */
  relayScope?: { scopeId: string; revision: number; state: 'active' | 'paused' | 'closed' };
  backoffMs: number;
  terminalAt?: number | null;
  tailLines?: string[];
  /** Source evidence: the process has exited (even if the result is unknown) -- outcome-unknown terminal-state signal */
  exited?: boolean;
  /** V3 group-task projection: member state and readiness time */
  members?: Array<{ watchId: string; taskState: string; lifecycle: string }>;
  groupReadyAt?: number | null;
  /** V2 semantic-layer projection (design 5.5/5.6): shadow only records candidates */
  semantic?: {
    mode: 'shadow' | 'active';
    lastWindowDigest?: string | null;
    lastJudgedAtMs?: number | null;
    lastJudgedSourceSeq?: number | null;
    /** Local observationSeq high-water mark ("window with new observations", design 5.5) */
    lastJudgedObservationSeq?: number | null;
    repeatingStreak: number;
    meaningfulProgress?: number | null;
    candidates?: Record<string, Json>;
    consentMissing?: boolean;
    budgetExhausted?: boolean;
    error?: string | null;
  };
}

export function emptySnapshot(): WatchSnapshot {
  return {
    taskState: 'unknown',
    stage: null,
    exitCode: null,
    summary: null,
    checks: [],
    artifactIds: [],
    coverage: { truncated: false, sourceGap: false },
    scopeRevision: 0,
    backoffMs: 0
  };
}

export interface WatchRow {
  watchId: string;
  generation: number;
  missionRevision: number;
  controlRevision: number;
  lifecycle: 'draft' | 'active' | 'paused' | 'closed' | 'expired';
  health: 'healthy' | 'degraded' | 'blind';
  ownerSession: string;
  ownerBindingEpoch: number;
  spec: WatchSpec;
  snapshot: WatchSnapshot;
  observationSeq: number;
  nextDueAt: number | null;
  updatedAt: number;
}

export interface EpisodeRow {
  episodeId: string;
  watchId: string;
  generation: number;
  kind: string;
  checkpointId: string;
  ordinal: number;
  revision: number;
  state: 'open' | 'snoozed' | 'acknowledged' | 'resolved' | 'superseded';
  body: Record<string, Json>;
  snoozeUntil: number | null;
  updatedAt: number;
}

export interface OutboxRow {
  eventId: string;
  watchId: string;
  episodeId: string | null;
  generation: number;
  eventType: string;
  eventBytes: Buffer;
  eventDigest: string;
  validUntil: number;
  admission: string;
  admissionJson: Record<string, Json>;
  createdAt: number;
  updatedAt: number;
}

export interface ControlOutboxRow {
  operationId: string;
  watchId: string;
  scopeId: string;
  expectedRevision: number;
  nextRevision: number;
  requestedState: 'active' | 'paused' | 'closed';
  status: string;
  createdAt: number;
}

export interface StoreTx {
  meta(): { schemaVersion: number; runtimeEpoch: number; mode: string; recoveryAttentionHold: boolean };
  clearRecoveryHold(): void;
  getWatchRow(watchId: string): WatchRow | undefined;
  getWatch(watchId: string): WatchSpec | undefined;
  compareRevision(basis: Basis): boolean;
  persistCommandResult(actorId: string, requestId: string, digest: string, result: Json): { duplicate: boolean; previous?: Json };
  getCommand(actorId: string, requestId: string): { digest: string; response: Json } | undefined;
  insertWatch(row: {
    watchId: string; generation: number; missionRevision: number; controlRevision: number;
    lifecycle: WatchRow['lifecycle']; health: WatchRow['health']; ownerSession: string;
    ownerBindingEpoch: number; spec: WatchSpec; snapshot: WatchSnapshot; nextDueAt: number | null; now: number;
  }): void;
  updateWatch(watchId: string, patch: {
    lifecycle?: WatchRow['lifecycle']; health?: WatchRow['health']; controlRevision?: number;
    missionRevision?: number; snapshot?: WatchSnapshot; observationSeq?: number; nextDueAt?: number | null;
  }, now: number): void;
  bumpObservationSeq(watchId: string): number;
  insertObservation(input: {
    observationId: string; watchId: string; generation: number; sourceId: string; attemptId: string;
    sourceSeq: number; localSeq: number; observedAt: number; digest: string; payload: Json;
  }): boolean;
  getCursor(watchId: string, sourceId: string, generation: number): Json | undefined;
  putCursor(watchId: string, sourceId: string, generation: number, cursor: Json): void;
  nextEpisodeOrdinal(watchId: string, generation: number, kind: string, checkpointId: string): number;
  insertEpisode(input: {
    episodeId: string; watchId: string; generation: number; kind: string; checkpointId: string;
    ordinal: number; body: Record<string, Json>; now: number;
  }): void;
  getEpisode(episodeId: string): EpisodeRow | undefined;
  getActiveEpisodeBySlot(watchId: string, generation: number, kind: string, checkpointId: string): EpisodeRow | undefined;
  updateEpisode(episodeId: string, expectedRevision: number, patch: {
    state?: EpisodeRow['state']; body?: Record<string, Json>; snoozeUntil?: number | null;
  }, now: number): number | null;
  listEpisodes(watchId: string, limit: number): EpisodeRow[];
  /** When closing a watch, fold its unresolved episodes (-> superseded); returns the number of affected rows */
  supersedeEpisodesOfWatch(watchId: string, now: number): number;
  /** Sweep: watch is already closed/expired but episode is still open/snoozed/acknowledged -> superseded */
  supersedeEpisodesOfInactiveWatches(now: number): number;
  /** Sweep: attention envelope is past validUntil and still pending/publishing -> expired */
  expireStaleAttentions(now: number): number;
  insertOutbox(input: {
    eventId: string; watchId: string; episodeId: string | null; generation: number; eventType: string;
    eventBytes: Buffer; eventDigest: string; validUntil: number; admissionJson?: Record<string, Json>;
    admission?: string; now: number;
  }): void;
  setOutboxAdmission(eventId: string, admission: string, admissionJson: Record<string, Json>, now: number): void;
  getOutbox(eventId: string): OutboxRow | undefined;
  listOutboxByWatch(watchId: string, limit: number): OutboxRow[];
  insertControlOutbox(input: {
    operationId: string; watchId: string; scopeId: string; expectedRevision: number;
    nextRevision: number; requestedState: 'active' | 'paused' | 'closed'; now: number;
  }): void;
  completeControlOutbox(
    operationId: string,
    status: 'source-applied' | 'complete' | 'unknown' | 'rejected',
    result?: Record<string, Json>
  ): void;
  getControlOutboxStatus(operationId: string): string | null;
  /** Unconfirmed control ops (pending/sending/unknown), FIFO; optionally for one watch. */
  listOpenControlOutbox(watchId?: string): ControlOutboxRow[];
  /** Shallow-merges keys into an outbox row's admission_json (identity columns stay immutable). */
  mergeOutboxAdmissionJson(eventId: string, patch: Record<string, Json>, now: number): void;
  /** Attention rows whose durable withdraw intent has no recorded relay result yet. */
  listOutboxAwaitingWithdraw(limit: number): OutboxRow[];
  insertAppliedResponse(input: {
    responseId: string; watchId: string; episodeId: string; deliveryRef: string;
    digest: string; result: Record<string, Json>; now: number;
  }): void;
  getAppliedResponse(responseId: string): { responseId: string; result: Record<string, Json> } | undefined;
  insertAppliedConfirmOutbox(operationId: string, responseId: string): void;
  listObservations(watchId: string, limit: number): Array<{
    observationId: string; sourceSeq: number; localSeq: number; observedAt: number; digest: string; payload: Json;
  }>;
  listWatches(ownerSession: string, cursor: string | undefined, limit: number, lifecycles?: Array<WatchRow['lifecycle']>): WatchRow[];
  listAllWatches(limit: number): WatchRow[];
  /** Count by lifecycle (terminal summary of the list projection); no filter = all watches of that owner */
  countWatches(ownerSession: string, lifecycles?: Array<WatchRow['lifecycle']>): number;
  countOpenEpisodes(watchId: string): number;
  /** Count of unresolved episodes across the whole DB or by ownerSession (widget projection) */
  countUnresolvedEpisodes(ownerSession?: string): number;
  /** Count of attention events whose episode is not yet acknowledged by the host (open/snoozed) and whose envelope is still within its validity period (widget projection, optionally filtered by ownerSession) */
  countPendingAttentions(now: number, ownerSession?: string): number;
  // --- V2 semantic layer (tables already in contract schema.sql: judgments/probe_runs/budget_reservations) ---
  insertJudgment(input: {
    judgmentId: string; watchId: string; generation: number; missionRevision: number; controlRevision: number;
    windowDigest: string; model: string; questionSet: string; status: 'pending' | 'inflight' | 'accepted' | 'discarded' | 'failed';
    data: Record<string, Json>; createdAt: number;
  }): void;
  getJudgmentByWindow(watchId: string, generation: number, windowDigest: string, model: string, questionSet: string): { judgmentId: string; status: string; data: Record<string, Json> } | undefined;
  listJudgments(watchId: string, limit: number): Array<{ judgmentId: string; windowDigest: string; model: string; questionSet: string; status: string; data: Record<string, Json>; createdAt: number }>;
  insertProbeRun(input: {
    probeRunId: string; watchId: string; episodeId: string | null; generation: number; controlRevision: number;
    probeId: string; status: 'pending' | 'inflight' | 'done' | 'discarded' | 'failed'; data: Record<string, Json>; createdAt: number;
  }): void;
  setProbeRunStatus(probeRunId: string, status: 'pending' | 'inflight' | 'done' | 'discarded' | 'failed', data: Record<string, Json>): void;
  countBudgetReservations(category: 'judge' | 'probe' | 'attention', periodKey: string, watchId?: string): number;
  insertBudgetReservation(input: {
    reservationId: string; watchId: string; category: 'judge' | 'probe' | 'attention'; periodKey: string;
    units: number; state: 'reserved' | 'spent' | 'unknown-cost' | 'released'; createdAt: number;
  }): void;
  setBudgetReservationState(reservationId: string, state: 'reserved' | 'spent' | 'unknown-cost' | 'released'): void;
  // --- relay cursors (contract table relay_cursors) ---
  getRelayCursor(streamId: string): number | undefined;
  putRelayCursor(streamId: string, cursor: number): void;
}

export interface WatchStoreOptions {
  mode?: 'embedded' | 'service';
  /** Skip flock (single-process tests may pass false to open a second view). */
  noLock?: boolean;
}

export class WatchStore {
  readonly rootDir: string;
  readonly dbPath: string;
  readonly evidenceDir: string;
  readonly resultsDir: string;
  readonly runtimeEpoch: number;
  private readonly db: Database.Database;
  private readonly lock: RootLock | null;
  private closed = false;

  private constructor(rootDir: string, db: Database.Database, lock: RootLock | null, epoch: number) {
    this.rootDir = rootDir;
    this.dbPath = path.join(rootDir, 'watcher.db');
    this.evidenceDir = path.join(rootDir, 'evidence');
    this.resultsDir = path.join(rootDir, 'results');
    this.db = db;
    this.lock = lock;
    this.runtimeEpoch = epoch;
  }

  static async open(rootDir: string, options: WatchStoreOptions = {}): Promise<WatchStore> {
    fs.mkdirSync(rootDir, { recursive: true });
    const lock = new RootLock(rootDir);
    if (!options.noLock) {
      await lock.requireAcquire();
    }
    const dbPath = path.join(rootDir, 'watcher.db');
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = FULL');
    db.pragma('foreign_keys = ON');
    const hasMeta = (db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='runtime_meta'"
    ).get() as { name: string } | undefined) !== undefined;
    if (!hasMeta) {
      const sql = fs.readFileSync(SCHEMA_SQL_PATH, 'utf8');
      db.exec(sql);
    }
    // runtime_meta singleton row: created on first open (CHECK requires epoch>0, initialized to 1), then incremented on every process open (design 7.2 fencing)
    db.prepare(`INSERT OR IGNORE INTO runtime_meta (singleton,schema_version,runtime_epoch,mode,recovery_attention_hold)
      VALUES (1,1,1,?,1)`).run(options.mode ?? 'embedded');
    // Bump runtime epoch for this process; every commit is fenced by it.
    const bump = db.transaction(() => {
      db.prepare("UPDATE runtime_meta SET runtime_epoch = runtime_epoch + 1, mode = ? WHERE singleton = 1")
        .run(options.mode ?? 'embedded');
      const row = db.prepare('SELECT runtime_epoch FROM runtime_meta WHERE singleton = 1').get() as { runtime_epoch: number };
      return row.runtime_epoch;
    });
    const epoch = bump();
    return new WatchStore(rootDir, db, options.noLock ? null : lock, epoch);
  }

  transaction<T>(body: (tx: StoreTx) => T): T {
    if (this.closed) throw new WatcherError('STORE_UNAVAILABLE', 'store is closed');
    const fence = this.db.prepare('SELECT runtime_epoch FROM runtime_meta WHERE singleton = 1').get() as { runtime_epoch: number };
    if (fence.runtime_epoch !== this.runtimeEpoch) {
      throw new WatcherError('STORE_UNAVAILABLE', 'stale runtime epoch: this store handle is fenced out');
    }
    this.db.exec('BEGIN IMMEDIATE');
    let tx: StoreTx;
    try {
      tx = this.makeTx();
      const out = body(tx);
      this.db.exec('COMMIT');
      return out;
    } catch (e) {
      try {
        this.db.exec('ROLLBACK');
      } catch {
        /* already rolled back */
      }
      throw e;
    }
  }

  get isClosed(): boolean {
    return this.closed;
  }

  /** Connection-level pragma diagnostics (WAL persists; synchronous/foreign_keys are connection-level). */
  pragmaReport(): { journalMode: unknown; synchronous: unknown; foreignKeys: unknown } {
    return {
      journalMode: this.db.pragma('journal_mode', { simple: true }),
      synchronous: this.db.pragma('synchronous', { simple: true }),
      foreignKeys: this.db.pragma('foreign_keys', { simple: true })
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.db.close();
    if (this.lock) {
      void this.lock.release();
    }
  }

  // --- evidence blocks: content-addressed files under root/evidence ---

  writeEvidenceBlock(content: string): { sha256: string; relativeLocator: string; bytes: number } {
    const { createHash } = require('node:crypto') as typeof import('node:crypto');
    const sha = createHash('sha256').update(content, 'utf8').digest('hex');
    fs.mkdirSync(this.evidenceDir, { recursive: true });
    const locator = path.join('evidence', `${sha}.txt`);
    const abs = path.join(this.rootDir, locator);
    if (!fs.existsSync(abs)) {
      fs.writeFileSync(abs, content, { encoding: 'utf8' });
    }
    return { sha256: sha, relativeLocator: locator, bytes: Buffer.byteLength(content, 'utf8') };
  }

  writeResultCard(watchId: string, resultId: string, card: Json): string {
    const dir = path.join(this.resultsDir, watchId);
    fs.mkdirSync(dir, { recursive: true });
    const abs = path.join(dir, `${resultId}.json`);
    fs.writeFileSync(abs, JSON.stringify(card, null, 2), 'encoding' in { encoding: 'utf8' } ? 'utf8' : undefined);
    return path.relative(this.rootDir, abs);
  }

  listResultCards(watchId: string): Json[] {
    const dir = path.join(this.resultsDir, watchId);
    if (!fs.existsSync(dir)) return [];
    return fs.readdirSync(dir)
      .filter(f => f.endsWith('.json'))
      .sort()
      .map(f => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')) as Json);
  }

  // --- transaction body ---

  private makeTx(): StoreTx {
    const db = this.db;
    const selWatch = db.prepare('SELECT * FROM watches WHERE watch_id = ?');
    const insWatch = db.prepare(`INSERT INTO watches
      (watch_id,generation,mission_revision,control_revision,lifecycle,health,owner_session,owner_binding_epoch,
       spec_json,snapshot_json,observation_seq,next_due_at,updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,0,?,?)`);
    const updWatch = db.prepare(`UPDATE watches SET
      lifecycle=COALESCE(?,lifecycle), health=COALESCE(?,health), control_revision=COALESCE(?,control_revision),
      mission_revision=COALESCE(?,mission_revision), snapshot_json=COALESCE(?,snapshot_json),
      observation_seq=COALESCE(?,observation_seq), next_due_at=COALESCE(?,next_due_at), updated_at=?
      WHERE watch_id=?`);
    const insObs = db.prepare(`INSERT OR IGNORE INTO observations
      (observation_id,watch_id,generation,source_id,attempt_id,source_seq,local_seq,observed_at,digest,payload_json)
      VALUES (?,?,?,?,?,?,?,?,?,?)`);
    const tx: StoreTx = {
      meta: () => {
        const r = db.prepare('SELECT schema_version,runtime_epoch,mode,recovery_attention_hold FROM runtime_meta WHERE singleton=1')
          .get() as { schema_version: number; runtime_epoch: number; mode: string; recovery_attention_hold: number };
        return {
          schemaVersion: r.schema_version,
          runtimeEpoch: r.runtime_epoch,
          mode: r.mode,
          recoveryAttentionHold: r.recovery_attention_hold === 1
        };
      },
      clearRecoveryHold: () => {
        db.prepare('UPDATE runtime_meta SET recovery_attention_hold = 0 WHERE singleton = 1').run();
      },
      getWatchRow: (watchId) => {
        const r = selWatch.get(watchId) as Record<string, unknown> | undefined;
        return r ? rowToWatchRow(r) : undefined;
      },
      getWatch: (watchId) => tx.getWatchRow(watchId)?.spec,
      compareRevision: (basis) => {
        const r = selWatch.get(basis.watchId) as Record<string, unknown> | undefined;
        if (!r) return false;
        return (
          r.generation === basis.generation &&
          r.mission_revision === basis.missionRevision &&
          r.control_revision === basis.controlRevision &&
          basis.observationSeq <= (r.observation_seq as number)
        );
      },
      persistCommandResult: (actorId, requestId, digest, result) => {
        const existing = db.prepare('SELECT digest,response_json FROM commands WHERE actor_id=? AND request_id=?')
          .get(actorId, requestId) as { digest: string; response_json: string } | undefined;
        if (existing) {
          if (existing.digest === digest) {
            return { duplicate: true, previous: JSON.parse(existing.response_json) as Json };
          }
          throw new WatcherError('REQUEST_CONFLICT', `requestId ${requestId} replayed with different digest`);
        }
        db.prepare('INSERT INTO commands (request_id,actor_id,digest,response_json,committed_at) VALUES (?,?,?,?,?)')
          .run(requestId, actorId, digest, JSON.stringify(result), Date.now());
        return { duplicate: false };
      },
      getCommand: (actorId, requestId) => {
        const r = db.prepare('SELECT digest,response_json FROM commands WHERE actor_id=? AND request_id=?')
          .get(actorId, requestId) as { digest: string; response_json: string } | undefined;
        return r ? { digest: r.digest, response: JSON.parse(r.response_json) as Json } : undefined;
      },
      insertWatch: (row) => {
        insWatch.run(
          row.watchId, row.generation, row.missionRevision, row.controlRevision, row.lifecycle, row.health,
          row.ownerSession, row.ownerBindingEpoch, JSON.stringify(row.spec), JSON.stringify(row.snapshot),
          row.nextDueAt, row.now
        );
      },
      updateWatch: (watchId, patch, now) => {
        const cur = selWatch.get(watchId) as Record<string, unknown> | undefined;
        if (!cur) throw new WatcherError('UNKNOWN_TARGET', `watch ${watchId} not found`);
        updWatch.run(
          patch.lifecycle ?? null,
          patch.health ?? null,
          patch.controlRevision ?? null,
          patch.missionRevision ?? null,
          patch.snapshot ? JSON.stringify(patch.snapshot) : null,
          patch.observationSeq ?? null,
          patch.nextDueAt === undefined ? null : patch.nextDueAt,
          now,
          watchId
        );
      },
      bumpObservationSeq: (watchId) => {
        db.prepare('UPDATE watches SET observation_seq = observation_seq + 1 WHERE watch_id = ?').run(watchId);
        const r = selWatch.get(watchId) as { observation_seq: number };
        return r.observation_seq;
      },
      insertObservation: (input) => {
        const info = insObs.run(
          input.observationId, input.watchId, input.generation, input.sourceId, input.attemptId,
          input.sourceSeq, input.localSeq, input.observedAt, input.digest, JSON.stringify(input.payload)
        );
        return info.changes === 1;
      },
      getCursor: (watchId, sourceId, generation) => {
        const r = db.prepare('SELECT cursor_json FROM cursors WHERE watch_id=? AND source_id=? AND generation=?')
          .get(watchId, sourceId, generation) as { cursor_json: string } | undefined;
        return r ? (JSON.parse(r.cursor_json) as Json) : undefined;
      },
      putCursor: (watchId, sourceId, generation, cursor) => {
        db.prepare(`INSERT INTO cursors (watch_id,source_id,generation,cursor_json) VALUES (?,?,?,?)
          ON CONFLICT(watch_id,source_id,generation) DO UPDATE SET cursor_json=excluded.cursor_json`)
          .run(watchId, sourceId, generation, JSON.stringify(cursor));
      },
      nextEpisodeOrdinal: (watchId, generation, kind, checkpointId) => {
        const r = db.prepare(
          'SELECT COALESCE(MAX(ordinal),0)+1 AS n FROM episodes WHERE watch_id=? AND generation=? AND kind=? AND checkpoint_id=?'
        ).get(watchId, generation, kind, checkpointId) as { n: number };
        return r.n;
      },
      insertEpisode: (input) => {
        db.prepare(`INSERT INTO episodes
          (episode_id,watch_id,generation,kind,checkpoint_id,ordinal,revision,state,body_json,snooze_until,updated_at)
          VALUES (?,?,?,?,?,?,1,'open',?,NULL,?)`)
          .run(input.episodeId, input.watchId, input.generation, input.kind, input.checkpointId, input.ordinal,
            JSON.stringify(input.body), input.now);
      },
      getEpisode: (episodeId) => {
        const r = db.prepare('SELECT * FROM episodes WHERE episode_id=?').get(episodeId) as Record<string, unknown> | undefined;
        return r ? rowToEpisodeRow(r) : undefined;
      },
      getActiveEpisodeBySlot: (watchId, generation, kind, checkpointId) => {
        const r = db.prepare(`SELECT * FROM episodes
          WHERE watch_id=? AND generation=? AND kind=? AND checkpoint_id=? AND state IN ('open','snoozed','acknowledged')
          ORDER BY ordinal DESC LIMIT 1`)
          .get(watchId, generation, kind, checkpointId) as Record<string, unknown> | undefined;
        return r ? rowToEpisodeRow(r) : undefined;
      },
      updateEpisode: (episodeId, expectedRevision, patch, now) => {
        const info = db.prepare(`UPDATE episodes SET
          state=COALESCE(?,state), body_json=COALESCE(?,body_json), snooze_until=COALESCE(?,snooze_until),
          revision=revision+1, updated_at=?
          WHERE episode_id=? AND revision=?`)
          .run(
            patch.state ?? null,
            patch.body ? JSON.stringify(patch.body) : null,
            patch.snoozeUntil === undefined ? null : patch.snoozeUntil,
            now, episodeId, expectedRevision
          );
        if (info.changes !== 1) return null;
        const r = db.prepare('SELECT revision FROM episodes WHERE episode_id=?').get(episodeId) as { revision: number };
        return r.revision;
      },
      listEpisodes: (watchId, limit) => {
        const rows = db.prepare('SELECT * FROM episodes WHERE watch_id=? ORDER BY updated_at DESC LIMIT ?')
          .all(watchId, limit) as Array<Record<string, unknown>>;
        return rows.map(rowToEpisodeRow);
      },
      insertOutbox: (input) => {
        db.prepare(`INSERT INTO outbox
          (event_id,watch_id,episode_id,generation,event_type,event_bytes,event_digest,valid_until,admission,admission_json,created_at,updated_at)
          VALUES (?,?,?,?,?,?,?,?, ?, ?, ?, ?)`)
          .run(input.eventId, input.watchId, input.episodeId, input.generation, input.eventType,
            input.eventBytes, input.eventDigest, input.validUntil, input.admission ?? 'pending',
            JSON.stringify(input.admissionJson ?? {}), input.now, input.now);
      },
      setOutboxAdmission: (eventId, admission, admissionJson, now) => {
        db.prepare('UPDATE outbox SET admission=?, admission_json=?, updated_at=? WHERE event_id=?')
          .run(admission, JSON.stringify(admissionJson), now, eventId);
      },
      getOutbox: (eventId) => {
        const r = db.prepare('SELECT * FROM outbox WHERE event_id=?').get(eventId) as Record<string, unknown> | undefined;
        return r ? rowToOutboxRow(r) : undefined;
      },
      listOutboxByWatch: (watchId, limit) => {
        const rows = db.prepare('SELECT * FROM outbox WHERE watch_id=? ORDER BY created_at DESC LIMIT ?')
          .all(watchId, limit) as Array<Record<string, unknown>>;
        return rows.map(rowToOutboxRow);
      },
      insertControlOutbox: (input) => {
        db.prepare(`INSERT INTO control_outbox
          (operation_id,watch_id,scope_id,expected_revision,next_revision,requested_state,status,result_json,created_at)
          VALUES (?,?,?,?,?,?, 'pending', '{}', ?)`)
          .run(input.operationId, input.watchId, input.scopeId, input.expectedRevision, input.nextRevision,
            input.requestedState, input.now);
      },
      completeControlOutbox: (operationId, status, result) => {
        if (result) {
          db.prepare('UPDATE control_outbox SET status=?, result_json=? WHERE operation_id=?')
            .run(status, JSON.stringify(result), operationId);
        } else {
          db.prepare('UPDATE control_outbox SET status=? WHERE operation_id=?')
            .run(status, operationId);
        }
      },
      getControlOutboxStatus: (operationId) => {
        const r = db.prepare('SELECT status FROM control_outbox WHERE operation_id=?').get(operationId) as { status: string } | undefined;
        return r?.status ?? null;
      },
      listOpenControlOutbox: (watchId) => {
        const rows = (watchId
          ? db.prepare(`SELECT * FROM control_outbox WHERE watch_id=? AND status IN ('pending','sending','unknown')
              ORDER BY created_at, rowid`).all(watchId)
          : db.prepare(`SELECT * FROM control_outbox WHERE status IN ('pending','sending','unknown')
              ORDER BY created_at, rowid`).all()) as Array<Record<string, unknown>>;
        return rows.map(r => ({
          operationId: r.operation_id as string,
          watchId: r.watch_id as string,
          scopeId: r.scope_id as string,
          expectedRevision: r.expected_revision as number,
          nextRevision: r.next_revision as number,
          requestedState: r.requested_state as ControlOutboxRow['requestedState'],
          status: r.status as string,
          createdAt: r.created_at as number
        }));
      },
      mergeOutboxAdmissionJson: (eventId, patch, now) => {
        const r = db.prepare('SELECT admission_json FROM outbox WHERE event_id=?').get(eventId) as { admission_json: string } | undefined;
        if (!r) return;
        const merged = { ...(JSON.parse(r.admission_json) as Record<string, Json>), ...patch };
        db.prepare('UPDATE outbox SET admission_json=?, updated_at=? WHERE event_id=?')
          .run(JSON.stringify(merged), now, eventId);
      },
      listOutboxAwaitingWithdraw: (limit) => {
        const rows = db.prepare(`SELECT * FROM outbox WHERE event_type='watcher.attention.v1'
          AND json_extract(admission_json,'$.withdraw.operationId') IS NOT NULL
          AND json_extract(admission_json,'$.withdraw.result') IS NULL
          ORDER BY created_at LIMIT ?`).all(limit) as Array<Record<string, unknown>>;
        return rows.map(rowToOutboxRow);
      },
      insertAppliedResponse: (input) => {
        db.prepare(`INSERT INTO applied_responses
          (response_id,watch_id,episode_id,delivery_ref,digest,result_json,applied_at)
          VALUES (?,?,?,?,?,?,?)`)
          .run(input.responseId, input.watchId, input.episodeId, input.deliveryRef, input.digest,
            JSON.stringify(input.result), input.now);
      },
      getAppliedResponse: (responseId) => {
        const r = db.prepare('SELECT response_id,result_json FROM applied_responses WHERE response_id=?')
          .get(responseId) as { response_id: string; result_json: string } | undefined;
        return r ? { responseId: r.response_id, result: JSON.parse(r.result_json) as Record<string, Json> } : undefined;
      },
      insertAppliedConfirmOutbox: (operationId, responseId) => {
        db.prepare(`INSERT INTO applied_confirmation_outbox (operation_id,response_id,status,result_json)
          VALUES (?,?,'pending','{}')`).run(operationId, responseId);
      },
      listObservations: (watchId, limit) => {
        const rows = db.prepare('SELECT observation_id,source_seq,local_seq,observed_at,digest,payload_json FROM observations WHERE watch_id=? ORDER BY local_seq DESC LIMIT ?')
          .all(watchId, limit) as Array<Record<string, unknown>>;
        return rows.map(r => ({
          observationId: r.observation_id as string,
          sourceSeq: r.source_seq as number,
          localSeq: r.local_seq as number,
          observedAt: r.observed_at as number,
          digest: r.digest as string,
          payload: JSON.parse(r.payload_json as string) as Json
        }));
      },
      listWatches: (ownerSession, cursor, limit, lifecycles) => {
        const lc = lifecycles && lifecycles.length > 0 ? ` AND lifecycle IN (${lifecycles.map(() => '?').join(',')})` : '';
        const rows = (
          cursor
            ? db.prepare(`SELECT * FROM watches WHERE owner_session=? AND watch_id > ?${lc} ORDER BY watch_id LIMIT ?`)
              .all(ownerSession, cursor, ...lifecycles ?? [], limit) as Array<Record<string, unknown>>
            : db.prepare(`SELECT * FROM watches WHERE owner_session=?${lc} ORDER BY watch_id LIMIT ?`)
              .all(ownerSession, ...lifecycles ?? [], limit) as Array<Record<string, unknown>>
        );
        return rows.map(rowToWatchRow);
      },
      countWatches: (ownerSession, lifecycles) => {
        const lc = lifecycles && lifecycles.length > 0 ? ` AND lifecycle IN (${lifecycles.map(() => '?').join(',')})` : '';
        const r = db.prepare(`SELECT COUNT(*) AS n FROM watches WHERE owner_session=?${lc}`)
          .get(ownerSession, ...lifecycles ?? []) as { n: number };
        return r.n;
      },
      listAllWatches: (limit) => {
        const rows = db.prepare('SELECT * FROM watches ORDER BY watch_id LIMIT ?')
          .all(limit) as Array<Record<string, unknown>>;
        return rows.map(rowToWatchRow);
      },
      countOpenEpisodes: (watchId) => {
        const r = db.prepare(`SELECT COUNT(*) AS n FROM episodes WHERE watch_id=? AND state IN ('open','snoozed','acknowledged')`)
          .get(watchId) as { n: number };
        return r.n;
      },
      countUnresolvedEpisodes: (ownerSession?: string) => {
        if (ownerSession) {
          const r = db.prepare(`SELECT COUNT(*) AS n FROM episodes e
            JOIN watches w ON w.watch_id = e.watch_id
            WHERE e.state IN ('open','snoozed','acknowledged') AND w.owner_session=?`)
            .get(ownerSession) as { n: number };
          return r.n;
        }
        const r = db.prepare(`SELECT COUNT(*) AS n FROM episodes WHERE state IN ('open','snoozed','acknowledged')`)
          .get() as { n: number };
        return r.n;
      },
      countPendingAttentions: (now, ownerSession?: string) => {
        if (ownerSession) {
          const r = db.prepare(`SELECT COUNT(*) AS n FROM outbox o
            JOIN episodes e ON e.episode_id = o.episode_id
            JOIN watches w ON w.watch_id = e.watch_id
            WHERE o.event_type='watcher.attention.v1' AND e.state IN ('open','snoozed')
              AND o.admission != 'expired' AND o.valid_until > ? AND w.owner_session=?`)
            .get(now, ownerSession) as { n: number };
          return r.n;
        }
        const r = db.prepare(`SELECT COUNT(*) AS n FROM outbox o JOIN episodes e ON e.episode_id = o.episode_id
          WHERE o.event_type='watcher.attention.v1' AND e.state IN ('open','snoozed')
            AND o.admission != 'expired' AND o.valid_until > ?`)
          .get(now) as { n: number };
        return r.n;
      },
      supersedeEpisodesOfWatch: (watchId, now) => {
        const info = db.prepare(`UPDATE episodes SET state='superseded', revision=revision+1, updated_at=?
          WHERE watch_id=? AND state IN ('open','snoozed','acknowledged')`).run(now, watchId);
        return info.changes;
      },
      supersedeEpisodesOfInactiveWatches: (now) => {
        const info = db.prepare(`UPDATE episodes SET state='superseded', revision=revision+1, updated_at=?
          WHERE state IN ('open','snoozed','acknowledged')
            AND watch_id IN (SELECT watch_id FROM watches WHERE lifecycle IN ('closed','expired'))`).run(now);
        return info.changes;
      },
      expireStaleAttentions: (now) => {
        const info = db.prepare(`UPDATE outbox SET admission='expired',
          admission_json=json_set(admission_json, '$.note', 'attention ttl elapsed before delivery; no longer deliverable'),
          updated_at=?
          WHERE event_type='watcher.attention.v1' AND admission IN ('pending','publishing') AND valid_until < ?`).run(now, now);
        return info.changes;
      },
      insertJudgment: (input) => {
        db.prepare(`INSERT INTO judgments (judgment_id,watch_id,generation,mission_revision,control_revision,window_digest,model,question_set,status,data_json,created_at)
          VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
          .run(input.judgmentId, input.watchId, input.generation, input.missionRevision, input.controlRevision,
            input.windowDigest, input.model, input.questionSet, input.status,
            JSON.stringify(input.data), input.createdAt);
      },
      getJudgmentByWindow: (watchId, generation, windowDigest, model, questionSet) => {
        const r = db.prepare(`SELECT judgment_id,status,data_json FROM judgments
          WHERE watch_id=? AND generation=? AND window_digest=? AND model=? AND question_set=?
          ORDER BY created_at DESC LIMIT 1`)
          .get(watchId, generation, windowDigest, model, questionSet) as Record<string, unknown> | undefined;
        if (!r) return undefined;
        return { judgmentId: r.judgment_id as string, status: r.status as string, data: JSON.parse(r.data_json as string) as Record<string, Json> };
      },
      listJudgments: (watchId, limit) => {
        const rows = db.prepare(`SELECT judgment_id,window_digest,model,question_set,status,data_json,created_at FROM judgments WHERE watch_id=? ORDER BY created_at DESC LIMIT ?`)
          .all(watchId, limit) as Array<Record<string, unknown>>;
        return rows.map(r => ({
          judgmentId: r.judgment_id as string,
          windowDigest: r.window_digest as string,
          model: r.model as string,
          questionSet: r.question_set as string,
          status: r.status as string,
          data: JSON.parse(r.data_json as string) as Record<string, Json>,
          createdAt: r.created_at as number
        }));
      },
      insertProbeRun: (input) => {
        db.prepare(`INSERT INTO probe_runs (probe_run_id,watch_id,episode_id,generation,control_revision,probe_id,status,data_json,created_at)
          VALUES (?,?,?,?,?,?,?,?,?)`)
          .run(input.probeRunId, input.watchId, input.episodeId, input.generation, input.controlRevision,
            input.probeId, input.status, JSON.stringify(input.data), input.createdAt);
      },
      setProbeRunStatus: (probeRunId, status, data) => {
        db.prepare('UPDATE probe_runs SET status=?, data_json=? WHERE probe_run_id=?')
          .run(status, JSON.stringify(data), probeRunId);
      },
      countBudgetReservations: (category, periodKey, watchId) => {
        const r = (watchId
          ? db.prepare(`SELECT COUNT(*) AS n FROM budget_reservations WHERE category=? AND period_key=? AND watch_id=? AND state IN ('reserved','spent','unknown-cost')`)
            .get(category, periodKey, watchId)
          : db.prepare(`SELECT COUNT(*) AS n FROM budget_reservations WHERE category=? AND period_key=? AND state IN ('reserved','spent','unknown-cost')`)
            .get(category, periodKey)) as { n: number };
        return r.n;
      },
      insertBudgetReservation: (input) => {
        db.prepare(`INSERT INTO budget_reservations (reservation_id,watch_id,category,period_key,units,state,created_at)
          VALUES (?,?,?,?,?,?,?)`)
          .run(input.reservationId, input.watchId, input.category, input.periodKey, input.units, input.state, input.createdAt);
      },
      setBudgetReservationState: (reservationId, state) => {
        db.prepare('UPDATE budget_reservations SET state=? WHERE reservation_id=?').run(state, reservationId);
      },
      getRelayCursor: (streamId) => {
        const r = db.prepare('SELECT cursor FROM relay_cursors WHERE stream_id=?').get(streamId) as { cursor: number } | undefined;
        return r?.cursor;
      },
      putRelayCursor: (streamId, cursor) => {
        db.prepare('INSERT INTO relay_cursors (stream_id,cursor) VALUES (?,?) ON CONFLICT(stream_id) DO UPDATE SET cursor=excluded.cursor')
          .run(streamId, cursor);
      }
    };
    return tx;
  }
}

/** Actor scoping removed: actorId is passed explicitly to persistCommandResult. */

function rowToWatchRow(r: Record<string, unknown>): WatchRow {
  return {
    watchId: r.watch_id as string,
    generation: r.generation as number,
    missionRevision: r.mission_revision as number,
    controlRevision: r.control_revision as number,
    lifecycle: r.lifecycle as WatchRow['lifecycle'],
    health: r.health as WatchRow['health'],
    ownerSession: r.owner_session as string,
    ownerBindingEpoch: r.owner_binding_epoch as number,
    spec: JSON.parse(r.spec_json as string) as WatchSpec,
    snapshot: { ...emptySnapshot(), ...(JSON.parse(r.snapshot_json as string) as object) } as WatchSnapshot,
    observationSeq: r.observation_seq as number,
    nextDueAt: (r.next_due_at as number | null) ?? null,
    updatedAt: r.updated_at as number
  };
}

function rowToEpisodeRow(r: Record<string, unknown>): EpisodeRow {
  return {
    episodeId: r.episode_id as string,
    watchId: r.watch_id as string,
    generation: r.generation as number,
    kind: r.kind as string,
    checkpointId: r.checkpoint_id as string,
    ordinal: r.ordinal as number,
    revision: r.revision as number,
    state: r.state as EpisodeRow['state'],
    body: JSON.parse(r.body_json as string) as Record<string, Json>,
    snoozeUntil: (r.snooze_until as number | null) ?? null,
    updatedAt: r.updated_at as number
  };
}

function rowToOutboxRow(r: Record<string, unknown>): OutboxRow {
  return {
    eventId: r.event_id as string,
    watchId: r.watch_id as string,
    episodeId: (r.episode_id as string | null) ?? null,
    generation: r.generation as number,
    eventType: r.event_type as string,
    eventBytes: r.event_bytes as Buffer,
    eventDigest: r.event_digest as string,
    validUntil: r.valid_until as number,
    admission: r.admission as string,
    admissionJson: JSON.parse(r.admission_json as string) as Record<string, Json>,
    createdAt: r.created_at as number,
    updatedAt: r.updated_at as number
  };
}
