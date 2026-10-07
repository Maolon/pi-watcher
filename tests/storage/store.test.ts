import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import Database from 'better-sqlite3';
import { WatchStore } from '../../src/storage/store.js';
import { WatcherError } from '../../src/util/result.js';

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pw-store-test-'));
}

function baseSpec(watchId: string) {
  return {
    schemaVersion: 1 as const,
    watchId,
    generation: 1,
    missionRevision: 1,
    controlRevision: 1,
    mode: 'embedded' as const,
    owner: { sessionId: 's1', originAnchor: null, bindingEpoch: 1, profileId: 'p' },
    target: { kind: 'run' as const, sourceId: 'src1', taskId: 't1', runId: 'r1', attemptId: 'a1' },
    mission: { objective: 'o', scope: 's', checkpointId: 'c1', requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required' as const },
    policy: { transport: 'local-display' as const, semanticMode: 'off' as const, notificationOwner: 'watcher' as const, requestKinds: [], maxRequestsPerEpisode: 1, episodeCooldownMs: 60000, attentionTtlMs: 120000 },
    limits: { pollMinMs: 1000, pollMaxMs: 30000, maxSilenceMs: 60000, maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10, expiresAt: new Date(Date.now() + 3600_000).toISOString() }
  };
}

function insertWatchTx(store: WatchStore, watchId: string): void {
  store.transaction(tx => {
    tx.insertWatch({
      watchId, generation: 1, missionRevision: 1, controlRevision: 1,
      lifecycle: 'active', health: 'healthy', ownerSession: 's1', ownerBindingEpoch: 1,
      spec: baseSpec(watchId) as never,
      snapshot: { taskState: 'unknown', coverage: { truncated: false, sourceGap: false }, scopeRevision: 0, backoffMs: 0 } as never,
      nextDueAt: Date.now(), now: Date.now()
    });
  });
}

test('store: WAL/FULL/FK pragmas active on native better-sqlite3', async () => {
  const dir = tmpDir();
  const store = await WatchStore.open(path.join(dir, 'root'), {});
  const rep = store.pragmaReport();
  assert.equal(rep.journalMode, 'wal', 'journal_mode=WAL is persistent');
  assert.equal(rep.synchronous, 2, 'synchronous=FULL on the store connection');
  assert.equal(rep.foreignKeys, 1, 'foreign_keys=ON on the store connection');
  // WAL persistence: a separately opened read-only connection should also see wal
  const db = new Database(path.join(dir, 'root', 'watcher.db'), { readonly: true });
  assert.equal(db.pragma('journal_mode', { simple: true }), 'wal');
  db.close();
  store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('store: runtime epoch increments per open; recovery hold present', async () => {
  const dir = tmpDir();
  const s1 = await WatchStore.open(path.join(dir, 'root'), {});
  const e1 = s1.runtimeEpoch;
  const meta1 = s1.transaction(tx => tx.meta());
  assert.ok(meta1.recoveryAttentionHold === true);
  s1.close();
  const s2 = await WatchStore.open(path.join(dir, 'root'), {});
  assert.ok(s2.runtimeEpoch > e1, 'epoch must increase across opens');
  s2.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('store: outbox identity is immutable (trigger rejects mutation)', async () => {
  const dir = tmpDir();
  const store = await WatchStore.open(path.join(dir, 'root'), {});
  insertWatchTx(store, 'w1');
  store.transaction(tx => {
    tx.insertOutbox({
      eventId: 'ev1', watchId: 'w1', episodeId: null, generation: 1, eventType: 'watcher.result.v1',
      eventBytes: Buffer.from('{}'), eventDigest: 'd1', validUntil: Date.now() + 1000, now: Date.now()
    });
  });
  const db = new Database(path.join(dir, 'root', 'watcher.db'));
  assert.throws(() => db.prepare("UPDATE outbox SET event_bytes=? WHERE event_id='ev1'").run(Buffer.from('x')));
  assert.throws(() => db.prepare("UPDATE outbox SET valid_until=valid_until+1 WHERE event_id='ev1'").run());
  // admission update allowed (non-identity field)
  db.prepare("UPDATE outbox SET admission='expired' WHERE event_id='ev1'").run();
  db.close();
  store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('store: observations unique by (watch,generation,source,attempt,sourceSeq); episodes one-active-per-slot', async () => {
  const dir = tmpDir();
  const store = await WatchStore.open(path.join(dir, 'root'), {});
  insertWatchTx(store, 'w2');
  const r1 = store.transaction(tx => tx.insertObservation({
    observationId: 'o1', watchId: 'w2', generation: 1, sourceId: 'src1', attemptId: 'a1',
    sourceSeq: 5, localSeq: 1, observedAt: 1, digest: 'x', payload: { a: 1 }
  }));
  const r2 = store.transaction(tx => tx.insertObservation({
    observationId: 'o1-dup', watchId: 'w2', generation: 1, sourceId: 'src1', attemptId: 'a1',
    sourceSeq: 5, localSeq: 2, observedAt: 1, digest: 'x', payload: { a: 1 }
  }));
  assert.equal(r1, true);
  assert.equal(r2, false, 'duplicate sourceSeq must be ignored');

  store.transaction(tx => {
    tx.insertEpisode({ episodeId: 'ep1', watchId: 'w2', generation: 1, kind: 'task.failed', checkpointId: 'c1', ordinal: 1, body: {}, now: 1 });
  });
  // a second active episode in the same slot must be rejected by the unique partial index
  assert.throws(() => store.transaction(tx => {
    tx.insertEpisode({ episodeId: 'ep2', watchId: 'w2', generation: 1, kind: 'task.failed', checkpointId: 'c1', ordinal: 2, body: {}, now: 2 });
  }));
  // a new ordinal can be opened after resolved
  store.transaction(tx => {
    tx.updateEpisode('ep1', 1, { state: 'resolved' }, 3);
    tx.insertEpisode({ episodeId: 'ep3', watchId: 'w2', generation: 1, kind: 'task.failed', checkpointId: 'c1', ordinal: 3, body: {}, now: 4 });
  });
  store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('store: command idempotency — same digest replays, different digest conflicts', async () => {
  const dir = tmpDir();
  const store = await WatchStore.open(path.join(dir, 'root'), {});
  const r1 = store.transaction(tx => tx.persistCommandResult('actor1', 'req1', 'digestA', { ok: true } as never));
  assert.equal(r1.duplicate, false);
  const r2 = store.transaction(tx => tx.persistCommandResult('actor1', 'req1', 'digestA', { ok: true } as never));
  assert.equal(r2.duplicate, true);
  assert.deepEqual(r2.previous, { ok: true });
  assert.throws(() => store.transaction(tx => tx.persistCommandResult('actor1', 'req1', 'digestB', { ok: true } as never)), WatcherError);
  store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('store: reopen recovers committed watches (durable truth)', async () => {
  const dir = tmpDir();
  const s1 = await WatchStore.open(path.join(dir, 'root'), {});
  insertWatchTx(s1, 'w-durable');
  s1.close();
  const s2 = await WatchStore.open(path.join(dir, 'root'), {});
  const row = s2.transaction(tx => tx.getWatchRow('w-durable'));
  assert.ok(row);
  assert.equal(row!.spec.watchId, 'w-durable');
  s2.close();
  fs.rmSync(dir, { recursive: true, force: true });
});
