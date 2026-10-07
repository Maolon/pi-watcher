-- pi-watcher WatchStore schema (schema_version 1).
-- Applied only when creating a NEW database; it is not a migration script.

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
CREATE TABLE runtime_meta (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  schema_version INTEGER NOT NULL CHECK(schema_version=1),
  runtime_epoch INTEGER NOT NULL CHECK(runtime_epoch>0),
  mode TEXT NOT NULL CHECK(mode IN ('embedded','service')),
  recovery_attention_hold INTEGER NOT NULL DEFAULT 1 CHECK(recovery_attention_hold IN (0,1))
);
CREATE TABLE watches (
  watch_id TEXT PRIMARY KEY,
  generation INTEGER NOT NULL CHECK(generation>0),
  mission_revision INTEGER NOT NULL CHECK(mission_revision>0),
  control_revision INTEGER NOT NULL CHECK(control_revision>0),
  lifecycle TEXT NOT NULL CHECK(lifecycle IN ('draft','active','paused','closed','expired')),
  health TEXT NOT NULL CHECK(health IN ('healthy','degraded','blind')),
  owner_session TEXT NOT NULL,
  owner_binding_epoch INTEGER NOT NULL CHECK(owner_binding_epoch>0),
  spec_json TEXT NOT NULL CHECK(json_valid(spec_json)),
  snapshot_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(snapshot_json)),
  observation_seq INTEGER NOT NULL DEFAULT 0 CHECK(observation_seq>=0),
  next_due_at INTEGER,
  updated_at INTEGER NOT NULL
);
CREATE TABLE evidence (
  evidence_id TEXT PRIMARY KEY,
  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  sha256 TEXT NOT NULL CHECK(length(sha256)=64),
  byte_count INTEGER NOT NULL CHECK(byte_count>=0),
  relative_locator TEXT NOT NULL,
  captured_at INTEGER NOT NULL,
  pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned IN (0,1))
);
CREATE TABLE observations (
  observation_id TEXT PRIMARY KEY,
  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  generation INTEGER NOT NULL,
  source_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL CHECK(source_seq>=0),
  local_seq INTEGER NOT NULL CHECK(local_seq>0),
  observed_at INTEGER NOT NULL,
  digest TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
  UNIQUE(watch_id,generation,source_id,attempt_id,source_seq),
  UNIQUE(watch_id,local_seq)
);
CREATE TABLE cursors (
  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  source_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  cursor_json TEXT NOT NULL CHECK(json_valid(cursor_json)),
  PRIMARY KEY(watch_id,source_id,generation)
);
CREATE TABLE episodes (
  episode_id TEXT PRIMARY KEY,
  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  generation INTEGER NOT NULL,
  kind TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  revision INTEGER NOT NULL CHECK(revision>0),
  state TEXT NOT NULL CHECK(state IN ('open','snoozed','acknowledged','resolved','superseded')),
  body_json TEXT NOT NULL CHECK(json_valid(body_json)),
  snooze_until INTEGER,
  updated_at INTEGER NOT NULL,
  UNIQUE(watch_id,generation,kind,checkpoint_id,ordinal)
);
CREATE UNIQUE INDEX one_active_episode_per_slot ON episodes(watch_id,generation,kind,checkpoint_id)
  WHERE state IN ('open','snoozed','acknowledged');
CREATE TABLE judgments (
  judgment_id TEXT PRIMARY KEY,
  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  generation INTEGER NOT NULL,
  mission_revision INTEGER NOT NULL,
  control_revision INTEGER NOT NULL,
  window_digest TEXT NOT NULL,
  model TEXT NOT NULL,
  question_set TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','inflight','accepted','discarded','failed')),
  data_json TEXT NOT NULL CHECK(json_valid(data_json)),
  created_at INTEGER NOT NULL
);
CREATE TABLE probe_runs (
  probe_run_id TEXT PRIMARY KEY,
  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  episode_id TEXT REFERENCES episodes(episode_id),
  generation INTEGER NOT NULL,
  control_revision INTEGER NOT NULL,
  probe_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','inflight','done','discarded','failed')),
  data_json TEXT NOT NULL CHECK(json_valid(data_json)),
  created_at INTEGER NOT NULL
);
CREATE TABLE outbox (
  event_id TEXT PRIMARY KEY,
  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  episode_id TEXT REFERENCES episodes(episode_id),
  generation INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  event_bytes BLOB NOT NULL,
  event_digest TEXT NOT NULL,
  valid_until INTEGER NOT NULL,
  admission TEXT NOT NULL CHECK(admission IN ('pending','publishing','source-staged','empty-audience','unknown','rejected','expired')),
  admission_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(admission_json)),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TRIGGER outbox_identity_immutable BEFORE UPDATE OF event_id,event_bytes,event_digest,valid_until ON outbox
WHEN OLD.event_id IS NOT NEW.event_id OR OLD.event_bytes IS NOT NEW.event_bytes
 OR OLD.event_digest IS NOT NEW.event_digest OR OLD.valid_until IS NOT NEW.valid_until
BEGIN SELECT RAISE(ABORT,'immutable outbox identity'); END;
CREATE TABLE host_acks (
  request_id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
  expected_revision INTEGER NOT NULL,
  action TEXT NOT NULL CHECK(action IN ('received','investigating','defer','resolved','dismiss')),
  actor_binding_epoch INTEGER NOT NULL,
  digest TEXT NOT NULL,
  response_json TEXT NOT NULL CHECK(json_valid(response_json)),
  created_at INTEGER NOT NULL
);
CREATE TABLE commands (
  request_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  digest TEXT NOT NULL,
  response_json TEXT NOT NULL CHECK(json_valid(response_json)),
  committed_at INTEGER NOT NULL,
  PRIMARY KEY(actor_id,request_id)
);
CREATE TABLE budget_reservations (
  reservation_id TEXT PRIMARY KEY,
  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  category TEXT NOT NULL CHECK(category IN ('judge','probe','attention')),
  period_key TEXT NOT NULL,
  units INTEGER NOT NULL CHECK(units>0),
  state TEXT NOT NULL CHECK(state IN ('reserved','spent','unknown-cost','released')),
  created_at INTEGER NOT NULL
);
CREATE INDEX due_watches ON watches(next_due_at) WHERE lifecycle='active';
CREATE TABLE control_outbox (
  operation_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  scope_id TEXT NOT NULL, expected_revision INTEGER NOT NULL, next_revision INTEGER NOT NULL,
  requested_state TEXT NOT NULL CHECK(requested_state IN ('active','paused','closed')),
  status TEXT NOT NULL CHECK(status IN ('pending','sending','source-applied','complete','unknown','rejected')),
  result_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(result_json)), created_at INTEGER NOT NULL,
  CHECK(next_revision=expected_revision+1)
);
CREATE TABLE applied_responses (
  response_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL REFERENCES watches(watch_id),
  episode_id TEXT NOT NULL REFERENCES episodes(episode_id), delivery_ref TEXT NOT NULL,
  digest TEXT NOT NULL, result_json TEXT NOT NULL CHECK(json_valid(result_json)), applied_at INTEGER NOT NULL
);
CREATE TABLE applied_confirmation_outbox (
  operation_id TEXT PRIMARY KEY, response_id TEXT NOT NULL UNIQUE REFERENCES applied_responses(response_id),
  status TEXT NOT NULL CHECK(status IN ('pending','sending','confirmed','unknown')), result_json TEXT NOT NULL CHECK(json_valid(result_json))
);
CREATE TABLE relay_projections (
  event_id TEXT PRIMARY KEY REFERENCES outbox(event_id), source_cursor INTEGER NOT NULL,
  snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)), observed_at INTEGER NOT NULL
);
CREATE TABLE relay_cursors (stream_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL CHECK(cursor>=0));
