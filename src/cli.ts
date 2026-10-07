/**
 * pi-watcher CLI (design 11.2 shell entry point).
 * - qualify: V0 qualification check, produces qualification.json (distinguishes candidate platforms from measured platforms)
 * - doctor: local diagnostics (lock/DB/relay negotiation/panel summary)
 * - producer: runs a real async task-status-v1 fixture producer
 * - demo: V1 display-only end-to-end demo
 */

import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn, execSync, type ChildProcess } from 'node:child_process';
import { startRuntime } from './runtime.js';
import { runScenario, scenarioSteps } from './source/task-status-v1/producer.js';
import { negotiateRelay } from './relay/negotiate.js';

function parseArgs(argv: string[]): { cmd: string; flags: Map<string, string> } {
  const [cmd, ...rest] = argv;
  const flags = new Map<string, string>();
  for (let i = 0; i < rest.length; i++) {
    if (rest[i].startsWith('--')) {
      const key = rest[i].slice(2);
      const next = rest[i + 1];
      if (next && !next.startsWith('--')) {
        flags.set(key, next);
        i += 1;
      } else {
        flags.set(key, 'true');
      }
    }
  }
  return { cmd: cmd ?? '', flags };
}

async function main(): Promise<void> {
  const { cmd, flags } = parseArgs(process.argv.slice(2));
  switch (cmd) {
    case 'qualify':
      await cmdQualify(flags);
      break;
    case 'doctor':
      await cmdDoctor(flags);
      break;
    case 'producer':
      await cmdProducer(flags);
      break;
    case 'demo':
      await cmdDemo(flags);
      break;
    case 'relay-setup':
      await cmdRelaySetup(flags);
      break;
    default:
      console.log('usage: pi-watcher <qualify|doctor|producer|demo|relay-setup> [options]');
      console.log('  qualify  [--out qualification.json]       V0 qualification check, writes evidence file');
      console.log('  doctor   [--root DIR]                     local diagnostics');
      console.log('  producer --dir D --scenario build-success|build-failure [--interval 200] [--run-id run-1]');
      console.log('  demo                                       V1 display-only end-to-end demo');
      console.log('  relay-setup [--root DIR] [--relay-home DIR] [--finalize]  relay consumer-side owner setup');
      process.exit(cmd ? 2 : 0);
  }
}

// --- qualify (V0) ---

interface QualifyCheck {
  check: string;
  passed: boolean;
  detail: string;
}

async function cmdQualify(flags: Map<string, string>): Promise<void> {
  const checks: QualifyCheck[] = [];
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'pw-qual-'));

  // 1. SQLite WAL/FULL/FK (measured with native better-sqlite3)
  try {
    const Database = (await import('better-sqlite3')).default;
    const db = new Database(path.join(tmp, 'qual.db'));
    const jm = db.pragma('journal_mode = WAL', { simple: true });
    db.pragma('synchronous = FULL');
    db.pragma('foreign_keys = ON');
    const sync = db.pragma('synchronous', { simple: true });
    const fk = db.pragma('foreign_keys', { simple: true });
    db.close();
    checks.push({
      check: 'sqlite-native-wal-full-fk',
      passed: jm === 'wal' && sync === 2 && fk === 1,
      detail: `journal_mode=${jm}, synchronous=${sync} (2=FULL), foreign_keys=${fk}`
    });
  } catch (e) {
    checks.push({ check: 'sqlite-native-wal-full-fk', passed: false, detail: String(e) });
  }

  // 2. flock fixed-inode mutual exclusion (child process holds the lock)
  try {
    const lockPath = path.join(tmp, 'watcher.lock');
    fs.writeFileSync(lockPath, '', 'utf8');
    const holder = `
      const fs = require('node:fs');
      const { flock } = require('fs-ext');
      const fd = fs.openSync(${JSON.stringify(lockPath)}, 'a');
      flock(fd, 'ex', () => {
        console.log('held');
        setTimeout(() => process.exit(0), 4000);
      });
    `;
    const child = spawnNode(['-e', holder]);
    await waitStdout(child, 'held');
    const fsExt = await import('fs-ext');
    const fsMod = await import('node:fs');
    const fd2 = fsMod.openSync(lockPath, 'a');
    const rejected = await new Promise<boolean>(resolve => {
      fsExt.flock(fd2, 'exnb', (err: Error | null) => {
        resolve(!!err);
      });
    });
    fsMod.closeSync(fd2);
    child.kill('SIGKILL');
    checks.push({
      check: 'root-flock-exclusive-contention',
      passed: rejected,
      detail: rejected ? 'second exclusive non-blocking flock rejected (EAGAIN)' : 'second flock unexpectedly acquired'
    });
  } catch (e) {
    checks.push({ check: 'root-flock-exclusive-contention', passed: false, detail: String(e) });
  }

  // 3. storage.sql schema constraints (triggers + uniqueness + partial unique indexes)
  try {
    const { WatchStore } = await import('./storage/store.js');
    const store = await WatchStore.open(path.join(tmp, 'root3'), { mode: 'embedded' });
    const violations: string[] = [];
    // outbox identity immutable
    try {
      store.transaction(tx => {
        tx.insertWatch({
          watchId: 'wq', generation: 1, missionRevision: 1, controlRevision: 1,
          lifecycle: 'active', health: 'healthy', ownerSession: 's', ownerBindingEpoch: 1,
          spec: { schemaVersion: 1, watchId: 'wq', generation: 1, missionRevision: 1, controlRevision: 1, mode: 'embedded',
            owner: { sessionId: 's', originAnchor: null, bindingEpoch: 1, profileId: 'p' },
            target: { kind: 'run', sourceId: 'src', taskId: 't', runId: 'r', attemptId: 'a' },
            mission: { objective: 'o', scope: 's', checkpointId: 'c', requiredArtifacts: [], requiresChecks: false, businessAcceptance: 'not_required' },
            policy: { transport: 'local-display', semanticMode: 'off', notificationOwner: 'watcher', requestKinds: [], maxRequestsPerEpisode: 1, episodeCooldownMs: 60000, attentionTtlMs: 120000 },
            limits: { pollMinMs: 1000, pollMaxMs: 30000, maxSilenceMs: 60000, maxProbesPerEpisode: 2, maxJudgeRequestsPerDay: 10, expiresAt: new Date(Date.now() + 3600_000).toISOString() } },
          snapshot: { taskState: 'unknown', coverage: { truncated: false, sourceGap: false }, scopeRevision: 0, backoffMs: 0 },
          nextDueAt: Date.now(), now: Date.now()
        });
        tx.insertOutbox({
          eventId: 'ev1', watchId: 'wq', episodeId: null, generation: 1, eventType: 'watcher.result.v1',
          eventBytes: Buffer.from('{}'), eventDigest: 'd', validUntil: Date.now() + 1000, now: Date.now()
        });
      });
      let immutableRejected = false;
      try {
        store.transaction(tx => {
          tx.setOutboxAdmission('ev1', 'pending', {}, Date.now());
          (tx as unknown as { mutateOutbox?: () => void }).mutateOutbox?.();
        });
      } catch {
        immutableRejected = true;
      }
      if (!immutableRejected) {
        // Verify triggers directly with SQL
        const Database = (await import('better-sqlite3')).default;
        const db = new Database(path.join(tmp, 'root3', 'watcher.db'));
        try {
          db.prepare("UPDATE outbox SET event_bytes = ? WHERE event_id = ?").run(Buffer.from('{"x":1}'), 'ev1');
        } catch {
          immutableRejected = true;
        }
        db.close();
      }
      if (!immutableRejected) violations.push('outbox identity trigger did not reject mutation');
    } catch (e) {
      violations.push(`setup: ${String(e)}`);
    }
    store.close();
    checks.push({
      check: 'storage-schema-constraints',
      passed: violations.length === 0,
      detail: violations.length === 0 ? 'outbox identity immutability enforced by trigger' : violations.join('; ')
    });
  } catch (e) {
    checks.push({ check: 'storage-schema-constraints', passed: false, detail: String(e) });
  }

  // 4. relay 1.2 feature negotiation
  const negotiation = await negotiateRelay();
  checks.push({
    check: 'relay-1-2-feature-negotiation',
    passed: negotiation.status === 'unavailable' || negotiation.status === 'managed-1-2',
    detail: `status=${negotiation.status}; transport=${negotiation.transport}; ${negotiation.detail}`
  });

  // 5. runtime versions
  checks.push({
    check: 'runtime-versions',
    passed: true,
    detail: `node=${process.versions.node}, platform=${os.platform()}/${os.arch()}`
  });

  fs.rmSync(tmp, { recursive: true, force: true });
  const out = {
    kind: 'V0_QUALIFICATION',
    date: new Date().toISOString(),
    commit: gitHead(),
    machine: `${os.platform()}/${os.arch()} node ${process.versions.node}`,
    candidatePlatform: 'POSIX (darwin) local',
    testedPlatform: `${os.platform()} local`,
    checks,
    passed: checks.every(c => c.passed),
    note: 'V0 qualification only; not product acceptance (see design 14).'
  };
  const outPath = flags.get('out') ?? 'qualification/qualification.json';
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2), 'utf8');
  console.log(JSON.stringify(out, null, 2));
  process.exit(out.passed ? 0 : 5);
}

function gitHead(): string | null {
  try {
    return execSync('git rev-parse HEAD', { cwd: process.cwd(), encoding: 'utf8' }).trim();
  } catch {
    return null;
  }
}

function spawnNode(args: string[]): ChildProcess {
  return spawn(process.execPath, args, { stdio: ['ignore', 'pipe', 'inherit'] });
}

async function waitStdout(child: ChildProcess, needle: string, timeoutMs = 5000): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let buf = '';
    const timer = setTimeout(() => reject(new Error('timeout waiting for holder lock')), timeoutMs);
    child.stdout?.on('data', (chunk: Buffer) => {
      buf += chunk.toString('utf8');
      if (buf.includes(needle)) {
        clearTimeout(timer);
        resolve();
      }
    });
    child.on('exit', code => {
      clearTimeout(timer);
      reject(new Error(`holder exited early: ${code}`));
    });
  });
}

// --- doctor ---

async function cmdDoctor(flags: Map<string, string>): Promise<void> {
  const root = path.resolve(flags.get('root') ?? './.pi-watcher');
  const negotiation = await negotiateRelay();
  const report: Record<string, unknown> = {
    root,
    lockFile: fs.existsSync(path.join(root, 'watcher.lock')),
    dbFile: fs.existsSync(path.join(root, 'watcher.db')),
    relay: negotiation,
    versions: { node: process.versions.node }
  };
  console.log(JSON.stringify(report, null, 2));
}

// --- producer ---

async function cmdProducer(flags: Map<string, string>): Promise<void> {
  const dir = flags.get('dir');
  const scenario = (flags.get('scenario') ?? 'build-success') as 'build-success' | 'build-failure';
  const interval = parseInt(flags.get('interval') ?? '200', 10);
  const runId = flags.get('run-id') ?? `run-${Date.now()}`;
  if (!dir) {
    console.error('--dir required');
    process.exit(2);
  }
  const producer = await runScenario(
    dir,
    { sourceId: 'executor-local', taskId: 'build', runId, attemptId: 'attempt-1' },
    scenarioSteps(scenario, interval)
  );
  console.log(JSON.stringify({ done: true, scenario, dir, runId, lastSnapshot: producer.lastSnapshot }, null, 2));
}

// --- demo (V1 display-only) ---

async function cmdDemo(_flags: Map<string, string>): Promise<void> {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'pw-demo-'));
  const sourceRoot = path.join(tmp, 'sources');
  const rootDir = path.join(tmp, 'state');
  const runtime = await startRuntime({
    rootDir,
    sourceRoots: new Map([['executor-local', sourceRoot]]),
    requiredCheckIds: new Map([['executor-local', ['unit-tests']]]),
    pollTickMs: 100
  });
  runtime.startLoop();
  const scenarioP = runScenario(
    sourceRoot,
    { sourceId: 'executor-local', taskId: 'build', runId: 'run-demo-1', attemptId: 'attempt-1' },
    scenarioSteps('build-failure', 150)
  );
  await scenarioP;
  await new Promise(r => setTimeout(r, 700));
  const actor = {
    actorId: 'demo',
    owner: { sessionId: 'demo-session', originAnchor: null, bindingEpoch: 1, profileId: 'demo' },
    profileRevision: 1,
    permitted: new Set<string>()
  };
  const spec = await runtime.service.register(`req-${Date.now()}`, {
    schemaVersion: 1,
    mode: 'embedded',
    target: { kind: 'run', sourceId: 'executor-local', taskId: 'build', runId: 'run-demo-1', attemptId: 'attempt-1' },
    mission: {
      objective: 'Run the existing async build task; observe status and report on failure or deliverable results.',
      scope: 'Observe run-demo-1 only; do not modify, retry, or terminate the build.',
      checkpointId: 'compile-and-test',
      requiredArtifacts: ['artifact-app'],
      requiresChecks: true,
      businessAcceptance: 'host'
    },
    policy: {
      transport: 'local-display',
      semanticMode: 'off',
      notificationOwner: 'watcher',
      requestKinds: ['task.failed', 'task.terminal'],
      maxRequestsPerEpisode: 1,
      episodeCooldownMs: 60000,
      attentionTtlMs: 120000
    },
    limits: {
      pollMinMs: 1000,
      pollMaxMs: 5000,
      maxSilenceMs: 60000,
      maxProbesPerEpisode: 2,
      maxJudgeRequestsPerDay: 10,
      expiresAt: new Date(Date.now() + 3600_000).toISOString()
    }
  } as never, actor);
  await new Promise(r => setTimeout(r, 1200));
  const listed = await runtime.service.list(undefined, 50, actor);
  const inspected = await runtime.service.inspect(spec.watchId, actor);
  console.log('=== demo register ===');
  console.log(JSON.stringify({ watchId: spec.watchId }, null, 2));
  console.log('=== list ===');
  console.log(JSON.stringify(listed, null, 2));
  console.log('=== inspect ===');
  console.log(JSON.stringify(inspected, null, 2));
  runtime.close();
  fs.rmSync(tmp, { recursive: true, force: true });
}

main().catch(e => {
  console.error(e);
  process.exit(5);
});


// --- relay-setup (owner side: consumer declaration + invite + audience/scope configuration) ---

async function cmdRelaySetup(flags: Map<string, string>): Promise<void> {
  const { WatcherRelaySource } = await import('./relay/managed.js');
  const cwd = process.cwd();
  const rootDir = path.resolve(flags.get('root') ?? path.join(cwd, '.pi-watcher', 'relay'));
  const relayHome = path.resolve(flags.get('relay-home') ?? process.env.PI_RELAY_HOME ?? path.join(os.homedir(), '.pi', 'relay'));
  const finalize = flags.get('finalize') === 'true';
  const relay = await WatcherRelaySource.open(rootDir, { relayHome });
  const decl = relay.ensureConsumerDeclaration();
  console.log(JSON.stringify({
    step: 'consumer-declaration',
    file: decl.file,
    note: 'Pi sessions (with the relay extension) scan <relay-home>/consumers at startup and register the guard'
  }, null, 2));
  if (!finalize) {
    const invite = relay.createInvite();
    console.log(JSON.stringify({
      step: 'invite',
      inviteFile: invite.inviteFile,
      note: 'Have the host-session model call the relay_bindings tool: action=bind, invitePath=<file above> (single-use), then run relay-setup --finalize'
    }, null, 2));
    await relay.close();
    return;
  }
  if (!relay.hasActiveMembership()) {
    console.log(JSON.stringify({
      step: 'finalize-blocked',
      reason: 'no membership yet — run bind first (see invite step), then retry --finalize',
      status: relay.statusJson()
    }, null, 2));
    await relay.close();
    return;
  }
  const prov = relay.provision();
  console.log(JSON.stringify({ step: 'provisioned', ...prov, transportAfter: 'relay' }, null, 2));
  await relay.close();
}
