# pi-watcher

Watch long-running work from [Pi](https://pi.dev) without babysitting it. pi-watcher tracks builds, test runs,
training jobs, CI pipelines and deadlines from durable evidence. It keeps the session quiet while things progress
and wakes the session (through [pi-relay](https://github.com/Maolon/pi-relay)) only when a fact needs a decision.

For what patterns cannot see (a stalled run, a fix loop that keeps repeating, a log that claims success while
showing errors), pi-watcher can optionally run a **semantic check with [Jev](https://typesafe.ai)**, TypeSafe's
fast discriminative classifier, using the Jev credentials you already have in Pi (`/login` → TypeSafe). See
[Semantic review](#semantic-review-jev-optional).

```bash
pi install npm:@maolon/pi-relay      # wake transport (recommended, see "pi-relay" below)
pi install npm:@maolon/pi-watcher
```

## Why

An agent that waits on a long task usually polls: `sleep`, re-run a status command, repeat. Each check spends a
model turn and grows the context just to learn "still running". pi-watcher moves that loop out of the model:

- **Facts first.** Explicit terminal states, exit markers, deadlines and silence are decided by code, not by a model.
- **Quiet by default.** Routine progress only updates a status widget. Nothing enters the conversation.
- **Semantic check with Jev (optional).** Per watch, Jev answers six bounded questions about a sanitized
  evidence window: progress, blocker, needs a decision, repeating, claim vs evidence, enough context. Its scores
  never override hard facts; they only raise or annotate episodes. Off unless you enable it.
- **Wake on decisions.** Failure, a crossed deadline, a ready dependency or (optionally) a Jev-detected blocker
  becomes one episode and one wake. The host then inspects the fresh facts and responds.
- **Honest state.** Unknown stays unknown. Delivery, withdrawal and completion are reported only with evidence.

## How it works

```
 exec log / build output / CI status          Pi session
            │                                      ▲
   watch-file │ watch-check                         │ wake (managed delivery)
            ▼                                      │
   pi-watcher engine ──► SQLite truth store ──► pi-relay ──► relay_respond ──► watcher applies the response
   hard rules, deadlines, silence,
   optional Jev semantic review
```

Each Pi session runs its own watcher runtime with state under `<cwd>/.pi-watcher/sessions/<session-id>/`.
The watcher never runs, retries, kills or fixes the task it watches.

## Requirements

- Pi 1.0 or newer (`@earendil-works/pi-coding-agent`); Jev through Pi's `/login` needs Pi 1.1+
- Node.js 22.16 or newer
- macOS or Linux. Windows is not supported.
- A native build toolchain for `fs-ext` (Python 3, `make`, a C++ compiler; on macOS the Xcode Command Line
  Tools). `better-sqlite3` ships prebuilt binaries for common platforms. pi-relay has the same requirements.

## Install

```bash
pi install npm:@maolon/pi-relay
pi install npm:@maolon/pi-watcher
```

Use `-l` to install into the current project (`.pi/settings.json`) instead of your personal settings, or try it
for one run with `pi -e npm:@maolon/pi-watcher`. Pin versions with `npm:@maolon/pi-watcher@0.1.0`.

Add the watcher's state directories to your project's `.gitignore`:

```gitignore
.pi-watcher/
.pi-watcher-sources/
```

## pi-relay

pi-watcher uses [`@maolon/pi-relay`](https://www.npmjs.com/package/@maolon/pi-relay) in two different roles:

| Role | What it is | How you get it |
|---|---|---|
| **Library** | The watcher publishes attentions, withdraws them on pause/close and reads host responses through pi-relay's managed-delivery source API (`@maolon/pi-relay/source`, `/consumer`, `/protocol`). | A regular npm dependency of pi-watcher, installed automatically. |
| **Pi extension** | Runs inside your Pi session as the delivery target: receives the wake, injects it when the session is idle, and gives the model the `relay_respond` tool. | Install it yourself: `pi install npm:@maolon/pi-relay`. |

**Compatibility.** pi-watcher 0.1.x requires pi-relay **0.2.x**: the `^0.2.0` dependency for the library, and
0.2.x for the extension you install. Both sides share the relay home on disk (`~/.pi/relay`, or `PI_RELAY_HOME`),
so keep them on the same minor version.

**Without the pi-relay extension**, the watcher still works, but in *local display* mode:

| | with pi-relay | without pi-relay |
|---|---|---|
| Watches, hard rules, deadlines, silence | yes | yes |
| Status widget and toasts in the session | yes | yes |
| Result cards, `inspect`, `/watcher` panel | yes | yes |
| Wake an idle session when a fact needs a decision | **yes** | no: you or the model must look |
| Host response loop (`relay_respond` → applied) | yes | no (`ack` returns `NO_DELIVERY`; use `/watcher ack`) |
| Withdraw pending wakes on `pause`/`close` | yes (per-route result) | nothing is in flight |

**Setup is automatic.** When the relay home exists, the watcher enables relay delivery. On session start it asks the
pi-relay extension in the same session for a local binding over Pi's event bus, then provisions its audience and
scope. No invite or manual step is needed. The binding is *local trust*: same machine, same OS user, no interactive
consent prompt. To opt out:

- `PI_WATCHER_RELAY=0`: never use relay (local display only).
- `PI_WATCHER_AUTO_BIND=0`: keep relay, but do not bind automatically. Use `pi-watcher relay-setup` instead.

**When a wake arrives**, the model is instructed to run `watcher inspect` for that watch first, act on the fresh
facts, and then answer with `relay_respond` (`received`, `investigating`, `defer`, `resolved` or `dismiss`). The
watcher applies the response to the episode, confirms the application back to the relay, and closes finished
watches shortly afterwards.

## Usage

You normally just ask: *"run the test suite and tell me when it's done"*, *"watch the training run"*, *"keep an eye
on the CI for this PR"*. The tool guidelines steer the model to the `watcher` tool instead of polling.

### Watch a file

Typical use: the log of a long `exec_command` session.

```json
{ "action": "watch-file", "path": "/tmp/build.log",
  "okPattern": "__EXEC_EXIT__:0", "failPattern": "__EXEC_EXIT__:-?[1-9]",
  "objective": "release build", "deadlineAt": "2026-10-08T18:00:00Z" }
```

A terminal state is decided only by the declared patterns (fail is checked first). End of file, a quiet log or a
line that merely says "done" never counts as completion.

### Watch a command

Use this for state you cannot tail: CI, cloud resources, remote health.

```json
{ "action": "watch-check", "cmd": "gh run view 123456 --json conclusion -q .conclusion",
  "okPattern": "^success$", "failPattern": "^(failure|cancelled)$", "intervalMs": 60000 }
```

The command runs on a fixed interval with a timeout (1–30 s). Exit codes 126/127 and timeouts mark the *monitor* as
degraded; they never mark the task as failed. The command text is stored as evidence, so never put secrets in it.

### Other actions

| Action | Purpose |
|---|---|
| `list` | Active and paused watches (`includeClosed: true` for history) |
| `inspect` | Authoritative facts for one watch: state, episodes, health, delivery |
| `check` | Schedule one bounded refresh now (needs `expectedControlRevision`) |
| `pause` / `close` | Stop watching. Pending wakes are withdrawn; the result is reported per route. |
| `register` | Full spec: `run`, `group` (up to 16 members) and `obligation` targets |

`/watcher` shows the panel; `/watcher ack <episodeId> <received|investigating|defer|resolved|dismiss> [until]`
handles an episode locally; `/watcher jev` manages the optional semantic review (see below).

### Exit markers for `exec_command`

To make shell-session outcomes decidable, the watcher appends `echo __EXEC_EXIT__:$?` to every `exec_command`
call. It creates no watch on its own. Disable it with `PI_WATCHER_EXEC_MARKER=0`. To have every long-running exec
session watched automatically, set `PI_WATCHER_AUTO_EXEC=1` (off by default).

## Semantic review (Jev, optional)

**Optional and off by default.** pi-watcher works fully without it: every watch is decided by facts (patterns,
exit markers, deadlines, silence). Semantic review is an extra layer you opt into per watch.

Some tasks cannot be judged by patterns alone: a training run that stalls, a fix loop that keeps repeating, a log
that claims success while showing errors. With `semanticMode` set on a watch, the watcher asks
[Jev](https://typesafe.ai), TypeSafe's fast discriminative classifier (not a chat model), six bounded questions
about a sanitized evidence window: progress, unresolved blocker, needs host decision, repeating without new
information, claim vs evidence conflict, and context sufficiency.

- `off` (default): facts only.
- `shadow`: record judgments and show them as toasts and widget hints; never wake.
- `active`: a confident blocker, decision request, repeating loop or claim conflict opens an episode and can wake the session.

Enabling it takes two separate steps, because authentication and permission to send data are different decisions.

**1. Authenticate Jev through Pi** (Pi 1.1+ ships Jev as a classifier model). Use any one of these:

- In Pi, run `/login`, choose **Sign in with an API key**, then pick **TypeSafe** (or another provider that serves
  Jev, such as OpenRouter or OpenCode). The credential lands in Pi's `auth.json` and the watcher picks it up,
  even mid-session.
- `export TYPESAFE_API_KEY=...` (Pi's standard variable for TypeSafe).
- `export JEV_API_KEY=...` uses the watcher's own direct client, pinned to `jev-1.13.0`. It takes precedence over Pi's registry.

When several Jev providers are configured, the watcher prefers TypeSafe direct, then OpenRouter, OpenCode,
Cloudflare Workers AI and Vercel AI Gateway. Force one with `PI_WATCHER_JEV_MODEL=<provider>/<model>`, for example
`openrouter/typesafe/jev-1.13`. The model that actually answered is recorded with every judgment.

**2. Grant egress consent** (user only; the model cannot grant it):

```
/watcher jev consent on      # stored in ~/.pi/agent/pi-watcher.json; `off` revokes it
/watcher jev                 # shows the active Jev source and the consent state
```

Alternatively set `JEV_CONSENT=1` (or `0` to force it off); the environment overrides the stored setting.

Without credentials, a watch with `semanticMode` shows "semantic review unavailable" with the reason, and the facts
keep working. Before anything is sent, credentials and tokens are redacted. Requests are budgeted per watch and
per day, and hard facts always win over model scores. Redaction is best effort: for sensitive repositories, leave
`semanticMode` off.

## Configuration

| Variable | Default | Effect |
|---|---|---|
| `PI_RELAY_HOME` | `~/.pi/relay` | Relay home shared with the pi-relay extension |
| `PI_WATCHER_RELAY` | auto | `0` disables relay delivery |
| `PI_WATCHER_AUTO_BIND` | on | `0` disables automatic local binding |
| `PI_WATCHER_EXEC_MARKER` | on | `0` disables `__EXEC_EXIT__` injection |
| `PI_WATCHER_AUTO_EXEC` | off | `1` auto-watches long-running exec sessions |
| `TYPESAFE_API_KEY` | unset | Pi's TypeSafe credential; enables Jev through Pi (same as `/login` → TypeSafe) |
| `JEV_API_KEY` | unset | Watcher's direct Jev client (`jev-1.13.0`); takes precedence over Pi's registry |
| `PI_WATCHER_JEV_MODEL` | auto | Force a Pi Jev model, e.g. `openrouter/typesafe/jev-1.13` |
| `JEV_CONSENT` | unset | `1`/`0` overrides the stored `/watcher jev consent` setting |
| `PI_WATCHER_CONFIG` | `~/.pi/agent/pi-watcher.json` | Where `/watcher jev consent` is stored |
| `JEV_BASE_URL` | `https://api.typesafe.ai` | Endpoint for the direct client |
| `JEV_TIMEOUT_MS` | `10000` | Per-request timeout for the direct client |

## Data and privacy

- All watcher state stays on your machine: SQLite (WAL, `synchronous=FULL`) under `<cwd>/.pi-watcher/`.
- Wakes travel through pi-relay's store under the relay home on the same machine.
- Shadow-mode judgments are shown as session toasts and widget hints, never as conversation messages.
- Only with Jev credentials **and** egress consent (`/watcher jev consent on` or `JEV_CONSENT=1`) does a sanitized,
  size-bounded evidence window leave the machine, and only for watches with `semanticMode` set.
- `watch-file` reads the paths you or the model declare. `watch-check` runs the declared command as your user.
  Both are as powerful as the session itself; review what the model registers.

## CLI

The package also installs `pi-watcher`:

```bash
pi-watcher doctor --root .pi-watcher       # local diagnostics
pi-watcher relay-setup [--finalize]        # manual relay owner setup (when auto-bind is off)
pi-watcher qualify                         # native dependency / storage qualification report
```

## Status

0.1 is an early release. What is solid and what is not:

- **Solid:** fact-driven watches (`watch-file`, `watch-check`, task-status), deadlines, silence, result cards,
  per-session isolation, relay wakes with withdraw-on-pause, idempotent host-response handling. Covered by the
  offline suite, including end-to-end tests against the real pi-relay libraries.
- **Experimental:** semantic review thresholds are initial values, not calibrated results. Use `shadow` before `active`.
- **Not yet:** a standalone watcher service (the watcher runs inside the Pi session), an interactive consent prompt
  for relay binding (it is local trust today), fork/tree-navigation holds, artifact-digest binding for checks,
  evidence retention limits, and the `update` action.

## Development

```bash
npm install
npm run typecheck
npm run test:unit        # offline suite
npm run build            # dist/
npm run test:package     # pack + leak scan + install + load with plain Node
```

To load your working copy in Pi: `pi -e ./` from this directory after `npm run build`. See [AGENTS.md](AGENTS.md)
for conventions.

## License

[MIT](LICENSE)
