# AGENTS.md

Guidance for coding agents (and humans) working in this repository.

## What this is

`@maolon/pi-watcher` is a Pi extension package. It watches long-running tasks and wakes the host session through
`@maolon/pi-relay` only when a fact needs a decision. Start with `README.md`. The entry points are
`src/pi-extension.ts` (Pi extension) and `src/cli.ts` (CLI).

## Commands

```bash
npm install
npm run typecheck          # tsc, src + tests
npm run test:unit          # offline: storage, sources, relay (real pi-relay libs), engine
npm run test:jev:unit      # Jev client/sanitizer/mock, offline
npm run test:jev:live      # real Jev API; needs JEV_API_KEY, costs requests
npm run build              # dist/ (tsc + runtime assets)
npm run test:package       # pack, leak-scan the tarball, install it, load the extension with plain Node
```

Run `typecheck` and `test:unit` before every change you hand back. Run `test:package` when you touch
`package.json`, the build, runtime asset paths, or the public entry points.

## Branches and releases

Git flow with `dev` as the integration branch and `main` as the release branch. Both are protected: changes land
only through pull requests with green CI.

- `feature/<topic>` from `dev`, PR back into `dev`.
- `release/<x.y.z>` from `dev`: only the `package.json`/`package-lock.json` version bump and a `CHANGELOG.md`
  entry. PR into `main`.
- `hotfix/<topic>` from `main` for urgent fixes, with the version bump and CHANGELOG entry included. PR into `main`.
- Merging into `main` publishes to npm: `release.yml` reruns CI, publishes the new version through trusted
  publishing, and creates the `v<x.y.z>` tag and GitHub release. The `release-guard` CI job blocks PRs into `main`
  that come from another branch, reuse a version already on npm, or lack a CHANGELOG entry.
- After every release, open a PR from `main` into `dev` so the merge commit and version bump flow back.

## Layout

```
src/pi-extension.ts      Pi entry: watcher tool, /watcher command, session lifecycle, relay auto-bind
src/runtime.ts           composition: store + adapters + engine + service + loop + relay response pump
src/engine/              WatchEngine (hard rules, episodes, semantic pass, delivery) and WatchService (API)
src/source/              evidence sources: agent-file, agent-check, task-status-v1
src/storage/             SQLite WatchStore (WAL, FULL, FK), root lock, schema.sql
src/relay/               pi-relay managed-delivery adapter and negotiation
src/jev/                 Jev: direct HTTP client, Pi model-registry judge, consent store, sanitizer, question set, MockJudge (tests only)
src/contracts/           ports and domain types, policy defaults
tests/                   node:test suites, run with tsx
scripts/                 build and package-smoke helpers
```

## Rules

- English only: code, comments, strings, tests and docs.
- Never commit local paths, usernames, session ids, credentials or real API keys. Test fixtures use obviously fake values.
- Truth lives in the SQLite store; anything rendered (widget, result cards, toasts) is a display copy.
- Never substitute a mock judge outside tests. Egress consent is user-only (`/watcher jev consent`, `JEV_CONSENT`), never a tool parameter.
- Facts come before models: a Jev judgment never overrides a hard fact, never grants permission, and never causes a business action.
- Only the relay managed path may wake the host. Never call `sendMessage` or add a second wake channel.
- External I/O (relay calls, Jev requests) happens outside SQL transactions. Intents are written to the store first.
- Every write the model can trigger is idempotent by `requestId`. Every control action carries `expectedControlRevision`.
- Report state honestly: unknown stays unknown, pending stays pending. Never report a withdrawal, delivery or completion without evidence.
- Keep `@maolon/pi-relay` on the version range documented in the README. `src/contracts/relay-next.interfaces.ts` mirrors its managed-delivery shapes.
