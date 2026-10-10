# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may break.

## [Unreleased]

## [0.1.5] - 2026-10-09

### Changed
- Requires `@maolon/pi-relay` `^0.2.1`. That release keeps the local relay path from wedging: the managed source client reconnects after a dead socket instead of failing forever, staged managed packets are never pushed to a binding revoked while they waited, and the relay store keeps a bounded idempotency window instead of refusing operations once full.

## [0.1.4] - 2026-10-08

### Added
- `/watcher cancel [reason]` closes every active or paused watch of the current session, no watchId needed. Each close withdraws pending attentions and supersedes open episodes like a per-watch `close`; other sessions are untouched.

## [0.1.3] - 2026-10-08

### Changed
- Attention toasts follow the outcome: a succeeded `task.terminal` notice shows at `info`; failures, cancellations, unknown exits, deadlines, semantic candidates and failed relay wakes stay at `warning`.
- Git flow: `dev` integration branch, release and hotfix PRs into `main` publish to npm.

## [0.1.2] - 2026-10-08

### Changed
- README: state up front that the optional semantic check uses Jev, with credentials from Pi's `/login`.

## [0.1.1] - 2026-10-08

### Changed
- Released from CI through npm trusted publishing (OIDC), with provenance. No functional changes from 0.1.0.

## [0.1.0] - 2026-10-08

First public release.

### Added
- `watcher` tool with `watch-file`, `watch-check`, `register`, `list`, `inspect`, `check`, `pause` and `close`.
- `/watcher` command: status panel and local episode acknowledgement.
- Deterministic hard rules: explicit terminal states, business deadlines (raised once per checkpoint), silence detection and result cards.
- Optional semantic review via Jev (`semanticMode: shadow | active`). Credentials come from Pi's model registry (`/login` → TypeSafe, `TYPESAFE_API_KEY`, or another Jev provider) or `JEV_API_KEY`. Egress needs separate user consent: `/watcher jev consent on` or `JEV_CONSENT=1`.
- `/watcher jev` status command.
- Relay-delivered wakes through `@maolon/pi-relay` managed delivery: one scope per watch, durable control outbox, withdraw on pause/close with per-route results, and an idempotent host-response pump.
- `pi-watcher` CLI: `doctor`, `qualify`, `relay-setup`, plus `producer`/`demo` fixtures.
