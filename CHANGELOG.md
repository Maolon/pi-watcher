# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may break.

## [0.1.0] - Unreleased

First public release.

### Added
- `watcher` tool with `watch-file`, `watch-check`, `register`, `list`, `inspect`, `check`, `pause` and `close`.
- `/watcher` command: status panel and local episode acknowledgement.
- Deterministic hard rules: explicit terminal states, business deadlines (raised once per checkpoint), silence detection and result cards.
- Optional semantic review via Jev (`semanticMode: shadow | active`), gated by `JEV_API_KEY` and explicit `JEV_CONSENT=1`.
- Relay-delivered wakes through `@maolon/pi-relay` managed delivery: one scope per watch, durable control outbox, withdraw on pause/close with per-route results, and an idempotent host-response pump.
- `pi-watcher` CLI: `doctor`, `qualify`, `relay-setup`, plus `producer`/`demo` fixtures.
