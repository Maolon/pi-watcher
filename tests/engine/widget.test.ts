import test from 'node:test';
import assert from 'node:assert/strict';
import { renderWatcherWidget, formatRelative } from '../../src/engine/widget.js';

const NOW = Date.parse('2026-09-19T12:00:00.000Z');

function lineOf(parts: string[]): string {
  assert.equal(parts.length, 1, 'widget renders exactly one line when visible');
  return parts[0]!;
}

test('widget: no watches → hidden', () => {
  assert.deepEqual(renderWatcherWidget({ watches: [], unresolvedEpisodes: 0, pendingAttentions: 0, now: NOW }), []);
  // closed/expired are not counted
  assert.deepEqual(renderWatcherWidget({
    watches: [{ lifecycle: 'closed', health: 'healthy' }, { lifecycle: 'expired', health: 'healthy' }],
    unresolvedEpisodes: 0, pendingAttentions: 0, now: NOW
  }), []);
});

test('widget: basic counts and singular/plural', () => {
  const out = lineOf(renderWatcherWidget({
    watches: [{ lifecycle: 'active', health: 'healthy' }],
    unresolvedEpisodes: 0, pendingAttentions: 0, now: NOW
  }));
  assert.equal(out, '[watcher] 1 watch');

  const out2 = lineOf(renderWatcherWidget({
    watches: [
      { lifecycle: 'active', health: 'healthy' },
      { lifecycle: 'active', health: 'healthy' },
      { lifecycle: 'paused', health: 'healthy' }
    ],
    unresolvedEpisodes: 2, pendingAttentions: 0, now: NOW
  }));
  assert.equal(out2, '[watcher] 2 watches · 1 paused · 2 open eps');
});

test('widget: nearest deadline relative time and overdue mark', () => {
  const base = { unresolvedEpisodes: 0, pendingAttentions: 0, now: NOW };
  assert.equal(
    lineOf(renderWatcherWidget({
      watches: [
        { lifecycle: 'active', health: 'healthy', deadlineAt: '2026-09-19T12:45:00.000Z' },
        { lifecycle: 'active', health: 'healthy', deadlineAt: '2026-09-19T14:00:00.000Z' }
      ], ...base
    })),
    '[watcher] 2 watches · due in 45m'
  );

  assert.equal(
    lineOf(renderWatcherWidget({
      watches: [{ lifecycle: 'active', health: 'healthy', deadlineAt: '2026-09-19T11:30:00.000Z' }], ...base
    })),
    '[watcher] 1 watch · [!] due 30m overdue'
  );

  // paused deadline is not nagged
  assert.equal(
    lineOf(renderWatcherWidget({
      watches: [{ lifecycle: 'paused', health: 'healthy', deadlineAt: '2026-09-19T11:00:00.000Z' }], ...base
    })),
    '[watcher] 0 watches · 1 paused'
  );
});

test('widget: degraded and pending attention marks', () => {
  const out = lineOf(renderWatcherWidget({
    watches: [
      { lifecycle: 'active', health: 'degraded' },
      { lifecycle: 'active', health: 'blind' },
      { lifecycle: 'active', health: 'healthy' }
    ],
    unresolvedEpisodes: 1, pendingAttentions: 2, now: NOW
  }));
  assert.equal(out, '[watcher] 3 watches · 1 open ep · [!] 2 degraded · 2 attn pending');
});

test('widget: formatRelative boundaries', () => {
  assert.equal(formatRelative(30_000), 'in <1m');
  assert.equal(formatRelative(90_000), 'in 2m');
  assert.equal(formatRelative(2 * 3_600_000), 'in 2h');
  assert.equal(formatRelative(3 * 86_400_000), 'in 3d');
  assert.equal(formatRelative(-5 * 60_000), '5m overdue');
});
