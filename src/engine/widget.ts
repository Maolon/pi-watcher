/**
 * TUI status bar widget rendering (design 11 §33: ordinary progress updates the same widget and produces no long transcript;
 * design 08 §35: normal status updates go to the widget and not into the LLM context).
 *
 * Parallels pi-scheduler's `[scheduler] N scheduled · ...`, but the semantics are fact-driven:
 * - watch counts (active / paused)
 * - unresolved episode count (open / snoozed / acknowledged)
 * - nearest deadline (relative time; add [!] when overdue)
 * - unhealthy watch (degraded/blind) count [!]
 * - number of attention items awaiting host response (attention events whose episode is still open/snoozed)
 *
 * Pure function: the string[] overload leaves coloring to Pi defaults; returns [] (hides the widget) when there are no watches.
 */

export interface WidgetWatch {
  lifecycle: string;
  health: string;
  deadlineAt?: string | null;
}

export interface WatcherWidgetInput {
  watches: WidgetWatch[];
  /** Total unresolved episodes (state in open/snoozed/acknowledged) */
  unresolvedEpisodes: number;
  /** Attention outbox events whose episode is still open/snoozed (awaiting host response) */
  pendingAttentions: number;
  now: number;
}

/** Relative time: before now -> "in 12m"; after -> "12m overdue"; <1m -> "now"/"just now". */
export function formatRelative(ms: number): string {
  const abs = Math.abs(ms);
  const text =
    abs < 60_000 ? '<1m'
    : abs < 3_600_000 ? `${Math.round(abs / 60_000)}m`
    : abs < 86_400_000 ? `${Math.round(abs / 3_600_000)}h`
    : `${Math.round(abs / 86_400_000)}d`;
  return ms > 0 ? `in ${text}` : `${text} overdue`;
}

export function renderWatcherWidget(input: WatcherWidgetInput): string[] {
  const watches = input.watches.filter(w => w.lifecycle !== 'closed' && w.lifecycle !== 'expired');
  if (watches.length === 0) return [];

  const active = watches.filter(w => w.lifecycle === 'active');
  const paused = watches.filter(w => w.lifecycle === 'paused');
  const unhealthy = watches.filter(w => w.health === 'degraded' || w.health === 'blind');

  const parts: string[] = [];
  parts.push(`${active.length} watch${active.length === 1 ? '' : 'es'}`);
  if (paused.length > 0) parts.push(`${paused.length} paused`);

  if (input.unresolvedEpisodes > 0) {
    parts.push(`${input.unresolvedEpisodes} open ep${input.unresolvedEpisodes === 1 ? '' : 's'}`);
  }

  // Nearest business deadline (taken only from active watches; paused ones are not nagged). Wording uses due (the moment the task should be done)
  // rather than deadline, to distinguish it from timing concepts like watch expiry and silence budget
  const deadlines = active
    .map(w => (w.deadlineAt ? Date.parse(w.deadlineAt) : NaN))
    .filter(Number.isFinite) as number[];
  if (deadlines.length > 0) {
    const nearest = Math.min(...deadlines);
    const rel = formatRelative(nearest - input.now);
    parts.push(nearest <= input.now ? `[!] due ${rel}` : `due ${rel}`);
  }

  if (unhealthy.length > 0) parts.push(`[!] ${unhealthy.length} degraded`);
  if (input.pendingAttentions > 0) parts.push(`${input.pendingAttentions} attn pending`);

  return [`[watcher] ${parts.join(' · ')}`];
}
