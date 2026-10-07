/**
 * Shared runtime IPC server (primary side).
 * Multiple pi sessions on the same project root share one embedded runtime: the lock-holding session (primary) serves
 * attached sessions' tool/panel/widget requests on this unix socket. Protocol: line-delimited JSON.
 *
 * Invariant (design 7.2): the whole project still has a single store writer (the only opener under epoch fencing).
 * Attached sessions never open the DB -- all mutations are serialized through the primary.
 */

import * as net from 'node:net';
import * as fs from 'node:fs';
import * as path from 'node:path';

/** JSON wire format of ActorContext (permitted: Set -> string[]). */
export interface WireActor {
  actorId: string;
  owner: Record<string, unknown>;
  profileRevision: number;
  permitted: string[];
}

export type IpcRequest =
  | { op: 'tool'; params: Record<string, unknown>; actor: WireActor }
  | { op: 'panel'; actor: WireActor }
  | { op: 'ack'; episodeId: string; action: string; note: string; until?: string; requestId: string; actor: WireActor }
  | { op: 'widget'; lastNotifySeq?: number; ownerSession?: string };

/** Same shape as ServiceResult: the result envelope across the wire. */
export interface IpcOk { ok: true; requestId?: string; value: unknown }
export interface IpcErr { ok: false; requestId?: string; error: { code: string; message: string } }
export type IpcResponse = IpcOk | IpcErr;

export interface IpcHandlers {
  tool(params: Record<string, unknown>, actor: WireActor): Promise<IpcResponse>;
  panel(actor: WireActor): Promise<IpcResponse>;
  ack(req: { episodeId: string; action: string; note: string; until?: string; requestId: string }, actor: WireActor): Promise<IpcResponse>;
  /** Widget snapshot + incremental attention notices (notices after lastNotifySeq, optionally filtered by ownerSession) */
  widget(lastNotifySeq?: number, ownerSession?: string): IpcResponse;
}

export function runtimeSocketPath(rootDir: string): string {
  return path.join(rootDir, 'runtime.sock');
}

export class RuntimeIpcServer {
  private server: net.Server | null = null;

  constructor(
    private readonly socketPath: string,
    private readonly handlers: IpcHandlers
  ) {}

  /**
   * Call only after holding the root flock: any existing socket file is then a leftover from a crashed primary,
   * so unlink is safe (a live primary's socket cannot coexist with us -- flock is exclusive).
   */
  async start(): Promise<void> {
    try { fs.unlinkSync(this.socketPath); } catch { /* no leftover */ }
    await new Promise<void>((resolve, reject) => {
      const srv = net.createServer(conn => this.handleConn(conn));
      srv.once('error', reject);
      srv.listen(this.socketPath, () => resolve());
      this.server = srv;
    });
  }

  async close(): Promise<void> {
    const srv = this.server;
    this.server = null;
    if (!srv) return;
    await new Promise<void>(resolve => {
      srv.close(() => resolve());
      (srv as { closeAllConnections?: () => void }).closeAllConnections?.();
    });
    try { fs.unlinkSync(this.socketPath); } catch { /* already removed on close or nonexistent */ }
  }

  private handleConn(conn: net.Socket): void {
    let buf = '';
    conn.on('data', d => {
      buf += d.toString('utf8');
      let idx: number;
      while ((idx = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        let req: unknown;
        try { req = JSON.parse(line); } catch {
          conn.write(JSON.stringify({ ok: false, error: { code: 'INVALID_SPEC', message: 'malformed IPC line' } }) + '\n');
          continue;
        }
        void this.dispatch(req)
          .then(res => { conn.write(JSON.stringify(res) + '\n'); })
          .catch(() => conn.destroy());
      }
    });
    conn.on('error', () => conn.destroy());
  }

  private async dispatch(req: unknown): Promise<IpcResponse> {
    const r = req as Partial<IpcRequest> & Record<string, unknown>;
    try {
      if (r?.op === 'tool' && r.actor) return await this.handlers.tool((r.params ?? {}) as Record<string, unknown>, r.actor as WireActor);
      if (r?.op === 'panel' && r.actor) return await this.handlers.panel(r.actor as WireActor);
      if (r?.op === 'ack' && r.actor) {
        return await this.handlers.ack({
          episodeId: String(r.episodeId ?? ''),
          action: String(r.action ?? ''),
          note: String(r.note ?? ''),
          until: typeof r.until === 'string' ? r.until : undefined,
          requestId: String(r.requestId ?? `ack-ipc-${Date.now()}`)
        }, r.actor as WireActor);
      }
      if (r?.op === 'widget') {
        const lastSeq = typeof r.lastNotifySeq === 'number' ? r.lastNotifySeq : undefined;
        const ownerSession = typeof r.ownerSession === 'string' && r.ownerSession.length > 0 ? r.ownerSession : undefined;
        return this.handlers.widget(lastSeq, ownerSession);
      }
      return { ok: false, error: { code: 'INVALID_SPEC', message: `unknown IPC op ${String(r?.op)}` } };
    } catch (e) {
      return { ok: false, error: { code: 'STORE_UNAVAILABLE', message: e instanceof Error ? e.message : String(e) } };
    }
  }
}
