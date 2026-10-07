/**
 * Shared runtime IPC client (attached session side).
 * Single in-flight queue + line-delimited JSON frames; a disconnect/timeout marks the backend as lost,
 * and the upper layer triggers failover (retry the lock to promote to primary, or attach to a new primary).
 */

import * as net from 'node:net';
import { WatcherError } from '../util/result.js';
import type { IpcRequest, IpcResponse } from './server.js';

export interface RuntimeIpcClientOptions {
  connectTimeoutMs?: number;
  requestTimeoutMs?: number;
}

export class RuntimeIpcClient {
  private buf = '';
  private dead = false;
  private queue: Promise<unknown> = Promise.resolve();
  private pending: { resolve: (v: IpcResponse) => void; reject: (e: Error) => void; timer: NodeJS.Timeout } | null = null;

  private constructor(
    private readonly socket: net.Socket,
    private readonly requestTimeoutMs: number
  ) {
    socket.setEncoding('utf8');
    socket.on('data', (d: string) => this.onData(d));
    socket.on('close', () => this.markDead());
    socket.on('error', () => this.markDead());
  }

  static async connect(socketPath: string, options: RuntimeIpcClientOptions = {}): Promise<RuntimeIpcClient> {
    const timeoutMs = options.connectTimeoutMs ?? 1500;
    const socket = await new Promise<net.Socket>((resolve, reject) => {
      const s = net.connect(socketPath);
      const timer = setTimeout(() => {
        s.destroy();
        reject(new WatcherError('ROOT_LOCK_HELD', `watcher shared runtime socket connect timeout: ${socketPath}`));
      }, timeoutMs);
      s.once('connect', () => { clearTimeout(timer); resolve(s); });
      s.once('error', e => {
        clearTimeout(timer);
        reject(new WatcherError('ROOT_LOCK_HELD', `watcher shared runtime socket unreachable (${e instanceof Error ? e.message : String(e)}): ${socketPath}`));
      });
    });
    return new RuntimeIpcClient(socket, options.requestTimeoutMs ?? 20000);
  }

  get isDead(): boolean { return this.dead; }

  request(req: IpcRequest): Promise<IpcResponse> {
    const run = async (): Promise<IpcResponse> => {
      if (this.dead) {
        throw new WatcherError('STORE_UNAVAILABLE', 'watcher shared runtime connection lost');
      }
      return await new Promise<IpcResponse>((resolve, reject) => {
        const timer = setTimeout(() => {
          this.failPending(new WatcherError('STORE_UNAVAILABLE', 'watcher shared runtime request timeout'));
        }, this.requestTimeoutMs);
        this.pending = { resolve, reject, timer };
        this.socket.write(JSON.stringify(req) + '\n', err => {
          if (err) this.failPending(new WatcherError('STORE_UNAVAILABLE', `watcher shared runtime write failed: ${err.message}`));
        });
      });
    };
    const p = this.queue.then(run, run);
    this.queue = p.catch(() => undefined);
    return p;
  }

  close(): void {
    this.markDead();
    this.socket.destroy();
  }

  private onData(d: string): void {
    this.buf += d;
    let idx: number;
    while ((idx = this.buf.indexOf('\n')) >= 0) {
      const line = this.buf.slice(0, idx);
      this.buf = this.buf.slice(idx + 1);
      if (!line.trim()) continue;
      const p = this.pending;
      this.pending = null;
      if (!p) continue;
      clearTimeout(p.timer);
      try {
        p.resolve(JSON.parse(line) as IpcResponse);
      } catch (e) {
        p.reject(new WatcherError('STORE_UNAVAILABLE', `watcher shared runtime malformed response: ${e instanceof Error ? e.message : String(e)}`));
      }
    }
  }

  private failPending(e: Error): void {
    const p = this.pending;
    this.pending = null;
    this.markDead();
    if (p) { clearTimeout(p.timer); p.reject(e); }
  }

  private markDead(): void {
    if (this.dead) return;
    this.dead = true;
    const p = this.pending;
    this.pending = null;
    if (p) { clearTimeout(p.timer); p.reject(new WatcherError('STORE_UNAVAILABLE', 'watcher shared runtime connection lost')); }
    this.socket.destroy();
  }
}
