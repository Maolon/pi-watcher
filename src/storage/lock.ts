/**
 * Root singleton lock: persistent advisory lock on a fixed-inode lock file (design 7.2).
 * The lock file is never deleted or replaced; a second instance may only report a conflict and must not start a second set of timers.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { flock } from 'fs-ext';
import { WatcherError } from '../util/result.js';

function flockAsync(fd: number, mode: 'ex' | 'exnb' | 'sh' | 'shnb' | 'un'): Promise<void> {
  return new Promise((resolve, reject) => {
    flock(fd, mode, (err: Error | null) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

export class RootLock {
  readonly lockPath: string;
  private fd: number | null = null;

  constructor(rootDir: string) {
    this.lockPath = path.join(rootDir, 'watcher.lock');
  }

  /** Exclusive, non-blocking. Returns true when acquired; false when already held. */
  async tryAcquire(): Promise<boolean> {
    if (this.fd !== null) return true;
    // 'a' keeps the existing inode; never truncate/replace the lock file.
    const fd = fs.openSync(this.lockPath, 'a');
    try {
      await flockAsync(fd, 'exnb');
      this.fd = fd;
      return true;
    } catch (e: unknown) {
      const code = (e as NodeJS.ErrnoException).code;
      fs.closeSync(fd);
      if (code === 'EAGAIN' || code === 'EWOULDBLOCK' || code === 'EACCES' || code === 'EDEADLK') {
        return false;
      }
      throw e;
    }
  }

  async requireAcquire(): Promise<void> {
    const got = await this.tryAcquire();
    if (!got) {
      throw new WatcherError(
        'ROOT_LOCK_HELD',
        `watcher root already locked by another process: ${this.lockPath}`
      );
    }
  }

  get held(): boolean {
    return this.fd !== null;
  }

  async release(): Promise<void> {
    if (this.fd === null) return;
    const fd = this.fd;
    this.fd = null;
    try {
      await flockAsync(fd, 'un');
    } finally {
      fs.closeSync(fd);
    }
  }
}
