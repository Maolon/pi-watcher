import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn, type ChildProcess } from 'node:child_process';
import { RootLock } from '../../src/storage/lock.js';

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pw-lock-test-'));
}

test('RootLock: second exclusive acquire in-process fails', async () => {
  const dir = tmpDir();
  const a = new RootLock(dir);
  const b = new RootLock(dir);
  assert.equal(await a.tryAcquire(), true);
  assert.equal(await b.tryAcquire(), false);
  await a.release();
  assert.equal(await b.tryAcquire(), true);
  await b.release();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('RootLock: cross-process contention rejected (fixed inode, no replace)', async () => {
  const dir = tmpDir();
  const holder = spawn(process.execPath, ['-e', `
    const fs = require('node:fs');
    const { flock } = require('fs-ext');
    const fd = fs.openSync(${JSON.stringify(path.join(dir, 'watcher.lock'))}, 'a');
    flock(fd, 'ex', () => {
      console.log('held');
      setTimeout(() => process.exit(0), 15000);
    });
  `], { cwd: path.resolve(import.meta.dirname, '../..'), stdio: ['ignore', 'pipe', 'inherit'] });
  await new Promise<void>((resolve, reject) => {
    let buf = '';
    holder.stdout!.on('data', c => {
      buf += String(c);
      if (buf.includes('held')) resolve();
    });
    holder.on('exit', c => reject(new Error(`holder exited: ${c}`)));
    setTimeout(() => reject(new Error('timeout')), 5000);
  });
  const mine = new RootLock(dir);
  assert.equal(await mine.tryAcquire(), false);
  holder.kill('SIGKILL');
  await new Promise(r => holder.on('exit', r));
  // lock file inode was not replaced; can be re-acquired after the holder exits
  const mine2 = new RootLock(dir);
  assert.equal(await mine2.tryAcquire(), true);
  await mine2.release();
  fs.rmSync(dir, { recursive: true, force: true });
});
