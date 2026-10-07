/**
 * Clock port per contracts/interfaces.ts.
 * wallNow: persisted wall-clock ms (basis across restarts).
 * monotonicNow: same-process monotonic ms (same-process elapsed).
 */

export interface Clock {
  wallNow(): number;
  monotonicNow(): number;
}

export class SystemClock implements Clock {
  wallNow(): number {
    return Date.now();
  }
  monotonicNow(): number {
    return performance.now();
  }
}

/** Injectable fake clock for tests. */
export class ManualClock implements Clock {
  private _wall: number;
  private _mono: number;
  constructor(wall = Date.now(), mono = 0) {
    this._wall = wall;
    this._mono = mono;
  }
  wallNow(): number {
    return this._wall;
  }
  monotonicNow(): number {
    return this._mono;
  }
  advanceWall(ms: number): void {
    this._wall += ms;
  }
  advanceMono(ms: number): void {
    this._mono += ms;
  }
}
