/**
 * Standard tool/service result envelope and error codes (design 11.4).
 */

export type WatcherErrorCode =
  | 'INVALID_SPEC'
  | 'STALE_REVISION'
  | 'UNKNOWN_TARGET'
  | 'CAPABILITY_DENIED'
  | 'SOURCE_UNAVAILABLE'
  | 'BUDGET_EXHAUSTED'
  | 'STORE_UNAVAILABLE'
  | 'ADMISSION_UNKNOWN'
  | 'DEPENDENCY_UNQUALIFIED'
  | 'ROOT_LOCK_HELD'
  | 'NO_DELIVERY'
  | 'REQUEST_CONFLICT';

export class WatcherError extends Error {
  readonly code: WatcherErrorCode;
  readonly exitCode: number;
  constructor(code: WatcherErrorCode, message: string) {
    super(message);
    this.name = 'WatcherError';
    this.code = code;
    // CLI exit code mapping (design 11.4): 2 invalid input; 3 permission/qualification; 4 delivery unknown; 5 local dependency/storage
    this.exitCode =
      code === 'INVALID_SPEC' || code === 'UNKNOWN_TARGET' ? 2
      : code === 'ADMISSION_UNKNOWN' ? 4
      : code === 'STORE_UNAVAILABLE' || code === 'ROOT_LOCK_HELD' ? 5
      : 3;
  }
}

export interface OkResult<T> {
  ok: true;
  requestId?: string;
  value: T;
}

export interface ErrResult {
  ok: false;
  requestId?: string;
  error: { code: WatcherErrorCode; message: string };
}

export type ServiceResult<T> = OkResult<T> | ErrResult;

export function ok<T>(value: T, requestId?: string): OkResult<T> {
  return { ok: true, requestId, value };
}

export function err(code: WatcherErrorCode, message: string, requestId?: string): ErrResult {
  return { ok: false, requestId, error: { code, message } };
}

export function toServiceResult<T>(requestId: string | undefined, fn: () => T): ServiceResult<T> {
  try {
    return ok(fn(), requestId);
  } catch (e) {
    if (e instanceof WatcherError) {
      return err(e.code, e.message, requestId);
    }
    const message = e instanceof Error ? e.message : String(e);
    return err('STORE_UNAVAILABLE', `unexpected failure: ${message}`, requestId);
  }
}

/** Async version: fn may be async; a thrown error object may carry a code field (lightweight tool-layer error). */
export async function toServiceResultAsync<T>(requestId: string | undefined, fn: () => Promise<T> | T): Promise<ServiceResult<T>> {
  try {
    const value = await fn();
    return ok(value, requestId);
  } catch (e) {
    if (e instanceof WatcherError) {
      return err(e.code, e.message, requestId);
    }
    const anyE = e as { code?: string; message?: string };
    if (typeof anyE?.code === 'string') {
      return err(anyE.code as WatcherErrorCode, anyE.message ?? 'error', requestId);
    }
    const message = e instanceof Error ? e.message : String(e);
    return err('STORE_UNAVAILABLE', `unexpected failure: ${message}`, requestId);
  }
}
