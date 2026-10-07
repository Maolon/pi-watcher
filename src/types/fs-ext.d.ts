declare module 'fs-ext' {
  type FlockMode = 'ex' | 'exnb' | 'sh' | 'shnb' | 'un';
  export function flock(fd: number, mode: FlockMode, callback: (err: Error | null) => void): void;
  export function flock(fd: number, mode: number, callback: (err: Error | null) => void): void;
  export function flockSync(fd: number, mode: FlockMode | number): void;
}
