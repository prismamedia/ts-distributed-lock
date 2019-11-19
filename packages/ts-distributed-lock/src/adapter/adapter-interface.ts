import { Lock, LockSet } from '../lock';

export type AdapterLockParams = {
  lock: Lock;
  gcInterval: number | null;
};

export type AdapterReleaseParams = {
  lock: Lock;
  gcInterval: number | null;
};

export type AdapterSetupParams = {
  /**
   * Either the garbage collector is enabled (>= 1, in ms) or not (null)
   */
  gcInterval: number | null;
};

export type AdapterGarbageCollectorParams = {
  lockSet: LockSet;
  gcInterval: number;
  at: Date;

  /**
   * The locks not refreshed before "staleAt" are stale: at - 2 * gcInterval
   */
  staleAt: Date;
};

/**
 * Number of locks collected
 */
export type GarbageCycle = number;

/**
 * AdapterInterface
 */
export interface AdapterInterface {
  /**
   * Acquires the given lock
   */
  lock: (lock: Lock) => Promise<void>;

  /**
   * Releases the given lock
   */
  release: (lock: Lock) => Promise<void>;

  /**
   * Release all the locks
   */
  releaseAll: () => Promise<void>;

  /**
   * Optional, the adapter may provides a garbage collector to clean the "lost" locks
   */
  gc?: (params: AdapterGarbageCollectorParams) => Promise<GarbageCycle>;

  /**
   * Optional, the adapter may needs to be setup before use
   */
  setup?: (params: AdapterSetupParams) => Promise<void>;
}
