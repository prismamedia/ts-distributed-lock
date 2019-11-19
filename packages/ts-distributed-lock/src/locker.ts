import { setInterval } from 'timers';
import { Memoize } from 'typescript-memoize';
import { AdapterInterface } from './adapter';
import { AcquireTimeoutLockError, LockError } from './error';
import { AcquiredLock, Lock, LockName, LockOptions, LockSet, LockStatus, LockType } from './lock';

export type LockerOptions = {
  /**
   * Optional, every "gc"ms, a garbage collector cleans the "lost" locks, default: 60000
   */
  gc?: number | null;
};

export class Locker {
  readonly lockSet = new LockSet();

  protected gcInterval: number | null;
  protected gcIntervalId?: ReturnType<typeof setInterval>;

  public constructor(readonly adapter: AdapterInterface, readonly options: Partial<LockerOptions> = {}) {
    this.gcInterval = adapter.gc && options.gc !== null ? Math.max(1, options.gc || 60000) : null;
  }

  public async gc(): Promise<void> {
    if (this.adapter.gc && this.gcInterval) {
      const staleAt = new Date(new Date().getTime() - this.gcInterval * 2);

      await this.adapter.gc({
        lockSet: this.lockSet,
        gcInterval: this.gcInterval,
        staleAt,
      });
    }
  }

  @Memoize()
  public async setup(): Promise<void> {
    if (this.adapter.setup) {
      await this.adapter.setup({ gcInterval: this.gcInterval });
    }
  }

  public async releaseAll(): Promise<void> {
    await this.adapter.releaseAll();
    this.lockSet.clear();
  }

  protected enableGc(): void {
    if (!this.gcIntervalId && this.gcInterval) {
      this.gcIntervalId = setInterval(async () => {
        if (this.lockSet.size === 0) {
          this.gcIntervalId && clearInterval(this.gcIntervalId);
        } else {
          await this.gc().catch(console.error);
        }
      }, this.gcInterval);
    }
  }

  public async release(lockOrIterableOfLocks: Lock | Iterable<Lock> = this.lockSet): Promise<void> {
    const locks: Lock[] = lockOrIterableOfLocks instanceof Lock ? [lockOrIterableOfLocks] : [...lockOrIterableOfLocks];

    await Promise.all(
      locks.map(async lock => {
        if (lock.status === LockStatus.Releasing || !this.lockSet.has(lock)) {
          // Do nothing, it's already releasing or released

          return;
        } else if (lock.status === LockStatus.Released) {
          this.lockSet.delete(lock);

          return;
        }

        lock.status = LockStatus.Releasing;

        try {
          await this.adapter.release(lock);
        } finally {
          this.lockSet.delete(lock);
        }
      }),
    );
  }

  protected async lock(name: LockName, as: LockType, options: Partial<LockOptions> = {}): Promise<AcquiredLock> {
    const lock = new Lock(name, as, options);
    this.lockSet.add(lock);
    this.enableGc();

    return new Promise<AcquiredLock>(async (resolve, reject) => {
      const acquireTimeout = lock.options.acquireTimeout;
      const acquireTimeoutId =
        acquireTimeout != null && acquireTimeout > 0
          ? setTimeout(() => {
              this.lockSet.delete(lock.reject(new AcquireTimeoutLockError(lock, acquireTimeout)));

              reject(lock.reason);
            }, acquireTimeout)
          : undefined;

      try {
        await this.adapter.lock(lock);

        if (lock.isAcquired()) {
          resolve(lock);
        } else {
          this.lockSet.delete(lock);

          reject(lock.reason || new LockError(lock, `The lock "${lock}" has not been acquired`));
        }
      } catch (error) {
        this.lockSet.delete(lock);

        reject(error);
      } finally {
        acquireTimeoutId && clearTimeout(acquireTimeoutId);
      }
    });
  }

  protected async ensureTaskConcurrency<TResult>(
    name: LockName,
    task: (lock: AcquiredLock) => TResult | Promise<TResult>,
    as: LockType,
    options?: Partial<LockOptions>,
  ): Promise<TResult> {
    const lock = await this.lock(name, as, options);

    try {
      const result = await task(lock);

      return result;
    } finally {
      await this.release(lock);
    }
  }

  public async lockAsWriter(name: LockName, options?: Partial<LockOptions>): Promise<Lock> {
    return this.lock(name, LockType.Writer, options);
  }

  public async ensureWritingTaskConcurrency<TResult>(
    name: LockName,
    task: (lock: AcquiredLock) => TResult | Promise<TResult>,
    options?: Partial<LockOptions>,
  ): Promise<TResult> {
    return this.ensureTaskConcurrency(name, task, LockType.Writer, options);
  }

  public async lockAsReader(name: LockName, options?: Partial<LockOptions>): Promise<Lock> {
    return this.lock(name, LockType.Reader, options);
  }

  public async ensureReadingTaskConcurrency<TResult>(
    name: LockName,
    task: (lock: AcquiredLock) => TResult | Promise<TResult>,
    options?: Partial<LockOptions>,
  ): Promise<TResult> {
    return this.ensureTaskConcurrency(name, task, LockType.Reader, options);
  }
}
