import { Memoize } from '@prismamedia/ts-memoize';
import { EventEmitter } from 'events';
import { setInterval } from 'timers';
import { AdapterInterface, GarbageCycle } from './adapter';
import { AcquireTimeoutLockError, LockError } from './error';
import {
  AcquiredLock,
  Lock,
  LockName,
  LockOptions,
  LockSet,
  LockStatus,
  LockType,
  RejectedLock,
  ReleasedLock,
} from './lock';

export enum LockerEventKind {
  RejectedLock = 'rejected_lock',
  AcquiredLock = 'acquired_lock',
  ReleasedLock = 'released_lock',
  GarbageCycle = 'garbage_cycle',
  Error = 'error',
}

export type LockerGarbageCycle = GarbageCycle & { tookInMs: number };

export type LockerEventMap = {
  [LockerEventKind.RejectedLock]: RejectedLock;
  [LockerEventKind.AcquiredLock]: AcquiredLock;
  [LockerEventKind.ReleasedLock]: ReleasedLock;
  [LockerEventKind.GarbageCycle]: LockerGarbageCycle;
  [LockerEventKind.Error]: Error;
};

export type TLockerOptions = Partial<{
  /**
   * Optional, every "gc"ms, a garbage collector cleans the "lost" locks, default: 60000
   */
  gc: number;
}>;

export class Locker extends EventEmitter {
  readonly lockSet = new LockSet();

  #gcInterval: number | undefined;
  #gcIntervalId: ReturnType<typeof setInterval> | undefined;
  #gcIsLocked: boolean = false;

  public constructor(
    readonly adapter: AdapterInterface,
    options?: TLockerOptions,
  ) {
    super();

    this.#gcInterval =
      adapter.gc && typeof options?.gc === 'number'
        ? Math.max(1, options.gc || 60000)
        : undefined;
  }

  public async gc(): Promise<LockerGarbageCycle | undefined> {
    if (this.adapter.gc && this.#gcInterval) {
      const start = process.hrtime.bigint();

      const at = new Date();
      const staleAt = new Date(at.getTime() - this.#gcInterval * 2);

      const cycle = await this.adapter.gc({
        lockSet: this.lockSet,
        gcInterval: this.#gcInterval,
        at,
        staleAt,
      });

      return {
        ...cycle,
        tookInMs: Math.round(Number(process.hrtime.bigint() - start) / 1000000),
      };
    }
  }

  protected enableGc(): void {
    if (!this.#gcIntervalId && this.#gcInterval) {
      this.#gcIntervalId = setInterval(async () => {
        if (this.#gcIsLocked) {
          this.emit(
            'error',
            new Error(
              `The garbage collector has been called despite the previous call is still collecting, the "gc" parameter should be increased, it is currently set at ${
                this.#gcInterval
              }ms`,
            ),
          );

          return;
        }

        this.#gcIsLocked = true;

        try {
          if (this.lockSet.size === 0) {
            if (this.#gcIntervalId) {
              clearInterval(this.#gcIntervalId);

              this.#gcIntervalId = undefined;
            }
          } else {
            let garbageCycle: GarbageCycle | undefined;
            try {
              garbageCycle = await this.gc();
            } catch (error) {
              this.emit(LockerEventKind.Error, error);
            }

            if (garbageCycle) {
              this.emit(LockerEventKind.GarbageCycle, garbageCycle);
            }
          }
        } finally {
          this.#gcIsLocked = false;
        }
      }, this.#gcInterval);
    }
  }

  @Memoize()
  public async setup(): Promise<void> {
    await this.adapter.setup?.({ gcInterval: this.#gcInterval });
  }

  public async releaseAll(): Promise<void> {
    await this.adapter.releaseAll();
    this.lockSet.clear();
  }

  public async release(lock: Lock): Promise<void> {
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

      if (lock.isReleased()) {
        this.emit(LockerEventKind.ReleasedLock, lock);
      }
    } finally {
      this.lockSet.delete(lock);
    }
  }

  public async releaseMany(locks: Iterable<Lock>): Promise<void> {
    await Promise.all([...locks].map((lock) => this.release(lock)));
  }

  protected async lock(
    name: LockName,
    as: LockType,
    options: Partial<LockOptions> = {},
  ): Promise<AcquiredLock> {
    const lock = new Lock(name, as, options);
    this.lockSet.add(lock);
    this.enableGc();

    try {
      return await new Promise<AcquiredLock>(async (resolve, reject) => {
        const acquireTimeout = lock.options.acquireTimeout;
        const acquireTimeoutId =
          acquireTimeout != null && acquireTimeout > 0
            ? setTimeout(
                () => reject(new AcquireTimeoutLockError(lock, acquireTimeout)),
                acquireTimeout,
              )
            : undefined;

        try {
          await this.adapter.lock(lock);

          lock.isAcquired()
            ? resolve(lock)
            : reject(
                lock.reason ||
                  new LockError(
                    lock,
                    `The lock "${lock}" has not been acquired`,
                  ),
              );
        } catch (error) {
          reject(error);
        } finally {
          acquireTimeoutId && clearTimeout(acquireTimeoutId);
        }
      });
    } catch (error) {
      this.lockSet.delete(lock);

      lock.reject(
        error instanceof LockError
          ? error
          : new LockError(
              lock,
              `The lock "${lock}" has not been acquired: ${error}`,
            ),
      );

      throw lock.reason;
    } finally {
      if (lock.isAcquired()) {
        this.emit(LockerEventKind.AcquiredLock, lock);
      } else if (lock.isRejected()) {
        this.emit(LockerEventKind.RejectedLock, lock);
      }
    }
  }

  protected async ensureTaskConcurrency<TResult>(
    name: LockName,
    task: (lock: AcquiredLock) => TResult | Promise<TResult>,
    as: LockType,
    options?: Partial<LockOptions>,
  ): Promise<TResult> {
    const lock = await this.lock(name, as, options);

    try {
      return await task(lock);
    } finally {
      await this.release(lock);
    }
  }

  public async lockAsWriter(
    name: LockName,
    options?: Partial<LockOptions>,
  ): Promise<Lock> {
    return this.lock(name, LockType.Writer, options);
  }

  public async ensureWritingTaskConcurrency<TResult>(
    name: LockName,
    task: (lock: AcquiredLock) => TResult | Promise<TResult>,
    options?: Partial<LockOptions>,
  ): Promise<TResult> {
    return this.ensureTaskConcurrency(name, task, LockType.Writer, options);
  }

  public async lockAsReader(
    name: LockName,
    options?: Partial<LockOptions>,
  ): Promise<Lock> {
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
