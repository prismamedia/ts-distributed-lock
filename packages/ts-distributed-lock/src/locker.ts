import { EventConfigMap, EventEmitter } from '@prismamedia/ts-async-event-emitter';
import { setInterval } from 'timers';
import { Memoize } from 'typescript-memoize';
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
}

export type LockerEventMap = {
  [LockerEventKind.RejectedLock]: RejectedLock;
  [LockerEventKind.AcquiredLock]: AcquiredLock;
  [LockerEventKind.ReleasedLock]: ReleasedLock;
  [LockerEventKind.GarbageCycle]: GarbageCycle;
};

export type LockerOptions<TLockerEventMap extends LockerEventMap = LockerEventMap> = {
  /**
   * Optional, every "gc"ms, a garbage collector cleans the "lost" locks, default: 60000
   */
  gc?: number | null;

  /**
   * Optional, act on some events
   */
  on?: EventConfigMap<TLockerEventMap>;
};

export class Locker<TLockerEventMap extends LockerEventMap = LockerEventMap> extends EventEmitter<TLockerEventMap> {
  readonly lockSet = new LockSet();

  protected gcInterval: number | null;
  protected gcIntervalId?: ReturnType<typeof setInterval>;

  public constructor(
    readonly adapter: AdapterInterface,
    readonly options: Partial<LockerOptions<TLockerEventMap>> = {},
  ) {
    super(options.on);

    this.gcInterval = adapter.gc && options.gc !== null ? Math.max(1, options.gc || 60000) : null;
  }

  public async gc(): Promise<GarbageCycle | undefined> {
    if (this.adapter.gc && this.gcInterval) {
      const at = new Date();
      const staleAt = new Date(at.getTime() - this.gcInterval * 2);

      const garbageCycle = await this.adapter.gc({
        lockSet: this.lockSet,
        gcInterval: this.gcInterval,
        at,
        staleAt,
      });

      if (garbageCycle > 0) {
        this.emit(LockerEventKind.GarbageCycle, garbageCycle).catch(error => {
          // Do nothing on error
        });
      }

      return garbageCycle;
    }
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
        this.emit(LockerEventKind.ReleasedLock, lock).catch(error => {
          // Do nothing on error
        });
      }
    } finally {
      this.lockSet.delete(lock);
    }
  }

  public async releaseMany(locks: Iterable<Lock>): Promise<void> {
    await Promise.all([...locks].map(lock => this.release(lock)));
  }

  protected async lock(name: LockName, as: LockType, options: Partial<LockOptions> = {}): Promise<AcquiredLock> {
    const lock = new Lock(name, as, options);
    this.lockSet.add(lock);
    this.enableGc();

    try {
      return await new Promise<AcquiredLock>(async (resolve, reject) => {
        const acquireTimeout = lock.options.acquireTimeout;
        const acquireTimeoutId =
          acquireTimeout != null && acquireTimeout > 0
            ? setTimeout(() => reject(new AcquireTimeoutLockError(lock, acquireTimeout)), acquireTimeout)
            : undefined;

        try {
          await this.adapter.lock(lock);

          lock.isAcquired()
            ? resolve(lock)
            : reject(lock.reason || new LockError(lock, `The lock "${lock}" has not been acquired`));
        } catch (error) {
          reject(error);
        } finally {
          acquireTimeoutId && clearTimeout(acquireTimeoutId);
        }
      });
    } catch (error) {
      this.lockSet.delete(lock);

      lock.reject(
        error instanceof LockError ? error : new LockError(lock, `The lock "${lock}" has not been acquired: ${error}`),
      );

      throw lock.reason;
    } finally {
      if (lock.isAcquired()) {
        this.emit(LockerEventKind.AcquiredLock, lock).catch(error => {
          // Do nothing on error
        });
      } else if (lock.isRejected()) {
        this.emit(LockerEventKind.RejectedLock, lock).catch(error => {
          // Do nothing on error
        });
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
