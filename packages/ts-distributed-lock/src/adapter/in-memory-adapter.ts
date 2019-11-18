import { AdapterError } from '../error/adapter-error';
import { Lock, LockName, LockStatus, LockType } from '../lock';
import { sleep } from '../utils';
import { AdapterGarbageCollectorParams, AdapterInterface } from './adapter-interface';

/**
 * For test & debug purpose as it can't be distributed
 */
export class InMemoryAdapter implements AdapterInterface {
  private storage: Map<LockName, Map<Lock, Date>> = new Map();

  public async setup() {
    // Do nothing
  }

  public async releaseAll() {
    this.storage.clear();
  }

  public async gc({ lockSet, staleAt }: AdapterGarbageCollectorParams) {
    this.storage.forEach(queue =>
      queue.forEach((at, lock) => {
        // We delete the locks not refreshed soon enought
        if (at < staleAt) {
          queue.delete(lock);
        }

        // We refresh the registered locks
        if (lockSet.has(lock)) {
          queue.set(lock, new Date());
        }
      }),
    );
  }

  public async lock(lock: Lock) {
    let queue = this.storage.get(lock.name);
    if (!queue) {
      queue = new Map();
      this.storage.set(lock.name, queue);
    }

    queue.set(lock, new Date());

    do {
      if (lock.type === LockType.Writer) {
        // A "write" lock is acquired when it's the first in the queue
        if ([...queue.keys()].indexOf(lock) === 0) {
          lock.status = LockStatus.Acquired;
        }
      } else {
        // A "read" lock is acquired when it's not preceded by a "write" lock in the queue
        if ([...queue.keys()].find(lockInSet => lockInSet === lock || lockInSet.type === LockType.Writer) === lock) {
          lock.status = LockStatus.Acquired;
        }
      }
    } while (lock.isAcquiring() && (await sleep(lock.pullInterval)));

    if (!lock.isAcquired()) {
      queue.delete(lock);
    }
  }

  public async release(lock: Lock) {
    if (!this.storage.get(lock.name)?.delete(lock)) {
      throw new AdapterError(this, `The lock "${lock}" was not in the queue anymore`);
    }

    lock.status = LockStatus.Released;
  }
}
