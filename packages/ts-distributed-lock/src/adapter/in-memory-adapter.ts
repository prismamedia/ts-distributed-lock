import { Lock, LockName, LockSet, LockStatus, LockType } from '../lock';
import { sleep } from '../utils';
import { AdapterInterface } from './adapter-interface';

/**
 * For test & debug purpose as it can't be distributed
 */
export class InMemoryAdapter implements AdapterInterface {
  private storage: Map<LockName, LockSet> = new Map();

  public async setup() {
    // Do nothing
  }

  public async releaseAll() {
    this.storage.clear();
  }

  public async gc() {
    // Do nothing
  }

  public async lock(lock: Lock) {
    let lockSet = this.storage.get(lock.name);
    if (!lockSet) {
      lockSet = new LockSet();
      this.storage.set(lock.name, lockSet);
    }

    lockSet.add(lock);

    do {
      if (lock.type === LockType.Writer) {
        // A "write" lock is acquired when it's the first in the queue
        if ([...lockSet].indexOf(lock) === 0) {
          lock.status = LockStatus.Acquired;
        }
      } else {
        // A "read" lock is acquired when it's not preceded by a "write" lock in the queue
        if ([...lockSet].find(lockInSet => lockInSet === lock || lockInSet.type === LockType.Writer) === lock) {
          lock.status = LockStatus.Acquired;
        }
      }
    } while (lock.isAcquiring() && (await sleep(lock.pullInterval)));

    if (!lock.isAcquired()) {
      lockSet.delete(lock);
    }
  }

  public async release(lock: Lock) {
    this.storage.get(lock.name)?.delete(lock);
    lock.status = LockStatus.Released;
  }
}
