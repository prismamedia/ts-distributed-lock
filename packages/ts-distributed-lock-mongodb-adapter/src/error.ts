import { Lock, LockError } from '@prismamedia/ts-distributed-lock';

export class AdapterLockError extends LockError {
  public constructor(lock: Lock, reason?: string) {
    super(lock, reason);
  }
}
