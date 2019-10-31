import { Lock } from '../lock';
import { LockError } from './lock-error';

export class AcquireTimeoutLockError extends LockError {
  public constructor(lock: Lock, acquireTimeout: number) {
    super(lock, `The lock "${lock}" has not been acquired before the timeout: ${acquireTimeout}ms`);
  }
}
