import { Lock, LockStatus } from '../lock';
import { LockError } from './lock-error';

export class WorkflowLockError extends LockError {
  public constructor(lock: Lock, to: LockStatus) {
    super(lock, `The lock "${lock}" can't be ${LockStatus[to]}`);
  }
}
