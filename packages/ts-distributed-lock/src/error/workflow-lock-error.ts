import { Lock, LockStatus } from '../lock';
import { LockError } from './lock-error';

export class WorkflowLockError extends LockError {
  public constructor(lock: Lock, protected to: LockStatus) {
    super(lock, `The lock "${lock}" cannot be set to ${to}`);
  }
}
