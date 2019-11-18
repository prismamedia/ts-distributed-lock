import { AdapterInterface } from '../adapter';
import { Lock } from '../lock';
import { LockError } from './lock-error';

export class AdapterLockError extends LockError {
  public constructor(readonly adapter: AdapterInterface, lock: Lock, reason?: string) {
    super(lock, reason);
  }
}
