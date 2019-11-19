import { Lock } from '../lock';
import { LockerError } from './locker-error';

export class LockError extends LockerError {
  public constructor(protected lock: Lock, reason?: string) {
    super(reason);
  }
}
