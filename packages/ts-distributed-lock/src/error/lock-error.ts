import { Lock } from '../lock';

export class LockError extends Error {
  public constructor(protected lock: Lock, reason?: string) {
    super(reason);

    Object.defineProperty(this, 'name', {
      value: new.target.name,
      enumerable: false,
    });

    Object.setPrototypeOf(this, new.target.prototype);
  }
}
