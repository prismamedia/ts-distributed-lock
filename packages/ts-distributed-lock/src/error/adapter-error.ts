import { AdapterInterface } from '../adapter';

export class AdapterError extends Error {
  public constructor(readonly adapter: AdapterInterface, reason?: string) {
    super(reason);

    Object.defineProperty(this, 'name', {
      value: new.target.name,
      enumerable: false,
    });

    Object.setPrototypeOf(this, new.target.prototype);
  }
}
