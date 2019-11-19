export class LockerError extends Error {
  public constructor(reason?: string) {
    super(reason);

    Object.defineProperty(this, 'name', {
      value: new.target.name,
      enumerable: false,
    });

    Object.setPrototypeOf(this, new.target.prototype);
  }
}
