import { Memoize } from 'typescript-memoize';
import uniqid from 'uniqid';
import { LockError, WorkflowLockError } from './error';

export * from './lock/set';

export type LockName = string;

export type LockId = string;

export enum LockType {
  Writer,
  Reader,
}

export enum LockStatus {
  Acquiring,
  Acquired,
  Releasing,
  Released,
  Rejected,
}

export type LockOptions = {
  /**
   * Optional, an error will be thrown if the lock is not acquired within "acquireTimeout"ms, default: none
   */
  acquireTimeout: number | null;

  /**
   * Optional, in case the lock is not acquired, a new try will occur every "pullInterval"ms, default: 25
   */
  pullInterval: number | null;
};

export class Lock {
  private _id: LockId;
  private _type: LockType;
  private _status: LockStatus;
  private _createdAt: Date;
  private _settledAt?: Date;
  private _releasedAt?: Date;
  public reason?: LockError;

  public constructor(readonly name: LockName, type: LockType, readonly options: Partial<LockOptions> = {}) {
    this._id = uniqid();
    this._type = type;
    this._status = LockStatus.Acquiring;
    this._createdAt = new Date();
  }

  public get id(): LockId {
    return this._id;
  }

  public get type(): LockType {
    return this._type;
  }

  public get status(): LockStatus {
    return this._status;
  }

  public get createdAt(): Date {
    return this._createdAt;
  }

  /**
   * A lock is settled when it has been acquired or rejected
   */
  public get settledAt(): Date | undefined {
    return this._settledAt;
  }

  public get releasedAt(): Date | undefined {
    return this._releasedAt;
  }

  public toString(): string {
    return `${this.name}/${this._id} (${LockType[this._type]} - ${LockStatus[this._status]})`;
  }

  public set status(status: LockStatus) {
    if (
      !(
        (this._status === LockStatus.Acquiring && (status === LockStatus.Acquired || status === LockStatus.Rejected)) ||
        (this._status === LockStatus.Acquired && (status === LockStatus.Releasing || status === LockStatus.Released)) ||
        (this._status === LockStatus.Releasing && status === LockStatus.Released)
      )
    ) {
      throw new WorkflowLockError(this, status);
    } else if (status === LockStatus.Acquired || status === LockStatus.Rejected) {
      this._settledAt = new Date();
    } else if (status === LockStatus.Released) {
      this._releasedAt = new Date();
    }

    this._status = status;
  }

  public isSettled(): boolean {
    return typeof this._settledAt !== 'undefined';
  }

  public isAcquiring(): boolean {
    return this._status === LockStatus.Acquiring;
  }

  public isAcquired(): boolean {
    return this._status === LockStatus.Acquired;
  }

  public isReleasing(): boolean {
    return this._status === LockStatus.Releasing;
  }

  public isReleased(): boolean {
    return this._status === LockStatus.Released;
  }

  public isRejected(): boolean {
    return this._status === LockStatus.Rejected;
  }

  public reject(reason: LockError): this {
    this.reason = reason;
    this.status = LockStatus.Rejected;

    return this;
  }

  @Memoize()
  public get acquireTimeout(): number | undefined {
    if (this.options.acquireTimeout != null) {
      if (this.options.acquireTimeout <= 0) {
        throw new LockError(this, `The lock "${this}"'s "acquireTimeout" option has to be greater than 0`);
      }

      return this.options.acquireTimeout;
    }

    return undefined;
  }

  @Memoize()
  public get pullInterval(): number {
    if (this.options.pullInterval != null) {
      if (this.options.pullInterval <= 0) {
        throw new LockError(this, `The lock "${this}"'s "pullInterval" option has to be greater than 0`);
      }

      return this.options.pullInterval;
    }

    return 25;
  }
}
