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

export interface SettledLock<TStatus extends LockStatus.Acquired | LockStatus.Rejected> extends Lock {
  settledAt: Date;
  settledIn: number;
  status: TStatus;
}

export interface AcquiredLock extends SettledLock<LockStatus.Acquired> {
  reason: never;
}

export interface RejectedLock extends SettledLock<LockStatus.Rejected> {}

export interface ReleasedLock extends Lock {
  settledAt: Date;
  settledIn: number;
  releasedAt: Date;
  acquiredFor: number;
  status: LockStatus.Released;
  reason: never;
}

export class Lock {
  private _id: LockId;
  private _type: LockType;
  private _status: LockStatus;
  private _createdAt: Date;
  private _settledAt?: Date;
  private _settledIn?: number;
  private _releasedAt?: Date;
  private _acquiredFor?: number;
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

  /**
   * Time, in ms, took by this lock to be settled
   */
  public get settledIn(): number | undefined {
    return this._settledIn;
  }

  public get releasedAt(): Date | undefined {
    return this._releasedAt;
  }

  /**
   * Time, in ms, this lock has been acquired
   */
  public get acquiredFor(): number | undefined {
    return this._acquiredFor;
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
      this._settledIn = this._settledAt.getTime() - this._createdAt.getTime();
    } else if (status === LockStatus.Released) {
      if (!this._settledAt) {
        throw new LockError(this, `Logic error: "${this}" has to be settled for being released`);
      }

      this._releasedAt = new Date();
      this._acquiredFor = this._releasedAt.getTime() - this._settledAt.getTime();
    }

    this._status = status;
  }

  public isSettled(): boolean {
    return typeof this._settledAt !== 'undefined';
  }

  public isAcquiring(): boolean {
    return this._status === LockStatus.Acquiring;
  }

  public isAcquired(): this is AcquiredLock {
    return this._status === LockStatus.Acquired;
  }

  public isReleasing(): boolean {
    return this._status === LockStatus.Releasing;
  }

  public isReleased(): this is ReleasedLock {
    return this._status === LockStatus.Released;
  }

  public isRejected(): this is RejectedLock {
    return this._status === LockStatus.Rejected;
  }

  public reject(reason: LockError): void {
    this.reason = reason;
    this.status = LockStatus.Rejected;
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
