import { Memoize } from '@prismamedia/ts-memoize';
import crypto from 'crypto';
import { LockError, WorkflowLockError } from './error';

export * from './lock/set';

export type LockName = string;

export type LockId = string;

export enum LockType {
  Writer = 'WRITER',
  Reader = 'READER',
}

export enum LockStatus {
  Acquiring = 'ACQUIRING',
  Acquired = 'ACQUIRED',
  Releasing = 'RELEASING',
  Released = 'RELEASED',
  Rejected = 'REJECTED',
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

export interface SettledLock<
  TStatus extends LockStatus.Acquired | LockStatus.Rejected
> extends Lock {
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
  #id: LockId;
  #type: LockType;
  #status: LockStatus;
  #createdAt: Date;
  #settledAt?: Date;
  #settledIn?: number;
  #releasedAt?: Date;
  #acquiredFor?: number;
  reason?: LockError;

  public constructor(
    readonly name: LockName,
    type: LockType,
    readonly options: Partial<LockOptions> = {},
  ) {
    this.#id = crypto.randomBytes(4).toString('hex');
    this.#type = type;
    this.#status = LockStatus.Acquiring;
    this.#createdAt = new Date();
  }

  public get id(): LockId {
    return this.#id;
  }

  public get type(): LockType {
    return this.#type;
  }

  public get status(): LockStatus {
    return this.#status;
  }

  public get createdAt(): Date {
    return this.#createdAt;
  }

  /**
   * A lock is settled when it has been acquired or rejected
   */
  public get settledAt(): Date | undefined {
    return this.#settledAt;
  }

  /**
   * Time, in ms, took by this lock to be settled
   */
  public get settledIn(): number | undefined {
    return this.#settledIn;
  }

  public get releasedAt(): Date | undefined {
    return this.#releasedAt;
  }

  /**
   * Time, in ms, this lock has been acquired
   */
  public get acquiredFor(): number | undefined {
    return this.#acquiredFor;
  }

  public toString(): string {
    return `${this.name}/${this.#id} (${this.#type} - ${this.#status})`;
  }

  public set status(status: LockStatus) {
    if (
      !(
        (this.#status === LockStatus.Acquiring &&
          (status === LockStatus.Acquired || status === LockStatus.Rejected)) ||
        (this.#status === LockStatus.Acquired &&
          (status === LockStatus.Releasing ||
            status === LockStatus.Released)) ||
        (this.#status === LockStatus.Releasing &&
          status === LockStatus.Released)
      )
    ) {
      throw new WorkflowLockError(this, status);
    } else if (
      status === LockStatus.Acquired ||
      status === LockStatus.Rejected
    ) {
      this.#settledAt = new Date();
      this.#settledIn = this.#settledAt.getTime() - this.#createdAt.getTime();
    } else if (status === LockStatus.Released) {
      if (!this.#settledAt) {
        throw new LockError(
          this,
          `Logic error: "${this}" has to be settled for being released`,
        );
      }

      this.#releasedAt = new Date();
      this.#acquiredFor =
        this.#releasedAt.getTime() - this.#settledAt.getTime();
    }

    this.#status = status;
  }

  public isSettled(): boolean {
    return typeof this.#settledAt !== 'undefined';
  }

  public isAcquiring(): boolean {
    return this.#status === LockStatus.Acquiring;
  }

  public isAcquired(): this is AcquiredLock {
    return this.#status === LockStatus.Acquired;
  }

  public isReleasing(): boolean {
    return this.#status === LockStatus.Releasing;
  }

  public isReleased(): this is ReleasedLock {
    return this.#status === LockStatus.Released;
  }

  public isRejected(): this is RejectedLock {
    return this.#status === LockStatus.Rejected;
  }

  public reject(reason: LockError): void {
    this.reason = reason;
    this.status = LockStatus.Rejected;
  }

  @Memoize()
  public get acquireTimeout(): number | undefined {
    if (this.options.acquireTimeout != null) {
      if (this.options.acquireTimeout <= 0) {
        throw new LockError(
          this,
          `The lock "${this}"'s "acquireTimeout" option has to be greater than 0`,
        );
      }

      return this.options.acquireTimeout;
    }

    return undefined;
  }

  @Memoize()
  public get pullInterval(): number {
    if (this.options.pullInterval != null) {
      if (this.options.pullInterval <= 0) {
        throw new LockError(
          this,
          `The lock "${this}"'s "pullInterval" option has to be greater than 0`,
        );
      }

      return this.options.pullInterval;
    }

    return 25;
  }
}
