import { WorkflowLockError } from '../error';
import { Lock, LockStatus, LockType } from '../lock';

describe('Lock', () => {
  it('has a proper worflow for acquiring locks', () => {
    const lock = new Lock('my-lock-name', LockType.Writer);
    expect(lock.status).toBe(LockStatus.Acquiring);

    // Can only set as "Acquired" or "Rejected"
    expect(() => (lock.status = LockStatus.Acquiring)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Releasing)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Released)).toThrowError(WorkflowLockError);

    // Not changed
    expect(lock.status).toBe(LockStatus.Acquiring);
  });

  it('has a proper worflow for rejected locks', () => {
    const lock = new Lock('my-lock-name', LockType.Writer);

    lock.status = LockStatus.Rejected;
    expect(lock.status).toBe(LockStatus.Rejected);

    // Cannot be changed
    expect(() => (lock.status = LockStatus.Acquiring)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Acquired)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Releasing)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Released)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Rejected)).toThrowError(WorkflowLockError);
  });

  it('has a proper worflow for acquired locks', () => {
    const lock = new Lock('my-lock-name', LockType.Writer);

    lock.status = LockStatus.Acquired;
    expect(lock.status).toBe(LockStatus.Acquired);

    // Can only set as "Releasing" or "Released"
    expect(() => (lock.status = LockStatus.Acquiring)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Acquired)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Rejected)).toThrowError(WorkflowLockError);

    lock.status = LockStatus.Releasing;
    expect(lock.status).toBe(LockStatus.Releasing);

    // Can only set as "Released"
    expect(() => (lock.status = LockStatus.Acquiring)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Acquired)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Releasing)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Rejected)).toThrowError(WorkflowLockError);

    lock.status = LockStatus.Released;
    expect(lock.status).toBe(LockStatus.Released);

    // Cannot be changed
    expect(() => (lock.status = LockStatus.Acquiring)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Acquired)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Releasing)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Released)).toThrowError(WorkflowLockError);
    expect(() => (lock.status = LockStatus.Rejected)).toThrowError(WorkflowLockError);
  });
});
