import Locker from '..';
import { AdapterInterface } from '../adapter';
import { AcquireTimeoutLockError } from '../error';
import { Lock, LockName, LockSet } from '../lock';
import { sleep } from '../utils';

export function testAdapter(adapter: () => AdapterInterface): void {
  let locker: Locker;

  beforeEach(async done => {
    locker = new Locker(adapter(), { gc: 5000 });

    try {
      await locker.setup();
      await locker.releaseAll();

      done();
    } catch (error) {
      done.fail(error);
    }
  });

  it('works as expected', async done => {
    const lockName: LockName = 'my-lock';

    // The first "read" lock is acquired
    await expect(locker.lockAsReader(lockName)).resolves.toBeInstanceOf(Lock);
    // The second "read" lock is acquired too
    await expect(locker.lockAsReader(lockName)).resolves.toBeInstanceOf(Lock);

    expect(locker.lockSet.size).toBe(2);

    // The "write" has to wait for the release of the 2 reads (so the timeout is reached and an error is triggered)
    await expect(locker.lockAsWriter(lockName, { acquireTimeout: 100 })).rejects.toThrow(AcquireTimeoutLockError);

    expect(locker.lockSet.size).toBe(2);

    // We release all the locks
    await locker.release(locker.lockSet.filterByName(lockName));

    expect(locker.lockSet.size).toBe(0);

    done();
  });

  it('works as expected for concurrency', async done => {
    const lockName: LockName = 'my-another-lock';

    async function getConcurrency(concurrentLockSet: LockSet, lock: Lock): Promise<number> {
      concurrentLockSet.add(lock);
      const concurrency = concurrentLockSet.size;
      await sleep(Math.floor(25 + Math.random() * 75));
      concurrentLockSet.delete(lock);

      return concurrency;
    }

    async function getMaxConcurrency(tasks: Promise<number>[]): Promise<number> {
      return Math.max(...((await Promise.all(tasks)) as any));
    }

    const concurrentLockSet = new LockSet();

    await expect(
      getMaxConcurrency([
        // 5 "read" tasks that run concurrently
        locker.ensureReadingTaskConcurrency(lockName, async lock => getConcurrency(concurrentLockSet, lock)),
        locker.ensureReadingTaskConcurrency(lockName, async lock => getConcurrency(concurrentLockSet, lock)),
        locker.ensureReadingTaskConcurrency(lockName, async lock => getConcurrency(concurrentLockSet, lock)),
        locker.ensureReadingTaskConcurrency(lockName, async lock => getConcurrency(concurrentLockSet, lock)),
        locker.ensureReadingTaskConcurrency(lockName, async lock => getConcurrency(concurrentLockSet, lock)),
      ]),
    ).resolves.toBe(5);

    expect(locker.lockSet.size).toBe(0);

    await expect(
      getMaxConcurrency([
        // 2 "write" tasks that run serially
        locker.ensureWritingTaskConcurrency(lockName, async lock => getConcurrency(concurrentLockSet, lock)),
        locker.ensureWritingTaskConcurrency(lockName, async lock => getConcurrency(concurrentLockSet, lock)),
      ]),
    ).resolves.toBe(1);

    expect(locker.lockSet.size).toBe(0);

    done();
  });
}
