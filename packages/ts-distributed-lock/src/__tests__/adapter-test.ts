import { Locker } from '..';
import { AdapterInterface } from '../adapter';
import { AcquireTimeoutLockError } from '../error';
import { Lock, LockName, LockSet, LockStatus } from '../lock';
import { LockerEventKind, LockerEventMap } from '../locker';
import { sleep } from '../utils';

export function testAdapter(adapter: () => AdapterInterface): void {
  return describe('Test adapter suite', () => {
    let locker: Locker;

    beforeEach(async () => {
      locker = new Locker(adapter(), { gc: 1000 });

      await locker.setup();
      await locker.releaseAll();
    });

    afterEach(async () => {
      await locker.releaseAll();
    });

    it(`has a working garbage collector - nothing happens`, async () => {
      const gc = 500;
      locker = new Locker(adapter(), { gc });

      let collectedCount = 0;
      let refreshedCount = 0;
      locker.on(
        LockerEventKind.GarbageCycle,
        (garbageCycle: LockerEventMap[LockerEventKind.GarbageCycle]) => {
          collectedCount += garbageCycle.collectedCount;
          refreshedCount += garbageCycle.refreshedCount;
        },
      );

      const firstLockName: LockName = 'my-gc-works';
      const secondLockName: LockName = 'my-gc-still-works';

      const locks = await Promise.all([
        locker.lockAsReader(firstLockName),
        locker.lockAsReader(firstLockName),
        locker.lockAsReader(secondLockName),
        locker.lockAsReader(secondLockName),
      ]);

      // Wait more than 2 * "gc" interval
      await sleep(4 * gc);

      expect(collectedCount).toBe(0);
      expect(refreshedCount).toBeGreaterThanOrEqual(2);

      // Did not "collect" any locks as they were still used
      await expect(locker.releaseMany(locks)).resolves.toBeUndefined();
    });

    it(`has a working garbage collector - some locks have actually been collected`, async () => {
      const gc = 500;
      locker = new Locker(adapter(), { gc });

      let collectedCount = 0;
      let refreshedCount = 0;
      locker.on(
        LockerEventKind.GarbageCycle,
        (garbageCycle: LockerEventMap[LockerEventKind.GarbageCycle]) => {
          collectedCount += garbageCycle.collectedCount;
          refreshedCount += garbageCycle.refreshedCount;
        },
      );

      const firstLockName: LockName = 'my-gc-has-collected-locks';
      const secondLockName: LockName = 'my-gc-has-collected-other-locks';

      const locks = await Promise.all([
        locker.lockAsReader(firstLockName),
        locker.lockAsReader(firstLockName),
        locker.lockAsReader(firstLockName),
        locker.lockAsReader(secondLockName),
        locker.lockAsReader(secondLockName),
      ]);

      // Let those 2 locks be "unmanaged", they won't be keep by the GC
      locker.lockSet.delete(locks[1]);
      locker.lockSet.delete(locks[3]);
      locker.lockSet.delete(locks[4]);

      // Wait more than 2 * "gc" interval
      await sleep(4 * gc);

      expect(collectedCount).toBeGreaterThanOrEqual(2);
      expect(refreshedCount).toBeGreaterThanOrEqual(2);

      await Promise.all([
        expect(locker.adapter.release(locks[1])).rejects.toThrowError(
          `The lock "${locks[1]}" was not in the queue anymore`,
        ),
        expect(locker.adapter.release(locks[3])).rejects.toThrowError(
          `The lock "${locks[3]}" was not in the queue anymore`,
        ),
        expect(locker.adapter.release(locks[4])).rejects.toThrowError(
          `The lock "${locks[4]}" was not in the queue anymore`,
        ),
      ]);
    });

    it('works as expected', async () => {
      locker
        .on(
          LockerEventKind.AcquiredLock,
          (lock: LockerEventMap[LockerEventKind.AcquiredLock]) => {
            expect(lock).toBeInstanceOf(Lock);
            expect(lock.status).toBe(LockStatus.Acquired);
            expect(lock.settledAt).toBeInstanceOf(Date);
            expect(lock.settledIn).toEqual(expect.any(Number));
          },
        )
        .on(
          LockerEventKind.ReleasedLock,
          (lock: LockerEventMap[LockerEventKind.ReleasedLock]) => {
            expect(lock).toBeInstanceOf(Lock);
            expect(lock.status).toBe(LockStatus.Released);
            expect(lock.settledAt).toBeInstanceOf(Date);
            expect(lock.settledIn).toEqual(expect.any(Number));
            expect(lock.releasedAt).toBeInstanceOf(Date);
            expect(lock.acquiredFor).toEqual(expect.any(Number));
          },
        );

      const lockName: LockName = 'my-lock';

      // The first "read" lock is acquired
      await expect(locker.lockAsReader(lockName)).resolves.toBeInstanceOf(Lock);
      // The second "read" lock is acquired too
      await expect(locker.lockAsReader(lockName)).resolves.toBeInstanceOf(Lock);

      // 2 locks are now registered
      expect(locker.lockSet.size).toBe(2);

      // The "write" has to wait for the release of the 2 reads (so the timeout is reached and an error is triggered)
      await expect(
        locker.lockAsWriter(lockName, { acquireTimeout: 100 }),
      ).rejects.toThrow(AcquireTimeoutLockError);

      // 2 locks still are now registered
      expect(locker.lockSet.size).toBe(2);

      // We release all the locks
      await locker.releaseMany(locker.lockSet);

      // no more lock is registered
      expect(locker.lockSet.size).toBe(0);
    });

    it('works as expected for concurrency', async () => {
      const lockName: LockName = 'my-another-lock';

      async function getConcurrency(
        concurrentLockSet: LockSet,
        lock: Lock,
      ): Promise<number> {
        concurrentLockSet.add(lock);
        const concurrency = concurrentLockSet.size;
        await sleep(Math.floor(100 + Math.random() * 400));
        concurrentLockSet.delete(lock);

        return concurrency;
      }

      async function getMaxConcurrency(
        tasks: Promise<number>[],
      ): Promise<number> {
        return Math.max(...((await Promise.all(tasks)) as any));
      }

      const concurrentLockSet = new LockSet();

      await expect(
        getMaxConcurrency([
          // 5 "read" tasks that run concurrently
          locker.ensureReadingTaskConcurrency(lockName, async (lock) =>
            getConcurrency(concurrentLockSet, lock),
          ),
          locker.ensureReadingTaskConcurrency(lockName, async (lock) =>
            getConcurrency(concurrentLockSet, lock),
          ),
          locker.ensureReadingTaskConcurrency(lockName, async (lock) =>
            getConcurrency(concurrentLockSet, lock),
          ),
          locker.ensureReadingTaskConcurrency(lockName, async (lock) =>
            getConcurrency(concurrentLockSet, lock),
          ),
          locker.ensureReadingTaskConcurrency(lockName, async (lock) =>
            getConcurrency(concurrentLockSet, lock),
          ),
        ]),
      ).resolves.toBe(5);

      expect(locker.lockSet.size).toBe(0);

      await expect(
        getMaxConcurrency([
          // 2 "write" tasks that run serially
          locker.ensureWritingTaskConcurrency(lockName, async (lock) =>
            getConcurrency(concurrentLockSet, lock),
          ),
          locker.ensureWritingTaskConcurrency(lockName, async (lock) =>
            getConcurrency(concurrentLockSet, lock),
          ),
        ]),
      ).resolves.toBe(1);

      expect(locker.lockSet.size).toBe(0);
    });

    it('works with high concurrency', async () => {
      const lockName = 'high-concurrency';

      await expect(
        Promise.all([
          Promise.all(
            [...new Array(10)].map(async () =>
              locker.ensureWritingTaskConcurrency(
                lockName,
                async () => sleep(100),
                { pullInterval: 5 },
              ),
            ),
          ),
          Promise.all(
            [...new Array(100)].map(async () =>
              locker.ensureReadingTaskConcurrency(
                lockName,
                async () => sleep(1000),
                { pullInterval: 5 },
              ),
            ),
          ),
        ]),
      ).resolves.toBeTruthy();
    }, 30000);
  });
}
