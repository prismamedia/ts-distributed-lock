import {
  AdapterGarbageCollectorParams,
  AdapterInterface,
  AdapterSetupParams,
  GarbageCycle,
  Lock,
  LockerError,
  LockError,
  LockId,
  LockName,
  LockStatus,
  LockType,
  sleep,
} from '@prismamedia/ts-distributed-lock';
import { Memoize } from '@prismamedia/ts-memoize';
import {
  Admin,
  Collection,
  Db,
  IndexSpecification,
  MongoClient,
  MongoClientOptions,
  MongoError,
  ReadPreference,
} from 'mongodb';
import semver, { SemVer } from 'semver';

type NamedIndexSpecification = IndexSpecification & {
  name: NonNullable<IndexSpecification['name']>;
};

type Document = {
  name: LockName;
  queue: {
    id: LockId;
    type: LockType;
    at: Date;
  }[];
  at: Date;
};

export type MongoDBAdapterOptions = Partial<
  Omit<
    MongoClientOptions,
    'validateOptions' | 'useNewUrlParser' | 'useUnifiedTopology'
  > & {
    // Name of the collection where the locks are stored, default: "locks"
    collectionName: string;

    // MongoDB's semantic version, saves a query if known (supports incomplete version like "3" or "3.2")
    serverVersion: string;
  }
>;

export class MongoDBAdapter implements AdapterInterface {
  #client: MongoClient;
  #collectionName: string;
  #serverVersion?: SemVer;

  public constructor(
    /**
     * @see: https://docs.mongodb.com/manual/reference/connection-string/
     */
    url: string,
    { collectionName, serverVersion, ...options }: MongoDBAdapterOptions = {},
  ) {
    this.#client = new MongoClient(url, {
      ...options,
      validateOptions: true,
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });

    this.#collectionName = collectionName || 'locks';

    if (typeof serverVersion !== 'undefined') {
      const coercedServerVersion = semver.coerce(serverVersion);
      if (!(semver.valid(coercedServerVersion) && coercedServerVersion)) {
        throw new LockerError(
          `The provided "serverVersion" is not a valid semantic version: ${serverVersion}`,
        );
      }

      this.#serverVersion = coercedServerVersion;
    }
  }

  @Memoize()
  protected async getDb(): Promise<Db> {
    await this.#client.connect();

    return this.#client.db();
  }

  @Memoize()
  protected async getAdmin(): Promise<Admin> {
    const db = await this.getDb();

    return db.admin();
  }

  @Memoize()
  protected async getServerVersion(): Promise<SemVer> {
    if (this.#serverVersion) {
      return this.#serverVersion;
    }

    const admin = await this.getAdmin();
    const { version: serverVersion } = await admin.serverStatus();

    const coercedServerVersion = semver.coerce(serverVersion);
    if (!(semver.valid(coercedServerVersion) && coercedServerVersion)) {
      throw new LockerError(
        `The returned "serverVersion" is not a valid semantic version: ${serverVersion}`,
      );
    }

    return coercedServerVersion;
  }

  @Memoize()
  protected async getCollection(): Promise<Collection<Document>> {
    const db = await this.getDb();

    return new Promise((resolve, reject) =>
      db.collection(
        this.#collectionName,
        { strict: true },
        (error, collection) => (error ? reject(error) : resolve(collection)),
      ),
    );
  }

  /**
   * Delete the locks not refreshed soon enought
   */
  protected async gcCollect({
    staleAt,
  }: AdapterGarbageCollectorParams): Promise<number> {
    const collection = await this.getCollection();

    const result = await collection.updateMany(
      {},
      { $pull: { queue: { at: { $lt: staleAt } } } },
    );

    return result.modifiedCount;
  }

  /**
   * Refresh the registered locks
   */
  protected async gcRefresh({
    lockSet,
    at,
  }: AdapterGarbageCollectorParams): Promise<number> {
    const collection = await this.getCollection();

    const result = await collection.bulkWrite(
      [...lockSet].map((lock) => ({
        updateOne: {
          filter: { 'queue.id': lock.id },
          update: {
            // By updating these "at" dates, we keep them fresh and out of the next garbage collector's collecting cycle
            $max: {
              // Keep this very lock
              'queue.$.at': at,
              // Keep all locks of the same name, because of the TTL on index
              at,
            },
          },
        },
      })),
      { ordered: false },
    );

    return result.modifiedCount || 0;
  }

  public async gc(
    params: AdapterGarbageCollectorParams,
  ): Promise<GarbageCycle> {
    const [collectedCount, refreshedCount] = await Promise.all([
      this.gcCollect(params),
      this.gcRefresh(params),
    ]);

    return {
      collectedCount,
      refreshedCount,
    };
  }

  public async setup({ gcInterval }: AdapterSetupParams) {
    const indices: NamedIndexSpecification[] = [
      { name: 'idx_name', key: { name: 1 }, unique: true },
      { name: 'idx_queue_id', key: { 'queue.id': 1 } },
    ];

    if (gcInterval) {
      indices.push({
        name: 'idx_at',
        key: { at: 1 },
        expireAfterSeconds: Math.ceil((gcInterval * 3) / 1000),
      });
    }

    const db = await this.getDb();

    try {
      await db.createCollection(this.#collectionName);
    } catch (error) {
      if (!(error instanceof MongoError && error.code === 48)) {
        throw error;
      }

      // Do nothing, the collection already axists
    }

    const collection = await this.getCollection();
    const currentIndices = await collection.listIndexes().toArray();

    await Promise.all([
      ...indices.map(async ({ key, ...options }) => {
        try {
          await collection.createIndex(key, options);
        } catch (error) {
          await collection.dropIndex(options.name);
          await collection.createIndex(key, options);
        }
      }),
      ...currentIndices.map(async ({ name }) => {
        if (
          name !== '_id_' &&
          !indices.find((indice) => indice.name === name)
        ) {
          await collection.dropIndex(name);
        }
      }),
    ]);
  }

  public async releaseAll() {
    const collection = await this.getCollection();
    await collection.deleteMany({});
  }

  protected async enqueueLock(
    lock: Lock,
    tries: number = 3,
  ): Promise<Document> {
    const collection = await this.getCollection();

    try {
      const { value } = await collection.findOneAndUpdate(
        { name: lock.name },
        {
          $setOnInsert: { name: lock.name },
          $max: { at: lock.createdAt },
          $push: {
            queue: { id: lock.id, type: lock.type, at: lock.createdAt },
          },
        },
        // Because "returnDocument" is not supported for now
        <{ upsert: true }>{
          upsert: true,
          returnDocument: 'after',
        },
      );

      if (!value) {
        throw new LockError(lock, `The lock "${lock}" has not been enqueued`);
      }

      return value;
    } catch (error) {
      // We try again in case of "duplicate key" error because of the unique index on "name"
      if (error instanceof MongoError && error.code === 11000 && tries > 1) {
        return this.enqueueLock(lock, tries - 1);
      } else {
        throw new LockError(
          lock,
          `The lock "${lock}" has not been enqueued: ${error.message}`,
        );
      }
    }
  }

  protected async dequeueLock(lock: Lock, ifExists: boolean): Promise<boolean> {
    const collection = await this.getCollection();

    const { modifiedCount } = await collection.updateOne(
      { name: lock.name },
      { $pull: { queue: { id: lock.id } } },
    );

    if (modifiedCount === 0 && !ifExists) {
      throw new LockError(
        lock,
        `The lock "${lock}" was not in the queue anymore`,
      );
    }

    return modifiedCount === 1;
  }

  protected isLockAcquired(lock: Lock, document: Document | null): boolean {
    if (!document) {
      throw new LockError(
        lock,
        `The lock "${lock}" is not in the queue anymore`,
      );
    }

    const acquired =
      lock.type === LockType.Writer
        ? // A "write" lock is acquired when it's the first in the queue
          document.queue[0]?.id === lock.id
        : // A "read" lock is acquired when it's not preceded by a "write" lock in the queue
          document.queue.find(
            ({ id, type }) => id === lock.id || type === LockType.Writer,
          )?.id === lock.id;

    if (acquired) {
      lock.status = LockStatus.Acquired;
    }

    return acquired;
  }

  public async lock(lock: Lock) {
    const collection = await this.getCollection();

    // Push the lock into the dedicated document
    const document = await this.enqueueLock(lock);

    // Either we acquired the lock immediately ...
    if (!this.isLockAcquired(lock, document)) {
      //... or we start pulling every "pullInterval"ms
      try {
        while (
          (await sleep(lock.pullInterval)) &&
          lock.isAcquiring() &&
          !this.isLockAcquired(
            lock,
            await collection.findOne(
              { 'queue.id': lock.id },
              { readPreference: ReadPreference.PRIMARY },
            ),
          )
        ) {
          // Nothing to do here
        }
      } finally {
        if (!lock.isAcquired()) {
          await this.dequeueLock(lock, true);
        }
      }
    }
  }

  public async release(lock: Lock) {
    await this.dequeueLock(lock, false);

    lock.status = LockStatus.Released;
  }
}
