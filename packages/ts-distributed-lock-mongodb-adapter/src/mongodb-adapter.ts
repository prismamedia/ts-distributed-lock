import {
  AdapterGarbageCollectorParams,
  AdapterInterface,
  AdapterSetupParams,
  GarbageCycle,
  Lock,
  LockError,
  LockId,
  LockName,
  LockStatus,
  LockType,
  sleep,
} from '@prismamedia/ts-distributed-lock';
import { Memoize } from '@prismamedia/ts-memoize';
import {
  Collection,
  CreateIndexesOptions,
  Db,
  IndexDirection,
  IndexSpecification,
  MongoClient,
  MongoClientOptions,
  MongoError,
  ReadPreference,
} from 'mongodb';
import { Except, SetRequired } from 'type-fest';

type IndexDefinition = {
  specs: IndexSpecification;
  options: SetRequired<CreateIndexesOptions, 'name'>;
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

export type MongoDBAdapterOptions = Except<
  MongoClientOptions,
  'readPreference'
> & {
  /**
   * Name of the collection where the locks are stored
   *
   * Default: locks
   */
  collectionName?: string;

  /**
   * MongoDB's semantic version, saves a query if known (supports incomplete version like "3" or "3.2")
   */
  serverVersion?: string;
};

export class MongoDBAdapter implements AdapterInterface {
  #client: MongoClient;
  #collectionName: string;

  public constructor(
    /**
     * @see: https://docs.mongodb.com/manual/reference/connection-string/
     */
    url: string,
    { collectionName, serverVersion, ...options }: MongoDBAdapterOptions = {},
  ) {
    this.#client = new MongoClient(url, {
      ...options,
      readPreference: ReadPreference.PRIMARY,
    });
    this.#collectionName = collectionName || 'locks';
  }

  @Memoize()
  protected async getDb(): Promise<Db> {
    await this.#client.connect();

    return this.#client.db();
  }

  @Memoize()
  protected async getCollection(): Promise<Collection<Document>> {
    const db = await this.getDb();

    return db.collection(this.#collectionName);
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
    const indices: IndexDefinition[] = [
      {
        specs: { name: 1 },
        options: {
          name: 'idx_name',
          unique: true,
        },
      },
      {
        specs: { 'queue.id': 1 },
        options: {
          name: 'idx_queue_id',
        },
      },
    ];

    if (gcInterval) {
      indices.push({
        specs: { at: 1 },
        options: {
          name: 'idx_at',
          expireAfterSeconds: Math.ceil((gcInterval * 3) / 1000),
        },
      });
    }

    const db = await this.getDb();

    try {
      await db.createCollection(this.#collectionName);
    } catch (error) {
      if (error instanceof MongoError && error.code === 48) {
        // Do nothing, the collection already axists
      } else {
        throw error;
      }
    }

    const collection = await this.getCollection();
    const currentIndices: {
      v: number;
      key: Record<string, IndexDirection>;
      name: string;
      ns: string;
    }[] = await collection.listIndexes().toArray();

    await Promise.all([
      ...indices.map(async ({ specs, options }) => {
        try {
          await collection.createIndex(specs, options);
        } catch (error) {
          console.debug(error);

          await collection.dropIndex(options.name);
          await collection.createIndex(specs, options);
        }
      }),
      ...currentIndices.map(async ({ key, name }) => {
        if (
          !(Object.keys(key).length === 1 && key._id === 1) &&
          !indices.find((indice) => indice.options.name === name)
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
        {
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

  protected isLockAcquired(
    lock: Lock,
    document: Document | null | undefined,
  ): boolean {
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
            await collection.findOne({ 'queue.id': lock.id }),
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
