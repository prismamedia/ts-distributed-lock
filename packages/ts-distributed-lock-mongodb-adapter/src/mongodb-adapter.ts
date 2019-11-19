import {
  AdapterError,
  AdapterGarbageCollectorParams,
  AdapterInterface,
  AdapterLockError,
  AdapterSetupParams,
  Lock,
  LockId,
  LockName,
  LockStatus,
  LockType,
  sleep,
} from '@prismamedia/ts-distributed-lock';
import { Admin, Collection, Db, IndexSpecification, MongoClient, MongoError, ReadPreference } from 'mongodb';
import semver, { SemVer } from 'semver';
import { Memoize } from 'typescript-memoize';

type NamedIndexSpecification = IndexSpecification & { name: NonNullable<IndexSpecification['name']> };

type Document = {
  name: LockName;
  queue: {
    id: LockId;
    type: LockType;
    at: Date;
  }[];
  at: Date;
};

export type MongoDBAdapterOptions = {
  // Name of the collection where the locks are stored
  collectionName?: string;

  // MongoDB's semantic version, saves a query if known (supports incomplete version like "3" or "3.2")
  serverVersion?: string;
};

export class MongoDBAdapter implements AdapterInterface {
  protected client: MongoClient;
  protected collectionName: string;
  protected serverVersion?: SemVer;

  public constructor(urlOrClient: string | MongoClient, protected options: Partial<MongoDBAdapterOptions> = {}) {
    this.client =
      urlOrClient instanceof MongoClient
        ? urlOrClient
        : new MongoClient(urlOrClient, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
            validateOptions: true,
          });

    this.collectionName = options.collectionName || 'locks';

    if (typeof options.serverVersion !== 'undefined') {
      const coercedServerVersion = semver.coerce(options.serverVersion);
      if (!(semver.valid(coercedServerVersion) && coercedServerVersion)) {
        throw new AdapterError(
          this,
          `The provided "serverVersion" is not a valid semantic version: ${options.serverVersion}`,
        );
      }

      this.serverVersion = coercedServerVersion;
    }
  }

  protected async connect(): Promise<void> {
    if (!this.client.isConnected()) {
      await this.client.connect();
    }
  }

  @Memoize()
  protected async getDb(): Promise<Db> {
    await this.connect();

    return this.client.db();
  }

  @Memoize()
  protected async getAdmin(): Promise<Admin> {
    const db = await this.getDb();

    return db.admin();
  }

  @Memoize()
  protected async getServerVersion(): Promise<SemVer> {
    if (this.serverVersion) {
      return this.serverVersion;
    }

    const admin = await this.getAdmin();
    const { version: serverVersion } = await admin.serverStatus();

    const coercedServerVersion = semver.coerce(serverVersion);
    if (!(semver.valid(coercedServerVersion) && coercedServerVersion)) {
      throw new AdapterError(this, `The returned "serverVersion" is not a valid semantic version: ${serverVersion}`);
    }

    return coercedServerVersion;
  }

  @Memoize()
  protected async getCollection(): Promise<Collection<Document>> {
    const db = await this.getDb();

    return new Promise((resolve, reject) =>
      db.collection(this.collectionName, { strict: true }, (error, collection) =>
        error ? reject(error) : resolve(collection),
      ),
    );
  }

  public async gc({ lockSet, staleAt }: AdapterGarbageCollectorParams): Promise<void> {
    const [collection, serverVersion] = await Promise.all([this.getCollection(), this.getServerVersion()]);

    await Promise.all([
      // We delete the locks not refreshed soon enought
      collection.updateMany({}, { $pull: { queue: { at: { $lt: staleAt } } as any } }),

      // We refresh the registered locks
      ...(lockSet.size > 0
        ? [
            semver.gte(serverVersion, '3.6.0')
              ? // https://docs.mongodb.com/manual/reference/operator/update/positional-all/
                collection.updateMany(
                  { 'queue.id': { $in: lockSet.getIds() } },
                  {
                    // By updating some dates, we keep them fresh and out of the next garbage collector's collecting cycle
                    $currentDate: {
                      // Keep those very locks
                      'queue.$[lock].at': true,
                      // Keep all locks of the same name, because of the TTL index
                      at: true,
                    },
                  },
                  {
                    arrayFilters: [{ 'lock.id': { $in: lockSet.getIds() } }],
                  },
                )
              : // Do not support the "$[]" operator, so we have to iterate over all the locks
                collection.bulkWrite(
                  [...lockSet].map(lock => ({
                    updateOne: {
                      filter: { 'queue.id': lock.id },
                      update: {
                        // By updating some dates, we keep them fresh and out of the next garbage collector's collecting cycle
                        $currentDate: {
                          // Keep this very lock
                          'queue.$.at': true,
                          // Keep all locks of the same name, because of the TTL index
                          at: true,
                        },
                      },
                    },
                  })),
                  { ordered: false },
                ),
          ]
        : []),
    ]);
  }

  public async setup({ gcInterval }: AdapterSetupParams) {
    const indices: NamedIndexSpecification[] = [
      { name: 'idx_name', key: { name: 1 }, unique: true },
      { name: 'idx_queue_id', key: { 'queue.id': 1 } },
    ];

    if (gcInterval) {
      indices.push({ name: 'idx_at', key: { at: 1 }, expireAfterSeconds: Math.ceil((gcInterval * 2) / 1000) });
    }

    const db = await this.getDb();
    await db.createCollection(this.collectionName);

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
        if (name !== '_id_' && !indices.find(indice => indice.name === name)) {
          await collection.dropIndex(name);
        }
      }),
    ]);
  }

  public async releaseAll() {
    const collection = await this.getCollection();
    await collection.deleteMany({});
  }

  protected async enqueueLock(lock: Lock, tries: number = 3): Promise<Document> {
    const collection = await this.getCollection();

    try {
      const { value } = await collection.findOneAndUpdate(
        { name: lock.name },
        {
          $setOnInsert: { name: lock.name },
          $set: { at: new Date() },
          $push: { queue: { id: lock.id, type: lock.type, at: new Date() } },
        },
        {
          upsert: true,
          returnOriginal: false,
        },
      );

      if (!value) {
        throw new AdapterLockError(this, lock, `The lock "${lock}" has not been enqueued`);
      }

      return value;
    } catch (error) {
      // We try again in case of "duplicate key" error because of the unique index on "name"
      if (error instanceof MongoError && error.code === 11000 && tries > 1) {
        return this.enqueueLock(lock, tries - 1);
      } else {
        throw new AdapterLockError(this, lock, `The lock "${lock}" has not been enqueued: ${error.message}`);
      }
    }
  }

  protected isLockAcquired(lock: Lock, document?: Document | null): boolean {
    if (!document) {
      throw new AdapterLockError(this, lock, `The lock "${lock}" is not in the queue anymore`);
    }

    const acquired =
      lock.type === LockType.Writer
        ? // A "write" lock is acquired when it's the first in the queue
          document.queue[0]?.id === lock.id
        : // A "read" lock is acquired when it's not preceded by a "write" lock in the queue
          document.queue.find(({ id, type }) => id === lock.id || type === LockType.Writer)?.id === lock.id;

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
            await collection.findOne({ 'queue.id': lock.id }, { readPreference: ReadPreference.PRIMARY }),
          )
        ) {
          // Nothing to do here
        }
      } finally {
        if (!lock.isAcquired()) {
          // Remove the current lock
          await collection.updateOne({ name: lock.name }, { $pull: { queue: { id: lock.id } as any } });
        }
      }
    }
  }

  public async release(lock: Lock) {
    const collection = await this.getCollection();

    const { modifiedCount } = await collection.updateOne(
      { name: lock.name },
      { $pull: { queue: { id: lock.id } as any } },
    );

    if (modifiedCount === 0) {
      throw new AdapterLockError(this, lock, `The lock "${lock}" was not in the queue anymore`);
    }

    lock.status = LockStatus.Released;
  }
}
