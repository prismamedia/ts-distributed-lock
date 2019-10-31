import {
  AdapterGarbageCollectorParams,
  AdapterInterface,
  AdapterSetupParams,
  Lock,
  LockId,
  LockName,
  LockSet,
  LockStatus,
  LockType,
  sleep,
} from '@prismamedia/ts-distributed-lock';
import { Collection, Db, IndexSpecification, MongoClient } from 'mongodb';
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
};

export class MongoDBAdapter implements AdapterInterface {
  protected client: MongoClient;
  protected collectionName: string;

  public constructor(urlOrClient: string | MongoClient, protected options: Partial<MongoDBAdapterOptions> = {}) {
    this.client =
      urlOrClient instanceof MongoClient
        ? urlOrClient
        : new MongoClient(urlOrClient, { useNewUrlParser: true, useUnifiedTopology: true });

    this.collectionName = options.collectionName || 'locks';
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
  protected async getCollection(): Promise<Collection<Document>> {
    const db = await this.getDb();

    return new Promise((resolve, reject) =>
      db.collection(this.collectionName, { strict: true }, (error, collection) =>
        error ? reject(error) : resolve(collection),
      ),
    );
  }

  public async gc({ lockSet, gcInterval }: AdapterGarbageCollectorParams): Promise<void> {
    const collection = await this.getCollection();
    const staleAt = new Date(new Date().getTime() - gcInterval * 2);

    await Promise.all([
      // We delete the locks not refreshed soon enought
      collection.updateMany({}, { $pull: { queue: { at: { $lt: staleAt } } as any } }),

      // We refresh the registered locks
      ...(lockSet.size > 0
        ? [
            collection.updateMany(
              { 'queue.id': { $in: lockSet.getIds() } },
              {
                // By updating some dates, we keep them fresh and out of the next garbage collector's collecting cycle
                $currentDate: {
                  // Keep this very lock
                  'queue.$.at': true,
                  // Keep all locks of the same name, because of the TTL index
                  at: true,
                },
              },
            ),
          ]
        : []),
    ]);
  }

  public async setup({ gcInterval }: AdapterSetupParams) {
    const indices: NamedIndexSpecification[] = [
      { name: 'idx_name', key: { name: 1 }, unique: true },
      { name: 'idx_queue_id', key: { 'queue.id': 1 }, unique: true },
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

    if (gcInterval) {
      await this.gc({ lockSet: new LockSet(), gcInterval });
    }
  }

  public async releaseAll() {
    const collection = await this.getCollection();
    await collection.deleteMany({});
  }

  protected async doRelease(lock: Lock): Promise<void> {
    const collection = await this.getCollection();

    await collection.updateOne(
      {
        name: lock.name,
      },
      {
        $pull: { queue: { id: lock.id } as any },
      },
    );
  }

  public async lock(lock: Lock) {
    const collection = await this.getCollection();

    // Push the lock into the dedicated document
    let value: Document | null | undefined = (await collection.findOneAndUpdate(
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
    )).value;

    try {
      if (value != null) {
        do {
          if (lock.type === LockType.Writer) {
            // A "write" lock is acquired when it's the first in the queue
            if (value.queue[0]?.id === lock.id) {
              lock.status = LockStatus.Acquired;
            }
          } else {
            // A "read" lock is acquired when it's not preceded by a "write" lock in the queue
            if (value.queue.find(({ id, type }) => id === lock.id || type === LockType.Writer)?.id === lock.id) {
              lock.status = LockStatus.Acquired;
            }
          }
        } while (
          lock.isAcquiring() &&
          (await sleep(lock.pullInterval)) &&
          (value = await collection.findOne({ 'queue.id': lock.id }))
        );
      }
    } finally {
      if (!lock.isAcquired()) {
        // Remove the current lock
        await collection.updateOne({ name: lock.name }, { $pull: { queue: { id: lock.id } as any } });
      }
    }
  }

  public async release(lock: Lock) {
    const collection = await this.getCollection();
    await collection.updateOne({ name: lock.name }, { $pull: { queue: { id: lock.id } as any } });

    lock.status = LockStatus.Released;
  }
}
