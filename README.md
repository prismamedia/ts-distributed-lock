**Typescript distributed lock**

[![npm version](https://badge.fury.io/js/%40prismamedia%2Fts-distributed-lock.svg)](https://badge.fury.io/js/%40prismamedia%2Fts-distributed-lock) [![github actions status](https://github.com/prismamedia/ts-distributed-lock/workflows/CI/badge.svg)](https://github.com/prismamedia/ts-distributed-lock/actions) [![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

Provide an easy-to-use "Readers-writers lock" https://en.wikipedia.org/wiki/Readers–writer_lock

> An RW lock allows concurrent access for read-only operations, while write operations require exclusive access. This means that multiple threads can read the data in parallel but an exclusive lock is needed for writing or modifying data. When a writer is writing the data, all other writers or readers will be blocked until the writer is finished writing.

# Configuration

```ts
// ./locker.ts
import { Locker } from '@prismamedia/ts-distributed-lock';
import { MongoDBAdapter } from '@prismamedia/ts-distributed-lock-mongodb-adapter';

const adapter = new MongoDBAdapter('mongodb://localhost:27017/my-database');

export const locker = new Locker(adapter);
```

# Setup

The adapter may needs some setup before use

```ts
// ./setup.ts
import { locker } from './locker';

await locker.setup();
```

# Usage

```ts
// ./usage.ts
import { locker } from './locker';

const firstLock = await locker.lockAsReader('my-lock-name');
try {
  // Everything I have to do ...
} finally {
  await locker.release(firstLock);
}

const secondLock = await locker.lockAsWriter('my-lock-name');
try {
  // Everything I have to do ...
} finally {
  await locker.release(secondLock);
}
```

Or with some helpers that ensure the lock is released

```ts
// ./usage.ts
import { locker } from './locker';

const firstTaskResult = await locker.ensureReadingTaskConcurrency(
  'my-lock-name',
  async () => {
    // Everything I have to do ...

    return 'myFirstTaskResult';
  },
);

const secondTaskResult = await locker.ensureWritingTaskConcurrency(
  'my-lock-name',
  async () => {
    // Everything I have to do ...

    return 'mySecondTaskResult';
  },
);
```
