import { testAdapter } from '@prismamedia/ts-distributed-lock';
import { MongoDBAdapter } from '../mongodb-adapter';

describe('MongoDBAdapter', () => {
  testAdapter(() => {
    if (!process.env.MONGODB_URL) {
      throw new TypeError(`The env variable "MONGODB_URL" has to be defined`);
    }

    return new MongoDBAdapter(process.env.MONGODB_URL);
  });
});
