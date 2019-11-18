import { testAdapter } from '@prismamedia/ts-distributed-lock';
import { MongoDBAdapter } from '../mongodb-adapter';

describe('MongoDBAdapter', () => {
  it(`handles "serverVersion", either it comes from "options" or from "serverStatus"`, () => {
    if (!process.env.MONGODB_URL) {
      throw new TypeError(`The env variable "MONGODB_URL" has to be defined`);
    }

    const mongodbUrl = process.env.MONGODB_URL;

    expect(new MongoDBAdapter(mongodbUrl, { serverVersion: '3' })).toBeInstanceOf(MongoDBAdapter);
    expect(new MongoDBAdapter(mongodbUrl, { serverVersion: '3.2' })).toBeInstanceOf(MongoDBAdapter);
    expect(new MongoDBAdapter(mongodbUrl, { serverVersion: '3.2.4' })).toBeInstanceOf(MongoDBAdapter);
  });

  testAdapter(() => {
    if (!process.env.MONGODB_URL) {
      throw new TypeError(`The env variable "MONGODB_URL" has to be defined`);
    }

    return new MongoDBAdapter(process.env.MONGODB_URL);
  });
});
