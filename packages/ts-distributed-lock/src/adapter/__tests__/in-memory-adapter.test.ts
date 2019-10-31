import { testAdapter } from '../../__tests__/adapter-test';
import { InMemoryAdapter } from '../in-memory-adapter';

describe('InMemoryAdapter', () => {
  testAdapter(() => new InMemoryAdapter());
});
