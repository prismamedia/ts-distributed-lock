import { Lock, LockId, LockName, LockStatus, LockType } from '../lock';

export class LockSet extends Set<Lock> {
  public filter(filter: (value: Lock, index: number) => boolean): this {
    return new (this.constructor as typeof LockSet)([...this].filter(filter)) as this;
  }

  public filterByName(name: LockName): this {
    return this.filter(lock => lock.name === name);
  }

  public filterByType(type: LockType): this {
    return this.filter(lock => lock.type === type);
  }

  public filterByStatus(status: LockStatus): this {
    return this.filter(lock => lock.status === status);
  }

  public getNames(): LockName[] {
    return [...new Set([...this].map(({ name }) => name))];
  }

  public getIds(): LockId[] {
    return [...this].map(({ id }) => id);
  }
}
