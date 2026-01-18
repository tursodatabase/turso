import {Storage} from '@tursodatabase/react-native';
import {
  beforeEach,
  describe,
  it,
  afterEach,
  expect,
} from '@op-engineering/op-test';

describe('Storage', () => {
  let storage: Storage;
  beforeEach(() => {
    storage = new Storage({encryptionKey: 'test'});
  });

  afterEach(() => {
    storage.delete();
  });

  it('Can set and get sync', async () => {
    storage.setItemSync('foo', 'bar');
    const res = storage.getItemSync('foo');
    expect(res).toEqual('bar');
  });

  it('Can set and get async', async () => {
    await storage.setItem('quack', 'bark');
    const res = await storage.getItem('quack');
    expect(res).toEqual('bark');
  });

  it('can remove item sync', async () => {
    storage.setItemSync('foo', 'bar');
    storage.removeItemSync('foo');
    const res = storage.getItemSync('foo');
    expect(res).toEqual(undefined);
  });

  it('can remove item async', async () => {
    await storage.setItem('quack', 'bark');
    await storage.removeItem('quack');
    const res = await storage.getItem('quack');
    expect(res).toEqual(undefined);
  });

  it('can clear', async () => {
    await storage.setItem('quack', 'bark');
    await storage.setItem('quack2', 'bark');
    await storage.clear();
    const res = await storage.getItem('quack');
    expect(res).toEqual(undefined);
  });

  it('can clear sync', async () => {
    storage.setItemSync('quack', 'bark');
    storage.setItemSync('quack2', 'bark');
    storage.clearSync();
    const res = storage.getItemSync('quack');
    expect(res).toEqual(undefined);
  });

  it('can get all keys', async () => {
    await storage.setItem('quack', 'bark');
    await storage.setItem('quack2', 'bark');
    const keys = storage.getAllKeys();
    expect(keys).toDeepEqual(['quack', 'quack2']);
  });
});
