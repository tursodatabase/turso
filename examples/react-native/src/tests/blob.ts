import {type DB, open} from '@tursodatabase/react-native';
import {
  afterAll,
  beforeEach,
  describe,
  it,
  expect,
} from '@op-engineering/op-test';

let db: DB;

describe('Blobs', () => {
  beforeEach(async () => {
    try {
      db = open({
        name: 'blobs',
        encryptionKey: 'test',
      });

      await db.execute('DROP TABLE IF EXISTS BlobTable;');
      await db.execute(
        'CREATE TABLE BlobTable ( id INT PRIMARY KEY, content BLOB) STRICT;',
      );
    } catch (e) {
      console.warn('error on before each', e);
    }
  });

  afterAll(() => {
    db.delete();
  });

  it('ArrayBuffer', async () => {
    const uint8 = new Uint8Array(2);
    uint8[0] = 42;

    await db.execute(`INSERT OR REPLACE INTO BlobTable VALUES (?, ?);`, [
      1,
      uint8.buffer,
    ]);

    const result = await db.execute('SELECT content FROM BlobTable;');

    const finalUint8 = new Uint8Array(result.rows[0]!.content as any);
    expect(finalUint8[0]).toBe(42);
  });

  it('Uint8Array', async () => {
    const uint8 = new Uint8Array(2);
    uint8[0] = 42;

    await db.execute(`INSERT OR REPLACE INTO BlobTable VALUES (?, ?);`, [
      1,
      uint8,
    ]);

    const result = await db.execute('SELECT content FROM BlobTable');

    const finalUint8 = new Uint8Array(result.rows[0]!.content as any);
    expect(finalUint8[0]).toBe(42);
  });

  it('Uint16Array', async () => {
    const uint8 = new Uint16Array(2);
    uint8[0] = 42;

    await db.execute(`INSERT OR REPLACE INTO BlobTable VALUES (?, ?);`, [
      1,
      uint8,
    ]);

    const result = await db.execute('SELECT content FROM BlobTable');

    const finalUint8 = new Uint8Array(result.rows[0]!.content as any);
    expect(finalUint8[0]).toBe(42);
  });

  it('Uint8Array in prepared statement', async () => {
    const uint8 = new Uint8Array(2);
    uint8[0] = 46;

    const statement = db.prepareStatement(
      'INSERT OR REPLACE INTO BlobTable VALUES (?, ?);',
    );
    await statement.bind([1, uint8]);

    await statement.execute();

    const result = await db.execute('SELECT content FROM BlobTable');

    const finalUint8 = new Uint8Array(result.rows[0]!.content as any);
    expect(finalUint8[0]).toBe(46);
  });

  it('Buffer in prepared statement', async () => {
    const uint8 = new Uint8Array(2);
    uint8[0] = 52;

    const statement = db.prepareStatement(
      'INSERT OR REPLACE INTO BlobTable VALUES (?, ?);',
    );

    await statement.bind([1, uint8.buffer]);

    await statement.execute();

    const result = await db.execute('SELECT content FROM BlobTable');

    const finalUint8 = new Uint8Array(result.rows[0]!.content as any);
    expect(finalUint8[0]).toBe(52);
  });
});
