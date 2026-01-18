import {open, type DB} from '@tursodatabase/react-native';
import {
  afterAll,
  beforeEach,
  describe,
  it,
  expect,
} from '@op-engineering/op-test';

describe('PreparedStatements', () => {
  let db: DB;
  beforeEach(async () => {
    try {
      if (db) {
        db.close();
        db.delete();
      }

      db = open({
        name: 'statements',
        encryptionKey: 'test',
      });

      await db.execute('DROP TABLE IF EXISTS User;');
      await db.execute(
        'CREATE TABLE User ( id INT PRIMARY KEY, name TEXT) STRICT;',
      );
      await db.execute('INSERT INTO "User" (id, name) VALUES(?,?)', [
        1,
        'Oscar',
      ]);
      await db.execute('INSERT INTO "User" (id, name) VALUES(?,?)', [
        2,
        'Pablo',
      ]);
      await db.execute('INSERT INTO "User" (id, name) VALUES(?,?)', [
        3,
        'Carlos',
      ]);
    } catch (e) {
      console.warn('error on before each', e);
    }
  });

  afterAll(() => {
    if (db) {
      db.close();
      db.delete();
      // @ts-ignore
      db = null;
    }
  });

  it('Creates a prepared statement and executes a prepared statement', async () => {
    const statement = db.prepareStatement('SELECT * FROM User;');
    let results = await statement.execute();

    expect(results.rows?.length).toEqual(3);
    results = await statement.execute();

    expect(results.rows.length).toEqual(3);
  });

  it('prepared statement, rebind select', async () => {
    const statement = db.prepareStatement('SELECT * FROM User WHERE id = ?;');
    await statement.bind([1]);

    let results = await statement.execute();
    expect(results.rows[0]!.name === 'Oscar');

    await statement.bind([2]);
    results = await statement.execute();
    expect(results.rows[0]!.name === 'Pablo');
  });

  it('prepared statement, rebind insert', async () => {
    const statement = db.prepareStatement(
      'INSERT INTO "User" (id, name) VALUES(?,?);',
    );
    await statement.bind([4, 'Juan']);
    await statement.execute();

    await statement.bind([5, 'Pedro']);
    await statement.execute();
  });

  it('prepared statement, bindsync', async () => {
    const statement = db.prepareStatement(
      'INSERT INTO "User" (id, name) VALUES(?,?);',
    );

    statement.bindSync([4, 'Juan']);
    await statement.execute();

    statement.bind([5, 'Pedro']);
    await statement.execute();

    const selectStatement = db.prepareStatement('SELECT * FROM User;');
    let results = await selectStatement.execute();
    expect(results.rows.length).toEqual(5);
  });
});
