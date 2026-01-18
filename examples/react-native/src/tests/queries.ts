import { chance } from './utils';
import {
  open
} from '@tursodatabase/react-native';
import {
  expect,
  afterEach,
  beforeEach,
  describe,
  it,
} from '@op-engineering/op-test';

describe('Queries tests', () => {
  let db: any;

  beforeEach(async () => {
    db = open({
      name: 'queries.sqlite',
      encryptionKey: 'test',
    });

    await db.execute('DROP TABLE IF EXISTS User;');
    await db.execute('DROP TABLE IF EXISTS T1;');
    await db.execute('DROP TABLE IF EXISTS T2;');
    await db.execute(
      'CREATE TABLE User (id INT PRIMARY KEY, name TEXT NOT NULL, age INT, networth REAL, nickname TEXT) STRICT;',
    );
  });

  afterEach(() => {
    if (db) {
      db.delete();
      // @ts-ignore
      db = null;
    }
  });

  it('executeSync', () => {
    const res = db.executeSync('SELECT 1');
    expect(res.rowsAffected).toEqual(0);

    const id = chance.integer();
    const name = chance.name();
    const age = chance.integer();
    const networth = chance.floating();
    const res2 = db.executeSync(
      'INSERT INTO "User" (id, name, age, networth) VALUES(?, ?, ?, ?)',
      [id, name, age, networth],
    );

    expect(res2.rowsAffected).toEqual(1);
    expect(res2.insertId).toEqual(1);
    // expect(res2.rows).toBe([]);
    expect(res2.rows?.length).toEqual(0);
  });
});
