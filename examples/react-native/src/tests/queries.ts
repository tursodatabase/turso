import { chance } from './utils';
import { createLocalDatabase } from '@tursodatabase/react-native';
import {
  expect,
  afterEach,
  beforeEach,
  describe,
  it,
} from '@op-engineering/op-test';

describe('Basic Queries', () => {
  let db: any;

  beforeEach(async () => {
    db = createLocalDatabase(':memory:');
  });

  afterEach(() => {
    if (db) {
      db.close();
      db = null;
    }
  });

  it('SELECT simple value', () => {
    const res = db.get('SELECT 1 as value');
    expect(res).toDeepEqual({ value: 1 });
  });

  it('SELECT with multiple columns', () => {
    const res = db.get('SELECT 1 as a, 2 as b, 3 as c');
    expect(res).toDeepEqual({ a: 1, b: 2, c: 3 });
  });

  it('SELECT with string', () => {
    const res = db.get("SELECT 'hello' as text");
    expect(res).toDeepEqual({ text: 'hello' });
  });

  it('SELECT with null', () => {
    const res = db.get('SELECT NULL as value');
    expect(res).toDeepEqual({ value: null });
  });

  it('SELECT with math', () => {
    const res = db.get('SELECT 2 + 2 as result');
    expect(res).toDeepEqual({ result: 4 });
  });
});


describe('Table Operations', () => {
  let db: any;

  beforeEach(async () => {
    db = createLocalDatabase(':memory:');
  });

  afterEach(() => {
    if (db) {
      db.close();
      db = null;
    }
  });

  it('CREATE TABLE', () => {
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    const res = db.get(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='users'",
    );
    expect(res).toDeepEqual({ name: 'users' });
  });

  it('INSERT and SELECT', () => {
    db.exec(
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)',
    );
    const result = db.run(
      'INSERT INTO users (name, age) VALUES (?, ?)',
      'Alice',
      30,
    );
    expect(result.changes).toBe(1);

    const row = db.get(
      'SELECT * FROM users WHERE id = ?',
      result.lastInsertRowid,
    );
    expect(row.name).toBe('Alice');
    expect(row.age).toBe(30);
  });


});