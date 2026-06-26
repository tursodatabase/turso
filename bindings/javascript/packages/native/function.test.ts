import { expect, test } from 'vitest'
import { Database } from './compat.js'
import BetterSqlite3 from 'better-sqlite3'

test('scalar function results match better-sqlite3', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('add2', (a, b) => a + b);
    }
    const sql = 'SELECT add2(2, 3)';
    expect(turso.prepare(sql).pluck().get()).toBe(5);
    expect(turso.prepare(sql).pluck().get()).toEqual(sqlite.prepare(sql).pluck().get());
})

test('function returns the database for chaining', () => {
    const turso = new Database(':memory:');
    expect(turso.function('id', (x) => x)).toBe(turso);
})

test('values round-trip through a user function', () => {
    const turso = new Database(':memory:');
    turso.function('echo', (x) => x);

    expect(turso.prepare('SELECT echo(42)').pluck().get()).toBe(42);
    expect(turso.prepare('SELECT echo(3.5)').pluck().get()).toBe(3.5);
    expect(turso.prepare("SELECT echo('hello')").pluck().get()).toBe('hello');
    expect(turso.prepare('SELECT echo(NULL)').pluck().get()).toBe(null);

    const blob = turso.prepare("SELECT echo(x'01ff')").pluck().get();
    expect(Buffer.isBuffer(blob)).toBe(true);
    expect([...blob]).toEqual([1, 255]);
})

test('null arguments are passed as null', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('arg_is_null', (x) => (x === null ? 1 : 0));
    }
    expect(turso.prepare('SELECT arg_is_null(NULL)').pluck().get()).toBe(1);
    expect(turso.prepare('SELECT arg_is_null(7)').pluck().get()).toBe(0);
    expect(turso.prepare('SELECT arg_is_null(NULL)').pluck().get())
        .toEqual(sqlite.prepare('SELECT arg_is_null(NULL)').pluck().get());
})

test('varargs functions receive every argument', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('joiner', { varargs: true }, (...args) => args.join('-'));
    }
    const sql = "SELECT joiner('a', 'b', 'c', 'd')";
    expect(turso.prepare(sql).pluck().get()).toBe('a-b-c-d');
    expect(turso.prepare(sql).pluck().get()).toEqual(sqlite.prepare(sql).pluck().get());
})

test('wrong number of arguments throws like better-sqlite3', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('one', (a) => a);
    }
    expect(() => turso.prepare('SELECT one(1, 2)').get()).toThrow();
    expect(() => sqlite.prepare('SELECT one(1, 2)').get()).toThrow();
})

test('returning a Buffer produces a BLOB', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('payload', () => Buffer.from([1, 2, 254]));
    }
    expect(turso.prepare('SELECT length(payload())').pluck().get()).toBe(3);
    expect(turso.prepare('SELECT hex(payload())').pluck().get()).toBe('0102FE');
    expect(turso.prepare('SELECT hex(payload())').pluck().get())
        .toEqual(sqlite.prepare('SELECT hex(payload())').pluck().get());
})

test('NaN return becomes NULL', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('nope', () => NaN);
    }
    expect(turso.prepare('SELECT nope() IS NULL').pluck().get()).toBe(1);
    expect(sqlite.prepare('SELECT nope() IS NULL').pluck().get()).toBe(1);
})

test('safeIntegers passes integer arguments as BigInt', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('isbig', { safeIntegers: true }, (n) => (typeof n === 'bigint' ? 1 : 0));
    }
    expect(turso.prepare('SELECT isbig(10)').pluck().get()).toBe(1);
    expect(sqlite.prepare('SELECT isbig(10)').pluck().get()).toBe(1);
})

test('returning a BigInt preserves 64-bit integers', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('big', () => 9007199254740993n);
    }
    expect(turso.prepare('SELECT big() = 9007199254740993').pluck().get()).toBe(1);
    expect(sqlite.prepare('SELECT big() = 9007199254740993').pluck().get()).toBe(1);
})

test('returning a boolean is rejected like better-sqlite3', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('truthy', () => true);
    }
    expect(() => turso.prepare('SELECT truthy()').get()).toThrow();
    expect(() => sqlite.prepare('SELECT truthy()').get()).toThrow();
})

test('a thrown error propagates out of the query', () => {
    const turso = new Database(':memory:');
    const sqlite = new BetterSqlite3(':memory:');
    for (const db of [turso, sqlite]) {
        db.function('boom', () => { throw new Error('kaboom'); });
    }
    expect(() => turso.prepare('SELECT boom()').get()).toThrow(/kaboom/);
    expect(() => sqlite.prepare('SELECT boom()').get()).toThrow(/kaboom/);
})

test('a deterministic function works in a WHERE clause', () => {
    const turso = new Database(':memory:');
    turso.function('is_even', { deterministic: true }, (x) => (x % 2 === 0 ? 1 : 0));
    turso.exec('CREATE TABLE t(x)');
    turso.exec('INSERT INTO t VALUES (1), (2), (3), (4)');
    const rows = turso.prepare('SELECT x FROM t WHERE is_even(x) ORDER BY x').pluck().all();
    expect(rows).toEqual([2, 4]);
})

test('invalid registration arguments throw', () => {
    const turso = new Database(':memory:');
    expect(() => turso.function('bad')).toThrow();
    expect(() => turso.function(42, () => 1)).toThrow();
})

test('re-registering a function name replaces it', () => {
    const turso = new Database(':memory:');
    turso.function('v', () => 1);
    expect(turso.prepare('SELECT v()').pluck().get()).toBe(1);
    turso.function('v', () => 2);
    expect(turso.prepare('SELECT v()').pluck().get()).toBe(2);
})
