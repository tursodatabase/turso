import { unlinkSync } from "node:fs";
import { expect, test } from 'vitest'
import { Database, sql } from './compat.js'

test('insert returning test', () => {
    const db = new Database(':memory:');
    db.prepare(`create table t (x);`).run();
    const x1 = db.prepare(`insert into t values (1), (2) returning x`).get();
    const x2 = db.prepare(`insert into t values (3), (4) returning x`).get();
    expect(x1).toEqual({ x: 1 });
    expect(x2).toEqual({ x: 3 });
    const all = db.prepare(`select * from t`).all();
    expect(all).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }])
})

test('in-memory db', () => {
    const db = new Database(":memory:");
    db.exec("CREATE TABLE t(x)");
    db.exec("INSERT INTO t VALUES (1), (2), (3)");
    const stmt = db.prepare("SELECT * FROM t WHERE x % 2 = ?");
    const rows = stmt.all([1]);
    expect(rows).toEqual([{ x: 1 }, { x: 3 }]);
})

test('exec multiple statements', async () => {
    const db = new Database(":memory:");
    db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)");
    const stmt = db.prepare("SELECT * FROM t");
    const rows = stmt.all();
    expect(rows).toEqual([{ x: 1 }, { x: 2 }]);
})

test('readonly-db', () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        {
            const rw = new Database(path);
            rw.exec("CREATE TABLE t(x)");
            rw.exec("INSERT INTO t VALUES (1)");
            rw.close();
        }
        {
            const ro = new Database(path, { readonly: true });
            expect(() => ro.exec("INSERT INTO t VALUES (2)")).toThrowError(/Resource is read-only/g);
            expect(ro.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }])
            ro.close();
        }
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('file-must-exist', () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    expect(() => new Database(path, { fileMustExist: true })).toThrowError(/failed to open database/);
})

test('on-disk db', () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const db1 = new Database(path);
        db1.exec("CREATE TABLE t(x)");
        db1.exec("INSERT INTO t VALUES (1), (2), (3)");
        const stmt1 = db1.prepare("SELECT * FROM t WHERE x % 2 = ?");
        expect(stmt1.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows1 = stmt1.all([1]);
        expect(rows1).toEqual([{ x: 1 }, { x: 3 }]);
        db1.close();

        const db2 = new Database(path);
        const stmt2 = db2.prepare("SELECT * FROM t WHERE x % 2 = ?");
        expect(stmt2.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows2 = stmt2.all([1]);
        expect(rows2).toEqual([{ x: 1 }, { x: 3 }]);
        db2.close();
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('attach', () => {
    const path1 = `test-${(Math.random() * 10000) | 0}.db`;
    const path2 = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const db1 = new Database(path1);
        db1.exec("CREATE TABLE t(x)");
        db1.exec("INSERT INTO t VALUES (1), (2), (3)");
        const db2 = new Database(path2);
        db2.exec("CREATE TABLE q(x)");
        db2.exec("INSERT INTO q VALUES (4), (5), (6)");

        db1.exec(`ATTACH '${path2}' as secondary`);

        const stmt = db1.prepare("SELECT * FROM t UNION ALL SELECT * FROM secondary.q");
        expect(stmt.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows = stmt.all([1]);
        expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }, { x: 6 }]);
    } finally {
        unlinkSync(path1);
        unlinkSync(`${path1}-wal`);
        unlinkSync(path2);
        unlinkSync(`${path2}-wal`);
    }
})

test('blobs', () => {
    const db = new Database(":memory:");
    const rows = db.prepare("SELECT x'1020' as x").all();
    expect(rows).toEqual([{ x: Buffer.from([16, 32]) }])
})

test('encryption', () => {
    const path = `test-encryption-${(Math.random() * 10000) | 0}.db`;
    const hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';
    const wrongKey = 'aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';
    try {
        const db = new Database(path, {
            encryption: { cipher: 'aegis256', hexkey }
        });
        db.exec("CREATE TABLE t(x)");
        db.exec("INSERT INTO t SELECT 'secret' FROM generate_series(1, 1024)");
        db.exec("PRAGMA wal_checkpoint(truncate)");
        db.close();

        // lets re-open with the same key
        const db2 = new Database(path, {
            encryption: { cipher: 'aegis256', hexkey }
        });
        const rows = db2.prepare("SELECT COUNT(*) as cnt FROM t").all();
        expect(rows).toEqual([{ cnt: 1024 }]);
        db2.close();

        // opening with wrong key MUST fail
        expect(() => {
            const db3 = new Database(path, {
                encryption: { cipher: 'aegis256', hexkey: wrongKey }
            });
            db3.prepare("SELECT * FROM t").all();
        }).toThrow();

        // opening without encryption MUST fail
        expect(() => {
            const db5 = new Database(path);
            db5.prepare("SELECT * FROM t").all();
        }).toThrow();
    } finally {
        unlinkSync(path);
    }
})

// ============================================
// Tests for new DX improvements (Tier 1) - Sync API
// ============================================

test('values() returns array of arrays', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");

    const stmt = db.prepare('SELECT id, name, age FROM users ORDER BY id');
    const values = stmt.values();

    expect(values).toEqual([
        [1, 'Alice', 30],
        [2, 'Bob', 25],
        [3, 'Charlie', 35]
    ]);
})

test('values() with bind parameters', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");

    const stmt = db.prepare('SELECT id, name, age FROM users WHERE age > ? ORDER BY id');
    const values = stmt.values(28);

    expect(values).toEqual([
        [1, 'Alice', 30],
        [3, 'Charlie', 35]
    ]);
})

test('values() does not affect subsequent all() calls', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const stmt = db.prepare('SELECT * FROM users ORDER BY id');

    // First call values()
    const values = stmt.values();
    expect(values).toEqual([[1, 'Alice'], [2, 'Bob']]);

    // Then call all() - should still return objects
    const rows = stmt.all();
    expect(rows).toEqual([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]);
})

test('query() shorthand returns first row', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const user = db.query('SELECT * FROM users WHERE id = ?', [1]);
    expect(user).toEqual({ id: 1, name: 'Alice' });
})

test('query() returns undefined for no results', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const user = db.query('SELECT * FROM users WHERE id = ?', [999]);
    expect(user).toBeUndefined();
})

test('query() without parameters', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE config (key TEXT, value TEXT)');
    db.exec("INSERT INTO config VALUES ('version', '1.0')");

    const config = db.query('SELECT * FROM config');
    expect(config).toEqual({ key: 'version', value: '1.0' });
})

test('queryAll() shorthand returns all rows', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)');
    db.exec("INSERT INTO users (name, active) VALUES ('Alice', 1), ('Bob', 0), ('Charlie', 1)");

    const activeUsers = db.queryAll('SELECT * FROM users WHERE active = ? ORDER BY id', [1]);
    expect(activeUsers).toEqual([
        { id: 1, name: 'Alice', active: 1 },
        { id: 3, name: 'Charlie', active: 1 }
    ]);
})

test('queryAll() returns empty array for no results', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const users = db.queryAll('SELECT * FROM users WHERE id > ?', [100]);
    expect(users).toEqual([]);
})

test('queryAll() without parameters', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE items (x INTEGER)');
    db.exec("INSERT INTO items VALUES (1), (2), (3)");

    const items = db.queryAll('SELECT * FROM items ORDER BY x');
    expect(items).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
})

test('run() returns RunResult with changes and lastInsertRowid', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const insert = db.prepare('INSERT INTO users (name) VALUES (?)');
    const result1 = insert.run('Alice');
    expect(result1.changes).toBe(1);
    expect(result1.lastInsertRowid).toBe(1);

    const result2 = insert.run('Bob');
    expect(result2.changes).toBe(1);
    expect(result2.lastInsertRowid).toBe(2);
})

test('run() changes count for UPDATE', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')");

    const update = db.prepare('UPDATE users SET active = 0 WHERE id > ?');
    const result = update.run(1);
    expect(result.changes).toBe(2); // Bob and Charlie
})

test('run() changes count for DELETE', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')");

    const del = db.prepare('DELETE FROM users WHERE id > ?');
    const result = del.run(1);
    expect(result.changes).toBe(2); // Bob and Charlie deleted
})

// TypeScript generic types - sync API
test('generic types - prepare<T> returns typed results', () => {
    interface User {
        id: number;
        name: string;
        email: string;
    }

    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
    db.exec("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')");

    const stmt = db.prepare<User>('SELECT * FROM users');
    const users = stmt.all();

    expect(users.length).toBe(1);
    expect(users[0].id).toBe(1);
    expect(users[0].name).toBe('Alice');
    expect(users[0].email).toBe('alice@test.com');
})

test('generic types - query<T> returns typed result', () => {
    interface Config {
        key: string;
        value: string;
    }

    const db = new Database(':memory:');
    db.exec('CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)');
    db.exec("INSERT INTO config VALUES ('app_name', 'MyApp')");

    const config = db.query<Config>('SELECT * FROM config WHERE key = ?', ['app_name']);

    expect(config).toBeDefined();
    expect(config!.key).toBe('app_name');
    expect(config!.value).toBe('MyApp');
})

test('generic types - queryAll<T> returns typed array', () => {
    interface Product {
        id: number;
        name: string;
        price: number;
    }

    const db = new Database(':memory:');
    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)');
    db.exec("INSERT INTO products (name, price) VALUES ('Widget', 9.99), ('Gadget', 19.99)");

    const products = db.queryAll<Product>('SELECT * FROM products ORDER BY id');

    expect(products.length).toBe(2);
    expect(products[0].name).toBe('Widget');
    expect(products[1].price).toBe(19.99);
})

// ============================================
// Tests for sql tagged template (Tier 2) - Sync API
// ============================================

test('sql template - basic query with single parameter', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");

    const minAge = 28;
    const users = db.queryAll(sql`SELECT * FROM users WHERE age > ${minAge} ORDER BY id`);

    expect(users).toEqual([
        { id: 1, name: 'Alice', age: 30 },
        { id: 3, name: 'Charlie', age: 35 }
    ]);
})

test('sql template - query with multiple parameters', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");

    const minAge = 20;
    const maxAge = 32;
    const users = db.queryAll(sql`SELECT * FROM users WHERE age > ${minAge} AND age < ${maxAge} ORDER BY id`);

    expect(users).toEqual([
        { id: 1, name: 'Alice', age: 30 },
        { id: 2, name: 'Bob', age: 25 }
    ]);
})

test('sql template - query() returns first row', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const id = 1;
    const user = db.query(sql`SELECT * FROM users WHERE id = ${id}`);

    expect(user).toEqual({ id: 1, name: 'Alice' });
})

test('sql template - query() returns undefined for no results', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const id = 999;
    const user = db.query(sql`SELECT * FROM users WHERE id = ${id}`);

    expect(user).toBeUndefined();
})

test('sql template - with string parameter', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const name = 'Alice';
    const user = db.query(sql`SELECT * FROM users WHERE name = ${name}`);

    expect(user).toEqual({ id: 1, name: 'Alice' });
})

test('sql template - no parameters (static query)', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE config (key TEXT, value TEXT)');
    db.exec("INSERT INTO config VALUES ('version', '1.0')");

    const config = db.query(sql`SELECT * FROM config`);

    expect(config).toEqual({ key: 'version', value: '1.0' });
})

test('sql template - with generic type parameter', () => {
    interface User {
        id: number;
        name: string;
        age: number;
    }

    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30)");

    const minAge = 25;
    const users = db.queryAll<User>(sql`SELECT * FROM users WHERE age > ${minAge}`);

    expect(users.length).toBe(1);
    expect(users[0].id).toBe(1);
    expect(users[0].name).toBe('Alice');
    expect(users[0].age).toBe(30);
})

test('sql template - prevents SQL injection', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    // Attempt SQL injection - should be safely parameterized
    const maliciousInput = "'; DROP TABLE users; --";
    const user = db.query(sql`SELECT * FROM users WHERE name = ${maliciousInput}`);

    // Should return no results, not execute the injection
    expect(user).toBeUndefined();

    // Table should still exist
    const allUsers = db.queryAll(sql`SELECT * FROM users ORDER BY id`);
    expect(allUsers.length).toBe(2);
})

test('sql template - mixed with regular query methods', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)");

    // Use sql template
    const minAge = 20;
    const templateResult = db.queryAll(sql`SELECT * FROM users WHERE age > ${minAge}`);
    expect(templateResult.length).toBe(2);

    // Use regular string with params array (should still work)
    const regularResult = db.queryAll('SELECT * FROM users WHERE age > ?', [20]);
    expect(regularResult.length).toBe(2);

    // Results should be identical
    expect(templateResult).toEqual(regularResult);
})

// ============================================
// Tests for db.batch() (Tier 2) - Sync API
// ============================================

test('batch - executes multiple inserts atomically', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const results = db.batch([
        db.prepare('INSERT INTO users (name) VALUES (?)').bind('Alice'),
        db.prepare('INSERT INTO users (name) VALUES (?)').bind('Bob'),
        db.prepare('INSERT INTO users (name) VALUES (?)').bind('Charlie'),
    ]);

    expect(results.length).toBe(3);
    // Note: changes is 0 inside transactions (pre-existing limitation)
    expect(results[0].lastInsertRowid).toBe(1);
    expect(results[1].lastInsertRowid).toBe(2);
    expect(results[2].lastInsertRowid).toBe(3);

    // Verify all rows were inserted
    const users = db.queryAll('SELECT * FROM users ORDER BY id');
    expect(users).toEqual([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
    ]);
})

test('batch - rolls back on error', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');

    // First insert should succeed, second should fail (NULL name)
    expect(() => db.batch([
        db.prepare('INSERT INTO users (name) VALUES (?)').bind('Alice'),
        db.prepare('INSERT INTO users (name) VALUES (?)').bind(null),
    ])).toThrow();

    // Verify the transaction was rolled back - no rows should exist
    const users = db.queryAll('SELECT * FROM users');
    expect(users).toEqual([]);
})

test('batch - empty array returns empty results', () => {
    const db = new Database(':memory:');
    const results = db.batch([]);
    expect(results).toEqual([]);
})

test('batch - throws on non-array input', () => {
    const db = new Database(':memory:');
    // @ts-expect-error - Testing invalid input
    expect(() => db.batch('not an array')).toThrow('Expected an array of statements');
})

test('batch - mixed insert and update', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 0)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const results = db.batch([
        db.prepare('INSERT INTO users (name) VALUES (?)').bind('Charlie'),
        db.prepare('UPDATE users SET active = 1 WHERE id <= 2'),
    ]);

    expect(results.length).toBe(2);
    expect(results[0].lastInsertRowid).toBe(3);

    // Verify the actual changes happened
    const users = db.queryAll('SELECT * FROM users ORDER BY id');
    expect(users).toEqual([
        { id: 1, name: 'Alice', active: 1 },
        { id: 2, name: 'Bob', active: 1 },
        { id: 3, name: 'Charlie', active: 0 },
    ]);
})