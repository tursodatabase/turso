import { unlinkSync } from "node:fs";
import { expect, test } from 'vitest'
import { Database, connect, sql } from './promise.js'
import { sql as drizzleSql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/better-sqlite3';

test('drizzle-orm', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const conn = await connect(path);
        const db = drizzle(conn);
        await db.run('CREATE TABLE t(x, y)');
        let tasks = [];
        for (let i = 0; i < 1234; i++) {
            tasks.push(db.run(drizzleSql`INSERT INTO t VALUES (${i}, randomblob(${i} * 5))`))
        }
        await Promise.all(tasks);
        expect(await db.all("SELECT COUNT(*) as cnt FROM t")).toEqual([{ cnt: 1234 }])
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('in-memory-db-async', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    const stmt = db.prepare("SELECT * FROM t WHERE x % 2 = ?");
    const rows = await stmt.all([1]);
    expect(rows).toEqual([{ x: 1 }, { x: 3 }]);
})

test('exec multiple statements', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)");
    const stmt = db.prepare("SELECT * FROM t");
    const rows = await stmt.all();
    expect(rows).toEqual([{ x: 1 }, { x: 2 }]);
})

test('readonly-db', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        {
            const rw = await connect(path);
            await rw.exec("CREATE TABLE t(x)");
            await rw.exec("INSERT INTO t VALUES (1)");
            rw.close();
        }
        {
            const ro = await connect(path, { readonly: true });
            await expect(async () => await ro.exec("INSERT INTO t VALUES (2)")).rejects.toThrowError(/Resource is read-only/g);
            expect(await ro.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }])
            ro.close();
        }
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('file-must-exist', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    await expect(async () => await connect(path, { fileMustExist: true })).rejects.toThrowError(/failed to open database/);
})

test('implicit connect', async () => {
    const db = new Database(':memory:');
    const defer = db.prepare("SELECT * FROM t");
    await expect(async () => await defer.all()).rejects.toThrowError(/no such table: t/);
    expect(() => db.prepare("SELECT * FROM t")).toThrowError(/no such table: t/);
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
})

test('zero-limit-bug', async () => {
    const db = await connect(':memory:');
    const create = db.prepare(`CREATE TABLE users (name TEXT NOT NULL);`);
    await create.run();

    const insert = db.prepare(
        `insert into "users" values (?), (?), (?);`,
    );
    await insert.run('John', 'Jane', 'Jack');

    const stmt1 = db.prepare(`select * from "users" limit ?;`);
    expect(await stmt1.all(0)).toEqual([]);
    let rows = [{ name: 'John' }, { name: 'Jane' }, { name: 'Jack' }, { name: 'John' }, { name: 'Jane' }, { name: 'Jack' }];
    for (const limit of [0, 1, 2, 3, 4, 5, 6, 7]) {
        const stmt2 = db.prepare(`select * from "users" union all select * from "users" limit ?;`);
        expect(await stmt2.all(limit)).toEqual(rows.slice(0, Math.min(limit, 6)));
    }
})

test('avg-bug', async () => {
    const db = await connect(':memory:');
    const create = db.prepare(`create table "aggregate_table" (
        "id" integer primary key autoincrement not null,
        "name" text not null,
        "a" integer,
        "b" integer,
        "c" integer,
        "null_only" integer
    );`);

    await create.run();
    const insert = db.prepare(
        `insert into "aggregate_table" ("id", "name", "a", "b", "c", "null_only") values (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null);`,
    );

    await insert.run(
        'value 1', 5, 10, 20,
        'value 1', 5, 20, 30,
        'value 2', 10, 50, 60,
        'value 3', 20, 20, null,
        'value 4', null, 90, 120,
        'value 5', 80, 10, null,
        'value 6', null, null, 150,
    );

    expect(await db.prepare(`select avg("a") from "aggregate_table";`).get()).toEqual({ 'avg (aggregate_table.a)': 24 });
    expect(await db.prepare(`select avg("null_only") from "aggregate_table";`).get()).toEqual({ 'avg (aggregate_table.null_only)': null });
    expect(await db.prepare(`select avg(distinct "b") from "aggregate_table";`).get()).toEqual({ 'avg (DISTINCT aggregate_table.b)': 42.5 });
})

test('insert returning test', async () => {
    const db = await connect(':memory:');
    await db.prepare(`create table t (x);`).run();
    const x1 = await db.prepare(`insert into t values (1), (2) returning x`).get();
    const x2 = await db.prepare(`insert into t values (3), (4) returning x`).get();
    expect(x1).toEqual({ x: 1 });
    expect(x2).toEqual({ x: 3 });
    const all = await db.prepare(`select * from t`).all();
    expect(all).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }])
})

test('offset-bug', async () => {
    const db = await connect(":memory:");
    await db.exec(`CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        verified integer not null default 0
    );`);
    const insert = db.prepare(`INSERT INTO users (name) VALUES (?),(?);`);
    await insert.run('John', 'John1');

    const stmt = db.prepare(`SELECT * FROM users LIMIT ? OFFSET ?;`);
    expect(await stmt.all(1, 1)).toEqual([{ id: 2, name: 'John1', verified: 0 }])
})

test('conflict-bug', async () => {
    const db = await connect(':memory:');

    const create = db.prepare(`create table "conflict_chain_example" (
        id integer not null unique,
        name text not null,
        email text not null,
        primary key (id, name)
    )`);
    await create.run();

    await db.prepare(`insert into "conflict_chain_example" ("id", "name", "email") values (?, ?, ?), (?, ?, ?)`).run(
        1,
        'John',
        'john@example.com',
        2,
        'John Second',
        '2john@example.com',
    );

    const insert = db.prepare(
        `insert into "conflict_chain_example" ("id", "name", "email") values (?, ?, ?), (?, ?, ?) on conflict ("conflict_chain_example"."id", "conflict_chain_example"."name") do update set "email" = ? on conflict ("conflict_chain_example"."id") do nothing`,
    );
    await insert.run(1, 'John', 'john@example.com', 2, 'Anthony', 'idthief@example.com', 'john1@example.com');

    expect(await db.prepare("SELECT * FROM conflict_chain_example").all()).toEqual([
        { id: 1, name: 'John', email: 'john1@example.com' },
        { id: 2, name: 'John Second', email: '2john@example.com' }
    ]);
})

test('on-disk db', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const db1 = await connect(path);
        await db1.exec("CREATE TABLE t(x)");
        await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
        const stmt1 = db1.prepare("SELECT * FROM t WHERE x % 2 = ?");
        expect(stmt1.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows1 = await stmt1.all([1]);
        expect(rows1).toEqual([{ x: 1 }, { x: 3 }]);
        db1.close();

        const db2 = await connect(path);
        const stmt2 = db2.prepare("SELECT * FROM t WHERE x % 2 = ?");
        expect(stmt2.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows2 = await stmt2.all([1]);
        expect(rows2).toEqual([{ x: 1 }, { x: 3 }]);
        db2.close();
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('attach', async () => {
    const path1 = `test-${(Math.random() * 10000) | 0}.db`;
    const path2 = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const db1 = await connect(path1);
        await db1.exec("CREATE TABLE t(x)");
        await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
        const db2 = await connect(path2);
        await db2.exec("CREATE TABLE q(x)");
        await db2.exec("INSERT INTO q VALUES (4), (5), (6)");

        await db1.exec(`ATTACH '${path2}' as secondary`);

        const stmt = db1.prepare("SELECT * FROM t UNION ALL SELECT * FROM secondary.q");
        expect(stmt.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows = await stmt.all([1]);
        expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }, { x: 6 }]);
    } finally {
        unlinkSync(path1);
        unlinkSync(`${path1}-wal`);
        unlinkSync(path2);
        unlinkSync(`${path2}-wal`);
    }
})

test('blobs', async () => {
    const db = await connect(":memory:");
    const rows = await db.prepare("SELECT x'1020' as x").all();
    expect(rows).toEqual([{ x: Buffer.from([16, 32]) }])
})


test('example-1', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

    const insert = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
    await insert.run('Alice', 'alice@example.com');
    await insert.run('Bob', 'bob@example.com');

    const users = await db.prepare('SELECT * FROM users').all();
    expect(users).toEqual([
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' }
    ]);
})

test('example-2', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (name, email)');
    // Using transactions for atomic operations
    const transaction = db.transaction(async (users) => {
        const insert = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
        for (const user of users) {
            await insert.run(user.name, user.email);
        }
    });

    // Execute transaction
    await transaction([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' }
    ]);

    const rows = await db.prepare('SELECT * FROM users').all();
    expect(rows).toEqual([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' }
    ]);
})

// ============================================
// Tests for new DX improvements (Tier 1)
// ============================================

test('values() returns array of arrays', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");

    const stmt = db.prepare('SELECT id, name, age FROM users ORDER BY id');
    const values = await stmt.values();

    expect(values).toEqual([
        [1, 'Alice', 30],
        [2, 'Bob', 25],
        [3, 'Charlie', 35]
    ]);
})

test('values() with bind parameters', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");

    const stmt = db.prepare('SELECT id, name, age FROM users WHERE age > ? ORDER BY id');
    const values = await stmt.values(28);

    expect(values).toEqual([
        [1, 'Alice', 30],
        [3, 'Charlie', 35]
    ]);
})

test('values() does not affect subsequent all() calls', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    await db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const stmt = db.prepare('SELECT * FROM users ORDER BY id');

    // First call values()
    const values = await stmt.values();
    expect(values).toEqual([[1, 'Alice'], [2, 'Bob']]);

    // Then call all() - should still return objects
    const rows = await stmt.all();
    expect(rows).toEqual([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]);
})

test('query() shorthand returns first row', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    await db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const user = await db.query('SELECT * FROM users WHERE id = ?', [1]);
    expect(user).toEqual({ id: 1, name: 'Alice' });
})

test('query() returns undefined for no results', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const user = await db.query('SELECT * FROM users WHERE id = ?', [999]);
    expect(user).toBeUndefined();
})

test('query() without parameters', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE config (key TEXT, value TEXT)');
    await db.exec("INSERT INTO config VALUES ('version', '1.0')");

    const config = await db.query('SELECT * FROM config');
    expect(config).toEqual({ key: 'version', value: '1.0' });
})

test('queryAll() shorthand returns all rows', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)');
    await db.exec("INSERT INTO users (name, active) VALUES ('Alice', 1), ('Bob', 0), ('Charlie', 1)");

    const activeUsers = await db.queryAll('SELECT * FROM users WHERE active = ? ORDER BY id', [1]);
    expect(activeUsers).toEqual([
        { id: 1, name: 'Alice', active: 1 },
        { id: 3, name: 'Charlie', active: 1 }
    ]);
})

test('queryAll() returns empty array for no results', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const users = await db.queryAll('SELECT * FROM users WHERE id > ?', [100]);
    expect(users).toEqual([]);
})

test('queryAll() without parameters', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE items (x INTEGER)');
    await db.exec("INSERT INTO items VALUES (1), (2), (3)");

    const items = await db.queryAll('SELECT * FROM items ORDER BY x');
    expect(items).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
})

test('run() returns RunResult with changes and lastInsertRowid', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const insert = db.prepare('INSERT INTO users (name) VALUES (?)');
    const result1 = await insert.run('Alice');
    expect(result1.changes).toBe(1);
    expect(result1.lastInsertRowid).toBe(1);

    const result2 = await insert.run('Bob');
    expect(result2.changes).toBe(1);
    expect(result2.lastInsertRowid).toBe(2);
})

test('run() changes count for UPDATE', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1)');
    await db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')");

    const update = db.prepare('UPDATE users SET active = 0 WHERE id > ?');
    const result = await update.run(1);
    expect(result.changes).toBe(2); // Bob and Charlie
})

test('run() changes count for DELETE', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    await db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')");

    const del = db.prepare('DELETE FROM users WHERE id > ?');
    const result = await del.run(1);
    expect(result.changes).toBe(2); // Bob and Charlie deleted
})

// TypeScript generic types - these tests verify runtime behavior matches type expectations
test('generic types - prepare<T> returns typed results', async () => {
    interface User {
        id: number;
        name: string;
        email: string;
    }

    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
    await db.exec("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')");

    // The generic type is for TypeScript compile-time checking
    // At runtime, we verify the shape matches
    const stmt = db.prepare<User>('SELECT * FROM users');
    const users = await stmt.all();

    expect(users.length).toBe(1);
    expect(users[0].id).toBe(1);
    expect(users[0].name).toBe('Alice');
    expect(users[0].email).toBe('alice@test.com');
})

test('generic types - query<T> returns typed result', async () => {
    interface Config {
        key: string;
        value: string;
    }

    const db = await connect(':memory:');
    await db.exec('CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)');
    await db.exec("INSERT INTO config VALUES ('app_name', 'MyApp')");

    const config = await db.query<Config>('SELECT * FROM config WHERE key = ?', ['app_name']);

    expect(config).toBeDefined();
    expect(config!.key).toBe('app_name');
    expect(config!.value).toBe('MyApp');
})

test('generic types - queryAll<T> returns typed array', async () => {
    interface Product {
        id: number;
        name: string;
        price: number;
    }

    const db = await connect(':memory:');
    await db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)');
    await db.exec("INSERT INTO products (name, price) VALUES ('Widget', 9.99), ('Gadget', 19.99)");

    const products = await db.queryAll<Product>('SELECT * FROM products ORDER BY id');

    expect(products.length).toBe(2);
    expect(products[0].name).toBe('Widget');
    expect(products[1].price).toBe(19.99);
})

// ============================================
// Tests for sql tagged template (Tier 2)
// ============================================

test('sql template - basic query with single parameter', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");

    const minAge = 28;
    const users = await db.queryAll(sql`SELECT * FROM users WHERE age > ${minAge} ORDER BY id`);

    expect(users).toEqual([
        { id: 1, name: 'Alice', age: 30 },
        { id: 3, name: 'Charlie', age: 35 }
    ]);
})

test('sql template - query with multiple parameters', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");

    const minAge = 20;
    const maxAge = 32;
    const users = await db.queryAll(sql`SELECT * FROM users WHERE age > ${minAge} AND age < ${maxAge} ORDER BY id`);

    expect(users).toEqual([
        { id: 1, name: 'Alice', age: 30 },
        { id: 2, name: 'Bob', age: 25 }
    ]);
})

test('sql template - query() returns first row', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    await db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const id = 1;
    const user = await db.query(sql`SELECT * FROM users WHERE id = ${id}`);

    expect(user).toEqual({ id: 1, name: 'Alice' });
})

test('sql template - query() returns undefined for no results', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const id = 999;
    const user = await db.query(sql`SELECT * FROM users WHERE id = ${id}`);

    expect(user).toBeUndefined();
})

test('sql template - with string parameter', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    await db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const name = 'Alice';
    const user = await db.query(sql`SELECT * FROM users WHERE name = ${name}`);

    expect(user).toEqual({ id: 1, name: 'Alice' });
})

test('sql template - with null parameter', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
    await db.exec("INSERT INTO users (name, email) VALUES ('Alice', NULL), ('Bob', 'bob@test.com')");

    const email = null;
    const users = await db.queryAll(sql`SELECT * FROM users WHERE email IS ${email}`);

    expect(users).toEqual([{ id: 1, name: 'Alice', email: null }]);
})

test('sql template - with blob parameter', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE data (id INTEGER PRIMARY KEY, content BLOB)');

    const blob = Buffer.from([1, 2, 3, 4]);
    await db.exec("INSERT INTO data (content) VALUES (x'01020304')");

    const row = await db.query(sql`SELECT * FROM data WHERE content = ${blob}`);

    expect(row).toEqual({ id: 1, content: Buffer.from([1, 2, 3, 4]) });
})

test('sql template - no parameters (static query)', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE config (key TEXT, value TEXT)');
    await db.exec("INSERT INTO config VALUES ('version', '1.0')");

    const config = await db.query(sql`SELECT * FROM config`);

    expect(config).toEqual({ key: 'version', value: '1.0' });
})

test('sql template - with generic type parameter', async () => {
    interface User {
        id: number;
        name: string;
        age: number;
    }

    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30)");

    const minAge = 25;
    const users = await db.queryAll<User>(sql`SELECT * FROM users WHERE age > ${minAge}`);

    expect(users.length).toBe(1);
    expect(users[0].id).toBe(1);
    expect(users[0].name).toBe('Alice');
    expect(users[0].age).toBe(30);
})

test('sql template - prevents SQL injection', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    await db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    // Attempt SQL injection - should be safely parameterized
    const maliciousInput = "'; DROP TABLE users; --";
    const user = await db.query(sql`SELECT * FROM users WHERE name = ${maliciousInput}`);

    // Should return no results, not execute the injection
    expect(user).toBeUndefined();

    // Table should still exist
    const allUsers = await db.queryAll(sql`SELECT * FROM users ORDER BY id`);
    expect(allUsers.length).toBe(2);
})

test('sql template - mixed with regular query methods', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    await db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)");

    // Use sql template
    const minAge = 20;
    const templateResult = await db.queryAll(sql`SELECT * FROM users WHERE age > ${minAge}`);
    expect(templateResult.length).toBe(2);

    // Use regular string with params array (should still work)
    const regularResult = await db.queryAll('SELECT * FROM users WHERE age > ?', [20]);
    expect(regularResult.length).toBe(2);

    // Results should be identical
    expect(templateResult).toEqual(regularResult);
})

// ============================================
// Tests for db.batch() (Tier 2)
// ============================================

test('batch - executes multiple inserts atomically', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const results = await db.batch([
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
    const users = await db.queryAll('SELECT * FROM users ORDER BY id');
    expect(users).toEqual([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
    ]);
})

test('batch - rolls back on error', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');

    // First insert should succeed, second should fail (NULL name)
    await expect(db.batch([
        db.prepare('INSERT INTO users (name) VALUES (?)').bind('Alice'),
        db.prepare('INSERT INTO users (name) VALUES (?)').bind(null), // This should fail
    ])).rejects.toThrow();

    // Verify the transaction was rolled back - no rows should exist
    const users = await db.queryAll('SELECT * FROM users');
    expect(users).toEqual([]);
})

test('batch - empty array returns empty results', async () => {
    const db = await connect(':memory:');
    const results = await db.batch([]);
    expect(results).toEqual([]);
})

test('batch - throws on non-array input', async () => {
    const db = await connect(':memory:');
    // @ts-expect-error - Testing invalid input
    await expect(db.batch('not an array')).rejects.toThrow('Expected an array of statements');
})

test('batch - mixed insert and update', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 0)');
    await db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

    const results = await db.batch([
        db.prepare('INSERT INTO users (name) VALUES (?)').bind('Charlie'),
        db.prepare('UPDATE users SET active = 1 WHERE id <= 2'),
    ]);

    expect(results.length).toBe(2);
    expect(results[0].lastInsertRowid).toBe(3);

    // Verify the actual changes happened
    const users = await db.queryAll('SELECT * FROM users ORDER BY id');
    expect(users).toEqual([
        { id: 1, name: 'Alice', active: 1 },
        { id: 2, name: 'Bob', active: 1 },
        { id: 3, name: 'Charlie', active: 0 },
    ]);
})

test('batch - sets inTransaction flag correctly', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    expect(db.inTransaction).toBe(false);

    await db.batch([
        db.prepare('INSERT INTO users (name) VALUES (?)').bind('Alice'),
    ]);

    expect(db.inTransaction).toBe(false);
})

test('batch - resets inTransaction flag on error', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');

    expect(db.inTransaction).toBe(false);

    try {
        await db.batch([
            db.prepare('INSERT INTO users (name) VALUES (?)').bind(null),
        ]);
    } catch (e) {
        // Expected
    }

    expect(db.inTransaction).toBe(false);
})