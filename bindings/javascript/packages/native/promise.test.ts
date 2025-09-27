import { unlinkSync } from "node:fs";
import { expect, test } from 'vitest'
import { Database, connect } from './promise.js'
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/better-sqlite3';

test('drizzle-orm', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const conn = await connect(path);
        const db = drizzle(conn);
        await db.run('CREATE TABLE t(x, y)');
        let tasks = [];
        for (let i = 0; i < 1234; i++) {
            tasks.push(db.run(sql`INSERT INTO t VALUES (${i}, randomblob(${i} * 5))`))
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
    await expect(async () => await connect(path, { fileMustExist: true })).rejects.toThrowError(/failed to open file/);
})

test('explicit connect', async () => {
    const db = new Database(':memory:');
    expect(() => db.prepare("SELECT 1")).toThrowError(/database must be connected/g);
    await db.connect();
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
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