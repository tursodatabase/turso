import { expect, test, afterAll } from 'vitest'
import { connect, Database } from './promise-default.js'
import { MainWorker } from './index-default.js'

afterAll(() => {
    MainWorker?.terminate();
})

test('vector-test', async () => {
    const db = await connect(":memory:");
    const v1 = new Array(1024).fill(0).map((_, i) => i);
    const v2 = new Array(1024).fill(0).map((_, i) => 1024 - i);
    const result = await db.prepare(`SELECT
        vector_distance_cos(vector32('${JSON.stringify(v1)}'), vector32('${JSON.stringify(v2)}')) as cosf32,
        vector_distance_cos(vector64('${JSON.stringify(v1)}'), vector64('${JSON.stringify(v2)}')) as cosf64,
        vector_distance_l2(vector32('${JSON.stringify(v1)}'), vector32('${JSON.stringify(v2)}')) as l2f32,
        vector_distance_l2(vector64('${JSON.stringify(v1)}'), vector64('${JSON.stringify(v2)}')) as l2f64
    `).all();
    console.info(result);
    await db.close();
})

test('explain', async () => {
    const db = await connect(":memory:");
    const stmt = db.prepare("EXPLAIN SELECT 1");
    expect(stmt.columns()).toEqual([
        {
            "name": "addr",
            "type": "INTEGER",
        },
        {
            "name": "opcode",
            "type": "TEXT",
        },
        {
            "name": "p1",
            "type": "INTEGER",
        },
        {
            "name": "p2",
            "type": "INTEGER",
        },
        {
            "name": "p3",
            "type": "INTEGER",
        },
        {
            "name": "p4",
            "type": "TEXT",
        },
        {
            "name": "p5",
            "type": "INTEGER",
        },
        {
            "name": "comment",
            "type": "TEXT",
        },
    ].map(x => ({ ...x, column: null, database: null, table: null })));
    expect(await stmt.all()).toEqual([
        {
            "addr": 0,
            "comment": "Start at 3",
            "opcode": "Init",
            "p1": 0,
            "p2": 3,
            "p3": 0,
            "p4": "",
            "p5": 0,
        },
        {
            "addr": 1,
            "comment": "output=r[1]",
            "opcode": "ResultRow",
            "p1": 1,
            "p2": 1,
            "p3": 0,
            "p4": "",
            "p5": 0,
        },
        {
            "addr": 2,
            "comment": "",
            "opcode": "Halt",
            "p1": 0,
            "p2": 0,
            "p3": 0,
            "p4": "",
            "p5": 0,
        },
        {
            "addr": 3,
            "comment": "r[1]=1",
            "opcode": "Integer",
            "p1": 1,
            "p2": 1,
            "p3": 0,
            "p4": "",
            "p5": 0,
        },
        {
            "addr": 4,
            "comment": "",
            "opcode": "Goto",
            "p1": 0,
            "p2": 1,
            "p3": 0,
            "p4": "",
            "p5": 0,
        },
    ]);
    await db.close();
})

test('in-memory db', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    const stmt = db.prepare("SELECT * FROM t WHERE x % 2 = ?");
    const rows = await stmt.all([1]);
    expect(rows).toEqual([{ x: 1 }, { x: 3 }]);
    await db.close();
})


test('implicit connect', async () => {
    const db = new Database(':memory:');
    const defer = db.prepare("SELECT * FROM t");
    await expect(async () => await defer.all()).rejects.toThrowError(/no such table: t/);
    expect(() => db.prepare("SELECT * FROM t")).toThrowError(/no such table: t/);
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
    await db.close();
})

test('on-disk db large inserts', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    const db1 = await connect(path);
    await db1.prepare("CREATE TABLE t(x)").run();
    await db1.prepare("INSERT INTO t VALUES (randomblob(10 * 4096 + 0))").run();
    await db1.prepare("INSERT INTO t VALUES (randomblob(10 * 4096 + 1))").run();
    await db1.prepare("INSERT INTO t VALUES (randomblob(10 * 4096 + 2))").run();
    const stmt1 = db1.prepare("SELECT length(x) as l FROM t");
    expect(stmt1.columns()).toEqual([{ name: "l", column: null, database: null, table: null, type: null }]);
    const rows1 = await stmt1.all();
    expect(rows1).toEqual([{ l: 10 * 4096 }, { l: 10 * 4096 + 1 }, { l: 10 * 4096 + 2 }]);

    await db1.exec("BEGIN");
    await db1.exec("INSERT INTO t VALUES (1)");
    await db1.exec("ROLLBACK");

    const rows2 = await db1.prepare("SELECT length(x) as l FROM t").all();
    expect(rows2).toEqual([{ l: 10 * 4096 }, { l: 10 * 4096 + 1 }, { l: 10 * 4096 + 2 }]);

    await db1.prepare("PRAGMA wal_checkpoint(TRUNCATE)").run();
    await db1.close();
})

test('on-disk db', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    const db1 = await connect(path);
    await db1.exec("CREATE TABLE t(x)");
    await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
    const stmt1 = db1.prepare("SELECT * FROM t WHERE x % 2 = ?");
    expect(stmt1.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
    const rows1 = await stmt1.all([1]);
    expect(rows1).toEqual([{ x: 1 }, { x: 3 }]);
    stmt1.close();
    await db1.close();

    const db2 = await connect(path);
    const stmt2 = db2.prepare("SELECT * FROM t WHERE x % 2 = ?");
    expect(stmt2.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
    const rows2 = await stmt2.all([1]);
    expect(rows2).toEqual([{ x: 1 }, { x: 3 }]);
    db2.close();
})

// attach is not supported in browser for now
// test('attach', async () => {
//     const path1 = `test-${(Math.random() * 10000) | 0}.db`;
//     const path2 = `test-${(Math.random() * 10000) | 0}.db`;
//     const db1 = await connect(path1);
//     await db1.exec("CREATE TABLE t(x)");
//     await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
//     const db2 = await connect(path2);
//     await db2.exec("CREATE TABLE q(x)");
//     await db2.exec("INSERT INTO q VALUES (4), (5), (6)");

//     await db1.exec(`ATTACH '${path2}' as secondary`);

//     const stmt = db1.prepare("SELECT * FROM t UNION ALL SELECT * FROM secondary.q");
//     expect(stmt.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
//     const rows = await stmt.all([1]);
//     expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }, { x: 6 }]);
// })

test('blobs', async () => {
    const db = await connect(":memory:");
    const rows = await db.prepare("SELECT x'1020' as x").all();
    expect(rows).toEqual([{ x: new Uint8Array([16, 32]) }])
    await db.close();
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
    await db.close();
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
    await db.close();
})

test('sorter-wasm', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE t (k, v)');
    for (let i = 0; i < 1024; i++) {
        await db.exec(`INSERT INTO t VALUES (${i}, randomblob(10 * 1024))`);
    }

    expect(await db.prepare("SELECT length(v) as l FROM (SELECT v FROM t ORDER BY k)").all()).toEqual(new Array(1024).fill({ l: 10 * 1024 }));
    await db.close();
})

test('hash-join-wasm', { timeout: 60_000 }, async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE a (k, v)');
    await db.exec('CREATE TABLE b (k, v)');
    for (let i = 0; i < 1024; i++) {
        await db.exec(`INSERT INTO a VALUES (${i}, randomblob(100 * 1024))`);
    }
    for (let i = 0; i < 1024; i++) {
        await db.exec(`INSERT INTO b VALUES (${i}, randomblob(100 * 1024))`);
    }

    expect(await db.prepare("SELECT length(a) as a, length(b) as b FROM (SELECT a.v as a, b.v as b FROM a INNER JOIN b ON a.k = b.k)").all()).toEqual(new Array(1024).fill({ a: 100 * 1024, b: 100 * 1024 }));
    await db.close();
})