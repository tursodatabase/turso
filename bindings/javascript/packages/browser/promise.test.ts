import { expect, test } from 'vitest'
import { connect, Database } from './promise-default.js'

test('in-memory db', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    const stmt = db.prepare("SELECT * FROM t WHERE x % 2 = ?");
    const rows = await stmt.all([1]);
    expect(rows).toEqual([{ x: 1 }, { x: 3 }]);
})

test('explicit connect', async () => {
    const db = new Database(':memory:');
    expect(() => db.prepare("SELECT 1")).toThrowError(/database must be connected/g);
    await db.connect();
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
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