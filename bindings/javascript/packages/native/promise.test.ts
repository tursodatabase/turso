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
    const stmt = await db.prepare("SELECT * FROM t WHERE x % 2 = ?");
    const rows = await stmt.all([1]);
    expect(rows).toEqual([{ x: 1 }, { x: 3 }]);
})

test('exec multiple statements', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)");
    const stmt = await db.prepare("SELECT * FROM t");
    const rows = await stmt.all();
    expect(rows).toEqual([{ x: 1 }, { x: 2 }]);
})

test('expanded rows preserve positional values for duplicate column names', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE role(path TEXT); CREATE TABLE org_unit(path TEXT)");
    await db.exec("INSERT INTO role VALUES ('/Employee'); INSERT INTO org_unit VALUES ('/')");

    const [row] = await db.all("SELECT role.path, org_unit.path FROM role JOIN org_unit");

    expect(Object.keys(row)).toEqual(["path"]);
    expect(row.path).toBe("/");
    expect(row[0]).toBe("/Employee");
    expect(row[1]).toBe("/");
    expect(row).toEqual({ path: "/" });
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
            expect(await (await ro.prepare("SELECT * FROM t")).all()).toEqual([{ x: 1 }])
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
    const defer = await db.prepare("SELECT * FROM t");
    await expect(async () => await defer.all()).rejects.toThrowError(/no such table: t/);
    await expect(async () => await db.prepare("SELECT * FROM t")).rejects.toThrowError(/no such table: t/);
    expect(await (await db.prepare("SELECT 1 as x")).all()).toEqual([{ x: 1 }]);
})

test('zero-limit-bug', async () => {
    const db = await connect(':memory:');
    const create = await db.prepare(`CREATE TABLE users (name TEXT NOT NULL);`);
    await create.run();

    const insert = await db.prepare(
        `insert into "users" values (?), (?), (?);`,
    );
    await insert.run('John', 'Jane', 'Jack');

    const stmt1 = await db.prepare(`select * from "users" limit ?;`);
    expect(await stmt1.all(0)).toEqual([]);
    let rows = [{ name: 'John' }, { name: 'Jane' }, { name: 'Jack' }, { name: 'John' }, { name: 'Jane' }, { name: 'Jack' }];
    for (const limit of [0, 1, 2, 3, 4, 5, 6, 7]) {
        const stmt2 = await db.prepare(`select * from "users" union all select * from "users" limit ?;`);
        expect(await stmt2.all(limit)).toEqual(rows.slice(0, Math.min(limit, 6)));
    }
})

test('avg-bug', async () => {
    const db = await connect(':memory:');
    const create = await db.prepare(`create table "aggregate_table" (
        "id" integer primary key autoincrement not null,
        "name" text not null,
        "a" integer,
        "b" integer,
        "c" integer,
        "null_only" integer
    );`);

    await create.run();
    const insert = await db.prepare(
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

    expect(await (await db.prepare(`select avg("a") from "aggregate_table";`)).get()).toEqual({ 'avg("a")': 24 });
    expect(await (await db.prepare(`select avg("null_only") from "aggregate_table";`)).get()).toEqual({ 'avg("null_only")': null });
    expect(await (await db.prepare(`select avg(distinct "b") from "aggregate_table";`)).get()).toEqual({ 'avg(distinct "b")': 42.5 });
})

test('insert returning test', async () => {
    const db = await connect(':memory:');
    await (await db.prepare(`create table t (x);`)).run();
    const x1 = await (await db.prepare(`insert into t values (1), (2) returning x`)).get();
    const x2 = await (await db.prepare(`insert into t values (3), (4) returning x`)).get();
    expect(x1).toEqual({ x: 1 });
    expect(x2).toEqual({ x: 3 });
    const all = await (await db.prepare(`select * from t`)).all();
    expect(all).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }])
})

test('offset-bug', async () => {
    const db = await connect(":memory:");
    await db.exec(`CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        verified integer not null default 0
    );`);
    const insert = await db.prepare(`INSERT INTO users (name) VALUES (?),(?);`);
    await insert.run('John', 'John1');

    const stmt = await db.prepare(`SELECT * FROM users LIMIT ? OFFSET ?;`);
    expect(await stmt.all(1, 1)).toEqual([{ id: 2, name: 'John1', verified: 0 }])
})

test('conflict-bug', async () => {
    const db = await connect(':memory:');

    const create = await db.prepare(`create table "conflict_chain_example" (
        id integer not null unique,
        name text not null,
        email text not null,
        primary key (id, name)
    )`);
    await create.run();

    await (await db.prepare(`insert into "conflict_chain_example" ("id", "name", "email") values (?, ?, ?), (?, ?, ?)`)).run(
        1,
        'John',
        'john@example.com',
        2,
        'John Second',
        '2john@example.com',
    );

    const insert = await db.prepare(
        `insert into "conflict_chain_example" ("id", "name", "email") values (?, ?, ?), (?, ?, ?) on conflict ("conflict_chain_example"."id", "conflict_chain_example"."name") do update set "email" = ? on conflict ("conflict_chain_example"."id") do nothing`,
    );
    await insert.run(1, 'John', 'john@example.com', 2, 'Anthony', 'idthief@example.com', 'john1@example.com');

    expect(await (await db.prepare("SELECT * FROM conflict_chain_example")).all()).toEqual([
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
        const stmt1 = await db1.prepare("SELECT * FROM t WHERE x % 2 = ?");
        expect(stmt1.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows1 = await stmt1.all([1]);
        expect(rows1).toEqual([{ x: 1 }, { x: 3 }]);
        db1.close();

        const db2 = await connect(path);
        const stmt2 = await db2.prepare("SELECT * FROM t WHERE x % 2 = ?");
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
        const db1 = await connect(path1, { experimental: ["attach"] });
        await db1.exec("CREATE TABLE t(x)");
        await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
        const db2 = await connect(path2, { experimental: ["attach"] });
        await db2.exec("CREATE TABLE q(x)");
        await db2.exec("INSERT INTO q VALUES (4), (5), (6)");

        await db1.exec(`ATTACH '${path2}' as secondary`);

        const stmt = await db1.prepare("SELECT * FROM t UNION ALL SELECT * FROM secondary.q");
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

test('fts', async () => {
    const db = await connect(":memory:", { experimental: ["index_method"] });
    await db.exec(`
        CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT, body TEXT);
        INSERT INTO documents VALUES (1, 'Introduction to Rust', 'Rust is a systems programming language focused on safety and performance');
        INSERT INTO documents VALUES (2, 'JavaScript Guide', 'JavaScript is a dynamic programming language used for web development');
        INSERT INTO documents VALUES (3, 'Database Internals', 'Understanding how databases store and retrieve data efficiently');
        CREATE INDEX documents_fts ON documents USING fts (title, body);
    `);

    // fts_match search
    const matchResults = await (await db.prepare(
        "SELECT id, title, fts_score(title, body, 'programming language') as score FROM documents WHERE fts_match(title, body, 'programming language')"
    )).all();
    expect(matchResults.length).toBe(2);
    expect(matchResults.map(r => r.id).sort()).toEqual([1, 2]);
    for (const row of matchResults) {
        expect(row.score).toBeGreaterThan(0);
    }

    // fts_highlight
    const highlightResults = await (await db.prepare(
        "SELECT id, fts_highlight(title, '<b>', '</b>', 'Rust') as highlighted FROM documents WHERE fts_match(title, body, 'Rust')"
    )).all();
    expect(highlightResults.length).toBe(1);
    expect(highlightResults[0].id).toBe(1);
    expect(highlightResults[0].highlighted).toContain('<b>');
    expect(highlightResults[0].highlighted).toContain('Rust');

    // no match
    const noResults = await (await db.prepare(
        "SELECT * FROM documents WHERE fts_match(title, body, 'nonexistentterm')"
    )).all();
    expect(noResults.length).toBe(0);
})

test('blobs', async () => {
    const db = await connect(":memory:");
    const rows = await (await db.prepare("SELECT x'1020' as x")).all();
    expect(rows).toEqual([{ x: Buffer.from([16, 32]) }])
})

test('encryption', async () => {
    const path = `test-encryption-${(Math.random() * 10000) | 0}.db`;
    const hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';
    const wrongKey = 'aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';
    try {
        const db = await connect(path, {
            encryption: { cipher: 'aegis256', hexkey }
        });
        await db.exec("CREATE TABLE t(x)");
        await db.exec("INSERT INTO t SELECT 'secret' FROM generate_series(1, 1024)");
        await db.exec("PRAGMA wal_checkpoint(truncate)");
        db.close();

        // Re-open with the same key - should work
        const db2 = await connect(path, {
            encryption: { cipher: 'aegis256', hexkey }
        });
        const rows = await (await db2.prepare("SELECT COUNT(*) as cnt FROM t")).all();
        expect(rows).toEqual([{ cnt: 1024 }]);
        db2.close();

        // Opening with wrong key MUST fail
        await expect(async () => {
            const db3 = await connect(path, {
                encryption: { cipher: 'aegis256', hexkey: wrongKey }
            });
            await (await db3.prepare("SELECT * FROM t")).all();
        }).rejects.toThrow();

        // Opening without encryption MUST fail
        await expect(async () => {
            const db4 = await connect(path);
            await (await db4.prepare("SELECT * FROM t")).all();
        }).rejects.toThrow();
    } finally {
        unlinkSync(path);
    }
})


test('example-1', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

    const insert = await db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
    await insert.run('Alice', 'alice@example.com');
    await insert.run('Bob', 'bob@example.com');

    const users = await (await db.prepare('SELECT * FROM users')).all();
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
        const insert = await db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
        for (const user of users) {
            await insert.run(user.name, user.email);
        }
    });

    // Execute transaction
    await transaction([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' }
    ]);

    const rows = await (await db.prepare('SELECT * FROM users')).all();
    expect(rows).toEqual([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' }
    ]);
})

// Each transaction() call runs on its own pooled connection, so concurrent
// calls must not interleave statements. The busy timeout lets the second
// write transaction wait for the first one instead of failing fast with
// "database is locked".
test('concurrent transaction() calls must not interleave', async () => {
    const db = await connect(':memory:', { timeout: 5000 });
    await db.exec('CREATE TABLE t(tag TEXT, i INTEGER)');

    const insertMany = db.transaction(async (tag: string, fail: boolean) => {
        for (let i = 0; i < 10; i++) {
            await db.run('INSERT INTO t VALUES (?, ?)', [tag, i]);
        }
        if (fail) {
            throw new Error('abort transaction');
        }
    });

    const [committed, aborted] = await Promise.allSettled([
        insertMany('committed', false),
        insertMany('rolled-back', true),
    ]);

    // the first transaction commits; the second fails only with its own error
    expect(committed).toEqual({ status: 'fulfilled', value: undefined });
    expect(aborted.status).toBe('rejected');
    expect((aborted as PromiseRejectedResult).reason.message).toBe('abort transaction');

    // atomicity: every row of the committed transaction survives and no row
    // of the rolled-back transaction does
    const rows = await db.all('SELECT tag, COUNT(*) AS cnt FROM t GROUP BY tag ORDER BY tag');
    expect(rows).toEqual([{ tag: 'committed', cnt: 10 }]);
})

// Transactions run on pooled connections while independent statements stay on
// the main connection, so an autocommit statement racing with a transaction()
// call must not slip into the open BEGIN..ROLLBACK window and be silently
// erased by the transaction's rollback.
test('statement racing a transaction() must not be lost to its rollback', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE t(tag TEXT)');

    const failing = db.transaction(async () => {
        await db.run('INSERT INTO t VALUES (?)', ['txn']);
        await new Promise((resolve) => setImmediate(resolve));
        throw new Error('abort transaction');
    });

    const [txnResult, plainResult] = await Promise.allSettled([
        failing(),
        db.run('INSERT INTO t VALUES (?)', ['plain']),
    ]);
    expect(txnResult.status).toBe('rejected');
    expect(plainResult.status).toBe('fulfilled');

    // the transaction rolled back, but the successful independent insert must survive
    expect(await db.all('SELECT tag FROM t')).toEqual([{ tag: 'plain' }]);
})

// The transaction pool grows by one connection per concurrent transaction and
// shrinks back to poolSize once they finish.
test('transaction pool grows on demand and shrinks to poolSize', async () => {
    const db = await connect(':memory:', { poolSize: 2 });
    await db.exec('CREATE TABLE t(x)');

    // read transactions so all three can stay open concurrently
    const gate: { resolve: () => void, promise: Promise<void> }[] = [];
    const txn = db.transaction(async () => {
        await db.get('SELECT COUNT(*) AS cnt FROM t');
        let resolve;
        const promise = new Promise<void>((r) => { resolve = r; });
        gate.push({ resolve, promise });
        await promise;
    });

    // three transactions suspended mid-body: one pooled connection each
    const tasks = [txn(), txn(), txn()];
    while (gate.length < 3) {
        await new Promise((resolve) => setImmediate(resolve));
    }
    expect((db as any).pool.length).toBe(3);
    expect((db as any).pool.every((entry) => entry.busy)).toBe(true);

    for (const g of gate) { g.resolve(); }
    await Promise.all(tasks);

    // idle connections beyond poolSize are deallocated
    expect((db as any).pool.length).toBe(2);
    expect((db as any).pool.some((entry) => entry.busy)).toBe(false);
})

// A statement prepared before a transaction() call must join the transaction
// when executed inside the callback (better-sqlite3 semantics): its writes
// commit and roll back with the transaction instead of running autocommit on
// the main connection.
test('statement prepared outside transaction() joins the transaction', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE t(x)');
    const insert = await db.prepare('INSERT INTO t VALUES (?)');

    const failing = db.transaction(async () => {
        await insert.run([1]);
        throw new Error('abort transaction');
    });
    await expect(failing()).rejects.toThrow('abort transaction');
    // the insert executed inside the transaction, so the rollback erased it
    expect(await db.all('SELECT x FROM t')).toEqual([]);

    await db.transaction(async () => { await insert.run([2]); })();
    expect(await db.all('SELECT x FROM t')).toEqual([{ x: 2 }]);

    // the statement keeps working on the main connection afterwards
    await insert.run([3]);
    expect(await db.all('SELECT x FROM t ORDER BY x')).toEqual([{ x: 2 }, { x: 3 }]);
})

// A transaction function invoked from inside another transaction's callback
// would run on a second pooled connection and deadlock against the outer
// transaction on the write lock, so it must be rejected upfront.
test('nested transaction() calls are forbidden', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE t(x)');

    const inner = db.transaction(async () => {
        await db.run('INSERT INTO t VALUES (2)');
    });
    const outer = db.transaction(async () => {
        await db.run('INSERT INTO t VALUES (1)');
        await inner();
    });

    await expect(outer()).rejects.toThrow('nested transactions are not supported');
    // the outer transaction rolled back cleanly
    expect(await db.all('SELECT x FROM t')).toEqual([]);
})

// When COMMIT and ROLLBACK both fail, the pooled connection is left with an
// open transaction; returning it to the pool would make every following
// transaction fail on BEGIN and keep the write lock held forever. It must be
// discarded instead.
test('a connection that fails to roll back is discarded, not pooled', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE t(x)');

    // populate the pool with one connection
    await db.transaction(async () => { await db.run('INSERT INTO t VALUES (0)'); })();
    expect((db as any).pool.length).toBe(1);

    const pooled = (db as any).pool[0].db;
    const originalExec = pooled.exec.bind(pooled);
    pooled.exec = async (sql: string, opts?: any) => {
        if (sql === 'ROLLBACK') {
            throw new Error('injected rollback failure');
        }
        return originalExec(sql, opts);
    };

    const failing = db.transaction(async () => {
        await db.run('INSERT INTO t VALUES (1)');
        throw new Error('abort transaction');
    });
    await expect(failing()).rejects.toThrow();

    // the dirty connection was closed and dropped from the pool
    expect((db as any).pool.length).toBe(0);

    // dropping it released the write lock: new transactions work again
    await db.transaction(async () => { await db.run('INSERT INTO t VALUES (2)'); })();
    expect(await db.all('SELECT x FROM t ORDER BY x')).toEqual([{ x: 0 }, { x: 2 }]);
})

// Failing to create a pooled connection must never silently run the
// transaction on the shared main connection with degraded isolation: hard
// errors propagate, and "cannot create right now" with no pooled connection
// to wait for fails with SQLITE_BUSY semantics.
test('transaction() propagates pooled-connection creation failures', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE t(x)');

    (db as any).newPooledNativeConnection = async () => { throw new Error('injected connect failure'); };
    await expect(db.transaction(async () => { })()).rejects.toThrow('injected connect failure');

    (db as any).newPooledNativeConnection = async () => null;
    await expect(db.transaction(async () => { })()).rejects.toThrow('cannot create a connection for the transaction');
    expect((db as any).pool.length).toBe(0);
})

test('transaction.concurrent uses BEGIN CONCURRENT', async () => {
    const db = await connect(':memory:');
    const originalExec = db.exec;
    const calls: string[] = [];
    db.exec = async (sql) => {
        calls.push(sql);
    };

    try {
        const txn = db.transaction(async () => {
            calls.push('body');
        }).concurrent;
        await txn();
        expect(calls).toEqual(['BEGIN CONCURRENT', 'body', 'COMMIT']);
    } finally {
        db.exec = originalExec;
        await db.close();
    }
})
