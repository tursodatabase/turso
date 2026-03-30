import { unlinkSync } from "node:fs";
import { expect, test } from 'vitest'
import { Database } from './compat.js'

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
        const db1 = new Database(path1, { experimental: ["attach"] });
        db1.exec("CREATE TABLE t(x)");
        db1.exec("INSERT INTO t VALUES (1), (2), (3)");
        const db2 = new Database(path2, { experimental: ["attach"] });
        db2.exec("CREATE TABLE q(x)");
        db2.exec("INSERT INTO q VALUES (4), (5), (6)");

        db1.exec(`ATTACH '${path2}' as secondary`);

        const stmt = db1.prepare("SELECT * FROM t UNION ALL SELECT * FROM secondary.q");
        expect(stmt.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows = stmt.all();
        expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }, { x: 6 }]);
    } finally {
        unlinkSync(path1);
        unlinkSync(`${path1}-wal`);
        unlinkSync(path2);
        unlinkSync(`${path2}-wal`);
    }
})

test('named parameters with $', () => {
    const db = new Database(":memory:");
    db.exec("CREATE TABLE users(name TEXT, age INTEGER)");
    db.exec("INSERT INTO users VALUES ('Alice', 30), ('Bob', 25), ('Carol', 35)");
    const stmt = db.prepare("SELECT * FROM users WHERE name = $name AND age = $age");
    const row = stmt.get({ name: 'Alice', age: 30 });
    expect(row).toEqual({ name: 'Alice', age: 30 });
})

test('named parameters with :', () => {
    const db = new Database(":memory:");
    db.exec("CREATE TABLE t(x, y)");
    db.exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    const stmt = db.prepare("SELECT * FROM t WHERE x = :x");
    const rows = stmt.all({ x: 2 });
    expect(rows).toEqual([{ x: 2, y: 'b' }]);
})

test('named parameters with @', () => {
    const db = new Database(":memory:");
    db.exec("CREATE TABLE t(x)");
    db.exec("INSERT INTO t VALUES (10), (20), (30)");
    const stmt = db.prepare("SELECT * FROM t WHERE x = @val");
    const row = stmt.get({ val: 20 });
    expect(row).toEqual({ x: 20 });
})

test('named parameters with mixed prefixes', () => {
    const db = new Database(":memory:");
    db.exec("CREATE TABLE t(a, b, c)");
    db.exec("INSERT INTO t VALUES (1, 2, 3)");
    const stmt = db.prepare("SELECT * FROM t WHERE a = $a AND b = :b AND c = @c");
    const row = stmt.get({ a: 1, b: 2, c: 3 });
    expect(row).toEqual({ a: 1, b: 2, c: 3 });
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