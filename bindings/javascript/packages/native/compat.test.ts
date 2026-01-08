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