import { expect, test, afterAll } from 'vitest'
import { connect } from './promise-default.js'
import { MainWorker } from './index-default.js'

afterAll(() => {
    MainWorker?.terminate();
})

// Regression test for https://github.com/tursodatabase/turso/issues/8171
//
// On the wasm driver, a busy-handler retry makes stepSync() return STEP_IO
// even though no OPFS I/O is in flight. The driver reacts to STEP_IO by
// waiting on the shared IONotifier, and with no completion pending the
// notifier never fires: the statement parks forever instead of failing with
// "database is locked" after the configured busy_timeout.
test('write on a locked database with busy_timeout errors instead of hanging', async () => {
    const dbName = `busy-hang-8171-${Date.now()}.db`;

    // Connection A takes and holds the write lock.
    const writer = await connect(dbName);
    await writer.exec("CREATE TABLE t (x INTEGER)");
    await writer.exec("BEGIN IMMEDIATE");
    await writer.exec("INSERT INTO t VALUES (1)");

    // Connection B tries to write with a 200ms busy timeout. It must settle
    // (with "database is locked") well within 10 seconds. No other OPFS I/O
    // happens while we wait, so if the statement parks on the IONotifier
    // nothing will ever wake it up.
    const blocked = await connect(dbName);
    await blocked.exec("PRAGMA busy_timeout = 200");

    const STILL_PENDING = 'still pending after 10s';
    const insert = blocked.exec("INSERT INTO t VALUES (2)").then(
        () => 'resolved',
        (err: Error) => `rejected: ${err.message}`,
    );
    const outcome = await Promise.race([
        insert,
        new Promise((resolve) => setTimeout(() => resolve(STILL_PENDING), 10_000)),
    ]);
    console.log(`INSERT outcome: ${outcome}`);
    expect(outcome).not.toBe(STILL_PENDING);

    // The writer still holds the lock, so the insert cannot have succeeded.
    expect(outcome).toContain('rejected');

    await writer.exec("ROLLBACK");
    await writer.close();
})
