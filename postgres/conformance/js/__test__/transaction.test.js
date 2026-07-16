import test from "ava";

import { connect } from "./_connect.js";

test.beforeEach(async (t) => {
  const { db, provider } = await connect();
  await db.exec("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)");
  await db.exec("INSERT INTO accounts VALUES (1, 100), (2, 50)");
  t.context = { db, provider };
});

test.afterEach.always(async (t) => {
  if (t.context.db && !t.context.db.closed) {
    await t.context.db.close();
  }
});

const balances = async (db) =>
  (await db.query("SELECT id, balance FROM accounts ORDER BY id")).rows;

test.serial("transaction() commits on success and returns callback result", async (t) => {
  const { db } = t.context;
  const result = await db.transaction(async (tx) => {
    await tx.query("UPDATE accounts SET balance = balance - $1 WHERE id = 1", [30]);
    await tx.query("UPDATE accounts SET balance = balance + $1 WHERE id = 2", [30]);
    return "transferred";
  });
  t.is(result, "transferred");
  t.deepEqual(await balances(db), [
    { id: 1, balance: 70 },
    { id: 2, balance: 80 },
  ]);
});

test.serial("transaction() rolls back when the callback throws", async (t) => {
  const { db } = t.context;
  const err = await t.throwsAsync(
    db.transaction(async (tx) => {
      await tx.query("UPDATE accounts SET balance = 0");
      throw new Error("abort transfer");
    }),
  );
  t.is(err.message, "abort transfer");
  t.deepEqual(await balances(db), [
    { id: 1, balance: 100 },
    { id: 2, balance: 50 },
  ]);
});

test.serial("transaction() rolls back when a statement errors", async (t) => {
  const { db } = t.context;
  const err = await t.throwsAsync(
    db.transaction(async (tx) => {
      await tx.query("UPDATE accounts SET balance = 0");
      await tx.query("INSERT INTO accounts VALUES (1, 1)"); // duplicate key
    }),
  );
  t.is(err.code, "23505");
  t.deepEqual(await balances(db), [
    { id: 1, balance: 100 },
    { id: 2, balance: 50 },
  ]);
});

test.serial("tx.rollback() discards changes without throwing", async (t) => {
  const { db } = t.context;
  const result = await db.transaction(async (tx) => {
    await tx.query("UPDATE accounts SET balance = 0");
    await tx.rollback();
    return "rolled back";
  });
  t.is(result, "rolled back");
  t.deepEqual(await balances(db), [
    { id: 1, balance: 100 },
    { id: 2, balance: 50 },
  ]);
});

test.serial("tx.closed reflects transaction state", async (t) => {
  const { db } = t.context;
  let inside;
  let after;
  const captured = await db.transaction(async (tx) => {
    inside = tx.closed;
    return tx;
  });
  after = captured.closed;
  t.false(inside);
  t.true(after);
});

test.serial("transaction sees its own uncommitted writes", async (t) => {
  const { db } = t.context;
  await db.transaction(async (tx) => {
    await tx.query("UPDATE accounts SET balance = 999 WHERE id = 1");
    const res = await tx.query("SELECT balance FROM accounts WHERE id = 1");
    t.deepEqual(res.rows, [{ balance: 999 }]);
    await tx.rollback();
  });
});

test.serial("tx.exec() runs multiple statements inside a transaction", async (t) => {
  const { db } = t.context;
  await db.transaction(async (tx) => {
    await tx.exec(`
      UPDATE accounts SET balance = balance + 1;
      UPDATE accounts SET balance = balance + 1;
    `);
  });
  t.deepEqual(await balances(db), [
    { id: 1, balance: 102 },
    { id: 2, balance: 52 },
  ]);
});

test.serial("tx.sql`` template works inside a transaction", async (t) => {
  const { db } = t.context;
  const id = 2;
  await db.transaction(async (tx) => {
    const res = await tx.sql`SELECT balance FROM accounts WHERE id = ${id}`;
    t.deepEqual(res.rows, [{ balance: 50 }]);
  });
});

test.serial("queries after a transaction see committed state", async (t) => {
  const { db } = t.context;
  await db.transaction(async (tx) => {
    await tx.query("INSERT INTO accounts VALUES (3, 10)");
  });
  const res = await db.query("SELECT count(*)::int4 AS n FROM accounts");
  t.deepEqual(res.rows, [{ n: 3 }]);
});
