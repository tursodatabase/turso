import test from "ava";

// Compat tests target a real Turso Cloud database over HTTP; they can't run
// without one. Skip the whole file when TURSO_DATABASE_URL is unset so the
// generic conformance job doesn't red-bar on it.
const hasCompatUrl = !!process.env.TURSO_DATABASE_URL;
const compatTest = hasCompatUrl ? test.serial : test.serial.skip;

const toCount = (result, key = "count") => {
  const row = result?.rows?.[0];
  if (!row) {
    return 0;
  }
  return Number(row[key] ?? row[0] ?? 0);
};

const getCompatProvider = () => {
  return process.env.COMPAT_PROVIDER || "serverless-compat";
};

const getConfig = () => ({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

const connectCompatClient = async () => {
  const provider = getCompatProvider();
  const config = getConfig();

  if (provider === "serverless-compat") {
    const mod = await import("@tursodatabase/serverless/compat");
    return {
      provider,
      client: mod.createClient(config),
      errorType: mod.LibsqlError,
    };
  }

  if (provider === "libsql-client") {
    const mod = await import("@libsql/client");
    return {
      provider,
      client: mod.createClient(config),
      errorType: mod.LibsqlError ?? Error,
    };
  }

  throw new Error(`Unknown COMPAT_PROVIDER: ${provider}`);
};

if (hasCompatUrl) {
  test.beforeEach(async (t) => {
    const { provider, client, errorType } = await connectCompatClient();
    await client.executeMultiple(`
      DROP TABLE IF EXISTS compat_users;
      CREATE TABLE compat_users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      );
      INSERT INTO compat_users (id, name) VALUES (1, 'Alice');
    `);

    t.context = { provider, client, errorType };
  });

  test.afterEach.always((t) => {
    if (t.context.client) {
      t.context.client.close();
    }
  });
}

compatTest("Compat interactive transaction COMMIT persists writes", async (t) => {
  const { client } = t.context;

  const tx = await client.transaction("write");
  await tx.execute({ sql: "INSERT INTO compat_users (name) VALUES (?)", args: ["TxCommit"] });

  const inside = await tx.execute({ sql: "SELECT COUNT(*) AS count FROM compat_users WHERE name = ?", args: ["TxCommit"] });
  t.is(toCount(inside), 1);

  await tx.commit();
  t.true(tx.closed);

  const outside = await client.execute({ sql: "SELECT COUNT(*) AS count FROM compat_users WHERE name = ?", args: ["TxCommit"] });
  t.is(toCount(outside), 1);
});

compatTest("Compat interactive transaction ROLLBACK discards writes", async (t) => {
  const { client } = t.context;

  const tx = await client.transaction("write");
  await tx.execute({ sql: "INSERT INTO compat_users (name) VALUES (?)", args: ["TxRollback"] });
  await tx.rollback();
  t.true(tx.closed);

  const outside = await client.execute({ sql: "SELECT COUNT(*) AS count FROM compat_users WHERE name = ?", args: ["TxRollback"] });
  t.is(toCount(outside), 0);
});

compatTest("Compat interactive transaction rollback after constraint error keeps client usable", async (t) => {
  const { client } = t.context;

  const tx = await client.transaction("write");
  await tx.execute({ sql: "INSERT INTO compat_users (name) VALUES (?)", args: ["WillRollback"] });

  const err = await t.throwsAsync(async () => {
    await tx.execute({ sql: "INSERT INTO compat_users (id, name) VALUES (?, ?)", args: [1, "DuplicateId"] });
  }, { any: true });
  t.truthy(err);
  const hint = `${err.code ?? ""} ${err.message ?? ""}`.toUpperCase();
  t.true(
    hint.includes("CONSTRAINT")
    || hint.includes("UNIQUE")
    || hint.includes("PRIMARYKEY"),
  );

  await tx.rollback();

  const countAfterRollback = await client.execute({ sql: "SELECT COUNT(*) AS count FROM compat_users WHERE name = ?", args: ["WillRollback"] });
  t.is(toCount(countAfterRollback), 0);

  const tx2 = await client.transaction("write");
  await tx2.execute({ sql: "INSERT INTO compat_users (name) VALUES (?)", args: ["AfterRollback"] });
  await tx2.commit();

  const countAfterCommit = await client.execute({ sql: "SELECT COUNT(*) AS count FROM compat_users WHERE name = ?", args: ["AfterRollback"] });
  t.is(toCount(countAfterCommit), 1);
});

// ==========================================================================
// client.batch()
//
// The libSQL-compatible client.batch(stmts, mode?) returns
// Promise<Array<ResultSet>> — one ResultSet per input statement, in order,
// each with { columns, columnTypes, rows, rowsAffected }.
// libSQL runs the whole batch as a single transaction, so any failure rolls
// the entire batch back.
// ==========================================================================

compatTest("Compat batch() returns one ResultSet per statement, in order", async (t) => {
  const { client } = t.context;

  const results = await client.batch([
    { sql: "INSERT INTO compat_users (name) VALUES (?)", args: ["Bob"] },
    "SELECT id, name FROM compat_users ORDER BY id",
  ]);

  t.true(Array.isArray(results), "batch() returns an array");
  t.is(results.length, 2, "one ResultSet per input statement");

  // INSERT: nothing to read back, one row affected.
  t.deepEqual(results[0].rows, []);
  t.is(results[0].rowsAffected, 1);

  // SELECT: rows surfaced, nothing affected.
  t.deepEqual(results[1].columns, ["id", "name"]);
  t.is(results[1].rowsAffected, 0);
  t.is(results[1].rows.length, 2);
  t.is(results[1].rows[0].name, "Alice");
  t.is(results[1].rows[1].name, "Bob");
});

compatTest("Compat batch() honors bound args", async (t) => {
  const { client } = t.context;

  // Regression guard: the args of each statement must be bound, not ignored.
  const results = await client.batch([
    { sql: "INSERT INTO compat_users (name) VALUES (?)", args: ["Carol"] },
    { sql: "SELECT name FROM compat_users WHERE name = ?", args: ["Carol"] },
  ]);

  t.is(results[1].rows.length, 1);
  t.is(results[1].rows[0].name, "Carol");
});

compatTest("Compat batch() raw option returns positional rows", async (t) => {
  const { client } = t.context;

  const [rs] = await client.batch([
    { sql: "SELECT id, name FROM compat_users WHERE id = ?", args: [1] },
  ], { raw: true });

  const row = rs.rows[0];
  t.true(Array.isArray(row));
  t.is(row[0], 1);
  t.is(row[1], "Alice");
});

compatTest("Compat batch() is atomic: a failure rolls back the whole batch", async (t) => {
  const { client } = t.context;

  await t.throwsAsync(async () => {
    await client.batch([
      { sql: "INSERT INTO compat_users (name) VALUES (?)", args: ["Atomic1"] },
      // Duplicate primary key with the seeded id = 1 row.
      { sql: "INSERT INTO compat_users (id, name) VALUES (?, ?)", args: [1, "Dup"] },
    ]);
  }, { any: true });

  const after = await client.execute({ sql: "SELECT COUNT(*) AS count FROM compat_users WHERE name = ?", args: ["Atomic1"] });
  t.is(toCount(after), 0, "the earlier insert must not survive the failed batch");
});
