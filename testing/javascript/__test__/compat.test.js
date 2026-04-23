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
