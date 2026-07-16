import test from "ava";

import { connect } from "./_connect.js";

test.beforeEach(async (t) => {
  const { db, provider } = await connect();
  t.context = { db, provider };
});

test.afterEach.always(async (t) => {
  if (t.context.db && !t.context.db.closed) {
    await t.context.db.close();
  }
});

// PGlite parses result values by dataTypeID into native JS values. These
// tests pin down the exact JS type each Postgres type maps to.

test.serial("boolean maps to JS boolean", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT true AS t, false AS f");
  t.is(res.rows[0].t, true);
  t.is(res.rows[0].f, false);
  t.is(res.fields[0].dataTypeID, 16); // BOOL
});

test.serial("integer types map to JS number", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT 1::int2 AS i2, 2::int4 AS i4, 3::int8 AS i8");
  t.is(res.rows[0].i2, 1);
  t.is(res.rows[0].i4, 2);
  t.is(res.rows[0].i8, 3);
  t.deepEqual(
    res.fields.map((f) => f.dataTypeID),
    [21, 23, 20], // INT2, INT4, INT8
  );
});

test.serial("int8 beyond Number.MAX_SAFE_INTEGER maps to BigInt", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT 9007199254740993::int8 AS big");
  t.is(typeof res.rows[0].big, "bigint");
  t.is(res.rows[0].big, 9007199254740993n);
});

test.serial("float types map to JS number", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT 1.5::float4 AS f4, 2.5::float8 AS f8");
  t.is(res.rows[0].f4, 1.5);
  t.is(res.rows[0].f8, 2.5);
});

test.serial("numeric maps to JS string", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT 1.50::numeric(10,2) AS n");
  t.is(res.rows[0].n, "1.50");
  t.is(res.fields[0].dataTypeID, 1700); // NUMERIC
});

test.serial("text and varchar map to JS string", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT 'a'::text AS t, 'b'::varchar(10) AS v");
  t.is(res.rows[0].t, "a");
  t.is(res.rows[0].v, "b");
});

test.serial("bytea maps to Uint8Array", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT '\\x010203'::bytea AS bin");
  t.deepEqual(res.rows[0].bin, new Uint8Array([1, 2, 3]));
});

test.serial("bytea parameter round-trips", async (t) => {
  const { db } = t.context;
  await db.exec("CREATE TABLE blobs (data BYTEA)");
  await db.query("INSERT INTO blobs VALUES ($1)", [new Uint8Array([0, 255, 128])]);
  const res = await db.query("SELECT data FROM blobs");
  t.deepEqual(res.rows[0].data, new Uint8Array([0, 255, 128]));
});

test.serial("json and jsonb map to parsed JS values", async (t) => {
  const { db } = t.context;
  const res = await db.query(
    `SELECT '{"a": 1, "b": [true, null]}'::jsonb AS j, '"str"'::json AS s`,
  );
  t.deepEqual(res.rows[0].j, { a: 1, b: [true, null] });
  t.is(res.rows[0].s, "str");
  t.is(res.fields[0].dataTypeID, 3802); // JSONB
});

test.serial("date and timestamp map to JS Date", async (t) => {
  const { db } = t.context;
  const res = await db.query(
    "SELECT '2024-01-15'::date AS d, '2024-01-15 12:30:00'::timestamp AS ts",
  );
  t.true(res.rows[0].d instanceof Date);
  t.true(res.rows[0].ts instanceof Date);
  t.is(res.rows[0].d.toISOString(), "2024-01-15T00:00:00.000Z");
});

test.serial("arrays map to JS arrays", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT ARRAY[1, 2, 3] AS ints, ARRAY['a', 'b'] AS strs");
  t.deepEqual(res.rows[0].ints, [1, 2, 3]);
  t.deepEqual(res.rows[0].strs, ["a", "b"]);
});

test.serial("NULL maps to JS null", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT NULL::text AS n");
  t.is(res.rows[0].n, null);
});

test.serial("uuid maps to JS string", async (t) => {
  const { db } = t.context;
  const res = await db.query(
    "SELECT '123e4567-e89b-12d3-a456-426614174000'::uuid AS u",
  );
  t.is(res.rows[0].u, "123e4567-e89b-12d3-a456-426614174000");
  t.is(res.fields[0].dataTypeID, 2950); // UUID
});

test.serial("JS parameter types serialize to matching Postgres types", async (t) => {
  const { db } = t.context;
  await db.exec("CREATE TABLE vals (b BOOL, n INT, f FLOAT8, s TEXT, j JSONB)");
  await db.query("INSERT INTO vals VALUES ($1, $2, $3, $4, $5)", [
    true,
    7,
    1.25,
    "text",
    { nested: [1, 2] },
  ]);
  const res = await db.query("SELECT * FROM vals");
  t.deepEqual(res.rows, [
    { b: true, n: 7, f: 1.25, s: "text", j: { nested: [1, 2] } },
  ]);
});

test.serial("custom parser overrides type parsing per query", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT 1.50::numeric(10,2) AS n", [], {
    parsers: { 1700: (v) => Number(v) },
  });
  t.is(res.rows[0].n, 1.5);
});
