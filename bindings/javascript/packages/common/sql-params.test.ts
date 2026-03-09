/**
 * Type-level tests for SQL named parameter extraction.
 *
 * Uses vitest's built-in expectTypeOf (powered by expect-type) for real
 * type assertions. Tests go end-to-end through Database.prepare() →
 * Statement method signatures.
 *
 * Run: cd bindings/javascript/packages/common && npx vitest --run
 */

import { describe, it, expectTypeOf } from "vitest";
import type { BindParams, SqlValue } from "./sql-params.js";
import type { Database as CompatDatabase, Statement as CompatStatement } from "./compat.js";
import type { Database as PromiseDatabase, Statement as PromiseStatement } from "./promise.js";

// ─── BindParams utility type ─────────────────────────────────────────────────

describe("BindParams: single named parameter", () => {
  it("extracts $-prefixed param", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $name">>()
      .toEqualTypeOf<[params: { name: SqlValue }]>();
  });

  it("extracts :-prefixed param", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = :name">>()
      .toEqualTypeOf<[params: { name: SqlValue }]>();
  });

  it("extracts @-prefixed param", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = @name">>()
      .toEqualTypeOf<[params: { name: SqlValue }]>();
  });

  it("extracts param with underscores", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $first_name">>()
      .toEqualTypeOf<[params: { first_name: SqlValue }]>();
  });

  it("extracts param with digits", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $param1">>()
      .toEqualTypeOf<[params: { param1: SqlValue }]>();
  });

  it("extracts param starting with digit", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $1abc">>()
      .toEqualTypeOf<[params: { "1abc": SqlValue }]>();
  });
});

describe("BindParams: multiple named parameters", () => {
  it("extracts two params with different prefixes", () => {
    expectTypeOf<BindParams<"SELECT * FROM users WHERE name = $name AND age = :age">>()
      .toEqualTypeOf<[params: { name: SqlValue; age: SqlValue }]>();
  });

  it("extracts three params with mixed prefixes", () => {
    expectTypeOf<BindParams<"INSERT INTO t VALUES ($a, :b, @c)">>()
      .toEqualTypeOf<[params: { a: SqlValue; b: SqlValue; c: SqlValue }]>();
  });

  it("deduplicates repeated param name", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $name OR y = $name">>()
      .toEqualTypeOf<[params: { name: SqlValue }]>();
  });

  it("handles five params", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE a=$a AND b=$b AND c=$c AND d=$d AND e=$e">>()
      .toEqualTypeOf<[params: { a: SqlValue; b: SqlValue; c: SqlValue; d: SqlValue; e: SqlValue }]>();
  });
});

describe("BindParams: param terminators", () => {
  it("param followed by comma", () => {
    expectTypeOf<BindParams<"INSERT INTO t VALUES ($a, $b)">>()
      .toEqualTypeOf<[params: { a: SqlValue; b: SqlValue }]>();
  });

  it("param followed by closing paren", () => {
    expectTypeOf<BindParams<"INSERT INTO t VALUES ($val)">>()
      .toEqualTypeOf<[params: { val: SqlValue }]>();
  });

  it("param followed by semicolon", () => {
    expectTypeOf<BindParams<"UPDATE t SET x = $val;">>()
      .toEqualTypeOf<[params: { val: SqlValue }]>();
  });

  it("param followed by newline", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $name\nAND y = :age">>()
      .toEqualTypeOf<[params: { name: SqlValue; age: SqlValue }]>();
  });
});

describe("BindParams: complex SQL", () => {
  it("param in subquery", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x IN (SELECT id FROM s WHERE y = $val)">>()
      .toEqualTypeOf<[params: { val: SqlValue }]>();
  });

  it("param in JOIN condition", () => {
    expectTypeOf<BindParams<"SELECT * FROM t JOIN s ON t.id = s.id WHERE t.x = :val">>()
      .toEqualTypeOf<[params: { val: SqlValue }]>();
  });

  it("named params mixed with positional ?", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $name AND y = ?">>()
      .toEqualTypeOf<[params: { name: SqlValue }]>();
  });
});

describe("BindParams: positional parameters", () => {
  it("single ? gives spread SqlValue[]", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = ?">>()
      .toEqualTypeOf<[...params: (SqlValue | SqlValue[])[]]>();
  });

  it("multiple ? gives spread SqlValue[]", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = ? AND y = ?">>()
      .toEqualTypeOf<[...params: (SqlValue | SqlValue[])[]]>();
  });

  it("accepts an array as a single argument for positional params", () => {
    type Stmt = CompatStatement<"SELECT * FROM t WHERE x = ?">;
    // stmt.all([1]) — passing an array instead of spreading
    expectTypeOf<Stmt["all"]>()
      .toBeCallableWith([1]);
  });

  it("accepts spread args for positional params", () => {
    type Stmt = CompatStatement<"SELECT * FROM t WHERE x = ? AND y = ?">;
    // stmt.all(1, 2) — spreading individual values
    expectTypeOf<Stmt["all"]>()
      .toBeCallableWith(1, 2);
  });
});

describe("BindParams: no parameters", () => {
  it("no params gives empty tuple (no args allowed)", () => {
    expectTypeOf<BindParams<"SELECT * FROM t">>()
      .toEqualTypeOf<[]>();
  });

  it("empty string gives empty tuple", () => {
    expectTypeOf<BindParams<"">>()
      .toEqualTypeOf<[]>();
  });
});

describe("BindParams: non-literal string fallback", () => {
  it("plain string type gives permissive any[]", () => {
    expectTypeOf<BindParams<string>>()
      .toEqualTypeOf<[...params: any[]]>();
  });
});

describe("BindParams: edge cases", () => {
  it("$$ (prefix not followed by alphanum) gives empty tuple", () => {
    expectTypeOf<BindParams<"SELECT $$ FROM t">>()
      .toEqualTypeOf<[]>();
  });
});

describe("BindParams: rejects incorrect types", () => {
  it("named params do not match positional spread", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $name">>()
      .not.toEqualTypeOf<[...params: (SqlValue | SqlValue[])[]]>();
  });

  it("named params reject wrong keys", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $name">>()
      .not.toEqualTypeOf<[params: { wrong: SqlValue }]>();
  });

  it("named params reject empty object", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $name">>()
      .not.toEqualTypeOf<[params: {}]>();
  });

  it("positional does not match named object", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = ?">>()
      .not.toEqualTypeOf<[params: { x: SqlValue }]>();
  });

  it("non-literal string does not match specific named params", () => {
    expectTypeOf<BindParams<string>>()
      .not.toEqualTypeOf<[params: { name: SqlValue }]>();
  });

  it("named params are not permissive any[]", () => {
    expectTypeOf<BindParams<"SELECT * FROM t WHERE x = $name">>()
      .not.toEqualTypeOf<[...params: any[]]>();
  });
});

// ─── End-to-end: Database.prepare() → Statement methods ─────────────────────

describe("compat: Database.prepare() returns typed Statement", () => {
  it("prepare with named params returns Statement<SQL>", () => {
    expectTypeOf<CompatDatabase["prepare"]>()
      .returns.toMatchTypeOf<CompatStatement<any>>();
  });

  it("Statement.all() requires named params object for named SQL", () => {
    type Stmt = CompatStatement<"SELECT * FROM t WHERE x = $name AND y = :age">;
    expectTypeOf<Stmt["all"]>()
      .parameters.toEqualTypeOf<[params: { name: SqlValue; age: SqlValue }]>();
  });

  it("Statement.get() requires named params object for named SQL", () => {
    type Stmt = CompatStatement<"SELECT * FROM t WHERE x = $name">;
    expectTypeOf<Stmt["get"]>()
      .parameters.toEqualTypeOf<[params: { name: SqlValue }]>();
  });

  it("Statement.run() requires named params object for named SQL", () => {
    type Stmt = CompatStatement<"INSERT INTO t VALUES ($a, $b)">;
    expectTypeOf<Stmt["run"]>()
      .parameters.toEqualTypeOf<[params: { a: SqlValue; b: SqlValue }]>();
  });

  it("Statement.iterate() requires named params object for named SQL", () => {
    type Stmt = CompatStatement<"SELECT * FROM t WHERE id = :id">;
    expectTypeOf<Stmt["iterate"]>()
      .parameters.toEqualTypeOf<[params: { id: SqlValue }]>();
  });

  it("Statement.bind() requires named params object for named SQL", () => {
    type Stmt = CompatStatement<"SELECT * FROM t WHERE x = @val">;
    expectTypeOf<Stmt["bind"]>()
      .parameters.toEqualTypeOf<[params: { val: SqlValue }]>();
  });

  it("Statement.all() accepts positional args for ? SQL", () => {
    type Stmt = CompatStatement<"SELECT * FROM t WHERE x = ?">;
    expectTypeOf<Stmt["all"]>()
      .parameters.toEqualTypeOf<[...params: (SqlValue | SqlValue[])[]]>();
  });

  it("Statement.all() accepts no args for parameterless SQL", () => {
    type Stmt = CompatStatement<"SELECT * FROM t">;
    expectTypeOf<Stmt["all"]>()
      .parameters.toEqualTypeOf<[]>();
  });

  it("Statement.run() accepts no args for parameterless SQL", () => {
    type Stmt = CompatStatement<"CREATE TABLE t (id INTEGER)">;
    expectTypeOf<Stmt["run"]>()
      .parameters.toEqualTypeOf<[]>();
  });

  it("Statement methods are permissive for non-literal string", () => {
    type Stmt = CompatStatement<string>;
    expectTypeOf<Stmt["all"]>()
      .parameters.toEqualTypeOf<[...params: any[]]>();
  });
});

describe("promise: Database.prepare() returns typed Statement", () => {
  it("Statement.all() requires named params object for named SQL", () => {
    type Stmt = PromiseStatement<"SELECT * FROM t WHERE x = $name AND y = :age">;
    expectTypeOf<Stmt["all"]>()
      .parameters.toEqualTypeOf<[params: { name: SqlValue; age: SqlValue }]>();
  });

  it("Statement.get() requires named params object for named SQL", () => {
    type Stmt = PromiseStatement<"SELECT * FROM t WHERE x = $name">;
    expectTypeOf<Stmt["get"]>()
      .parameters.toEqualTypeOf<[params: { name: SqlValue }]>();
  });

  it("Statement.run() requires named params object for named SQL", () => {
    type Stmt = PromiseStatement<"INSERT INTO t VALUES ($a, $b)">;
    expectTypeOf<Stmt["run"]>()
      .parameters.toEqualTypeOf<[params: { a: SqlValue; b: SqlValue }]>();
  });

  it("Statement.iterate() requires named params for named SQL", () => {
    type Stmt = PromiseStatement<"SELECT * FROM t WHERE id = :id">;
    expectTypeOf<Stmt["iterate"]>()
      .parameters.toEqualTypeOf<[params: { id: SqlValue }]>();
  });

  it("Statement.bind() requires named params for named SQL", () => {
    type Stmt = PromiseStatement<"SELECT * FROM t WHERE x = @val">;
    expectTypeOf<Stmt["bind"]>()
      .parameters.toEqualTypeOf<[params: { val: SqlValue }]>();
  });

  it("Statement.all() accepts positional args for ? SQL", () => {
    type Stmt = PromiseStatement<"SELECT * FROM t WHERE x = ?">;
    expectTypeOf<Stmt["all"]>()
      .parameters.toEqualTypeOf<[...params: (SqlValue | SqlValue[])[]]>();
  });

  it("Statement.all() accepts no args for parameterless SQL", () => {
    type Stmt = PromiseStatement<"SELECT * FROM t">;
    expectTypeOf<Stmt["all"]>()
      .parameters.toEqualTypeOf<[]>();
  });

  it("Statement.run() accepts no args for parameterless SQL", () => {
    type Stmt = PromiseStatement<"CREATE TABLE t (id INTEGER)">;
    expectTypeOf<Stmt["run"]>()
      .parameters.toEqualTypeOf<[]>();
  });

  it("Statement methods are permissive for non-literal string", () => {
    type Stmt = PromiseStatement<string>;
    expectTypeOf<Stmt["all"]>()
      .parameters.toEqualTypeOf<[...params: any[]]>();
  });
});
