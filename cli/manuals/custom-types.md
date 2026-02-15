---
display_name: "custom types"
---

# Custom Types

## Overview

Turso extends SQLite's STRICT table type system with user-defined custom types. Custom types let you define how values are encoded before storage and decoded when read, enforce domain constraints at the storage layer, attach operators, and provide defaults — all declared in pure SQL.

Custom types work only with **STRICT** tables and require the `--experimental-strict` flag:

```
tursodb --experimental-strict mydb.db
```

Without this flag, `CREATE TYPE`, `DROP TYPE`, the `sqlite_turso_types` virtual table, and all built-in custom types (date, varchar, numeric, etc.) are unavailable. `PRAGMA list_types` will only show the five base SQLite types (INTEGER, REAL, TEXT, BLOB, ANY).

## Creating a Type

```sql
CREATE TYPE type_name BASE base_type
    ENCODE encode_expr
    DECODE decode_expr
    [OPERATOR 'op' [function_name] ...]
    [DEFAULT default_expr];
```

- **BASE** — The underlying SQLite storage type (`text`, `integer`, `real`, `blob`).
- **ENCODE** — Expression applied to `value` before writing to disk.
- **DECODE** — Expression applied to `value` when reading from disk.
- **OPERATOR** — (Optional) Custom operator overloads for the type. If `function_name` is omitted, the base type's built-in comparison is used (see [Ordering](#ordering) below).
- **DEFAULT** — (Optional) Default value for columns of this type when no value is supplied.

The special identifier `value` refers to the input being encoded or decoded.

## Dropping a Type

```sql
DROP TYPE type_name;
DROP TYPE IF EXISTS type_name;
```

A type cannot be dropped while any table has a column using it.

## Basic Examples

### Identity Type (Passthrough)

The simplest custom type — stores and reads values unchanged:

```sql
CREATE TYPE passthrough BASE text ENCODE value DECODE value;
CREATE TABLE t1(val passthrough) STRICT;
INSERT INTO t1 VALUES ('hello');
SELECT val FROM t1;
-- hello
```

### Reversed Text

Encode reverses the string for storage; decode reverses it back:

```sql
CREATE TYPE reversed BASE text
    ENCODE string_reverse(value)
    DECODE string_reverse(value);
CREATE TABLE t1(val reversed) STRICT;
INSERT INTO t1 VALUES ('hello');
SELECT val FROM t1;
-- hello  (stored on disk as 'olleh')
```

### Cents (Expression-Based Encode/Decode)

Store monetary values as integers (cents) but present them as whole units:

```sql
CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100;
CREATE TABLE prices(amount cents) STRICT;
INSERT INTO prices VALUES (42);
SELECT amount FROM prices;
-- 42  (stored on disk as 4200)
```

### JSON Validation

Use `json()` as the encoder to reject malformed JSON at insert time:

```sql
CREATE TYPE jsontype BASE text ENCODE json(value) DECODE value;
CREATE TABLE t1(val jsontype) STRICT;
INSERT INTO t1 VALUES ('{"key": 1}');  -- OK
INSERT INTO t1 VALUES ('not json');    -- Error: malformed JSON
```

## Operators

Custom types can overload SQL operators so expressions like `val + val` or `val < 10` call user-defined functions:

```sql
CREATE TYPE uint BASE text
    ENCODE test_uint_encode(value)
    DECODE test_uint_decode(value)
    OPERATOR '+' (uint) -> test_uint_add
    OPERATOR '<' (uint) -> test_uint_lt
    OPERATOR '=' (uint) -> test_uint_eq;

CREATE TABLE t1(val uint) STRICT;
INSERT INTO t1 VALUES (20);
INSERT INTO t1 VALUES (30);

SELECT val + val FROM t1;
-- 40
-- 60

SELECT val FROM t1 WHERE val < 25;
-- 20
```

## Ordering

Sorting and indexing always operate on **encoded (on-disk) values**, not decoded values. DECODE is purely a presentation layer — it controls how values appear in query results, but has no effect on sort order or index structure.

Custom types that support ordering must declare `OPERATOR '<'`. Without it, `ORDER BY` and `CREATE INDEX` on columns of that type are **forbidden** — attempting either produces a clear error.

### Naked `OPERATOR '<'` (Base Type Comparison)

A naked `OPERATOR '<'` (no function name) tells Turso to compare encoded values using the base type's built-in comparison. This works correctly when the encoding preserves the desired sort order:

```sql
-- ENCODE value * 100 is monotonic: 10→100, 20→200, 30→300.
-- Sorting encoded integers preserves numeric order.
CREATE TYPE cents BASE integer
    ENCODE value * 100
    DECODE value / 100
    OPERATOR '<';

CREATE TABLE prices(id INTEGER PRIMARY KEY, amount cents) STRICT;
INSERT INTO prices VALUES (1, 30), (2, 10), (3, 20);
SELECT amount FROM prices ORDER BY amount;
-- 10
-- 20
-- 30
```

If the encoding does **not** preserve order, the sort will reflect the encoded representation:

```sql
-- string_reverse is NOT monotonic: encoded text sorts differently than decoded.
-- Encoded: apple→elppa, banana→ananab, cherry→yrrehc.
-- Encoded text sort: ananab < elppa < yrrehc → display: banana, apple, cherry.
CREATE TYPE reversed BASE text
    ENCODE string_reverse(value)
    DECODE string_reverse(value)
    OPERATOR '<';

CREATE TABLE t(id INTEGER PRIMARY KEY, val reversed) STRICT;
INSERT INTO t VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry');
SELECT val FROM t ORDER BY val;
-- banana
-- apple
-- cherry
```

### `OPERATOR '<'` with a Function (Custom Comparator)

For types where the base type comparison on encoded values is not suitable, provide a custom comparator function. The comparator transforms encoded values before comparing:

```sql
-- numeric stores values as blobs; standard blob comparison is wrong.
-- numeric_lt knows how to compare encoded blobs numerically.
CREATE TYPE numeric(precision, scale) BASE blob
    ENCODE numeric_encode(value, precision, scale)
    DECODE numeric_decode(value)
    OPERATOR '<' numeric_lt;
```

A comparator can also recover a desired sort order from a non-order-preserving encoding:

```sql
-- Same encoding as above, but the comparator reverses encoded values
-- before comparing, recovering alphabetical order.
CREATE TYPE reversed_alpha BASE text
    ENCODE string_reverse(value)
    DECODE string_reverse(value)
    OPERATOR '<' string_reverse;

CREATE TABLE t(id INTEGER PRIMARY KEY, val reversed_alpha) STRICT;
INSERT INTO t VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry');
SELECT val FROM t ORDER BY val;
-- apple
-- banana
-- cherry
```

### Non-Orderable Types

Types without `OPERATOR '<'` cannot be used in `ORDER BY` or `CREATE INDEX`:

```sql
CREATE TYPE mytype BASE text ENCODE value DECODE value;
CREATE TABLE t(val mytype) STRICT;

SELECT val FROM t ORDER BY val;
-- Error: cannot ORDER BY column 'val' of type 'mytype': type does not declare OPERATOR '<'

CREATE INDEX idx ON t(val);
-- Error: cannot create index on column 'val' of type 'mytype': type does not declare OPERATOR '<'
```

Expression indexes that compute a regular value from a non-orderable column are still allowed:

```sql
CREATE INDEX idx ON t(length(val));  -- OK: length() returns an integer
```

### Built-In Types with Ordering

The following built-in types declare `OPERATOR '<'` and support `ORDER BY` and indexing: `date`, `time`, `timestamp`, `varchar`, `smallint`, `boolean`, `uuid`, `bytea`, `numeric`.

Types without ordering support: `json`, `jsonb`, `inet`.

## Defaults

### Type-Level Default

A default defined on the type applies to all columns of that type unless overridden:

```sql
CREATE TYPE uint BASE text
    ENCODE test_uint_encode(value)
    DECODE test_uint_decode(value)
    DEFAULT 0;

CREATE TABLE t1(id INTEGER PRIMARY KEY, val uint) STRICT;
INSERT INTO t1(id) VALUES (1);
SELECT id, val FROM t1;
-- 1|0
```

### Column-Level Override

A column definition can override the type's default:

```sql
CREATE TABLE t1(id INTEGER PRIMARY KEY, val uint DEFAULT 42) STRICT;
INSERT INTO t1(id) VALUES (1);
SELECT id, val FROM t1;
-- 1|42
```

### Function Default

The default can be an expression or function call:

```sql
CREATE TYPE reversed BASE text
    ENCODE string_reverse(value)
    DECODE string_reverse(value)
    DEFAULT string_reverse('auto');

CREATE TABLE t1(id INTEGER PRIMARY KEY, val reversed) STRICT;
INSERT INTO t1(id) VALUES (1);
SELECT id, val FROM t1;
-- 1|otua
```

## Validation with CASE/RAISE

Use `CASE ... ELSE RAISE(ABORT, ...)` in the ENCODE expression to validate values and reject invalid input with a clear error message:

```sql
CREATE TYPE positive_int BASE integer
    ENCODE CASE WHEN value > 0 THEN value
                ELSE RAISE(ABORT, 'value must be positive') END
    DECODE value;

CREATE TABLE t1(val positive_int) STRICT;
INSERT INTO t1 VALUES (42);   -- OK
INSERT INTO t1 VALUES (-1);   -- Error: value must be positive
```

This pattern is how built-in types like `varchar` and `smallint` enforce their constraints:

```sql
-- varchar checks length against the maxlen parameter
CREATE TYPE varchar(maxlen) BASE text
    ENCODE CASE WHEN length(value) <= maxlen THEN value
                ELSE RAISE(ABORT, 'value too long for varchar') END
    DECODE value;

-- smallint checks the integer range
CREATE TYPE smallint BASE integer
    ENCODE CASE WHEN value BETWEEN -32768 AND 32767 THEN value
                ELSE RAISE(ABORT, 'integer out of range for smallint') END
    DECODE value;
```

## Parametric Types

Types can declare parameters that are substituted into ENCODE/DECODE expressions. Parameters are specified in parentheses after the type name:

```sql
CREATE TYPE varchar(maxlen) BASE text
    ENCODE CASE WHEN length(value) <= maxlen THEN value
                ELSE RAISE(ABORT, 'value too long for varchar') END
    DECODE value;

CREATE TABLE t1(name varchar(10)) STRICT;
INSERT INTO t1 VALUES ('hello');      -- OK (length 5 <= 10)
INSERT INTO t1 VALUES ('toolongname'); -- Error: value too long for varchar
```

When a column is declared as `varchar(10)`, the parameter `maxlen` is replaced with `10` in the ENCODE expression.

## Encode Validation

Encoding runs **before** constraint checks (NOT NULL, type affinity). If an encode function returns NULL for a NOT NULL or PRIMARY KEY column, the insert is rejected:

```sql
CREATE TYPE my_uuid BASE text ENCODE uuid_blob(value) DECODE uuid_str(value);
CREATE TABLE t1(id my_uuid PRIMARY KEY, name TEXT) STRICT;
INSERT INTO t1 VALUES ('invalid-uuid', 'bad');
-- Error: NOT NULL constraint failed (uuid_blob returned NULL)
```

## CHECK Constraints

In STRICT tables, CHECK constraint comparisons are type-checked at table creation time. A custom type column cannot be directly compared to a raw literal — the types must match. Use `CAST` to convert literals to the custom type:

```sql
-- ERROR: type mismatch in CHECK constraint (cents vs INTEGER)
CREATE TABLE t1(amount cents CHECK(amount < 50)) STRICT;

-- OK: CAST converts the literal to cents, both sides have the same type
CREATE TABLE t1(amount cents CHECK(amount < CAST(50 AS cents))) STRICT;
```

This rule applies to all comparisons in STRICT tables, not just custom types:

```sql
-- ERROR: type mismatch (INTEGER vs TEXT)
CREATE TABLE t1(age INTEGER CHECK(age < 'old')) STRICT;

-- OK: same types
CREATE TABLE t1(age INTEGER CHECK(age >= 18)) STRICT;
```

Function calls in CHECK expressions also require CAST, because the return type cannot be determined at table creation time:

```sql
-- ERROR: cannot determine return type of length()
CREATE TABLE t1(name TEXT CHECK(length(name) < 10)) STRICT;

-- OK: CAST makes the type explicit
CREATE TABLE t1(name TEXT CHECK(CAST(length(name) AS INTEGER) < 10)) STRICT;
```

## NULL Handling

NULL values bypass encoding and decoding entirely:

```sql
CREATE TYPE uint BASE text
    ENCODE test_uint_encode(value)
    DECODE test_uint_decode(value);
CREATE TABLE t1(val uint) STRICT;
INSERT INTO t1 VALUES (NULL);
SELECT COALESCE(val, 'IS_NULL') FROM t1;
-- IS_NULL
```

## CAST Support

You can cast values to a custom type, which applies the encode function:

```sql
CREATE TYPE reversed BASE text
    ENCODE string_reverse(value)
    DECODE string_reverse(value);
SELECT CAST('hello' AS reversed);
-- olleh
```

## Inspecting Types

### PRAGMA list_types

List all available types (built-in and custom) with their metadata:

```sql
PRAGMA list_types;
-- type      | parent | encode                | decode                | default | operators
-- INTEGER   |        |                       |                       |         |
-- REAL      |        |                       |                       |         |
-- TEXT      |        |                       |                       |         |
-- BLOB      |        |                       |                       |         |
-- ANY       |        |                       |                       |         |
-- uint      | text   | test_uint_encode(...) | test_uint_decode(...) | 0       | +(uint) -> test_uint_add
```

### sqlite_turso_types

All types (built-in and user-defined) are available through the `sqlite_turso_types` virtual table:

```sql
SELECT name, sql FROM sqlite_turso_types;
```

## Using with ALTER TABLE

Custom types work with `ALTER TABLE ADD COLUMN`:

```sql
CREATE TYPE uint BASE text
    ENCODE test_uint_encode(value)
    DECODE test_uint_decode(value);
CREATE TABLE t1(id INTEGER PRIMARY KEY) STRICT;
ALTER TABLE t1 ADD COLUMN val uint;
INSERT INTO t1 VALUES (1, 42);
SELECT id, val FROM t1;
-- 1|42
```

## Restrictions

- Custom types require **STRICT** tables and the `--experimental-strict` flag.
- A type cannot be dropped while any table column uses it.
- `CREATE TYPE IF NOT EXISTS` silently succeeds if the type already exists.
- Encode/decode expressions use the identifier `value` to reference the input.
