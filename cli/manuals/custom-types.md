---
display_name: "custom types"
---

# Custom Types

## Overview

Turso extends SQLite's STRICT table type system with user-defined custom types. Custom types let you define how values are encoded before storage and decoded when read, enforce domain constraints at the storage layer, attach operators, and provide defaults — all declared in pure SQL.

Custom types work only with **STRICT** tables.

## Creating a Type

```sql
CREATE TYPE type_name BASE base_type
    ENCODE encode_expr
    DECODE decode_expr
    [OPERATOR 'op' (operand_type) -> function_name ...]
    [DEFAULT default_expr];
```

- **BASE** — The underlying SQLite storage type (`text`, `integer`, `real`, `blob`).
- **ENCODE** — Expression applied to `value` before writing to disk.
- **DECODE** — Expression applied to `value` when reading from disk.
- **OPERATOR** — (Optional) Custom operator overloads for the type.
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
    ENCODE test_reverse_encode(value)
    DECODE test_reverse_decode(value);
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
    ENCODE test_reverse_encode(value)
    DECODE test_reverse_decode(value)
    DEFAULT test_reverse_encode('auto');

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
    ENCODE test_reverse_encode(value)
    DECODE test_reverse_decode(value);
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

- Custom types require **STRICT** tables.
- A type cannot be dropped while any table column uses it.
- `CREATE TYPE IF NOT EXISTS` silently succeeds if the type already exists.
- Encode/decode expressions use the identifier `value` to reference the input.
