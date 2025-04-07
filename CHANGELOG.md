# Changelog

## 0.0.18 - 2025-04-02

### Added

* Jsonb support update (Ihor Andrianov)
* Add BTree balancing after `delete` (Krishna Vishal)
* Introduce Register struct (Pere Diaz Bou)
* Introduce immutable record (Pere Diaz Bou)
* Introduce libFuzzer (Levy A.)
* WAL frame checksum support (Daniel Boll)
* Initial JavaScript bindings with napi-rs (Pekka Enberg)
* Initial pass at `UPDATE` support (Preston Thorpe)
* Add `commit()` and placeholding insert to Python binding (Diego Reis)

### Updated

* Create plan for Update queries (Preston Thorpe)
* Validate cells inside a page after each operation (Pere Diaz Bou)
* Refactor Cli Repl Commands to use clap (Pedro Muniz)
* Allow balance_root to balance with interior pages (Pere Diaz Bou)
* Let remainder (%) accept textual arguments (Anton Harniakou)
* JSON code cleanups (Pekka Enberg)
* Allocation improvements with ImmutableRecord, OwnedRecord and read_record (Pere Diaz Bou)
* JavaScript binding improvements (Pekka Enberg)
* Kill test environment (Pekka Enberg)
* Remove public unlock method from `SpinLock` to prevent unsafe aliasing (Krishna Vishal)
* Handle limit zero case in query plan emitter (Preston Thorpe)
* Reduce MVCC cursor memory consumption (Ihor Andrianov)
* Unary `+` is a noop (Levy A.)
* JSON cache (Ihor Andrianov)
* Bump `rusqlite` to 0.34 (Pere Diaz Bou)
* core: Rename FileStorage to DatabaseFile (Pekka Enberg)
* Improve Python bindings (Diego Reis)
* Schema translation cleanups (Pekka Enberg)
* Various JSON improvements (Ihor Andrianov)
* Enable pretty mode in shell by default (Pekka Enberg)
* Improve CLI color scheme (Pekka Enberg)
* Impl Copy on some types in the pager to prevent explicit clones (Preston Thorpe)
* Syntax highlighting and hinting (Pedro Muniz)
* chore: gitignore files with an extension *.db (Anton Harniakou)
* Organize extension library and feature gate VFS (Preston Thorpe)
* fragment bench functions (Pere Diaz Bou)

### Fixed

* Remove unnecessary balance code that crashes (Pere Diaz Bou)
* Fix propagation of divider cell balancing interior page (Pere Diaz Bou)
* Fuzz test btree fix seeking. (Pere Diaz Bou)
* Fix IdxCmp insn comparisons (Jussi Saurio)
* Fixes probably all floating point math issues and floating point display issues. (Ihor Andrianov)
* Make BTreeCell/read_payload  not allocate any data + overflow fixes (Pere Diaz Bou)
* Fix `compute_shl` negate with overflow (Krishna Vishal)
* Fix a typo in README.md (Tshepang Mbambo)
* Fix platform specific FFI C pointer type casts (Preston Thorpe)
* core: Fix Destroy opcode root page handling (Pekka Enberg)
* Fix `SELECT 0.0 = 0` returning false (lgualtieri75)
* bindings/python: Fix flaky tests (Diego Reis)
* Fix io_uring WAL write corruption by ensuring buffer lifetime (Daniel Boll)

## 0.0.17 - 2025-03-19

### Added

* `BEGIN DEFERRED` support (Diego Reis)
* Experimental MVCC integration (Pekka Enberg)
* `DROP TABLE` support (Zaid Humayun)
* Initial pass on Virtual FileSystem extension module (Preston Thorpe)
* JSONB support (Ihor Andrianov)
* Shell command completion (Pedro Muniz)

### Updated

### Fixed

* Fixes and improvements to Rust bindings (yirt grek and 南宫茜)
* Transaction management fixes (Pere Diaz Bou and Diego Reis)
* JSON function fixes (Ihor Andrianov)


## 0.0.16 - 2025-03-05

### Added

* Virtual table support (Preston Thorpe)
* Improvements to Java bindings (Kim Seon Woo)
* Improvements to Rust bindings (Pekka Enberg)
* Add sqlean ipaddr extension (EmNudge)
* Add "dump" and "load" to the help menu (EmNudge)
* Initial Antithesis testing tool (Pekka Enberg)

### Fixed

* SQLite B-Tree balancing algorithm (Pere Diaz Bou)
* B-Tree improves and fixes (Pere Diaz Bou and Perston Thorpe)
* Display blobs as blob literals in `.dump` (from Mohamed Hossam)
* Fix wrong count() result if the column specified contains a NULL (lgualtieri75)
* Fix casting text to integer to match SQLite' (Preston Thorpe)
* Improve `SELECT 1` performance to be on par with SQLite (Pekka Enberg)
* Fix offset_sec normalization in extensions/time (meteorgan)
* Handle parsing URI according to SQLite specification (Preston Thorpe)
* Escape character is ignored in LIKE function (lgualtieri75)
* Fix cast_text_to_number compatibility (Pedro Muniz)
* Modify the LIKE function to work with all types (Mohamed Hossam)

## 0.0.15 - 2025-02-18

### Added

**Core:**

* Initial pass on virtual tables (Preston Thorpe)
* Import MVCC code to the source tree -- not enabled (Pekka Enberg, Piotr Sarna, Avinash Sajjanshetty)
* Implement `json_set` (Marcus Nilsson)
* Initial support for WITH clauses (common table expressions) (Jussi Saurio)
* `BEGIN IMMEDIATE` + `COMMIT` support (Pekka Enberg)
* `BEGIN EXCLUSIVE` support (Pekka Enberg)
* Add Printf Support (Zaid Humayun)
* Add support for `delete` row (Krishna Vishal)
* Implement json_quote (Pedro Muniz)
* Add read implementation of user_version pragma with ReadCookie opcode (Jonathan Webb)
* Json path refine (Ihor Andrianov)
* cli: Basic dump support (Glauber Costa)
* Support numeric column references in GROUP BY (Jussi Saurio)
* Implement the legacy_file_format pragma (Glauber Costa)
* Added IdxLE and IdxLT opcodes (Omolola Olamide)

**Java bindings:*

* Improve JDBC support with, for example, prepared statements (Kim Seon Woo)
* Rename package name `tech.turso` (Kim Seon Woo)

**Extensions:**

* Sqlean Crypto extension (Diego Reis)
* Sqlean Time extension (Pedro Muniz)
* Add support for `regexp_replace()` (lgualtieri75)

**Simulator:**

* Add NoREC testing property (Alperen Keleş)
* Add `--differential` mode against SQLite (Alperen Keleş)

### Fixed

**Core:**

* Fix 24/48 bit width serial types parsing (Nikita Sivukhin)
* Fix substr (Nikita Sivukhin)
* Fix math binary (Nikita Sivukhin)
* Fix and predicate (Nikita Sivukhin)
* Fix IdxGt, IdxGe, IdxLt, and IdxLe instructions (Jussi Saurio)
* Fix not evaling constant conditions when no tables in query (Jussi Saurio)
* Fix remainder panic on zero right-hand-side (Jussi Saurio)
* Fix invalid text columns generated by dump (Kingsley Yung)
* Fix incorrect CAST text->numeric if valid prefix is 1 char long (Jussi Saurio)
* Improve SQL statement prepare performance (Jussi Saurio)
* Fix VCC write conflict handling (Jussi Saurio)
* Fix various bugs in B-Tree handling (Nikita Sivukhin)
* Fix case and emit (Nikita Sivukhin)
* Fix coalesce (Nikita Sivukhin)
* Fix cast (Nikita Sivukhin)
* Fix string funcs (Nikita Sivukhin)
* Fix floating point truncation in JSON #877 (lgualtieri75)
* Fix bug with `SELECT` referring to a mixed-case alias (Jussi Saurio)

## 0.0.14 - 2025-02-04

### Added

**Core:**

* Improve changes() and total_changes() functions and add tests (Ben Li)
* Add support for `json_object` function (Jorge Hermo)
* Implemented json_valid function (Harin)
* Implement Not (Vrishabh)
* Initial support for wal_checkpoint pragma (Sonny)
* Implement Or and And bytecodes (Diego Reis)
* Implement strftime function (Pedro Muniz)
* implement sqlite_source_id function (Glauber Costa)
* json_patch() function implementation (Ihor Andrianov)
* json_remove() function implementation (Ihor Andrianov)
* Implement isnull / not null for filter expressions (Glauber Costa)
* Add support for offset in select queries (Ben Li)
* Support returning column names from prepared statement (Preston Thorpe)
* Implement Concat opcode (Harin)
* Table info (Glauber Costa)
* Pragma list (Glauber Costa)
* Implement Noop bytecode (Pedro Muniz)
* implement is and is not where constraints (Glauber Costa)
* Pagecount (Glauber Costa)
* Support column aliases in GROUP BY, ORDER BY and HAVING (Jussi Saurio)
* Implement json_pretty (Pedro Muniz)

**Extensions:**

* Initial pass on vector extension (Pekka Enberg)
* Enable static linking for 'built-in' extensions (Preston Thorpe)

**Go Bindings:**

* Initial support for Go database/sql driver (Preston Thorpe)
* Avoid potentially expensive operations on prepare' (Glauber Costa)

**Java Bindings:**

* Implement JDBC `ResultSet` (Kim Seon Woo)
* Implement LimboConnection `close()` (Kim Seon Woo)
* Implement close() for `LimboStatement` and `LimboResultSet` (Kim Seon Woo)
* Implement methods in `JDBC4ResultSet` (Kim Seon Woo)
* Load native library from Jar (Kim Seon Woo)
* Change logger dependency (Kim Seon Woo)
* Log driver loading error (Pekka Enberg)

**Simulator:**

* Implement `--load` and `--watch` flags (Alperen Keleş)

**Build system and CI:**

* Add Nyrkiö change point detection to 'cargo bench' workflow (Henrik Ingo)

### Fixed

* Fix `select X'1';` causes limbo to go in infinite loop (Krishna Vishal)
* Fix rowid search codegen (Nikita Sivukhin)
* Fix logical codegen (Nikita Sivukhin)
* Fix parser panic when duplicate column names are given to `CREATE TABLE` (Krishna Vishal)
* Fix panic when double quoted strings are used for column names. (Krishna Vishal)
* Fix `SELECT -9223372036854775808` result differs from SQLite (Krishna Vishal)
* Fix `SELECT ABS(-9223372036854775808)` causes limbo to panic.  (Krishna Vishal)
* Fix memory leaks, make extension types more efficient (Preston Thorpe)
* Fix table with single column PRIMARY KEY to not create extra btree (Krishna Vishal)
* Fix null cmp codegen (Nikita Sivukhin)
* Fix null expr codegen (Nikita Sivukhin)
* Fix rowid generation (Nikita Sivukhin)
* Fix shr instruction (Nikita Sivukhin)
* Fix strftime function compatibility problems (Pedro Muniz)
* Dont fsync the WAL on read queries (Jussi Saurio)

## 0.0.13 - 2025-01-19

### Added

* Initial support for native Limbo extensions (Preston Thorpe)
      
* npm packaging for node and web (Elijah Morgan)

* Add support for `rowid` keyword' (Kould)

* Add support for shift left, shift right, is and is not operators (Vrishabh)

* Add regexp extension (Vrishabh)
      
* Add counterexample minimization to simulator (Alperen Keleş)

* Initial support for binding values to prepared statements (Levy A.)

### Updated

* Java binding improvements (Kim Seon Woo)

* Reduce `liblimbo_sqlite3.a` size' (Pekka Enberg)

### Fixed

* Fix panics on invalid aggregate function arguments (Krishna Vishal)
 
* Fix null compare operations not giving null (Vrishabh)

* Run all statements from SQL argument in CLI (Vrishabh)

* Fix MustBeInt opcode semantics (Vrishabh)

* Fix recursive binary operation logic (Jussi Saurio)

* Fix SQL comment parsing in Limbo shell (Diego Reis and Clyde)

## 0.0.12 - 2025-01-14

### Added

**Core:**

* Improve JSON function support (Kacper Madej, Peter Sooley)

* Support nested parenthesized conditional expressions (Preston Thorpe)

* Add support for changes() and total_changes() functions (Lemon-Peppermint)

* Auto-create index in CREATE TABLE when necessary (Jussi Saurio)

* Add partial support for datetime() function (Preston Thorpe)

* SQL parser performance improvements (Jussi Saurio)

**Shell:**

* Show pretty parse errors in the shell (Samyak Sarnayak)

* Add CSV import support to shell (Vrishabh)

* Selectable IO backend with --io={syscall,io-uring} argument (Jorge López Tello)

**Bindings:**

* Initial version of Java bindings (Kim Seon Woo)

* Initial version of Rust bindings (Pekka Enberg)

* Add OPFS support to Wasm bindings (Elijah Morgan)

* Support uncorrelated FROM clause subqueries (Jussi Saurio)

* In-memory support to `sqlite3_open()` (Pekka Enberg)

### Fixed

* Make iterate() lazy in JavaScript bindings (Diego Reis)

* Fix integer overflow output to be same as sqlite3 (Vrishabh)

* Fix 8-bit serial type to encoding (Preston Thorpe)

* Query plan optimizer bug fixes (Jussi Saurio)

* B-Tree balancing fixes (Pere Diaz Bou)

* Fix index seek wrong on `SeekOp::LT`\`SeekOp::GT` (Kould)

* Fix arithmetic operations for text values' from Vrishabh

* Fix quote escape in SQL literals (Vrishabh)

## 0.0.11 - 2024-12-31

### Added

* Add in-memory mode to Python bindings (Jean Arhancet)

* Add json_array_length function (Peter Sooley)

* Add support for the UUID extension (Preston Thorpe)

### Changed

* Enable sqpoll by default in io_uring (Preston Thorpe)

* Simulator improvements (Alperen Keleş)

### Fixed

* Fix escaping issues with like and glob functions (Vrishabh)

* Fix `sqlite_version()` out of bound panics' (Diego Reis)

* Fix on-disk file format bugs (Jussi Saurio)

## 0.0.10 - 2024-12-18

### Added

* In-memory mode (Preston Thorpe)

* More CLI improvements (Preston Thorpe)

* Add support for replace() function (Alperen Keleş)

* Unary operator improvements (Jean Arhancet)

* Add support for unex(x, y) function (Kacper Kołodziej)

### Fixed

* Fix primary key handling when there's rowid and PK is not alias (Jussi Saurio)

## 0.0.9 - 2024-12-12

### Added

* Improve CLI (Preston Thorpe)

* Add support for iif() function (Alex Miller)

* Add support for last_insert_rowid() function (Krishna Vishal)

* Add support JOIN USING and NATURAL JOIN (Jussi Saurio)

* Add support for more scalar functions (Kacper Kołodziej)

* Add support for `HAVING` clause (Jussi Saurio)

* Add `get()` and `iterate()` to JavaScript/Wasm API (Jean Arhancet)

## 0.0.8 - 2024-11-20

### Added

* Python package build and example usage (Pekka Enberg)

## 0.0.7 - 2024-11-20

### Added

* Minor improvements to JavaScript API (Pekka Enberg)
* `CAST` support (Jussi Saurio)

### Fixed

* Fix issues found in-btree code with the DST (Pere Diaz Bou)

## 0.0.6 - 2024-11-18

### Fixed

- Fix database truncation caused by `limbo-wasm` opening file in wrong mode (Pere Diaz Bou)

## 0.0.5 - 2024-11-18

### Added

- `CREATE TABLE` support (Pere Diaz Bou)

- Add Add Database.prepare() and Statement.all() to Wasm bindings (Pekka Enberg)

- WAL improvements (Pere Diaz Bou)

- Primary key index scans and single-column secondary index scans (Jussi Saurio)

- `GROUP BY` support (Jussi Saurio)

- Overflow page support (Pere Diaz Bou)

- Improvements to Python bindings (Jean Arhancet and Lauri Virtanen)

- Improve scalar function support (Lauri Virtanen)

### Fixed

- Panic in codegen with `COUNT(*)` (Jussi Saurio)

- Fix `LIKE` to be case insensitive (RJ Barman)

## 0.0.4 - 2024-08-22

- Query planner rewrite (Jussi Saurio)

- Initial pass on Python bindings (Jean Arhancet)

- Improve scalar function support (Kim Seon Woo and Jean Arhancet)

### Added

- Partial support for `json()` function (Jean Arhancet)

## 0.0.3 - 2024-08-01

### Added

- Initial pass on the write path. Note that the write path is not transactional yet. (Pere Diaz Bou)

- More scalar functions: `unicode()` (Ethan Niser)

- Optimize point queries with integer keys (Jussi Saurio)

### Fixed

- `ORDER BY` support for nullable sorting columns and qualified identifiers (Jussi Saurio)

- Fix `.schema` command crash in the CLI ([#212](https://github.com/tursodatabase/limbo/issues/212) (Jussi Saurio)

## 0.0.2 - 2024-07-24

### Added

- Partial `LEFT JOIN` support.

- Partial `ORDER BY` support.

- Partial scalar function support.

### Fixed

- Lock database file with POSIX filesystem advisory lock when database
  is opened to prevent concurrent processes from corrupting a file.
  Please note that the locking scheme differs from SQLite, which uses
  POSIX advisory locks for every transaction. We're defaulting to
  locking on open because it's faster. (Issue #94)

### Changed

- Install to `~/.limbo/` instead of `CARGO_HOME`.

## 0.0.1 - 2024-07-17

### Added

- Partial `SELECT` statement support, including `WHERE`, `LIKE`,
  `LIMIT`, `CROSS JOIN`, and `INNER JOIN`.

- Aggregate function support.

- `EXPLAIN` statement support.

- Partial `PRAGMA` statement support, including `cache_size`.

- Asynchronous I/O support with Linux io_uring using direct I/O and
  Darwin kqueue.

- Initial pass on command line shell with following commands:
