# Turso compatibility with SQLite

This document describes the compatibility of Turso with SQLite.

## Table of contents

- [Turso compatibility with SQLite](#turso-compatibility-with-sqlite)
  - [Table of contents](#table-of-contents)
  - [Overview](#overview)
    - [Features](#features)
    - [Limitations](#limitations)
  - [SQLite query language](#sqlite-query-language)
    - [Statements](#statements)
      - [PRAGMA](#pragma)
    - [Expressions](#expressions)
    - [SQL functions](#sql-functions)
      - [Scalar functions](#scalar-functions)
      - [Mathematical functions](#mathematical-functions)
      - [Aggregate functions](#aggregate-functions)
      - [Date and time functions](#date-and-time-functions)
      - [JSON functions](#json-functions)
  - [SQLite C API](#sqlite-c-api)
  - [SQLite VDBE opcodes](#sqlite-vdbe-opcodes)
  - [SQLite journaling modes](#sqlite-journaling-modes)
  - [Extensions](#extensions)
    - [UUID](#uuid)
    - [regexp](#regexp)
    - [Vector](#vector)
    - [Time](#time)

## Overview

Turso aims to be fully compatible with SQLite, with opt-in features not supported by SQLite.

### Features

* ✅ SQLite file format is fully supported
* 🚧 SQLite query language [[status](#sqlite-query-language)] is partially supported
* 🚧 SQLite C API [[status](#sqlite-c-api)] is partially supported

### Limitations

* ⛔️ Concurrent access from multiple processes is not supported.
* ⛔️ Savepoints are not supported.
* ⛔️ Triggers are not supported.
* ⛔️ Vacuum is not supported.

## SQLite query language

### Statements

| Statement                 | Status  | Comment                                                                           |
|---------------------------|---------|-----------------------------------------------------------------------------------|
| ALTER TABLE               | Yes     |                                                                                   |
| ANALYZE                   | Yes     |                                                                                   |
| ATTACH DATABASE           | Partial | Only for reads. All modifications will currently fail to find the table           |
| BEGIN TRANSACTION         | Partial | Transaction names are not supported.                                              |
| COMMIT TRANSACTION        | Partial | Transaction names are not supported.                                              |
| CREATE INDEX              | Yes     |                                                                                   |
| CREATE TABLE              | Yes     |                                                                                   |
| CREATE TABLE ... STRICT   | Partial | Strict schema mode is experimental.                                               |
| CREATE TRIGGER            | Yes     |                                                                                   |
| CREATE VIEW               | Yes     |                                                                                   |
| CREATE VIRTUAL TABLE      | Yes     |                                                                                   |
| DELETE                    | Yes     |                                                                                   |
| DETACH DATABASE           | Yes     |                                                                                   |
| DROP INDEX                | Partial | Disabled by default.                                                              |
| DROP TABLE                | Yes     |                                                                                   |
| DROP TRIGGER              | Yes     |                                                                                   |
| DROP VIEW                 | Yes     |                                                                                   |
| END TRANSACTION           | Partial | Alias for `COMMIT TRANSACTION`                                                    |
| EXPLAIN                   | Yes     |                                                                                   |
| INDEXED BY                | No      |                                                                                   |
| INSERT                    | Yes     |                                                                                   |
| ON CONFLICT clause        | Yes     |                                                                                   |
| REINDEX                   | No      |                                                                                   |
| RELEASE SAVEPOINT         | No      |                                                                                   |
| REPLACE                   | Yes     |                                                                                   |
| RETURNING clause          | Yes     |                                                                                   |
| ROLLBACK TRANSACTION      | Yes     |                                                                                   |
| SAVEPOINT                 | No      |                                                                                   |
| SELECT                    | Yes     |                                                                                   |
| SELECT ... WHERE          | Yes     |                                                                                   |
| SELECT ... WHERE ... LIKE | Yes     |                                                                                   |
| SELECT ... LIMIT          | Yes     |                                                                                   |
| SELECT ... ORDER BY       | Yes     |                                                                                   |
| SELECT ... GROUP BY       | Yes     |                                                                                   |
| SELECT ... HAVING         | Yes     |                                                                                   |
| SELECT ... JOIN           | Yes     |                                                                                   |
| SELECT ... CROSS JOIN     | Yes     | SQLite CROSS JOIN means "do not reorder joins". We don't support that yet anyway. |
| SELECT ... INNER JOIN     | Yes     |                                                                                   |
| SELECT ... OUTER JOIN     | Partial | no RIGHT JOIN                                                                     |
| SELECT ... JOIN USING     | Yes     |                                                                                   |
| SELECT ... NATURAL JOIN   | Yes     |                                                                                   |
| UPDATE                    | Yes     |                                                                                   |
| VACUUM                    | No      |                                                                                   |
| WITH clause               | Partial | No RECURSIVE, no MATERIALIZED, only SELECT supported in CTEs                      |
| WINDOW functions             | Partial | only default frame definition, no window-specific functions (rank() etc)         |

#### [PRAGMA](https://www.sqlite.org/pragma.html)


| Statement                        | Status     | Comment                                      |
|----------------------------------|------------|----------------------------------------------|
| PRAGMA analysis_limit            | No         |                                              |
| PRAGMA application_id            | Yes        |                                              |
| PRAGMA auto_vacuum               | No         |                                              |
| PRAGMA automatic_index           | No         |                                              |
| PRAGMA busy_timeout              | No         |                                              |
| PRAGMA busy_timeout              | No         |                                              |
| PRAGMA cache_size                | Yes        |                                              |
| PRAGMA cache_spill               | Partial    | Enabled/Disabled only                        |
| PRAGMA case_sensitive_like       | Not Needed | deprecated in SQLite                         |
| PRAGMA cell_size_check           | No         |                                              |
| PRAGMA checkpoint_fullsync       | No         |                                              |
| PRAGMA collation_list            | No         |                                              |
| PRAGMA compile_options           | No         |                                              |
| PRAGMA count_changes             | Not Needed | deprecated in SQLite                         |
| PRAGMA data_store_directory      | Not Needed | deprecated in SQLite                         |
| PRAGMA data_version              | No         |                                              |
| PRAGMA database_list             | Yes        |                                              |
| PRAGMA default_cache_size        | Not Needed | deprecated in SQLite                         |
| PRAGMA defer_foreign_keys        | No         |                                              |
| PRAGMA empty_result_callbacks    | Not Needed | deprecated in SQLite                         |
| PRAGMA encoding                  | Yes        |                                              |
| PRAGMA foreign_key_check         | No         |                                              |
| PRAGMA foreign_key_list          | No         |                                              |
| PRAGMA foreign_keys              | No         |                                              |
| PRAGMA freelist_count            | Yes        |                                              |
| PRAGMA full_column_names         | Not Needed | deprecated in SQLite                         |
| PRAGMA fullsync                  | No         |                                              |
| PRAGMA function_list             | No         |                                              |
| PRAGMA hard_heap_limit           | No         |                                              |
| PRAGMA ignore_check_constraints  | No         |                                              |
| PRAGMA incremental_vacuum        | No         |                                              |
| PRAGMA index_info                | No         |                                              |
| PRAGMA index_list                | No         |                                              |
| PRAGMA index_xinfo               | No         |                                              |
| PRAGMA integrity_check           | Yes        |                                              |
| PRAGMA journal_mode              | Yes        |                                              |
| PRAGMA journal_size_limit        | No         |                                              |
| PRAGMA legacy_alter_table        | No         |                                              |
| PRAGMA legacy_file_format        | Yes        |                                              |
| PRAGMA locking_mode              | No         |                                              |
| PRAGMA max_page_count            | Yes        |                                              |
| PRAGMA mmap_size                 | No         |                                              |
| PRAGMA module_list               | No         |                                              |
| PRAGMA optimize                  | No         |                                              |
| PRAGMA page_count                | Yes        |                                              |
| PRAGMA page_size                 | Yes        |                                              |
| PRAGMA parser_trace              | No         |                                              |
| PRAGMA pragma_list               | Yes        |                                              |
| PRAGMA query_only                | Yes        |                                              |
| PRAGMA quick_check               | Yes        |                                              |
| PRAGMA read_uncommitted          | No         |                                              |
| PRAGMA recursive_triggers        | No         |                                              |
| PRAGMA reverse_unordered_selects | No         |                                              |
| PRAGMA schema_version            | Yes        | For writes, emulate defensive mode (always noop)|
| PRAGMA secure_delete             | No         |                                              |
| PRAGMA short_column_names        | Not Needed | deprecated in SQLite                         |
| PRAGMA shrink_memory             | No         |                                              |
| PRAGMA soft_heap_limit           | No         |                                              |
| PRAGMA stats                     | No         | Used for testing in SQLite                   |
| PRAGMA synchronous               | Partial    | `OFF` and `FULL` supported                   |
| PRAGMA table_info                | Yes        |                                              |
| PRAGMA table_list                | No         |                                              |
| PRAGMA table_xinfo               | Yes        |                                              |
| PRAGMA temp_store                | No         |                                              |
| PRAGMA temp_store_directory      | Not Needed | deprecated in SQLite                         |
| PRAGMA threads                   | No         |                                              |
| PRAGMA trusted_schema            | No         |                                              |
| PRAGMA user_version              | Yes        |                                              |
| PRAGMA vdbe_addoptrace           | No         |                                              |
| PRAGMA vdbe_debug                | No         |                                              |
| PRAGMA vdbe_listing              | No         |                                              |
| PRAGMA vdbe_trace                | No         |                                              |
| PRAGMA wal_autocheckpoint        | No         |                                              |
| PRAGMA wal_checkpoint            | Partial    | Not Needed calling with param (pragma-value) |
| PRAGMA writable_schema           | No         |                                              |

### Expressions

Feature support of [sqlite expr syntax](https://www.sqlite.org/lang_expr.html).

| Syntax                    | Status  | Comment                                  |
|---------------------------|---------|------------------------------------------|
| literals                  | Yes     |                                          |
| schema.table.column       | Partial | Schemas aren't supported                 |
| unary operator            | Yes     |                                          |
| binary operator           | Partial | Only `%`, `!<`, and `!>` are unsupported |
| agg() FILTER (WHERE ...)  | No      | Is incorrectly ignored                   |
| ... OVER (...)            | No      | Is incorrectly ignored                   |
| (expr)                    | Yes     |                                          |
| CAST (expr AS type)       | Yes     |                                          |
| COLLATE                   | Partial | Custom Collations not supported          |
| (NOT) LIKE                | Yes     |                                          |
| (NOT) GLOB                | Yes     |                                          |
| (NOT) REGEXP              | No      |                                          |
| (NOT) MATCH               | No      |                                          |
| IS (NOT)                  | Yes     |                                          |
| IS (NOT) DISTINCT FROM    | Yes     |                                          |
| (NOT) BETWEEN ... AND ... | Yes     | Expression is rewritten in the optimizer |
| (NOT) IN (SELECT...)       | Yes      |                                          |
| (NOT) EXISTS (SELECT...)   | Yes      |                                          |
| x <operator> (SELECT...))   | Partial  | Only scalar subqueries supported, i.e. not (x,y) = (SELECT...)
| CASE WHEN THEN ELSE END   | Yes     |                                          |
| RAISE                     | No      |                                          |

### SQL functions

#### Scalar functions

| Function                     | Status  | Comment                                              |
|------------------------------|---------|------------------------------------------------------|
| abs(X)                       | Yes     |                                                      |
| changes()                    | Partial | Still need to support update statements and triggers |
| char(X1,X2,...,XN)           | Yes     |                                                      |
| coalesce(X,Y,...)            | Yes     |                                                      |
| concat(X,...)                | Yes     |                                                      |
| concat_ws(SEP,X,...)         | Yes     |                                                      |
| format(FORMAT,...)           | Yes     |                                                      |
| glob(X,Y)                    | Yes     |                                                      |
| hex(X)                       | Yes     |                                                      |
| ifnull(X,Y)                  | Yes     |                                                      |
| iif(X,Y,Z)                   | Yes     |                                                      |
| instr(X,Y)                   | Yes     |                                                      |
| last_insert_rowid()          | Yes     |                                                      |
| length(X)                    | Yes     |                                                      |
| like(X,Y)                    | Yes     |                                                      |
| like(X,Y,Z)                  | Yes     |                                                      |
| likelihood(X,Y)              | Yes     |                                                      |
| likely(X)                    | Yes     |                                                      |
| load_extension(X)            | Yes     | sqlite3 extensions not yet supported                 |
| load_extension(X,Y)          | No      |                                                      |
| lower(X)                     | Yes     |                                                      |
| ltrim(X)                     | Yes     |                                                      |
| ltrim(X,Y)                   | Yes     |                                                      |
| max(X,Y,...)                 | Yes     |                                                      |
| min(X,Y,...)                 | Yes     |                                                      |
| nullif(X,Y)                  | Yes     |                                                      |
| octet_length(X)              | Yes     |                                                      |
| printf(FORMAT,...)           | Yes     | Still need support additional modifiers              |
| quote(X)                     | Yes     |                                                      |
| random()                     | Yes     |                                                      |
| randomblob(N)                | Yes     |                                                      |
| replace(X,Y,Z)               | Yes     |                                                      |
| round(X)                     | Yes     |                                                      |
| round(X,Y)                   | Yes     |                                                      |
| rtrim(X)                     | Yes     |                                                      |
| rtrim(X,Y)                   | Yes     |                                                      |
| sign(X)                      | Yes     |                                                      |
| soundex(X)                   | Yes     |                                                      |
| sqlite_compileoption_get(N)  | No      |                                                      |
| sqlite_compileoption_used(X) | No      |                                                      |
| sqlite_offset(X)             | No      |                                                      |
| sqlite_source_id()           | Yes     |                                                      |
| sqlite_version()             | Yes     |                                                      |
| substr(X,Y,Z)                | Yes     |                                                      |
| substr(X,Y)                  | Yes     |                                                      |
| substring(X,Y,Z)             | Yes     |                                                      |
| substring(X,Y)               | Yes     |                                                      |
| total_changes()              | Partial | Still need to support update statements and triggers |
| trim(X)                      | Yes     |                                                      |
| trim(X,Y)                    | Yes     |                                                      |
| typeof(X)                    | Yes     |                                                      |
| unhex(X)                     | Yes     |                                                      |
| unhex(X,Y)                   | Yes     |                                                      |
| unicode(X)                   | Yes     |                                                      |
| unlikely(X)                  | Yes     |                                                      |
| upper(X)                     | Yes     |                                                      |
| zeroblob(N)                  | Yes     |                                                      |

#### Mathematical functions

| Function   | Status | Comment |
|------------|--------|---------|
| acos(X)    | Yes    |         |
| acosh(X)   | Yes    |         |
| asin(X)    | Yes    |         |
| asinh(X)   | Yes    |         |
| atan(X)    | Yes    |         |
| atan2(Y,X) | Yes    |         |
| atanh(X)   | Yes    |         |
| ceil(X)    | Yes    |         |
| ceiling(X) | Yes    |         |
| cos(X)     | Yes    |         |
| cosh(X)    | Yes    |         |
| degrees(X) | Yes    |         |
| exp(X)     | Yes    |         |
| floor(X)   | Yes    |         |
| ln(X)      | Yes    |         |
| log(B,X)   | Yes    |         |
| log(X)     | Yes    |         |
| log10(X)   | Yes    |         |
| log2(X)    | Yes    |         |
| mod(X,Y)   | Yes    |         |
| pi()       | Yes    |         |
| pow(X,Y)   | Yes    |         |
| power(X,Y) | Yes    |         |
| radians(X) | Yes    |         |
| sin(X)     | Yes    |         |
| sinh(X)    | Yes    |         |
| sqrt(X)    | Yes    |         |
| tan(X)     | Yes    |         |
| tanh(X)    | Yes    |         |
| trunc(X)   | Yes    |         |

#### Aggregate functions

| Function                     | Status  | Comment |
|------------------------------|---------|---------|
| avg(X)                       | Yes     |         |
| count()                      | Yes     |         |
| count(*)                     | Yes     |         |
| group_concat(X)              | Yes     |         |
| group_concat(X,Y)            | Yes     |         |
| string_agg(X,Y)              | Yes     |         |
| max(X)                       | Yes     |         |
| min(X)                       | Yes     |         |
| sum(X)                       | Yes     |         |
| total(X)                     | Yes     |         |

#### Date and time functions

| Function    | Status  | Comment                      |
|-------------|---------|------------------------------|
| date()      | Yes     | partially supports modifiers |
| time()      | Yes     | partially supports modifiers |
| datetime()  | Yes     | partially supports modifiers |
| julianday() | Yes     | partially support modifiers  |
| unixepoch() | Partial | does not support modifiers   |
| strftime()  | Yes     | partially supports modifiers |
| timediff()  | Yes     | partially supports modifiers |

Modifiers:

|  Modifier      | Status|  Comment                        |
|----------------|-------|---------------------------------|
| Days           | Yes 	 |                                 |
| Hours          | Yes	 |                                 |
| Minutes        | Yes	 |                                 |
| Seconds        | Yes	 |                                 |
| Months         | Yes	 |                                 |
| Years          | Yes	 |                                 |
| TimeOffset     | Yes	 |                                 |
| DateOffset	 | Yes   |                                 |
| DateTimeOffset | Yes   |                                 |
| Ceiling	     | Yes   |                                 |
| Floor          | No    |                                 |
| StartOfMonth	 | Yes	 |                                 |
| StartOfYear	 | Yes	 |                                 |
| StartOfDay	 | Yes	 |                                 |
| Weekday(N)	 | Yes   |                                 |
| Auto           | No    |                                 |
| UnixEpoch      | No    |                                 |
| JulianDay      | No    |                                 |
| Localtime      |Partial| requires fixes to avoid double conversions.|
| Utc            |Partial| requires fixes to avoid double conversions.|
| Subsec         | Yes   |                                  |

#### JSON functions

| Function                           | Status  | Comment                                                                                                                                      |
| ---------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| json(json)                         | Yes     |                                                                                                                                              |
| jsonb(json)                        | Yes     |                                                                                                                                              |
| json_array(value1,value2,...)      | Yes     |                                                                                                                                              |
| jsonb_array(value1,value2,...)     | Yes     |                                                                                                                                              |
| json_array_length(json)            | Yes     |                                                                                                                                              |
| json_array_length(json,path)       | Yes     |                                                                                                                                              |
| json_error_position(json)          | Yes     |                                                                                                                                              |
| json_extract(json,path,...)        | Yes     |                                                                                                                                              |
| jsonb_extract(json,path,...)       | Yes     |                                                                                                                                              |
| json -> path                       | Yes     |                                                                                                                                              |
| json ->> path                      | Yes     |                                                                                                                                              |
| json_insert(json,path,value,...)   | Yes     |                                                                                                                                              |
| jsonb_insert(json,path,value,...)  | Yes     |                                                                                                                                              |
| json_object(label1,value1,...)     | Yes     |                                                                                                                                              |
| jsonb_object(label1,value1,...)    | Yes     |                                                                                                                                              |
| json_patch(json1,json2)            | Yes     |                                                                                                                                              |
| jsonb_patch(json1,json2)           | Yes     |                                                                                                                                              |
| json_pretty(json)                  | Yes     |                                                                                                                                              |
| json_remove(json,path,...)         | Yes     |                                                                                                                                              |
| jsonb_remove(json,path,...)        | Yes     |                                                                                                                                              |
| json_replace(json,path,value,...)  | Yes     |                                                                                                                                              |
| jsonb_replace(json,path,value,...) | Yes     |                                                                                                                                              |
| json_set(json,path,value,...)      | Yes     |                                                                                                                                              |
| jsonb_set(json,path,value,...)     | Yes     |                                                                                                                                              |
| json_type(json)                    | Yes     |                                                                                                                                              |
| json_type(json,path)               | Yes     |                                                                                                                                              |
| json_valid(json)                   | Yes     |                                                                                                                                              |
| json_valid(json,flags)             | Yes     |                                                                                                                                              |
| json_quote(value)                  | Yes     |                                                                                                                                              |
| json_group_array(value)            | Yes     |                                                                                                                                              |
| jsonb_group_array(value)           | Yes     |                                                                                                                                              |
| json_group_object(label,value)     | Yes     |                                                                                                                                              |
| jsonb_group_object(name,value)     | Yes     |                                                                                                                                              |
| json_each(json)                    | Yes     |                                                                                                                                              |
| json_each(json,path)               | Yes     |                                                                                                                                              |
| json_tree(json)                    | Partial | see commented-out tests in json.test                                                                                                         |
| json_tree(json,path)               | Partial | see commented-out tests in json.test                                                                                                         |

## SQLite C API

| Interface           | Status  | Comment |
|---------------------|---------|---------|
| sqlite3_open        | Partial |         |
| sqlite3_close       | Yes     |         |
| sqlite3_prepare     | Partial |         |
| sqlite3_finalize    | Yes     |         |
| sqlite3_step        | Yes     |         |
| sqlite3_column_text | Yes     |         |

## SQLite VDBE opcodes

| Opcode         | Status | Comment |
|----------------|--------|---------|
| Add            | Yes    |         |
| AddImm         | Yes    |         |
| Affinity       | Yes    |         |
| AggFinal       | Yes    |         |
| AggStep        | Yes    |         |
| AggValue       | Yes    |         |
| And            | Yes    |         |
| AutoCommit     | Yes    |         |
| BitAnd         | Yes    |         |
| BitNot         | Yes    |         |
| BitOr          | Yes    |         |
| Blob           | Yes    |         |
| BeginSubrtn    | Yes    |         |
| Cast           | Yes    |         |
| Checkpoint     | Yes    |         |
| Clear          | No     |         |
| Close          | Yes    |         |
| CollSeq        | Yes    |         |
| Column         | Yes    |         |
| Compare        | Yes    |         |
| Concat         | Yes    |         |
| Copy           | Yes    |         |
| Count          | Yes    |         |
| CreateBTree    | Partial| no temp databases |
| DecrJumpZero   | Yes    |         |
| Delete         | Yes    |         |
| Destroy        | Yes    |         |
| Divide         | Yes    |         |
| DropIndex      | Yes    |         |
| DropTable      | Yes    |         |
| DropTrigger    | No     |         |
| EndCoroutine   | Yes    |         |
| Eq             | Yes    |         |
| Expire         | No     |         |
| Explain        | No     |         |
| FkCounter      | Yes    |         |
| FkIfZero       | Yes    |         |
| Found          | Yes    |         |
| Filter         | Yes    |         |
| FilterAdd      | Yes    |         |
| Function       | Yes    |         |
| Ge             | Yes    |         |
| Gosub          | Yes    |         |
| Goto           | Yes    |         |
| Gt             | Yes    |         |
| Halt           | Yes    |         |
| HaltIfNull     | Yes    |         |
| IdxDelete      | Yes    |         |
| IdxGE          | Yes    |         |
| IdxInsert      | Yes    |         |
| IdxLE          | Yes    |         |
| IdxLT          | Yes    |         |
| IdxRowid       | Yes    |         |
| If             | Yes    |         |
| IfNeg          | No     |         |
| IfNot          | Yes    |         |
| IfPos          | Yes    |         |
| IfZero         | No     |         |
| IncrVacuum     | No     |         |
| Init           | Yes    |         |
| InitCoroutine  | Yes    |         |
| Insert         | Yes    |         |
| Int64          | Yes    |         |
| Integer        | Yes    |         |
| IntegrityCk    | Yes    |         |
| IsNull         | Yes    |         |
| IsUnique       | No     |         |
| JournalMode    | Yes    |         |
| Jump           | Yes    |         |
| Last           | Yes    |         |
| Le             | Yes    |         |
| LoadAnalysis   | No     |         |
| Lt             | Yes    |         |
| MakeRecord     | Yes    |         |
| MaxPgcnt       | Yes    |         |
| MemMax         | Yes     |         |
| Move           | Yes    |         |
| Multiply       | Yes    |         |
| MustBeInt      | Yes    |         |
| Ne             | Yes    |         |
| NewRowid       | Yes    |         |
| Next           | Yes     |         |
| Noop           | Yes     |         |
| Not            | Yes    |         |
| NotExists      | Yes    |         |
| NotFound       | Yes    |         |
| NotNull        | Yes    |         |
| Null           | Yes    |         |
| NullRow        | Yes    |         |
| Once           | Yes     |         |
| OpenAutoindex  | Yes     |         |
| OpenDup        | Yes     |         |
| OpenEphemeral  | Yes     |         |
| OpenPseudo     | Yes    |         |
| OpenRead       | Yes    |         |
| OpenWrite      | Yes     |         |
| Or             | Yes    |         |
| Pagecount      | Partial| no temp databases |
| Param          | No     |         |
| ParseSchema    | Yes    |         |
| Permutation    | No     |         |
| Prev           | Yes     |         |
| Program        | No     |         |
| ReadCookie     | Partial| no temp databases, only user_version supported |
| Real           | Yes    |         |
| RealAffinity   | Yes    |         |
| Remainder      | Yes    |         |
| ResetCount     | No     |         |
| ResetSorter    | Partial| sorter cursors are not supported yet; only ephemeral tables are |
| ResultRow      | Yes    |         |
| Return         | Yes    |         |
| Rewind         | Yes    |         |
| RowData        | Yes     |         |
| RowId          | Yes    |         |
| RowKey         | No     |         |
| RowSetAdd      | No     |         |
| RowSetRead     | No     |         |
| RowSetTest     | No     |         |
| Rowid          | Yes    |         |
| SCopy          | No     |         |
| Savepoint      | No     |         |
| Seek           | No     |         |
| SeekGe         | Yes    |         |
| SeekGt         | Yes    |         |
| SeekLe         | Yes    |         |
| SeekLt         | Yes    |         |
| SeekRowid      | Yes    |         |
| SeekEnd        | Yes    |         |
| Sequence       | Yes    |         |
| SequenceTest   | Yes    |         |
| SetCookie      | Yes    |         |
| ShiftLeft      | Yes    |         |
| ShiftRight     | Yes    |         |
| SoftNull       | Yes    |         |
| Sort           | No     |         |
| SorterCompare  | No     |         |
| SorterData     | Yes    |         |
| SorterInsert   | Yes    |         |
| SorterNext     | Yes    |         |
| SorterOpen     | Yes    |         |
| SorterSort     | Yes    |         |
| String         | NotNeeded | SQLite uses String for sized strings and String8 for null-terminated. All our strings are sized |
| String8        | Yes    |         |
| Subtract       | Yes    |         |
| TableLock      | No     |         |
| Trace          | No     |         |
| Transaction    | Yes    |         |
| VBegin         | Yes    |         |
| VColumn        | Yes    |         |
| VCreate        | Yes    |         |
| VDestroy       | Yes    |         |
| VFilter        | Yes    |         |
| VNext          | Yes    |         |
| VOpen          | Yes    |         |
| VRename        | Yes    |         |
| VUpdate        | Yes    |         |
| Vacuum         | No     |         |
| Variable       | Yes    |         |
| Yield          | Yes    |         |
| ZeroOrNull     | Yes    |         |

##  [SQLite journaling modes](https://www.sqlite.org/pragma.html#pragma_journal_mode)

We currently don't have plan to support the rollback journal mode as it locks the database file during writes.
Therefore, all rollback-type modes (delete, truncate, persist, memory) are marked are `Not Needed` below.

| Journal mode | Status     | Comment                        |
|--------------|------------|--------------------------------|
| wal          | Yes        |                                |
| wal2         | No         | experimental feature in sqlite |
| delete       | Not Needed |                                |
| truncate     | Not Needed |                                |
| persist      | Not Needed |                                |
| memory       | Not Needed |                                |

##  Extensions

Turso has in-tree extensions.

### UUID

UUID's in Turso are `blobs` by default.

| Function              | Status | Comment                                                       |
|-----------------------|--------|---------------------------------------------------------------|
| uuid4()               | Yes    | UUID version 4                                                |
| uuid4_str()           | Yes    | UUID v4 string alias `gen_random_uuid()` for PG compatibility |
| uuid7(X?)             | Yes    | UUID version 7 (optional parameter for seconds since epoch)   |
| uuid7_timestamp_ms(X) | Yes    | Convert a UUID v7 to milliseconds since epoch                 |
| uuid_str(X)           | Yes    | Convert a valid UUID to string                                |
| uuid_blob(X)          | Yes    | Convert a valid UUID to blob                                  |

### regexp

The `regexp` extension is compatible with [sqlean-regexp](https://github.com/nalgeon/sqlean/blob/main/docs/regexp.md).

| Function                                       | Status | Comment |
|------------------------------------------------|--------|---------|
| regexp(pattern, source)                        | Yes    |         |
| regexp_like(source, pattern)                   | Yes    |         |
| regexp_substr(source, pattern)                 | Yes    |         |
| regexp_capture(source, pattern[, n])           | Yes    |         |
| regexp_replace(source, pattern, replacement)   | Yes    |         |

### Vector

The `vector` extension is compatible with libSQL native vector search.

| Function                                       | Status | Comment |
|------------------------------------------------|--------|---------|
| vector(x)                                      | Yes    |         |
| vector32(x)                                    | Yes    |         |
| vector64(x)                                    | Yes    |         |
| vector_extract(x)                              | Yes    |         |
| vector_distance_cos(x, y)                      | Yes    |         |
| vector_distance_l2(x, y)                              | Yes    |Euclidean distance|
| vector_concat(x, y)                            | Yes    |         |
| vector_slice(x, start_index, end_index)        | Yes    |         |

### Time

The `time` extension is compatible with [sqlean-time](https://github.com/nalgeon/sqlean/blob/main/docs/time.md).


| Function                                                            | Status | Comment |
| ------------------------------------------------------------------- | ------ |---------|
| time_now()                                                          | Yes    |         |
| time_date(year, month, day[, hour, min, sec[, nsec[, offset_sec]]]) | Yes    |         |
| time_get_year(t)                                                    | Yes    |         |
| time_get_month(t)                                                   | Yes    |         |
| time_get_day(t)                                                     | Yes    |         |
| time_get_hour(t)                                                    | Yes    |         |
| time_get_minute(t)                                                  | Yes    |         |
| time_get_second(t)                                                  | Yes    |         |
| time_get_nano(t)                                                    | Yes    |         |
| time_get_weekday(t)                                                 | Yes    |         |
| time_get_yearday(t)                                                 | Yes    |         |
| time_get_isoyear(t)                                                 | Yes    |         |
| time_get_isoweek(t)                                                 | Yes    |         |
| time_get(t, field)                                                  | Yes    |         |
| time_unix(sec[, nsec])                                              | Yes    |         |
| time_milli(msec)                                                    | Yes    |         |
| time_micro(usec)                                                    | Yes    |         |
| time_nano(nsec)                                                     | Yes    |         |
| time_to_unix(t)                                                     | Yes    |         |
| time_to_milli(t)                                                    | Yes    |         |
| time_to_micro(t)                                                    | Yes    |         |
| time_to_nano(t)                                                     | Yes    |         |
| time_after(t, u)                                                    | Yes    |         |
| time_before(t, u)                                                   | Yes    |         |
| time_compare(t, u)                                                  | Yes    |         |
| time_equal(t, u)                                                    | Yes    |         |
| time_add(t, d)                                                      | Yes    |         |
| time_add_date(t, years[, months[, days]])                           | Yes    |         |
| time_sub(t, u)                                                      | Yes    |         |
| time_since(t)                                                       | Yes    |         |
| time_until(t)                                                       | Yes    |         |
| time_trunc(t, field)                                                | Yes    |         |
| time_trunc(t, d)                                                    | Yes    |         |
| time_round(t, d)                                                    | Yes    |         |
| time_fmt_iso(t[, offset_sec])                                       | Yes    |         |
| time_fmt_datetime(t[, offset_sec])                                  | Yes    |         |
| time_fmt_date(t[, offset_sec])                                      | Yes    |         |
| time_fmt_time(t[, offset_sec])                                      | Yes    |         |
| time_parse(s)                                                       | Yes    |         |
| dur_ns()                                                            | Yes    |         |
| dur_us()                                                            | Yes    |         |
| dur_ms()                                                            | Yes    |         |
| dur_s()                                                             | Yes    |         |
| dur_m()                                                             | Yes    |         |
| dur_h()                                                             | Yes    |         |
