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
    - [Database Connection](#database-connection)
    - [Prepared Statements](#prepared-statements)
    - [Binding Parameters](#binding-parameters)
    - [Result Columns](#result-columns)
    - [Result Values](#result-values-sqlite3_value)
    - [Error Handling](#error-handling)
    - [Changes and Row IDs](#changes-and-row-ids)
    - [Memory Management](#memory-management)
    - [Callback Functions](#callback-functions)
    - [User-Defined Functions](#user-defined-functions)
    - [Collation Functions](#collation-functions)
    - [Backup API](#backup-api)
    - [BLOB I/O](#blob-io)
    - [WAL Functions](#wal-functions)
    - [Utility Functions](#utility-functions)
    - [Table Metadata](#table-metadata)
    - [Virtual Tables](#virtual-tables)
    - [Loadable Extensions](#loadable-extensions)
    - [Serialization](#serialization)
    - [Miscellaneous](#miscellaneous)
    - [Turso-specific Extensions](#turso-specific-extensions)
  - [SQLite VDBE opcodes](#sqlite-vdbe-opcodes)
  - [SQLite journaling modes](#sqlite-journaling-modes)
  - [Extensions](#extensions)
    - [UUID](#uuid)
    - [regexp](#regexp)
    - [Vector](#vector)
    - [Time](#time)
    - [Full-Text Search (FTS)](#full-text-search-fts)
    - [CSV](#csv)
    - [Percentile](#percentile)
    - [Table-Valued Functions](#table-valued-functions)
    - [Internal Virtual Tables](#internal-virtual-tables)

## Overview

Turso aims to be fully compatible with SQLite, with opt-in features not supported by SQLite.

### Features

* ✅ SQLite file format is fully supported
* 🚧 SQLite query language [[status](#sqlite-query-language)] is partially supported
* 🚧 SQLite C API [[status](#sqlite-c-api)] is partially supported

### Limitations

* ⛔️ Concurrent access from multiple processes is not supported.
* ⛔️ Savepoints are not supported.
* ⛔️ Vacuum is not supported.

## SQLite query language

### Statements

| Statement                 | Status  | Comment                                                                           |
|---------------------------|---------|-----------------------------------------------------------------------------------|
| ALTER TABLE               | ✅ Yes     |                                                                                   |
| ANALYZE                   | ✅ Yes     |                                                                                   |
| ATTACH DATABASE           | 🚧 Partial | Only for reads. All modifications will currently fail to find the table           |
| BEGIN TRANSACTION         | ✅ Yes     |                                                                                   |
| COMMIT TRANSACTION        | ✅ Yes     |                                                                                   |
| CHECK                     | ✅ Yes     |                                                                                   |
| CREATE INDEX              | ✅ Yes     |                                                                                   |
| CREATE TABLE              | ✅ Yes     |                                                                                   |
| CREATE TABLE ... STRICT   | 🚧 Partial | Strict schema mode is experimental.                                               |
| CREATE TRIGGER            | ✅ Yes     |                                                                                   |
| CREATE VIEW               | ✅ Yes     |                                                                                   |
| CREATE VIRTUAL TABLE      | ✅ Yes     |                                                                                   |
| DELETE                    | ✅ Yes     |                                                                                   |
| DETACH DATABASE           | ✅ Yes     |                                                                                   |
| DROP INDEX                | 🚧 Partial | Disabled by default.                                                              |
| DROP TABLE                | ✅ Yes     |                                                                                   |
| DROP TRIGGER              | ✅ Yes     |                                                                                   |
| DROP VIEW                 | ✅ Yes     |                                                                                   |
| END TRANSACTION           | ✅ Yes     |                                                                                   |
| EXPLAIN                   | ✅ Yes     |                                                                                   |
| INDEXED BY                | ❌ No      |                                                                                   |
| INSERT                    | ✅ Yes     |                                                                                   |
| INSERT ... ON CONFLICT (UPSERT) | ✅ Yes |                                                                                   |
| ON CONFLICT clause        | ✅ Yes     |                                                                                   |
| REINDEX                   | ❌ No      |                                                                                   |
| RELEASE SAVEPOINT         | ✅ No      |                                                                                   |
| REPLACE                   | ✅ Yes     |                                                                                   |
| RETURNING clause          | ✅ Yes     |                                                                                   |
| ROLLBACK TRANSACTION      | ✅ Yes     |                                                                                   |
| SAVEPOINT                 | ✅ No      |                                                                                   |
| SELECT                    | ✅ Yes     |                                                                                   |
| SELECT ... WHERE          | ✅ Yes     |                                                                                   |
| SELECT ... WHERE ... LIKE | ✅ Yes     |                                                                                   |
| SELECT ... LIMIT          | ✅ Yes     |                                                                                   |
| SELECT ... ORDER BY       | ✅ Yes     |                                                                                   |
| SELECT ... GROUP BY       | ✅ Yes     |                                                                                   |
| SELECT ... HAVING         | ✅ Yes     |                                                                                   |
| SELECT ... JOIN           | ✅ Yes     |                                                                                   |
| SELECT ... CROSS JOIN     | ❌ No     | SQLite CROSS JOIN means "do not reorder joins". |
| SELECT ... INNER JOIN     | ✅ Yes     |                                                                                   |
| SELECT ... OUTER JOIN     | 🚧 Partial | no RIGHT JOIN                                                                     |
| SELECT ... JOIN USING     | ✅ Yes     |                                                                                   |
| SELECT ... NATURAL JOIN   | ✅ Yes     |                                                                                   |
| UPDATE                    | ✅ Yes     |                                                                                   |
| VACUUM                    | ❌ No      |                                                                                   |
| WITH clause               | 🚧 Partial | ❌ No RECURSIVE, no MATERIALIZED, only SELECT supported in CTEs                      |
| WINDOW functions             | 🚧 Partial | only default frame definition, no window-specific functions (rank() etc)         |
| GENERATED                 | ❌ No      |                                                                                   |

#### [PRAGMA](https://www.sqlite.org/pragma.html)


| Statement                        | Status     | Comment                                      |
|----------------------------------|------------|----------------------------------------------|
| PRAGMA analysis_limit            | ❌ No         |                                              |
| PRAGMA application_id            | ✅ Yes        |                                              |
| PRAGMA auto_vacuum               | ❌ No         |                                              |
| PRAGMA automatic_index           | ❌ No         |                                              |
| PRAGMA busy_timeout              | ✅ Yes         |                                              |
| PRAGMA cache_size                | ✅ Yes        |                                              |
| PRAGMA cache_spill               | 🚧 Partial    | Enabled/Disabled only                        |
| PRAGMA case_sensitive_like       | Not Needed | deprecated in SQLite                         |
| PRAGMA cell_size_check           | ❌ No         |                                              |
| PRAGMA checkpoint_fullsync       | ❌ No         |                                              |
| PRAGMA collation_list            | ❌ No         |                                              |
| PRAGMA compile_options           | ❌ No         |                                              |
| PRAGMA count_changes             | Not Needed | deprecated in SQLite                         |
| PRAGMA data_store_directory      | Not Needed | deprecated in SQLite                         |
| PRAGMA data_version              | ❌ No         |                                              |
| PRAGMA database_list             | ✅ Yes        |                                              |
| PRAGMA default_cache_size        | Not Needed | deprecated in SQLite                         |
| PRAGMA defer_foreign_keys        | ❌ No         |                                              |
| PRAGMA empty_result_callbacks    | Not Needed | deprecated in SQLite                         |
| PRAGMA encoding                  | ✅ Yes        |                                              |
| PRAGMA foreign_key_check         | ❌ No         |                                              |
| PRAGMA foreign_key_list          | ❌ No         |                                              |
| PRAGMA foreign_keys              | ✅ Yes         |                                              |
| PRAGMA freelist_count            | ✅ Yes        |                                              |
| PRAGMA full_column_names         | Not Needed | deprecated in SQLite                         |
| PRAGMA fullsync                  | ❌ No         |                                              |
| PRAGMA function_list             | ✅ Yes        |                                              |
| PRAGMA hard_heap_limit           | ❌ No         |                                              |
| PRAGMA ignore_check_constraints  | ✅ Yes        |                                              |
| PRAGMA incremental_vacuum        | ❌ No         |                                              |
| PRAGMA index_info                | ✅ Yes        |                                              |
| PRAGMA index_list                | ✅ Yes        |                                              |
| PRAGMA index_xinfo               | ✅ Yes        |                                              |
| PRAGMA integrity_check           | ✅ Yes        |                                              |
| PRAGMA journal_mode              | ✅ Yes        |                                              |
| PRAGMA journal_size_limit        | ❌ No         |                                              |
| PRAGMA legacy_alter_table        | ❌ No         |                                              |
| PRAGMA legacy_file_format        | ✅ Yes        |                                              |
| PRAGMA locking_mode              | ❌ No         |                                              |
| PRAGMA max_page_count            | ✅ Yes        |                                              |
| PRAGMA mmap_size                 | ❌ No         |                                              |
| PRAGMA module_list               | ❌ No         |                                              |
| PRAGMA optimize                  | ❌ No         |                                              |
| PRAGMA page_count                | ✅ Yes        |                                              |
| PRAGMA page_size                 | ✅ Yes        |                                              |
| PRAGMA parser_trace              | ❌ No         |                                              |
| PRAGMA pragma_list               | ✅ Yes        |                                              |
| PRAGMA query_only                | ✅ Yes        |                                              |
| PRAGMA quick_check               | ✅ Yes        |                                              |
| PRAGMA read_uncommitted          | ❌ No         |                                              |
| PRAGMA recursive_triggers        | ❌ No         |                                              |
| PRAGMA reverse_unordered_selects | ❌ No         |                                              |
| PRAGMA schema_version            | ✅ Yes        | For writes, emulate defensive mode (always noop)|
| PRAGMA secure_delete             | ❌ No         |                                              |
| PRAGMA short_column_names        | Not Needed | deprecated in SQLite                         |
| PRAGMA shrink_memory             | ❌ No         |                                              |
| PRAGMA soft_heap_limit           | ❌ No         |                                              |
| PRAGMA stats                     | ❌ No         | Used for testing in SQLite                   |
| PRAGMA synchronous               | 🚧 Partial    | `OFF` and `FULL` supported                   |
| PRAGMA table_info                | ✅ Yes        |                                              |
| PRAGMA table_list                | ✅ Yes        |                                              |
| PRAGMA table_xinfo               | ✅ Yes        |                                              |
| PRAGMA temp_store                | ✅ Yes        |                                              |
| PRAGMA temp_store_directory      | Not Needed | deprecated in SQLite                         |
| PRAGMA threads                   | ❌ No         |                                              |
| PRAGMA trusted_schema            | ❌ No         |                                              |
| PRAGMA user_version              | ✅ Yes        |                                              |
| PRAGMA vdbe_addoptrace           | ❌ No         |                                              |
| PRAGMA vdbe_debug                | ❌ No         |                                              |
| PRAGMA vdbe_listing              | ❌ No         |                                              |
| PRAGMA vdbe_trace                | ❌ No         |                                              |
| PRAGMA wal_autocheckpoint        | ❌ No         |                                              |
| PRAGMA wal_checkpoint            | 🚧 Partial    | Not Needed calling with param (pragma-value) |
| PRAGMA writable_schema           | ❌ No         |                                              |

### Expressions

Feature support of [sqlite expr syntax](https://www.sqlite.org/lang_expr.html).

| Syntax                    | Status  | Comment                                  |
|---------------------------|---------|------------------------------------------|
| literals                  | ✅ Yes     |                                          |
| schema.table.column       | 🚧 Partial | Schemas aren't supported                 |
| unary operator            | ✅ Yes     |                                          |
| binary operator           | 🚧 Partial | Only `%`, `!<`, and `!>` are unsupported |
| agg() FILTER (WHERE ...)  | ❌ No      | Is incorrectly ignored                   |
| ... OVER (...)            | ❌ No      | Is incorrectly ignored                   |
| (expr)                    | ✅ Yes     |                                          |
| CAST (expr AS type)       | ✅ Yes     |                                          |
| COLLATE                   | 🚧 Partial | Custom Collations not supported          |
| (NOT) LIKE                | ✅ Yes     |                                          |
| (NOT) GLOB                | ✅ Yes     |                                          |
| (NOT) REGEXP              | ✅ Yes     |                                          |
| (NOT) MATCH               | ❌ No      |                                          |
| IS (NOT)                  | ✅ Yes     |                                          |
| IS (NOT) DISTINCT FROM    | ✅ Yes     |                                          |
| (NOT) BETWEEN ... AND ... | ✅ Yes     | Expression is rewritten in the optimizer |
| (NOT) IN (SELECT...)       | ✅ Yes      |                                          |
| (NOT) EXISTS (SELECT...)   | ✅ Yes      |                                          |
| x <operator> (SELECT...))   | 🚧 Partial  | Only scalar subqueries supported, i.e. not (x,y) = (SELECT...)
| CASE WHEN THEN ELSE END   | ✅ Yes     |                                          |
| RAISE                     | ✅ Yes     | |

### SQL functions

#### Scalar functions

| Function                     | Status  | Comment                                              |
|------------------------------|---------|------------------------------------------------------|
| abs(X)                       | ✅ Yes     |                                                      |
| changes()                    | 🚧 Partial | Still need to support update statements and triggers |
| char(X1,X2,...,XN)           | ✅ Yes     |                                                      |
| coalesce(X,Y,...)            | ✅ Yes     |                                                      |
| concat(X,...)                | ✅ Yes     |                                                      |
| concat_ws(SEP,X,...)         | ✅ Yes     |                                                      |
| format(FORMAT,...)           | ✅ Yes     |                                                      |
| glob(X,Y)                    | ✅ Yes     |                                                      |
| hex(X)                       | ✅ Yes     |                                                      |
| ifnull(X,Y)                  | ✅ Yes     |                                                      |
| if(X,Y,Z)                    | ✅ Yes     | Alias of iif                                         |
| iif(X,Y,Z)                   | ✅ Yes     |                                                      |
| instr(X,Y)                   | ✅ Yes     |                                                      |
| last_insert_rowid()          | ✅ Yes     |                                                      |
| length(X)                    | ✅ Yes     |                                                      |
| like(X,Y)                    | ✅ Yes     |                                                      |
| like(X,Y,Z)                  | ✅ Yes     |                                                      |
| likelihood(X,Y)              | ✅ Yes     |                                                      |
| likely(X)                    | ✅ Yes     |                                                      |
| load_extension(X)            | 🚧 Partial | Only Turso-native extensions, not SQLite .so/.dll    |
| load_extension(X,Y)          | ❌ No      |                                                      |
| lower(X)                     | ✅ Yes     |                                                      |
| ltrim(X)                     | ✅ Yes     |                                                      |
| ltrim(X,Y)                   | ✅ Yes     |                                                      |
| max(X,Y,...)                 | ✅ Yes     |                                                      |
| min(X,Y,...)                 | ✅ Yes     |                                                      |
| nullif(X,Y)                  | ✅ Yes     |                                                      |
| octet_length(X)              | ✅ Yes     |                                                      |
| printf(FORMAT,...)           | ✅ Yes     |                                                      |
| quote(X)                     | ✅ Yes     |                                                      |
| random()                     | ✅ Yes     |                                                      |
| randomblob(N)                | ✅ Yes     |                                                      |
| replace(X,Y,Z)               | ✅ Yes     |                                                      |
| round(X)                     | ✅ Yes     |                                                      |
| round(X,Y)                   | ✅ Yes     |                                                      |
| rtrim(X)                     | ✅ Yes     |                                                      |
| rtrim(X,Y)                   | ✅ Yes     |                                                      |
| sign(X)                      | ✅ Yes     |                                                      |
| soundex(X)                   | ✅ Yes     |                                                      |
| sqlite_compileoption_get(N)  | ❌ No      |                                                      |
| sqlite_compileoption_used(X) | ❌ No      |                                                      |
| sqlite_offset(X)             | ❌ No      |                                                      |
| sqlite_source_id()           | ✅ Yes     |                                                      |
| sqlite_version()             | ✅ Yes     |                                                      |
| substr(X,Y,Z)                | ✅ Yes     |                                                      |
| substr(X,Y)                  | ✅ Yes     |                                                      |
| substring(X,Y,Z)             | ✅ Yes     |                                                      |
| substring(X,Y)               | ✅ Yes     |                                                      |
| total_changes()              | 🚧 Partial | Still need to support update statements and triggers |
| trim(X)                      | ✅ Yes     |                                                      |
| trim(X,Y)                    | ✅ Yes     |                                                      |
| typeof(X)                    | ✅ Yes     |                                                      |
| unhex(X)                     | ✅ Yes     |                                                      |
| unhex(X,Y)                   | ✅ Yes     |                                                      |
| unicode(X)                   | ✅ Yes     |                                                      |
| unlikely(X)                  | ✅ Yes     |                                                      |
| upper(X)                     | ✅ Yes     |                                                      |
| unistr(X)                    | ❌ No      |                                                      |
| zeroblob(N)                  | ✅ Yes     |                                                      |

#### Mathematical functions

| Function   | Status | Comment |
|------------|--------|---------|
| acos(X)    | ✅ Yes    |         |
| acosh(X)   | ✅ Yes    |         |
| asin(X)    | ✅ Yes    |         |
| asinh(X)   | ✅ Yes    |         |
| atan(X)    | ✅ Yes    |         |
| atan2(Y,X) | ✅ Yes    |         |
| atanh(X)   | ✅ Yes    |         |
| ceil(X)    | ✅ Yes    |         |
| ceiling(X) | ✅ Yes    |         |
| cos(X)     | ✅ Yes    |         |
| cosh(X)    | ✅ Yes    |         |
| degrees(X) | ✅ Yes    |         |
| exp(X)     | ✅ Yes    |         |
| floor(X)   | ✅ Yes    |         |
| ln(X)      | ✅ Yes    |         |
| log(B,X)   | ✅ Yes    |         |
| log(X)     | ✅ Yes    |         |
| log10(X)   | ✅ Yes    |         |
| log2(X)    | ✅ Yes    |         |
| mod(X,Y)   | ✅ Yes    |         |
| pi()       | ✅ Yes    |         |
| pow(X,Y)   | ✅ Yes    |         |
| power(X,Y) | ✅ Yes    |         |
| radians(X) | ✅ Yes    |         |
| sin(X)     | ✅ Yes    |         |
| sinh(X)    | ✅ Yes    |         |
| sqrt(X)    | ✅ Yes    |         |
| tan(X)     | ✅ Yes    |         |
| tanh(X)    | ✅ Yes    |         |
| trunc(X)   | ✅ Yes    |         |

#### Aggregate functions

| Function                     | Status  | Comment |
|------------------------------|---------|---------|
| avg(X)                       | ✅ Yes     |         |
| count(X)                     | ✅ Yes     |         |
| count(*)                     | ✅ Yes     |         |
| group_concat(X)              | ✅ Yes     |         |
| group_concat(X,Y)            | ✅ Yes     |         |
| string_agg(X,Y)              | ✅ Yes     |         |
| max(X)                       | ✅ Yes     |         |
| min(X)                       | ✅ Yes     |         |
| sum(X)                       | ✅ Yes     |         |
| total(X)                     | ✅ Yes     |         |
| median(X)                    | ✅ Yes     | Requires percentile extension                        |
| percentile(Y,P)              | ✅ Yes     | Requires percentile extension                        |
| percentile_cont(Y,P)         | ✅ Yes     | Requires percentile extension                        |
| percentile_disc(Y,P)         | ✅ Yes     | Requires percentile extension                        |
| stddev(X)                    | ✅ Yes     | Turso extension                                      |

#### Date and time functions

| Function    | Status  | Comment                      |
|-------------|---------|------------------------------|
| date()      | ✅ Yes     |                              |
| time()      | ✅ Yes     |                              |
| datetime()  | ✅ Yes     |                              |
| julianday() | ✅ Yes     |                              |
| unixepoch() | ✅ Yes     |                              |
| strftime()  | ✅ Yes     |                              |
| timediff()  | ✅ Yes     |                              |

Modifiers:

|  Modifier      | Status|  Comment                        |
|----------------|-------|---------------------------------|
| Days           | ✅ Yes 	 |                                 |
| Hours          | ✅ Yes	 |                                 |
| Minutes        | ✅ Yes	 |                                 |
| Seconds        | ✅ Yes	 |                                 |
| Months         | ✅ Yes	 |                                 |
| Years          | ✅ Yes	 |                                 |
| TimeOffset     | ✅ Yes	 |                                 |
| DateOffset	 | ✅ Yes   |                                 |
| DateTimeOffset | ✅ Yes   |                                 |
| Ceiling	     | ✅ Yes   |                                 |
| Floor          | ✅ Yes   |                                 |
| StartOfMonth	 | ✅ Yes	 |                                 |
| StartOfYear	 | ✅ Yes	 |                                 |
| StartOfDay	 | ✅ Yes	 |                                 |
| Weekday(N)	 | ✅ Yes   |                                 |
| Auto           | ✅ Yes   |                                 |
| UnixEpoch      | ✅ Yes   |                                 |
| JulianDay      | ✅ Yes   |                                 |
| Localtime      | ✅ Yes   |                                 |
| Utc            | ✅ Yes   |                                 |
| Subsec         | ✅ Yes   |                                 |

#### JSON functions

| Function                           | Status  | Comment                                                                                                                                      |
| ---------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| json(json)                         | ✅ Yes     |                                                                                                                                              |
| jsonb(json)                        | ✅ Yes     |                                                                                                                                              |
| json_array(value1,value2,...)      | ✅ Yes     |                                                                                                                                              |
| jsonb_array(value1,value2,...)     | ✅ Yes     |                                                                                                                                              |
| json_array_length(json)            | ✅ Yes     |                                                                                                                                              |
| json_array_length(json,path)       | ✅ Yes     |                                                                                                                                              |
| json_error_position(json)          | ✅ Yes     |                                                                                                                                              |
| json_extract(json,path,...)        | ✅ Yes     |                                                                                                                                              |
| jsonb_extract(json,path,...)       | ✅ Yes     |                                                                                                                                              |
| json -> path                       | ✅ Yes     |                                                                                                                                              |
| json ->> path                      | ✅ Yes     |                                                                                                                                              |
| json_insert(json,path,value,...)   | ✅ Yes     |                                                                                                                                              |
| jsonb_insert(json,path,value,...)  | ✅ Yes     |                                                                                                                                              |
| json_object(label1,value1,...)     | ✅ Yes     |                                                                                                                                              |
| jsonb_object(label1,value1,...)    | ✅ Yes     |                                                                                                                                              |
| json_patch(json1,json2)            | ✅ Yes     |                                                                                                                                              |
| jsonb_patch(json1,json2)           | ✅ Yes     |                                                                                                                                              |
| json_pretty(json)                  | ✅ Yes     |                                                                                                                                              |
| json_remove(json,path,...)         | ✅ Yes     |                                                                                                                                              |
| jsonb_remove(json,path,...)        | ✅ Yes     |                                                                                                                                              |
| json_replace(json,path,value,...)  | ✅ Yes     |                                                                                                                                              |
| jsonb_replace(json,path,value,...) | ✅ Yes     |                                                                                                                                              |
| json_set(json,path,value,...)      | ✅ Yes     |                                                                                                                                              |
| jsonb_set(json,path,value,...)     | ✅ Yes     |                                                                                                                                              |
| json_type(json)                    | ✅ Yes     |                                                                                                                                              |
| json_type(json,path)               | ✅ Yes     |                                                                                                                                              |
| json_valid(json)                   | ✅ Yes     |                                                                                                                                              |
| json_valid(json,flags)             | ✅ Yes     |                                                                                                                                              |
| json_quote(value)                  | ✅ Yes     |                                                                                                                                              |
| json_group_array(value)            | ✅ Yes     |                                                                                                                                              |
| jsonb_group_array(value)           | ✅ Yes     |                                                                                                                                              |
| json_group_object(label,value)     | ✅ Yes     |                                                                                                                                              |
| jsonb_group_object(name,value)     | ✅ Yes     |                                                                                                                                              |
| json_each(json)                    | ✅ Yes     |                                                                                                                                              |
| json_each(json,path)               | ✅ Yes     |                                                                                                                                              |
| json_tree(json)                    | 🚧 Partial | see commented-out tests in json.test                                                                                                         |
| json_tree(json,path)               | 🚧 Partial | see commented-out tests in json.test                                                                                                         |

## SQLite C API

### Database Connection

| Interface              | Status  | Comment |
|------------------------|---------|---------|
| sqlite3_open           | ✅ Yes     |         |
| sqlite3_open_v2        | 🚧 Partial | Delegates to sqlite3_open, flags/VFS ignored |
| sqlite3_open16         | ❌ No      |         |
| sqlite3_close          | ✅ Yes     |         |
| sqlite3_close_v2       | ✅ Yes     | Same as sqlite3_close |
| sqlite3_db_filename    | ✅ Yes     |         |
| sqlite3_db_config      | ❌ No      | Stub    |
| sqlite3_db_handle      | ❌ No      | Stub    |
| sqlite3_db_readonly    | ❌ No      |         |
| sqlite3_db_status      | ❌ No      |         |
| sqlite3_db_cacheflush  | ❌ No      |         |
| sqlite3_db_release_memory | ❌ No   |         |
| sqlite3_db_name        | ❌ No      |         |
| sqlite3_db_mutex       | ❌ No      |         |
| sqlite3_get_autocommit | ✅ Yes     |         |
| sqlite3_limit          | ❌ No      | Stub    |
| sqlite3_initialize     | ✅ Yes     |         |
| sqlite3_shutdown       | ✅ Yes     |         |
| sqlite3_config         | ❌ No      |         |

### Prepared Statements

| Interface                   | Status  | Comment |
|-----------------------------|---------|---------|
| sqlite3_prepare             | ❌ No      |         |
| sqlite3_prepare_v2          | ✅ Yes     |         |
| sqlite3_prepare_v3          | ❌ No      |         |
| sqlite3_prepare16           | ❌ No      |         |
| sqlite3_prepare16_v2        | ❌ No      |         |
| sqlite3_finalize            | ✅ Yes     |         |
| sqlite3_step                | ✅ Yes     |         |
| sqlite3_reset               | ✅ Yes     |         |
| sqlite3_exec                | ✅ Yes     |         |
| sqlite3_stmt_readonly       | ❌ No      | Stub    |
| sqlite3_stmt_busy           | ❌ No      | Stub    |
| sqlite3_stmt_status         | ❌ No      |         |
| sqlite3_sql                 | ❌ No      |         |
| sqlite3_expanded_sql        | ❌ No      | Stub    |
| sqlite3_normalized_sql      | ❌ No      |         |
| sqlite3_next_stmt           | ✅ Yes     |         |

### Binding Parameters

| Interface                    | Status  | Comment |
|------------------------------|---------|---------|
| sqlite3_bind_parameter_count | ✅ Yes     |         |
| sqlite3_bind_parameter_name  | ✅ Yes     |         |
| sqlite3_bind_parameter_index | ✅ Yes     |         |
| sqlite3_bind_null            | ✅ Yes     |         |
| sqlite3_bind_int             | ✅ Yes     |         |
| sqlite3_bind_int64           | ✅ Yes     |         |
| sqlite3_bind_double          | ✅ Yes     |         |
| sqlite3_bind_text            | ✅ Yes     |         |
| sqlite3_bind_text16          | ❌ No      |         |
| sqlite3_bind_text64          | ❌ No      |         |
| sqlite3_bind_blob            | ✅ Yes     |         |
| sqlite3_bind_blob64          | ❌ No      |         |
| sqlite3_bind_value           | ❌ No      |         |
| sqlite3_bind_pointer         | ❌ No      |         |
| sqlite3_bind_zeroblob        | ❌ No      |         |
| sqlite3_bind_zeroblob64      | ❌ No      |         |
| sqlite3_clear_bindings       | ✅ Yes     |         |

### Result Columns

| Interface                | Status  | Comment |
|--------------------------|---------|---------|
| sqlite3_column_count     | ✅ Yes     |         |
| sqlite3_column_name      | ✅ Yes     |         |
| sqlite3_column_name16    | ❌ No      |         |
| sqlite3_column_decltype  | ✅ Yes     |         |
| sqlite3_column_decltype16| ❌ No      |         |
| sqlite3_column_type      | ✅ Yes     |         |
| sqlite3_column_int       | ✅ Yes     |         |
| sqlite3_column_int64     | ✅ Yes     |         |
| sqlite3_column_double    | ✅ Yes     |         |
| sqlite3_column_text      | ✅ Yes     |         |
| sqlite3_column_text16    | ❌ No      |         |
| sqlite3_column_blob      | ✅ Yes     |         |
| sqlite3_column_bytes     | ✅ Yes     |         |
| sqlite3_column_bytes16   | ❌ No      |         |
| sqlite3_column_value     | ❌ No      |         |
| sqlite3_column_table_name| ✅ Yes     |         |
| sqlite3_column_database_name | ❌ No  |         |
| sqlite3_column_origin_name | ❌ No    |         |
| sqlite3_data_count       | ✅ Yes     |         |

### Result Values (sqlite3_value)

| Interface              | Status  | Comment |
|------------------------|---------|---------|
| sqlite3_value_type     | ✅ Yes     |         |
| sqlite3_value_int      | ❌ No      |         |
| sqlite3_value_int64    | ✅ Yes     |         |
| sqlite3_value_double   | ✅ Yes     |         |
| sqlite3_value_text     | ✅ Yes     |         |
| sqlite3_value_text16   | ❌ No      |         |
| sqlite3_value_blob     | ✅ Yes     |         |
| sqlite3_value_bytes    | ✅ Yes     |         |
| sqlite3_value_bytes16  | ❌ No      |         |
| sqlite3_value_dup      | ❌ No      |         |
| sqlite3_value_free     | ❌ No      |         |
| sqlite3_value_nochange | ❌ No      |         |
| sqlite3_value_frombind | ❌ No      |         |
| sqlite3_value_subtype  | ❌ No      |         |
| sqlite3_value_pointer  | ❌ No      |         |
| sqlite3_value_encoding | ❌ No      |         |
| sqlite3_value_numeric_type | ❌ No  |         |

### Error Handling

| Interface              | Status  | Comment |
|------------------------|---------|---------|
| sqlite3_errcode        | ✅ Yes     |         |
| sqlite3_errmsg         | ✅ Yes     |         |
| sqlite3_errmsg16       | ❌ No      |         |
| sqlite3_errstr         | ✅ Yes     |         |
| sqlite3_extended_errcode | ✅ Yes   |         |
| sqlite3_extended_result_codes | ❌ No |        |
| sqlite3_error_offset   | ❌ No      |         |
| sqlite3_system_errno   | ❌ No      |         |

### Changes and Row IDs

| Interface              | Status  | Comment |
|------------------------|---------|---------|
| sqlite3_changes        | ✅ Yes     |         |
| sqlite3_changes64      | ✅ Yes     |         |
| sqlite3_total_changes  | ✅ Yes     |         |
| sqlite3_total_changes64| ❌ No      |         |
| sqlite3_last_insert_rowid | ✅ Yes  |         |
| sqlite3_set_last_insert_rowid | ❌ No |       |

### Memory Management

| Interface              | Status  | Comment |
|------------------------|---------|---------|
| sqlite3_malloc         | ✅ Yes     |         |
| sqlite3_malloc64       | ✅ Yes     |         |
| sqlite3_free           | ✅ Yes     |         |
| sqlite3_realloc        | ❌ No      |         |
| sqlite3_realloc64      | ❌ No      |         |
| sqlite3_msize          | ❌ No      |         |
| sqlite3_memory_used    | ❌ No      |         |
| sqlite3_memory_highwater | ❌ No    |         |
| sqlite3_soft_heap_limit64 | ❌ No   |         |
| sqlite3_hard_heap_limit64 | ❌ No   |         |
| sqlite3_release_memory | ❌ No      |         |

### Callback Functions

| Interface                | Status  | Comment |
|--------------------------|---------|---------|
| sqlite3_busy_handler     | ✅ Yes     |         |
| sqlite3_busy_timeout     | ✅ Yes     |         |
| sqlite3_trace_v2         | ❌ No      | Stub    |
| sqlite3_progress_handler | ❌ No      | Stub    |
| sqlite3_set_authorizer   | ❌ No      | Stub    |
| sqlite3_commit_hook      | ❌ No      |         |
| sqlite3_rollback_hook    | ❌ No      |         |
| sqlite3_update_hook      | ❌ No      |         |
| sqlite3_preupdate_hook   | ❌ No      |         |
| sqlite3_unlock_notify    | ❌ No      |         |
| sqlite3_wal_hook         | ❌ No      |         |

### User-Defined Functions

| Interface                    | Status  | Comment |
|------------------------------|---------|---------|
| sqlite3_create_function      | ❌ No      |         |
| sqlite3_create_function_v2   | ❌ No      | Stub    |
| sqlite3_create_function16    | ❌ No      |         |
| sqlite3_create_window_function | ❌ No    | Stub    |
| sqlite3_aggregate_context    | ❌ No      | Stub    |
| sqlite3_user_data            | ❌ No      | Stub    |
| sqlite3_context_db_handle    | ❌ No      | Stub    |
| sqlite3_get_auxdata          | ❌ No      |         |
| sqlite3_set_auxdata          | ❌ No      |         |
| sqlite3_result_null          | ❌ No      | Stub    |
| sqlite3_result_int           | ❌ No      |         |
| sqlite3_result_int64         | ❌ No      | Stub    |
| sqlite3_result_double        | ❌ No      | Stub    |
| sqlite3_result_text          | ❌ No      | Stub    |
| sqlite3_result_text16        | ❌ No      |         |
| sqlite3_result_text64        | ❌ No      |         |
| sqlite3_result_blob          | ❌ No      | Stub    |
| sqlite3_result_blob64        | ❌ No      |         |
| sqlite3_result_value         | ❌ No      |         |
| sqlite3_result_pointer       | ❌ No      |         |
| sqlite3_result_zeroblob      | ❌ No      |         |
| sqlite3_result_zeroblob64    | ❌ No      |         |
| sqlite3_result_error         | ❌ No      | Stub    |
| sqlite3_result_error16       | ❌ No      |         |
| sqlite3_result_error_code    | ❌ No      |         |
| sqlite3_result_error_nomem   | ❌ No      | Stub    |
| sqlite3_result_error_toobig  | ❌ No      | Stub    |
| sqlite3_result_subtype       | ❌ No      |         |

### Collation Functions

| Interface                   | Status  | Comment |
|-----------------------------|---------|---------|
| sqlite3_create_collation    | ❌ No      |         |
| sqlite3_create_collation_v2 | ❌ No      | Stub    |
| sqlite3_create_collation16  | ❌ No      |         |
| sqlite3_collation_needed    | ❌ No      |         |
| sqlite3_collation_needed16  | ❌ No      |         |
| sqlite3_stricmp             | ❌ No      | Stub    |
| sqlite3_strnicmp            | ❌ No      |         |

### Backup API

| Interface                | Status  | Comment |
|--------------------------|---------|---------|
| sqlite3_backup_init      | ❌ No      | Stub    |
| sqlite3_backup_step      | ❌ No      | Stub    |
| sqlite3_backup_finish    | ❌ No      | Stub    |
| sqlite3_backup_remaining | ❌ No      | Stub    |
| sqlite3_backup_pagecount | ❌ No      | Stub    |

### BLOB I/O

| Interface              | Status  | Comment |
|------------------------|---------|---------|
| sqlite3_blob_open      | ❌ No      | Stub    |
| sqlite3_blob_close     | ❌ No      | Stub    |
| sqlite3_blob_bytes     | ❌ No      | Stub    |
| sqlite3_blob_read      | ❌ No      | Stub    |
| sqlite3_blob_write     | ❌ No      | Stub    |
| sqlite3_blob_reopen    | ❌ No      |         |

### WAL Functions

| Interface                  | Status  | Comment |
|----------------------------|---------|---------|
| sqlite3_wal_checkpoint     | ✅ Yes     |         |
| sqlite3_wal_checkpoint_v2  | ✅ Yes     |         |
| sqlite3_wal_autocheckpoint | ❌ No      |         |
| sqlite3_wal_hook           | ❌ No      |         |

### Utility Functions

| Interface              | Status  | Comment |
|------------------------|---------|---------|
| sqlite3_libversion     | ✅ Yes     | Returns "3.42.0" |
| sqlite3_libversion_number | ✅ Yes  | Returns 3042000 |
| sqlite3_sourceid       | ❌ No      |         |
| sqlite3_threadsafe     | ✅ Yes     | Returns 1 |
| sqlite3_complete       | ❌ No      | Stub    |
| sqlite3_interrupt      | ❌ No      | Stub    |
| sqlite3_sleep          | ❌ No      | Stub    |
| sqlite3_randomness     | ❌ No      |         |
| sqlite3_get_table      | ✅ Yes     |         |
| sqlite3_free_table     | ✅ Yes     |         |
| sqlite3_mprintf        | ❌ No      |         |
| sqlite3_vmprintf       | ❌ No      |         |
| sqlite3_snprintf       | ❌ No      |         |
| sqlite3_vsnprintf      | ❌ No      |         |
| sqlite3_strglob        | ❌ No      |         |
| sqlite3_strlike        | ❌ No      |         |

### Table Metadata

| Interface                    | Status  | Comment |
|------------------------------|---------|---------|
| sqlite3_table_column_metadata | ✅ Yes    |         |

### Virtual Tables

| Interface                | Status  | Comment |
|--------------------------|---------|---------|
| sqlite3_create_module    | ❌ No      |         |
| sqlite3_create_module_v2 | ❌ No      |         |
| sqlite3_drop_modules     | ❌ No      |         |
| sqlite3_declare_vtab     | ❌ No      |         |
| sqlite3_overload_function| ❌ No      |         |
| sqlite3_vtab_config      | ❌ No      |         |
| sqlite3_vtab_on_conflict | ❌ No      |         |
| sqlite3_vtab_nochange    | ❌ No      |         |
| sqlite3_vtab_collation   | ❌ No      |         |
| sqlite3_vtab_distinct    | ❌ No      |         |
| sqlite3_vtab_in          | ❌ No      |         |
| sqlite3_vtab_in_first    | ❌ No      |         |
| sqlite3_vtab_in_next     | ❌ No      |         |
| sqlite3_vtab_rhs_value   | ❌ No      |         |

### Loadable Extensions

| Interface                    | Status  | Comment |
|------------------------------|---------|---------|
| sqlite3_load_extension       | ❌ No      |         |
| sqlite3_enable_load_extension| ❌ No      |         |
| sqlite3_auto_extension       | ❌ No      |         |
| sqlite3_cancel_auto_extension| ❌ No      |         |
| sqlite3_reset_auto_extension | ❌ No      |         |

### Serialization

| Interface              | Status  | Comment |
|------------------------|---------|---------|
| sqlite3_serialize      | ❌ No      | Stub    |
| sqlite3_deserialize    | ❌ No      | Stub    |

### Miscellaneous

| Interface                | Status  | Comment |
|--------------------------|---------|---------|
| sqlite3_keyword_count    | ❌ No      |         |
| sqlite3_keyword_name     | ❌ No      |         |
| sqlite3_keyword_check    | ❌ No      |         |
| sqlite3_txn_state        | ❌ No      |         |
| sqlite3_file_control     | ❌ No      |         |
| sqlite3_status           | ❌ No      |         |
| sqlite3_status64         | ❌ No      |         |
| sqlite3_test_control     | ❌ No      | Testing only |
| sqlite3_log              | ❌ No      |         |

### Turso-specific Extensions

| Interface                      | Status  | Comment |
|--------------------------------|---------|---------|
| libsql_wal_frame_count         | ✅ Yes     | Get WAL frame count |
| libsql_wal_get_frame           | ✅ Yes     | Extract frame from WAL |
| libsql_wal_insert_frame        | ✅ Yes     | Insert frame into WAL |
| libsql_wal_disable_checkpoint  | ✅ Yes     | Disable checkpointing |

## SQLite VDBE opcodes

| Opcode         | Status | Comment |
|----------------|--------|---------|
| Add            | ✅ Yes    |         |
| AddImm         | ✅ Yes    |         |
| Affinity       | ✅ Yes    |         |
| AggFinal       | ✅ Yes    |         |
| AggStep        | ✅ Yes    |         |
| AggValue       | ✅ Yes    |         |
| And            | ✅ Yes    |         |
| AutoCommit     | ✅ Yes    |         |
| BitAnd         | ✅ Yes    |         |
| BitNot         | ✅ Yes    |         |
| BitOr          | ✅ Yes    |         |
| Blob           | ✅ Yes    |         |
| BeginSubrtn    | ✅ Yes    |         |
| Cast           | ✅ Yes    |         |
| Checkpoint     | ✅ Yes    |         |
| Clear          | ❌ No     |         |
| Close          | ✅ Yes    |         |
| CollSeq        | ✅ Yes    |         |
| Column         | ✅ Yes    |         |
| Compare        | ✅ Yes    |         |
| Concat         | ✅ Yes    |         |
| Copy           | ✅ Yes    |         |
| Count          | ✅ Yes    |         |
| CreateBTree    | 🚧 Partial| no temp databases |
| DecrJumpZero   | ✅ Yes    |         |
| Delete         | ✅ Yes    |         |
| Destroy        | ✅ Yes    |         |
| Divide         | ✅ Yes    |         |
| DropIndex      | ✅ Yes    |         |
| DropTable      | ✅ Yes    |         |
| DropTrigger    | ✅ Yes     |         |
| EndCoroutine   | ✅ Yes    |         |
| Eq             | ✅ Yes    |         |
| Expire         | ❌ No     |         |
| Explain        | ❌ No     |         |
| FkCheck        | ✅ Yes    |         |
| FkCounter      | ✅ Yes    |         |
| FkIfZero       | ✅ Yes    |         |
| Found          | ✅ Yes    |         |
| Filter         | ✅ Yes    |         |
| FilterAdd      | ✅ Yes    |         |
| Function       | ✅ Yes    |         |
| Ge             | ✅ Yes    |         |
| Gosub          | ✅ Yes    |         |
| Goto           | ✅ Yes    |         |
| Gt             | ✅ Yes    |         |
| Halt           | ✅ Yes    |         |
| HaltIfNull     | ✅ Yes    |         |
| IdxDelete      | ✅ Yes    |         |
| IdxGE          | ✅ Yes    |         |
| IdxInsert      | ✅ Yes    |         |
| IdxLE          | ✅ Yes    |         |
| IdxLT          | ✅ Yes    |         |
| IdxRowid       | ✅ Yes    |         |
| If             | ✅ Yes    |         |
| IfNeg          | ✅ Yes     |         |
| IfNot          | ✅ Yes    |         |
| IfPos          | ✅ Yes    |         |
| IfZero         | ❌ No     |         |
| IncrVacuum     | ❌ No     |         |
| Init           | ✅ Yes    |         |
| InitCoroutine  | ✅ Yes    |         |
| Insert         | ✅ Yes    |         |
| Int64          | ✅ Yes    |         |
| Integer        | ✅ Yes    |         |
| IntegrityCk    | ✅ Yes    |         |
| IsNull         | ✅ Yes    |         |
| IsUnique       | ❌ No     |         |
| JournalMode    | ✅ Yes    |         |
| Jump           | ✅ Yes    |         |
| Last           | ✅ Yes    |         |
| Le             | ✅ Yes    |         |
| LoadAnalysis   | ❌ No     |         |
| Lt             | ✅ Yes    |         |
| MakeRecord     | ✅ Yes    |         |
| MaxPgcnt       | ✅ Yes    |         |
| MemMax         | ✅ Yes     |         |
| Move           | ✅ Yes    |         |
| Multiply       | ✅ Yes    |         |
| MustBeInt      | ✅ Yes    |         |
| Ne             | ✅ Yes    |         |
| NewRowid       | ✅ Yes    |         |
| Next           | ✅ Yes     |         |
| Noop           | ✅ Yes     |         |
| Not            | ✅ Yes    |         |
| NotExists      | ✅ Yes    |         |
| NotFound       | ✅ Yes    |         |
| NotNull        | ✅ Yes    |         |
| Null           | ✅ Yes    |         |
| NullRow        | ✅ Yes    |         |
| Once           | ✅ Yes     |         |
| OpenAutoindex  | ✅ Yes     |         |
| OpenDup        | ✅ Yes     |         |
| OpenEphemeral  | ✅ Yes     |         |
| OpenPseudo     | ✅ Yes    |         |
| OpenRead       | ✅ Yes    |         |
| OpenWrite      | ✅ Yes     |         |
| Or             | ✅ Yes    |         |
| Pagecount      | 🚧 Partial| no temp databases |
| Param          | ❌ No     |         |
| ParseSchema    | ✅ Yes    |         |
| Permutation    | ❌ No     |         |
| Prev           | ✅ Yes     |         |
| Program        | ✅ Yes     |         |
| ReadCookie     | 🚧 Partial| no temp databases, only user_version supported |
| Real           | ✅ Yes    |         |
| RealAffinity   | ✅ Yes    |         |
| Remainder      | ✅ Yes    |         |
| ResetCount     | ❌ No     |         |
| ResetSorter    | 🚧 Partial| sorter cursors are not supported yet; only ephemeral tables are |
| ResultRow      | ✅ Yes    |         |
| Return         | ✅ Yes    |         |
| Rewind         | ✅ Yes    |         |
| RowData        | ✅ Yes     |         |
| RowId          | ✅ Yes    |         |
| RowKey         | ❌ No     |         |
| RowSetAdd      | ✅ Yes     |         |
| RowSetRead     | ✅ Yes    |         |
| RowSetTest     | ✅ Yes     |         |
| Rowid          | ✅ Yes    |         |
| SCopy          | ❌ No     |         |
| Savepoint      | ❌ No     |         |
| Seek           | ❌ No     |         |
| SeekGe         | ✅ Yes    |         |
| SeekGt         | ✅ Yes    |         |
| SeekLe         | ✅ Yes    |         |
| SeekLt         | ✅ Yes    |         |
| SeekRowid      | ✅ Yes    |         |
| SeekEnd        | ✅ Yes    |         |
| Sequence       | ✅ Yes    |         |
| SequenceTest   | ✅ Yes    |         |
| SetCookie      | ✅ Yes    |         |
| ShiftLeft      | ✅ Yes    |         |
| ShiftRight     | ✅ Yes    |         |
| SoftNull       | ✅ Yes    |         |
| Sort           | ❌ No     |         |
| SorterCompare  | ✅ Yes     |         |
| SorterData     | ✅ Yes    |         |
| SorterInsert   | ✅ Yes    |         |
| SorterNext     | ✅ Yes    |         |
| SorterOpen     | ✅ Yes    |         |
| SorterSort     | ✅ Yes    |         |
| String         | NotNeeded | SQLite uses String for sized strings and String8 for null-terminated. All our strings are sized |
| String8        | ✅ Yes    |         |
| Subtract       | ✅ Yes    |         |
| TableLock      | ❌ No     |         |
| Trace          | ❌ No     |         |
| Transaction    | ✅ Yes    |         |
| VBegin         | ✅ Yes    |         |
| VColumn        | ✅ Yes    |         |
| VCreate        | ✅ Yes    |         |
| VDestroy       | ✅ Yes    |         |
| VFilter        | ✅ Yes    |         |
| VNext          | ✅ Yes    |         |
| VOpen          | ✅ Yes    |         |
| VRename        | ✅ Yes    |         |
| VUpdate        | ✅ Yes    |         |
| Vacuum         | ❌ No     |         |
| Variable       | ✅ Yes    |         |
| Yield          | ✅ Yes    |         |
| ZeroOrNull     | ✅ Yes    |         |

##  [SQLite journaling modes](https://www.sqlite.org/pragma.html#pragma_journal_mode)

We currently don't have plan to support the rollback journal mode as it locks the database file during writes.
Therefore, all rollback-type modes (delete, truncate, persist, memory) are marked are `Not Needed` below.

| Journal mode | Status     | Comment                        |
|--------------|------------|--------------------------------|
| wal          | ✅ Yes        |                                |
| wal2         | ❌ No         | experimental feature in sqlite |
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
| uuid4()               | ✅ Yes    | UUID version 4                                                |
| uuid4_str()           | ✅ Yes    | UUID v4 string alias `gen_random_uuid()` for PG compatibility |
| uuid7(X?)             | ✅ Yes    | UUID version 7 (optional parameter for seconds since epoch)   |
| uuid7_timestamp_ms(X) | ✅ Yes    | Convert a UUID v7 to milliseconds since epoch                 |
| uuid_str(X)           | ✅ Yes    | Convert a valid UUID to string                                |
| uuid_blob(X)          | ✅ Yes    | Convert a valid UUID to blob                                  |

### regexp

The `regexp` extension is compatible with [sqlean-regexp](https://github.com/nalgeon/sqlean/blob/main/docs/regexp.md).

| Function                                       | Status | Comment |
|------------------------------------------------|--------|---------|
| regexp(pattern, source)                        | ✅ Yes    |         |
| regexp_like(source, pattern)                   | ✅ Yes    |         |
| regexp_substr(source, pattern)                 | ✅ Yes    |         |
| regexp_capture(source, pattern[, n])           | ✅ Yes    |         |
| regexp_replace(source, pattern, replacement)   | ✅ Yes    |         |

### Vector

The `vector` extension is compatible with libSQL native vector search.

| Function                                       | Status | Comment |
|------------------------------------------------|--------|---------|
| vector(x)                                      | ✅ Yes    |         |
| vector32(x)                                    | ✅ Yes    |         |
| vector64(x)                                    | ✅ Yes    |         |
| vector_extract(x)                              | ✅ Yes    |         |
| vector_distance_cos(x, y)                      | ✅ Yes    |         |
| vector_distance_l2(x, y)                              | ✅ Yes    |Euclidean distance|
| vector_concat(x, y)                            | ✅ Yes    |         |
| vector_slice(x, start_index, end_index)        | ✅ Yes    |         |

### Time

The `time` extension is compatible with [sqlean-time](https://github.com/nalgeon/sqlean/blob/main/docs/time.md).


| Function                                                            | Status | Comment |
| ------------------------------------------------------------------- | ------ |---------|
| time_now()                                                          | ✅ Yes    |         |
| time_date(year, month, day[, hour, min, sec[, nsec[, offset_sec]]]) | ✅ Yes    |         |
| time_get_year(t)                                                    | ✅ Yes    |         |
| time_get_month(t)                                                   | ✅ Yes    |         |
| time_get_day(t)                                                     | ✅ Yes    |         |
| time_get_hour(t)                                                    | ✅ Yes    |         |
| time_get_minute(t)                                                  | ✅ Yes    |         |
| time_get_second(t)                                                  | ✅ Yes    |         |
| time_get_nano(t)                                                    | ✅ Yes    |         |
| time_get_weekday(t)                                                 | ✅ Yes    |         |
| time_get_yearday(t)                                                 | ✅ Yes    |         |
| time_get_isoyear(t)                                                 | ✅ Yes    |         |
| time_get_isoweek(t)                                                 | ✅ Yes    |         |
| time_get(t, field)                                                  | ✅ Yes    |         |
| time_unix(sec[, nsec])                                              | ✅ Yes    |         |
| time_milli(msec)                                                    | ✅ Yes    |         |
| time_micro(usec)                                                    | ✅ Yes    |         |
| time_nano(nsec)                                                     | ✅ Yes    |         |
| time_to_unix(t)                                                     | ✅ Yes    |         |
| time_to_milli(t)                                                    | ✅ Yes    |         |
| time_to_micro(t)                                                    | ✅ Yes    |         |
| time_to_nano(t)                                                     | ✅ Yes    |         |
| time_after(t, u)                                                    | ✅ Yes    |         |
| time_before(t, u)                                                   | ✅ Yes    |         |
| time_compare(t, u)                                                  | ✅ Yes    |         |
| time_equal(t, u)                                                    | ✅ Yes    |         |
| time_add(t, d)                                                      | ✅ Yes    |         |
| time_add_date(t, years[, months[, days]])                           | ✅ Yes    |         |
| time_sub(t, u)                                                      | ✅ Yes    |         |
| time_since(t)                                                       | ✅ Yes    |         |
| time_until(t)                                                       | ✅ Yes    |         |
| time_trunc(t, field)                                                | ✅ Yes    |         |
| time_trunc(t, d)                                                    | ✅ Yes    |         |
| time_round(t, d)                                                    | ✅ Yes    |         |
| time_fmt_iso(t[, offset_sec])                                       | ✅ Yes    |         |
| time_fmt_datetime(t[, offset_sec])                                  | ✅ Yes    |         |
| time_fmt_date(t[, offset_sec])                                      | ✅ Yes    |         |
| time_fmt_time(t[, offset_sec])                                      | ✅ Yes    |         |
| time_parse(s)                                                       | ✅ Yes    |         |
| dur_ns()                                                            | ✅ Yes    |         |
| dur_us()                                                            | ✅ Yes    |         |
| dur_ms()                                                            | ✅ Yes    |         |
| dur_s()                                                             | ✅ Yes    |         |
| dur_m()                                                             | ✅ Yes    |         |
| dur_h()                                                             | ✅ Yes    |         |

### Full-Text Search (FTS)

Turso implements FTS using Tantivy instead of SQLite's FTS3/FTS4/FTS5.

| Feature | Status | Comment |
|---------|--------|---------|
| CREATE INDEX ... USING fts | ✅ Yes | Turso-specific syntax |
| fts_match() | ✅ Yes | |
| fts_score() | ✅ Yes | BM25 relevance scoring |
| fts_highlight() | ✅ Yes | |
| MATCH operator | ✅ Yes | |
| SQLite FTS3/FTS4/FTS5 | ❌ No | Use Turso FTS instead |
| snippet() | ❌ No | |

### CSV

The CSV extension provides RFC 4180 compliant CSV file reading.

| Feature | Status | Comment |
|---------|--------|---------|
| CSV virtual table | ✅ Yes | `CREATE VIRTUAL TABLE ... USING csv(...)` |

### Percentile

Statistical aggregate functions.

| Function | Status | Comment |
|----------|--------|---------|
| median(X) | ✅ Yes | |
| percentile(Y,P) | ✅ Yes | |
| percentile_cont(Y,P) | ✅ Yes | |
| percentile_disc(Y,P) | ✅ Yes | |

### Table-Valued Functions

| Function | Status | Comment |
|----------|--------|---------|
| generate_series(start, stop[, step]) | ✅ Yes | All parameters supported |
| carray() | ❌ No | C-API specific |

### Internal Virtual Tables

| Virtual Table | Status | Comment |
|---------------|--------|---------|
| sqlite_dbpage | 🚧 Partial | readonly, no attach support |
