# Simulator coverage of Turso

This document describes how much of the Turso capabilities
simulator covers and tests.

## Table of contents

- [Simulator coverage of Turso](#simulator-coverage-of-turso)
  - [Table of contents](#table-of-contents)
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
  - [SQLite journaling modes](#sqlite-journaling-modes)
  - [Extensions](#extensions)
    - [UUID](#uuid)
    - [regexp](#regexp)
    - [Vector](#vector)
    - [Time](#time)

## SQLite query language

### Statements

| Statement                 | Status  | Comment                                                                           |
| ------------------------- | ------- | --------------------------------------------------------------------------------- |
| ALTER TABLE               | Partial | TODO                                                                              |
| ANALYZE                   | No      |                                                                                   |
| ATTACH DATABASE           | No      |                                                                                   |
| BEGIN TRANSACTION         | Partial | TODO                                                                              |
| COMMIT TRANSACTION        | Partial | TODO                                                                              |
| CREATE INDEX              | Partial | TODO                                                                              |
| CREATE TABLE              | Partial | TODO                                                                              |
| CREATE TABLE ... STRICT   | No      |                                                                                   |
| CREATE TRIGGER            | No      |                                                                                   |
| CREATE VIEW               | No      |                                                                                   |
| CREATE VIRTUAL TABLE      | No      |                                                                                   |
| DELETE                    | Partial | TODO                                                                              |
| DETACH DATABASE           | No      |                                                                                   |
| DROP INDEX                | Partial | TODO                                                                              |
| DROP TABLE                | Partial | TODO                                                                              |
| DROP TRIGGER              | No      |                                                                                   |
| DROP VIEW                 | No      |                                                                                   |
| END TRANSACTION           | Partial | TODO                                                                              |
| EXPLAIN                   | Partial |                                                                                   |
| INDEXED BY                | No      |                                                                                   |
| INSERT                    | Partial | TODO                                                                              |
| ON CONFLICT clause        | No      |                                                                                   |
| REINDEX                   | No      |                                                                                   |
| RELEASE SAVEPOINT         | No      |                                                                                   |
| REPLACE                   | No      |                                                                                   |
| RETURNING clause          | No      | TODO                                                                              |
| ROLLBACK TRANSACTION      | Partial | TODO                                                                              |
| SAVEPOINT                 | No      |                                                                                   |
| SELECT                    | Partial | TODO                                                                              |
| SELECT ... WHERE          | Partial | TODO                                                                              |
| SELECT ... WHERE ... LIKE | No      |                                                                                   |
| SELECT ... LIMIT          | Partial | TODO                                                                              |
| SELECT ... ORDER BY       | Partial | TODO                                                                              |
| SELECT ... GROUP BY       | No      |                                                                                   |
| SELECT ... HAVING         | No      |                                                                                   |
| SELECT ... JOIN           | Partial | TODO                                                                              |
| SELECT ... CROSS JOIN     | No      | SQLite CROSS JOIN means "do not reorder joins". We don't support that yet anyway. |
| SELECT ... INNER JOIN     | Partial | TODO                                                                              |
| SELECT ... OUTER JOIN     | No      |                                                                                   |
| SELECT ... JOIN USING     | No      |                                                                                   |
| SELECT ... NATURAL JOIN   | No      |                                                                                   |
| UPDATE                    | Partial |                                                                                   |
| VACUUM                    | No      |                                                                                   |
| WITH clause               | No      |                                                                                   |
| WINDOW functions          | No      |                                                                                   |

#### [PRAGMA](https://www.sqlite.org/pragma.html)

| Statement                        | Status     | Comment                                          |
| -------------------------------- | ---------- | ------------------------------------------------ |
| PRAGMA analysis_limit            | No         |                                                  |
| PRAGMA application_id            | No         |                                                  |
| PRAGMA auto_vacuum               | Yes        |                                                  |
| PRAGMA automatic_index           | No         |                                                  |
| PRAGMA busy_timeout              | No         |                                                  |
| PRAGMA busy_timeout              | No         |                                                  |
| PRAGMA cache_size                | No         |                                                  |
| PRAGMA cache_spill               | No         |                                                  |
| PRAGMA case_sensitive_like       | Not Needed | deprecated in SQLite                             |
| PRAGMA cell_size_check           | No         |                                                  |
| PRAGMA checkpoint_fullsync       | No         |                                                  |
| PRAGMA collation_list            | No         |                                                  |
| PRAGMA compile_options           | No         |                                                  |
| PRAGMA count_changes             | Not Needed | deprecated in SQLite                             |
| PRAGMA data_store_directory      | Not Needed | deprecated in SQLite                             |
| PRAGMA data_version              | No         |                                                  |
| PRAGMA database_list             | No         |                                                  |
| PRAGMA default_cache_size        | Not Needed | deprecated in SQLite                             |
| PRAGMA defer_foreign_keys        | No         |                                                  |
| PRAGMA empty_result_callbacks    | Not Needed | deprecated in SQLite                             |
| PRAGMA encoding                  | No         |                                                  |
| PRAGMA foreign_key_check         | No         |                                                  |
| PRAGMA foreign_key_list          | No         |                                                  |
| PRAGMA foreign_keys              | No         |                                                  |
| PRAGMA freelist_count            | No         |                                                  |
| PRAGMA full_column_names         | Not Needed | deprecated in SQLite                             |
| PRAGMA fullsync                  | No         |                                                  |
| PRAGMA function_list             | No         |                                                  |
| PRAGMA hard_heap_limit           | No         |                                                  |
| PRAGMA ignore_check_constraints  | No         |                                                  |
| PRAGMA incremental_vacuum        | No         |                                                  |
| PRAGMA index_info                | No         |                                                  |
| PRAGMA index_list                | No         |                                                  |
| PRAGMA index_xinfo               | No         |                                                  |
| PRAGMA integrity_check           | No         |                                                  |
| PRAGMA journal_mode              | No         |                                                  |
| PRAGMA journal_size_limit        | No         |                                                  |
| PRAGMA legacy_alter_table        | No         |                                                  |
| PRAGMA legacy_file_format        | No         |                                                  |
| PRAGMA locking_mode              | No         |                                                  |
| PRAGMA max_page_count            | No         |                                                  |
| PRAGMA mmap_size                 | No         |                                                  |
| PRAGMA module_list               | No         |                                                  |
| PRAGMA optimize                  | No         |                                                  |
| PRAGMA page_count                | No         |                                                  |
| PRAGMA page_size                 | No         |                                                  |
| PRAGMA parser_trace              | No         |                                                  |
| PRAGMA pragma_list               | No         |                                                  |
| PRAGMA query_only                | No         |                                                  |
| PRAGMA quick_check               | No         |                                                  |
| PRAGMA read_uncommitted          | No         |                                                  |
| PRAGMA recursive_triggers        | No         |                                                  |
| PRAGMA reverse_unordered_selects | No         |                                                  |
| PRAGMA schema_version            | No         | For writes, emulate defensive mode (always noop) |
| PRAGMA secure_delete             | No         |                                                  |
| PRAGMA short_column_names        | Not Needed | deprecated in SQLite                             |
| PRAGMA shrink_memory             | No         |                                                  |
| PRAGMA soft_heap_limit           | No         |                                                  |
| PRAGMA stats                     | No         | Used for testing in SQLite                       |
| PRAGMA synchronous               | No         |                                                  |
| PRAGMA table_info                | No         |                                                  |
| PRAGMA table_list                | No         |                                                  |
| PRAGMA table_xinfo               | No         |                                                  |
| PRAGMA temp_store                | No         |                                                  |
| PRAGMA temp_store_directory      | Not Needed | deprecated in SQLite                             |
| PRAGMA threads                   | No         |                                                  |
| PRAGMA trusted_schema            | No         |                                                  |
| PRAGMA user_version              | No         |                                                  |
| PRAGMA vdbe_addoptrace           | No         |                                                  |
| PRAGMA vdbe_debug                | No         |                                                  |
| PRAGMA vdbe_listing              | No         |                                                  |
| PRAGMA vdbe_trace                | No         |                                                  |
| PRAGMA wal_autocheckpoint        | No         |                                                  |
| PRAGMA wal_checkpoint            | No         |                                                  |
| PRAGMA writable_schema           | No         |                                                  |

### Expressions

Feature support of [sqlite expr syntax](https://www.sqlite.org/lang_expr.html).

| Syntax                    | Status  | Comment |
| ------------------------- | ------- | ------- |
| literals                  | Yes     |         |
| schema.table.column       | No      |         |
| unary operator            | Yes     |         |
| binary operator           | Partial | TODO    |
| agg() FILTER (WHERE ...)  | No      |         |
| ... OVER (...)            | No      |         |
| (expr)                    | Yes     |         |
| CAST (expr AS type)       | No      |         |
| COLLATE                   | No      |         |
| (NOT) LIKE                | Yes     |         |
| (NOT) GLOB                | No      |         |
| (NOT) REGEXP              | No      |         |
| (NOT) MATCH               | No      |         |
| IS (NOT)                  | No      |         |
| IS (NOT) DISTINCT FROM    | No      |         |
| (NOT) BETWEEN ... AND ... | No      |         |
| (NOT) IN (SELECT...)      | No      |         |
| (NOT) EXISTS (SELECT...)  | No      |         |
| x <operator> (SELECT...)) | No      |         |
| CASE WHEN THEN ELSE END   | No      |         |
| RAISE                     | No      |         |

### SQL functions

#### Scalar functions

| Function                     | Status | Comment |
| ---------------------------- | ------ | ------- |
| abs(X)                       | No     |         |
| changes()                    | No     |         |
| char(X1,X2,...,XN)           | No     |         |
| coalesce(X,Y,...)            | No     |         |
| concat(X,...)                | No     |         |
| concat_ws(SEP,X,...)         | No     |         |
| format(FORMAT,...)           | No     |         |
| glob(X,Y)                    | No     |         |
| hex(X)                       | No     |         |
| ifnull(X,Y)                  | No     |         |
| iif(X,Y,Z)                   | No     |         |
| instr(X,Y)                   | No     |         |
| last_insert_rowid()          | No     |         |
| length(X)                    | No     |         |
| like(X,Y)                    | No     |         |
| like(X,Y,Z)                  | No     |         |
| likelihood(X,Y)              | No     |         |
| likely(X)                    | No     |         |
| load_extension(X)            | No     |         |
| load_extension(X,Y)          | No     |         |
| lower(X)                     | No     |         |
| ltrim(X)                     | No     |         |
| ltrim(X,Y)                   | No     |         |
| max(X,Y,...)                 | No     |         |
| min(X,Y,...)                 | No     |         |
| nullif(X,Y)                  | No     |         |
| octet_length(X)              | No     |         |
| printf(FORMAT,...)           | No     |         |
| quote(X)                     | No     |         |
| random()                     | No     |         |
| randomblob(N)                | No     |         |
| replace(X,Y,Z)               | No     |         |
| round(X)                     | No     |         |
| round(X,Y)                   | No     |         |
| rtrim(X)                     | No     |         |
| rtrim(X,Y)                   | No     |         |
| sign(X)                      | No     |         |
| soundex(X)                   | No     |         |
| sqlite_compileoption_get(N)  | No     |         |
| sqlite_compileoption_used(X) | No     |         |
| sqlite_offset(X)             | No     |         |
| sqlite_source_id()           | No     |         |
| sqlite_version()             | No     |         |
| substr(X,Y,Z)                | No     |         |
| substr(X,Y)                  | No     |         |
| substring(X,Y,Z)             | No     |         |
| substring(X,Y)               | No     |         |
| total_changes()              | No     |         |
| trim(X)                      | No     |         |
| trim(X,Y)                    | No     |         |
| typeof(X)                    | No     |         |
| unhex(X)                     | No     |         |
| unhex(X,Y)                   | No     |         |
| unicode(X)                   | No     |         |
| unlikely(X)                  | No     |         |
| upper(X)                     | No     |         |
| zeroblob(N)                  | No     |         |

#### Mathematical functions

| Function   | Status | Comment |
| ---------- | ------ | ------- |
| acos(X)    | No     |         |
| acosh(X)   | No     |         |
| asin(X)    | No     |         |
| asinh(X)   | No     |         |
| atan(X)    | No     |         |
| atan2(Y,X) | No     |         |
| atanh(X)   | No     |         |
| ceil(X)    | No     |         |
| ceiling(X) | No     |         |
| cos(X)     | No     |         |
| cosh(X)    | No     |         |
| degrees(X) | No     |         |
| exp(X)     | No     |         |
| floor(X)   | No     |         |
| ln(X)      | No     |         |
| log(B,X)   | No     |         |
| log(X)     | No     |         |
| log10(X)   | No     |         |
| log2(X)    | No     |         |
| mod(X,Y)   | No     |         |
| pi()       | No     |         |
| pow(X,Y)   | No     |         |
| power(X,Y) | No     |         |
| radians(X) | No     |         |
| sin(X)     | No     |         |
| sinh(X)    | No     |         |
| sqrt(X)    | No     |         |
| tan(X)     | No     |         |
| tanh(X)    | No     |         |
| trunc(X)   | No     |         |

#### Aggregate functions

| Function          | Status | Comment |
| ----------------- | ------ | ------- |
| avg(X)            | No     |         |
| count()           | No     |         |
| count(*)          | No     |         |
| group_concat(X)   | No     |         |
| group_concat(X,Y) | No     |         |
| string_agg(X,Y)   | No     |         |
| max(X)            | No     |         |
| min(X)            | No     |         |
| sum(X)            | No     |         |
| total(X)          | No     |         |

#### Date and time functions

| Function    | Status | Comment |
| ----------- | ------ | ------- |
| date()      | No     |         |
| time()      | No     |         |
| datetime()  | No     |         |
| julianday() | No     |         |
| unixepoch() | No     |         |
| strftime()  | No     |         |
| timediff()  | No     |         |

Modifiers:

| Modifier       | Status  | Comment                                     |
| -------------- | ------- | ------------------------------------------- |
| Days           | Yes     |                                             |
| Hours          | Yes     |                                             |
| Minutes        | Yes     |                                             |
| Seconds        | Yes     |                                             |
| Months         | Yes     |                                             |
| Years          | Yes     |                                             |
| TimeOffset     | Yes     |                                             |
| DateOffset     | Yes     |                                             |
| DateTimeOffset | Yes     |                                             |
| Ceiling        | Yes     |                                             |
| Floor          | No      |                                             |
| StartOfMonth   | Yes     |                                             |
| StartOfYear    | Yes     |                                             |
| StartOfDay     | Yes     |                                             |
| Weekday(N)     | Yes     |                                             |
| Auto           | No      |                                             |
| UnixEpoch      | No      |                                             |
| JulianDay      | No      |                                             |
| Localtime      | Partial | requires fixes to avoid double conversions. |
| Utc            | Partial | requires fixes to avoid double conversions. |
| Subsec         | Yes     |                                             |

#### JSON functions

| Function                           | Status | Comment |
| ---------------------------------- | ------ | ------- |
| json(json)                         | No     |         |
| jsonb(json)                        | No     |         |
| json_array(value1,value2,...)      | No     |         |
| jsonb_array(value1,value2,...)     | No     |         |
| json_array_length(json)            | No     |         |
| json_array_length(json,path)       | No     |         |
| json_error_position(json)          | No     |         |
| json_extract(json,path,...)        | No     |         |
| jsonb_extract(json,path,...)       | No     |         |
| json -> path                       | No     |         |
| json ->> path                      | No     |         |
| json_insert(json,path,value,...)   | No     |         |
| jsonb_insert(json,path,value,...)  | No     |         |
| json_object(label1,value1,...)     | No     |         |
| jsonb_object(label1,value1,...)    | No     |         |
| json_patch(json1,json2)            | No     |         |
| jsonb_patch(json1,json2)           | No     |         |
| json_pretty(json)                  | No     |         |
| json_remove(json,path,...)         | No     |         |
| jsonb_remove(json,path,...)        | No     |         |
| json_replace(json,path,value,...)  | No     |         |
| jsonb_replace(json,path,value,...) | No     |         |
| json_set(json,path,value,...)      | No     |         |
| jsonb_set(json,path,value,...)     | No     |         |
| json_type(json)                    | No     |         |
| json_type(json,path)               | No     |         |
| json_valid(json)                   | No     |         |
| json_valid(json,flags)             | No     |         |
| json_quote(value)                  | No     |         |
| json_group_array(value)            | No     |         |
| jsonb_group_array(value)           | No     |         |
| json_group_object(label,value)     | No     |         |
| jsonb_group_object(name,value)     | No     |         |
| json_each(json)                    | No     |         |
| json_each(json,path)               | No     |         |
| json_tree(json)                    | No     |         |
| json_tree(json,path)               | No     |         |

##  [SQLite journaling modes](https://www.sqlite.org/pragma.html#pragma_journal_mode)

We currently don't have plan to support the rollback journal mode as it locks the database file during writes.
Therefore, all rollback-type modes (delete, truncate, persist, memory) are marked are `Not Needed` below.

| Journal mode | Status | Comment                        |
| ------------ | ------ | ------------------------------ |
| wal          | No     |                                |
| wal2         | No     | experimental feature in sqlite |
| delete       | No     |                                |
| truncate     | No     |                                |
| persist      | No     |                                |
| memory       | No     |                                |

## Extensions

**We currently do not support testing any Turso extensions.**

### UUID

UUID's in Turso are `blobs` by default.

| Function              | Status | Comment |
| --------------------- | ------ | ------- |
| uuid4()               | No     |         |
| uuid4_str()           | No     |         |
| uuid7(X?)             | No     |         |
| uuid7_timestamp_ms(X) | No     |         |
| uuid_str(X)           | No     |         |
| uuid_blob(X)          | No     |         |

### regexp

The `regexp` extension is compatible with [sqlean-regexp](https://github.com/nalgeon/sqlean/blob/main/docs/regexp.md).

| Function                                     | Status | Comment |
| -------------------------------------------- | ------ | ------- |
| regexp(pattern, source)                      | No     |         |
| regexp_like(source, pattern)                 | No     |         |
| regexp_substr(source, pattern)               | No     |         |
| regexp_capture(source, pattern[, n])         | No     |         |
| regexp_replace(source, pattern, replacement) | No     |         |

### Vector

The `vector` extension is compatible with libSQL native vector search.

| Function                                | Status | Comment |
| --------------------------------------- | ------ | ------- |
| vector(x)                               | No     |         |
| vector32(x)                             | No     |         |
| vector64(x)                             | No     |         |
| vector_extract(x)                       | No     |         |
| vector_distance_cos(x, y)               | No     |         |
| vector_distance_l2(x, y)                | No     |         |
| vector_concat(x, y)                     | No     |         |
| vector_slice(x, start_index, end_index) | No     |         |

### Time



| Function                                                            | Status | Comment |
| ------------------------------------------------------------------- | ------ | ------- |
| time_now()                                                          | No     |         |
| time_date(year, month, day[, hour, min, sec[, nsec[, offset_sec]]]) | No     |         |
| time_get_year(t)                                                    | No     |         |
| time_get_month(t)                                                   | No     |         |
| time_get_day(t)                                                     | No     |         |
| time_get_hour(t)                                                    | No     |         |
| time_get_minute(t)                                                  | No     |         |
| time_get_second(t)                                                  | No     |         |
| time_get_nano(t)                                                    | No     |         |
| time_get_weekday(t)                                                 | No     |         |
| time_get_yearday(t)                                                 | No     |         |
| time_get_isoyear(t)                                                 | No     |         |
| time_get_isoweek(t)                                                 | No     |         |
| time_get(t, field)                                                  | No     |         |
| time_unix(sec[, nsec])                                              | No     |         |
| time_milli(msec)                                                    | No     |         |
| time_micro(usec)                                                    | No     |         |
| time_nano(nsec)                                                     | No     |         |
| time_to_unix(t)                                                     | No     |         |
| time_to_milli(t)                                                    | No     |         |
| time_to_micro(t)                                                    | No     |         |
| time_to_nano(t)                                                     | No     |         |
| time_after(t, u)                                                    | No     |         |
| time_before(t, u)                                                   | No     |         |
| time_compare(t, u)                                                  | No     |         |
| time_equal(t, u)                                                    | No     |         |
| time_add(t, d)                                                      | No     |         |
| time_add_date(t, years[, months[, days]])                           | No     |         |
| time_sub(t, u)                                                      | No     |         |
| time_since(t)                                                       | No     |         |
| time_until(t)                                                       | No     |         |
| time_trunc(t, field)                                                | No     |         |
| time_trunc(t, d)                                                    | No     |         |
| time_round(t, d)                                                    | No     |         |
| time_fmt_iso(t[, offset_sec])                                       | No     |         |
| time_fmt_datetime(t[, offset_sec])                                  | No     |         |
| time_fmt_date(t[, offset_sec])                                      | No     |         |
| time_fmt_time(t[, offset_sec])                                      | No     |         |
| time_parse(s)                                                       | No     |         |
| dur_ns()                                                            | No     |         |
| dur_us()                                                            | No     |         |
| dur_ms()                                                            | No     |         |
| dur_s()                                                             | No     |         |
| dur_m()                                                             | No     |         |
| dur_h()                                                             | No     |         |
