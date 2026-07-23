# The SQL over HTTP Protocol

Version 3

## 1. Introduction

The SQL over HTTP protocol provides remote SQL execution over plain HTTP. It
is designed for clients in serverless and edge environments where the only
available networking primitive is an HTTP request, such as `fetch()`. The
protocol requires no persistent connections, no WebSockets, and no custom
framing below HTTP.

The protocol supports:

* Executing single SQL statements with positional or named arguments.
* Executing batches of statements with conditional execution between steps.
* Interactive transactions spanning multiple HTTP requests.
* Streaming large result sets incrementally.
* Storing SQL text on the server to avoid re-sending it on every request.

### 1.1 Terminology

* **Stream**: a logical database connection held by the server on behalf of a
  client. All SQL executed on a stream shares one connection state, including
  transaction state.
* **Baton**: an opaque token that identifies a stream across HTTP requests.
* **Statement**: a single SQL statement together with its arguments.
* **Batch**: an ordered list of statements, each optionally guarded by a
  condition on the outcome of earlier statements.
* **Cursor**: a server-side iterator over the results of a batch, returned to
  the client as an incremental stream of entries.

The key words MUST, MUST NOT, SHOULD, and MAY in this document are to be
interpreted as described in RFC 2119.

All request and response bodies are JSON encoded in UTF-8. Requests with a
body SHOULD carry a `Content-Type: application/json` header. For forward
compatibility, clients MUST ignore fields they do not recognize in server
responses.

## 2. Overview

The protocol defines two endpoints:

| Method | Path           | Purpose                                          |
|--------|----------------|--------------------------------------------------|
| POST   | `/v3/pipeline` | Execute a sequence of requests on a stream       |
| POST   | `/v3/cursor`   | Execute a batch and stream its results back      |

A client interacts with the server by sending a pipeline of one or more
requests. The server executes the requests in order on a single stream and
returns one result per request. When the client needs to keep the stream
alive across HTTP requests, for example inside an interactive transaction, it
passes the baton returned by the previous response.

The server also accepts pipeline requests at `POST /v2/pipeline` for older
clients. The endpoint behaves identically, except that the `get_autocommit`
request type (section 6.7) and the `is_autocommit` batch condition (section
6.2.1) are version 3 additions; a server MAY reject them on `/v2` as a
protocol-level failure (section 9.3). New clients SHOULD use the `/v3`
paths.

The protocol has no version negotiation or discovery mechanism. This
document assumes version 3 throughout: a client simply sends its requests
to the paths of the version it implements, and a request to a path the
server does not support fails with `404 Not Found`.

## 3. Authentication

Clients authenticate with a bearer token in the `Authorization` header:

```
Authorization: Bearer <token>
```

The server validates the token before processing the request. Requests with a
missing or invalid token fail with HTTP status `401 Unauthorized`. The format
and provisioning of tokens are deployment specific and outside the scope of
this document.

## 4. Streams

### 4.1 Opening a stream

A client opens a stream implicitly by sending a request with a `null` baton
(or omitting it). The server creates a fresh stream backed by a database
connection and executes the requests on it.

### 4.2 The baton

The baton is an opaque string. Clients MUST NOT parse it or make assumptions
about its contents; its format may change at any time.

The stream lifecycle over the pipeline endpoint works as follows:

1. The client sends a pipeline request with `"baton": null`. The server
   creates a new stream.
2. If the stream must stay alive after the response (see section 4.3), the
   server returns a fresh baton in the response. Otherwise it returns
   `"baton": null` and closes the stream.
3. To continue the stream, the client sends its next pipeline request with
   the baton it received in the previous response.
4. The server MAY return a different baton value on every response. The
   client MUST always use the baton from the most recent response. A baton
   from an older response is invalid.

A `null` baton in a response always means the stream is closed. Any state
associated with it, including open transactions and stored SQL, is gone.

Batons identify streams, not endpoints: both endpoints operate on the same
streams, and a baton returned by one endpoint may be sent to the other. For
example, a client may open a transaction over the pipeline endpoint and
then stream a large result on the same stream through the cursor endpoint.

### 4.3 Stream retention

After processing a pipeline, the server keeps the stream open at least as
long as it carries client-visible state, namely when at least one of the
following holds:

* A transaction is open on the stream's connection.
* SQL text stored with `store_sql` (section 6.5) has not been closed.
* A statement changed session-scoped connection state (for example a
  `PRAGMA` that affects subsequent statements).

When none of these hold, the server MAY close the stream after the response
and return a `null` baton, even if the client did not send a `close`
request. Clients MUST therefore not assume that a stream survives between
requests unless they hold a non-null baton from the latest response. The
server MAY also keep a stateless stream open and return a baton; clients
MUST NOT take a non-null baton as evidence that a transaction or other
stream state exists.

Streams used with the cursor endpoint (section 7) issue their baton before
the batch executes, so they are always retained after the batch completes
and stay alive until closed or expired.

### 4.4 Ordering

Requests on one stream are processed strictly in order. A stream supports
only one HTTP request in flight at a time: because a request consumes the
baton and issues a new one, a client cannot legally issue two concurrent
requests on the same stream. If a request arrives carrying a baton for a
stream that is currently executing another request, the server rejects it
with a protocol-level failure (section 9.3), which need not be
distinguishable from an expired stream.

### 4.5 Expiry

The server expires streams that stay idle for longer than a server-defined
timeout. A request carrying a baton for an expired or unknown stream fails
with a non-200 status, typically `404 Not Found` (section 9.3); a baton
that is not from the most recent response fails the same way. The client
MUST treat this as fatal for the stream: any open transaction was rolled
back, and the client has to open a new stream with a `null` baton.

### 4.6 The base URL

Responses on both endpoints carry a `base_url` field. When it is non-null,
the client MUST send all subsequent requests for this stream to the returned
base URL instead of the one it used before. This allows the server to pin a
stream to a specific node. When `base_url` is `null`, the client keeps using
its current URL.

### 4.7 Interactive transactions

Interactive transactions fall out of the stream mechanism: the client sends
`BEGIN` in one pipeline, receives a baton because the stream is now in a
transaction, and sends further statements and finally `COMMIT` or `ROLLBACK`
in later pipelines carrying that baton. If the stream expires or the client
loses the baton, the server rolls the transaction back.

## 5. The pipeline endpoint

```
POST /v3/pipeline
```

### 5.1 Request

```json
{
  "baton": null,
  "requests": [
    { "type": "execute", "stmt": { "sql": "SELECT 1" } },
    { "type": "close" }
  ]
}
```

| Field      | Type             | Description                                        |
|------------|------------------|----------------------------------------------------|
| `baton`    | `string \| null` | Baton from the previous response, or `null` to open a new stream. |
| `requests` | `array`          | Requests to execute in order (section 6).          |

Clients SHOULD NOT send an empty `requests` array; the server MAY respond
to an empty pipeline by closing the stream.

### 5.2 Response

```json
{
  "baton": null,
  "base_url": null,
  "results": [
    { "type": "ok", "response": { "type": "execute", "result": { "...": "..." } } },
    { "type": "ok", "response": { "type": "close" } }
  ]
}
```

| Field      | Type             | Description                                        |
|------------|------------------|----------------------------------------------------|
| `baton`    | `string \| null` | Baton for the next request, or `null` if the stream is closed. |
| `base_url` | `string \| null` | Base URL for subsequent requests on this stream (section 4.6). |
| `results`  | `array`          | One result per request, in request order.          |

Each element of `results` is one of:

```json
{ "type": "ok", "response": { "type": "<request type>", "...": "..." } }
{ "type": "error", "error": { "message": "...", "code": "...", "extended_code": "..." } }
```

### 5.3 Execution model

The server executes the requests of a pipeline strictly in order. A failed
request produces an `error` result but does not abort the pipeline: the
server still executes the remaining requests. Clients that need
all-or-nothing semantics should use a transaction, a `batch` request with
conditions, or a `sequence` request.

The whole HTTP request fails (with a non-200 status and no `results`) for
protocol-level problems, including:

* a malformed request body,
* an invalid, expired, or already-consumed baton,
* an unknown database or failed authentication,
* a `close` request that is not the last request of the pipeline,
* a malformed batch condition: a reference to the current or a later step,
  or nesting beyond the server's depth limit (section 6.2.1),
* a request timeout.

Such a failure does not mean that nothing was executed. Requests that
completed before the failure keep their effects, including committed
writes, but their results are discarded, the stream is closed, and any
transaction left open on it is rolled back.

The server responds with HTTP status `200 OK` whenever the pipeline itself
was processed, even if every request in it failed.

## 6. Request types

Every request is a JSON object with a `type` field naming the request type.
Every successful result carries a `response` object whose `type` matches the
request.

### 6.1 `execute`

Executes a single SQL statement.

Request:

```json
{
  "type": "execute",
  "stmt": {
    "sql": "INSERT INTO users (name, age) VALUES (?, ?)",
    "args": [
      { "type": "text", "value": "Alice" },
      { "type": "integer", "value": "30" }
    ],
    "want_rows": false
  }
}
```

The `stmt` object is described in section 8.1.

Response:

```json
{
  "type": "execute",
  "result": {
    "cols": [],
    "rows": [],
    "affected_row_count": 1,
    "last_insert_rowid": "42",
    "rows_read": 0,
    "rows_written": 1,
    "query_duration_ms": 0.3
  }
}
```

The `result` object is described in section 8.4.

### 6.2 `batch`

Executes a batch of statements. Each step may carry a condition that decides,
based on the outcome of earlier steps, whether the step runs.

Request:

```json
{
  "type": "batch",
  "batch": {
    "steps": [
      { "stmt": { "sql": "BEGIN" } },
      {
        "condition": { "type": "ok", "step": 0 },
        "stmt": { "sql": "INSERT INTO t VALUES (1)" }
      },
      {
        "condition": { "type": "ok", "step": 1 },
        "stmt": { "sql": "COMMIT" }
      },
      {
        "condition": { "type": "not", "cond": { "type": "ok", "step": 2 } },
        "stmt": { "sql": "ROLLBACK" }
      }
    ]
  }
}
```

Each step is:

| Field       | Type     | Description                                          |
|-------------|----------|------------------------------------------------------|
| `condition` | `object` | Optional condition (section 6.2.1). A step without a condition always runs. |
| `stmt`      | `object` | The statement to execute (section 8.1).              |

Response:

```json
{
  "type": "batch",
  "result": {
    "step_results": [ { "...": "..." }, null ],
    "step_errors": [ null, { "message": "...", "code": "..." } ]
  }
}
```

`step_results` and `step_errors` are arrays with exactly one entry per step,
in step order. For each step, exactly one of the following holds:

* The step executed successfully: its entry in `step_results` is a statement
  result (section 8.4) and its entry in `step_errors` is `null`.
* The step failed: its entry in `step_results` is `null` and its entry in
  `step_errors` is an error object (section 9.1).
* The step was skipped because its condition evaluated to false: both entries
  are `null`.

A failed step does not abort the batch by itself. Later steps run or are
skipped purely according to their conditions.

The batch result also carries a `replication_index` field, which is
reserved (section 8.4); clients MAY ignore it.

#### 6.2.1 Conditions

Step indices in conditions are zero-based and MUST refer to an earlier step
of the same batch. A condition that refers to its own step or a later step
is a protocol error and fails the whole HTTP request (section 5.3).

| Condition                                 | True when                                     |
|-------------------------------------------|-----------------------------------------------|
| `{ "type": "ok", "step": <n> }`           | Step `n` executed and succeeded.              |
| `{ "type": "error", "step": <n> }`        | Step `n` did not succeed: it failed or was skipped. |
| `{ "type": "not", "cond": <c> }`          | Condition `c` is false.                       |
| `{ "type": "and", "conds": [<c>, ...] }`  | All listed conditions are true.               |
| `{ "type": "or", "conds": [<c>, ...] }`   | At least one listed condition is true.        |
| `{ "type": "is_autocommit" }`             | The stream's connection is in autocommit mode. |

`error` is the negation of `ok`, not a test that the step ran: for a
skipped step, `ok` is false and `error` is true. The `ROLLBACK` guard in
the example above therefore fires both when `COMMIT` failed and when it was
never reached.

The server limits the nesting depth of conditions and rejects batches that
exceed the limit as a protocol error.

### 6.3 `sequence`

Executes a sequence of SQL statements separated by semicolons, in order. Rows
produced by the statements are discarded. Execution stops at the first
statement that fails, and the request then returns that statement's error.

Request:

```json
{
  "type": "sequence",
  "sql": "CREATE TABLE t (x); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)"
}
```

| Field    | Type      | Description                                        |
|----------|-----------|----------------------------------------------------|
| `sql`    | `string`  | SQL text containing one or more statements. Exactly one of `sql` and `sql_id` MUST be set. |
| `sql_id` | `integer` | Id of a stored SQL text (section 6.5).             |

Response on success:

```json
{ "type": "sequence" }
```

### 6.4 `describe`

Describes a SQL statement without executing it.

Request:

```json
{ "type": "describe", "sql": "SELECT * FROM users WHERE id = :id" }
```

| Field    | Type      | Description                                        |
|----------|-----------|----------------------------------------------------|
| `sql`    | `string`  | The statement to describe. Exactly one of `sql` and `sql_id` MUST be set. |
| `sql_id` | `integer` | Id of a stored SQL text (section 6.5).             |

Response:

```json
{
  "type": "describe",
  "result": {
    "params": [ { "name": "id" } ],
    "cols": [ { "name": "id", "decltype": "INTEGER" }, { "name": "name", "decltype": "TEXT" } ],
    "is_explain": false,
    "is_readonly": true
  }
}
```

| Field         | Type      | Description                                     |
|---------------|-----------|-------------------------------------------------|
| `params`      | `array`   | One entry per bind parameter, in parameter order. Each entry has an optional `name`. Positional parameters have no name. |
| `cols`        | `array`   | Columns of the result set (section 8.3).        |
| `is_explain`  | `boolean` | Whether the statement is an `EXPLAIN` statement. |
| `is_readonly` | `boolean` | Whether the statement is read-only.             |

### 6.5 `store_sql`

Stores a SQL text on the stream under a client-chosen id. Later requests on
the same stream may refer to it with `sql_id` instead of repeating the text.
Stored SQL is scoped to the stream and disappears when the stream closes.

Request:

```json
{ "type": "store_sql", "sql_id": 1, "sql": "SELECT * FROM users WHERE id = ?" }
```

Response on success:

```json
{ "type": "store_sql" }
```

The server limits the number of SQL texts stored on one stream and returns an
error result when the limit is exceeded. Storing a `sql_id` that is already
in use replaces the previously stored text. Note that servers implementing
the predecessor of this protocol reject a `sql_id` that is already in use
instead of replacing it; portable clients SHOULD send `close_sql` before
reusing an id.

### 6.6 `close_sql`

Removes a stored SQL text from the stream. Closing an unknown `sql_id` is not
an error.

Request:

```json
{ "type": "close_sql", "sql_id": 1 }
```

Response:

```json
{ "type": "close_sql" }
```

### 6.7 `get_autocommit`

Returns whether the stream's connection is in autocommit mode, that is,
whether no explicit transaction is open. This request type is a version 3
addition.

Request:

```json
{ "type": "get_autocommit" }
```

Response:

```json
{ "type": "get_autocommit", "is_autocommit": true }
```

### 6.8 `close`

Closes the stream. Any open transaction is rolled back and stored SQL is
released. If a pipeline contains a `close` request, it MUST be the last
request of the pipeline; otherwise the server rejects the whole pipeline.

Request:

```json
{ "type": "close" }
```

Response:

```json
{ "type": "close" }
```

Clients SHOULD close streams they no longer need instead of letting them
expire, since streams hold server-side resources.

## 7. The cursor endpoint

```
POST /v3/cursor
```

The cursor endpoint executes a batch and returns its results incrementally,
so a client can process rows as they arrive without buffering the whole
result set. It is the streaming counterpart of the `batch` request type.

### 7.1 Request

```json
{
  "baton": null,
  "batch": {
    "steps": [
      { "stmt": { "sql": "SELECT * FROM big_table" } }
    ]
  }
}
```

| Field   | Type             | Description                                    |
|---------|------------------|------------------------------------------------|
| `baton` | `string \| null` | Baton from a previous response, or `null` to open a new stream. |
| `batch` | `object`         | The batch to execute, as in section 6.2.       |

### 7.2 Response

The response body is a stream of JSON values separated by newline characters
(`\n`). The client MUST process it incrementally, line by line.

The first line is the cursor response:

```json
{ "baton": "<baton>", "base_url": null }
```

Unlike the pipeline endpoint, the cursor endpoint issues the baton at the
start of the request, before the batch has finished. The baton and `base_url`
follow the same rules as in sections 4.2 and 4.6.

Every subsequent line is a cursor entry, a JSON object tagged with a `type`
field. Entries describing the steps of the batch arrive in execution order.

#### 7.2.1 `step_begin`

Marks the start of a step's results and carries the result columns:

```json
{ "type": "step_begin", "step": 0, "cols": [ { "name": "id", "decltype": "INTEGER" } ] }
```

`step` is the zero-based index of the step within the batch. Steps that are
skipped because their condition evaluated to false produce no entries at all,
so consecutive `step_begin` entries need not have consecutive step indices.

Only `step_begin` and `step_error` entries carry a `step` field. `row` and
`step_end` entries always apply to the step opened by the most recent
`step_begin`.

#### 7.2.2 `row`

One row of the current step, between its `step_begin` and `step_end`:

```json
{ "type": "row", "row": [ { "type": "integer", "value": "1" } ] }
```

`row` is an array of values (section 8.2), one per column.

#### 7.2.3 `step_end`

Marks the successful end of the current step:

```json
{ "type": "step_end", "affected_row_count": 0, "last_insert_rowid": null }
```

| Field                | Type              | Description                       |
|----------------------|-------------------|-----------------------------------|
| `affected_row_count` | `integer`         | As in section 8.4.                |
| `last_insert_rowid`  | `number \| null`  | Rowid of the last inserted row, or `null` if the step did not insert a row. |

Unlike the statement results of the pipeline endpoint (section 8.4), the
server emits `last_insert_rowid` here as a JSON number. Clients MUST accept
both a number and a decimal string in this field, and SHOULD parse it with
64-bit precision.

#### 7.2.4 `step_error`

Reports that a step failed. A step that fails after its `step_begin` entry
produces a `step_error` instead of `step_end`; a step that fails before
producing any results produces only the `step_error` entry.

```json
{ "type": "step_error", "step": 0, "error": { "message": "...", "code": "..." } }
```

As with the `batch` request type, a failed step does not abort the batch.

#### 7.2.5 `error`

Reports a fatal error that terminates the cursor. No further entries follow.

```json
{ "type": "error", "error": { "message": "...", "code": "..." } }
```

When the server fails mid-stream, it aborts the response body, with or
without emitting an `error` entry first. Clients rely on the transport to
report such an abort as a truncated body. A body that the transport
delivers completely and that did not end with an `error` entry is a
successfully completed cursor, whether or not it carries a final
`replication_index` entry (section 7.2.6).

#### 7.2.6 `replication_index`

The server SHOULD emit a `replication_index` entry as the final entry of a
successfully completed cursor:

```json
{ "type": "replication_index", "replication_index": null }
```

The `replication_index` value is reserved and may be a string, a number, or
`null`. Clients MAY ignore this entry and MUST NOT require it: the server
MAY end a successfully completed cursor without it. Clients MUST ignore
entry types they do not recognize, to allow future extensions.

## 8. Data types

### 8.1 Statements

A statement object describes one SQL statement and its arguments:

| Field        | Type      | Description                                      |
|--------------|-----------|--------------------------------------------------|
| `sql`        | `string`  | The SQL text. Exactly one of `sql` and `sql_id` MUST be set. |
| `sql_id`     | `integer` | Id of a SQL text previously stored on this stream with `store_sql`. |
| `args`       | `array`   | Positional argument values, in order. Optional.  |
| `named_args` | `array`   | Named arguments as `{ "name": <string>, "value": <value> }` objects. Optional. |
| `want_rows`  | `boolean` | Whether the response should include result rows. Defaults to `true`. When `false`, the server executes the statement but returns no rows. |

A statement MUST NOT pass both positional and named arguments: setting both
`args` and `named_args` to non-empty arrays is an error, and the server
rejects such statements. An empty array is equivalent to omitting the
field. The number of arguments MUST match the number of bind parameters in
the statement exactly.

Named arguments bind to SQL parameters of the forms `:name`, `@name`, and
`$name`. The `name` field may be sent with or without the prefix character,
and the server matches names with the prefix stripped: `"id"`, `":id"`, and
`"@id"` all bind the parameter `:id` (or `@id`, or `$id`). A statement that
mixes named parameters with positional `?` parameters cannot be bound with
`named_args` and is rejected.

### 8.2 Values

SQL values are encoded as JSON objects tagged with a `type` field:

| Type      | Encoding                                             | Example                                     |
|-----------|------------------------------------------------------|---------------------------------------------|
| `null`    | `{ "type": "null" }`                                 | `{ "type": "null" }`                        |
| `integer` | 64-bit signed integer, as a decimal **string**       | `{ "type": "integer", "value": "42" }`      |
| `float`   | 64-bit float, as a JSON number; `null` for non-finite values | `{ "type": "float", "value": 1.5 }`  |
| `text`    | UTF-8 string                                         | `{ "type": "text", "value": "hello" }`      |
| `blob`    | base64 (standard alphabet), in the `base64` field    | `{ "type": "blob", "base64": "3q2+7w" }`    |

Integers are transported as strings because JSON numbers cannot represent the
full 64-bit range faithfully.

JSON has no representation for NaN or infinite floats. Clients MUST NOT
send non-finite float values. When a query produces a non-finite float, the
server encodes it as `{ "type": "float", "value": null }`; clients SHOULD
decode such a value as NaN.

The server emits base64 without padding. Clients MUST accept unpadded
base64; the server accepts both padded and unpadded input.

### 8.3 Columns

Result columns are described by:

| Field      | Type             | Description                                  |
|------------|------------------|----------------------------------------------|
| `name`     | `string \| null` | Column name, or `null` if the column has none. |
| `decltype` | `string \| null` | Declared type of the column, or `null` when not applicable (for example expressions). |

### 8.4 Statement results

A successful statement execution produces:

| Field                | Type             | Description                            |
|----------------------|------------------|----------------------------------------|
| `cols`               | `array`          | Result columns (section 8.3).          |
| `rows`               | `array`          | Result rows. Each row is an array of values (section 8.2), one per column. Empty when `want_rows` is `false`. |
| `affected_row_count` | `integer`        | Number of rows changed by the statement. Zero for statements that do not modify rows. |
| `last_insert_rowid`  | `string \| null` | Rowid of the last inserted row, as a decimal string, or `null` if the statement did not insert a row. |
| `rows_read`          | `integer`        | Number of rows read during execution.  |
| `rows_written`       | `integer`        | Number of rows written during execution. |
| `query_duration_ms`  | `number`         | Server-side execution time in milliseconds. |
| `replication_index`  | `string \| null` | Reserved. Clients MAY ignore it.       |

## 9. Errors

### 9.1 Error objects

All errors reported inside pipeline results, batch step errors, and cursor
entries share one shape:

| Field           | Type     | Description                                     |
|-----------------|----------|-------------------------------------------------|
| `message`       | `string` | Human-readable description of the error.        |
| `code`          | `string` | Machine-readable error code.                    |
| `extended_code` | `string` | Optional. More specific error code, present for errors that have one. |

Example:

```json
{
  "message": "UNIQUE constraint failed: users.email",
  "code": "SQLITE_CONSTRAINT",
  "extended_code": "SQLITE_CONSTRAINT_UNIQUE"
}
```

### 9.2 Error codes

The `code` field commonly takes one of the following values:

| Code                  | Meaning                                            |
|-----------------------|----------------------------------------------------|
| `SQL_PARSE_ERROR`     | The SQL text could not be parsed, or contained no statement. |
| `SQL_MANY_STATEMENTS` | Multiple statements were given where one was expected. |
| `SQL_INPUT_ERROR`     | The SQL text or its arguments were invalid.        |
| `BLOCKED`             | The statement was blocked by server policy.        |
| `SQLITE_INTERNAL`     | Internal database error.                           |
| `SQLITE_PERM`         | Access permission denied.                          |
| `SQLITE_ABORT`        | The operation was aborted.                         |
| `SQLITE_BUSY`         | The database is busy.                              |
| `SQLITE_LOCKED`       | A table in the database is locked.                 |
| `SQLITE_NOMEM`        | The server ran out of memory.                      |
| `SQLITE_READONLY`     | Attempt to write a read-only database.             |
| `SQLITE_INTERRUPT`    | The operation was interrupted.                     |
| `SQLITE_IOERR`        | Disk I/O error.                                    |
| `SQLITE_CORRUPT`      | The database file is malformed.                    |
| `SQLITE_NOTFOUND`     | Object not found.                                  |
| `SQLITE_FULL`         | The database is full.                              |
| `SQLITE_CANTOPEN`     | The database file could not be opened.             |
| `SQLITE_PROTOCOL`     | Database lock protocol error.                      |
| `SQLITE_SCHEMA`       | The database schema changed.                       |
| `SQLITE_TOOBIG`       | A string or blob exceeds the size limit.           |
| `SQLITE_CONSTRAINT`   | A constraint was violated.                         |
| `SQLITE_MISMATCH`     | Data type mismatch.                                |
| `SQLITE_MISUSE`       | API misuse.                                        |
| `SQLITE_NOLFS`        | Large file support is unavailable.                 |
| `SQLITE_AUTH`         | Authorization denied.                              |
| `SQLITE_RANGE`        | Bind parameter index out of range.                 |
| `SQLITE_NOTADB`       | The file is not a database.                        |
| `SQLITE_UNKNOWN`      | Unknown error.                                     |

Constraint violations carry an `extended_code` identifying the constraint:

| Extended code                  | Meaning                                  |
|--------------------------------|------------------------------------------|
| `SQLITE_CONSTRAINT_CHECK`      | A `CHECK` constraint failed.             |
| `SQLITE_CONSTRAINT_COMMITHOOK` | A commit hook aborted the transaction.   |
| `SQLITE_CONSTRAINT_DATATYPE`   | A `STRICT` table rejected a value of the wrong type. |
| `SQLITE_CONSTRAINT_FOREIGNKEY` | A foreign key constraint failed.         |
| `SQLITE_CONSTRAINT_FUNCTION`   | A constraint raised by an SQL function.  |
| `SQLITE_CONSTRAINT_NOTNULL`    | A `NOT NULL` constraint failed.          |
| `SQLITE_CONSTRAINT_PINNED`     | An `UPDATE` trigger deleted the row being updated. |
| `SQLITE_CONSTRAINT_PRIMARYKEY` | A `PRIMARY KEY` constraint failed.       |
| `SQLITE_CONSTRAINT_ROWID`      | A rowid was not unique.                  |
| `SQLITE_CONSTRAINT_TRIGGER`    | A `RAISE` function inside a trigger fired. |
| `SQLITE_CONSTRAINT_UNIQUE`     | A `UNIQUE` constraint failed.            |
| `SQLITE_CONSTRAINT_VTAB`       | A virtual table constraint failed.       |

Clients MUST tolerate codes and extended codes not listed here.

### 9.3 HTTP status codes

Errors that occur while executing SQL are reported in-band, inside a `200 OK`
response, as described in the previous sections. The server uses non-200
status codes for failures of the HTTP request itself:

| Status                      | Meaning                                       |
|-----------------------------|-----------------------------------------------|
| `400 Bad Request`           | Malformed request body, malformed baton, or other protocol violation. |
| `401 Unauthorized`          | Missing or invalid authentication token.      |
| `403 Forbidden`             | The database exists but access to it is blocked. |
| `404 Not Found`             | Unknown endpoint, unknown database, or a baton referring to an unknown or expired stream (section 4.5). |
| `408 Request Timeout`       | The request timed out.                        |
| `429 Too Many Requests`     | The client is rate limited and should retry later. |
| `500 Internal Server Error` | Unexpected server-side failure.               |
| `503 Service Unavailable`   | The server is temporarily out of resources.   |

Error bodies of non-200 responses are JSON objects with at least an `error`
field containing a human-readable message:

```json
{ "error": "stream not found: ..." }
```

Clients SHOULD surface the message but MUST NOT rely on the exact body shape
of non-200 responses. Clients SHOULD treat any non-200 response to a request
that carried a baton as fatal for that stream.

## 10. Security considerations

The protocol provides no confidentiality or integrity protection of its
own; it relies entirely on the transport. Production deployments MUST
serve the endpoints over HTTPS, since the bearer token, the SQL text, and
all result data are otherwise exposed to the network path.

The bearer token is the sole client credential. Clients SHOULD keep tokens
out of URLs, logs, and error reports. Batons are not a substitute for
authentication: every request, with or without a baton, is authenticated
by the bearer token.

Error messages (section 9.1) may echo SQL text, schema names, and data
values, for example the failing column of a constraint violation.
Applications that log server errors or forward them to end users SHOULD
take this into account.

## Appendix A. Examples

### A.1 Simple query

Request:

```
POST /v3/pipeline
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "baton": null,
  "requests": [
    {
      "type": "execute",
      "stmt": {
        "sql": "SELECT id, name FROM users WHERE id = ?",
        "args": [ { "type": "integer", "value": "1" } ],
        "want_rows": true
      }
    },
    { "type": "close" }
  ]
}
```

Response:

```json
{
  "baton": null,
  "base_url": null,
  "results": [
    {
      "type": "ok",
      "response": {
        "type": "execute",
        "result": {
          "cols": [
            { "name": "id", "decltype": "INTEGER" },
            { "name": "name", "decltype": "TEXT" }
          ],
          "rows": [
            [ { "type": "integer", "value": "1" }, { "type": "text", "value": "Alice" } ]
          ],
          "affected_row_count": 0,
          "last_insert_rowid": null,
          "rows_read": 1,
          "rows_written": 0,
          "query_duration_ms": 0.2
        }
      }
    },
    { "type": "ok", "response": { "type": "close" } }
  ]
}
```

### A.2 Interactive transaction

First request opens a stream and begins a transaction:

```json
{
  "baton": null,
  "requests": [
    { "type": "execute", "stmt": { "sql": "BEGIN" } },
    { "type": "execute", "stmt": { "sql": "UPDATE accounts SET balance = balance - 100 WHERE id = 1" } }
  ]
}
```

The response carries a baton because the transaction keeps the stream open:

```json
{
  "baton": "m7...q2:1",
  "base_url": null,
  "results": [ "..." ]
}
```

The second request continues on the same stream and commits:

```json
{
  "baton": "m7...q2:1",
  "requests": [
    { "type": "execute", "stmt": { "sql": "UPDATE accounts SET balance = balance + 100 WHERE id = 2" } },
    { "type": "execute", "stmt": { "sql": "COMMIT" } },
    { "type": "close" }
  ]
}
```

The final response returns `"baton": null` because the stream was closed.

### A.3 Streaming a large result

Request:

```
POST /v3/cursor
```

```json
{
  "baton": null,
  "batch": {
    "steps": [ { "stmt": { "sql": "SELECT id FROM big_table" } } ]
  }
}
```

Response body (one JSON value per line):

```json
{ "baton": "k4...f8:1", "base_url": null }
{ "type": "step_begin", "step": 0, "cols": [ { "name": "id", "decltype": "INTEGER" } ] }
{ "type": "row", "row": [ { "type": "integer", "value": "1" } ] }
{ "type": "row", "row": [ { "type": "integer", "value": "2" } ] }
{ "type": "row", "row": [ { "type": "integer", "value": "3" } ] }
{ "type": "step_end", "affected_row_count": 0, "last_insert_rowid": null }
{ "type": "replication_index", "replication_index": null }
```
