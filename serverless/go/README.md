# Turso Serverless Driver for Go

A pure Go driver for Turso Cloud that speaks the [SQL over HTTP
protocol](https://github.com/tursodatabase/turso/blob/main/serverless/PROTOCOL.md).
Designed for serverless and edge environments: no persistent connections,
no cgo, no native libraries, just HTTP requests from the standard library.

The driver implements `database/sql` and mirrors the embedded
[`turso.tech/database/tursogo`](https://github.com/tursodatabase/turso/tree/main/bindings/go)
driver, so the same application code can run against a local database or
Turso Cloud.

## Installation

```console
$ go get turso.tech/database/tursogo-serverless
```

## Usage

```go
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "turso.tech/database/tursogo-serverless"
)

func main() {
	dsn := os.Getenv("TURSO_DATABASE_URL") + "?auth_token=" + os.Getenv("TURSO_AUTH_TOKEN")
	db, err := sql.Open("turso-serverless", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("INSERT INTO users (name) VALUES (?)", "Alice")

	rows, _ := db.Query("SELECT id, name FROM users")
	defer rows.Close()
	for rows.Next() {
		var id int64
		var name string
		rows.Scan(&id, &name)
		fmt.Println(id, name)
	}
}
```

To keep the auth token out of the connection string, open the database
through a connector instead:

```go
import turso "turso.tech/database/tursogo-serverless"

db := sql.OpenDB(turso.NewConnector(url, authToken))
```

For a database encrypted with a customer-managed key, pass the key through
the connector:

```go
db := sql.OpenDB(turso.NewConnector(url, authToken).WithRemoteEncryptionKey(key))
```

The key can also be given as a `remote_encryption_key` query parameter in
the connection string. Like any query parameter its value must be
URL-encoded — base64 keys contain `+`, `/`, and `=`, which query-string
decoding otherwise mangles — and it shares the connection string's
exposure in logs and error messages, so prefer the connector:

```go
db, err := sql.Open("turso-serverless", dsn+"&remote_encryption_key="+url.QueryEscape(key))
```

Interactive transactions span multiple HTTP requests; the server keeps the
connection state alive between them:

```go
tx, err := db.Begin()
tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
tx.Exec("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
tx.Commit()
```

## Conformance tests

The test suite runs against a live database. Point it at a Turso Cloud
instance:

```console
$ export TURSO_DATABASE_URL=libsql://<your-db>.turso.io
$ export TURSO_AUTH_TOKEN=<your-token>
$ go test ./...
```

The tests that need a server skip themselves when the environment
variables are not set.

## Dedicated sessions for gateways

Use one `Connection` per frontend connection when session state must survive
across autocommit statements. Configure each connector before opening it:

```go
connector := turso.NewConnector("http://db-router:8080", projectJWT).
    WithHTTPClient(&http.Client{Transport: transport}).
    WithRequestHeaders(http.Header{"Host": {projectHost}}).
    WithSessionRetention()
conn, err := connector.OpenConnection(ctx)
if err != nil {
    return err
}
defer conn.Close()

desc, err := conn.DescribeContext(ctx, "SELECT name FROM users WHERE id = ?")
// desc.Columns contains nullable Name/Decltype; desc.Params contains nullable
// parameter names. IsReadOnly and IsExplain are supplied by the server.
```

`WithSessionRetention` stores an unused SQL string on the backend, keeping the
stream alive even when no transaction is open. It adds one opening request.
The driver reuses the latest baton for queries, Describe, and transactions.
The backend must implement `store_sql`, `describe`, cursors, and autocommit probes.
Parameter types are not provided by the protocol; gateways must resolve their
own protocol's type metadata. Declared column types can also be absent.

`WithHTTPClient` copies the client configuration and shares its transport.
Use a dedicated client without a cookie jar for JWT-authenticated gateways.
`WithRequestHeaders` copies headers and sends them on every request, including
cleanup. Explicit headers override defaults, including Authorization, Content-Type,
and the encryption key header. Header names are case-insensitive; duplicate names
with different casing are rejected. Host requires exactly one non-empty value and
changes the HTTP Host, not the URL destination or TLS server name. Other headers
can have multiple values; an empty value slice suppresses the driver's default.

HTTP redirects follow the configured HTTP client's policy (Go's default policy
when no custom client is supplied). The driver adopts server-provided `base_url`
values for subsequent requests, including changes to another origin. Headers and credentials
belong to each connector; existing connections keep their own header copy.


`conn.ExecuteContext(ctx, query, args)` executes one statement once and returns a
buffered `StatementResult` with `Columns []Column`, `Rows [][]any`, and
`RowsAffected int64`. Column names and declared types come from the actual
execution response and remain nullable. Rows contain raw decoded `int64`,
`float64`, `string`, `[]byte`, or `nil` values; date/time text stays unchanged.
The affected count comes from the backend, including for DML with `RETURNING`.
This API returns no partial result on error and does not issue a Describe request.

```go
result, err := conn.ExecuteContext(ctx,
    "UPDATE users SET name = ? WHERE id = ? RETURNING id, name",
    []driver.NamedValue{{Ordinal: 1, Value: "Alice"}, {Ordinal: 2, Value: int64(1)}},
)
if err != nil {
    return err
}
// Encode result.Rows using result.Columns; use result.RowsAffected for command tags.
```

`conn.QueryContext(ctx, query, args)` buffers the complete result before returning
rows. Arguments use `[]driver.NamedValue` with `driver.Value` values; no
argument conversion is performed by this direct API. Queries retain the existing
`database/sql` date/time conversion behavior. `conn.Autocommit()` reports the
state after the request completes.

Cancel the query context to interrupt an in-flight HTTP request. Close the
connection to attempt backend cleanup, bounded to three seconds; failed cleanup
relies on backend session expiry.

Cursor responses may omit `replication_index` if the executed steps completed.
A clean HTTP end after the caller step, with no autocommit probe, means the
conditional probe was skipped. It does not require another HTTP request.

After an HTTP request failure, retained sessions and connections already in a
transaction are permanently invalidated; later operations return `ErrSessionLost`.
Default autocommit connections clear the baton and may open a new stream on a
later call, matching the existing driver behavior. The failed request is never
retried automatically. Applications needing session continuity across failures
should use `WithSessionRetention` and reconnect after session loss.
Malformed or incomplete responses invalidate the connection in either mode. A null
cursor baton is a protocol error and invalidates the connection, because cursor
responses must retain their backend session.
`database/sql` discards invalid connections when they return to its pool.
If an autocommit refresh fails, the original statement result or SQL error is
preserved. Recovery follows the same rules: default autocommit connections can
recover after an HTTP failure; retained sessions, known transactions, and malformed
responses leave the connection unusable.

PostgreSQL message translation, parameter type inference, and COPY text encoding
remain the gateway's responsibility.

## HTTP errors and authentication checks

Returned non-2xx HTTP failures from pipeline and cursor requests are
`*turso.HTTPError`, with `StatusCode int` and `Message string`. Use `errors.As`
to inspect them, including through wrapped errors. Hrana statement and stream
errors remain `*turso.Error`; transport errors preserve their underlying cause
and do not match `*turso.HTTPError`.

Ordinary `OpenConnection` is lazy: call `Ping` to check authentication. For example,
a gateway can pass its project secret through the header expected by its backend
and map authentication failures separately from backend/transport failures:

```go
func checkAccess(ctx context.Context, url, secret string) int {
    conn, err := turso.NewConnector(url, "").
        WithRequestHeaders(http.Header{"X-API-Key": {secret}}).
        OpenConnection(ctx)
    if err == nil {
        defer conn.Close()
        err = conn.Ping(ctx)
    }
    if err == nil {
        return http.StatusOK
    }
    var httpErr *turso.HTTPError
    if errors.As(err, &httpErr) &&
        (httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
        return http.StatusUnauthorized
    }
    return http.StatusBadGateway
}
```

The example uses `context`, `errors`, `net/http`, and the `turso` import above;
replace `X-API-Key` with your backend's authentication header.
`WithSessionRetention` also makes an opening HTTP request, whose failures use the
same type. `Close` attempts cleanup but intentionally ignores cleanup errors.

Error text remains `turso: HTTP status <code>` with `: <message>` when available.
The driver reads at most 1 MiB and extracts only a string `error` field (preferred)
or `message` field from valid JSON. Empty, malformed, or truncated JSON gives an
empty message; raw HTML and other response bodies are never included. Parsed
messages are still untrusted server text; escape them as appropriate when displaying
them and avoid forwarding backend details to callers.
