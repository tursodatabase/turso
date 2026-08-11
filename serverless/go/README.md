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
