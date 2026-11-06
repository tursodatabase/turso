// Compiled twins of the README connection examples. They have no Output
// comment, so `go test` compiles them without running them: a README
// snippet that stops compiling, or drops the URL-escaping the DSN form
// needs, fails here first.

package turso_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	turso "turso.tech/database/tursogo-serverless"
)

func ExampleNewConnector() {
	dbURL := "turso://my-db.turso.io"
	authToken := "token"

	db := sql.OpenDB(turso.NewConnector(dbURL, authToken))
	defer db.Close()
}

func ExampleConnector_WithRemoteEncryptionKey() {
	dbURL := "turso://my-db.turso.io"
	authToken := "token"
	key := "aB3+xY9/zQ=="

	db := sql.OpenDB(turso.NewConnector(dbURL, authToken).WithRemoteEncryptionKey(key))
	defer db.Close()
}

// The DSN form of the encryption key. Base64 keys contain +, /, and =,
// which query-string decoding mangles, so the value must be URL-encoded.
func Example_remoteEncryptionKeyDSN() {
	dsn := "turso://my-db.turso.io?auth_token=token"
	key := "aB3+xY9/zQ=="

	db, err := sql.Open("turso-serverless", dsn+"&remote_encryption_key="+url.QueryEscape(key))
	if err != nil {
		panic(err)
	}
	defer db.Close()
}

func ExampleConnector_OpenConnection() {
	ctx := context.Background()
	conn, err := turso.NewConnector("http://db-router:8080", "project-jwt").
		WithHTTPClient(&http.Client{Transport: http.DefaultTransport}).
		WithRequestHeaders(http.Header{"Host": {"project.example"}}).WithSessionRetention().OpenConnection(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	desc, err := conn.DescribeContext(ctx, "SELECT name FROM users WHERE id = ?")
	if err != nil {
		panic(err)
	}
	_ = desc.Columns
	rows, err := conn.QueryContext(ctx, "SELECT name FROM users WHERE id = ?", []driver.NamedValue{{Ordinal: 1, Value: int64(1)}})
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	values := make([]driver.Value, len(rows.Columns()))
	for {
		err := rows.Next(values)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		// Encode values into the frontend protocol here.
	}
}

func ExampleConnection_ExecuteContext() {
	ctx := context.Background()
	conn, err := turso.NewConnector("https://my-db.turso.io", "token").OpenConnection(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	result, err := conn.ExecuteContext(ctx, "UPDATE users SET name = ? WHERE id = ? RETURNING id, name",
		[]driver.NamedValue{{Ordinal: 1, Value: "Alice"}, {Ordinal: 2, Value: int64(1)}})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Columns, result.Rows, result.RowsAffected)
}

func ExampleHTTPError() {
	ctx := context.Background()
	conn, err := turso.NewConnector("https://my-db.turso.io", "").
		WithRequestHeaders(http.Header{"X-API-Key": {"project-secret"}}).
		OpenConnection(ctx)
	if err == nil {
		defer conn.Close()
		err = conn.Ping(ctx)
	}
	status := http.StatusOK
	if err != nil {
		status = http.StatusBadGateway
		var httpErr *turso.HTTPError
		if errors.As(err, &httpErr) &&
			(httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
			status = http.StatusUnauthorized
		}
	}
	fmt.Println(status)
}
