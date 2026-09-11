// Compiled twins of the README connection examples. They have no Output
// comment, so `go test` compiles them without running them: a README
// snippet that stops compiling, or drops the URL-escaping the DSN form
// needs, fails here first.

package turso_test

import (
	"database/sql"
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
