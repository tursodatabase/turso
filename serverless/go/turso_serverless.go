package tursogo_serverless

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
)

// ConnectorOption configures a TursoConnector. The name and shape mirror the
// embedded turso driver so code written against it compiles here.
type ConnectorOption func(*TursoConnector)

// WithBusyTimeout is accepted for parity with the embedded driver. The
// serverless HTTP transport has no busy handler, so the value is applied as
// the per-request (query) timeout instead. Use 0 to disable.
func WithBusyTimeout(ms int) ConnectorOption {
	return func(c *TursoConnector) { c.queryTimeoutMs = ms }
}

// WithQueryTimeout sets the per-request timeout in milliseconds (0 disables).
func WithQueryTimeout(ms int) ConnectorOption {
	return func(c *TursoConnector) { c.queryTimeoutMs = ms }
}

// WithAuthToken sets the auth token (also settable via ?auth_token= in the DSN).
func WithAuthToken(token string) ConnectorOption {
	return func(c *TursoConnector) { c.authToken = token }
}

// WithRequestHeaders attaches extra HTTP headers to every request. Passing a
// "Host" key (case-insensitive) makes NewConnector return an error.
func WithRequestHeaders(headers map[string]string) ConnectorOption {
	return func(c *TursoConnector) { c.requestHeaders = headers }
}

// WithEncryptionKey sets the base64 x-turso-encryption-key header used to
// access encrypted Turso Cloud databases.
func WithEncryptionKey(key string) ConnectorOption {
	return func(c *TursoConnector) { c.encryptionKey = key }
}

// TursoConnector implements driver.Connector for programmatic configuration.
// Named to match the embedded turso driver's connector for source-level
// compatibility.
type TursoConnector struct {
	url            string
	authToken      string
	requestHeaders map[string]string
	encryptionKey  string
	queryTimeoutMs int
}

var _ driver.Connector = (*TursoConnector)(nil)

// NewConnector creates a new TursoConnector from a DSN (which may carry
// ?auth_token=) and options. Mirrors the embedded turso.NewConnector.
func NewConnector(dsn string, opts ...ConnectorOption) (*TursoConnector, error) {
	u, token, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &TursoConnector{url: u, authToken: token}
	for _, opt := range opts {
		opt(c)
	}
	for name := range c.requestHeaders {
		if strings.EqualFold(name, "host") {
			return nil, errors.New("turso: overwriting the 'Host' header is not supported")
		}
	}
	return c, nil
}

// Connect implements driver.Connector.
func (c *TursoConnector) Connect(_ context.Context) (driver.Conn, error) {
	return newRemoteConn(c.url, c.authToken, c.requestHeaders, c.encryptionKey, c.queryTimeoutMs), nil
}

// Driver implements driver.Connector.
func (c *TursoConnector) Driver() driver.Driver {
	return &remoteDriver{}
}

// Statement is a single statement in a Batch, with optional positional args.
type Statement struct {
	SQL  string
	Args []any
}

// BatchResult is the per-statement result of a Batch call, in input order.
type BatchResult struct {
	Columns      []string
	Rows         [][]any
	RowsAffected int64
}

// Batch runs several statements in one round trip against a serverless
// connection, returning one BatchResult per input statement. When mode is
// non-empty ("deferred"/"immediate"/"exclusive"/"concurrent"/…) the batch runs
// atomically via a server-side BEGIN/COMMIT/ROLLBACK condition chain.
//
// It operates on the driver connection underlying a *sql.Conn via Raw, the
// idiomatic way to reach driver-specific features from database/sql:
//
//	conn, _ := db.Conn(ctx)
//	results, err := tursogo_serverless.Batch(ctx, conn, stmts, "immediate")
func Batch(ctx context.Context, conn *sql.Conn, statements []Statement, mode string) ([]BatchResult, error) {
	var out []BatchResult
	err := conn.Raw(func(dc any) error {
		rc, ok := dc.(*remoteConn)
		if !ok {
			return fmt.Errorf("turso: Batch requires a serverless connection, got %T", dc)
		}
		var e error
		out, e = rc.batch(ctx, statements, mode)
		return e
	})
	return out, err
}

// InTransaction reports whether the serverless connection underlying a
// *sql.Conn currently has an open transaction, from the server's authoritative
// autocommit state.
func InTransaction(conn *sql.Conn) (bool, error) {
	var inTx bool
	err := conn.Raw(func(dc any) error {
		rc, ok := dc.(*remoteConn)
		if !ok {
			return fmt.Errorf("turso: InTransaction requires a serverless connection, got %T", dc)
		}
		inTx = rc.sess.inTransaction()
		return nil
	})
	return inTx, err
}

// Transaction runs fn inside a transaction on conn, committing if fn returns
// nil and rolling back if it returns an error. mode selects the BEGIN variant —
// "" (deferred), "immediate", "exclusive", or "concurrent" (BEGIN CONCURRENT) —
// mirroring the JavaScript driver's transaction() modes.
//
// This is the way to run a BEGIN CONCURRENT transaction: database/sql's
// BeginTx cannot express it, so it always sends a plain BEGIN.
//
//	err := tursogo_serverless.Transaction(ctx, conn, "concurrent", func(ctx context.Context) error {
//		_, err := conn.ExecContext(ctx, "INSERT INTO t VALUES (1)")
//		return err
//	})
func Transaction(ctx context.Context, conn *sql.Conn, mode string, fn func(context.Context) error) error {
	begin := "BEGIN"
	if mode != "" {
		begin = "BEGIN " + normalizeBatchMode(mode)
	}
	if _, err := conn.ExecContext(ctx, begin); err != nil {
		return err
	}
	if err := fn(ctx); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	_, err := conn.ExecContext(ctx, "COMMIT")
	return err
}
