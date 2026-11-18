// Package turso provides a database/sql driver for Turso Cloud that speaks
// the SQL over HTTP protocol (see serverless/PROTOCOL.md). Designed for
// serverless and edge environments: no persistent connections, no native
// libraries, just HTTP requests.
//
// The driver registers as "turso-serverless" and mirrors the embedded
// turso.tech/database/tursogo driver, so the same application code can run
// against a local database or Turso Cloud:
//
//	db, err := sql.Open("turso-serverless", "turso://my-db.turso.io?auth_token=...")
package turso

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Sentinel errors matching the embedded turso Go driver.
var (
	ErrTursoStmtClosed = errors.New("turso: statement closed")
	ErrTursoConnClosed = errors.New("turso: connection closed")
	ErrTursoTxDone     = errors.New("turso: transaction done")
)

func init() {
	sql.Register("turso-serverless", &serverlessDriver{})
}

// Ensure interface compliance.
var (
	_ driver.Driver             = (*serverlessDriver)(nil)
	_ driver.Connector          = (*Connector)(nil)
	_ driver.Conn               = (*Connection)(nil)
	_ driver.ConnPrepareContext = (*Connection)(nil)
	_ driver.ExecerContext      = (*Connection)(nil)
	_ driver.QueryerContext     = (*Connection)(nil)
	_ driver.Pinger             = (*Connection)(nil)
	_ driver.Validator          = (*Connection)(nil)
	_ driver.ConnBeginTx        = (*Connection)(nil)
	_ driver.Stmt               = (*stmt)(nil)
	_ driver.StmtExecContext    = (*stmt)(nil)
	_ driver.StmtQueryContext   = (*stmt)(nil)
	_ driver.Rows               = (*rows)(nil)
	_ driver.Result             = (*execResult)(nil)
	_ driver.Tx                 = (*tx)(nil)
)

// --- driver.Driver ---

type serverlessDriver struct{}

func (d *serverlessDriver) Open(dsn string) (driver.Conn, error) {
	u, token, encryptionKey, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return newConn(u, token, encryptionKey), nil
}

// parseDSN parses "<url>[?auth_token=<token>&remote_encryption_key=<key>]"
// into a base URL, an auth token, and a remote encryption key. The URL can
// use the turso://, libsql://, https://, or http:// scheme; other query
// parameters are preserved.
func parseDSN(dsn string) (baseURL, authToken, encryptionKey string, err error) {
	if dsn == "" {
		return "", "", "", errors.New("turso: empty DSN")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		// A url.Error echoes the full DSN, which can carry the auth token
		// and encryption key; report only the underlying reason.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return "", "", "", fmt.Errorf("turso: invalid DSN: %w", err)
	}
	q := u.Query()
	authToken = q.Get("auth_token")
	q.Del("auth_token")
	encryptionKey = q.Get("remote_encryption_key")
	q.Del("remote_encryption_key")
	u.RawQuery = q.Encode()
	return normalizeURL(u.String()), authToken, encryptionKey, nil
}

// --- driver.Connector ---

// Connector opens serverless connections without going through a DSN. Use
// it with sql.OpenDB when the auth token should not appear in a connection
// string.
type Connector struct {
	url                 string
	authToken           string
	remoteEncryptionKey string
	client              *http.Client
	requestHeaders      http.Header
	requestHeadersErr   error
	retainSession       bool
}

// NewConnector creates a connector for the given database URL (turso://,
// libsql://, https://, or http://) and auth token.
func NewConnector(url, authToken string) *Connector {
	return &Connector{url: normalizeURL(url), authToken: authToken}
}

// WithRemoteEncryptionKey sets the customer-managed encryption key for an
// encrypted database and returns the connector for chaining:
//
//	db := sql.OpenDB(turso.NewConnector(url, token).WithRemoteEncryptionKey(key))
func (c *Connector) WithRemoteEncryptionKey(key string) *Connector {
	c.remoteEncryptionKey = key
	return c
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.OpenConnection(ctx)
}

// WithHTTPClient sets the HTTP client. Configure connectors before opening connections.
// The client is copied; its Transport is shared and must support concurrent requests.
func (c *Connector) WithHTTPClient(client *http.Client) *Connector {
	c.client = client
	return c
}

// WithRequestHeaders copies headers to send on every request, overriding defaults.
// Host sets the request host independently of the URL destination and TLS server name.
// Configure connectors before opening connections.
func (c *Connector) WithRequestHeaders(headers http.Header) *Connector {
	c.requestHeadersErr = nil
	c.requestHeaders = make(http.Header, len(headers))
	for name, values := range headers {
		name = http.CanonicalHeaderKey(name)
		if _, exists := c.requestHeaders[name]; exists {
			c.requestHeadersErr = fmt.Errorf("turso: duplicate request header %q", name)
			return c
		}
		if name == "Host" && (len(values) != 1 || values[0] == "") {
			c.requestHeadersErr = errors.New("turso: Host requires exactly one non-empty value")
			return c
		}
		c.requestHeaders[name] = append([]string(nil), values...)
	}
	return c
}

// WithSessionRetention keeps one backend session across autocommit calls.
// Session loss permanently invalidates the connection, including outside transactions.
func (c *Connector) WithSessionRetention() *Connector { c.retainSession = true; return c }

// OpenConnection opens a dedicated connection for protocol adapters. Call Close when done.
// Arguments to its driver methods must use database/sql/driver value types.
func (c *Connector) OpenConnection(ctx context.Context) (*Connection, error) {
	if c.requestHeadersErr != nil {
		return nil, c.requestHeadersErr
	}
	conn := newConn(c.url, c.authToken, c.remoteEncryptionKey)
	if c.client != nil {
		client := *c.client
		conn.sess.client = &client
	}
	conn.sess.requestHeaders = c.requestHeaders.Clone()
	conn.sess.retainSession = c.retainSession
	if c.retainSession {
		id := 0
		results, err := conn.sess.executePipeline(ctx, []streamRequest{{Type: "store_sql", SQL: "SELECT 1", SQLID: &id}}, true)
		if err == nil && results[0].Error != nil {
			err = serverError(results[0].Error)
		}
		if err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (c *Connector) Driver() driver.Driver {
	return &serverlessDriver{}
}

// --- driver.Conn ---

// Connection is a dedicated backend connection. Operations are serialized.
type Connection struct {
	sess   *session
	mu     sync.Mutex
	closed bool
}

func newConn(url, authToken, remoteEncryptionKey string) *Connection {
	return &Connection{sess: newSession(url, authToken, remoteEncryptionKey)}
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	// Closing the stream rolls back any open transaction server-side,
	// matching the embedded driver: uncommitted changes are lost on close.
	c.sess.close()
	return nil
}

// IsValid prevents database/sql from pooling a lost backend session.
func (c *Connection) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && !c.sess.broken
}

func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	desc, err := c.describe(ctx, query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, sql: query, numInputs: len(desc.Params)}, nil
}

// DescribeContext returns metadata without executing the SQL or binding parameters.
func (c *Connection) DescribeContext(ctx context.Context, query string) (*Description, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	return c.describe(ctx, query)
}

func (c *Connection) describe(ctx context.Context, query string) (*Description, error) {
	results, err := c.sess.executePipeline(ctx, []streamRequest{{Type: "describe", SQL: query}}, true)
	if err != nil {
		return nil, err
	}
	result := results[0]
	if result.Error != nil {
		return nil, serverError(result.Error)
	}
	if result.Response == nil || result.Response.Result == nil {
		return nil, fmt.Errorf("turso: expected describe result in pipeline response")
	}
	var desc Description
	if err := json.Unmarshal(result.Response.Result, &desc); err != nil {
		return nil, fmt.Errorf("turso: invalid describe result: %w", err)
	}
	return &desc, nil
}

// Autocommit reports the last completed operation's transaction state.
func (c *Connection) Autocommit() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return false, err
	}
	return c.sess.autocommit, nil
}

func (c *Connection) ready() error {
	if c.closed {
		return ErrTursoConnClosed
	}
	if c.sess.broken {
		return ErrSessionLost
	}
	return nil
}

func (c *Connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Connection) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	if _, err := c.sess.executeStmt(ctx, rawStmt("BEGIN")); err != nil {
		return nil, err
	}
	return &tx{conn: c}, nil
}

func (c *Connection) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return err
	}
	_, err := c.sess.executeStmt(ctx, rawStmt("SELECT 1"))
	return err
}

func (c *Connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	// Multi-statement strings run through the sequence request
	// (section 6.3): statements execute in order and execution stops at
	// the first failure. Statement-level counts are not reported.
	if len(args) == 0 && len(splitStatements(query)) > 1 {
		results, err := c.sess.executePipeline(ctx, []streamRequest{{Type: "sequence", SQL: query}}, true)
		if err != nil {
			return nil, err
		}
		if results[0].Error != nil {
			return nil, serverError(results[0].Error)
		}
		return &execResult{lastInsertId: c.sess.lastInsertRowid}, nil
	}
	body, err := buildStmt(query, args, false)
	if err != nil {
		return nil, err
	}
	out, err := c.sess.executeStmt(ctx, body)
	if err != nil {
		return nil, err
	}
	return &execResult{
		lastInsertId: c.sess.lastInsertRowid,
		rowsAffected: out.RowsAffected,
	}, nil
}

func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	out, err := c.ExecuteContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	result := &rows{columns: make([]string, len(out.Columns)), decltypes: make([]string, len(out.Columns)), rows: out.Rows}
	for i, col := range out.Columns {
		if col.Name != nil {
			result.columns[i] = *col.Name
		}
		if col.Decltype != nil {
			result.decltypes[i] = *col.Decltype
		}
	}
	return result, nil
}

// StatementResult contains the buffered output of one statement execution.
type StatementResult struct {
	Columns []Column
	// Rows contains int64, float64, string, []byte, or nil values. Date/time text remains a string.
	Rows         [][]any
	RowsAffected int64
}

// ExecuteContext executes one statement and returns its columns, raw decoded rows,
// and backend affected-row count together. It does not describe or reexecute SQL.
// Arguments use database/sql/driver value types. Errors return no partial result.
func (c *Connection) ExecuteContext(ctx context.Context, query string, args []driver.NamedValue) (*StatementResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	body, err := buildStmt(query, args, true)
	if err != nil {
		return nil, err
	}
	return c.sess.executeStmt(ctx, body)
}

// buildStmt encodes a query and its arguments into a protocol statement
// (section 8.1).
func buildStmt(query string, args []driver.NamedValue, wantRows bool) (stmtBody, error) {
	positional := []protoValue{}
	named := []namedArg{}
	for _, nv := range args {
		value, err := encodeValue(nv.Value)
		if err != nil {
			return stmtBody{}, err
		}
		if nv.Name != "" {
			named = append(named, namedArg{Name: nv.Name, Value: value})
		} else {
			positional = append(positional, value)
		}
	}
	return stmtBody{
		SQL:       query,
		Args:      positional,
		NamedArgs: named,
		WantRows:  wantRows,
	}, nil
}

// rawStmt builds a protocol statement for driver-issued SQL with no
// arguments.
func rawStmt(query string) stmtBody {
	return stmtBody{
		SQL:       query,
		Args:      []protoValue{},
		NamedArgs: []namedArg{},
		WantRows:  false,
	}
}

// splitStatements splits a SQL string into individual statements at
// semicolons outside of single- or double-quoted strings.
func splitStatements(sqlText string) []string {
	var stmts []string
	var current strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		switch {
		case ch == '\'' && !inDoubleQuote:
			inSingleQuote = !inSingleQuote
			current.WriteByte(ch)
		case ch == '"' && !inSingleQuote:
			inDoubleQuote = !inDoubleQuote
			current.WriteByte(ch)
		case ch == ';' && !inSingleQuote && !inDoubleQuote:
			if s := strings.TrimSpace(current.String()); s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if s := strings.TrimSpace(current.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// --- driver.Stmt ---

type stmt struct {
	conn      *Connection
	sql       string
	numInputs int
	closed    bool
}

func (s *stmt) Close() error {
	s.closed = true
	return nil
}

func (s *stmt) NumInput() int {
	return s.numInputs
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedValues(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, ErrTursoStmtClosed
	}
	return s.conn.ExecContext(ctx, s.sql, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedValues(args))
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed {
		return nil, ErrTursoStmtClosed
	}
	return s.conn.QueryContext(ctx, s.sql, args)
}

func namedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return named
}

// --- driver.Rows ---

type rows struct {
	columns   []string
	decltypes []string
	rows      [][]any
	pos       int
	closed    bool
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	r.closed = true
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed || r.pos >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.pos]
	r.pos++
	for i, v := range row {
		if i >= len(dest) {
			break
		}
		// Matches the embedded driver: text in a date-typed column scans
		// as time.Time when it parses.
		if s, ok := v.(string); ok && i < len(r.decltypes) && isTimeColumn(r.decltypes[i]) {
			if t, err := parseTimeString(s); err == nil {
				dest[i] = t
				continue
			}
		}
		dest[i] = v
	}
	return nil
}

// isTimeColumn reports whether the declared column type is a date or time
// type, matching go-sqlite3 and the embedded driver.
func isTimeColumn(decltype string) bool {
	upper := strings.ToUpper(decltype)
	return upper == "TIMESTAMP" || upper == "DATETIME" || upper == "DATE"
}

// sqliteTimestampFormats are the timestamp formats accepted by go-sqlite3.
var sqliteTimestampFormats = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

func parseTimeString(s string) (time.Time, error) {
	trimmed := strings.TrimSuffix(s, "Z")
	for _, format := range sqliteTimestampFormats {
		if t, err := time.ParseInLocation(format, trimmed, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("turso: cannot parse %q as time", s)
}

// --- driver.Result ---

type execResult struct {
	lastInsertId int64
	rowsAffected int64
}

func (r *execResult) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *execResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// --- driver.Tx ---

type tx struct {
	conn *Connection
	done bool
}

func (t *tx) Commit() error {
	return t.finish("COMMIT")
}

func (t *tx) Rollback() error {
	return t.finish("ROLLBACK")
}

func (t *tx) finish(sqlText string) error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	if t.done {
		return ErrTursoTxDone
	}
	if err := t.conn.ready(); err != nil {
		return err
	}
	t.done = true
	_, err := t.conn.sess.executeStmt(context.Background(), rawStmt(sqlText))
	return err
}
