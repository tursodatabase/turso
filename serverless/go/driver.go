package tursogo_serverless

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Sentinel errors matching the turso Go driver.
var (
	ErrTursoStmtClosed = errors.New("turso: statement closed")
	ErrTursoConnClosed = errors.New("turso: connection closed")
	ErrTursoRowsClosed = errors.New("turso: rows closed")
	ErrTursoTxDone     = errors.New("turso: transaction done")
	ErrTursoConstraint = errors.New("turso: constraint violation")
)

// --- Driver registration ---

func init() {
	sql.Register("turso-serverless", &remoteDriver{})
}

// --- driver.Driver ---

type remoteDriver struct{}

func (d *remoteDriver) Open(dsn string) (driver.Conn, error) {
	u, token, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return newRemoteConn(u, token), nil
}

// Ensure interface compliance.
var (
	_ driver.Driver             = (*remoteDriver)(nil)
	_ driver.Conn               = (*remoteConn)(nil)
	_ driver.ConnPrepareContext = (*remoteConn)(nil)
	_ driver.ExecerContext      = (*remoteConn)(nil)
	_ driver.QueryerContext     = (*remoteConn)(nil)
	_ driver.Pinger             = (*remoteConn)(nil)
	_ driver.ConnBeginTx        = (*remoteConn)(nil)
)

// --- driver.Conn ---

type remoteConn struct {
	sess   *session
	mu     sync.Mutex
	closed bool
}

func newRemoteConn(url, authToken string) *remoteConn {
	return &remoteConn{
		sess: newSession(url, authToken),
	}
}

func (c *remoteConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *remoteConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	numInputs := 0
	resp, err := c.sess.executePipelineCtx(ctx, []pipelineRequestEntry{
		{Type: "describe", SQL: query},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Results) > 0 {
		r := resp.Results[0]
		if r.Error != nil {
			return nil, fmt.Errorf("turso: %s", r.Error.Message)
		}
		if r.Response != nil && r.Response.Result != nil {
			var desc describeResult
			if json.Unmarshal(*r.Response.Result, &desc) == nil {
				numInputs = len(desc.Params)
			}
		}
	}

	return &remoteStmt{
		conn:      c,
		sql:       query,
		numInputs: numInputs,
	}, nil
}

func (c *remoteConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.sess.close()
}

func (c *remoteConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *remoteConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if opts.Isolation != 0 && opts.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, fmt.Errorf("turso: unsupported isolation level: %d", opts.Isolation)
	}
	if opts.ReadOnly {
		return nil, fmt.Errorf("turso: read-only transactions not supported")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	step := batchStep{
		Stmt: stmtBody{
			SQL:       "BEGIN",
			Args:      []protoValue{},
			NamedArgs: []namedArg{},
			WantRows:  false,
		},
	}
	entries, err := c.sess.executeCursorCtx(ctx, []batchStep{step})
	if err != nil {
		return nil, err
	}
	if stepErr := extractStepError(entries); stepErr != nil {
		return nil, stepErr
	}
	return &remoteTx{conn: c}, nil
}

func (c *remoteConn) Ping(ctx context.Context) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	step := batchStep{
		Stmt: stmtBody{
			SQL:       "SELECT 1",
			Args:      []protoValue{},
			NamedArgs: []namedArg{},
			WantRows:  false,
		},
	}
	entries, err := c.sess.executeCursorCtx(ctx, []batchStep{step})
	if err != nil {
		return err
	}
	if stepErr := extractStepError(entries); stepErr != nil {
		return stepErr
	}
	return nil
}

func (c *remoteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if multi-statement
	stmts := splitStatements(query)
	if len(stmts) <= 1 {
		// Single statement via cursor
		step := buildBatchStep(query, args, false)
		entries, err := c.sess.executeCursorCtx(ctx, []batchStep{step})
		if err != nil {
			return nil, err
		}
		return extractResult(entries)
	}

	// Multi-statement: use pipeline sequence
	combined := strings.Join(stmts, ";\n")
	resp, err := c.sess.executePipelineCtx(ctx, []pipelineRequestEntry{
		{Type: "sequence", SQL: combined},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Results) > 0 {
		r := resp.Results[0]
		if r.Error != nil {
			return nil, fmt.Errorf("turso: %s", r.Error.Message)
		}
	}
	return &remoteResult{lastInsertId: 0, rowsAffected: 0}, nil
}

func (c *remoteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	step := buildBatchStep(query, args, true)
	entries, err := c.sess.executeCursorCtx(ctx, []batchStep{step})
	if err != nil {
		return nil, err
	}

	return parseRows(entries)
}

func (c *remoteConn) checkOpen() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrTursoConnClosed
	}
	return nil
}

// --- driver.Stmt ---

type remoteStmt struct {
	conn      *remoteConn
	sql       string
	numInputs int
	closed    atomic.Bool
}

var (
	_ driver.Stmt             = (*remoteStmt)(nil)
	_ driver.StmtExecContext  = (*remoteStmt)(nil)
	_ driver.StmtQueryContext = (*remoteStmt)(nil)
)

func (s *remoteStmt) Close() error {
	s.closed.Store(true)
	return nil
}

func (s *remoteStmt) NumInput() int {
	return s.numInputs
}

func (s *remoteStmt) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), named)
}

func (s *remoteStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed.Load() {
		return nil, ErrTursoStmtClosed
	}
	return s.conn.ExecContext(ctx, s.sql, args)
}

func (s *remoteStmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), named)
}

func (s *remoteStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed.Load() {
		return nil, ErrTursoStmtClosed
	}
	return s.conn.QueryContext(ctx, s.sql, args)
}

// --- driver.Rows ---

type remoteRows struct {
	columns   []string
	decltypes []string
	rows      [][]any // eagerly fetched
	pos       int
	closed    bool
}

var _ driver.Rows = (*remoteRows)(nil)

func (r *remoteRows) Columns() []string {
	return r.columns
}

func (r *remoteRows) Close() error {
	r.closed = true
	return nil
}

func (r *remoteRows) Next(dest []driver.Value) error {
	if r.closed {
		return ErrTursoRowsClosed
	}
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.pos]
	r.pos++
	for i, v := range row {
		if i >= len(dest) {
			break
		}
		// Check if this text value should be parsed as time
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

// --- driver.Result ---

type remoteResult struct {
	lastInsertId int64
	rowsAffected int64
}

var _ driver.Result = (*remoteResult)(nil)

func (r *remoteResult) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *remoteResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// --- driver.Tx ---

type remoteTx struct {
	conn *remoteConn
	done bool
}

var _ driver.Tx = (*remoteTx)(nil)

func (tx *remoteTx) Commit() error {
	if tx.done {
		return ErrTursoTxDone
	}
	tx.done = true
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if tx.conn.closed {
		return ErrTursoConnClosed
	}

	step := batchStep{
		Stmt: stmtBody{
			SQL:       "COMMIT",
			Args:      []protoValue{},
			NamedArgs: []namedArg{},
			WantRows:  false,
		},
	}
	entries, err := tx.conn.sess.executeCursor([]batchStep{step})
	if err != nil {
		return err
	}
	if stepErr := extractStepError(entries); stepErr != nil {
		return stepErr
	}
	return nil
}

func (tx *remoteTx) Rollback() error {
	if tx.done {
		return ErrTursoTxDone
	}
	tx.done = true
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if tx.conn.closed {
		return ErrTursoConnClosed
	}

	step := batchStep{
		Stmt: stmtBody{
			SQL:       "ROLLBACK",
			Args:      []protoValue{},
			NamedArgs: []namedArg{},
			WantRows:  false,
		},
	}
	entries, err := tx.conn.sess.executeCursor([]batchStep{step})
	if err != nil {
		return err
	}
	if stepErr := extractStepError(entries); stepErr != nil {
		return stepErr
	}
	return nil
}

// --- DSN parsing ---

// parseDSN parses "<url>[?auth_token=<token>]" into url and auth token.
func parseDSN(dsn string) (string, string, error) {
	if dsn == "" {
		return "", "", errors.New("turso: empty DSN")
	}

	// Parse the URL to extract auth_token query param
	u, err := url.Parse(dsn)
	if err != nil {
		return "", "", fmt.Errorf("turso: invalid DSN: %w", err)
	}

	token := u.Query().Get("auth_token")

	// Rebuild URL without auth_token param
	q := u.Query()
	q.Del("auth_token")
	u.RawQuery = q.Encode()

	baseURL := u.String()
	// Remove trailing '?' if no query params remain
	baseURL = strings.TrimSuffix(baseURL, "?")

	return normalizeURL(baseURL), token, nil
}

// --- Helpers ---

// buildBatchStep creates a batchStep from a query and named values.
func buildBatchStep(query string, args []driver.NamedValue, wantRows bool) batchStep {
	var positional []protoValue
	var named []namedArg

	for _, nv := range args {
		if nv.Name != "" {
			named = append(named, namedArg{
				Name:  nv.Name,
				Value: encodeValue(nv.Value),
			})
		} else {
			positional = append(positional, encodeValue(nv.Value))
		}
	}

	if positional == nil {
		positional = []protoValue{}
	}
	if named == nil {
		named = []namedArg{}
	}

	return batchStep{
		Stmt: stmtBody{
			SQL:       query,
			Args:      positional,
			NamedArgs: named,
			WantRows:  wantRows,
		},
	}
}

// extractStepError checks cursor entries for step_error or error entries.
func extractStepError(entries []cursorEntry) error {
	for _, e := range entries {
		switch e.Type {
		case "step_error":
			if e.Error != nil {
				msg := e.Error.Message
				if strings.Contains(strings.ToUpper(msg), "CONSTRAINT") ||
					strings.Contains(strings.ToUpper(msg), "UNIQUE") ||
					strings.Contains(strings.ToUpper(msg), "PRIMARY KEY") {
					return fmt.Errorf("%w: %s", ErrTursoConstraint, msg)
				}
				return fmt.Errorf("turso: %s", msg)
			}
		case "error":
			if e.Error != nil {
				return fmt.Errorf("turso: %s", e.Error.Message)
			}
		}
	}
	return nil
}

// extractResult extracts a driver.Result from cursor entries.
func extractResult(entries []cursorEntry) (driver.Result, error) {
	if stepErr := extractStepError(entries); stepErr != nil {
		return nil, stepErr
	}

	var lastInsert int64
	var affected int64
	for _, e := range entries {
		if e.Type == "step_end" {
			if e.AffectedRowCount != nil {
				affected = *e.AffectedRowCount
			}
			if e.LastInsertRowid != nil {
				if n, err := strconv.ParseInt(e.LastInsertRowid.String(), 10, 64); err == nil {
					lastInsert = n
				}
			}
		}
	}

	return &remoteResult{
		lastInsertId: lastInsert,
		rowsAffected: affected,
	}, nil
}

// parseRows parses cursor entries into a remoteRows with eagerly fetched data.
func parseRows(entries []cursorEntry) (*remoteRows, error) {
	if stepErr := extractStepError(entries); stepErr != nil {
		return nil, stepErr
	}

	var columns []string
	var decltypes []string
	var rows [][]any

	for _, e := range entries {
		switch e.Type {
		case "step_begin":
			for _, col := range e.Cols {
				columns = append(columns, col.Name)
				if col.Decltype != nil {
					decltypes = append(decltypes, *col.Decltype)
				} else {
					decltypes = append(decltypes, "")
				}
			}
		case "row":
			row := make([]any, len(e.Row))
			for i, pv := range e.Row {
				v, _ := decodeValue(pv)
				row[i] = v
			}
			rows = append(rows, row)
		}
	}

	if columns == nil {
		columns = []string{}
	}

	return &remoteRows{
		columns:   columns,
		decltypes: decltypes,
		rows:      rows,
	}, nil
}

// splitStatements splits a SQL string into individual statements by semicolons.
// It respects quoted strings and only splits on semicolons outside of quotes.
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch {
		case ch == '\'' && !inDoubleQuote:
			inSingleQuote = !inSingleQuote
			current.WriteByte(ch)
		case ch == '"' && !inSingleQuote:
			inDoubleQuote = !inDoubleQuote
			current.WriteByte(ch)
		case ch == ';' && !inSingleQuote && !inDoubleQuote:
			s := strings.TrimSpace(current.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	s := strings.TrimSpace(current.String())
	if s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// isTimeColumn checks if the column declared type indicates a time/date column.
func isTimeColumn(decltype string) bool {
	if decltype == "" {
		return false
	}
	upper := strings.ToUpper(decltype)
	return upper == "TIMESTAMP" || upper == "DATETIME" || upper == "DATE"
}

// sqliteTimestampFormats are the timestamp formats supported by go-sqlite3.
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

// parseTimeString attempts to parse a string as a time.Time value.
func parseTimeString(s string) (time.Time, error) {
	s = strings.TrimSuffix(s, "Z")
	for _, format := range sqliteTimestampFormats {
		if t, err := time.ParseInLocation(format, s, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as time", s)
}
