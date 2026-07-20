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
	return newRemoteConn(u, token, nil, "", 0), nil
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

func newRemoteConn(url, authToken string, requestHeaders map[string]string, encryptionKey string, queryTimeoutMs int) *remoteConn {
	return &remoteConn{
		sess: newSession(url, authToken, requestHeaders, encryptionKey, queryTimeoutMs),
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
	}, true)
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
	// The baton is always persisted and the server's autocommit state (tracked
	// by the session) now reflects the open transaction — no keepAlive toggle.
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
	}, true)
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

// normalizeBatchMode maps a batch/transaction mode name to its BEGIN <mode>
// keyword (same values as the JS driver's transaction variants).
func normalizeBatchMode(mode string) string {
	switch strings.ToLower(mode) {
	case "write", "immediate":
		return "IMMEDIATE"
	case "read", "deferred":
		return "DEFERRED"
	case "exclusive":
		return "EXCLUSIVE"
	case "concurrent":
		return "CONCURRENT"
	default:
		return strings.ToUpper(mode)
	}
}

// stepErrorToError classifies a batch/step error message into a sentinel-wrapped
// error, matching extractStepError's constraint detection.
func stepErrorToError(e *protoError) error {
	if e == nil {
		return fmt.Errorf("turso: batch execution failed")
	}
	up := strings.ToUpper(e.Message)
	if strings.Contains(up, "CONSTRAINT") || strings.Contains(up, "UNIQUE") || strings.Contains(up, "PRIMARY KEY") {
		return fmt.Errorf("%w: %s", ErrTursoConstraint, e.Message)
	}
	return fmt.Errorf("turso: %s", e.Message)
}

// batch runs statements in one round trip, one BatchResult per statement. With
// a non-empty mode it wraps them in an atomic BEGIN/COMMIT/ROLLBACK condition
// chain. Ported from the JS driver's batch().
func (c *remoteConn) batch(ctx context.Context, statements []Statement, mode string) ([]BatchResult, error) { //nolint:gocyclo
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Inside an outer transaction the surrounding BEGIN already opened one on
	// this stream; emitting another BEGIN would fail, so drop the mode and run
	// the statements within the existing transaction (matches the JS driver).
	if mode != "" && c.sess.inTransaction() {
		mode = ""
	}

	n := len(statements)
	userSteps := make([]batchStep, n)
	for i, st := range statements {
		args := make([]protoValue, len(st.Args))
		for j, a := range st.Args {
			args[j] = encodeValue(a)
		}
		userSteps[i] = batchStep{Stmt: stmtBody{SQL: st.SQL, Args: args, NamedArgs: []namedArg{}, WantRows: true}}
	}

	var steps []batchStep
	firstUserIdx := 0
	lastUserIdx := n - 1
	rollbackIdx := -1
	if mode == "" {
		steps = userSteps
	} else {
		beginIdx := 0
		firstUserIdx = 1
		lastUserIdx = n // user steps occupy 1..n inclusive
		commitIdx := lastUserIdx + 1
		rollbackIdx = commitIdx + 1
		steps = make([]batchStep, 0, n+3)
		steps = append(steps, batchStep{Stmt: stmtBody{SQL: "BEGIN " + normalizeBatchMode(mode), Args: []protoValue{}, NamedArgs: []namedArg{}, WantRows: false}})
		for i := range userSteps {
			s := userSteps[i]
			if i == 0 {
				s.Condition = condOK(beginIdx)
			} else {
				s.Condition = condOK(firstUserIdx + i - 1)
			}
			steps = append(steps, s)
		}
		steps = append(steps, batchStep{Stmt: stmtBody{SQL: "COMMIT", Args: []protoValue{}, NamedArgs: []namedArg{}, WantRows: false}, Condition: condOK(lastUserIdx)})
		steps = append(steps, batchStep{Stmt: stmtBody{SQL: "ROLLBACK", Args: []protoValue{}, NamedArgs: []namedArg{}, WantRows: false}, Condition: condAnd(*condOK(beginIdx), *condNot(condOK(commitIdx)))})
	}

	entries, err := c.sess.executeCursorCtx(ctx, steps)
	if err != nil {
		return nil, err
	}

	results := make([]BatchResult, n)
	for i := range results {
		results[i] = BatchResult{Columns: []string{}, Rows: [][]any{}}
	}
	var deferredErr error
	currentIdx := -1
	nextNonAtomicIdx := 0

	stepToResultIdx := func(step *int) int {
		if mode == "" {
			if step != nil {
				return *step
			}
			return nextNonAtomicIdx
		}
		if step != nil && *step >= firstUserIdx && *step <= lastUserIdx {
			return *step - firstUserIdx
		}
		return -1
	}

	for _, e := range entries {
		if deferredErr != nil && e.Type != "error" {
			continue
		}
		switch e.Type {
		case "step_begin":
			currentIdx = stepToResultIdx(e.Step)
			if currentIdx >= 0 && currentIdx < n && len(e.Cols) > 0 {
				cols := make([]string, len(e.Cols))
				for k, col := range e.Cols {
					cols[k] = col.Name
				}
				results[currentIdx].Columns = cols
			}
		case "row":
			if currentIdx >= 0 && currentIdx < n {
				row := make([]any, len(e.Row))
				for k, pv := range e.Row {
					v, _ := decodeValue(pv)
					row[k] = v
				}
				results[currentIdx].Rows = append(results[currentIdx].Rows, row)
			}
		case "step_end":
			idx := currentIdx
			if idx < 0 && mode == "" {
				idx = nextNonAtomicIdx
			}
			if idx >= 0 && idx < n && e.AffectedRowCount != nil && len(results[idx].Columns) == 0 {
				results[idx].RowsAffected = *e.AffectedRowCount
			}
			if mode == "" && idx >= 0 {
				nextNonAtomicIdx = idx + 1
			}
			currentIdx = -1
		case "step_error":
			if deferredErr == nil && (e.Step == nil || *e.Step != rollbackIdx) {
				deferredErr = stepErrorToError(e.Error)
			}
			currentIdx = -1
		case "error":
			return nil, stepErrorToError(e.Error)
		}
	}
	if deferredErr != nil {
		return nil, deferredErr
	}
	return results, nil
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
