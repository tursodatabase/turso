package turso

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
)

// define all package level errors here
var (
	ErrTursoStmtClosed = errors.New("turso: statement closed")
	ErrTursoConnClosed = errors.New("turso: connection closed")
	ErrTursoRowsClosed = errors.New("turso: rows closed")
	ErrTursoTxDone     = errors.New("turso: transaction done")
)

// define all package level structs here

type tursoDbDriver struct{}

type tursoDbConnection struct {
	db      TursoDatabase
	conn    TursoConnection
	extraIo func() error

	mu          sync.Mutex
	closed      bool
	busyTimeout int // current busy timeout in milliseconds
	// keep flags for configuration if needed
	async bool
}

type tursoDbStatement struct {
	conn      *tursoDbConnection
	sql       string
	numInputs int
	closed    bool
}

type tursoDbRows struct {
	conn      *tursoDbConnection
	stmt      TursoStatement
	columns   []string
	decltypes []string

	closed bool
	err    error
}

type tursoDbResult struct {
	lastInsertId int64
	rowsAffected int64
}

type tursoDbTx struct {
	conn *tursoDbConnection
	done bool
}

// register driver
func init() {
	sql.Register("turso", &tursoDbDriver{})
}

// Extra constructor for *tursoDbConnection instance which can be used to intergrate with turso Db driver
// extr_io parameter is the arbitrary IO function which will be executed together with turso_statement_run_io
func NewConnection(conn TursoConnection, extraIo func() error) *tursoDbConnection {
	return &tursoDbConnection{
		conn:    conn,
		extraIo: extraIo,
	}
}

// Optional helper to run global setup (logger and log level).
func Setup(config TursoConfig) error {
	InitLibrary(turso_libs.LoadTursoLibraryConfig{})
	return turso_setup(config)
}

// Implement sql.Driver methods
func (d *tursoDbDriver) Open(dsn string) (driver.Conn, error) {
	InitLibrary(turso_libs.LoadTursoLibraryConfig{})
	config, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := turso_database_new(config)
	if err != nil {
		return nil, err
	}
	if err := turso_database_open(db); err != nil {
		turso_database_deinit(db)
		return nil, err
	}
	c, err := turso_database_connect(db)
	if err != nil {
		turso_database_deinit(db)
		return nil, err
	}
	// Apply busy timeout - use default if not explicitly set
	// A value of -1 in config means explicitly disabled (no timeout)
	// A value of 0 means use the default timeout
	// A positive value is used as-is
	timeout := config.BusyTimeout
	if timeout == 0 {
		timeout = DefaultBusyTimeout // Apply sensible default
	} else if timeout < 0 {
		timeout = 0 // -1 means explicitly disable
	}
	if timeout > 0 {
		turso_connection_set_busy_timeout_ms(c, int64(timeout))
	}
	return &tursoDbConnection{
		db:          db,
		conn:        c,
		busyTimeout: timeout,
		async:       config.AsyncIO,
	}, nil
}

// --- driver.Conn and friends ---

// Ensure tursoDbConnection implements required interfaces.
var (
	_ driver.Conn               = (*tursoDbConnection)(nil)
	_ driver.ConnPrepareContext = (*tursoDbConnection)(nil)
	_ driver.ExecerContext      = (*tursoDbConnection)(nil)
	_ driver.QueryerContext     = (*tursoDbConnection)(nil)
	_ driver.Pinger             = (*tursoDbConnection)(nil)
	_ driver.ConnBeginTx        = (*tursoDbConnection)(nil)
)

func (c *tursoDbConnection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *tursoDbConnection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	// PREPARE in Prepare - do not delay that
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	stmt, err := turso_connection_prepare_single(c.conn, query)
	if err != nil {
		return nil, err
	}
	// determine number of inputs and then finalize immediately to avoid keeping state
	num := int(turso_statement_parameters_count(stmt))
	_ = turso_statement_finalize(stmt)
	turso_statement_deinit(stmt)

	return &tursoDbStatement{
		conn:      c,
		sql:       query,
		numInputs: num,
	}, nil
}

func (c *tursoDbConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	// Close connection and deinit resources
	if c.conn != nil {
		_ = turso_connection_close(c.conn)
		turso_connection_deinit(c.conn)
		c.conn = nil
	}
	if c.db != nil {
		turso_database_deinit(c.db)
		c.db = nil
	}
	c.closed = true
	return nil
}

func (c *tursoDbConnection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *tursoDbConnection) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	// Use BEGIN (snapshot isolation)
	_, err := c.ExecContext(ctx, "BEGIN", nil)
	if err != nil {
		return nil, err
	}
	return &tursoDbTx{conn: c}, nil
}

func (c *tursoDbConnection) Ping(ctx context.Context) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	// trivial ping: simple select constant
	_, err := c.QueryContext(ctx, "SELECT 1", nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *tursoDbConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	// Multi-statement support for Exec-family
	var totalAffected int64
	c.mu.Lock()
	defer c.mu.Unlock()

	offset := 0
	first := true
	var lastInsert int64 = 0
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		rest := query[offset:]
		if strings.TrimSpace(rest) == "" {
			break
		}
		stmt, tail, err := turso_connection_prepare_first(c.conn, rest)
		if err != nil {
			return nil, err
		}
		// Calculate absolute offset advance
		offset += tail

		// Bind only for the first statement
		if first && len(args) > 0 {
			if err := bindArgs(stmt, args); err != nil {
				_ = turso_statement_finalize(stmt)
				turso_statement_deinit(stmt)
				return nil, err
			}
		}
		// Execute statement fully
		affected, err := c.executeFully(ctx, stmt)
		// finalize and deinit regardless of status
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
		if err != nil {
			return nil, err
		}
		// rows affected is capped at MaxInt64
		if affected > uint64(math.MaxInt64-totalAffected) {
			totalAffected = math.MaxInt64
		} else {
			totalAffected += int64(affected)
		}
		lastInsert = turso_connection_last_insert_rowid(c.conn)
		first = false
		// continue with the rest of the query string
	}
	return &tursoDbResult{
		lastInsertId: lastInsert,
		rowsAffected: totalAffected,
	}, nil
}

func (c *tursoDbConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// Only single-statement queries supported here
	stmt, err := turso_connection_prepare_single(c.conn, query)
	if err != nil {
		return nil, err
	}
	if len(args) > 0 {
		if err := bindArgs(stmt, args); err != nil {
			_ = turso_statement_finalize(stmt)
			turso_statement_deinit(stmt)
			return nil, err
		}
	}
	// Return rows wrapper; do not step yet, leave cursor before first row
	return &tursoDbRows{
		conn: c,
		stmt: stmt,
	}, nil
}

func (c *tursoDbConnection) checkOpen() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.conn == nil {
		return ErrTursoConnClosed
	}
	return nil
}

// SetBusyTimeout sets the busy timeout for this connection in milliseconds.
// Pass 0 to disable the busy handler (immediate SQLITE_BUSY on contention).
// This method is thread-safe.
func (c *tursoDbConnection) SetBusyTimeout(timeoutMs int) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if timeoutMs < 0 {
		timeoutMs = 0
	}
	turso_connection_set_busy_timeout_ms(c.conn, int64(timeoutMs))
	c.busyTimeout = timeoutMs
	return nil
}

// GetBusyTimeout returns the current busy timeout in milliseconds.
// Returns 0 if the busy handler is disabled.
func (c *tursoDbConnection) GetBusyTimeout() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.busyTimeout
}

// --- Connector Pattern ---

// ConnectorOption configures a TursoConnector.
type ConnectorOption func(*TursoConnector)

// WithBusyTimeout sets the busy timeout in milliseconds.
// Use 0 to disable the busy handler, -1 to use the default (5000ms).
func WithBusyTimeout(ms int) ConnectorOption {
	return func(c *TursoConnector) {
		c.busyTimeout = ms
	}
}

// TursoConnector implements driver.Connector for programmatic configuration.
type TursoConnector struct {
	dsn         string
	busyTimeout int // -1 = use default, 0 = disabled, >0 = custom
}

// NewConnector creates a new TursoConnector with the given DSN and options.
// By default, uses the DefaultBusyTimeout (5000ms).
func NewConnector(dsn string, opts ...ConnectorOption) (*TursoConnector, error) {
	c := &TursoConnector{
		dsn:         dsn,
		busyTimeout: -1, // -1 means use default
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Connect implements driver.Connector.
func (c *TursoConnector) Connect(ctx context.Context) (driver.Conn, error) {
	InitLibrary(turso_libs.LoadTursoLibraryConfig{})
	config, err := parseDSN(c.dsn)
	if err != nil {
		return nil, err
	}
	// Override busy timeout from connector if set
	if c.busyTimeout >= 0 {
		// If connector explicitly sets 0, that means disabled
		// We use -1 internally to signal "disabled" to Open logic
		if c.busyTimeout == 0 {
			config.BusyTimeout = -1 // Will be converted to 0 in Open
		} else {
			config.BusyTimeout = c.busyTimeout
		}
	}
	// If busyTimeout is -1 (use default) and DSN didn't set one, leave it as 0
	// which will trigger the default in Open()

	db, err := turso_database_new(config)
	if err != nil {
		return nil, err
	}
	if err := turso_database_open(db); err != nil {
		turso_database_deinit(db)
		return nil, err
	}
	conn, err := turso_database_connect(db)
	if err != nil {
		turso_database_deinit(db)
		return nil, err
	}

	// Apply busy timeout - same logic as Open()
	timeout := config.BusyTimeout
	if timeout == 0 {
		timeout = DefaultBusyTimeout
	} else if timeout < 0 {
		timeout = 0
	}
	if timeout > 0 {
		turso_connection_set_busy_timeout_ms(conn, int64(timeout))
	}

	return &tursoDbConnection{
		db:          db,
		conn:        conn,
		busyTimeout: timeout,
		async:       config.AsyncIO,
	}, nil
}

// Driver implements driver.Connector.
func (c *TursoConnector) Driver() driver.Driver {
	return &tursoDbDriver{}
}

// Ensure TursoConnector implements driver.Connector
var _ driver.Connector = (*TursoConnector)(nil)

// --- driver.Stmt and friends ---

// Ensure tursoDbStatement implements required interfaces.
var (
	_ driver.Stmt             = (*tursoDbStatement)(nil)
	_ driver.StmtExecContext  = (*tursoDbStatement)(nil)
	_ driver.StmtQueryContext = (*tursoDbStatement)(nil)
)

func (s *tursoDbStatement) Close() error {
	s.closed = true
	return nil
}

func (s *tursoDbStatement) NumInput() int {
	return s.numInputs
}

func (s *tursoDbStatement) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), named)
}

func (s *tursoDbStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, ErrTursoStmtClosed
	}
	return s.conn.ExecContext(ctx, s.sql, args)
}

func (s *tursoDbStatement) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), named)
}

func (s *tursoDbStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed {
		return nil, ErrTursoStmtClosed
	}
	return s.conn.QueryContext(ctx, s.sql, args)
}

// --- driver.Rows ---

// Ensure tursoDbRows implements the required interface.
var _ driver.Rows = (*tursoDbRows)(nil)

func (r *tursoDbRows) Columns() []string {
	if r.columns != nil {
		return r.columns
	}
	n := int(turso_statement_column_count(r.stmt))
	names := make([]string, n)
	decltypes := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = turso_statement_column_name(r.stmt, i)
		decltypes[i] = turso_statement_column_decltype(r.stmt, i)
	}
	r.columns = names
	r.decltypes = decltypes
	return r.columns
}

func (r *tursoDbRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	_ = turso_statement_finalize(r.stmt)
	turso_statement_deinit(r.stmt)
	return nil
}

func (r *tursoDbRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}
	// Ensure decltypes are populated
	_ = r.Columns()
	for {
		status, err := turso_statement_step(r.stmt)
		if err != nil {
			r.err = err
			return err
		}
		switch status {
		case TURSO_ROW:
			// Fill destination
			n := int(turso_statement_column_count(r.stmt))
			if len(dest) != n {
				return fmt.Errorf("turso: expected %d dests, got %d", n, len(dest))
			}
			for i := 0; i < n; i++ {
				kind := turso_statement_row_value_kind(r.stmt, i)
				switch kind {
				case TURSO_TYPE_NULL:
					dest[i] = nil
				case TURSO_TYPE_INTEGER:
					dest[i] = turso_statement_row_value_int(r.stmt, i)
				case TURSO_TYPE_REAL:
					dest[i] = turso_statement_row_value_double(r.stmt, i)
				case TURSO_TYPE_TEXT:
					text := turso_statement_row_value_text(r.stmt, i)
					// Check if column type indicates a time value
					if i < len(r.decltypes) && isTimeColumn(r.decltypes[i]) {
						if t, err := parseTimeString(text); err == nil {
							dest[i] = t
						} else {
							dest[i] = text
						}
					} else {
						dest[i] = text
					}
				case TURSO_TYPE_BLOB:
					dest[i] = turso_statement_row_value_bytes(r.stmt, i)
				default:
					dest[i] = nil
				}
			}
			return nil
		case TURSO_DONE:
			return io.EOF
		case TURSO_IO:
			// Run IO iteration
			if r.conn.extraIo != nil {
				if err := r.conn.extraIo(); err != nil {
					r.err = err
					return err
				}
			}
			if err := turso_statement_run_io(r.stmt); err != nil {
				r.err = err
				return err
			}
			continue
		case TURSO_OK:
			// Continue stepping
			continue
		default:
			return ErrTursoGeneric
		}
	}
}

// --- driver.Result ---

var _ driver.Result = (*tursoDbResult)(nil)

func (r *tursoDbResult) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *tursoDbResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// --- driver.Tx ---

var _ driver.Tx = (*tursoDbTx)(nil)

func (tx *tursoDbTx) Commit() error {
	if tx.done {
		return ErrTursoTxDone
	}
	_, err := tx.conn.ExecContext(context.Background(), "COMMIT", nil)
	tx.done = true
	return err
}

func (tx *tursoDbTx) Rollback() error {
	if tx.done {
		return ErrTursoTxDone
	}
	_, err := tx.conn.ExecContext(context.Background(), "ROLLBACK", nil)
	tx.done = true
	return err
}

// Helpers

// parseDSN supports format: <path>[?experimental=<string>&async=0|1&vfs=<string>&encryption_cipher=<string>&encryption_hexkey=<string>&_busy_timeout=<int>]
func parseDSN(dsn string) (TursoDatabaseConfig, error) {
	config := TursoDatabaseConfig{Path: dsn}
	qMark := strings.IndexByte(dsn, '?')
	if qMark >= 0 {
		config.Path = dsn[:qMark]
		rawQuery := dsn[qMark+1:]
		vals, err := url.ParseQuery(rawQuery)
		if err != nil {
			return TursoDatabaseConfig{}, err
		}
		if v := vals.Get("experimental"); v != "" {
			config.ExperimentalFeatures = v
		}
		if v := vals.Get("async"); v != "" {
			config.AsyncIO = v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
		}
		if v := vals.Get("vfs"); v != "" {
			config.Vfs = v
		}
		if v := vals.Get("encryption_cipher"); v != "" {
			config.Encryption.Cipher = v
		}
		if v := vals.Get("encryption_hexkey"); v != "" {
			config.Encryption.Hexkey = v
		}
		if v := vals.Get("_busy_timeout"); v != "" {
			var timeout int
			if _, err := fmt.Sscanf(v, "%d", &timeout); err == nil {
				config.BusyTimeout = timeout
			}
		}
	}
	return config, nil
}

func (c *tursoDbConnection) executeFully(ctx context.Context, stmt TursoStatement) (uint64, error) {
	var latest uint64
	for {
		if ctx != nil && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		status, changes, err := turso_statement_execute(stmt)
		if err != nil {
			return 0, err
		}
		latest = changes
		switch status {
		case TURSO_DONE:
			return latest, nil
		case TURSO_IO:
			// perform one IO iteration and retry
			if c.extraIo != nil {
				if err := c.extraIo(); err != nil {
					return 0, err
				}
			}
			if err := turso_statement_run_io(stmt); err != nil {
				return 0, err
			}
			continue
		case TURSO_ROW:
			// Exhaust rows until DONE
			for {
				if ctx != nil && ctx.Err() != nil {
					return 0, ctx.Err()
				}
				st, err := turso_statement_step(stmt)
				if err != nil {
					return 0, err
				}
				if st == TURSO_ROW {
					continue
				}
				if st == TURSO_DONE {
					return latest, nil
				}
				if st == TURSO_IO {
					if c.extraIo != nil {
						if err := c.extraIo(); err != nil {
							return 0, err
						}
					}
					if err := turso_statement_run_io(stmt); err != nil {
						return 0, err
					}
					continue
				}
				// Continue on OK or others
			}
		case TURSO_OK:
			// keep going; step to progress
			st, err := turso_statement_step(stmt)
			if err != nil {
				return 0, err
			}
			if st == TURSO_DONE {
				return latest, nil
			}
			if st == TURSO_IO {
				if c.extraIo != nil {
					if err := c.extraIo(); err != nil {
						return 0, err
					}
				}
				if err := turso_statement_run_io(stmt); err != nil {
					return 0, err
				}
			}
			// and loop again
		default:
			return 0, statusToError(status, "")
		}
	}
}

// bindArgs binds ordered and named values to a statement.
// Named values are resolved via turso_statement_named_position, otherwise ordinal positions are used (1-based).
func bindArgs(stmt TursoStatement, args []driver.NamedValue) error {
	// Validate number of inputs if no named args present
	if len(args) > 0 {
		hasNamed := false
		for _, nv := range args {
			if nv.Name != "" {
				hasNamed = true
				break
			}
		}
		if !hasNamed {
			paramCount := int(turso_statement_parameters_count(stmt))
			if paramCount >= 0 && len(args) != paramCount {
				return fmt.Errorf("turso: got %d args, want %d", len(args), paramCount)
			}
		}
	}
	for idx, nv := range args {
		pos := idx + 1
		if nv.Name != "" {
			np := int(turso_statement_named_position(stmt, nv.Name))
			if np <= 0 {
				return fmt.Errorf("turso: unknown named parameter %q", nv.Name)
			}
			pos = np
		} else if nv.Ordinal > 0 {
			pos = nv.Ordinal
		}
		if err := bindOne(stmt, pos, nv.Value); err != nil {
			return err
		}
	}
	return nil
}

func bindOne(stmt TursoStatement, position int, v any) error {
	if v == nil {
		return turso_statement_bind_positional_null(stmt, position)
	}
	switch x := v.(type) {
	case int:
		return turso_statement_bind_positional_int(stmt, position, int64(x))
	case int8:
		return turso_statement_bind_positional_int(stmt, position, int64(x))
	case int16:
		return turso_statement_bind_positional_int(stmt, position, int64(x))
	case int32:
		return turso_statement_bind_positional_int(stmt, position, int64(x))
	case int64:
		return turso_statement_bind_positional_int(stmt, position, x)
	case uint:
		return turso_statement_bind_positional_int(stmt, position, int64(x))
	case uint8:
		return turso_statement_bind_positional_int(stmt, position, int64(x))
	case uint16:
		return turso_statement_bind_positional_int(stmt, position, int64(x))
	case uint32:
		return turso_statement_bind_positional_int(stmt, position, int64(x))
	case uint64:
		// cap at MaxInt64 to avoid overflow
		i := int64(0)
		if x > uint64(math.MaxInt64) {
			i = math.MaxInt64
		} else {
			i = int64(x)
		}
		return turso_statement_bind_positional_int(stmt, position, i)
	case float32:
		return turso_statement_bind_positional_double(stmt, position, float64(x))
	case float64:
		return turso_statement_bind_positional_double(stmt, position, x)
	case bool:
		if x {
			return turso_statement_bind_positional_int(stmt, position, 1)
		}
		return turso_statement_bind_positional_int(stmt, position, 0)
	case []byte:
		return turso_statement_bind_positional_blob(stmt, position, x)
	case string:
		return turso_statement_bind_positional_text(stmt, position, x)
	case time.Time:
		// encode as RFC3339Nano string
		return turso_statement_bind_positional_text(stmt, position, x.Format(time.RFC3339Nano))
	default:
		// Fallback to fmt to string
		return turso_statement_bind_positional_text(stmt, position, fmt.Sprint(v))
	}
}

// isTimeColumn checks if the column declared type indicates a time/date column.
// This matches the behavior of github.com/mattn/go-sqlite3.
func isTimeColumn(decltype string) bool {
	if decltype == "" {
		return false
	}
	// Case-insensitive exact match for TIMESTAMP, DATETIME, DATE
	// Matches go-sqlite3 behavior: https://github.com/mattn/go-sqlite3/blob/master/sqlite3_type.go
	upper := strings.ToUpper(decltype)
	return upper == "TIMESTAMP" || upper == "DATETIME" || upper == "DATE"
}

// SQLiteTimestampFormats are the timestamp formats supported by go-sqlite3.
// https://github.com/mattn/go-sqlite3/blob/master/sqlite3.go
var SQLiteTimestampFormats = []string{
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
// This matches the behavior of github.com/mattn/go-sqlite3.
func parseTimeString(s string) (time.Time, error) {
	// Strip trailing "Z" suffix before parsing (go-sqlite3 behavior)
	s = strings.TrimSuffix(s, "Z")
	for _, format := range SQLiteTimestampFormats {
		if t, err := time.ParseInLocation(format, s, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as time", s)
}
