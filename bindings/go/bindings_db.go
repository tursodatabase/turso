package turso // import "github.com/tursodatabase/turso/go"

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// package-level errors
var (
	ErrTursoBusy         = errors.New("turso: database is busy")
	ErrTursoInterrupt    = errors.New("turso: interrupted")
	ErrTursoGeneric      = errors.New("turso: error")
	ErrTursoMisuse       = errors.New("turso: API misuse")
	ErrTursoConstraint   = errors.New("turso: constraint failed")
	ErrTursoReadOnly     = errors.New("turso: database is readonly")
	ErrTursoDatabaseFull = errors.New("turso: database is full")
	ErrTursoNotADb       = errors.New("turso: not a database")
	ErrTursoCorrupt      = errors.New("turso: database is corrupt")
)

// define all necessary constants first
type TursoStatusCode int32

const (
	TURSO_OK            TursoStatusCode = 0
	TURSO_DONE          TursoStatusCode = 1
	TURSO_ROW           TursoStatusCode = 2
	TURSO_IO            TursoStatusCode = 3
	TURSO_BUSY          TursoStatusCode = 4
	TURSO_INTERRUPT     TursoStatusCode = 5
	TURSO_ERROR         TursoStatusCode = 127
	TURSO_MISUSE        TursoStatusCode = 128
	TURSO_CONSTRAINT    TursoStatusCode = 129
	TURSO_READONLY      TursoStatusCode = 130
	TURSO_DATABASE_FULL TursoStatusCode = 131
	TURSO_NOTADB        TursoStatusCode = 132
	TURSO_CORRUPT       TursoStatusCode = 133
)

type TursoType int32

const (
	TURSO_TYPE_UNKNOWN TursoType = 0
	TURSO_TYPE_INTEGER TursoType = 1
	TURSO_TYPE_REAL    TursoType = 2
	TURSO_TYPE_TEXT    TursoType = 3
	TURSO_TYPE_BLOB    TursoType = 4
	TURSO_TYPE_NULL    TursoType = 5
)

type TursoTracingLevel int32

const (
	TURSO_TRACING_LEVEL_ERROR TursoTracingLevel = 1
	TURSO_TRACING_LEVEL_WARN  TursoTracingLevel = 2
	TURSO_TRACING_LEVEL_INFO  TursoTracingLevel = 3
	TURSO_TRACING_LEVEL_DEBUG TursoTracingLevel = 4
	TURSO_TRACING_LEVEL_TRACE TursoTracingLevel = 5
)

// define opaque pointers as-is and accept them as exact arguments
type turso_database_t struct{}
type turso_connection_t struct{}
type turso_statement_t struct{}

type TursoDatabase *turso_database_t
type TursoConnection *turso_connection_t
type TursoStatement *turso_statement_t

// define all public binding types
type TursoLog struct {
	Message   string
	Target    string
	File      string
	Timestamp uint64
	Line      uint
	Level     TursoTracingLevel
}

type TursoConfig struct {
	// Logger is an optional callback invoked by the library.
	Logger   func(log TursoLog)
	LogLevel string // zero-terminated C string expected by C; wrapper converts
}

type TursoDatabaseEncryptionOpts struct {
	Cipher string
	Hexkey string
}

type TursoDatabaseConfig struct {
	// Path to the database file or ":memory:"
	Path string
	// Optional comma separated list of experimental features to enable
	ExperimentalFeatures string
	// Parameter which defines who drives the IO - callee or the caller
	AsyncIO bool
	// optional VFS parameter explicitly specifying FS backend for the database.
	// Available options are:
	// - "memory": in-memory backend
	// - "syscall": generic syscall backend
	// - "io_uring": IO uring (supported only on Linux)
	Vfs string
	// optional encryption parameters
	// as encryption is experimental - ExperimentalFeatures must have "encryption" in the list
	Encryption TursoDatabaseEncryptionOpts
}

// define all necessary private C structs
type turso_slice_ref_t struct {
	ptr uintptr
	len uintptr
}

type turso_log_t struct {
	message   uintptr // const char*
	target    uintptr // const char*
	file      uintptr // const char*
	timestamp uint64
	line      uint  // size_t
	level     int32 // turso_tracing_level_t
}

type turso_config_t struct {
	logger    uintptr // void (*logger)(const turso_log_t *log)
	log_level uintptr
}

type turso_database_config_t struct {
	async_io              uint64  // non-zero value interpreted as async IO
	path                  uintptr // const char*
	experimental_features uintptr // const char* or null
	vfs                   uintptr // const char* or null
	encryption_cipher     uintptr // const char* or null
	encryption_hexkey     uintptr // const char* or null
}

// C extern method types
type turso_status_code_t = int32

// then, define C extern methods
var (
	c_turso_setup                            func(config *turso_config_t, error_opt_out **byte) turso_status_code_t
	c_turso_database_new                     func(config *turso_database_config_t, database **turso_database_t, error_opt_out **byte) turso_status_code_t
	c_turso_database_open                    func(database TursoDatabase, error_opt_out **byte) turso_status_code_t
	c_turso_database_connect                 func(self TursoDatabase, connection **turso_connection_t, error_opt_out **byte) turso_status_code_t
	c_turso_connection_get_autocommit        func(self TursoConnection) bool
	c_turso_connection_set_busy_timeout_ms   func(self TursoConnection, timeout_ms int64)
	c_turso_connection_last_insert_rowid     func(self TursoConnection) int64
	c_turso_connection_prepare_single        func(self TursoConnection, sql string, statement **turso_statement_t, error_opt_out **byte) turso_status_code_t
	c_turso_connection_prepare_first         func(self TursoConnection, sql string, statement **turso_statement_t, tail_idx *uintptr, error_opt_out **byte) turso_status_code_t
	c_turso_connection_close                 func(self TursoConnection, error_opt_out **byte) turso_status_code_t
	c_turso_statement_execute                func(self TursoStatement, rows_changes *uint64, error_opt_out **byte) turso_status_code_t
	c_turso_statement_step                   func(self TursoStatement, error_opt_out **byte) turso_status_code_t
	c_turso_statement_run_io                 func(self TursoStatement, error_opt_out **byte) turso_status_code_t
	c_turso_statement_reset                  func(self TursoStatement, error_opt_out **byte) turso_status_code_t
	c_turso_statement_finalize               func(self TursoStatement, error_opt_out **byte) turso_status_code_t
	c_turso_statement_n_change               func(self TursoStatement) int64
	c_turso_statement_column_count           func(self TursoStatement) int64
	c_turso_statement_column_name            func(self TursoStatement, index uintptr) *byte
	c_turso_statement_row_value_kind         func(self TursoStatement, index uintptr) int32
	c_turso_statement_row_value_bytes_count  func(self TursoStatement, index uintptr) int64
	c_turso_statement_row_value_bytes_ptr    func(self TursoStatement, index uintptr) *byte
	c_turso_statement_row_value_int          func(self TursoStatement, index uintptr) int64
	c_turso_statement_row_value_double       func(self TursoStatement, index uintptr) float64
	c_turso_statement_named_position         func(self TursoStatement, name string) int64
	c_turso_statement_parameters_count       func(self TursoStatement) int64
	c_turso_statement_bind_positional_null   func(self TursoStatement, position uintptr) turso_status_code_t
	c_turso_statement_bind_positional_int    func(self TursoStatement, position uintptr, value int64) turso_status_code_t
	c_turso_statement_bind_positional_double func(self TursoStatement, position uintptr, value float64) turso_status_code_t
	c_turso_statement_bind_positional_blob   func(self TursoStatement, position uintptr, ptr *byte, len uintptr) turso_status_code_t
	c_turso_statement_bind_positional_text   func(self TursoStatement, position uintptr, ptr *byte, len uintptr) turso_status_code_t
	c_turso_str_deinit                       func(self *byte)
	c_turso_database_deinit                  func(self TursoDatabase)
	c_turso_connection_deinit                func(self TursoConnection)
	c_turso_statement_deinit                 func(self TursoStatement)
)

// implement a function to register extern methods from loaded lib
// DO NOT load lib - as it will be done externally
func registerTursoDb(handle uintptr) error {
	purego.RegisterLibFunc(&c_turso_setup, handle, "turso_setup")
	purego.RegisterLibFunc(&c_turso_database_new, handle, "turso_database_new")
	purego.RegisterLibFunc(&c_turso_database_open, handle, "turso_database_open")
	purego.RegisterLibFunc(&c_turso_database_connect, handle, "turso_database_connect")
	purego.RegisterLibFunc(&c_turso_connection_get_autocommit, handle, "turso_connection_get_autocommit")
	purego.RegisterLibFunc(&c_turso_connection_set_busy_timeout_ms, handle, "turso_connection_set_busy_timeout_ms")
	purego.RegisterLibFunc(&c_turso_connection_last_insert_rowid, handle, "turso_connection_last_insert_rowid")
	purego.RegisterLibFunc(&c_turso_connection_prepare_single, handle, "turso_connection_prepare_single")
	purego.RegisterLibFunc(&c_turso_connection_prepare_first, handle, "turso_connection_prepare_first")
	purego.RegisterLibFunc(&c_turso_connection_close, handle, "turso_connection_close")
	purego.RegisterLibFunc(&c_turso_statement_execute, handle, "turso_statement_execute")
	purego.RegisterLibFunc(&c_turso_statement_step, handle, "turso_statement_step")
	purego.RegisterLibFunc(&c_turso_statement_run_io, handle, "turso_statement_run_io")
	purego.RegisterLibFunc(&c_turso_statement_reset, handle, "turso_statement_reset")
	purego.RegisterLibFunc(&c_turso_statement_finalize, handle, "turso_statement_finalize")
	purego.RegisterLibFunc(&c_turso_statement_n_change, handle, "turso_statement_n_change")
	purego.RegisterLibFunc(&c_turso_statement_column_count, handle, "turso_statement_column_count")
	purego.RegisterLibFunc(&c_turso_statement_column_name, handle, "turso_statement_column_name")
	purego.RegisterLibFunc(&c_turso_statement_row_value_kind, handle, "turso_statement_row_value_kind")
	purego.RegisterLibFunc(&c_turso_statement_row_value_bytes_count, handle, "turso_statement_row_value_bytes_count")
	purego.RegisterLibFunc(&c_turso_statement_row_value_bytes_ptr, handle, "turso_statement_row_value_bytes_ptr")
	purego.RegisterLibFunc(&c_turso_statement_row_value_int, handle, "turso_statement_row_value_int")
	purego.RegisterLibFunc(&c_turso_statement_row_value_double, handle, "turso_statement_row_value_double")
	purego.RegisterLibFunc(&c_turso_statement_named_position, handle, "turso_statement_named_position")
	purego.RegisterLibFunc(&c_turso_statement_parameters_count, handle, "turso_statement_parameters_count")
	purego.RegisterLibFunc(&c_turso_statement_bind_positional_null, handle, "turso_statement_bind_positional_null")
	purego.RegisterLibFunc(&c_turso_statement_bind_positional_int, handle, "turso_statement_bind_positional_int")
	purego.RegisterLibFunc(&c_turso_statement_bind_positional_double, handle, "turso_statement_bind_positional_double")
	purego.RegisterLibFunc(&c_turso_statement_bind_positional_blob, handle, "turso_statement_bind_positional_blob")
	purego.RegisterLibFunc(&c_turso_statement_bind_positional_text, handle, "turso_statement_bind_positional_text")
	purego.RegisterLibFunc(&c_turso_str_deinit, handle, "turso_str_deinit")
	purego.RegisterLibFunc(&c_turso_database_deinit, handle, "turso_database_deinit")
	purego.RegisterLibFunc(&c_turso_connection_deinit, handle, "turso_connection_deinit")
	purego.RegisterLibFunc(&c_turso_statement_deinit, handle, "turso_statement_deinit")
	return nil
}

// Helper: map status code to default error kind
func statusToError(status TursoStatusCode, msg string) error {
	var base error
	switch status {
	case TURSO_BUSY:
		base = ErrTursoBusy
	case TURSO_INTERRUPT:
		base = ErrTursoInterrupt
	case TURSO_ERROR:
		base = ErrTursoGeneric
	case TURSO_MISUSE:
		base = ErrTursoMisuse
	case TURSO_CONSTRAINT:
		base = ErrTursoConstraint
	case TURSO_READONLY:
		base = ErrTursoReadOnly
	case TURSO_DATABASE_FULL:
		base = ErrTursoDatabaseFull
	case TURSO_NOTADB:
		base = ErrTursoNotADb
	case TURSO_CORRUPT:
		base = ErrTursoCorrupt
	default:
		// for unknown error codes, fallback to generic
		base = ErrTursoGeneric
	}
	if msg != "" {
		return fmt.Errorf("%w: %s", base, msg)
	}
	return base
}

func decodeAndFreeCString(p *byte) string {
	if p == nil {
		return ""
	}
	// determine length
	var n uintptr
	for {
		b := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + n))
		if b == 0 {
			break
		}
		n++
	}
	s := string(unsafe.Slice(p, n))
	// free C-allocated string
	c_turso_str_deinit(p)
	return s
}

func decodeCStringNoFree(p uintptr) string {
	if p == 0 {
		return ""
	}
	cur := (*byte)(unsafe.Pointer(p))
	// determine length
	var n uintptr
	for {
		b := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(cur)) + n))
		if b == 0 {
			break
		}
		n++
	}
	return string(unsafe.Slice(cur, n))
}

func makeCStringBytes(s string) ([]byte, uintptr) {
	if s == "" {
		return nil, 0
	}
	b := make([]byte, 0, len(s)+1)
	b = append(b, s...)
	b = append(b, 0)
	return b, uintptr(unsafe.Pointer(&b[0]))
}

// ------- Logger callback plumbing --------
var (
	loggerCallback uintptr
	loggerHandler  func(TursoLog)
)

func init() {
	loggerCallback = purego.NewCallback(func(p uintptr) uintptr {
		if p == 0 || loggerHandler == nil {
			return 0
		}
		cl := (*turso_log_t)(unsafe.Pointer(p))
		log := TursoLog{
			Message:   decodeCStringNoFree(cl.message),
			Target:    decodeCStringNoFree(cl.target),
			File:      decodeCStringNoFree(cl.file),
			Timestamp: cl.timestamp,
			Line:      uint(cl.line),
			Level:     TursoTracingLevel(cl.level),
		}
		// SAFETY: strings are copied above; no freeing needed.
		loggerHandler(log)
		return 0
	})
}

// Go wrappers over imported C bindings

// turso_setup sets up global database info.
func turso_setup(config TursoConfig) error {
	var cconf turso_config_t
	// Set logger callback
	if config.Logger != nil {
		loggerHandler = config.Logger
		cconf.logger = loggerCallback
	}
	// Log level C-string pointer
	var levelBytes []byte
	levelBytes, cconf.log_level = makeCStringBytes(config.LogLevel)
	var errPtr *byte
	status := c_turso_setup(&cconf, &errPtr)
	// Keep Go memory alive during C call
	runtime.KeepAlive(levelBytes)
	if status == int32(TURSO_OK) {
		return nil
	}
	msg := decodeAndFreeCString(errPtr)
	return statusToError(TursoStatusCode(status), msg)
}

// turso_database_new creates database holder but do not open it.
func turso_database_new(config TursoDatabaseConfig) (TursoDatabase, error) {
	var cconf turso_database_config_t
	var pathBytes []byte
	var expBytes []byte
	var vfsBytes []byte
	var encryptionCipherBytes []byte
	var encryptionHexkeyBytes []byte
	pathBytes, cconf.path = makeCStringBytes(config.Path)
	if config.ExperimentalFeatures != "" {
		expBytes, cconf.experimental_features = makeCStringBytes(config.ExperimentalFeatures)
	}
	if config.Vfs != "" {
		vfsBytes, cconf.vfs = makeCStringBytes(config.Vfs)
	}
	if config.Encryption.Cipher != "" {
		encryptionCipherBytes, cconf.encryption_cipher = makeCStringBytes(config.Encryption.Cipher)
	}
	if config.Encryption.Hexkey != "" {
		encryptionHexkeyBytes, cconf.encryption_hexkey = makeCStringBytes(config.Encryption.Hexkey)
	}
	cconf.async_io = 0
	if config.AsyncIO {
		cconf.async_io = 1
	}

	var db *turso_database_t
	var errPtr *byte
	status := c_turso_database_new(&cconf, &db, &errPtr)
	runtime.KeepAlive(pathBytes)
	runtime.KeepAlive(expBytes)
	runtime.KeepAlive(vfsBytes)
	runtime.KeepAlive(encryptionCipherBytes)
	runtime.KeepAlive(encryptionHexkeyBytes)
	if status == int32(TURSO_OK) {
		return TursoDatabase(db), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_database_open opens the database.
func turso_database_open(database TursoDatabase) error {
	var errPtr *byte
	status := c_turso_database_open(database, &errPtr)
	if status == int32(TURSO_OK) {
		return nil
	}
	msg := decodeAndFreeCString(errPtr)
	return statusToError(TursoStatusCode(status), msg)
}

// turso_database_connect connects to the database and returns a connection.
func turso_database_connect(self TursoDatabase) (TursoConnection, error) {
	var conn *turso_connection_t
	var errPtr *byte
	status := c_turso_database_connect(self, &conn, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoConnection(conn), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_connection_get_autocommit returns the autocommit state of the connection.
func turso_connection_get_autocommit(self TursoConnection) bool {
	return c_turso_connection_get_autocommit(self)
}

// turso_connection_set_busy_timeout_ms sets busy timeout for the connection
func turso_connection_set_busy_timeout_ms(self TursoConnection, timeoutMs int64) {
	c_turso_connection_set_busy_timeout_ms(self, timeoutMs)
}

// turso_connection_last_insert_rowid returns last insert rowid.
func turso_connection_last_insert_rowid(self TursoConnection) int64 {
	return c_turso_connection_last_insert_rowid(self)
}

// turso_connection_prepare_single prepares a single statement in a connection.
func turso_connection_prepare_single(self TursoConnection, sql string) (TursoStatement, error) {
	var stmt *turso_statement_t
	var errPtr *byte
	status := c_turso_connection_prepare_single(self, sql, &stmt, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoStatement(stmt), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_connection_prepare_first prepares the first statement from a string containing multiple statements.
func turso_connection_prepare_first(self TursoConnection, sql string) (TursoStatement, int, error) {
	var stmt *turso_statement_t
	var tail uintptr
	var errPtr *byte
	status := c_turso_connection_prepare_first(self, sql, &stmt, &tail, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoStatement(stmt), int(tail), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, 0, statusToError(TursoStatusCode(status), msg)
}

// turso_connection_close closes the connection preventing any further operations.
func turso_connection_close(self TursoConnection) error {
	var errPtr *byte
	status := c_turso_connection_close(self, &errPtr)
	if status == int32(TURSO_OK) {
		return nil
	}
	msg := decodeAndFreeCString(errPtr)
	return statusToError(TursoStatusCode(status), msg)
}

// turso_statement_execute executes a single statement
// * execute returns TURSO_DONE if execution completed
// * execute returns TURSO_IO if async_io was set and execution needs IO in order to make progress
func turso_statement_execute(self TursoStatement) (TursoStatusCode, uint64, error) {
	var changes uint64
	var errPtr *byte
	status := c_turso_statement_execute(self, &changes, &errPtr)
	switch TursoStatusCode(status) {
	case TURSO_OK, TURSO_DONE, TURSO_ROW, TURSO_IO:
		return TursoStatusCode(status), changes, nil
	default:
		msg := decodeAndFreeCString(errPtr)
		return TursoStatusCode(status), 0, statusToError(TursoStatusCode(status), msg)
	}
}

// turso_statement_step steps statement execution once. Returns DONE, ROW, IO, or error.
func turso_statement_step(self TursoStatement) (TursoStatusCode, error) {
	var errPtr *byte
	status := c_turso_statement_step(self, &errPtr)
	switch TursoStatusCode(status) {
	case TURSO_OK, TURSO_DONE, TURSO_ROW, TURSO_IO:
		return TursoStatusCode(status), nil
	default:
		msg := decodeAndFreeCString(errPtr)
		return TursoStatusCode(status), statusToError(TursoStatusCode(status), msg)
	}
}

// turso_statement_run_io executes one iteration of underlying IO backend after TURSO_IO.
func turso_statement_run_io(self TursoStatement) error {
	var errPtr *byte
	status := c_turso_statement_run_io(self, &errPtr)
	if status == int32(TURSO_OK) {
		return nil
	}
	msg := decodeAndFreeCString(errPtr)
	return statusToError(TursoStatusCode(status), msg)
}

// turso_statement_reset resets a statement.
// this method must be called in order to cleanup statement resources and prepare it for re-execution
// any pending execution will be aborted - be careful and in certain cases ensure that turso_statement_finalize called before turso_statement_reset
func turso_statement_reset(self TursoStatement) error {
	var errPtr *byte
	status := c_turso_statement_reset(self, &errPtr)
	if status == int32(TURSO_OK) {
		return nil
	}
	msg := decodeAndFreeCString(errPtr)
	return statusToError(TursoStatusCode(status), msg)
}

// turso_statement_finalize finalizes a statement.
func turso_statement_finalize(self TursoStatement) error {
	var errPtr *byte
	status := c_turso_statement_finalize(self, &errPtr)
	if status == int32(TURSO_OK) {
		return nil
	}
	msg := decodeAndFreeCString(errPtr)
	return statusToError(TursoStatusCode(status), msg)
}

// turso_statement_n_change returns amount of row modifications (insert/delete operations) made by the most recent executed statement.
func turso_statement_n_change(self TursoStatement) int64 {
	return c_turso_statement_n_change(self)
}

// turso_statement_column_count returns the number of columns.
func turso_statement_column_count(self TursoStatement) int64 {
	return c_turso_statement_column_count(self)
}

// turso_statement_column_name returns the column name at the index.
// The underlying C string is freed automatically.
func turso_statement_column_name(self TursoStatement, index int) string {
	ptr := c_turso_statement_column_name(self, uintptr(index))
	return decodeAndFreeCString(ptr)
}

// turso_statement_row_value_kind returns the row value kind at index.
func turso_statement_row_value_kind(self TursoStatement, index int) TursoType {
	return TursoType(c_turso_statement_row_value_kind(self, uintptr(index)))
}

// turso_statement_row_value_bytes_count returns number of bytes for BLOB or TEXT, -1 otherwise.
func turso_statement_row_value_bytes_count(self TursoStatement, index int) int64 {
	return c_turso_statement_row_value_bytes_count(self, uintptr(index))
}

// turso_statement_row_value_bytes_ptr returns pointer to start of BLOB/TEXT slice, or nil otherwise.
func turso_statement_row_value_bytes_ptr(self TursoStatement, index int) uintptr {
	return uintptr(unsafe.Pointer(c_turso_statement_row_value_bytes_ptr(self, uintptr(index))))
}

// turso_statement_row_value_int returns INTEGER value at index, or 0 otherwise.
func turso_statement_row_value_int(self TursoStatement, index int) int64 {
	return c_turso_statement_row_value_int(self, uintptr(index))
}

// turso_statement_row_value_double returns REAL value at index, or 0 otherwise.
func turso_statement_row_value_double(self TursoStatement, index int) float64 {
	return c_turso_statement_row_value_double(self, uintptr(index))
}

// turso_statement_named_position returns named argument position in a statement.
func turso_statement_named_position(self TursoStatement, name string) int64 {
	return c_turso_statement_named_position(self, name)
}

// turso_statement_parameters_count returns parameters count for the statement.
func turso_statement_parameters_count(self TursoStatement) int64 {
	return c_turso_statement_parameters_count(self)
}

// turso_statement_bind_positional_null binds a positional argument as NULL.
func turso_statement_bind_positional_null(self TursoStatement, position int) error {
	status := c_turso_statement_bind_positional_null(self, uintptr(position))
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_statement_bind_positional_int binds a positional argument as INTEGER.
func turso_statement_bind_positional_int(self TursoStatement, position int, value int64) error {
	status := c_turso_statement_bind_positional_int(self, uintptr(position), value)
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_statement_bind_positional_double binds a positional argument as REAL.
func turso_statement_bind_positional_double(self TursoStatement, position int, value float64) error {
	status := c_turso_statement_bind_positional_double(self, uintptr(position), value)
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_statement_bind_positional_blob binds a positional argument as BLOB.
func turso_statement_bind_positional_blob(self TursoStatement, position int, value []byte) error {
	var ptr *byte
	var length uintptr
	if len(value) > 0 {
		ptr = &value[0]
		length = uintptr(len(value))
	}
	status := c_turso_statement_bind_positional_blob(self, uintptr(position), ptr, length)
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_statement_bind_positional_text binds a positional argument as TEXT.
// Note: underlying C API expects a pointer and length, not a zero-terminated string.
func turso_statement_bind_positional_text(self TursoStatement, position int, value string) error {
	var ptr *byte
	var length uintptr
	if value != "" {
		// Point directly to string data; valid for the duration of the call.
		ptr = (*byte)(unsafe.Pointer(unsafe.StringData(value)))
		length = uintptr(len(value))
	}
	status := c_turso_statement_bind_positional_text(self, uintptr(position), ptr, length)
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_database_deinit deallocates and closes a database.
// SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited database.
func turso_database_deinit(self TursoDatabase) {
	c_turso_database_deinit(self)
}

// turso_connection_deinit deallocates and closes a connection.
// SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited connection.
func turso_connection_deinit(self TursoConnection) {
	c_turso_connection_deinit(self)
}

// turso_statement_deinit deallocates and closes a statement.
// SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited statement.
func turso_statement_deinit(self TursoStatement) {
	c_turso_statement_deinit(self)
}

// Additional ergonomic helpers (the only non-direct translations):
// turso_statement_row_value_bytes returns a copy of bytes for BLOB or TEXT values, nil otherwise.
func turso_statement_row_value_bytes(self TursoStatement, index int) []byte {
	n := c_turso_statement_row_value_bytes_count(self, uintptr(index))
	if n <= 0 {
		return nil
	}
	ptr := c_turso_statement_row_value_bytes_ptr(self, uintptr(index))
	if ptr == nil {
		return nil
	}
	src := unsafe.Slice(ptr, n)
	dst := make([]byte, n)
	copy(dst, src)
	return dst
}

// turso_statement_row_value_text returns a copy of text for TEXT values, "" otherwise.
func turso_statement_row_value_text(self TursoStatement, index int) string {
	n := c_turso_statement_row_value_bytes_count(self, uintptr(index))
	if n <= 0 {
		return ""
	}
	ptr := c_turso_statement_row_value_bytes_ptr(self, uintptr(index))
	if ptr == nil {
		return ""
	}
	bs := unsafe.Slice(ptr, n)
	// converting []byte to string copies
	return string(bs)
}
