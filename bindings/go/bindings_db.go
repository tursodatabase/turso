package turso

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// define all package level errors here

// note, that OK, DONE, ROW, IO are statuses - so you don't need to create errors for them
var (
	TursoBusyErr         = errors.New("turso: database is busy")
	TursoInterruptErr    = errors.New("turso: operation interrupted")
	TursoErrorErr        = errors.New("turso: generic error")
	TursoMisuseErr       = errors.New("turso: API misuse")
	TursoConstraintErr   = errors.New("turso: constraint failed")
	TursoReadonlyErr     = errors.New("turso: database is read-only")
	TursoDatabaseFullErr = errors.New("turso: database or disk is full")
	TursoNotADbErr       = errors.New("turso: not a database")
	TursoCorruptErr      = errors.New("turso: database is corrupt")
)

// define all necessary constants first
type TursoStatusCode int32

// note, that the only real statuses are OK, DONE, ROW, IO - everything else is errors
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
// the public binding types MUST have fields with native safe go types
type TursoLog struct {
	Message   string
	Target    string
	File      string
	Timestamp uint64
	Line      uint
	Level     TursoTracingLevel
}

// Logger callback signature
type TursoLogger func(log TursoLog)

type TursoConfig struct {
	// Logger is an optional callback to receive log events from the database.
	// All string fields in TursoLog are copied and safe to use beyond the callback return.
	Logger   TursoLogger
	LogLevel string
}

/*
*
Database description.
*/
type TursoDatabaseConfig struct {
	// Path to the database file or ":memory:"
	Path string
	// Optional comma separated list of experimental features to enable
	ExperimentalFeatures string
	// Parameter which defines who drives the IO - callee or the caller
	AsyncIO bool
}

// define all necessary private C structs
// private C structs MUST have fields with low level types (e.g. uintptr, numbers)
type turso_status_code_t int32
type turso_type_t int32

// Used only for reading callback payloads.
type c_turso_log_t struct {
	Message   uintptr // const char*
	Target    uintptr // const char*
	File      uintptr // const char*
	Timestamp uint64
	Line      uintptr // size_t
	Level     int32   // turso_tracing_level_t
	_         [4]byte // padding to match C alignment (ensure 8-byte struct alignment)
}

type c_turso_config_t struct {
	Logger   uintptr // void (*)(const turso_log_t*)
	LogLevel uintptr // const char*
}

type c_turso_database_config_t struct {
	Path                 uintptr // const char*
	ExperimentalFeatures uintptr // const char* | NULL
	AsyncIO              uint8   // _Bool
	_                    [7]byte // padding to 8-byte alignment
}

// then, define C extern methods
var (
	// always use c_ structs here - never mix them with exported public types
	c_turso_setup func(
		config unsafe.Pointer,
		errorOptOut unsafe.Pointer,
	) turso_status_code_t

	c_turso_database_new func(
		config unsafe.Pointer,
		database unsafe.Pointer, // turso_database_t**
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_database_open func(
		database unsafe.Pointer, // const turso_database_t*
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_database_connect func(
		self unsafe.Pointer, // const turso_database_t*
		connection unsafe.Pointer, // turso_connection_t**
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_connection_get_autocommit func(
		self unsafe.Pointer, // const turso_connection_t*
	) bool

	c_turso_connection_prepare_single func(
		self unsafe.Pointer, // const turso_connection_t*
		sql string, // const char*
		statement unsafe.Pointer, // turso_statement_t**
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_connection_prepare_first func(
		self unsafe.Pointer, // const turso_connection_t*
		sql string, // const char*
		statement unsafe.Pointer, // turso_statement_t**
		tailIdx unsafe.Pointer, // size_t*
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_connection_close func(
		self unsafe.Pointer, // const turso_connection_t*
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_statement_execute func(
		self unsafe.Pointer, // const turso_statement_t*
		rowsChanges unsafe.Pointer, // uint64_t*
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_statement_step func(
		self unsafe.Pointer, // const turso_statement_t*
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_statement_run_io func(
		self unsafe.Pointer, // const turso_statement_t*
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_statement_reset func(
		self unsafe.Pointer, // const turso_statement_t*
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_statement_finalize func(
		self unsafe.Pointer, // const turso_statement_t*
		errorOptOut unsafe.Pointer, // const char**
	) turso_status_code_t

	c_turso_statement_column_count func(
		self unsafe.Pointer, // const turso_statement_t*
	) int64

	c_turso_statement_column_name func(
		self unsafe.Pointer, // const turso_statement_t*
		index uintptr, // size_t
	) unsafe.Pointer // const char*

	c_turso_statement_row_value_kind func(
		self unsafe.Pointer, // const turso_statement_t*
		index uintptr, // size_t
	) turso_type_t

	c_turso_statement_row_value_bytes_count func(
		self unsafe.Pointer,
		index uintptr,
	) int64

	c_turso_statement_row_value_bytes_ptr func(
		self unsafe.Pointer,
		index uintptr,
	) unsafe.Pointer // const char*

	c_turso_statement_row_value_int func(
		self unsafe.Pointer,
		index uintptr,
	) int64

	c_turso_statement_row_value_double func(
		self unsafe.Pointer,
		index uintptr,
	) float64

	c_turso_statement_named_position func(
		self unsafe.Pointer, // const turso_statement_t*
		name string, // const char*
	) int64

	c_turso_statement_bind_positional_null func(
		self unsafe.Pointer,
		position uintptr, // size_t
	) turso_status_code_t

	c_turso_statement_bind_positional_int func(
		self unsafe.Pointer,
		position uintptr,
		value int64,
	) turso_status_code_t

	c_turso_statement_bind_positional_double func(
		self unsafe.Pointer,
		position uintptr,
		value float64,
	) turso_status_code_t

	c_turso_statement_bind_positional_blob func(
		self unsafe.Pointer,
		position uintptr,
		ptr unsafe.Pointer, // const uint8_t*
		len uintptr, // size_t
	) turso_status_code_t

	c_turso_statement_bind_positional_text func(
		self unsafe.Pointer,
		position uintptr,
		ptr string, // const char*
		len uintptr, // size_t
	) turso_status_code_t

	c_turso_str_deinit func(
		self unsafe.Pointer, // const char*
	)

	c_turso_database_deinit func(
		self unsafe.Pointer, // const turso_database_t*
	)

	c_turso_connection_deinit func(
		self unsafe.Pointer, // const turso_connection_t*
	)

	c_turso_statement_deinit func(
		self unsafe.Pointer, // const turso_statement_t*
	)
)

// implement a function to register extern methods from loaded lib
// DO NOT load lib - as it will be done externally
func register_turso_db(handle uintptr) error {
	purego.RegisterLibFunc(&c_turso_setup, handle, "turso_setup")
	purego.RegisterLibFunc(&c_turso_database_new, handle, "turso_database_new")
	purego.RegisterLibFunc(&c_turso_database_open, handle, "turso_database_open")
	purego.RegisterLibFunc(&c_turso_database_connect, handle, "turso_database_connect")
	purego.RegisterLibFunc(&c_turso_connection_get_autocommit, handle, "turso_connection_get_autocommit")
	purego.RegisterLibFunc(&c_turso_connection_prepare_single, handle, "turso_connection_prepare_single")
	purego.RegisterLibFunc(&c_turso_connection_prepare_first, handle, "turso_connection_prepare_first")
	purego.RegisterLibFunc(&c_turso_connection_close, handle, "turso_connection_close")
	purego.RegisterLibFunc(&c_turso_statement_execute, handle, "turso_statement_execute")
	purego.RegisterLibFunc(&c_turso_statement_step, handle, "turso_statement_step")
	purego.RegisterLibFunc(&c_turso_statement_run_io, handle, "turso_statement_run_io")
	purego.RegisterLibFunc(&c_turso_statement_reset, handle, "turso_statement_reset")
	purego.RegisterLibFunc(&c_turso_statement_finalize, handle, "turso_statement_finalize")
	purego.RegisterLibFunc(&c_turso_statement_column_count, handle, "turso_statement_column_count")
	purego.RegisterLibFunc(&c_turso_statement_column_name, handle, "turso_statement_column_name")
	purego.RegisterLibFunc(&c_turso_statement_row_value_kind, handle, "turso_statement_row_value_kind")
	purego.RegisterLibFunc(&c_turso_statement_row_value_bytes_count, handle, "turso_statement_row_value_bytes_count")
	purego.RegisterLibFunc(&c_turso_statement_row_value_bytes_ptr, handle, "turso_statement_row_value_bytes_ptr")
	purego.RegisterLibFunc(&c_turso_statement_row_value_int, handle, "turso_statement_row_value_int")
	purego.RegisterLibFunc(&c_turso_statement_row_value_double, handle, "turso_statement_row_value_double")
	purego.RegisterLibFunc(&c_turso_statement_named_position, handle, "turso_statement_named_position")
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

// Helpers

func statusToError(code TursoStatusCode, msg string) error {
	switch code {
	case TURSO_OK, TURSO_DONE, TURSO_ROW, TURSO_IO:
		return nil
	case TURSO_BUSY:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoBusyErr
	case TURSO_INTERRUPT:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoInterruptErr
	case TURSO_ERROR:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoErrorErr
	case TURSO_MISUSE:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoMisuseErr
	case TURSO_CONSTRAINT:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoConstraintErr
	case TURSO_READONLY:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoReadonlyErr
	case TURSO_DATABASE_FULL:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoDatabaseFullErr
	case TURSO_NOTADB:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoNotADbErr
	case TURSO_CORRUPT:
		if msg != "" {
			return errors.New(msg)
		}
		return TursoCorruptErr
	default:
		if msg != "" {
			return errors.New(msg)
		}
		return fmt.Errorf("turso: unknown status code %d", code)
	}
}

func readErrorAndFree(errPtr uintptr) string {
	if errPtr == 0 {
		return ""
	}
	defer c_turso_str_deinit(unsafe.Pointer(errPtr))
	return copyCString(unsafe.Pointer(errPtr))
}

func copyCString(p unsafe.Pointer) string {
	if p == nil {
		return ""
	}
	// Determine length
	base := uintptr(p)
	n := 0
	for {
		b := *(*byte)(unsafe.Pointer(base + uintptr(n)))
		if b == 0 {
			break
		}
		n++
	}
	if n == 0 {
		return ""
	}
	buf := make([]byte, n)
	for i := 0; i < n; i++ {
		buf[i] = *(*byte)(unsafe.Pointer(base + uintptr(i)))
	}
	return string(buf)
}

func cStringPtr(s string) (ptr unsafe.Pointer, keepAlive func()) {
	// Allocate Go memory with null terminator; valid during the call
	if len(s) == 0 {
		return nil, func() {}
	}
	b := make([]byte, len(s)+1)
	copy(b, s)
	return unsafe.Pointer(&b[0]), func() { runtime.KeepAlive(b) }
}

// Go wrappers over imported C bindings

// turso_setup initializes global database info.
// If config.Logger is provided, it will be registered as a callback.
func turso_setup(config TursoConfig) error {
	var cCfg c_turso_config_t
	// Prepare logger callback if provided
	if config.Logger != nil {
		cb := purego.NewCallback(func(logPtr uintptr) uintptr {
			if logPtr == 0 {
				return 0
			}
			cl := (*c_turso_log_t)(unsafe.Pointer(logPtr))
			log := TursoLog{
				Message:   copyCString(unsafe.Pointer(cl.Message)),
				Target:    copyCString(unsafe.Pointer(cl.Target)),
				File:      copyCString(unsafe.Pointer(cl.File)),
				Timestamp: cl.Timestamp,
				Line:      uint(cl.Line),
				Level:     TursoTracingLevel(cl.Level),
			}
			config.Logger(log)
			return 0
		})
		cCfg.Logger = cb
	}

	// Prepare log level C string pointer
	var keep func()
	if config.LogLevel != "" {
		ptr, k := cStringPtr(config.LogLevel)
		cCfg.LogLevel = uintptr(ptr)
		keep = k
	} else {
		keep = func() {}
	}

	var cerr uintptr
	code := c_turso_setup(unsafe.Pointer(&cCfg), unsafe.Pointer(&cerr))
	keep()
	errMsg := readErrorAndFree(cerr)
	return statusToError(TursoStatusCode(code), errMsg)
}

/** Create database holder but do not open it */
func turso_database_new(config TursoDatabaseConfig) (TursoDatabase, error) {
	var cCfg c_turso_database_config_t
	// Path
	pathPtr, keepPath := cStringPtr(config.Path)
	cCfg.Path = uintptr(pathPtr)
	// Experimental
	var keepExp func()
	if config.ExperimentalFeatures != "" {
		expPtr, k := cStringPtr(config.ExperimentalFeatures)
		cCfg.ExperimentalFeatures = uintptr(expPtr)
		keepExp = k
	} else {
		keepExp = func() {}
	}
	if config.AsyncIO {
		cCfg.AsyncIO = 1
	}
	var db TursoDatabase
	var cerr uintptr
	code := c_turso_database_new(
		unsafe.Pointer(&cCfg),
		unsafe.Pointer(&db),
		unsafe.Pointer(&cerr),
	)
	keepPath()
	keepExp()
	errMsg := readErrorAndFree(cerr)
	if err := statusToError(TursoStatusCode(code), errMsg); err != nil {
		return nil, err
	}
	return db, nil
}

/** Open database */
func turso_database_open(database TursoDatabase) error {
	var cerr uintptr
	code := c_turso_database_open(unsafe.Pointer(database), unsafe.Pointer(&cerr))
	errMsg := readErrorAndFree(cerr)
	return statusToError(TursoStatusCode(code), errMsg)
}

/** Connect to the database */
func turso_database_connect(self TursoDatabase) (TursoConnection, error) {
	var conn TursoConnection
	var cerr uintptr
	code := c_turso_database_connect(unsafe.Pointer(self), unsafe.Pointer(&conn), unsafe.Pointer(&cerr))
	errMsg := readErrorAndFree(cerr)
	if err := statusToError(TursoStatusCode(code), errMsg); err != nil {
		return nil, err
	}
	return conn, nil
}

/** Get autocommit state of the connection */
func turso_connection_get_autocommit(self TursoConnection) bool {
	return c_turso_connection_get_autocommit(unsafe.Pointer(self))
}

/** Prepare single statement in a connection */
func turso_connection_prepare_single(self TursoConnection, sql string) (TursoStatement, error) {
	var stmt TursoStatement
	var cerr uintptr
	code := c_turso_connection_prepare_single(
		unsafe.Pointer(self),
		sql,
		unsafe.Pointer(&stmt),
		unsafe.Pointer(&cerr),
	)
	errMsg := readErrorAndFree(cerr)
	if err := statusToError(TursoStatusCode(code), errMsg); err != nil {
		return nil, err
	}
	return stmt, nil
}

/** Prepare first statement in a string containing multiple statements in a connection */
func turso_connection_prepare_first(self TursoConnection, sql string) (TursoStatement, int, error) {
	var stmt TursoStatement
	var tailIdx uintptr
	var cerr uintptr
	code := c_turso_connection_prepare_first(
		unsafe.Pointer(self),
		sql,
		unsafe.Pointer(&stmt),
		unsafe.Pointer(&tailIdx),
		unsafe.Pointer(&cerr),
	)
	errMsg := readErrorAndFree(cerr)
	if err := statusToError(TursoStatusCode(code), errMsg); err != nil {
		return nil, 0, err
	}
	return stmt, int(tailIdx), nil
}

/** Close the connection preventing any further operations executed over it */
func turso_connection_close(self TursoConnection) error {
	var cerr uintptr
	code := c_turso_connection_close(unsafe.Pointer(self), unsafe.Pointer(&cerr))
	errMsg := readErrorAndFree(cerr)
	return statusToError(TursoStatusCode(code), errMsg)
}

/** Execute single statement */
func turso_statement_execute(self TursoStatement) (uint64, error) {
	var cerr uintptr
	var rows uint64
	code := c_turso_statement_execute(unsafe.Pointer(self), unsafe.Pointer(&rows), unsafe.Pointer(&cerr))
	errMsg := readErrorAndFree(cerr)
	return rows, statusToError(TursoStatusCode(code), errMsg)
}

/** Step statement execution once
 * Returns TURSO_DONE if execution finished
 * Returns TURSO_ROW if execution generated the row (row values can be inspected with corresponding statement methods)
 * Returns TURSO_IO if async_io was set and statement needs to execute IO to make progress
 */
func turso_statement_step(self TursoStatement) (TursoStatusCode, error) {
	var cerr uintptr
	code := c_turso_statement_step(unsafe.Pointer(self), unsafe.Pointer(&cerr))
	errMsg := readErrorAndFree(cerr)
	if err := statusToError(TursoStatusCode(code), errMsg); err != nil {
		return TursoStatusCode(code), err
	}
	return TursoStatusCode(code), nil
}

/** Execute one iteration of underlying IO backend */
func turso_statement_run_io(self TursoStatement) error {
	var cerr uintptr
	code := c_turso_statement_run_io(unsafe.Pointer(self), unsafe.Pointer(&cerr))
	errMsg := readErrorAndFree(cerr)
	return statusToError(TursoStatusCode(code), errMsg)
}

/** Reset a statement */
func turso_statement_reset(self TursoStatement) error {
	var cerr uintptr
	code := c_turso_statement_reset(unsafe.Pointer(self), unsafe.Pointer(&cerr))
	errMsg := readErrorAndFree(cerr)
	return statusToError(TursoStatusCode(code), errMsg)
}

/** Finalize a statement
 * This method must be called in the end of statement execution (either successfull or not)
 */
func turso_statement_finalize(self TursoStatement) error {
	var cerr uintptr
	code := c_turso_statement_finalize(unsafe.Pointer(self), unsafe.Pointer(&cerr))
	errMsg := readErrorAndFree(cerr)
	return statusToError(TursoStatusCode(code), errMsg)
}

/** Get column count */
func turso_statement_column_count(self TursoStatement) int64 {
	return c_turso_statement_column_count(unsafe.Pointer(self))
}

/** Get the column name at the index
 * C string allocated by Turso must be freed after the usage with corresponding turso_str_deinit(...) method
 */
func turso_statement_column_name(self TursoStatement, index int) string {
	ptr := c_turso_statement_column_name(unsafe.Pointer(self), uintptr(index))
	if ptr == nil {
		return ""
	}
	defer c_turso_str_deinit(ptr)
	return copyCString(ptr)
}

/** Get the row value kind at the index for a current statement state */
func turso_statement_row_value_kind(self TursoStatement, index int) TursoType {
	return TursoType(c_turso_statement_row_value_kind(unsafe.Pointer(self), uintptr(index)))
}

/** Get amount of bytes in the BLOB or TEXT values
 * Return -1 for other kinds
 */
func turso_statement_row_value_bytes_count(self TursoStatement, index int) int64 {
	return c_turso_statement_row_value_bytes_count(unsafe.Pointer(self), uintptr(index))
}

/** Return pointer to the start of the slice  for BLOB or TEXT values
 * Return NULL for other kinds
 */
func turso_statement_row_value_bytes_ptr(self TursoStatement, index int) unsafe.Pointer {
	return c_turso_statement_row_value_bytes_ptr(unsafe.Pointer(self), uintptr(index))
}

/** Return value of INTEGER kind
 * Return 0 for other kinds
 */
func turso_statement_row_value_int(self TursoStatement, index int) int64 {
	return c_turso_statement_row_value_int(unsafe.Pointer(self), uintptr(index))
}

/** Return value of REAL kind
 * Return 0 for other kinds
 */
func turso_statement_row_value_double(self TursoStatement, index int) float64 {
	return c_turso_statement_row_value_double(unsafe.Pointer(self), uintptr(index))
}

/** Return named argument position in a statement */
func turso_statement_named_position(self TursoStatement, name string) int64 {
	return c_turso_statement_named_position(unsafe.Pointer(self), name)
}

/** Bind a positional argument to a statement: NULL */
func turso_statement_bind_positional_null(self TursoStatement, position int) error {
	code := c_turso_statement_bind_positional_null(unsafe.Pointer(self), uintptr(position))
	return statusToError(TursoStatusCode(code), "")
}

/** Bind a positional argument to a statement: INTEGER */
func turso_statement_bind_positional_int(self TursoStatement, position int, value int64) error {
	code := c_turso_statement_bind_positional_int(unsafe.Pointer(self), uintptr(position), value)
	return statusToError(TursoStatusCode(code), "")
}

/** Bind a positional argument to a statement: DOUBLE */
func turso_statement_bind_positional_double(self TursoStatement, position int, value float64) error {
	code := c_turso_statement_bind_positional_double(unsafe.Pointer(self), uintptr(position), value)
	return statusToError(TursoStatusCode(code), "")
}

/** Bind a positional argument to a statement: BLOB */
func turso_statement_bind_positional_blob(self TursoStatement, position int, value []byte) error {
	var ptr unsafe.Pointer
	var ln uintptr
	if len(value) > 0 {
		ptr = unsafe.Pointer(&value[0])
		ln = uintptr(len(value))
	} else {
		ptr = nil
		ln = 0
	}
	code := c_turso_statement_bind_positional_blob(unsafe.Pointer(self), uintptr(position), ptr, ln)
	return statusToError(TursoStatusCode(code), "")
}

/** Bind a positional argument to a statement: TEXT */
func turso_statement_bind_positional_text(self TursoStatement, position int, value string) error {
	code := c_turso_statement_bind_positional_text(unsafe.Pointer(self), uintptr(position), value, uintptr(len(value)))
	return statusToError(TursoStatusCode(code), "")
}

/** Deallocate C string allocated by Turso */
func turso_str_deinit(self unsafe.Pointer) {
	if self == nil {
		return
	}
	c_turso_str_deinit(self)
}

/** Deallocate and close a database
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited database
 */
func turso_database_deinit(self TursoDatabase) {
	if self == nil {
		return
	}
	c_turso_database_deinit(unsafe.Pointer(self))
}

/** Deallocate and close a connection
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited connection
 */
func turso_connection_deinit(self TursoConnection) {
	if self == nil {
		return
	}
	c_turso_connection_deinit(unsafe.Pointer(self))
}

/** Deallocate and close a statement
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited statement
 */
func turso_statement_deinit(self TursoStatement) {
	if self == nil {
		return
	}
	c_turso_statement_deinit(unsafe.Pointer(self))
}

// Additional ergonomic helpers (the only non-direct translations)

/** Return BLOB value as a Go byte slice (copied).
 * If the value at index is not BLOB or TEXT, returns nil.
 * The underlying C memory is copied into a new Go slice.
 */
func turso_statement_row_value_bytes(self TursoStatement, index int) []byte {
	n := c_turso_statement_row_value_bytes_count(unsafe.Pointer(self), uintptr(index))
	if n <= 0 {
		return nil
	}
	ptr := c_turso_statement_row_value_bytes_ptr(unsafe.Pointer(self), uintptr(index))
	if ptr == nil {
		return nil
	}
	out := make([]byte, n)
	base := uintptr(ptr)
	for i := int64(0); i < n; i++ {
		out[i] = *(*byte)(unsafe.Pointer(base + uintptr(i)))
	}
	return out
}

/** Return TEXT value as a Go string (copied).
 * If the value at index is not TEXT or BLOB, returns empty string.
 * The underlying C memory is copied into a new Go string.
 */
func turso_statement_row_value_text(self TursoStatement, index int) string {
	n := c_turso_statement_row_value_bytes_count(unsafe.Pointer(self), uintptr(index))
	if n <= 0 {
		return ""
	}
	ptr := c_turso_statement_row_value_bytes_ptr(unsafe.Pointer(self), uintptr(index))
	if ptr == nil {
		return ""
	}
	// Copy bytes and convert to string
	buf := make([]byte, n)
	base := uintptr(ptr)
	for i := int64(0); i < n; i++ {
		buf[i] = *(*byte)(unsafe.Pointer(base + uintptr(i)))
	}
	return string(buf)
}
