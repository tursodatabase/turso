#ifndef TURSO_H
#define TURSO_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/// SAFETY: slice with non-null ptr must points to the valid memory range [ptr..ptr + len)
/// ownership of the slice is not transferred - so its either caller owns the data or turso
/// as the owner doesn't change - there is no method to free the slice reference - because:
/// 1. if tursodb owns it - it will clean it in appropriate time
/// 2. if caller owns it - it must clean it in appropriate time with appropriate method and tursodb doesn't know how to properly free the data
typedef struct
{
    const void *ptr;
    size_t len;
} turso_slice_ref_t;

typedef enum
{
    TURSO_OK = 0,
    TURSO_DONE = 1,
    TURSO_ROW = 2,
    TURSO_IO = 3,
    TURSO_BUSY = 4,
    TURSO_INTERRUPT = 5,
    TURSO_BUSY_SNAPSHOT = 6,
    TURSO_ERROR = 127,
    TURSO_MISUSE = 128,
    TURSO_CONSTRAINT = 129,
    TURSO_READONLY = 130,
    TURSO_DATABASE_FULL = 131,
    TURSO_NOTADB = 132,
    TURSO_CORRUPT = 133,
    TURSO_IOERR = 134,
} turso_status_code_t;

// enumeration of value types supported by the database
typedef enum
{
    TURSO_TYPE_UNKNOWN = 0,
    TURSO_TYPE_INTEGER = 1,
    TURSO_TYPE_REAL = 2,
    TURSO_TYPE_TEXT = 3,
    TURSO_TYPE_BLOB = 4,
    TURSO_TYPE_NULL = 5,
} turso_type_t;


typedef enum
{
    TURSO_EXTENSION_VALUE_NULL = 0,
    TURSO_EXTENSION_VALUE_INTEGER = 1,
    TURSO_EXTENSION_VALUE_FLOAT = 2,
    TURSO_EXTENSION_VALUE_TEXT = 3,
    TURSO_EXTENSION_VALUE_BLOB = 4,
    TURSO_EXTENSION_VALUE_ERROR = 5,
} turso_extension_value_type_t;

typedef enum
{
    TURSO_EXTENSION_RESULT_OK = 0,
    TURSO_EXTENSION_RESULT_ERROR = 1,
    TURSO_EXTENSION_RESULT_INVALID_ARGS = 2,
    TURSO_EXTENSION_RESULT_UNKNOWN = 3,
    TURSO_EXTENSION_RESULT_OOM = 4,
    TURSO_EXTENSION_RESULT_CORRUPT = 5,
    TURSO_EXTENSION_RESULT_NOT_FOUND = 6,
    TURSO_EXTENSION_RESULT_ALREADY_EXISTS = 7,
    TURSO_EXTENSION_RESULT_PERMISSION_DENIED = 8,
    TURSO_EXTENSION_RESULT_ABORTED = 9,
    TURSO_EXTENSION_RESULT_OUT_OF_RANGE = 10,
    TURSO_EXTENSION_RESULT_UNIMPLEMENTED = 11,
    TURSO_EXTENSION_RESULT_INTERNAL = 12,
    TURSO_EXTENSION_RESULT_UNAVAILABLE = 13,
    TURSO_EXTENSION_RESULT_CUSTOM_ERROR = 14,
    TURSO_EXTENSION_RESULT_EOF = 15,
    TURSO_EXTENSION_RESULT_READ_ONLY = 16,
    TURSO_EXTENSION_RESULT_ROWID = 17,
    TURSO_EXTENSION_RESULT_ROW = 18,
    TURSO_EXTENSION_RESULT_INTERRUPT = 19,
    TURSO_EXTENSION_RESULT_BUSY = 20,
    TURSO_EXTENSION_RESULT_CONSTRAINT_VIOLATION = 21,
} turso_extension_result_code_t;

typedef enum
{
    TURSO_EXTENSION_TEXT_TEXT = 0,
    TURSO_EXTENSION_TEXT_JSON = 1,
} turso_extension_text_subtype_t;

typedef struct
{
    turso_extension_text_subtype_t subtype;
    const uint8_t *text;
    uint32_t len;
} turso_extension_text_t;

typedef struct
{
    const uint8_t *data;
    uint64_t size;
} turso_extension_blob_t;

typedef struct
{
    turso_extension_result_code_t code;
    turso_extension_text_t *message;
} turso_extension_error_t;

typedef union
{
    int64_t int_value;
    double float_value;
    const turso_extension_text_t *text;
    const turso_extension_blob_t *blob;
    const turso_extension_error_t *error;
} turso_extension_value_data_t;

typedef struct
{
    turso_extension_value_type_t value_type;
    turso_extension_value_data_t value;
} turso_value_t;

typedef struct
{
    void *state;
} turso_agg_ctx_t;

/** Frees per-registration state supplied to scalar, aggregate, or collation callbacks. */
typedef void (*turso_context_destructor_t)(uintptr_t context);

/** Frees a value returned by a managed scalar or aggregate callback after Turso copies it. */
typedef void (*turso_value_destructor_t)(turso_value_t *result);

/** Scalar callback. argv points to argc immutable turso_value_t entries valid only for the call. */
typedef turso_value_t (*turso_scalar_function_t)(uintptr_t context, int32_t argc, const turso_value_t *argv, turso_context_destructor_t context_destructor, turso_value_destructor_t value_destructor);

/** Aggregate initializer. Return value is passed unchanged to step/final/destructor callbacks. */
typedef turso_agg_ctx_t *(*turso_aggregate_init_function_t)(uintptr_t context);

/** Aggregate step callback. argv points to argc immutable turso_value_t entries valid only for the call. */
typedef turso_value_t (*turso_aggregate_step_function_t)(uintptr_t context, turso_agg_ctx_t *aggregate_context, int32_t argc, const turso_value_t *argv);

/** Aggregate final callback. The aggregate_context is the value returned by the initializer. */
typedef turso_value_t (*turso_aggregate_final_function_t)(uintptr_t context, turso_agg_ctx_t *aggregate_context);

/** Collation callback. Byte ranges are UTF-8 text valid only for the call. Return follows strcmp ordering. */
typedef int32_t (*turso_collation_function_t)(uintptr_t context, const uint8_t *left_ptr, size_t left_len, const uint8_t *right_ptr, size_t right_len);

typedef enum
{
    TURSO_TRACING_LEVEL_ERROR = 1,
    TURSO_TRACING_LEVEL_WARN,
    TURSO_TRACING_LEVEL_INFO,
    TURSO_TRACING_LEVEL_DEBUG,
    TURSO_TRACING_LEVEL_TRACE,
} turso_tracing_level_t;

/// opaque pointer to the TursoDatabase instance
/// SAFETY: the database must be opened and closed only once but can be used concurrently
typedef struct turso_database turso_database_t;

/// opaque pointer to the TursoConnection instance
/// SAFETY: the connection must be used exclusive and can't be accessed concurrently
typedef struct turso_connection turso_connection_t;

/// opaque pointer to the TursoStatement instance
/// SAFETY: the statement must be used exclusive and can't be accessed concurrently
typedef struct turso_statement turso_statement_t;

// return STATIC zero-terminated C-string with turso version (sem-ver string e.g. x.y.z-...)
// (this string DO NOT need to be deallocated as it static)
const char *turso_version();

typedef struct
{
    /* zero-terminated C string */
    const char *message;
    /* zero-terminated C string */
    const char *target;
    /* zero-terminated C string */
    const char *file;
    uint64_t timestamp;
    size_t line;
    turso_tracing_level_t level;
} turso_log_t;

typedef struct
{
    /// SAFETY: turso_log_t log argument fields have lifetime scoped to the logger invocation
    /// caller must ensure that data is properly copied if it wants it to have longer lifetime
    void (*logger)(const turso_log_t *log);
    /* zero-terminated C string */
    const char *log_level;
} turso_config_t;

/**
 * Database description.
 */
typedef struct
{
    /** Parameter which defines who drives the IO - callee or the caller (non-zero parameter value interpreted as async IO) */
    uint64_t async_io;
    /** Path to the database file or `:memory:`
     * zero-terminated C string
     */
    const char *path;
    /** Optional comma separated list of experimental features to enable
     * zero-terminated C string or null pointer
     */
    const char *experimental_features;
    /** optional VFS parameter explicitly specifying FS backend for the database.
     * Available options are:
     * - "memory": in-memory backend
     * - "syscall": generic syscall backend
     * - "io_uring": IO uring (supported only on Linux)
	 * - "experimental_win_iocp": Windows IOCP [experimental](supported only on Windows)
     */
    const char *vfs;
    /** optional encryption cipher
     * as encryption is experimental - experimental_features must have "encryption" in the list
     */
    const char *encryption_cipher;
    /** optional encryption hexkey
     * as encryption is experimental - experimental_features must have "encryption" in the list
     */
    const char *encryption_hexkey;
} turso_database_config_t;

/** Setup global database info */
turso_status_code_t turso_setup(
    const turso_config_t *config,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Create database holder but do not open it */
turso_status_code_t turso_database_new(
    const turso_database_config_t *config,
    /** reference to pointer which will be set to database instance in case of TURSO_OK result */
    const turso_database_t **database,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Open database
 *  Can return TURSO_IO result if async_io=true is set
 */
turso_status_code_t turso_database_open(
    const turso_database_t *database,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Connect to the database */
turso_status_code_t turso_database_connect(
    const turso_database_t *self,
    /** reference to pointer which will be set to connection instance in case of TURSO_OK result */
    turso_connection_t **connection,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Set busy timeout for the connection */
void turso_connection_set_busy_timeout_ms(const turso_connection_t *self, int64_t timeout_ms);

/** Get autocommit state of the connection */
bool turso_connection_get_autocommit(const turso_connection_t *self);

/** Get last insert rowid for the connection or 0 if no inserts happened before */
int64_t turso_connection_last_insert_rowid(const turso_connection_t *self);


/** Register or replace a per-connection managed scalar function. */
turso_status_code_t turso_connection_register_scalar_function(
    const turso_connection_t *self,
    const char *name,
    int32_t argc,
    bool deterministic,
    uintptr_t context,
    turso_scalar_function_t callback,
    turso_context_destructor_t context_destructor,
    turso_value_destructor_t value_destructor,
    const char **error_opt_out);

/** Register or replace a per-connection managed aggregate function. */
turso_status_code_t turso_connection_register_aggregate_function(
    const turso_connection_t *self,
    const char *name,
    int32_t argc,
    uintptr_t context,
    turso_aggregate_init_function_t init,
    turso_aggregate_step_function_t step,
    turso_aggregate_final_function_t finalize,
    turso_context_destructor_t context_destructor,
    turso_context_destructor_t aggregate_destructor,
    turso_value_destructor_t value_destructor,
    const char **error_opt_out);

/** Unregister a per-connection managed scalar or aggregate function. */
turso_status_code_t turso_connection_unregister_function(
    const turso_connection_t *self,
    const char *name,
    const char **error_opt_out);

/** Register or replace a per-connection managed collation. */
turso_status_code_t turso_connection_register_collation(
    const turso_connection_t *self,
    const char *name,
    uintptr_t context,
    turso_collation_function_t callback,
    turso_context_destructor_t context_destructor,
    const char **error_opt_out);

/** Unregister a per-connection managed collation. */
turso_status_code_t turso_connection_unregister_collation(
    const turso_connection_t *self,
    const char *name,
    const char **error_opt_out);

/** Enable or disable SQL load_extension() for this connection. */
turso_status_code_t turso_connection_enable_load_extension(
    const turso_connection_t *self,
    bool enabled,
    const char **error_opt_out);

/** Load an extension library on this connection using Turso's native extension loader. */
turso_status_code_t turso_connection_load_extension(
    const turso_connection_t *self,
    const char *path,
    const char **error_opt_out);

/** Prepare single statement in a connection */
turso_status_code_t
turso_connection_prepare_single(
    const turso_connection_t *self,
    /* zero-terminated C string */
    const char *sql,
    /** reference to pointer which will be set to statement instance in case of TURSO_OK result */
    turso_statement_t **statement,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Prepare first statement in a string containing multiple statements in a connection */
turso_status_code_t
turso_connection_prepare_first(
    const turso_connection_t *self,
    /* zero-terminated C string */
    const char *sql,
    /** reference to pointer which will be set to statement instance in case of TURSO_OK result; can be null if no statements can be parsed from the input string */
    turso_statement_t **statement,
    /** offset in the sql string right after the parsed statement */
    size_t *tail_idx,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** close the connection preventing any further operations executed over it
 * caller still need to call deinit method to reclaim memory from the instance holding connection
 * SAFETY: caller must guarantee that no ongoing operations are running over connection before calling turso_connection_close(...) method
 */
turso_status_code_t turso_connection_close(
    const turso_connection_t *self,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Execute single statement
 * execute returns TURSO_DONE if execution completed
 * execute returns TURSO_IO if async_io was set and execution needs IO in order to make progress
 */
turso_status_code_t turso_statement_execute(
    const turso_statement_t *self,
    uint64_t *rows_changes,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Step statement execution once
 * Returns TURSO_DONE if execution finished
 * Returns TURSO_ROW if execution generated the row (row values can be inspected with corresponding statement methods)
 * Returns TURSO_IO if async_io was set and statement needs to execute IO to make progress
 */
turso_status_code_t turso_statement_step(const turso_statement_t *self, const char **error_opt_out);

/** Execute one iteration of underlying IO backend after TURSO_IO status code
 * This function either return some ERROR status or TURSO_OK
 */
turso_status_code_t turso_statement_run_io(const turso_statement_t *self, const char **error_opt_out);

/** Reset a statement
 * This method must be called in order to cleanup statement resources and prepare it for re-execution
 * Any pending execution will be aborted - be careful and in certain cases ensure that turso_statement_finalize called before turso_statement_reset
 */
turso_status_code_t turso_statement_reset(const turso_statement_t *self, const char **error_opt_out);

/** Finalize a statement
 * finalize returns TURSO_DONE if finalization completed
 * This method must be called in the end of statement execution (either successfull or not)
 */
turso_status_code_t turso_statement_finalize(const turso_statement_t *self, const char **error_opt_out);

/** return amount of row modifications (insert/delete operations) made by the most recent executed statement */
int64_t turso_statement_n_change(const turso_statement_t *self);

/** Get column count */
int64_t turso_statement_column_count(const turso_statement_t *self);

/** Get the column name at the index
 * C string allocated by Turso must be freed after the usage with corresponding turso_str_deinit(...) method
 */
const char *turso_statement_column_name(const turso_statement_t *self, size_t index);

/** Get the column declared type at the index (e.g. "INTEGER", "TEXT", "DATETIME", etc.)
 * Returns NULL if the column type is not available.
 * C string allocated by Turso must be freed after the usage with corresponding turso_str_deinit(...) method
 */
const char *turso_statement_column_decltype(const turso_statement_t *self, size_t index);

/** Classification of a result column's declared type.
 *
 * Returned by turso_statement_column_kind. Values match the
 * ColumnTypeKind variants in the Rust API and are kept stable: new kinds
 * are appended.
 */
typedef enum
{
    TURSO_COLUMN_KIND_NONE = -1,
    TURSO_COLUMN_KIND_BUILTIN = 0,
    TURSO_COLUMN_KIND_CUSTOM = 1,
    TURSO_COLUMN_KIND_DOMAIN = 2,
    TURSO_COLUMN_KIND_STRUCT = 3,
    TURSO_COLUMN_KIND_UNION = 4,
} turso_column_kind_t;

/** Get the declared type name of the column at `index`.
 *
 * Returns the same string as turso_statement_column_decltype, but resolved
 * through the richer type-info path so the result is consistent with the
 * other turso_statement_column_* getters declared below.
 *
 * Returns NULL when no type info is available (statement finalized, index
 * out of bounds, or the result column is not a direct table-column ref).
 * C string allocated by Turso must be freed with turso_str_deinit.
 */
const char *turso_statement_column_declared_name(const turso_statement_t *self, size_t index);

/** Get the array depth of the column at `index`.
 *
 * Returns 0 for scalar table columns, n for n-dimensional array columns
 * (e.g. INTEGER[][] -> 2), and 0 when no type info is available. To
 * distinguish "scalar column" from "no info", check
 * turso_statement_column_kind first: it returns TURSO_COLUMN_KIND_NONE in
 * the latter case.
 */
uint32_t turso_statement_column_array_dimensions(const turso_statement_t *self, size_t index);

/** Get the underlying primitive type name for columns whose declared type
 * is a CREATE TYPE or CREATE DOMAIN.
 *
 * Returns one of "INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC". Returns NULL
 * when the declared type is a built-in primitive directly, when no type
 * info is available, or when the column is not a direct table-column
 * reference. C string allocated by Turso must be freed with
 * turso_str_deinit.
 */
const char *turso_statement_column_base_type(const turso_statement_t *self, size_t index);

/** Classify the column's declared type as builtin / custom / domain /
 * struct / union (see turso_column_kind_t).
 *
 * Returns TURSO_COLUMN_KIND_NONE when no type information is available.
 */
turso_column_kind_t turso_statement_column_kind(const turso_statement_t *self, size_t index);

/** Get the row value at the the index for a current statement state
 * SAFETY: returned pointers will be valid only until next invocation of statement operation (step, finalize, reset, etc)
 * Caller must make sure that any non-owning memory is copied appropriated if it will be used for longer lifetime
 */
turso_type_t turso_statement_row_value_kind(const turso_statement_t *self, size_t index);
/* Get amount of bytes in the BLOB or TEXT values
 * Return -1 for other kinds
 */
int64_t turso_statement_row_value_bytes_count(const turso_statement_t *self, size_t index);
/* Get pointer to the start of the slice  for BLOB or TEXT values
 * Return NULL for other kinds
 */
const char *turso_statement_row_value_bytes_ptr(const turso_statement_t *self, size_t index);
/* Return value of INTEGER kind
 * Return 0 for other kinds
 */
int64_t turso_statement_row_value_int(const turso_statement_t *self, size_t index);
/* Return value of REAL kind
 * Return 0 for other kinds
 */
double turso_statement_row_value_double(const turso_statement_t *self, size_t index);

/** Return named argument position in a statement
    Return positive integer with 1-indexed position if named parameter was found
    Return -1 if parameter was not found
*/
int64_t turso_statement_named_position(
    const turso_statement_t *self,
    /* zero-terminated C string */
    const char *name);

/** Return parameters count for the statement
 * -1 if pointer is invalid
 */
int64_t
turso_statement_parameters_count(const turso_statement_t *self);

/** Return the name of the parameter at 1-based index, including the SQL prefix.
Returns NULL for positional-only parameters or out-of-range indices.
The caller must free the returned string with turso_str_deinit.
*/
const char *
turso_statement_parameter_name(const turso_statement_t *self, int64_t index);

/** Bind a positional argument to a statement */
turso_status_code_t
turso_statement_bind_positional_null(const turso_statement_t *self, size_t position);
turso_status_code_t
turso_statement_bind_positional_int(const turso_statement_t *self, size_t position, int64_t value);
turso_status_code_t
turso_statement_bind_positional_double(const turso_statement_t *self, size_t position, double value);
turso_status_code_t
turso_statement_bind_positional_blob(
    const turso_statement_t *self,
    size_t position,
    /* pointer to the start of BLOB slice */
    const char *ptr,
    /* length of BLOB slice */
    size_t len);
turso_status_code_t
turso_statement_bind_positional_text(
    const turso_statement_t *self,
    size_t position,
    /* pointer to the start of TEXT slice */
    const char *ptr,
    /* length of TEXT slice */
    size_t len);

/** Deallocate C string allocated by Turso */
void turso_str_deinit(const char *self);
/** Deallocate and close a database
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited database
 */
void turso_database_deinit(const turso_database_t *self);
/** Deallocate and close a connection
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited connection
 */
void turso_connection_deinit(const turso_connection_t *self);
/** Deallocate and close a statement
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited statement
 */
void turso_statement_deinit(const turso_statement_t *self);

#endif /* TURSO_H */
