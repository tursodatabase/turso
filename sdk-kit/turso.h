#ifndef TURSO_H
#define TURSO_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum
{
    TURSO_OK = 0,
    TURSO_DONE = 1,
    TURSO_ROW = 2,
    TURSO_IO = 3,
    TURSO_BUSY = 4,
    TURSO_INTERRUPT = 5,
    TURSO_ERROR = 127,
    TURSO_MISUSE = 128,
    TURSO_CONSTRAINT = 129,
    TURSO_READONLY = 130,
    TURSO_DATABASE_FULL = 131,
    TURSO_NOTADB = 132,
    TURSO_CORRUPT = 133,
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

/** Setup global database info */
turso_status_code_t turso_setup(
    /// SAFETY: turso_log_t log argument fields have lifetime scoped to the logger invocation
    /// caller must ensure that data is properly copied if it wants it to have longer lifetime
    void (*logger)(
        const char *message,
        const char *target,
        const char *file,
        uint64_t timestamp,
        size_t line,
        turso_tracing_level_t level),
    const char *log_level,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Create database holder but do not open it */
turso_status_code_t turso_database_new(
    /** Path to the database file or `:memory:` */
    const char *path,
    /** Optional comma separated list of experimental features to enable */
    const char *experimental_features,
    /** Parameter which defines who drives the IO - callee or the caller */
    bool async_io,
    /** reference to pointer which will be set to database instance in case of TURSO_OK result */
    turso_database_t **database,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Open database */
turso_status_code_t turso_database_open(
    turso_database_t *database,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Connect to the database */
turso_status_code_t turso_database_connect(
    turso_database_t *self,
    /** reference to pointer which will be set to connection instance in case of TURSO_OK result */
    turso_connection_t **connection,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Get autocommit state of the connection */
bool turso_connection_get_autocommit(turso_connection_t *self);

/** Prepare single statement in a connection */
turso_status_code_t
turso_connection_prepare_single(
    turso_connection_t *self,
    const char *sql,
    /** reference to pointer which will be set to statement instance in case of TURSO_OK result */
    turso_statement_t **statement,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Prepare first statement in a string containing multiple statements in a connection */
turso_status_code_t
turso_connection_prepare_first(
    turso_connection_t *self,
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
    turso_connection_t *self,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Execute single statement */
turso_status_code_t turso_statement_execute(
    turso_statement_t *self,
    uint64_t *rows_changes,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Step statement execution once
 * Returns TURSO_DONE if execution finished
 * Returns TURSO_ROW if execution generated the row (row values can be inspected with corresponding statement methods)
 * Returns TURSO_IO if async_io was set and statement needs to execute IO to make progress
 */
turso_status_code_t turso_statement_step(turso_statement_t *self, const char **error_opt_out);

/** Execute one iteration of underlying IO backend */
turso_status_code_t turso_statement_run_io(turso_statement_t *self, const char **error_opt_out);

/** Reset a statement */
turso_status_code_t turso_statement_reset(turso_statement_t *self, const char **error_opt_out);

/** Finalize a statement
 * This method must be called in the end of statement execution (either successfull or not)
 */
turso_status_code_t turso_statement_finalize(turso_statement_t *self, const char **error_opt_out);

/** Get column count */
int64_t turso_statement_column_count(turso_statement_t *self);

/** Get the column name at the index
 * C string allocated by Turso must be freed after the usage with corresponding turso_str_deinit(...) method
 */
const char *turso_statement_column_name(turso_statement_t *self, size_t index);

/** Get the row value at the the index for a current statement state
 * SAFETY: returned pointers will be valid only until next invocation of statement operation (step, finalize, reset, etc)
 * Caller must make sure that any non-owning memory is copied appropriated if it will be used for longer lifetime
 */
turso_type_t turso_statement_row_value_kind(turso_statement_t *self, size_t index);
int64_t turso_statement_row_value_bytes_count(turso_statement_t *self, size_t index);
const char *turso_statement_row_value_bytes_ptr(turso_statement_t *self, size_t index);
int64_t turso_statement_row_value_int(turso_statement_t *self, size_t index);
double turso_statement_row_value_double(turso_statement_t *self, size_t index);

/** Return named argument position in a statement */
int64_t turso_statement_named_position(
    turso_statement_t *self,
    const char *name);

/** Bind a positional argument to a statement */
turso_status_code_t
turso_statement_bind_positional_null(turso_statement_t *self, size_t position);
turso_status_code_t
turso_statement_bind_positional_int(turso_statement_t *self, size_t position, int64_t value);
turso_status_code_t
turso_statement_bind_positional_double(turso_statement_t *self, size_t position, double value);
turso_status_code_t
turso_statement_bind_positional_blob(turso_statement_t *self, size_t position, const uint8_t *ptr, size_t len);
turso_status_code_t
turso_statement_bind_positional_text(turso_statement_t *self, size_t position, const char *ptr, size_t len);

/** Deallocate C string allocated by Turso */
void turso_str_deinit(const char *self);
/** Deallocate and close a database
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited database
 */
void turso_database_deinit(turso_database_t *self);
/** Deallocate and close a connection
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited connection
 */
void turso_connection_deinit(turso_connection_t *self);
/** Deallocate and close a statement
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited statement
 */
void turso_statement_deinit(turso_statement_t *self);

#endif /* TURSO_H */