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

// status of the operation
// in most cases - this status acts as a signal for the error
// in case of execution, status can return non-error code like ROW/DONE/IO which must be handled by the caller accordingly
typedef struct turso_status_t
{
    turso_status_code_t code;
    const char *error;
} turso_status_t;

// enumeration of value types supported by the database
typedef enum
{
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

typedef struct
{
    const char *message;
    const char *target;
    const char *file;
    uint64_t timestamp;
    size_t line;
    turso_tracing_level_t level;
} turso_log_t;

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

// owned slice - must be freed by the caller with corresponding method
typedef struct
{
    const void *ptr;
    size_t len;
} turso_slice_owned_t;

/// structure holding opaque pointer to the TursoDatabase instance
/// SAFETY: the database must be opened and closed only once but can be used concurrently
typedef struct
{
    void *inner;
} turso_database_t;

/// structure holding opaque pointer to the TursoConnection instance
/// SAFETY: the connection must be used exclusive and can't be accessed concurrently
typedef struct
{
    void *inner;
} turso_connection_t;

/// structure holding opaque pointer to the TursoStatement instance
/// SAFETY: the statement must be used exclusive and can't be accessed concurrently
typedef struct
{
    void *inner;
} turso_statement_t;

// typeless union holding one possible value from the database row
typedef union
{
    int64_t integer;
    double real;
    turso_slice_ref_t text;
    turso_slice_ref_t blob;
} turso_value_union_t;

// type-tagged union holding one possible value from the database row
typedef struct
{
    turso_value_union_t value;
    turso_type_t type;
} turso_value_t;

/**
 * Database description.
 */
typedef struct
{
    /** Path to the database file or `:memory:` */
    const char *path;
    /** Optional comma separated list of experimental features to enable */
    const char *experimental_features;
    /** Parameter which defines who drives the IO - callee or the caller */
    bool async_io;
} turso_database_config_t;

typedef struct
{
    /// SAFETY: turso_log_t log argument fields have lifetime scoped to the logger invocation
    /// caller must ensure that data is properly copied if it wants it to have longer lifetime
    void (*logger)(turso_log_t log);
    const char *log_level;
} turso_config_t;

// return STATIC zero-terminated C-string with turso version (sem-ver string e.g. x.y.z-...)
// (this string DO NOT need to be deallocated as it static)
const char *turso_version();

/** Setup global database info */
turso_status_t turso_setup(turso_config_t config);

typedef struct
{
    turso_status_t status;
    turso_database_t database;
} turso_database_create_result_t;

/** Create database holder but do not open it */
turso_database_create_result_t turso_database_new(turso_database_config_t config);

/** Open database */
turso_status_t turso_database_open(turso_database_t database);

typedef struct
{
    turso_status_t status;
    turso_connection_t connection;
} turso_database_connect_result_t;

/** Connect to the database */
turso_database_connect_result_t turso_database_connect(turso_database_t self);

typedef struct
{
    turso_status_t status;
    bool auto_commit;
} turso_connection_get_autocommit_result_t;

/** Get autocommit state of the connection */
turso_connection_get_autocommit_result_t
turso_connection_get_autocommit(turso_connection_t self);

typedef struct
{
    turso_status_t status;
    turso_statement_t statement;
} turso_connection_prepare_single_t;

/** Prepare single statement in a connection */
turso_connection_prepare_single_t
turso_connection_prepare_single(turso_connection_t self, turso_slice_ref_t sql);

typedef struct
{
    turso_status_t status;
    turso_statement_t statement;
    size_t tail_idx;
} turso_connection_prepare_first_t;

/** Prepare first statement in a string containing multiple statements in a connection */
turso_connection_prepare_first_t
turso_connection_prepare_first(turso_connection_t self, turso_slice_ref_t sql);

/** close the connection preventing any further operations executed over it
 * caller still need to call deinit method to reclaim memory from the instance holding connection
 * SAFETY: caller must guarantee that no ongoing operations are running over connection before calling turso_connection_close(...) method
 */
turso_status_t turso_connection_close(turso_connection_t self);

/** Check if no more statements was parsed after execution of turso_connection_prepare_first method */
bool turso_connection_prepare_first_result_empty(turso_connection_prepare_first_t result);

// result of the statement execution
typedef struct
{
    turso_status_t status;
    uint64_t rows_changed;
} turso_statement_execute_t;

/** Execute single statement */
turso_statement_execute_t turso_statement_execute(turso_statement_t self);

/** Step statement execution once
 * Returns TURSO_DONE if execution finished
 * Returns TURSO_ROW if execution generated the row (row values can be inspected with corresponding statement methods)
 * Returns TURSO_IO if async_io was set and statement needs to execute IO to make progress
 */
turso_status_t turso_statement_step(turso_statement_t self);

/** Execute one iteration of underlying IO backend */
turso_status_t turso_statement_run_io(turso_statement_t self);

/** Reset a statement */
turso_status_t turso_statement_reset(turso_statement_t self);

/** Finalize a statement
 * This method must be called in the end of statement execution (either successfull or not)
 */
turso_status_t turso_statement_finalize(turso_statement_t self);

typedef struct
{
    turso_status_t status;
    size_t column_count;
} turso_statement_column_count_result_t;

/** Get column count */
turso_statement_column_count_result_t
turso_statement_column_count(turso_statement_t self);

typedef struct
{
    turso_status_t status;
    const char *column_name;
} turso_statement_column_name_result_t;

/** Get the column name at the index
 * C string allocated by Turso must be freed after the usage with corresponding turso_str_deinit(...) method
 */
turso_statement_column_name_result_t
turso_statement_column_name(turso_statement_t self, size_t index);

typedef struct
{
    turso_status_t status;
    turso_value_t value;
} turso_statement_row_value_t;

/** Get the row value at the the index for a current statement state
 * SAFETY: returned turso_value_t will be valid only until next invocation of statement operation (step, finalize, reset, etc)
 * Caller must make sure that any non-owning memory is copied appropriated if it will be used for longer lifetime
 */
turso_statement_row_value_t turso_statement_row_value(turso_statement_t self, size_t index);

/** Bind a named argument to a statement */
turso_status_t turso_statement_bind_named(
    turso_statement_t self,
    turso_slice_ref_t name,
    turso_value_t value);
/** Bind a positional argument to a statement */
turso_status_t
turso_statement_bind_positional(turso_statement_t self, size_t position, turso_value_t value);

/** Create a turso integer value */
turso_value_t turso_integer(int64_t integer);
/** Create a turso real value */
turso_value_t turso_real(double real);
/** Create a turso text value */
turso_value_t turso_text(const char *ptr, size_t len);
/** Create a turso blob value */
turso_value_t turso_blob(const uint8_t *ptr, size_t len);
/** Create a turso null value */
turso_value_t turso_null();

/** Deallocate a status */
void turso_status_deinit(turso_status_t self);
/** Deallocate C string allocated by Turso */
void turso_str_deinit(const char *self);
/** Deallocate and close a database
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited database
 */
void turso_database_deinit(turso_database_t self);
/** Deallocate and close a connection
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited connection
 */
void turso_connection_deinit(turso_connection_t self);
/** Deallocate and close a statement
 * SAFETY: caller must ensure that no other code can concurrently or later call methods over deinited statement
 */
void turso_statement_deinit(turso_statement_t self);

#endif /* TURSO_H */