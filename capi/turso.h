#ifndef TURSO_H
#define TURSO_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
    TURSO_OK = 0,
    TURSO_ERROR = 1,
} turso_status_code_t;

typedef struct turso_status_t {
    const char *error;
    turso_status_code_t code;
} turso_status_t;

typedef enum {
    TURSO_TYPE_INTEGER = 1,
    TURSO_TYPE_REAL = 2,
    TURSO_TYPE_TEXT = 3,
    TURSO_TYPE_BLOB = 4,
    TURSO_TYPE_NULL = 5,
} turso_type_t;

typedef enum {
    TURSO_TRACING_LEVEL_ERROR = 1,
    TURSO_TRACING_LEVEL_WARN,
    TURSO_TRACING_LEVEL_INFO,
    TURSO_TRACING_LEVEL_DEBUG,
    TURSO_TRACING_LEVEL_TRACE,
} turso_tracing_level_t;

typedef struct {
    const char *message;
    const char *target;
    const char *file;
    uint64_t timestamp;
    size_t line;
    turso_tracing_level_t level;
} turso_log_t;

typedef struct {
    turso_status_t err;
    void *inner;
} turso_database_t;

typedef struct {
    turso_status_t err;
    void *inner;
} turso_connection_t;

typedef struct {
    turso_status_t err;
    void *inner;
} turso_statement_t;

typedef struct {
    turso_status_t err;
    void *inner;
} turso_rows_t;

typedef struct {
    turso_status_t err;
    void *inner;
} turso_row_t;

typedef struct {
    const void *ptr;
    size_t len;
} turso_slice_t;

typedef union {
    int64_t integer;
    double real;
    turso_slice_t text;
    turso_slice_t blob;
} turso_value_union_t;

typedef struct {
    turso_value_union_t value;
    turso_type_t type;
} turso_value_t;

typedef struct {
    turso_status_t err;
    turso_value_t ok;
} turso_result_value_t;

typedef struct {
    turso_status_t err;
    uint64_t rows_changed;
} turso_execute_t;

/**
 * Database description.
 */
typedef struct {
    /** Path to the database file or `:memory:` */
    const char *path;
} turso_database_config_t;

typedef struct {
    void (*logger)(turso_log_t log);
    const char *version;
} turso_config_t;

/** Setup some global info */
turso_status_t turso_setup(turso_config_t config);

/** Create or open a database */
turso_database_t turso_database_init(turso_database_config_t config);

/** Connect with the database */
turso_connection_t turso_database_connect(turso_database_t self);

/** Prepare a statement in a connection */
turso_statement_t
turso_connection_prepare(turso_connection_t self, const char *sql);

/** Execute a statement */
turso_execute_t turso_statement_execute(turso_statement_t self);
/** Query a statement */
turso_rows_t turso_statement_query(turso_statement_t self);
/** Reset a statement */
turso_status_t turso_statement_reset(turso_statement_t self);

/** Get the next row from rows */
turso_row_t turso_rows_next(turso_rows_t self);
/** Get the column name at the index */
turso_slice_t turso_rows_column_name(turso_rows_t self, int32_t index);
/** Get rows column count */
int32_t turso_rows_column_count(turso_rows_t self);

/** Get the value at the the index */
turso_result_value_t turso_row_value(turso_row_t self, int32_t index);
/** Get the column name at the the index */
turso_slice_t turso_row_name(turso_row_t self, int32_t index);
/** Check if the row is empty, indicating the end of `turso_rows_next` */
bool turso_row_empty(turso_row_t self);

/** Bind a named argument to a statement */
turso_status_t turso_statement_bind_named(
    turso_statement_t self,
    const char *name,
    turso_value_t value
);
/** Bind a positional argument to a statement */
turso_status_t
turso_statement_bind_positional(turso_statement_t self, int32_t position, turso_value_t value);

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

/** Deallocate and close a error */
void turso_status_deinit(turso_status_t self);
/** Deallocate and close a database */
void turso_database_deinit(turso_database_t self);
/** Deallocate and close a connection */
void turso_connection_deinit(turso_connection_t self);
/** Deallocate and close a statement */
void turso_statement_deinit(turso_statement_t self);
/** Deallocate and close rows */
void turso_rows_deinit(turso_rows_t self);
/** Deallocate and close a row */
void turso_row_deinit(turso_row_t self);
/** Deallocate a slice */
void turso_slice_deinit(turso_slice_t value);

#endif /* TURSO_H */