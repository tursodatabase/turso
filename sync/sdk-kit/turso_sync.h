#ifndef TURSO_SYNC_H
#define TURSO_SYNC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <turso.h>

/******** TURSO_DATABASE_SYNC_IO_REQUEST ********/

// TODO
typedef enum
{
    TURSO_SYNC_IO_NONE = 0,
    TURSO_SYNC_IO_HTTP = 1,
    TURSO_SYNC_IO_FULL_READ = 2,
    TURSO_SYNC_IO_FULL_WRITE = 3,
} turso_sync_io_request_type_t;

// TODO
typedef struct
{
    turso_slice_ref_t method;
    turso_slice_ref_t path;
    turso_slice_ref_t body;
    int32_t headers;
} turso_sync_io_http_request_t;

typedef struct
{
    turso_slice_ref_t key;
    turso_slice_ref_t value;
} turso_sync_io_http_header_t;

// TODO
typedef struct
{
    turso_slice_ref_t path;
} turso_sync_io_full_read_request_t;

// TODO
typedef struct
{
    turso_slice_ref_t path;
    turso_slice_ref_t content;
} turso_sync_io_full_write_request_t;

// TODO
typedef union
{
    turso_sync_io_http_request_t http;
    turso_sync_io_full_read_request_t full_read;
    turso_sync_io_full_write_request_t full_write;
} turso_sync_io_request_union_t;

// TODO
typedef struct
{
    turso_sync_io_request_type_t type;
    turso_sync_io_request_union_t request;
} turso_sync_io_request_t;

/******** TURSO_ASYNC_OPERATION_RESULT ********/

// TODO
typedef enum
{
    TURSO_ASYNC_RESULT_NONE = 0,
    TURSO_ASYNC_RESULT_CONNECTION = 1,
    TURSO_ASYNC_RESULT_CHANGES = 2,
    TURSO_ASYNC_RESULT_STATS = 3,
} turso_sync_operation_result_type_t;

/// structure holding opaque pointer to the TursoDatabaseSyncChanges instance
/// SAFETY: todo
typedef struct
{
    void *inner;
} turso_sync_changes_t;

/// structure holding opaque pointer to the TursoDatabaseSyncChanges instance
/// SAFETY: todo
typedef struct
{
    int64_t cdc_operations;
    int64_t main_wal_size;
    int64_t revert_wal_size;
    int64_t last_pull_unix_time;
    int64_t last_push_unix_time;
    int64_t network_sent_bytes;
    int64_t network_received_bytes;
    turso_slice_ref_t revision;
} turso_sync_stats_t;

typedef union
{
    turso_connection_t connection;
    turso_sync_changes_t changes;
    turso_sync_stats_t stats;
} turso_sync_operation_result_union_t;

typedef struct
{
    turso_sync_operation_result_type_t type;
    turso_sync_operation_result_union_t result;
} turso_sync_operation_result_t;

/******** MAIN TYPES ********/

/**
 * Database description.
 */
typedef struct
{
    const char *path;
    const char *client_name;
    int32_t wal_pull_batch_size;
    int32_t long_poll_timeout_ms;
    bool bootstrap_if_empty;
    int32_t reserved_bytes;
    int32_t partial_bootstrap_strategy_prefix;
    const char *partial_bootstrap_strategy_query;
} turso_sync_database_config_t;

/// structure holding opaque pointer to the TursoDatabaseSync instance
/// SAFETY: todo
typedef struct
{
    void *inner;
} turso_sync_database_t;

/// structure holding opaque pointer to the TursoAsyncOperation instance
/// SAFETY: todo
typedef struct
{
    void *inner;
} turso_sync_operation_t;

typedef struct
{
    void *inner;
} turso_sync_io_item_t;

/******** METHODS ********/

typedef struct
{
    turso_status_t status;
    turso_sync_database_t database_sync;
} turso_sync_database_new_result_t;

/** Create database sync holder but do not open it */
turso_sync_database_new_result_t
turso_sync_database_new(turso_database_config_t db_config, turso_sync_database_config_t sync_config);

typedef struct
{
    turso_status_t status;
    turso_sync_operation_t operation;
} turso_sync_operation_return_t;

/** Prepare synced database for use (bootstrap if needed, setup necessary database parameters for first access) */
turso_sync_operation_return_t turso_sync_database_init(turso_sync_database_t self);

/** Open prepared synced database, fail if no properly setup database exists */
turso_sync_operation_return_t turso_sync_database_open(turso_sync_database_t self);

/** Open or prepared synced database or create it if no properly setup database exists */
turso_sync_operation_return_t turso_sync_database_create(turso_sync_database_t self);

/** Create turso database connection
 * SAFETY: synced database must be opened before that operation (with either turso_database_sync_create or turso_database_sync_open)
 */
turso_sync_operation_return_t turso_sync_database_connect(turso_sync_database_t self);

/** Collect stats about synced database */
turso_sync_operation_return_t turso_sync_database_stats(turso_sync_database_t self);

/** Checkpoint WAL of the synced database */
turso_sync_operation_return_t turso_sync_database_checkpoint(turso_sync_database_t self);

/** Push local changes to remote */
turso_sync_operation_return_t turso_sync_database_push_changes(turso_sync_database_t self);

/** Wait for remote changes */
turso_sync_operation_return_t turso_sync_database_wait_changes(turso_sync_database_t self);

/** Apply remote changes locally */
turso_sync_operation_return_t turso_sync_database_apply_changes(turso_sync_database_t self, turso_sync_changes_t changes);

typedef struct
{
    turso_status_t status;
    turso_sync_operation_result_t result;
} turso_sync_operation_resume_result_t;

/** Resume async operation */
turso_sync_operation_resume_result_t turso_sync_operation_resume(turso_sync_operation_t self);

typedef struct
{
    turso_status_t status;
    turso_sync_io_item_t result;
} turso_sync_database_io_take_item_t;

/** Try to take IO request from the sync engine IO queue */
turso_sync_database_io_take_item_t
turso_sync_database_io_take_item(turso_sync_database_t self);

/** Try to take IO request from the sync engine IO queue */
turso_status_t
turso_sync_database_io_step_callbacks(turso_sync_database_t self);

typedef struct
{
    turso_status_t status;
    turso_sync_io_request_t request;
} turso_sync_database_io_request_t;

/** Get request reference from the IO request */
turso_sync_database_io_request_t
turso_sync_database_io_request(turso_sync_io_item_t self);

typedef struct
{
    turso_status_t status;
    turso_sync_io_http_header_t header;
} turso_sync_database_io_request_header_t;

/** Get request reference from the IO request */
turso_sync_database_io_request_header_t
turso_sync_database_io_request_header(turso_sync_io_item_t self, int32_t header_idx);

/** Poison IO request completion with error */
turso_status_t turso_sync_database_io_poison(turso_sync_io_item_t self, turso_slice_ref_t error);

/** Set IO request completion status */
turso_status_t turso_sync_database_io_status(turso_sync_io_item_t self, int32_t status);

/** Push bytes to the IO completion buffer */
turso_status_t turso_sync_database_io_push_buffer(turso_sync_io_item_t self, turso_slice_ref_t buffer);

/** Set IO request completion as done */
turso_status_t turso_sync_database_io_done(turso_sync_io_item_t self);

/** Deallocate a TursoDatabaseSync */
void turso_sync_database_deinit(turso_sync_database_t self);

/** Deallocate a TursoAsyncOperation */
void turso_sync_operation_deinit(turso_sync_operation_t self);

/** TODO */
void turso_sync_database_io_item_deinit(turso_sync_io_item_t self);

/** Deallocate a turso async opeartion result */
void turso_sync_operation_result_deinit(turso_sync_operation_result_t self);

#endif /* TURSO_SYNC_H */