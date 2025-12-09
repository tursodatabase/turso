#ifndef TURSO_SYNC_H
#define TURSO_SYNC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <turso.h>

/******** TURSO_DATABASE_SYNC_IO_REQUEST ********/

// sync engine IO request type
typedef enum
{
    // no IO needed
    TURSO_SYNC_IO_NONE = 0,
    // HTTP request (secure layer can be added by the caller which actually execute the IO)
    TURSO_SYNC_IO_HTTP = 1,
    // atomic read of the file (not found file must be treated as empty file)
    TURSO_SYNC_IO_FULL_READ = 2,
    // atomic write of the file (operation either succeed or no, on most FS this will be write to temp file followed by rename)
    TURSO_SYNC_IO_FULL_WRITE = 3,
} turso_sync_io_request_type_t;

// sync engine IO HTTP request fields
typedef struct
{
    // method name slice (e.g. GET, POST, etc)
    turso_slice_ref_t method;
    // method path slice
    turso_slice_ref_t path;
    // method body slice
    turso_slice_ref_t body;
    // amount of headers in the request (header key-value pairs can be extracted through turso_sync_database_io_request_header method)
    int32_t headers;
} turso_sync_io_http_request_t;

// sync engine IO HTTP request header key-value pair
typedef struct
{
    turso_slice_ref_t key;
    turso_slice_ref_t value;
} turso_sync_io_http_header_t;

// sync engine IO atomic read request
typedef struct
{
    // file path
    turso_slice_ref_t path;
} turso_sync_io_full_read_request_t;

// sync engine IO atomic write request
typedef struct
{
    // file path
    turso_slice_ref_t path;
    // file content
    turso_slice_ref_t content;
} turso_sync_io_full_write_request_t;

// typeless union holding one possible value for the sync engine IO request
typedef union
{
    turso_sync_io_http_request_t http;
    turso_sync_io_full_read_request_t full_read;
    turso_sync_io_full_write_request_t full_write;
} turso_sync_io_request_union_t;

// sync engine IO request
typedef struct
{
    turso_sync_io_request_type_t type;
    turso_sync_io_request_union_t request;
} turso_sync_io_request_t;

/******** TURSO_ASYNC_OPERATION_RESULT ********/

// async operation result type
typedef enum
{
    // no extra result was returned ("void" async operation)
    TURSO_ASYNC_RESULT_NONE = 0,
    // turso_connection_t result
    TURSO_ASYNC_RESULT_CONNECTION = 1,
    // turso_sync_changes_t result
    TURSO_ASYNC_RESULT_CHANGES = 2,
    // turso_sync_stats_t result
    TURSO_ASYNC_RESULT_STATS = 3,
} turso_sync_operation_result_type_t;

/// structure holding opaque pointer to the TursoDatabaseSyncChanges instance
/// SAFETY: turso_sync_changes_t have independent lifetime and must be explicitly deallocated with turso_sync_changes_deinit method OR passed to the turso_sync_database_apply_changes method which gather ownership to this object
typedef struct
{
    void *inner;
} turso_sync_changes_t;

/// structure holding opaque pointer to the SyncEngineStats instance
/// SAFETY: revision string will be valid only during async operation lifetime (until turso_sync_operation_deinit)
/// Most likely, caller will need to copy revision slice to its internal buffer for longer lifetime
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

// typeless union holding one possible value for async operation result
typedef union
{
    turso_connection_t connection;
    turso_sync_changes_t changes;
    turso_sync_stats_t stats;
} turso_sync_operation_result_union_t;

// async operation result
typedef struct
{
    turso_sync_operation_result_type_t type;
    turso_sync_operation_result_union_t result;
} turso_sync_operation_result_t;

/******** MAIN TYPES ********/

/**
 * Database sync description.
 */
typedef struct
{
    // path to the main database file (auxilary files like metadata, WAL, revert, changes will derive names from this path)
    const char *path;
    // arbitrary client name which will be used as a prefix for unique client id
    const char *client_name;
    // long poll timeout for pull method (if not zero, server will hold connection for the given timeout until new changes will appear)
    int32_t long_poll_timeout_ms;
    // bootstrap db if empty; if set - client will be able to connect to fresh db only when network is online
    bool bootstrap_if_empty;
    // reserved bytes which must be set for the database - necessary if remote encryption is set for the db in cloud
    int32_t reserved_bytes;
    // prefix bootstrap strategy which will enable partial sync which lazily pull necessary pages on demand and bootstrap db with pages from first N bytes of the db
    int32_t partial_bootstrap_strategy_prefix;
    // query bootstrap strategy which will enable partial sync which lazily pull necessary pages on demand and bootstrap db with pages touched by the server with given SQL query
    const char *partial_bootstrap_strategy_query;
    // optional parameter which defines segment size for lazy loading from remote server
    // one of valid partial_bootstrap_strategy_* values MUST be set in order for this setting to have some effect
    size_t partial_boostrap_segment_size;
    // optional parameter which defines if speculative pages load must be enabled
    // one of valid partial_bootstrap_strategy_* values MUST be set in order for this setting to have some effect
    bool partial_boostrap_speculative_load;
} turso_sync_database_config_t;

/// structure holding opaque pointer to the TursoDatabaseSync instance
typedef struct
{
    void *inner;
} turso_sync_database_t;

/// structure holding opaque pointer to the TursoAsyncOperation instance
/// SAFETY: methods for the turso_sync_operation_t can't be called concurrently
typedef struct
{
    void *inner;
} turso_sync_operation_t;

/// structure holding opaque pointer to the SyncEngineIoQueueItem instance
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

/** Prepare synced database for use (bootstrap if needed, setup necessary database parameters for first access)
 * AsyncOperation returns None
 */
turso_sync_operation_return_t turso_sync_database_init(turso_sync_database_t self);

/** Open prepared synced database, fail if no properly setup database exists
 * AsyncOperation returns None
 */
turso_sync_operation_return_t turso_sync_database_open(turso_sync_database_t self);

/** Open or prepared synced database or create it if no properly setup database exists
 * AsyncOperation returns None
 */
turso_sync_operation_return_t turso_sync_database_create(turso_sync_database_t self);

/** Create turso database connection
 * SAFETY: synced database must be opened before that operation (with either turso_database_sync_create or turso_database_sync_open)
 * AsyncOperation returns Connection
 */
turso_sync_operation_return_t turso_sync_database_connect(turso_sync_database_t self);

/** Collect stats about synced database
 * AsyncOperation returns Stats
 */
turso_sync_operation_return_t turso_sync_database_stats(turso_sync_database_t self);

/** Checkpoint WAL of the synced database
 * AsyncOperation returns None
 */
turso_sync_operation_return_t turso_sync_database_checkpoint(turso_sync_database_t self);

/** Push local changes to remote
 * AsyncOperation returns None
 */
turso_sync_operation_return_t turso_sync_database_push_changes(turso_sync_database_t self);

/** Wait for remote changes
 * AsyncOperation returns Changes (which must be properly deinited or used in the [turso_sync_database_apply_changes] method)
 */
turso_sync_operation_return_t turso_sync_database_wait_changes(turso_sync_database_t self);

/** Apply remote changes locally
 * SAFETY: caller must guarantee that no other methods are executing concurrently (push/wait/checkpoint)
 * otherwise, operation will return MISUSE error
 *
 * AsyncOperation returns None
 */
turso_sync_operation_return_t turso_sync_database_apply_changes(turso_sync_database_t self, turso_sync_changes_t changes);

typedef struct
{
    turso_status_t status;
    bool empty;
} turso_sync_changes_empty_result_t;

/** Return if no changes were fetched from remote */
turso_sync_changes_empty_result_t
turso_sync_changes_empty(turso_sync_changes_t changes);

typedef struct
{
    turso_status_t status;
    turso_sync_operation_result_t result;
} turso_sync_operation_resume_result_t;

/** Resume async operation
 * If return error status - turso_status_t must be properly cleaned up
 * If return TURSO_IO - caller must drive IO
 * If return TURSO_DONE - caller must inspect result and clean up it or use it accordingly
 */
turso_sync_operation_resume_result_t turso_sync_operation_resume(turso_sync_operation_t self);

typedef struct
{
    turso_status_t status;
    turso_sync_io_item_t result;
} turso_sync_database_io_take_item_t;

/** Try to take IO request from the sync engine IO queue */
turso_sync_database_io_take_item_t
turso_sync_database_io_take_item(turso_sync_database_t self);

/** Run extra database callbacks after IO execution */
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

/** Get HTTP request header reference from the IO request */
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

/** Deallocate a SyncEngineIoQueueItem */
void turso_sync_database_io_item_deinit(turso_sync_io_item_t self);

/** Deallocate a TursoDatabaseSyncChanges */
void turso_sync_changes_deinit(turso_sync_changes_t self);

#endif /* TURSO_SYNC_H */