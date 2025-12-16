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

/// opaque pointer to the TursoDatabaseSyncChanges instance
/// SAFETY: turso_sync_changes_t have independent lifetime and must be explicitly deallocated with turso_sync_changes_deinit method OR passed to the turso_sync_database_apply_changes method which gather ownership to this object
typedef struct turso_sync_changes turso_sync_changes_t;

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
    size_t partial_bootstrap_segment_size;
    // optional parameter which defines if pages prefetch must be enabled
    // one of valid partial_bootstrap_strategy_* values MUST be set in order for this setting to have some effect
    bool partial_bootstrap_prefetch;
} turso_sync_database_config_t;

/// opaque pointer to the TursoDatabaseSync instance
typedef struct turso_sync_database turso_sync_database_t;

/// opaque pointer to the TursoAsyncOperation instance
/// SAFETY: methods for the turso_sync_operation_t can't be called concurrently
typedef struct turso_sync_operation turso_sync_operation_t;

/// opaque pointer to the SyncEngineIoQueueItem instance
typedef struct turso_sync_io_item turso_sync_io_item_t;

/******** METHODS ********/

/** Create database sync holder but do not open it */
turso_status_code_t turso_sync_database_new(
    const turso_database_config_t *db_config,
    const turso_sync_database_config_t *sync_config,
    /** reference to pointer which will be set to database instance in case of TURSO_OK result */
    const turso_sync_database_t **database,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Prepare synced database for use (bootstrap if needed, setup necessary database parameters for first access)
 * AsyncOperation returns None
 */
turso_status_code_t turso_sync_database_init(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Open prepared synced database, fail if no properly setup database exists
 * AsyncOperation returns None
 */
turso_status_code_t turso_sync_database_open(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Open or prepared synced database or create it if no properly setup database exists
 * AsyncOperation returns None
 */
turso_status_code_t turso_sync_database_create(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Create turso database connection
 * SAFETY: synced database must be opened before that operation (with either turso_database_sync_create or turso_database_sync_open)
 * AsyncOperation returns Connection
 */
turso_status_code_t turso_sync_database_connect(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Collect stats about synced database
 * AsyncOperation returns Stats
 */
turso_status_code_t turso_sync_database_stats(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Checkpoint WAL of the synced database
 * AsyncOperation returns None
 */
turso_status_code_t turso_sync_database_checkpoint(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Push local changes to remote
 * AsyncOperation returns None
 */
turso_status_code_t turso_sync_database_push_changes(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Wait for remote changes
 * AsyncOperation returns Changes (which must be properly deinited or used in the [turso_sync_database_apply_changes] method)
 */
turso_status_code_t turso_sync_database_wait_changes(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Apply remote changes locally
 * SAFETY: caller must guarantee that no other methods are executing concurrently (push/wait/checkpoint)
 * otherwise, operation will return MISUSE error
 *
 * the method CONSUMES turso_sync_changes_t instance and caller no longer owns it after the call
 * So, the changes MUST NOT be explicitly deallocated after the method call (either successful or not)
 *
 * AsyncOperation returns None
 */
turso_status_code_t turso_sync_database_apply_changes(
    const turso_sync_database_t *self,
    const turso_sync_changes_t *changes,
    /** reference to pointer which will be set to async operation instance in case of TURSO_OK result */
    const turso_sync_operation_t **operation,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Resume async operation
 * If return error status - turso_status_t must be properly cleaned up
 * If return TURSO_IO - caller must drive IO
 * If return TURSO_DONE - caller must inspect result and clean up it or use it accordingly
 */
turso_status_code_t turso_sync_operation_resume(
    const turso_sync_operation_t *self,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Extract operation result kind
 */
turso_sync_operation_result_type_t turso_sync_operation_result_kind(const turso_sync_operation_t *self);

/** Extract Connection result from finished async operation
 */
turso_status_code_t turso_sync_operation_result_extract_connection(
    const turso_sync_operation_t *self,
    const turso_connection_t **connection);

/** Extract Changes result from finished async operation
 * If no changes were fetched - return TURSO_OK and set changes to null pointer
 */
turso_status_code_t turso_sync_operation_result_extract_changes(
    const turso_sync_operation_t *self,
    const turso_sync_changes_t **changes);

/** Extract Stats result from finished async operation
 */
turso_status_code_t turso_sync_operation_result_extract_stats(
    const turso_sync_operation_t *self,
    turso_sync_stats_t *stats);

/** Try to take IO request from the sync engine IO queue */
turso_status_code_t
turso_sync_database_io_take_item(
    const turso_sync_database_t *self,
    /** reference to pointer which will be set to async io item instance in case of TURSO_OK result */
    const turso_sync_io_item_t **item,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Run extra database callbacks after IO execution */
turso_status_code_t
turso_sync_database_io_step_callbacks(
    const turso_sync_database_t *self,
    /** Optional return error parameter (can be null) */
    const char **error_opt_out);

/** Get request IO kind */
turso_sync_io_request_type_t
turso_sync_database_io_request_kind(const turso_sync_io_item_t *self);

/** Get HTTP request header key-value pair */
turso_status_code_t
turso_sync_database_io_request_http(const turso_sync_io_item_t *self, turso_sync_io_http_request_t *request);

/** Get HTTP request fields */
turso_status_code_t
turso_sync_database_io_request_http_header(const turso_sync_io_item_t *self, size_t index, turso_sync_io_http_header_t *header);

/** Get HTTP request fields */
turso_status_code_t
turso_sync_database_io_request_full_read(const turso_sync_io_item_t *self, turso_sync_io_full_read_request_t *request);

/** Get HTTP request fields */
turso_status_code_t
turso_sync_database_io_request_full_write(const turso_sync_io_item_t *self, turso_sync_io_full_write_request_t *request);

/** Poison IO request completion with error */
turso_status_code_t turso_sync_database_io_poison(const turso_sync_io_item_t *self, turso_slice_ref_t *error);

/** Set IO request completion status */
turso_status_code_t turso_sync_database_io_status(const turso_sync_io_item_t *self, int32_t status);

/** Push bytes to the IO completion buffer */
turso_status_code_t turso_sync_database_io_push_buffer(const turso_sync_io_item_t *self, turso_slice_ref_t *buffer);

/** Set IO request completion as done */
turso_status_code_t turso_sync_database_io_done(const turso_sync_io_item_t *self);

/** Deallocate a TursoDatabaseSync */
void turso_sync_database_deinit(const turso_sync_database_t *self);

/** Deallocate a TursoAsyncOperation */
void turso_sync_operation_deinit(const turso_sync_operation_t *self);

/** Deallocate a SyncEngineIoQueueItem */
void turso_sync_database_io_item_deinit(const turso_sync_io_item_t *self);

/** Deallocate a TursoDatabaseSyncChanges */
void turso_sync_changes_deinit(const turso_sync_changes_t *self);

#endif /* TURSO_SYNC_H */