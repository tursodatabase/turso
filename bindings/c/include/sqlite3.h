#ifndef LIMBO_SQLITE3_H
#define LIMBO_SQLITE3_H

#include <stdint.h>

#define SQLITE_VERSION        "3.42.0"
#define SQLITE_VERSION_NUMBER 3042000

/* SQLite C extension loading is not supported: Turso extensions are a
** different mechanism. Consumers (e.g. PHP's ext/sqlite3) compile their
** loadExtension paths out cleanly under this define. */
#define SQLITE_OMIT_LOAD_EXTENSION 1

#define SQLITE_OK          0
#define SQLITE_ERROR       1
#define SQLITE_INTERNAL    2
#define SQLITE_PERM        3
#define SQLITE_ABORT       4
#define SQLITE_BUSY        5
#define SQLITE_LOCKED      6
#define SQLITE_NOMEM       7
#define SQLITE_READONLY    8
#define SQLITE_INTERRUPT   9
#define SQLITE_IOERR      10
#define SQLITE_CORRUPT    11
#define SQLITE_NOTFOUND   12
#define SQLITE_FULL       13
#define SQLITE_CANTOPEN   14
#define SQLITE_PROTOCOL   15
#define SQLITE_EMPTY      16
#define SQLITE_SCHEMA     17
#define SQLITE_TOOBIG     18
#define SQLITE_CONSTRAINT 19
#define SQLITE_MISMATCH   20
#define SQLITE_MISUSE     21
#define SQLITE_NOLFS      22
#define SQLITE_AUTH       23
#define SQLITE_FORMAT     24
#define SQLITE_RANGE      25
#define SQLITE_NOTADB     26
#define SQLITE_NOTICE     27
#define SQLITE_WARNING    28

#define SQLITE_ROW 100

#define SQLITE_DONE 101

#define SQLITE_ABORT_ROLLBACK (SQLITE_ABORT | (2 << 8))

#define SQLITE_STATE_OPEN 118

#define SQLITE_STATE_SICK 186

#define SQLITE_STATE_BUSY 109

/* Flags for sqlite3_open_v2 */
#define SQLITE_OPEN_READONLY      0x00000001
#define SQLITE_OPEN_READWRITE     0x00000002
#define SQLITE_OPEN_CREATE        0x00000004
#define SQLITE_OPEN_URI           0x00000040
#define SQLITE_OPEN_MEMORY        0x00000080
#define SQLITE_OPEN_NOMUTEX       0x00008000
#define SQLITE_OPEN_FULLMUTEX     0x00010000
#define SQLITE_OPEN_SHAREDCACHE   0x00020000
#define SQLITE_OPEN_PRIVATECACHE  0x00040000
#define SQLITE_OPEN_NOFOLLOW      0x01000000
#define SQLITE_OPEN_EXRESCODE     0x02000000

#define SQLITE_CHECKPOINT_PASSIVE 0

#define SQLITE_CHECKPOINT_FULL 1

#define SQLITE_CHECKPOINT_RESTART 2

#define SQLITE_CHECKPOINT_TRUNCATE 3

#define SQLITE_INTEGER  1
#define SQLITE_FLOAT    2
#define SQLITE_BLOB     4
#define SQLITE_NULL     5
#define SQLITE_TEXT     3
#define SQLITE3_TEXT     3

typedef void (*sqlite3_destructor_type)(void*);
#define SQLITE_STATIC    ((sqlite3_destructor_type)0)
#define SQLITE_TRANSIENT ((sqlite3_destructor_type)-1)

typedef struct sqlite3 sqlite3;

typedef struct sqlite3_stmt sqlite3_stmt;
typedef struct sqlite3_context sqlite3_context;
typedef struct sqlite3_value sqlite3_value;
typedef struct sqlite3_blob sqlite3_blob;
typedef struct sqlite3_backup sqlite3_backup;

/* Text encodings and function flags for sqlite3_create_function */
#define SQLITE_UTF8           1
#define SQLITE_UTF16LE        2
#define SQLITE_UTF16BE        3
#define SQLITE_UTF16          4
#define SQLITE_ANY            5
#define SQLITE_UTF16_ALIGNED  8
#define SQLITE_DETERMINISTIC  0x000000800
#define SQLITE_DIRECTONLY     0x000080000
#define SQLITE_SUBTYPE        0x000100000
#define SQLITE_INNOCUOUS      0x000200000
typedef int64_t sqlite3_int64;
typedef sqlite3_int64 sqlite_int64;

typedef int (*exec_callback)(void *context, int n_column, char **argv, char **colv);

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

int sqlite3_initialize(void);

int sqlite3_shutdown(void);

int sqlite3_open(const char *filename, sqlite3 **db_out);

int sqlite3_open_v2(const char *filename, sqlite3 **db_out, int _flags, const char *_z_vfs);

int sqlite3_close(sqlite3 *db);

int sqlite3_close_v2(sqlite3 *db);

const char *sqlite3_db_filename(sqlite3 *db, const char *db_name);

int sqlite3_trace_v2(sqlite3 *_db,
                     unsigned int _mask,
                     void (*_callback)(unsigned int, void*, void*, void*),
                     void *_context);

void sqlite3_progress_handler(sqlite3 *_db, int _n, int (*_callback)(void *), void *_context);

int sqlite3_busy_timeout(sqlite3 *_db, int _ms);

int sqlite3_set_authorizer(sqlite3 *db,
                           int (*xAuth)(void*, int, const char*, const char*, const char*, const char*),
                           void *pUserData);

/* Authorizer callback return codes */
#define SQLITE_DENY   1
#define SQLITE_IGNORE 2

/* Authorizer action codes (third parameter meanings per SQLite docs) */
#define SQLITE_CREATE_INDEX          1
#define SQLITE_CREATE_TABLE          2
#define SQLITE_CREATE_TEMP_INDEX     3
#define SQLITE_CREATE_TEMP_TABLE     4
#define SQLITE_CREATE_TEMP_TRIGGER   5
#define SQLITE_CREATE_TEMP_VIEW      6
#define SQLITE_CREATE_TRIGGER        7
#define SQLITE_CREATE_VIEW           8
#define SQLITE_DELETE                9
#define SQLITE_DROP_INDEX           10
#define SQLITE_DROP_TABLE           11
#define SQLITE_DROP_TEMP_INDEX      12
#define SQLITE_DROP_TEMP_TABLE      13
#define SQLITE_DROP_TEMP_TRIGGER    14
#define SQLITE_DROP_TEMP_VIEW       15
#define SQLITE_DROP_TRIGGER         16
#define SQLITE_DROP_VIEW            17
#define SQLITE_INSERT               18
#define SQLITE_PRAGMA               19
#define SQLITE_READ                 20
#define SQLITE_SELECT               21
#define SQLITE_TRANSACTION          22
#define SQLITE_UPDATE               23
#define SQLITE_ATTACH               24
#define SQLITE_DETACH               25
#define SQLITE_ALTER_TABLE          26
#define SQLITE_REINDEX              27
#define SQLITE_ANALYZE              28
#define SQLITE_CREATE_VTABLE        29
#define SQLITE_DROP_VTABLE          30
#define SQLITE_FUNCTION             31
#define SQLITE_SAVEPOINT            32
#define SQLITE_COPY                  0
#define SQLITE_RECURSIVE            33

sqlite3 *sqlite3_context_db_handle(sqlite3_context *context);

int sqlite3_prepare_v2(sqlite3 *db, const char *sql, int _len, sqlite3_stmt **out_stmt, const char **_tail);

int sqlite3_prepare_v3(sqlite3 *db, const char *sql, int _len, unsigned int _prep_flags, sqlite3_stmt **out_stmt, const char **_tail);

#define SQLITE_PREPARE_PERSISTENT 0x01
#define SQLITE_PREPARE_NORMALIZE  0x02
#define SQLITE_PREPARE_NO_VTAB    0x04

int sqlite3_finalize(sqlite3_stmt *stmt);

int sqlite3_step(sqlite3_stmt *stmt);

int sqlite3_exec(sqlite3 *db, const char *sql, exec_callback _callback, void *_context, char **_err);

int sqlite3_reset(sqlite3_stmt *stmt);

int sqlite3_clear_bindings(sqlite3_stmt *stmt);

int sqlite3_changes(sqlite3 *_db);

int64_t sqlite3_changes64(sqlite3 *_db);

int sqlite3_stmt_readonly(sqlite3_stmt *_stmt);

int sqlite3_stmt_busy(sqlite3_stmt *_stmt);

int sqlite3_stmt_status(sqlite3_stmt *stmt, int op, int resetFlg);

#define SQLITE_STMTSTATUS_FULLSCAN_STEP 1
#define SQLITE_STMTSTATUS_SORT 2
#define SQLITE_STMTSTATUS_AUTOINDEX 3
#define SQLITE_STMTSTATUS_VM_STEP 4
#define SQLITE_STMTSTATUS_REPREPARE 5
#define SQLITE_STMTSTATUS_RUN 6
#define SQLITE_STMTSTATUS_FILTER_MISS 7
#define SQLITE_STMTSTATUS_FILTER_HIT 8
#define SQLITE_STMTSTATUS_MEMUSED 99

#define LIBSQL_STMTSTATUS_BASE 1024
#define LIBSQL_STMTSTATUS_ROWS_READ (LIBSQL_STMTSTATUS_BASE + 1)
#define LIBSQL_STMTSTATUS_ROWS_WRITTEN (LIBSQL_STMTSTATUS_BASE + 2)

sqlite3_stmt *sqlite3_next_stmt(sqlite3 *db, sqlite3_stmt *stmt);

int sqlite3_serialize(sqlite3 *_db, const char *_schema, void **_out, int *_out_bytes, unsigned int _flags);

int sqlite3_deserialize(sqlite3 *_db, const char *_schema, const void *_in_, int _in_bytes, unsigned int _flags);

int sqlite3_get_autocommit(sqlite3 *_db);

int sqlite3_total_changes(sqlite3 *_db);

int64_t sqlite3_last_insert_rowid(sqlite3 *_db);

void sqlite3_interrupt(sqlite3 *_db);

int sqlite3_db_config(sqlite3 *db, int op, ...);

#define SQLITE_DBCONFIG_MAINDBNAME            1000
#define SQLITE_DBCONFIG_LOOKASIDE             1001
#define SQLITE_DBCONFIG_ENABLE_FKEY           1002
#define SQLITE_DBCONFIG_ENABLE_TRIGGER        1003
#define SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER 1004
#define SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION 1005
#define SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE      1006
#define SQLITE_DBCONFIG_ENABLE_QPSG           1007
#define SQLITE_DBCONFIG_TRIGGER_EQP           1008
#define SQLITE_DBCONFIG_RESET_DATABASE        1009
#define SQLITE_DBCONFIG_DEFENSIVE             1010
#define SQLITE_DBCONFIG_WRITABLE_SCHEMA       1011
#define SQLITE_DBCONFIG_LEGACY_ALTER_TABLE    1012
#define SQLITE_DBCONFIG_DQS_DML               1013
#define SQLITE_DBCONFIG_DQS_DDL               1014
#define SQLITE_DBCONFIG_ENABLE_VIEW           1015
#define SQLITE_DBCONFIG_LEGACY_FILE_FORMAT    1016
#define SQLITE_DBCONFIG_TRUSTED_SCHEMA        1017

int sqlite3_extended_result_codes(sqlite3 *db, int onoff);

sqlite3 *sqlite3_db_handle(sqlite3_stmt *_stmt);

void sqlite3_sleep(int _ms);

int sqlite3_limit(sqlite3 *_db, int _id, int _new_value);

void *sqlite3_malloc64(int _n);

void sqlite3_free(void *_ptr);

char *sqlite3_mprintf(const char *fmt, ...);

char *sqlite3_snprintf(int n, char *buf, const char *fmt, ...);

int sqlite3_errcode(sqlite3 *_db);

const char *sqlite3_errstr(int _err);

void *sqlite3_user_data(sqlite3_context *context);

sqlite3_backup *sqlite3_backup_init(sqlite3 *dest_db, const char *dest_name, sqlite3 *source_db, const char *source_name);

int sqlite3_backup_step(sqlite3_backup *backup, int n_pages);

int sqlite3_backup_remaining(sqlite3_backup *backup);

int sqlite3_backup_pagecount(sqlite3_backup *backup);

int sqlite3_backup_finish(sqlite3_backup *backup);

const char *sqlite3_sql(sqlite3_stmt *stmt);

char *sqlite3_expanded_sql(sqlite3_stmt *stmt);

int sqlite3_data_count(sqlite3_stmt *stmt);

int sqlite3_bind_parameter_count(sqlite3_stmt *_stmt);

const char *sqlite3_bind_parameter_name(sqlite3_stmt *_stmt, int _idx);

int sqlite3_bind_parameter_index(sqlite3_stmt *_stmt, const char *_name);

int sqlite3_bind_null(sqlite3_stmt *_stmt, int _idx);

int sqlite3_bind_int64(sqlite3_stmt *_stmt, int _idx, int64_t _val);

int sqlite3_bind_double(sqlite3_stmt *_stmt, int _idx, double _val);

int sqlite3_bind_text(sqlite3_stmt *stmt, int idx, const char *text, int len, sqlite3_destructor_type destroy);

int sqlite3_bind_blob(sqlite3_stmt *stmt, int idx, const void *blob, int len, sqlite3_destructor_type destroy);

int sqlite3_column_type(sqlite3_stmt *_stmt, int _idx);

int sqlite3_column_count(sqlite3_stmt *_stmt);

const char *sqlite3_column_decltype(sqlite3_stmt *_stmt, int _idx);

const char *sqlite3_column_name(sqlite3_stmt *_stmt, int _idx);

const char *sqlite3_column_table_name(sqlite3_stmt *_stmt, int _idx);

int64_t sqlite3_column_int64(sqlite3_stmt *_stmt, int _idx);

double sqlite3_column_double(sqlite3_stmt *_stmt, int _idx);

const void *sqlite3_column_blob(sqlite3_stmt *_stmt, int _idx);

int sqlite3_column_bytes(sqlite3_stmt *_stmt, int _idx);

sqlite3_value *sqlite3_column_value(sqlite3_stmt *stmt, int idx);

int sqlite3_value_type(sqlite3_value *value);

int64_t sqlite3_value_int64(sqlite3_value *value);

int sqlite3_value_int(sqlite3_value *value);

double sqlite3_value_double(sqlite3_value *value);

const unsigned char *sqlite3_value_text(sqlite3_value *value);

const void *sqlite3_value_blob(sqlite3_value *value);

int sqlite3_value_bytes(sqlite3_value *value);

sqlite3_value *sqlite3_value_dup(sqlite3_value *value);

void sqlite3_value_free(sqlite3_value *value);

const unsigned char *sqlite3_column_text(sqlite3_stmt *stmt, int idx);

int sqlite_get_table_cb(void *context, int n_column, char **argv, char **colv);

int sqlite3_get_table(sqlite3 *db, const char *sql, char ***paz_result, int *pn_row, int *pn_column, char **pz_err_msg);

void sqlite3_free_table(char **az_result);

void sqlite3_result_null(sqlite3_context *context);

void sqlite3_result_int64(sqlite3_context *context, int64_t val);

void sqlite3_result_int(sqlite3_context *context, int val);

void sqlite3_result_double(sqlite3_context *context, double val);

void sqlite3_result_text(sqlite3_context *context, const char *text, int len, sqlite3_destructor_type destroy);

void sqlite3_result_blob(sqlite3_context *context, const void *blob, int len, sqlite3_destructor_type destroy);

void sqlite3_result_error_nomem(sqlite3_context *context);

void sqlite3_result_error_toobig(sqlite3_context *context);

void sqlite3_result_error(sqlite3_context *context, const char *err, int len);

void *sqlite3_aggregate_context(sqlite3_context *context, int n);

int sqlite3_blob_open(sqlite3 *_db,
                      const char *_db_name,
                      const char *_table_name,
                      const char *_column_name,
                      int64_t _rowid,
                      int _flags,
                      sqlite3_blob **blob_out);

int sqlite3_blob_read(sqlite3_blob *blob, void *data, int n, int offset);

int sqlite3_blob_write(sqlite3_blob *blob, const void *data, int n, int offset);

int sqlite3_blob_bytes(sqlite3_blob *blob);

int sqlite3_blob_close(sqlite3_blob *blob);

int sqlite3_stricmp(const char *_a, const char *_b);

int sqlite3_create_collation(sqlite3 *db,
                             const char *name,
                             int enc,
                             void *context,
                             int (*xCompare)(void*, int, const void*, int, const void*));

int sqlite3_create_collation_v2(sqlite3 *db,
                                const char *name,
                                int enc,
                                void *context,
                                int (*xCompare)(void*, int, const void*, int, const void*),
                                void (*xDestroy)(void*));

int sqlite3_create_function(sqlite3 *db,
                            const char *name,
                            int n_args,
                            int enc,
                            void *context,
                            void (*xFunc)(sqlite3_context*, int, sqlite3_value**),
                            void (*xStep)(sqlite3_context*, int, sqlite3_value**),
                            void (*xFinal)(sqlite3_context*));

int sqlite3_create_function_v2(sqlite3 *db,
                               const char *name,
                               int n_args,
                               int enc,
                               void *context,
                               void (*xFunc)(sqlite3_context*, int, sqlite3_value**),
                               void (*xStep)(sqlite3_context*, int, sqlite3_value**),
                               void (*xFinal)(sqlite3_context*),
                               void (*xDestroy)(void*));

int sqlite3_create_window_function(sqlite3 *_db,
                                   const char *_name,
                                   int _n_args,
                                   int _enc,
                                   void *_context,
                                   void (*_x_step)(void),
                                   void (*_x_final)(void),
                                   void (*_x_value)(void),
                                   void (*_x_inverse)(void),
                                   void (*_destroy)(void));

const char *sqlite3_errmsg(sqlite3 *_db);

int sqlite3_extended_errcode(sqlite3 *_db);

int sqlite3_complete(const char *_sql);

int sqlite3_threadsafe(void);

const char *sqlite3_libversion(void);

int sqlite3_libversion_number(void);

int sqlite3_wal_checkpoint(sqlite3 *_db, const char *_db_name);

int sqlite3_wal_checkpoint_v2(sqlite3 *db, const char *_db_name, int _mode, int *_log_size, int *_checkpoint_count);

/**
 * Get the number of frames in the WAL.
 *
 * The `libsql_wal_frame_count` function returns the number of frames
 * in the WAL in the `p_frame_count` parameter.
 *
 * # Returns
 *
 * - `SQLITE_OK` if the number of frames in the WAL file is
 *   successfully returned.
 * - `SQLITE_MISUSE` if the `db` is `NULL`.
 * - `SQLITE_ERROR` if an error occurs while getting the number of frames
 *   in the WAL file.
 *
 * # Safety
 *
 * - The `db` must be a valid pointer to a `sqlite3` database connection.
 * - The `p_frame_count` must be a valid pointer to a `u32` that will store
 *   the number of frames in the WAL file.
 */
int libsql_wal_frame_count(sqlite3 *db, uint32_t *p_frame_count);

/**
 * Return meta information about a specific column of a database table.
 * 
 * @param db Connection handle
 * @param zDbName Database name or NULL for main database
 * @param zTableName Table name
 * @param zColumnName Column name
 * @param pzDataType OUTPUT: Declared data type
 * @param pzCollSeq OUTPUT: Collation sequence name
 * @param pNotNull OUTPUT: True if NOT NULL constraint exists
 * @param pPrimaryKey OUTPUT: True if column part of PK
 * @param pAutoinc OUTPUT: True if column is auto-increment
 * @return SQLITE_OK on success, SQLITE_ERROR on error
 */
int sqlite3_table_column_metadata(
    sqlite3 *db,
    const char *zDbName,
    const char *zTableName,
    const char *zColumnName,
    char const **pzDataType,
    char const **pzCollSeq,
    int *pNotNull,
    int *pPrimaryKey,
    int *pAutoinc
);

/*
** Enable all Turso experimental features for subsequently opened databases.
*/
void turso_enable_experimental(void);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* LIMBO_SQLITE3_H */
