#ifndef LIMBO_SQLITE3_H
#define LIMBO_SQLITE3_H

#include <stdint.h>

#define SQLITE_OK 0

#define SQLITE_ERROR 1

#define SQLITE_ABORT 4

#define SQLITE_BUSY 5

#define SQLITE_NOMEM 7

#define SQLITE_INTERRUPT 9

#define SQLITE_NOTFOUND 12

#define SQLITE_CANTOPEN 14

#define SQLITE_MISUSE 21

#define SQLITE_ROW 100

#define SQLITE_DONE 101

#define SQLITE_ABORT_ROLLBACK (SQLITE_ABORT | (2 << 8))

#define SQLITE_STATE_OPEN 118

#define SQLITE_STATE_SICK 186

#define SQLITE_STATE_BUSY 109

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

int sqlite3_progress_handler(sqlite3 *_db, int _n, int (*_callback)(void), void *_context);

int sqlite3_busy_timeout(sqlite3 *_db, int _ms);

int sqlite3_set_authorizer(sqlite3 *_db, int (*_callback)(void), void *_context);

void *sqlite3_context_db_handle(void *_context);

int sqlite3_prepare_v2(sqlite3 *db, const char *sql, int _len, sqlite3_stmt **out_stmt, const char **_tail);

int sqlite3_finalize(sqlite3_stmt *stmt);

int sqlite3_step(sqlite3_stmt *stmt);

int sqlite3_exec(sqlite3 *db, const char *sql, exec_callback _callback, void *_context, char **_err);

int sqlite3_reset(sqlite3_stmt *stmt);

int sqlite3_changes(sqlite3 *_db);

int64_t sqlite3_changes64(sqlite3 *_db);

int sqlite3_stmt_readonly(sqlite3_stmt *_stmt);

int sqlite3_stmt_busy(sqlite3_stmt *_stmt);

sqlite3_stmt *sqlite3_next_stmt(sqlite3 *db, sqlite3_stmt *stmt);

int sqlite3_serialize(sqlite3 *_db, const char *_schema, void **_out, int *_out_bytes, unsigned int _flags);

int sqlite3_deserialize(sqlite3 *_db, const char *_schema, const void *_in_, int _in_bytes, unsigned int _flags);

int sqlite3_get_autocommit(sqlite3 *_db);

int sqlite3_total_changes(sqlite3 *_db);

int64_t sqlite3_last_insert_rowid(sqlite3 *_db);

void sqlite3_interrupt(sqlite3 *_db);

int sqlite3_db_config(sqlite3 *_db, int _op);

sqlite3 *sqlite3_db_handle(sqlite3_stmt *_stmt);

void sqlite3_sleep(int _ms);

int sqlite3_limit(sqlite3 *_db, int _id, int _new_value);

void *sqlite3_malloc64(int _n);

void sqlite3_free(void *_ptr);

int sqlite3_errcode(sqlite3 *_db);

const char *sqlite3_errstr(int _err);

void *sqlite3_user_data(void *_context);

void *sqlite3_backup_init(sqlite3 *_dest_db, const char *_dest_name, sqlite3 *_source_db, const char *_source_name);

int sqlite3_backup_step(void *_backup, int _n_pages);

int sqlite3_backup_remaining(void *_backup);

int sqlite3_backup_pagecount(void *_backup);

int sqlite3_backup_finish(void *_backup);

char *sqlite3_expanded_sql(sqlite3_stmt *_stmt);

int sqlite3_data_count(sqlite3_stmt *stmt);

int sqlite3_bind_parameter_count(sqlite3_stmt *_stmt);

const char *sqlite3_bind_parameter_name(sqlite3_stmt *_stmt, int _idx);

int sqlite3_bind_parameter_index(sqlite3_stmt *_stmt, const char *_name);

int sqlite3_bind_null(sqlite3_stmt *_stmt, int _idx);

int sqlite3_bind_int64(sqlite3_stmt *_stmt, int _idx, int64_t _val);

int sqlite3_bind_double(sqlite3_stmt *_stmt, int _idx, double _val);

int sqlite3_bind_text(sqlite3_stmt *_stmt, int _idx, const char *_text, int _len, void *_destroy);

int sqlite3_bind_blob(sqlite3_stmt *_stmt, int _idx, const void *_blob, int _len, void *_destroy);

int sqlite3_column_type(sqlite3_stmt *_stmt, int _idx);

int sqlite3_column_count(sqlite3_stmt *_stmt);

const char *sqlite3_column_decltype(sqlite3_stmt *_stmt, int _idx);

const char *sqlite3_column_name(sqlite3_stmt *_stmt, int _idx);

const char *sqlite3_column_table_name(sqlite3_stmt *_stmt, int _idx);

int64_t sqlite3_column_int64(sqlite3_stmt *_stmt, int _idx);

double sqlite3_column_double(sqlite3_stmt *_stmt, int _idx);

const void *sqlite3_column_blob(sqlite3_stmt *_stmt, int _idx);

int sqlite3_column_bytes(sqlite3_stmt *_stmt, int _idx);

int sqlite3_value_type(void *value);

int64_t sqlite3_value_int64(void *value);

double sqlite3_value_double(void *value);

const unsigned char *sqlite3_value_text(void *value);

const void *sqlite3_value_blob(void *value);

int sqlite3_value_bytes(void *value);

const unsigned char *sqlite3_column_text(sqlite3_stmt *stmt, int idx);

int sqlite_get_table_cb(void *context, int n_column, char **argv, char **colv);

int sqlite3_get_table(sqlite3 *db, const char *sql, char ***paz_result, int *pn_row, int *pn_column, char **pz_err_msg);

void sqlite3_free_table(char ***paz_result);

void sqlite3_result_null(void *_context);

void sqlite3_result_int64(void *_context, int64_t _val);

void sqlite3_result_double(void *_context, double _val);

void sqlite3_result_text(void *_context, const char *_text, int _len, void *_destroy);

void sqlite3_result_blob(void *_context, const void *_blob, int _len, void *_destroy);

void sqlite3_result_error_nomem(void *_context);

void sqlite3_result_error_toobig(void *_context);

void sqlite3_result_error(void *_context, const char *_err, int _len);

void *sqlite3_aggregate_context(void *_context, int _n);

int sqlite3_blob_open(sqlite3 *_db,
                      const char *_db_name,
                      const char *_table_name,
                      const char *_column_name,
                      int64_t _rowid,
                      int _flags,
                      void **_blob_out);

int sqlite3_blob_read(void *_blob, void *_data, int _n, int _offset);

int sqlite3_blob_write(void *_blob, const void *_data, int _n, int _offset);

int sqlite3_blob_bytes(void *_blob);

int sqlite3_blob_close(void *_blob);

int sqlite3_stricmp(const char *_a, const char *_b);

int sqlite3_create_collation_v2(sqlite3 *_db,
                                const char *_name,
                                int _enc,
                                void *_context,
                                int (*_cmp)(void),
                                void (*_destroy)(void));

int sqlite3_create_function_v2(sqlite3 *_db,
                               const char *_name,
                               int _n_args,
                               int _enc,
                               void *_context,
                               void (*_func)(void),
                               void (*_step)(void),
                               void (*_final_)(void),
                               void (*_destroy)(void));

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

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* LIMBO_SQLITE3_H */
