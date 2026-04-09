/*
 * turso_tcl.c — Native Tcl extension for Turso/Limbo database.
 *
 * Provides the `sqlite3` Tcl command that creates in-process database
 * connections, replacing the subprocess-based shim in testing/conformance/sqlite3/tester.tcl.
 *
 * Supported db sub-commands:
 *   eval SQL ?array? ?script?   — execute SQL, return results as list
 *   one  SQL                    — return first column of first row
 *   exists SQL                  — return 1 if query returns any row
 *   changes                     — rows affected by last DML
 *   total_changes               — total rows changed since open
 *   last_insert_rowid           — rowid of last INSERT
 *   errorcode                   — most recent error code
 *   errmsg                      — most recent error message
 *   null ?value?                — get/set NULL representation string
 *   func name ?arg...? body     — register a Tcl-backed scalar SQL function
 *   close                       — close database and delete command
 *   limit ...                   — stub returning a default value
 */

#include <tcl.h>
#include <sqlite3.h>
#include <string.h>
#include <stdlib.h>

/* Tcl_Size was introduced in Tcl 9.0; fall back to int for 8.x */
#ifndef TCL_SIZE_MAX
typedef int Tcl_Size;
#endif

#define TURSO_TCL_VERSION "1.0"
#define MAX_FUNC_ARGS 64
#define STMT_CACHE_SIZE 32

/* ------------------------------------------------------------------ */
/* TursoDb — state for a single open database connection               */
/* ------------------------------------------------------------------ */
typedef struct CachedStmt {
    char         *sql;     /* SQL text (cache key) */
    sqlite3_stmt *stmt;    /* prepared statement */
} CachedStmt;

typedef struct TursoDb {
    sqlite3    *db;
    Tcl_Interp *interp;
    Tcl_Obj    *null_obj;   /* replacement string for NULL values */
    CachedStmt  stmt_cache[STMT_CACHE_SIZE];
    int         cache_count;
} TursoDb;

/* ------------------------------------------------------------------ */
/* TclFuncData — state for a Tcl-backed scalar SQL function            */
/* ------------------------------------------------------------------ */
typedef struct TclFuncData {
    Tcl_Interp *interp;
    Tcl_Obj    *script;                    /* function body */
    int         n_args;
    Tcl_Obj    *arg_names[MAX_FUNC_ARGS];  /* argument variable names */
} TclFuncData;

/* ------------------------------------------------------------------ */
/* Value helpers                                                        */
/* ------------------------------------------------------------------ */

/* Convert a column value to a Tcl_Obj. */
static Tcl_Obj *column_to_obj(sqlite3_stmt *stmt, int i, const char *null_str)
{
    int ctype = sqlite3_column_type(stmt, i);
    switch (ctype) {
    case SQLITE_INTEGER:
        return Tcl_NewWideIntObj((Tcl_WideInt)sqlite3_column_int64(stmt, i));
    case SQLITE_FLOAT:
        return Tcl_NewDoubleObj(sqlite3_column_double(stmt, i));
    case SQLITE_TEXT: {
        const char *text = (const char *)sqlite3_column_text(stmt, i);
        return Tcl_NewStringObj(text ? text : "", -1);
    }
    case SQLITE_BLOB: {
        const void *blob = sqlite3_column_blob(stmt, i);
        int nbytes = sqlite3_column_bytes(stmt, i);
        return Tcl_NewByteArrayObj((const unsigned char *)blob, nbytes);
    }
    default: /* NULL */
        return Tcl_NewStringObj(null_str ? null_str : "", -1);
    }
}

/* Convert a function argument (sqlite3_value*) to a Tcl_Obj. */
static Tcl_Obj *value_to_obj(void *argv_i)
{
    int vtype = sqlite3_value_type(argv_i);
    switch (vtype) {
    case SQLITE_INTEGER:
        return Tcl_NewWideIntObj((Tcl_WideInt)sqlite3_value_int64(argv_i));
    case SQLITE_FLOAT:
        return Tcl_NewDoubleObj(sqlite3_value_double(argv_i));
    case SQLITE_TEXT: {
        const char *text = (const char *)sqlite3_value_text(argv_i);
        return Tcl_NewStringObj(text ? text : "", -1);
    }
    case SQLITE_BLOB: {
        const void *blob = sqlite3_value_blob(argv_i);
        int nbytes = sqlite3_value_bytes(argv_i);
        return Tcl_NewByteArrayObj((const unsigned char *)blob, nbytes);
    }
    default: /* NULL */
        return Tcl_NewStringObj("", 0);
    }
}

/* ------------------------------------------------------------------ */
/* Prepared statement cache                                            */
/* ------------------------------------------------------------------ */

static sqlite3_stmt *cache_find(TursoDb *tdb, const char *sql)
{
    int i;
    for (i = 0; i < tdb->cache_count; i++) {
        if (strcmp(tdb->stmt_cache[i].sql, sql) == 0) {
            sqlite3_stmt *stmt = tdb->stmt_cache[i].stmt;
            sqlite3_reset(stmt);
            return stmt;
        }
    }
    return NULL;
}

static void cache_store(TursoDb *tdb, const char *sql, sqlite3_stmt *stmt)
{
    if (tdb->cache_count >= STMT_CACHE_SIZE) return;
    CachedStmt *cs = &tdb->stmt_cache[tdb->cache_count++];
    cs->sql = strdup(sql);
    cs->stmt = stmt;
}

static void cache_free(TursoDb *tdb)
{
    int i;
    for (i = 0; i < tdb->cache_count; i++) {
        sqlite3_finalize(tdb->stmt_cache[i].stmt);
        free(tdb->stmt_cache[i].sql);
    }
    tdb->cache_count = 0;
}

/* ------------------------------------------------------------------ */
/* TCL variable binding                                                */
/* ------------------------------------------------------------------ */

static void bind_tcl_variables(Tcl_Interp *interp, sqlite3_stmt *stmt)
{
    int nparams = sqlite3_bind_parameter_count(stmt);
    int i;
    for (i = 1; i <= nparams; i++) {
        const char *name = sqlite3_bind_parameter_name(stmt, i);
        if (!name) continue;

        /* Skip the leading $ : or @ */
        const char *varname = name;
        if (varname[0] == '$' || varname[0] == ':' || varname[0] == '@') {
            varname++;
        }

        Tcl_Obj *val = Tcl_GetVar2Ex(interp, varname, NULL, 0);
        if (!val) {
            sqlite3_bind_null(stmt, i);
            continue;
        }

        Tcl_WideInt ival;
        double dval;
        if (Tcl_GetWideIntFromObj(NULL, val, &ival) == TCL_OK) {
            sqlite3_bind_int64(stmt, i, (sqlite3_int64)ival);
        } else if (Tcl_GetDoubleFromObj(NULL, val, &dval) == TCL_OK) {
            sqlite3_bind_double(stmt, i, dval);
        } else {
            Tcl_Size len;
            const char *str = Tcl_GetStringFromObj(val, &len);
            sqlite3_bind_text(stmt, i, str, (int)len, SQLITE_TRANSIENT);
        }
    }
}

/* ------------------------------------------------------------------ */
/* Tcl scalar function bridge                                           */
/* ------------------------------------------------------------------ */

static void tcl_scalar_bridge(void *ctx, int argc, void **argv)
{
    TclFuncData *func = (TclFuncData *)sqlite3_user_data(ctx);
    Tcl_Interp  *interp = func->interp;
    int          i, rc;

    /* Bind argument variables in the calling scope. */
    for (i = 0; i < argc && i < func->n_args; i++) {
        Tcl_Obj *val = value_to_obj(argv[i]);
        if (Tcl_ObjSetVar2(interp, func->arg_names[i], NULL, val,
                           TCL_LEAVE_ERR_MSG) == NULL) {
            sqlite3_result_error(ctx, Tcl_GetString(Tcl_GetObjResult(interp)), -1);
            return;
        }
    }

    /* Evaluate the script body. */
    rc = Tcl_EvalObjEx(interp, func->script, 0);

    if (rc == TCL_ERROR) {
        const char *err = Tcl_GetString(Tcl_GetObjResult(interp));
        sqlite3_result_error(ctx, err, -1);
        return;
    }

    /* Convert the Tcl result to an SQL value. */
    Tcl_Obj    *result = Tcl_GetObjResult(interp);
    Tcl_WideInt ival;
    double      dval;

    if (Tcl_GetWideIntFromObj(NULL, result, &ival) == TCL_OK) {
        sqlite3_result_int64(ctx, (int64_t)ival);
    } else if (Tcl_GetDoubleFromObj(NULL, result, &dval) == TCL_OK) {
        sqlite3_result_double(ctx, dval);
    } else {
        Tcl_Size    slen;
        const char *str = Tcl_GetStringFromObj(result, &slen);
        sqlite3_result_text(ctx, str, slen, SQLITE_TRANSIENT);
    }
}

static void tcl_func_destroy(void *pApp)
{
    TclFuncData *func = (TclFuncData *)pApp;
    int i;
    if (!func) return;
    if (func->script) Tcl_DecrRefCount(func->script);
    for (i = 0; i < func->n_args; i++) {
        if (func->arg_names[i]) Tcl_DecrRefCount(func->arg_names[i]);
    }
    Tcl_Free((char *)func);
}

/* ------------------------------------------------------------------ */
/* Multi-statement SQL execution helpers                                */
/* ------------------------------------------------------------------ */

/*
 * Execute all statements in `sql`, collecting result rows from the last
 * statement that returns rows into `result_list`.
 * Returns TCL_OK or TCL_ERROR; sets the interpreter result on error.
 *
 * Uses prepared statement caching: single-statement SQL with bind parameters
 * (e.g. $varname) is cached and reused on subsequent calls.  TCL variables
 * referenced by parameter names are automatically bound.
 */
static int exec_sql_collect(TursoDb *tdb,
                             const char *sql, const char *null_str,
                             Tcl_Obj **result_list_out)
{
    Tcl_Interp *interp = tdb->interp;
    sqlite3    *db     = tdb->db;

    /* Fast path: check if this exact SQL string has a cached statement */
    sqlite3_stmt *cached_stmt = cache_find(tdb, sql);
    if (cached_stmt) {
        bind_tcl_variables(interp, cached_stmt);

        Tcl_Obj *result_list = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(result_list);
        int ncols = sqlite3_column_count(cached_stmt);
        int rc;

        while ((rc = sqlite3_step(cached_stmt)) == SQLITE_ROW) {
            int i;
            for (i = 0; i < ncols; i++) {
                Tcl_Obj *val = column_to_obj(cached_stmt, i, null_str);
                Tcl_ListObjAppendElement(interp, result_list, val);
            }
        }

        if (rc != SQLITE_DONE) {
            Tcl_DecrRefCount(result_list);
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(db), TCL_VOLATILE);
            return TCL_ERROR;
        }

        *result_list_out = result_list;
        return TCL_OK;
    }

    /* Regular multi-statement path */
    Tcl_Obj    *result_list = Tcl_NewListObj(0, NULL);
    Tcl_IncrRefCount(result_list);
    const char *remaining   = sql;
    int         rc;

    while (remaining && *remaining) {
        /* skip leading whitespace and bare semicolons */
        while (*remaining == ' ' || *remaining == '\n' ||
               *remaining == '\t' || *remaining == '\r' ||
               *remaining == ';') {
            remaining++;
        }
        if (!*remaining) break;

        sqlite3_stmt *stmt = NULL;
        const char   *tail = NULL;

        rc = sqlite3_prepare_v2(db, remaining, -1, &stmt, &tail);
        if (rc != SQLITE_OK) {
            Tcl_DecrRefCount(result_list);
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(db), TCL_VOLATILE);
            return TCL_ERROR;
        }
        if (!stmt) {
            /* empty / comment-only statement */
            remaining = tail;
            continue;
        }

        /* Bind TCL variables to any parameters */
        bind_tcl_variables(interp, stmt);

        /* reset the list for each non-empty statement so the caller
           sees the results of the final one (matches SQLite tclsqlite behaviour) */
        Tcl_DecrRefCount(result_list);
        result_list = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(result_list);

        int ncols = sqlite3_column_count(stmt);

        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            int i;
            for (i = 0; i < ncols; i++) {
                Tcl_Obj *val = column_to_obj(stmt, i, null_str);
                Tcl_ListObjAppendElement(interp, result_list, val);
            }
        }

        if (rc != SQLITE_DONE) {
            sqlite3_finalize(stmt);
            Tcl_DecrRefCount(result_list);
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(db), TCL_VOLATILE);
            return TCL_ERROR;
        }

        /* Cache single-statement SQL with bind parameters */
        if (sqlite3_bind_parameter_count(stmt) > 0) {
            /* Check if tail is empty (single statement) */
            const char *p = tail;
            if (p) {
                while (*p == ' ' || *p == '\n' || *p == '\t' ||
                       *p == '\r' || *p == ';') {
                    p++;
                }
            }
            if (!p || !*p) {
                cache_store(tdb, sql, stmt);
            } else {
                sqlite3_finalize(stmt);
            }
        } else {
            sqlite3_finalize(stmt);
        }

        remaining = tail;
    }

    *result_list_out = result_list;
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* db command dispatcher                                                */
/* ------------------------------------------------------------------ */

static void TursoDbFree(ClientData cd)
{
    TursoDb *tdb = (TursoDb *)cd;
    if (!tdb) return;
    cache_free(tdb);
    if (tdb->db)       sqlite3_close(tdb->db);
    if (tdb->null_obj) Tcl_DecrRefCount(tdb->null_obj);
    Tcl_Free((char *)tdb);
}

static int TursoDbCmd(ClientData cd, Tcl_Interp *interp,
                      int objc, Tcl_Obj *const objv[])
{
    TursoDb    *tdb = (TursoDb *)cd;
    static const char *cmds[] = {
        "eval", "one", "exists", "changes", "total_changes",
        "last_insert_rowid", "errorcode", "errmsg", "null",
        "func", "function", "close", "limit",
        NULL
    };
    enum {
        CMD_EVAL, CMD_ONE, CMD_EXISTS, CMD_CHANGES, CMD_TOTAL_CHANGES,
        CMD_LAST_INSERT_ROWID, CMD_ERRORCODE, CMD_ERRMSG, CMD_NULL,
        CMD_FUNC, CMD_FUNCTION, CMD_CLOSE, CMD_LIMIT
    };
    int cmdIdx;

    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj(interp, objv[1], cmds, "subcommand", 0,
                            &cmdIdx) != TCL_OK) {
        return TCL_ERROR;
    }

    switch (cmdIdx) {

    /* ---- simple counters / metadata ---- */

    case CMD_CHANGES:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_changes(tdb->db)));
        return TCL_OK;

    case CMD_TOTAL_CHANGES:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_total_changes(tdb->db)));
        return TCL_OK;

    case CMD_LAST_INSERT_ROWID:
        Tcl_SetObjResult(interp,
            Tcl_NewWideIntObj((Tcl_WideInt)sqlite3_last_insert_rowid(tdb->db)));
        return TCL_OK;

    case CMD_ERRORCODE:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_errcode(tdb->db)));
        return TCL_OK;

    case CMD_ERRMSG:
        Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
        return TCL_OK;

    /* ---- null value string ---- */

    case CMD_NULL:
        if (objc == 3) {
            if (tdb->null_obj) Tcl_DecrRefCount(tdb->null_obj);
            tdb->null_obj = objv[2];
            Tcl_IncrRefCount(tdb->null_obj);
        }
        Tcl_SetObjResult(interp,
            tdb->null_obj ? tdb->null_obj : Tcl_NewStringObj("", 0));
        return TCL_OK;

    /* ---- close ---- */

    case CMD_CLOSE:
        Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
        return TCL_OK;

    /* ---- limit (stub) ---- */

    case CMD_LIMIT:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(1000000));
        return TCL_OK;

    /* ---- eval ---- */

    case CMD_EVAL: {
        if (objc < 3 || objc > 5) {
            Tcl_WrongNumArgs(interp, 2, objv, "sql ?array? ?script?");
            return TCL_ERROR;
        }

        const char *sql      = Tcl_GetString(objv[2]);
        const char *null_str = tdb->null_obj
                               ? Tcl_GetString(tdb->null_obj) : "";

        /* db eval sql — collect all result values into a flat list */
        if (objc == 3) {
            Tcl_Obj *result_list = NULL;
            int rc = exec_sql_collect(tdb, sql, null_str,
                                      &result_list);
            if (rc != TCL_OK) return rc;
            Tcl_SetObjResult(interp, result_list);
            Tcl_DecrRefCount(result_list);
            return TCL_OK;
        }

        /* db eval sql array script — per-row callback */
        if (objc == 5) {
            Tcl_Obj *array_name = objv[3];
            Tcl_Obj *script     = objv[4];

            const char   *remaining = sql;
            int           loop_rc   = TCL_OK;

            while (remaining && *remaining) {
                while (*remaining == ' ' || *remaining == '\n' ||
                       *remaining == '\t' || *remaining == '\r' ||
                       *remaining == ';') {
                    remaining++;
                }
                if (!*remaining) break;

                sqlite3_stmt *stmt = NULL;
                const char   *tail = NULL;

                int rc = sqlite3_prepare_v2(tdb->db, remaining, -1, &stmt, &tail);
                if (rc != SQLITE_OK) {
                    Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db),
                                  TCL_VOLATILE);
                    return TCL_ERROR;
                }
                if (!stmt) { remaining = tail; continue; }

                /* Bind TCL variables to any parameters */
                bind_tcl_variables(interp, stmt);

                int ncols = sqlite3_column_count(stmt);

                /* Set array(*) to the list of column names. */
                Tcl_Obj *col_list = Tcl_NewListObj(0, NULL);
                int i;
                for (i = 0; i < ncols; i++) {
                    const char *col = sqlite3_column_name(stmt, i);
                    Tcl_ListObjAppendElement(interp, col_list,
                        Tcl_NewStringObj(col ? col : "", -1));
                }
                Tcl_ObjSetVar2(interp, array_name,
                               Tcl_NewStringObj("*", 1), col_list, 0);

                while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                    for (i = 0; i < ncols; i++) {
                        const char *col = sqlite3_column_name(stmt, i);
                        Tcl_Obj *val = column_to_obj(stmt, i, null_str);
                        Tcl_ObjSetVar2(interp, array_name,
                                       Tcl_NewStringObj(col ? col : "", -1),
                                       val, 0);
                    }

                    loop_rc = Tcl_EvalObjEx(interp, script, 0);
                    if (loop_rc == TCL_BREAK) {
                        loop_rc = TCL_OK;
                        break;
                    } else if (loop_rc == TCL_CONTINUE) {
                        loop_rc = TCL_OK;
                    } else if (loop_rc != TCL_OK) {
                        break;
                    }
                }

                sqlite3_finalize(stmt);

                if (loop_rc != TCL_OK) return loop_rc;

                if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
                    Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db),
                                  TCL_VOLATILE);
                    return TCL_ERROR;
                }

                remaining = tail;
            }

            Tcl_ResetResult(interp);
            return TCL_OK;
        }

        /* objc == 4: not a standard form we support */
        Tcl_WrongNumArgs(interp, 2, objv, "sql ?array script?");
        return TCL_ERROR;
    }

    /* ---- one ---- */

    case CMD_ONE: {
        if (objc != 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "sql");
            return TCL_ERROR;
        }
        const char *sql      = Tcl_GetString(objv[2]);
        const char *null_str = tdb->null_obj
                               ? Tcl_GetString(tdb->null_obj) : "";

        sqlite3_stmt *stmt = NULL;
        int rc = sqlite3_prepare_v2(tdb->db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
            return TCL_ERROR;
        }

        Tcl_Obj *result = Tcl_NewStringObj(null_str, -1);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            result = column_to_obj(stmt, 0, null_str);
        }
        sqlite3_finalize(stmt);
        Tcl_SetObjResult(interp, result);
        return TCL_OK;
    }

    /* ---- exists ---- */

    case CMD_EXISTS: {
        if (objc != 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "sql");
            return TCL_ERROR;
        }
        const char *sql = Tcl_GetString(objv[2]);

        sqlite3_stmt *stmt = NULL;
        int rc = sqlite3_prepare_v2(tdb->db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
            return TCL_ERROR;
        }
        int exists = (sqlite3_step(stmt) == SQLITE_ROW) ? 1 : 0;
        sqlite3_finalize(stmt);
        Tcl_SetObjResult(interp, Tcl_NewBooleanObj(exists));
        return TCL_OK;
    }

    /* ---- func / function ---- */

    case CMD_FUNC:
    case CMD_FUNCTION: {
        /*
         * db func name ?arglist? body
         * db function name ?arglist? body
         *
         * Registers a Tcl proc body as a scalar SQL function.  The arglist
         * mirrors proc syntax: it may be a single Tcl list object ({a b}) or
         * multiple individual words (a b) — both result in named variables
         * being bound before the body is evaluated.
         *
         *   objv[2]         = function name
         *   objv[3..objc-2] = argument variable names, OR a single Tcl list
         *   objv[objc-1]    = script body
         */
        if (objc < 4) {
            Tcl_WrongNumArgs(interp, 2, objv, "name ?arglist? body");
            return TCL_ERROR;
        }

        const char *func_name = Tcl_GetString(objv[2]);
        Tcl_Obj    *body      = objv[objc - 1];
        int         i;

        /* Resolve the argument variable names.
         *
         * objc == 4: db func name body          → no named args
         * objc == 5: db func name argspec body  → argspec is a Tcl list
         * objc >= 6: db func name a b … body    → each word is a name
         */
        Tcl_Size    n_args   = 0;
        Tcl_Obj   **arg_objs = NULL;

        if (objc == 5) {
            /* Single argspec object — split it as a Tcl list so that both
             * `db func f x body` and `db func f {x y} body` work. */
            if (Tcl_ListObjGetElements(interp, objv[3],
                                       &n_args, &arg_objs) != TCL_OK) {
                return TCL_ERROR;
            }
        } else if (objc > 5) {
            n_args   = objc - 4;
            arg_objs = (Tcl_Obj **)&objv[3];
        }

        TclFuncData *func_data =
            (TclFuncData *)Tcl_Alloc(sizeof(TclFuncData));
        memset(func_data, 0, sizeof(TclFuncData));
        func_data->interp  = interp;
        func_data->script  = body;
        Tcl_IncrRefCount(body);
        func_data->n_args  = (n_args < MAX_FUNC_ARGS) ? n_args : MAX_FUNC_ARGS;

        for (i = 0; i < func_data->n_args; i++) {
            func_data->arg_names[i] = arg_objs[i];
            Tcl_IncrRefCount(func_data->arg_names[i]);
        }

        int sql_n_args = (n_args == 0) ? -1 : n_args;
        int rc = sqlite3_create_function_v2(
            tdb->db,
            func_name,
            sql_n_args,
            0, /* SQLITE_UTF8 */
            (void *)func_data,
            (void (*)(void))tcl_scalar_bridge,
            NULL, NULL,
            (void (*)(void))tcl_func_destroy
        );

        if (rc != SQLITE_OK) {
            tcl_func_destroy(func_data);
            Tcl_SetResult(interp,
                (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
            return TCL_ERROR;
        }
        return TCL_OK;
    }

    } /* switch */

    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* sqlite3 open command                                                 */
/* ------------------------------------------------------------------ */

static int TursoOpenCmd(ClientData cd, Tcl_Interp *interp,
                        int objc, Tcl_Obj *const objv[])
{
    (void)cd;

    if (objc < 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "name filename ?options?");
        return TCL_ERROR;
    }

    const char *handle_name = Tcl_GetString(objv[1]);
    const char *filename    = Tcl_GetString(objv[2]);

    sqlite3 *db  = NULL;
    int      rc  = sqlite3_open(filename, &db);

    if (rc != SQLITE_OK) {
        const char *errmsg = db ? sqlite3_errmsg(db) : "out of memory";
        Tcl_SetResult(interp, (char *)errmsg, TCL_VOLATILE);
        if (db) sqlite3_close(db);
        return TCL_ERROR;
    }

    TursoDb *tdb = (TursoDb *)Tcl_Alloc(sizeof(TursoDb));
    tdb->db          = db;
    tdb->interp      = interp;
    tdb->null_obj    = NULL;
    tdb->cache_count = 0;

    Tcl_CreateObjCommand(interp, handle_name, TursoDbCmd,
                         (ClientData)tdb, TursoDbFree);
    Tcl_SetResult(interp, (char *)handle_name, TCL_VOLATILE);
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* Extension initialisation                                             */
/* ------------------------------------------------------------------ */

int Tursotcl_Init(Tcl_Interp *interp)
{
    if (Tcl_InitStubs(interp, TCL_VERSION, 0) == NULL) {
        return TCL_ERROR;
    }

    turso_enable_experimental();

    Tcl_CreateObjCommand(interp, "sqlite3", TursoOpenCmd, NULL, NULL);

    Tcl_PkgProvide(interp, "tursotcl", TURSO_TCL_VERSION);
    return TCL_OK;
}
