/*
** This extension mimics Limbo's kv_store and is used in tests to verify
** compatibility between Limbo and SQLite extension handling.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *comment;
    char *key;
    char *value;
    sqlite3_int64 rowid;
} kv_row;

typedef struct {
    sqlite3_vtab base;
    kv_row *rows;
    int row_count;
    sqlite3_int64 next_rowid;
} kv_table;

typedef struct {
    sqlite3_vtab_cursor base;
    int current;
    int last;
    kv_table *table;
} kv_cursor;

static int kvstoreConnect(
    sqlite3 *db,
    void *pAux,
    int argc, const char *const *argv,
    sqlite3_vtab **ppVtab,
    char **pzErr
) {
    kv_table *pNew;
    int rc;

    rc = sqlite3_declare_vtab(db,
        "CREATE TABLE x("
        "  comment TEXT HIDDEN NOT NULL DEFAULT 'default comment',"
        "  key TEXT PRIMARY KEY,"
        "  value TEXT"
        ")");

    if (rc == SQLITE_OK) {
        pNew = sqlite3_malloc(sizeof(*pNew));
        *ppVtab = (sqlite3_vtab *)pNew;
        if (pNew == 0) {
            return SQLITE_NOMEM;
        }
        memset(pNew, 0, sizeof(*pNew));
    }
    return rc;
}

static int kvstoreDisconnect(sqlite3_vtab *pVtab) {
    kv_table *table = (kv_table *)pVtab;
    for (int i = 0; i < table->row_count; i++) {
        sqlite3_free(table->rows[i].comment);
        sqlite3_free(table->rows[i].key);
        sqlite3_free(table->rows[i].value);
    }
    sqlite3_free(table->rows);
    sqlite3_free(table);
    return SQLITE_OK;
}

static int kvstoreOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
    kv_cursor *cursor = sqlite3_malloc(sizeof(kv_cursor));
    memset(cursor, 0, sizeof(kv_cursor));
    cursor->table = (kv_table *)p;
    *ppCursor = (sqlite3_vtab_cursor *)cursor;
    return SQLITE_OK;
}

static int kvstoreClose(sqlite3_vtab_cursor *cur) {
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int kvstoreFilter(
    sqlite3_vtab_cursor *pVtabCursor,
    int idxNum, const char *idxStr,
    int argc, sqlite3_value **argv
) {
    kv_cursor *cursor = (kv_cursor *)pVtabCursor;
    kv_table *table = cursor->table;

    // If filtering by key, only return the matching row (or EOF)
    if (idxNum == 1 && idxStr && strcmp(idxStr, "key_eq") == 0 && argc >= 1) {
        const char *key = (const char *)sqlite3_value_text(argv[0]);
        int found = 0;
        for (int i = 0; i < table->row_count; i++) {
            if (strcmp(table->rows[i].key, key) == 0) {
                cursor->current = i;
                cursor->last = i + 1; // Only this row
                found = 1;
                break;
            }
        }
        if (!found) {
            // EOF
            cursor->current = table->row_count;
            cursor->last = table->row_count;
        }
    } else {
        // full scan
        cursor->current = 0;
        cursor->last = table->row_count;
    }
    return SQLITE_OK;
}

static int kvstoreNext(sqlite3_vtab_cursor *cur) {
    kv_cursor *pCur = (kv_cursor *)cur;
    pCur->current++;
    return SQLITE_OK;
}

static int kvstoreEof(sqlite3_vtab_cursor *cur) {
    kv_cursor *cursor = (kv_cursor *)cur;
    return cursor->current >= cursor->last;
}

static int kvstoreColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col) {
    kv_cursor *cursor = (kv_cursor *)cur;
    kv_row *row = &cursor->table->rows[cursor->current];
    switch (col) {
        case 0:
            sqlite3_result_text(ctx, row->comment, -1, SQLITE_TRANSIENT);
            break;
        case 1:
            sqlite3_result_text(ctx, row->key, -1, SQLITE_TRANSIENT);
            break;
        case 2:
            sqlite3_result_text(ctx, row->value, -1, SQLITE_TRANSIENT);
            break;
    }
    return SQLITE_OK;
}

static int kvstoreRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid) {
    kv_cursor *cursor = (kv_cursor *)cur;
    *pRowid = cursor->table->rows[cursor->current].rowid;
    return SQLITE_OK;
}

static int kvstoreCreate(
    sqlite3 *db,
    void *pAux,
    int argc, const char *const *argv,
    sqlite3_vtab **ppVtab,
    char **pzErr
) {
    return kvstoreConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

static int kvstoreBestIndex(
    sqlite3_vtab *tab,
    sqlite3_index_info *pIdxInfo) {
    int key_eq_idx = -1;
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        struct sqlite3_index_constraint *c = &pIdxInfo->aConstraint[i];
        if (c->usable && c->op == SQLITE_INDEX_CONSTRAINT_EQ && c->iColumn == 1) {
            key_eq_idx = i;
            break;
        }
    }
    if (key_eq_idx >= 0) {
        pIdxInfo->idxNum = 1;
        pIdxInfo->idxStr = (char *)"key_eq";
        pIdxInfo->orderByConsumed = (pIdxInfo->nOrderBy > 0 &&
                                     pIdxInfo->aOrderBy[0].iColumn == 2 &&
                                     !pIdxInfo->aOrderBy[0].desc) ? 1 : 0;
        pIdxInfo->estimatedCost = 10.0;
        pIdxInfo->estimatedRows = 4;
        for (int i = 0; i < pIdxInfo->nConstraint; i++) {
            pIdxInfo->aConstraintUsage[i].omit = (i == key_eq_idx) ? 1 : 0;
            pIdxInfo->aConstraintUsage[i].argvIndex = (i == key_eq_idx) ? 1 : 0;
        }
    } else {
        pIdxInfo->idxNum = -1;
        pIdxInfo->idxStr = NULL;
        pIdxInfo->orderByConsumed = 0;
        pIdxInfo->estimatedCost = 1000.0;
        pIdxInfo->estimatedRows = 1000;
        for (int i = 0; i < pIdxInfo->nConstraint; i++) {
            pIdxInfo->aConstraintUsage[i].omit = 0;
            pIdxInfo->aConstraintUsage[i].argvIndex = 0;
        }
    }
    return SQLITE_OK;
}

static int kvUpsert(
    sqlite3_vtab *pVTab,
    sqlite3_value **argv,
    sqlite3_int64 *pRowid
) {
    kv_table *table = (kv_table *)pVTab;

    const char *comment;
    if (sqlite3_value_type(argv[2]) == SQLITE_NULL) {
        comment = "auto-generated";
    } else {
        comment = (const char *)sqlite3_value_text(argv[2]);
    }
    const char *key = (const char *)sqlite3_value_text(argv[3]);
    const char *value = (const char *)sqlite3_value_text(argv[4]);

    // Check if key exists; if so, replace
    for (int i = 0; i < table->row_count; i++) {
        if (strcmp(table->rows[i].key, key) == 0) {
            sqlite3_free(table->rows[i].comment);
            sqlite3_free(table->rows[i].value);
            table->rows[i].comment = sqlite3_mprintf("%s", comment);
            table->rows[i].value = sqlite3_mprintf("%s", value);
            return SQLITE_OK;
        }
    }

    // Otherwise, insert new
    table->rows = sqlite3_realloc(table->rows, sizeof(kv_row) * (table->row_count + 1));
    kv_row *row = &table->rows[table->row_count++];
    row->comment = sqlite3_mprintf("%s", comment);
    row->key = sqlite3_mprintf("%s", key);
    row->value = sqlite3_mprintf("%s", value);
    row->rowid = table->next_rowid;
    *pRowid = table->next_rowid;
    table->next_rowid++;
    return SQLITE_OK;
}

static int kvDelete(sqlite3_vtab *pVTab, sqlite3_int64 rowid) {
    kv_table *table = (kv_table *)pVTab;

    int idx = -1;
    for (int i = 0; i < table->row_count; i++) {
        if (table->rows[i].rowid == rowid) {
            idx = i;
            break;
        }
    }

    if (idx > -1) {
        sqlite3_free(table->rows[idx].comment);
        sqlite3_free(table->rows[idx].key);
        sqlite3_free(table->rows[idx].value);

        for (int i = idx + 1; i < table->row_count; i++) {
            table->rows[i - 1] = table->rows[i];
        }
        table->row_count--;
        return SQLITE_OK;
    }

    return SQLITE_ERROR;
}

static int kvstoreUpdate(
    sqlite3_vtab *pVTab,
    int argc, sqlite3_value **argv,
    sqlite3_int64 *pRowid
) {
    if (argc == 1) {
        return kvDelete(pVTab, sqlite3_value_int64(argv[0]));
    } else {
        assert(argc == 5);
        return kvUpsert(pVTab, argv, pRowid);
    }
}

static sqlite3_module kvstoreModule = {
    /* iVersion    */ 0,
    /* xCreate     */ kvstoreCreate,
    /* xConnect    */ kvstoreConnect,
    /* xBestIndex  */ kvstoreBestIndex,
    /* xDisconnect */ kvstoreDisconnect,
    /* xDestroy    */ kvstoreDisconnect,
    /* xOpen       */ kvstoreOpen,
    /* xClose      */ kvstoreClose,
    /* xFilter     */ kvstoreFilter,
    /* xNext       */ kvstoreNext,
    /* xEof        */ kvstoreEof,
    /* xColumn     */ kvstoreColumn,
    /* xRowid      */ kvstoreRowid,
    /* xUpdate     */ kvstoreUpdate,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0,
    /* xIntegrity  */ 0
};

#ifdef _WIN32
__declspec(dllexport)
#endif
int
sqlite3_kvstore_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi);
    rc = sqlite3_create_module(db, "kv_store", &kvstoreModule, 0);
    return rc;
}
