/*
** Variadic C entry points that cannot be written in stable Rust (C variadics
** are a nightly-only Rust feature, and on some ABIs — e.g. Apple arm64 —
** variadic arguments are passed differently from named arguments, so the
** entry point must genuinely be variadic).
**
** Each function here decodes its va_list and forwards to a non-variadic
** `turso_*` function exported from the Rust side of the crate. The public
** sqlite3_* symbol itself is a naked-function trampoline in lib.rs that
** tail-jumps here: rustc only exports Rust items from cdylibs, so the
** exported symbol must be a Rust item, and a tail jump preserves the
** variadic call frame intact.
*/
#include <stdarg.h>
#include <stddef.h>

typedef struct sqlite3 sqlite3;

#define SQLITE_ERROR 1

extern int turso_db_config_int(sqlite3 *db, int op, int value, int *p_out);

/*
** sqlite3_db_config() argument shapes, by op:
**   SQLITE_DBCONFIG_MAINDBNAME (1000)       takes (const char*)
**   SQLITE_DBCONFIG_LOOKASIDE  (1001)       takes (void*, int, int)
**   the boolean ops (1002..1017)            take (int, int*)
** The va_list is decoded only for ops known to use the (int, int*) shape;
** any other op — including ops newer than this header — is rejected without
** reading the va_list, since decoding an unknown shape is undefined
** behavior when the caller passed something else.
*/
#define DBCONFIG_BOOL_FIRST 1002 /* SQLITE_DBCONFIG_ENABLE_FKEY */
#define DBCONFIG_BOOL_LAST  1017 /* SQLITE_DBCONFIG_TRUSTED_SCHEMA */

int turso_sqlite3_db_config_va(sqlite3 *db, int op, ...) {
    va_list ap;
    int value;
    int *p_out;

    if (op < DBCONFIG_BOOL_FIRST || op > DBCONFIG_BOOL_LAST) {
        return SQLITE_ERROR;
    }
    va_start(ap, op);
    value = va_arg(ap, int);
    p_out = va_arg(ap, int *);
    va_end(ap);
    return turso_db_config_int(db, op, value, p_out);
}
