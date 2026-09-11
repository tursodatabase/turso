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
#include <stdint.h>
#include <string.h>

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

extern char *turso_printf_va(const char *fmt, void *ap);
extern void sqlite3_free(void *p);

/*
** One-line va_arg pumps. The Rust bridge walks the format string through
** core's specifier grammar (printf_c_arg_plan) and calls back here for
** each argument with the C type that grammar dictates — the only work a
** va_list requires from C.
*/
int turso_va_i32(void *ap) { return va_arg(*(va_list *)ap, int); }
long long turso_va_i64(void *ap) { return va_arg(*(va_list *)ap, long long); }
double turso_va_f64(void *ap) { return va_arg(*(va_list *)ap, double); }
void *turso_va_ptr(void *ap) { return va_arg(*(va_list *)ap, void *); }

char *turso_sqlite3_mprintf_va(const char *fmt, ...) {
    va_list ap;
    char *r;

    va_start(ap, fmt);
    r = turso_printf_va(fmt, &ap);
    va_end(ap);
    return r;
}

char *turso_sqlite3_snprintf_va(int n, char *buf, const char *fmt, ...) {
    va_list ap;
    char *s;
    size_t len;

    if (n <= 0 || !buf) {
        return buf;
    }
    va_start(ap, fmt);
    s = turso_printf_va(fmt, &ap);
    va_end(ap);
    if (!s) {
        buf[0] = 0;
        return buf;
    }
    len = strlen(s);
    if (len >= (size_t)n) {
        len = (size_t)n - 1;
    }
    memcpy(buf, s, len);
    buf[len] = 0;
    sqlite3_free(s);
    return buf;
}
