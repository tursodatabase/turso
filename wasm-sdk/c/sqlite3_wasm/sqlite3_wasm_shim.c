/*
 * sqlite3_wasm_shim.c — Universal shim for loading unmodified SQLite C
 * extensions as Turso WASM UDFs.
 *
 * This file bridges the sqlite3 C API to the Turso WASM ABI. It is compiled
 * together with the unmodified extension source using wasi-sdk:
 *
 *   $WASI_SDK/bin/clang --sysroot=$WASI_SDK/share/wasi-sysroot \
 *     -O2 -I wasm-sdk/c/sqlite3_wasm/ \
 *     -DTURSO_SQLITE3_ENTRY=sqlite3_rot_init \
 *     wasm-sdk/c/sqlite3_wasm/sqlite3_wasm_shim.c \
 *     wasm-sdk/examples/sqlite3/rot13.c \
 *     -o rot13.wasm -Wl,--no-entry \
 *     -Wl,--export=turso_malloc -Wl,--export=turso_ext_init \
 *     -Wl,--export=__turso_call
 *
 * The only per-extension customisation is -DTURSO_SQLITE3_ENTRY=<init_func>
 * and the source file path.
 *
 * Architecture:
 *   LOAD TIME: host calls turso_ext_init() → extension calls sqlite3_create_function()
 *     → shim calls __turso_register_func host import → host captures registrations.
 *   CALL TIME: host calls __turso_call(func_ptr, user_data, argc, argv)
 *     → shim does call_indirect to the extension callback.
 */

/* Define SQLITE_CORE so that sqlite3ext.h gives us the struct definition
 * without activating the sqlite3_api-> redirection macros. */
#define SQLITE_CORE
#include "sqlite3ext.h"    /* Defines sqlite3_api_routines struct */
#include <stdlib.h>        /* musl: malloc, free */
#include <string.h>        /* musl: strlen, memcpy, memset, strncpy */

/* ── Complete opaque type declarations from sqlite3.h ───────────────────── */

struct sqlite3 {
    int unused;
};

struct sqlite3_context {
    int64_t result;        /* Turso ABI return value */
    void   *user_data;     /* pApp from create_function */
};

struct sqlite3_value {
    int64_t raw;           /* Turso ABI i64 from argv slot */
};

/* ── Turso ABI tag constants (must match core/wasm/mod.rs) ──────────────── */

#define TURSO_TAG_INTEGER 0x01
#define TURSO_TAG_REAL    0x02
#define TURSO_TAG_TEXT    0x03
#define TURSO_TAG_BLOB    0x04
#define TURSO_TAG_NULL    0x05

/* ── Bump allocator for Turso ABI marshalling ───────────────────────────── */

static uint32_t __turso_bump_ptr = 1024;

__attribute__((export_name("turso_malloc")))
int32_t turso_malloc(int32_t size) {
    uint32_t ptr = __turso_bump_ptr;
    __turso_bump_ptr += (uint32_t)size;
    return (int32_t)ptr;
}

/* ── Host import for function registration ──────────────────────────────── */

__attribute__((import_module("env"), import_name("__turso_register_func")))
extern void __turso_register_func(
    int32_t name_ptr, int32_t name_len,
    int32_t nArg, int32_t xFunc, int32_t pApp
);

/* ── sqlite3_value_* implementations ────────────────────────────────────── */

/*
 * Determine type from the Turso ABI encoding.
 * TEXT/BLOB/NULL are stored as pointers to [tag][data].
 * INTEGER/REAL are stored as raw i64 values (no pointer).
 *
 * Limitation: the Turso ABI does not tag INTEGER vs REAL arguments,
 * so we cannot distinguish them here. Both return SQLITE_INTEGER.
 * Extensions that need float detection should use sqlite3_value_double().
 */
static int shim_value_type(sqlite3_value *pVal) {
    int64_t raw = pVal->raw;
    uint32_t ptr = (uint32_t)(raw & 0xFFFFFFFF);
    /* A valid Turso pointer has upper 32 bits clear and is >= 1024 (bump start) */
    if ((raw >> 32) == 0 && ptr >= 1024) {
        uint8_t tag = *(const uint8_t *)(uintptr_t)ptr;
        switch (tag) {
            case TURSO_TAG_TEXT: return SQLITE_TEXT;
            case TURSO_TAG_BLOB: return SQLITE_BLOB;
            case TURSO_TAG_NULL: return SQLITE_NULL;
        }
    }
    return SQLITE_INTEGER;
}

static const unsigned char *shim_value_text(sqlite3_value *pVal) {
    uint32_t ptr = (uint32_t)(pVal->raw & 0xFFFFFFFF);
    /* Skip TAG_TEXT byte */
    return (const unsigned char *)(uintptr_t)(ptr + 1);
}

static int shim_value_bytes(sqlite3_value *pVal) {
    uint32_t addr = (uint32_t)(pVal->raw & 0xFFFFFFFF);
    uint8_t tag = *(const uint8_t *)(uintptr_t)addr;
    if (tag == TURSO_TAG_TEXT) {
        return (int)strlen((const char *)(uintptr_t)(addr + 1));
    }
    if (tag == TURSO_TAG_BLOB) {
        uint32_t size;
        memcpy(&size, (const void *)(uintptr_t)(addr + 1), 4);
        return (int)size;
    }
    return 0;
}

static const void *shim_value_blob(sqlite3_value *pVal) {
    uint32_t addr = (uint32_t)(pVal->raw & 0xFFFFFFFF);
    uint8_t tag = *(const uint8_t *)(uintptr_t)addr;
    if (tag == TURSO_TAG_BLOB) {
        return (const void *)(uintptr_t)(addr + 5);
    }
    return NULL;
}

static int shim_value_int(sqlite3_value *pVal) {
    return (int)pVal->raw;
}

static sqlite3_int64 shim_value_int64(sqlite3_value *pVal) {
    return (sqlite3_int64)pVal->raw;
}

static double shim_value_double(sqlite3_value *pVal) {
    double val;
    int64_t raw = pVal->raw;
    memcpy(&val, &raw, sizeof(double));
    return val;
}

/* ── sqlite3_result_* implementations ───────────────────────────────────── */

static void shim_result_text(
    sqlite3_context *ctx, const char *z, int n, void (*xDel)(void *)
) {
    if (z == NULL) {
        int32_t p = turso_malloc(1);
        *(uint8_t *)(uintptr_t)p = TURSO_TAG_NULL;
        ctx->result = (int64_t)p;
        return;
    }
    int len = (n >= 0) ? n : (int)strlen(z);
    int32_t p = turso_malloc(1 + len + 1);
    uint8_t *base = (uint8_t *)(uintptr_t)p;
    base[0] = TURSO_TAG_TEXT;
    memcpy(base + 1, z, (size_t)len);
    base[1 + len] = 0;
    ctx->result = (int64_t)p;
    /* Call destructor if it's a real function (not SQLITE_STATIC / SQLITE_TRANSIENT) */
    if (xDel != SQLITE_STATIC && xDel != SQLITE_TRANSIENT) {
        xDel((void *)z);
    }
}

static void shim_result_int(sqlite3_context *ctx, int val) {
    ctx->result = (int64_t)val;
}

static void shim_result_int64(sqlite3_context *ctx, sqlite3_int64 val) {
    ctx->result = (int64_t)val;
}

static void shim_result_double(sqlite3_context *ctx, double val) {
    int32_t p = turso_malloc(9);
    uint8_t *base = (uint8_t *)(uintptr_t)p;
    base[0] = TURSO_TAG_REAL;
    memcpy(base + 1, &val, 8);
    ctx->result = (int64_t)p;
}

static void shim_result_null(sqlite3_context *ctx) {
    int32_t p = turso_malloc(1);
    *(uint8_t *)(uintptr_t)p = TURSO_TAG_NULL;
    ctx->result = (int64_t)p;
}

static void shim_result_blob(
    sqlite3_context *ctx, const void *data, int n, void (*xDel)(void *)
) {
    if (data == NULL || n <= 0) {
        shim_result_null(ctx);
        return;
    }
    int32_t p = turso_malloc(1 + 4 + n);
    uint8_t *base = (uint8_t *)(uintptr_t)p;
    base[0] = TURSO_TAG_BLOB;
    uint32_t size = (uint32_t)n;
    memcpy(base + 1, &size, 4);
    memcpy(base + 5, data, (size_t)n);
    ctx->result = (int64_t)p;
    if (xDel != SQLITE_STATIC && xDel != SQLITE_TRANSIENT) {
        xDel((void *)data);
    }
}

static void shim_result_error(sqlite3_context *ctx, const char *z, int n) {
    (void)z; (void)n;
    shim_result_null(ctx);
}

static void shim_result_error_nomem(sqlite3_context *ctx) {
    shim_result_null(ctx);
}

static void shim_result_value(sqlite3_context *ctx, sqlite3_value *pVal) {
    ctx->result = pVal->raw;
}

/* ── Other sqlite3 API stubs ────────────────────────────────────────────── */

static void *shim_user_data(sqlite3_context *ctx) {
    return ctx->user_data;
}

static void *shim_malloc_fn(int n) {
    return malloc((size_t)n);
}

static void *shim_malloc64(sqlite3_uint64 n) {
    return malloc((size_t)n);
}

static void shim_free(void *p) {
    free(p);
}

static void *shim_realloc(void *p, int n) {
    return realloc(p, (size_t)n);
}

static int shim_create_function(
    sqlite3 *db, const char *zName, int nArg, int eTextRep, void *pApp,
    void (*xFunc)(sqlite3_context*, int, sqlite3_value**),
    void (*xStep)(sqlite3_context*, int, sqlite3_value**),
    void (*xFinal)(sqlite3_context*)
) {
    (void)db; (void)eTextRep;
    if (xFunc == NULL) return SQLITE_OK; /* Skip aggregate/window functions for now */
    __turso_register_func(
        (int32_t)(uintptr_t)zName, (int32_t)strlen(zName),
        nArg, (int32_t)(uintptr_t)xFunc, (int32_t)(uintptr_t)pApp
    );
    return SQLITE_OK;
}

static int shim_create_function_v2(
    sqlite3 *db, const char *zName, int nArg, int eTextRep, void *pApp,
    void (*xFunc)(sqlite3_context*, int, sqlite3_value**),
    void (*xStep)(sqlite3_context*, int, sqlite3_value**),
    void (*xFinal)(sqlite3_context*),
    void (*xDestroy)(void*)
) {
    (void)xDestroy;
    return shim_create_function(db, zName, nArg, eTextRep, pApp,
                                xFunc, xStep, xFinal);
}

static int shim_create_collation(
    sqlite3 *db, const char *zName, int eTextRep, void *pArg,
    int (*xCompare)(void*, int, const void*, int, const void*)
) {
    (void)db; (void)zName; (void)eTextRep; (void)pArg; (void)xCompare;
    /* Collation support is future work — no-op stub */
    return SQLITE_OK;
}

/* ── Extension entry point (configurable at build time) ─────────────────── */

#ifndef TURSO_SQLITE3_ENTRY
#define TURSO_SQLITE3_ENTRY sqlite3_extension_init
#endif

extern int TURSO_SQLITE3_ENTRY(sqlite3 *, char **, const sqlite3_api_routines *);

/* ── turso_ext_init: call extension init, return rc ─────────────────────── */

__attribute__((export_name("turso_ext_init")))
int32_t turso_ext_init(void) {
    /* Populate the API routines struct with our shim implementations.
     * Zero-initialise first — unimplemented pointers remain NULL. */
    static sqlite3_api_routines api;
    memset(&api, 0, sizeof(api));

    /* Memory */
    api.malloc            = shim_malloc_fn;
    api.malloc64          = shim_malloc64;
    api.free              = shim_free;
    api.realloc           = shim_realloc;

    /* Value readers */
    api.value_type        = shim_value_type;
    api.value_text        = shim_value_text;
    api.value_bytes       = shim_value_bytes;
    api.value_blob        = shim_value_blob;
    api.value_int         = shim_value_int;
    api.value_int64       = shim_value_int64;
    api.value_double      = shim_value_double;

    /* Result setters */
    api.result_text       = shim_result_text;
    api.result_int        = shim_result_int;
    api.result_int64      = shim_result_int64;
    api.result_double     = shim_result_double;
    api.result_null       = shim_result_null;
    api.result_blob       = shim_result_blob;
    api.result_error      = shim_result_error;
    api.result_error_nomem = shim_result_error_nomem;
    api.result_value      = shim_result_value;

    /* Context */
    api.user_data         = shim_user_data;

    /* Registration */
    api.create_function   = shim_create_function;
    api.create_function_v2 = shim_create_function_v2;
    api.create_collation  = shim_create_collation;

    /* Call the extension's init function */
    static sqlite3 fake_db;
    char *errmsg = NULL;
    return TURSO_SQLITE3_ENTRY(&fake_db, &errmsg, &api);
}

/* ── __turso_call: single dispatch export ───────────────────────────────── */

typedef void (*xFunc_t)(sqlite3_context*, int, sqlite3_value**);

__attribute__((export_name("__turso_call")))
int64_t __turso_call(int32_t func_ptr, int32_t user_data_ptr, int32_t argc, int32_t argv) {
    xFunc_t fn = (xFunc_t)(uintptr_t)func_ptr;

    sqlite3_value  values[128];
    sqlite3_value *value_ptrs[128];
    sqlite3_context ctx;

    memset(&ctx, 0, sizeof(ctx));
    /* Default result is NULL (in case extension returns without calling result_*) */
    int32_t null_ptr = turso_malloc(1);
    *(uint8_t *)(uintptr_t)null_ptr = TURSO_TAG_NULL;
    ctx.result = (int64_t)null_ptr;
    ctx.user_data = (void *)(uintptr_t)user_data_ptr;

    int n = argc < 128 ? argc : 128;
    for (int i = 0; i < n; i++) {
        int64_t raw;
        memcpy(&raw, (const void *)(uintptr_t)((uint32_t)argv + (uint32_t)i * 8), 8);
        values[i].raw = raw;
        value_ptrs[i] = &values[i];
    }

    fn(&ctx, argc, value_ptrs);
    return ctx.result;
}
