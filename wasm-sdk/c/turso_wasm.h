/*
 * turso_wasm.h — Single-file C header for Turso WASM UDFs.
 *
 * Provides:
 *   - ABI tag constants
 *   - Default bump allocator (turso_malloc / turso_reset_allocator)
 *   - Argument readers (turso_arg_int, turso_arg_double, turso_arg_text, ...)
 *   - Return value builders (turso_ret_int, turso_ret_double, turso_ret_text, ...)
 *
 * All functions are static inline for zero overhead.
 *
 * Example:
 *   #include "turso_wasm.h"
 *
 *   __attribute__((export_name("add")))
 *   int64_t add(int32_t argc, int32_t argv) {
 *       return turso_arg_int(argv, 0) + turso_arg_int(argv, 1);
 *   }
 *
 * Build:
 *   clang --target=wasm32-unknown-unknown -nostdlib \
 *         -Wl,--export=memory -Wl,--no-entry -O2 -o add.wasm add.c
 */

#ifndef TURSO_WASM_H
#define TURSO_WASM_H

#include <stdint.h>
#include <stddef.h>

/* ── ABI tag constants (must match core/wasm/mod.rs) ───────────────────────── */

#define TURSO_TAG_INTEGER 0x01
#define TURSO_TAG_REAL    0x02
#define TURSO_TAG_TEXT    0x03
#define TURSO_TAG_BLOB    0x04
#define TURSO_TAG_NULL    0x05

/* Sideband address where the return type tag is written. */
#define TURSO_RET_TYPE_ADDR 1017

/* ── Default bump allocator ────────────────────────────────────────────────── */

/* Bump region starts at byte 1024 (after the 128-slot argv area). */
static uint32_t __turso_bump_ptr = 1024;

__attribute__((export_name("turso_malloc")))
int32_t turso_malloc(int32_t size) {
    uint32_t ptr = __turso_bump_ptr;
    __turso_bump_ptr += (uint32_t)size;
    return (int32_t)ptr;
}

static inline void turso_reset_allocator(void) {
    __turso_bump_ptr = 1024;
}

/* ── Helpers ───────────────────────────────────────────────────────────────── */

/* Inline strlen replacement — prevents the optimizer from emitting a libc strlen call. */
static inline size_t __turso_strlen(const char *s) {
    const char *p = s;
    while (*p) p++;
    return (size_t)(p - s);
}

static inline int64_t __turso_read_slot(int32_t argv, int32_t index) {
    const uint8_t *base = (const uint8_t *)(uintptr_t)argv;
    const uint8_t *slot = base + index * 8;
    int64_t val;
    __builtin_memcpy(&val, slot, 8);
    return val;
}

static inline void __turso_write_bytes(int32_t dst, const uint8_t *src, size_t n) {
    uint8_t *p = (uint8_t *)(uintptr_t)dst;
    for (size_t i = 0; i < n; i++) {
        p[i] = src[i];
    }
}

/* ── Argument readers ──────────────────────────────────────────────────────── */

static inline int64_t turso_arg_int(int32_t argv, int32_t index) {
    return __turso_read_slot(argv, index);
}

static inline double turso_arg_double(int32_t argv, int32_t index) {
    int64_t raw = __turso_read_slot(argv, index);
    double val;
    __builtin_memcpy(&val, &raw, 8);
    return val;
}

/* Returns pointer to the text bytes (past the TAG_TEXT byte). Null-terminated. */
static inline const char *turso_arg_text(int32_t argv, int32_t index) {
    int64_t raw = __turso_read_slot(argv, index);
    const uint8_t *ptr = (const uint8_t *)(uintptr_t)raw;
    /* Skip TAG_TEXT byte */
    return (const char *)(ptr + 1);
}

/* Returns pointer to blob data (past TAG_BLOB + 4-byte size header). */
static inline const uint8_t *turso_arg_blob_data(int32_t argv, int32_t index) {
    int64_t raw = __turso_read_slot(argv, index);
    const uint8_t *ptr = (const uint8_t *)(uintptr_t)raw;
    /* Skip TAG_BLOB (1) + size (4) */
    return ptr + 5;
}

/* Returns the blob size in bytes. */
static inline uint32_t turso_arg_blob_size(int32_t argv, int32_t index) {
    int64_t raw = __turso_read_slot(argv, index);
    const uint8_t *ptr = (const uint8_t *)(uintptr_t)raw;
    uint32_t size;
    __builtin_memcpy(&size, ptr + 1, 4);
    return size;
}

static inline int turso_arg_is_null(int32_t argv, int32_t index) {
    int64_t raw = __turso_read_slot(argv, index);
    const uint8_t *ptr = (const uint8_t *)(uintptr_t)raw;
    return *ptr == TURSO_TAG_NULL;
}

/* ── Return value builders ─────────────────────────────────────────────────── */

/* Write the return type tag to the sideband address. */
static inline void __turso_set_ret_type(uint8_t tag) {
    *(volatile uint8_t *)TURSO_RET_TYPE_ADDR = tag;
}

/* Return an integer. Zero allocation — writes type to sideband. */
static inline int64_t turso_ret_int(int64_t val) {
    __turso_set_ret_type(TURSO_TAG_INTEGER);
    return val;
}

/* Return a double. Zero allocation — writes type to sideband, returns f64 bits. */
static inline int64_t turso_ret_double(double val) {
    __turso_set_ret_type(TURSO_TAG_REAL);
    int64_t bits;
    __builtin_memcpy(&bits, &val, 8);
    return bits;
}

/* Return a text string. Allocates [bytes][0x00] (no tag prefix). */
static inline int64_t turso_ret_text(const char *s) {
    __turso_set_ret_type(TURSO_TAG_TEXT);
    size_t len = __turso_strlen(s);
    int32_t ptr = turso_malloc((int32_t)(len + 1));
    uint8_t *base = (uint8_t *)(uintptr_t)ptr;
    for (size_t i = 0; i < len; i++) {
        base[i] = (uint8_t)s[i];
    }
    base[len] = 0;
    return (int64_t)ptr;
}

/* Return a blob. Allocates [4-byte LE size][data] (no tag prefix). */
static inline int64_t turso_ret_blob(const uint8_t *data, uint32_t size) {
    __turso_set_ret_type(TURSO_TAG_BLOB);
    int32_t ptr = turso_malloc((int32_t)(4 + size));
    uint8_t *base = (uint8_t *)(uintptr_t)ptr;
    __builtin_memcpy(base, &size, 4);
    for (uint32_t i = 0; i < size; i++) {
        base[4 + i] = data[i];
    }
    return (int64_t)ptr;
}

/* Return NULL. Zero allocation — writes type to sideband. */
static inline int64_t turso_ret_null(void) {
    __turso_set_ret_type(TURSO_TAG_NULL);
    return 0;
}

#endif /* TURSO_WASM_H */
