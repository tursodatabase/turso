#include "../../c/turso_wasm.h"

/* Integer add: SELECT add(1, 2) → 3 */
__attribute__((export_name("add")))
int64_t add(int32_t argc, int32_t argv) {
    return turso_ret_int(turso_arg_int(argv, 0) + turso_arg_int(argv, 1));
}

/* ASCII uppercasing: SELECT upper('hello') → 'HELLO' */
__attribute__((export_name("upper")))
int64_t upper(int32_t argc, int32_t argv) {
    const char *s = turso_arg_text(argv, 0);

    size_t len = __turso_strlen(s);

    __turso_set_ret_type(TURSO_TAG_TEXT);
    int32_t ptr = turso_malloc((int32_t)(len + 1));
    uint8_t *base = (uint8_t *)(uintptr_t)ptr;
    for (size_t i = 0; i < len; i++) {
        uint8_t c = (uint8_t)s[i];
        if (c >= 'a' && c <= 'z') c -= 32;
        base[i] = c;
    }
    base[len] = 0;
    return (int64_t)ptr;
}
