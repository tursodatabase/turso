#!/usr/bin/env sh

set -xe

bindgen turso.h -o src/bindings.rs \
    --with-derive-default \
    --allowlist-type "turso_.*_t" \
    --allowlist-function "turso_.*" \
    --rustified-enum "turso_type_t" \
    --rustified-enum "turso_tracing_level_t" \
    --rustified-enum "turso_status_code_t"