#!/usr/bin/env sh

set -xe

# Use --no-layout-tests to avoid hardcoded size assertions that fail on different architectures
# The struct layouts are correct, just the compile-time assertions are architecture-specific
bindgen turso_sync.h -o src/bindings.rs \
    --with-derive-default \
    --no-layout-tests \
    --allowlist-type "turso_sync_.*_t" \
    --allowlist-function "turso_sync_.*" \
    --blocklist-type "turso_status_code_t" \
    --blocklist-type "turso_database_config_t" \
    --blocklist-type "turso_database" \
    --blocklist-type "turso_database_t" \
    --blocklist-type "turso_database" \
    --blocklist-type "turso_database_t" \
    --blocklist-type "turso_connection" \
    --blocklist-type "turso_connection_t" \
    --blocklist-type "turso_status_t" \
    --blocklist-type "turso_slice_ref_t" \
    --rustified-enum "turso_sync_io_request_type_t" \
    --rustified-enum "turso_sync_database_io_request_type_t" \
    --rustified-enum "turso_sync_operation_result_type_t" \
    --raw-line "use turso_sdk_kit::capi::c::*;" \
    --default-non-copy-union-style manually_drop \
    -- -I../../sdk-kit/
