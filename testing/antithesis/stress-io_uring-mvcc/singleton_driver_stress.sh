#!/usr/bin/env bash

# RUST_LOG: keep global noise low (warn) but capture the temporary
# `mvcc_ckpt_index` checkpoint index-delete trace at TRACE so a reproduction's
# `.jsonl` shows whether each lost old-key index delete was collected / skipped /
# applied. (Diagnostic for the orphan-index-entries corruption.)
RUST_LOG="${RUST_LOG:-warn,mvcc_ckpt_index=trace}" \
  /bin/turso_stress --nr-threads 2 --nr-iterations 10000 --vfs io_uring --tx-mode concurrent
