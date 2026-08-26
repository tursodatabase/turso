#!/bin/sh
# Finds how many transactions per second each engine can sustain, so you can
# pick a rate for bench.sh that sits below both. Closed-loop numbers, so read
# them as throughput, not as latency.
set -eu

cargo build --release -p txn-latency

RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"
BIN="$RELEASE_DIR/txn-latency"

CONNECTIONS=${CONNECTIONS:-4}
BATCH_SIZE=${BATCH_SIZE:-10}
DURATION=${DURATION:-20}
WARMUP=${WARMUP:-3}

for engine in sqlite turso; do
  "$BIN" --engine "$engine" --closed-loop --connections "$CONNECTIONS" \
      --batch-size "$BATCH_SIZE" --duration "$DURATION" --warmup "$WARMUP" \
      > /dev/null
done
