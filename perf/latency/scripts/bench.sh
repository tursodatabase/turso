#!/bin/sh
set -eu

cargo build --release -p txn-latency

# The build above leaves gigabytes of dirty pages behind, and every fsync in
# the run would wait for that writeback. Flush it, then drop the page cache
# so both engines start cold. The redirect has to run as root too.
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"
BIN="$RELEASE_DIR/txn-latency"

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}

RATE=${RATE:-200}
# Space-separated list. The offered rate is the total across connections, so
# more connections means more transactions in flight at once, not more load.
CONNECTIONS=${CONNECTIONS:-1}
CHECKPOINTER=${CHECKPOINTER:-1000}
BATCH_SIZE=${BATCH_SIZE:-10}
DURATION=${DURATION:-60}
WARMUP=${WARMUP:-5}

mkdir -p "$OUT"

for connections in $CONNECTIONS; do
  for engine in sqlite turso; do
    echo "running $engine at $RATE transactions/s over $connections connection(s)" >&2
    "$BIN" --engine "$engine" --rate "$RATE" --connections "$connections" \
        --checkpointer "$CHECKPOINTER" --batch-size "$BATCH_SIZE" \
        --duration "$DURATION" --warmup "$WARMUP" \
        > "$OUT/$engine-c$connections.csv"
  done
done

echo "wrote $OUT/{sqlite,turso}-c{$(echo $CONNECTIONS | tr ' ' ',')}.csv" >&2
