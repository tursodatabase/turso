#!/bin/sh
set -eu

cargo build --release -p txn-latency

# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"
BIN="$RELEASE_DIR/txn-latency"

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}

RATE=${RATE:-200}
CONNECTIONS=${CONNECTIONS:-4}
BATCH_SIZE=${BATCH_SIZE:-10}
DURATION=${DURATION:-60}
WARMUP=${WARMUP:-5}

mkdir -p "$OUT"

for engine in sqlite turso; do
  echo "running $engine at $RATE transactions/s" >&2
  "$BIN" --engine "$engine" --rate "$RATE" --connections "$CONNECTIONS" \
      --batch-size "$BATCH_SIZE" --duration "$DURATION" --warmup "$WARMUP" \
      > "$OUT/$engine.csv"
done

echo "wrote $OUT/sqlite.csv and $OUT/turso.csv" >&2
