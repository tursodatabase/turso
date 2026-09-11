#!/bin/sh
# Runs both engines at every connection count and writes one CSV per engine
# and count into plot/. Every setting below can be overridden from the
# environment, e.g. `CONNECTIONS="1 8" REPEATS=1 scripts/bench.sh` for a
# quick look. run.sh calls this with the defaults and then draws the figure.
set -eu

cargo build --release -p turso-txn-latency

# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"
BIN="$RELEASE_DIR/turso-txn-latency"

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}
# Every run writes its own database file here, and each invocation gets its
# own directory, so no run ever sees another's file and nothing is deleted.
DB_DIR=${DB_DIR:-"$HERE/db/$(date +%Y%m%d-%H%M%S)"}

# Transactions per second offered, the total across connections, so more
# connections means more transactions in flight at once, not more load.
RATE=${RATE:-1000}
CONNECTIONS=${CONNECTIONS:-"1 8 16 32"}
# Runs per engine and connection count. Every run writes its own CSV.
REPEATS=${REPEATS:-3}
# Seconds the drive is left alone before each run, after telling it which
# blocks are free. A consumer SSD drains its write cache and collects
# garbage while idle; without the pause, runs late in a long session
# inherit the write backlog of the runs before them.
IDLE=${IDLE:-60}
# poisson spaces arrivals with exponential gaps around 1/RATE; fixed puts
# them exactly 1/RATE apart. The seed gives every run the same schedule.
ARRIVALS=${ARRIVALS:-poisson}
SEED=${SEED:-1}
CHECKPOINTER=${CHECKPOINTER:-1000}
BATCH_SIZE=${BATCH_SIZE:-10}
DURATION=${DURATION:-60}
WARMUP=${WARMUP:-5}

mkdir -p "$OUT" "$DB_DIR"

# Every run's summary goes to the terminal and to this log.
LOG="$OUT/bench.log"
echo "=== $(date) rate $RATE connections \"$CONNECTIONS\" repeats $REPEATS idle $IDLE arrivals $ARRIVALS seed $SEED db $DB_DIR" >> "$LOG"

MOUNT="$(df --output=target "$DB_DIR" | tail -1)"

for connections in $CONNECTIONS; do
  for engine in sqlite turso; do
    for run in $(seq 1 "$REPEATS"); do
      sync
      sudo fstrim "$MOUNT"
      sleep "$IDLE"
      echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
      echo "running $engine at $RATE transactions/s over $connections connection(s), run $run of $REPEATS" | tee -a "$LOG" >&2
      # tee would hide the harness's exit status, so it is passed out by hand.
      status="$OUT/.status"
      { "$BIN" --engine "$engine" --rate "$RATE" --connections "$connections" \
            --checkpointer "$CHECKPOINTER" --batch-size "$BATCH_SIZE" \
            --duration "$DURATION" --warmup "$WARMUP" --run "$run" \
            --arrivals "$ARRIVALS" --seed "$SEED" --db-dir "$DB_DIR" --out-dir "$OUT"
        echo $? > "$status"; } 2>&1 | tee -a "$LOG" >&2
      [ "$(cat "$status")" = 0 ] || exit 1
    done
  done
done
rm -f "$status"

echo "wrote $OUT/{sqlite,turso}-c{$(echo $CONNECTIONS | tr ' ' ',')}-r{1..$REPEATS}.csv; database files are under $DB_DIR" >&2
