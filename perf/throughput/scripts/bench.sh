#!/bin/sh
# Runs both engines at every connection count, REPEATS times each, and writes
# one result file per run into plot/. Every setting below
# can be overridden from the environment, e.g. `CONNS="1 8" REPEATS=1
# scripts/bench.sh` for a quick look. run.sh calls this with the defaults and
# then draws the figures.
set -eu

cargo build --release -p txn-throughput

# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"
BIN="$RELEASE_DIR/txn-throughput"

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}
# Every run writes its own database file here, and each invocation gets its
# own directory, so no run ever sees another's file and nothing is deleted.
DB_DIR=${DB_DIR:-"$HERE/db/$(date +%Y%m%d-%H%M%S)"}

# Connections writing at once. Goes well past the core count on purpose:
# a database is loaded with several connections per core, and the
# interesting range starts where one per core ends.
CONNS=${CONNS:-"1 2 4 8 16 32 64"}
# Runs per configuration. Odd repeats walk the connection counts upwards, even
# ones downwards, so an effect of the order shows up as a difference
# between them.
REPEATS=${REPEATS:-3}
# Seconds the drive is left alone before each run, after telling it which
# blocks are free. A consumer SSD drains its write cache and collects
# garbage while idle; without the pause, runs late in a long session
# inherit the write backlog of the runs before them.
IDLE=${IDLE:-30}
BATCH_SIZE=${BATCH_SIZE:-100}
CHECKPOINTER=${CHECKPOINTER:-1000}
DURATION=${DURATION:-30}
WARMUP=${WARMUP:-3}

mkdir -p "$OUT" "$DB_DIR"
MOUNT="$(df --output=target "$DB_DIR" | tail -1)"

# Every run's summary goes to the terminal and to this log, under a line
# that says what the machine is.
LOG="$OUT/bench.log"
CPU="$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
DEVICE="$(df --output=source "$DB_DIR" | tail -1)"
PARENT="$(lsblk -no PKNAME "$DEVICE" 2>/dev/null | head -1)"
DISK="$(lsblk -dno MODEL "/dev/${PARENT:-$(basename "$DEVICE")}" 2>/dev/null | sed 's/ *$//')"
FSTYPE="$(df --output=fstype "$DB_DIR" | tail -1)"
echo "=== $(date) platform: $CPU, $(nproc) hardware threads, Linux $(uname -r), disk ${DISK:-unknown} ($FSTYPE on $DEVICE)" >> "$LOG"
echo "=== connections \"$CONNS\" repeats $REPEATS idle $IDLE duration $DURATION warmup $WARMUP batch $BATCH_SIZE checkpointer $CHECKPOINTER db $DB_DIR" >> "$LOG"

for run in $(seq 1 "$REPEATS"); do
  if [ $((run % 2)) -eq 1 ]; then
    order="$CONNS"
  else
    order="$(echo "$CONNS" | tr ' ' '\n' | tac | tr '\n' ' ')"
  fi
  for conns in $order; do
    for engine in sqlite turso; do
      sync
      sudo fstrim "$MOUNT"
      sleep "$IDLE"
      echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
      echo "running $engine with $conns connection(s), run $run of $REPEATS" | tee -a "$LOG" >&2
      # tee would hide the harness's exit status, so it is passed out by hand.
      status="$OUT/.status"
      { "$BIN" --engine "$engine" --connections "$conns" \
            --batch-size "$BATCH_SIZE" --checkpointer "$CHECKPOINTER" \
            --duration "$DURATION" --warmup "$WARMUP" --run "$run" \
            --db-dir "$DB_DIR" --out-dir "$OUT"
        echo $? > "$status"; } 2>&1 | tee -a "$LOG" >&2
      [ "$(cat "$status")" = 0 ] || exit 1
    done
  done
done
rm -f "$status"

echo "wrote $OUT/{sqlite,turso}-c<connections>-r<run>{,-result,-timeline,-checkpoints}.csv; database files are under $DB_DIR" >&2
