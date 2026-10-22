#!/bin/sh
# Runs both engines at every database size and connection count, REPEATS
# times each, and writes one set of result files per run into plot/. Each
# engine's database is populated once per size, and every run starts from
# its own copy of it: the rows come from a fixed seed, so a copy is the same
# database a fresh population would build, as the TATP specification
# requires, without populating again for every run. Every setting below can
# be overridden from the environment, e.g. `SIZES=100000 CONNS="1 8"
# REPEATS=1 scripts/bench.sh` for a quick look. run.sh calls this with the
# defaults and then draws the figures.
set -eu

# Every run trims the drive and drops the page cache with sudo. Ask for the
# password now, before anything slow starts, and keep sudo's cached login
# fresh until the script ends, so it never times out waiting for a password
# in the middle of the sweep.
sudo -v
while sudo -n -v 2> /dev/null; do sleep 60; done &
SUDO_KEEPALIVE=$!
trap 'kill "$SUDO_KEEPALIVE" 2> /dev/null' EXIT

cargo build --release -p tatp

# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
RELEASE_DIR="$("$(git rev-parse --show-toplevel)/scripts/cargo-target-dir")/release"
BIN="$RELEASE_DIR/tatp"

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}
# The populated databases and every run's copy go here, and each invocation
# gets its own directory, so no run ever sees another's file. A run's copy
# is deleted once the run is done, because the 1M-subscriber copies alone
# would take several gigabytes; the populated databases are kept.
DB_DIR=${DB_DIR:-"$HERE/db/$(date +%Y%m%d-%H%M%S)"}

# Rows in the subscriber table; the other tables are sized from it. These are
# the sizes the SQLite VLDB paper (Gaffney et al., PVLDB 15) runs TATP at.
SIZES=${SIZES:-"10000 100000 1000000"}
# Connections running transactions at once. One is what the SQLite VLDB
# paper measures; four and sixteen show whether more connections help a
# workload that is mostly reads.
CONNS=${CONNS:-"1 4 16"}
# Page cache of every connection, for both engines. The SQLite VLDB paper
# gives its engines 1 GB, which holds even the largest database.
CACHE_SIZE_MB=${CACHE_SIZE_MB:-1024}
# standard is 80% reads and 20% writes; read runs only the three reads.
MIX=${MIX:-standard}
# nurand draws subscriber ids with the specification's non-uniform
# distribution; uniform draws every subscriber equally often. Results of the
# two cannot be compared.
DISTRIBUTION=${DISTRIBUTION:-nurand}
case "$DISTRIBUTION" in
  nurand) DISTRIBUTION_FLAG="" ;;
  uniform) DISTRIBUTION_FLAG="--uniform" ;;
  *) echo "DISTRIBUTION must be nurand or uniform" >&2; exit 1 ;;
esac
# Runs per configuration. Odd repeats walk the sizes and connection counts
# upwards, even ones downwards, so an effect of the order shows up as a
# difference between them.
REPEATS=${REPEATS:-3}
# Seconds the drive is left alone before each run, after telling it which
# blocks are free. A consumer SSD drains its write cache and collects
# garbage while idle; without the pause, a run inherits the write backlog
# of the run and the copy before it.
IDLE=${IDLE:-30}
CHECKPOINTER=${CHECKPOINTER:-1000}
DURATION=${DURATION:-60}
WARMUP=${WARMUP:-10}
# off makes every Turso transaction write and sync the logical log on its
# own instead of sharing a sync with the transactions committing next to it.
GROUP_COMMIT=${GROUP_COMMIT:-on}
case "$GROUP_COMMIT" in
  on) GROUP_COMMIT_FLAG="" ;;
  off) GROUP_COMMIT_FLAG="--no-group-commit" ;;
  *) echo "GROUP_COMMIT must be on or off" >&2; exit 1 ;;
esac

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
echo "=== sizes \"$SIZES\" connections \"$CONNS\" cache $CACHE_SIZE_MB MiB mix $MIX distribution $DISTRIBUTION repeats $REPEATS idle $IDLE duration $DURATION warmup $WARMUP checkpointer $CHECKPOINTER group commit $GROUP_COMMIT db $DB_DIR" >> "$LOG"

# tee would hide the harness's exit status, so it is passed out by hand.
status="$OUT/.status"
for size in $SIZES; do
  for engine in sqlite turso; do
    populated="$DB_DIR/$engine-s$size-populated.db"
    echo "populating $engine with $size subscribers" | tee -a "$LOG" >&2
    { "$BIN" populate --engine "$engine" --db "$populated" --subscribers "$size"
      echo $? > "$status"; } 2>&1 | tee -a "$LOG" >&2
    [ "$(cat "$status")" = 0 ] || exit 1
    # Populating ends with a checkpoint that moves everything into the
    # database file, so copying that file alone copies the whole database.
    for sidecar in "$populated"-*; do
      if [ -s "$sidecar" ]; then
        echo "$sidecar is not empty, so a copy of $populated would miss rows" >&2
        exit 1
      fi
    done
  done
done
reverse() {
  echo "$1" | tr ' ' '\n' | tac | tr '\n' ' '
}
for run in $(seq 1 "$REPEATS"); do
  if [ $((run % 2)) -eq 1 ]; then
    sizes="$SIZES"
    conns_order="$CONNS"
  else
    sizes="$(reverse "$SIZES")"
    conns_order="$(reverse "$CONNS")"
  fi
  for size in $sizes; do
    for conns in $conns_order; do
      for engine in sqlite turso; do
        db="$DB_DIR/$engine-s$size-c$conns-r$run.db"
        cp "$DB_DIR/$engine-s$size-populated.db" "$db"
        sync
        sudo fstrim "$MOUNT"
        sleep "$IDLE"
        # Every run starts with the database file, and nothing else, in the
        # operating system's cache, so no run inherits pages from the one
        # before it and none spends its warmup reading the file from disk.
        echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
        cat "$db" > /dev/null
        echo "running $engine with $size subscribers and $conns connection(s), run $run of $REPEATS" | tee -a "$LOG" >&2
        { "$BIN" run --engine "$engine" --db "$db" --subscribers "$size" \
              --connections "$conns" --cache-size-mb "$CACHE_SIZE_MB" \
              --mix "$MIX" $DISTRIBUTION_FLAG --checkpointer "$CHECKPOINTER" \
              --duration "$DURATION" --warmup "$WARMUP" --run "$run" \
              --out-dir "$OUT" $GROUP_COMMIT_FLAG
          echo $? > "$status"; } 2>&1 | tee -a "$LOG" >&2
        [ "$(cat "$status")" = 0 ] || exit 1
        rm -f "$db" "$db"-*
      done
    done
  done
done
rm -f "$status"

echo "wrote $OUT/{sqlite,turso}-s<subscribers>-c<connections>-r<run>{,-result,-transactions,-timeline,-checkpoints}.csv; populated databases are under $DB_DIR" >&2
