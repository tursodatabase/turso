#!/bin/sh
# Runs both engines at every connection count, REPEATS times each, and writes
# one set of result files per run into plot/. Every setting below can be
# overridden from the environment, e.g. `CONNS="1 8" REPEATS=1
# scripts/bench.sh` for a quick look. run.sh calls this with the defaults and
# then draws the figures.
set -eu

HERE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(git -C "$HERE" rev-parse --show-toplevel)"
OUT=${OUT:-"$HERE/plot"}
# Each engine's database is loaded once and every run starts from a copy of
# it, so no run sees another's writes. Every invocation gets its own
# directory and nothing is deleted.
DB_DIR=${DB_DIR:-"$HERE/db/$(date +%Y%m%d-%H%M%S)"}

# Warehouses in the database. Connections pick a home warehouse at random,
# so more warehouses means less contention on the warehouse and district
# rows that Payment and New-Order update.
WAREHOUSES=${WAREHOUSES:-4}
# Connections running transactions at once. Each runs its transactions back
# to back with no think time, so this is the number of transactions in
# flight.
CONNS=${CONNS:-"1 2 4 8 16"}
# Runs per configuration. Odd repeats walk the connection counts upwards, even
# ones downwards, so an effect of the order shows up as a difference
# between them.
REPEATS=${REPEATS:-3}
# Seconds the drive is left alone before each run, after telling it which
# blocks are free. A consumer SSD drains its write cache and collects
# garbage while idle; without the pause, runs late in a long session
# inherit the write backlog of the runs before them.
IDLE=${IDLE:-30}
WARMUP=${WARMUP:-5}
MEASURE=${MEASURE:-30}
# Seconds between timeline rows; also the resolution of the ramp-up.
INTERVAL=${INTERVAL:-1}

cargo build --release -p turso_sqlite3
# Ask cargo where build artefacts live (honours CARGO_TARGET_DIR)
TURSO_LIB="$("$ROOT/scripts/cargo-target-dir")/release"
make -C "$HERE/src" clean -s
make -C "$HERE/src" -j all BACKEND=turso TURSO_ROOT="$ROOT" TURSO_LIB="$TURSO_LIB" -s
make -C "$HERE/src" clean -s
make -C "$HERE/src" -j all BACKEND=sqlite -s

mkdir -p "$OUT" "$DB_DIR"
MOUNT="$(df --output=target "$DB_DIR" | tail -1)"

# Every run's output goes to the terminal and to this log, under a line
# that says what the machine is.
LOG="$OUT/bench.log"
CPU="$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
DEVICE="$(df --output=source "$DB_DIR" | tail -1)"
PARENT="$(lsblk -no PKNAME "$DEVICE" 2>/dev/null | head -1)"
DISK="$(lsblk -dno MODEL "/dev/${PARENT:-$(basename "$DEVICE")}" 2>/dev/null | sed 's/ *$//')"
FSTYPE="$(df --output=fstype "$DB_DIR" | tail -1)"
echo "=== $(date) platform: $CPU, $(nproc) hardware threads, Linux $(uname -r), disk ${DISK:-unknown} ($FSTYPE on $DEVICE)" >> "$LOG"
echo "=== warehouses $WAREHOUSES connections \"$CONNS\" repeats $REPEATS idle $IDLE warmup $WARMUP measure $MEASURE interval $INTERVAL db $DB_DIR" >> "$LOG"

for engine in sqlite turso; do
  template="$DB_DIR/$engine-template.db"
  echo "loading $WAREHOUSES warehouse(s) into $template" | tee -a "$LOG" >&2
  sqlite3 "$template" < "$HERE/create_table_sqlite.sql"
  sqlite3 "$template" < "$HERE/add_fkey_idx_sqlite.sql"
  "$HERE/tpcc_load-$engine" -w "$WAREHOUSES" -d "$template" >> "$LOG" 2>&1
done

for run in $(seq 1 "$REPEATS"); do
  if [ $((run % 2)) -eq 1 ]; then
    order="$CONNS"
  else
    order="$(echo "$CONNS" | tr ' ' '\n' | tac | tr '\n' ' ')"
  fi
  for conns in $order; do
    for engine in sqlite turso; do
      db="$DB_DIR/$engine-c$conns-r$run.db"
      cp "$DB_DIR/$engine-template.db" "$db"
      [ -f "$DB_DIR/$engine-template.db-wal" ] && cp "$DB_DIR/$engine-template.db-wal" "$db-wal"
      sync
      sudo fstrim "$MOUNT"
      sleep "$IDLE"
      echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
      echo "running $engine with $conns connection(s), run $run of $REPEATS" | tee -a "$LOG" >&2
      # tee would hide the harness's exit status, so it is passed out by hand.
      status="$OUT/.status"
      { "$HERE/tpcc_start-$engine" -w "$WAREHOUSES" -c "$conns" \
            -r "$WARMUP" -l "$MEASURE" -i "$INTERVAL" \
            -d "$db" -o "$OUT/$engine-c$conns-r$run"
        echo $? > "$status"; } 2>&1 | tee -a "$LOG" >&2
      [ "$(cat "$status")" = 0 ] || exit 1
    done
  done
done
rm -f "$status"

echo "wrote $OUT/{sqlite,turso}-c<connections>-r<run>-{result,timeline,hist}.csv; database files are under $DB_DIR" >&2
