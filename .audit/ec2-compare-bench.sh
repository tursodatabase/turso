#!/bin/bash
# Four-way NVMe compare: sqlite, origin/main, reclaim, reclaim+group-commit.
# Runs as root via SSM; builds as ubuntu. Harness patches stay on the box.
set -euo pipefail
MNT=/mnt/nvme
RUN=$MNT/run/compare-rebase-20260904
mkdir -p "$RUN/latency/db" "$RUN/throughput/db"
exec > >(tee -a "$RUN/outer.log") 2>&1
echo "=== $(date -Is) compare start ==="

export HOME=/root
export CARGO_HOME=/home/ubuntu/.cargo
export RUSTUP_HOME=/home/ubuntu/.rustup
export PATH="/home/ubuntu/.cargo/bin:/home/ubuntu/.local/bin:/usr/local/bin:$PATH"

SRC=$MNT/limbo
MAIN=$MNT/limbo-main
RECLAIM=$MNT/limbo-reclaim
LAT_OUT=$RUN/latency
THR_OUT=$RUN/throughput

git config --global --add safe.directory '*' || true
chown -R ubuntu:ubuntu "$RUN" "$SRC" || true

if ! sudo -n true; then
  echo "need passwordless sudo for fstrim/drop_caches"
  exit 1
fi

if [ ! -x /home/ubuntu/.local/bin/uv ] && ! command -v uv >/dev/null; then
  sudo -u ubuntu -H bash -lc 'curl -LsSf https://astral.sh/uv/install.sh | sh'
fi
export PATH="/home/ubuntu/.local/bin:$PATH"

as_ubuntu() {
  sudo -u ubuntu -H env \
    HOME=/home/ubuntu \
    CARGO_HOME=/home/ubuntu/.cargo \
    RUSTUP_HOME=/home/ubuntu/.rustup \
    PATH="/home/ubuntu/.cargo/bin:/home/ubuntu/.local/bin:/usr/local/bin:$PATH" \
    "$@"
}

git config --global --add safe.directory "$SRC"
as_ubuntu git -C "$SRC" remote add turso https://github.com/tursodatabase/turso.git 2>/dev/null || true
echo "=== fetch remotes ==="
# Drop broken remote-tracking refs left by prior shallow/rebase fetches.
as_ubuntu rm -f "$SRC/.git/refs/remotes/turso/main"   "$SRC/.git/refs/remotes/origin/mvcc-retire-checkpoint-reclaim"
as_ubuntu git -C "$SRC" update-ref -d refs/remotes/turso/main 2>/dev/null || true
as_ubuntu git -C "$SRC" update-ref -d refs/remotes/origin/mvcc-retire-checkpoint-reclaim 2>/dev/null || true
if as_ubuntu test -f "$SRC/.git/packed-refs"; then
  as_ubuntu bash -lc "cd \"$SRC\" && grep -Ev 'refs/remotes/(turso/main|origin/mvcc-retire-checkpoint-reclaim)$' .git/packed-refs > /tmp/packed-refs.new && mv /tmp/packed-refs.new .git/packed-refs"
fi
as_ubuntu git -C "$SRC" fetch --prune --depth=120 turso +main:refs/remotes/turso/main
as_ubuntu git -C "$SRC" fetch --prune origin +mvcc-retire-checkpoint-reclaim:refs/remotes/origin/mvcc-retire-checkpoint-reclaim
as_ubuntu git -C "$SRC" log -1 --oneline turso/main
as_ubuntu git -C "$SRC" log -1 --oneline origin/mvcc-retire-checkpoint-reclaim

setup_tree() {
  local dir="$1" ref="$2"
  if [ -d "$dir/.git" ] || [ -f "$dir/.git" ]; then
    as_ubuntu git -C "$dir" checkout --force --detach "$ref"
    as_ubuntu git -C "$dir" reset --hard "$ref"
  else
    as_ubuntu git -C "$SRC" worktree remove --force "$dir" 2>/dev/null || rm -rf "$dir"
    as_ubuntu git -C "$SRC" worktree add --detach "$dir" "$ref"
  fi
  python3 /tmp/patch-bench-harness.py "$dir"
  echo "tree $dir -> $(as_ubuntu git -C "$dir" rev-parse --short HEAD) $(as_ubuntu git -C "$dir" log -1 --format=%s)"
}

setup_tree "$MAIN" turso/main
setup_tree "$RECLAIM" origin/mvcc-retire-checkpoint-reclaim

echo "=== build main ==="
as_ubuntu bash -lc "export CARGO_TARGET_DIR=$MNT/target-main; cd '$MAIN' && cargo build --release -p turso-txn-latency -p txn-throughput"
echo "=== build reclaim ==="
as_ubuntu bash -lc "export CARGO_TARGET_DIR=$MNT/target-reclaim; cd '$RECLAIM' && cargo build --release -p turso-txn-latency -p txn-throughput"
chown -R ubuntu:ubuntu "$MNT/target-main" "$MNT/target-reclaim"

LAT_MAIN=$MNT/target-main/release/turso-txn-latency
THR_MAIN=$MNT/target-main/release/txn-throughput
LAT_RECLAIM=$MNT/target-reclaim/release/turso-txn-latency
THR_RECLAIM=$MNT/target-reclaim/release/txn-throughput
test -x "$LAT_MAIN" && test -x "$THR_MAIN" && test -x "$LAT_RECLAIM" && test -x "$THR_RECLAIM"

echo "main    $(as_ubuntu git -C "$MAIN" rev-parse HEAD)"
echo "reclaim $(as_ubuntu git -C "$RECLAIM" rev-parse HEAD)"
"$LAT_MAIN" --help | grep -E 'label|group-commit' || true
"$LAT_RECLAIM" --help | grep -E 'label|group-commit' || true

# Match the harness README connection lists. IDLE is 30s on instance-store NVMe
# (README uses 60s for consumer SSDs). Three repeats so a stddev exists.
LAT_CONNS=${LAT_CONNS:-"1 8 16 32"}
THR_CONNS=${THR_CONNS:-"1 2 4 8 16 32 64"}
REPEATS=${REPEATS:-3}
IDLE=${IDLE:-30}
LAT_RATE=${LAT_RATE:-1000}
LAT_DURATION=${LAT_DURATION:-60}
THR_DURATION=${THR_DURATION:-30}

prep_disk() {
  local dir="$1"
  mkdir -p "$dir"
  sync
  sudo fstrim "$(df --output=target "$dir" | tail -1)"
  sleep "$IDLE"
  echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
}

run_latency() {
  local bin="$1" engine="$2" label="$3"
  shift 3
  local db="$LAT_OUT/db"
  mkdir -p "$db"
  for c in $LAT_CONNS; do
    for r in $(seq 1 "$REPEATS"); do
      prep_disk "$db"
      echo "=== $(date -Is) LAT $label c=$c r=$r $* ==="
      "$bin" --engine "$engine" --label "$label" "$@" \
        --rate "$LAT_RATE" --connections "$c" --run "$r" \
        --checkpointer 1000 --batch-size 10 \
        --duration "$LAT_DURATION" --warmup 5 \
        --arrivals poisson --seed 1 \
        --db-dir "$db" --out-dir "$LAT_OUT"
    done
  done
}

run_throughput() {
  local bin="$1" engine="$2" label="$3"
  shift 3
  local db="$THR_OUT/db"
  mkdir -p "$db"
  for r in $(seq 1 "$REPEATS"); do
    if [ $((r % 2)) -eq 1 ]; then
      order="$THR_CONNS"
    else
      order="$(echo "$THR_CONNS" | tr ' ' '\n' | tac | tr '\n' ' ')"
    fi
    for c in $order; do
      prep_disk "$db"
      echo "=== $(date -Is) THR $label c=$c r=$r $* ==="
      "$bin" --engine "$engine" --label "$label" "$@" \
        --connections "$c" --run "$r" \
        --batch-size 100 --checkpointer 1000 \
        --duration "$THR_DURATION" --warmup 3 \
        --db-dir "$db" --out-dir "$THR_OUT"
    done
  done
}

echo "=== latency suite ==="
run_latency "$LAT_MAIN" sqlite sqlite
run_latency "$LAT_MAIN" turso turso-main
run_latency "$LAT_RECLAIM" turso turso-reclaim
run_latency "$LAT_RECLAIM" turso turso-gc --group-commit

echo "=== throughput suite ==="
run_throughput "$THR_MAIN" sqlite sqlite
run_throughput "$THR_MAIN" turso turso-main
run_throughput "$THR_RECLAIM" turso turso-reclaim
run_throughput "$THR_RECLAIM" turso turso-gc --group-commit

echo "=== plots ==="
cd "$LAT_OUT"
as_ubuntu bash -lc '
set -euo pipefail
files=$(ls sqlite-c*-r*.csv turso-*-c*-r*.csv | grep -v checkpoint)
uv run '"$RECLAIM"'/perf/latency/plot/plot-latency-ecdf.py $files \
  --name sqlite=SQLite \
  --name "turso-main=Turso main" \
  --name "turso-reclaim=Turso reclaim" \
  --name "turso-gc=Turso reclaim+GC" \
  -o latency-ecdf.png -o latency-ecdf.pdf
'

cd "$THR_OUT"
as_ubuntu uv run "$RECLAIM/perf/throughput/plot/plot-throughput.py" \
  *-result.csv \
  --name sqlite=SQLite \
  --name 'turso-main=Turso main' \
  --name 'turso-reclaim=Turso reclaim' \
  --name 'turso-gc=Turso reclaim+GC' \
  -o throughput.png -o throughput.pdf || echo "throughput plot failed"

python3 /tmp/summarize-compare.py

echo "=== $(date -Is) compare done ==="
