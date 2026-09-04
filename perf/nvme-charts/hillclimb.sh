#!/bin/bash
# Frozen screening harness for p99 commit ablation on the NVMe box.
# One change, one measurement. Reset to BASE between attempts.
set -euo pipefail

export HOME=/root
export PATH="/home/ubuntu/.cargo/bin:/root/.cargo/bin:$PATH"
export CARGO_TARGET_DIR=/mnt/nvme/target

REPO=/mnt/nvme/limbo
OUT=/mnt/nvme/run/hillclimb
LOG="$OUT/decisions.tsv"
SUMMARY="$OUT/summary.txt"
export RATE=1000
export CONNECTIONS="8 16"
export REPEATS=1
export IDLE=20
export DURATION=30
export WARMUP=3
export ARRIVALS=poisson
export SEED=1

mkdir -p "$OUT"
cd "$REPO"
git config --global --add safe.directory "$REPO" || true
git config --global advice.detachedHead false

if [ ! -f "$LOG" ]; then
  printf 'ts\tphase\tdecision\twhy\tevidence\tresult\n' > "$LOG"
fi

log_row() {
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$ts" "$1" "$2" "$3" "$4" "$5" >> "$LOG"
}

extract_metrics() {
  python3 - "$1" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
chunks = re.split(r"(?=running \S+ at )", text)
rows = []
for ch in chunks:
    m = re.search(r"running (\S+) at .* over (\d+) connection", ch)
    if not m:
        continue
    engine, conns = m.group(1), int(m.group(2))
    am = re.search(r"(\d+)/s achieved", ch)
    pm = re.search(r"p50 ([0-9.]+)ms\s+p99 ([0-9.]+)ms\s+p99\.9 ([0-9.]+)ms", ch)
    cpu = re.search(r"([0-9]+)% of one core", ch)
    if not (am and pm):
        continue
    rows.append((engine, conns, int(am.group(1)), float(pm.group(1)), float(pm.group(2)), float(pm.group(3)), int(cpu.group(1)) if cpu else -1))
best = {}
for r in rows:
    best[(r[0], r[1])] = r
for k in sorted(best, key=lambda x: (x[0], x[1])):
    e,c,a,p50,p99,p999,cpu = best[k]
    print(f"{e}\t{c}\t{a}\t{p50}\t{p99}\t{p999}\t{cpu}")
PY
}

run_bench() {
  local tag="$1"
  local mode="${2:-ours}"
  local dest="$OUT/$tag"
  rm -rf "$dest"
  mkdir -p "$dest/db"
  : > "$dest/bench.log"
  cd "$REPO"
  echo "building $tag at $(date -u +%H:%M:%S)" | tee -a "$dest/bench.log"
  if [ "$mode" = "ours" ]; then
    apply_group_commit_flag
  fi
  cargo build --release -p turso-txn-latency
  local release_dir
  release_dir="$("$REPO/scripts/cargo-target-dir")/release"
  local bin="$release_dir/turso-txn-latency"
  local mount
  mount="$(df --output=target "$dest/db" | tail -1)"
  echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) tag=$tag mode=$mode" | tee -a "$dest/bench.log"
  local engines extra
  if [ "$mode" = "p99" ]; then
    engines="turso"
  else
    engines="turso turso-gc"
  fi
  for connections in $CONNECTIONS; do
    for engine in $engines; do
      for run in $(seq 1 "$REPEATS"); do
        sync
        fstrim "$mount" 2>/dev/null || true
        sleep "$IDLE"
        echo 3 >/proc/sys/vm/drop_caches 2>/dev/null || true
        echo "running $engine at $RATE transactions/s over $connections connection(s), run $run of $REPEATS" | tee -a "$dest/bench.log"
        extra=""
        local eng_flag="$engine"
        if [ "$engine" = "turso-gc" ]; then
          eng_flag="turso"
          extra="--group-commit"
        fi
        "$bin" --engine "$eng_flag" --rate "$RATE" --connections "$connections" \
          --checkpointer 1000 --batch-size 10 \
          --duration "$DURATION" --warmup "$WARMUP" --run "$run" \
          --arrivals "$ARRIVALS" --seed "$SEED" --db-dir "$dest/db" --out-dir "$dest" \
          $extra >> "$dest/bench.log" 2>&1
      done
    done
  done
  extract_metrics "$dest/bench.log" | tee "$dest/metrics.tsv"
}

apply_group_commit_flag() {
  # The engine pragma exists on BASE. The CLI flag was an uncommitted harness
  # patch and git checkout drops it. Re-apply after every ours-mode checkout.
  python3 - <<'PY'
from pathlib import Path

main = Path("/mnt/nvme/limbo/perf/latency/main.rs")
eng = Path("/mnt/nvme/limbo/perf/latency/turso_engine.rs")
t = main.read_text()
if "group_commit:" not in t:
    t = t.replace(
        """    io: String,
}""",
        """    io: String,

    #[arg(
        long = "group-commit",
        help = "Enable MVCC group commit (PRAGMA mvcc_group_commit). Turso only"
    )]
    group_commit: bool,
}""",
        1,
    )
    t = t.replace(
        """    pub max_overrun: f64,
}""",
        """    pub max_overrun: f64,
    pub group_commit: bool,
}""",
        1,
    )
    t = t.replace(
        """        max_overrun: args.max_overrun,
    };""",
        """        max_overrun: args.max_overrun,
        group_commit: args.group_commit,
    };""",
        1,
    )
    t = t.replace(
        """    let engine_label = match args.engine {
        Engine::Sqlite => "sqlite",
        Engine::Turso => "turso",
    };""",
        """    let engine_label = match args.engine {
        Engine::Sqlite => "sqlite",
        Engine::Turso if args.group_commit => "turso-gc",
        Engine::Turso => "turso",
    };""",
        1,
    )
    main.write_text(t)

t = eng.read_text()
needle = """        if config.checkpointer.is_some() {
            // -1 turns the writer's auto-checkpoint off; the checkpointer
            // connection does it instead.
            conn.pragma_update("mvcc_checkpoint_threshold", -1)
                .await
                .unwrap();
        }
    }"""
insert = """        if config.checkpointer.is_some() {
            // -1 turns the writer's auto-checkpoint off; the checkpointer
            // connection does it instead.
            conn.pragma_update("mvcc_checkpoint_threshold", -1)
                .await
                .unwrap();
        }
        if config.group_commit {
            conn.execute("PRAGMA mvcc_group_commit = on", ()).await.unwrap();
        }
    }"""
if "mvcc_group_commit" not in t:
    if needle not in t:
        raise SystemExit("turso_engine.rs setup() did not match")
    eng.write_text(t.replace(needle, insert, 1))
print("group-commit flag patched")
PY
}

reset_base() {
  cd "$REPO"
  git cherry-pick --abort 2>/dev/null || true
  git checkout -f "$BASE"
}

echo "cargo=$(command -v cargo)"
cd "$REPO"
git remote get-url penberg 2>/dev/null || git remote add penberg https://github.com/penberg/turso.git
git fetch --depth=200 penberg p99
BASE=34fbedafc1c161018200a28f1a11d94c95d1981d
echo "BASE=$BASE"
echo "$BASE" > "$OUT/base.sha"
reset_base

log_row "frame" "metric is turso-gc p99.9 at 16 connections, lower better; secondary is turso no-GC achieved tx/s at 16" "collapse vs Pekka gap" "RATE=1000 CONNECTIONS=8,16 REPEATS=1 DURATION=30 IDLE=20" "open"

declare -a COMMITS=(
  "ed7270f2e951|ckpt-reclaim|free materialized row versions under passive checkpoints"
  "8cfdd7828f36|rightmost-leaf|append rows at the rightmost leaf without re-seeking"
  "43944f0f093d|keep-cursors|keep B-tree cursors across statement executions"
)

echo "===== baseline on $BASE ====="
run_bench baseline ours
log_row "harness" "recorded screening baseline" "frozen harness before any cherry-pick" "$OUT/baseline/metrics.tsv" "baseline captured"

for spec in "${COMMITS[@]}"; do
  sha="${spec%%|*}"
  rest="${spec#*|}"
  tag="${rest%%|*}"
  desc="${rest#*|}"
  echo "===== attempt $tag ====="
  reset_base
  if ! git cherry-pick "$sha"; then
    git cherry-pick --abort 2>/dev/null || true
    reset_base
    log_row "attempt" "cherry-pick $tag failed" "cannot measure" "$sha" "INCONCLUSIVE"
    continue
  fi
  run_bench "$tag" ours
  log_row "attempt" "measured $tag ($desc)" "one cherry-pick on BASE, same harness" "$sha $OUT/$tag/metrics.tsv" "measured"
  reset_base
done

echo "===== p99 full vs p99 without park-waiters ====="
cd "$REPO"
git checkout -f penberg/p99
run_bench p99-full p99
log_row "attempt" "measured p99 tip as Turso with group commit and park-waiters baked in" "upper bound, many commits at once" "penberg/p99 $OUT/p99-full/metrics.tsv" "measured"

git revert --no-edit 911a3cf989d3
run_bench p99-no-park p99
log_row "attempt" "measured p99 with park-waiters reverted" "isolates 911a3cf on the branch where it compiles" "revert 911a3cf $OUT/p99-no-park/metrics.tsv" "measured"

reset_base

{
  echo "BASE $BASE"
  echo "=== baseline ==="
  cat "$OUT/baseline/metrics.tsv" 2>/dev/null || true
  for spec in "${COMMITS[@]}"; do
    rest="${spec#*|}"
    tag="${rest%%|*}"
    echo "=== $tag ==="
    cat "$OUT/$tag/metrics.tsv" 2>/dev/null || true
  done
  echo "=== p99-full ==="
  cat "$OUT/p99-full/metrics.tsv" 2>/dev/null || true
  echo "=== p99-no-park ==="
  cat "$OUT/p99-no-park/metrics.tsv" 2>/dev/null || true
} | tee "$SUMMARY"

log_row "stop" "finished screening loop" "three cherry-picks on BASE plus p99 park-waiters revert" "$SUMMARY" "done"
echo HILLCLIMB_DONE
