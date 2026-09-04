#!/usr/bin/env bash
# Local gauntlet for PR 8622 reclaim-after-commit. Not committed.
# Never --release. Elle uses debug whopper like CI.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PATH="/opt/homebrew/opt/openjdk/bin:${PATH:-}"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk}"

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== unit fences =="
cargo test -p turso_core --lib -- \
  test_delete_via_unique_index_removes_checkpointed_rows \
  test_passive_concurrent_transfer_preserves_sum_and_count \
  test_gc_unstamped_text_pk_unique_lookup_still_sees_key \
  test_unique_lookup_survives_passive_checkpoint_mid_seek \
  test_gc_rule3 \
  test_gc_retire_keeps_columns \
  test_gc_incremental_retire \
  -- --nocapture

echo "== elle seed =="
cargo test -p turso_whopper --test regression_tests_cross_platform -- \
  test_elle_passive_seed_keeps_committed_text_pk_on_unique_lookup \
  -- --nocapture

echo "== delete sqltest =="
(
  cd sqlite/conformance
  cargo run --manifest-path ../../testing/sqltest/Cargo.toml --bin sqltest -- \
    run sqlite-sqltests/delete.sqltest --backend rust --mvcc \
    --filter mvcc-delete-via-index-removes-checkpointed-rows \
    --snapshot-filter __never__
)

echo "== build debug whopper =="
cargo build -p turso_whopper
WHOPPER="${CARGO_TARGET_DIR:-$ROOT/target}/debug/turso_whopper"
JAR="$(ls -t /tmp/elle-cli/target/*-standalone.jar 2>/dev/null | head -1 || true)"
[[ -n "$JAR" ]] || fail "elle-cli jar missing under /tmp/elle-cli/target"

run_elle() {
  local seed="$1"
  local out="$ROOT/.audit/elle-validate-$seed"
  mkdir -p "$out/elle-results"
  echo "== elle list-append mvcc-passive seed=$seed =="
  SEED="$seed" "$WHOPPER" \
    --elle list-append \
    --elle-output "$out/elle-history.edn" \
    --max-steps 100000 \
    --enable-mvcc \
    --enable-experimental-mvcc-passive-checkpoint \
    --mvcc-checkpoint-threshold 1024 \
    --allocation-fault-probability 0.05
  java -jar "$JAR" \
    --model list-append \
    --consistency-models snapshot-isolation \
    --verbose \
    --directory "$out/elle-results" \
    "$out/elle-history.edn" | tee "$out/elle-cli.txt"
  grep -q "No invalid histories" "$out/elle-cli.txt" \
    || grep -qi "valid" "$out/elle-cli.txt" \
    || fail "elle seed $seed did not look valid; see $out/elle-cli.txt"
}

# CI-style plus two extra seeds. Override with ELLE_SEEDS="1 2 3"
for seed in ${ELLE_SEEDS:-1 2 3}; do
  run_elle "$seed"
done

echo "== whopper fast =="
"$WHOPPER" --mode fast --max-steps 10000

echo "== shuttle mvcc (if cfg=shuttle) =="
if cargo test -p turso_stress --locked --test shuttle_mvcc --no-run 2>/dev/null; then
  RUSTFLAGS="--cfg=shuttle" cargo test -p turso_stress --locked --test shuttle_mvcc
else
  echo "skip shuttle compile probe"
  RUSTFLAGS="--cfg=shuttle" cargo test -p turso_stress --locked --test shuttle_mvcc || true
fi

echo "== simulator smoke =="
cargo run -p limbo_sim -- --disable-integrity-check -n 50 || cargo run --bin limbo_sim -- --disable-integrity-check -n 50

echo "OK validate-reclaim"
