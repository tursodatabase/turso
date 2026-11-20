#!/usr/bin/env bash
# Runs the cursor tests on both variants and prints how many pass and fail:
#   - the 35 tests in cursor_repro_tests.rs (the four claims of the PR)
#   - the 89 new unit-test cases of the PR
#   - the 6 new integration tests of the PR
# Then prints each row sequence that base gets wrong.
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_binaries unit int
D=$OUT/cursor_tests
mkdir -p "$D"

PR_UNIT_TESTS=(
  test_cursor_steps_back_after_stepping_forward
  test_cursor_steps_on_from_row_found_by_exists
  test_cursor_steps_on_from_row_it_inserted
  test_exact_index_seek_can_continue_across_mvcc_and_btree_rows
  test_exact_index_seek_can_continue_past_shadowed_target
  test_exact_index_seek_on_mvcc_only_key_keeps_btree_rows
  test_exact_index_seek_on_mvcc_only_keys_across_pages
  test_exact_index_seek_steps_past_matching_rows
  test_exact_index_seek_with_duplicate_and_null_prefixes
  test_exact_seek_miss_can_move_in_opposite_direction
  test_index_cursor_steps_forward_after_stepping_back_past_deleted_row
  test_index_delete_reaches_btree_rows_after_mvcc_only_keys
)
PR_INTEGRATION_TESTS=(
  statement_lifecycle::index_method_exact_seek_can_continue_through_mvcc_rows
  statement_lifecycle::index_method_exact_seek_can_continue_backward_through_mvcc_rows
  statement_lifecycle::index_method_exists_can_continue_through_mvcc_and_btree_rows
  mvcc::database::test_exact_index_seek_over_shadowed_btree_rows
  mvcc::database::test_index_insert_over_shadowed_btree_entries
  mvcc::database::test_exact_unique_index_seek_over_shadowed_btree_rows
)

# Prints "<n> ok <m> FAILED" for the unique test names in a log.
summary() {
  grep -E '^test .* (ok|FAILED)$' "$1" | sort -u \
    | awk '{n[$NF]++} END {printf "%d ok, %d FAILED\n", n["ok"], n["FAILED"]}'
}

for v in base pr; do
  echo "== $v"
  "$BIN/unit_$v" mvcc::database::tests::repro_claim --test-threads=4 --nocapture \
    > "$D/repro_claims.$v.log" 2>&1 || true
  echo "  35 tests of the four claims:         $(summary "$D/repro_claims.$v.log")"
  : > "$D/pr_unit_tests.$v.log"
  for t in "${PR_UNIT_TESTS[@]}"; do
    "$BIN/unit_$v" "mvcc::database::tests::$t" --test-threads=4 \
      >> "$D/pr_unit_tests.$v.log" 2>&1 || true
  done
  echo "  89 new unit-test cases of the PR:    $(summary "$D/pr_unit_tests.$v.log")"
  : > "$D/pr_integration_tests.$v.log"
  for t in "${PR_INTEGRATION_TESTS[@]}"; do
    # On the PR, test_exact_index_seek_over_shadowed_btree_rows takes about 2 minutes
    # in a debug build. On base it fails at its first check.
    "$BIN/int_$v" "$t" --exact --test-threads=1 >> "$D/pr_integration_tests.$v.log" 2>&1 || true
  done
  echo "  6 new integration tests of the PR:   $(summary "$D/pr_integration_tests.$v.log")"
done

echo
echo "Row sequences that each variant gets wrong (actual differs from expected):"
for v in base pr; do
  echo "== $v"
  grep -h '^REPRO' "$D/repro_claims.$v.log" | sort | awk '{
    a = $0; sub(/.*actual=/, "", a); sub(/ expected=.*/, "", a)
    e = $0; sub(/.*expected=/, "", e)
    if (a != e) { print "  " $0; n++ }
  } END { if (n == 0) print "  none" }'
done
