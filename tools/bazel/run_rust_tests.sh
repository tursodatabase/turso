#!/usr/bin/env bash

set -uo pipefail

if [[ "$1" = /* ]]; then
  binary="$1"
else
  binary="${RUNFILES_DIR:-$TEST_SRCDIR}/${TEST_WORKSPACE:-_main}/$1"
fi
shard_count="${TEST_TOTAL_SHARDS:-1}"
shard_index="${TEST_SHARD_INDEX:-0}"
status=0
test_list="${TEST_TMPDIR:-${TMPDIR:-/tmp}}/rust-test-list-$shard_index"
test_output="${TEST_TMPDIR:-${TMPDIR:-/tmp}}/rust-test-output-$shard_index"

if [[ -n "${TEST_SHARD_STATUS_FILE:-}" ]]; then
  touch "$TEST_SHARD_STATUS_FILE"
fi

"$binary" --list --format terse >"$test_list" || exit 1

while IFS= read -r line; do
  [[ "$line" == *": test" ]] || continue
  test_name="${line%: test}"
  read -r hash _ < <(printf '%s' "$test_name" | cksum)
  ((hash % shard_count == shard_index)) || continue
  if ! "$binary" --exact "$test_name" >"$test_output" 2>&1; then
    cat "$test_output"
    status=1
  fi
done <"$test_list"

exit "$status"
