#!/usr/bin/env bash

set -euo pipefail

runner="$TEST_SRCDIR/$TEST_WORKSPACE/$1"
tmp_dir="${TEST_TMPDIR:?}"
fake_test="$tmp_dir/fake-test"
trace="$tmp_dir/trace"

cat >"$fake_test" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--list" ]]; then
  [[ -z "${FAIL_LIST:-}" ]] || exit 1
  printf '%s\n' 'alpha: test' 'beta: test' 'gamma: test' 'ignored: test'
  exit 0
fi
printf '%s\n' "$2" >>"$TRACE"
[[ "${FAIL_TEST:-}" != "$2" ]]
EOF
chmod +x "$fake_test"
export TRACE="$trace"

for shard_index in 0 1 2; do
  TEST_TOTAL_SHARDS=3 \
    TEST_SHARD_INDEX="$shard_index" \
    TEST_SHARD_STATUS_FILE="$tmp_dir/status-$shard_index" \
    "$runner" "$fake_test"
  test -f "$tmp_dir/status-$shard_index"
done

diff -u <(printf '%s\n' alpha beta gamma ignored) <(sort "$trace")

if FAIL_LIST=1 "$runner" "$fake_test"; then
  echo "runner ignored a test-listing failure" >&2
  exit 1
fi

>"$trace"
if FAIL_TEST=beta "$runner" "$fake_test"; then
  echo "runner ignored a test failure" >&2
  exit 1
fi
diff -u <(printf '%s\n' alpha beta gamma ignored) <(sort "$trace")
