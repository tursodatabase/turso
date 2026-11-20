#!/usr/bin/env bash
# Builds two variants of the same source tree and copies the binaries to $BIN:
#   pr    the PR head (PR_COMMIT) as committed
#   base  the same tree with core/mvcc/cursor.rs from the merge base (BASE_COMMIT)
# This is the method of the PR description: the two variants differ only in
# core/mvcc/cursor.rs. Both variants also contain cursor_repro_tests.rs,
# appended to core/mvcc/database/tests.rs.
#
# Binaries for each variant:
#   pull_<v>     sync engine test binary with the pull benchmark (bench-profile)
#   unit_<v>     turso_core unit tests (dev profile)
#   int_<v>      core_tester integration tests (dev profile)
#   tursodb_<v>  CLI (bench-profile)
set -euo pipefail
source "$(dirname "$0")/common.sh"

mkdir -p "$WORK" "$BIN"
echo "Work directory: $WORK"
git -C "$REPO" fetch --quiet origin "$PR_COMMIT" "$BASE_COMMIT"
if [ ! -d "$TREE" ]; then
  git -C "$REPO" worktree add --detach "$TREE" "$PR_COMMIT"
  cat "$KIT/cursor_repro_tests.rs" >> "$TREE/core/mvcc/database/tests.rs"
fi
TARGET=${CARGO_TARGET_DIR:-$TREE/target}

# Runs a cargo command in the tree and prints the path of the last executable
# that cargo reports.
cargo_executable() {
  local path
  path=$( (cd "$TREE" && cargo "$@" 2>&1) | tee -a "$WORK/build.log" \
    | sed -n 's/^ *Executable .*(\(.*\))$/\1/p' | tail -1)
  if [ -z "$path" ]; then
    echo "cargo $* did not report an executable. See $WORK/build.log." >&2
    exit 1
  fi
  case $path in
    /*) echo "$path" ;;
    *) echo "$TREE/$path" ;;
  esac
}

build_variant() {
  local v=$1
  echo "$(date +%T) building $v: pull benchmark"
  cp "$(cargo_executable test --locked --profile bench-profile -p turso_sync_engine \
    --test mvcc_logical_pull --no-run)" "$BIN/pull_$v"
  echo "$(date +%T) building $v: turso_core unit tests"
  cp "$(cargo_executable test --locked -p turso_core --lib --no-run)" "$BIN/unit_$v"
  echo "$(date +%T) building $v: integration tests"
  cp "$(cargo_executable test --locked -p core_tester --test integration_tests --no-run)" \
    "$BIN/int_$v"
  echo "$(date +%T) building $v: tursodb"
  (cd "$TREE" && cargo build --locked --profile bench-profile --bin tursodb 2>&1) \
    >> "$WORK/build.log"
  cp "$TARGET/bench-profile/tursodb" "$BIN/tursodb_$v"
}

restore_cursor() {
  git -C "$TREE" checkout --quiet -- core/mvcc/cursor.rs
}
trap restore_cursor EXIT

restore_cursor
build_variant pr
git -C "$TREE" show "$BASE_COMMIT:core/mvcc/cursor.rs" > "$TREE/core/mvcc/cursor.rs"
build_variant base
restore_cursor

# seek_past_current_row exists only in the cursor.rs of the PR.
if ! grep -qa seek_past_current_row "$BIN/unit_pr" || grep -qa seek_past_current_row "$BIN/unit_base"; then
  echo "The two unit-test binaries do not contain the expected cursor.rs." >&2
  exit 1
fi
echo "$(date +%T) done. Binaries are in $BIN."
