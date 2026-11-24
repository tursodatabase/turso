#!/bin/bash
# check-deep-queries.sh - Run the deepest queries that Turso permits with a small stack
#
# Usage:
#   scripts/stack/check-deep-queries.sh [-b BINARY] [-s KIB]
#
#   -b BINARY  tursodb binary to run (default: target/release/tursodb)
#   -s KIB     stack limit of each run (default: 2048, the stack that Rust gives
#              to a thread that it starts)
#
# Examples:
#   scripts/stack/check-deep-queries.sh
#   scripts/stack/check-deep-queries.sh -b /tmp/tursodb-main -s 1792
#
# Writes the scripts of scripts/stack/deep-queries.py, which nest queries as
# deeply as the limits of Turso permit. Then runs each script two times on an
# in-memory database: with the largest stack that the shell allows, and with
# KIB of stack. A script passes if the first run succeeds and the second run
# gives the same exit code and output. Prints one row for each script, and
# exits with status 1 if a script fails. Use a release build: debug frames are
# much larger. To see how much stack a script needs, give it to
# scripts/stack/min-stack.sh.

set -uo pipefail
source "$(dirname "$0")/lib.sh"

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

bin=target/release/tursodb
kib=2048
while getopts "b:s:h" opt; do
  case "$opt" in
    b) bin="$OPTARG" ;;
    s) kib="$OPTARG" ;;
    h) usage 0 ;;
    *) usage 1 ;;
  esac
done
shift $((OPTIND - 1))
if [ $# -ne 0 ]; then
  usage 1
fi
if [ ! -x "$bin" ]; then
  echo "error: $bin is not an executable" >&2
  exit 1
fi

workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT
if ! python3 "$(dirname "$0")/deep-queries.py" "$workdir" > /dev/null; then
  echo "error: scripts/stack/deep-queries.py failed" >&2
  exit 1
fi

max_kib=$(max_stack_kib)
failed=0
for script in "$workdir"/*.sql; do
  name=$(basename "$script" .sql)
  expected=$(run_with_stack tursodb "$bin" "$script" "$max_kib")
  if [ "$(head -n 1 <<< "$expected")" != 0 ]; then
    result="FAIL: the run with $max_kib KiB of stack fails"
    failed=1
  elif [ "$(run_with_stack tursodb "$bin" "$script" "$kib")" = "$expected" ]; then
    result="pass"
  else
    result="FAIL: the run with $kib KiB of stack fails"
    failed=1
  fi
  printf '%-20s %s\n' "$name" "$result"
done
exit "$failed"
