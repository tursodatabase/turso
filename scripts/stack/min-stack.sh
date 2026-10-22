#!/bin/bash
# min-stack.sh - Smallest main-thread stack that runs a SQL script
#
# Usage:
#   scripts/stack/min-stack.sh [-b BINARY]... [-S] [-e SQL]... [SQL_FILE]...
#
#   -b BINARY  tursodb binary to measure; repeat to compare binaries
#              (default: target/release/tursodb)
#   -S         also measure the sqlite3 on PATH, as a reference
#   -e SQL     SQL text to run; may be repeated
#   SQL_FILE   SQL script to run; may be repeated
#
# Prints one row per script and one column per binary, in KiB.
#
# Examples:
#   scripts/stack/min-stack.sh -S upsert.sql
#   scripts/stack/min-stack.sh -b /tmp/tursodb-main -b target/release/tursodb -e "SELECT 1;" *.sql
#
# Each binary runs the script on an in-memory database with the stack limited
# by `ulimit -s`. The CLI runs statements on the main thread, so the limit
# applies to them. A run passes if its exit code and output equal those of a
# run with the largest stack the shell allows; this way a SQL error is not
# mistaken for a stack overflow. The result is a binary search to 4 KiB.
# "crash" means the script crashes even with the largest stack.
#
# The number includes everything the process needs before the first
# statement (for example, SELECT 1 alone needs some stack). Compare against
# that baseline, or against sqlite3 with -S. Use a release build: debug frames
# are much larger.

set -uo pipefail

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

binaries=()
with_sqlite=0
texts=()
while getopts "b:Se:h" opt; do
  case "$opt" in
    b) binaries+=("$OPTARG") ;;
    S) with_sqlite=1 ;;
    e) texts+=("$OPTARG") ;;
    h) usage 0 ;;
    *) usage 1 ;;
  esac
done
shift $((OPTIND - 1))
files=("$@")

if [ ${#binaries[@]} -eq 0 ]; then
  binaries=(target/release/tursodb)
fi
for bin in "${binaries[@]}"; do
  if [ ! -x "$bin" ]; then
    echo "error: $bin is not an executable" >&2
    exit 1
  fi
done
kinds=()
for _ in "${binaries[@]}"; do kinds+=(tursodb); done
if [ "$with_sqlite" -eq 1 ]; then
  if ! command -v sqlite3 > /dev/null; then
    echo "error: -S needs sqlite3 on PATH" >&2
    exit 1
  fi
  binaries+=("$(command -v sqlite3)")
  kinds+=(sqlite3)
fi
if [ ${#files[@]} -eq 0 ] && [ ${#texts[@]} -eq 0 ]; then
  usage 1
fi

workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT

scripts=()
labels=()
for f in "${files[@]+"${files[@]}"}"; do
  if [ ! -f "$f" ]; then
    echo "error: $f does not exist" >&2
    exit 1
  fi
  scripts+=("$f")
  labels+=("$(basename "$f")")
done
i=0
for text in "${texts[@]+"${texts[@]}"}"; do
  i=$((i + 1))
  printf '%s\n' "$text" > "$workdir/e$i.sql"
  scripts+=("$workdir/e$i.sql")
  label="$text"
  if [ ${#label} -gt 30 ]; then label="${label:0:27}..."; fi
  labels+=("$label")
done

max_kib=$(ulimit -H -s)
if [ "$max_kib" = "unlimited" ] || [ "$max_kib" -gt 1048576 ]; then
  max_kib=1048576
fi

# Prints the exit code and output of one run with the given stack size.
run() {
  local kind="$1" bin="$2" script="$3" kib="$4" out code
  if [ "$kind" = sqlite3 ]; then
    out=$( (ulimit -s "$kib" && "$bin" :memory: < "$script") 2>&1)
  else
    out=$( (ulimit -s "$kib" && "$bin" -q -m list :memory: < "$script") 2>&1)
  fi
  code=$?
  printf '%s\n%s' "$code" "$out"
}

measure() {
  local kind="$1" bin="$2" script="$3" expected lo hi mid
  expected=$(run "$kind" "$bin" "$script" "$max_kib")
  if [ "$(head -n 1 <<< "$expected")" -gt 128 ]; then
    echo crash
    return
  fi
  lo=0
  hi=$max_kib
  while [ $((hi - lo)) -gt 4 ]; do
    mid=$(((lo + hi) / 2))
    if [ "$(run "$kind" "$bin" "$script" "$mid")" = "$expected" ]; then hi=$mid; else lo=$mid; fi
  done
  echo "$hi"
}

width=6
for label in "${labels[@]}"; do
  if [ ${#label} -gt $width ]; then width=${#label}; fi
done

names=()
for bin in "${binaries[@]}"; do
  name=$(basename "$bin")
  if [ ${#name} -lt 6 ]; then name=$(printf '%6s' "$name"); fi
  names+=("$name")
done

printf "%-${width}s" "script"
for name in "${names[@]}"; do printf '  %s' "$name"; done
printf '\n'
for s in "${!scripts[@]}"; do
  printf "%-${width}s" "${labels[$s]}"
  for b in "${!binaries[@]}"; do
    result=$(measure "${kinds[$b]}" "${binaries[$b]}" "${scripts[$s]}")
    printf "  %${#names[$b]}s" "$result"
  done
  printf '\n'
done
