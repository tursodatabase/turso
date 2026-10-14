#!/bin/bash
# stack-profile.sh - Which functions use the stack when a SQL script needs the most of it
#
# Usage:
#   scripts/stack/stack-profile.sh [-b BINARY] [-s KIB] [-n TOP] SQL_FILE
#
#   -b BINARY  tursodb binary to run (default: target/release/tursodb)
#   -s KIB     stack limit to run with; must be small enough to overflow
#              (default: the largest limit, in steps of 8 KiB below what
#              scripts/stack/min-stack.sh finds, that overflows under lldb)
#   -n TOP     print at most TOP functions (default: 25)
#
# Examples:
#   scripts/stack/stack-profile.sh upsert.sql
#   scripts/stack/stack-profile.sh -s 200 -n 10 deep-case.sql
#
# Runs the script under lldb with the stack limited by `ulimit -s`, so that it
# overflows at the point where it needs the most stack. Then it reads the
# stack pointer of every frame in the backtrace. The size of a frame is the
# distance from its stack pointer to the stack pointer of its caller. The
# result is the number of stack bytes per function, summed over all of its
# frames, which shows what to shrink. A function that recurses has many frames.
#
# Inlined functions have no frame of their own; their bytes are counted in
# the function they were inlined into. Use a release build: debug frames are
# much larger. Needs lldb.

set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

binary=target/release/tursodb
limit=""
top=25
while getopts "b:s:n:h" opt; do
  case "$opt" in
    b) binary="$OPTARG" ;;
    s) limit="$OPTARG" ;;
    n) top="$OPTARG" ;;
    h) usage 0 ;;
    *) usage 1 ;;
  esac
done
shift $((OPTIND - 1))
if [ $# -ne 1 ]; then
  usage 1
fi
script="$1"

if ! command -v lldb > /dev/null; then
  echo "error: needs lldb on PATH" >&2
  exit 1
fi
if [ ! -x "$binary" ]; then
  echo "error: $binary is not an executable" >&2
  exit 1
fi
if [ ! -f "$script" ]; then
  echo "error: $script does not exist" >&2
  exit 1
fi
workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT

launcher="$workdir/stack-limit-exec"
if ! cc -O1 -o "$launcher" "$(dirname "$0")/stack-limit-exec.c"; then
  echo "error: could not compile scripts/stack/stack-limit-exec.c; needs cc" >&2
  exit 1
fi
binary_path=$(cd "$(dirname "$binary")" && pwd)/$(basename "$binary")

# Runs the script under lldb with the given stack limit. Succeeds if it overflowed.
# The launcher sets the limit and then execs the binary; lldb follows the
# exec, which first stops the process, so it is continued once.
overflows_under_lldb() {
  lldb --batch \
    -o "settings set frame-format 'FRAME \${frame.sp} \${frame.pc} \${module.file.basename}\n'" \
    -o "process launch -i $script -- $1 $binary_path -q -m list :memory:" \
    -o "continue" \
    -k "image list -o -f $(basename "$binary")" \
    -k "thread backtrace -c 1000000" \
    -k "quit 1" \
    "$launcher" > "$workdir/lldb.txt" 2>&1 || true
  grep -q "stop reason = EXC_BAD_ACCESS\|stop reason = signal SIGSEGV" "$workdir/lldb.txt"
}

if [ -n "$limit" ]; then
  if ! overflows_under_lldb "$limit"; then
    echo "error: $script did not overflow a $limit KiB stack; pass a smaller -s" >&2
    exit 1
  fi
else
  # Step down from the smallest stack that runs the script until it
  # overflows. The first overflow is the one closest to the deepest point.
  needed=$("$(dirname "$0")/min-stack.sh" -b "$binary" "$script" | tail -n 1 | awk '{print $NF}')
  if [ "$needed" = crash ]; then
    echo "error: $script crashes even with the largest stack" >&2
    exit 1
  fi
  limit=$needed
  while true; do
    limit=$((limit - 4))
    if [ "$limit" -lt 16 ]; then
      echo "error: $script did not overflow under lldb even with 16 KiB" >&2
      exit 1
    fi
    if overflows_under_lldb "$limit"; then break; fi
  done
fi

nm -n "$binary" | awk '$2 ~ /^[tT]$/ { print $1, $3 }' > "$workdir/symbols.txt"

# `image list -o` prints how far the binary was moved from the addresses in
# its symbol table, e.g. "[  0] 0x0000000000000000 /path/to/tursodb".
slide=$(awk -v bin="$binary_path" '/^\[ *[0-9]+\] 0x[0-9a-fA-F]+ / && $NF == bin { print $(NF - 1); exit }' \
  "$workdir/lldb.txt")
if [ -z "$slide" ]; then
  echo "error: lldb did not report where $binary_path was loaded" >&2
  exit 1
fi

# The stop report prints the innermost frame once before the backtrace,
# which starts at the frame marked with "*".
awk '/^ *\* FRAME / { on = 1 } on && /FRAME / { sub(/^ *(\* )?FRAME /, ""); print }' \
  "$workdir/lldb.txt" > "$workdir/frames.txt"

awk -v slide="$slide" -v module="$(basename "$binary")" '
  function hex(s,   i, c, v) {
    s = tolower(s)
    sub(/^0x/, "", s)
    v = 0
    for (i = 1; i <= length(s); i++) {
      c = index("0123456789abcdef", substr(s, i, 1)) - 1
      v = v * 16 + c
    }
    return v
  }
  # Index of the last symbol that starts at or before addr.
  function lookup(addr,   lo, hi, mid) {
    lo = 1
    hi = nsym
    while (lo < hi) {
      mid = int((lo + hi + 1) / 2)
      if (sym_addr[mid] <= addr) lo = mid; else hi = mid - 1
    }
    return lo
  }
  FILENAME == ARGV[1] { nsym++; sym_addr[nsym] = hex($1); sym_name[nsym] = $2; next }
  {
    # Frames of inlined functions share the stack pointer of the function
    # they were inlined into. Keep one frame per stack pointer.
    sp = hex($1)
    if (n > 0 && sp == frame_sp[n]) next
    n++
    frame_sp[n] = sp
    if ($3 == module) {
      frame_name[n] = sym_name[lookup(hex($2) - hex(slide))]
    } else {
      frame_name[n] = "<" $3 ">"
    }
  }
  END {
    # Frame 1 is the innermost one. The stack grows down, so the caller of
    # frame i has the higher stack pointer frame_sp[i + 1].
    for (i = 1; i < n; i++) {
      size = frame_sp[i + 1] - frame_sp[i]
      bytes[frame_name[i]] += size
      frames[frame_name[i]]++
      total += size
    }
    printf "%d\t%d\tTOTAL\n", total, n
    for (f in bytes) printf "%d\t%d\t%s\n", bytes[f], frames[f], f
  }
' "$workdir/symbols.txt" "$workdir/frames.txt" | demangle_rust > "$workdir/summary.txt"

read -r total frames _ < <(grep "	TOTAL$" "$workdir/summary.txt")
echo "Overflowed a $limit KiB stack with $frames frames, $total bytes in frames."
echo
printf '%8s  %6s  %9s  %s\n' "bytes" "frames" "per frame" "function"
grep -v "	TOTAL$" "$workdir/summary.txt" | sort -t$'\t' -k1,1 -rn | head -n "$top" \
  | awk -F'\t' '{ printf "%8d  %6d  %9d  %s\n", $1, $2, $1 / $2, $3 }'
