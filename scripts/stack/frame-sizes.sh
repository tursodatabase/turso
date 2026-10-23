#!/bin/bash
# frame-sizes.sh - Stack frame size of each function in a binary, read from its prologue
#
# Usage:
#   scripts/stack/frame-sizes.sh [-b BINARY] [-c BASELINE] [-n TOP] [PATTERN...]
#
#   -b BINARY    binary to inspect (default: target/release/tursodb)
#   -c BASELINE  also inspect BASELINE and print before, after, and the difference
#   -n TOP       print at most TOP rows (default: 30 without PATTERN, all with PATTERN)
#   PATTERN      extended regex matched against the demangled function name;
#                a function is printed if it matches any PATTERN
#
# Examples:
#   scripts/stack/frame-sizes.sh                       # 30 largest frames in tursodb
#   scripts/stack/frame-sizes.sh 'translate::expr::'   # all expression translation functions
#   scripts/stack/frame-sizes.sh -c /tmp/tursodb-main 'translate_expr$' 'binary_expr_shared$'
#
# Build first with `cargo build --release --bin tursodb`. Debug builds do not
# reuse stack slots, so their frame sizes say little about what users run.
#
# The size is the number of bytes the prologue moves the stack pointer by:
# saved registers plus locals. On x86_64 it does not include the 8-byte return
# address that the call pushes. A function that was inlined has no symbol; its
# locals are part of its caller's frame. Functions that recurse pay their frame
# size once per level.
#
# Supports aarch64 and x86_64 disassembly from llvm-objdump or GNU objdump.

set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

binary=target/release/tursodb
baseline=""
top=""
while getopts "b:c:n:h" opt; do
  case "$opt" in
    b) binary="$OPTARG" ;;
    c) baseline="$OPTARG" ;;
    n) top="$OPTARG" ;;
    h) usage 0 ;;
    *) usage 1 ;;
  esac
done
shift $((OPTIND - 1))
patterns=("$@")

if [ -z "$top" ]; then
  if [ ${#patterns[@]} -eq 0 ]; then top=30; else top=0; fi
fi

# Prints "<size>\t<demangled name>" for every function in the binary.
frame_sizes() {
  local bin="$1"
  if [ ! -f "$bin" ]; then
    echo "error: $bin does not exist" >&2
    exit 1
  fi
  objdump -d --no-show-raw-insn "$bin" | awk '
    function num(s,   i, c, v, base) {
      s = tolower(s)
      base = 10
      if (s ~ /^0x/) { base = 16; s = substr(s, 3) }
      v = 0
      for (i = 1; i <= length(s); i++) {
        c = index("0123456789abcdef", substr(s, i, 1)) - 1
        v = v * base + c
      }
      return v
    }
    function imm(line, prefix,   m) {
      if (!match(line, prefix "(0x[0-9a-fA-F]+|[0-9]+)")) return 0
      m = substr(line, RSTART + length(prefix), RLENGTH - length(prefix))
      return num(m)
    }
    function shifted(line, v) {
      return line ~ /lsl #12/ ? v * 4096 : v
    }
    function flush() {
      if (sym != "") print total "\t" sym
    }
    /^[0-9a-f]+ <.*>:$/ {
      flush()
      sym = $0
      sub(/^[0-9a-f]+ </, "", sym)
      sub(/>:$/, "", sym)
      total = 0; seen = 0; probe = 0; in_probe_loop = 0; probe_call_size = 0
      next
    }
    sym == "" || seen >= 40 { next }
    /^ *[0-9a-f]+:/ {
      seen++
      line = $0
      # aarch64: register saves with pre-decrement, e.g. stp x29, x30, [sp, #-0x60]!
      if (line ~ /\[sp, #-[0-9a-fx]+\]!/) { total += imm(line, "#-"); next }
      # aarch64: probe loop for very large frames, e.g. sub x9, sp, #0x10, lsl #12
      if (line ~ /sub[ \t]+x9, sp, #/) { probe = shifted(line, imm(line, "#")); in_probe_loop = 1; next }
      if (line ~ /cmp[ \t]+sp, x9/) { total += probe; in_probe_loop = 0; next }
      if (line ~ /mov[ \t]+sp, x9/) { total += probe; in_probe_loop = 0; next }
      # aarch64: sub sp, sp, #0x1, lsl #12
      if (line ~ /sub[ \t]+sp, sp, #/) { if (!in_probe_loop) total += shifted(line, imm(line, "#")); next }
      # x86_64: push %rbp
      if (line ~ /push[q]?[ \t]+%r/) { total += 8; next }
      # x86_64: probe loop, e.g. mov %rsp,%r11 / sub $0x10000,%r11
      if (line ~ /sub[q]?[ \t]+\$[0-9a-fx]+,%r11/) { probe = imm(line, "\\$"); in_probe_loop = 1; next }
      if (line ~ /cmp[q]?[ \t]+%r11,%rsp/) { total += probe; in_probe_loop = 0; next }
      # x86_64: probe function call, e.g. mov $0x2000,%eax / call __rust_probestack / sub %rax,%rsp
      if (line ~ /mov[l]?[ \t]+\$[0-9a-fx]+,%eax/) { probe_call_size = imm(line, "\\$"); next }
      if (line ~ /sub[q]?[ \t]+%rax,%rsp/) { total += probe_call_size; next }
      # x86_64: sub $0x88,%rsp
      if (line ~ /sub[q]?[ \t]+\$[0-9a-fx]+,%rsp/) { if (!in_probe_loop) total += imm(line, "\\$"); next }
    }
    END { flush() }
  ' | awk -F'\t' '$1 > 0' | demangle_rust | largest_per_name
}

# Generic functions have one copy per instantiation, all with the same demangled
# name. Keep the largest frame among them.
largest_per_name() {
  awk -F'\t' '
    !($2 in size) || $1 > size[$2] { size[$2] = $1 }
    END { for (name in size) print size[name] "\t" name }
  '
}

filter() {
  if [ ${#patterns[@]} -eq 0 ]; then
    cat
    return
  fi
  local regex
  regex=$(IFS='|'; echo "${patterns[*]}")
  awk -F'\t' -v re="$regex" '$NF ~ re'
}

limit() {
  if [ "$top" -gt 0 ]; then head -n "$top"; else cat; fi
}

if [ -z "$baseline" ]; then
  printf '%8s  %s\n' "bytes" "function"
  frame_sizes "$binary" | filter | sort -t$'\t' -k1,1 -rn | limit \
    | awk -F'\t' '{ printf "%8d  %s\n", $1, $2 }'
  exit 0
fi

after=$(mktemp)
before=$(mktemp)
trap 'rm -f "$after" "$before"' EXIT
frame_sizes "$binary" | filter > "$after"
frame_sizes "$baseline" | filter > "$before"

printf '%8s  %8s  %8s  %s\n' "before" "after" "diff" "function"
awk -F'\t' '
  NR == FNR { before[$2] = $1; next }
  { after[$2] = $1 }
  END {
    for (name in before) if (!(name in after)) after[name] = 0
    for (name in after) {
      b = (name in before) ? before[name] : 0
      printf "%d\t%d\t%d\t%s\n", b, after[name], after[name] - b, name
    }
  }
' "$before" "$after" | sort -t$'\t' -k2,2 -rn -k1,1 -rn | limit \
  | awk -F'\t' '{
      b = $1 == 0 ? "-" : $1
      a = $2 == 0 ? "-" : $2
      printf "%8s  %8s  %+8d  %s\n", b, a, $3, $4
    }'
