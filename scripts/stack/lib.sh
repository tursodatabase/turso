# Shared helpers for the scripts in scripts/stack/. Source this file; do not run it.

# Demangles Rust symbol names on stdin and drops their `::h<hash>` suffix.
# Uses rustfilt when it is installed, otherwise c++filt. c++filt leaves the
# escapes of the legacy Rust mangling in place, e.g. `_$u7b$$u7b$closure$u7d$$u7d$`
# for `{{closure}}`, so they are decoded here.
demangle_rust() {
  if command -v rustfilt > /dev/null; then
    rustfilt | strip_rust_hash
    return
  fi
  if ! command -v c++filt > /dev/null; then
    echo "error: need rustfilt or c++filt on PATH to demangle symbols" >&2
    exit 1
  fi
  c++filt | sed -E \
    -e 's/\$u7b\$/{/g' -e 's/\$u7d\$/}/g' -e 's/\$LT\$/</g' -e 's/\$GT\$/>/g' \
    -e 's/\$u20\$/ /g' -e 's/\$u27\$/'"'"'/g' -e 's/\$u5b\$/[/g' -e 's/\$u5d\$/]/g' \
    -e 's/\$RF\$/\&/g' -e 's/\$BP\$/*/g' -e 's/\$C\$/,/g' -e 's/\.\./::/g' \
    -e 's/::_\{/::{/g' -e 's/(^|\t)_</\1</' \
    | strip_rust_hash
}

strip_rust_hash() {
  sed -E 's/::h[0-9a-f]{16}$//'
}

# Prints the largest stack, in KiB, that a run with `ulimit -s` can get: the
# hard limit of the shell, but at most 1 GiB.
max_stack_kib() {
  local max_kib
  max_kib=$(ulimit -H -s)
  if [ "$max_kib" = "unlimited" ] || [ "$max_kib" -gt 1048576 ]; then
    max_kib=1048576
  fi
  echo "$max_kib"
}

# Runs a SQL script on an in-memory database with a stack of the given size,
# and prints the exit code on the first line, then the output. KIND is tursodb
# or sqlite3. The CLI runs statements on the main thread, so `ulimit -s` sets
# the stack that they get.
run_with_stack() {
  local kind="$1" bin="$2" script="$3" kib="$4" out code
  if [ "$kind" = sqlite3 ]; then
    out=$( (ulimit -s "$kib" && "$bin" :memory: < "$script") 2>&1)
  else
    out=$( (ulimit -s "$kib" && "$bin" -q -m list :memory: < "$script") 2>&1)
  fi
  code=$?
  printf '%s\n%s' "$code" "$out"
}
