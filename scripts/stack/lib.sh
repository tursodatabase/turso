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
