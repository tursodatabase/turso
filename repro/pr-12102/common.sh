# Shared settings for the scripts in this directory. The scripts source this file.

# The PR head that was measured. core/mvcc/cursor.rs is the same file at the
# later PR head 8f3b7ef741ea9d952291a0865c3b91405318edd1.
PR_COMMIT=a4c5e653b8b01f0d87690929291334119418e15d
# The merge base of PR_COMMIT with main. The "base" variant uses its cursor.rs.
BASE_COMMIT=91d399c41142057539681a49f58b9d30049179e2

KIT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=${REPO:-$(git -C "$KIT" rev-parse --show-toplevel)}
# The build tree, the binaries and the outputs go outside the repository.
WORK=${WORK:-$(dirname "$REPO")/pr-12102-repro-work}
TREE=$WORK/tree
BIN=$WORK/bin
OUT=$WORK/out

# Reads tursodb output and prints the time of each statement that `.timer on`
# reports, in seconds, one statement on each line.
timer_seconds() {
  grep 'Execution:' | sed 's/.*total=//' | awk '{
    v = $1; u = $2
    if (u == "ns") v /= 1e9; else if (u == "us") v /= 1e6; else if (u == "ms") v /= 1e3
    printf "%.6f\n", v
  }'
}

# Copies a database and its sidecar files to a new name.
copy_db() {
  local from=$1 to=$2 f
  rm -f "$to" "$to"-log "$to"-wal
  for f in "$from" "$from"-log "$from"-wal; do
    if [ -e "$f" ]; then cp "$f" "${to}${f#"$from"}"; fi
  done
}

require_binaries() {
  local name
  for name in "$@"; do
    if [ ! -x "$BIN/${name}_base" ] || [ ! -x "$BIN/${name}_pr" ]; then
      echo "Missing $BIN/${name}_base or $BIN/${name}_pr. Run build.sh first." >&2
      exit 1
    fi
  done
}
