#!/bin/bash
# hir-conformance.sh - run the .sqltest corpora with SELECT prepared through HIR
#
# Usage:
#   scripts/hir-conformance.sh [output-dir]
#
# Runs every .sqltest file in its own process with TURSO_EXPERIMENTAL_HIR set,
# so a crash or hang in one file does not hide the results of the others.
# Each file gets <name>.json (sqltest JSON output), <name>.err, and <name>.exit
# in the output directory. Files are killed after 10 minutes. The summary at
# the end groups failures by error message.
#
# Requires python3. Builds the sqltest runner in debug mode first.

set -u

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${1:-$ROOT/target/hir-conformance}
JOBS=${JOBS:-4}

mkdir -p "$OUT"
rm -f "$OUT"/*.json "$OUT"/*.err "$OUT"/*.exit

cargo build -q --manifest-path "$ROOT/testing/sqltest/Cargo.toml" --bin sqltest || exit 1

run_one() {
  name=$(echo "$1" | tr / _)
  TURSO_EXPERIMENTAL_HIR=1 timeout 600 "$ROOT/target/debug/sqltest" run "$1" \
    --backend rust -j 1 -o json --snapshot-filter __never__ \
    > "$OUT/$name.json" 2> "$OUT/$name.err"
  echo $? > "$OUT/$name.exit"
}
export -f run_one
export ROOT OUT

cd "$ROOT/sqlite/conformance" || exit 1
find sqlite-sqltests turso-sqltests -name "*.sqltest" | sort |
  xargs -P "$JOBS" -I{} bash -c 'run_one {}'

python3 - "$OUT" <<'EOF'
import collections, glob, json, os, re, sys

out = sys.argv[1]
outcomes = collections.Counter()
causes = collections.Counter()
examples = {}
crashed = []
for path in sorted(glob.glob(os.path.join(out, "*.json"))):
    try:
        report = json.load(open(path))
    except ValueError:
        crashed.append(os.path.basename(path)[: -len(".json")])
        continue
    for file in report["files"]:
        for result in file["results"]:
            outcomes[result["outcome"]] += 1
            if result["outcome"] != "failed":
                continue
            reason = result.get("reason", "")
            match = re.search(r"(Internal error|Parse error|Runtime error)[:\s]+([^\n]*)", reason)
            cause = "wrong result" if match is None else match.group(1) + ": " + match.group(2)
            cause = re.sub(r"\d+", "N", cause)[:100]
            causes[cause] += 1
            examples.setdefault(cause, f'{file["path"]}: {result["name"]}')

print(dict(outcomes))
print("files that crashed or timed out:", crashed)
for cause, count in causes.most_common():
    print(f"{count:5}  {cause}\n       e.g. {examples[cause]}")
EOF
