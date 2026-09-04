#!/usr/bin/env bash
# Rerun CI's Elle list-append (mvcc-passive) locally, then elle-cli SI check.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WHOPPER="${CARGO_TARGET_DIR:-$ROOT/target}/debug/turso_whopper"
OUT="${1:-$ROOT/.audit/elle-loop-tick2}"
FAULT="${2:-0.05}"
mkdir -p "$OUT/elle-results"
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk}"
cd "$ROOT"
set +e
"$WHOPPER" \
  --elle list-append \
  --elle-output "$OUT/elle-history.edn" \
  --max-steps 100000 \
  --enable-mvcc \
  --enable-experimental-mvcc-passive-checkpoint \
  --mvcc-checkpoint-threshold 1024 \
  --allocation-fault-probability "$FAULT"
whopper_rc=$?
set -e
echo "whopper_exit=$whopper_rc fault=$FAULT"
if [ ! -f "$OUT/elle-history.edn" ]; then
  echo "AGENT_LOOP_WAKE_ci-ec2 {\"prompt\":\"Elle whopper failed before writing history. Continue unique-miss fix. Never merge.\"}"
  exit "$whopper_rc"
fi
JAR="$(ls -t /tmp/elle-cli/target/*-standalone.jar | head -1)"
java -jar "$JAR" \
  --model list-append \
  --consistency-models snapshot-isolation \
  --verbose \
  --directory "$OUT/elle-results" \
  "$OUT/elle-history.edn" | tee "$OUT/elle-cli.txt"
echo "AGENT_LOOP_WAKE_ci-ec2 {\"prompt\":\"Elle local SI check finished in $OUT. Read elle-cli.txt, keep driving PR 8622 unique miss, never merge, re-arm.\"}"
