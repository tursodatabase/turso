#!/usr/bin/env bash
set -euo pipefail

# Clean lines from simulator output by:
# 1) Removing everything up to and including "interaction="
# 2) Replacing everything from "}:" to the end with a single semicolon
# 3) Only retaining lines containing CREATE/INSERT/UPDATE/DELETE/DROP (the rest are usually meaningless for debugging)
#
# The purpose of this is to transform the interaction plan into a list of executable SQL statements
# in cases where:
# 1. Shrinking the plan failed
# 2. We know the point at which the simulator failure occurred.
#
# I use this script like this in the simulator directory:
# cargo run &> raw_output.txt
# manually edit out the shrinking parts and the WarGames intro graphics etc and save the file
# then run:
# ./clean_interactions.sh raw_output.txt > interactions.sql
#
# Usage:
#   clean_interactions.sh INPUT [OUTPUT]
#
# If OUTPUT is omitted, the result is written to stdout.

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 INPUT [OUTPUT]" >&2
  exit 1
fi

input_path="$1"
output_path="${2:-}"

if [[ -z "${output_path}" ]]; then
  awk '{ line=$0; sub(/^[^\n]*interaction=/, "", line); sub(/}:.*/, ";", line); print line }' "${input_path}" | grep -E 'CREATE|INSERT|UPDATE|DELETE|DROP'
else
  awk '{ line=$0; sub(/^[^\n]*interaction=/, "", line); sub(/}:.*/, ";", line); print line }' "${input_path}" | grep -E 'CREATE|INSERT|UPDATE|DELETE|DROP' > "${output_path}"
fi


