#!/usr/bin/env bash
set -u

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <command> [args...]" >&2
  exit 1
fi

# Run the given command repeatedly until it exits with non‑zero status.
while "$@"; do
  :
done

echo "Command failed with exit code $?; stopping."
