#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

python3 tools/bazel/check_cargo_bazel_parity_test.py
python3 tools/bazel/check_cargo_bazel_parity.py
CARGO_BAZEL_REPIN=1 bazel mod deps --lockfile_mode=update >/dev/null
git diff --exit-code -- MODULE.bazel.lock ':(glob)**/cargo-bazel-lock.json'
