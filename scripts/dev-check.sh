#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 || ! $1 =~ ^[a-zA-Z0-9_][a-zA-Z0-9_-]*$ || -z $2 || $2 == -* ]]; then
    echo "Usage: bash scripts/dev-check.sh PACKAGE LIB_TEST_FILTER [FEATURES]" >&2
    exit 2
fi
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
if [[ ! $CARGO_BUILD_JOBS =~ ^[1-9][0-9]*$ ]]; then
    echo "CARGO_BUILD_JOBS must be a positive integer" >&2
    exit 2
fi
export CARGO_PROFILE_DEV_DEBUG=${CARGO_PROFILE_DEV_DEBUG:-line-tables-only}
export CARGO_PROFILE_TEST_DEBUG=${CARGO_PROFILE_TEST_DEBUG:-line-tables-only}
cd "$(dirname "${BASH_SOURCE[0]}")/.."
package=$1
filter=$2
features=()
if [[ $# == 3 ]]; then
    features=(--features "$3")
fi
trap 'status=$?; echo "Failed: $stage (exit $status)" >&2; exit "$status"' ERR
stage=fmt
cargo fmt --all -- --check
stage=check
cargo check --locked -p "$package" --lib "${features[@]}"
stage=test
cargo test --locked -p "$package" --lib "${features[@]}" "$filter" -- --test-threads=1
stage=clippy
cargo clippy --locked -p "$package" --lib "${features[@]}" -- --deny=warnings
echo "Targeted library verification passed for $package; review the test count for your filter."
