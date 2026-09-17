#!/usr/bin/env python3
"""Write a tuned parameter set into `CostModelParams::new()`.

Only the fields named in the JSON change. The field order, the comments and
the layout of the Rust file stay as they are.
"""

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bench
import space

COST_PARAMS_RS = os.path.join(
    bench.REPO_ROOT, "core", "translate", "optimizer", "cost_params.rs"
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--params", required=True)
    parser.add_argument("--file", default=COST_PARAMS_RS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    params = space.repair({**space.DEFAULTS, **json.load(open(args.params))})
    source = open(args.file).read()
    body = between(source, "    pub const fn new() -> Self {", "\n    }")

    updated = body
    for name, value in params.items():
        pattern = re.compile(rf"^(\s*{name}: )[^,]+,$", re.MULTILINE)
        if not pattern.search(updated):
            sys.exit(f"{name} is not set in CostModelParams::new()")
        updated = pattern.sub(lambda m: f"{m.group(1)}{rust_float(value)},", updated)

    for line_before, line_after in zip(body.splitlines(), updated.splitlines()):
        if line_before != line_after:
            print(f"-{line_before}\n+{line_after}")
    if args.dry_run:
        return
    open(args.file, "w").write(source.replace(body, updated))
    print(f"\nwrote {args.file}")


def between(text, start_marker, end_marker):
    start = text.index(start_marker) + len(start_marker)
    return text[start : text.index(end_marker, start)]


def rust_float(value):
    """Format a number the way the Rust file writes its constants."""
    if value == int(value) and abs(value) >= 1000:
        return f"{int(value):_}.0"
    if value == int(value):
        return f"{int(value)}.0"
    return repr(round(value, 8))


if __name__ == "__main__":
    main()
