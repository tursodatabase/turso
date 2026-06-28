#!/usr/bin/env python3
"""Squash a unified diff into an Antithesis targeted-coverage file.

Reads a unified diff (the ``git.diff`` produced from a GitHub ``compare`` API
call) and writes the changed new-side line ranges in the JSON shape Antithesis
uses to focus the fuzzer on a slice of the code:

    {"antithesis_targeted_coverage": {"locations": [
        {"file": "core/foo.rs", "begin_line": 10, "end_line": 13}, ...
    ]}}

Only Rust sources are emitted: the coverage-instrumented binary baked into the
workload (turso_stress) is pure Rust, so line ranges in lockfiles, snapshots,
YAML, TypeScript, etc. cannot map to anything the fuzzer can target. Deleted
files and pure-deletion hunks have no new-side lines and are skipped.
"""
import argparse
import json
import re
import sys

HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

SOURCE_SUFFIX = ".rs"


def squash(diff_text):
    locations = []
    current_file = None
    skip = False
    old_left = new_left = 0

    for line in diff_text.splitlines():
        if old_left > 0 or new_left > 0:
            head = line[:1]
            if head == "\\":
                continue
            if head == "+":
                new_left -= 1
            elif head == "-":
                old_left -= 1
            else:
                old_left -= 1
                new_left -= 1
            continue

        if line.startswith("diff --git"):
            current_file, skip = None, False
        elif line.startswith("+++ "):
            path = line[4:].split("\t", 1)[0].rstrip()
            if path == "/dev/null":
                current_file, skip = None, True
            else:
                current_file = path[2:] if path.startswith("b/") else path
                skip = False
        elif line.startswith("@@"):
            m = HUNK_RE.match(line)
            if not m:
                continue
            old_left = int(m.group(2)) if m.group(2) is not None else 1
            new_left = int(m.group(4)) if m.group(4) is not None else 1
            if skip or current_file is None or new_left == 0:
                continue
            if not current_file.endswith(SOURCE_SUFFIX):
                continue
            begin = int(m.group(3))
            locations.append(
                {
                    "file": current_file,
                    "begin_line": begin,
                    "end_line": begin + new_left - 1,
                }
            )

    return {"antithesis_targeted_coverage": {"locations": locations}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("diff", nargs="?", help="diff file to read (default: stdin)")
    parser.add_argument("-o", "--output", help="file to write JSON to (default: stdout)")
    args = parser.parse_args()

    if args.diff and args.diff != "-":
        with open(args.diff, encoding="utf-8", errors="replace") as f:
            diff_text = f.read()
    else:
        diff_text = sys.stdin.read()

    out = json.dumps(squash(diff_text), indent=2) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out)
    else:
        sys.stdout.write(out)


if __name__ == "__main__":
    main()
