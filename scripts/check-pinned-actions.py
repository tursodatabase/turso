#!/usr/bin/env python3
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
USES = re.compile(r"""^\s*(?:-\s+)?uses:\s*["']?(?P<target>[^"'\s#]+)""")
FULL_COMMIT_SHA = re.compile(r"@[0-9a-f]{40}$")


def main():
    unpinned = [
        f"{path.relative_to(REPO_ROOT)}:{line_number}: {target}"
        for path in workflow_files()
        for line_number, target in action_references(path)
        if not FULL_COMMIT_SHA.search(target)
    ]
    if unpinned:
        print("These actions are not pinned to a full-length commit SHA:")
        print("\n".join(unpinned))
        print("Pin them, for example: uses: actions/checkout@<commit-sha> # v4.4.0")
        print("pinact (https://github.com/suzuki-shunsuke/pinact) can do this for you.")
        sys.exit(1)
    print("All actions are pinned to a full-length commit SHA.")


def workflow_files():
    github_dir = REPO_ROOT / ".github"
    return sorted(path for path in github_dir.rglob("*") if path.suffix in (".yml", ".yaml"))


def action_references(path):
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        match = USES.match(line)
        if match and not match["target"].startswith(("./", "docker://")):
            yield line_number, match["target"]


if __name__ == "__main__":
    main()
