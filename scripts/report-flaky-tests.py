#!/usr/bin/env python3
"""File an issue for each flaky test in a CI run's nextest JUnit reports.

A test is flaky when it failed and then passed on a retry in the same job, on
the same commit (nextest's FLAKY; retries are set in .config/nextest.toml).
Nothing else counts: a test that fails every attempt is a plain failure.

Each flaky test gets one open issue, titled "Flaky test: <binary> <test>".
When that issue is already open, the run is added to it as a comment instead.

Usage: report-flaky-tests.py [--dry-run] <dir>
<dir> holds one subdirectory per downloaded artifact (junit-Linux, ...), each
with a junit.xml somewhere below it. Without --dry-run, GH_TOKEN must be set.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

ISSUE_AUTHOR = "github-actions[bot]"
# For people filtering issues; nothing here reads it back.
ISSUE_LABEL = "flaky test"
# GitHub rejects bodies over 65536 characters; leave room for the template.
MAX_OUTPUT_CHARS = 30000


def flaky_tests(report_dir):
    """Map "<binary> <test>" to its failed attempts: (platform, attempt XML element)."""
    tests = {}
    for path in sorted(Path(report_dir).rglob("junit.xml")):
        artifact = path.relative_to(report_dir).parts[0]
        platform = artifact.removeprefix("junit-")
        for case in ET.parse(path).iter("testcase"):
            if case.find("failure") is not None or case.find("error") is not None:
                continue
            attempts = case.findall("flakyFailure") + case.findall("flakyError")
            if attempts:
                test = f"{case.get('classname')} {case.get('name')}"
                tests.setdefault(test, []).extend((platform, a) for a in attempts)
    return tests


def attempt_output(attempt):
    # The element's own text is the panic message, which system-err repeats.
    parts = []
    for stream in ("system-out", "system-err"):
        node = attempt.find(stream)
        if node is not None and (node.text or "").strip():
            parts.append(f"--- {stream} ---\n{node.text.strip()}")
    output = "\n\n".join(parts) or (attempt.text or "").strip()
    if len(output) > MAX_OUTPUT_CHARS:
        output = "[... earlier output cut ...]\n" + output[-MAX_OUTPUT_CHARS:]
    return output


def code_block(text):
    longest = max((len(m) for m in re.findall(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}\n{text}\n{fence}"


def occurrence(attempts):
    """The run's facts and the first failed attempt's output, shared by issue and comment."""
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_url = f"{server}/{repo}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"
    sha = os.environ.get("GITHUB_SHA", "")
    counts = {}
    for platform, _ in attempts:
        counts[platform] = counts.get(platform, 0) + 1
    platforms = ", ".join(f"{p} ({n} failed attempt{'s' if n > 1 else ''})" for p, n in sorted(counts.items()))
    platform, first = attempts[0]
    return f"""| | |
|---|---|
| Commit | {sha} |
| Run | {run_url} |
| Platforms | {platforms} |

<details><summary>Output of the first failed attempt ({platform})</summary>

{code_block(attempt_output(first))}

</details>"""


def issue_body(test, attempts):
    binary, name = test.split(" ", 1)
    intro = (
        f"`{name}` in `{binary}` failed and then passed on a retry, in the same CI job on the same commit. "
        "A test that both fails and passes on the same code is flaky."
    )
    advice = (
        "CI retries hide the failure, so this issue is the only record of it. "
        "A flaky test in a database is often a real race or ordering bug: "
        "find the cause, rather than adding a retry, a sleep, or a longer timeout."
    )
    return f"""{intro}

{occurrence(attempts)}

Run it locally:

```
cargo nextest run --workspace --all-features -E 'binary_id({binary}) & test(={name})'
```

{advice}

Filed by `scripts/report-flaky-tests.py`. Later occurrences are added as comments."""


def gh(*args, payload=None):
    result = subprocess.run(
        ["gh", "api", *args],
        input=json.dumps(payload) if payload is not None else None,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error: gh api {args[0]} failed: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def open_issues_by_title():
    out = gh(
        "--paginate",
        "--jq",
        ".[] | select(.pull_request == null) | {number, title}",
        f"repos/{{owner}}/{{repo}}/issues?state=open&creator={quote(ISSUE_AUTHOR)}&per_page=100",
    )
    issues = [json.loads(line) for line in out.splitlines() if line.strip()]
    return {i["title"]: i["number"] for i in issues}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("report_dir")
    parser.add_argument("--dry-run", action="store_true", help="print the issues and comments instead of filing them")
    args = parser.parse_args()

    tests = flaky_tests(args.report_dir)
    if not tests:
        print("No flaky tests.")
        return
    existing = {} if args.dry_run else open_issues_by_title()
    for test, attempts in sorted(tests.items()):
        title = f"Flaky test: {test}"
        number = existing.get(title)
        if number is None:
            body = issue_body(test, attempts)
            if args.dry_run:
                print(f"=== new issue: {title}\n{body}\n")
            else:
                payload = {"title": title, "body": body, "labels": [ISSUE_LABEL]}
                issue = gh("--method", "POST", "--input", "-", "repos/{owner}/{repo}/issues", payload=payload)
                url = json.loads(issue)["html_url"]
                print(f"{title}: filed {url}")
        else:
            body = f"Flaky again.\n\n{occurrence(attempts)}"
            if args.dry_run:
                print(f"=== comment on #{number}: {title}\n{body}\n")
            else:
                path = f"repos/{{owner}}/{{repo}}/issues/{number}/comments"
                gh("--method", "POST", "--input", "-", path, payload={"body": body})
                print(f"{title}: commented on #{number}")


if __name__ == "__main__":
    main()
