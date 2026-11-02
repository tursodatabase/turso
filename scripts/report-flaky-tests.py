#!/usr/bin/env python3
"""File an issue for each flaky test in a CI run's nextest JUnit reports.

A test is flaky when it failed and then passed on a retry in the same job, on
the same commit (nextest's FLAKY; retries are set in .config/nextest.toml).
Nothing else counts: a test that fails every attempt is a plain failure.

Each flaky test gets one open issue, titled "Flaky test: <binary> <test>".
When that issue is already open, the run is added to it as a comment instead.

Usage: report-flaky-tests.py [--dry-run] <dir>
<dir> holds one subdirectory per downloaded artifact (junit-Linux, ...), each
with a junit.xml somewhere below it and a job-name.txt naming the CI job that
wrote it. Without --dry-run, GH_TOKEN must be set.

Not everyone who reads the issues can open CI logs, so the issue itself shows
each attempt's result and the first failure's output.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

ISSUE_AUTHOR = "github-actions[bot]"
# For people filtering issues; nothing here reads it back.
ISSUE_LABEL = "flaky test"
# GitHub rejects bodies over 65536 characters; leave room for the template.
MAX_OUTPUT_CHARS = 30000


@dataclass
class FlakyRun:
    """One job's attempts at a test that failed and then passed."""

    platform: str
    job: str
    # The flakyFailure/flakyError elements, oldest first.
    failures: list
    # The testcase element; its timestamp and time are the passing attempt's.
    case: ET.Element


def flaky_tests(report_dir):
    """Map "<binary> <test>" to its flaky runs, one per job."""
    tests = {}
    for path in sorted(Path(report_dir).rglob("junit.xml")):
        artifact_dir = Path(report_dir) / path.relative_to(report_dir).parts[0]
        platform = artifact_dir.name.removeprefix("junit-")
        job_file = artifact_dir / "job-name.txt"
        job = job_file.read_text().strip() if job_file.exists() else f"unknown {platform} job"
        for case in ET.parse(path).iter("testcase"):
            if case.find("failure") is not None or case.find("error") is not None:
                continue
            failures = case.findall("flakyFailure") + case.findall("flakyError")
            if failures:
                failures.sort(key=lambda f: f.get("timestamp", ""))
                test = f"{case.get('classname')} {case.get('name')}"
                tests.setdefault(test, []).append(FlakyRun(platform, job, failures, case))
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


def occurrence(runs, job_urls):
    """The run's facts, each job's attempts, and the first failed attempt's output.

    Shared by the new issue and the comments on it.
    """
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_url = f"{server}/{repo}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"
    sha = os.environ.get("GITHUB_SHA", "")
    sections = [
        f"""| | |
|---|---|
| Commit | {sha} |
| Run | {run_url} |"""
    ]
    for run in runs:
        job = f"[{run.job}]({job_urls[run.job]})" if run.job in job_urls else f"`{run.job}`"
        sections.append(f"**{run.platform}**, job {job}:\n\n{attempts_table(run)}")
    sections.append(
        "In the job log, nextest prints each attempt as a `TRY <n> FAIL` or `TRY <n> PASS` line with the test name."
    )
    first = runs[0]
    sections.append(f"""<details><summary>Output of the first failed attempt ({first.platform})</summary>

{code_block(attempt_output(first.failures[0]))}

</details>""")
    return "\n\n".join(sections)


def attempts_table(run):
    rows = [(f, "FAIL") for f in run.failures] + [(run.case, "PASS")]
    lines = ["| Attempt | Result | Started | Duration |", "|---|---|---|---|"]
    for n, (element, result) in enumerate(rows, start=1):
        lines.append(f"| {n} | {result} | {element.get('timestamp', '?')} | {element.get('time', '?')}s |")
    return "\n".join(lines)


def issue_body(test, runs, job_urls):
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

{occurrence(runs, job_urls)}

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


def job_urls_by_name():
    run = os.environ["GITHUB_RUN_ID"]
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    out = gh(
        "--paginate",
        "--jq",
        ".jobs[] | {name, html_url}",
        f"repos/{{owner}}/{{repo}}/actions/runs/{run}/attempts/{attempt}/jobs?per_page=100",
    )
    jobs = [json.loads(line) for line in out.splitlines() if line.strip()]
    return {j["name"]: j["html_url"] for j in jobs}


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
    job_urls = {} if args.dry_run else job_urls_by_name()
    for test, runs in sorted(tests.items()):
        title = f"Flaky test: {test}"
        number = existing.get(title)
        if number is None:
            body = issue_body(test, runs, job_urls)
            if args.dry_run:
                print(f"=== new issue: {title}\n{body}\n")
            else:
                payload = {"title": title, "body": body, "labels": [ISSUE_LABEL]}
                issue = gh("--method", "POST", "--input", "-", "repos/{owner}/{repo}/issues", payload=payload)
                url = json.loads(issue)["html_url"]
                print(f"{title}: filed {url}")
        else:
            body = f"Flaky again.\n\n{occurrence(runs, job_urls)}"
            if args.dry_run:
                print(f"=== comment on #{number}: {title}\n{body}\n")
            else:
                path = f"repos/{{owner}}/{{repo}}/issues/{number}/comments"
                gh("--method", "POST", "--input", "-", path, payload={"body": body})
                print(f"{title}: commented on #{number}")


if __name__ == "__main__":
    main()
