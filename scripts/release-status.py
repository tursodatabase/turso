#!/usr/bin/env python3
"""Generate a Slack-friendly release status summary for a GitHub milestone."""

import argparse
from datetime import date
import json
import os
import subprocess
import sys
from urllib.request import urlopen, Request


def gh_api(endpoint):
    result = subprocess.run(
        ["gh", "api", "--paginate", endpoint],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Error: gh api failed: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def find_milestone(title):
    milestones = gh_api("repos/{owner}/{repo}/milestones?state=all&per_page=100")
    for m in milestones:
        if m["title"] == title:
            return m
    return None


def get_issues(milestone_number, state):
    return gh_api(
        f"repos/{{owner}}/{{repo}}/issues?milestone={milestone_number}"
        f"&state={state}&per_page=100&sort=created&direction=asc"
    )


def get_linked_prs(issue_numbers):
    """Batch-fetch linked PRs for issues using GitHub GraphQL API."""
    result = {}
    # GraphQL queries are limited in size, batch in chunks of 50
    for i in range(0, len(issue_numbers), 50):
        chunk = issue_numbers[i:i + 50]
        fields = "\n".join(
            f'i{n}: issue(number: {n}) {{'
            f'  number'
            f'  timelineItems(itemTypes: [CROSS_REFERENCED_EVENT], first: 10) {{'
            f'    nodes {{'
            f'      ... on CrossReferencedEvent {{'
            f'        source {{'
            f'          ... on PullRequest {{ number url state }}'
            f'        }}'
            f'      }}'
            f'    }}'
            f'  }}'
            f'}}'
            for n in chunk
        )
        query = f'{{ repository(owner: "tursodatabase", name: "turso") {{ {fields} }} }}'
        proc = subprocess.run(
            ["gh", "api", "graphql", "-f", f"query={query}"],
            capture_output=True, text=True
        )
        if proc.returncode != 0:
            print(f"Warning: GraphQL query failed: {proc.stderr.strip()}", file=sys.stderr)
            continue
        data = json.loads(proc.stdout).get("data", {}).get("repository", {})
        for key, issue_data in data.items():
            issue_num = issue_data["number"]
            prs = []
            for node in issue_data["timelineItems"]["nodes"]:
                source = node.get("source", {})
                if source.get("number"):
                    prs.append(source)
            if prs:
                result[issue_num] = prs
    return result


def format_issue(issue, linked_prs):
    labels = [l["name"] for l in issue["labels"]]
    assignees = [a["login"] for a in issue["assignees"]]

    url = issue["html_url"]
    parts = [f"{issue['title']} (<{url}|#{issue['number']}>)"]
    if assignees:
        parts.append(f"({', '.join(assignees)})")
    prs = linked_prs.get(issue["number"], [])
    if prs:
        pr = prs[0]  # show the most recent/first linked PR
        parts.append(f"→ <{pr['url']}|PR #{pr['number']}>")
    if "high priority" in labels:
        parts.append("[HIGH]")
    return "• " + " ".join(parts)


def slack_api(token, method, payload):
    url = f"https://slack.com/api/{method}"
    data = json.dumps(payload).encode()
    req = Request(url, data=data, headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    })
    resp = urlopen(req)
    body = json.loads(resp.read())
    if not body.get("ok"):
        print(f"Error: Slack {method} failed: {body.get('error')}", file=sys.stderr)
        sys.exit(1)
    return body


def post_message(token, channel, text, thread_ts=None):
    payload = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    return slack_api(token, "chat.postMessage", payload)


def main():
    parser = argparse.ArgumentParser(description="Release status for a GitHub milestone")
    parser.add_argument("milestone", help="Milestone title (e.g. '0.5')")
    parser.add_argument("--closed", action="store_true", help="Also show closed issues")
    parser.add_argument("--post", action="store_true",
                        help="Post to Slack via SLACK_BOT_TOKEN and SLACK_CHANNEL")
    args = parser.parse_args()

    token = os.environ.get("SLACK_BOT_TOKEN")
    channel = os.environ.get("SLACK_CHANNEL")
    if args.post and not token:
        print("Error: SLACK_BOT_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)
    if args.post and not channel:
        print("Error: SLACK_CHANNEL environment variable not set", file=sys.stderr)
        sys.exit(1)

    milestone = find_milestone(args.milestone)
    if not milestone:
        print(f"Error: milestone '{args.milestone}' not found", file=sys.stderr)
        sys.exit(1)

    open_issues = get_issues(milestone["number"], "open")
    closed_issues = get_issues(milestone["number"], "closed") if args.closed else []

    # Separate PRs from issues (GitHub API returns both)
    open_issues = [i for i in open_issues if "pull_request" not in i]
    closed_issues = [i for i in closed_issues if "pull_request" not in i]

    # Group open issues by label
    high_priority = []
    bugs = []
    features = []
    other = []

    for issue in open_issues:
        labels = {l["name"] for l in issue["labels"]}
        if "high priority" in labels:
            high_priority.append(issue)
        elif "bug" in labels:
            bugs.append(issue)
        elif "enhancement" in labels or "feature" in labels:
            features.append(issue)
        else:
            other.append(issue)

    # Fetch linked PRs for all open issues
    all_issue_numbers = [i["number"] for i in open_issues]
    print("Fetching linked PRs...", file=sys.stderr)
    linked_prs = get_linked_prs(all_issue_numbers)

    # Has PR / no PR
    has_pr = [i for i in open_issues if i["number"] in linked_prs]
    no_pr = [i for i in open_issues if i["number"] not in linked_prs]

    # Progress bar
    total = len(open_issues) + len(closed_issues) if args.closed else milestone["open_issues"] + milestone["closed_issues"]
    n_closed = milestone["closed_issues"]
    n_has_pr = len(has_pr)
    n_no_pr = len(no_pr)
    bar_len = 10
    closed_blocks = round(n_closed / total * bar_len) if total else 0
    pr_blocks = round(n_has_pr / total * bar_len) if total else 0
    no_pr_blocks = bar_len - closed_blocks - pr_blocks
    progress_bar = (
        ":large_green_square:" * closed_blocks
        + ":large_yellow_square:" * pr_blocks
        + ":white_large_square:" * no_pr_blocks
    )
    pct = round(n_closed / total * 100) if total else 0

    # Build header (main message)
    header_lines = []
    today = date.today().isoformat()
    header_lines.append(f"*Turso Release Status: {args.milestone}* ({today})")
    header_lines.append(f"{progress_bar} {pct}%")
    header_lines.append("")
    header_lines.append(f":large_green_square: Closed: {n_closed}")
    header_lines.append(f":large_yellow_square: Has PR: {n_has_pr}")
    header_lines.append(f":white_large_square: No PR: {n_no_pr}")
    if milestone.get("due_on"):
        header_lines.append(f"Due: {milestone['due_on'][:10]}")
    header = "\n".join(header_lines)

    # Build category messages (thread replies)
    categories = []
    if high_priority:
        lines = [f":rotating_light: *High Priority ({len(high_priority)})*"]
        for issue in high_priority:
            lines.append(format_issue(issue, linked_prs))
        categories.append("\n".join(lines))

    if bugs:
        lines = [f":bug: *Bugs ({len(bugs)})*"]
        for issue in bugs:
            lines.append(format_issue(issue, linked_prs))
        categories.append("\n".join(lines))

    if features:
        lines = [f":sparkles: *Features/Enhancements ({len(features)})*"]
        for issue in features:
            lines.append(format_issue(issue, linked_prs))
        categories.append("\n".join(lines))

    if other:
        lines = [f":clipboard: *Other ({len(other)})*"]
        for issue in other:
            lines.append(format_issue(issue, linked_prs))
        categories.append("\n".join(lines))

    if args.closed and closed_issues:
        lines = [f":white_check_mark: *Closed ({len(closed_issues)})*"]
        for issue in closed_issues:
            lines.append(format_issue(issue, linked_prs))
        categories.append("\n".join(lines))

    if args.post:
        resp = post_message(token, channel, header)
        thread_ts = resp["ts"]
        for cat in categories:
            post_message(token, channel, cat, thread_ts=thread_ts)
        print("Posted to Slack.")
    else:
        print(header)
        print()
        for cat in categories:
            print(cat)
            print()


if __name__ == "__main__":
    main()
