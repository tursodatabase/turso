#!/usr/bin/env python3
#
# Copyright 2024 the Turso authors. All rights reserved. MIT license.
#
# A script to merge a pull requests with a nice merge commit using GitHub CLI.
#
# Requirements:
# - GitHub CLI (`gh`) must be installed and authenticated
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import textwrap
import time
from collections import Counter


def run_command(command, capture_output=True):
    if capture_output:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        return output.decode("utf-8").strip(), error.decode("utf-8").strip(), process.returncode
    else:
        return "", "", subprocess.call(command, shell=True)


def load_user_mapping(file_path=".github.json"):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    return {}


user_mapping = load_user_mapping()


def get_user_email(username):
    if username in user_mapping:
        return f"{user_mapping[username]['name']} <{user_mapping[username]['email']}>"

    # Try to get user info from gh CLI
    output, _, returncode = run_command(f"gh api users/{username}")
    if returncode == 0:
        user_data = json.loads(output)
        name = user_data.get("name", username)
        email = user_data.get("email")
        if email:
            return f"{name} <{email}>"
        return f"{name} (@{username})"

    # Fallback to noreply address
    return f"{username} <{username}@users.noreply.github.com>"


def get_pr_info(pr_number):
    output, error, returncode = run_command(
        f"gh pr view {pr_number} --json number,title,author,headRefName,body,reviews"
    )
    if returncode != 0:
        print(f"Error fetching PR #{pr_number}: {error}")
        sys.exit(1)

    pr_data = json.loads(output)

    reviewed_by = []
    for review in pr_data.get("reviews", []):
        if review["state"] == "APPROVED":
            reviewed_by.append(get_user_email(review["author"]["login"]))

    # Remove duplicates while preserving order
    reviewed_by = list(dict.fromkeys(reviewed_by))

    return {
        "number": pr_data["number"],
        "title": pr_data["title"],
        "author": pr_data["author"]["login"],
        "author_name": pr_data["author"].get("name", pr_data["author"]["login"]),
        "head": pr_data["headRefName"],
        "body": (pr_data.get("body") or "").strip(),
        "reviewed_by": reviewed_by,
    }


def load_pr_template(template_path=".github/pull_request_template.md") -> str:
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            # Normalize newlines
            return f.read().replace("\r\n", "\n").replace("\r", "\n")
    except FileNotFoundError:
        return ""

def truncate_body_at_marker(body: str) -> str:
    """
    Truncate PR body at the given markdown header marker (inclusive).
    Everything at or below the marker is removed.
    """
    if not body:
        return ""

    lines = body.split("\n")
    for i, line in enumerate(lines):
        if line.strip() == "### Description of AI":
            return "\n".join(lines[:i]).rstrip()

    return body.strip()

def strip_pr_template_from_body(body: str, template_path=".github/pull_request_template.md") -> str:
    """
    Remove unchanged PR template lines from the PR body.
    Strategy: line-level subtraction of template lines (multiset) outside of fenced
    code blocks. Lines that the author edited will not match and will be retained.
    """
    template = load_pr_template(template_path)
    if not template or not body:
        return (body or "").strip()

    # Normalize newlines and compare lines ignoring trailing whitespace
    template_lines = [ln.rstrip() for ln in template.split("\n")]
    body_lines = body.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    # Use a multiset so repeated lines (e.g., blank lines, repeated headings) are handled correctly
    tmpl_counts = Counter([ln for ln in template_lines if ln != ""])
    out_lines = []
    in_code_block = False

    for raw in body_lines:
        line = raw.rstrip()
        # Preserve code blocks verbatim
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            out_lines.append(raw)
            continue

        if in_code_block:
            out_lines.append(raw)
            continue

        # Drop lines that exactly match a template line (outside code blocks)
        if line != "" and tmpl_counts.get(line, 0) > 0:
            tmpl_counts[line] -= 1
            continue
        out_lines.append(raw)
    cleaned = "\n".join(out_lines).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned

def wrap_text(text, width=72):
    lines = text.split("\n")
    wrapped_lines = []
    in_code_block = False
    for line in lines:
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            wrapped_lines.append(line)
        elif in_code_block:
            wrapped_lines.append(line)
        else:
            wrapped_lines.extend(textwrap.wrap(line, width=width))
    return "\n".join(wrapped_lines)


def check_pr_status(pr_number):
    """Check the status of all checks for a PR

    Returns a tuple of (has_failing, has_pending) indicating if there are
    any failing or pending checks respectively.
    """
    output, error, returncode = run_command(f"gh pr checks {pr_number} --json state,name,startedAt,completedAt")
    if returncode != 0:
        print(f"Warning: Unable to get PR check status: {error}")
        return False, False

    checks_data = json.loads(output)
    if not checks_data:
        return False, False

    has_failing = any(check.get("state") == "FAILURE" for check in checks_data)
    has_pending = any(
        check.get("startedAt") and not check.get("completedAt") or check.get("state") == "IN_PROGRESS"
        for check in checks_data
    )
    return has_failing, has_pending


def run_gh_api_json(cmd):
    output, error, _ = run_command(cmd)
    try:
        result = json.loads(output)
    except ValueError:
        result = None
    if not isinstance(result, dict) or "status" not in result:
        print(f"Error calling the asynchronous merge API: {error or output}")
        sys.exit(1)
    return result


def request_async_merge(pr_number: int, commit_message: str, commit_title: str):
    payload = json.dumps(
        {
            "merge_method": "merge",
            "commit_title": commit_title,
            "commit_message": commit_message,
        }
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete_on_close=False) as temp_file:
        temp_file.write(payload)
        temp_file.close()
        path = shlex.quote(f"repos/{{owner}}/{{repo}}/pulls/{pr_number}/merge-async")
        cmd = f'gh api --method PUT {path} --input "{temp_file.name}"'
        return run_gh_api_json(cmd)


def get_async_merge_result(pr_number: int, uuid: str):
    path = shlex.quote(f"repos/{{owner}}/{{repo}}/pulls/{pr_number}/merge-async/{uuid}")
    cmd = f"gh api {path}"
    return run_gh_api_json(cmd)


def wait_for_async_merge(pr_number: int, result: dict, timeout_seconds: int) -> dict:
    deadline = time.monotonic() + timeout_seconds
    while result.get("status") == "pending":
        details = result.get("details") or {}
        uuid = details.get("uuid")
        if not uuid:
            print(f"Error merging PR: merge request is pending but has no UUID: {details.get('message', '')}")
            sys.exit(1)
        if time.monotonic() >= deadline:
            print(f"Timed out waiting for the merge to finish, check the PR on GitHub (merge request {uuid})")
            sys.exit(1)
        time.sleep(2)
        result = get_async_merge_result(pr_number, uuid)
    return result


def merge_remote(pr_number: int, commit_message: str, commit_title: str, timeout_seconds: int = 300):
    has_failing, has_pending = check_pr_status(pr_number)

    prompt_needed = False
    warning_msg = ""

    if has_failing:
        prompt_needed = True
        warning_msg = "Warning: Some checks are failing"
    elif has_pending:
        prompt_needed = True
        warning_msg = "Warning: Some checks are still running"

    if prompt_needed:
        print(warning_msg)
        if input("Do you want to proceed with the merge? (y/N): ").strip().lower() != "y":
            exit(0)

    print(f"\nMerging PR #{pr_number} with custom commit message...")
    result = request_async_merge(pr_number, commit_message, commit_title)
    result = wait_for_async_merge(pr_number, result, timeout_seconds)

    status = result.get("status")
    details = result.get("details") or {}
    message = details.get("message", "")

    if status == "merged":
        print(f"\nPull request #{pr_number} merged successfully!")
        sha = details.get("sha")
        if sha:
            print(f"Merge commit: {sha}")
        print(f"\nMerge commit message:\n{commit_title}\n\n{commit_message}")
        return
    if status == "enqueued":
        print(f"\nPull request #{pr_number} was added to the merge queue: {message}")
        return
    print(f"Error merging PR: {message or f'unexpected merge status {status!r}'}")
    sys.exit(1)


def merge_local(pr_number: int, commit_message: str):
    has_failing, has_pending = check_pr_status(pr_number)

    prompt_needed = False
    warning_msg = ""

    if has_failing:
        prompt_needed = True
        warning_msg = "Warning: Some checks are failing"
    elif has_pending:
        prompt_needed = True
        warning_msg = "Warning: Some checks are still running"

    if prompt_needed:
        print(warning_msg)
        if input("Do you want to proceed with the merge? (y/N): ").strip().lower() != "y":
            exit(0)

    current_branch, _, _ = run_command("git branch --show-current")

    print(f"Fetching PR #{pr_number}...")
    cmd = f"gh pr checkout {pr_number}"
    _, error, returncode = run_command(cmd)
    if returncode != 0:
        print(f"Error checking out PR: {error}")
        sys.exit(1)

    pr_branch, _, _ = run_command("git branch --show-current")

    cmd = "git checkout main"
    _, error, returncode = run_command(cmd)
    if returncode != 0:
        print(f"Error checking out main branch: {error}")
        sys.exit(1)

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write(commit_message)
        temp_file_path = temp_file.name

    try:
        # Merge the PR branch with the custom message
        # Using -F with the full message (title + body)
        cmd = f"git merge --no-ff {pr_branch} -F {temp_file_path}"
        _, error, returncode = run_command(cmd)
        if returncode != 0:
            print(f"Error merging PR: {error}")
            # Try to go back to original branch
            run_command(f"git checkout {current_branch}")
            sys.exit(1)

        print("\nPull request merged successfully locally!")
        print(f"\nMerge commit message:\n{commit_message}")

    finally:
        # Clean up the temporary file
        os.unlink(temp_file_path)


def merge_pr(pr_number, use_api=True):
    """Merge a pull request with a formatted commit message"""
    check_gh_auth()

    print(f"Fetching PR #{pr_number}...")
    pr_info = get_pr_info(pr_number)
    print(f"PR found: '{pr_info['title']}' by {pr_info['author']}")

    commit_title = f"Merge '{pr_info['title']}' from {pr_info['author_name']}"
    body = pr_info["body"]
    body = truncate_body_at_marker(body)
    body = strip_pr_template_from_body(body, ".github/pull_request_template.md")

    commit_body = wrap_text(body)

    commit_message_parts = [commit_title]
    if commit_body:
        commit_message_parts.append("")  # Empty line between title and body
        commit_message_parts.append(commit_body)
    if pr_info["reviewed_by"]:
        commit_message_parts.append("")  # Empty line before reviewed-by
        for approver in pr_info["reviewed_by"]:
            commit_message_parts.append(f"Reviewed-by: {approver}")
    commit_message_parts.append("")  # Empty line before Closes
    commit_message_parts.append(f"Closes #{pr_info['number']}")
    commit_message = "\n".join(commit_message_parts)

    if use_api:
        # For remote merge, we need to separate title from body
        commit_body_for_api = "\n".join(commit_message_parts[2:])
        merge_remote(pr_number, commit_body_for_api, commit_title)
    else:
        merge_local(pr_number, commit_message)


def check_gh_auth():
    """Check if gh CLI is authenticated"""
    _, _, returncode = run_command("gh auth status")
    if returncode != 0:
        print("Error: GitHub CLI is not authenticated. Run 'gh auth login' first.")
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Merge a pull request with a nice merge commit using GitHub CLI")
    parser.add_argument("pr_number", type=str, help="Pull request number to merge")
    parser.add_argument("--local", action="store_true", help="Use local git commands instead of GitHub API")
    args = parser.parse_args()
    if not re.match(r"^\d+$", args.pr_number):
        print("Error: PR number must be a positive integer")
        sys.exit(1)
    use_api = not args.local
    merge_pr(args.pr_number, use_api)
