#!/usr/bin/env python3
import re
import subprocess
import sys
from collections import defaultdict


def get_git_merges(prev_version):
    """Get merge commits since the previous version tag."""
    try:
        command = f"git log {prev_version}..HEAD | grep 'Merge '"
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)

        merge_lines = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip() or "Merge:" in line:
                continue

            # Extract the commit message and author
            match = re.search(r"Merge '([^']+)' from ([^(]+)", line)
            if match:
                message = match.group(1).strip()
                author = match.group(2).strip()
                merge_lines.append((message, author))

        return merge_lines
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to get git merge logs: {e}")
        return []


def categorize_commits(merge_lines):
    """Categorize commits into Added, Updated, Fixed."""
    categories = defaultdict(list)

    for message, author in merge_lines:
        # Format the line for our output
        formatted_line = f"* {message} ({author})"

        # Categorize based on keywords in the commit message
        message_lower = message.lower()
        if re.search(r"add|new|implement|support|initial|introduce", message_lower):
            categories["Added"].append(formatted_line)
        elif re.search(r"fix|bug|issue|error|crash|resolve|typo", message_lower):
            categories["Fixed"].append(formatted_line)
        else:
            categories["Updated"].append(formatted_line)

    return categories


def format_changelog(categories):
    """Format the categorized commits into a changelog."""
    changelog = "## Unreleased\n"

    for category in ["Added", "Updated", "Fixed"]:
        changelog += f"### {category}\n"

        if not categories[category]:
            changelog += "\n"
            continue

        for commit_message in categories[category]:
            changelog += f"{commit_message}\n"

        changelog += "\n"

    return changelog


def main():
    if len(sys.argv) != 2:
        print("Usage: python changelog_generator.py <previous_version_tag>")
        print("Example: python changelog_generator.py v0.0.17")
        sys.exit(1)

    prev_version = sys.argv[1]

    # Get merge commits since previous version
    merge_lines = get_git_merges(prev_version)

    if not merge_lines:
        print(f"No merge commits found since {prev_version}")
        return

    # Categorize commits
    categories = categorize_commits(merge_lines)

    # Format changelog
    changelog = format_changelog(categories)

    # Output changelog
    print(changelog)

    # Optionally write to file
    write_to_file = input("Write to CHANGELOG.md? (y/n): ")
    if write_to_file.lower() == "y":
        try:
            with open("CHANGELOG.md", "r") as f:
                content = f.read()
            with open("CHANGELOG.md", "w") as f:
                f.write(changelog + content)
            print("Changelog written to CHANGELOG.md")
        except FileNotFoundError:
            with open("CHANGELOG.md", "w") as f:
                f.write(changelog)
            print("Created new CHANGELOG.md file")


if __name__ == "__main__":
    main()
