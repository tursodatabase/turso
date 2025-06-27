#!/usr/bin/env python3
"""
Script to update version in Cargo.toml, package.json, and package-lock.json files for the Turso project.
This script updates all occurrences of the version in the workspace configuration,
updates the JavaScript and WebAssembly bindings package.json and package-lock.json files,
uses cargo update to update Cargo.lock, creates a git commit, and adds a version tag.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

# Define all npm package paths in one place
NPM_PACKAGES = [
    "bindings/javascript",
    "bindings/javascript/npm/darwin-universal",
    "bindings/javascript/npm/linux-x64-gnu",
    "bindings/javascript/npm/win32-x64-msvc",
    "bindings/wasm",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Update version in project files")

    # Version argument
    parser.add_argument("version", help="The new version to set (e.g., 0.1.0)")

    return parser.parse_args()


def extract_current_version(content):
    """Extract the current version from Cargo.toml content."""
    match = re.search(r'workspace\.package\]\s*\nversion\s*=\s*"([^"]+)"', content)
    if not match:
        sys.exit(1)
    return match.group(1)


def update_cargo_toml(new_version):
    """Update all version references in Cargo.toml to the new version."""
    try:
        cargo_path = Path("Cargo.toml")
        if not cargo_path.exists():
            sys.exit(1)

        content = cargo_path.read_text()

        current_version = extract_current_version(content)

        # Pattern to match version in various contexts while maintaining the quotes
        pattern = r'(version\s*=\s*)"' + re.escape(current_version) + r'"'
        updated_content = re.sub(pattern, rf'\1"{new_version}"', content)

        cargo_path.write_text(updated_content)
        return True
    except Exception:
        sys.exit(1)


def update_package_json(dir_path, new_version):  # noqa: C901
    """Update version in package.json and package-lock.json files."""
    dir_path = Path(dir_path)

    # Update package.json
    try:
        package_path = dir_path / "package.json"
        if not package_path.exists():
            return False

        # Read and parse the package.json file
        with open(package_path, "r") as f:
            package_data = json.load(f)

        # Update version regardless of current value
        package_data["version"] = new_version

        # Write updated package.json
        with open(package_path, "w") as f:
            json.dump(package_data, f, indent=2)
    except Exception:
        return False

    # Update package-lock.json if it exists
    try:
        lock_path = dir_path / "package-lock.json"
        if not lock_path.exists():
            return True  # package.json was updated successfully

        # Read and parse the package-lock.json file
        with open(lock_path, "r") as f:
            lock_data = json.load(f)

        # Update version in multiple places in package-lock.json
        if "version" in lock_data:
            lock_data["version"] = new_version

        # Update version in packages section if it exists (npm >= 7)
        if "packages" in lock_data:
            if "" in lock_data["packages"]:  # Root package
                if "version" in lock_data["packages"][""]:
                    lock_data["packages"][""]["version"] = new_version

        # Update version in dependencies section if it exists (older npm)
        package_name = package_data.get("name", "")
        if "dependencies" in lock_data and package_name in lock_data["dependencies"]:
            if "version" in lock_data["dependencies"][package_name]:
                lock_data["dependencies"][package_name]["version"] = new_version

        # Write updated package-lock.json
        with open(lock_path, "w") as f:
            json.dump(lock_data, f, indent=2)

        return True
    except Exception:
        return False


def update_all_packages(new_version):
    """Update all npm packages with the new version."""
    results = []
    for package_path in NPM_PACKAGES:
        result = update_package_json(package_path, new_version)
        results.append((package_path, result))
    return results


def run_cargo_update():
    """Run cargo update to update the Cargo.lock file."""
    try:
        # Run cargo update showing its output with verbose flag
        subprocess.run(["cargo", "update", "--workspace", "--verbose"], check=True)
        return True
    except Exception:
        return False


def create_git_commit_and_tag(version):
    """Create a git commit with all changes and add a version tag."""
    try:
        # Add files that exist and have changes
        files_to_add = ["Cargo.toml", "Cargo.lock"]

        # Add all potential package.json and package-lock.json files
        for package_path in NPM_PACKAGES:
            package_json = f"{package_path}/package.json"
            package_lock = f"{package_path}/package-lock.json"

            if os.path.exists(package_json):
                files_to_add.append(package_json)
            if os.path.exists(package_lock):
                files_to_add.append(package_lock)

        # Add each file individually
        for file in files_to_add:
            try:
                subprocess.run(["git", "add", file], check=True)
            except subprocess.CalledProcessError:
                print(f"Warning: Could not add {file} to git")

        # Create commit
        commit_message = f"Turso {version}"
        subprocess.run(["git", "commit", "-m", commit_message], check=True)

        # Create tag
        tag_name = f"v{version}"
        subprocess.run(["git", "tag", "-a", tag_name, "-m", f"Version {version}"], check=True)

        return True
    except Exception as e:
        print(f"Error in git operations: {e}")
        return False


def main():
    args = parse_args()
    new_version = args.version

    # Update Cargo.toml
    update_cargo_toml(new_version)

    # Update all npm packages
    update_all_packages(new_version)

    # Update Cargo.lock using cargo update
    run_cargo_update()

    # Create git commit and tag
    create_git_commit_and_tag(new_version)


if __name__ == "__main__":
    main()
