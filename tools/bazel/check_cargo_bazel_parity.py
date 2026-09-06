#!/usr/bin/env python3
"""Check that Cargo workspace targets have matching Bazel target markers."""

import json
import pathlib
import subprocess
import sys
import xml.etree.ElementTree as ET


ROOT = pathlib.Path(__file__).resolve().parents[2]
EXCLUSIONS = {
    ("extensions/core", "custom-build", "build-script-build"): (
        "Cargo runs cbindgen for published headers; Bazel consumes the checked-in header."
    ),
    ("testing/stress", "test", "shuttle_mvcc"): (
        "The Shuttle stress test is release-only and is not part of Bazel CI."
    ),
}


def main() -> int:
    metadata = json.loads(
        run(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"])
    )
    query_xml = run(
        [
            "bazel",
            "query",
            "--lockfile_mode=error",
            'attr(tags, "cargo-target=", //...)',
            "--output=xml",
        ]
    )
    errors = parity_errors(metadata, query_xml, ROOT, EXCLUSIONS)
    if errors:
        print("Cargo/Bazel target parity failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    print(f"Cargo/Bazel target parity passed ({len(workspace_targets(metadata, ROOT))} Cargo targets).")
    return 0


def run(command: list[str]) -> str:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout


def parity_errors(
    metadata: dict,
    query_xml: str,
    root: pathlib.Path,
    exclusions: dict[tuple[str, str, str], str],
) -> list[str]:
    targets = workspace_targets(metadata, root)
    markers, marker_errors = bazel_markers(query_xml)
    errors = list(marker_errors)
    valid_markers = {
        (package, kind, name)
        for package, kinds, name in targets
        for kind in kinds
    }
    for marker in sorted(markers - valid_markers):
        errors.append(f"Bazel marker has no matching Cargo target: {':'.join(marker)}")
    used_exclusions = set()
    for package, kinds, name in targets:
        if any((package, kind, name) in markers for kind in kinds):
            continue
        matching_exclusions = [
            key for key in exclusions if key[0] == package and key[2] == name and key[1] in kinds
        ]
        if matching_exclusions:
            used_exclusions.update(matching_exclusions)
            continue
        errors.append(f"missing Bazel marker for {package} {','.join(kinds)} target {name}")
    for exclusion in sorted(set(exclusions) - used_exclusions):
        errors.append(f"stale exclusion for {':'.join(exclusion)}")
    return errors


def workspace_targets(metadata: dict, root: pathlib.Path) -> list[tuple[str, tuple[str, ...], str]]:
    workspace_members = set(metadata["workspace_members"])
    targets = []
    for package in metadata["packages"]:
        if package["id"] not in workspace_members:
            continue
        package_path = str(pathlib.Path(package["manifest_path"]).parent.relative_to(root))
        for target in package["targets"]:
            targets.append((package_path, tuple(target["kind"]), target["name"]))
    return targets


def bazel_markers(query_xml: str) -> tuple[set[tuple[str, str, str]], list[str]]:
    markers = set()
    errors = []
    for rule in ET.fromstring(query_xml).iter("rule"):
        package = rule.attrib["name"][2:].split(":", 1)[0]
        for value in rule.findall("list[@name='tags']/string"):
            tag = value.attrib["value"]
            if not tag.startswith("cargo-target="):
                continue
            marker = tag.removeprefix("cargo-target=")
            if ":" not in marker:
                errors.append(f"invalid Cargo target marker on {rule.attrib['name']}: {tag}")
                continue
            kind, name = marker.split(":", 1)
            markers.add((package, kind, name))
    return markers, errors


if __name__ == "__main__":
    sys.exit(main())
