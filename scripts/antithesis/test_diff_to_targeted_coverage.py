#!/usr/bin/env python3
"""Tests for diff_to_targeted_coverage.squash. Run: python3 <thisfile>."""
from diff_to_targeted_coverage import squash

SAMPLE = """diff --git a/core/foo.rs b/core/foo.rs
index 1111111..2222222 100644
--- a/core/foo.rs
+++ b/core/foo.rs
@@ -10,3 +10,4 @@ fn foo() {
 a
 b
-c
+d
+e
@@ -40,0 +41,2 @@
+f
+g
diff --git a/old.rs b/old.rs
deleted file mode 100644
index 3333333..0000000
--- a/old.rs
+++ /dev/null
@@ -1,2 +0,0 @@
-x
-y
diff --git a/new.rs b/new.rs
new file mode 100644
index 0000000..4444444
--- /dev/null
+++ b/new.rs
@@ -0,0 +1,3 @@
+p
+q
+r
"""


def test_basic():
    locs = squash(SAMPLE)["antithesis_targeted_coverage"]["locations"]
    assert locs == [
        {"file": "core/foo.rs", "begin_line": 10, "end_line": 13},
        {"file": "core/foo.rs", "begin_line": 41, "end_line": 42},
        {"file": "new.rs", "begin_line": 1, "end_line": 3},
    ], locs


def test_single_line_hunk():
    diff = "--- a/x.rs\n+++ b/x.rs\n@@ -5 +7 @@\n-a\n+b\n"
    locs = squash(diff)["antithesis_targeted_coverage"]["locations"]
    assert locs == [{"file": "x.rs", "begin_line": 7, "end_line": 7}], locs


def test_added_line_starting_with_plus_is_not_a_header():
    diff = "--- a/x.rs\n+++ b/x.rs\n@@ -1,1 +1,2 @@\n a\n+++ not a header\n"
    locs = squash(diff)["antithesis_targeted_coverage"]["locations"]
    assert locs == [{"file": "x.rs", "begin_line": 1, "end_line": 2}], locs


def test_non_rust_files_are_excluded():
    diff = (
        "--- a/Cargo.lock\n+++ b/Cargo.lock\n@@ -1,1 +1,2 @@\n a\n+b\n"
        "--- a/x.snap\n+++ b/x.snap\n@@ -1,1 +1,2 @@\n a\n+b\n"
        "--- a/core/y.rs\n+++ b/core/y.rs\n@@ -1,1 +1,2 @@\n a\n+b\n"
    )
    locs = squash(diff)["antithesis_targeted_coverage"]["locations"]
    assert locs == [{"file": "core/y.rs", "begin_line": 1, "end_line": 2}], locs


def test_empty():
    assert squash("") == {"antithesis_targeted_coverage": {"locations": []}}


if __name__ == "__main__":
    test_basic()
    test_single_line_hunk()
    test_added_line_starting_with_plus_is_not_a_header()
    test_non_rust_files_are_excluded()
    test_empty()
    print("ok")
