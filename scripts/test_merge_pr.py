#!/usr/bin/env python3
"""Tests for the --message-from mode of merge-pr.py. Run: python3 <thisfile>."""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

PR_9684 = {
    "number": 9684,
    "title": "core/translate: skip subqueries that read tables outside a hash build input",
    "author": {"login": "app/turso-github-handyman"},
    "headRefName": "auto/9648-panic-when-a-correlated-subquery-in-wher-627052",
    "body": (
        "## Description\n"
        "\n"
        "A query with a hash join and a correlated subquery in WHERE panicked at compile time when the "
        "subquery read a table that comes before the hash join's probe table (here `s`), and the query had "
        "LIMIT (without LIMIT, EXISTS becomes a semi-join and does not hit this path).\n"
        "\n"
        "```sql\n"
        "CREATE TABLE t1(a, b); CREATE TABLE t2(a, b); CREATE TABLE t4(a, c);\n"
        "CREATE INDEX t4_c ON t4(c);\n"
        "INSERT INTO t1 VALUES (1, 2); INSERT INTO t2 VALUES (3, 2); INSERT INTO t4 VALUES (3, 10);\n"
        "SELECT 1 FROM t1 AS s, t2, t4\n"
        "WHERE s.b = t2.b AND t2.a = t4.a AND EXISTS (SELECT 1 FROM t1 AS x WHERE x.a = s.a) AND t4.c = 10\n"
        "LIMIT 1;\n"
        '-- before: panic "No index cursor found for table t1"\n'
        "-- after: 1\n"
        "```\n"
        "\n"
        "The subplan that saves the hash build input (`t4`) to a temporary table copied all of the parent's "
        "WHERE subqueries. The WHERE terms that read tables outside the subplan were already skipped there, "
        "but the subqueries themselves were still emitted. Because `s` is not in the subplan's join order, "
        "the subquery was scheduled before the loop and read a cursor that does not exist. With a FROM "
        'subquery in place of `s`, the panic was "Outer query subquery result_columns_start_reg must be set '
        'in program".\n'
        "\n"
        "The subplan now keeps only the subqueries whose outer references are all tables included in it. "
        "This is the same set of tables used to decide which WHERE terms run there.\n"
        "\n"
        "## Motivation and context\n"
        "\n"
        "Fixes #9648\n"
        "\n"
        "## Description of AI Usage\n"
        "\n"
        "Autofixer wrote this change with Claude Code in an isolated sandbox. A separate verifier agent "
        "tested and reviewed it in its own container before this PR was opened. A maintainer must review it "
        "before merge."
    ),
    "reviews": [{"state": "APPROVED", "author": {"login": "jussisaurio"}}],
}


def test_message_for_pr_9684_leaves_out_the_ai_usage_section():
    message = commit_message_for(PR_9684)
    assert message == {
        "commit_title": "Merge 'core/translate: skip subqueries that read tables outside a hash build input' "
        "from app/turso-github-handyman",
        "commit_message": (
            "A query with a hash join and a correlated subquery in WHERE panicked at\n"
            "compile time when the subquery read a table that comes before the hash\n"
            "join's probe table (here `s`), and the query had LIMIT (without LIMIT,\n"
            "EXISTS becomes a semi-join and does not hit this path).\n"
            "\n"
            "```sql\n"
            "CREATE TABLE t1(a, b); CREATE TABLE t2(a, b); CREATE TABLE t4(a, c);\n"
            "CREATE INDEX t4_c ON t4(c);\n"
            "INSERT INTO t1 VALUES (1, 2); INSERT INTO t2 VALUES (3, 2); INSERT INTO t4 VALUES (3, 10);\n"
            "SELECT 1 FROM t1 AS s, t2, t4\n"
            "WHERE s.b = t2.b AND t2.a = t4.a AND EXISTS (SELECT 1 FROM t1 AS x WHERE x.a = s.a) AND t4.c = 10\n"
            "LIMIT 1;\n"
            '-- before: panic "No index cursor found for table t1"\n'
            "-- after: 1\n"
            "```\n"
            "\n"
            "The subplan that saves the hash build input (`t4`) to a temporary table\n"
            "copied all of the parent's WHERE subqueries. The WHERE terms that read\n"
            "tables outside the subplan were already skipped there, but the\n"
            "subqueries themselves were still emitted. Because `s` is not in the\n"
            "subplan's join order, the subquery was scheduled before the loop and\n"
            "read a cursor that does not exist. With a FROM subquery in place of `s`,\n"
            'the panic was "Outer query subquery result_columns_start_reg must be set\n'
            'in program".\n'
            "\n"
            "The subplan now keeps only the subqueries whose outer references are all\n"
            "tables included in it. This is the same set of tables used to decide\n"
            "which WHERE terms run there.\n"
            "\n"
            "Fixes #9648\n"
            "\n"
            "Reviewed-by: Jussi Saurio <jussi.saurio@gmail.com>\n"
            "\n"
            "Closes #9684"
        ),
    }, message


def test_pr_without_body_or_approval_only_closes_the_pr():
    pr = {
        "number": 42,
        "title": "Fix a typo",
        "author": {"login": "penberg", "name": "Pekka Enberg"},
        "headRefName": "typo",
        "body": "",
        "reviews": [{"state": "COMMENTED", "author": {"login": "jussisaurio"}}],
    }
    message = commit_message_for(pr)
    assert message == {"commit_title": "Merge 'Fix a typo' from Pekka Enberg", "commit_message": "Closes #42"}, message


def commit_message_for(pr_data):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as pr_json:
        json.dump(pr_data, pr_json)
        pr_json.flush()
        output = subprocess.run(
            [sys.executable, "scripts/merge-pr.py", "--message-from", pr_json.name],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    return json.loads(output)


if __name__ == "__main__":
    test_message_for_pr_9684_leaves_out_the_ai_usage_section()
    test_pr_without_body_or_approval_only_closes_the_pr()
    print("ok")
