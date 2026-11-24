"""Writes SQL scripts that nest queries as deeply as Turso permits.

Usage: python3 scripts/stack/deep-queries.py OUT_DIR

Each script uses one kind of nesting at its limit: MAX_QUERY_DEPTH nested
queries, MAX_COMPOUND_SELECT compound terms, or one level less than
MAX_EXPR_DEPTH of scalar subqueries in an expression. The script reads these
limits from the Rust sources, so that the scripts follow a change of a limit.
Prints the path of each script that it writes.
scripts/stack/check-deep-queries.sh runs the scripts with a small stack.

Trigger chains and foreign key actions can be 1000 levels deep, and their
programs still use more stack than these queries, so they are not here.
"""

import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")


def rust_limit(path, name):
    """Returns the value of `pub const NAME: usize = VALUE;` in a Rust source file."""
    with open(os.path.join(ROOT, path)) as f:
        match = re.search(rf"pub const {name}: usize = (\d+);", f.read())
    if match is None:
        sys.exit(f"error: {path} has no `pub const {name}: usize = ...;`")
    return int(match.group(1))


MAX_QUERY_DEPTH = rust_limit("ast/lib.rs", "MAX_QUERY_DEPTH")
MAX_COMPOUND_SELECT = rust_limit("sqlite/parser/src/parser.rs", "MAX_COMPOUND_SELECT")
MAX_EXPR_DEPTH = rust_limit("sqlite/parser/src/parser.rs", "MAX_EXPR_DEPTH")

TABLE = "CREATE TABLE t(x);\nINSERT INTO t VALUES(1);\n"


def from_subquery():
    depth = MAX_QUERY_DEPTH
    return TABLE + "SELECT * FROM " + "(SELECT * FROM " * depth + "t" + ")" * depth + ";\n"


def from_parentheses():
    depth = MAX_QUERY_DEPTH
    return TABLE + "SELECT * FROM " + "(" * depth + "t" + ")" * depth + ";\n"


def join_subquery():
    depth = MAX_QUERY_DEPTH
    body = "SELECT t.x FROM t JOIN (" * depth + "SELECT x FROM t" + ") ON 1" * depth
    return TABLE + body + ";\n"


def lateral_subquery():
    depth = MAX_QUERY_DEPTH
    body = "SELECT t.x FROM t, LATERAL (" * depth + "SELECT t.x" + ")" * depth
    return TABLE + body + ";\n"


def compound_subquery():
    depth = MAX_QUERY_DEPTH
    body = "SELECT * FROM (SELECT 1 AS x UNION ALL " * depth + "SELECT 1" + ")" * depth
    return body + ";\n"


def ordered_subquery():
    depth = MAX_QUERY_DEPTH
    body = "SELECT x FROM (" * depth + "SELECT x FROM t" + ") ORDER BY x LIMIT 5" * depth
    return TABLE + body + ";\n"


def scalar_subquery():
    depth = MAX_EXPR_DEPTH - 1
    return "SELECT " + "(SELECT " * depth + "1" + ")" * depth + ";\n"


def nested_cte():
    depth = MAX_QUERY_DEPTH
    sql = "WITH n0 AS (SELECT x FROM t) SELECT * FROM n0"
    for i in range(1, depth):
        sql = f"WITH n{i} AS ({sql}) SELECT * FROM n{i}"
    return TABLE + sql + ";\n"


def cte_chain():
    depth = MAX_QUERY_DEPTH
    ctes = ["n0 AS (SELECT x FROM t)"]
    ctes += [f"n{i} AS (SELECT * FROM n{i - 1})" for i in range(1, depth)]
    return TABLE + f"WITH {', '.join(ctes)} SELECT * FROM n{depth - 1};\n"


def recursive_cte():
    # Each level is two nested queries: the body of the CTE, and the FROM
    # subquery that holds the next level.
    depth = MAX_QUERY_DEPTH // 2
    sql = "SELECT 1 AS x"
    for i in range(depth):
        sql = (
            f"WITH RECURSIVE r{i}(x) AS (SELECT x FROM ({sql}) UNION ALL "
            f"SELECT x + 1 FROM r{i} WHERE x < 2) SELECT x FROM r{i}"
        )
    return sql + ";\n"


def view_chain():
    depth = MAX_QUERY_DEPTH
    views = ["CREATE VIEW v0 AS SELECT x FROM t;"]
    views += [f"CREATE VIEW v{i} AS SELECT * FROM v{i - 1};" for i in range(1, depth)]
    return TABLE + "\n".join(views) + f"\nSELECT * FROM v{depth - 1};\n"


def view_subquery_chain():
    # Each view is two nested queries: the view and its FROM subquery.
    depth = MAX_QUERY_DEPTH // 2
    views = ["CREATE VIEW v0 AS SELECT x FROM t;"]
    views += [
        f"CREATE VIEW v{i} AS SELECT * FROM (SELECT * FROM v{i - 1});" for i in range(1, depth)
    ]
    return TABLE + "\n".join(views) + f"\nSELECT * FROM v{depth - 1};\n"


def view_after_rename():
    # ALTER TABLE parses and rewrites the SQL of the view. The query that reads
    # the view is one more level.
    depth = MAX_QUERY_DEPTH - 1
    body = "SELECT * FROM (" * depth + "SELECT x FROM t" + ")" * depth
    return (
        TABLE
        + f"CREATE VIEW v AS {body};\n"
        + "ALTER TABLE t RENAME TO t2;\n"
        + "SELECT * FROM v;\n"
    )


def trigger_body():
    depth = MAX_QUERY_DEPTH
    body = "SELECT * FROM (" * depth + "SELECT new.x" + ")" * depth
    return (
        "CREATE TABLE t(x);\nCREATE TABLE u(x);\n"
        + f"CREATE TRIGGER tr AFTER INSERT ON u BEGIN INSERT INTO t {body}; END;\n"
        + "INSERT INTO u VALUES(1);\n"
        + "SELECT * FROM t;\n"
    )


def windows():
    # Each different window adds one level of subquery to the plan.
    depth = MAX_QUERY_DEPTH
    columns = ", ".join(f"sum(x) OVER (ORDER BY x ROWS {i} PRECEDING)" for i in range(depth))
    return TABLE + f"SELECT {columns} FROM t;\n"


def compound():
    terms = MAX_COMPOUND_SELECT
    return "SELECT count(*) FROM (" + " UNION ALL ".join(["SELECT 1"] * terms) + ");\n"


SCRIPTS = [
    from_subquery,
    from_parentheses,
    join_subquery,
    lateral_subquery,
    compound_subquery,
    ordered_subquery,
    scalar_subquery,
    nested_cte,
    cte_chain,
    recursive_cte,
    view_chain,
    view_subquery_chain,
    view_after_rename,
    trigger_body,
    windows,
    compound,
]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    out_dir = sys.argv[1]
    for script in SCRIPTS:
        path = os.path.join(out_dir, f"{script.__name__}.sql")
        with open(path, "w") as f:
            f.write(script())
        print(path)
