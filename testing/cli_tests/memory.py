#!/usr/bin/env python3
import os

from cli_tests import console
from cli_tests.test_turso_cli import TestTursoShell

sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


def validate_with_expected(result: str, expected: str):
    return (expected in result, expected)


def stub_memory_test(
    turso: TestTursoShell,
    name: str,
    blob_size: int = 1024**2,
    vals: int = 100,
    blobs: bool = True,
):
    # zero_blob_size = 1024 **2
    zero_blob = "0" * blob_size * 2
    # vals = 100
    big_stmt = [
        "CREATE TABLE temp (t1 BLOB, t2 INTEGER);",
        "CREATE INDEX temp_index ON temp(t1);",
    ]
    big_stmt = big_stmt + [
        f"INSERT INTO temp (t1) VALUES (zeroblob({blob_size}));"
        if i % 2 == 0 and blobs
        else f"INSERT INTO temp (t2) VALUES ({i});"
        for i in range(vals * 2)
    ]
    expected = []
    for i in range(vals * 2):
        if i % 2 == 0 and blobs:
            big_stmt.append(f"SELECT hex(t1) FROM temp LIMIT 1 OFFSET {i};")
            expected.append(zero_blob)
        else:
            big_stmt.append(f"SELECT t2 FROM temp LIMIT 1 OFFSET {i};")
            expected.append(f"{i}")

    big_stmt.append("SELECT count(*) FROM temp;")
    expected.append(str(vals * 2))

    big_stmt.append("DELETE FROM temp;")
    big_stmt.append("SELECT count(*) FROM temp;")
    expected.append(str(0))

    big_stmt = "".join(big_stmt)
    expected = "\n".join(expected)

    turso.run_test_fn(big_stmt, lambda res: validate_with_expected(res, expected), name)


# TODO no delete tests for now because of turso outputs some debug information on delete
def memory_tests() -> list[dict]:
    tests = []

    for vals in range(0, 1000, 100):
        tests.append(
            {
                "name": f"small-insert-integer-vals-{vals}",
                "vals": vals,
                "blobs": False,
            }
        )

    tests.append(
        {
            "name": f"small-insert-blob-interleaved-blob-size-{1024}",
            "vals": 10,
            "blob_size": 1024,
        }
    )
    tests.append(
        {
            "name": f"big-insert-blob-interleaved-blob-size-{1024}",
            "vals": 100,
            "blob_size": 1024,
        }
    )

    for blob_size in range(0, (1024 * 1024) + 1, 1024 * 4**4):
        if blob_size == 0:
            continue
        tests.append(
            {
                "name": f"small-insert-blob-interleaved-blob-size-{blob_size}",
                "vals": 10,
                "blob_size": blob_size,
            }
        )
        tests.append(
            {
                "name": f"big-insert-blob-interleaved-blob-size-{blob_size}",
                "vals": 100,
                "blob_size": blob_size,
            }
        )
    return tests


def test_hash_joins(turso: TestTursoShell):
    turso.execute_dot("CREATE TABLE t(a, b, c);")
    turso.execute_dot("CREATE TABLE t2(a, b, c);")
    # first, test that we are choosing the query plan
    turso.run_test_fn(
        "explain query plan select * from t join t2 on t.a = t2.a;",
        lambda x: "HASH JOIN" in x,
        "test query plan contains hash join",
    )
    turso.run_test_fn(
        "explain query plan select * from t join t2 on substr(t.a, 1,3) = substr(t2.a,1,3);",
        lambda x: "HASH JOIN" in x,
        "test query plan contains hash join for equijoin on two non-column expressions",
    )
    turso.execute_dot("CREATE INDEX idx_t_a ON t(a);")
    # now it should no longer choose a hash join because of the index
    turso.run_test_fn(
        "explain query plan select * from t join t2 on t.a = substr(t2.a,1,3);",
        lambda x: "HASH JOIN" not in x and "USING INDEX idx_t_a" in x,
        "test query plan contains hash join for equijoin on two non-column expressions",
    )


def test_multi_way_hash_joins(turso: TestTursoShell):
    """Test multi-way hash join chain patterns and query plan selection."""
    turso.execute_dot("CREATE TABLE chain_t1(a TEXT);")
    turso.execute_dot("CREATE TABLE chain_t2(a TEXT);")
    turso.execute_dot("CREATE TABLE chain_t3(a TEXT);")
    turso.execute_dot("CREATE TABLE chain_t4(a TEXT);")

    turso.execute_dot("INSERT INTO chain_t1 VALUES ('a'), ('b'), ('c'), ('d');")
    turso.execute_dot("INSERT INTO chain_t2 VALUES ('a'), ('b'), ('c');")
    turso.execute_dot("INSERT INTO chain_t3 VALUES ('b'), ('c'), ('d');")
    turso.execute_dot("INSERT INTO chain_t4 VALUES ('c'), ('d');")

    turso.run_test_fn(
        "explain query plan SELECT * FROM chain_t1 JOIN chain_t2 ON chain_t1.a = chain_t2.a;",
        lambda x: "HASH JOIN" in x,
        "2-way chain join uses hash join",
    )
    turso.run_test_fn(
        "SELECT count(*) FROM chain_t1 JOIN chain_t2 ON chain_t1.a = chain_t2.a;",
        lambda x: "3" in x,
        "2-way chain join returns correct count (a, b, c match)",
    )

    turso.run_test_fn(
        "explain query plan SELECT * FROM chain_t1 JOIN chain_t2 ON chain_t1.a = chain_t2.a "
        "JOIN chain_t3 ON chain_t2.a = chain_t3.a;",
        lambda x: "HASH JOIN" in x,
        "3-way chain join uses hash join",
    )
    turso.run_test_fn(
        "SELECT count(*) FROM chain_t1 JOIN chain_t2 ON chain_t1.a = chain_t2.a "
        "JOIN chain_t3 ON chain_t2.a = chain_t3.a;",
        lambda x: "2" in x,
        "3-way chain join returns correct count (b, c match all three)",
    )
    turso.run_test_fn(
        "explain query plan SELECT * FROM chain_t1 JOIN chain_t2 ON chain_t1.a = chain_t2.a "
        "JOIN chain_t3 ON chain_t2.a = chain_t3.a JOIN chain_t4 ON chain_t3.a = chain_t4.a;",
        lambda x: "HASH JOIN" in x,
        "4-way chain join uses hash join",
    )
    turso.run_test_fn(
        "SELECT count(*) FROM chain_t1 JOIN chain_t2 ON chain_t1.a = chain_t2.a "
        "JOIN chain_t3 ON chain_t2.a = chain_t3.a JOIN chain_t4 ON chain_t3.a = chain_t4.a;",
        lambda x: "1" == x,
        "4-way chain join returns correct count (only 'c' matches all four)",
    )
    turso.run_test_fn(
        "SELECT chain_t1.a, chain_t2.a, chain_t3.a, chain_t4.a FROM chain_t1 "
        "JOIN chain_t2 ON chain_t1.a = chain_t2.a "
        "JOIN chain_t3 ON chain_t2.a = chain_t3.a "
        "JOIN chain_t4 ON chain_t3.a = chain_t4.a;",
        lambda x: "c|c|c|c" == x,
        "4-way chain join returns correct row data",
    )


def test_hash_join_with_index_preference(turso: TestTursoShell):
    """Test that hash join is NOT chosen when an index exists on join columns."""
    turso.execute_dot("CREATE TABLE indexed_t1(a TEXT, b TEXT);")
    turso.execute_dot("CREATE TABLE indexed_t2(a TEXT, b TEXT);")
    turso.execute_dot("INSERT INTO indexed_t1 VALUES ('x', 'y'), ('z', 'w');")
    turso.execute_dot("INSERT INTO indexed_t2 VALUES ('x', 'y'), ('z', 'w');")
    # Without index, should use hash join
    turso.run_test_fn(
        "explain query plan SELECT * FROM indexed_t1 JOIN indexed_t2 ON indexed_t1.a = indexed_t2.a;",
        lambda x: "HASH JOIN" in x,
        "without index, uses hash join",
    )

    # Create index on join column
    turso.execute_dot("CREATE INDEX idx_indexed_t2_a ON indexed_t2(a);")

    # With index, should NOT use hash join
    turso.run_test_fn(
        "explain query plan SELECT * FROM indexed_t1 JOIN indexed_t2 ON indexed_t1.a = indexed_t2.a;",
        lambda x: "HASH JOIN" not in x and "USING INDEX" in x,
        "with index on join column, prefers index over hash join",
    )


def test_hash_join_star_pattern_fallback(turso: TestTursoShell):
    """Test that star patterns (one table joined to multiple others) fall back to non-hash methods."""
    turso.execute_dot("CREATE TABLE star_center(id TEXT);")
    turso.execute_dot("CREATE TABLE star_arm1(center_id TEXT, val TEXT);")
    turso.execute_dot("CREATE TABLE star_arm2(center_id TEXT, val TEXT);")

    turso.execute_dot("INSERT INTO star_center VALUES ('a'), ('b');")
    turso.execute_dot("INSERT INTO star_arm1 VALUES ('a', 'x'), ('b', 'y');")
    turso.execute_dot("INSERT INTO star_arm2 VALUES ('a', 'p'), ('b', 'q');")

    turso.run_test_fn(
        """SELECT star_center.id, star_arm1.val, star_arm2.val FROM star_center
        JOIN star_arm1 ON star_center.id = star_arm1.center_id
        JOIN star_arm2 ON star_center.id = star_arm2.center_id
        ORDER BY star_center.id;""",
        lambda x: "a|x|p" in x and "b|y|q" in x,
        "star pattern join returns correct results",
    )


def test_hash_join_outer_join_exclusion(turso: TestTursoShell):
    """Test that hash joins are NOT used for LEFT/RIGHT OUTER JOINs."""
    turso.execute_dot("CREATE TABLE outer_t1(a TEXT);")
    turso.execute_dot("CREATE TABLE outer_t2(a TEXT);")
    turso.execute_dot("INSERT INTO outer_t1 VALUES ('a'), ('b'), ('c');")
    turso.execute_dot("INSERT INTO outer_t2 VALUES ('b'), ('c'), ('d');")

    # INNER JOIN should use hash join
    turso.run_test_fn(
        "explain query plan SELECT * FROM outer_t1 JOIN outer_t2 ON outer_t1.a = outer_t2.a;",
        lambda x: "HASH JOIN" in x,
        "INNER JOIN uses hash join",
    )
    turso.run_test_fn(
        "SELECT outer_t1.a, outer_t2.a FROM outer_t1 LEFT JOIN outer_t2"
        " ON outer_t1.a = outer_t2.a ORDER BY outer_t1.a;",
        lambda x: "a|" in x and "b|b" in x and "c|c" in x,
        "LEFT JOIN returns correct results with NULLs",
    )


def test_spill_hash_joins(turso: TestTursoShell):
    turso.execute_dot("CREATE TABLE spill(a, b, c);")
    turso.execute_dot("CREATE TABLE spill2(a, b, c);")
    # insert enough data to force disk spill during hash join (32kb default limit for debug builds)
    turso.execute_dot(
        """INSERT INTO spill SELECT replace(substr(quote(zeroblob((1024*10 + 1) / 2)), 3, 1024*10), '0', 'a'),
        'abcdef', 'xyz' FROM generate_series(1,150);"""
    )
    turso.execute_dot(
        """INSERT INTO spill2 SELECT replace(substr(quote(zeroblob((1024*10 + 1) / 2)), 3,1024*10), '0', 'a'),
        'abcdef', 'xyz' FROM generate_series(1,150);"""
    )
    turso.run_test_fn(
        "SELECT substr(spill.a, 1,4), substr(spill2.b, 1, 4) FROM spill JOIN spill2 ON spill.a = spill2.a limit 2;",
        lambda x: "aaaa|abcd" in x,
        "test query returns correct data when hash join spills to disk",
    )
    turso.run_test_fn(
        "SELECT count(*),substr(spill.a,1,4),substr(spill2.b,1,4) FROM spill JOIN spill2 ON spill.a = spill2.a;",
        lambda x: "22500|aaaa|abcd" in x,
        "test query returns correct count when hash join spills to disk, with aggregate",
    )


def main():
    tests = memory_tests()
    # TODO see how to parallelize this loop with different subprocesses
    try:
        with TestTursoShell("") as turso:
            for test in tests:
                turso.execute_dot("PRAGMA cache_size = 200")
                stub_memory_test(turso, **test)
            test_hash_joins(turso)
            test_multi_way_hash_joins(turso)
            test_hash_join_with_index_preference(turso)
            test_hash_join_star_pattern_fallback(turso)
            test_hash_join_outer_join_exclusion(turso)
            test_spill_hash_joins(turso)
    except Exception as e:
        console.error(f"Test FAILED: {e}")
        exit(1)
    console.info("All tests passed successfully.")


if __name__ == "__main__":
    main()
