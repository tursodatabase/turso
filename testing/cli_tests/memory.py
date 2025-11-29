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
                stub_memory_test(turso, **test)
            test_hash_joins(turso)
            test_spill_hash_joins(turso)
    except Exception as e:
        console.error(f"Test FAILED: {e}")
        exit(1)
    console.info("All tests passed successfully.")


if __name__ == "__main__":
    main()
