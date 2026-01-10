#!/usr/bin/env python3
import os
import time
from pathlib import Path

from cli_tests import console
from cli_tests.test_turso_cli import TestTursoShell


def test_basic_queries():
    shell = TestTursoShell()
    shell.run_test("select-1", "SELECT 1;", "1")
    shell.run_test("select-avg", "SELECT avg(age) FROM users;", "47.75")
    shell.run_test("select-sum", "SELECT sum(age) FROM users;", "191")
    shell.run_test("mem-sum-zero", "SELECT sum(first_name) FROM users;", "0.0")
    shell.run_test("mem-total-age", "SELECT total(age) FROM users;", "191.0")
    shell.run_test("mem-typeof", "SELECT typeof(id) FROM users LIMIT 1;", "integer")
    shell.quit()


def test_schema_operations():
    shell = TestTursoShell(init_blobs_table=True)
    expected = (
        "CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER);\n"
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);\n"
        "CREATE TABLE t (x1, x2, x3, x4);"
    )
    shell.run_test("schema-memory", ".schema", expected)
    shell.quit()


def test_file_operations():
    shell = TestTursoShell()
    shell.run_test("file-open", ".open testing/testing.db", "")
    shell.run_test("file-users-count", "select count(*) from users;", "10000")
    shell.quit()

    shell = TestTursoShell()
    shell.run_test("file-schema-1", ".open testing/testing.db", "")
    expected_user_schema = (
        "CREATE TABLE users (\n"
        "id INTEGER PRIMARY KEY,\n"
        "first_name TEXT,\n"
        "last_name TEXT,\n"
        "email TEXT,\n"
        "phone_number TEXT,\n"
        "address TEXT,\n"
        "city TEXT,\n"
        "state TEXT,\n"
        "zipcode TEXT,\n"
        "age INTEGER\n"
        ");\n"
        "CREATE INDEX age_idx on users (age);"
    )
    shell.run_test("file-schema-users", ".schema users", expected_user_schema)
    shell.quit()


def test_joins():
    shell = TestTursoShell()
    shell.run_test("open-file", ".open testing/testing.db", "")
    shell.run_test("verify-tables", ".tables", "products users")
    shell.run_test(
        "file-cross-join",
        "select * from users, products limit 1;",
        "1|Jamie|Foster|dylan00@example.com|496-522-9493|62375 Johnson Rest Suite 322|West Lauriestad|IL|35865|94|1|hat|79.0",  # noqa: E501
    )
    shell.quit()


def test_left_join_self():
    shell = TestTursoShell(
        init_commands="""
    .open testing/testing.db
    """
    )

    shell.run_test(
        "file-left-join-self",
        "select u1.first_name as user_name, u2.first_name as neighbor_name from users u1 left join users as u2 on u1.id = u2.id + 1 limit 2;",  # noqa: E501
        "Jamie|\nCindy|Jamie",
    )
    shell.quit()


def test_where_clauses():
    shell = TestTursoShell()
    shell.run_test("open-testing-db-file", ".open testing/testing.db", "")
    shell.run_test(
        "where-clause-eq-string",
        "select count(1) from users where last_name = 'Rodriguez';",
        "61",
    )
    shell.quit()


def test_switch_back_to_in_memory():
    shell = TestTursoShell()
    # First, open the file-based DB.
    shell.run_test("open-testing-db-file", ".open testing/testing.db", "")
    # Then switch back to :memory:
    shell.run_test("switch-back", ".open :memory:", "")
    shell.run_test("schema-in-memory", ".schema users", "")
    shell.quit()


def test_verify_null_value():
    shell = TestTursoShell()
    shell.run_test("verify-null", "select NULL;", "TURSO")
    shell.quit()


def verify_output_file(filepath: Path, expected_lines: dict) -> None:
    with open(filepath, "r") as f:
        contents = f.read()
    for line, description in expected_lines.items():
        assert line in contents, f"Missing: {description}"


def test_output_file():
    shell = TestTursoShell()
    output_filename = "turso_output.txt"
    output_file = shell.config.test_dir / shell.config.py_folder / output_filename

    shell.execute_dot(".open testing/testing.db")

    shell.execute_dot(f".cd {shell.config.test_dir}/{shell.config.py_folder}")
    shell.execute_dot(".echo on")
    shell.execute_dot(f".output {output_filename}")
    shell.execute_dot(f".cd {shell.config.test_dir}/{shell.config.py_folder}")
    shell.execute_dot(".mode pretty")
    shell.execute_dot("SELECT 'TEST_ECHO';")
    shell.execute_dot("")
    shell.execute_dot(".echo off")
    shell.execute_dot(".nullvalue turso")
    shell.execute_dot(".show")
    shell.execute_dot(".output stdout")
    time.sleep(3)

    with open(output_file, "r") as f:
        contents = f.read()

    expected_lines = {
        f"Output: {output_filename}": "Can direct output to a file",
        "Output mode: list": "Output mode remains list when output is redirected",
        "Error: pretty output can only be written to a tty": "Error message for pretty mode",
        "SELECT 'TEST_ECHO'": "Echoed command",
        "TEST_ECHO": "Echoed result",
        "Null value: turso": "Null value setting",
        f"CWD: {shell.config.cwd}/{shell.config.test_dir}": "Working directory changed",
        "DB: testing/testing.db": "File database opened",
        "Echo: off": "Echo turned off",
    }

    for line, test in expected_lines.items():
        assert line in contents, f"Expected line not found in file: {line} for {test}"

    # Clean up
    os.remove(output_file)
    shell.quit()


def test_multi_line_single_line_comments_succession():
    shell = TestTursoShell()
    comments = """-- First of the comments
-- Second line of the comments
SELECT 2;"""
    shell.run_test("multi-line-single-line-comments", comments, "2")
    shell.quit()


def test_comments():
    shell = TestTursoShell()
    shell.run_test("single-line-comment", "-- this is a comment\nSELECT 1;", "1")
    shell.run_test("multi-line-comments", "-- First comment\n-- Second comment\nSELECT 2;", "2")
    shell.run_test("block-comment", "/*\nMulti-line block comment\n*/\nSELECT 3;", "3")
    shell.run_test(
        "inline-comments",
        "SELECT id, -- comment here\nfirst_name FROM users LIMIT 1;",
        "1|Alice",
    )
    shell.quit()


def test_import_csv():
    shell = TestTursoShell()
    shell.run_test("memory-db", ".open :memory:", "")
    shell.run_test("create-csv-table", "CREATE TABLE csv_table (c1 INT, c2 REAL, c3 String);", "")
    shell.run_test(
        "import-csv-no-options",
        ".import --csv ./testing/test_files/test.csv csv_table",
        "",
    )
    shell.run_test(
        "verify-csv-no-options",
        "select * from csv_table;",
        "1|2.0|String'1\n3|4.0|String2",
    )
    shell.quit()


def test_import_csv_verbose():
    shell = TestTursoShell()
    shell.run_test("open-memory", ".open :memory:", "")
    shell.run_test("create-csv-table", "CREATE TABLE csv_table (c1 INT, c2 REAL, c3 String);", "")
    shell.run_test(
        "import-csv-verbose",
        ".import --csv -v ./testing/test_files/test.csv csv_table",
        "Added 2 rows with 0 errors using 2 lines of input",
    )
    shell.run_test(
        "verify-csv-verbose",
        "select * from csv_table;",
        "1|2.0|String'1\n3|4.0|String2",
    )
    shell.quit()


def test_import_csv_skip():
    shell = TestTursoShell()
    shell.run_test("open-memory", ".open :memory:", "")
    shell.run_test("create-csv-table", "CREATE TABLE csv_table (c1 INT, c2 REAL, c3 String);", "")
    shell.run_test(
        "import-csv-skip",
        ".import --csv --skip 1 ./testing/test_files/test.csv csv_table",
        "",
    )
    shell.run_test("verify-csv-skip", "select * from csv_table;", "3|4.0|String2")
    shell.quit()


def test_import_csv_create_table_from_header():
    shell = TestTursoShell()
    shell.run_test("open-memory", ".open :memory:", "")
    # Import CSV with header - should create table automatically
    shell.run_test(
        "import-csv-create-table",
        ".import --csv ./testing/test_files/test_w_header.csv auto_table",
        "",
    )
    # Verify table was created with correct column names
    shell.run_test(
        "verify-auto-table-schema",
        ".schema auto_table",
        "CREATE TABLE auto_table (id, interesting_number, interesting_string);",
    )
    # Verify data was imported correctly (header row excluded)
    shell.run_test(
        "verify-auto-table-data",
        "select * from auto_table;",
        "1|2.0|String'1\n3|4.0|String2",
    )
    shell.quit()


def test_table_patterns():
    shell = TestTursoShell()
    shell.run_test("tables-pattern", ".tables us%", "users")
    shell.quit()


def test_update_with_limit():
    turso = TestTursoShell(
        "CREATE TABLE t (a,b,c); insert into t values (1,2,3), (4,5,6), (7,8,9), (1,2,3),(4,5,6), (7,8,9);"
    )
    turso.run_test("update-limit", "UPDATE t SET a = 10 LIMIT 1;", "")
    turso.run_test("update-limit-result", "SELECT COUNT(*) from t WHERE a = 10;", "1")
    turso.run_test("update-limit-zero", "UPDATE t SET a = 100 LIMIT 0;", "")
    turso.run_test("update-limit-zero-result", "SELECT COUNT(*) from t WHERE a = 100;", "0")
    turso.run_test("update-limit-all", "UPDATE t SET a = 100 LIMIT -1;", "")
    # negative limit is treated as no limit in sqlite due to check for --val = 0
    turso.run_test("update-limit-result", "SELECT COUNT(*) from t WHERE a = 100;", "6")
    turso.run_test("udpate-limit-where", "UPDATE t SET a = 333 WHERE b = 5 LIMIT 1;", "")
    turso.run_test("update-limit-where-result", "SELECT COUNT(*) from t WHERE a = 333;", "1")
    turso.quit()


def test_update_with_limit_and_offset():
    turso = TestTursoShell(
        "CREATE TABLE t (a,b,c); insert into t values (1,2,3), (4,5,6), (7,8,9), (1,2,3),(4,5,6), (7,8,9);"
    )
    turso.run_test("update-limit-offset", "UPDATE t SET a = 10 LIMIT 1 OFFSET 3;", "")
    turso.run_test("update-limit-offset-result", "SELECT COUNT(*) from t WHERE a = 10;", "1")
    turso.run_test("update-limit-result", "SELECT a from t LIMIT 4;", "1\n4\n7\n10")
    turso.run_test("update-limit-offset-zero", "UPDATE t SET a = 100 LIMIT 0 OFFSET 0;", "")
    turso.run_test("update-limit-zero-result", "SELECT COUNT(*) from t WHERE a = 100;", "0")
    turso.run_test("update-limit-all", "UPDATE t SET a = 100 LIMIT -1 OFFSET 1;", "")
    turso.run_test("update-limit-result", "SELECT COUNT(*) from t WHERE a = 100;", "5")
    turso.run_test("udpate-limit-where", "UPDATE t SET a = 333 WHERE b = 5 LIMIT 1 OFFSET 2;", "")
    turso.run_test("update-limit-where-result", "SELECT COUNT(*) from t WHERE a = 333;", "0")
    turso.quit()


def test_insert_default_values():
    turso = TestTursoShell("CREATE TABLE t (a integer default(42),b integer default (43),c integer default(44));")
    for _ in range(1, 10):
        turso.execute_dot("INSERT INTO t DEFAULT VALUES;")
    turso.run_test("insert-default-values", "SELECT * FROM t;", "42|43|44\n" * 9)
    turso.quit()


def test_uri_readonly():
    turso = TestTursoShell(flags="file:testing/testing_small.db?mode=ro", init_commands="")
    turso.run_test("read-only-uri-reads-work", "SELECT COUNT(*) FROM demo;", "5")
    turso.run_test_fn(
        "INSERT INTO demo (id, value) values (6, 'demo');",
        lambda res: "read-only" in res,
        "read-only-uri-writes-fail",
    )
    turso.run_test_fn("CREATE TABLE t(a);", lambda res: "read-only" in res, "read-only-uri-cant-create-table")
    turso.run_test_fn("DROP TABLE demo;", lambda res: "read-only" in res, "read-only-uri-cant-drop-table")
    turso.init_test_db()
    turso.quit()


def test_copy_db_file():
    testpath = "testing/test_copy.db"
    if Path(testpath).exists():
        os.unlink(Path(testpath))
        time.sleep(0.2)  # make sure closed
    time.sleep(0.3)
    turso = TestTursoShell(init_commands="")
    turso.execute_dot("create table testing(a,b,c);")
    turso.run_test_fn(".schema", lambda x: "CREATE TABLE testing (a, b, c)" in x, "test-database-has-expected-schema")
    for i in range(100):
        turso.execute_dot(f"insert into testing (a,b,c) values ({i},{i + 1}, {i + 2});")
    turso.run_test_fn("SELECT COUNT(*) FROM testing;", lambda x: "100" == x, "test-database-has-expected-count")
    turso.run_test_fn(f".clone {testpath}", lambda res: "testing... done" in res)

    turso.execute_dot(f".open {testpath}")
    turso.run_test_fn(".schema", lambda x: "CREATE TABLE testing" in x, "test-copied-database-has-expected-schema")
    turso.run_test_fn("SELECT COUNT(*) FROM testing;", lambda x: "100" == x, "test-copied-database-has-expected-count")
    turso.quit()


def test_copy_memory_db_to_file():
    testpath = "testing/memory.db"
    if Path(testpath).exists():
        os.unlink(Path(testpath))
        time.sleep(0.2)  # make sure closed

    turso = TestTursoShell(init_commands="")
    turso.execute_dot("create table testing(a,b,c);")
    for i in range(100):
        turso.execute_dot(f"insert into testing (a, b, c) values ({i},{i + 1}, {i + 2});")
    turso.run_test_fn(f".clone {testpath}", lambda res: "testing... done" in res)
    turso.quit()
    time.sleep(0.3)
    sqlite = TestTursoShell(exec_name="sqlite3", flags=f" {testpath}")
    sqlite.run_test_fn(
        ".schema", lambda x: "CREATE TABLE testing (a, b, c)" in x, "test-copied-database-has-expected-schema"
    )
    sqlite.run_test_fn(
        "SELECT COUNT(*) FROM testing;", lambda x: "100" == x, "test-copied-database-has-expected-user-count"
    )
    sqlite.quit()


def test_parse_error():
    testpath = "testing/memory.db"
    if Path(testpath).exists():
        os.unlink(Path(testpath))
        time.sleep(0.2)  # make sure closed

    turso = TestTursoShell(init_commands="")
    turso.run_test_fn(
        "select * from sqlite_schema limit asdf;",
        lambda res: "Parse error: " in res,
        "Try to LIMIT using an identifier should trigger a Parse error",
    )
    turso.quit()


def test_tables_with_attached_db():
    shell = TestTursoShell()
    shell.execute_dot(".open :memory:")
    shell.execute_dot("CREATE TABLE orders(a);")
    shell.execute_dot("ATTACH DATABASE 'testing/testing.db' AS attached;")
    shell.run_test("tables-with-attached-database", ".tables", "orders attached.products attached.users")
    shell.quit()


def test_dbtotxt():
    shell = TestTursoShell(init_commands="")
    shell.run_test(
        "dbtotxt-empty",
        ".dbtotxt",
        "| size 0 pagesize 4096 filename :memory:\n| end :memory:",
    )
    shell.quit()

    shell = TestTursoShell(init_commands="")
    shell.execute_dot("CREATE TABLE t(x);")
    expected = (
        "| size 8192 pagesize 4096 filename :memory:\n"
        "| page 1 offset 0\n"
        "|      0: 53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00   SQLite format 3.\n"
        "|     16: 10 00 02 02 00 40 20 20 00 00 00 01 00 00 00 02   .....@  ........\n"
        "|     32: 00 00 00 00 00 00 00 00 00 00 00 01 00 00 00 04   ................\n"
        "|     48: ff ff f8 30 00 00 00 00 00 00 00 01 00 00 00 00   ...0............\n"
        "|     80: 00 00 00 00 00 00 00 00 00 00 00 00 00 2e 7e 58   ..............~X\n"
        "|     96: 00 2e 7e 58 0d 00 00 00 01 0f de 00 0f de 00 00   ..~X............\n"
        "|   4048: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 20 01   .............. .\n"
        "|   4064: 06 17 0f 0f 01 31 74 61 62 6c 65 74 74 02 43 52   .....1tablett.CR\n"
        "|   4080: 45 41 54 45 20 54 41 42 4c 45 20 74 20 28 78 29   EATE TABLE t (x)\n"
        "| page 2 offset 4096\n"
        "|      0: 0d 00 00 00 00 10 00 00 00 00 00 00 00 00 00 00   ................\n"
        "| end :memory:"
    )
    shell.run_test("dbtotxt-with-table", ".dbtotxt", expected)
    shell.quit()

def test_read_command():
    shell = TestTursoShell()
    try:
        shell.run_test("read-non-existing-file", ".read /12jo/ddwuidu/s.sql",
            (
                'Error: cannot open "/12jo/ddwuidu/s.sql" - '
                'No such file or directory (os error 2)'
            )
        )


        wrong_sql_file = Path("wrong.sql")
        wrong_sql_file.write_text("""
        DROP TABLE IF EXISTS students;
        CREATE TABLE students (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        );

        INSERT INTO students (name,email) VALUES ('Alice','a@a.com');

        -- THIS LINE IS INTENTIONALLY BROKEN
        INSRT INTO students (name,email) VALUES ('Broken','b@b.com');

        INSERT INTO students (name,email) VALUES ('Charlie','c@c.com');
        """)

        shell.run_test("read-wrong-query", ".read wrong.sql",
"""× unexpected token 'INSRT' at offset 55
╭─[4:9]
3 │         -- THIS LINE IS INTENTIONALLY BROKEN
4 │         INSRT INTO students (name,email) VALUES ('Broken','b@b.com');
·         ──┬──
·           ╰── here
╰────
help: expected BEGIN, COMMIT, END, ROLLBACK, SAVEPOINT, RELEASE, CREATE,
SELECT, VALUES, WITH, ANALYZE, ATTACH, DETACH, PRAGMA, VACUUM, ALTER,
DELETE, DROP, INSERT, REPLACE, UPDATE, or REINDEX but found 'INSRT'"""
        )

        emp_sql_file = Path("empty.sql")
        emp_sql_file.write_text("")
        shell.run_test("read-empty-file", ".read empty.sql", "")

        happy_sql_file = Path("happy.sql")
        happy_sql_file.write_text("""
        DROP TABLE IF EXISTS students;
        CREATE TABLE students (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        );

        INSERT INTO students (name,email) VALUES ('Alice','a@a.com');

        INSERT INTO students (name,email) VALUES ('Broken','b@b.com');

        INSERT INTO students (name,email) VALUES ('Charlie','c@c.com');
        """)
        shell.run_test("read-happy-file", ".read happy.sql", "")

        binary_sql_file = Path("binary_test.bin")
        binary_sql_file.write_bytes(os.urandom(512))

        shell.run_test("read-binary-file", ".read binary_test.bin",
            (
                'Error: cannot open "binary_test.bin" - '
                'stream did not contain valid UTF-8'
            )
        )

    finally:
        for f in ["wrong.sql", "empty.sql", "happy.sql", "binary_test.bin"]:
            p = Path(f)
            if p.exists():
                p.unlink()

    shell.quit()

def main():
    console.info("Running all turso CLI tests...")
    test_read_command()
    test_basic_queries()
    test_schema_operations()
    test_file_operations()
    test_joins()
    test_left_join_self()
    test_where_clauses()
    test_switch_back_to_in_memory()
    test_verify_null_value()
    test_output_file()
    test_multi_line_single_line_comments_succession()
    test_comments()
    test_import_csv()
    test_import_csv_verbose()
    test_import_csv_skip()
    test_import_csv_create_table_from_header()
    test_table_patterns()
    test_update_with_limit()
    test_update_with_limit_and_offset()
    test_uri_readonly()
    test_copy_db_file()
    test_copy_memory_db_to_file()
    test_parse_error()
    test_tables_with_attached_db()
    test_dbtotxt()
    console.info("All tests have passed")


if __name__ == "__main__":
    main()
