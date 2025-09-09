#!/usr/bin/env python3
import os

from cli_tests import console
from cli_tests.test_turso_cli import TestTursoShell

sqlite_exec = "./scripts/limbo-sqlite3"
sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")

test_data = """CREATE TABLE numbers ( id INTEGER PRIMARY KEY, value FLOAT NOT NULL, category TEXT DEFAULT 'A');
INSERT INTO numbers (value, category) VALUES (1.0, 'A');
INSERT INTO numbers (value, category) VALUES (2.0, 'A');
INSERT INTO numbers (value, category) VALUES (3.0, 'A');
INSERT INTO numbers (value, category) VALUES (4.0, 'B');
INSERT INTO numbers (value, category) VALUES (5.0, 'B');
INSERT INTO numbers (value, category) VALUES (6.0, 'B');
INSERT INTO numbers (value, category) VALUES (7.0, 'B');
CREATE TABLE test (value REAL, percent REAL, category TEXT);
INSERT INTO test values (10, 25, 'A');
INSERT INTO test values (20, 25, 'A');
INSERT INTO test values (30, 25, 'B');
INSERT INTO test values (40, 25, 'C');
INSERT INTO test values (50, 25, 'C');
INSERT INTO test values (60, 25, 'C');
INSERT INTO test values (70, 25, 'D');
"""


def validate_string_uuid(res):
    return len(res) == 36


def test_uuid():
    limbo = TestTursoShell()
    specific_time = "01945ca0-3189-76c0-9a8f-caf310fc8b8e"
    # these are built into the binary, so we just test they work
    limbo.run_test_fn(
        "SELECT hex(uuid4());",
        lambda res: int(res, 16) is not None,
        "uuid functions are registered properly with ext loaded",
    )
    limbo.run_test_fn("SELECT uuid4_str();", lambda res: len(res) == 36)
    limbo.run_test_fn("SELECT hex(uuid7());", lambda res: int(res, 16) is not None)
    limbo.run_test_fn("SELECT uuid7_timestamp_ms(uuid7()) / 1000;", lambda res: res.isdigit())
    limbo.run_test_fn("SELECT uuid7_str();", validate_string_uuid)
    limbo.run_test_fn("SELECT uuid_str(uuid7());", validate_string_uuid)
    limbo.run_test_fn("SELECT hex(uuid_blob(uuid7_str()));", lambda res: int(res, 16) is not None)
    limbo.run_test_fn("SELECT uuid_str(uuid_blob(uuid7_str()));", validate_string_uuid)
    limbo.run_test_fn(
        f"SELECT uuid7_timestamp_ms('{specific_time}') / 1000;",
        lambda res: res == "1736720789",
    )
    limbo.run_test_fn(
        "SELECT gen_random_uuid();",
        validate_string_uuid,
        "scalar alias's are registered properly",
    )
    limbo.quit()


def true(res):
    return res == "1"


def false(res):
    return res == "0"


def null(res):
    return res == ""


def test_regexp():
    limbo = TestTursoShell(test_data)
    extension_path = "./target/debug/liblimbo_regexp"
    # before extension loads, assert no function
    limbo.run_test_fn(
        "SELECT regexp('a.c', 'abc');",
        lambda res: "Parse error: no such function" in res,
    )
    limbo.run_test_fn(f".load {extension_path}", null)
    console.info(f"Extension {extension_path} loaded successfully.")
    limbo.run_test_fn("SELECT regexp('a.c', 'abc');", true)
    limbo.run_test_fn("SELECT regexp('a.c', 'ac');", false)
    limbo.run_test_fn("SELECT regexp('[0-9]+', 'the year is 2021');", true)
    limbo.run_test_fn("SELECT regexp('[0-9]+', 'the year is unknow');", false)
    limbo.run_test_fn("SELECT regexp_like('the year is 2021', '[0-9]+');", true)
    limbo.run_test_fn("SELECT regexp_like('the year is unknow', '[0-9]+');", false)
    limbo.run_test_fn(
        "SELECT regexp_substr('the year is 2021', '[0-9]+') = '2021';",
        true,
    )
    limbo.run_test_fn("SELECT regexp_substr('the year is unknow', '[0-9]+');", null)
    limbo.run_test_fn(
        "select regexp_replace('the year is 2021', '[0-9]+', '2050') = 'the year is 2050';",
        true,
    )
    limbo.run_test_fn(
        "select regexp_replace('the year is 2021', '2k21', '2050') = 'the year is 2021';",
        true,
    )
    limbo.run_test_fn(
        "select regexp_replace('the year is 2021', '([0-9]+)', '$1 or 2050') = 'the year is 2021 or 2050';",
        true,
    )
    limbo.run_test_fn(
        "select regexp_capture('the year is 2021', '([0-9]+)') = '2021';",
        true,
    )
    limbo.run_test_fn(
        "select regexp_capture('abc 123 def', '([a-z]+) ([0-9]+) ([a-z]+)', 2) = '123';",
        true,
    )
    limbo.run_test_fn(
        "select regexp_capture('no digits here', '([0-9]+)');",
        null,
    )
    limbo.quit()


def validate_median(res):
    return res == "4.0"


def validate_median_odd(res):
    return res == "4.5"


def validate_percentile1(res):
    return res == "25.0"


def validate_percentile2(res):
    return res == "43.0"


def validate_percentile_disc(res):
    return res == "40.0"


def test_aggregates():
    limbo = TestTursoShell(init_commands=test_data)
    extension_path = "./target/debug/liblimbo_percentile"
    # assert no function before extension loads
    limbo.run_test_fn(
        "SELECT median(1);",
        lambda res: "error: no such function: " in res,
        "median agg function returns null when ext not loaded",
    )
    limbo.execute_dot(f".load {extension_path}")
    limbo.run_test_fn(
        "select median(value) from numbers;",
        validate_median,
        "median agg function works",
    )
    limbo.run_test_fn(
        "select CASE WHEN median(value) > 0 THEN median(value) ELSE 0 END from numbers;",
        validate_median,
        "median agg function wrapped in expression works",
    )
    limbo.execute_dot("INSERT INTO numbers (value) VALUES (8.0);\n")
    limbo.run_test_fn(
        "select median(value) from numbers;",
        validate_median_odd,
        "median agg function works with odd number of elements",
    )
    limbo.run_test_fn(
        "SELECT percentile(value, percent) from test;",
        validate_percentile1,
        "test aggregate percentile function with 2 arguments works",
    )
    limbo.run_test_fn(
        "SELECT percentile(value, 55) from test;",
        validate_percentile2,
        "test aggregate percentile function with 1 argument works",
    )
    limbo.run_test_fn("SELECT percentile_cont(value, 0.25) from test;", validate_percentile1)
    limbo.run_test_fn("SELECT percentile_disc(value, 0.55) from test;", validate_percentile_disc)
    limbo.quit()


def test_grouped_aggregates():
    limbo = TestTursoShell(init_commands=test_data)
    extension_path = "./target/debug/liblimbo_percentile"
    limbo.execute_dot(f".load {extension_path}")

    limbo.run_test_fn(
        "SELECT median(value) FROM numbers GROUP BY category;",
        lambda res: "2.0\n5.5" == res,
        "median aggregate function works",
    )
    limbo.run_test_fn(
        "select CASE WHEN median(value) > 0 THEN median(value) ELSE 0 END from numbers GROUP BY category;",
        lambda res: "2.0\n5.5" == res,
        "median aggregate function wrapped in expression works",
    )
    limbo.run_test_fn(
        "SELECT percentile(value, percent) FROM test GROUP BY category;",
        lambda res: "12.5\n30.0\n45.0\n70.0" == res,
        "grouped aggregate percentile function with 2 arguments works",
    )
    limbo.run_test_fn(
        "SELECT percentile(value, 55) FROM test GROUP BY category;",
        lambda res: "15.5\n30.0\n51.0\n70.0" == res,
        "grouped aggregate percentile function with 1 argument works",
    )
    limbo.run_test_fn(
        "SELECT percentile_cont(value, 0.25) FROM test GROUP BY category;",
        lambda res: "12.5\n30.0\n45.0\n70.0" == res,
        "grouped aggregate percentile_cont function works",
    )
    limbo.run_test_fn(
        "SELECT percentile_disc(value, 0.55) FROM test GROUP BY category;",
        lambda res: "10.0\n30.0\n50.0\n70.0" == res,
        "grouped aggregate percentile_disc function works",
    )
    limbo.quit()


# Encoders and decoders
def validate_url_encode(a):
    return a == "%2Fhello%3Ftext%3D%28%E0%B2%A0_%E0%B2%A0%29"


def validate_url_decode(a):
    return a == "/hello?text=(ಠ_ಠ)"


def validate_hex_encode(a):
    return a == "68656c6c6f"


def validate_hex_decode(a):
    return a == "hello"


def validate_base85_encode(a):
    return a == "BOu!rDZ"


def validate_base85_decode(a):
    return a == "hello"


def validate_base32_encode(a):
    return a == "NBSWY3DP"


def validate_base32_decode(a):
    return a == "hello"


def validate_base64_encode(a):
    return a == "aGVsbG8="


def validate_base64_decode(a):
    return a == "hello"


def test_crypto():
    limbo = TestTursoShell()
    extension_path = "./target/debug/liblimbo_crypto"
    # assert no function before extension loads
    limbo.run_test_fn(
        "SELECT crypto_blake('a');",
        lambda res: "Parse error" in res,
        "crypto_blake3 returns null when ext not loaded",
    )
    limbo.execute_dot(f".load {extension_path}")
    # Hashing and Decode
    limbo.run_test_fn(
        "SELECT crypto_encode(crypto_blake3('abc'), 'hex');",
        lambda res: res == "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85",
        "blake3 should encrypt correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode(crypto_md5('abc'), 'hex');",
        lambda res: res == "900150983cd24fb0d6963f7d28e17f72",
        "md5 should encrypt correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode(crypto_sha1('abc'), 'hex');",
        lambda res: res == "a9993e364706816aba3e25717850c26c9cd0d89d",
        "sha1 should encrypt correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode(crypto_sha256('abc'), 'hex');",
        lambda a: a == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "sha256 should encrypt correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode(crypto_sha384('abc'), 'hex');",
        lambda a: a
        == "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
        "sha384 should encrypt correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode(crypto_sha512('abc'), 'hex');",
        lambda a: a
        == "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",  # noqa: E501
        "sha512 should encrypt correctly",
    )

    # Encoding and Decoding
    limbo.run_test_fn(
        "SELECT crypto_encode('hello', 'base32');",
        validate_base32_encode,
        "base32 should encode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_decode('NBSWY3DP', 'base32');",
        validate_base32_decode,
        "base32 should decode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode('hello', 'base64');",
        validate_base64_encode,
        "base64 should encode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_decode('aGVsbG8=', 'base64');",
        validate_base64_decode,
        "base64 should decode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode('hello', 'base85');",
        validate_base85_encode,
        "base85 should encode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_decode('BOu!rDZ', 'base85');",
        validate_base85_decode,
        "base85 should decode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode('hello', 'hex');",
        validate_hex_encode,
        "hex should encode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_decode('68656c6c6f', 'hex');",
        validate_hex_decode,
        "hex should decode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_encode('/hello?text=(ಠ_ಠ)', 'url');",
        validate_url_encode,
        "url should encode correctly",
    )
    limbo.run_test_fn(
        "SELECT crypto_decode('%2Fhello%3Ftext%3D%28%E0%B2%A0_%E0%B2%A0%29', 'url');",
        validate_url_decode,
        "url should decode correctly",
    )
    limbo.quit()


def test_series():
    console.info("Running test_series for Limbo")
    limbo = TestTursoShell()
    _test_series(limbo)

    console.info("Running test_series for SQLite")
    limbo = TestTursoShell(exec_name="sqlite3")
    _test_series(limbo)


def _test_series(limbo: TestTursoShell):
    limbo.run_test_fn(
        "SELECT * FROM generate_series(1, 10);",
        lambda res: res == "1\n2\n3\n4\n5\n6\n7\n8\n9\n10",
    )
    limbo.run_test_fn(
        "SELECT * FROM generate_series;",
        lambda res: "Invalid Argument" in res or 'first argument to "generate_series()" missing or unusable' in res,
    )
    limbo.run_test_fn(
        "SELECT * FROM generate_series(1, 10, 2);",
        lambda res: res == "1\n3\n5\n7\n9",
    )
    limbo.run_test_fn(
        "SELECT * FROM generate_series(10, 1, -2);",
        lambda res: res == "10\n8\n6\n4\n2",
    )
    limbo.run_test_fn(
        "SELECT * FROM generate_series(b.start, b.stop) b;",
        lambda res: "Invalid Argument" in res or 'first argument to "generate_series()" missing or unusable' in res,
        "self-reference in generate_series arguments",
    )
    limbo.quit()


def test_kv():
    _test_kv(exec_name=None, ext_path="target/debug/libturso_ext_tests")
    _test_kv(exec_name="sqlite3", ext_path="target/debug/liblimbo_sqlite_test_ext")


def _test_kv(exec_name, ext_path):
    console.info(f"Running test_kv for {ext_path} in {exec_name}")

    limbo = TestTursoShell(
        exec_name=exec_name,
    )
    # first, create a normal table to ensure no issues
    limbo.execute_dot("CREATE TABLE other (a,b,c);")
    limbo.execute_dot("INSERT INTO other values (23,32,23);")
    limbo.run_test_fn(
        "create virtual table t using kv_store;",
        lambda res: "no such module: kv_store" in res,
    )
    limbo.execute_dot(f".load {ext_path}")
    limbo.execute_dot(
        "create virtual table t using kv_store;",
    )
    limbo.run_test_fn(".schema", lambda res: "CREATE VIRTUAL TABLE t" in res)
    limbo.run_test_fn(
        "insert into t values ('hello', 'world');",
        null,
        "can insert into kv_store vtable",
    )
    limbo.run_test_fn(
        "select value from t where key = 'hello';",
        lambda res: "world" == res,
        "can select from kv_store",
    )
    limbo.run_test_fn(
        "delete from t where key = 'hello';",
        null,
        "can delete from kv_store",
    )
    limbo.run_test_fn("insert into t values ('other', 'value');", null)
    limbo.run_test_fn(
        "select value from t where key = 'hello';",
        lambda res: "" == res,
        "proper data is deleted",
    )
    limbo.run_test_fn(
        "select * from t;",
        lambda res: "other|value" == res,
        "can select after deletion",
    )
    limbo.run_test_fn(
        "delete from t where key = 'other';",
        null,
        "can delete from kv_store",
    )
    limbo.run_test_fn(
        "select * from t;",
        lambda res: "" == res,
        "can select empty table without error",
    )
    limbo.run_test_fn(
        "delete from t;",
        null,
        "can delete from empty table without error",
    )
    for i in range(100):
        limbo.execute_dot(f"insert into t values ('key{i}', 'val{i}');")
    limbo.run_test_fn("select count(*) from t;", lambda res: "100" == res, "can insert 100 rows")
    limbo.run_test_fn("update t set value = 'updated' where key = 'key33';", null)
    limbo.run_test_fn(
        "select * from t where key = 'key33';",
        lambda res: res == "key33|updated",
        "can update single row",
    )
    limbo.run_test_fn(
        "select COUNT(*) from t where value = 'updated';",
        lambda res: res == "1",
        "only updated a single row",
    )
    limbo.run_test_fn("update t set value = 'updated2';", null)
    limbo.run_test_fn(
        "select COUNT(*) from t where value = 'updated2';",
        lambda res: res == "100",
        "can update all rows",
    )
    if exec_name is None:
        # Test only on Limbo, since SQLite supports the DELETE ... LIMIT syntax only when compiled
        # with the SQLITE_ENABLE_UPDATE_DELETE_LIMIT option: https://www.sqlite.org/lang_delete.html
        limbo.run_test_fn("delete from t limit 96;", null, "can delete 96 rows")
        limbo.run_test_fn("select count(*) from t;", lambda res: "4" == res, "four rows remain")
    limbo.run_test_fn("update t set key = '100' where 1;", null, "where clause evaluates properly")
    limbo.run_test_fn(
        "select * from t where key = '100';",
        lambda res: res == "100|updated2",
        "there is only 1 key remaining after setting all keys to same value",
    )
    limbo.run_test_fn(
        "select * from t a, other b where b.c = 23 and a.key='100';",
        lambda res: "100|updated2|23|32|23" == res,
    )
    limbo.quit()


def test_ipaddr():
    limbo = TestTursoShell()
    ext_path = "./target/debug/liblimbo_ipaddr"

    limbo.run_test_fn(
        "SELECT ipfamily('192.168.1.1');",
        lambda res: "error: no such function: " in res,
        "ipfamily function returns null when ext not loaded",
    )
    limbo.execute_dot(f".load {ext_path}")

    limbo.run_test_fn(
        "SELECT ipfamily('192.168.1.1');",
        lambda res: "4" == res,
        "ipfamily function returns 4 for IPv4",
    )
    limbo.run_test_fn(
        "SELECT ipfamily('2001:db8::1');",
        lambda res: "6" == res,
        "ipfamily function returns 6 for IPv6",
    )

    limbo.run_test_fn(
        "SELECT ipcontains('192.168.16.0/24', '192.168.16.3');",
        lambda res: "1" == res,
        "ipcontains function returns 1 for IPv4",
    )
    limbo.run_test_fn(
        "SELECT ipcontains('192.168.1.0/24', '192.168.2.1');",
        lambda res: "0" == res,
        "ipcontains function returns 0 for IPv4",
    )

    limbo.run_test_fn(
        "SELECT iphost('192.168.1.0/24');",
        lambda res: "192.168.1.0" == res,
        "iphost function returns the host for IPv4",
    )
    limbo.run_test_fn(
        "SELECT iphost('2001:db8::1/128');",
        lambda res: "2001:db8::1" == res,
        "iphost function returns the host for IPv6",
    )

    limbo.run_test_fn(
        "SELECT ipmasklen('192.168.1.0/24');",
        lambda res: "24" == res,
        "ipmasklen function returns the mask length for IPv4",
    )
    limbo.run_test_fn(
        "SELECT ipmasklen('2001:db8::1');",
        lambda res: "128" == res,
        "ipmasklen function returns the mask length for IPv6",
    )

    limbo.run_test_fn(
        "SELECT ipnetwork('192.168.16.12/24');",
        lambda res: "192.168.16.0/24" == res,
        "ipnetwork function returns the flattened CIDR for IPv4",
    )
    limbo.run_test_fn(
        "SELECT ipnetwork('2001:db8::1');",
        lambda res: "2001:db8::1/128" == res,
        "ipnetwork function returns the network for IPv6",
    )
    limbo.quit()


def test_vfs():
    limbo = TestTursoShell()
    ext_path = "target/debug/libturso_ext_tests"
    limbo.run_test_fn(".vfslist", lambda x: "testvfs" not in x, "testvfs not loaded")
    limbo.execute_dot(f".load {ext_path}")
    limbo.run_test_fn(".vfslist", lambda res: "testvfs" in res, "testvfs extension loaded")
    limbo.execute_dot(".open testing/vfs.db testvfs")
    limbo.execute_dot("create table test (id integer primary key, value float);")
    limbo.execute_dot("create table vfs (id integer primary key, value blob);")
    for i in range(50):
        limbo.execute_dot("insert into test (value) values (randomblob(32*1024));")
        limbo.execute_dot(f"insert into vfs (value) values ({i});")
    limbo.run_test_fn(
        "SELECT count(*) FROM test;",
        lambda res: res == "50",
        "Tested large write to testfs",
    )
    limbo.run_test_fn(
        "SELECT count(*) FROM vfs;",
        lambda res: res == "50",
        "Tested large write to testfs",
    )
    console.info("Tested large write to testfs")
    limbo.quit()


def test_sqlite_vfs_compat():
    sqlite = TestTursoShell(
        init_commands="",
        exec_name="sqlite3",
        flags="testing/vfs.db",
    )
    sqlite.run_test_fn(
        ".show",
        lambda res: "filename: testing/vfs.db" in res,
        "Opened db file created with vfs extension in sqlite3",
    )
    sqlite.run_test_fn(
        ".schema",
        lambda res: "CREATE TABLE test (id integer PRIMARY KEY, value float);" in res,
        "Tables created by vfs extension exist in db file",
    )
    sqlite.run_test_fn(
        "SELECT count(*) FROM test;",
        lambda res: res == "50",
        "Tested large write to testfs",
    )
    sqlite.run_test_fn(
        "SELECT count(*) FROM vfs;",
        lambda res: res == "50",
        "Tested large write to testfs",
    )
    sqlite.quit()


def test_csv():
    # open new empty connection explicitly to test whether we can load an extension
    # with brand new connection/uninitialized database.
    limbo = TestTursoShell(init_commands="")
    test_module_list(limbo, "target/debug/liblimbo_csv", "csv")

    limbo.run_test_fn(
        "CREATE VIRTUAL TABLE temp.csv USING csv(filename=./testing/test_files/test.csv);",
        null,
        "Create virtual table from CSV file",
    )
    limbo.run_test_fn(
        "SELECT * FROM temp.csv;",
        lambda res: res == "1|2.0|String'1\n3|4.0|String2",
        "Read all rows from CSV table",
    )
    limbo.run_test_fn(
        "SELECT * FROM temp.csv WHERE c2 = 'String2';",
        lambda res: res == "3|4.0|String2",
        "Filter rows with WHERE clause",
    )
    limbo.run_test_fn(
        "INSERT INTO temp.csv VALUES (5, 6.0, 'String3');",
        lambda res: "Table is read-only" in res,
        "INSERT into CSV table should fail",
    )
    limbo.run_test_fn(
        "UPDATE temp.csv SET c0 = 10 WHERE c1 = '2.0';",
        lambda res: "is read-only" in res,
        "UPDATE on CSV table should fail",
    )
    limbo.run_test_fn(
        "DELETE FROM temp.csv WHERE c1 = '2.0';",
        lambda res: "is read-only" in res,
        "DELETE on CSV table should fail",
    )
    limbo.run_test_fn("DROP TABLE temp.csv;", null, "Drop CSV table")
    limbo.run_test_fn(
        "SELECT * FROM temp.csv;",
        lambda res: "Parse error: no such table: csv" in res,
        "Query dropped CSV table should fail",
    )
    limbo.run_test_fn(
        "create virtual table t1 using csv(data='1'\\'2');",
        lambda res: "unrecognized token at" in res,
        "Create CSV table with malformed escape sequence",
    )
    limbo.run_test_fn(
        "create virtual table t1 using csv(data=\"12');",
        lambda res: "non-terminated literal at" in res,
        "Create CSV table with unterminated quoted string",
    )

    limbo.run_debug("create virtual table t1 using csv(data='');")
    limbo.run_test_fn(
        "SELECT c0 FROM t1;",
        lambda res: res == "",
        "Empty CSV table without a header should have one column: 'c0'",
    )
    limbo.run_test_fn(
        "SELECT c1 FROM t1;",
        lambda res: "Parse error: no such column: c1" in res,
        "Empty CSV table without header should not have columns other than 'c0'",
    )

    limbo.run_debug("create virtual table t2 using csv(data='', header=true);")
    limbo.run_test_fn(
        'SELECT "(NULL)" FROM t2;',
        lambda res: res == "",
        "Empty CSV table with header should have one column named '(NULL)'",
    )
    limbo.run_test_fn(
        "SELECT c0 FROM t2;",
        lambda res: "Parse error: no such column: c0" in res,
        "Empty CSV table with header should not have columns other than '(NULL)'",
    )

    limbo.quit()


def cleanup():
    if os.path.exists("testing/vfs.db"):
        os.remove("testing/vfs.db")
    if os.path.exists("testing/vfs.db-wal"):
        os.remove("testing/vfs.db-wal")


def test_tablestats():
    ext_path = "target/debug/libturso_ext_tests"
    limbo = TestTursoShell(use_testing_db=True)
    test_module_list(limbo, ext_path=ext_path, module_name="tablestats")
    limbo.execute_dot("CREATE TABLE people(id INTEGER PRIMARY KEY, name TEXT);")
    limbo.execute_dot("INSERT INTO people(name) VALUES ('Ada'), ('Grace'), ('Linus');")

    limbo.execute_dot("CREATE TABLE logs(ts INT, msg TEXT);")
    limbo.execute_dot("INSERT INTO logs VALUES (1,'boot ok');")

    # verify counts
    limbo.run_test_fn(
        "SELECT COUNT(*) FROM people;",
        lambda res: res == "3",
        "three people rowsverify user count",
    )
    limbo.run_test_fn(
        "SELECT COUNT(*) FROM logs;",
        lambda res: res == "1",
        "one logs rowverify logs count",
    )
    limbo.execute_dot("CREATE VIRTUAL TABLE stats USING tablestats;")

    def _split(res):
        return [ln.strip() for ln in res.splitlines() if ln.strip()]

    limbo.run_test_fn(
        "SELECT * FROM stats ORDER BY name;",
        lambda res: sorted(_split(res)) == sorted(["logs|1", "people|3", "products|11", "users|10000"]),
        "stats shows correct initial counts (and skips itself)",
    )

    limbo.execute_dot("INSERT INTO logs VALUES (2,'panic'), (3,'recovery');")
    limbo.execute_dot("DELETE FROM people WHERE name='Linus';")

    limbo.run_test_fn(
        "SELECT * FROM stats WHERE name='logs';",
        lambda res: res == "logs|3",
        "row‑count grows after INSERT",
    )
    limbo.run_test_fn(
        "SELECT * FROM stats WHERE name='people';",
        lambda res: res == "people|2",
        "row‑count shrinks after DELETE",
    )

    # existing tables in the testing database (users, products)
    # the test extension is also doing a testing insert on every query
    # as part of its own testing, so we cannot assert 'products|11'
    # we need to add 3 for the 3 queries we did above.
    limbo.run_test_fn(
        "SELECT * FROM stats WHERE name='products';",
        lambda x: x == "products|14",
        "products table reflects changes",
    )
    # an insert to products with (name,price) ('xConnect', 42)
    # has happened on each query (4 so far) in the testing extension.
    # so at this point the sum should be 168
    limbo.run_test_fn(
        "SELECT sum(price) FROM products WHERE name = 'xConnect';",
        lambda x: x == "168.0",
        "price sum for 'xConnect' inserts happenning in the testing extension",
    )

    limbo.run_test_fn(
        "SELECT * FROM stats WHERE name='users';",
        lambda x: x == "users|10000",
        "users table unchanged",
    )

    limbo.execute_dot("CREATE TABLE misc(x);")
    limbo.run_test_fn(
        "SELECT * FROM stats WHERE name='misc';",
        lambda res: res == "misc|0",
        "newly‑created table shows up with zero rows",
    )

    limbo.execute_dot("DROP TABLE logs;")
    limbo.run_test_fn(
        "SELECT name FROM stats WHERE name='logs';",
        lambda res: res == "",
        "dropped table disappears from stats",
    )
    limbo.quit()


def test_module_list(turso_shell, ext_path, module_name):
    """loads the extension at the provided path and asserts that 'PRAGMA module_list;' displays 'module_name'"""
    console.info(f"Running test_module_list for {ext_path}")

    turso_shell.run_test_fn(
        "PRAGMA module_list;",
        lambda res: "generate_series" in res and module_name not in res,
        "lists built in modules but doesn't contain the module name yet",
    )

    turso_shell.run_test_fn("PRAGMA module_list;", lambda res: module_name not in res, "does not include module list")
    turso_shell.execute_dot(f".load {ext_path}")
    turso_shell.run_test_fn(
        "PRAGMA module_list;",
        lambda res: module_name in res,
        f"includes {module_name} after loading extension",
    )


def main():
    try:
        test_regexp()
        test_uuid()
        test_aggregates()
        test_grouped_aggregates()
        test_crypto()
        test_series()
        test_ipaddr()
        test_vfs()
        test_sqlite_vfs_compat()
        test_kv()
        test_csv()
        test_tablestats()
    except Exception as e:
        console.error(f"Test FAILED: {e}")
        cleanup()
        exit(1)
    cleanup()
    console.info("All tests passed successfully.")


if __name__ == "__main__":
    main()
