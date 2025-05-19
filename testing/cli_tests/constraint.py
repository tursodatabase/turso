#!/usr/bin/env python3

# Eventually extract these tests to be in the fuzzing integration tests
import os
from faker import Faker
from faker.providers.lorem.en_US import Provider as P
from cli_tests.test_limbo_cli import TestLimboShell
from pydantic import BaseModel
from cli_tests import console
from enum import Enum
import random
import sqlite3

sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


keywords = [
    "ABORT",
    "ACTION",
    "ADD",
    "AFTER",
    "ALL",
    "ALTER",
    "ALWAYS",
    "ANALYZE",
    "AND",
    "AS",
    "ASC",
    "ATTACH",
    "AUTOINCREMENT",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BY",
    "CASCADE",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "COMMIT",
    "CONFLICT",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "DATABASE",
    "DEFAULT",
    "DEFERRABLE",
    "DEFERRED",
    "DELETE",
    "DESC",
    "DETACH",
    "DISTINCT",
    "DO",
    "DROP",
    "EACH",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXCLUDE",
    "EXCLUSIVE",
    "EXISTS",
    "EXPLAIN",
    "FAIL",
    "FILTER",
    "FIRST",
    "FOLLOWING",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "GENERATED",
    "GLOB",
    "GROUP",
    "GROUPS",
    "HAVING",
    "IF",
    "IGNORE",
    "IMMEDIATE",
    "IN",
    "INDEX",
    "INDEXED",
    "INITIALLY",
    "INNER",
    "INSERT",
    "INSTEAD",
    "INTERSECT",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "KEY",
    "LAST",
    "LEFT",
    "LIKE",
    "LIMIT",
    "MATCH",
    "MATERIALIZED",
    "NATURAL",
    "NO",
    "NOT",
    "NOTHING",
    "NOTNULL",
    "NULL",
    "NULLS",
    "OF",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OTHERS",
    "OUTER",
    "OVER",
    "PARTITION",
    "PLAN",
    "PRAGMA",
    "PRECEDING",
    "PRIMARY",
    "QUERY",
    "RAISE",
    "RANGE",
    "RECURSIVE",
    "REFERENCES",
    "REGEXP",
    "REINDEX",
    "RELEASE",
    "RENAME",
    "REPLACE",
    "RESTRICT",
    "RETURNING",
    "RIGHT",
    "ROLLBACK",
    "ROW",
    "ROWS",
    "SAVEPOINT",
    "SELECT",
    "SET",
    "TABLE",
    "TEMP",
    "TEMPORARY",
    "THEN",
    "TIES",
    "TO",
    "TRANSACTION",
    "TRIGGER",
    "UNBOUNDED",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USING",
    "VACUUM",
    "VALUES",
    "VIEW",
    "VIRTUAL",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "WITHOUT",
]
P.word_list = tuple(word for word in P.word_list if word.upper() not in keywords)
del P
fake: Faker = Faker(locale="en_US").unique
Faker.seed(0)


class ColumnType(Enum):
    blob = "blob"
    integer = "integer"
    real = "real"
    text = "text"

    def generate(self, faker: Faker) -> str:
        match self.value:
            case "blob":
                blob = sqlite3.Binary(faker.binary(length=4)).hex()
                return f"x'{blob}'"
            case "integer":
                return str(faker.pyint())
            case "real":
                return str(faker.pyfloat())
            case "text":
                return f"'{faker.text(max_nb_chars=20)}'"

    def __str__(self) -> str:
        return self.value.upper()


class Column(BaseModel):
    name: str
    col_type: ColumnType
    primary_key: bool

    def generate(faker: Faker) -> "Column":
        name = faker.word().replace(" ", "_")
        return Column(
            name=name,
            col_type=Faker().enum(ColumnType),
            primary_key=False,
        )

    def __str__(self) -> str:
        return f"{self.name} {str(self.col_type)}"


class Table(BaseModel):
    columns: list[Column]
    name: str

    def create_table(self) -> str:
        accum = f"CREATE TABLE {self.name} "
        col_strings = [str(col) for col in self.columns]

        pk_columns = [col.name for col in self.columns if col.primary_key]
        primary_key_stmt = "PRIMARY KEY (" + ", ".join(pk_columns) + ")"
        col_strings.append(primary_key_stmt)

        accum = accum + "(" + ", ".join(col_strings) + ");"

        return accum

    def generate_insert(self) -> str:
        vals = [col.col_type.generate(fake) for col in self.columns]
        vals = ", ".join(vals)

        return f"INSERT INTO {self.name} VALUES ({vals});"

    # These statements should always cause a constraint error as there is no where clause here
    def generate_update(self) -> str:
        vals = [
            f"{col.name} = {col.col_type.generate(fake)}"
            for col in self.columns
            if col.primary_key
        ]
        vals = ", ".join(vals)

        return f"UPDATE {self.name} SET {vals};"


class ConstraintTest(BaseModel):
    table: Table
    db_path: str = "testing/constraint.db"
    insert_stmts: list[str]
    insert_errors: list[str]
    update_errors: list[str]

    def run(
        self,
        limbo: TestLimboShell,
    ):
        big_stmt = [self.table.create_table()]
        for insert_stmt in self.insert_stmts:
            big_stmt.append(insert_stmt)

        limbo.run_test("Inserting values into table", "\n".join(big_stmt), "")

        for insert_stmt in self.insert_errors:
            limbo.run_test_fn(
                insert_stmt,
                lambda val: "Runtime error: UNIQUE constraint failed" in val,
            )
        limbo.run_test(
            "Nothing was inserted after error",
            f"SELECT count(*) from {self.table.name};",
            str(len(self.insert_stmts)),
        )

        for update_stmt in self.update_errors:
            limbo.run_test_fn(
                update_stmt,
                lambda val: "Runtime error: UNIQUE constraint failed" in val,
            )

        # TODO: When we implement rollbacks, have a test here to assure the values did not change


def validate_with_expected(result: str, expected: str):
    return (expected in result, expected)


def generate_test(col_amount: int, primary_keys: int) -> ConstraintTest:
    assert col_amount >= primary_keys, "Cannot have more primary keys than columns"
    cols: list[Column] = []
    for _ in range(col_amount):
        cols.append(Column.generate(fake))

    pk_cols = random.sample(
        population=cols,
        k=primary_keys,
    )

    for col in pk_cols:
        for c in cols:
            if col.name == c.name:
                c.primary_key = True

    table = Table(columns=cols, name=fake.word())
    insert_stmts = [table.generate_insert() for _ in range(col_amount)]

    update_errors = []
    if len(insert_stmts) > 1:
        # TODO: As we have no rollback we just generate one update statement
        update_errors = [table.generate_update()]

    return ConstraintTest(
        table=table,
        insert_stmts=insert_stmts,
        insert_errors=insert_stmts,
        update_errors=update_errors,
    )


def custom_test_1() -> ConstraintTest:
    cols = [
        Column(name="id", col_type="integer", primary_key=True),
        Column(name="username", col_type="text", primary_key=True),
    ]
    table = Table(columns=cols, name="users")
    insert_stmts = [
        "INSERT INTO users VALUES (1, 'alice');",
        "INSERT INTO users VALUES (2, 'bob');",
    ]
    update_stmts = [
        "UPDATE users SET id = 2, username = 'bob' WHERE id == 1;",
    ]
    return ConstraintTest(
        table=table,
        insert_stmts=insert_stmts,
        insert_errors=insert_stmts,
        update_errors=update_stmts,
    )


def custom_test_2(limbo: TestLimboShell):
    create = "CREATE TABLE users (id INT PRIMARY KEY, username TEXT);"
    first_insert = "INSERT INTO users VALUES (1, 'alice');"
    limbo.run_test("Create unique INT index", create + first_insert, "")
    fail_insert = "INSERT INTO users VALUES (1, 'bob');"
    limbo.run_test_fn(
        fail_insert,
        lambda val: "Runtime error: UNIQUE constraint failed" in val,
    )


# Issue #1482
def regression_test_update_single_key(limbo: TestLimboShell):
    create = "CREATE TABLE t(a unique);"
    first_insert = "INSERT INTO t VALUES (1);"
    limbo.run_test("Create simple table with 1 unique value", create + first_insert, "")
    update_single = "UPDATE t SET a=1 WHERE a=1;"
    limbo.run_test("Update one single key to the same value", update_single, "")


def all_tests() -> list[ConstraintTest]:
    tests: list[ConstraintTest] = []
    max_cols = 10

    curr_fake = Faker()
    for _ in range(25):
        num_cols = curr_fake.pyint(1, max_cols)
        test = generate_test(num_cols, curr_fake.pyint(1, num_cols))
        tests.append(test)

    tests.append(custom_test_1())
    return tests


def cleanup(db_fullpath: str):
    wal_path = f"{db_fullpath}-wal"
    shm_path = f"{db_fullpath}-shm"
    paths = [db_fullpath, wal_path, shm_path]
    for path in paths:
        if os.path.exists(path):
            os.remove(path)


def main():
    tests = all_tests()
    for test in tests:
        console.info(test.table)
        db_path = test.db_path
        try:
            # Use with syntax to automatically close shell on error
            with TestLimboShell("") as limbo:
                limbo.execute_dot(f".open {db_path}")
                test.run(limbo)

        except Exception as e:
            console.error(f"Test FAILED: {e}")
            console.debug(test.table.create_table(), test.insert_stmts)
            cleanup(db_path)
            exit(1)
        # delete db after every compat test so we we have fresh db for next test
        cleanup(db_path)

    db_path = "testing/constraint.db"
    tests = [custom_test_2, regression_test_update_single_key]
    for test in tests:
        try:
            with TestLimboShell("") as limbo:
                limbo.execute_dot(f".open {db_path}")
                test(limbo)
        except Exception as e:
            console.error(f"Test FAILED: {e}")
            cleanup(db_path)
            exit(1)
        cleanup(db_path)
    console.info("All tests passed successfully.")


if __name__ == "__main__":
    main()
