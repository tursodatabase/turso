# AI Instructions

## Prompt

You are tasked with converting TCL tests involving sql statements to Rust. Below are some guidelines that you should follow

- Preserve comments from TCL and from the SQL statements
- If a test is commented out, still port it, but leave it commented in the Rust version as well.
- Columns outputs are separated by a `|` and rows are separated by a newline

## Context

### Example 1

```tcl
foreach {testname lhs rhs ans} {
  int-int-1               8     1       0 
  int-int-2               8     8       1
  int-null                8     NULL    {}
} {
  do_execsql_test compare-eq-$testname "SELECT $lhs = $rhs" $::ans
}

foreach {testname lhs rhs ans} {
  float-float-1             8.0     1.0     0
  float-float-2             8.0     8.0     1
  float-null                8.0     NULL    {}
} {
  do_execsql_test compare-eq-$testname "SELECT $lhs = $rhs" $::ans
}

foreach {testname lhs rhs ans} {
   text-text-1                'a'       'b'    0
   text-text-2                'a'       'a'    1
   text-null                  'a'       NULL   {}
} {
  do_execsql_test compare-eq-$testname "SELECT $lhs = $rhs" $::ans
}

foreach {testname lhs rhs ans} {
   null-int                NULL       1      {}
   null-float              NULL       1.0    {}
   null-text               NULL       'a'    {}
   null-null               NULL       NULL   {}
} {
  do_execsql_test compare-eq-$testname "SELECT $lhs = $rhs" $::ans
}

foreach {testname lhs rhs ans} {
  int-int-1               8     1       1 
  int-int-2               8     8       0
  int-null                8     NULL    {}
} {
  do_execsql_test compare-neq-$testname "SELECT $lhs <> $rhs" $::ans
}

foreach {testname lhs rhs ans} {
  float-float-1             8.0     1.0     1
  float-float-2             8.0     8.0     0
  float-null                8.0     NULL    {}
} {
  do_execsql_test compare-neq-$testname "SELECT $lhs <> $rhs" $::ans
}

foreach {testname lhs rhs ans} {
   text-text-1                'a'       'b'    1
   text-text-2                'a'       'a'    0
   text-null                  'a'       NULL   {}
} {
  do_execsql_test compare-neq-$testname "SELECT $lhs <> $rhs" $::ans
}

foreach {testname lhs rhs ans} {
   null-int                NULL       1      {}
   null-float              NULL       1.0    {}
   null-text               NULL       'a'    {}
   null-null               NULL       NULL   {}
} {
  do_execsql_test compare-neq-$testname "SELECT $lhs <> $rhs" $::ans
}

foreach {testname lhs rhs ans} {
  int-int-1               1     8       0 
  int-int-2               1     1       0
  int-int-3               8     0       1
  int-null                8     NULL    {}
} {
  do_execsql_test compare-gt-$testname "SELECT $lhs > $rhs" $::ans
}

foreach {testname lhs rhs ans} {
  float-float-1             1.0     2.0     0
  float-float-2             1.0     1.0     0
  float-float-3             7.0     6.0     1
  float-null                8.0     NULL    {}
} {
  do_execsql_test compare-gt-$testname "SELECT $lhs > $rhs" $::ans
}

foreach {testname lhs rhs ans} {
   text-text-1                'b'       'c'    0
   text-text-2                'b'       'b'    0
   text-text-3                'b'       'a'    1
   text-null                  'a'       NULL   {}
} {
  do_execsql_test compare-gt-$testname "SELECT $lhs > $rhs" $::ans
}

foreach {testname lhs rhs ans} {
   null-int                NULL       1      {}
   null-float              NULL       1.0    {}
   null-text               NULL       'a'    {}
   null-null               NULL       NULL   {}
} {
  do_execsql_test compare-gt-$testname "SELECT $lhs > $rhs" $::ans
}

foreach {testname lhs rhs ans} {
  int-int-1               1     8       0 
  int-int-2               1     1       1
  int-int-3               8     0       1
  int-null                8     NULL    {}
} {
  do_execsql_test compare-gte-$testname "SELECT $lhs >= $rhs" $::ans
}

foreach {testname lhs rhs ans} {
  float-float-1             1.0     2.0     0
  float-float-2             1.0     1.0     1
  float-float-3             7.0     6.0     1
  float-null                8.0     NULL    {}
} {
  do_execsql_test compare-gte-$testname "SELECT $lhs >= $rhs" $::ans
}

foreach {testname lhs rhs ans} {
   text-text-1                'b'       'c'    0
   text-text-2                'b'       'b'    1
   text-text-3                'b'       'a'    1
   text-null                  'a'       NULL   {}
} {
  do_execsql_test compare-gte-$testname "SELECT $lhs >= $rhs" $::ans
}

foreach {testname lhs rhs ans} {
   null-int                NULL       1      {}
   null-float              NULL       1.0    {}
   null-text               NULL       'a'    {}
   null-null               NULL       NULL   {}
} {
  do_execsql_test compare-gte-$testname "SELECT $lhs >= $rhs" $::ans
}
```

```rust
#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(compare_eq_int_int_1, "SELECT 8 = 1", 0);

    db_test!(compare_eq_int_int_2, "SELECT 8 = 8", 1);

    db_test!(compare_eq_int_null, "SELECT 8 = NULL", [Null]);

    db_test!(compare_eq_float_float_1, "SELECT 8.0 = 1.0", 0);

    db_test!(compare_eq_float_float_2, "SELECT 8.0 = 8.0", 1);

    db_test!(compare_eq_float_null, "SELECT 8.0 = NULL", [Null]);

    db_test!(compare_eq_text_text_1, "SELECT 'a' = 'b'", 0);
    db_test!(compare_eq_text_text_2, "SELECT 'a' = 'a'", 1);

    db_test!(compare_eq_text_null, "SELECT 'a' = NULL", [Null]);

    db_test!(compare_eq_null_int, "SELECT NULL = 1", [Null]);

    db_test!(compare_eq_null_float, "SELECT NULL = 1.0", [Null]);

    db_test!(compare_eq_null_text, "SELECT NULL = 'a'", [Null]);

    db_test!(compare_eq_null_null, "SELECT NULL = NULL", [Null]);

    db_test!(compare_neq_int_int_1, "SELECT 8 <> 1", 1);

    db_test!(compare_neq_int_int_2, "SELECT 8 <> 8", 0);

    db_test!(compare_neq_int_null, "SELECT 8 <> NULL", [Null]);

    db_test!(compare_neq_float_float_1, "SELECT 8.0 <> 1.0", 1);

    db_test!(compare_neq_float_float_2, "SELECT 8.0 <> 8.0", 0);

    db_test!(compare_neq_float_null, "SELECT 8.0 <> NULL", [Null]);

    db_test!(compare_neq_text_text_1, "SELECT 'a' <> 'b'", 1);

    db_test!(compare_neq_text_text_2, "SELECT 'a' <> 'a'", 0);

    db_test!(compare_neq_text_null, "SELECT 'a' <> NULL", [Null]);

    db_test!(compare_neq_null_int, "SELECT NULL <> 1", [Null]);

    db_test!(compare_neq_null_float, "SELECT NULL <> 1.0", [Null]);

    db_test!(compare_neq_null_text, "SELECT NULL <> 'a'", [Null]);

    db_test!(compare_neq_null_null, "SELECT NULL <> NULL", [Null]);

    db_test!(compare_gt_int_int_1, "SELECT 1 > 8", 0);

    db_test!(compare_gt_int_int_2, "SELECT 1 > 1", 0);

    db_test!(compare_gt_int_int_3, "SELECT 8 > 0", 1);

    db_test!(compare_gt_int_null, "SELECT 8 > NULL", [Null]);

    db_test!(compare_gt_float_float_1, "SELECT 1.0 > 2.0", 0);

    db_test!(compare_gt_float_float_2, "SELECT 1.0 > 1.0", 0);

    db_test!(compare_gt_float_float_3, "SELECT 7.0 > 6.0", 1);

    db_test!(compare_gt_float_null, "SELECT 8.0 > NULL", [Null]);

    db_test!(compare_gt_text_text_1, "SELECT 'b' > 'c'", 0);

    db_test!(compare_gt_text_text_2, "SELECT 'b' > 'b'", 0);

    db_test!(compare_gt_text_text_3, "SELECT 'b' > 'a'", 1);

    db_test!(compare_gt_text_null, "SELECT 'a' > NULL", [Null]);

    db_test!(compare_gt_null_int, "SELECT NULL > 1", [Null]);

    db_test!(compare_gt_null_float, "SELECT NULL > 1.0", [Null]);

    db_test!(compare_gt_null_text, "SELECT NULL > 'a'", [Null]);

    db_test!(compare_gt_null_null, "SELECT NULL > NULL", [Null]);

    db_test!(compare_gte_int_int_1, "SELECT 1 >= 8", 0);

    db_test!(compare_gte_int_int_2, "SELECT 1 >= 1", 1);

    db_test!(compare_gte_int_int_3, "SELECT 8 >= 0", 1);

    db_test!(compare_gte_int_null, "SELECT 8 >= NULL", [Null]);

    db_test!(compare_gte_float_float_1, "SELECT 1.0 >= 2.0", 0);

    db_test!(compare_gte_float_float_2, "SELECT 1.0 >= 1.0", 1);

    db_test!(compare_gte_float_float_3, "SELECT 7.0 >= 6.0", 1);

    db_test!(compare_gte_float_null, "SELECT 8.0 >= NULL", [Null]);

    db_test!(compare_gte_text_text_1, "SELECT 'b' >= 'c'", 0);

    db_test!(compare_gte_text_text_2, "SELECT 'b' >= 'b'", 1);

    db_test!(compare_gte_text_text_3, "SELECT 'b' >= 'a'", 1);

    db_test!(compare_gte_text_null, "SELECT 'a' >= NULL", [Null]);

    db_test!(compare_gte_null_int, "SELECT NULL >= 1", [Null]);

    db_test!(compare_gte_null_float, "SELECT NULL >= 1.0", [Null]);

    db_test!(compare_gte_null_text, "SELECT NULL >= 'a'", [Null]);

    db_test!(compare_gte_null_int, "SELECT NULL >= NULL", [Null]);
}
```

### Example 2

```tcl
do_execsql_test_on_specific_db {:memory:} changes-on-basic-insert {
    create table temp (t1 integer, primary key (t1));
    insert into temp values (1);
    select changes();
} {1}

do_execsql_test_on_specific_db {:memory:} changes-on-multiple-row-insert {
    create table temp (t1 integer, primary key (t1));
    insert into temp values (1), (2), (3);
    select changes();
} {3}

do_execsql_test_on_specific_db {:memory:} changes-shows-most-recent {
    create table temp (t1 integer, primary key (t1));
    insert into temp values (1), (2), (3);
    insert into temp values (4), (5), (6), (7);
    select changes();
} {4}
```

```rust
#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(
        memory,
        changes_on_basic_insert,
        [
            "create table temp (t1 integer, primary key (t1))",
            "insert into temp values (1)",
            "select changes()"
        ],
        [1]
    );

    db_test!(
        memory,
        changes_on_multiple_row_insert,
        [
            "create table temp (t1 integer, primary key (t1))",
            "insert into temp values (1), (2), (3)",
            "select changes()"
        ],
        [3]
    );

    db_test!(
        memory,
        changes_shows_most_recent,
        [
            "create table temp (t1 integer, primary key (t1))",
            "insert into temp values (1), (2), (3)",
            "insert into temp values (4), (5), (6), (7)",
            "select changes()"
        ],
        [4]
    );
}
```

### Exampl3 

```tcl
do_execsql_test_on_specific_db {:memory:} basic-insert {
    create table temp (t1 integer, primary key (t1));
    insert into temp values (1);
    select * from temp;
} {1}

do_execsql_test_on_specific_db {:memory:} must-be-int-insert {
    create table temp (t1 integer, primary key (t1));
    insert into temp values (1),(2.0),('3'),('4.0');
    select * from temp;
} {1
2
3
4}

do_execsql_test_on_specific_db {:memory:} strict-basic-creation {
    CREATE TABLE test1(id INTEGER, name TEXT, price REAL) STRICT;
    INSERT INTO test1 VALUES(1, 'item1', 10.5);
    SELECT * FROM test1;
} {1|item1|10.5}

do_execsql_test_in_memory_any_error  strict-require-datatype {
    CREATE TABLE test2(id INTEGER, name) STRICT;
}

do_execsql_test_in_memory_any_error strict-valid-datatypes {
    CREATE TABLE test2(id INTEGER, value DATETIME) STRICT;
}

do_execsql_test_in_memory_any_error strict-type-enforcement {
    CREATE TABLE test3(id INTEGER, name TEXT, price REAL) STRICT;
    INSERT INTO test3 VALUES(1, 'item1', 'not-a-number');
}

do_execsql_test_on_specific_db {:memory:} strict-type-coercion {
    CREATE TABLE test4(id INTEGER, name TEXT, price REAL) STRICT;
    INSERT INTO test4 VALUES(1, 'item1', '10.5');
    SELECT typeof(price), price FROM test4;
} {real|10.5}

do_execsql_test_on_specific_db {:memory:} strict-any-flexibility {
    CREATE TABLE test5(id INTEGER, data ANY) STRICT;
    INSERT INTO test5 VALUES(1, 100);
    INSERT INTO test5 VALUES(2, 'text');
    INSERT INTO test5 VALUES(3, 3.14);
    SELECT id, typeof(data) FROM test5 ORDER BY id;
} {1|integer
2|text
3|real}

do_execsql_test_on_specific_db {:memory:} strict-any-preservation {
    CREATE TABLE test6(id INTEGER, code ANY) STRICT;
    INSERT INTO test6 VALUES(1, '000123');
    SELECT typeof(code), code FROM test6;
} {text|000123}

do_execsql_test_in_memory_any_error strict-int-vs-integer-pk {
    CREATE TABLE test8(id INT PRIMARY KEY, name TEXT) STRICT
    INSERT INTO test8 VALUES(NULL, 'test');
}

do_execsql_test_on_specific_db {:memory:} strict-integer-pk-behavior {
    CREATE TABLE test9(id INTEGER PRIMARY KEY, name TEXT) STRICT;
    INSERT INTO test9 VALUES(NULL, 'test');
    SELECT id, name FROM test9;
} {1|test}


do_execsql_test_on_specific_db {:memory:} strict-mixed-inserts {
    CREATE TABLE test11(
        id INTEGER PRIMARY KEY,
        name TEXT,
        price REAL,
        quantity INT,
        tags ANY
    ) STRICT;

    INSERT INTO test11 VALUES(1, 'item1', 10.5, 5, 'tag1');
    INSERT INTO test11 VALUES(2, 'item2', 20.75, 10, 42);

    SELECT id, name, price, quantity, typeof(tags) FROM test11 ORDER BY id;
} {1|item1|10.5|5|text
2|item2|20.75|10|integer}

do_execsql_test_on_specific_db {:memory:} strict-update-basic {
    CREATE TABLE test1(id INTEGER, name TEXT, price REAL) STRICT;
    INSERT INTO test1 VALUES(1, 'item1', 10.5);
    UPDATE test1 SET price = 15.75 WHERE id = 1;
    SELECT * FROM test1;
} {1|item1|15.75}

do_execsql_test_in_memory_any_error  strict-update-type-enforcement {
    CREATE TABLE test2(id INTEGER, name TEXT, price REAL) STRICT;
    INSERT INTO test2 VALUES(1, 'item1', 10.5);
    UPDATE test2 SET price = 'not-a-number' WHERE id = 1;
}

do_execsql_test_on_specific_db {:memory:} strict-update-type-coercion {
    CREATE TABLE test3(id INTEGER, name TEXT, price REAL) STRICT;
    INSERT INTO test3 VALUES(1, 'item1', 10.5);
    UPDATE test3 SET price = '15.75' WHERE id = 1;
    SELECT id, typeof(price), price FROM test3;
} {1|real|15.75}

do_execsql_test_on_specific_db {:memory:} strict-update-any-flexibility {
    CREATE TABLE test4(id INTEGER, data ANY) STRICT;
    INSERT INTO test4 VALUES(1, 100);
    UPDATE test4 SET data = 'text' WHERE id = 1;
    INSERT INTO test4 VALUES(2, 'original');
    UPDATE test4 SET data = 3.14 WHERE id = 2;
    SELECT id, typeof(data), data FROM test4 ORDER BY id;
} {1|text|text
2|real|3.14}

do_execsql_test_on_specific_db {:memory:} strict-update-any-preservation {
    CREATE TABLE test5(id INTEGER, code ANY) STRICT;
    INSERT INTO test5 VALUES(1, 'text');
    UPDATE test5 SET code = '000123' WHERE id = 1;
    SELECT typeof(code), code FROM test5;
} {text|000123}

do_execsql_test_in_memory_any_error strict-update-not-null-constraint {
    CREATE TABLE test7(id INTEGER, name TEXT NOT NULL) STRICT;
    INSERT INTO test7 VALUES(1, 'name');
    UPDATE test7 SET name = NULL WHERE id = 1;
}

# Uncomment following test case when unique constraint is added
#do_execsql_test_any_error strict-update-pk-constraint {
#    CREATE TABLE test8(id INTEGER PRIMARY KEY, name TEXT) STRICT;
#    INSERT INTO test8 VALUES(1, 'name1');
#    INSERT INTO test8 VALUES(2, 'name2');
#    UPDATE test8 SET id = 2 WHERE id = 1;
#}

do_execsql_test_on_specific_db {:memory:} strict-update-multiple-columns {
    CREATE TABLE test9(id INTEGER, name TEXT, price REAL, quantity INT) STRICT;
    INSERT INTO test9 VALUES(1, 'item1', 10.5, 5);
    UPDATE test9 SET name = 'updated', price = 20.75, quantity = 10 WHERE id = 1;
    SELECT * FROM test9;
} {1|updated|20.75|10}

do_execsql_test_on_specific_db {:memory:} strict-update-where-clause {
    CREATE TABLE test10(id INTEGER, category TEXT, price REAL) STRICT;
    INSERT INTO test10 VALUES(1, 'A', 10);
    INSERT INTO test10 VALUES(2, 'A', 20);
    INSERT INTO test10 VALUES(3, 'B', 30);
    UPDATE test10 SET price = price * 2 WHERE category = 'A';
    SELECT id, price FROM test10 ORDER BY id;
} {1|20.0
2|40.0
3|30.0}

do_execsql_test_on_specific_db {:memory:} strict-update-expression {
    CREATE TABLE test11(id INTEGER, name TEXT, price REAL, discount REAL) STRICT;
    INSERT INTO test11 VALUES(1, 'item1', 100, 0.1);
    UPDATE test11 SET price = price - (price * discount);
    SELECT id, price FROM test11;
} {1|90.0}
```

```rust
#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(
        memory,
        basic_insert,
        [
            "create table temp (t1 integer, primary key (t1))",
            "insert into temp values (1)",
            "select * from temp"
        ],
        [1]
    );

    db_test!(
        memory,
        must_be_int_insert,
        [
            "create table temp (t1 integer, primary key (t1))",
            "insert into temp values (1),(2.0),('3'),('4.0')",
            "select * from temp"
        ],
        [[1], [2], [3], [4]]
    );

    db_test!(
        memory,
        strict_basic_creation,
        [
            "CREATE TABLE test1(id INTEGER, name TEXT, price REAL) STRICT",
            "INSERT INTO test1 VALUES(1, 'item1', 10.5)",
            "SELECT * FROM test1"
        ],
        [1, "item1", 10.5]
    );

    db_test!(
        memory_expect_error,
        strict_require_datatype,
        ["CREATE TABLE test2(id INTEGER, name) STRICT"]
    );

    db_test!(
        memory_expect_error,
        strict_valid_datatypes,
        ["CREATE TABLE test2(id INTEGER, value DATETIME) STRICT"]
    );

    db_test!(
        memory_expect_error,
        strict_type_enforcement,
        [
            "CREATE TABLE test3(id INTEGER, name TEXT, price REAL) STRICT",
            "INSERT INTO test3 VALUES(1, 'item1', 'not-a-number')"
        ]
    );

    db_test!(
        memory,
        strict_type_coercion,
        [
            "CREATE TABLE test4(id INTEGER, name TEXT, price REAL) STRICT",
            "INSERT INTO test4 VALUES(1, 'item1', '10.5')",
            "SELECT typeof(price), price FROM test4"
        ],
        ["real", 10.5]
    );

    db_test!(
        memory,
        strict_any_flexibility,
        [
            "CREATE TABLE test5(id INTEGER, data ANY) STRICT",
            "INSERT INTO test5 VALUES(1, 100)",
            "INSERT INTO test5 VALUES(2, 'text')",
            "INSERT INTO test5 VALUES(3, 3.14)",
            "SELECT id, typeof(data) FROM test5 ORDER BY id"
        ],
        [[1, "integer"], [2, "text"], [3, "real"]]
    );

    db_test!(
        memory,
        strict_any_preservation,
        [
            "CREATE TABLE test6(id INTEGER, code ANY) STRICT",
            "INSERT INTO test6 VALUES(1, '000123')",
            "SELECT typeof(code), code FROM test6"
        ],
        ["text", "000123"]
    );

    db_test!(
        memory_expect_error,
        strict_int_vs_integer_pk,
        [
            "CREATE TABLE test8(id INT PRIMARY KEY, name TEXT) STRICT",
            "INSERT INTO test8 VALUES(NULL, 'test')"
        ]
    );

    db_test!(
        memory,
        strict_integer_pk_behavior,
        [
            "CREATE TABLE test9(id INTEGER PRIMARY KEY, name TEXT) STRICT",
            "INSERT INTO test9 VALUES(NULL, 'test')",
            "SELECT id, name FROM test9"
        ],
        [1, "test"]
    );

    db_test!(
        memory,
        strict_mixed_inserts,
        [
            "CREATE TABLE test11(
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL,
                quantity INT,
                tags ANY
            ) STRICT",
            "INSERT INTO test11 VALUES(1, 'item1', 10.5, 5, 'tag1')",
            "INSERT INTO test11 VALUES(2, 'item2', 20.75, 10, 42)",
            "SELECT id, name, price, quantity, typeof(tags) FROM test11 ORDER BY id"
        ],
        [
            [1, "item1", 10.5, 5, "text"],
            [2, "item2", 20.75, 10, "integer"]
        ]
    );

    db_test!(
        memory,
        strict_update_basic,
        [
            "CREATE TABLE test1(id INTEGER, name TEXT, price REAL) STRICT",
            "INSERT INTO test1 VALUES(1, 'item1', 10.5)",
            "UPDATE test1 SET price = 15.75 WHERE id = 1",
            "SELECT * FROM test1"
        ],
        [1, "item1", 15.75]
    );

    db_test!(
        memory_expect_error,
        strict_update_type_enforcement,
        [
            "CREATE TABLE test2(id INTEGER, name TEXT, price REAL) STRICT",
            "INSERT INTO test2 VALUES(1, 'item1', 10.5)",
            "UPDATE test2 SET price = 'not-a-number' WHERE id = 1"
        ]
    );

    db_test!(
        memory,
        strict_update_type_coercion,
        [
            "CREATE TABLE test3(id INTEGER, name TEXT, price REAL) STRICT",
            "INSERT INTO test3 VALUES(1, 'item1', 10.5)",
            "UPDATE test3 SET price = '15.75' WHERE id = 1",
            "SELECT id, typeof(price), price FROM test3"
        ],
        [1, "real", 15.75]
    );

    db_test!(
        memory,
        strict_update_any_flexibility,
        [
            "CREATE TABLE test4(id INTEGER, data ANY) STRICT",
            "INSERT INTO test4 VALUES(1, 100)",
            "UPDATE test4 SET data = 'text' WHERE id = 1",
            "INSERT INTO test4 VALUES(2, 'original')",
            "UPDATE test4 SET data = 3.14 WHERE id = 2",
            "SELECT id, typeof(data), data FROM test4 ORDER BY id"
        ],
        [[1, "text", "text"], [2, "real", 3.14]]
    );

    db_test!(
        memory,
        strict_update_any_preservation,
        [
            "CREATE TABLE test5(id INTEGER, code ANY) STRICT",
            "INSERT INTO test5 VALUES(1, 'text')",
            "UPDATE test5 SET code = '000123' WHERE id = 1",
            "SELECT typeof(code), code FROM test5"
        ],
        ["text", "000123"]
    );

    db_test!(
        memory_expect_error,
        strict_update_not_null_constraint,
        [
            "CREATE TABLE test7(id INTEGER, name TEXT NOT NULL) STRICT",
            "INSERT INTO test7 VALUES(1, 'name')",
            "UPDATE test7 SET name = NULL WHERE id = 1"
        ]
    );

    // Uncomment following test case when unique constraint is added
    // db_test!(
    //     memory_expect_error,
    //     strict_update_pk_constraint,
    //     [
    //         "CREATE TABLE test8(id INTEGER PRIMARY KEY, name TEXT) STRICT",
    //         "INSERT INTO test8 VALUES(1, 'name1')",
    //         "INSERT INTO test8 VALUES(2, 'name2')",
    //         "UPDATE test8 SET id = 2 WHERE id = 1"
    //     ]
    // );

    db_test!(
        memory,
        strict_update_multiple_columns,
        [
            "CREATE TABLE test9(id INTEGER, name TEXT, price REAL, quantity INT) STRICT",
            "INSERT INTO test9 VALUES(1, 'item1', 10.5, 5)",
            "UPDATE test9 SET name = 'updated', price = 20.75, quantity = 10 WHERE id = 1",
            "SELECT * FROM test9"
        ],
        [1, "updated", 20.75, 10]
    );

    db_test!(
        memory,
        strict_update_where_clause,
        [
            "CREATE TABLE test10(id INTEGER, category TEXT, price REAL) STRICT",
            "INSERT INTO test10 VALUES(1, 'A', 10)",
            "INSERT INTO test10 VALUES(2, 'A', 20)",
            "INSERT INTO test10 VALUES(3, 'B', 30)",
            "UPDATE test10 SET price = price * 2 WHERE category = 'A'",
            "SELECT id, price FROM test10 ORDER BY id"
        ],
        [[1, 20.0], [2, 40.0], [3, 30.0]]
    );

    db_test!(
        memory,
        strict_update_expression,
        [
            "CREATE TABLE test11(id INTEGER, name TEXT, price REAL, discount REAL) STRICT",
            "INSERT INTO test11 VALUES(1, 'item1', 100, 0.1)",
            "UPDATE test11 SET price = price - (price * discount)",
            "SELECT id, price FROM test11"
        ],
        [1, 90.0]
    );
}
```

