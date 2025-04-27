# AI Instructions

## Prompt

You are tasked with converting TCL tests involving sql statements to Rust. Below are some guidelines that you should follow

- Preserve comments from TCL and from the SQL statements
- If a test is commented out, still port it, but leave it commented in the Rust version as well.

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

