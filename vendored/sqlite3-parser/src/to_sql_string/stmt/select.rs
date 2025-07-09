#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    to_sql_string_test!(test_select_basic, "SELECT 1");

    to_sql_string_test!(test_select_table, "SELECT * FROM t");

    to_sql_string_test!(test_select_table_2, "SELECT a FROM t");

    to_sql_string_test!(test_select_multiple_columns, "SELECT a, b, c FROM t");

    to_sql_string_test!(test_select_with_alias, "SELECT a AS col1 FROM t");

    to_sql_string_test!(test_select_with_table_alias, "SELECT t1.a FROM t AS t1");

    to_sql_string_test!(test_select_with_where, "SELECT a FROM t WHERE b = 1");

    to_sql_string_test!(
        test_select_with_multiple_conditions,
        "SELECT a FROM t WHERE b = 1 AND c > 2"
    );

    to_sql_string_test!(test_select_with_order_by, "SELECT a FROM t ORDER BY a DESC");

    to_sql_string_test!(test_select_with_limit, "SELECT a FROM t LIMIT 10");

    to_sql_string_test!(test_select_with_offset, "SELECT a FROM t LIMIT 10 OFFSET 5");

    to_sql_string_test!(
        test_select_with_join,
        "SELECT a FROM t JOIN t2 ON t.b = t2.b"
    );

    to_sql_string_test!(
        test_select_with_group_by,
        "SELECT a, COUNT (*) FROM t GROUP BY a"
    );

    to_sql_string_test!(
        test_select_with_having,
        "SELECT a, COUNT (*) FROM t GROUP BY a HAVING COUNT (*) > 1"
    );

    to_sql_string_test!(test_select_with_distinct, "SELECT DISTINCT a FROM t");

    to_sql_string_test!(test_select_with_function, "SELECT COUNT (a) FROM t");

    to_sql_string_test!(
        test_select_with_subquery,
        "SELECT a FROM (SELECT b FROM t) AS sub"
    );

    to_sql_string_test!(
        test_select_nested_subquery,
        "SELECT a FROM (SELECT b FROM (SELECT c FROM t WHERE c > 10) AS sub1 WHERE b < 20) AS sub2"
    );

    to_sql_string_test!(
        test_select_multiple_joins,
        "SELECT t1.a, t2.b, t3.c FROM t1 JOIN t2 ON t1.id = t2.id LEFT OUTER JOIN t3 ON t2.id = t3.id"
    );

    to_sql_string_test!(
        test_select_with_cte,
        "WITH cte AS (SELECT a FROM t WHERE b = 1) SELECT a FROM cte WHERE a > 10"
    );

    to_sql_string_test!(
        test_select_with_window_function,
        "SELECT a, ROW_NUMBER () OVER (PARTITION BY b ORDER BY c DESC) AS rn FROM t"
    );

    to_sql_string_test!(
        test_select_with_complex_where,
        "SELECT a FROM t WHERE b IN (1, 2, 3) AND c BETWEEN 10 AND 20 OR d IS NULL"
    );

    to_sql_string_test!(
        test_select_with_case,
        "SELECT CASE WHEN a > 0 THEN 'positive' ELSE 'non-positive' END AS result FROM t"
    );

    to_sql_string_test!(test_select_with_aggregate_and_join, "SELECT t1.a, COUNT (t2.b) FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id GROUP BY t1.a HAVING COUNT (t2.b) > 5");

    to_sql_string_test!(test_select_with_multiple_ctes, "WITH cte1 AS (SELECT a FROM t WHERE b = 1), cte2 AS (SELECT c FROM t2 WHERE d = 2) SELECT cte1.a, cte2.c FROM cte1 JOIN cte2 ON cte1.a = cte2.c");

    to_sql_string_test!(
        test_select_with_union,
        "SELECT a FROM t1 UNION SELECT b FROM t2"
    );

    to_sql_string_test!(
        test_select_with_union_all,
        "SELECT a FROM t1 UNION ALL SELECT b FROM t2"
    );

    to_sql_string_test!(
        test_select_with_exists,
        "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.b = t.a)"
    );

    to_sql_string_test!(
        test_select_with_correlated_subquery,
        "SELECT a, (SELECT COUNT (*) FROM t2 WHERE t2.b = t.a) AS count_b FROM t"
    );

    to_sql_string_test!(
        test_select_with_complex_order_by,
        "SELECT a, b FROM t ORDER BY CASE WHEN a IS NULL THEN 1 ELSE 0 END, b ASC, c DESC"
    );

    to_sql_string_test!(
        test_select_with_full_outer_join,
        "SELECT t1.a, t2.b FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id",
        ignore = "OUTER JOIN is incorrectly parsed in parser"
    );

    to_sql_string_test!(test_select_with_aggregate_window, "SELECT a, SUM (b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS running_sum FROM t");

    to_sql_string_test!(
        test_select_with_exclude,
        "SELECT 
    c.name,
    o.order_id,
    o.order_amount,
    SUM (o.order_amount) OVER (PARTITION BY c.id
        ORDER BY o.order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        EXCLUDE CURRENT ROW) AS running_total_excluding_current
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE EXISTS (SELECT 1
    FROM orders o2
    WHERE o2.customer_id = c.id
    AND o2.order_amount > 1000)"
    );
}
