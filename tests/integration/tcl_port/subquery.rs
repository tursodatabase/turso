#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(
        subquery_inner_filter,
        "SELECT sub.loud_hat FROM (SELECT concat(name, '!!!') AS loud_hat FROM products WHERE name = 'hat') sub",
        "hat!!!"
    );

    db_test!(
        subquery_inner_filter_cte,
        "WITH sub AS (SELECT concat(name, '!!!') AS loud_hat FROM products WHERE name = 'hat') SELECT sub.loud_hat FROM sub",
        "hat!!!"
    );

    db_test!(
        subquery_outer_filter,
        "SELECT sub.loud_hat FROM (SELECT concat(name, '!!!') AS loud_hat FROM products) sub WHERE sub.loud_hat = 'hat!!!'",
        "hat!!!"
    );

    db_test!(
        subquery_outer_filter_cte,
        "WITH sub AS (SELECT concat(name, '!!!') AS loud_hat FROM products) SELECT sub.loud_hat FROM sub WHERE sub.loud_hat = 'hat!!!'",
        "hat!!!"
    );

    db_test!(
        subquery_without_alias,
        "SELECT loud_hat FROM (SELECT concat(name, '!!!') AS loud_hat FROM products WHERE name = 'hat')",
        "hat!!!"
    );

    db_test!(
        subquery_without_alias_cte,
        "WITH cte AS (SELECT concat(name, '!!!') AS loud_hat FROM products WHERE name = 'hat') SELECT loud_hat FROM cte",
        "hat!!!"
    );

    db_test!(
        subquery_no_alias_on_col,
        "SELECT price FROM (SELECT * FROM products WHERE name = 'hat')",
        79.0
    );

    db_test!(
        subquery_no_alias_on_col_cte,
        "WITH cte AS (SELECT * FROM products WHERE name = 'hat') SELECT price FROM cte",
        79.0
    );

    db_test!(
        subquery_no_alias_on_col_named,
        "SELECT price FROM (SELECT price FROM products WHERE name = 'hat')",
        79.0
    );

    db_test!(
        subquery_no_alias_on_col_named_cte,
        "WITH cte AS (SELECT price FROM products WHERE name = 'hat') SELECT price FROM cte",
        79.0
    );

    db_test!(
        subquery_select_star,
        "SELECT * FROM (SELECT price, price + 1.0, name FROM products WHERE name = 'hat')",
        [79.0, 80.0, "hat"]
    );

    db_test!(
        subquery_select_star_cte,
        "WITH cte AS (SELECT price, price + 1.0, name FROM products WHERE name = 'hat') SELECT * FROM cte",
        [79.0, 80.0, "hat"]
    );

    db_test!(
        subquery_select_table_star,
        "SELECT sub.* FROM (SELECT price, price + 1.0, name FROM products WHERE name = 'hat') sub",
        [79.0, 80.0, "hat"]
    );

    db_test!(
        subquery_select_table_star_cte,
        "WITH sub AS (SELECT price, price + 1.0, name FROM products WHERE name = 'hat') SELECT sub.* FROM sub",
        [79.0, 80.0, "hat"]
    );

    db_test!(
        nested_subquery,
        "SELECT sub.loudest_hat FROM (SELECT upper(nested_sub.loud_hat) AS loudest_hat FROM (SELECT concat(name, '!!!') AS loud_hat FROM products WHERE name = 'hat') nested_sub) sub",
        "HAT!!!"
    );

    db_test!(
        nested_subquery_cte,
        "WITH nested_sub AS (SELECT concat(name, '!!!') AS loud_hat FROM products WHERE name = 'hat'), sub AS (SELECT upper(nested_sub.loud_hat) AS loudest_hat FROM nested_sub) SELECT sub.loudest_hat FROM sub",
        "HAT!!!"
    );

    db_test!(
        subquery_orderby_limit,
        "SELECT upper(sub.loud_name) AS loudest_name FROM (SELECT concat(name, '!!!') AS loud_name FROM products ORDER BY name LIMIT 3) sub",
        [["ACCESSORIES!!!"], ["BOOTS!!!"], ["CAP!!!"]]
    );

    db_test!(
        subquery_orderby_limit_cte,
        "WITH sub AS (SELECT concat(name, '!!!') AS loud_name FROM products ORDER BY name LIMIT 3) SELECT upper(sub.loud_name) AS loudest_name FROM sub",
        [["ACCESSORIES!!!"], ["BOOTS!!!"], ["CAP!!!"]]
    );

    db_test!(
        table_join_subquery,
        "SELECT sub.product_name, p.name FROM products p JOIN (SELECT name AS product_name FROM products) sub ON p.name = sub.product_name WHERE p.name = 'hat'",
        ["hat", "hat"]
    );

    db_test!(
        table_join_subquery_cte,
        "WITH sub AS (SELECT name AS product_name FROM products) SELECT sub.product_name, p.name FROM products p JOIN sub ON p.name = sub.product_name WHERE p.name = 'hat'",
        ["hat", "hat"]
    );

    db_test!(
        subquery_join_table,
        "SELECT sub.product_name, p.name FROM (SELECT name AS product_name FROM products) sub JOIN products p ON sub.product_name = p.name WHERE sub.product_name = 'hat'",
        ["hat", "hat"]
    );

    db_test!(
        subquery_join_table_cte,
        "WITH sub AS (SELECT name AS product_name FROM products) SELECT sub.product_name, p.name FROM sub JOIN products p ON sub.product_name = p.name WHERE sub.product_name = 'hat'",
        ["hat", "hat"]
    );

    db_test!(
        subquery_join_subquery,
        "SELECT sub1.sus_name, sub2.truthful_name FROM (SELECT name AS sus_name FROM products WHERE name = 'cap') sub1 JOIN (SELECT concat('no ', name) AS truthful_name FROM products WHERE name = 'cap') sub2",
        ["cap", "no cap"]
    );

    db_test!(
        subquery_join_subquery_cte,
        "WITH sub1 AS (SELECT name AS sus_name FROM products WHERE name = 'cap'), sub2 AS (SELECT concat('no ', name) AS truthful_name FROM products WHERE name = 'cap') SELECT sub1.sus_name, sub2.truthful_name FROM sub1 JOIN sub2",
        ["cap", "no cap"]
    );

    db_test!(
        select_star_table_subquery,
        "SELECT * FROM products p JOIN (SELECT name, price FROM products WHERE name = 'hat') sub ON p.name = sub.name",
        [1, "hat", 79.0, "hat", 79.0]
    );

    db_test!(
        select_star_table_subquery_cte,
        "WITH sub AS (SELECT name, price FROM products WHERE name = 'hat') SELECT * FROM products p JOIN sub ON p.name = sub.name",
        [1, "hat", 79.0, "hat", 79.0]
    );

    db_test!(
        select_star_subquery_table,
        "SELECT * FROM (SELECT name, price FROM products WHERE name = 'hat') sub JOIN products p ON sub.name = p.name",
        ["hat", 79.0, 1, "hat", 79.0]
    );

    db_test!(
        select_star_subquery_table_cte,
        "WITH sub AS (SELECT name, price FROM products WHERE name = 'hat') SELECT * FROM sub JOIN products p ON sub.name = p.name",
        ["hat", 79.0, 1, "hat", 79.0]
    );

    db_test!(
        select_star_subquery_subquery,
        "SELECT * FROM (SELECT name, price FROM products WHERE name = 'hat') sub1 JOIN (SELECT price FROM products WHERE name = 'hat') sub2 ON sub1.price = sub2.price",
        ["hat", 79.0, 79.0]
    );

    db_test!(
        select_star_subquery_subquery_cte,
        "WITH sub1 AS (SELECT name, price FROM products WHERE name = 'hat'), sub2 AS (SELECT price FROM products WHERE name = 'hat') SELECT * FROM sub1 JOIN sub2 ON sub1.price = sub2.price",
        ["hat", 79.0, 79.0]
    );

    db_test!(
        subquery_inner_grouping,
        "SELECT is_jennifer, person_count FROM (SELECT first_name = 'Jennifer' AS is_jennifer, count(1) AS person_count FROM users GROUP BY first_name = 'Jennifer') ORDER BY person_count ASC",
        [[1, 151], [0, 9849]]
    );

    db_test!(
        subquery_inner_grouping_cte,
        "WITH cte AS (SELECT first_name = 'Jennifer' AS is_jennifer, count(1) AS person_count FROM users GROUP BY first_name = 'Jennifer') SELECT is_jennifer, person_count FROM cte ORDER BY person_count ASC",
        [[1, 151], [0, 9849]]
    );

    db_test!(
        subquery_outer_grouping,
        "SELECT is_jennifer, count(1) AS person_count FROM (SELECT first_name = 'Jennifer' AS is_jennifer FROM users) GROUP BY is_jennifer ORDER BY count(1) ASC",
        [[1, 151], [0, 9849]]
    );

    db_test!(
        subquery_outer_grouping_cte,
        "WITH cte AS (SELECT first_name = 'Jennifer' AS is_jennifer FROM users) SELECT is_jennifer, count(1) AS person_count FROM cte GROUP BY is_jennifer ORDER BY count(1) ASC",
        [[1, 151], [0, 9849]]
    );

    db_test!(
        subquery_join_using_with_outer_limit,
        "SELECT p.name, sub.funny_name FROM products p JOIN (SELECT id, concat(name, '-lol') AS funny_name FROM products) sub USING (id) LIMIT 3",
        [["hat", "hat-lol"], ["cap", "cap-lol"], ["shirt", "shirt-lol"]]
    );

    db_test!(
        subquery_join_using_with_outer_limit_cte,
        "WITH sub AS (SELECT id, concat(name, '-lol') AS funny_name FROM products) SELECT p.name, sub.funny_name FROM products p JOIN sub USING (id) LIMIT 3",
        [["hat", "hat-lol"], ["cap", "cap-lol"], ["shirt", "shirt-lol"]]
    );

    db_test!(
        subquery_join_using_with_inner_limit,
        "SELECT p.name, sub.funny_name FROM products p JOIN (SELECT id, concat(name, '-lol') AS funny_name FROM products LIMIT 3) sub USING (id)",
        [["hat", "hat-lol"], ["cap", "cap-lol"], ["shirt", "shirt-lol"]]
    );

    db_test!(
        subquery_join_using_with_inner_limit_cte,
        "WITH sub AS (SELECT id, concat(name, '-lol') AS funny_name FROM products LIMIT 3) SELECT p.name, sub.funny_name FROM products p JOIN sub USING (id)",
        [["hat", "hat-lol"], ["cap", "cap-lol"], ["shirt", "shirt-lol"]]
    );

    db_test!(
        subquery_join_using_with_both_limits,
        "SELECT p.name, sub.funny_name FROM products p JOIN (SELECT id, concat(name, '-lol') AS funny_name FROM products LIMIT 3) sub USING (id) LIMIT 2",
        [["hat", "hat-lol"], ["cap", "cap-lol"]]
    );

    db_test!(
        subquery_join_using_with_both_limits_cte,
        "WITH sub AS (SELECT id, concat(name, '-lol') AS funny_name FROM products LIMIT 3) SELECT p.name, sub.funny_name FROM products p JOIN sub USING (id) LIMIT 2",
        [["hat", "hat-lol"], ["cap", "cap-lol"]]
    );

    db_test!(
        subquery_containing_join,
        "SELECT foo, bar FROM (SELECT p.name AS foo, u.first_name AS bar FROM products p JOIN users u USING (id)) LIMIT 3",
        [["hat", "Jamie"], ["cap", "Cindy"], ["shirt", "Tommy"]]
    );

    db_test!(
        subquery_containing_join_cte,
        "WITH cte AS (SELECT p.name AS foo, u.first_name AS bar FROM products p JOIN users u USING (id)) SELECT foo, bar FROM cte LIMIT 3",
        [["hat", "Jamie"], ["cap", "Cindy"], ["shirt", "Tommy"]]
    );

    db_test!(
        subquery_ignore_unused_cte,
        "WITH unused AS (SELECT last_name FROM users), sub AS (SELECT first_name FROM users WHERE first_name = 'Jamie' LIMIT 1) SELECT * FROM sub",
        "Jamie"
    );
}
