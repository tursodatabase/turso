//! Tests for valid PostgreSQL SQL that should parse successfully

use turso_parser_pg::{parse, split_statements};

#[test]
fn test_basic_select() {
    let queries = vec![
        "SELECT 1",
        "SELECT * FROM users",
        "SELECT id, name FROM users",
        "SELECT u.* FROM users u",
        "SELECT users.* FROM users",
        "SELECT schema.table.column FROM schema.table",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_dollar_parameters() {
    let queries = vec![
        "SELECT * FROM users WHERE id = $1",
        "SELECT * FROM users WHERE id = $1 AND name = $2",
        "INSERT INTO users (name, age) VALUES ($1, $2)",
        "UPDATE users SET name = $1 WHERE id = $2",
        "DELETE FROM users WHERE id = $1",
        "SELECT * FROM users WHERE id BETWEEN $1 AND $2",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_type_casting() {
    let queries = vec![
        "SELECT '123'::integer",
        "SELECT '123'::int",
        "SELECT '3.14'::float",
        "SELECT '2024-01-01'::date",
        "SELECT 'true'::boolean",
        "SELECT '{1,2,3}'::int[]",
        "SELECT CAST('123' AS integer)",
        "SELECT CAST('3.14' AS double precision)",
        "SELECT col::text::json", // "column" is a reserved keyword in PostgreSQL
        "SELECT $1::text",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_dollar_quoted_strings() {
    let queries = vec![
        "SELECT $$Hello World$$",
        "SELECT $tag$Content with 'quotes'$tag$",
        "SELECT $$ $$ || $$test$$",
        "SELECT $function$\nBEGIN\n  RETURN 1;\nEND;\n$function$",
        "INSERT INTO code (body) VALUES ($$function() { return 'test'; }$$)",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_arrays() {
    let queries = vec![
        "SELECT ARRAY[1, 2, 3]",
        "SELECT ARRAY[]::integer[]",
        "SELECT (ARRAY[1, 2, 3])[1]", // Array subscript on constructor requires parentheses
        "SELECT '{1,2,3}'::int[]",
        "SELECT array_column[1]",
        "SELECT array_column[1:3]",
        "INSERT INTO table_with_array VALUES (ARRAY[1, 2, 3])",
        "SELECT * FROM users WHERE tags @> ARRAY['admin']",
        "SELECT * FROM users WHERE ARRAY['admin'] <@ tags",
        "SELECT * FROM users WHERE tags && ARRAY['user', 'admin']",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_json_jsonb_operators() {
    let queries = vec![
        "SELECT data->'field' FROM json_table",
        "SELECT data->>'field' FROM json_table",
        "SELECT data#>'{nested,field}' FROM json_table",
        "SELECT data#>>'{nested,field}' FROM json_table",
        "SELECT * FROM json_table WHERE data @> '{\"key\": \"value\"}'",
        "SELECT * FROM json_table WHERE '{\"key\": \"value\"}' <@ data",
        "SELECT * FROM json_table WHERE data ? 'key'",
        "SELECT * FROM json_table WHERE data ?& ARRAY['key1', 'key2']",
        "SELECT * FROM json_table WHERE data ?| ARRAY['key1', 'key2']",
        "SELECT data->'items'->0->>'name' FROM json_table",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_distinct_on() {
    let queries = vec![
        "SELECT DISTINCT ON (department) name, salary FROM employees",
        "SELECT DISTINCT ON (department, location) * FROM employees",
        "SELECT DISTINCT ON (1) * FROM employees ORDER BY department, salary DESC",
        "SELECT DISTINCT ON (department) name FROM employees ORDER BY department, salary DESC",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_returning_clause() {
    let queries = vec![
        "INSERT INTO users (name) VALUES ('John') RETURNING id",
        "INSERT INTO users (name) VALUES ('John') RETURNING *",
        "INSERT INTO users (name) VALUES ('John') RETURNING id, name",
        "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING *",
        "DELETE FROM users WHERE id = 1 RETURNING id, name",
        "INSERT INTO users (name) VALUES ('John') RETURNING id AS user_id",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_on_conflict() {
    let queries = vec![
        "INSERT INTO users (email) VALUES ('test@example.com') ON CONFLICT DO NOTHING",
        "INSERT INTO users (email) VALUES ('test@example.com') ON CONFLICT (email) DO NOTHING",
        "INSERT INTO users (email, name) VALUES ('test@example.com', 'Test') ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name",
        "INSERT INTO users (id, name) VALUES (1, 'Test') ON CONFLICT (id) DO UPDATE SET name = 'Updated'",
        "INSERT INTO users (email) VALUES ('test@example.com') ON CONFLICT ON CONSTRAINT users_email_key DO NOTHING",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_window_functions() {
    let queries = vec![
        "SELECT ROW_NUMBER() OVER () FROM users",
        "SELECT ROW_NUMBER() OVER (ORDER BY salary) FROM employees",
        "SELECT ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary) FROM employees",
        "SELECT name, salary, AVG(salary) OVER (PARTITION BY department) FROM employees",
        "SELECT name, RANK() OVER (ORDER BY salary DESC) FROM employees",
        "SELECT name, LAG(salary, 1) OVER (ORDER BY hire_date) FROM employees",
        "SELECT SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_cte_with_recursive() {
    let queries = vec![
        "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
        "WITH cte (id, name) AS (SELECT id, name FROM users) SELECT * FROM cte",
        "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 10) SELECT * FROM cte",
        "WITH cte1 AS (SELECT * FROM users), cte2 AS (SELECT * FROM orders) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.user_id",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_case_expressions() {
    let queries = vec![
        "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM users",
        "SELECT CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END",
        "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END FROM users",
        "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END FROM users",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_joins() {
    let queries = vec![
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
        "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        "SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
        "SELECT * FROM users CROSS JOIN departments",
        "SELECT * FROM users NATURAL JOIN orders",
        "SELECT * FROM users u JOIN orders o USING (user_id)",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_lateral_joins() {
    let queries = vec![
        "SELECT * FROM users, LATERAL (SELECT * FROM orders WHERE orders.user_id = users.id) o",
        "SELECT * FROM users JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = users.id) o ON true",
        "SELECT * FROM users LEFT JOIN LATERAL get_orders(users.id) ON true",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_for_update() {
    let queries = vec![
        "SELECT * FROM users FOR UPDATE",
        "SELECT * FROM users FOR UPDATE NOWAIT",
        "SELECT * FROM users FOR UPDATE SKIP LOCKED",
        "SELECT * FROM users FOR NO KEY UPDATE",
        "SELECT * FROM users FOR SHARE",
        "SELECT * FROM users FOR KEY SHARE",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_nulls_ordering() {
    let queries = vec![
        "SELECT * FROM users ORDER BY name NULLS FIRST",
        "SELECT * FROM users ORDER BY name NULLS LAST",
        "SELECT * FROM users ORDER BY name ASC NULLS FIRST",
        "SELECT * FROM users ORDER BY name DESC NULLS LAST",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_regex_operators() {
    let queries = vec![
        "SELECT * FROM users WHERE name ~ 'John.*'",
        "SELECT * FROM users WHERE name ~* 'john.*'",
        "SELECT * FROM users WHERE name !~ 'John.*'",
        "SELECT * FROM users WHERE name !~* 'john.*'",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_similar_to() {
    let queries = vec![
        "SELECT * FROM users WHERE name SIMILAR TO 'John%'",
        "SELECT * FROM users WHERE name NOT SIMILAR TO 'John%'",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_exists_subqueries() {
    let queries = vec![
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
        "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_in_subqueries() {
    let queries = vec![
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
        "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        "SELECT * FROM users WHERE name IN ('John', 'Jane')",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_set_operations() {
    let queries = vec![
        "SELECT id FROM users UNION SELECT user_id FROM orders",
        "SELECT id FROM users UNION ALL SELECT user_id FROM orders",
        "SELECT id FROM users INTERSECT SELECT user_id FROM orders",
        "SELECT id FROM users EXCEPT SELECT user_id FROM orders",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_aggregate_functions() {
    let queries = vec![
        "SELECT COUNT(*) FROM users",
        "SELECT COUNT(DISTINCT department) FROM employees",
        "SELECT SUM(salary) FROM employees",
        "SELECT AVG(salary) FROM employees",
        "SELECT MIN(salary), MAX(salary) FROM employees",
        "SELECT STRING_AGG(name, ', ' ORDER BY name) FROM users",
        "SELECT ARRAY_AGG(name ORDER BY id) FROM users",
        "SELECT JSON_AGG(row_to_json(users)) FROM users",
        "SELECT COUNT(*) FILTER (WHERE active = true) FROM users",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_create_table() {
    let queries = vec![
        "CREATE TABLE users (id INTEGER, name TEXT)",
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, email TEXT UNIQUE)",
        "CREATE TABLE orders (id SERIAL, user_id INTEGER REFERENCES users(id))",
        "CREATE TABLE products (id SERIAL, price DECIMAL(10, 2), tags TEXT[])",
        "CREATE TABLE logs (id BIGSERIAL, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW())",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_create_index() {
    let queries = vec![
        "CREATE INDEX idx_users_name ON users (name)",
        "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)",
        "CREATE INDEX idx_users_name_email ON users (name, email)",
        "CREATE INDEX idx_users_lower_name ON users (LOWER(name))",
        "CREATE INDEX idx_active_users ON users (name) WHERE active = true",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_alter_table() {
    let queries = vec![
        "ALTER TABLE users ADD COLUMN age INTEGER",
        "ALTER TABLE users DROP COLUMN age",
        "ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)",
        "ALTER TABLE users DROP CONSTRAINT users_email_unique",
        "ALTER TABLE users ALTER COLUMN name SET NOT NULL",
        "ALTER TABLE users ALTER COLUMN name DROP NOT NULL",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_transactions() {
    let queries = vec![
        "BEGIN",
        "BEGIN TRANSACTION",
        "BEGIN ISOLATION LEVEL READ COMMITTED",
        "BEGIN ISOLATION LEVEL REPEATABLE READ",
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
        "COMMIT",
        "ROLLBACK",
        "BEGIN READ ONLY",
        "BEGIN READ WRITE",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_multiple_statements() {
    let sql = "SELECT 1; SELECT 2; SELECT 3";
    let stmts = split_statements(sql).expect("Failed to split statements");
    assert_eq!(stmts.len(), 3);

    let sql = "CREATE TABLE users (id INTEGER); INSERT INTO users VALUES (1); SELECT * FROM users";
    let stmts = split_statements(sql).expect("Failed to split statements");
    assert_eq!(stmts.len(), 3);

    // Semicolons inside string literals should not split
    let sql = "INSERT INTO t VALUES ('a;b'); SELECT 1";
    let stmts = split_statements(sql).expect("Failed to split statements");
    assert_eq!(stmts.len(), 2);
}

#[test]
fn test_postgresql_types() {
    let queries = vec![
        "CREATE TABLE test (
            a SMALLINT,
            b INTEGER,
            c BIGINT,
            d DECIMAL(10, 2),
            e NUMERIC(5, 3),
            f REAL,
            g DOUBLE PRECISION,
            h SMALLSERIAL,
            i SERIAL,
            j BIGSERIAL,
            k VARCHAR(100),
            l CHAR(10),
            m TEXT,
            n BYTEA,
            o DATE,
            p TIME,
            q TIMESTAMP,
            r TIMESTAMPTZ,
            s INTERVAL,
            t BOOLEAN,
            u UUID,
            v INET,
            w CIDR,
            x JSON,
            y JSONB,
            z INTEGER[]
        )",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

// Additional tests based on PostgreSQL regression test failures

#[test]
fn test_explain_statements() {
    let queries = vec![
        "EXPLAIN SELECT * FROM users",
        "EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM users",
        "EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM users WHERE id = 1",
        "EXPLAIN ANALYZE SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_copy_statements() {
    let queries = vec![
        "COPY users FROM '/path/to/file.csv'",
        "COPY users (id, name) FROM STDIN",
        "COPY users TO '/path/to/output.csv'",
        "COPY (SELECT * FROM users WHERE active = true) TO STDOUT",
        "COPY users FROM STDIN WITH DELIMITER ',' CSV HEADER",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_analyze_statements() {
    let queries = vec![
        "ANALYZE",
        "ANALYZE users",
        "ANALYZE users (name, email)",
        "VACUUM ANALYZE users",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_set_statements() {
    let queries = vec![
        "SET search_path TO public, other_schema",
        "SET TIME ZONE 'UTC'",
        "SET extra_float_digits = 3",
        "SET ROLE admin",
        "SET SESSION AUTHORIZATION 'user'",
        "RESET ALL",
        "RESET search_path",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_show_statements() {
    let queries = vec![
        "SHOW ALL",
        "SHOW search_path",
        "SHOW TIME ZONE",
        "SHOW TRANSACTION ISOLATION LEVEL",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_row_constructors() {
    let queries = vec![
        "SELECT ROW(1, 2, 'hello')",
        "SELECT (1, 2, 'hello')",
        "SELECT * FROM users WHERE (id, name) = (1, 'John')",
        // Note: ROW() is not valid in INSERT VALUES in PostgreSQL, only in expressions
        "SELECT ROW(1, 'John')",
        "SELECT max(ROW(a, b)) FROM test_table",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_order_by_using() {
    let queries = vec![
        "SELECT * FROM users ORDER BY name USING <",
        "SELECT * FROM users ORDER BY id USING >",
        "SELECT * FROM users ORDER BY created_at USING <=",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_generate_series() {
    let queries = vec![
        "SELECT * FROM generate_series(1, 10)",
        "SELECT * FROM generate_series(1, 10, 2)",
        "SELECT generate_series(1, 10) AS n",
        "SELECT s FROM generate_series(1, 5) s",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_array_literals() {
    let queries = vec![
        "SELECT '{1,2,3}'::int[]",
        "SELECT '{a,b,c}'::text[]",
        "SELECT '{{1,2},{3,4}}'::int[][]",
        "SELECT ARRAY[1,2,3]",
        "SELECT ARRAY[[1,2],[3,4]]",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_bit_string_literals() {
    let queries = vec!["SELECT B'1010'", "SELECT X'1F2E'", "SELECT B'1010'::bit(4)"];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_table_functions() {
    let queries = vec![
        "SELECT * FROM unnest(ARRAY[1,2,3]) AS t(x)",
        "SELECT * FROM json_each('{\"a\":1,\"b\":2}')",
        "SELECT * FROM json_array_elements('[1,2,3]')",
        "SELECT * FROM regexp_split_to_table('hello,world', ',')",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_create_temporary_table() {
    let queries = vec![
        "CREATE TEMPORARY TABLE temp_users (id INTEGER, name TEXT)",
        "CREATE TEMP TABLE temp_orders AS SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '1 day'",
        "CREATE TEMPORARY TABLE IF NOT EXISTS temp_stats (count INTEGER)",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_grouping_sets() {
    let queries = vec![
        "SELECT a, b, SUM(c) FROM test GROUP BY GROUPING SETS ((a), (b), ())",
        "SELECT a, b, SUM(c) FROM test GROUP BY ROLLUP(a, b)",
        "SELECT a, b, SUM(c) FROM test GROUP BY CUBE(a, b)",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_filter_aggregates() {
    let queries = vec![
        "SELECT COUNT(*) FILTER (WHERE active = true) FROM users",
        "SELECT SUM(amount) FILTER (WHERE status = 'completed') FROM orders",
        "SELECT AVG(score) FILTER (WHERE score > 0) FROM tests",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_over_clause_ranges() {
    let queries = vec![
        "SELECT SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales",
        "SELECT AVG(price) OVER (PARTITION BY category ORDER BY date RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) FROM products",
        "SELECT ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM users",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_update_tuple_assignment() {
    let queries = vec![
        "UPDATE test_table SET (a, b) = (1, 2)",
        "UPDATE test_table SET (c, b, a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo'",
        "UPDATE test_table SET (c, b) = ('car', a+b), a = a + 1 WHERE a = 10",
        "UPDATE test_table t SET (a, b) = (SELECT b, a FROM test_table s WHERE s.a = t.a)",
        "UPDATE users SET (name, email) = ('John', 'john@example.com') WHERE id = 1",
        "UPDATE products SET (price, discount) = (99.99, 0.1) WHERE category = 'electronics'",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_insert_array_element_columns() {
    let queries = vec![
        "INSERT INTO inserttest (f2[1], f2[2]) VALUES (1, 2)",
        "INSERT INTO inserttest (f2[1], f2[2]) VALUES (3, 4), (5, 6)",
        "INSERT INTO inserttest (f2[1], f2[2]) SELECT 7, 8",
        "INSERT INTO inserttest (f2[1], f2[2]) VALUES (1, DEFAULT)",
        "INSERT INTO array_table (data[1], data[2], data[3]) VALUES ('a', 'b', 'c')",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_insert_field_access_columns() {
    let queries = vec![
        "INSERT INTO inserttest (f3.if1, f3.if2) VALUES (1, '{foo}'), (2, '{bar}')",
        "INSERT INTO inserttest (f3.if1, f3.if2) SELECT 3, '{baz,quux}'",
        "INSERT INTO inserttest (f3.if1, f3.if2) VALUES (1, DEFAULT)",
        "INSERT INTO composite_table (point.x, point.y) VALUES (10.5, 20.3)",
        "INSERT INTO nested_table (obj.name, obj.value) VALUES ('test', 42)",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_insert_complex_column_access() {
    let queries = vec![
        "INSERT INTO inserttest (f3.if2[1], f3.if2[2]) VALUES ('foo', 'bar'), ('baz', 'quux')",
        "INSERT INTO inserttest (f3.if2[1], f3.if2[2]) SELECT 'bear', 'beer'",
        "INSERT INTO inserttest (f4[1].if2[1], f4[1].if2[2]) VALUES ('foo', 'bar')",
        "INSERT INTO complex_table (data[0].name, data[0].value[1]) VALUES ('test', 'value')",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_update_array_element_assignment() {
    let queries = vec![
        "UPDATE array_table SET data[1] = 'new_value' WHERE id = 1",
        "UPDATE multi_array SET matrix[1][2] = 42 WHERE name = 'test'",
        "UPDATE nested_array SET items[index] = new_value WHERE condition = true",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_update_field_assignment() {
    let queries = vec![
        "UPDATE composite_table SET point.x = 15.5 WHERE id = 1",
        "UPDATE nested_table SET obj.name = 'updated' WHERE obj.id = 42",
        "UPDATE complex_table SET config.settings = 'new_setting' WHERE active = true",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_update_complex_assignment() {
    let queries = vec![
        "UPDATE complex_table SET data[0].name = 'updated' WHERE id = 1",
        "UPDATE nested_structures SET items[1].properties.value = 42",
        "UPDATE array_of_objects SET objects[index].field = new_value",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_create_table_partition() {
    let queries = vec![
        "CREATE TABLE upsert_test_1 PARTITION OF upsert_test FOR VALUES IN (1)",
        "CREATE TABLE part_b_20_b_30 PARTITION OF range_parted FOR VALUES FROM ('b', 20) TO ('b', 30)",
        "CREATE TABLE part_a_10_a_20 PARTITION OF range_parted FOR VALUES FROM ('a', 10) TO ('a', 20)",
        "CREATE TABLE part_a_1_a_10 PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('a', 10)",
        "CREATE TABLE list_partition PARTITION OF parent_table FOR VALUES IN ('value1', 'value2', 'value3')",
        "CREATE TABLE range_partition PARTITION OF measurement FOR VALUES FROM (1, '2021-01-01') TO (2, '2021-02-01')",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}

#[test]
fn test_alter_table_attach_partition() {
    let queries = vec![
        "ALTER TABLE upsert_test ATTACH PARTITION upsert_test_2 FOR VALUES IN (2)",
        "ALTER TABLE range_parted ATTACH PARTITION part_b_20_b_30 FOR VALUES FROM ('b', 20) TO ('b', 30)",
        "ALTER TABLE range_parted ATTACH PARTITION part_b_10_b_20 FOR VALUES FROM ('b', 10) TO ('b', 20)",
        "ALTER TABLE measurement ATTACH PARTITION year_2021 FOR VALUES FROM ('2021-01-01') TO ('2022-01-01')",
        "ALTER TABLE sales ATTACH PARTITION region_east FOR VALUES IN ('east', 'northeast', 'southeast')",
        "ALTER TABLE multi_key ATTACH PARTITION partition_1_100 FOR VALUES FROM (1, 0) TO (2, 0)",
    ];

    for sql in queries {
        assert!(parse(sql).is_ok(), "Failed to parse: {sql}");
    }
}
