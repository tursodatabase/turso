use super::*;

#[test]
fn test_parse_simple_select() {
    let sql = "SELECT * FROM users WHERE id = 1";
    let result = parse(sql);
    assert!(result.is_ok());

    let tables = get_tables(sql).unwrap();
    assert_eq!(tables, vec!["users"]);
}

#[test]
fn test_parse_complex_query() {
    let sql = "WITH regional_sales AS (
            SELECT region, SUM(amount) AS total_sales
            FROM orders
            GROUP BY region
        )
        SELECT * FROM regional_sales ORDER BY total_sales DESC";

    assert!(parse(sql).is_ok());
}

#[test]
fn test_normalize() {
    let sql = "SELECT * FROM users WHERE age > 25 AND name = 'John'";
    let normalized = normalize(sql).unwrap();
    assert!(normalized.contains("$1"));
    assert!(normalized.contains("$2"));
}

#[test]
fn test_parse_postgresql_specific() {
    // Test PostgreSQL-specific syntax that we struggled with before
    let queries = vec![
        "SELECT * FROM users ORDER BY name USING >",
        "SELECT * FROM person* p",
        "VALUES (1,2), (3,4)",
        "SELECT foo FROM (SELECT 1) AS foo",
        "INSERT INTO users (name, data) VALUES ('John', '{\"key\": \"value\"}'::jsonb)",
        "SELECT * FROM users WHERE data @> '{\"active\": true}'",
        "UPDATE users SET (name, age) = ('John', 30) WHERE id = 1",
        "CREATE TABLE posts PARTITION OF main_posts FOR VALUES IN (1, 2, 3)",
        "SELECT COUNT(*) FILTER (WHERE active) FROM users",
        "SELECT DISTINCT ON (region) * FROM sales ORDER BY region, amount DESC",
    ];

    for sql in queries {
        let result = parse(sql);
        assert!(result.is_ok(), "Failed to parse: {sql}");
    }
}
