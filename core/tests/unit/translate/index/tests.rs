use super::canonical_create_index_sql;
use turso_parser::{ast, parser::Parser};

#[test]
fn canonical_create_index_sql_drops_schema_qualifier_from_index_name_only() {
    let mut parser = Parser::new(b"CREATE INDEX aux.idx1 ON t1(name);");
    let stmt = match parser.next_cmd().unwrap().unwrap() {
        ast::Cmd::Stmt(stmt) => stmt,
        other => panic!("expected statement, got {other:?}"),
    };

    // let's confirm that the stmt does carry `aux`
    let ast::Stmt::CreateIndex { idx_name, .. } = &stmt else {
        panic!("expected CREATE INDEX statement");
    };
    assert_eq!(
        idx_name
            .db_name
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("aux")
    );

    let canonical_sql = canonical_create_index_sql(&stmt);
    assert_eq!(canonical_sql, "CREATE INDEX idx1 ON t1 (name)");
}
