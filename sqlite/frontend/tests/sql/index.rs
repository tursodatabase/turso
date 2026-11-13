#[allow(unused_imports)]
use turso_core::alloc::TursoIteratorExt;
#[allow(unused_imports)]
use turso_core::alloc::TursoVecExt;
use turso_core::translate::index::canonical_create_index_sql;
#[allow(unused_imports)]
use turso_core::CaptureDataChangesExt;
use turso_parser::ast;
use turso_sqlite::SqliteDialect;

#[test]
fn canonical_create_index_sql_drops_schema_qualifier_from_index_name_only() {
    let stmt = match turso_core::dialect::parse_statement(
        &SqliteDialect,
        "CREATE INDEX aux.idx1 ON t1(name);",
    )
    .unwrap()
    .unwrap()
    {
        ast::Cmd::Stmt(stmt) => stmt,
        other => panic!("expected statement, got {other:?}"),
    };

    // let's confirm that the stmt does carry `aux`
    let ast::Stmt::CreateIndex { idx_name, .. } = &stmt else {
        panic!("expected CREATE INDEX statement");
    };
    assert_eq!(
        idx_name
            .namespace
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("aux")
    );

    let ast::Stmt::CreateIndex { tbl_name, .. } = &stmt else {
        panic!("expected CREATE INDEX statement");
    };
    let canonical_sql = canonical_create_index_sql(&stmt, idx_name.name.clone(), tbl_name.clone());
    assert_eq!(canonical_sql, "CREATE INDEX idx1 ON t1 (name)");
    let stored_sql = canonical_create_index_sql(
        &stmt,
        ast::Name::exact("__turso_ns3_idx1".to_string()),
        ast::Name::exact("__turso_ns3_t1".to_string()),
    );
    assert_eq!(
        stored_sql,
        "CREATE INDEX __turso_ns3_idx1 ON __turso_ns3_t1 (name)"
    );
}
