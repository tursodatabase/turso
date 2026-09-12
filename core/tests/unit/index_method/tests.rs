use super::{ensure_mvcc_support, IndexMethodDefinition, IndexMethodMvccSupport};

fn definition(support: IndexMethodMvccSupport) -> IndexMethodDefinition<'static> {
    IndexMethodDefinition {
        method_name: "test_method",
        table_name: "test_table",
        index_name: "test_index",
        patterns: &[],
        backing_btree: false,
        results_materialized: true,
        mvcc_support: support,
    }
}

#[test]
fn mvcc_support_declaration_rejects_unsupported_access() {
    let error =
        ensure_mvcc_support(&definition(IndexMethodMvccSupport::Unsupported), false).unwrap_err();
    assert!(matches!(error, crate::LimboError::ParseError(_)));

    ensure_mvcc_support(&definition(IndexMethodMvccSupport::ReadOnly), false).unwrap();
    assert!(ensure_mvcc_support(&definition(IndexMethodMvccSupport::ReadOnly), true).is_err());
    ensure_mvcc_support(
        &definition(IndexMethodMvccSupport::TransactionalBackingStore),
        true,
    )
    .unwrap();
}
