//! Compile-time checks that the public module layout and root exports stay
//! stable. The root exports must be the functional definitions from the
//! submodules, and the canonical module paths must remain public.

#[test]
fn root_exports_match_module_definitions() {
    fn same_type<T>(_: fn() -> T, _: fn() -> T) {}

    same_type::<turso::Params>(|| turso::Params::None, || turso::params::Params::None);
    same_type::<turso::TransactionBehavior>(
        || turso::TransactionBehavior::Deferred,
        || turso::transaction::TransactionBehavior::Deferred,
    );

    fn takes_root_transaction(_: turso::Transaction<'_>) {}
    fn takes_module_transaction(t: turso::transaction::Transaction<'_>) {
        takes_root_transaction(t)
    }
    let _ = takes_module_transaction;

    fn root_into_value<T: turso::IntoValue>() {}
    let _ = root_into_value::<turso::Value>;

    let _: turso::BoxError = Box::new(std::fmt::Error);

    let _ = turso::transaction::DropBehavior::Rollback;
    let _ = turso::value::ValueType::Null;
    fn takes_value_ref(_: turso::value::ValueRef<'_>) {}
    let _ = takes_value_ref;
    fn takes_connection(_: turso::connection::Connection) {}
    let _ = takes_connection;
}

/// Pins the pragma_query callback signature: fallible with the crate's own
/// public Error, so callbacks can use `?` and stop iteration early, and the
/// same callback works against the serverless driver.
#[allow(dead_code)]
async fn pragma_query_surface(conn: &turso::Connection) -> turso::Result<()> {
    conn.pragma_query("journal_mode", |row| {
        let _ = row.get_value(0)?;
        Ok(())
    })
    .await
}

#[test]
fn pragma_query_surface_compiles() {
    let _ = pragma_query_surface;
}
