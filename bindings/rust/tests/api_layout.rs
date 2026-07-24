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

/// Pins the trait implementations shared with the serverless driver.
#[test]
fn shared_trait_impls() {
    fn assert_clone<T: Clone>() {}
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_clone::<turso::Row>();
    assert_clone::<turso::Column>();
    assert_debug::<turso::Column>();
    assert_debug::<turso::Transaction<'static>>();
}

/// Pins the value API surface shared with the serverless driver: borrowed
/// access through ValueRef, type inspection through ValueType, and the
/// blob conversions.
#[test]
fn value_ref_surface() {
    use turso::value::{Value, ValueRef, ValueType};

    let owned = Value::Text("hi".to_string());
    let vr = ValueRef::from(&owned);
    assert!(matches!(vr.data_type(), ValueType::Text));
    assert_eq!(vr.as_text(), Some("hi".as_bytes()));
    assert_eq!(Value::from(vr), owned);

    let _: Value = [1u8, 2].into();
    let vr: ValueRef<'_> = (&[1u8, 2][..]).into();
    assert!(vr.is_blob());
    let vr: ValueRef<'_> = Option::<&str>::None.into();
    assert!(vr.is_null());
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
