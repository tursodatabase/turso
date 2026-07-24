//! Compile-time checks that the public module layout and root exports stay
//! stable and mirror the embedded `turso` driver: the root exports must be
//! the functional definitions from the submodules, and the canonical module
//! paths (`connection`, `transaction`, `value`) must remain public.

#[test]
fn root_exports_match_module_definitions() {
    fn same_type<T>(_: fn() -> T, _: fn() -> T) {}

    same_type::<turso_serverless::Params>(
        || turso_serverless::Params::None,
        || turso_serverless::params::Params::None,
    );
    same_type::<turso_serverless::TransactionBehavior>(
        || turso_serverless::TransactionBehavior::Deferred,
        || turso_serverless::transaction::TransactionBehavior::Deferred,
    );

    fn takes_root_transaction(_: turso_serverless::Transaction) {}
    fn takes_module_transaction(t: turso_serverless::transaction::Transaction) {
        takes_root_transaction(t)
    }
    let _ = takes_module_transaction;

    fn root_into_value<T: turso_serverless::IntoValue>() {}
    let _ = root_into_value::<turso_serverless::Value>;

    let _: turso_serverless::BoxError = Box::new(std::fmt::Error);

    fn takes_value(_: turso_serverless::value::Value) {}
    let _ = takes_value;
    fn takes_connection(_: turso_serverless::connection::Connection) {}
    let _ = takes_connection;

    let _ = turso_serverless::value::ValueType::Null;
    fn takes_value_ref(_: turso_serverless::value::ValueRef<'_>) {}
    let _ = takes_value_ref;
}

/// Pins the value API surface shared with the embedded driver: borrowed
/// access through ValueRef, type inspection through ValueType, and the
/// blob conversions.
#[test]
fn value_ref_surface() {
    use turso_serverless::value::{Value, ValueRef, ValueType};

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

/// Pins the transaction API surface shared with the embedded driver:
/// `DropBehavior`, `Transaction::{new, new_unchecked, prepare,
/// drop_behavior, set_drop_behavior, finish}`, and
/// `Connection::{unchecked_transaction, set_transaction_behavior}`.
#[allow(dead_code)]
async fn transaction_api_surface(
    conn: &mut turso_serverless::Connection,
) -> turso_serverless::Result<()> {
    use turso_serverless::transaction::{DropBehavior, Transaction, TransactionBehavior};

    conn.set_transaction_behavior(TransactionBehavior::Immediate);
    {
        let tx = conn.unchecked_transaction().await?;
        let _stmt: turso_serverless::Statement = tx.prepare("SELECT 1").await?;
        tx.finish().await?;
    }
    let mut tx = Transaction::new(conn, TransactionBehavior::Deferred).await?;
    let _: DropBehavior = tx.drop_behavior();
    tx.set_drop_behavior(DropBehavior::Commit);
    tx.commit().await?;
    let tx = Transaction::new_unchecked(&*conn, TransactionBehavior::Deferred).await?;
    tx.rollback().await?;

    // The pragma_query callback is fallible with the crate's own Error, so
    // it can use `?` and stop iteration early, as in the embedded driver.
    conn.pragma_query("journal_mode", |row| {
        let _ = row.get_value(0)?;
        Ok(())
    })
    .await?;

    let _stmt = conn.prepare_cached("SELECT 1").await?;
    Ok(())
}

/// Pins the trait implementations shared with the embedded driver.
#[test]
fn shared_trait_impls() {
    fn assert_clone<T: Clone>() {}
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_clone::<turso_serverless::Row>();
    assert_clone::<turso_serverless::Column>();
    assert_debug::<turso_serverless::Column>();
    assert_debug::<turso_serverless::Transaction<'static>>();
}

#[test]
fn transaction_api_surface_compiles() {
    let _ = transaction_api_surface;
}
