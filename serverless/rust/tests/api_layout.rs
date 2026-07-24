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
    Ok(())
}

#[test]
fn transaction_api_surface_compiles() {
    let _ = transaction_api_surface;
}
