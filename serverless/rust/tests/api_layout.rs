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
