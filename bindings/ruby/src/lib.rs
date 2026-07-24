mod blocking_op;
mod connection;
mod database;
mod error;
mod gvl;
mod statement;
mod value;

use connection::Connection;
use database::Database;
use error::ErrorClasses;
use magnus::{function, method, prelude::*, value::ReprValue, Error, Module, Ruby, Value};
use statement::Statement;
use std::mem::transmute;
use std::sync::OnceLock;

pub(crate) static ERROR_CLASSES: OnceLock<ErrorClasses> = OnceLock::new();

static RAW_DATABASE_CLASS: OnceLock<usize> = OnceLock::new();
static RAW_CONNECTION_CLASS: OnceLock<usize> = OnceLock::new();
static RAW_STATEMENT_CLASS: OnceLock<usize> = OnceLock::new();

pub(crate) fn database_class() -> usize {
    *RAW_DATABASE_CLASS
        .get()
        .expect("Database class not initialized")
}
pub(crate) fn connection_class() -> usize {
    *RAW_CONNECTION_CLASS
        .get()
        .expect("Connection class not initialized")
}
pub(crate) fn statement_class() -> usize {
    *RAW_STATEMENT_CLASS
        .get()
        .expect("Statement class not initialized")
}

fn val_to_usize(val: Value) -> usize {
    unsafe { transmute::<Value, usize>(val) }
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Turso")?;
    let classes = ErrorClasses::define(ruby, &module)?;
    let _ = ERROR_CLASSES.set(classes);

    let database_class = module.define_class("NativeDatabase", ruby.class_object())?;
    let _ = RAW_DATABASE_CLASS.set(val_to_usize(database_class.as_value()));
    database_class.define_singleton_method("new", function!(Database::new, 2))?;
    database_class.define_method("close", method!(Database::close, 0))?;
    database_class.define_method("path", method!(Database::path, 0))?;
    database_class.define_method("open?", method!(Database::is_open, 0))?;
    database_class.define_method("last_insert_rowid", method!(Database::last_insert_rowid, 0))?;
    database_class.define_method("in_transaction?", method!(Database::in_transaction, 0))?;
    database_class.define_method("connection", method!(Database::connection, 0))?;
    database_class.define_method("total_changes", method!(Database::total_changes, 0))?;
    database_class.undef_default_alloc_func();

    let connection_class = module.define_class("NativeConnection", ruby.class_object())?;
    let _ = RAW_CONNECTION_CLASS.set(val_to_usize(connection_class.as_value()));
    connection_class.define_method("prepare", method!(Connection::prepare_single, 1))?;
    connection_class.define_method("execute_batch", method!(Connection::execute_batch, 1))?;
    connection_class.define_method("auto_commit?", method!(Connection::get_auto_commit, 0))?;
    connection_class.define_method(
        "last_insert_rowid",
        method!(Connection::last_insert_rowid, 0),
    )?;
    connection_class.define_method("busy_timeout=", method!(Connection::set_busy_timeout, 1))?;
    connection_class.define_method("query_timeout=", method!(Connection::set_query_timeout, 1))?;
    connection_class.define_method("interrupt", method!(Connection::interrupt, 0))?;
    connection_class.define_method("total_changes", method!(Connection::total_changes, 0))?;
    connection_class.define_method("changes", method!(Connection::changes, 0))?;
    connection_class.define_method("close", method!(Connection::close, 0))?;
    connection_class.undef_default_alloc_func();

    let statement_class = module.define_class("NativeStatement", ruby.class_object())?;
    let _ = RAW_STATEMENT_CLASS.set(val_to_usize(statement_class.as_value()));
    statement_class.define_method("parameter_count", method!(Statement::parameter_count, 0))?;
    statement_class.define_method("parameter_name", method!(Statement::parameter_name, 1))?;
    statement_class.define_method("named_position", method!(Statement::named_position, 1))?;
    statement_class.define_method("column_count", method!(Statement::column_count, 0))?;
    statement_class.define_method("column_name", method!(Statement::column_name, 1))?;
    statement_class.define_method("column_decltype", method!(Statement::column_decltype, 1))?;
    statement_class.define_method("bind_positional", method!(Statement::bind_positional, 1))?;
    statement_class.define_method("bind_null", method!(Statement::bind_null, 1))?;
    statement_class.define_method("bind_int", method!(Statement::bind_int, 2))?;
    statement_class.define_method("bind_double", method!(Statement::bind_double, 2))?;
    statement_class.define_method("bind_text", method!(Statement::bind_text, 2))?;
    statement_class.define_method("bind_blob", method!(Statement::bind_blob, 2))?;
    statement_class.define_method("step", method!(Statement::step, 0))?;
    statement_class.define_method("execute", method!(Statement::execute, 0))?;
    statement_class.define_method("row", method!(Statement::row, 0))?;
    statement_class.define_method("row_value", method!(Statement::row_value, 1))?;
    statement_class.define_method("finalize", method!(Statement::finalize, 0))?;
    statement_class.define_method("close", method!(Statement::finalize, 0))?;
    statement_class.define_method("reset", method!(Statement::reset, 0))?;
    statement_class.define_method("n_change", method!(Statement::n_change, 0))?;
    statement_class.undef_default_alloc_func();

    Ok(())
}
