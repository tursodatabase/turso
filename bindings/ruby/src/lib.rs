mod connection;
mod database;
mod error;
mod statement;
mod value;

use connection::Connection;
use database::Database;
use error::ErrorClasses;
use magnus::{define_module, function, method, prelude::*, Error, Module, Ruby};
use statement::Statement;
use std::sync::OnceLock;

pub(crate) static ERROR_CLASSES: OnceLock<ErrorClasses> = OnceLock::new();

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Turso")?;
    let classes = ErrorClasses::define(ruby, &module)?;
    let _ = ERROR_CLASSES.set(classes);

    let database_class = module.define_class("Database", ruby.class_object())?;
    database_class.define_singleton_method("new", function!(Database::new, 1))?;
    database_class.define_method("close", method!(Database::close, 0))?;
    database_class.define_method("path", method!(Database::path, 0))?;
    database_class.define_method("open?", method!(Database::is_open, 0))?;
    database_class.define_method("last_insert_rowid", method!(Database::last_insert_rowid, 0))?;
    database_class.define_method("in_transaction?", method!(Database::in_transaction, 0))?;
    database_class.define_method("connection", method!(Database::connection, 0))?;

    let connection_class = module.define_class("Connection", ruby.class_object())?;
    connection_class.define_method("prepare", method!(Connection::prepare_single, 1))?;
    connection_class.define_method("auto_commit?", method!(Connection::get_auto_commit, 0))?;
    connection_class.define_method("readonly?", method!(Connection::is_readonly, 0))?;
    connection_class.define_method("last_insert_rowid", method!(Connection::last_insert_rowid, 0))?;
    connection_class.define_method("busy_timeout=", method!(Connection::set_busy_timeout, 1))?;
    connection_class.define_method("query_timeout=", method!(Connection::set_query_timeout, 1))?;
    connection_class.define_method("interrupt", method!(Connection::interrupt, 0))?;
    connection_class.define_method("close", method!(Connection::close, 0))?;

    let statement_class = module.define_class("Statement", ruby.class_object())?;
    statement_class.define_method("parameter_count", method!(Statement::parameter_count, 0))?;
    statement_class.define_method("column_count", method!(Statement::column_count, 0))?;
    statement_class.define_method("column_name", method!(Statement::column_name, 1))?;
    statement_class.define_method("bind_positional", method!(Statement::bind_positional, 1))?;
    statement_class.define_method("step", method!(Statement::step, 0))?;
    statement_class.define_method("execute", method!(Statement::execute, 0))?;
    statement_class.define_method("row", method!(Statement::row, 0))?;
    statement_class.define_method("finalize", method!(Statement::finalize, 0))?;
    statement_class.define_method("reset", method!(Statement::reset, 0))?;
    statement_class.define_method("n_change", method!(Statement::n_change, 0))?;

    Ok(())
}
