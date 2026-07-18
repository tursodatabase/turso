mod database;
mod error;
mod value;

use database::Database;
use error::ErrorClasses;
use magnus::{define_module, function, method, prelude::*, Error, Module, Ruby};
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

    Ok(())
}
