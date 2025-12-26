#[cfg(feature = "extension-module")]
use magnus::{Error, Ruby};

#[cfg(feature = "extension-module")]
mod connection;
#[cfg(feature = "extension-module")]
mod database;
#[cfg(feature = "extension-module")]
mod errors;
#[cfg(feature = "extension-module")]
mod statement;
#[cfg(feature = "extension-module")]
mod value;
#[cfg(feature = "extension-module")]
mod setup;

#[cfg(feature = "extension-module")]
#[magnus::init(name = "turso")]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Turso")?;
    errors::define_exceptions(ruby, &module)?;
    database::define_database(ruby, &module)?;
    connection::define_connection(ruby, &module)?;
    statement::define_statement(ruby, &module)?;
    setup::init(ruby, module)?;
    Ok(())
}
