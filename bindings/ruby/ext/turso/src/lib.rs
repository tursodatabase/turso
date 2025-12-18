use magnus::{Error, Ruby};

mod connection;
mod database;
mod errors;
mod statement;
mod value;

#[magnus::init(name = "turso")]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Turso")?;
    errors::define_exceptions(ruby, &module)?;
    database::define_database(ruby, &module)?;
    connection::define_connection(ruby, &module)?;
    statement::define_statement(ruby, &module)?;
    Ok(())
}
