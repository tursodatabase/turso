mod error;
mod value;

use error::ErrorClasses;
use magnus::{define_module, function, prelude::*, Error, Ruby};

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Turso")?;
    let _classes = ErrorClasses::define(ruby, &module)?;
    Ok(())
}
