use magnus::{define_module, function, prelude::*, Error, Ruby};

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let _module = ruby.define_module("Turso")?;
    Ok(())
}
