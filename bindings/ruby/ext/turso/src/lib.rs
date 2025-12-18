use magnus::{prelude::*, Error, Ruby};

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Turso")?;  
    Ok(())
}
