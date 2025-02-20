use magnus::{function, Error, Ruby};

fn distance(a: (f64, f64), b: (f64, f64)) -> f64 {
    ((b.0 - a.0).powi(2) + (b.1 - a.1).powi(2)).sqrt()
}

#[magnus::init(name="limbo")]
fn init(ruby: &Ruby) -> Result<(), Error> {
    ruby.define_global_function("distance", function!(distance, 2));
    Ok(())
}
