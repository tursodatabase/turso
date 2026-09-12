extern "C" {
    pub fn exp(x: f64) -> f64;
    pub fn log(x: f64) -> f64;
    pub fn log10(x: f64) -> f64;
    pub fn log2(x: f64) -> f64;
    pub fn pow(x: f64, y: f64) -> f64;

    pub fn sin(x: f64) -> f64;
    pub fn sinh(x: f64) -> f64;
    pub fn asin(x: f64) -> f64;
    pub fn asinh(x: f64) -> f64;

    pub fn cos(x: f64) -> f64;
    pub fn cosh(x: f64) -> f64;
    pub fn acos(x: f64) -> f64;
    pub fn acosh(x: f64) -> f64;

    pub fn tan(x: f64) -> f64;
    pub fn tanh(x: f64) -> f64;
    pub fn atan(x: f64) -> f64;
    pub fn atanh(x: f64) -> f64;
    pub fn atan2(x: f64, y: f64) -> f64;
}

// SQLite's M_PI constant (same value as SQLite's func.c)
#[allow(clippy::excessive_precision)]
const M_PI: f64 = 3.141592653589793238462643383279502884;

pub fn degrees(x: f64) -> f64 {
    x * 180.0 / M_PI
}
pub fn radians(x: f64) -> f64 {
    x * M_PI / 180.0
}
