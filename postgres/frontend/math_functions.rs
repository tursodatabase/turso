//! The math functions of float8 and numeric that PostgreSQL refuses for some
//! arguments instead of answering NULL or infinity.

use crate::error::sqlstate_error;
use turso_core::{LimboError, Register, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_DIVISION_BY_ZERO, SQLSTATE_INVALID_ARGUMENT_FOR_LOG,
    SQLSTATE_INVALID_ARGUMENT_FOR_POWER_FUNCTION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub(crate) fn square_root(args: &[Register]) -> Result<Option<f64>> {
    let Some(x) = float_arg(args, 0) else {
        return Ok(None);
    };
    if x < 0.0 {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
            "cannot take square root of a negative number",
            None,
        ));
    }
    Ok(Some(x.sqrt()))
}

pub(crate) fn natural_log(args: &[Register]) -> Result<Option<f64>> {
    let Some(x) = float_arg(args, 0) else {
        return Ok(None);
    };
    check_log_argument(x)?;
    Ok(Some(x.ln()))
}

pub(crate) fn log_base_10(args: &[Register]) -> Result<Option<f64>> {
    let Some(x) = float_arg(args, 0) else {
        return Ok(None);
    };
    check_log_argument(x)?;
    Ok(Some(x.log10()))
}

/// `log(base, x)`.
pub(crate) fn log_with_base(args: &[Register]) -> Result<Option<f64>> {
    let (Some(base), Some(x)) = (float_arg(args, 0), float_arg(args, 1)) else {
        return Ok(None);
    };
    check_log_argument(base)?;
    check_log_argument(x)?;
    let divisor = base.ln();
    if divisor == 0.0 {
        return Err(sqlstate_error(
            SQLSTATE_DIVISION_BY_ZERO,
            "division by zero",
            None,
        ));
    }
    Ok(Some(x.ln() / divisor))
}

pub(crate) fn power(args: &[Register]) -> Result<Option<f64>> {
    let (Some(base), Some(exponent)) = (float_arg(args, 0), float_arg(args, 1)) else {
        return Ok(None);
    };
    if base == 0.0 && exponent < 0.0 {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
            "zero raised to a negative power is undefined",
            None,
        ));
    }
    if base < 0.0 && exponent.fract() != 0.0 {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
            "a negative number raised to a non-integer power yields a complex result",
            None,
        ));
    }
    let result = base.powf(exponent);
    if result.is_infinite() && base.is_finite() && exponent.is_finite() {
        return Err(out_of_range());
    }
    Ok(Some(result))
}

fn check_log_argument(x: f64) -> Result<()> {
    let message = if x == 0.0 {
        "cannot take logarithm of zero"
    } else if x < 0.0 {
        "cannot take logarithm of a negative number"
    } else {
        return Ok(());
    };
    Err(sqlstate_error(
        SQLSTATE_INVALID_ARGUMENT_FOR_LOG,
        message,
        None,
    ))
}

fn out_of_range() -> LimboError {
    sqlstate_error(
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        "value out of range: overflow",
        None,
    )
}

fn float_arg(args: &[Register], index: usize) -> Option<f64> {
    match args[index].get_value() {
        Value::Null => None,
        value => Some(value.to_float_or_zero()),
    }
}
