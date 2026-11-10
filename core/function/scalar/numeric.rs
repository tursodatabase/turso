use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::numeric::decimal::{numeric_from_value, MAX_FRACTION_DIGITS};
use crate::{numeric::Numeric, LimboError, Result, Value};
use bigdecimal::{BigDecimal, Zero};

#[derive(Debug)]
pub(crate) struct BooleanToInt;

impl ScalarFunction for BooleanToInt {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "boolean_to_int",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        let val = args.get(0);
        let result = match val {
            Value::Null => Value::Null,
            Value::Numeric(Numeric::Integer(i)) => match *i {
                0 => Value::from_i64(0),
                1 => Value::from_i64(1),
                _ => {
                    return Err(LimboError::Constraint(format!(
                        "invalid input for type boolean: \"{i}\""
                    )));
                }
            },
            Value::Text(t) => {
                let v = &t.value;
                match v.to_ascii_lowercase().as_str() {
                    "true" | "t" | "yes" | "on" | "1" => Value::from_i64(1),
                    "false" | "f" | "no" | "off" | "0" => Value::from_i64(0),
                    _ => {
                        return Err(LimboError::Constraint(format!(
                            "invalid input for type boolean: \"{v}\""
                        )));
                    }
                }
            }
            other => {
                return Err(LimboError::Constraint(format!(
                    "invalid input for type boolean: \"{other}\""
                )));
            }
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct IntToBoolean;

impl ScalarFunction for IntToBoolean {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "int_to_boolean",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        let val = args.get(0);
        let result = match val {
            Value::Null => Value::Null,
            Value::Numeric(Numeric::Integer(0)) => Value::build_text("false".to_string()),
            _ => Value::build_text("true".to_string()),
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct ValidateIpAddr;

impl ScalarFunction for ValidateIpAddr {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "validate_ipaddr",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        let val = args.get(0);
        let result = match val {
            Value::Null => Value::Null,
            Value::Text(t) => {
                let v = &t.value;
                v.parse::<std::net::IpAddr>().map_err(|_| {
                    LimboError::Constraint(format!("invalid input for type inet: \"{v}\""))
                })?;
                val.clone()
            }
            other => {
                return Err(LimboError::Constraint(format!(
                    "invalid input for type inet: \"{other}\""
                )));
            }
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct NumericEncode;

impl ScalarFunction for NumericEncode {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_encode",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Blob),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        let result = match args.get(0) {
            Value::Null => Value::Null,
            other => {
                let bd = numeric_from_value(other)?;
                Value::from_blob(crate::numeric::decimal::bigdecimal_to_blob(&bd))
            }
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct NumericFit;

impl ScalarFunction for NumericFit {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_fit",
            arity: FunctionArity::Exact(3),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Blob),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        let val = args.get(0);
        let precision_value = args.get(1);
        let scale_value = args.get(2);
        // COERCE runs on the value ENCODE stored, so the value is a
        // stored numeric.
        let result = match val {
            Value::Null => Value::Null,
            Value::Blob(stored) => {
                use crate::numeric::decimal::{
                    bigdecimal_to_blob, blob_to_bigdecimal, validate_precision_scale,
                };

                let bd = blob_to_bigdecimal(stored)?;
                let (
                    Value::Numeric(Numeric::Integer(precision)),
                    Value::Numeric(Numeric::Integer(scale)),
                ) = (precision_value, scale_value)
                else {
                    return Err(LimboError::Constraint(
                        "numeric_fit: precision and scale must be integers".to_string(),
                    ));
                };
                let fitted = validate_precision_scale(&bd, *precision, *scale)?;
                Value::from_blob(bigdecimal_to_blob(&fitted))
            }
            other => {
                return Err(LimboError::InternalError(format!(
                    "numeric_fit expects a stored numeric, got {}",
                    other.value_type()
                )));
            }
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct NumericDecode;

impl ScalarFunction for NumericDecode {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_decode",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        let val = args.get(0);
        let result = match val {
            Value::Null => Value::Null,
            Value::Blob(b) => {
                let bd = crate::numeric::decimal::blob_to_bigdecimal(b)?;
                Value::build_text(crate::numeric::decimal::format_numeric(&bd))
            }
            other => {
                return Err(LimboError::Constraint(format!(
                    "numeric_decode: expected blob, got \"{other}\""
                )));
            }
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct NumericNeg;

impl ScalarFunction for NumericNeg {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_neg",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        let val = args.get(0);
        let result = match val {
            Value::Null => Value::Null,
            // Negating keeps the decimal places: -(1.50) is -1.50.
            _ => Value::build_text(crate::numeric::decimal::format_numeric(
                &-numeric_from_value(val)?,
            )),
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct Gcd;

impl ScalarFunction for Gcd {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "gcd",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::math::exec_gcd(args.get(0), args.get(1))
    }
}

#[derive(Debug)]
pub(crate) struct Lcm;

impl ScalarFunction for Lcm {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "lcm",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::math::exec_lcm(args.get(0), args.get(1))
    }
}

#[derive(Debug)]
pub(crate) struct NumericAdd;

impl ScalarFunction for NumericAdd {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_add",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        execute_binary(args, |a, b| {
            let (a_scale, b_scale) = (a.fractional_digit_count(), b.fractional_digit_count());
            Ok((a + b).with_scale(a_scale.max(b_scale)))
        })
    }
}

#[derive(Debug)]
pub(crate) struct NumericSub;

impl ScalarFunction for NumericSub {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_sub",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        execute_binary(args, |a, b| {
            let (a_scale, b_scale) = (a.fractional_digit_count(), b.fractional_digit_count());
            Ok((a - b).with_scale(a_scale.max(b_scale)))
        })
    }
}

#[derive(Debug)]
pub(crate) struct NumericMul;

impl ScalarFunction for NumericMul {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_mul",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        execute_binary(args, |a, b| {
            let (a_scale, b_scale) = (a.fractional_digit_count(), b.fractional_digit_count());
            Ok((a * b).with_scale_round(
                (a_scale + b_scale).min(MAX_FRACTION_DIGITS),
                bigdecimal::RoundingMode::HalfUp,
            ))
        })
    }
}

#[derive(Debug)]
pub(crate) struct NumericDiv;

impl ScalarFunction for NumericDiv {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_div",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        execute_binary(args, |a, b| {
            if b.is_zero() {
                return Err(LimboError::Constraint("division by zero".to_string()));
            }
            Ok(a / b)
        })
    }
}

#[derive(Debug)]
pub(crate) struct NumericMod;

impl ScalarFunction for NumericMod {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "numeric_mod",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        execute_binary(args, |a, b| {
            if b.is_zero() {
                return Err(LimboError::Constraint("division by zero".to_string()));
            }
            let (a_scale, b_scale) = (a.fractional_digit_count(), b.fractional_digit_count());
            Ok((a % b).with_scale(a_scale.max(b_scale)))
        })
    }
}

fn execute_binary(
    args: FunctionArgs<'_>,
    operation: impl FnOnce(BigDecimal, BigDecimal) -> Result<BigDecimal>,
) -> Result<Value> {
    let (lhs, rhs) = (args.get(0), args.get(1));
    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return Ok(Value::Null);
    }
    let result = operation(numeric_from_value(lhs)?, numeric_from_value(rhs)?)?;
    crate::numeric::decimal::check_numeric_limits(&result)?;
    Ok(Value::build_text(crate::numeric::decimal::format_numeric(
        &result,
    )))
}
