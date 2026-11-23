//! PostgreSQL's smallint type.

use super::integer::out_of_range;
use super::integer::{integer_functions, operator_signatures, value_param};
use turso_core::ast;
use turso_core::schema::{OperatorSignature, RangeOffsets};
use turso_core::types::{Conversion, Type};
use turso_core::Numeric;
use turso_core::{Register, Result, Value};

/// PostgreSQL's smallint: an integer in the range of 16 bits.
#[derive(Debug)]
pub(super) struct Int2;

impl Type for Int2 {
    fn name(&self) -> &str {
        "int2"
    }

    fn display_name(&self) -> &str {
        "smallint"
    }

    fn aliases(&self) -> &[&str] {
        &["smallint"]
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("integer")]
    }

    fn base(&self) -> &str {
        "integer"
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(int2_coerce))
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        vec![ast::TypeOperator {
            op: "<".to_string(),
            func_name: None,
        }]
    }

    fn range_offsets(&self) -> RangeOffsets {
        RangeOffsets::Integer
    }

    fn stored_form_sorts_in_value_order(&self) -> bool {
        true
    }

    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        operator_signatures("int2", "int2", &[], false)
    }
}

fn int2_coerce(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Numeric(Numeric::Integer(number)) if i16::try_from(*number).is_err() => {
            Err(out_of_range("smallint"))
        }
        other => Ok(other.clone()),
    }
}

integer_functions!(
    i16, "smallint", int2pl, int2mi, int2mul, int2div, int2mod, int2and, int2or, int2not, int2um
);
