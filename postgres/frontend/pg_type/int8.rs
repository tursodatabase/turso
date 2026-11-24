//! PostgreSQL's bigint type.

use super::integer::{integer_functions, operator_signatures, value_param};
use turso_core::ast;
use turso_core::schema::{OperatorSignature, RangeOffsets};
use turso_core::types::Type;
use turso_core::{Register, Result, Value};

/// PostgreSQL's bigint: an integer in the range of 64 bits.
#[derive(Debug)]
pub(super) struct Int8;

impl Type for Int8 {
    fn name(&self) -> &str {
        "int8"
    }

    fn display_name(&self) -> &str {
        "bigint"
    }

    fn aliases(&self) -> &[&str] {
        &["bigint"]
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("integer")]
    }

    fn base(&self) -> &str {
        "integer"
    }

    fn range_offsets(&self) -> RangeOffsets {
        RangeOffsets::Integer
    }

    fn stored_form_sorts_in_value_order(&self) -> bool {
        true
    }

    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        operator_signatures("int8", "int8", &["int2"], true)
    }
}

integer_functions!(
    i64, "bigint", int8pl, int8mi, int8mul, int8div, int8mod, int8and, int8or, int8not, int8um
);
