//! The functions that run PostgreSQL's operators on smallint, integer and
//! bigint, such as `int4pl` for `integer + integer` and `int48mul` for
//! `integer * bigint`.

use super::IntegerType;
use crate::error::sqlstate_error;
use turso_core::dialect::ScalarFuncContext;
use turso_core::native_ext::{FunctionArity, ScalarCall, ScalarFunction};
use turso_core::types::IOResultOr;
use turso_core::{
    ExternalFunc, IOResult, LimboError, Numeric, OpenOptions, Register, Result, Value,
};
use turso_pg_parser::sqlstate::SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE;

/// An operator on integers, and the types of its operands and of its result.
/// A prefix operator has only a right operand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IntegerOperator {
    kind: Kind,
    left: Option<IntegerType>,
    right: IntegerType,
    result: IntegerType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    And,
    Or,
    ShiftLeft,
    ShiftRight,
    Negate,
    Not,
}

const TYPES: [IntegerType; 3] = [IntegerType::Int2, IntegerType::Int4, IntegerType::Int8];

impl IntegerOperator {
    /// FIXME(pg-catalog): This hand-codes `oprcode` of the pg_operator rows of the integer types, and which pairs of them have a row. The function a row names, once pg_proc has PostgreSQL's function oids, replaces this.
    /// FIXME(pg-catalog): Only a row that takes exactly the types of the operands is found. For another pair, such as `smallint % integer`, `integer << bigint` or `integer + numeric`, PostgreSQL picks a row after an implicit cast of an operand, and here the engine runs its own operator, without a range check. The implicit casts of pg_cast, applied to the operands of the chosen row, replace this.
    /// The operator `op` for operands of exactly these types, `None` when
    /// PostgreSQL has no such row.
    pub(crate) fn find(
        op: &str,
        left: Option<IntegerType>,
        right: IntegerType,
    ) -> Option<IntegerOperator> {
        Self::all().into_iter().find(|operator| {
            operator.kind.symbol() == op && operator.left == left && operator.right == right
        })
    }

    fn all() -> Vec<IntegerOperator> {
        let mut operators = Vec::new();
        for left in TYPES {
            for right in TYPES {
                for kind in [Kind::Add, Kind::Subtract, Kind::Multiply, Kind::Divide] {
                    operators.push(IntegerOperator {
                        kind,
                        left: Some(left),
                        right,
                        result: left.max(right),
                    });
                }
            }
            for kind in [Kind::Remainder, Kind::And, Kind::Or] {
                operators.push(IntegerOperator {
                    kind,
                    left: Some(left),
                    right: left,
                    result: left,
                });
            }
            for kind in [Kind::ShiftLeft, Kind::ShiftRight] {
                operators.push(IntegerOperator {
                    kind,
                    left: Some(left),
                    right: IntegerType::Int4,
                    result: left,
                });
            }
            for kind in [Kind::Negate, Kind::Not] {
                operators.push(IntegerOperator {
                    kind,
                    left: None,
                    right: left,
                    result: left,
                });
            }
        }
        operators
    }

    /// The name PostgreSQL gives the function in pg_proc.
    pub(crate) fn function_name(&self) -> String {
        let types = match (self.left, self.kind) {
            (Some(left), Kind::ShiftLeft | Kind::ShiftRight) => left.digits().to_string(),
            (Some(left), _) if left != self.right => {
                format!("{}{}", left.digits(), self.right.digits())
            }
            _ => self.right.digits().to_string(),
        };
        format!("int{types}{}", self.kind.suffix())
    }

    pub(crate) fn result(&self) -> IntegerType {
        self.result
    }

    /// FIXME(registered-types): An integer written in the statement and the result of a function such as count(*) have no registered type, and are taken to be an integer (int4). So an operand outside the range of its type is such a value, and the result may then be any bigint, although the operator says its result is an integer: `i + 3000000000` is described as int4. A type for an integer literal by its value, and for a function result from prorettype of its pg_proc row, replace this.
    fn run(&self, left: Option<i64>, right: i64) -> Result<i64> {
        let operand_is_wider_than_its_type = !self.right.contains(right)
            || matches!((self.left, left), (Some(ty), Some(value)) if !ty.contains(value));
        let result_type = if operand_is_wider_than_its_type {
            IntegerType::Int8
        } else {
            self.result
        };
        let out_of_range = || {
            sqlstate_error(
                SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                format!("{} out of range", result_type.sql_name()),
                None,
            )
        };
        let left = left.unwrap_or_default();
        let result = match self.kind {
            Kind::Add => left.checked_add(right),
            Kind::Subtract => left.checked_sub(right),
            Kind::Multiply => left.checked_mul(right),
            Kind::Divide | Kind::Remainder if right == 0 => {
                return Err(LimboError::DivisionByZero);
            }
            Kind::Divide => left.checked_div(right),
            // The remainder of the smallest integer by -1 is 0, where the
            // division overflows.
            Kind::Remainder => Some(left.checked_rem(right).unwrap_or(0)),
            Kind::And => Some(left & right),
            Kind::Or => Some(left | right),
            Kind::ShiftLeft | Kind::ShiftRight => Some(self.shift(left, right)),
            Kind::Negate => right.checked_neg(),
            Kind::Not => Some(!right),
        };
        result
            .filter(|result| result_type.contains(*result))
            .ok_or_else(out_of_range)
    }

    /// PostgreSQL shifts with the processor's instruction, which reads only
    /// the low bits of the count: 5 bits for a smallint or an integer, which
    /// it shifts as 32 bits, and 6 bits for a bigint. A smallint keeps the low
    /// 16 bits of the result.
    fn shift(&self, value: i64, count: i64) -> i64 {
        let left = self.kind == Kind::ShiftLeft;
        match self.result {
            IntegerType::Int8 => {
                let count = (count & 63) as u32;
                if left {
                    value.wrapping_shl(count)
                } else {
                    value >> count
                }
            }
            IntegerType::Int2 | IntegerType::Int4 => {
                let (value, count) = (value as i32, (count & 31) as u32);
                let shifted = if left {
                    value.wrapping_shl(count)
                } else {
                    value >> count
                };
                if self.result == IntegerType::Int2 {
                    i64::from(shifted as i16)
                } else {
                    i64::from(shifted)
                }
            }
        }
    }
}

impl Kind {
    fn symbol(self) -> &'static str {
        match self {
            Kind::Add => "+",
            Kind::Subtract | Kind::Negate => "-",
            Kind::Multiply => "*",
            Kind::Divide => "/",
            Kind::Remainder => "%",
            Kind::And => "&",
            Kind::Or => "|",
            Kind::ShiftLeft => "<<",
            Kind::ShiftRight => ">>",
            Kind::Not => "~",
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Kind::Add => "pl",
            Kind::Subtract => "mi",
            Kind::Multiply => "mul",
            Kind::Divide => "div",
            Kind::Remainder => "mod",
            Kind::And => "and",
            Kind::Or => "or",
            Kind::ShiftLeft => "shl",
            Kind::ShiftRight => "shr",
            Kind::Negate => "um",
            Kind::Not => "not",
        }
    }
}

impl ScalarFunction for IntegerOperator {
    type Call = Self;

    fn create_call(&self) -> Result<Self::Call> {
        Ok(*self)
    }
}

impl ScalarCall for IntegerOperator {
    type Output = Option<i64>;

    fn step(
        &mut self,
        _context: &ScalarFuncContext<'_>,
        args: &[Register],
    ) -> IOResultOr<Option<i64>> {
        let mut integers = Vec::with_capacity(args.len());
        for arg in args {
            match arg.get_value() {
                Value::Null => return Ok(IOResult::Done(None)),
                Value::Numeric(Numeric::Integer(integer)) => integers.push(*integer),
                other => {
                    return Err(Box::new(LimboError::InternalError(format!(
                        "{} got {other}, which is not an integer",
                        self.function_name()
                    ))));
                }
            }
        }
        let result = match integers[..] {
            [right] => self.run(None, right)?,
            [left, right] => self.run(Some(left), right)?,
            _ => unreachable!("an operator has one or two operands"),
        };
        Ok(IOResult::Done(Some(result)))
    }
}

/// FIXME(pg-catalog): The functions are registered in the engine by name only. They are not rows of pg_proc with PostgreSQL's oids, argument types and return types, so `oprcode` of their pg_operator rows is 0 and a direct call such as `int4pl(1, 2)` returns an untyped integer. pg_proc rows under PostgreSQL's oids replace this.
pub(crate) fn register_integer_operator_functions(mut options: OpenOptions) -> OpenOptions {
    for operator in IntegerOperator::all() {
        let arity = if operator.left.is_some() { 2 } else { 1 };
        let function = ExternalFunc::new_native_scalar(
            operator.function_name(),
            FunctionArity::Exact(arity),
            true,
            operator,
        )
        .expect("an operator function takes one or two arguments");
        options = options.extension_function(function);
    }
    options
}

#[cfg(test)]
mod tests {
    use super::{IntegerOperator, IntegerType};
    use IntegerType::{Int2, Int4, Int8};

    fn run(op: &str, left: (IntegerType, i64), right: (IntegerType, i64)) -> Result<i64, String> {
        IntegerOperator::find(op, Some(left.0), right.0)
            .unwrap()
            .run(Some(left.1), right.1)
            .map_err(|error| error.to_string())
    }

    #[test]
    fn functions_have_the_names_of_pg_proc() {
        let name = |op, left, right| {
            IntegerOperator::find(op, left, right)
                .unwrap()
                .function_name()
        };
        assert_eq!(name("+", Some(Int4), Int4), "int4pl");
        assert_eq!(name("-", Some(Int2), Int8), "int28mi");
        assert_eq!(name("*", Some(Int8), Int4), "int84mul");
        assert_eq!(name("/", Some(Int2), Int2), "int2div");
        assert_eq!(name("%", Some(Int8), Int8), "int8mod");
        assert_eq!(name("<<", Some(Int2), Int4), "int2shl");
        assert_eq!(name(">>", Some(Int8), Int4), "int8shr");
        assert_eq!(name("-", None, Int4), "int4um");
        assert_eq!(name("~", None, Int2), "int2not");
        assert!(IntegerOperator::find("%", Some(Int2), Int4).is_none());
        assert!(IntegerOperator::find("<<", Some(Int4), Int8).is_none());
    }

    #[test]
    fn the_result_has_the_wider_type_of_the_operands() {
        assert_eq!(run("+", (Int4, 2147483647), (Int8, 1)), Ok(2147483648));
        assert_eq!(run("*", (Int2, 32767), (Int4, 2)), Ok(65534));
        assert_eq!(
            run("+", (Int4, 2147483647), (Int4, 1)),
            Err("integer out of range".to_string())
        );
        assert_eq!(
            run("*", (Int2, 32767), (Int2, 2)),
            Err("smallint out of range".to_string())
        );
        assert_eq!(
            run("-", (Int8, i64::MIN), (Int2, 1)),
            Err("bigint out of range".to_string())
        );
    }

    #[test]
    fn division_overflows_only_for_the_smallest_integer_by_minus_one() {
        assert_eq!(run("/", (Int4, 7), (Int4, -2)), Ok(-3));
        assert_eq!(run("%", (Int4, -7), (Int4, 2)), Ok(-1));
        assert_eq!(
            run("/", (Int4, -2147483648), (Int4, -1)),
            Err("integer out of range".to_string())
        );
        assert_eq!(run("%", (Int8, i64::MIN), (Int8, -1)), Ok(0));
        assert!(run("/", (Int4, 1), (Int4, 0)).is_err());
    }

    #[test]
    fn an_operand_outside_the_range_of_its_type_makes_the_result_a_bigint() {
        assert_eq!(run("+", (Int4, 1), (Int4, 3000000000)), Ok(3000000001));
        assert_eq!(
            run("+", (Int4, 1), (Int4, i64::MAX)),
            Err("bigint out of range".to_string())
        );
    }

    #[test]
    fn shifts_read_the_low_bits_of_the_count() {
        assert_eq!(run("<<", (Int4, 1), (Int4, 33)), Ok(2));
        assert_eq!(run("<<", (Int4, 1), (Int4, 31)), Ok(-2147483648));
        assert_eq!(run("<<", (Int2, 1), (Int4, 15)), Ok(-32768));
        assert_eq!(run("<<", (Int8, 1), (Int4, 65)), Ok(2));
        assert_eq!(run(">>", (Int4, -8), (Int4, 1)), Ok(-4));
    }
}
