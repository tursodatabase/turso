use super::ScalarState;
use super::Volatility;
use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::IOResult;
use crate::{Clock, MonotonicInstant, WallClockInstant};
use crate::{Result, Value};
use std::cell::Cell;
use turso_parser::ast::{self, Expr};

#[derive(Debug)]
pub(crate) struct Date;

impl ScalarFunction for Date {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "date",
            arity: FunctionArity::Variadic,
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn volatility(&self, args: &[Box<Expr>]) -> Volatility {
        time_value_volatility(args, 0)
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(crate::functions::datetime::exec_date(
            context.clock(),
            args.iter(),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct Time;

impl ScalarFunction for Time {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "time",
            arity: FunctionArity::Variadic,
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn volatility(&self, args: &[Box<Expr>]) -> Volatility {
        time_value_volatility(args, 0)
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(crate::functions::datetime::exec_time(
            context.clock(),
            args.iter(),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct DateTime;

impl ScalarFunction for DateTime {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "datetime",
            arity: FunctionArity::Variadic,
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn volatility(&self, args: &[Box<Expr>]) -> Volatility {
        time_value_volatility(args, 0)
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(
            crate::functions::datetime::exec_datetime_full(context.clock(), args.iter()),
        ))
    }
}

#[derive(Debug)]
pub(crate) struct JulianDay;

impl ScalarFunction for JulianDay {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "julianday",
            arity: FunctionArity::Variadic,
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Real),
        }
    }
    fn volatility(&self, args: &[Box<Expr>]) -> Volatility {
        time_value_volatility(args, 0)
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(crate::functions::datetime::exec_julianday(
            context.clock(),
            args.iter(),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct UnixEpoch;

impl ScalarFunction for UnixEpoch {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "unixepoch",
            arity: FunctionArity::Variadic,
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Unknown,
        }
    }
    fn volatility(&self, args: &[Box<Expr>]) -> Volatility {
        time_value_volatility(args, 0)
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(crate::functions::datetime::exec_unixepoch(
            context.clock(),
            args.iter(),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct StrfTime;

impl ScalarFunction for StrfTime {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "strftime",
            arity: FunctionArity::Variadic,
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn volatility(&self, args: &[Box<Expr>]) -> Volatility {
        time_value_volatility(args, 1)
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(crate::functions::datetime::exec_strftime(
            context.clock(),
            args.iter(),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct TimeDiff;

impl ScalarFunction for TimeDiff {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "timediff",
            arity: FunctionArity::Exact(2),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn volatility(&self, args: &[Box<Expr>]) -> Volatility {
        if args.iter().any(|arg| is_current_time_expr(arg)) {
            Volatility::Stable
        } else {
            Volatility::Immutable
        }
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(crate::functions::datetime::exec_timediff(
            context.clock(),
            args.iter(),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct DatePlus;

impl ScalarFunction for DatePlus {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "date_plus",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        crate::functions::datetime::exec_date_plus(context.clock(), args.get(0), args.get(1))
            .map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct DateMinus;

impl ScalarFunction for DateMinus {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "date_minus",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        crate::functions::datetime::exec_date_minus(context.clock(), args.get(0), args.get(1))
            .map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct DateDiff;

impl ScalarFunction for DateDiff {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "date_diff",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        crate::functions::datetime::exec_date_diff(context.clock(), args.get(0), args.get(1))
            .map(IOResult::Done)
    }
}

fn time_value_volatility(args: &[Box<Expr>], index: usize) -> Volatility {
    if args.len() > index
        && !is_current_time_expr(&args[index])
        && !args[index + 1..]
            .iter()
            .any(|arg| is_unsafe_datetime_modifier(arg))
    {
        Volatility::Immutable
    } else {
        Volatility::Stable
    }
}

fn is_current_time_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ast::Literal::String(value)) if string_literal_eq(value, "now")
    ) || matches!(
        expr,
        Expr::Literal(
            ast::Literal::CurrentDate | ast::Literal::CurrentTime | ast::Literal::CurrentTimestamp
        )
    )
}

fn is_unsafe_datetime_modifier(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ast::Literal::String(value))
            if string_literal_eq(value, "localtime") || string_literal_eq(value, "utc")
    ) || is_current_time_expr(expr)
}

fn string_literal_eq(value: &str, expected: &str) -> bool {
    value.trim_matches('\'').eq_ignore_ascii_case(expected)
}

/// The clock seen by the date and time functions. Like SQLite's
/// `sqlite3StmtCurrentTime`, it reads the wall clock on the first request and
/// returns that same time until the step that requested it ends.
pub(super) struct StatementClock<'a> {
    io: &'a dyn Clock,
    now: &'a Cell<Option<WallClockInstant>>,
}

impl<'a> StatementClock<'a> {
    pub(super) fn new(io: &'a dyn Clock, now: &'a mut Option<WallClockInstant>) -> Self {
        Self {
            io,
            now: Cell::from_mut(now),
        }
    }
}

impl Clock for StatementClock<'_> {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.io.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        let now = self
            .now
            .get()
            .unwrap_or_else(|| self.io.current_time_wall_clock());
        self.now.set(Some(now));
        now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volatility_depends_on_time_values_and_modifiers() {
        let cases: &[(&str, &[&str], Volatility)] = &[
            ("date", &["2020-01-02"], Volatility::Immutable),
            ("date", &[], Volatility::Stable),
            ("time", &["now"], Volatility::Stable),
            ("datetime", &["NoW"], Volatility::Stable),
            ("julianday", &["2020-01-02", "utc"], Volatility::Stable),
            (
                "unixepoch",
                &["2020-01-02", "LOCALTIME"],
                Volatility::Stable,
            ),
            ("strftime", &["now", "2020-01-02"], Volatility::Immutable),
            ("strftime", &["%Y", "now"], Volatility::Stable),
            ("strftime", &["%Y"], Volatility::Stable),
            (
                "timediff",
                &["2020-01-02", "2020-01-01"],
                Volatility::Immutable,
            ),
            ("timediff", &["now", "2020-01-01"], Volatility::Stable),
            ("timediff", &["2020-01-02", "now"], Volatility::Stable),
            ("abs", &["1"], Volatility::Immutable),
            ("random", &[], Volatility::Volatile),
        ];
        for &(name, values, expected) in cases {
            let args: Vec<_> = values
                .iter()
                .map(|value| Box::new(Expr::Literal(ast::Literal::String(format!("'{value}'")))))
                .collect();
            let function = crate::Func::resolve_function(name, args.len())
                .unwrap()
                .unwrap();
            assert_eq!(function.volatility(&args), expected, "{name}({values:?})");
        }
    }
}
