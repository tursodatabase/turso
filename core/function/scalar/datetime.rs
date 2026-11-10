use super::DateTimeArguments;
use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::{Clock, MonotonicInstant, WallClockInstant};
use crate::{Result, Value};
use std::cell::Cell;

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
    fn date_time_arguments(&self) -> Option<DateTimeArguments> {
        Some(DateTimeArguments::TimeValue { index: 0 })
    }

    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(crate::functions::datetime::exec_date(
            context.clock(),
            args.iter(),
        ))
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
    fn date_time_arguments(&self) -> Option<DateTimeArguments> {
        Some(DateTimeArguments::TimeValue { index: 0 })
    }

    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(crate::functions::datetime::exec_time(
            context.clock(),
            args.iter(),
        ))
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
    fn date_time_arguments(&self) -> Option<DateTimeArguments> {
        Some(DateTimeArguments::TimeValue { index: 0 })
    }

    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(crate::functions::datetime::exec_datetime_full(
            context.clock(),
            args.iter(),
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
    fn date_time_arguments(&self) -> Option<DateTimeArguments> {
        Some(DateTimeArguments::TimeValue { index: 0 })
    }

    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(crate::functions::datetime::exec_julianday(
            context.clock(),
            args.iter(),
        ))
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
    fn date_time_arguments(&self) -> Option<DateTimeArguments> {
        Some(DateTimeArguments::TimeValue { index: 0 })
    }

    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(crate::functions::datetime::exec_unixepoch(
            context.clock(),
            args.iter(),
        ))
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
    fn date_time_arguments(&self) -> Option<DateTimeArguments> {
        Some(DateTimeArguments::TimeValue { index: 1 })
    }

    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(crate::functions::datetime::exec_strftime(
            context.clock(),
            args.iter(),
        ))
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
    fn date_time_arguments(&self) -> Option<DateTimeArguments> {
        Some(DateTimeArguments::TimePair)
    }

    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(crate::functions::datetime::exec_timediff(
            context.clock(),
            args.iter(),
        ))
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
    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::datetime::exec_date_plus(context.clock(), args.get(0), args.get(1))
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
    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::datetime::exec_date_minus(context.clock(), args.get(0), args.get(1))
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
    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::datetime::exec_date_diff(context.clock(), args.get(0), args.get(1))
    }
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
