use super::ExtensionState;
use crate::sync::Arc;
use crate::types::{AggContext, IOResultOr, ValueBlob};
use crate::{Connection, IOResult, LimboError, Register, Result, Value};
use std::fmt::Debug;

pub trait ScalarFunction: Debug + Send + Sync {
    type Call: ScalarCall + 'static;

    fn create_call(&self) -> Result<Self::Call>;
}

pub trait ScalarCall: Send + Sync {
    /// The planner reads the function's SQL return type from this type, so a
    /// function cannot declare one type and return another.
    type Output: ScalarReturn;

    fn step(&mut self, connection: &Arc<Connection>, args: &[Register])
        -> IOResultOr<Self::Output>;
}

/// A Rust type a native scalar function can return.
pub trait ScalarReturn {
    /// The SQL type of every non-NULL result, or `None` when results can have
    /// any type.
    const TYPE: Option<ReturnType>;

    fn into_value(self) -> Value;
}

impl ScalarReturn for Value {
    const TYPE: Option<ReturnType> = None;

    fn into_value(self) -> Value {
        self
    }
}

impl ScalarReturn for i64 {
    const TYPE: Option<ReturnType> = Some(ReturnType::Integer);

    fn into_value(self) -> Value {
        Value::from_i64(self)
    }
}

/// Stored as 0 or 1, like SQLite booleans.
impl ScalarReturn for bool {
    const TYPE: Option<ReturnType> = Some(ReturnType::Integer);

    fn into_value(self) -> Value {
        Value::from_i64(self as i64)
    }
}

impl ScalarReturn for f64 {
    const TYPE: Option<ReturnType> = Some(ReturnType::Real);

    fn into_value(self) -> Value {
        Value::from_f64(self)
    }
}

impl ScalarReturn for String {
    const TYPE: Option<ReturnType> = Some(ReturnType::Text);

    fn into_value(self) -> Value {
        Value::build_text(self)
    }
}

impl ScalarReturn for &'static str {
    const TYPE: Option<ReturnType> = Some(ReturnType::Text);

    fn into_value(self) -> Value {
        Value::build_text(self)
    }
}

impl ScalarReturn for ValueBlob {
    const TYPE: Option<ReturnType> = Some(ReturnType::Blob);

    fn into_value(self) -> Value {
        Value::from_blob(self)
    }
}

/// `None` is SQL NULL, which every function may return whatever its type.
impl<T: ScalarReturn> ScalarReturn for Option<T> {
    const TYPE: Option<ReturnType> = T::TYPE;

    fn into_value(self) -> Value {
        self.map_or(Value::Null, ScalarReturn::into_value)
    }
}

/// `ScalarCall` with its output converted to `Value`, so calls of different
/// output types can be stored behind one trait object.
pub trait ScalarStep: Send + Sync {
    fn step(&mut self, connection: &Arc<Connection>, args: &[Register]) -> IOResultOr<Value>;
}

impl<C: ScalarCall> ScalarStep for C {
    fn step(&mut self, connection: &Arc<Connection>, args: &[Register]) -> IOResultOr<Value> {
        Ok(ScalarCall::step(self, connection, args)?.map(ScalarReturn::into_value))
    }
}

pub trait AggregateFunction: Debug + Send + Sync {
    type Accumulator: Aggregate + 'static;

    fn create_accumulator(&self) -> Result<Self::Accumulator>;
}

pub trait Aggregate: Debug + Send + Sync {
    fn step(&mut self, args: &[Value]) -> IOResultOr<()>;
    fn finalize(&mut self) -> IOResultOr<Value>;
}

pub trait ScalarFactory: Debug + Send + Sync {
    fn create_call(&self) -> Result<Box<dyn ScalarStep>>;
}

impl<F: ScalarFunction> ScalarFactory for F {
    fn create_call(&self) -> Result<Box<dyn ScalarStep>> {
        Ok(Box::new(ScalarFunction::create_call(self)?))
    }
}

pub(crate) fn step_scalar(
    state: &mut ExtensionState,
    factory: &dyn ScalarFactory,
    connection: &Arc<Connection>,
    args: &[Register],
) -> IOResultOr<Value> {
    if matches!(state, ExtensionState::None) {
        *state = ExtensionState::ScalarCall(factory.create_call()?);
    }
    let ExtensionState::ScalarCall(call) = state else {
        unreachable!("scalar instruction requires scalar call state");
    };
    let result = call.step(connection, args);
    if !matches!(result, Ok(IOResult::IO(_))) {
        *state = ExtensionState::None;
    }
    result
}

pub trait AggregateFactory: Debug + Send + Sync {
    fn create_accumulator(&self) -> Result<AggregateState>;
}

impl<F: AggregateFunction> AggregateFactory for F {
    fn create_accumulator(&self) -> Result<AggregateState> {
        Ok(AggregateState(Box::new(
            AggregateFunction::create_accumulator(self)?,
        )))
    }
}

pub(crate) fn step_aggregate(
    accumulator: &mut Register,
    factory: &dyn AggregateFactory,
    args: &[Value],
) -> IOResultOr<()> {
    let accumulator = ensure_accumulator(accumulator, factory)?;
    accumulator.step(args)
}

pub(crate) fn finalize_aggregate(
    accumulator: &mut Register,
    factory: &dyn AggregateFactory,
) -> IOResultOr<Value> {
    let result = ensure_accumulator(accumulator, factory)?.finalize();
    if !matches!(result, Ok(IOResult::IO(_))) {
        accumulator.set_value(Value::Null);
    }
    result
}

fn ensure_accumulator<'a>(
    accumulator: &'a mut Register,
    factory: &dyn AggregateFactory,
) -> Result<&'a mut AggregateState> {
    if matches!(accumulator, Register::Value(Value::Null)) {
        *accumulator = Register::Aggregate(AggContext::Native(factory.create_accumulator()?));
    }
    let Register::Aggregate(AggContext::Native(accumulator)) = accumulator else {
        unreachable!("native aggregate requires a native accumulator");
    };
    Ok(accumulator)
}

#[derive(Debug)]
pub struct AggregateState(Box<dyn Aggregate>);

impl AggregateState {
    pub(crate) fn step(&mut self, args: &[Value]) -> IOResultOr<()> {
        self.0.step(args)
    }

    pub(crate) fn finalize(&mut self) -> IOResultOr<Value> {
        self.0.finalize()
    }
}

impl PartialEq for AggregateState {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0.as_ref(), other.0.as_ref())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnType {
    Integer,
    Real,
    Text,
    Blob,
}

#[derive(Debug, Clone, Copy)]
pub enum FunctionArity {
    Exact(usize),
    OneOf(&'static [usize]),
    Variadic,
}

impl FunctionArity {
    pub fn accepts(self, count: usize) -> bool {
        match self {
            Self::Exact(expected) => count == expected,
            Self::OneOf(counts) => counts.contains(&count),
            Self::Variadic => true,
        }
    }

    pub(crate) fn validate(self) -> Result<()> {
        let valid = match self {
            Self::Exact(count) => i32::try_from(count).is_ok(),
            Self::OneOf(counts) => {
                !counts.is_empty() && counts.iter().all(|count| i32::try_from(*count).is_ok())
            }
            Self::Variadic => true,
        };
        if !valid {
            return Err(LimboError::InvalidArgument(
                "function argument counts must be nonempty and fit in i32".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn arg_counts(self) -> Vec<i32> {
        match self {
            Self::Exact(count) => vec![i32::try_from(count).expect("validated argument count")],
            Self::OneOf(counts) => counts
                .iter()
                .map(|count| i32::try_from(*count).expect("validated argument count"))
                .collect(),
            Self::Variadic => vec![-1],
        }
    }
}
