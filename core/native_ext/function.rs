use super::ExtensionState;
use crate::alloc::TryClone;
use crate::types::{AggContext, IOResultOr};
use crate::{IOResult, Register, Result, Value};
use std::fmt::Debug;

pub trait ScalarFunction: Debug + Send + Sync {
    type Call: ScalarCall + 'static;

    fn create_call(&self, args: Vec<Value>) -> Result<Self::Call>;
}

pub trait ScalarCall: Send + Sync {
    fn step(&mut self) -> IOResultOr<Value>;
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
    fn create_call(&self, args: Vec<Value>) -> Result<Box<dyn ScalarCall>>;
}

impl<F: ScalarFunction> ScalarFactory for F {
    fn create_call(&self, args: Vec<Value>) -> Result<Box<dyn ScalarCall>> {
        Ok(Box::new(ScalarFunction::create_call(self, args)?))
    }
}

pub(crate) fn step_scalar(
    state: &mut ExtensionState,
    factory: &dyn ScalarFactory,
    args: &[Register],
) -> IOResultOr<Value> {
    if matches!(state, ExtensionState::None) {
        let args = args
            .iter()
            .map(|arg| arg.get_value().try_clone())
            .collect::<Result<Vec<_>, _>>()?;
        *state = ExtensionState::ScalarCall(factory.create_call(args)?);
    }
    let ExtensionState::ScalarCall(call) = state else {
        unreachable!("scalar instruction requires scalar call state");
    };
    let result = call.step();
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
