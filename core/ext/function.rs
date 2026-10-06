use crate::alloc::TryClone;
use crate::function::{ExtFunc, ExternalFunc};
use crate::native_ext::{AggregateFunction, ExtensionState, ScalarFunction};
use crate::sync::Arc;
use crate::types::{AggContext, ExternalAggState, IOResultOr};
use crate::{IOResult, OpenOptions, Register, Result, Value};
use turso_ext::ValueDestructor;

impl OpenOptions {
    pub fn extension_function(mut self, mut function: ExternalFunc) -> Self {
        function.name = crate::util::normalize_ident(&function.name);
        self.native_extensions.functions.push(Arc::new(function));
        self
    }
}

impl ExternalFunc {
    pub fn new_native_scalar<F: ScalarFunction + 'static>(
        name: String,
        argc: i32,
        deterministic: bool,
        function: F,
    ) -> Result<Self> {
        Self::validate_arg_count(argc)?;
        Ok(Self {
            name,
            func: ExtFunc::NativeScalar {
                argc,
                deterministic,
                function: Arc::new(function),
            },
        })
    }

    pub fn new_native_aggregate<F: AggregateFunction + 'static>(
        name: String,
        argc: i32,
        function: F,
    ) -> Result<Self> {
        Self::validate_arg_count(argc)?;
        Ok(Self {
            name,
            func: ExtFunc::NativeAggregate {
                argc,
                function: Arc::new(function),
            },
        })
    }
}

impl ExtFunc {
    pub(crate) fn call_scalar(
        &self,
        state: &mut ExtensionState,
        args: &[Register],
    ) -> IOResultOr<Value> {
        match self {
            Self::NativeScalar { function, .. } => {
                crate::native_ext::step_scalar(state, function.as_ref(), args)
            }
            Self::Scalar {
                context,
                callback,
                context_destructor,
                value_destructor,
                ..
            } => {
                let args: Vec<_> = args.iter().map(|arg| arg.get_value().to_ffi()).collect();
                let argv = if args.is_empty() {
                    std::ptr::null()
                } else {
                    args.as_ptr()
                };
                let result = unsafe {
                    callback(
                        *context,
                        args.len() as i32,
                        argv,
                        *context_destructor,
                        *value_destructor,
                    )
                };
                finish_call(result, *value_destructor, args)
                    .map(IOResult::Done)
                    .map_err(Into::into)
            }
            _ => unreachable!("aggregate called in scalar context"),
        }
    }

    pub(crate) fn step_aggregate(
        &self,
        registers: &mut [Register],
        acc_reg: usize,
        start_reg: usize,
    ) -> IOResultOr<()> {
        match self {
            Self::NativeAggregate { argc, function } => {
                let args = registers[start_reg..start_reg + (*argc).max(0) as usize]
                    .iter()
                    .map(|arg| arg.get_value().try_clone())
                    .collect::<Result<Vec<_>, _>>()?;
                crate::native_ext::step_aggregate(&mut registers[acc_reg], function.as_ref(), &args)
            }
            Self::Aggregate { .. } => {
                let aggregate = self.c_aggregate(&mut registers[acc_reg]).clone();
                let args: Vec<_> = registers[start_reg..start_reg + aggregate.argc]
                    .iter()
                    .map(|arg| arg.get_value().to_ffi())
                    .collect();
                let argv = if args.is_empty() {
                    std::ptr::null()
                } else {
                    args.as_ptr()
                };
                let result = unsafe {
                    (aggregate.step_fn)(
                        aggregate.context,
                        aggregate.state,
                        aggregate.argc as i32,
                        argv,
                    )
                };
                if let Err(err) = finish_call(result, aggregate.value_destructor, args) {
                    if let Some(destructor) = aggregate.aggregate_destructor {
                        unsafe { destructor(aggregate.state as usize) };
                    }
                    registers[acc_reg].set_null();
                    return Err(err.into());
                }
                Ok(IOResult::Done(()))
            }
            _ => unreachable!("scalar function called in aggregate context"),
        }
    }

    pub(crate) fn finalize_aggregate(&self, accumulator: &mut Register) -> IOResultOr<Value> {
        match self {
            Self::NativeAggregate { function, .. } => {
                crate::native_ext::finalize_aggregate(accumulator, function.as_ref())
            }
            Self::Aggregate { .. } => {
                let aggregate = self.c_aggregate(accumulator).clone();
                accumulator.set_null();
                let result = unsafe { (aggregate.finalize_fn)(aggregate.context, aggregate.state) };
                let value = finish_call(result, aggregate.value_destructor, Vec::new());
                if let Some(destructor) = aggregate.aggregate_destructor {
                    unsafe { destructor(aggregate.state as usize) };
                }
                value.map(IOResult::Done).map_err(Into::into)
            }
            _ => unreachable!("scalar function called in aggregate context"),
        }
    }

    fn c_aggregate<'a>(&self, accumulator: &'a mut Register) -> &'a ExternalAggState {
        if matches!(accumulator, Register::Value(Value::Null)) {
            let Self::Aggregate {
                context,
                init,
                step,
                finalize,
                argc,
                aggregate_destructor,
                value_destructor,
                ..
            } = self
            else {
                unreachable!("C aggregate requires C callbacks");
            };
            *accumulator = Register::Aggregate(AggContext::External(ExternalAggState {
                context: *context,
                state: unsafe { init(*context) },
                argc: (*argc).max(0) as usize,
                step_fn: *step,
                finalize_fn: *finalize,
                aggregate_destructor: *aggregate_destructor,
                value_destructor: *value_destructor,
            }));
        }
        let Register::Aggregate(AggContext::External(aggregate)) = accumulator else {
            unreachable!("C aggregate requires C accumulator state");
        };
        aggregate
    }
}

fn finish_call(
    mut result: turso_ext::Value,
    value_destructor: Option<ValueDestructor>,
    args: Vec<turso_ext::Value>,
) -> Result<Value> {
    let value = Value::from_ffi_ref(&result);
    if let Some(destructor) = value_destructor {
        unsafe { destructor(&mut result) };
    } else {
        unsafe { result.__free_internal_type() };
    }
    for arg in args {
        unsafe { arg.__free_internal_type() };
    }
    value
}
