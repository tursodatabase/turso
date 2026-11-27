use crate::alloc::TryClone;
use crate::function::{ExtFunc, ExternalFunc};
use crate::native_ext::{
    AggregateFunction, ExtensionState, FunctionArity, ScalarCall, ScalarFunction, ScalarReturn,
};
use crate::sync::Arc;
use crate::types::{AggContext, ExternalAggState, IOResultOr};
use crate::{dialect::ScalarFuncContext, IOResult, OpenOptions, Register, Result, Value};
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
        arity: FunctionArity,
        deterministic: bool,
        function: F,
    ) -> Result<Self> {
        arity.validate()?;
        Ok(Self {
            name,
            func: ExtFunc::NativeScalar {
                arity,
                deterministic,
                return_type: <<F::Call as ScalarCall>::Output as ScalarReturn>::TYPE,
                receives_argument_types: false,
                check_argument_types: None,
                returns_argument_type: None,
                function: Arc::new(function),
            },
        })
    }

    /// Makes each call also get the type name of each argument, such as
    /// `boolean[]` or `integer`, as one more text argument per argument, after
    /// them. A function that writes values whose stored form doesn't tell
    /// their type, such as a boolean stored as 1, uses this.
    pub fn receiving_argument_types(mut self) -> Self {
        let ExtFunc::NativeScalar {
            receives_argument_types,
            ..
        } = &mut self.func
        else {
            panic!("only a native scalar function can receive its argument types");
        };
        *receives_argument_types = true;
        self
    }

    /// FIXME(pg-catalog): HACK. Its only callers are pg_timestamp_plus_interval,
    /// date_trunc and chr in postgres/frontend/functions.rs. It stands in for
    /// PostgreSQL choosing an operator or function by its argument types from
    /// pg_operator or pg_proc when it plans the statement, which tursopg cannot
    /// do yet. A pg_proc and pg_operator lookup replaces it; remove it then.
    ///
    /// Sets a function that is called at compile time with the type names of
    /// the arguments, as [`crate::function::ArgumentTypeCheck`] describes; an
    /// error refuses the statement. Needs `receiving_argument_types`.
    pub fn checking_argument_types(mut self, check: crate::function::ArgumentTypeCheck) -> Self {
        let ExtFunc::NativeScalar {
            receives_argument_types,
            check_argument_types,
            ..
        } = &mut self.func
        else {
            panic!("only a native scalar function can check its argument types");
        };
        assert!(
            *receives_argument_types,
            "checking argument types needs receiving_argument_types"
        );
        *check_argument_types = Some(check);
        self
    }

    /// FIXME(pg-catalog): HACK. Its only callers are pg_timestamp_plus_interval (index 0) and
    /// date_trunc (index 1) in postgres/frontend/functions.rs. PostgreSQL takes
    /// a result type from prorettype in pg_proc or oprresult in pg_operator,
    /// not from an argument. A pg_proc and pg_operator lookup replaces it;
    /// remove it then.
    ///
    /// Gives the function's result the type of the argument at `index`. The
    /// function returns `Value`, which declares no type of its own.
    pub fn returning_argument_type(mut self, index: usize) -> Self {
        let ExtFunc::NativeScalar {
            return_type,
            returns_argument_type,
            ..
        } = &mut self.func
        else {
            panic!("only a native scalar function can return an argument's type");
        };
        assert!(
            return_type.is_none(),
            "a function that returns an argument's type declares no return type"
        );
        *returns_argument_type = Some(index);
        self
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
    pub fn call_scalar(
        &self,
        state: &mut ExtensionState,
        context: &ScalarFuncContext<'_>,
        args: &[Register],
    ) -> IOResultOr<Value> {
        match self {
            Self::NativeScalar { function, .. } => {
                crate::native_ext::step_scalar(state, function.as_ref(), context, args)
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
