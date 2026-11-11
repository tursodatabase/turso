use super::ScalarState;
use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::function::intrinsic::{FunctionCompileContext, IntrinsicFunction};
use crate::IOResult;
use crate::{namespace::mangle_name, LimboError};
use crate::{Result, Value};

#[derive(Debug)]
pub(crate) struct NextVal;

impl IntrinsicFunction for NextVal {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.nextval()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "nextval",
            arity: FunctionArity::Exact(1),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
}

#[derive(Debug)]
pub(crate) struct CurrVal;

impl IntrinsicFunction for CurrVal {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        ScalarFunction::metadata(self)
    }
    fn compile(&self, mut context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.func_ctx.func = crate::Func::Scalar(&CurrVal);
        context.currval()
    }
}

impl ScalarFunction for CurrVal {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "currval",
            arity: FunctionArity::Exact(1),
            deterministic: false,
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
        let seq_name = match args.get(0) {
            Value::Text(t) => t.as_str().to_string(),
            _ => {
                return Err(crate::LimboError::ParseError(
                    "currval() requires a text argument".to_string(),
                ));
            }
        };
        // The connection's currval session map persists across
        // DROP SEQUENCE (the entry is keyed by name with no link
        // to the schema). Resolve the sequence first so currval
        // on a dropped sequence reports "does not exist" rather
        // than silently returning the stale per-session value.
        let (namespace, sequence) = context.connection.find_sequence(&seq_name)?;
        match context.connection.get_sequence_currval(
            namespace.database_id,
            &mangle_name(namespace.id, &sequence.name),
        ) {
            Some(val) => Ok(IOResult::Done(Value::from_i64(val))),
            None => Err(LimboError::ParseError(format!(
                "currval of sequence \"{}\" is not yet defined in this session",
                sequence.name
            ))),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SetVal;

impl IntrinsicFunction for SetVal {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        ScalarFunction::metadata(self)
    }
    fn compile(&self, mut context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.func_ctx.func = crate::Func::Scalar(&SetVal);
        context.setval()
    }
}

impl ScalarFunction for SetVal {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "setval",
            arity: FunctionArity::OneOf(&[2, 3]),
            deterministic: false,
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
        if context.connection.is_tx_read_only() {
            return Err(LimboError::ReadOnly);
        }
        // Pre-write validation: argument type + range check. The
        // translator emits this Function call before the inline
        // DELETE-all + INSERT bytecode, so any error returned here
        // aborts the statement BEFORE any disk write — which is what
        // keeps the op_insert "expected integer key" assertion safe
        // and the user-facing error message intact. After validation
        // the result value is placed in `dest`; currval bookkeeping
        // and dirty-marking are emitted by the translator via the
        // separate SetSequenceCurrval opcode.
        //
        // Note: in the disk-only design there is no pending-setval
        // queue, no in-memory atomic to mutate, and no exclusive-tx
        // requirement. The autonomous-tx mechanism (planned for MVCC
        // concurrency in a follow-up) will subsume serialization.
        let seq_name = match args.get(0) {
            Value::Text(t) => t.as_str().to_string(),
            _ => {
                return Err(crate::LimboError::ParseError(
                    "setval() requires a text argument".to_string(),
                ));
            }
        };
        let value = match args.get(1).as_int() {
            Some(v) => v,
            None => {
                return Err(crate::LimboError::ParseError(
                    "setval() requires an integer value".to_string(),
                ));
            }
        };
        if args.len() > 2 && args.get(2).as_int().is_none() {
            return Err(crate::LimboError::ParseError(
                "setval() requires an integer is_called argument".to_string(),
            ));
        }
        let (_, seq) = context.connection.find_sequence(&seq_name)?;
        if value < seq.min_value || value > seq.max_value {
            return Err(crate::LimboError::ParseError(format!(
                "setval: value {} is out of bounds for sequence \"{}\" ({}..{})",
                value, seq.name, seq.min_value, seq.max_value
            )));
        }
        Ok(IOResult::Done(Value::from_i64(value)))
    }
}

#[derive(Debug)]
pub(crate) struct SequenceWatermark;

impl ScalarFunction for SequenceWatermark {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "sequence_watermark_experimental",
            arity: FunctionArity::Exact(1),
            deterministic: false,
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
        let sequence_name = match args.get(0) {
            Value::Text(text) => text.as_str(),
            Value::Null => {
                return Ok(IOResult::Done(Value::Null));
            }
            _ => {
                return Err(LimboError::InternalError(
                    "sequence_watermark_experimental() argument must be TEXT".to_string(),
                ));
            }
        };
        let (namespace, sequence) = context.connection.find_sequence(sequence_name)?;
        let watermark = context
            .connection
            .mv_store_for_db(namespace.database_id)
            .and_then(|mv_store| {
                mv_store.sequence_watermark(&mangle_name(namespace.id, &sequence.name))
            });
        match watermark {
            Some(watermark) => Ok(IOResult::Done(Value::from_i64(watermark))),
            None => Ok(IOResult::Done(Value::Null)),
        }
    }
}
