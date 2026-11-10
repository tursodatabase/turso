use super::FunctionCompileContext;
use super::ScalarState;
use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::numeric::Numeric;
use crate::stats::StatAccum;
use crate::IOResult;
use crate::LimboError;
#[cfg(feature = "json")]
use crate::{json, namespace::NamespaceId, ValueRef};
use crate::{Result, Value};
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct StatInit;

impl ScalarFunction for StatInit {
    fn is_internal(&self) -> bool {
        true
    }

    fn compile(&self, _context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        crate::bail_parse_error!(
            "{} is an internal function used by ANALYZE",
            self.metadata().name
        );
    }
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "stat_init",
            arity: FunctionArity::Exact(1),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Blob),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        // stat_init(n_col): Initialize a statistics accumulator
        // Returns a blob containing the serialized StatAccum
        let n_col = match args.get(0) {
            Value::Numeric(Numeric::Integer(n)) => *n as usize,
            _ => 0,
        };
        let accum = StatAccum::new(n_col);
        Ok(IOResult::Done(Value::from_blob(accum.to_bytes())))
    }
}

#[derive(Debug)]
pub(crate) struct StatPush;

impl ScalarFunction for StatPush {
    fn is_internal(&self) -> bool {
        true
    }

    fn compile(&self, _context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        crate::bail_parse_error!(
            "{} is an internal function used by ANALYZE",
            self.metadata().name
        );
    }
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "stat_push",
            arity: FunctionArity::Exact(2),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Blob),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        // stat_push(accum_blob, i_chng): Push a row into the accumulator
        // i_chng is the index of the leftmost column that changed from the previous row
        // Returns the updated accumulator blob
        let accum_blob = args.get(0);
        let i_chng = match args.get(1) {
            Value::Numeric(Numeric::Integer(n)) => *n as usize,
            _ => 0,
        };
        let result = match accum_blob {
            Value::Blob(bytes) => {
                if let Some(mut accum) = StatAccum::from_bytes(bytes) {
                    accum.push(i_chng);
                    Value::Blob(accum.to_bytes())
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        };
        Ok(IOResult::Done(result))
    }
}

#[derive(Debug)]
pub(crate) struct StatGet;

impl ScalarFunction for StatGet {
    fn is_internal(&self) -> bool {
        true
    }

    fn compile(&self, _context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        crate::bail_parse_error!(
            "{} is an internal function used by ANALYZE",
            self.metadata().name
        );
    }
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "stat_get",
            arity: FunctionArity::Exact(1),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        // stat_get(accum_blob): Get the stat1 string from the accumulator
        // Returns the stat string "total avg1 avg2 ..."
        let accum_blob = args.get(0);
        let result = match accum_blob {
            Value::Blob(bytes) => {
                if let Some(accum) = StatAccum::from_bytes(bytes) {
                    let stat_str = accum.get_stat1();
                    if stat_str.is_empty() {
                        Value::Null
                    } else {
                        Value::build_text(stat_str)
                    }
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        };
        Ok(IOResult::Done(result))
    }
}

#[derive(Debug)]
pub(crate) struct ConnTxnId;

impl ScalarFunction for ConnTxnId {
    fn is_internal(&self) -> bool {
        true
    }

    fn compile(&self, _context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        crate::bail_parse_error!(
            "{} is an internal function used by CDC",
            self.metadata().name
        );
    }
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "conn_txn_id",
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
        // conn_txn_id(candidate): get-or-set semantics for CDC transaction ID.
        // If unset (-1), store the candidate and return it.
        // If already set, return the existing value, ignoring the candidate.
        let candidate = match args.get(0) {
            Value::Numeric(Numeric::Integer(n)) => *n,
            _ => -1,
        };
        let current = context.connection.get_cdc_transaction_id();
        if current == -1 {
            context.connection.set_cdc_transaction_id(candidate);
            Ok(IOResult::Done(Value::from_i64(candidate)))
        } else {
            Ok(IOResult::Done(Value::from_i64(current)))
        }
    }
}

#[derive(Debug)]
pub(crate) struct IsAutocommit;

impl ScalarFunction for IsAutocommit {
    fn is_internal(&self) -> bool {
        true
    }

    fn compile(&self, _context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        crate::bail_parse_error!(
            "{} is an internal function used by CDC",
            self.metadata().name
        );
    }
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "is_autocommit",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        _args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        // is_autocommit(): returns 1 if autocommit, 0 otherwise.
        let auto_commit = context.connection.auto_commit.load(Ordering::SeqCst);
        Ok(IOResult::Done(Value::from_i64(if auto_commit {
            1
        } else {
            0
        })))
    }
}

#[derive(Debug)]
pub(crate) struct TableColumnsJsonArray;

impl ScalarFunction for TableColumnsJsonArray {
    fn is_internal(&self) -> bool {
        true
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "table_columns_json_array",
            arity: FunctionArity::Exact(1),
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
        #[cfg(not(feature = "json"))]
        {
            return Err(LimboError::InvalidArgument(
                "table_columns_json_array: turso must be compiled with JSON support".to_string(),
            ));
        }
        #[cfg(feature = "json")]
        {
            use crate::types::TextSubtype;

            let table = args.get(0);
            let Value::Text(table) = table else {
                return Err(LimboError::InvalidArgument(
                    "table_columns_json_array: function argument must be of type TEXT".to_string(),
                ));
            };
            let table = {
                let schema = context.connection.schema.read();
                match schema.get_table(NamespaceId::ROOT, table.as_str()) {
                    Some(table) => table,
                    None => {
                        return Err(LimboError::InvalidArgument(format!(
                            "table_columns_json_array: table {table} doesn't exists"
                        )));
                    }
                }
            };

            let mut json = json::jsonb::Jsonb::make_empty_array(table.columns().len() * 10)?;
            for column in table.columns() {
                use crate::types::TextRef;

                let name = column.name.as_ref().expect("column should have a name");
                let name_json = json::convert_ref_dbtype_to_jsonb(
                    ValueRef::Text(TextRef::new(name, TextSubtype::Text)),
                    json::Conv::ToString,
                )?;
                json.append_jsonb_to_end(name_json.data());
            }
            json.finalize_unsafe(json::jsonb::ElementType::ARRAY)?;
            json::json_string_to_db_type(
                json,
                json::jsonb::ElementType::ARRAY,
                json::OutputVariant::String,
            )
            .map(IOResult::Done)
        }
    }
}

#[derive(Debug)]
pub(crate) struct BinRecordJsonObject;

impl ScalarFunction for BinRecordJsonObject {
    fn is_internal(&self) -> bool {
        true
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "bin_record_json_object",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        #[cfg(not(feature = "json"))]
        {
            return Err(LimboError::InvalidArgument(
                "bin_record_json_object: turso must be compiled with JSON support".to_string(),
            ));
        }
        #[cfg(feature = "json")]
        {
            use crate::types::ValueIterator;
            use std::str::FromStr;

            let columns_str = args.get(0);
            let bin_record = args.get(1);
            let Value::Text(columns_str) = columns_str else {
                return Err(LimboError::InvalidArgument(
                            "bin_record_json_object: function arguments must be of type TEXT and BLOB correspondingly".to_string()
                        ));
            };

            if let Value::Null = bin_record {
                return Ok(IOResult::Done(Value::Null));
            }

            let Value::Blob(bin_record) = bin_record else {
                return Err(LimboError::InvalidArgument(
                            "bin_record_json_object: function arguments must be of type TEXT and BLOB correspondingly".to_string()
                        ));
            };
            let mut columns_json_array = json::jsonb::Jsonb::from_str(columns_str.as_str())?;
            let columns_len = columns_json_array.array_len()?;

            let mut payload_iterator = ValueIterator::new(bin_record.as_slice())?;

            let mut json = json::jsonb::Jsonb::make_empty_obj(columns_len)?;
            for i in 0..columns_len {
                let mut op = json::jsonb::SearchOperation::new(0)?;
                let path = json::path::JsonPath {
                    elements: vec![
                        json::path::PathElement::Root(),
                        json::path::PathElement::ArrayLocator(Some(i as i32)),
                    ],
                };

                columns_json_array.operate_on_path(&path, &mut op)?;
                let column_name = op.result();
                json.append_jsonb_to_end(column_name.data());

                let val = match payload_iterator.next() {
                    Some(Ok(v)) => v,
                    Some(Err(e)) => return Err(e),
                    None => {
                        return Err(LimboError::InvalidArgument(
                                    "bin_record_json_object: binary record has fewer columns than specified in the columns argument".to_string()
                                ));
                    }
                };

                if let ValueRef::Blob(..) = val {
                    return Err(LimboError::InvalidArgument(
                                "bin_record_json_object: formatting of BLOB values stored in binary record is not supported".to_string()
                            ));
                }
                let val_json = json::convert_ref_dbtype_to_jsonb(val, json::Conv::NotStrict)?;
                json.append_jsonb_to_end(val_json.data());
            }
            json.finalize_unsafe(json::jsonb::ElementType::OBJECT)?;

            json::json_string_to_db_type(
                json,
                json::jsonb::ElementType::OBJECT,
                json::OutputVariant::String,
            )
            .map(IOResult::Done)
        }
    }
}

#[derive(Debug)]
pub(crate) struct TestUintEncode;

impl ScalarFunction for TestUintEncode {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "test_uint_encode",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        let val = args.get(0);
        let result = match val {
            Value::Null => Value::Null,
            Value::Numeric(Numeric::Integer(i)) => {
                if *i < 0 {
                    return Err(LimboError::InternalError(
                        "test_uint_encode: negative value".to_string(),
                    ));
                }
                Value::build_text(i.to_string())
            }
            Value::Numeric(Numeric::Float(f)) => {
                if *f < 0.0 || f.fract() != 0.0 {
                    return Err(LimboError::InternalError(
                        "test_uint_encode: not a non-negative integer".to_string(),
                    ));
                }
                Value::build_text((f64::from(*f) as u64).to_string())
            }
            Value::Text(t) => {
                let s = t.to_string();
                s.parse::<u64>().map_err(|_| {
                    LimboError::InternalError(format!("test_uint_encode: invalid uint: {s}"))
                })?;
                Value::build_text(s)
            }
            _ => {
                return Err(LimboError::InternalError(
                    "test_uint_encode: unsupported type".to_string(),
                ));
            }
        };
        Ok(IOResult::Done(result))
    }
}

#[derive(Debug)]
pub(crate) struct TestUintDecode;

impl ScalarFunction for TestUintDecode {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "test_uint_decode",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::FirstArgument,
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        let val = args.get(0);
        let result = match val {
            Value::Null => Value::Null,
            other => other.clone(),
        };
        Ok(IOResult::Done(result))
    }
}

#[cfg(feature = "test_helper")]
#[derive(Debug)]
pub(crate) struct TestNondetCounter;

#[cfg(feature = "test_helper")]
impl ScalarFunction for TestNondetCounter {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "test_nondet_counter",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        _args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        // Test-only: per-connection counter that increments on every
        // evaluation. Used in sqltests to verify that the planner does
        // not deduplicate equivalent SQL calls that contain
        // nondeterministic functions.
        let value = context
            .connection
            .test_nondet_counter
            .fetch_add(1, Ordering::Relaxed);
        Ok(IOResult::Done(Value::from_i64(value)))
    }
}
