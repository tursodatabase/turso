//! The built-in types: the types that every database has when custom types
//! are on.

use super::{Conversion, StorageClass, Type};
use crate::ast::{self, ResolveType};
use crate::functions::builtin::{
    exec_boolean_to_int, exec_numeric_decode, exec_numeric_encode, exec_numeric_fit,
    exec_validate_ipaddr,
};
use crate::functions::datetime_input::exec_temporal_encode;
use crate::schema::{
    sqlite_operator, OperatorArg, OperatorSignature, RangeOffsets, BINARY_OPERATORS,
    PREFIX_OPERATORS,
};
use crate::type_ordering::TypeOrdering;
use crate::{LimboError, Result, Value};

/// The built-in types, in the order they are registered.
pub(crate) fn builtin_types() -> Vec<Box<dyn Type>> {
    vec![
        #[cfg(feature = "uuid")]
        Box::new(Uuid),
        Box::new(Boolean),
        #[cfg(feature = "json")]
        Box::new(Json),
        #[cfg(feature = "json")]
        Box::new(Jsonb),
        Box::new(Varchar),
        Box::new(Date),
        Box::new(DateTime {
            name: "time",
            encode: time_encode,
        }),
        Box::new(DateTime {
            name: "timestamp",
            encode: timestamp_encode,
        }),
        Box::new(DateTime {
            name: "timestamptz",
            encode: timestamptz_encode,
        }),
        Box::new(Smallint),
        Box::new(Bigint),
        Box::new(Inet),
        Box::new(UncheckedText("cidr")),
        Box::new(UncheckedText("macaddr")),
        Box::new(UncheckedText("macaddr8")),
        Box::new(Bytea),
        Box::new(Numeric),
    ]
}

/// A universally unique identifier. It is stored as 16 bytes and read as
/// text, such as `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`.
#[cfg(feature = "uuid")]
#[derive(Debug)]
struct Uuid;

#[cfg(feature = "uuid")]
impl Type for Uuid {
    fn name(&self) -> &str {
        "uuid"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("any")]
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(uuid_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(uuid_decode))
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        less_than()
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Text)
    }
}

/// A blob of 16 bytes is a UUID already. Text is read as a UUID.
#[cfg(feature = "uuid")]
fn uuid_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Blob(blob) if blob.len() == 16 => Ok(value.clone()),
        Value::Text(text) => match uuid::Uuid::parse_str(text.as_str()) {
            Ok(uuid) => Ok(Value::from_slice(uuid.as_bytes())?),
            Err(_) => Err(raise_abort("invalid UUID value")),
        },
        _ => Err(raise_abort("invalid UUID value")),
    }
}

/// NULL for a stored value that is not 16 bytes.
#[cfg(feature = "uuid")]
fn uuid_decode(value: &Value, _params: &[Value]) -> Result<Value> {
    let Value::Blob(blob) = value else {
        return Ok(Value::Null);
    };
    Ok(match uuid::Uuid::from_slice(blob) {
        Ok(uuid) => Value::build_text(uuid.to_string()),
        Err(_) => Value::Null,
    })
}

/// True or false, stored as the integer 1 or 0.
#[derive(Debug)]
struct Boolean;

impl Type for Boolean {
    fn name(&self) -> &str {
        "boolean"
    }

    fn aliases(&self) -> &[&str] {
        &["bool"]
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("any")]
    }

    fn base(&self) -> &str {
        "integer"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(boolean_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(boolean_decode))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Integer)
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        less_than()
    }
}

fn boolean_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_boolean_to_int(value)
}

/// 1 for a stored value that is true as a condition, 0 for another.
fn boolean_decode(value: &Value, _params: &[Value]) -> Result<Value> {
    Ok(Value::from_i64(i64::from(value.exec_if(false, false))))
}

/// JSON, stored as text.
#[cfg(feature = "json")]
#[derive(Debug)]
struct Json;

#[cfg(feature = "json")]
impl Type for Json {
    fn name(&self) -> &str {
        "json"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("text")]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(json_text))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }
}

/// JSON, stored in the binary form of SQLite's `jsonb()` and read as text.
#[cfg(feature = "json")]
#[derive(Debug)]
struct Jsonb;

#[cfg(feature = "json")]
impl Type for Jsonb {
    fn name(&self) -> &str {
        "jsonb"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("text")]
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(jsonb_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(json_text))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Text)
    }

    fn casts_to_numbers_and_booleans(&self) -> bool {
        true
    }
}

/// The JSON text of `value`, as the SQL function `json()` gives it.
#[cfg(feature = "json")]
fn json_text(value: &Value, _params: &[Value]) -> Result<Value> {
    crate::json::get_json(value, None)
}

/// The binary JSON of `value`, as the SQL function `jsonb()` gives it.
#[cfg(feature = "json")]
fn jsonb_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    crate::json::jsonb(value, &crate::json::JsonCacheCell::new())
}

/// Text of at most `maxlen` characters. A column declared `varchar`, without
/// a length, has no limit.
#[derive(Debug)]
struct Varchar;

impl Type for Varchar {
    fn name(&self) -> &str {
        "varchar"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("text"), param("maxlen", "integer")]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(varchar_coerce))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        less_than()
    }

    /// In PostgreSQL only text types take a COLLATE, so of the built-in
    /// types only varchar does.
    fn accepts_collate(&self) -> bool {
        true
    }
}

fn varchar_coerce(value: &Value, params: &[Value]) -> Result<Value> {
    // Compares as the SQL `length(value) <= maxlen` does. A NULL `maxlen`
    // sorts before every length, so no value fits it.
    if value.exec_length() <= params[0] {
        Ok(value.clone())
    } else {
        Err(raise_abort("value too long for varchar"))
    }
}

/// A calendar date. Dates and times read PostgreSQL's input forms and store
/// one text form with a 4-digit year, which sorts in time order (see
/// functions/datetime_input.rs).
#[derive(Debug)]
struct Date;

impl Type for Date {
    fn name(&self) -> &str {
        "date"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("text")]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(date_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        less_than()
    }

    fn stored_form_sorts_in_value_order(&self) -> bool {
        true
    }

    /// `date + integer`, `integer + date` and `date - integer` add or
    /// subtract days and give a date. `date - date` gives the number of days
    /// between them; a text literal on either side is encoded to a date
    /// first. Each signature has its own function, so its result type is
    /// known before the statement runs. A date has no other operators:
    /// `-date` is an error, as in PostgreSQL.
    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        use OperatorArg::{EncodableLiteral, Integer, Value};
        vec![
            call("+", &[Value, Integer], "date_plus", "date"),
            call("+", &[Integer, Value], "date_plus", "date"),
            call("-", &[Value, Value], "date_diff", "integer"),
            call("-", &[Value, EncodableLiteral], "date_diff", "integer"),
            call("-", &[EncodableLiteral, Value], "date_diff", "integer"),
            call("-", &[Value, Integer], "date_minus", "date"),
        ]
    }
}

/// A time of day, or a date with a time of day. See [`Date`].
#[derive(Debug)]
struct DateTime {
    name: &'static str,
    encode: super::ConversionFn,
}

impl Type for DateTime {
    fn name(&self) -> &str {
        self.name
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("text")]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(self.encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        less_than()
    }

    fn stored_form_sorts_in_value_order(&self) -> bool {
        true
    }
}

fn date_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_temporal_encode(value, &Value::build_text("date"))
}

fn time_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_temporal_encode(value, &Value::build_text("time"))
}

fn timestamp_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_temporal_encode(value, &Value::build_text("timestamp"))
}

fn timestamptz_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_temporal_encode(value, &Value::build_text("timestamptz"))
}

/// An integer from -32768 to 32767.
#[derive(Debug)]
struct Smallint;

impl Type for Smallint {
    fn name(&self) -> &str {
        "smallint"
    }

    fn aliases(&self) -> &[&str] {
        &["int2"]
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("integer")]
    }

    fn base(&self) -> &str {
        "integer"
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(smallint_coerce))
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        less_than()
    }

    fn range_offsets(&self) -> RangeOffsets {
        RangeOffsets::Integer
    }

    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        sqlite_operators()
    }
}

fn smallint_coerce(value: &Value, _params: &[Value]) -> Result<Value> {
    // Compares as the SQL `value BETWEEN -32768 AND 32767` does.
    if *value >= Value::from_i64(-32768) && *value <= Value::from_i64(32767) {
        Ok(value.clone())
    } else {
        Err(raise_abort("integer out of range for smallint"))
    }
}

/// A 64-bit integer, as INTEGER is.
#[derive(Debug)]
struct Bigint;

impl Type for Bigint {
    fn name(&self) -> &str {
        "bigint"
    }

    fn aliases(&self) -> &[&str] {
        &["int8"]
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

    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        sqlite_operators()
    }
}

/// An IPv4 or IPv6 address, stored as the text that was written.
#[derive(Debug)]
struct Inet;

impl Type for Inet {
    fn name(&self) -> &str {
        "inet"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("text")]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(inet_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }
}

fn inet_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_validate_ipaddr(value)
}

/// A type that stores the text that was written and does not check it: cidr,
/// macaddr and macaddr8.
#[derive(Debug)]
struct UncheckedText(&'static str);

impl Type for UncheckedText {
    fn name(&self) -> &str {
        self.0
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("text")]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }
}

/// Binary data, as BLOB is.
#[derive(Debug)]
struct Bytea;

impl Type for Bytea {
    fn name(&self) -> &str {
        "bytea"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![value_param("blob")]
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        less_than()
    }
}

/// An exact decimal number. `numeric(precision, scale)` limits the digits; a
/// column declared `numeric` has no limits. It is stored as a blob, which
/// does not sort in the order of the numbers, and read as text.
#[derive(Debug)]
struct Numeric;

impl Type for Numeric {
    fn name(&self) -> &str {
        "numeric"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![
            value_param("any"),
            param("precision", "integer"),
            param("scale", "integer"),
        ]
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(numeric_encode))
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(numeric_coerce))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(numeric_decode))
    }

    fn range_offsets(&self) -> RangeOffsets {
        RangeOffsets::Numeric
    }

    fn ordering(&self) -> Option<TypeOrdering> {
        Some(TypeOrdering::Numeric)
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Text)
    }

    /// `numeric op x` and `x op numeric` for `+ - * / %`, and `-numeric`,
    /// compute exactly, and the result is a numeric. The functions read plain
    /// values as they are, so nothing is encoded first: encoding a literal to
    /// the column's type would round it to the column's scale first
    /// (`n * 0.005` would be `n * 0.01`). With a REAL value `+ - * /` are
    /// SQLite's operators and give a REAL, as PostgreSQL gives double
    /// precision; `%` with a REAL value does not exist, as in PostgreSQL. A
    /// numeric has no other operators: `numeric & 1` is an error, as in
    /// PostgreSQL.
    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        use OperatorArg::{Exact, Real, Value};
        let mut signatures = Vec::new();
        for (op, func) in [
            ("+", "numeric_add"),
            ("-", "numeric_sub"),
            ("*", "numeric_mul"),
            ("/", "numeric_div"),
            ("%", "numeric_mod"),
        ] {
            for args in [[Value, Value], [Value, Exact], [Exact, Value]] {
                signatures.push(call(op, &args, func, "numeric"));
            }
            if op != "%" {
                signatures.extend(sqlite_operator(op, Real));
            }
        }
        signatures.push(call("-", &[Value], "numeric_neg", "numeric"));
        signatures
    }
}

fn numeric_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_numeric_encode(value)
}

fn numeric_coerce(value: &Value, params: &[Value]) -> Result<Value> {
    exec_numeric_fit(value, &params[0], &params[1])
}

fn numeric_decode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_numeric_decode(value)
}

/// The `value` parameter of a type that takes values of type `ty`.
fn value_param(ty: &str) -> ast::TypeParam {
    param("value", ty)
}

fn param(name: &str, ty: &str) -> ast::TypeParam {
    ast::TypeParam {
        name: name.to_string(),
        ty: Some(ty.to_string()),
    }
}

/// `OPERATOR '<'`: the stored forms sort in the order of the values, so
/// ORDER BY and CREATE INDEX accept the type.
fn less_than() -> Vec<ast::TypeOperator> {
    vec![ast::TypeOperator {
        op: "<".to_string(),
        func_name: None,
    }]
}

/// The signature of an operator that runs the SQL function `func`, which
/// returns a value of the type `result`.
fn call(
    op: &'static str,
    args: &[OperatorArg],
    func: &str,
    result: &'static str,
) -> OperatorSignature {
    OperatorSignature {
        op,
        args: args.to_vec(),
        func: Some(func.to_string()),
        result: Some(result),
    }
}

/// The operators of a type that stores plain integers: SQLite's operators
/// run, including `-`, `~`, `&`, `|`, `<<` and `>>`.
fn sqlite_operators() -> Vec<OperatorSignature> {
    let binary = BINARY_OPERATORS
        .into_iter()
        .flat_map(|op| sqlite_operator(op, OperatorArg::PlainValue));
    let prefix = PREFIX_OPERATORS.into_iter().map(|op| OperatorSignature {
        op,
        args: vec![OperatorArg::Value],
        func: None,
        result: None,
    });
    binary.chain(prefix).collect()
}

/// The error that `RAISE(ABORT, message)` raises outside a trigger.
fn raise_abort(message: &str) -> LimboError {
    LimboError::Raise(
        ResolveType::Abort,
        Box::new(LimboError::SqlError(message.to_string())),
    )
}
