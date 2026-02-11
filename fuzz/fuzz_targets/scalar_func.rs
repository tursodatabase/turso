#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use turso_core::functions::datetime::{
    exec_date, exec_datetime_full, exec_julianday, exec_strftime, exec_time, exec_timediff,
    exec_unixepoch,
};
use turso_core::functions::printf::exec_printf;
use turso_core::json::{
    is_json_valid, json_array, json_array_length, json_error_position, json_extract, json_insert,
    json_object, json_patch, json_quote, json_remove, json_replace, json_set, json_type,
    JsonCacheCell,
};
use turso_core::vdbe::Register;
use turso_core::MathFunc;
use turso_core::Value as CoreValue;

const MAX_VARIADIC_ARGS: usize = 100;

#[derive(Arbitrary, Debug, Clone)]
enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<Value> for CoreValue {
    fn from(value: Value) -> CoreValue {
        match value {
            Value::Null => CoreValue::Null,
            Value::Integer(v) => CoreValue::from_i64(v),
            Value::Real(v) => {
                if v.is_nan() {
                    CoreValue::Null
                } else {
                    CoreValue::from_f64(v)
                }
            }
            Value::Text(v) => CoreValue::from_text(v),
            Value::Blob(v) => CoreValue::from_blob(v),
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum UnaryFunc {
    Lower,
    Upper,
    Length,
    OctetLength,
    Sign,
    Soundex,
    Abs,
    Quote,
    Typeof,
    Hex,
    Unicode,
    ZeroBlob,
    BitNot,
    BooleanNot,
}

#[derive(Arbitrary, Debug, Clone)]
enum UnaryFuncWithOptionalArg {
    Trim,
    LTrim,
    RTrim,
    Round,
    Unhex,
}

#[derive(Arbitrary, Debug, Clone)]
enum BinaryFunc {
    Instr,
    Add,
    Subtract,
    Multiply,
    Divide,
    BitAnd,
    BitOr,
    Remainder,
    ShiftLeft,
    ShiftRight,
    Concat,
    And,
    Or,
    Glob,
    Substring2,
    Timediff,
}

#[derive(Arbitrary, Debug, Clone)]
enum TernaryFunc {
    Substring,
    Replace,
}

#[derive(Arbitrary, Debug, Clone)]
enum VariadicFunc {
    Min,
    Max,
    ConcatStrings,
    ConcatWs,
    Char,
    Printf,
    Date,
    Time,
    DateTime,
    JulianDay,
    UnixEpoch,
    Strftime,
}

#[derive(Arbitrary, Debug, Clone)]
enum CastType {
    Text,
    Real,
    Integer,
    Numeric,
    Blob,
}

#[derive(Arbitrary, Debug, Clone)]
enum MathUnaryFunc {
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atanh,
    Ceil,
    Ceiling,
    Cos,
    Cosh,
    Degrees,
    Exp,
    Floor,
    Ln,
    Log10,
    Log2,
    Radians,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,
    Trunc,
}

impl From<MathUnaryFunc> for MathFunc {
    fn from(f: MathUnaryFunc) -> MathFunc {
        match f {
            MathUnaryFunc::Acos => MathFunc::Acos,
            MathUnaryFunc::Acosh => MathFunc::Acosh,
            MathUnaryFunc::Asin => MathFunc::Asin,
            MathUnaryFunc::Asinh => MathFunc::Asinh,
            MathUnaryFunc::Atan => MathFunc::Atan,
            MathUnaryFunc::Atanh => MathFunc::Atanh,
            MathUnaryFunc::Ceil => MathFunc::Ceil,
            MathUnaryFunc::Ceiling => MathFunc::Ceiling,
            MathUnaryFunc::Cos => MathFunc::Cos,
            MathUnaryFunc::Cosh => MathFunc::Cosh,
            MathUnaryFunc::Degrees => MathFunc::Degrees,
            MathUnaryFunc::Exp => MathFunc::Exp,
            MathUnaryFunc::Floor => MathFunc::Floor,
            MathUnaryFunc::Ln => MathFunc::Ln,
            MathUnaryFunc::Log10 => MathFunc::Log10,
            MathUnaryFunc::Log2 => MathFunc::Log2,
            MathUnaryFunc::Radians => MathFunc::Radians,
            MathUnaryFunc::Sin => MathFunc::Sin,
            MathUnaryFunc::Sinh => MathFunc::Sinh,
            MathUnaryFunc::Sqrt => MathFunc::Sqrt,
            MathUnaryFunc::Tan => MathFunc::Tan,
            MathUnaryFunc::Tanh => MathFunc::Tanh,
            MathUnaryFunc::Trunc => MathFunc::Trunc,
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum MathBinaryFunc {
    Atan2,
    Mod,
    Pow,
    Power,
}

impl From<MathBinaryFunc> for MathFunc {
    fn from(f: MathBinaryFunc) -> MathFunc {
        match f {
            MathBinaryFunc::Atan2 => MathFunc::Atan2,
            MathBinaryFunc::Mod => MathFunc::Mod,
            MathBinaryFunc::Pow => MathFunc::Pow,
            MathBinaryFunc::Power => MathFunc::Power,
        }
    }
}

#[derive(Arbitrary, Debug)]
enum ScalarFuncCall {
    Unary(UnaryFunc, Value),
    UnaryWithOpt(UnaryFuncWithOptionalArg, Value, Option<Value>),
    Binary(BinaryFunc, Value, Value),
    Ternary(TernaryFunc, Value, Value, Value),
    Variadic(VariadicFunc, Vec<Value>),
    Cast(Value, CastType),
    Nullif(Value, Value),
    Like(Value, Value, Value),
    RandomBlob(Value),
    MathUnary(MathUnaryFunc, Value),
    MathBinary(MathBinaryFunc, Value, Value),
    MathLog(Value, Option<Value>),
    MathPi,
    // JSON functions
    JsonArray(Vec<Value>),
    JsonObject(Vec<Value>),
    JsonExtract(Vec<Value>),
    JsonType(Value, Option<Value>),
    JsonValid(Value),
    JsonQuote(Value),
    JsonArrayLength(Value, Option<Value>),
    JsonErrorPosition(Value),
    JsonSet(Vec<Value>),
    JsonInsert(Vec<Value>),
    JsonReplace(Vec<Value>),
    JsonRemove(Vec<Value>),
    JsonPatch(Value, Value),
}

fn execute_scalar_func(call: ScalarFuncCall) {
    match call {
        ScalarFuncCall::Unary(func, val) => {
            let v: CoreValue = val.into();
            match func {
                UnaryFunc::Lower => {
                    let _ = v.exec_lower();
                }
                UnaryFunc::Upper => {
                    let _ = v.exec_upper();
                }
                UnaryFunc::Length => {
                    let _ = v.exec_length();
                }
                UnaryFunc::OctetLength => {
                    let _ = v.exec_octet_length();
                }
                UnaryFunc::Sign => {
                    let _ = v.exec_sign();
                }
                UnaryFunc::Soundex => {
                    let _ = v.exec_soundex();
                }
                UnaryFunc::Abs => {
                    let _ = v.exec_abs();
                }
                UnaryFunc::Quote => {
                    let _ = v.exec_quote();
                }
                UnaryFunc::Typeof => {
                    let _ = v.exec_typeof();
                }
                UnaryFunc::Hex => {
                    let _ = v.exec_hex();
                }
                UnaryFunc::Unicode => {
                    let _ = v.exec_unicode();
                }
                UnaryFunc::ZeroBlob => {
                    let _ = v.exec_zeroblob();
                }
                UnaryFunc::BitNot => {
                    let _ = v.exec_bit_not();
                }
                UnaryFunc::BooleanNot => {
                    let _ = v.exec_boolean_not();
                }
            }
        }
        ScalarFuncCall::UnaryWithOpt(func, val, opt) => {
            let v: CoreValue = val.into();
            let opt_v: Option<CoreValue> = opt.map(|x| x.into());
            match func {
                UnaryFuncWithOptionalArg::Trim => {
                    let _ = v.exec_trim(opt_v.as_ref());
                }
                UnaryFuncWithOptionalArg::LTrim => {
                    let _ = v.exec_ltrim(opt_v.as_ref());
                }
                UnaryFuncWithOptionalArg::RTrim => {
                    let _ = v.exec_rtrim(opt_v.as_ref());
                }
                UnaryFuncWithOptionalArg::Round => {
                    let _ = v.exec_round(opt_v.as_ref());
                }
                UnaryFuncWithOptionalArg::Unhex => {
                    let _ = v.exec_unhex(opt_v.as_ref());
                }
            }
        }
        ScalarFuncCall::Binary(func, v1, v2) => {
            let v1: CoreValue = v1.into();
            let v2: CoreValue = v2.into();
            match func {
                BinaryFunc::Instr => {
                    let _ = v1.exec_instr(&v2);
                }
                BinaryFunc::Add => {
                    let _ = v1.exec_add(&v2);
                }
                BinaryFunc::Subtract => {
                    let _ = v1.exec_subtract(&v2);
                }
                BinaryFunc::Multiply => {
                    let _ = v1.exec_multiply(&v2);
                }
                BinaryFunc::Divide => {
                    let _ = v1.exec_divide(&v2);
                }
                BinaryFunc::BitAnd => {
                    let _ = v1.exec_bit_and(&v2);
                }
                BinaryFunc::BitOr => {
                    let _ = v1.exec_bit_or(&v2);
                }
                BinaryFunc::Remainder => {
                    let _ = v1.exec_remainder(&v2);
                }
                BinaryFunc::ShiftLeft => {
                    let _ = v1.exec_shift_left(&v2);
                }
                BinaryFunc::ShiftRight => {
                    let _ = v1.exec_shift_right(&v2);
                }
                BinaryFunc::Concat => {
                    let _ = v1.exec_concat(&v2);
                }
                BinaryFunc::And => {
                    let _ = v1.exec_and(&v2);
                }
                BinaryFunc::Or => {
                    let _ = v1.exec_or(&v2);
                }
                BinaryFunc::Glob => {
                    let _ = CoreValue::exec_glob(&v1.to_string(), &v2.to_string());
                }
                BinaryFunc::Substring2 => {
                    let _ = CoreValue::exec_substring(&v1, &v2, None);
                }
                BinaryFunc::Timediff => {
                    let _ = exec_timediff([v1, v2]);
                }
            }
        }
        ScalarFuncCall::Ternary(func, v1, v2, v3) => {
            let v1: CoreValue = v1.into();
            let v2: CoreValue = v2.into();
            let v3: CoreValue = v3.into();
            match func {
                TernaryFunc::Substring => {
                    let _ = CoreValue::exec_substring(&v1, &v2, Some(&v3));
                }
                TernaryFunc::Replace => {
                    let _ = CoreValue::exec_replace(&v1, &v2, &v3);
                }
            }
        }
        ScalarFuncCall::Variadic(func, mut vals) => {
            vals.truncate(MAX_VARIADIC_ARGS);
            let vals: Vec<CoreValue> = vals.into_iter().map(|v| v.into()).collect();
            match func {
                VariadicFunc::Min => {
                    let _ = CoreValue::exec_min(vals.iter());
                }
                VariadicFunc::Max => {
                    let _ = CoreValue::exec_max(vals.iter());
                }
                VariadicFunc::ConcatStrings => {
                    let _ = CoreValue::exec_concat_strings(vals.iter());
                }
                VariadicFunc::ConcatWs => {
                    let _ = CoreValue::exec_concat_ws(vals.iter());
                }
                VariadicFunc::Char => {
                    let _ = CoreValue::exec_char(vals.iter());
                }
                VariadicFunc::Printf => {
                    let regs: Vec<Register> = vals.into_iter().map(Register::Value).collect();
                    let _ = exec_printf(&regs);
                }
                VariadicFunc::Date => {
                    let _ = exec_date(vals.iter());
                }
                VariadicFunc::Time => {
                    let _ = exec_time(vals.iter());
                }
                VariadicFunc::DateTime => {
                    let _ = exec_datetime_full(vals.iter());
                }
                VariadicFunc::JulianDay => {
                    let _ = exec_julianday(vals.iter());
                }
                VariadicFunc::UnixEpoch => {
                    let _ = exec_unixepoch(vals.iter());
                }
                VariadicFunc::Strftime => {
                    let _ = exec_strftime(vals.iter());
                }
            }
        }
        ScalarFuncCall::Cast(val, cast_type) => {
            let v: CoreValue = val.into();
            let type_str = match cast_type {
                CastType::Text => "TEXT",
                CastType::Real => "REAL",
                CastType::Integer => "INTEGER",
                CastType::Numeric => "NUMERIC",
                CastType::Blob => "BLOB",
            };
            let _ = v.exec_cast(type_str);
        }
        ScalarFuncCall::Nullif(v1, v2) => {
            let v1: CoreValue = v1.into();
            let v2: CoreValue = v2.into();
            let _ = v1.exec_nullif(&v2);
        }
        ScalarFuncCall::Like(pattern, text, escape) => {
            let pattern: CoreValue = pattern.into();
            let text: CoreValue = text.into();
            let escape_char = CoreValue::from(escape).to_string().chars().next();
            let _ = CoreValue::exec_like(&pattern.to_string(), &text.to_string(), escape_char);
        }
        ScalarFuncCall::RandomBlob(val) => {
            let v: CoreValue = val.into();
            let _ = v.exec_randomblob(|bytes| {
                for b in bytes.iter_mut() {
                    *b = 0x42;
                }
            });
        }
        ScalarFuncCall::MathUnary(func, val) => {
            let v: CoreValue = val.into();
            let math_func: MathFunc = func.into();
            let _ = v.exec_math_unary(&math_func);
        }
        ScalarFuncCall::MathBinary(func, v1, v2) => {
            let v1: CoreValue = v1.into();
            let v2: CoreValue = v2.into();
            let math_func: MathFunc = func.into();
            let _ = v1.exec_math_binary(&v2, &math_func);
        }
        ScalarFuncCall::MathLog(val, base) => {
            let v: CoreValue = val.into();
            let base_v: Option<CoreValue> = base.map(|b| b.into());
            let _ = v.exec_math_log(base_v.as_ref());
        }
        ScalarFuncCall::MathPi => {
            // pi() has no inputs, just returns a constant - nothing to fuzz
            let _ = CoreValue::from_f64(std::f64::consts::PI);
        }
        // JSON functions
        ScalarFuncCall::JsonArray(mut vals) => {
            vals.truncate(MAX_VARIADIC_ARGS);
            let vals: Vec<CoreValue> = vals.into_iter().map(|v| v.into()).collect();
            let _ = json_array(vals.iter());
        }
        ScalarFuncCall::JsonObject(mut vals) => {
            vals.truncate(MAX_VARIADIC_ARGS);
            let vals: Vec<CoreValue> = vals.into_iter().map(|v| v.into()).collect();
            let _ = json_object(vals.iter());
        }
        ScalarFuncCall::JsonExtract(mut vals) => {
            if vals.len() >= 2 {
                vals.truncate(MAX_VARIADIC_ARGS);
                let vals: Vec<CoreValue> = vals.into_iter().map(|v| v.into()).collect();
                let cache = JsonCacheCell::new();
                let (json_val, paths) = vals.split_first().unwrap();
                let _ = json_extract(json_val, paths, &cache);
            }
        }
        ScalarFuncCall::JsonType(val, path) => {
            let v: CoreValue = val.into();
            let path_v: Option<CoreValue> = path.map(|p| p.into());
            let _ = json_type(&v, path_v.as_ref());
        }
        ScalarFuncCall::JsonValid(val) => {
            let v: CoreValue = val.into();
            let _ = is_json_valid(&v);
        }
        ScalarFuncCall::JsonQuote(val) => {
            let v: CoreValue = val.into();
            let _ = json_quote(&v);
        }
        ScalarFuncCall::JsonArrayLength(val, path) => {
            let v: CoreValue = val.into();
            let path_v: Option<CoreValue> = path.map(|p| p.into());
            let cache = JsonCacheCell::new();
            let _ = json_array_length(&v, path_v.as_ref(), &cache);
        }
        ScalarFuncCall::JsonErrorPosition(val) => {
            let v: CoreValue = val.into();
            let _ = json_error_position(&v);
        }
        ScalarFuncCall::JsonSet(mut vals) => {
            vals.truncate(MAX_VARIADIC_ARGS);
            let vals: Vec<CoreValue> = vals.into_iter().map(|v| v.into()).collect();
            let cache = JsonCacheCell::new();
            let _ = json_set(vals.iter(), &cache);
        }
        ScalarFuncCall::JsonInsert(mut vals) => {
            vals.truncate(MAX_VARIADIC_ARGS);
            let vals: Vec<CoreValue> = vals.into_iter().map(|v| v.into()).collect();
            let cache = JsonCacheCell::new();
            let _ = json_insert(vals.iter(), &cache);
        }
        ScalarFuncCall::JsonReplace(mut vals) => {
            vals.truncate(MAX_VARIADIC_ARGS);
            let vals: Vec<CoreValue> = vals.into_iter().map(|v| v.into()).collect();
            let cache = JsonCacheCell::new();
            let _ = json_replace(vals.iter(), &cache);
        }
        ScalarFuncCall::JsonRemove(mut vals) => {
            vals.truncate(MAX_VARIADIC_ARGS);
            let vals: Vec<CoreValue> = vals.into_iter().map(|v| v.into()).collect();
            let cache = JsonCacheCell::new();
            let _ = json_remove(vals.iter(), &cache);
        }
        ScalarFuncCall::JsonPatch(v1, v2) => {
            let v1: CoreValue = v1.into();
            let v2: CoreValue = v2.into();
            let cache = JsonCacheCell::new();
            let _ = json_patch(&v1, &v2, &cache);
        }
    }
}

fuzz_target!(|call: ScalarFuncCall| {
    execute_scalar_func(call);
});
