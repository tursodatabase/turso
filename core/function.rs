use crate::ext::ExtFunc;
use std::fmt;
use std::fmt::Display;
#[cfg(feature = "json")]
#[derive(Debug, Clone, PartialEq)]
pub enum JsonFunc {
    Json,
    JsonArray,
}

#[cfg(feature = "json")]
impl Display for JsonFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Json => "json".to_string(),
                Self::JsonArray => "json_array".to_string(),
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Avg,
    Count,
    GroupConcat,
    Max,
    Min,
    StringAgg,
    Sum,
    Total,
}

impl AggFunc {
    pub fn to_string(&self) -> &str {
        match self {
            Self::Avg => "avg",
            Self::Count => "count",
            Self::GroupConcat => "group_concat",
            Self::Max => "max",
            Self::Min => "min",
            Self::StringAgg => "string_agg",
            Self::Sum => "sum",
            Self::Total => "total",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarFunc {
    Cast,
    Char,
    Coalesce,
    Concat,
    ConcatWs,
    Glob,
    IfNull,
    Iif,
    Instr,
    Like,
    Abs,
    Upper,
    Lower,
    Random,
    RandomBlob,
    Trim,
    LTrim,
    RTrim,
    Round,
    Length,
    OctetLength,
    Min,
    Max,
    Nullif,
    Sign,
    Substr,
    Substring,
    Soundex,
    Date,
    Time,
    Typeof,
    Unicode,
    Quote,
    SqliteVersion,
    UnixEpoch,
    Hex,
    Unhex,
    ZeroBlob,
    LastInsertRowid,
    Replace,
}

impl Display for ScalarFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Cast => "cast".to_string(),
            Self::Char => "char".to_string(),
            Self::Coalesce => "coalesce".to_string(),
            Self::Concat => "concat".to_string(),
            Self::ConcatWs => "concat_ws".to_string(),
            Self::Glob => "glob".to_string(),
            Self::IfNull => "ifnull".to_string(),
            Self::Iif => "iif".to_string(),
            Self::Instr => "instr".to_string(),
            Self::Like => "like(2)".to_string(),
            Self::Abs => "abs".to_string(),
            Self::Upper => "upper".to_string(),
            Self::Lower => "lower".to_string(),
            Self::Random => "random".to_string(),
            Self::RandomBlob => "randomblob".to_string(),
            Self::Trim => "trim".to_string(),
            Self::LTrim => "ltrim".to_string(),
            Self::RTrim => "rtrim".to_string(),
            Self::Round => "round".to_string(),
            Self::Length => "length".to_string(),
            Self::OctetLength => "octet_length".to_string(),
            Self::Min => "min".to_string(),
            Self::Max => "max".to_string(),
            Self::Nullif => "nullif".to_string(),
            Self::Sign => "sign".to_string(),
            Self::Substr => "substr".to_string(),
            Self::Substring => "substring".to_string(),
            Self::Soundex => "soundex".to_string(),
            Self::Date => "date".to_string(),
            Self::Time => "time".to_string(),
            Self::Typeof => "typeof".to_string(),
            Self::Unicode => "unicode".to_string(),
            Self::Quote => "quote".to_string(),
            Self::SqliteVersion => "sqlite_version".to_string(),
            Self::UnixEpoch => "unixepoch".to_string(),
            Self::Hex => "hex".to_string(),
            Self::Unhex => "unhex".to_string(),
            Self::ZeroBlob => "zeroblob".to_string(),
            Self::LastInsertRowid => "last_insert_rowid".to_string(),
            Self::Replace => "replace".to_string(),
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MathFunc {
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atan2,
    Atanh,
    Ceil,
    Ceiling,
    Cos,
    Cosh,
    Degrees,
    Exp,
    Floor,
    Ln,
    Log,
    Log10,
    Log2,
    Mod,
    Pi,
    Pow,
    Power,
    Radians,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,
    Trunc,
}

pub enum MathFuncArity {
    Nullary,
    Unary,
    Binary,
    UnaryOrBinary,
}

impl MathFunc {
    pub fn arity(&self) -> MathFuncArity {
        match self {
            Self::Pi => MathFuncArity::Nullary,
            Self::Acos
            | Self::Acosh
            | Self::Asin
            | Self::Asinh
            | Self::Atan
            | Self::Atanh
            | Self::Ceil
            | Self::Ceiling
            | Self::Cos
            | Self::Cosh
            | Self::Degrees
            | Self::Exp
            | Self::Floor
            | Self::Ln
            | Self::Log10
            | Self::Log2
            | Self::Radians
            | Self::Sin
            | Self::Sinh
            | Self::Sqrt
            | Self::Tan
            | Self::Tanh
            | Self::Trunc => MathFuncArity::Unary,

            Self::Atan2 | Self::Mod | Self::Pow | Self::Power => {
                MathFuncArity::Binary
            }

            Self::Log => MathFuncArity::UnaryOrBinary,
        }
    }
}

impl Display for MathFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Acos => "acos".to_string(),
            Self::Acosh => "acosh".to_string(),
            Self::Asin => "asin".to_string(),
            Self::Asinh => "asinh".to_string(),
            Self::Atan => "atan".to_string(),
            Self::Atan2 => "atan2".to_string(),
            Self::Atanh => "atanh".to_string(),
            Self::Ceil => "ceil".to_string(),
            Self::Ceiling => "ceiling".to_string(),
            Self::Cos => "cos".to_string(),
            Self::Cosh => "cosh".to_string(),
            Self::Degrees => "degrees".to_string(),
            Self::Exp => "exp".to_string(),
            Self::Floor => "floor".to_string(),
            Self::Ln => "ln".to_string(),
            Self::Log => "log".to_string(),
            Self::Log10 => "log10".to_string(),
            Self::Log2 => "log2".to_string(),
            Self::Mod => "mod".to_string(),
            Self::Pi => "pi".to_string(),
            Self::Pow => "pow".to_string(),
            Self::Power => "power".to_string(),
            Self::Radians => "radians".to_string(),
            Self::Sin => "sin".to_string(),
            Self::Sinh => "sinh".to_string(),
            Self::Sqrt => "sqrt".to_string(),
            Self::Tan => "tan".to_string(),
            Self::Tanh => "tanh".to_string(),
            Self::Trunc => "trunc".to_string(),
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Func {
    Agg(AggFunc),
    Scalar(ScalarFunc),
    Math(MathFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
    Extension(ExtFunc),
}

impl Display for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Agg(agg_func) => write!(f, "{}", agg_func.to_string()),
            Self::Scalar(scalar_func) => write!(f, "{}", scalar_func),
            Self::Math(math_func) => write!(f, "{}", math_func),
            #[cfg(feature = "json")]
            Self::Json(json_func) => write!(f, "{}", json_func),
            Self::Extension(ext_func) => write!(f, "{}", ext_func),
        }
    }
}

#[derive(Debug)]
pub struct FuncCtx {
    pub func: Func,
    pub arg_count: usize,
}

impl Func {
    pub fn resolve_function(name: &str, arg_count: usize) -> Result<Func, ()> {
        match name {
            "avg" => Ok(Func::Agg(AggFunc::Avg)),
            "count" => Ok(Func::Agg(AggFunc::Count)),
            "group_concat" => Ok(Func::Agg(AggFunc::GroupConcat)),
            "max" if arg_count == 0 || arg_count == 1 => Ok(Func::Agg(AggFunc::Max)),
            "max" if arg_count > 1 => Ok(Func::Scalar(ScalarFunc::Max)),
            "min" if arg_count == 0 || arg_count == 1 => Ok(Func::Agg(AggFunc::Min)),
            "min" if arg_count > 1 => Ok(Func::Scalar(ScalarFunc::Min)),
            "nullif" if arg_count == 2 => Ok(Func::Scalar(ScalarFunc::Nullif)),
            "string_agg" => Ok(Func::Agg(AggFunc::StringAgg)),
            "sum" => Ok(Func::Agg(AggFunc::Sum)),
            "total" => Ok(Func::Agg(AggFunc::Total)),
            "char" => Ok(Func::Scalar(ScalarFunc::Char)),
            "coalesce" => Ok(Func::Scalar(ScalarFunc::Coalesce)),
            "concat" => Ok(Func::Scalar(ScalarFunc::Concat)),
            "concat_ws" => Ok(Func::Scalar(ScalarFunc::ConcatWs)),
            "glob" => Ok(Func::Scalar(ScalarFunc::Glob)),
            "ifnull" => Ok(Func::Scalar(ScalarFunc::IfNull)),
            "iif" => Ok(Func::Scalar(ScalarFunc::Iif)),
            "instr" => Ok(Func::Scalar(ScalarFunc::Instr)),
            "like" => Ok(Func::Scalar(ScalarFunc::Like)),
            "abs" => Ok(Func::Scalar(ScalarFunc::Abs)),
            "upper" => Ok(Func::Scalar(ScalarFunc::Upper)),
            "lower" => Ok(Func::Scalar(ScalarFunc::Lower)),
            "random" => Ok(Func::Scalar(ScalarFunc::Random)),
            "randomblob" => Ok(Func::Scalar(ScalarFunc::RandomBlob)),
            "trim" => Ok(Func::Scalar(ScalarFunc::Trim)),
            "ltrim" => Ok(Func::Scalar(ScalarFunc::LTrim)),
            "rtrim" => Ok(Func::Scalar(ScalarFunc::RTrim)),
            "round" => Ok(Func::Scalar(ScalarFunc::Round)),
            "length" => Ok(Func::Scalar(ScalarFunc::Length)),
            "octet_length" => Ok(Func::Scalar(ScalarFunc::OctetLength)),
            "sign" => Ok(Func::Scalar(ScalarFunc::Sign)),
            "substr" => Ok(Func::Scalar(ScalarFunc::Substr)),
            "substring" => Ok(Func::Scalar(ScalarFunc::Substring)),
            "date" => Ok(Func::Scalar(ScalarFunc::Date)),
            "time" => Ok(Func::Scalar(ScalarFunc::Time)),
            "typeof" => Ok(Func::Scalar(ScalarFunc::Typeof)),
            "last_insert_rowid" => Ok(Func::Scalar(ScalarFunc::LastInsertRowid)),
            "unicode" => Ok(Func::Scalar(ScalarFunc::Unicode)),
            "quote" => Ok(Func::Scalar(ScalarFunc::Quote)),
            "sqlite_version" => Ok(Func::Scalar(ScalarFunc::SqliteVersion)),
            "replace" => Ok(Func::Scalar(ScalarFunc::Replace)),
            #[cfg(feature = "json")]
            "json" => Ok(Func::Json(JsonFunc::Json)),
            #[cfg(feature = "json")]
            "json_array" => Ok(Func::Json(JsonFunc::JsonArray)),
            "unixepoch" => Ok(Func::Scalar(ScalarFunc::UnixEpoch)),
            "hex" => Ok(Func::Scalar(ScalarFunc::Hex)),
            "unhex" => Ok(Func::Scalar(ScalarFunc::Unhex)),
            "zeroblob" => Ok(Func::Scalar(ScalarFunc::ZeroBlob)),
            "soundex" => Ok(Func::Scalar(ScalarFunc::Soundex)),
            "acos" => Ok(Func::Math(MathFunc::Acos)),
            "acosh" => Ok(Func::Math(MathFunc::Acosh)),
            "asin" => Ok(Func::Math(MathFunc::Asin)),
            "asinh" => Ok(Func::Math(MathFunc::Asinh)),
            "atan" => Ok(Func::Math(MathFunc::Atan)),
            "atan2" => Ok(Func::Math(MathFunc::Atan2)),
            "atanh" => Ok(Func::Math(MathFunc::Atanh)),
            "ceil" => Ok(Func::Math(MathFunc::Ceil)),
            "ceiling" => Ok(Func::Math(MathFunc::Ceiling)),
            "cos" => Ok(Func::Math(MathFunc::Cos)),
            "cosh" => Ok(Func::Math(MathFunc::Cosh)),
            "degrees" => Ok(Func::Math(MathFunc::Degrees)),
            "exp" => Ok(Func::Math(MathFunc::Exp)),
            "floor" => Ok(Func::Math(MathFunc::Floor)),
            "ln" => Ok(Func::Math(MathFunc::Ln)),
            "log" => Ok(Func::Math(MathFunc::Log)),
            "log10" => Ok(Func::Math(MathFunc::Log10)),
            "log2" => Ok(Func::Math(MathFunc::Log2)),
            "mod" => Ok(Func::Math(MathFunc::Mod)),
            "pi" => Ok(Func::Math(MathFunc::Pi)),
            "pow" => Ok(Func::Math(MathFunc::Pow)),
            "power" => Ok(Func::Math(MathFunc::Power)),
            "radians" => Ok(Func::Math(MathFunc::Radians)),
            "sin" => Ok(Func::Math(MathFunc::Sin)),
            "sinh" => Ok(Func::Math(MathFunc::Sinh)),
            "sqrt" => Ok(Func::Math(MathFunc::Sqrt)),
            "tan" => Ok(Func::Math(MathFunc::Tan)),
            "tanh" => Ok(Func::Math(MathFunc::Tanh)),
            "trunc" => Ok(Func::Math(MathFunc::Trunc)),
            _ => match ExtFunc::resolve_function(name, arg_count) {
                Some(ext_func) => Ok(Func::Extension(ext_func)),
                None => Err(()),
            },
        }
    }
}
