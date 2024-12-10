use std::fmt;
use std::fmt::Display;

#[cfg(feature = "json")]
#[derive(Debug, Clone, PartialEq)]
pub enum JsonFunc {
    Json,
}

#[cfg(feature = "json")]
impl Display for JsonFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Json => "json".to_string(),
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
    Min,
    Max,
    Nullif,
    Sign,
    Substr,
    Substring,
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
}

impl Display for ScalarFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Cast => "cast",
            Self::Char => "char.",
            Self::Coalesce => "coalesce",
            Self::Concat => "concat",
            Self::ConcatWs => "concat_ws",
            Self::Glob => "glob",
            Self::IfNull => "ifnull",
            Self::Instr => "instr",
            Self::Like => "like(2)",
            Self::Abs => "abs",
            Self::Upper => "upper",
            Self::Lower => "lower",
            Self::Random => "random",
            Self::RandomBlob => "randomblob",
            Self::Trim => "trim",
            Self::LTrim => "ltrim",
            Self::RTrim => "rtrim",
            Self::Round => "round",
            Self::Length => "length",
            Self::Min => "min",
            Self::Max => "max",
            Self::Nullif => "nullif",
            Self::Sign => "sign",
            Self::Substr => "substr",
            Self::Substring => "substring",
            Self::Date => "date",
            Self::Time => "time",
            Self::Typeof => "typeof",
            Self::Unicode => "unicode",
            Self::Quote => "quote",
            Self::SqliteVersion => "sqlite_version",
            Self::UnixEpoch => "unixepoch",
            Self::Hex => "hex",
            Self::Unhex => "unhex",
            Self::ZeroBlob => "zeroblob",
        };
        write!(f, "{}", str.to_string())
    }
}

#[derive(Debug)]
pub enum Func {
    Agg(AggFunc),
    Scalar(ScalarFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
}

impl Display for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Agg(agg_func) => write!(f, "{}", agg_func.to_string()),
            Self::Scalar(scalar_func) => write!(f, "{}", scalar_func),
            #[cfg(feature = "json")]
            Self::Json(json_func) => write!(f, "{}", json_func),
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
            "sign" => Ok(Func::Scalar(ScalarFunc::Sign)),
            "substr" => Ok(Func::Scalar(ScalarFunc::Substr)),
            "substring" => Ok(Func::Scalar(ScalarFunc::Substring)),
            "date" => Ok(Func::Scalar(ScalarFunc::Date)),
            "time" => Ok(Func::Scalar(ScalarFunc::Time)),
            "typeof" => Ok(Func::Scalar(ScalarFunc::Typeof)),
            "unicode" => Ok(Func::Scalar(ScalarFunc::Unicode)),
            "quote" => Ok(Func::Scalar(ScalarFunc::Quote)),
            "sqlite_version" => Ok(Func::Scalar(ScalarFunc::SqliteVersion)),
            #[cfg(feature = "json")]
            "json" => Ok(Func::Json(JsonFunc::Json)),
            "unixepoch" => Ok(Func::Scalar(ScalarFunc::UnixEpoch)),
            "hex" => Ok(Func::Scalar(ScalarFunc::Hex)),
            "unhex" => Ok(Func::Scalar(ScalarFunc::Unhex)),
            "zeroblob" => Ok(Func::Scalar(ScalarFunc::ZeroBlob)),
            _ => Err(()),
        }
    }
}
