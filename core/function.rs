use strum::Display;

#[cfg(feature = "json")]
#[derive(Debug, Clone, PartialEq, Display)]
pub enum JsonFunc {
    #[strum(to_string = "json")]
    Json,
}

#[derive(Debug, Clone, PartialEq, Display)]
#[strum(serialize_all="snake_case")]
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


#[derive(Debug, Clone, PartialEq)]
#[derive(strum_macros::Display)]
#[strum(serialize_all="snake_case")]
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

#[derive(Debug, Display)]
pub enum Func {
    #[strum(to_string="{0}")]
    Agg(AggFunc),
    #[strum(to_string="{0}")]
    Scalar(ScalarFunc),
    #[cfg(feature = "json")]
    #[strum(to_string="{0}")]
    Json(JsonFunc),
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
            "unixepoch" => Ok(Func::Scalar(ScalarFunc::UnixEpoch)),
            "hex" => Ok(Func::Scalar(ScalarFunc::Hex)),
            "unhex" => Ok(Func::Scalar(ScalarFunc::Unhex)),
            "zeroblob" => Ok(Func::Scalar(ScalarFunc::ZeroBlob)),
            "soundex" => Ok(Func::Scalar(ScalarFunc::Soundex)),
            _ => Err(()),
        }
    }
}
