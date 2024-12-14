use strum::{Display, EnumString};

#[cfg(feature = "json")]
#[derive(Debug, Clone, PartialEq, Display, EnumString)]
pub enum JsonFunc {
    #[strum(to_string = "json")]
    Json,
}

#[derive(Debug, Clone, PartialEq, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
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

#[derive(Debug, Clone, PartialEq, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum ScalarFunc {
    Cast,
    Char,
    Coalesce,
    Concat,
    ConcatWs,
    Glob,
    Ifnull,
    Iif,
    Instr,
    #[strum(to_string = "like(2)", serialize = "like")]
    Like,
    Abs,
    Upper,
    Lower,
    Random,
    Randomblob,
    Trim,
    Ltrim,
    Rtrim,
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
    Unixepoch,
    Hex,
    Unhex,
    Zeroblob,
    LastInsertRowid,
    Replace,
}

#[derive(Debug, Display)]
pub enum Func {
    #[strum(to_string = "{0}")]
    Agg(AggFunc),
    #[strum(to_string = "{0}")]
    Scalar(ScalarFunc),
    #[cfg(feature = "json")]
    #[strum(to_string = "{0}")]
    Json(JsonFunc),
}

#[derive(Debug)]
pub struct FuncCtx {
    pub func: Func,
    pub arg_count: usize,
}

impl Func {
    //taking help of strum EnumString to parse string to Enum
    pub fn resolve_function(name: &str, arg_count: usize) -> Result<Func, ()> {
        //try to parse the function name to be of AggFunc
        if let Ok(agg_func) = name.parse::<AggFunc>() {
            match agg_func {
                AggFunc::Max | AggFunc::Min if arg_count > 1 => {
                    // Handle min and max functions with respect to argument counter.
                    return Ok(Func::Scalar(match agg_func {
                        AggFunc::Max => ScalarFunc::Max,
                        AggFunc::Min => ScalarFunc::Min,
                        _ => unreachable!(),
                    }));
                }
                _ => return Ok(Func::Agg(agg_func)),
            }
        }

        //try to parse the function name to be of ScalarFunc
        if let Ok(scalar_func) = name.parse::<ScalarFunc>() {
            match scalar_func {
                //handling nullif condition
                ScalarFunc::Nullif if arg_count != 2 => return Err(()), // Argument count validation.
                _ => return Ok(Func::Scalar(scalar_func)),
            }
        }

        #[cfg(feature = "json")]
        if let Ok(json_func) = name.parse::<JsonFunc>() {
            return Ok(Func::Json(json_func));
        }

        //return Err if the string is not handled in any of the above if let blocks

        Err(())
    }
}
