use crate::sync::Arc;
use std::fmt;
use std::fmt::{Debug, Display};
use strum::IntoEnumIterator;
use turso_ext::{FinalizeFunction, InitAggFunction, ScalarFunction, StepFunction};

use crate::LimboError;

pub trait Deterministic: std::fmt::Display {
    fn is_deterministic(&self) -> bool;
}

pub struct ExternalFunc {
    pub name: String,
    pub func: ExtFunc,
}

impl Deterministic for ExternalFunc {
    fn is_deterministic(&self) -> bool {
        // external functions can be whatever so let's just default to false
        false
    }
}

#[derive(Debug, Clone)]
pub enum ExtFunc {
    Scalar(ScalarFunction),
    Aggregate {
        argc: usize,
        init: InitAggFunction,
        step: StepFunction,
        finalize: FinalizeFunction,
    },
}

impl ExtFunc {
    pub fn agg_args(&self) -> Result<usize, ()> {
        if let ExtFunc::Aggregate { argc, .. } = self {
            return Ok(*argc);
        }
        Err(())
    }
}

impl ExternalFunc {
    pub fn new_scalar(name: String, func: ScalarFunction) -> Self {
        Self {
            name,
            func: ExtFunc::Scalar(func),
        }
    }

    pub fn new_aggregate(
        name: String,
        argc: i32,
        func: (InitAggFunction, StepFunction, FinalizeFunction),
    ) -> Self {
        Self {
            name,
            func: ExtFunc::Aggregate {
                argc: argc as usize,
                init: func.0,
                step: func.1,
                finalize: func.2,
            },
        }
    }
}

impl Debug for ExternalFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Display for ExternalFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(feature = "json")]
#[derive(Debug, Clone, PartialEq, strum::EnumIter)]
pub enum JsonFunc {
    Json,
    Jsonb,
    JsonArray,
    JsonbArray,
    JsonArrayLength,
    JsonArrowExtract,
    JsonArrowShiftExtract,
    JsonExtract,
    JsonbExtract,
    JsonObject,
    JsonbObject,
    JsonType,
    JsonErrorPosition,
    JsonValid,
    JsonPatch,
    JsonbPatch,
    JsonRemove,
    JsonbRemove,
    JsonReplace,
    JsonbReplace,
    JsonInsert,
    JsonbInsert,
    JsonPretty,
    JsonSet,
    JsonbSet,
    JsonQuote,
}

#[cfg(feature = "json")]
impl Deterministic for JsonFunc {
    fn is_deterministic(&self) -> bool {
        true
    }
}

#[cfg(feature = "json")]
impl Display for JsonFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Json => "json",
                Self::Jsonb => "jsonb",
                Self::JsonArray => "json_array",
                Self::JsonbArray => "jsonb_array",
                Self::JsonExtract => "json_extract",
                Self::JsonbExtract => "jsonb_extract",
                Self::JsonArrayLength => "json_array_length",
                Self::JsonArrowExtract => "->",
                Self::JsonArrowShiftExtract => "->>",
                Self::JsonObject => "json_object",
                Self::JsonbObject => "jsonb_object",
                Self::JsonType => "json_type",
                Self::JsonErrorPosition => "json_error_position",
                Self::JsonValid => "json_valid",
                Self::JsonPatch => "json_patch",
                Self::JsonbPatch => "jsonb_patch",
                Self::JsonRemove => "json_remove",
                Self::JsonbRemove => "jsonb_remove",
                Self::JsonReplace => "json_replace",
                Self::JsonbReplace => "jsonb_replace",
                Self::JsonInsert => "json_insert",
                Self::JsonbInsert => "jsonb_insert",
                Self::JsonPretty => "json_pretty",
                Self::JsonSet => "json_set",
                Self::JsonbSet => "jsonb_set",
                Self::JsonQuote => "json_quote",
            }
        )
    }
}

#[cfg(feature = "json")]
impl JsonFunc {
    /// Returns true for operator-style entries that should not appear in PRAGMA function_list.
    pub fn is_internal(&self) -> bool {
        matches!(self, Self::JsonArrowExtract | Self::JsonArrowShiftExtract)
    }

    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::Json
            | Self::Jsonb
            | Self::JsonQuote
            | Self::JsonErrorPosition
            | Self::JsonValid => &[1],
            Self::JsonPatch | Self::JsonbPatch => &[2],
            Self::JsonArrayLength | Self::JsonType => &[1, 2],
            // Operators — filtered out, arity doesn't matter
            Self::JsonArrowExtract | Self::JsonArrowShiftExtract => &[2],
            // Variable-arg
            _ => &[-1],
        }
    }
}

#[derive(Debug, Clone, strum::EnumIter)]
pub enum VectorFunc {
    Vector,
    Vector32,
    Vector32Sparse,
    Vector64,
    Vector8,
    Vector1Bit,
    VectorExtract,
    VectorDistanceCos,
    VectorDistanceL2,
    VectorDistanceJaccard,
    VectorDistanceDot,
    VectorConcat,
    VectorSlice,
}

impl Deterministic for VectorFunc {
    fn is_deterministic(&self) -> bool {
        true
    }
}

impl Display for VectorFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Vector => "vector",
            Self::Vector32 => "vector32",
            Self::Vector32Sparse => "vector32_sparse",
            Self::Vector64 => "vector64",
            Self::Vector8 => "vector8",
            Self::Vector1Bit => "vector1bit",
            Self::VectorExtract => "vector_extract",
            Self::VectorDistanceCos => "vector_distance_cos",
            Self::VectorDistanceL2 => "vector_distance_l2",
            Self::VectorDistanceJaccard => "vector_distance_jaccard",
            Self::VectorDistanceDot => "vector_distance_dot",
            Self::VectorConcat => "vector_concat",
            Self::VectorSlice => "vector_slice",
        };
        write!(f, "{str}")
    }
}

impl VectorFunc {
    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::Vector
            | Self::Vector32
            | Self::Vector32Sparse
            | Self::Vector64
            | Self::Vector8
            | Self::Vector1Bit
            | Self::VectorExtract => &[1],
            Self::VectorDistanceCos
            | Self::VectorDistanceL2
            | Self::VectorDistanceJaccard
            | Self::VectorDistanceDot => &[2],
            Self::VectorSlice => &[3],
            Self::VectorConcat => &[-1],
        }
    }
}

/// Full-text search functions
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[derive(Debug, Clone, PartialEq, strum::EnumIter)]
pub enum FtsFunc {
    /// fts_score(col1, col2, ..., query): computes FTS relevance score
    /// When used with an FTS index, the optimizer routes through the index method
    Score,
    /// fts_match(col1, col2, ..., query): returns true if document matches query
    /// Used in WHERE clause for filtering rows by FTS match
    Match,
    /// fts_highlight(text, query, before_tag, after_tag): returns text with matching terms highlighted
    /// Wraps matching query terms in the text with before_tag and after_tag markers
    Highlight,
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
impl FtsFunc {
    pub fn is_deterministic(&self) -> bool {
        true
    }

    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::Highlight => &[4],
            // Score and Match take variable columns + query
            Self::Score | Self::Match => &[-1],
        }
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
impl Display for FtsFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Score => "fts_score",
            Self::Match => "fts_match",
            Self::Highlight => "fts_highlight",
        };
        write!(f, "{str}")
    }
}

#[derive(Debug, Clone, strum::EnumIter)]
pub enum AggFunc {
    Avg,
    Count,
    Count0,
    GroupConcat,
    Max,
    Min,
    StringAgg,
    Sum,
    Total,
    #[cfg(feature = "json")]
    JsonbGroupArray,
    #[cfg(feature = "json")]
    JsonGroupArray,
    #[cfg(feature = "json")]
    JsonbGroupObject,
    #[cfg(feature = "json")]
    JsonGroupObject,
    #[strum(disabled)]
    External(Arc<ExtFunc>),
}

impl PartialEq for AggFunc {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Avg, Self::Avg)
            | (Self::Count, Self::Count)
            | (Self::GroupConcat, Self::GroupConcat)
            | (Self::Max, Self::Max)
            | (Self::Min, Self::Min)
            | (Self::StringAgg, Self::StringAgg)
            | (Self::Sum, Self::Sum)
            | (Self::Total, Self::Total) => true,
            (Self::External(a), Self::External(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Deterministic for AggFunc {
    fn is_deterministic(&self) -> bool {
        false // consider aggregate functions nondeterministic since they depend on the number of rows, not only the input arguments
    }
}
impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl AggFunc {
    pub fn num_args(&self) -> usize {
        match self {
            Self::Avg => 1,
            Self::Count0 => 0,
            Self::Count => 1,
            Self::GroupConcat => 1,
            Self::Max => 1,
            Self::Min => 1,
            Self::StringAgg => 2,
            Self::Sum => 1,
            Self::Total => 1,
            #[cfg(feature = "json")]
            Self::JsonGroupArray | Self::JsonbGroupArray => 1,
            #[cfg(feature = "json")]
            Self::JsonGroupObject | Self::JsonbGroupObject => 2,
            Self::External(func) => func.agg_args().unwrap_or(0),
        }
    }

    /// Returns all valid arities for this aggregate function.
    /// Most aggregates have a single arity, but group_concat accepts 1 or 2 args.
    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::Avg => &[1],
            Self::Count0 => &[0],
            Self::Count => &[1],
            Self::GroupConcat => &[1, 2],
            Self::Max => &[1],
            Self::Min => &[1],
            Self::StringAgg => &[2],
            Self::Sum => &[1],
            Self::Total => &[1],
            #[cfg(feature = "json")]
            Self::JsonGroupArray | Self::JsonbGroupArray => &[1],
            #[cfg(feature = "json")]
            Self::JsonGroupObject | Self::JsonbGroupObject => &[2],
            Self::External(_) => &[-1],
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Count0 => "count",
            Self::Count => "count",
            Self::GroupConcat => "group_concat",
            Self::Max => "max",
            Self::Min => "min",
            Self::StringAgg => "string_agg",
            Self::Sum => "sum",
            Self::Total => "total",
            #[cfg(feature = "json")]
            Self::JsonbGroupArray => "jsonb_group_array",
            #[cfg(feature = "json")]
            Self::JsonGroupArray => "json_group_array",
            #[cfg(feature = "json")]
            Self::JsonbGroupObject => "jsonb_group_object",
            #[cfg(feature = "json")]
            Self::JsonGroupObject => "json_group_object",
            Self::External(_) => "extension function",
        }
    }
}

#[derive(Debug, Clone, PartialEq, strum::EnumIter)]
pub enum ScalarFunc {
    Cast,
    Changes,
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
    TotalChanges,
    DateTime,
    Typeof,
    Unicode,
    Quote,
    SqliteVersion,
    TursoVersion,
    SqliteSourceId,
    UnixEpoch,
    JulianDay,
    Hex,
    Unhex,
    ZeroBlob,
    LastInsertRowid,
    Replace,
    #[cfg(feature = "fs")]
    #[cfg(not(target_family = "wasm"))]
    LoadExtension,
    StrfTime,
    Printf,
    Likely,
    TimeDiff,
    Likelihood,
    TableColumnsJsonArray,
    BinRecordJsonObject,
    Attach,
    Detach,
    Unlikely,
    StatInit,
    StatPush,
    StatGet,
    ConnTxnId,
    IsAutocommit,
}

impl Deterministic for ScalarFunc {
    fn is_deterministic(&self) -> bool {
        match self {
            ScalarFunc::Cast => true,
            ScalarFunc::Changes => false, // depends on DB state
            ScalarFunc::Char => true,
            ScalarFunc::Coalesce => true,
            ScalarFunc::Concat => true,
            ScalarFunc::ConcatWs => true,
            ScalarFunc::Glob => true,
            ScalarFunc::IfNull => true,
            ScalarFunc::Iif => true,
            ScalarFunc::Instr => true,
            ScalarFunc::Like => true,
            ScalarFunc::Abs => true,
            ScalarFunc::Upper => true,
            ScalarFunc::Lower => true,
            ScalarFunc::Random => false,     // duh
            ScalarFunc::RandomBlob => false, // duh
            ScalarFunc::Trim => true,
            ScalarFunc::LTrim => true,
            ScalarFunc::RTrim => true,
            ScalarFunc::Round => true,
            ScalarFunc::Length => true,
            ScalarFunc::OctetLength => true,
            ScalarFunc::Min => true,
            ScalarFunc::Max => true,
            ScalarFunc::Nullif => true,
            ScalarFunc::Sign => true,
            ScalarFunc::Substr => true,
            ScalarFunc::Substring => true,
            ScalarFunc::Soundex => true,
            ScalarFunc::Date => false,
            ScalarFunc::Time => false,
            ScalarFunc::TotalChanges => false,
            ScalarFunc::DateTime => false,
            ScalarFunc::Typeof => true,
            ScalarFunc::Unicode => true,
            ScalarFunc::Quote => true,
            ScalarFunc::SqliteVersion => true,
            ScalarFunc::TursoVersion => true,
            ScalarFunc::SqliteSourceId => true,
            ScalarFunc::UnixEpoch => false,
            ScalarFunc::JulianDay => false,
            ScalarFunc::Hex => true,
            ScalarFunc::Unhex => true,
            ScalarFunc::ZeroBlob => true,
            ScalarFunc::LastInsertRowid => false,
            ScalarFunc::Replace => true,
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            ScalarFunc::LoadExtension => false,
            ScalarFunc::StrfTime => false,
            ScalarFunc::Printf => true,
            ScalarFunc::Likely => true,
            ScalarFunc::TimeDiff => false,
            ScalarFunc::Likelihood => true,
            ScalarFunc::TableColumnsJsonArray => true, // while columns of the table can change with DDL statements, within single query plan it's static
            ScalarFunc::BinRecordJsonObject => true,
            ScalarFunc::Attach => false, // changes database state
            ScalarFunc::Detach => false, // changes database state
            ScalarFunc::Unlikely => true,
            ScalarFunc::StatInit => false, // internal ANALYZE function
            ScalarFunc::StatPush => false, // internal ANALYZE function
            ScalarFunc::StatGet => false,  // internal ANALYZE function
            ScalarFunc::ConnTxnId => false, // depends on connection state
            ScalarFunc::IsAutocommit => false, // depends on connection state
        }
    }
}

impl Display for ScalarFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Cast => "cast",
            Self::Changes => "changes",
            Self::Char => "char",
            Self::Coalesce => "coalesce",
            Self::Concat => "concat",
            Self::ConcatWs => "concat_ws",
            Self::Glob => "glob",
            Self::IfNull => "ifnull",
            Self::Iif => "iif",
            Self::Instr => "instr",
            Self::Like => "like",
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
            Self::OctetLength => "octet_length",
            Self::Min => "min",
            Self::Max => "max",
            Self::Nullif => "nullif",
            Self::Sign => "sign",
            Self::Substr => "substr",
            Self::Substring => "substring",
            Self::Soundex => "soundex",
            Self::Date => "date",
            Self::Time => "time",
            Self::TotalChanges => "total_changes",
            Self::Typeof => "typeof",
            Self::Unicode => "unicode",
            Self::Quote => "quote",
            Self::SqliteVersion => "sqlite_version",
            Self::TursoVersion => "turso_version",
            Self::SqliteSourceId => "sqlite_source_id",
            Self::JulianDay => "julianday",
            Self::UnixEpoch => "unixepoch",
            Self::Hex => "hex",
            Self::Unhex => "unhex",
            Self::ZeroBlob => "zeroblob",
            Self::LastInsertRowid => "last_insert_rowid",
            Self::Replace => "replace",
            Self::DateTime => "datetime",
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            Self::LoadExtension => "load_extension",
            Self::StrfTime => "strftime",
            Self::Printf => "printf",
            Self::Likely => "likely",
            Self::TimeDiff => "timediff",
            Self::Likelihood => "likelihood",
            Self::TableColumnsJsonArray => "table_columns_json_array",
            Self::BinRecordJsonObject => "bin_record_json_object",
            Self::Attach => "attach",
            Self::Detach => "detach",
            Self::Unlikely => "unlikely",
            Self::StatInit => "stat_init",
            Self::StatPush => "stat_push",
            Self::StatGet => "stat_get",
            Self::ConnTxnId => "conn_txn_id",
            Self::IsAutocommit => "is_autocommit",
        };
        write!(f, "{str}")
    }
}

impl ScalarFunc {
    /// Returns true for internal functions that should not appear in PRAGMA function_list.
    pub fn is_internal(&self) -> bool {
        matches!(
            self,
            Self::Cast
                | Self::StatInit
                | Self::StatPush
                | Self::StatGet
                | Self::Attach
                | Self::Detach
                | Self::TableColumnsJsonArray
                | Self::BinRecordJsonObject
                | Self::ConnTxnId
                | Self::IsAutocommit
        )
    }

    /// Returns the valid arities for this function.
    /// Each value becomes a separate row in PRAGMA function_list.
    /// -1 means truly variable arguments (e.g. coalesce, printf).
    pub fn arities(&self) -> &'static [i32] {
        match self {
            // 0-arg
            Self::Changes
            | Self::LastInsertRowid
            | Self::Random
            | Self::SqliteVersion
            | Self::TursoVersion
            | Self::SqliteSourceId
            | Self::TotalChanges => &[0],
            // 1-arg
            Self::Abs
            | Self::Hex
            | Self::Length
            | Self::Lower
            | Self::OctetLength
            | Self::Quote
            | Self::RandomBlob
            | Self::Sign
            | Self::Soundex
            | Self::Typeof
            | Self::Unicode
            | Self::Upper
            | Self::ZeroBlob
            | Self::Likely
            | Self::Unlikely => &[1],
            // 2-arg
            Self::Glob
            | Self::Instr
            | Self::Nullif
            | Self::IfNull
            | Self::Likelihood
            | Self::TimeDiff => &[2],
            // 3-arg
            Self::Iif | Self::Replace => &[3],
            // Multi-arity (one row per valid arity)
            Self::Like => &[2, 3],
            Self::Trim | Self::LTrim | Self::RTrim | Self::Round | Self::Unhex => &[1, 2],
            Self::Substr | Self::Substring => &[2, 3],
            // Truly variable-arg
            Self::Char
            | Self::Coalesce
            | Self::Concat
            | Self::ConcatWs
            | Self::Date
            | Self::Time
            | Self::DateTime
            | Self::UnixEpoch
            | Self::JulianDay
            | Self::StrfTime
            | Self::Printf => &[-1],
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            Self::LoadExtension => &[-1],
            // Internal functions — arity doesn't matter since they're filtered out
            Self::Cast
            | Self::StatInit
            | Self::StatPush
            | Self::StatGet
            | Self::Attach
            | Self::Detach
            | Self::TableColumnsJsonArray
            | Self::BinRecordJsonObject
            | Self::ConnTxnId
            | Self::IsAutocommit => &[0],
            // Scalar max/min (multi-arg)
            Self::Max | Self::Min => &[-1],
        }
    }
}

#[derive(Debug, Clone, PartialEq, strum::EnumIter)]
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

impl Deterministic for MathFunc {
    fn is_deterministic(&self) -> bool {
        true
    }
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

            Self::Atan2 | Self::Mod | Self::Pow | Self::Power => MathFuncArity::Binary,

            Self::Log => MathFuncArity::UnaryOrBinary,
        }
    }

    pub fn arities(&self) -> &'static [i32] {
        match self.arity() {
            MathFuncArity::Nullary => &[0],
            MathFuncArity::Unary => &[1],
            MathFuncArity::Binary => &[2],
            MathFuncArity::UnaryOrBinary => &[1, 2],
        }
    }
}

impl Display for MathFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Acos => "acos",
            Self::Acosh => "acosh",
            Self::Asin => "asin",
            Self::Asinh => "asinh",
            Self::Atan => "atan",
            Self::Atan2 => "atan2",
            Self::Atanh => "atanh",
            Self::Ceil => "ceil",
            Self::Ceiling => "ceiling",
            Self::Cos => "cos",
            Self::Cosh => "cosh",
            Self::Degrees => "degrees",
            Self::Exp => "exp",
            Self::Floor => "floor",
            Self::Ln => "ln",
            Self::Log => "log",
            Self::Log10 => "log10",
            Self::Log2 => "log2",
            Self::Mod => "mod",
            Self::Pi => "pi",
            Self::Pow => "pow",
            Self::Power => "power",
            Self::Radians => "radians",
            Self::Sin => "sin",
            Self::Sinh => "sinh",
            Self::Sqrt => "sqrt",
            Self::Tan => "tan",
            Self::Tanh => "tanh",
            Self::Trunc => "trunc",
        };
        write!(f, "{str}")
    }
}

#[derive(Debug, Clone)]
pub enum AlterTableFunc {
    RenameTable,
    AlterColumn,
    RenameColumn,
}

impl Display for AlterTableFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterTableFunc::RenameTable => write!(f, "limbo_rename_table"),
            AlterTableFunc::RenameColumn => write!(f, "limbo_rename_column"),
            AlterTableFunc::AlterColumn => write!(f, "limbo_alter_column"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Func {
    Agg(AggFunc),
    Scalar(ScalarFunc),
    Math(MathFunc),
    Vector(VectorFunc),
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    Fts(FtsFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
    AlterTable(AlterTableFunc),
    External(Arc<ExternalFunc>),
}

impl Display for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Agg(agg_func) => write!(f, "{}", agg_func.as_str()),
            Self::Scalar(scalar_func) => write!(f, "{scalar_func}"),
            Self::Math(math_func) => write!(f, "{math_func}"),
            Self::Vector(vector_func) => write!(f, "{vector_func}"),
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            Self::Fts(fts_func) => write!(f, "{fts_func}"),
            #[cfg(feature = "json")]
            Self::Json(json_func) => write!(f, "{json_func}"),
            Self::External(generic_func) => write!(f, "{generic_func}"),
            Self::AlterTable(alter_func) => write!(f, "{alter_func}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FuncCtx {
    pub func: Func,
    pub arg_count: usize,
}

impl Deterministic for Func {
    fn is_deterministic(&self) -> bool {
        match self {
            Self::Agg(agg_func) => agg_func.is_deterministic(),
            Self::Scalar(scalar_func) => scalar_func.is_deterministic(),
            Self::Math(math_func) => math_func.is_deterministic(),
            Self::Vector(vector_func) => vector_func.is_deterministic(),
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            Self::Fts(fts_func) => fts_func.is_deterministic(),
            #[cfg(feature = "json")]
            Self::Json(json_func) => json_func.is_deterministic(),
            Self::External(external_func) => external_func.is_deterministic(),
            Self::AlterTable(_) => true,
        }
    }
}

impl Func {
    pub fn supports_star_syntax(&self) -> bool {
        // Functions that need star expansion also support star syntax
        if self.needs_star_expansion() {
            return true;
        }
        match self {
            Self::Scalar(scalar_func) => {
                matches!(
                    scalar_func,
                    ScalarFunc::Changes
                        | ScalarFunc::Random
                        | ScalarFunc::TotalChanges
                        | ScalarFunc::SqliteVersion
                        | ScalarFunc::TursoVersion
                        | ScalarFunc::SqliteSourceId
                        | ScalarFunc::LastInsertRowid
                )
            }
            Self::Math(math_func) => {
                matches!(math_func.arity(), MathFuncArity::Nullary)
            }
            // Aggregate functions with (*) syntax are handled separately in the planner
            Self::Agg(_) => false,
            _ => false,
        }
    }

    /// Returns true if the function needs the `*` to be expanded to all columns
    /// from the referenced tables. This is used for functions like `json_object(*)`
    /// and `jsonb_object(*)` which create a JSON object with column names as keys
    /// and column values as values.
    #[cfg(feature = "json")]
    pub fn needs_star_expansion(&self) -> bool {
        matches!(
            self,
            Self::Json(JsonFunc::JsonObject) | Self::Json(JsonFunc::JsonbObject)
        )
    }

    #[cfg(not(feature = "json"))]
    pub fn needs_star_expansion(&self) -> bool {
        false
    }
    pub fn resolve_function(name: &str, arg_count: usize) -> Result<Self, LimboError> {
        let normalized_name = crate::util::normalize_ident(name);
        match normalized_name.as_str() {
            "avg" => {
                if arg_count != 1 {
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
                Ok(Self::Agg(AggFunc::Avg))
            }
            "count" => {
                // Handle both COUNT() and COUNT(expr) cases
                if arg_count == 0 {
                    Ok(Self::Agg(AggFunc::Count0)) // COUNT() case
                } else if arg_count == 1 {
                    Ok(Self::Agg(AggFunc::Count)) // COUNT(expr) case
                } else {
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
            }
            "group_concat" => {
                if arg_count != 1 && arg_count != 2 {
                    println!("{arg_count}");
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
                Ok(Self::Agg(AggFunc::GroupConcat))
            }
            "max" if arg_count > 1 => Ok(Self::Scalar(ScalarFunc::Max)),
            "max" => {
                if arg_count < 1 {
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
                Ok(Self::Agg(AggFunc::Max))
            }
            "min" if arg_count > 1 => Ok(Self::Scalar(ScalarFunc::Min)),
            "min" => {
                if arg_count < 1 {
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
                Ok(Self::Agg(AggFunc::Min))
            }
            "nullif" if arg_count == 2 => Ok(Self::Scalar(ScalarFunc::Nullif)),
            "string_agg" => {
                if arg_count != 2 {
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
                Ok(Self::Agg(AggFunc::StringAgg))
            }
            "sum" => {
                if arg_count != 1 {
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
                Ok(Self::Agg(AggFunc::Sum))
            }
            "total" => {
                if arg_count != 1 {
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
                Ok(Self::Agg(AggFunc::Total))
            }
            "timediff" => {
                if arg_count != 2 {
                    crate::bail_parse_error!("wrong number of arguments to function {}()", name)
                }
                Ok(Self::Scalar(ScalarFunc::TimeDiff))
            }
            #[cfg(feature = "json")]
            "jsonb_group_array" => Ok(Self::Agg(AggFunc::JsonbGroupArray)),
            #[cfg(feature = "json")]
            "json_group_array" => Ok(Self::Agg(AggFunc::JsonGroupArray)),
            #[cfg(feature = "json")]
            "jsonb_group_object" => Ok(Self::Agg(AggFunc::JsonbGroupObject)),
            #[cfg(feature = "json")]
            "json_group_object" => Ok(Self::Agg(AggFunc::JsonGroupObject)),
            "char" => Ok(Self::Scalar(ScalarFunc::Char)),
            "coalesce" => Ok(Self::Scalar(ScalarFunc::Coalesce)),
            "concat" => Ok(Self::Scalar(ScalarFunc::Concat)),
            "concat_ws" => Ok(Self::Scalar(ScalarFunc::ConcatWs)),
            "changes" => Ok(Self::Scalar(ScalarFunc::Changes)),
            "total_changes" => Ok(Self::Scalar(ScalarFunc::TotalChanges)),
            "glob" => Ok(Self::Scalar(ScalarFunc::Glob)),
            "ifnull" => Ok(Self::Scalar(ScalarFunc::IfNull)),
            "if" | "iif" => Ok(Self::Scalar(ScalarFunc::Iif)),
            "instr" => Ok(Self::Scalar(ScalarFunc::Instr)),
            "like" => Ok(Self::Scalar(ScalarFunc::Like)),
            "abs" => Ok(Self::Scalar(ScalarFunc::Abs)),
            "upper" => Ok(Self::Scalar(ScalarFunc::Upper)),
            "lower" => Ok(Self::Scalar(ScalarFunc::Lower)),
            "random" => Ok(Self::Scalar(ScalarFunc::Random)),
            "randomblob" => Ok(Self::Scalar(ScalarFunc::RandomBlob)),
            "trim" => Ok(Self::Scalar(ScalarFunc::Trim)),
            "ltrim" => Ok(Self::Scalar(ScalarFunc::LTrim)),
            "rtrim" => Ok(Self::Scalar(ScalarFunc::RTrim)),
            "round" => Ok(Self::Scalar(ScalarFunc::Round)),
            "length" => Ok(Self::Scalar(ScalarFunc::Length)),
            "octet_length" => Ok(Self::Scalar(ScalarFunc::OctetLength)),
            "sign" => Ok(Self::Scalar(ScalarFunc::Sign)),
            "substr" => Ok(Self::Scalar(ScalarFunc::Substr)),
            "substring" => Ok(Self::Scalar(ScalarFunc::Substring)),
            "date" => Ok(Self::Scalar(ScalarFunc::Date)),
            "time" => Ok(Self::Scalar(ScalarFunc::Time)),
            "datetime" => Ok(Self::Scalar(ScalarFunc::DateTime)),
            "typeof" => Ok(Self::Scalar(ScalarFunc::Typeof)),
            "last_insert_rowid" => Ok(Self::Scalar(ScalarFunc::LastInsertRowid)),
            "unicode" => Ok(Self::Scalar(ScalarFunc::Unicode)),
            "quote" => Ok(Self::Scalar(ScalarFunc::Quote)),
            "sqlite_version" => Ok(Self::Scalar(ScalarFunc::SqliteVersion)),
            "turso_version" => Ok(Self::Scalar(ScalarFunc::TursoVersion)),
            "sqlite_source_id" => Ok(Self::Scalar(ScalarFunc::SqliteSourceId)),
            "replace" => Ok(Self::Scalar(ScalarFunc::Replace)),
            "likely" => Ok(Self::Scalar(ScalarFunc::Likely)),
            "likelihood" => Ok(Self::Scalar(ScalarFunc::Likelihood)),
            "unlikely" => Ok(Self::Scalar(ScalarFunc::Unlikely)),
            #[cfg(feature = "json")]
            "json" => Ok(Self::Json(JsonFunc::Json)),
            #[cfg(feature = "json")]
            "jsonb" => Ok(Self::Json(JsonFunc::Jsonb)),
            #[cfg(feature = "json")]
            "json_array_length" => Ok(Self::Json(JsonFunc::JsonArrayLength)),
            #[cfg(feature = "json")]
            "json_array" => Ok(Self::Json(JsonFunc::JsonArray)),
            #[cfg(feature = "json")]
            "jsonb_array" => Ok(Self::Json(JsonFunc::JsonbArray)),
            #[cfg(feature = "json")]
            "json_extract" => Ok(Func::Json(JsonFunc::JsonExtract)),
            #[cfg(feature = "json")]
            "jsonb_extract" => Ok(Func::Json(JsonFunc::JsonbExtract)),
            #[cfg(feature = "json")]
            "json_object" => Ok(Func::Json(JsonFunc::JsonObject)),
            #[cfg(feature = "json")]
            "jsonb_object" => Ok(Func::Json(JsonFunc::JsonbObject)),
            #[cfg(feature = "json")]
            "json_type" => Ok(Func::Json(JsonFunc::JsonType)),
            #[cfg(feature = "json")]
            "json_error_position" => Ok(Self::Json(JsonFunc::JsonErrorPosition)),
            #[cfg(feature = "json")]
            "json_valid" => Ok(Self::Json(JsonFunc::JsonValid)),
            #[cfg(feature = "json")]
            "json_patch" => Ok(Self::Json(JsonFunc::JsonPatch)),
            #[cfg(feature = "json")]
            "json_remove" => Ok(Self::Json(JsonFunc::JsonRemove)),
            #[cfg(feature = "json")]
            "jsonb_remove" => Ok(Self::Json(JsonFunc::JsonbRemove)),
            #[cfg(feature = "json")]
            "json_replace" => Ok(Self::Json(JsonFunc::JsonReplace)),
            #[cfg(feature = "json")]
            "json_insert" => Ok(Self::Json(JsonFunc::JsonInsert)),
            #[cfg(feature = "json")]
            "jsonb_insert" => Ok(Self::Json(JsonFunc::JsonbInsert)),
            #[cfg(feature = "json")]
            "jsonb_replace" => Ok(Self::Json(JsonFunc::JsonReplace)),
            #[cfg(feature = "json")]
            "json_pretty" => Ok(Self::Json(JsonFunc::JsonPretty)),
            #[cfg(feature = "json")]
            "json_set" => Ok(Self::Json(JsonFunc::JsonSet)),
            #[cfg(feature = "json")]
            "jsonb_set" => Ok(Self::Json(JsonFunc::JsonbSet)),
            #[cfg(feature = "json")]
            "json_quote" => Ok(Self::Json(JsonFunc::JsonQuote)),
            "unixepoch" => Ok(Self::Scalar(ScalarFunc::UnixEpoch)),
            "julianday" => Ok(Self::Scalar(ScalarFunc::JulianDay)),
            "hex" => Ok(Self::Scalar(ScalarFunc::Hex)),
            "unhex" => Ok(Self::Scalar(ScalarFunc::Unhex)),
            "zeroblob" => Ok(Self::Scalar(ScalarFunc::ZeroBlob)),
            "soundex" => Ok(Self::Scalar(ScalarFunc::Soundex)),
            "table_columns_json_array" => Ok(Self::Scalar(ScalarFunc::TableColumnsJsonArray)),
            "bin_record_json_object" => Ok(Self::Scalar(ScalarFunc::BinRecordJsonObject)),
            "conn_txn_id" => Ok(Self::Scalar(ScalarFunc::ConnTxnId)),
            "is_autocommit" => Ok(Self::Scalar(ScalarFunc::IsAutocommit)),
            "acos" => Ok(Self::Math(MathFunc::Acos)),
            "acosh" => Ok(Self::Math(MathFunc::Acosh)),
            "asin" => Ok(Self::Math(MathFunc::Asin)),
            "asinh" => Ok(Self::Math(MathFunc::Asinh)),
            "atan" => Ok(Self::Math(MathFunc::Atan)),
            "atan2" => Ok(Self::Math(MathFunc::Atan2)),
            "atanh" => Ok(Self::Math(MathFunc::Atanh)),
            "ceil" => Ok(Self::Math(MathFunc::Ceil)),
            "ceiling" => Ok(Self::Math(MathFunc::Ceiling)),
            "cos" => Ok(Self::Math(MathFunc::Cos)),
            "cosh" => Ok(Self::Math(MathFunc::Cosh)),
            "degrees" => Ok(Self::Math(MathFunc::Degrees)),
            "exp" => Ok(Self::Math(MathFunc::Exp)),
            "floor" => Ok(Self::Math(MathFunc::Floor)),
            "ln" => Ok(Self::Math(MathFunc::Ln)),
            "log" => Ok(Self::Math(MathFunc::Log)),
            "log10" => Ok(Self::Math(MathFunc::Log10)),
            "log2" => Ok(Self::Math(MathFunc::Log2)),
            "mod" => Ok(Self::Math(MathFunc::Mod)),
            "pi" => Ok(Self::Math(MathFunc::Pi)),
            "pow" => Ok(Self::Math(MathFunc::Pow)),
            "power" => Ok(Self::Math(MathFunc::Power)),
            "radians" => Ok(Self::Math(MathFunc::Radians)),
            "sin" => Ok(Self::Math(MathFunc::Sin)),
            "sinh" => Ok(Self::Math(MathFunc::Sinh)),
            "sqrt" => Ok(Self::Math(MathFunc::Sqrt)),
            "tan" => Ok(Self::Math(MathFunc::Tan)),
            "tanh" => Ok(Self::Math(MathFunc::Tanh)),
            "trunc" => Ok(Self::Math(MathFunc::Trunc)),
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            "load_extension" => Ok(Self::Scalar(ScalarFunc::LoadExtension)),
            "strftime" => Ok(Self::Scalar(ScalarFunc::StrfTime)),
            "printf" | "format" => Ok(Self::Scalar(ScalarFunc::Printf)),
            "vector" => Ok(Self::Vector(VectorFunc::Vector)),
            "vector32" => Ok(Self::Vector(VectorFunc::Vector32)),
            "vector32_sparse" => Ok(Self::Vector(VectorFunc::Vector32Sparse)),
            "vector64" => Ok(Self::Vector(VectorFunc::Vector64)),
            "vector8" => Ok(Self::Vector(VectorFunc::Vector8)),
            "vector1bit" => Ok(Self::Vector(VectorFunc::Vector1Bit)),
            "vector_extract" => Ok(Self::Vector(VectorFunc::VectorExtract)),
            "vector_distance_cos" => Ok(Self::Vector(VectorFunc::VectorDistanceCos)),
            "vector_distance_l2" => Ok(Self::Vector(VectorFunc::VectorDistanceL2)),
            "vector_distance_jaccard" => Ok(Self::Vector(VectorFunc::VectorDistanceJaccard)),
            "vector_distance_dot" => Ok(Self::Vector(VectorFunc::VectorDistanceDot)),
            "vector_concat" => Ok(Self::Vector(VectorFunc::VectorConcat)),
            "vector_slice" => Ok(Self::Vector(VectorFunc::VectorSlice)),
            // FTS functions
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            "fts_score" => Ok(Self::Fts(FtsFunc::Score)),
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            "fts_match" => Ok(Self::Fts(FtsFunc::Match)),
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            "fts_highlight" => Ok(Self::Fts(FtsFunc::Highlight)),
            _ => crate::bail_parse_error!("no such function: {}", name),
        }
    }

    /// Returns a list of all built-in functions for PRAGMA function_list.
    /// Derives the list from enum iteration so it stays in sync automatically.
    /// Functions with multiple valid arities get one row per arity.
    pub fn builtin_function_list() -> Vec<FunctionListEntry> {
        let mut funcs = Vec::new();

        // Helper: push one entry per arity for a function
        let mut push = |name: String, func_type: &'static str, arities: &[i32], det: bool| {
            for &narg in arities {
                funcs.push(FunctionListEntry {
                    name: name.clone(),
                    func_type,
                    narg,
                    deterministic: det,
                });
            }
        };

        // Scalar functions (filter out internal-only variants)
        for f in ScalarFunc::iter() {
            if f.is_internal() {
                continue;
            }
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // Aggregate functions (External is #[strum(disabled)], skipped automatically).
        // SQLite reports built-in aggregates as "w" (window-capable) since they
        // can all be used with OVER clauses.
        for f in AggFunc::iter() {
            push(f.to_string(), "w", f.arities(), f.is_deterministic());
        }

        // Math functions (all scalar)
        for f in MathFunc::iter() {
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // Vector functions (all scalar)
        for f in VectorFunc::iter() {
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // JSON functions (feature-gated, filter out operator-style entries)
        #[cfg(feature = "json")]
        for f in JsonFunc::iter() {
            if f.is_internal() {
                continue;
            }
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // FTS functions (feature-gated)
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        for f in FtsFunc::iter() {
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // Aliases: functions callable under multiple names.
        // These are additional names that resolve_function() accepts
        // but that map to existing enum variants.
        funcs.push(FunctionListEntry {
            name: "format".into(),
            func_type: "s",
            narg: -1,
            deterministic: true,
        });
        funcs.push(FunctionListEntry {
            name: "if".into(),
            func_type: "s",
            narg: 3,
            deterministic: true,
        });

        funcs
    }
}

pub struct FunctionListEntry {
    pub name: String,
    pub func_type: &'static str, // "s" = scalar, "a" = aggregate, "w" = window
    pub narg: i32,               // -1 = variable
    pub deterministic: bool,
}
