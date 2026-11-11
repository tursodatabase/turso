//! The engine's own SQL and the built-ins every dialect composes with.
//!
//! The engine compiles SQLite syntax: a frontend dialect translates its
//! statements into the SQLite AST, and the statements the engine runs for
//! itself are written in SQLite SQL. This module has the parts of that
//! language that every dialect shares: the parser entry points, the
//! built-in functions, and the built-in catalog tables (`pragma_*`
//! table-valued functions, `json_each`/`json_tree`, `sqlite_dbpage`,
//! `btree_dump`, and `sqlite_turso_types`), which each dialect registers
//! with [`register_builtin_catalog`].
//!
//! [`EngineDialect`] is the dialect of the statements the engine runs for
//! itself. The SQLite dialect that users open databases with is
//! `SqliteDialect` in the `turso_sqlite` crate.

use super::ScalarFuncContext;
use crate::functions::datetime::{
    exec_date, exec_date_diff, exec_date_minus, exec_date_plus, exec_datetime_full, exec_julianday,
    exec_strftime, exec_time, exec_timediff, exec_unixepoch,
};
use crate::functions::math::{exec_gcd, exec_lcm};
use crate::functions::printf::exec_printf;
use crate::functions::string::{exec_lpad, exec_repeat, exec_rpad};
use crate::info;
#[cfg(feature = "json")]
use crate::json;
use crate::namespace::{mangle_name, NamespaceId};
use crate::numeric::decimal::{
    bigdecimal_to_blob, blob_to_bigdecimal, check_numeric_limits, format_numeric,
    numeric_from_value, validate_precision_scale, MAX_FRACTION_DIGITS,
};
use crate::numeric::Numeric;
use crate::pragma::PragmaVirtualTable;
use crate::schema::{Schema, Table};
use crate::stats::StatAccum;
use crate::sync::Arc;
use crate::types::AsValueRef;
use crate::vdbe::array::{
    compute_array_length_at_dim, exec_array_append, exec_array_cat, exec_array_contains,
    exec_array_contains_all, exec_array_overlap, exec_array_position, exec_array_prepend,
    exec_array_remove, exec_array_slice, exec_array_to_string, exec_string_to_array,
};
use crate::vdbe::{registers_to_ref_values, Register};
use crate::vtab::{VirtualTable, VirtualTableType};
use crate::{LimboError, Value, ValueRef};
use bigdecimal::Zero;
use branches::{mark_unlikely, unlikely};
use std::borrow::Cow;
use std::sync::atomic::Ordering;
use turso_ext::VTabKind;

/// SQLite version reported by compatibility APIs.
///
/// This must equal `sqlite/version`, which `scripts/install-sqlite3.sh`
/// reads; a test checks that, and that [`SQLITE_VERSION_NUMBER`] matches.
/// Keep README.md and COMPAT.md in sync when bumping it.
pub const SQLITE_VERSION: &str = "3.50.4";

/// Integer form of [`SQLITE_VERSION`] used by `sqlite3_libversion_number()`.
pub const SQLITE_VERSION_NUMBER: i32 = 3_050_004;

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use crate::function::FtsFunc;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
#[allow(unused_imports)]
use crate::function::{AggFunc, Func, MathFunc, ScalarFunc, VectorFunc, WindowFunc};

/// The dialect of the statements the engine runs for itself: SQLite SQL
/// with the built-in functions and catalog tables.
pub(crate) struct EngineDialect;

impl super::Dialect for EngineDialect {
    fn name(&self) -> &'static str {
        "engine"
    }
}

/// Refuses table SQL stored by the PostgreSQL frontend. The SQL comment that
/// marks it is valid SQLite syntax, so without this check the SQLite parser
/// would accept the PostgreSQL DDL with SQLite column semantics.
pub fn reject_postgres_table_sql(sql: &str) -> crate::Result<()> {
    if sql.starts_with(super::POSTGRES_TABLE_SQL_PREFIX) {
        return Err(crate::LimboError::Corrupt(format!(
            "malformed database schema: table was created by the postgres frontend: {sql}"
        )));
    }
    Ok(())
}

/// Parse the first SQLite statement in `sql` and return its consumed byte count.
pub fn parse(sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let cmd = parser.next_cmd()?;
    Ok((cmd, parser.offset()))
}

/// Formats canonical SQLite `CREATE TABLE` text from its AST.
pub fn format_table_sql(
    tbl_name: &turso_parser::ast::QualifiedName,
    body: &turso_parser::ast::CreateTableBody,
) -> crate::Result<String> {
    match body {
        turso_parser::ast::CreateTableBody::ColumnsAndConstraints { .. } => Ok(format!(
            "CREATE TABLE {} {}",
            tbl_name.name.as_ident(),
            body
        )),
        turso_parser::ast::CreateTableBody::AsSelect(_) => {
            crate::bail_parse_error!("CREATE TABLE AS SELECT is not supported")
        }
    }
}

/// Parse persisted SQLite table SQL into a `CREATE TABLE` statement.
pub fn parse_table_sql_ast(sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
    let (cmd, _) = parse(sql)?;
    match cmd {
        Some(turso_parser::ast::Cmd::Stmt(stmt @ turso_parser::ast::Stmt::CreateTable { .. })) => {
            Ok(stmt)
        }
        other => Err(crate::LimboError::Corrupt(format!(
            "persisted table SQL is not CREATE TABLE: {other:?}"
        ))),
    }
}

/// Recover persisted SQLite table SQL for schema replay.
///
/// SQLite text without a database qualifier can be replayed unchanged. A
/// qualified name must be removed because the vacuum target only exposes its
/// main schema.
pub fn table_sql_for_replay(sql: &str) -> crate::Result<String> {
    let stmt = parse_table_sql_ast(sql)?;
    let turso_parser::ast::Stmt::CreateTable {
        mut tbl_name,
        temporary,
        if_not_exists,
        body,
    } = stmt
    else {
        unreachable!("parse_table_sql_ast returned a non-CREATE TABLE statement");
    };

    if tbl_name.namespace.take().is_none() {
        return Ok(sql.to_string());
    }

    Ok(turso_parser::ast::Stmt::CreateTable {
        tbl_name,
        temporary,
        if_not_exists,
        body,
    }
    .to_string())
}

/// Insert the standard SQLite-style catalog tables into `schema`.
///
/// `pragma_*` virtual tables use the dedicated `VirtualTableType::Pragma`
/// variant (they aren't `InternalVirtualTable`), so they are inserted
/// directly. The rest go through [`Schema::register_internal_vtab`] — the
/// same path external callers use via [`crate::Database::register_internal_vtab`].
pub fn register_builtin_catalog(
    schema: &mut Schema,
    enable_custom_types: bool,
) -> crate::Result<()> {
    for vtab in pragma_vtabs() {
        schema.insert_table(
            NamespaceId::ROOT,
            vtab.name.to_owned(),
            Arc::new(Table::Virtual(Arc::new((*vtab).clone()))),
        );
    }

    #[cfg(feature = "json")]
    {
        schema.register_internal_vtab(crate::json::vtab::JsonVirtualTable::json_each())?;
        schema.register_internal_vtab(crate::json::vtab::JsonVirtualTable::json_tree())?;
    }
    #[cfg(feature = "cli_only")]
    {
        schema.register_internal_vtab(crate::dbpage::DbPageTable::new())?;
        schema.register_internal_vtab(crate::btree_dump::BtreeDumpTable::new())?;
    }
    if enable_custom_types {
        schema.register_internal_vtab(crate::turso_types_vtab::TursoTypesTable::new())?;
    }
    Ok(())
}

/// Build a `VirtualTable` for each PRAGMA table-valued function.
fn pragma_vtabs() -> Vec<Arc<VirtualTable>> {
    PragmaVirtualTable::functions()
        .into_iter()
        .map(|(tab, schema_sql)| {
            Arc::new(VirtualTable {
                name: format!("pragma_{}", tab.pragma_name),
                columns: VirtualTable::resolve_columns(schema_sql)
                    .expect("pragma table-valued function schema resolution should not fail"),
                kind: VTabKind::TableValuedFunction,
                vtab_type: VirtualTableType::Pragma(tab),
                vtab_id: 0,
                is_droppable: false,
                innocuous: true,
            })
        })
        .collect()
}

/// Resolve a name against the built-in SQLite function set.
///
/// This table is the SQLite dialect's scalar/aggregate/window name surface,
/// and doubles as the shared helper other dialects compose with when they
/// want a built-in under the same name. Engine mechanics that classify
/// already-translated AST (aggregate/window detection in the planner,
/// determinism checks on schema expressions) also resolve against it
/// directly, because the translated AST always references the engine
/// surface regardless of the frontend dialect.
pub fn resolve_builtin_function(name: &str, arg_count: usize) -> crate::Result<Option<Func>> {
    let func = resolve_builtin_function_name(name, arg_count)?;
    // Reject scalar calls whose argument count is not in `arities()`.
    if let Some(Func::Scalar(srf)) = &func {
        let arities = srf.arities();
        if !arities.contains(&-1) && !arities.contains(&(arg_count as i32)) {
            crate::bail_parse_error!("wrong number of arguments to function {}()", name)
        }
    }
    Ok(func)
}

/// Resolves a built-in function by name without checking the argument count
/// of scalar functions.
pub(crate) fn resolve_builtin_function_name(
    name: &str,
    arg_count: usize,
) -> crate::Result<Option<Func>> {
    let normalized_name = crate::util::normalize_ident(name);
    match normalized_name.as_str() {
        "avg" => {
            if arg_count != 1 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Agg(AggFunc::Avg)))
        }
        "count" => {
            // Handle both COUNT() and COUNT(expr) cases
            if arg_count == 0 {
                Ok(Some(Func::Agg(AggFunc::Count0))) // COUNT() case
            } else if arg_count == 1 {
                Ok(Some(Func::Agg(AggFunc::Count))) // COUNT(expr) case
            } else {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
        }
        "group_concat" => {
            if arg_count != 1 && arg_count != 2 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Agg(AggFunc::GroupConcat)))
        }
        "max" if arg_count > 1 => Ok(Some(Func::Scalar(ScalarFunc::Max))),
        "max" => {
            if arg_count < 1 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Agg(AggFunc::Max)))
        }
        "min" if arg_count > 1 => Ok(Some(Func::Scalar(ScalarFunc::Min))),
        "min" => {
            if arg_count < 1 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Agg(AggFunc::Min)))
        }
        "nullif" if arg_count == 2 => Ok(Some(Func::Scalar(ScalarFunc::Nullif))),
        "string_agg" => {
            if arg_count != 2 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Agg(AggFunc::StringAgg)))
        }
        "sum" => {
            if arg_count != 1 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Agg(AggFunc::Sum)))
        }
        "total" => {
            if arg_count != 1 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Agg(AggFunc::Total)))
        }
        "row_number" => {
            if arg_count != 0 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::RowNumber)))
        }
        "rank" => {
            if arg_count != 0 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::Rank)))
        }
        "dense_rank" => {
            if arg_count != 0 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::DenseRank)))
        }
        "first_value" => {
            if arg_count != 1 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::FirstValue)))
        }
        "last_value" => {
            if arg_count != 1 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::LastValue)))
        }
        "nth_value" => {
            if arg_count != 2 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::NthValue)))
        }
        "lag" => {
            if !(1..=3).contains(&arg_count) {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::Lag)))
        }
        "lead" => {
            if !(1..=3).contains(&arg_count) {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::Lead)))
        }
        "ntile" => {
            if arg_count != 1 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::Ntile)))
        }
        "percent_rank" => {
            if arg_count != 0 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::PercentRank)))
        }
        "cume_dist" => {
            if arg_count != 0 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Window(WindowFunc::CumeDist)))
        }
        "timediff" => {
            if arg_count != 2 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Scalar(ScalarFunc::TimeDiff)))
        }
        "array_agg" => Ok(Some(Func::Agg(AggFunc::ArrayAgg))),
        #[cfg(feature = "json")]
        "jsonb_group_array" => Ok(Some(Func::Agg(AggFunc::JsonbGroupArray))),
        #[cfg(feature = "json")]
        "json_group_array" => Ok(Some(Func::Agg(AggFunc::JsonGroupArray))),
        #[cfg(feature = "json")]
        "jsonb_group_object" => Ok(Some(Func::Agg(AggFunc::JsonbGroupObject))),
        #[cfg(feature = "json")]
        "json_group_object" => Ok(Some(Func::Agg(AggFunc::JsonGroupObject))),
        "char" | "chr" => Ok(Some(Func::Scalar(ScalarFunc::Char))),
        "coalesce" => Ok(Some(Func::Scalar(ScalarFunc::Coalesce))),
        "concat" => {
            if arg_count == 0 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Scalar(ScalarFunc::Concat)))
        }
        "concat_ws" => {
            if arg_count < 2 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Scalar(ScalarFunc::ConcatWs)))
        }
        "changes" => Ok(Some(Func::Scalar(ScalarFunc::Changes))),
        "total_changes" => Ok(Some(Func::Scalar(ScalarFunc::TotalChanges))),
        "glob" => Ok(Some(Func::Scalar(ScalarFunc::Glob))),
        "ifnull" => Ok(Some(Func::Scalar(ScalarFunc::IfNull))),
        "if" | "iif" => Ok(Some(Func::Scalar(ScalarFunc::Iif))),
        "instr" | "strpos" => Ok(Some(Func::Scalar(ScalarFunc::Instr))),
        "like" => Ok(Some(Func::Scalar(ScalarFunc::Like))),
        "abs" => Ok(Some(Func::Scalar(ScalarFunc::Abs))),
        "upper" => Ok(Some(Func::Scalar(ScalarFunc::Upper))),
        "lower" => Ok(Some(Func::Scalar(ScalarFunc::Lower))),
        "random" => Ok(Some(Func::Scalar(ScalarFunc::Random))),
        "randomblob" => Ok(Some(Func::Scalar(ScalarFunc::RandomBlob))),
        "trim" | "btrim" => Ok(Some(Func::Scalar(ScalarFunc::Trim))),
        "ltrim" => Ok(Some(Func::Scalar(ScalarFunc::LTrim))),
        "rtrim" => Ok(Some(Func::Scalar(ScalarFunc::RTrim))),
        "round" => Ok(Some(Func::Scalar(ScalarFunc::Round))),
        "length" | "char_length" | "character_length" => Ok(Some(Func::Scalar(ScalarFunc::Length))),
        "octet_length" => Ok(Some(Func::Scalar(ScalarFunc::OctetLength))),
        "sign" => Ok(Some(Func::Scalar(ScalarFunc::Sign))),
        "substr" => {
            if arg_count != 2 && arg_count != 3 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Scalar(ScalarFunc::Substr)))
        }
        "substring" => {
            if arg_count != 2 && arg_count != 3 {
                crate::bail_parse_error!("wrong number of arguments to function {}()", name)
            }
            Ok(Some(Func::Scalar(ScalarFunc::Substring)))
        }
        "date" => Ok(Some(Func::Scalar(ScalarFunc::Date))),
        "time" => Ok(Some(Func::Scalar(ScalarFunc::Time))),
        "datetime" => Ok(Some(Func::Scalar(ScalarFunc::DateTime))),
        "typeof" => Ok(Some(Func::Scalar(ScalarFunc::Typeof))),
        "subtype" => Ok(Some(Func::Scalar(ScalarFunc::Subtype))),
        "last_insert_rowid" => Ok(Some(Func::Scalar(ScalarFunc::LastInsertRowid))),
        "unicode" => Ok(Some(Func::Scalar(ScalarFunc::Unicode))),
        "unistr" => Ok(Some(Func::Scalar(ScalarFunc::Unistr))),
        "unistr_quote" => Ok(Some(Func::Scalar(ScalarFunc::UnistrQuote))),
        "quote" => Ok(Some(Func::Scalar(ScalarFunc::Quote))),
        "sqlite_version" => Ok(Some(Func::Scalar(ScalarFunc::SqliteVersion))),
        "turso_version" => Ok(Some(Func::Scalar(ScalarFunc::TursoVersion))),
        "sqlite_source_id" => Ok(Some(Func::Scalar(ScalarFunc::SqliteSourceId))),
        "replace" => Ok(Some(Func::Scalar(ScalarFunc::Replace))),
        "likely" => Ok(Some(Func::Scalar(ScalarFunc::Likely))),
        "likelihood" => Ok(Some(Func::Scalar(ScalarFunc::Likelihood))),
        "unlikely" => Ok(Some(Func::Scalar(ScalarFunc::Unlikely))),
        #[cfg(feature = "json")]
        "json" => Ok(Some(Func::Json(JsonFunc::Json))),
        #[cfg(feature = "json")]
        "jsonb" => Ok(Some(Func::Json(JsonFunc::Jsonb))),
        #[cfg(feature = "json")]
        "json_array_length" => Ok(Some(Func::Json(JsonFunc::JsonArrayLength))),
        #[cfg(feature = "json")]
        "json_array" => Ok(Some(Func::Json(JsonFunc::JsonArray))),
        #[cfg(feature = "json")]
        "jsonb_array" => Ok(Some(Func::Json(JsonFunc::JsonbArray))),
        #[cfg(feature = "json")]
        "json_extract" => Ok(Some(Func::Json(JsonFunc::JsonExtract))),
        #[cfg(feature = "json")]
        "jsonb_extract" => Ok(Some(Func::Json(JsonFunc::JsonbExtract))),
        #[cfg(feature = "json")]
        "json_object" => Ok(Some(Func::Json(JsonFunc::JsonObject))),
        #[cfg(feature = "json")]
        "jsonb_object" => Ok(Some(Func::Json(JsonFunc::JsonbObject))),
        #[cfg(feature = "json")]
        "json_type" => Ok(Some(Func::Json(JsonFunc::JsonType))),
        #[cfg(feature = "json")]
        "json_error_position" => Ok(Some(Func::Json(JsonFunc::JsonErrorPosition))),
        #[cfg(feature = "json")]
        "json_valid" => Ok(Some(Func::Json(JsonFunc::JsonValid))),
        #[cfg(feature = "json")]
        "json_patch" => Ok(Some(Func::Json(JsonFunc::JsonPatch))),
        #[cfg(feature = "json")]
        "jsonb_patch" => Ok(Some(Func::Json(JsonFunc::JsonbPatch))),
        #[cfg(feature = "json")]
        "json_remove" => Ok(Some(Func::Json(JsonFunc::JsonRemove))),
        #[cfg(feature = "json")]
        "jsonb_remove" => Ok(Some(Func::Json(JsonFunc::JsonbRemove))),
        #[cfg(feature = "json")]
        "json_replace" => Ok(Some(Func::Json(JsonFunc::JsonReplace))),
        #[cfg(feature = "json")]
        "json_insert" => Ok(Some(Func::Json(JsonFunc::JsonInsert))),
        #[cfg(feature = "json")]
        "jsonb_insert" => Ok(Some(Func::Json(JsonFunc::JsonbInsert))),
        #[cfg(feature = "json")]
        "jsonb_replace" => Ok(Some(Func::Json(JsonFunc::JsonbReplace))),
        #[cfg(feature = "json")]
        "json_pretty" => Ok(Some(Func::Json(JsonFunc::JsonPretty))),
        #[cfg(feature = "json")]
        "json_set" => Ok(Some(Func::Json(JsonFunc::JsonSet))),
        #[cfg(feature = "json")]
        "jsonb_set" => Ok(Some(Func::Json(JsonFunc::JsonbSet))),
        #[cfg(feature = "json")]
        "json_quote" => Ok(Some(Func::Json(JsonFunc::JsonQuote))),
        "unixepoch" => Ok(Some(Func::Scalar(ScalarFunc::UnixEpoch))),
        "julianday" => Ok(Some(Func::Scalar(ScalarFunc::JulianDay))),
        "hex" => Ok(Some(Func::Scalar(ScalarFunc::Hex))),
        "unhex" => Ok(Some(Func::Scalar(ScalarFunc::Unhex))),
        "get_byte" => Ok(Some(Func::Scalar(ScalarFunc::GetByte))),
        "set_byte" => Ok(Some(Func::Scalar(ScalarFunc::SetByte))),
        "zeroblob" => Ok(Some(Func::Scalar(ScalarFunc::ZeroBlob))),
        "soundex" => Ok(Some(Func::Scalar(ScalarFunc::Soundex))),
        "table_columns_json_array" => Ok(Some(Func::Scalar(ScalarFunc::TableColumnsJsonArray))),
        "bin_record_json_object" => Ok(Some(Func::Scalar(ScalarFunc::BinRecordJsonObject))),
        "conn_txn_id" => Ok(Some(Func::Scalar(ScalarFunc::ConnTxnId))),
        "is_autocommit" => Ok(Some(Func::Scalar(ScalarFunc::IsAutocommit))),
        "sequence_watermark_experimental" => Ok(Some(Func::Scalar(ScalarFunc::SequenceWatermark))),
        "acos" => Ok(Some(Func::Math(MathFunc::Acos))),
        "acosh" => Ok(Some(Func::Math(MathFunc::Acosh))),
        "asin" => Ok(Some(Func::Math(MathFunc::Asin))),
        "asinh" => Ok(Some(Func::Math(MathFunc::Asinh))),
        "atan" => Ok(Some(Func::Math(MathFunc::Atan))),
        "atan2" => Ok(Some(Func::Math(MathFunc::Atan2))),
        "atanh" => Ok(Some(Func::Math(MathFunc::Atanh))),
        "ceil" => Ok(Some(Func::Math(MathFunc::Ceil))),
        "ceiling" => Ok(Some(Func::Math(MathFunc::Ceiling))),
        "cos" => Ok(Some(Func::Math(MathFunc::Cos))),
        "cosh" => Ok(Some(Func::Math(MathFunc::Cosh))),
        "degrees" => Ok(Some(Func::Math(MathFunc::Degrees))),
        "exp" => Ok(Some(Func::Math(MathFunc::Exp))),
        "floor" => Ok(Some(Func::Math(MathFunc::Floor))),
        "ln" => Ok(Some(Func::Math(MathFunc::Ln))),
        "log" => Ok(Some(Func::Math(MathFunc::Log))),
        "log10" => Ok(Some(Func::Math(MathFunc::Log10))),
        "log2" => Ok(Some(Func::Math(MathFunc::Log2))),
        "mod" => Ok(Some(Func::Math(MathFunc::Mod))),
        "pi" => Ok(Some(Func::Math(MathFunc::Pi))),
        "pow" => Ok(Some(Func::Math(MathFunc::Pow))),
        "power" => Ok(Some(Func::Math(MathFunc::Power))),
        "radians" => Ok(Some(Func::Math(MathFunc::Radians))),
        "sin" => Ok(Some(Func::Math(MathFunc::Sin))),
        "sinh" => Ok(Some(Func::Math(MathFunc::Sinh))),
        "sqrt" => Ok(Some(Func::Math(MathFunc::Sqrt))),
        "tan" => Ok(Some(Func::Math(MathFunc::Tan))),
        "tanh" => Ok(Some(Func::Math(MathFunc::Tanh))),
        "trunc" => Ok(Some(Func::Math(MathFunc::Trunc))),
        #[cfg(feature = "fs")]
        #[cfg(not(target_family = "wasm"))]
        "load_extension" => Ok(Some(Func::Scalar(ScalarFunc::LoadExtension))),
        "strftime" => Ok(Some(Func::Scalar(ScalarFunc::StrfTime))),
        "printf" | "format" => Ok(Some(Func::Scalar(ScalarFunc::Printf))),
        "vector" => Ok(Some(Func::Vector(VectorFunc::Vector))),
        "vector32" => Ok(Some(Func::Vector(VectorFunc::Vector32))),
        "vector32_sparse" => Ok(Some(Func::Vector(VectorFunc::Vector32Sparse))),
        "vector64" => Ok(Some(Func::Vector(VectorFunc::Vector64))),
        "vector8" => Ok(Some(Func::Vector(VectorFunc::Vector8))),
        "vector1bit" => Ok(Some(Func::Vector(VectorFunc::Vector1Bit))),
        "vector_extract" => Ok(Some(Func::Vector(VectorFunc::VectorExtract))),
        "vector_distance_cos" => Ok(Some(Func::Vector(VectorFunc::VectorDistanceCos))),
        "vector_distance_l2" => Ok(Some(Func::Vector(VectorFunc::VectorDistanceL2))),
        "vector_distance_jaccard" => Ok(Some(Func::Vector(VectorFunc::VectorDistanceJaccard))),
        "vector_distance_dot" => Ok(Some(Func::Vector(VectorFunc::VectorDistanceDot))),
        "vector_concat" => Ok(Some(Func::Vector(VectorFunc::VectorConcat))),
        "vector_slice" => Ok(Some(Func::Vector(VectorFunc::VectorSlice))),
        // FTS functions
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        "fts_score" => Ok(Some(Func::Fts(FtsFunc::Score))),
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        "fts_match" => Ok(Some(Func::Fts(FtsFunc::Match))),
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        "fts_highlight" => Ok(Some(Func::Fts(FtsFunc::Highlight))),
        // Test type functions (for custom type system testing)
        "test_uint_encode" => Ok(Some(Func::Scalar(ScalarFunc::TestUintEncode))),
        "test_uint_decode" => Ok(Some(Func::Scalar(ScalarFunc::TestUintDecode))),
        #[cfg(feature = "test_helper")]
        "test_nondet_counter" => Ok(Some(Func::Scalar(ScalarFunc::TestNondetCounter))),
        "string_reverse" | "reverse" => Ok(Some(Func::Scalar(ScalarFunc::StringReverse))),
        "gcd" => Ok(Some(Func::Scalar(ScalarFunc::Gcd))),
        "lcm" => Ok(Some(Func::Scalar(ScalarFunc::Lcm))),
        "repeat" => Ok(Some(Func::Scalar(ScalarFunc::Repeat))),
        "lpad" => Ok(Some(Func::Scalar(ScalarFunc::Lpad))),
        "rpad" => Ok(Some(Func::Scalar(ScalarFunc::Rpad))),
        // Built-in type support functions
        "boolean_to_int" => Ok(Some(Func::Scalar(ScalarFunc::BooleanToInt))),
        "int_to_boolean" => Ok(Some(Func::Scalar(ScalarFunc::IntToBoolean))),
        "validate_ipaddr" => Ok(Some(Func::Scalar(ScalarFunc::ValidateIpAddr))),
        "bytea_encode" => Ok(Some(Func::Scalar(ScalarFunc::ByteaEncode))),
        "numeric_encode" => Ok(Some(Func::Scalar(ScalarFunc::NumericEncode))),
        "numeric_fit" => Ok(Some(Func::Scalar(ScalarFunc::NumericFit))),
        "numeric_decode" => Ok(Some(Func::Scalar(ScalarFunc::NumericDecode))),
        "numeric_add" => Ok(Some(Func::Scalar(ScalarFunc::NumericAdd))),
        "numeric_sub" => Ok(Some(Func::Scalar(ScalarFunc::NumericSub))),
        "numeric_mul" => Ok(Some(Func::Scalar(ScalarFunc::NumericMul))),
        "numeric_div" => Ok(Some(Func::Scalar(ScalarFunc::NumericDiv))),
        "numeric_mod" => Ok(Some(Func::Scalar(ScalarFunc::NumericMod))),
        "numeric_neg" => Ok(Some(Func::Scalar(ScalarFunc::NumericNeg))),
        "date_plus" => Ok(Some(Func::Scalar(ScalarFunc::DatePlus))),
        "date_minus" => Ok(Some(Func::Scalar(ScalarFunc::DateMinus))),
        "date_diff" => Ok(Some(Func::Scalar(ScalarFunc::DateDiff))),
        // Array construction / element access (desugared from syntax)
        "array" => Ok(Some(Func::Scalar(ScalarFunc::Array))),
        "array_element" => Ok(Some(Func::Scalar(ScalarFunc::ArrayElement))),
        "array_set_element" => Ok(Some(Func::Scalar(ScalarFunc::ArraySetElement))),
        // Array functions
        "array_length" | "array_upper" => Ok(Some(Func::Scalar(ScalarFunc::ArrayLength))),
        "array_append" => Ok(Some(Func::Scalar(ScalarFunc::ArrayAppend))),
        "array_prepend" => Ok(Some(Func::Scalar(ScalarFunc::ArrayPrepend))),
        "array_cat" => Ok(Some(Func::Scalar(ScalarFunc::ArrayCat))),
        "array_remove" => Ok(Some(Func::Scalar(ScalarFunc::ArrayRemove))),
        "array_contains" => Ok(Some(Func::Scalar(ScalarFunc::ArrayContains))),
        "array_position" => Ok(Some(Func::Scalar(ScalarFunc::ArrayPosition))),
        "array_slice" => Ok(Some(Func::Scalar(ScalarFunc::ArraySlice))),
        "string_to_array" => Ok(Some(Func::Scalar(ScalarFunc::StringToArray))),
        "array_to_string" => Ok(Some(Func::Scalar(ScalarFunc::ArrayToString))),
        "array_overlap" | "array_overlaps" => Ok(Some(Func::Scalar(ScalarFunc::ArrayOverlap))),
        "array_contains_all" => Ok(Some(Func::Scalar(ScalarFunc::ArrayContainsAll))),
        // Struct/Union functions
        "struct_pack" => Ok(Some(Func::Scalar(ScalarFunc::StructPack))),
        "struct_extract" => Ok(Some(Func::Scalar(ScalarFunc::StructExtractFunc))),
        "union_value" => Ok(Some(Func::Scalar(ScalarFunc::UnionValueFunc))),
        "union_tag" => Ok(Some(Func::Scalar(ScalarFunc::UnionTagFunc))),
        "union_extract" => Ok(Some(Func::Scalar(ScalarFunc::UnionExtractFunc))),
        // Sequence functions
        "nextval" => Ok(Some(Func::Scalar(ScalarFunc::NextVal))),
        "currval" => Ok(Some(Func::Scalar(ScalarFunc::CurrVal))),
        "setval" => Ok(Some(Func::Scalar(ScalarFunc::SetVal))),
        _ => Ok(None),
    }
}

/// Runs built-in scalar function `func` on the registers that hold its
/// arguments. Shared by every dialect that resolves names onto the built-in
/// set through [`resolve_builtin_function`].
pub fn call_builtin_scalar(
    context: &ScalarFuncContext<'_>,
    func: &ScalarFunc,
    args: &[Register],
) -> crate::Result<Value> {
    let connection = context.connection;
    match func {
        ScalarFunc::Array | ScalarFunc::ArrayElement | ScalarFunc::ArraySetElement => {
            unreachable!("desugared to dedicated instructions, not Function")
        }
        ScalarFunc::Coalesce | ScalarFunc::IfNull | ScalarFunc::Iif => {
            unreachable!("{func} is compiled to jumps, not Function")
        }
        ScalarFunc::Attach | ScalarFunc::TursoVersion => Err(LimboError::InternalError(format!(
            "{func} is executed by the VDBE, not the dialect"
        ))),
        #[cfg(feature = "fs")]
        #[cfg(not(target_family = "wasm"))]
        ScalarFunc::LoadExtension => Err(LimboError::InternalError(format!(
            "{func} is executed by the VDBE, not the dialect"
        ))),
        ScalarFunc::Cast => {
            assert_eq!(args.len(), 2);
            let Value::Text(cast_type) = args[1].get_value() else {
                unreachable!("Cast with non-text type");
            };
            Ok(args[0].get_value().exec_cast(cast_type.as_str())?)
        }
        ScalarFunc::Changes => Ok(Value::from_i64(connection.changes.load(Ordering::SeqCst))),
        ScalarFunc::Char => Ok(Value::exec_char(args.iter().map(|reg| reg.get_value()))),
        ScalarFunc::Concat => Ok(Value::exec_concat_strings(
            args.iter().map(|reg| reg.get_value()),
        )),
        ScalarFunc::ConcatWs => Ok(Value::exec_concat_ws(
            args.iter().map(|reg| reg.get_value()),
        )),
        ScalarFunc::Glob => {
            if args.len() != 2 {
                mark_unlikely();
                return Err(LimboError::ParseError(
                    "wrong number of arguments to function GLOB()".to_string(),
                ));
            }
            let pattern_value = args[0].get_value();
            let match_value = args[1].get_value();
            if matches!(pattern_value, Value::Null) || matches!(match_value, Value::Null) {
                return Ok(Value::Null);
            }
            let pattern_cow = text_or_cast_to_text(pattern_value)?;
            let match_cow = text_or_cast_to_text(match_value)?;
            let matches = Value::exec_glob(&pattern_cow, &match_cow)?;
            Ok(Value::from_i64(matches as i64))
        }
        ScalarFunc::Instr => Ok(match args[0].get_value().exec_instr(args[1].get_value()) {
            Value::Numeric(Numeric::Integer(i)) => Value::from_i64(i),
            _ => Value::Null,
        }),
        ScalarFunc::LastInsertRowid => Ok(Value::from_i64(connection.last_insert_rowid())),
        ScalarFunc::Like => {
            let pattern_value = args[0].get_value();
            let match_value = args[1].get_value();
            if matches!(pattern_value, Value::Null) || matches!(match_value, Value::Null) {
                return Ok(Value::Null);
            }
            let mut escape_char = None;
            if args.len() == 3 {
                let escape_value = args[2].get_value();
                if matches!(escape_value, Value::Null) {
                    return Ok(Value::Null);
                }
                let escape_cow = text_or_cast_to_text(escape_value)?;
                let mut chars = escape_cow.chars();
                let c = chars.next();
                if c.is_none() || chars.next().is_some() {
                    return Err(LimboError::SqlError(
                        "ESCAPE expression must be a single character".to_string(),
                    ));
                }
                escape_char = c;
            }
            let matches = match (pattern_value, match_value) {
                (Value::Text(pattern), Value::Text(text)) => {
                    Value::exec_like(pattern.as_str(), text.as_str(), escape_char)?
                }
                _ => exec_like_converted(pattern_value, match_value, escape_char)?,
            };
            Ok(Value::from_i64(matches as i64))
        }
        ScalarFunc::NextVal => Err(LimboError::InternalError(
            "ScalarFunc::NextVal handler should be unreachable under \
             the disk-only sequence design"
                .to_string(),
        )),
        ScalarFunc::CurrVal => {
            let seq_name = match args[0].get_value() {
                Value::Text(t) => t.as_str().to_string(),
                _ => {
                    return Err(LimboError::ParseError(
                        "currval() requires a text argument".to_string(),
                    ));
                }
            };
            let (namespace, sequence) = connection.find_sequence(&seq_name)?;
            match connection.get_sequence_currval(
                namespace.database_id,
                &mangle_name(namespace.id, &sequence.name),
            ) {
                Some(val) => Ok(Value::from_i64(val)),
                None => Err(LimboError::ParseError(format!(
                    "currval of sequence \"{}\" is not yet defined in this session",
                    sequence.name
                ))),
            }
        }
        ScalarFunc::SetVal => {
            if connection.is_tx_read_only() {
                return Err(LimboError::ReadOnly);
            }
            let seq_name = match args[0].get_value() {
                Value::Text(t) => t.as_str().to_string(),
                _ => {
                    return Err(LimboError::ParseError(
                        "setval() requires a text argument".to_string(),
                    ));
                }
            };
            let value = match args[1].get_value().as_int() {
                Some(v) => v,
                None => {
                    return Err(LimboError::ParseError(
                        "setval() requires an integer value".to_string(),
                    ));
                }
            };
            if args.len() > 2 && args[2].get_value().as_int().is_none() {
                return Err(LimboError::ParseError(
                    "setval() requires an integer is_called argument".to_string(),
                ));
            }
            let (_, seq) = connection.find_sequence(&seq_name)?;
            if value < seq.min_value || value > seq.max_value {
                return Err(LimboError::ParseError(format!(
                    "setval: value {} is out of bounds for sequence \"{}\" ({}..{})",
                    value, seq.name, seq.min_value, seq.max_value
                )));
            }
            Ok(Value::from_i64(value))
        }
        ScalarFunc::Abs
        | ScalarFunc::Lower
        | ScalarFunc::Upper
        | ScalarFunc::Length
        | ScalarFunc::OctetLength
        | ScalarFunc::Subtype
        | ScalarFunc::Typeof
        | ScalarFunc::Unicode
        | ScalarFunc::Unistr
        | ScalarFunc::UnistrQuote
        | ScalarFunc::Quote
        | ScalarFunc::RandomBlob
        | ScalarFunc::Sign
        | ScalarFunc::Soundex
        | ScalarFunc::ZeroBlob => {
            let reg_value = args[0].get_value();
            let result = match func {
                ScalarFunc::Sign => reg_value.exec_sign(),
                ScalarFunc::Abs => Some(reg_value.exec_abs()?),
                ScalarFunc::Lower => reg_value.exec_lower(),
                ScalarFunc::Upper => reg_value.exec_upper(),
                ScalarFunc::Length => Some(reg_value.exec_length()),
                ScalarFunc::OctetLength => Some(reg_value.exec_octet_length()),
                ScalarFunc::Subtype => Some(reg_value.exec_subtype()),
                ScalarFunc::Typeof => Some(reg_value.exec_typeof()),
                ScalarFunc::Unicode => Some(reg_value.exec_unicode()),
                ScalarFunc::Unistr => Some(reg_value.exec_unistr()?),
                ScalarFunc::Quote => Some(reg_value.exec_quote()),
                ScalarFunc::UnistrQuote => Some(reg_value.exec_unistr_quote()),
                ScalarFunc::RandomBlob => {
                    Some(reg_value.exec_randomblob(|dest| context.io.fill_bytes(dest))?)
                }
                ScalarFunc::ZeroBlob => Some(reg_value.exec_zeroblob()?),
                ScalarFunc::Soundex => Some(reg_value.exec_soundex()),
                _ => unreachable!(),
            };
            Ok(match result {
                Some(value) => value,
                None => Value::Null,
            })
        }
        ScalarFunc::Hex => Ok(args[0].get_value().exec_hex()),
        ScalarFunc::Unhex => {
            let ignored_chars = if args.len() == 2 { args.get(1) } else { None };
            Ok(args[0]
                .get_value()
                .exec_unhex(ignored_chars.map(|x| x.get_value())))
        }
        ScalarFunc::GetByte => args[0].get_value().exec_get_byte(args[1].get_value()),
        ScalarFunc::SetByte => args[0]
            .get_value()
            .exec_set_byte(args[1].get_value(), args[2].get_value()),
        ScalarFunc::Random => Ok(Value::from_i64(context.io.generate_random_number())),
        #[cfg(feature = "test_helper")]
        ScalarFunc::TestNondetCounter => Ok(Value::from_i64(
            connection
                .test_nondet_counter
                .fetch_add(1, Ordering::Relaxed),
        )),
        ScalarFunc::Trim => {
            let pattern_value = if args.len() == 2 { args.get(1) } else { None };
            Ok(args[0]
                .get_value()
                .exec_trim(pattern_value.map(|x| x.get_value())))
        }
        ScalarFunc::LTrim => {
            let pattern_value = if args.len() == 2 { args.get(1) } else { None };
            Ok(args[0]
                .get_value()
                .exec_ltrim(pattern_value.map(|x| x.get_value())))
        }
        ScalarFunc::RTrim => {
            let pattern_value = if args.len() == 2 { args.get(1) } else { None };
            Ok(args[0]
                .get_value()
                .exec_rtrim(pattern_value.map(|x| x.get_value())))
        }
        ScalarFunc::Round => {
            assert!(args.len() == 1 || args.len() == 2);
            let precision_value = if args.len() > 1 { args.get(1) } else { None };
            Ok(args[0]
                .get_value()
                .exec_round(precision_value.map(|x| x.get_value())))
        }
        ScalarFunc::Min => {
            let collation = context.collation;
            Value::exec_min(args.iter().map(|v| v.get_value()), |a, b| {
                Ok(collation.compare(a.as_value_ref(), b.as_value_ref()))
            })
        }
        ScalarFunc::Max => {
            let collation = context.collation;
            Value::exec_max(args.iter().map(|v| v.get_value()), |a, b| {
                Ok(collation.compare(a.as_value_ref(), b.as_value_ref()))
            })
        }
        ScalarFunc::Nullif => {
            let collation = context.collation;
            args[0]
                .get_value()
                .exec_nullif(args[1].get_value(), |a, b| {
                    Ok(collation.compare(a.as_value_ref(), b.as_value_ref()))
                })
        }
        ScalarFunc::Substr | ScalarFunc::Substring => {
            let length_value = if args.len() == 3 {
                Some(&args[2])
            } else {
                None
            };
            Ok(Value::exec_substring(
                args[0].get_value(),
                args[1].get_value(),
                length_value.map(|x| x.get_value()),
            )?)
        }
        ScalarFunc::Date => Ok(exec_date(context.clock, registers_to_ref_values(args))),
        ScalarFunc::Time => Ok(exec_time(context.clock, registers_to_ref_values(args))),
        ScalarFunc::TimeDiff => {
            if args.len() != 2 {
                return Ok(Value::Null);
            }
            Ok(exec_timediff(
                context.clock,
                [args[0].get_value(), args[1].get_value()],
            ))
        }
        ScalarFunc::TotalChanges => Ok(Value::from_i64(
            connection.total_changes.load(Ordering::SeqCst),
        )),
        ScalarFunc::DateTime => Ok(exec_datetime_full(
            context.clock,
            registers_to_ref_values(args),
        )),
        ScalarFunc::JulianDay => Ok(exec_julianday(context.clock, registers_to_ref_values(args))),
        ScalarFunc::UnixEpoch => Ok(exec_unixepoch(context.clock, registers_to_ref_values(args))),
        ScalarFunc::SqliteVersion => Ok(Value::build_text(execute_sqlite_version())),
        ScalarFunc::SqliteSourceId => Ok(Value::build_text(format!(
            "{} {}",
            info::build::BUILT_TIME_SQLITE,
            info::build::GIT_COMMIT_HASH.unwrap_or("unknown")
        ))),
        ScalarFunc::Replace => {
            assert_eq!(args.len(), 3);
            Ok(Value::exec_replace(
                args[0].get_value(),
                args[1].get_value(),
                args[2].get_value(),
            )?)
        }
        ScalarFunc::StrfTime => Ok(exec_strftime(context.clock, registers_to_ref_values(args))),
        ScalarFunc::Printf => exec_printf(args),
        ScalarFunc::TableColumnsJsonArray => {
            assert_eq!(args.len(), 1);
            #[cfg(not(feature = "json"))]
            {
                Err(LimboError::InvalidArgument(
                    "table_columns_json_array: turso must be compiled with JSON support"
                        .to_string(),
                ))
            }
            #[cfg(feature = "json")]
            {
                use crate::types::{TextRef, TextSubtype};

                let Value::Text(table) = args[0].get_value() else {
                    return Err(LimboError::InvalidArgument(
                        "table_columns_json_array: function argument must be of type TEXT"
                            .to_string(),
                    ));
                };
                let table = {
                    let schema = connection.schema.read();
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
            }
        }
        ScalarFunc::BinRecordJsonObject => {
            assert_eq!(args.len(), 2);
            #[cfg(not(feature = "json"))]
            {
                Err(LimboError::InvalidArgument(
                    "bin_record_json_object: turso must be compiled with JSON support".to_string(),
                ))
            }
            #[cfg(feature = "json")]
            {
                use crate::types::ValueIterator;
                use std::str::FromStr;

                let columns_str = args[0].get_value();
                let bin_record = args[1].get_value();
                let Value::Text(columns_str) = columns_str else {
                    return Err(LimboError::InvalidArgument(
                        "bin_record_json_object: function arguments must be of type TEXT and BLOB correspondingly".to_string()
                    ));
                };

                if let Value::Null = bin_record {
                    return Ok(Value::Null);
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
            }
        }
        ScalarFunc::Detach => {
            assert_eq!(args.len(), 1);
            let Value::Text(dbname_str) = args[0].get_value() else {
                return Err(LimboError::InvalidArgument(
                    "detach: database name argument must be text".to_string(),
                ));
            };
            connection.detach_database(dbname_str.as_str())?;
            Ok(Value::Null)
        }
        ScalarFunc::Unlikely | ScalarFunc::Likely | ScalarFunc::Likelihood => {
            panic!(
                "{func:?} should be stripped during expression translation and never reach VDBE"
            );
        }
        ScalarFunc::StatInit => {
            assert!(!args.is_empty());
            let n_col = match args[0].get_value() {
                Value::Numeric(Numeric::Integer(n)) => *n as usize,
                _ => 0,
            };
            Ok(Value::Blob(StatAccum::new(n_col).to_bytes()))
        }
        ScalarFunc::StatPush => {
            assert!(args.len() >= 2);
            let i_chng = match args[1].get_value() {
                Value::Numeric(Numeric::Integer(n)) => *n as usize,
                _ => 0,
            };
            Ok(match args[0].get_value() {
                Value::Blob(bytes) => {
                    if let Some(mut accum) = StatAccum::from_bytes(bytes) {
                        accum.push(i_chng);
                        Value::Blob(accum.to_bytes())
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            })
        }
        ScalarFunc::StatGet => {
            assert!(!args.is_empty());
            Ok(match args[0].get_value() {
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
            })
        }
        ScalarFunc::ConnTxnId => {
            assert_eq!(args.len(), 1);
            let candidate = match args[0].get_value() {
                Value::Numeric(Numeric::Integer(n)) => *n,
                _ => -1,
            };
            let current = connection.get_cdc_transaction_id();
            if current == -1 {
                connection.set_cdc_transaction_id(candidate);
                Ok(Value::from_i64(candidate))
            } else {
                Ok(Value::from_i64(current))
            }
        }
        ScalarFunc::IsAutocommit => {
            let auto_commit = connection.auto_commit.load(Ordering::SeqCst);
            Ok(Value::from_i64(if auto_commit { 1 } else { 0 }))
        }
        ScalarFunc::SequenceWatermark => {
            assert_eq!(args.len(), 1);
            let sequence_name = match args[0].get_value() {
                Value::Text(text) => text.as_str(),
                Value::Null => return Ok(Value::Null),
                _ => {
                    return Err(LimboError::InternalError(
                        "sequence_watermark_experimental() argument must be TEXT".to_string(),
                    ));
                }
            };
            let (namespace, sequence) = connection.find_sequence(sequence_name)?;
            let watermark =
                connection
                    .mv_store_for_db(namespace.database_id)
                    .and_then(|mv_store| {
                        mv_store.sequence_watermark(&mangle_name(namespace.id, &sequence.name))
                    });
            Ok(match watermark {
                Some(watermark) => Value::from_i64(watermark),
                None => Value::Null,
            })
        }
        ScalarFunc::TestUintEncode => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
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
            })
        }
        ScalarFunc::TestUintDecode => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                other => other.clone(),
            })
        }
        ScalarFunc::StringReverse => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                Value::Text(t) => {
                    let reversed: String = t.to_string().chars().rev().collect();
                    Value::build_text(reversed)
                }
                other => {
                    let s = other.to_string();
                    let reversed: String = s.chars().rev().collect();
                    Value::build_text(reversed)
                }
            })
        }
        ScalarFunc::Gcd => {
            check_arg_count(args, 2)?;
            exec_gcd(args[0].get_value(), args[1].get_value())
        }
        ScalarFunc::Lcm => {
            check_arg_count(args, 2)?;
            exec_lcm(args[0].get_value(), args[1].get_value())
        }
        ScalarFunc::Repeat => {
            check_arg_count(args, 2)?;
            exec_repeat(args[0].get_value(), args[1].get_value())
        }
        ScalarFunc::Lpad => {
            let fill = if args.len() >= 3 {
                Some(args[2].get_value())
            } else {
                None
            };
            exec_lpad(args[0].get_value(), args[1].get_value(), fill)
        }
        ScalarFunc::Rpad => {
            let fill = if args.len() >= 3 {
                Some(args[2].get_value())
            } else {
                None
            };
            exec_rpad(args[0].get_value(), args[1].get_value(), fill)
        }
        ScalarFunc::BooleanToInt => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                Value::Numeric(Numeric::Integer(i)) => match *i {
                    0 => Value::from_i64(0),
                    1 => Value::from_i64(1),
                    _ => {
                        return Err(LimboError::Constraint(format!(
                            "invalid input for type boolean: \"{i}\""
                        )));
                    }
                },
                Value::Text(t) => {
                    let v = &t.value;
                    match v.to_ascii_lowercase().as_str() {
                        "true" | "t" | "yes" | "on" | "1" => Value::from_i64(1),
                        "false" | "f" | "no" | "off" | "0" => Value::from_i64(0),
                        _ => {
                            return Err(LimboError::Constraint(format!(
                                "invalid input for type boolean: \"{v}\""
                            )));
                        }
                    }
                }
                other => {
                    return Err(LimboError::Constraint(format!(
                        "invalid input for type boolean: \"{other}\""
                    )));
                }
            })
        }
        ScalarFunc::IntToBoolean => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                Value::Numeric(Numeric::Integer(0)) => Value::build_text("false".to_string()),
                _ => Value::build_text("true".to_string()),
            })
        }
        ScalarFunc::ValidateIpAddr => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                Value::Text(t) => {
                    let v = &t.value;
                    v.parse::<std::net::IpAddr>().map_err(|_| {
                        LimboError::Constraint(format!("invalid input for type inet: \"{v}\""))
                    })?;
                    args[0].get_value().clone()
                }
                other => {
                    return Err(LimboError::Constraint(format!(
                        "invalid input for type inet: \"{other}\""
                    )));
                }
            })
        }
        ScalarFunc::ByteaEncode => {
            // Text is PostgreSQL bytea input. Other values are stored as
            // they are, so a blob keeps its bytes and the column's type check
            // still refuses a number.
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Text(t) => Value::from_blob(
                    crate::bytea_from_text(t.as_str())
                        .map_err(|e| LimboError::InvalidArgument(e.to_string()))?,
                ),
                other => other.clone(),
            })
        }
        ScalarFunc::NumericEncode => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                other => {
                    let bd = numeric_from_value(other)?;
                    Value::from_blob(bigdecimal_to_blob(&bd))
                }
            })
        }
        ScalarFunc::NumericFit => {
            check_arg_count(args, 3)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                Value::Blob(stored) => {
                    let bd = blob_to_bigdecimal(stored)?;
                    let (
                        Value::Numeric(Numeric::Integer(precision)),
                        Value::Numeric(Numeric::Integer(scale)),
                    ) = (args[1].get_value(), args[2].get_value())
                    else {
                        return Err(LimboError::Constraint(
                            "numeric_fit: precision and scale must be integers".to_string(),
                        ));
                    };
                    let fitted = validate_precision_scale(&bd, *precision, *scale)?;
                    Value::from_blob(bigdecimal_to_blob(&fitted))
                }
                other => {
                    return Err(LimboError::InternalError(format!(
                        "numeric_fit expects a stored numeric, got {}",
                        other.value_type()
                    )));
                }
            })
        }
        ScalarFunc::NumericDecode => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                Value::Blob(b) => Value::build_text(format_numeric(&blob_to_bigdecimal(b)?)),
                other => {
                    return Err(LimboError::Constraint(format!(
                        "numeric_decode: expected blob, got \"{other}\""
                    )));
                }
            })
        }
        ScalarFunc::NumericAdd
        | ScalarFunc::NumericSub
        | ScalarFunc::NumericMul
        | ScalarFunc::NumericDiv
        | ScalarFunc::NumericMod => {
            check_arg_count(args, 2)?;
            let lhs_val = args[0].get_value();
            let rhs_val = args[1].get_value();
            Ok(match (lhs_val, rhs_val) {
                (Value::Null, _) | (_, Value::Null) => Value::Null,
                _ => {
                    let a = numeric_from_value(lhs_val)?;
                    let b = numeric_from_value(rhs_val)?;
                    let (a_scale, b_scale) =
                        (a.fractional_digit_count(), b.fractional_digit_count());
                    let res = match func {
                        ScalarFunc::NumericAdd => (a + b).with_scale(a_scale.max(b_scale)),
                        ScalarFunc::NumericSub => (a - b).with_scale(a_scale.max(b_scale)),
                        ScalarFunc::NumericMul => (a * b).with_scale_round(
                            (a_scale + b_scale).min(MAX_FRACTION_DIGITS),
                            bigdecimal::RoundingMode::HalfUp,
                        ),
                        ScalarFunc::NumericDiv | ScalarFunc::NumericMod => {
                            if b.is_zero() {
                                return Err(LimboError::Constraint("division by zero".to_string()));
                            }
                            if matches!(func, ScalarFunc::NumericDiv) {
                                a / b
                            } else {
                                (a % b).with_scale(a_scale.max(b_scale))
                            }
                        }
                        _ => unreachable!(),
                    };
                    check_numeric_limits(&res)?;
                    Value::build_text(format_numeric(&res))
                }
            })
        }
        ScalarFunc::NumericNeg => {
            check_arg_count(args, 1)?;
            Ok(match args[0].get_value() {
                Value::Null => Value::Null,
                val => Value::build_text(format_numeric(&-numeric_from_value(val)?)),
            })
        }
        ScalarFunc::DatePlus | ScalarFunc::DateMinus | ScalarFunc::DateDiff => {
            check_arg_count(args, 2)?;
            let lhs = args[0].get_value();
            let rhs = args[1].get_value();
            match func {
                ScalarFunc::DatePlus => exec_date_plus(context.clock, lhs, rhs),
                ScalarFunc::DateMinus => exec_date_minus(context.clock, lhs, rhs),
                ScalarFunc::DateDiff => exec_date_diff(context.clock, lhs, rhs),
                _ => unreachable!(),
            }
        }
        ScalarFunc::ArrayAppend => {
            check_arg_count(args, 2)?;
            exec_array_append(args[0].get_value(), args[1].get_value())
        }
        ScalarFunc::ArrayPrepend => {
            check_arg_count(args, 2)?;
            exec_array_prepend(args[1].get_value(), args[0].get_value())
        }
        ScalarFunc::ArrayCat => {
            check_arg_count(args, 2)?;
            exec_array_cat(args[0].get_value(), args[1].get_value())
        }
        ScalarFunc::ArrayRemove => {
            check_arg_count(args, 2)?;
            exec_array_remove(args[0].get_value(), args[1].get_value())
        }
        ScalarFunc::ArrayContains => {
            check_arg_count(args, 2)?;
            Ok(exec_array_contains(
                args[0].get_value(),
                args[1].get_value(),
            ))
        }
        ScalarFunc::ArrayPosition => {
            check_arg_count(args, 2)?;
            Ok(exec_array_position(
                args[0].get_value(),
                args[1].get_value(),
            ))
        }
        ScalarFunc::ArrayLength => {
            let dim = if args.len() >= 2 {
                args[1].get_value().as_int().unwrap_or(0)
            } else {
                1
            };
            Ok(
                match compute_array_length_at_dim(args[0].get_value(), dim) {
                    Some(count) => Value::from_i64(count),
                    None => Value::Null,
                },
            )
        }
        ScalarFunc::ArraySlice => {
            check_arg_count(args, 3)?;
            exec_array_slice(
                args[0].get_value(),
                args[1].get_value(),
                args[2].get_value(),
            )
        }
        ScalarFunc::StringToArray => {
            let null_str = if args.len() >= 3 {
                Some(args[2].get_value())
            } else {
                None
            };
            exec_string_to_array(args[0].get_value(), args[1].get_value(), null_str)
        }
        ScalarFunc::ArrayToString => {
            let null_str = if args.len() >= 3 {
                Some(args[2].get_value())
            } else {
                None
            };
            Ok(exec_array_to_string(
                args[0].get_value(),
                args[1].get_value(),
                null_str,
            ))
        }
        ScalarFunc::ArrayOverlap => {
            check_arg_count(args, 2)?;
            Ok(exec_array_overlap(args[0].get_value(), args[1].get_value()))
        }
        ScalarFunc::ArrayContainsAll => {
            check_arg_count(args, 2)?;
            Ok(exec_array_contains_all(
                args[0].get_value(),
                args[1].get_value(),
            ))
        }
        ScalarFunc::StructPack
        | ScalarFunc::StructExtractFunc
        | ScalarFunc::UnionValueFunc
        | ScalarFunc::UnionTagFunc
        | ScalarFunc::UnionExtractFunc => Err(LimboError::InternalError(format!(
            "{func} should be desugared to a dedicated instruction, not Function"
        ))),
    }
}

fn text_or_cast_to_text(value: &Value) -> crate::Result<Cow<'_, str>> {
    match value {
        Value::Text(s) => Ok(Cow::Borrowed(s.as_str())),
        v => match v.exec_cast("TEXT")? {
            Value::Text(s) => Ok(Cow::Owned(s.to_string())),
            _ => unreachable!("Cast to TEXT should yield Text"),
        },
    }
}

/// LIKE with an operand that is not TEXT: both are converted to TEXT first.
#[inline(never)]
fn exec_like_converted(
    pattern_value: &Value,
    match_value: &Value,
    escape_char: Option<char>,
) -> crate::Result<bool> {
    let pattern_cow = text_or_cast_to_text(pattern_value)?;
    let match_cow = text_or_cast_to_text(match_value)?;
    Value::exec_like(&pattern_cow, &match_cow, escape_char)
}

fn check_arg_count(args: &[Register], expected: usize) -> crate::Result<()> {
    if unlikely(args.len() != expected) {
        return Err(wrong_arg_count(expected, args.len()));
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn wrong_arg_count(expected: usize, actual: usize) -> LimboError {
    LimboError::InternalError(format!("expected {expected} argument(s), got {actual}"))
}

// Compat for applications that test for SQLite.
fn execute_sqlite_version() -> String {
    SQLITE_VERSION.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_sqlite_version() {
        assert_eq!(execute_sqlite_version(), SQLITE_VERSION);
    }

    /// The constants cannot read `sqlite/version` while building, because the
    /// file is outside this crate and would be missing from the published
    /// package. This test fails when they differ from it.
    #[test]
    fn sqlite_version_constants_match_sqlite_version_file() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../sqlite/version");
        let text = std::fs::read_to_string(path).expect("sqlite/version should exist");
        let version = text.trim();

        assert_eq!(SQLITE_VERSION, version);

        let [major, minor, patch]: [i32; 3] = version
            .split('.')
            .map(|part| part.parse().unwrap())
            .collect::<Vec<_>>()
            .try_into()
            .expect("sqlite/version should have three parts, like 3.50.4");
        assert_eq!(
            SQLITE_VERSION_NUMBER,
            major * 1_000_000 + minor * 1_000 + patch
        );
    }
}
