//! The SQLite dialect and built-in SQLite-style catalog tables.
//!
//! [`SqliteDialect`] is the [`super::Dialect`] for SQLite: schema
//! rows are plain SQLite text parsed with the SQLite parser. The catalog
//! tables here (`pragma_*` table-valued functions, `json_each`/`json_tree`,
//! `sqlite_dbpage`, `btree_dump`, and `sqlite_turso_types`) are registered
//! into every fresh schema by [`crate::schema::Schema::with_options`]
//! through [`register_builtin_catalog`].

use crate::pragma::PragmaVirtualTable;
use crate::schema::{BTreeTable, Schema, Table};
use crate::sync::Arc;
use crate::vtab::{VirtualTable, VirtualTableType};
use turso_ext::VTabKind;

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use crate::function::FtsFunc;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
#[allow(unused_imports)]
use crate::function::{AggFunc, Func, MathFunc, ScalarFunc, VectorFunc, WindowFunc};

/// The SQLite dialect: statements are SQLite SQL and `sqlite_schema`
/// rows are canonical SQLite text.
pub struct SqliteDialect;

impl super::Dialect for SqliteDialect {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
        parse(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
        BTreeTable::from_sql(sql, root_page)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
        parse_table_sql_ast(sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
        table_sql_for_replay(sql)
    }

    fn format_table_sql(
        &self,
        _input: &str,
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

    fn register_catalog(
        &self,
        schema: &mut Schema,
        enable_custom_types: bool,
    ) -> crate::Result<()> {
        register_builtin_catalog(schema, enable_custom_types)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<Func>> {
        resolve_builtin_function(name, arg_count)
    }
}

/// Parse the first SQLite statement in `sql` and return its consumed byte count.
pub fn parse(sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let cmd = parser.next_cmd()?;
    Ok((cmd, parser.offset()))
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

    if tbl_name.db_name.take().is_none() {
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
/// `pragma_*` virtual tables use the dedicated [`VirtualTableType::Pragma`]
/// variant (they aren't `InternalVirtualTable`), so they are inserted
/// directly. The rest go through [`Schema::register_internal_vtab`] — the
/// same path external callers use via [`crate::Database::register_internal_vtab`].
pub fn register_builtin_catalog(
    schema: &mut Schema,
    enable_custom_types: bool,
) -> crate::Result<()> {
    for vtab in pragma_vtabs() {
        schema.tables.insert(
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
                println!("{arg_count}");
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
        "test_uint_add" => Ok(Some(Func::Scalar(ScalarFunc::TestUintAdd))),
        "test_uint_sub" => Ok(Some(Func::Scalar(ScalarFunc::TestUintSub))),
        "test_uint_mul" => Ok(Some(Func::Scalar(ScalarFunc::TestUintMul))),
        "test_uint_div" => Ok(Some(Func::Scalar(ScalarFunc::TestUintDiv))),
        "test_uint_lt" => Ok(Some(Func::Scalar(ScalarFunc::TestUintLt))),
        "test_uint_eq" => Ok(Some(Func::Scalar(ScalarFunc::TestUintEq))),
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
        "numeric_encode" => Ok(Some(Func::Scalar(ScalarFunc::NumericEncode))),
        "numeric_decode" => Ok(Some(Func::Scalar(ScalarFunc::NumericDecode))),
        "numeric_add" => Ok(Some(Func::Scalar(ScalarFunc::NumericAdd))),
        "numeric_sub" => Ok(Some(Func::Scalar(ScalarFunc::NumericSub))),
        "numeric_mul" => Ok(Some(Func::Scalar(ScalarFunc::NumericMul))),
        "numeric_div" => Ok(Some(Func::Scalar(ScalarFunc::NumericDiv))),
        "numeric_lt" => Ok(Some(Func::Scalar(ScalarFunc::NumericLt))),
        "numeric_eq" => Ok(Some(Func::Scalar(ScalarFunc::NumericEq))),
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
