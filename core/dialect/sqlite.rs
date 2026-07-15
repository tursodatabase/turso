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
                innocuous: true,
            })
        })
        .collect()
}
