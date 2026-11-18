//! The engine's own SQL.
//!
//! The engine compiles SQLite syntax: a frontend dialect translates its
//! statements into the SQLite AST, and the statements the engine runs for
//! itself are written in SQLite SQL. This module has the parser entry
//! points for that SQL.
//!
//! [`EngineDialect`] is the dialect of the statements the engine runs for
//! itself. The SQLite dialect that users open databases with is
//! `SqliteDialect` in the `turso_sqlite` crate.

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

/// Parse the first SQLite statement in `sql`.
pub fn parse_statement(sql: &str) -> crate::Result<Option<turso_parser::ast::Cmd>> {
    Ok(parse(sql)?.0)
}

/// Parse the column definition of an `ALTER TABLE` statement.
pub fn parse_column_definition(
    sql: &str,
    in_alter: bool,
) -> crate::Result<turso_parser::ast::ColumnDefinition> {
    Ok(turso_parser::parser::Parser::new(sql.as_bytes()).parse_column_definition(in_alter)?)
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
