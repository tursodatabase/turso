//! The SQLite frontend.
//!
//! [`SqliteDialect`] is the [`Dialect`] for SQLite: statements are SQLite
//! SQL and `sqlite_schema` rows are canonical SQLite text, parsed with the
//! SQLite parser.
//!
//! The unit tests of `turso_core` compile this file as a module of
//! `turso_core`, because they cannot depend on this crate. So this file
//! names items of `turso_core` by `turso_core::` paths, never by `crate::`,
//! and uses only crates that `turso_core` depends on.

use std::sync::Arc;
use turso_core::{Dialect, Result};
use turso_parser::{ast, parser::Parser};

pub use turso_core::dialect::{SQLITE_VERSION, SQLITE_VERSION_NUMBER};

/// The SQLite dialect.
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn parse(&self, sql: &str) -> Result<(Option<ast::Cmd>, usize)> {
        parse_first_statement(Parser::new(sql.as_bytes()))
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<ast::Cmd>, usize)> {
        parse_first_statement(Parser::new_for_stored_sql(sql.as_bytes()))
    }

    fn parse_internal_column_definition(&self, sql: &str) -> Result<ast::ColumnDefinition> {
        Ok(Parser::new(sql.as_bytes()).parse_column_definition(true)?)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }
}

/// Parses the first statement of the input of `parser`. Returns it with the
/// number of input bytes that it uses.
fn parse_first_statement(mut parser: Parser<'_>) -> Result<(Option<ast::Cmd>, usize)> {
    let cmd = parser.next_cmd()?;
    Ok((cmd, parser.offset()))
}
