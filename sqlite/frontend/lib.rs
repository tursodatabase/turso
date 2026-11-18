//! The SQLite frontend.
//!
//! [`SqliteDialect`] is the [`Dialect`] for SQLite: statements are SQLite
//! SQL and `sqlite_schema` rows are canonical SQLite text, parsed with the
//! SQLite parser.

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
        self.parse_internal(sql)
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<ast::Cmd>, usize)> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        Ok((cmd, parser.offset()))
    }

    fn parse_internal_column_definition(&self, sql: &str) -> Result<ast::ColumnDefinition> {
        Ok(Parser::new(sql.as_bytes()).parse_column_definition(true)?)
    }
}
