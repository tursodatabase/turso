//! [`EngineDialect`] is the dialect of the statements the engine runs for
//! itself. The SQLite dialect that users open databases with is
//! `SqliteDialect` in the `turso_sqlite` crate.

use turso_parser::{ast, parser::Parser};

/// The dialect of the statements the engine runs for itself: SQLite SQL
/// with the built-in functions and catalog tables.
pub(crate) struct EngineDialect;

impl super::Dialect for EngineDialect {
    fn name(&self) -> &'static str {
        "engine"
    }

    fn parse(&self, sql: &str) -> crate::Result<(Option<ast::Cmd>, usize)> {
        self.parse_internal(sql)
    }

    fn parse_internal(&self, sql: &str) -> crate::Result<(Option<ast::Cmd>, usize)> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        Ok((cmd, parser.offset()))
    }

    fn parse_internal_column_definition(&self, sql: &str) -> crate::Result<ast::ColumnDefinition> {
        Ok(Parser::new(sql.as_bytes()).parse_column_definition(true)?)
    }
}
