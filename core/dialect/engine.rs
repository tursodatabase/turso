//! [`EngineDialect`] is the dialect that the unit tests of core open
//! databases with. The SQLite dialect that users open databases with is
//! `SqliteDialect` in the `turso_sqlite` crate, which core cannot depend on.

use std::sync::Arc;
use turso_parser::{ast, parser::Parser};

/// SQLite SQL with the built-in functions and catalog tables.
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

    fn get_internal_dialect(&self) -> Arc<dyn super::Dialect> {
        Arc::new(EngineDialect)
    }
}
