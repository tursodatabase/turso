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

use std::{ops::Range, sync::Arc};
use turso_core::{Dialect, Result};
use turso_parser::{ast, lexer::Lexer, parser::Parser, token::TokenType};

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

    fn parameter_marker_ranges(&self, sql: &str) -> Result<Vec<Range<usize>>> {
        let mut markers = Vec::new();
        for token in Lexer::new(sql.as_bytes()) {
            let token = token?;
            if token.token_type == TokenType::TK_VARIABLE {
                let start = token.value.as_ptr() as usize - sql.as_ptr() as usize;
                markers.push(start..start + token.value.len());
            }
        }
        Ok(markers)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }
}
