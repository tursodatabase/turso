//! Dialect definitions that the frontends share.
//!
//! [`SqliteDialect`] parses SQLite SQL and keeps the default rules of
//! [`Dialect`]. It is the dialect of the SQLite frontend. Every frontend
//! also uses it for the statements that core and the frontend run for
//! themselves, because those statements are SQLite syntax in every database.

use std::{ops::Range, sync::Arc};
use turso_core::{ast, Dialect, Result};
use turso_parser::{lexer::Lexer, parser::Parser, token::TokenType};

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
