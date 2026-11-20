//! A dialect for unit tests that cannot parse SQL.
//!
//! Core has no SQL parser of its own; the frontends supply it. Unit tests
//! that need a dialect but no SQL use this one, so a test that parses by
//! accident fails with the SQL it was asked to parse.

use crate::dialect::Dialect;
use crate::sync::Arc;
use crate::{LimboError, Result};
use std::ops::Range;
use turso_parser::ast;

pub(crate) struct DialectThatCannotParse;

impl Dialect for DialectThatCannotParse {
    fn name(&self) -> &'static str {
        "cannot-parse"
    }

    fn parse(&self, sql: &str) -> Result<(Option<ast::Cmd>, usize)> {
        self.parse_internal(sql)
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<ast::Cmd>, usize)> {
        Err(asked_to_parse(sql))
    }

    fn parse_internal_column_definition(&self, sql: &str) -> Result<ast::ColumnDefinition> {
        Err(asked_to_parse(sql))
    }

    fn parameter_marker_ranges(&self, sql: &str) -> Result<Vec<Range<usize>>> {
        Err(asked_to_parse(sql))
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DialectThatCannotParse)
    }
}

fn asked_to_parse(sql: &str) -> LimboError {
    LimboError::ParseError(format!("asked to parse: {sql}"))
}
