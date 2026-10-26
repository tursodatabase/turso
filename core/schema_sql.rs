//! Source text retained by SQLite-style schema entries.

/// Replace only identifier tokens selected by the existing semantic rewriters.
pub(crate) fn renamed_sql(input: &str, stmt: &Stmt, new_name: &Name) -> Option<String> {
    let mut parser = Parser::with_source(input.as_bytes());
    parser.next_cmd().ok()??;
    struct Renames<'a, 'b> {
        input: &'a str,
        names: &'b [&'a [u8]],
        ranges: Vec<(std::ops::Range<usize>, String)>,
        new_name: &'b Name,
        has_source: bool,
    }
    impl TokenStream for Renames<'_, '_> {
        type Error = Infallible;
        fn append(&mut self, _: TokenType, _: Option<&str>) -> Result<(), Infallible> {
            Ok(())
        }
        fn identifier(&mut self, name: &Name) -> Result<(), Infallible> {
            if let Some(index) = name.source_token() {
                self.has_source = true;
                if name.source_was_rewritten() {
                    let token = self.names[index];
                    let start = token.as_ptr() as usize - self.input.as_ptr() as usize;
                    let is_requested = name.as_str().eq_ignore_ascii_case(self.new_name.as_str());
                    let replacement = if is_requested {
                        self.new_name.as_str()
                    } else {
                        name.as_str()
                    };
                    let quoted = if is_requested {
                        if self.new_name.quoted() {
                            format!("\"{}\"", replacement.replace('"', "\"\""))
                        } else {
                            replacement.to_owned()
                        }
                    } else {
                        Name::exact(replacement.to_owned()).as_ident()
                    };
                    self.ranges.push((start..start + token.len(), quoted));
                }
            }
            Ok(())
        }
    }
    let mut renames = Renames {
        input,
        names: parser.source_names()?,
        ranges: Vec::new(),
        has_source: false,
        new_name,
    };
    stmt.to_tokens(&mut renames, &BlankContext).unwrap();
    if !renames.has_source {
        return None;
    }
    renames
        .ranges
        .sort_unstable_by_key(|(range, _)| (range.start, range.end));
    renames.ranges.dedup_by(|a, b| a.0 == b.0);
    let mut result = input.to_owned();
    for (range, quoted) in renames.ranges.into_iter().rev() {
        result.replace_range(range, &quoted);
    }
    Some(result)
}

use std::convert::Infallible;
use turso_parser::ast::fmt::{BlankContext, ToTokens, TokenStream};
use turso_parser::ast::{Cmd, Name, Stmt};
use turso_parser::parser::Parser;
use turso_parser::token::TokenType;

fn object_name(stmt: &Stmt) -> Option<(&Name, &'static str)> {
    match stmt {
        Stmt::CreateTable { tbl_name, .. } => Some((&tbl_name.name, "CREATE TABLE")),
        Stmt::CreateIndex {
            idx_name, unique, ..
        } => Some((
            &idx_name.name,
            if *unique {
                "CREATE UNIQUE INDEX"
            } else {
                "CREATE INDEX"
            },
        )),
        Stmt::CreateTrigger { trigger_name, .. } => Some((&trigger_name.name, "CREATE TRIGGER")),
        Stmt::CreateView { view_name, .. } => Some((&view_name.name, "CREATE VIEW")),
        _ => None,
    }
}

fn comparison_form(mut stmt: Stmt) -> Option<Stmt> {
    match &mut stmt {
        Stmt::CreateTable {
            tbl_name,
            temporary,
            if_not_exists,
            ..
        } => {
            tbl_name.namespace = None;
            *temporary = false;
            *if_not_exists = false;
        }
        Stmt::CreateIndex {
            idx_name,
            if_not_exists,
            ..
        } => {
            idx_name.namespace = None;
            *if_not_exists = false;
        }
        Stmt::CreateView {
            view_name,
            temporary,
            if_not_exists,
            ..
        } => {
            view_name.namespace = None;
            *temporary = false;
            *if_not_exists = false;
        }
        Stmt::CreateTrigger {
            trigger_name,
            temporary,
            if_not_exists,
            ..
        } => {
            trigger_name.namespace = None;
            *temporary = false;
            *if_not_exists = false;
        }
        _ => return None,
    }
    Some(stmt)
}

/// Use source only when it describes the actual lowered statement. Synthesized
/// statements and frontend translations keep their existing AST formatting.
pub(crate) fn original_create_sql(input: &str, expected: &Stmt) -> Option<String> {
    object_name(expected)?;
    let mut parser = Parser::with_source(input.as_bytes());
    let parsed = match parser.next_cmd().ok()?? {
        Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan { stmt, .. } => stmt,
    };
    if comparison_form(parsed.clone()) != comparison_form(expected.clone()) {
        return None;
    }
    let (name, prefix) = object_name(&parsed)?;
    let token = *parser.source_names()?.get(name.source_token()?)?;
    let start = token.as_ptr() as usize - input.as_ptr() as usize;
    Some(format!(
        "{prefix} {}",
        &input[start..parser.statement_end()]
    ))
}
