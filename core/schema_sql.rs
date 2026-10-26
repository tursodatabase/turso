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

/// Use source only when it describes the actual lowered statement. Synthesized
/// statements and frontend translations keep their existing AST formatting.
pub(crate) fn original_create_sql(input: &str, expected: &Stmt) -> Option<String> {
    object_name(expected)?;
    original_source_sql(input, |parsed| same_definition(parsed, expected))
}

pub(crate) fn original_table_sql(
    input: &str,
    name: &turso_parser::ast::QualifiedName,
    body: &turso_parser::ast::CreateTableBody,
) -> Option<String> {
    original_source_sql(input, |parsed| {
        matches!(parsed, Stmt::CreateTable { tbl_name, body: parsed_body, .. }
            if same_object_name(tbl_name, name) && parsed_body == body)
    })
}

fn original_source_sql(
    input: &str,
    matches_definition: impl FnOnce(&Stmt) -> bool,
) -> Option<String> {
    let mut parser = Parser::with_source(input.as_bytes());
    let parsed = match parser.next_cmd().ok()?? {
        Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan { stmt, .. } => stmt,
    };
    if !matches_definition(&parsed) {
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

fn same_definition(parsed: &Stmt, expected: &Stmt) -> bool {
    match (parsed, expected) {
        (
            Stmt::CreateTable {
                tbl_name: a,
                body: ab,
                ..
            },
            Stmt::CreateTable {
                tbl_name: b,
                body: bb,
                ..
            },
        ) => same_object_name(a, b) && ab == bb,
        (
            Stmt::CreateIndex {
                unique: au,
                idx_name: an,
                tbl_name: at,
                columns: ac,
                where_clause: aw,
                using: ax,
                with_clause: av,
                ..
            },
            Stmt::CreateIndex {
                unique: bu,
                idx_name: bn,
                tbl_name: bt,
                columns: bc,
                where_clause: bw,
                using: bx,
                with_clause: bv,
                ..
            },
        ) => {
            au == bu
                && same_object_name(an, bn)
                && at == bt
                && ac == bc
                && aw == bw
                && ax == bx
                && av == bv
        }
        (
            Stmt::CreateView {
                view_name: an,
                columns: ac,
                select: aq,
                ..
            },
            Stmt::CreateView {
                view_name: bn,
                columns: bc,
                select: bq,
                ..
            },
        ) => same_object_name(an, bn) && ac == bc && aq == bq,
        (
            Stmt::CreateTrigger {
                trigger_name: an,
                time: ai,
                event: ae,
                tbl_name: at,
                for_each_row: ar,
                when_clause: aw,
                commands: ac,
                ..
            },
            Stmt::CreateTrigger {
                trigger_name: bn,
                time: bi,
                event: be,
                tbl_name: bt,
                for_each_row: br,
                when_clause: bw,
                commands: bc,
                ..
            },
        ) => {
            same_object_name(an, bn)
                && ai == bi
                && ae == be
                && at == bt
                && ar == br
                && aw == bw
                && ac == bc
        }
        _ => false,
    }
}

fn same_object_name(
    a: &turso_parser::ast::QualifiedName,
    b: &turso_parser::ast::QualifiedName,
) -> bool {
    a.name == b.name && a.alias == b.alias
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statement(sql: &str) -> Stmt {
        let Some(Cmd::Stmt(stmt)) = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("valid CREATE")
        else {
            panic!("expected CREATE statement");
        };
        stmt
    }

    #[test]
    fn source_comparison_keeps_definition_fields() {
        for (original, changed) in [
            ("CREATE TABLE t(a DEFAULT 1)", "CREATE TABLE t(a DEFAULT 2)"),
            ("CREATE INDEX i ON t(a)", "CREATE UNIQUE INDEX i ON t(a)"),
            ("CREATE INDEX i ON t(a)", "CREATE INDEX i ON u(a)"),
            ("CREATE INDEX i ON t(a)", "CREATE INDEX i ON t(b)"),
            (
                "CREATE INDEX i ON t(a) WHERE a>0",
                "CREATE INDEX i ON t(a) WHERE a>1",
            ),
            ("CREATE VIEW v AS SELECT 1", "CREATE VIEW v AS SELECT 2"),
            (
                "CREATE VIEW v(a) AS SELECT 1",
                "CREATE VIEW v(b) AS SELECT 1",
            ),
            (
                "CREATE TRIGGER g BEFORE INSERT ON t BEGIN SELECT 1; END",
                "CREATE TRIGGER g AFTER INSERT ON t BEGIN SELECT 1; END",
            ),
            (
                "CREATE TRIGGER g BEFORE INSERT ON t BEGIN SELECT 1; END",
                "CREATE TRIGGER g BEFORE DELETE ON t BEGIN SELECT 1; END",
            ),
            (
                "CREATE TRIGGER g BEFORE INSERT ON t BEGIN SELECT 1; END",
                "CREATE TRIGGER g BEFORE INSERT ON u BEGIN SELECT 1; END",
            ),
            (
                "CREATE TRIGGER g BEFORE INSERT ON t WHEN 1 BEGIN SELECT 1; END",
                "CREATE TRIGGER g BEFORE INSERT ON t WHEN 0 BEGIN SELECT 1; END",
            ),
            (
                "CREATE TRIGGER g BEFORE INSERT ON t BEGIN SELECT 1; END",
                "CREATE TRIGGER g BEFORE INSERT ON t BEGIN SELECT 2; END",
            ),
        ] {
            assert!(same_definition(&statement(original), &statement(original)));
            assert!(!same_definition(&statement(original), &statement(changed)));
        }
        assert!(same_definition(
            &statement("CREATE TEMP TABLE IF NOT EXISTS temp.t(a)"),
            &statement("CREATE TABLE t(a)")
        ));
    }
}
