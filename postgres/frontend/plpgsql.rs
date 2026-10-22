//! Compile a PL/pgSQL trigger function to the commands of an engine trigger.
//!
//! `pg_query::parse_plpgsql` gives the parse tree of the function as JSON.
//! The compiler walks the tree and translates each SQL statement in it with
//! the PostgreSQL translator. A function is compiled again for each trigger
//! and each event, so each `TG_*` variable is a constant and becomes a
//! literal.

use serde_json::Value as Json;
use turso_core::dialect::TriggerFunctionCall;
use turso_core::schema::Schema;
use turso_core::{walk_stmt_expressions_mut, LimboError, Result, WalkControl};
use turso_parser::ast::{self, Expr, Literal, TriggerCmd, TriggerEvent, TriggerTime};
use turso_pg_parser::function::PgFunction;
use turso_pg_parser::translator::PostgreSQLTranslator;

/// What is constant for one trigger and one event.
pub(crate) struct TriggerContext<'a> {
    pub trigger_name: &'a str,
    pub table_name: &'a str,
    pub table_schema: &'a str,
    /// `BEFORE` or `AFTER`
    pub timing: &'a str,
    /// `INSERT`, `UPDATE` or `DELETE`
    pub event: &'a str,
    pub args: &'a [String],
}

/// The commands of the trigger `call`, compiled from its function.
pub(crate) fn expand_trigger_function(
    schema: &Schema,
    call: &TriggerFunctionCall<'_>,
) -> Result<Vec<TriggerCmd>> {
    let name = call.function.name.as_str();
    let Some(stored) = schema.get_user_function(name, 0) else {
        return Err(parse_error(format!("function {name}() does not exist")));
    };
    let function = PgFunction::parse(&stored.sql).map_err(|e| parse_error(e.to_string()))?;
    if !function.is_trigger() {
        return Err(parse_error(format!(
            "function {name} must return type trigger"
        )));
    }
    if function.language != "plpgsql" {
        return Err(parse_error(format!(
            "trigger function {name} is written in {}: only PL/pgSQL trigger functions are supported",
            function.language
        )));
    }
    let context = TriggerContext {
        trigger_name: &call.trigger.name,
        table_name: &call.table.name,
        table_schema: call.database_name.unwrap_or("public"),
        timing: match call.trigger.time {
            TriggerTime::Before => "BEFORE",
            TriggerTime::After => "AFTER",
            TriggerTime::InsteadOf => "INSTEAD OF",
        },
        event: match call.trigger.event {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update | TriggerEvent::UpdateOf(_) => "UPDATE",
            TriggerEvent::Delete => "DELETE",
        },
        args: call.args,
    };
    compile_trigger_function(&stored.sql, &context)
}

/// Compile the PL/pgSQL function that `function_sql` creates for the
/// trigger of `context`.
pub(crate) fn compile_trigger_function(
    function_sql: &str,
    context: &TriggerContext<'_>,
) -> Result<Vec<TriggerCmd>> {
    let parsed =
        turso_pg_parser::parse_plpgsql(function_sql).map_err(|e| parse_error(e.to_string()))?;
    let function = parsed
        .get(0)
        .and_then(|function| function.get("PLpgSQL_function"))
        .ok_or_else(|| parse_error("PL/pgSQL function has no parse tree".to_string()))?;

    for datum in json_array(function.get("datums")) {
        if let Some(variable) = datum.get("PLpgSQL_var") {
            if variable.get("lineno").is_some() {
                return Err(not_supported_yet(&format!(
                    "the variable {} of DECLARE",
                    json_str(variable.get("refname"))
                )));
            }
        }
    }

    let body = function
        .get("action")
        .and_then(|action| action.get("PLpgSQL_stmt_block"))
        .ok_or_else(|| parse_error("PL/pgSQL function has no body".to_string()))?;
    let mut compiler = Compiler {
        context,
        translator: PostgreSQLTranslator::new(),
        commands: Vec::new(),
    };
    compiler.compile_block(body)?;
    Ok(compiler.commands)
}

struct Compiler<'a> {
    context: &'a TriggerContext<'a>,
    translator: PostgreSQLTranslator,
    commands: Vec<TriggerCmd>,
}

impl Compiler<'_> {
    fn compile_block(&mut self, block: &Json) -> Result<()> {
        if block.get("exceptions").is_some() {
            return Err(not_supported_yet("EXCEPTION"));
        }
        for statement in json_array(block.get("body")) {
            if !self.compile_statement(statement)? {
                break;
            }
        }
        Ok(())
    }

    /// Compile one statement. Returns false when the statements after it
    /// never run.
    fn compile_statement(&mut self, statement: &Json) -> Result<bool> {
        let Some((kind, node)) = statement
            .as_object()
            .and_then(|object| object.iter().next())
        else {
            return Err(parse_error("PL/pgSQL statement has no kind".to_string()));
        };
        match kind.as_str() {
            "PLpgSQL_stmt_execsql" => {
                if node.get("into").is_some() {
                    return Err(not_supported_yet("SELECT INTO"));
                }
                let query = json_str(
                    node.get("sqlstmt")
                        .and_then(|e| e.get("PLpgSQL_expr"))
                        .and_then(|e| e.get("query")),
                );
                let command = self.compile_sql(query)?;
                self.commands.push(command);
                Ok(true)
            }
            "PLpgSQL_stmt_return" => {
                let value = node
                    .get("expr")
                    .and_then(|e| e.get("PLpgSQL_expr"))
                    .and_then(|e| e.get("query"))
                    .and_then(Json::as_str)
                    .map(|query| query.trim().to_ascii_uppercase());
                if self.context.timing == "BEFORE" && value.as_deref() != Some("NEW") {
                    return Err(not_supported_yet(&format!(
                        "RETURN {} in a BEFORE trigger",
                        value.unwrap_or_default()
                    )));
                }
                Ok(false)
            }
            "PLpgSQL_stmt_block" => {
                self.compile_block(node)?;
                Ok(true)
            }
            "PLpgSQL_stmt_perform" => Err(not_supported_yet("PERFORM")),
            "PLpgSQL_stmt_assign" => Err(not_supported_yet("assignment")),
            "PLpgSQL_stmt_if" => Err(not_supported_yet("IF")),
            "PLpgSQL_stmt_case" => Err(not_supported_yet("CASE")),
            "PLpgSQL_stmt_raise" => Err(not_supported_yet("RAISE")),
            "PLpgSQL_stmt_loop"
            | "PLpgSQL_stmt_while"
            | "PLpgSQL_stmt_fori"
            | "PLpgSQL_stmt_fors"
            | "PLpgSQL_stmt_forc"
            | "PLpgSQL_stmt_foreach_a"
            | "PLpgSQL_stmt_exit" => Err(not_supported("a loop")),
            "PLpgSQL_stmt_open" | "PLpgSQL_stmt_fetch" | "PLpgSQL_stmt_close" => {
                Err(not_supported("a cursor"))
            }
            "PLpgSQL_stmt_dynexecute" | "PLpgSQL_stmt_dynfors" => Err(not_supported("EXECUTE")),
            "PLpgSQL_stmt_return_query" | "PLpgSQL_stmt_return_next" => {
                Err(not_supported("RETURN QUERY"))
            }
            other => Err(not_supported(other.trim_start_matches("PLpgSQL_stmt_"))),
        }
    }

    /// Translate one SQL statement of the function body to a trigger command.
    fn compile_sql(&self, query: &str) -> Result<TriggerCmd> {
        let parse_result = turso_pg_parser::parse(query).map_err(|e| parse_error(e.to_string()))?;
        let mut stmt = self
            .translator
            .translate(&parse_result)
            .map_err(|e| parse_error(e.to_string()))?;
        self.replace_trigger_variables(&mut stmt)?;
        match stmt {
            ast::Stmt::Insert {
                with: None,
                or_conflict,
                tbl_name,
                columns,
                body: ast::InsertBody::Select(select, upsert),
                returning,
            } => {
                if !returning.is_empty() {
                    return Err(no_destination());
                }
                Ok(TriggerCmd::Insert {
                    or_conflict,
                    tbl_name: self.local_table_name(tbl_name)?,
                    col_names: columns,
                    select,
                    upsert,
                    returning: vec![],
                })
            }
            ast::Stmt::Update(ast::Update {
                with: None,
                or_conflict,
                tbl_name,
                indexed: None,
                sets,
                from,
                where_clause,
                returning,
            }) => {
                if !returning.is_empty() {
                    return Err(no_destination());
                }
                Ok(TriggerCmd::Update {
                    or_conflict,
                    tbl_name: self.local_table_name(tbl_name)?,
                    sets,
                    from,
                    where_clause,
                })
            }
            ast::Stmt::Delete {
                with: None,
                tbl_name,
                indexed: None,
                where_clause,
                returning,
            } => {
                if !returning.is_empty() {
                    return Err(no_destination());
                }
                Ok(TriggerCmd::Delete {
                    tbl_name: self.local_table_name(tbl_name)?,
                    where_clause,
                })
            }
            ast::Stmt::Select(_) => Err(no_destination()),
            other => Err(not_supported(&format!(
                "the statement {} in a trigger function",
                statement_kind(&other)
            ))),
        }
    }

    /// The table name without its schema: a trigger reads and writes tables
    /// of its own schema only.
    fn local_table_name(&self, name: ast::QualifiedName) -> Result<ast::Name> {
        if let Some(schema) = &name.namespace {
            let is_own_schema = schema.as_str().eq_ignore_ascii_case("main")
                || schema
                    .as_str()
                    .eq_ignore_ascii_case(self.context.table_schema);
            if !is_own_schema {
                return Err(not_supported(&format!(
                    "the table {}.{} of another schema in a trigger function",
                    schema.as_str(),
                    name.name.as_str()
                )));
            }
        }
        Ok(name.name)
    }

    /// Each `TG_*` variable is a constant for this trigger and event.
    fn replace_trigger_variables(&self, stmt: &mut ast::Stmt) -> Result<()> {
        let context = self.context;
        walk_stmt_expressions_mut(stmt, &mut |expr| {
            let replacement = match expr {
                Expr::Id(id) => match id.as_str().to_ascii_lowercase().as_str() {
                    "tg_op" => Some(text(context.event)),
                    "tg_when" => Some(text(context.timing)),
                    "tg_level" => Some(text("ROW")),
                    "tg_name" => Some(text(context.trigger_name)),
                    "tg_table_name" | "tg_relname" => Some(text(context.table_name)),
                    "tg_table_schema" => Some(text(context.table_schema)),
                    "tg_nargs" => Some(Expr::Literal(Literal::Numeric(
                        context.args.len().to_string(),
                    ))),
                    "tg_relid" => return Err(not_supported("TG_RELID")),
                    "tg_argv" => return Err(not_supported("TG_ARGV without an index")),
                    _ => None,
                },
                Expr::FunctionCall { name, args, .. }
                    if name.as_str().eq_ignore_ascii_case("array_element")
                        && args.len() == 2
                        && matches!(args[0].as_ref(), Expr::Id(id) if id.as_str().eq_ignore_ascii_case("tg_argv")) =>
                {
                    let Expr::Literal(Literal::Numeric(index)) = args[1].as_ref() else {
                        return Err(not_supported("TG_ARGV with an index that is not a number"));
                    };
                    let index: usize = index
                        .parse()
                        .map_err(|_| not_supported("TG_ARGV with an index that is not a number"))?;
                    Some(match context.args.get(index) {
                        Some(arg) => text(arg),
                        None => Expr::Literal(Literal::Null),
                    })
                }
                _ => None,
            };
            if let Some(replacement) = replacement {
                *expr = replacement;
                return Ok(WalkControl::SkipChildren);
            }
            Ok(WalkControl::Continue)
        })
    }
}

fn text(value: &str) -> Expr {
    Expr::Literal(Literal::String(format!("'{}'", value.replace('\'', "''"))))
}

fn json_array(value: Option<&Json>) -> &[Json] {
    value.and_then(Json::as_array).map_or(&[], Vec::as_slice)
}

fn json_str(value: Option<&Json>) -> &str {
    value.and_then(Json::as_str).unwrap_or_default()
}

fn statement_kind(stmt: &ast::Stmt) -> &'static str {
    match stmt {
        ast::Stmt::Insert { .. } => "INSERT",
        ast::Stmt::Update(_) => "UPDATE",
        ast::Stmt::Delete { .. } => "DELETE",
        ast::Stmt::Select(_) => "SELECT",
        ast::Stmt::CreateTable { .. } => "CREATE TABLE",
        ast::Stmt::DropTable { .. } => "DROP TABLE",
        ast::Stmt::Begin { .. } | ast::Stmt::Commit { .. } | ast::Stmt::Rollback { .. } => {
            "transaction control"
        }
        _ => "DDL",
    }
}

fn no_destination() -> LimboError {
    parse_error("query has no destination for result data".to_string())
}

fn not_supported_yet(what: &str) -> LimboError {
    parse_error(format!("{what} is not supported yet in trigger functions"))
}

fn not_supported(what: &str) -> LimboError {
    parse_error(format!("{what} is not supported in trigger functions"))
}

fn parse_error(message: String) -> LimboError {
    LimboError::ParseError(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile(body: &str, event: &str) -> Result<Vec<String>> {
        let function_sql = format!(
            "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {body} END $$"
        );
        let context = TriggerContext {
            trigger_name: "trg",
            table_name: "t",
            table_schema: "public",
            timing: "AFTER",
            event,
            args: &["a".to_string(), "b".to_string()],
        };
        Ok(compile_trigger_function(&function_sql, &context)?
            .iter()
            .map(|command| command.to_string())
            .collect())
    }

    #[test]
    fn sql_statements_become_trigger_commands() {
        assert_eq!(
            compile(
                "INSERT INTO log (id, op) VALUES (NEW.id, TG_OP); \
                 UPDATE counts SET n = n + 1 WHERE name = TG_TABLE_NAME; \
                 DELETE FROM old_rows WHERE id = OLD.id; \
                 RETURN NEW;",
                "UPDATE"
            )
            .unwrap(),
            vec![
                "INSERT INTO log (id, op) VALUES (new.id, 'UPDATE')",
                "UPDATE counts SET n = n + 1 WHERE name = 't'",
                "DELETE FROM old_rows WHERE id = old.id",
            ]
        );
    }

    #[test]
    fn trigger_variables_are_constants() {
        assert_eq!(
            compile(
                "INSERT INTO log VALUES (TG_WHEN, TG_LEVEL, TG_NAME, TG_TABLE_SCHEMA, TG_NARGS, \
                 TG_ARGV[0], TG_ARGV[1], TG_ARGV[2]);",
                "INSERT"
            )
            .unwrap(),
            vec!["INSERT INTO log VALUES ('AFTER', 'ROW', 'trg', 'public', 2, 'a', 'b', NULL)"]
        );
    }

    #[test]
    fn statements_after_return_are_dropped() {
        assert_eq!(
            compile("RETURN NULL; INSERT INTO log VALUES (1);", "INSERT").unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn unsupported_constructs_are_errors() {
        for (body, message) in [
            ("SELECT 1;", "no destination"),
            (
                "IF NEW.x > 1 THEN RETURN NEW; END IF; RETURN NEW;",
                "IF is not supported yet",
            ),
            ("PERFORM f(); RETURN NEW;", "PERFORM is not supported yet"),
            ("NEW.x := 1; RETURN NEW;", "assignment is not supported yet"),
            (
                "SELECT 1 INTO NEW.x; RETURN NEW;",
                "SELECT INTO is not supported yet",
            ),
            (
                "RAISE NOTICE 'x'; RETURN NEW;",
                "RAISE is not supported yet",
            ),
            (
                "LOOP EXIT; END LOOP; RETURN NEW;",
                "a loop is not supported",
            ),
            (
                "EXECUTE 'SELECT 1'; RETURN NEW;",
                "EXECUTE is not supported",
            ),
            (
                "INSERT INTO other.t VALUES (1); RETURN NEW;",
                "another schema",
            ),
            (
                "CREATE TABLE x (a int); RETURN NEW;",
                "CREATE TABLE in a trigger function",
            ),
        ] {
            let err = compile(body, "INSERT").unwrap_err();
            assert!(err.to_string().contains(message), "{body}: {err}");
        }
    }

    #[test]
    fn declared_variables_are_not_supported_yet() {
        let function_sql = "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ \
                            DECLARE v int; BEGIN RETURN NEW; END $$";
        let context = TriggerContext {
            trigger_name: "trg",
            table_name: "t",
            table_schema: "public",
            timing: "AFTER",
            event: "INSERT",
            args: &[],
        };
        let err = compile_trigger_function(function_sql, &context).unwrap_err();
        assert!(err.to_string().contains("variable v of DECLARE"), "{err}");
    }
}
