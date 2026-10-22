//! Compile a PL/pgSQL trigger function to the commands of an engine trigger.
//!
//! `pg_query::parse_plpgsql` gives the parse tree of the function as JSON.
//! The compiler walks the tree and translates each SQL statement in it with
//! the PostgreSQL translator. A function is compiled again for each trigger
//! and each event, so each `TG_*` variable is a constant and becomes a
//! literal.

use serde_json::Value as Json;
use turso_core::dialect::{TriggerFunctionBody, TriggerFunctionCall, TriggerReturn};
use turso_core::schema::Schema;
use turso_core::{
    walk_expr_with_subqueries_mut, walk_stmt_expressions_mut, LimboError, Result, WalkControl,
};
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
    /// The columns of the table, in table order.
    pub columns: &'a [TriggerColumn],
}

pub(crate) struct TriggerColumn {
    pub name: String,
    /// The declared type, as the engine keeps it.
    pub type_name: String,
    pub is_generated: bool,
    /// The column is the INTEGER PRIMARY KEY, which is the rowid.
    pub is_rowid_alias: bool,
}

/// The body of the trigger `call`, compiled from its function.
pub(crate) fn expand_trigger_function(
    schema: &Schema,
    call: &TriggerFunctionCall<'_>,
) -> Result<TriggerFunctionBody> {
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
    let columns: Vec<TriggerColumn> = call
        .table
        .columns()
        .iter()
        .map(|column| TriggerColumn {
            name: column.name.clone().unwrap_or_default(),
            type_name: column.ty_str.clone(),
            is_generated: column.is_virtual_generated(),
            is_rowid_alias: column.is_rowid_alias(),
        })
        .collect();
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
        columns: &columns,
    };
    compile_trigger_function(&stored.sql, &context)
}

/// Compile the PL/pgSQL function that `function_sql` creates for the
/// trigger of `context`.
pub(crate) fn compile_trigger_function(
    function_sql: &str,
    context: &TriggerContext<'_>,
) -> Result<TriggerFunctionBody> {
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
        datums: json_array(function.get("datums")),
        commands: Vec::new(),
        new_values: vec![None; context.columns.len()],
        returned_row: None,
    };
    compiler.compile_block(body)?;
    let Some(returned_row) = compiler.returned_row else {
        return Err(parse_error(
            "control reached end of trigger procedure without RETURN".to_string(),
        ));
    };
    Ok(TriggerFunctionBody {
        commands: compiler.commands,
        returned_row,
    })
}

struct Compiler<'a> {
    context: &'a TriggerContext<'a>,
    translator: PostgreSQLTranslator,
    /// The variables of the function: `varno` of an assignment is an index
    /// into this list.
    datums: &'a [Json],
    commands: Vec<TriggerCmd>,
    /// The value of each column of NEW after the assignments so far, as an
    /// expression over the NEW and OLD rows of the statement. `None` when
    /// no assignment changed the column.
    new_values: Vec<Option<Expr>>,
    /// What RETURN returned.
    returned_row: Option<TriggerReturn>,
}

impl Compiler<'_> {
    /// Compile a block. Returns false when the block ends with RETURN, so
    /// the statements after it never run.
    fn compile_block(&mut self, block: &Json) -> Result<bool> {
        if block.get("exceptions").is_some() {
            return Err(not_supported_yet("EXCEPTION"));
        }
        for statement in json_array(block.get("body")) {
            if !self.compile_statement(statement)? {
                return Ok(false);
            }
        }
        Ok(true)
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
                if value.is_none() && node.get("lineno").is_none() {
                    // The parser adds a RETURN without a value and without a
                    // line to a body that does not end with RETURN.
                    return Err(parse_error(
                        "control reached end of trigger procedure without RETURN".to_string(),
                    ));
                }
                self.returned_row = Some(self.returned_row(value.as_deref())?);
                Ok(false)
            }
            "PLpgSQL_stmt_block" => self.compile_block(node),
            "PLpgSQL_stmt_assign" => {
                let varno = node.get("varno").and_then(Json::as_u64).unwrap_or(0) as usize;
                let expr = node.get("expr").and_then(|e| e.get("PLpgSQL_expr"));
                let query = json_str(expr.and_then(|e| e.get("query")));
                let name_parts = expr
                    .and_then(|e| e.get("parseMode"))
                    .and_then(Json::as_u64)
                    .unwrap_or(0)
                    .saturating_sub(2) as usize;
                let Some(value) = assigned_value(query, name_parts) else {
                    return Err(parse_error(format!("invalid assignment: {query}")));
                };
                self.compile_assignment(varno, value)?;
                Ok(true)
            }
            "PLpgSQL_stmt_perform" => Err(not_supported_yet("PERFORM")),
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

    /// What the trigger returns for `RETURN <value>`: the row a BEFORE
    /// trigger returns is written in place of the row of the statement.
    /// `RETURN` without a value returns NULL.
    fn returned_row(&self, value: Option<&str>) -> Result<TriggerReturn> {
        if self.context.timing != "BEFORE" {
            return Ok(TriggerReturn::Unchanged);
        }
        match (self.context.event, value.unwrap_or("NULL")) {
            ("DELETE", "OLD") => Ok(TriggerReturn::Unchanged),
            ("DELETE", "NEW" | "NULL") | ("INSERT", "OLD") | (_, "NULL") => Ok(TriggerReturn::Skip),
            ("UPDATE", "OLD") => Ok(TriggerReturn::Row {
                skip_when: None,
                columns: self
                    .context
                    .columns
                    .iter()
                    .map(|column| {
                        (!column.is_generated && !column.is_rowid_alias).then(|| {
                            Expr::Qualified(
                                ast::Name::exact("old".to_string()),
                                ast::Name::exact(column.name.clone()),
                            )
                        })
                    })
                    .collect(),
            }),
            (_, "NEW") if self.new_values.iter().all(Option::is_none) => {
                Ok(TriggerReturn::Unchanged)
            }
            (_, "NEW") => Ok(TriggerReturn::Row {
                skip_when: None,
                columns: self.new_values.clone(),
            }),
            (_, other) => Err(not_supported_yet(&format!("RETURN {other}"))),
        }
    }

    /// `NEW.column := value`: the value, as an expression over the rows of
    /// the statement, replaces the column in the statements that follow and
    /// in the returned row.
    fn compile_assignment(&mut self, varno: usize, value: &str) -> Result<()> {
        let (record, field) = self.assignment_target(varno)?;
        if !record.eq_ignore_ascii_case("new") {
            return Err(not_supported(&format!(
                "assignment to {}.{field}",
                record.to_ascii_uppercase()
            )));
        }
        if self.context.event == "DELETE" {
            return Err(parse_error(
                "record \"new\" is not assigned yet in a DELETE trigger".to_string(),
            ));
        }
        let Some(column) = self.column_index(&field) else {
            return Err(parse_error(format!(
                "record \"new\" has no field \"{field}\""
            )));
        };
        if self.context.columns[column].is_generated {
            return Err(parse_error(format!(
                "a trigger cannot change the generated column {field}"
            )));
        }
        let select = format!("SELECT {value}");
        let parse_result =
            turso_pg_parser::parse(&select).map_err(|e| parse_error(e.to_string()))?;
        let stmt = self
            .translator
            .translate(&parse_result)
            .map_err(|e| parse_error(e.to_string()))?;
        let ast::Stmt::Select(select) = stmt else {
            return Err(parse_error(format!("invalid assignment value: {value}")));
        };
        let ast::OneSelect::Select { mut columns, .. } = select.body.select else {
            return Err(parse_error(format!("invalid assignment value: {value}")));
        };
        let Some(ast::ResultColumn::Expr(mut expr, _)) = columns.pop() else {
            return Err(parse_error(format!("invalid assignment value: {value}")));
        };
        walk_expr_with_subqueries_mut(&mut expr, &mut |expr| self.rewrite_trigger_reference(expr))?;
        self.new_values[column] = Some(cast_to_column_type(
            *expr,
            &self.context.columns[column].type_name,
        ));
        Ok(())
    }

    /// The record and field that the variable `varno` of an assignment
    /// names, such as `("new", "updated_at")`.
    fn assignment_target(&self, varno: usize) -> Result<(String, String)> {
        let datum = self
            .datums
            .get(varno)
            .ok_or_else(|| parse_error(format!("unknown assignment target {varno}")))?;
        if let Some(variable) = datum.get("PLpgSQL_var") {
            return Err(not_supported_yet(&format!(
                "assignment to the variable {}",
                json_str(variable.get("refname"))
            )));
        }
        let Some(field) = datum.get("PLpgSQL_recfield") else {
            return Err(not_supported_yet("this assignment target"));
        };
        let parent = field
            .get("recparentno")
            .and_then(Json::as_u64)
            .and_then(|parent| self.datums.get(parent as usize))
            .and_then(|parent| parent.get("PLpgSQL_rec"))
            .ok_or_else(|| parse_error(format!("unknown assignment target {varno}")))?;
        Ok((
            json_str(parent.get("refname")).to_string(),
            json_str(field.get("fieldname")).to_string(),
        ))
    }

    fn column_index(&self, name: &str) -> Option<usize> {
        self.context
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
    }

    /// Translate one SQL statement of the function body to a trigger command.
    fn compile_sql(&self, query: &str) -> Result<TriggerCmd> {
        let parse_result = turso_pg_parser::parse(query).map_err(|e| parse_error(e.to_string()))?;
        let mut stmt = self
            .translator
            .translate(&parse_result)
            .map_err(|e| parse_error(e.to_string()))?;
        walk_stmt_expressions_mut(&mut stmt, &mut |expr| self.rewrite_trigger_reference(expr))?;
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

    /// Each `TG_*` variable is a constant for this trigger and event, and
    /// `NEW.column` is the value the assignments so far gave the column.
    fn rewrite_trigger_reference(&self, expr: &mut Expr) -> Result<WalkControl> {
        let context = self.context;
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
            Expr::Qualified(record, column) if record.as_str().eq_ignore_ascii_case("new") => self
                .column_index(column.as_str())
                .and_then(|column| self.new_values[column].clone())
                .map(parenthesized),
            _ => None,
        };
        if let Some(replacement) = replacement {
            *expr = replacement;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    }
}

/// The value of an assignment `target := value` or `target = value`, where
/// the target is `name_parts` names separated by dots.
fn assigned_value(query: &str, name_parts: usize) -> Option<&str> {
    let mut rest = query.trim_start();
    for part in 0..name_parts {
        if part > 0 {
            rest = rest.strip_prefix('.')?.trim_start();
        }
        rest = if let Some(quoted) = rest.strip_prefix('"') {
            let mut end = 0;
            loop {
                let close = quoted[end..].find('"')?;
                end += close + 1;
                if !quoted[end..].starts_with('"') {
                    break;
                }
                end += 1;
            }
            &quoted[end..]
        } else {
            let end = rest
                .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
                .unwrap_or(rest.len());
            if end == 0 {
                return None;
            }
            &rest[end..]
        };
        rest = rest.trim_start();
    }
    let value = rest.strip_prefix(":=").or_else(|| rest.strip_prefix('='))?;
    Some(value.trim())
}

/// The assigned value gets the cast that the translator gives `value::type`
/// for the type of the column. A column of a custom type keeps the
/// conversion of that type.
fn cast_to_column_type(expr: Expr, type_name: &str) -> Expr {
    let cast_type = match type_name.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" => "INTEGER",
        "REAL" => "REAL",
        "TEXT" => "TEXT",
        "BLOB" => "BLOB",
        _ => return expr,
    };
    Expr::Cast {
        expr: Box::new(expr),
        type_name: Some(ast::Type {
            name: cast_type.to_string(),
            size: None,
            array_dimensions: 0,
        }),
    }
}

/// An expression put in place of a column reference keeps its meaning
/// inside a larger expression.
fn parenthesized(expr: Expr) -> Expr {
    match expr {
        Expr::Literal(_)
        | Expr::Id(_)
        | Expr::Qualified(..)
        | Expr::DoublyQualified(..)
        | Expr::FunctionCall { .. }
        | Expr::FunctionCallStar { .. }
        | Expr::Cast { .. }
        | Expr::Parenthesized(_)
        | Expr::Subquery(_) => expr,
        other => Expr::Parenthesized(vec![Box::new(other)]),
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

    fn columns() -> Vec<TriggerColumn> {
        let column =
            |name: &str, type_name: &str, is_generated: bool, is_rowid_alias: bool| TriggerColumn {
                name: name.to_string(),
                type_name: type_name.to_string(),
                is_generated,
                is_rowid_alias,
            };
        vec![
            column("id", "INTEGER", false, true),
            column("x", "INTEGER", false, false),
            column("y", "timestamptz", false, false),
            column("Camel", "TEXT", false, false),
            column("doubled", "INTEGER", true, false),
        ]
    }

    fn compile_for(body: &str, timing: &str, event: &str) -> Result<(Vec<String>, String)> {
        let function_sql = format!(
            "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {body} END $$"
        );
        let columns = columns();
        let context = TriggerContext {
            trigger_name: "trg",
            table_name: "t",
            table_schema: "public",
            timing,
            event,
            args: &["a".to_string(), "b".to_string()],
            columns: &columns,
        };
        let body = compile_trigger_function(&function_sql, &context)?;
        let returned = match body.returned_row {
            TriggerReturn::Unchanged => "unchanged".to_string(),
            TriggerReturn::Skip => "skip".to_string(),
            TriggerReturn::Row { skip_when, columns } => {
                let columns: Vec<String> = columns
                    .iter()
                    .zip(context.columns)
                    .filter_map(|(value, column)| {
                        value
                            .as_ref()
                            .map(|value| format!("{} = {value}", column.name))
                    })
                    .collect();
                match skip_when {
                    Some(skip_when) => format!("row {} unless {skip_when}", columns.join(", ")),
                    None => format!("row {}", columns.join(", ")),
                }
            }
        };
        Ok((
            body.commands
                .iter()
                .map(|command| command.to_string())
                .collect(),
            returned,
        ))
    }

    fn compile(body: &str, event: &str) -> Result<Vec<String>> {
        Ok(compile_for(body, "AFTER", event)?.0)
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
                 TG_ARGV[0], TG_ARGV[1], TG_ARGV[2]); RETURN NEW;",
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
    fn assignments_change_the_returned_row_and_the_statements_after_them() {
        let (commands, returned) = compile_for(
            "NEW.x := NEW.x + 1; \
             NEW.y = NEW.x * 2; \
             NEW.\"Camel\" := upper(TG_OP); \
             INSERT INTO log VALUES (NEW.x, NEW.y, OLD.x); \
             NEW.x := 0; \
             RETURN NEW;",
            "BEFORE",
            "UPDATE",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "INSERT INTO log VALUES (CAST (new.x + 1 AS INTEGER), \
                 (CAST (new.x + 1 AS INTEGER) * 2), old.x)"
            ]
        );
        assert_eq!(
            returned,
            "row x = CAST (0 AS INTEGER), y = CAST (new.x + 1 AS INTEGER) * 2, \
             Camel = CAST (upper ('UPDATE') AS TEXT)"
        );
    }

    #[test]
    fn return_values_of_before_triggers() {
        for (body, event, expected) in [
            ("RETURN NEW;", "INSERT", "unchanged"),
            ("RETURN NULL;", "INSERT", "skip"),
            ("RETURN;", "INSERT", "skip"),
            ("RETURN OLD;", "INSERT", "skip"),
            ("RETURN NEW;", "DELETE", "skip"),
            ("RETURN NULL;", "DELETE", "skip"),
            ("RETURN OLD;", "DELETE", "unchanged"),
            ("RETURN NULL;", "UPDATE", "skip"),
            (
                "RETURN OLD;",
                "UPDATE",
                "row x = old.x, y = old.y, Camel = old.Camel",
            ),
            ("NEW.y := 1; RETURN NEW;", "INSERT", "row y = 1"),
            (
                "NEW.id := 7; RETURN NEW;",
                "INSERT",
                "row id = CAST (7 AS INTEGER)",
            ),
        ] {
            let (_, returned) = compile_for(body, "BEFORE", event).unwrap();
            assert_eq!(returned, expected, "{body} {event}");
        }
    }

    #[test]
    fn after_triggers_ignore_the_returned_row() {
        for body in ["NEW.y := 1; RETURN NEW;", "RETURN NULL;", "RETURN OLD;"] {
            let (_, returned) = compile_for(body, "AFTER", "UPDATE").unwrap();
            assert_eq!(returned, "unchanged", "{body}");
        }
    }

    #[test]
    fn invalid_assignments_and_returns_are_errors() {
        for (body, event, message) in [
            (
                "NEW.nope := 1; RETURN NEW;",
                "INSERT",
                "has no field \"nope\"",
            ),
            (
                "NEW.doubled := 1; RETURN NEW;",
                "INSERT",
                "cannot change the generated column doubled",
            ),
            ("OLD.x := 1; RETURN NEW;", "UPDATE", "assignment to OLD.x"),
            (
                "NEW.x := 1; RETURN OLD;",
                "DELETE",
                "not assigned yet in a DELETE",
            ),
            ("RETURN 1;", "INSERT", "RETURN 1 is not supported yet"),
            (
                "INSERT INTO log VALUES (1);",
                "INSERT",
                "end of trigger procedure without RETURN",
            ),
        ] {
            let err = compile_for(body, "BEFORE", event).unwrap_err();
            assert!(err.to_string().contains(message), "{body}: {err}");
        }
    }

    #[test]
    fn the_value_of_an_assignment_follows_its_target() {
        assert_eq!(assigned_value("NEW.x := a = b", 2), Some("a = b"));
        assert_eq!(assigned_value("NEW.\"a\"\"b\" = 1", 2), Some("1"));
        assert_eq!(assigned_value("v:=1", 1), Some("1"));
        assert_eq!(assigned_value("x.y.z := 2", 3), Some("2"));
        assert_eq!(assigned_value("NEW.x 1", 2), None);
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
            columns: &[],
        };
        let err = compile_trigger_function(function_sql, &context).unwrap_err();
        assert!(err.to_string().contains("variable v of DECLARE"), "{err}");
    }
}
