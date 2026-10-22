//! Compile a PL/pgSQL trigger function to the commands of an engine trigger.
//!
//! `pg_query::parse_plpgsql` gives the parse tree of the function as JSON.
//! The compiler walks the tree and translates each SQL statement in it with
//! the PostgreSQL translator. A function is compiled again for each trigger
//! and each event, so each `TG_*` variable is a constant and becomes a
//! literal.

use serde_json::Value as Json;
use std::cell::Cell;
use std::collections::HashMap;
use turso_core::dialect::{TriggerFunctionBody, TriggerFunctionCall, TriggerReturn};
use turso_core::schema::Schema;
use turso_core::{
    walk_expr, walk_expr_with_subqueries_mut, walk_stmt_expressions_mut, LimboError, Result,
    WalkControl,
};
use turso_parser::ast::{self, Expr, Literal, TriggerCmd, TriggerEvent, TriggerTime};
use turso_pg_parser::function::PgFunction;
use turso_pg_parser::translator::{cast_type_for_pg_type, PostgreSQLTranslator};

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
    /// The columns of a table of the schema, by table name: a name in a
    /// statement that is both a column and a variable is an error.
    pub table_columns: &'a dyn Fn(&str) -> Option<Vec<String>>,
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
        table_columns: &|table| {
            schema.get_table(table).map(|table| {
                table
                    .columns()
                    .iter()
                    .filter_map(|column| column.name.clone())
                    .collect()
            })
        },
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
    let body = function
        .get("action")
        .and_then(|action| action.get("PLpgSQL_stmt_block"))
        .ok_or_else(|| parse_error("PL/pgSQL function has no body".to_string()))?;
    let mut compiler = Compiler {
        context,
        translator: PostgreSQLTranslator::new(),
        datums: json_array(function.get("datums")),
        prologue: Vec::new(),
        commands: Vec::new(),
        new_values: vec![None; context.columns.len()],
        column_variables: vec![false; context.columns.len()],
        branch: None,
        has_early_return: false,
        early_returns: Vec::new(),
        final_return: None,
        case_variables: HashMap::new(),
        variables: HashMap::new(),
        found_used: Cell::new(false),
        keys: 0,
    };
    compiler.declare_variables()?;
    compiler.compile_block(body)?;
    compiler.finish()
}

/// The parser names the value of `CASE expr` like this.
const CASE_VARIABLE_PREFIX: &str = "__Case__Variable_";

/// The level of `RAISE EXCEPTION`: the lower levels are messages.
const ERROR_LEVEL: i64 = 21;

/// What RETURN returns, for the row of a BEFORE trigger.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ReturnKind {
    /// Write the row with the assignments: `RETURN NEW`, or `RETURN OLD`
    /// in a DELETE trigger.
    Proceed,
    /// Do not write the row: `RETURN NULL`, `RETURN OLD` in an INSERT
    /// trigger, or `RETURN NEW` in a DELETE trigger.
    Skip,
    /// Write the old values: `RETURN OLD` in an UPDATE trigger.
    Old,
}

impl ReturnKind {
    /// The value that the `ret` variable gets for an early RETURN.
    fn name(self) -> &'static str {
        match self {
            ReturnKind::Proceed => "proceed",
            ReturnKind::Skip => "skip",
            ReturnKind::Old => "old",
        }
    }
}

/// What runs when no WHEN branch of a CASE or IF statement runs.
enum ElseBranch<'a> {
    Nothing,
    Statements(&'a [Json]),
    /// A CASE statement without ELSE and without a match is an error.
    CaseNotFound,
}

struct Compiler<'a> {
    context: &'a TriggerContext<'a>,
    translator: PostgreSQLTranslator,
    /// The variables of the function: `varno` of an assignment is an index
    /// into this list.
    datums: &'a [Json],
    /// Commands that run at the start of each firing: they give the
    /// variables their first value.
    prologue: Vec<TriggerCmd>,
    commands: Vec<TriggerCmd>,
    /// The value of each column of NEW after the assignments so far, as an
    /// expression over the NEW and OLD rows of the statement. `None` when
    /// no assignment changed the column.
    new_values: Vec<Option<Expr>>,
    /// The columns of NEW that a variable holds, because an assignment to
    /// them was in a branch or read a table.
    column_variables: Vec<bool>,
    /// The condition of the branch the compiler is in: `None` at the top
    /// level.
    branch: Option<Expr>,
    /// A RETURN in a branch was compiled: the statements after it run only
    /// while the `ret` variable is NULL.
    has_early_return: bool,
    early_returns: Vec<ReturnKind>,
    /// The RETURN at the top level, which ends the function.
    final_return: Option<ReturnKind>,
    /// The variable key of the value of each `CASE expr`, by the name the
    /// parser gives it.
    case_variables: HashMap<String, String>,
    /// The variables of DECLARE and `FOUND`, by lowercase name.
    variables: HashMap<String, Variable>,
    /// The body reads or sets `FOUND`, so each firing starts it false.
    found_used: Cell<bool>,
    keys: usize,
}

/// A variable of the function.
struct Variable {
    key: String,
    /// The declared type: a value the variable gets is cast to it.
    type_name: String,
}

/// What an assignment or an INTO list writes.
enum AssignmentTarget {
    /// A variable, by lowercase name.
    Variable(String),
    /// A column of NEW, by index.
    NewColumn(usize),
}

impl Compiler<'_> {
    /// Compile a block. Returns false when the block ends with RETURN, so
    /// the statements after it never run.
    fn compile_block(&mut self, block: &Json) -> Result<bool> {
        if block.get("exceptions").is_some() {
            return Err(not_supported_yet("EXCEPTION"));
        }
        self.compile_statements(json_array(block.get("body")))
    }

    fn compile_statements(&mut self, statements: &[Json]) -> Result<bool> {
        for statement in statements {
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
                self.compile_execsql(node)?;
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
                    return self.compile_end_of_body();
                }
                self.compile_return(value.as_deref().unwrap_or("NULL"))
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
            "PLpgSQL_stmt_if" => self.compile_if(node),
            "PLpgSQL_stmt_case" => self.compile_case(node),
            "PLpgSQL_stmt_raise" => {
                self.compile_raise(node)?;
                Ok(true)
            }
            "PLpgSQL_stmt_perform" => {
                self.compile_perform(expr_query(node.get("expr")))?;
                Ok(true)
            }
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

    /// The end of the body without RETURN, which is an error when the
    /// function runs, as in PostgreSQL: a RAISE before it comes first.
    fn compile_end_of_body(&mut self) -> Result<bool> {
        let message = "control reached end of trigger procedure without RETURN";
        let condition = self.condition();
        self.commands
            .push(select_command(raise_abort(text(message)), condition));
        Ok(false)
    }

    /// `RETURN <value>`: at the top level it ends the function, and the
    /// row a BEFORE trigger returns is written in place of the row of the
    /// statement. In a branch it sets the `ret` variable, which stops the
    /// statements after the branch.
    fn compile_return(&mut self, value: &str) -> Result<bool> {
        let kind = self.return_kind(value)?;
        match self.branch.as_ref().and(self.condition()) {
            None => self.final_return = Some(kind),
            Some(condition) => {
                let key = self.key("ret");
                self.commands.push(select_command(
                    self.pl_set(&key, text(kind.name())),
                    Some(condition),
                ));
                self.early_returns.push(kind);
                self.has_early_return = true;
            }
        }
        Ok(false)
    }

    fn return_kind(&self, value: &str) -> Result<ReturnKind> {
        match (self.context.event, value) {
            ("DELETE", "OLD") => Ok(ReturnKind::Proceed),
            ("DELETE", "NEW" | "NULL") | ("INSERT", "OLD") | (_, "NULL") => Ok(ReturnKind::Skip),
            ("UPDATE", "OLD") => Ok(ReturnKind::Old),
            (_, "NEW") => Ok(ReturnKind::Proceed),
            (_, other) => Err(not_supported_yet(&format!("RETURN {other}"))),
        }
    }

    /// `IF a THEN ... ELSIF b THEN ... ELSE ... END IF`.
    fn compile_if(&mut self, node: &Json) -> Result<bool> {
        let outer = self.condition();
        let mut branches = vec![(
            self.translate_expression(expr_query(node.get("cond")))?,
            json_array(node.get("then_body")),
        )];
        for elsif in json_array(node.get("elsif_list")) {
            let Some(elsif) = elsif.get("PLpgSQL_if_elsif") else {
                continue;
            };
            branches.push((
                self.translate_expression(expr_query(elsif.get("cond")))?,
                json_array(elsif.get("stmts")),
            ));
        }
        let else_branch = match node.get("else_body") {
            Some(body) => ElseBranch::Statements(json_array(Some(body))),
            None => ElseBranch::Nothing,
        };
        self.compile_branches(outer, branches, else_branch)
    }

    /// `CASE [expr] WHEN ... THEN ... ELSE ... END CASE`. The value of
    /// `expr` is evaluated one time into a variable, which the conditions
    /// of the WHEN branches read.
    fn compile_case(&mut self, node: &Json) -> Result<bool> {
        let outer = self.condition();
        if let Some(value) = node.get("t_expr") {
            let key = self.new_key("case");
            let mut value = self.translate_expression(expr_query(Some(value)))?;
            if let Some(outer) = &outer {
                value = case_when(outer.clone(), value, Expr::Literal(Literal::Null));
            }
            self.commands
                .push(select_command(self.pl_set(&key, value), None));
            let variable = node
                .get("t_varno")
                .and_then(Json::as_u64)
                .and_then(|varno| self.datums.get(varno as usize))
                .and_then(|datum| datum.get("PLpgSQL_var"))
                .map(|variable| json_str(variable.get("refname")).to_ascii_lowercase())
                .ok_or_else(|| parse_error("CASE has no value variable".to_string()))?;
            self.case_variables.insert(variable, key);
        }
        let mut branches = Vec::new();
        for when in json_array(node.get("case_when_list")) {
            let Some(when) = when.get("PLpgSQL_case_when") else {
                continue;
            };
            branches.push((
                self.translate_expression(expr_query(when.get("expr")))?,
                json_array(when.get("stmts")),
            ));
        }
        let has_else = node
            .get("have_else")
            .and_then(Json::as_bool)
            .unwrap_or(false)
            || node.get("else_stmts").is_some();
        let else_branch = if has_else {
            ElseBranch::Statements(json_array(node.get("else_stmts")))
        } else {
            ElseBranch::CaseNotFound
        };
        self.compile_branches(outer, branches, else_branch)
    }

    /// Compile the branches of an IF or CASE statement. Each condition is
    /// evaluated one time, in order, into a variable: a branch runs when
    /// its condition is true, the branches before it did not run, and the
    /// enclosing branch runs. Returns false when every branch ends with
    /// RETURN, so the statements after the IF never run.
    fn compile_branches(
        &mut self,
        outer: Option<Expr>,
        branches: Vec<(Expr, &[Json])>,
        else_branch: ElseBranch<'_>,
    ) -> Result<bool> {
        let parent = self.branch.clone();
        let mut earlier_keys: Vec<String> = Vec::new();
        let mut every_branch_returns = true;
        for (condition, body) in branches {
            let key = self.new_key("c");
            let mut value = coalesce(condition, number(0));
            if let Some(earlier) = or_all(earlier_keys.iter().map(|key| self.pl_get(key)).collect())
            {
                value = case_when(earlier, number(0), value);
            }
            if let Some(outer) = &outer {
                value = case_when(outer.clone(), value, number(0));
            }
            self.commands
                .push(select_command(self.pl_set(&key, value), None));
            self.branch = Some(self.pl_get(&key));
            every_branch_returns &= !self.compile_statements(body)?;
            self.branch = parent.clone();
            earlier_keys.push(key);
        }
        let no_earlier_branch = earlier_keys
            .iter()
            .map(|key| not(self.pl_get(key)))
            .reduce(|left, right| and(Some(left), right));
        let else_condition = match no_earlier_branch {
            Some(no_earlier_branch) => Some(and(parent.clone(), no_earlier_branch)),
            None => parent.clone(),
        };
        match else_branch {
            ElseBranch::Nothing => every_branch_returns = false,
            ElseBranch::Statements(body) => {
                self.branch = else_condition;
                every_branch_returns &= !self.compile_statements(body)?;
                self.branch = parent;
            }
            ElseBranch::CaseNotFound => {
                self.branch = else_condition;
                let condition = self.condition();
                self.commands.push(select_command(
                    raise_abort(text("case not found")),
                    condition,
                ));
                self.branch = parent;
            }
        }
        Ok(!every_branch_returns)
    }

    /// `RAISE EXCEPTION 'message % ...', params` stops the statement with
    /// the message. The lower levels, such as NOTICE, are messages to the
    /// client, which the engine has no channel for, so they go away.
    fn compile_raise(&mut self, node: &Json) -> Result<()> {
        let level = node
            .get("elog_level")
            .and_then(Json::as_i64)
            .unwrap_or(ERROR_LEVEL);
        if level < ERROR_LEVEL {
            return Ok(());
        }
        let mut message = match (
            node.get("message").and_then(Json::as_str),
            node.get("condname").and_then(Json::as_str),
        ) {
            (Some(message), _) => {
                Some(self.raise_message(message, json_array(node.get("params")))?)
            }
            (None, Some(condition_name)) => Some(text(condition_name)),
            (None, None) => None,
        };
        const MESSAGE_OPTION: i64 = 1;
        for option in json_array(node.get("options")) {
            let Some(option) = option.get("PLpgSQL_raise_option") else {
                continue;
            };
            if option.get("opt_type").and_then(Json::as_i64) == Some(MESSAGE_OPTION) {
                message = Some(self.translate_expression(expr_query(option.get("expr")))?);
            }
        }
        let Some(message) = message else {
            return Err(parse_error(
                "RAISE without parameters cannot be used outside an exception handler".to_string(),
            ));
        };
        let condition = self.condition();
        self.commands
            .push(select_command(raise_abort(message), condition));
        Ok(())
    }

    /// The message of RAISE: each `%` takes the text of the next parameter,
    /// `<NULL>` for a NULL, and `%%` is a `%`.
    fn raise_message(&self, message: &str, params: &[Json]) -> Result<Expr> {
        let mut parts: Vec<Expr> = Vec::new();
        let mut literal = String::new();
        let mut params = params.iter();
        let mut chars = message.chars().peekable();
        while let Some(c) = chars.next() {
            if c != '%' {
                literal.push(c);
                continue;
            }
            if chars.peek() == Some(&'%') {
                chars.next();
                literal.push('%');
                continue;
            }
            let Some(param) = params.next() else {
                return Err(parse_error(
                    "too few parameters specified for RAISE".to_string(),
                ));
            };
            if !literal.is_empty() {
                parts.push(text(&literal));
                literal.clear();
            }
            let value = self.translate_expression(expr_query(Some(param)))?;
            parts.push(coalesce(
                Expr::Cast {
                    expr: Box::new(value),
                    type_name: Some(ast::Type {
                        name: "TEXT".to_string(),
                        size: None,
                        array_dimensions: 0,
                    }),
                },
                text("<NULL>"),
            ));
        }
        if params.next().is_some() {
            return Err(parse_error(
                "too many parameters specified for RAISE".to_string(),
            ));
        }
        if !literal.is_empty() || parts.is_empty() {
            parts.push(text(&literal));
        }
        Ok(parts
            .into_iter()
            .reduce(|left, right| {
                Expr::Binary(Box::new(left), ast::Operator::Concat, Box::new(right))
            })
            .expect("the message has at least one part"))
    }

    /// `PERFORM query` runs the query for its effect.
    fn compile_perform(&mut self, query: &str) -> Result<()> {
        let Some(stmt) = self.translate_statement(query)? else {
            return Ok(());
        };
        let ast::Stmt::Select(select) = stmt else {
            return Err(parse_error(format!("PERFORM needs a query: {query}")));
        };
        self.push_command(TriggerCmd::Select(select));
        Ok(())
    }

    /// `NEW.column := value` or `variable := value`. A variable gets the
    /// value when the assignment runs. At the top level, a value for a
    /// column that reads only the rows of the statement replaces the
    /// column in the statements that follow and in the returned row. In a
    /// branch, or when the value reads a table or a variable, a variable
    /// holds the column from this point on: the assignment sets it when it
    /// runs.
    fn compile_assignment(&mut self, varno: usize, value: &str) -> Result<()> {
        let target = self.assignment_target(varno)?;
        let value = self.translate_expression(value)?;
        let condition = self.condition();
        let column = match target {
            AssignmentTarget::Variable(name) => {
                let variable = &self.variables[&name];
                let key = variable.key.clone();
                let value = cast_to_variable_type(value, &variable.type_name);
                self.commands
                    .push(select_command(self.pl_set(&key, value), condition));
                return Ok(());
            }
            AssignmentTarget::NewColumn(column) => column,
        };
        let value = cast_to_column_type(value, &self.context.columns[column].type_name);
        if condition.is_none() && !self.column_variables[column] && !reads_table_or_variable(&value)
        {
            self.new_values[column] = Some(value);
            return Ok(());
        }
        let key = self.column_variable_key(column);
        self.commands
            .push(select_command(self.pl_set(&key, value), condition));
        Ok(())
    }

    /// The key of the variable that holds the column of NEW from now on.
    /// The variable starts with the value the column has so far.
    fn column_variable_key(&mut self, column: usize) -> String {
        let name = self.context.columns[column].name.clone();
        let key = self.key(&format!("new.{name}"));
        if !self.column_variables[column] {
            let first_value = self.new_values[column]
                .clone()
                .unwrap_or_else(|| record_column("new", &name));
            self.prologue
                .push(select_command(self.pl_set(&key, first_value), None));
            self.column_variables[column] = true;
            self.new_values[column] = Some(self.pl_get(&key));
        }
        key
    }

    /// The variable or the column of NEW that the variable `varno` of an
    /// assignment names.
    fn assignment_target(&self, varno: usize) -> Result<AssignmentTarget> {
        let datum = self
            .datums
            .get(varno)
            .ok_or_else(|| parse_error(format!("unknown assignment target {varno}")))?;
        if let Some(variable) = datum.get("PLpgSQL_var") {
            let name = json_str(variable.get("refname")).to_ascii_lowercase();
            if !self.variables.contains_key(&name) {
                return Err(not_supported(&format!("assignment to {name}")));
            }
            return Ok(AssignmentTarget::Variable(name));
        }
        let Some(field) = datum.get("PLpgSQL_recfield") else {
            return Err(not_supported_yet("this assignment target"));
        };
        let record = field
            .get("recparentno")
            .and_then(Json::as_u64)
            .and_then(|parent| self.datums.get(parent as usize))
            .and_then(|parent| parent.get("PLpgSQL_rec"))
            .map(|parent| json_str(parent.get("refname")))
            .ok_or_else(|| parse_error(format!("unknown assignment target {varno}")))?;
        let field = json_str(field.get("fieldname"));
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
        let Some(column) = self.column_index(field) else {
            return Err(parse_error(format!(
                "record \"new\" has no field \"{field}\""
            )));
        };
        if self.context.columns[column].is_generated {
            return Err(parse_error(format!(
                "a trigger cannot change the generated column {field}"
            )));
        }
        Ok(AssignmentTarget::NewColumn(column))
    }

    /// The variables of DECLARE get their first value at the start of each
    /// firing, in the order of the declarations. `FOUND` starts false.
    fn declare_variables(&mut self) -> Result<()> {
        for datum in self.datums {
            if let Some(record) = datum.get("PLpgSQL_rec") {
                if record.get("lineno").is_some() {
                    return Err(not_supported(&format!(
                        "the record variable {}",
                        json_str(record.get("refname"))
                    )));
                }
            }
            let Some(variable) = datum.get("PLpgSQL_var") else {
                continue;
            };
            let name = json_str(variable.get("refname")).to_string();
            if name.starts_with(CASE_VARIABLE_PREFIX) {
                continue;
            }
            let key = self.key(&format!("var.{}", name.to_ascii_lowercase()));
            let type_name = json_str(
                variable
                    .get("datatype")
                    .and_then(|datatype| datatype.get("PLpgSQL_type"))
                    .and_then(|datatype| datatype.get("typname")),
            )
            .trim()
            .to_string();
            if type_name.contains('%') {
                return Err(not_supported(&format!(
                    "the type {type_name} of the variable {name}"
                )));
            }
            if type_name.eq_ignore_ascii_case("record") {
                return Err(not_supported(&format!("the record variable {name}")));
            }
            if variable.get("lineno").is_none() {
                // FOUND, which the parser declares.
                self.variables
                    .insert(name.to_ascii_lowercase(), Variable { key, type_name });
                continue;
            }
            let value = match variable.get("default_val") {
                Some(default) => cast_to_variable_type(
                    self.translate_expression(expr_query(Some(default)))?,
                    &type_name,
                ),
                None => Expr::Literal(Literal::Null),
            };
            self.prologue
                .push(select_command(self.pl_set(&key, value), None));
            self.variables
                .insert(name.to_ascii_lowercase(), Variable { key, type_name });
        }
        Ok(())
    }

    /// The key of `FOUND`, which each firing then starts false.
    fn found_key(&self) -> String {
        self.found_used.set(true);
        self.variables["found"].key.clone()
    }

    /// A SQL statement of the body. With INTO, the row of its result goes
    /// to variables or columns of NEW.
    fn compile_execsql(&mut self, node: &Json) -> Result<()> {
        if node.get("strict").and_then(Json::as_bool).unwrap_or(false) {
            return Err(not_supported_yet("SELECT INTO STRICT"));
        }
        let targets = match node.get("target") {
            Some(target) => self.targets_of_into(target)?,
            None => Vec::new(),
        };
        let Some(stmt) = self.translate_statement(expr_query(node.get("sqlstmt")))? else {
            return Ok(());
        };
        if targets.is_empty() {
            let (command, returning) = command_from_statement(stmt, self.context.table_schema)?;
            if !returning.is_empty() {
                return Err(no_destination());
            }
            self.push_command(command);
            return Ok(());
        }
        match stmt {
            ast::Stmt::Select(select) => self.compile_select_into(select, targets),
            other => self.compile_returning_into(other, targets),
        }
    }

    /// The variables and columns of NEW of an INTO list.
    fn targets_of_into(&self, target: &Json) -> Result<Vec<AssignmentTarget>> {
        let Some(row) = target.get("PLpgSQL_row") else {
            return Err(not_supported("this INTO target"));
        };
        json_array(row.get("fields"))
            .iter()
            .map(|field| {
                let varno = field.get("varno").and_then(Json::as_u64).unwrap_or(0) as usize;
                self.assignment_target(varno)
            })
            .collect()
    }

    fn target_key(&mut self, target: &AssignmentTarget) -> String {
        match target {
            AssignmentTarget::Variable(name) => self.variables[name].key.clone(),
            AssignmentTarget::NewColumn(column) => self.column_variable_key(*column),
        }
    }

    /// `SELECT a, b INTO x, y FROM ...`: one statement sets each variable
    /// from the first row and `FOUND` from whether there is a row:
    /// `SELECT __pl_set(x, q.c0), __pl_set(y, q.c1), __pl_set(found, q.k IS NOT NULL)
    /// FROM (SELECT 1) LEFT JOIN (SELECT *, 1 AS k FROM (query) LIMIT 1) q ON 1`.
    /// Without a row, the variables get NULL.
    fn compile_select_into(
        &mut self,
        mut select: ast::Select,
        targets: Vec<AssignmentTarget>,
    ) -> Result<()> {
        let ast::OneSelect::Select { columns, .. } = &mut select.body.select else {
            return Err(not_supported("VALUES with INTO"));
        };
        if columns.len() != targets.len() {
            return Err(parse_error(format!(
                "the query gives {} columns for {} targets of INTO",
                columns.len(),
                targets.len()
            )));
        }
        for (i, column) in columns.iter_mut().enumerate() {
            let ast::ResultColumn::Expr(_, alias) = column else {
                return Err(not_supported("SELECT * with INTO"));
            };
            *alias = Some(ast::As::As(ast::Name::exact(format!("c{i}"))));
        }
        let keys: Vec<String> = targets
            .iter()
            .map(|target| self.target_key(target))
            .collect();
        let found_key = self.found_key();

        let mut first_row = simple_select(
            vec![
                ast::ResultColumn::Star,
                ast::ResultColumn::Expr(
                    Box::new(number(1)),
                    Some(ast::As::As(ast::Name::exact("k".to_string()))),
                ),
            ],
            Some(ast::FromClause {
                select: Box::new(ast::SelectTable::Select {
                    select,
                    alias: None,
                    lateral: false,
                }),
                joins: vec![],
            }),
            None,
        );
        first_row.limit = Some(ast::Limit {
            expr: Box::new(number(1)),
            offset: None,
        });

        let mut columns: Vec<ast::ResultColumn> = keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                ast::ResultColumn::Expr(
                    Box::new(self.pl_set(key, record_column("q", &format!("c{i}")))),
                    None,
                )
            })
            .collect();
        columns.push(ast::ResultColumn::Expr(
            Box::new(self.pl_set(&found_key, Expr::NotNull(Box::new(record_column("q", "k"))))),
            None,
        ));
        let from = ast::FromClause {
            select: Box::new(ast::SelectTable::Select {
                select: simple_select(
                    vec![ast::ResultColumn::Expr(Box::new(number(1)), None)],
                    None,
                    None,
                ),
                alias: None,
                lateral: false,
            }),
            joins: vec![ast::JoinedSelectTable {
                operator: ast::JoinOperator::TypedJoin(Some(
                    ast::JoinType::LEFT | ast::JoinType::OUTER,
                )),
                table: Box::new(ast::SelectTable::Select {
                    select: first_row,
                    alias: Some(ast::As::As(ast::Name::exact("q".to_string()))),
                    lateral: false,
                }),
                constraint: Some(ast::JoinConstraint::On(Box::new(number(1)))),
            }],
        };
        let condition = self.condition();
        self.commands.push(TriggerCmd::Select(simple_select(
            columns,
            Some(from),
            condition,
        )));
        Ok(())
    }

    /// `INSERT ... RETURNING a, b INTO x, y`: the variables start as NULL,
    /// the RETURNING list sets them for the row, `__pl_set_one` gives an
    /// error on a second row, and `FOUND` tells whether there was a row.
    fn compile_returning_into(
        &mut self,
        stmt: ast::Stmt,
        targets: Vec<AssignmentTarget>,
    ) -> Result<()> {
        let (command, returning) = command_from_statement(stmt, self.context.table_schema)?;
        if returning.is_empty() {
            return Err(parse_error(
                "INTO used with a command that cannot return data".to_string(),
            ));
        }
        if !matches!(command, TriggerCmd::Insert { .. }) {
            return Err(not_supported_yet("RETURNING INTO with UPDATE or DELETE"));
        }
        if returning.len() != targets.len() {
            return Err(parse_error(format!(
                "RETURNING gives {} columns for {} targets of INTO",
                returning.len(),
                targets.len()
            )));
        }
        let keys: Vec<String> = targets
            .iter()
            .map(|target| self.target_key(target))
            .collect();
        let found_key = self.found_key();
        let condition = self.condition();
        for key in &keys {
            self.commands.push(select_command(
                self.pl_set(key, Expr::Literal(Literal::Null)),
                condition.clone(),
            ));
        }
        self.commands.push(select_command(
            self.pl_set(&format!("{}.rows", keys[0]), number(0)),
            condition.clone(),
        ));
        self.commands.push(select_command(
            self.pl_set(&found_key, number(0)),
            condition,
        ));
        let mut values = Vec::new();
        for (i, column) in returning.into_iter().enumerate() {
            let ast::ResultColumn::Expr(value, _) = column else {
                return Err(not_supported("RETURNING * with INTO"));
            };
            let value = if i == 0 {
                function_call("__pl_set_one", vec![text(&keys[0]), *value])
            } else {
                self.pl_set(&keys[i], *value)
            };
            values.push(ast::ResultColumn::Expr(Box::new(value), None));
        }
        values.push(ast::ResultColumn::Expr(
            Box::new(self.pl_set(&found_key, number(1))),
            None,
        ));
        self.push_command(with_returning(command, values));
        Ok(())
    }

    fn column_index(&self, name: &str) -> Option<usize> {
        self.context
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
    }

    /// The expression `sql` of the function body, over the rows of the
    /// statement.
    fn translate_expression(&self, sql: &str) -> Result<Expr> {
        let select = format!("SELECT {sql}");
        let parse_result =
            turso_pg_parser::parse(&select).map_err(|e| parse_error(e.to_string()))?;
        let stmt = self
            .translator
            .translate(&parse_result)
            .map_err(|e| parse_error(e.to_string()))?;
        let ast::Stmt::Select(select) = stmt else {
            return Err(parse_error(format!("invalid expression: {sql}")));
        };
        let ast::OneSelect::Select { mut columns, .. } = select.body.select else {
            return Err(parse_error(format!("invalid expression: {sql}")));
        };
        let Some(ast::ResultColumn::Expr(mut expr, _)) = columns.pop() else {
            return Err(parse_error(format!("invalid expression: {sql}")));
        };
        walk_expr_with_subqueries_mut(&mut expr, &mut |expr| self.rewrite_trigger_reference(expr))?;
        Ok(*expr)
    }

    /// A SQL statement of the body, over the rows of the statement that
    /// fires the trigger. A name that is both a column of a table of the
    /// statement and a variable gives the PostgreSQL error when the
    /// statement runs.
    fn translate_statement(&mut self, query: &str) -> Result<Option<ast::Stmt>> {
        let parse_result = turso_pg_parser::parse(query).map_err(|e| parse_error(e.to_string()))?;
        let mut stmt = self
            .translator
            .translate(&parse_result)
            .map_err(|e| parse_error(e.to_string()))?;
        if let Some(name) = self.ambiguous_name(&stmt)? {
            let condition = self.condition();
            self.commands.push(select_command(
                raise_abort(text(&format!("column reference \"{name}\" is ambiguous"))),
                condition,
            ));
            return Ok(None);
        }
        walk_stmt_expressions_mut(&mut stmt, &mut |expr| self.rewrite_trigger_reference(expr))?;
        Ok(Some(stmt))
    }

    /// A variable of the function that a table of the statement also has as
    /// a column.
    fn ambiguous_name(&self, stmt: &ast::Stmt) -> Result<Option<String>> {
        let tables = scoped_tables(stmt);
        if tables.is_empty() {
            return Ok(None);
        }
        let mut names: Vec<String> = Vec::new();
        let mut copy = stmt.clone();
        walk_stmt_expressions_mut(&mut copy, &mut |expr| {
            if let Expr::Id(id) = expr {
                let name = id.as_str().to_ascii_lowercase();
                if self.variables.contains_key(&name) && !names.contains(&name) {
                    names.push(name);
                }
            }
            Ok(WalkControl::Continue)
        })?;
        for name in names {
            for table in &tables {
                let Some(columns) = (self.context.table_columns)(table) else {
                    continue;
                };
                if columns
                    .iter()
                    .any(|column| column.eq_ignore_ascii_case(&name))
                {
                    return Ok(Some(name));
                }
            }
        }
        Ok(None)
    }

    /// Add a command that runs under the condition of this point of the
    /// Add a command that runs under the condition of this point of the
    /// body.
    fn push_command(&mut self, command: TriggerCmd) {
        let command = match self.condition() {
            Some(condition) => with_condition(command, condition),
            None => command,
        };
        self.commands.push(command);
    }

    /// The condition under which a statement at this point runs: the
    /// branch and, after an early RETURN, that no RETURN ran yet.
    fn condition(&self) -> Option<Expr> {
        let mut condition = self.branch.clone();
        if self.has_early_return {
            let no_return = Expr::IsNull(Box::new(self.pl_get(&self.key("ret"))));
            condition = Some(and(condition, no_return));
        }
        condition
    }

    /// The key of a variable of this trigger and event.
    fn key(&self, name: &str) -> String {
        format!(
            "{}.{}.{name}",
            self.context.trigger_name, self.context.event
        )
    }

    fn new_key(&mut self, prefix: &str) -> String {
        self.keys += 1;
        self.key(&format!("{prefix}{}", self.keys))
    }

    fn pl_get(&self, key: &str) -> Expr {
        function_call("__pl_get", vec![text(key)])
    }

    fn pl_set(&self, key: &str, value: Expr) -> Expr {
        function_call("__pl_set", vec![text(key), value])
    }

    /// Each `TG_*` variable is a constant for this trigger and event,
    /// `NEW.column` is the value the assignments so far gave the column,
    /// and the value of a `CASE expr` is its variable.
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
                name => self
                    .case_variables
                    .get(name)
                    .map(|key| self.pl_get(key))
                    .or_else(|| {
                        let variable = self.variables.get(name)?;
                        if name == "found" {
                            self.found_used.set(true);
                        }
                        Some(self.pl_get(&variable.key))
                    }),
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

    /// The commands of the trigger, and the row the function returns.
    fn finish(mut self) -> Result<TriggerFunctionBody> {
        let returned_row = self.returned_row();
        let mut commands = Vec::new();
        if self.found_used.get() {
            let key = self.variables["found"].key.clone();
            commands.push(select_command(self.pl_set(&key, number(0)), None));
        }
        commands.append(&mut self.prologue);
        if self.has_early_return {
            let key = self.key("ret");
            commands.push(select_command(
                self.pl_set(&key, Expr::Literal(Literal::Null)),
                None,
            ));
        }
        commands.append(&mut self.commands);
        Ok(TriggerFunctionBody {
            commands,
            returned_row,
        })
    }

    /// The row a BEFORE trigger returns: the assignments, the old values
    /// when a `RETURN OLD` ran, and no row when a `RETURN NULL` ran.
    fn returned_row(&self) -> TriggerReturn {
        if self.context.timing != "BEFORE" {
            return TriggerReturn::Unchanged;
        }
        let ret = self.pl_get(&self.key("ret"));
        let early = |kind: ReturnKind| {
            self.early_returns
                .contains(&kind)
                .then(|| equals(ret.clone(), text(kind.name())))
        };
        let no_early_return = || {
            self.has_early_return
                .then(|| Expr::IsNull(Box::new(ret.clone())))
        };

        let mut skip_terms: Vec<Expr> = early(ReturnKind::Skip).into_iter().collect();
        if self.final_return == Some(ReturnKind::Skip) {
            match no_early_return() {
                Some(condition) => skip_terms.push(condition),
                None => return TriggerReturn::Skip,
            }
        }
        let mut old_terms: Vec<Expr> = early(ReturnKind::Old).into_iter().collect();
        let mut always_old = false;
        if self.final_return == Some(ReturnKind::Old) {
            match no_early_return() {
                Some(condition) => old_terms.push(condition),
                None => always_old = true,
            }
        }
        let old_condition = or_all(old_terms);

        let columns: Vec<Option<Expr>> = self
            .context
            .columns
            .iter()
            .zip(&self.new_values)
            .map(|(column, value)| {
                if column.is_generated || column.is_rowid_alias {
                    return value.clone();
                }
                if always_old {
                    return Some(record_column("old", &column.name));
                }
                match &old_condition {
                    Some(condition) => Some(case_when(
                        condition.clone(),
                        record_column("old", &column.name),
                        value
                            .clone()
                            .unwrap_or_else(|| record_column("new", &column.name)),
                    )),
                    None => value.clone(),
                }
            })
            .collect();
        let skip_when = or_all(skip_terms);
        if skip_when.is_none() && columns.iter().all(Option::is_none) {
            return TriggerReturn::Unchanged;
        }
        TriggerReturn::Row { skip_when, columns }
    }
}

/// The trigger command of a SQL statement of the body, and its RETURNING
/// list.
fn command_from_statement(
    stmt: ast::Stmt,
    table_schema: &str,
) -> Result<(TriggerCmd, Vec<ast::ResultColumn>)> {
    match stmt {
        ast::Stmt::Insert {
            with: None,
            or_conflict,
            tbl_name,
            columns,
            body: ast::InsertBody::Select(select, upsert),
            returning,
        } => Ok((
            TriggerCmd::Insert {
                or_conflict,
                tbl_name: local_table_name(tbl_name, table_schema)?,
                col_names: columns,
                select,
                upsert,
                returning: vec![],
            },
            returning,
        )),
        ast::Stmt::Update(ast::Update {
            with: None,
            or_conflict,
            tbl_name,
            indexed: None,
            sets,
            from,
            where_clause,
            returning,
        }) => Ok((
            TriggerCmd::Update {
                or_conflict,
                tbl_name: local_table_name(tbl_name, table_schema)?,
                sets,
                from,
                where_clause,
            },
            returning,
        )),
        ast::Stmt::Delete {
            with: None,
            tbl_name,
            indexed: None,
            where_clause,
            returning,
        } => Ok((
            TriggerCmd::Delete {
                tbl_name: local_table_name(tbl_name, table_schema)?,
                where_clause,
            },
            returning,
        )),
        ast::Stmt::Select(_) => Err(no_destination()),
        other => Err(not_supported(&format!(
            "the statement {} in a trigger function",
            statement_kind(&other)
        ))),
    }
}

/// The table name without its schema: a trigger reads and writes tables
/// of its own schema only.
fn local_table_name(name: ast::QualifiedName, table_schema: &str) -> Result<ast::Name> {
    if let Some(schema) = &name.namespace {
        let is_own_schema = schema.as_str().eq_ignore_ascii_case("main")
            || schema.as_str().eq_ignore_ascii_case(table_schema);
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

/// The INSERT command with a RETURNING list. The engine runs the list for
/// each row and drops the rows. The engine commands for UPDATE and DELETE
/// have no RETURNING list.
fn with_returning(command: TriggerCmd, returning: Vec<ast::ResultColumn>) -> TriggerCmd {
    match command {
        TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            col_names,
            select,
            upsert,
            ..
        } => TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            col_names,
            select,
            upsert,
            returning,
        },
        other => other,
    }
}

/// The tables whose columns the names of a statement can refer to: the
/// tables of its FROM clauses and the table that UPDATE and DELETE change.
fn scoped_tables(stmt: &ast::Stmt) -> Vec<String> {
    let mut tables = Vec::new();
    match stmt {
        ast::Stmt::Select(select) => select_tables(select, &mut tables),
        ast::Stmt::Insert {
            body: ast::InsertBody::Select(select, _),
            ..
        } => select_tables(select, &mut tables),
        ast::Stmt::Update(update) => {
            tables.push(update.tbl_name.name.as_str().to_string());
            if let Some(from) = &update.from {
                from_tables(from, &mut tables);
            }
        }
        ast::Stmt::Delete { tbl_name, .. } => tables.push(tbl_name.name.as_str().to_string()),
        _ => {}
    }
    tables
}

fn select_tables(select: &ast::Select, tables: &mut Vec<String>) {
    one_select_tables(&select.body.select, tables);
    for compound in &select.body.compounds {
        one_select_tables(&compound.select, tables);
    }
}

fn one_select_tables(select: &ast::OneSelect, tables: &mut Vec<String>) {
    if let ast::OneSelect::Select {
        from: Some(from), ..
    } = select
    {
        from_tables(from, tables);
    }
}

fn from_tables(from: &ast::FromClause, tables: &mut Vec<String>) {
    select_table_tables(&from.select, tables);
    for join in &from.joins {
        select_table_tables(&join.table, tables);
    }
}

fn select_table_tables(table: &ast::SelectTable, tables: &mut Vec<String>) {
    match table {
        ast::SelectTable::Table(name, ..) => tables.push(name.name.as_str().to_string()),
        ast::SelectTable::Select { select, .. } => select_tables(select, tables),
        ast::SelectTable::Sub(from, _) => from_tables(from, tables),
        ast::SelectTable::TableCall(..) => {}
    }
}

/// The value of a variable gets the cast of the translator for the
/// declared type, as an assigned column does. A boolean keeps its value,
/// because a cast to INTEGER would lose the text forms of true and false.
fn cast_to_variable_type(expr: Expr, type_name: &str) -> Expr {
    let base = type_name.trim();
    let base = base.strip_prefix("pg_catalog.").unwrap_or(base);
    let base = base
        .split(['(', '['])
        .next()
        .unwrap_or(base)
        .trim()
        .trim_matches('"');
    if base.eq_ignore_ascii_case("boolean") || base.eq_ignore_ascii_case("bool") {
        return expr;
    }
    cast_to_column_type(expr, &cast_type_for_pg_type(base, 0).name)
}

/// The query text of a `PLpgSQL_expr` node.
fn expr_query(node: Option<&Json>) -> &str {
    json_str(
        node.and_then(|e| e.get("PLpgSQL_expr"))
            .and_then(|e| e.get("query")),
    )
}

/// The command runs only when `condition` is true.
fn with_condition(command: TriggerCmd, condition: Expr) -> TriggerCmd {
    match command {
        TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            col_names,
            select,
            upsert,
            returning,
        } => TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            col_names,
            select: conditional_select(select, condition),
            upsert,
            returning,
        },
        TriggerCmd::Update {
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause,
        } => TriggerCmd::Update {
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause: Some(Box::new(and(where_clause.map(|e| *e), condition))),
        },
        TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => TriggerCmd::Delete {
            tbl_name,
            where_clause: Some(Box::new(and(where_clause.map(|e| *e), condition))),
        },
        TriggerCmd::Select(select) => TriggerCmd::Select(conditional_select(select, condition)),
        other @ TriggerCmd::ExecuteFunction { .. } => other,
    }
}

/// The query gives its rows only when `condition` is true. One row of
/// VALUES becomes `SELECT values WHERE condition`; every other query is
/// read through `SELECT * FROM (query) WHERE condition`, so that a query
/// with an aggregate gives no row.
fn conditional_select(select: ast::Select, condition: Expr) -> ast::Select {
    let is_simple = select.with.is_none()
        && select.body.compounds.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none();
    if is_simple {
        if let ast::OneSelect::Values(rows) = &select.body.select {
            if let [row] = rows.as_slice() {
                let columns = row
                    .iter()
                    .map(|value| ast::ResultColumn::Expr(value.clone(), None))
                    .collect();
                return simple_select(columns, None, Some(condition));
            }
        }
    }
    simple_select(
        vec![ast::ResultColumn::Star],
        Some(ast::FromClause {
            select: Box::new(ast::SelectTable::Select {
                select,
                alias: None,
                lateral: false,
            }),
            joins: vec![],
        }),
        Some(condition),
    )
}

fn simple_select(
    columns: Vec<ast::ResultColumn>,
    from: Option<ast::FromClause>,
    where_clause: Option<Expr>,
) -> ast::Select {
    ast::Select {
        with: None,
        body: ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: None,
                columns,
                from,
                where_clause: where_clause.map(Box::new),
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
        },
        order_by: vec![],
        limit: None,
    }
}

/// `SELECT expr [WHERE condition]` as a trigger command.
fn select_command(expr: Expr, condition: Option<Expr>) -> TriggerCmd {
    TriggerCmd::Select(simple_select(
        vec![ast::ResultColumn::Expr(Box::new(expr), None)],
        None,
        condition,
    ))
}

fn raise_abort(message: Expr) -> Expr {
    Expr::Raise(ast::ResolveType::Abort, Some(Box::new(message)))
}

/// Whether the value of an assignment depends on when it is evaluated: it
/// reads a table or a variable.
fn reads_table_or_variable(expr: &Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |expr: &Expr| {
        match expr {
            Expr::Subquery(_) | Expr::Exists(_) | Expr::InSelect { .. } => found = true,
            Expr::FunctionCall { name, .. } if name.as_str().eq_ignore_ascii_case("__pl_get") => {
                found = true
            }
            _ => {}
        }
        Ok(if found {
            WalkControl::SkipChildren
        } else {
            WalkControl::Continue
        })
    });
    found
}

fn function_call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall {
        name: ast::Name::exact(name.to_string()),
        distinctness: None,
        args: args.into_iter().map(Box::new).collect(),
        order_by: vec![],
        within_group: vec![],
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    }
}

fn record_column(record: &str, column: &str) -> Expr {
    Expr::Qualified(
        ast::Name::exact(record.to_string()),
        ast::Name::exact(column.to_string()),
    )
}

fn coalesce(value: Expr, default: Expr) -> Expr {
    function_call("coalesce", vec![value, default])
}

fn case_when(condition: Expr, then: Expr, otherwise: Expr) -> Expr {
    Expr::Case {
        base: None,
        when_then_pairs: vec![(Box::new(condition), Box::new(then))],
        else_expr: Some(Box::new(otherwise)),
    }
}

fn and(left: Option<Expr>, right: Expr) -> Expr {
    match left {
        None => right,
        Some(left) => Expr::Binary(
            Box::new(parenthesized(left)),
            ast::Operator::And,
            Box::new(parenthesized(right)),
        ),
    }
}

fn or_all(terms: Vec<Expr>) -> Option<Expr> {
    terms.into_iter().reduce(|left, right| {
        Expr::Binary(
            Box::new(parenthesized(left)),
            ast::Operator::Or,
            Box::new(parenthesized(right)),
        )
    })
}

fn not(expr: Expr) -> Expr {
    Expr::Unary(ast::UnaryOperator::Not, Box::new(parenthesized(expr)))
}

fn equals(left: Expr, right: Expr) -> Expr {
    Expr::Binary(
        Box::new(parenthesized(left)),
        ast::Operator::Equals,
        Box::new(parenthesized(right)),
    )
}

fn number(value: i64) -> Expr {
    Expr::Literal(Literal::Numeric(value.to_string()))
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

    fn test_table_columns(table: &str) -> Option<Vec<String>> {
        let columns: &[&str] = match table {
            "log" => &["id", "name"],
            "t" => &["id", "x", "y"],
            _ => return None,
        };
        Some(columns.iter().map(|column| column.to_string()).collect())
    }

    fn compile_for(body: &str, timing: &str, event: &str) -> Result<(Vec<String>, String)> {
        compile_source(&format!("BEGIN {body} END"), timing, event)
    }

    /// Compile a function whose source starts with DECLARE or BEGIN.
    fn compile_source(source: &str, timing: &str, event: &str) -> Result<(Vec<String>, String)> {
        let function_sql =
            format!("CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ {source} $$");
        let columns = columns();
        let context = TriggerContext {
            trigger_name: "trg",
            table_name: "t",
            table_schema: "public",
            timing,
            event,
            args: &["a".to_string(), "b".to_string()],
            columns: &columns,
            table_columns: &test_table_columns,
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
        ] {
            let err = compile_for(body, "BEFORE", event).unwrap_err();
            assert!(err.to_string().contains(message), "{body}: {err}");
        }
    }

    #[test]
    fn a_body_without_return_raises_the_error_when_it_runs() {
        let (commands, returned) =
            compile_for("INSERT INTO log VALUES (1);", "BEFORE", "INSERT").unwrap();
        assert_eq!(
            commands,
            vec![
                "INSERT INTO log VALUES (1)",
                "SELECT RAISE (ABORT, 'control reached end of trigger procedure without RETURN')",
            ]
        );
        assert_eq!(returned, "unchanged");
    }

    #[test]
    fn if_branches_run_under_conditions_held_in_variables() {
        let (commands, returned) = compile_for(
            "IF NEW.x > 1 THEN \
               INSERT INTO log VALUES (NEW.x, 'big'); \
             ELSIF NEW.x > 0 THEN \
               UPDATE counts SET n = n + 1 WHERE name = 'small'; \
             ELSE \
               DELETE FROM counts; \
             END IF; \
             RETURN NEW;",
            "BEFORE",
            "UPDATE",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT __pl_set ('trg.UPDATE.c1', coalesce (new.x > 1, 0))",
                "INSERT INTO log SELECT new.x, 'big' WHERE __pl_get ('trg.UPDATE.c1')",
                "SELECT __pl_set ('trg.UPDATE.c2', CASE WHEN __pl_get ('trg.UPDATE.c1') THEN 0 \
                 ELSE coalesce (new.x > 0, 0) END)",
                "UPDATE counts SET n = n + 1 WHERE (name = 'small') AND __pl_get ('trg.UPDATE.c2')",
                "DELETE FROM counts WHERE (NOT __pl_get ('trg.UPDATE.c1')) AND \
                 (NOT __pl_get ('trg.UPDATE.c2'))",
            ]
        );
        assert_eq!(returned, "unchanged");
    }

    #[test]
    fn a_nested_if_adds_the_outer_condition() {
        let (commands, _) = compile_for(
            "IF NEW.x > 1 THEN \
               IF NEW.y IS NULL THEN DELETE FROM counts; END IF; \
             END IF; \
             RETURN NEW;",
            "BEFORE",
            "UPDATE",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT __pl_set ('trg.UPDATE.c1', coalesce (new.x > 1, 0))",
                "SELECT __pl_set ('trg.UPDATE.c2', CASE WHEN __pl_get ('trg.UPDATE.c1') \
                 THEN coalesce (new.y ISNULL, 0) ELSE 0 END)",
                "DELETE FROM counts WHERE __pl_get ('trg.UPDATE.c2')",
            ]
        );
    }

    #[test]
    fn an_early_return_stops_the_statements_after_it() {
        let (commands, returned) = compile_for(
            "IF NEW.x < 0 THEN RETURN NULL; END IF; \
             NEW.y := 1; \
             INSERT INTO log VALUES (NEW.y); \
             RETURN NEW;",
            "BEFORE",
            "INSERT",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT __pl_set ('trg.INSERT.new.y', new.y)",
                "SELECT __pl_set ('trg.INSERT.ret', NULL)",
                "SELECT __pl_set ('trg.INSERT.c1', coalesce (new.x < 0, 0))",
                "SELECT __pl_set ('trg.INSERT.ret', 'skip') WHERE __pl_get ('trg.INSERT.c1')",
                "SELECT __pl_set ('trg.INSERT.new.y', 1) WHERE __pl_get ('trg.INSERT.ret') ISNULL",
                "INSERT INTO log SELECT __pl_get ('trg.INSERT.new.y') WHERE \
                 __pl_get ('trg.INSERT.ret') ISNULL",
            ]
        );
        assert_eq!(
            returned,
            "row y = __pl_get ('trg.INSERT.new.y') unless __pl_get ('trg.INSERT.ret') = 'skip'"
        );
    }

    #[test]
    fn a_body_that_can_end_without_return_raises_the_error_at_the_end() {
        let (commands, returned) =
            compile_for("IF NEW.x < 0 THEN RETURN OLD; END IF;", "BEFORE", "UPDATE").unwrap();
        assert_eq!(
            commands.last().unwrap(),
            "SELECT RAISE (ABORT, 'control reached end of trigger procedure without RETURN') \
             WHERE __pl_get ('trg.UPDATE.ret') ISNULL"
        );
        assert_eq!(
            returned,
            "row x = CASE WHEN __pl_get ('trg.UPDATE.ret') = 'old' THEN old.x ELSE new.x END, \
             y = CASE WHEN __pl_get ('trg.UPDATE.ret') = 'old' THEN old.y ELSE new.y END, \
             Camel = CASE WHEN __pl_get ('trg.UPDATE.ret') = 'old' THEN old.Camel ELSE new.Camel END"
        );
    }

    #[test]
    fn an_assignment_that_reads_a_table_runs_in_place() {
        let (commands, returned) = compile_for(
            "NEW.x := 5; \
             NEW.y := (SELECT count(*) FROM log); \
             INSERT INTO log VALUES (NEW.x, NEW.y); \
             RETURN NEW;",
            "BEFORE",
            "INSERT",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT __pl_set ('trg.INSERT.new.y', new.y)",
                "SELECT __pl_set ('trg.INSERT.new.y', (SELECT count (*) count FROM log))",
                "INSERT INTO log VALUES (CAST (5 AS INTEGER), __pl_get ('trg.INSERT.new.y'))",
            ]
        );
        assert_eq!(
            returned,
            "row x = CAST (5 AS INTEGER), y = __pl_get ('trg.INSERT.new.y')"
        );
    }

    #[test]
    fn raise_exception_becomes_a_raise_with_the_message_and_notices_go_away() {
        let (commands, _) = compile_for(
            "RAISE NOTICE 'row %', NEW.x; \
             RAISE EXCEPTION 'bad % for %%', NEW.x; \
             RAISE unique_violation; \
             RAISE EXCEPTION USING MESSAGE = 'made ' || NEW.x; \
             RETURN NEW;",
            "BEFORE",
            "INSERT",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT RAISE (ABORT, 'bad ' || coalesce (CAST (new.x AS TEXT), '<NULL>') || ' for %')",
                "SELECT RAISE (ABORT, 'unique_violation')",
                "SELECT RAISE (ABORT, 'made ' || new.x)",
            ]
        );
        for (body, message) in [
            ("RAISE EXCEPTION 'bad %';", "too few parameters"),
            ("RAISE EXCEPTION 'bad', 1;", "too many parameters"),
            ("RAISE;", "RAISE without parameters"),
        ] {
            let err = compile_for(body, "BEFORE", "INSERT").unwrap_err();
            assert!(err.to_string().contains(message), "{body}: {err}");
        }
    }

    #[test]
    fn case_statements_are_branches_over_a_value() {
        let (commands, _) = compile_for(
            "CASE NEW.x WHEN 1, 2 THEN NEW.y := 1; ELSE NEW.y := 2; END CASE; \
             CASE WHEN NEW.x > 9 THEN PERFORM log_big(NEW.x); END CASE; \
             RETURN NEW;",
            "BEFORE",
            "INSERT",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT __pl_set ('trg.INSERT.new.y', new.y)",
                "SELECT __pl_set ('trg.INSERT.case1', new.x)",
                "SELECT __pl_set ('trg.INSERT.c2', coalesce (__pl_get ('trg.INSERT.case1') IN (1, 2), 0))",
                "SELECT __pl_set ('trg.INSERT.new.y', 1) WHERE __pl_get ('trg.INSERT.c2')",
                "SELECT __pl_set ('trg.INSERT.new.y', 2) WHERE NOT __pl_get ('trg.INSERT.c2')",
                "SELECT __pl_set ('trg.INSERT.c3', coalesce (new.x > 9, 0))",
                "SELECT * FROM (SELECT log_big (new.x) log_big) WHERE __pl_get ('trg.INSERT.c3')",
                "SELECT RAISE (ABORT, 'case not found') WHERE NOT __pl_get ('trg.INSERT.c3')",
            ]
        );
    }

    #[test]
    fn declared_variables_get_their_first_value_at_the_start_of_each_firing() {
        let (commands, returned) = compile_source(
            "DECLARE v int := 5; w text; ok boolean := NEW.x > 0; \
             BEGIN \
               v := v + NEW.x; \
               INSERT INTO log VALUES (v, w); \
               IF ok THEN NEW.y := v; END IF; \
               RETURN NEW; \
             END",
            "BEFORE",
            "INSERT",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT __pl_set ('trg.INSERT.var.v', CAST (5 AS INTEGER))",
                "SELECT __pl_set ('trg.INSERT.var.w', NULL)",
                "SELECT __pl_set ('trg.INSERT.var.ok', new.x > 0)",
                "SELECT __pl_set ('trg.INSERT.new.y', new.y)",
                "SELECT __pl_set ('trg.INSERT.var.v', CAST (__pl_get ('trg.INSERT.var.v') + new.x AS INTEGER))",
                "INSERT INTO log VALUES (__pl_get ('trg.INSERT.var.v'), __pl_get ('trg.INSERT.var.w'))",
                "SELECT __pl_set ('trg.INSERT.c1', coalesce (__pl_get ('trg.INSERT.var.ok'), 0))",
                "SELECT __pl_set ('trg.INSERT.new.y', __pl_get ('trg.INSERT.var.v')) WHERE __pl_get ('trg.INSERT.c1')",
            ]
        );
        assert_eq!(returned, "row y = __pl_get ('trg.INSERT.new.y')");
    }

    #[test]
    fn select_into_sets_the_variables_and_found_from_the_first_row() {
        let (commands, _) = compile_source(
            "DECLARE a int; b text; \
             BEGIN \
               SELECT id, name INTO a, b FROM log WHERE id = NEW.x ORDER BY id; \
               IF NOT FOUND THEN RETURN NULL; END IF; \
               RETURN NEW; \
             END",
            "BEFORE",
            "INSERT",
        )
        .unwrap();
        assert_eq!(
            &commands[..4],
            &[
                "SELECT __pl_set ('trg.INSERT.var.found', 0)",
                "SELECT __pl_set ('trg.INSERT.var.a', NULL)",
                "SELECT __pl_set ('trg.INSERT.var.b', NULL)",
                "SELECT __pl_set ('trg.INSERT.ret', NULL)",
            ]
        );
        assert_eq!(
            commands[4],
            "SELECT __pl_set ('trg.INSERT.var.a', q.c0), __pl_set ('trg.INSERT.var.b', q.c1), \
             __pl_set ('trg.INSERT.var.found', q.k NOTNULL) \
             FROM (SELECT 1) LEFT OUTER JOIN (SELECT *, 1 AS k \
             FROM (SELECT id AS c0, name AS c1 FROM log WHERE id = new.x ORDER BY id NULLS LAST) LIMIT 1) \
             AS q ON 1"
        );
        assert_eq!(
            commands[5],
            "SELECT __pl_set ('trg.INSERT.c1', coalesce (NOT __pl_get ('trg.INSERT.var.found'), 0))"
        );
    }

    #[test]
    fn returning_into_sets_the_variable_from_the_one_row() {
        let (commands, _) = compile_source(
            "DECLARE v int; \
             BEGIN \
               INSERT INTO log (id) VALUES (NEW.x) RETURNING id INTO v; \
               NEW.y := v; \
               RETURN NEW; \
             END",
            "BEFORE",
            "INSERT",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT __pl_set ('trg.INSERT.var.found', 0)",
                "SELECT __pl_set ('trg.INSERT.var.v', NULL)",
                "SELECT __pl_set ('trg.INSERT.new.y', new.y)",
                "SELECT __pl_set ('trg.INSERT.var.v', NULL)",
                "SELECT __pl_set ('trg.INSERT.var.v.rows', 0)",
                "SELECT __pl_set ('trg.INSERT.var.found', 0)",
                "INSERT INTO log (id) VALUES (new.x) RETURNING __pl_set_one ('trg.INSERT.var.v', id), \
                 __pl_set ('trg.INSERT.var.found', 1)",
                "SELECT __pl_set ('trg.INSERT.new.y', __pl_get ('trg.INSERT.var.v'))",
            ]
        );
    }

    #[test]
    fn a_name_that_is_a_column_and_a_variable_is_an_error_when_the_statement_runs() {
        let (commands, _) = compile_source(
            "DECLARE id int := 1; \
             BEGIN \
               UPDATE log SET name = 'x' WHERE id = id; \
               INSERT INTO log (id, name) VALUES (id, 'y'); \
               RETURN NEW; \
             END",
            "AFTER",
            "INSERT",
        )
        .unwrap();
        assert_eq!(
            commands,
            vec![
                "SELECT __pl_set ('trg.INSERT.var.id', CAST (1 AS INTEGER))",
                "SELECT RAISE (ABORT, 'column reference \"id\" is ambiguous')",
                "INSERT INTO log (id, name) VALUES (__pl_get ('trg.INSERT.var.id'), 'y')",
            ]
        );
    }

    #[test]
    fn unsupported_variable_forms_are_errors() {
        for (source, message) in [
            (
                "DECLARE n log.id%TYPE; BEGIN RETURN NEW; END",
                "the type log.id%TYPE of the variable n",
            ),
            (
                "DECLARE r record; BEGIN RETURN NEW; END",
                "the record variable r",
            ),
            (
                "DECLARE v int; BEGIN SELECT id INTO STRICT v FROM log; RETURN NEW; END",
                "SELECT INTO STRICT",
            ),
            (
                "DECLARE v int; BEGIN SELECT * INTO v FROM log; RETURN NEW; END",
                "SELECT * with INTO",
            ),
            (
                "DECLARE v int; w int; BEGIN SELECT id INTO v, w FROM log; RETURN NEW; END",
                "1 columns for 2 targets",
            ),
            (
                "DECLARE v int; BEGIN INSERT INTO log (id) VALUES (1) INTO v; RETURN NEW; END",
                "INTO used with a command that cannot return data",
            ),
            (
                "DECLARE v int; BEGIN UPDATE log SET name = 'x' RETURNING id INTO v; RETURN NEW; END",
                "RETURNING INTO with UPDATE or DELETE is not supported yet",
            ),
        ] {
            let err = compile_source(source, "BEFORE", "INSERT").unwrap_err();
            assert!(err.to_string().contains(message), "{source}: {err}");
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
}
