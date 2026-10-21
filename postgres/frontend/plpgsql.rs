use crate::ast_walk::{walk_stmt_exprs_mut, walk_tree_mut};
use crate::session::{set_trigger_variable, trigger_variable};
use crate::trigger::{parse_trigger_definition, postgres_schema_name, trigger_definition};
use crate::user_function::{Language, UserFunction};
use pg_query::protobuf::node::Node;
use serde_json::Value as Json;
use std::sync::Arc;
use turso_core::dialect::{ChangedColumn, FunctionCatalog, TriggerBody};
use turso_core::native_ext::{FunctionArity, ScalarCall, ScalarFunction};
use turso_core::schema::{BTreeTable, Trigger};
use turso_core::types::IOResultOr;
use turso_core::{
    walk_expr, Connection, ExternalFunc, IOResult, LimboError, OpenOptions, Register, Result,
    Value, WalkControl,
};
use turso_parser::ast;
use turso_pg_parser::translator::{call_name, PostgreSQLTranslator};

pub(crate) fn compile_trigger(
    trigger: &Trigger,
    table: &BTreeTable,
    functions: &dyn FunctionCatalog,
) -> Result<Option<TriggerBody>> {
    let Some(definition) = trigger_definition(trigger) else {
        return Ok(None);
    };
    let statement = parse_trigger_definition(&definition)?;
    let function_call_name =
        call_name(&statement.funcname).map_err(|e| LimboError::ParseError(e.to_string()))?;
    let Some(function) = functions.find_function(&function_call_name, 0) else {
        return Err(LimboError::ParseError(format!(
            "function {function_call_name}() does not exist"
        )));
    };
    let user_function = UserFunction::parse(&function)?;
    if user_function.language != Language::Plpgsql {
        return Err(LimboError::ParseError(format!(
            "function {} must return type trigger",
            function.name
        )));
    }
    let parsed = pg_query::parse_plpgsql(&function.definition)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    let malformed =
        || LimboError::Corrupt(format!("malformed PL/pgSQL function {}", function.name));
    let plpgsql_function = parsed
        .get(0)
        .and_then(|function| function.get("PLpgSQL_function"))
        .ok_or_else(malformed)?;
    let action = plpgsql_function.get("action").ok_or_else(malformed)?;
    let args = statement
        .args
        .iter()
        .filter_map(|arg| match &arg.node {
            Some(Node::String(arg)) => Some(arg.sval.clone()),
            _ => None,
        })
        .collect();
    let schema = statement.relation.as_ref().map_or("public", |relation| {
        postgres_schema_name(&relation.schemaname)
    });
    let op = match trigger.event {
        ast::TriggerEvent::Insert => "INSERT",
        ast::TriggerEvent::Update | ast::TriggerEvent::UpdateOf(_) => "UPDATE",
        ast::TriggerEvent::Delete => "DELETE",
    };
    let mut compiler = Compiler {
        function_name: function.name.clone(),
        variable_prefix: format!(
            "{schema}.{}.{}.{op}",
            trigger.table_name, statement.trigname
        ),
        trigger: TriggerConstants {
            op,
            when: match trigger.time {
                ast::TriggerTime::Before => "BEFORE",
                ast::TriggerTime::After => "AFTER",
                ast::TriggerTime::InsteadOf => "INSTEAD OF",
            },
            name: statement.trigname.clone(),
            table_name: trigger.table_name.clone(),
            table_schema: schema.to_string(),
            args,
        },
        table,
        records: Records::new(plpgsql_function),
        variable_count: 0,
        statements: Vec::new(),
        new_values: Vec::new(),
        branch_condition: None,
        return_variable: None,
        conditional_returns: Vec::new(),
        changed_columns: Vec::new(),
    };
    let flow = compiler.compile_statement(action)?;
    compiler.finish(flow)?;
    Ok(Some(TriggerBody {
        statements: compiler.statements,
        changed_columns: compiler.changed_columns,
    }))
}

/// Register `__pl_set(key, value)` and `__pl_get(key)`, which keep the values
/// of the variables of the compiled triggers in the session.
pub(crate) fn register_trigger_variable_functions(mut options: OpenOptions) -> OpenOptions {
    for (name, arg_count, function) in [
        (SET_VARIABLE, 2, TriggerVariableFunction::Set),
        (GET_VARIABLE, 1, TriggerVariableFunction::Get),
    ] {
        options = options.extension_function(
            ExternalFunc::new_native_scalar(
                name.to_string(),
                FunctionArity::Exact(arg_count),
                false,
                function,
            )
            .expect("the trigger variable functions have valid argument counts"),
        );
    }
    options
}

const SET_VARIABLE: &str = "__pl_set";
const GET_VARIABLE: &str = "__pl_get";

struct TriggerConstants {
    op: &'static str,
    when: &'static str,
    name: String,
    table_name: String,
    table_schema: String,
    args: Vec<String>,
}

struct Compiler<'a> {
    function_name: String,
    /// The start of the keys of the variables of this trigger and event.
    variable_prefix: String,
    trigger: TriggerConstants,
    table: &'a BTreeTable,
    records: Records<'a>,
    variable_count: usize,
    statements: Vec<ast::Stmt>,
    /// The fields of NEW that the function assigned, in the order of the
    /// first assignment.
    new_values: Vec<(String, NewValue)>,
    /// The condition of the branch that compiles now. `None` at the top level.
    branch_condition: Option<ast::Expr>,
    /// The variable that a RETURN in a branch sets, so that the later
    /// statements do not run.
    return_variable: Option<String>,
    conditional_returns: Vec<ReturnAction>,
    changed_columns: Vec<ChangedColumn>,
}

/// The value of a field of NEW after an assignment.
#[derive(Clone)]
enum NewValue {
    /// An expression that reads NEW as the trigger got it. The compiler puts
    /// it in each place that reads the field, so it must give the same value
    /// each time.
    Expression(ast::Expr),
    /// A variable that has the value.
    Variable(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continues,
    Returns,
}

/// What core does with the row when the trigger function returns.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReturnAction {
    WriteNew,
    WriteOld,
    Skip,
    Continue,
}

impl ReturnAction {
    fn name(self) -> &'static str {
        match self {
            Self::WriteNew => "new",
            Self::WriteOld => "old",
            Self::Skip => "skip",
            Self::Continue => "continue",
        }
    }
}

/// A branch of IF or CASE: the condition and the statements.
struct Branch<'j> {
    condition: ast::Expr,
    statements: Vec<&'j Json>,
}

/// What happens when no branch of IF or CASE runs.
enum NoBranch<'j> {
    Else(Vec<&'j Json>),
    CaseNotFound,
}

impl Compiler<'_> {
    fn compile_statement(&mut self, statement: &Json) -> Result<Flow> {
        let (kind, body) = statement
            .as_object()
            .and_then(|object| object.iter().next())
            .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL statement".into()))?;
        let line = body.get("lineno").and_then(Json::as_u64).unwrap_or(0);
        let statements_before = self.statements.len();
        let new_values_before = self.new_values.clone();
        let result = match kind.as_str() {
            "PLpgSQL_stmt_block" => self.compile_block(body),
            "PLpgSQL_stmt_execsql" => self.compile_sql_statement(body).map(|()| Flow::Continues),
            "PLpgSQL_stmt_assign" => self.compile_assignment(body).map(|()| Flow::Continues),
            "PLpgSQL_stmt_if" => self.compile_if(body),
            "PLpgSQL_stmt_case" => self.compile_case(body),
            "PLpgSQL_stmt_raise" => self.compile_raise(body),
            "PLpgSQL_stmt_perform" => self.compile_perform(body).map(|()| Flow::Continues),
            "PLpgSQL_stmt_return" if body.get("lineno").is_none() => Ok(Flow::Continues),
            "PLpgSQL_stmt_return" => self.compile_return(body),
            kind => Err(LimboError::ParseError(format!(
                "{} is not supported yet",
                statement_description(kind)
            ))),
        };
        match result.map_err(|error| self.with_context(error, line, kind)) {
            Err(LimboError::ParseError(message)) if !has_nested_statements(kind) => {
                self.statements.truncate(statements_before);
                self.new_values = new_values_before;
                self.statements.push(select_where(
                    raise_abort_expr(text_literal(&message)),
                    self.statement_condition(),
                ));
                Ok(self.flow_after_error())
            }
            result => result,
        }
    }

    fn compile_block(&mut self, block: &Json) -> Result<Flow> {
        if block.get("exceptions").is_some() {
            return Err(LimboError::ParseError(
                "EXCEPTION blocks are not supported".into(),
            ));
        }
        self.compile_statements(json_array(block, "body"))
    }

    fn compile_statements<'j>(
        &mut self,
        statements: impl IntoIterator<Item = &'j Json>,
    ) -> Result<Flow> {
        for statement in statements {
            if self.compile_statement(statement)? == Flow::Returns {
                return Ok(Flow::Returns);
            }
        }
        Ok(Flow::Continues)
    }

    fn compile_sql_statement(&mut self, body: &Json) -> Result<()> {
        if body.get("into").and_then(Json::as_bool) == Some(true) {
            return Err(LimboError::ParseError("INTO is not supported yet".into()));
        }
        let query = expression_query(body.get("sqlstmt"))?;
        let mut stmt = self.translate_statement(query)?;
        if matches!(stmt, ast::Stmt::Select(_)) {
            return Err(LimboError::ParseError(
                "query has no destination for result data".into(),
            ));
        }
        walk_stmt_exprs_mut(&mut stmt, &mut |expr| self.replace_trigger_values(expr))?;
        let stmt = with_condition(stmt, self.statement_condition())?;
        self.statements.push(stmt);
        Ok(())
    }

    fn compile_perform(&mut self, body: &Json) -> Result<()> {
        let query = expression_query(body.get("expr"))?;
        let mut stmt = self.translate_statement(query)?;
        walk_stmt_exprs_mut(&mut stmt, &mut |expr| self.replace_trigger_values(expr))?;
        let ast::Stmt::Select(select) = stmt else {
            return Err(LimboError::Corrupt("PERFORM without a query".into()));
        };
        let select = select_with_condition(select, self.statement_condition())?;
        self.statements.push(ast::Stmt::Select(select));
        Ok(())
    }

    fn compile_assignment(&mut self, body: &Json) -> Result<()> {
        let varno = body
            .get("varno")
            .and_then(Json::as_u64)
            .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL assignment".into()))?;
        let field = match self.records.assignment_target(varno) {
            Target::NewField(field) => field,
            Target::OldField => {
                return Err(LimboError::ParseError(
                    "assignment to OLD is not supported".into(),
                ))
            }
            Target::Variable => {
                return Err(LimboError::ParseError(
                    "variables are not supported yet".into(),
                ))
            }
        };
        let query = expression_query(body.get("expr"))?;
        let mut value = translate_expression(assigned_value(query)?)?;
        walk_tree_mut(&mut value, &mut |expr| self.replace_trigger_values(expr))?;
        let condition = self.statement_condition();
        match self.table.get_column(field) {
            None => {
                self.statements.push(select_where(
                    raise_abort_expr(text_literal(&format!(
                        "record \"new\" has no field \"{field}\""
                    ))),
                    condition,
                ));
                return Ok(());
            }
            Some((_, column)) if column.is_generated() => return Ok(()),
            Some(_) => {}
        }
        let position = self
            .new_values
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case(field));
        let current_variable = position.and_then(|position| match &self.new_values[position].1 {
            NewValue::Variable(key) => Some(key.clone()),
            NewValue::Expression(_) => None,
        });
        if condition.is_none() && current_variable.is_none() && can_be_calculated_again(&value) {
            match position {
                Some(position) => self.new_values[position].1 = NewValue::Expression(value),
                None => self
                    .new_values
                    .push((field.to_string(), NewValue::Expression(value))),
            }
            return Ok(());
        }
        let key = match current_variable {
            Some(key) => key,
            None => {
                let key = self.new_variable(&format!("new.{field}"));
                let current = match position {
                    Some(position) => self.read_new_value(position),
                    None => {
                        let mut current = record_field("new", field);
                        walk_tree_mut(&mut current, &mut |expr| self.replace_trigger_values(expr))?;
                        current
                    }
                };
                self.statements
                    .push(select_statement(set_variable(&key, current)));
                match position {
                    Some(position) => self.new_values[position].1 = NewValue::Variable(key.clone()),
                    None => self
                        .new_values
                        .push((field.to_string(), NewValue::Variable(key.clone()))),
                }
                key
            }
        };
        self.statements
            .push(select_where(set_variable(&key, value), condition));
        Ok(())
    }

    fn compile_if(&mut self, body: &Json) -> Result<Flow> {
        let mut branches = vec![Branch {
            condition: self.condition_expression(body.get("cond"), None)?,
            statements: json_array(body, "then_body").collect(),
        }];
        for elsif in
            json_array(body, "elsif_list").filter_map(|elsif| elsif.get("PLpgSQL_if_elsif"))
        {
            branches.push(Branch {
                condition: self.condition_expression(elsif.get("cond"), None)?,
                statements: json_array(elsif, "stmts").collect(),
            });
        }
        let no_branch = NoBranch::Else(json_array(body, "else_body").collect());
        self.compile_branches(branches, no_branch)
    }

    fn compile_case(&mut self, body: &Json) -> Result<Flow> {
        let case_value = match body.get("t_expr") {
            Some(value) => {
                let varno = body.get("t_varno").and_then(Json::as_u64).unwrap_or(0);
                let mut value = translate_expression(expression_query(Some(value))?)?;
                walk_tree_mut(&mut value, &mut |expr| self.replace_trigger_values(expr))?;
                let value = if constant_value(&value).is_some() {
                    value
                } else {
                    let key = self.new_variable("case");
                    let value = match self.statement_condition() {
                        Some(condition) => case_when(condition, value, null()),
                        None => value,
                    };
                    self.statements
                        .push(select_statement(set_variable(&key, value)));
                    get_variable(&key)
                };
                Some((format!("__Case__Variable_{varno}__"), value))
            }
            None => None,
        };
        let mut branches = Vec::new();
        for when in
            json_array(body, "case_when_list").filter_map(|when| when.get("PLpgSQL_case_when"))
        {
            branches.push(Branch {
                condition: self.condition_expression(when.get("expr"), case_value.as_ref())?,
                statements: json_array(when, "stmts").collect(),
            });
        }
        let no_branch = if body.get("have_else").and_then(Json::as_bool) == Some(true) {
            NoBranch::Else(json_array(body, "else_stmts").collect())
        } else {
            NoBranch::CaseNotFound
        };
        self.compile_branches(branches, no_branch)
    }

    /// Compile the branches of IF or CASE. Each condition is calculated one
    /// time into a variable, because a statement in a branch can change what
    /// the condition reads. A condition that is constant, such as one on
    /// `TG_OP`, selects its branch at compile time.
    fn compile_branches(
        &mut self,
        branches: Vec<Branch<'_>>,
        no_branch: NoBranch<'_>,
    ) -> Result<Flow> {
        let outer_branch = self.branch_condition.clone();
        let outer = self.statement_condition();
        let mut earlier = Vec::new();
        let mut every_branch_returns = true;
        let mut can_reach_no_branch = true;
        for branch in branches {
            let condition = branch.condition;
            match constant_value(&condition).as_ref().map(Constant::is_true) {
                Some(false) => continue,
                Some(true) => {
                    let branch_condition = and_all(outer_branch.clone(), earlier.iter().map(not));
                    let flow = self.compile_branch(branch_condition, branch.statements)?;
                    every_branch_returns &= flow == Flow::Returns;
                    can_reach_no_branch = false;
                    break;
                }
                None => {
                    let key = self.new_variable("condition");
                    let value = coalesce_false(condition);
                    let value = match and_all(outer.clone(), earlier.iter().map(not)) {
                        Some(reached_when) => {
                            case_when(reached_when, value, ast::Expr::Literal(ast::Literal::False))
                        }
                        None => value,
                    };
                    self.statements
                        .push(select_statement(set_variable(&key, value)));
                    let branch_condition = and(outer_branch.clone(), get_variable(&key));
                    let flow = self.compile_branch(Some(branch_condition), branch.statements)?;
                    every_branch_returns &= flow == Flow::Returns;
                    earlier.push(get_variable(&key));
                }
            }
        }
        if can_reach_no_branch {
            let branch_condition = and_all(outer_branch, earlier.iter().map(not));
            let flow = match no_branch {
                NoBranch::Else(statements) => self.compile_branch(branch_condition, statements)?,
                NoBranch::CaseNotFound => {
                    let saved = std::mem::replace(&mut self.branch_condition, branch_condition);
                    let condition = self.statement_condition();
                    self.branch_condition = saved;
                    self.statements.push(select_where(
                        raise_abort_expr(text_literal("case not found")),
                        condition.clone(),
                    ));
                    if condition.is_none() {
                        Flow::Returns
                    } else {
                        Flow::Continues
                    }
                }
            };
            every_branch_returns &= flow == Flow::Returns;
        }
        Ok(if every_branch_returns {
            Flow::Returns
        } else {
            Flow::Continues
        })
    }

    fn compile_branch(
        &mut self,
        condition: Option<ast::Expr>,
        statements: Vec<&Json>,
    ) -> Result<Flow> {
        let saved = std::mem::replace(&mut self.branch_condition, condition);
        let flow = self.compile_statements(statements);
        self.branch_condition = saved;
        flow
    }

    fn condition_expression(
        &self,
        condition: Option<&Json>,
        case_value: Option<&(String, ast::Expr)>,
    ) -> Result<ast::Expr> {
        let mut condition = translate_expression(expression_query(condition)?)?;
        walk_tree_mut(&mut condition, &mut |expr| {
            if let (Some((name, value)), ast::Expr::Id(id)) = (case_value, &*expr) {
                if id.as_str() == name {
                    *expr = value.clone();
                    return Ok(WalkControl::SkipChildren);
                }
            }
            self.replace_trigger_values(expr)
        })?;
        Ok(condition)
    }

    fn compile_raise(&mut self, body: &Json) -> Result<Flow> {
        const ERROR_LEVEL: u64 = 21;
        if body
            .get("elog_level")
            .and_then(Json::as_u64)
            .unwrap_or(ERROR_LEVEL)
            < ERROR_LEVEL
        {
            return Ok(Flow::Continues);
        }
        let mut message = match body.get("message").and_then(Json::as_str) {
            Some(format) => Some(self.raise_message(format, json_array(body, "params"))?),
            None => None,
        };
        let mut error_code = None;
        for option in
            json_array(body, "options").filter_map(|option| option.get("PLpgSQL_raise_option"))
        {
            const ERRCODE_OPTION: u64 = 0;
            const MESSAGE_OPTION: u64 = 1;
            let option_type = option.get("opt_type").and_then(Json::as_u64);
            if option_type != Some(MESSAGE_OPTION) && option_type != Some(ERRCODE_OPTION) {
                continue;
            }
            if option_type == Some(MESSAGE_OPTION) && message.is_some() {
                return Err(LimboError::ParseError(
                    "RAISE option already specified: MESSAGE".into(),
                ));
            }
            let mut value = translate_expression(expression_query(option.get("expr"))?)?;
            walk_tree_mut(&mut value, &mut |expr| self.replace_trigger_values(expr))?;
            if option_type == Some(MESSAGE_OPTION) {
                message = Some(cast_to_text(value));
            } else {
                error_code = Some(cast_to_text(value));
            }
        }
        let message = match (message, body.get("condname").and_then(Json::as_str)) {
            (Some(message), _) => message,
            (None, Some(condition_name)) => text_literal(condition_name),
            (None, None) => match error_code {
                Some(error_code) => error_code,
                None => {
                    return Err(LimboError::ParseError(
                        "RAISE without parameters is not supported".into(),
                    ))
                }
            },
        };
        let condition = self.statement_condition();
        let flow = if condition.is_none() {
            Flow::Returns
        } else {
            Flow::Continues
        };
        self.statements
            .push(select_where(raise_abort_expr(message), condition));
        Ok(flow)
    }

    /// The message of `RAISE '<format>', params`: each `%` takes the next
    /// parameter, and `%%` is one `%`.
    fn raise_message<'j>(
        &self,
        format: &str,
        params: impl Iterator<Item = &'j Json>,
    ) -> Result<ast::Expr> {
        let mut params = params.peekable();
        let mut parts = Vec::new();
        let mut text = String::new();
        let mut chars = format.chars().peekable();
        while let Some(c) = chars.next() {
            if c != '%' {
                text.push(c);
                continue;
            }
            if chars.peek() == Some(&'%') {
                chars.next();
                text.push('%');
                continue;
            }
            let Some(param) = params.next() else {
                return Err(LimboError::ParseError(
                    "too few parameters specified for RAISE".into(),
                ));
            };
            let mut value = translate_expression(expression_query(Some(param))?)?;
            walk_tree_mut(&mut value, &mut |expr| self.replace_trigger_values(expr))?;
            parts.push(text_literal(&std::mem::take(&mut text)));
            parts.push(function_call(
                "coalesce",
                vec![cast_to_text(value), text_literal("<NULL>")],
            ));
        }
        if params.peek().is_some() {
            return Err(LimboError::ParseError(
                "too many parameters specified for RAISE".into(),
            ));
        }
        parts.push(text_literal(&text));
        Ok(parts
            .into_iter()
            .reduce(|left, right| {
                ast::Expr::Binary(Box::new(left), ast::Operator::Concat, Box::new(right))
            })
            .expect("the message has at least one part"))
    }

    fn compile_return(&mut self, body: &Json) -> Result<Flow> {
        let Some(value) = body.get("expr") else {
            return Err(LimboError::ParseError(
                "RETURN in a trigger function needs a value".into(),
            ));
        };
        let record = self.returned_record(expression_query(Some(value))?)?;
        let action = match (self.trigger.when, record, self.trigger.op) {
            ("BEFORE", Record::New, "INSERT" | "UPDATE") => ReturnAction::WriteNew,
            ("BEFORE", Record::Old, "UPDATE") => ReturnAction::WriteOld,
            ("BEFORE", Record::Old, "DELETE") => ReturnAction::Continue,
            ("BEFORE", _, _) => ReturnAction::Skip,
            _ => ReturnAction::Continue,
        };
        match action {
            ReturnAction::WriteNew => self.check_new_row()?,
            ReturnAction::WriteOld => self.check_old_row_key(),
            ReturnAction::Skip | ReturnAction::Continue => {}
        }
        match self.statement_condition() {
            None => match action {
                ReturnAction::WriteNew => self.changed_columns = self.new_row(),
                ReturnAction::WriteOld => self.changed_columns = self.old_row(None),
                ReturnAction::Skip => self.statements.push(raise_ignore()),
                ReturnAction::Continue => {}
            },
            Some(condition) => {
                let key = self.return_variable();
                self.statements.push(select_where(
                    set_variable(&key, text_literal(action.name())),
                    Some(condition),
                ));
                self.conditional_returns.push(action);
            }
        }
        Ok(Flow::Returns)
    }

    /// After the body: the end of the function without RETURN gives an
    /// error, and a RETURN in a branch decides what happens to the row.
    fn finish(&mut self, flow: Flow) -> Result<()> {
        if flow == Flow::Continues {
            self.statements.push(select_where(
                raise_abort_expr(text_literal(
                    "control reached end of trigger procedure without RETURN",
                )),
                self.statement_condition(),
            ));
        }
        let Some(key) = self.return_variable.clone() else {
            return Ok(());
        };
        let returned = |action: ReturnAction| {
            ast::Expr::Binary(
                Box::new(get_variable(&key)),
                ast::Operator::Equals,
                Box::new(text_literal(action.name())),
            )
        };
        if self.conditional_returns.contains(&ReturnAction::Skip) {
            self.statements.push(select_where(
                ast::Expr::Raise(ast::ResolveType::Ignore, None),
                Some(returned(ReturnAction::Skip)),
            ));
        }
        let writes_old = self.conditional_returns.contains(&ReturnAction::WriteOld);
        if writes_old {
            self.changed_columns = self.old_row(Some(returned(ReturnAction::WriteOld)));
        } else if self.conditional_returns.contains(&ReturnAction::WriteNew) {
            self.changed_columns = self.new_row();
        }
        Ok(())
    }

    /// The changed columns of RETURN NEW.
    fn new_row(&self) -> Vec<ChangedColumn> {
        (0..self.new_values.len())
            .map(|position| ChangedColumn {
                name: self.new_values[position].0.clone(),
                value: self.read_new_value(position),
            })
            .collect()
    }

    /// The changed columns of RETURN OLD. With `old_when`, the function
    /// returned OLD only when the condition is true, and NEW otherwise.
    fn old_row(&self, old_when: Option<ast::Expr>) -> Vec<ChangedColumn> {
        self.table
            .columns()
            .iter()
            .filter(|column| !column.is_generated() && !column.is_rowid_alias())
            .filter_map(|column| column.name.clone())
            .map(|name| {
                let old = record_field("old", &name);
                let value = match &old_when {
                    None => old,
                    Some(condition) => {
                        let new = match self
                            .new_values
                            .iter()
                            .position(|(field, _)| field.eq_ignore_ascii_case(&name))
                        {
                            Some(position) => self.read_new_value(position),
                            None => record_field("new", &name),
                        };
                        case_when(condition.clone(), old, new)
                    }
                };
                ChangedColumn { name, value }
            })
            .collect()
    }

    fn check_new_row(&self) -> Result<()> {
        match self.new_values.iter().find(|(name, _)| {
            self.table
                .get_column(name)
                .is_some_and(|(_, column)| column.is_rowid_alias())
        }) {
            Some((name, _)) => Err(LimboError::ParseError(format!(
                "a BEFORE trigger cannot change the INTEGER PRIMARY KEY column \"{name}\" yet"
            ))),
            None => Ok(()),
        }
    }

    /// RETURN OLD in an UPDATE that changes the INTEGER PRIMARY KEY must keep
    /// the old key, but core cannot change that column after the triggers.
    fn check_old_row_key(&mut self) {
        let Some(name) = self
            .table
            .columns()
            .iter()
            .find(|column| column.is_rowid_alias())
            .and_then(|column| column.name.clone())
        else {
            return;
        };
        let key_changes = ast::Expr::Binary(
            Box::new(record_field("new", &name)),
            ast::Operator::IsNot,
            Box::new(record_field("old", &name)),
        );
        self.statements.push(select_where(
            raise_abort_expr(text_literal(&format!(
                "RETURN OLD cannot keep the old value of the INTEGER PRIMARY KEY column \"{name}\" yet"
            ))),
            Some(and(self.statement_condition(), key_changes)),
        ));
    }

    /// The record that `RETURN <query>` returns for this event. NEW is null in
    /// a delete trigger, and OLD is null in an insert trigger.
    fn returned_record(&self, query: &str) -> Result<Record> {
        let unsupported = || LimboError::ParseError(format!("RETURN {query} is not supported yet"));
        let parsed = parse_expression(query)?;
        let candidates = match &parsed.node {
            Some(Node::CoalesceExpr(coalesce)) => coalesce.args.iter().collect(),
            _ => vec![&parsed],
        };
        for candidate in candidates {
            let record = match &candidate.node {
                Some(Node::AConst(constant)) if constant.isnull => Record::Null,
                Some(Node::ColumnRef(column)) => match column.fields.as_slice() {
                    [field] => match &field.node {
                        Some(Node::String(name)) if name.sval == "new" => Record::New,
                        Some(Node::String(name)) if name.sval == "old" => Record::Old,
                        _ => return Err(unsupported()),
                    },
                    _ => return Err(unsupported()),
                },
                _ => return Err(unsupported()),
            };
            let is_null = match record {
                Record::New => self.trigger.op == "DELETE",
                Record::Old => self.trigger.op == "INSERT",
                Record::Null => true,
            };
            if !is_null {
                return Ok(record);
            }
        }
        Ok(Record::Null)
    }

    /// The condition of the statement that compiles now: the condition of
    /// the branch, and no RETURN yet.
    fn statement_condition(&self) -> Option<ast::Expr> {
        let no_return_yet = self
            .return_variable
            .as_ref()
            .map(|key| ast::Expr::IsNull(Box::new(get_variable(key))));
        match (self.branch_condition.clone(), no_return_yet) {
            (Some(branch), Some(no_return_yet)) => Some(and(Some(branch), no_return_yet)),
            (branch, no_return_yet) => branch.or(no_return_yet),
        }
    }

    /// The flow after a statement that always gives an error when it runs.
    fn flow_after_error(&self) -> Flow {
        match self.statement_condition() {
            None => Flow::Returns,
            Some(_) => Flow::Continues,
        }
    }

    fn return_variable(&mut self) -> String {
        if let Some(key) = &self.return_variable {
            return key.clone();
        }
        let key = format!("{}.return", self.variable_prefix);
        self.statements
            .push(select_statement(set_variable(&key, null())));
        self.return_variable = Some(key.clone());
        key
    }

    fn new_variable(&mut self, name: &str) -> String {
        self.variable_count += 1;
        format!("{}.{name}.{}", self.variable_prefix, self.variable_count)
    }

    fn read_new_value(&self, position: usize) -> ast::Expr {
        match &self.new_values[position].1 {
            NewValue::Expression(value) => value.clone(),
            NewValue::Variable(key) => get_variable(key),
        }
    }

    fn translate_statement(&self, query: &str) -> Result<ast::Stmt> {
        let parsed =
            turso_pg_parser::parse(query).map_err(|e| LimboError::ParseError(e.to_string()))?;
        if parsed.protobuf.stmts.len() != 1 {
            return Err(LimboError::ParseError(format!(
                "expected one SQL statement: {query}"
            )));
        }
        PostgreSQLTranslator::new()
            .translate(&parsed)
            .map_err(|e| LimboError::ParseError(e.to_string()))
    }

    fn replace_trigger_values(&self, expr: &mut ast::Expr) -> Result<WalkControl> {
        let replacement = match expr {
            ast::Expr::Id(name) => match name.as_str().to_ascii_lowercase().as_str() {
                "tg_op" => text_literal(self.trigger.op),
                "tg_when" => text_literal(self.trigger.when),
                "tg_level" => text_literal("ROW"),
                "tg_name" => text_literal(&self.trigger.name),
                "tg_table_name" | "tg_relname" => text_literal(&self.trigger.table_name),
                "tg_table_schema" => text_literal(&self.trigger.table_schema),
                "tg_nargs" => {
                    ast::Expr::Literal(ast::Literal::Numeric(self.trigger.args.len().to_string()))
                }
                "tg_argv" => {
                    return Err(LimboError::ParseError(
                        "TG_ARGV without a subscript is not supported".into(),
                    ))
                }
                _ => return Ok(WalkControl::Continue),
            },
            ast::Expr::FunctionCall { name, args, .. }
                if name.as_str().eq_ignore_ascii_case("array_element")
                    && args.len() == 2
                    && matches!(args[0].as_ref(), ast::Expr::Id(array) if array.as_str().eq_ignore_ascii_case("tg_argv")) =>
            {
                let mut index = args[1].as_ref().clone();
                walk_tree_mut(&mut index, &mut |e| self.replace_trigger_values(e))?;
                self.trigger_argument(index)
            }
            ast::Expr::Qualified(record, field) => {
                let record = record.as_str().to_ascii_lowercase();
                if record == "new" {
                    if let Some(position) = self
                        .new_values
                        .iter()
                        .position(|(name, _)| name.eq_ignore_ascii_case(field.as_str()))
                    {
                        *expr = self.read_new_value(position);
                        return Ok(WalkControl::SkipChildren);
                    }
                }
                let missing = (record == "new" && self.trigger.op == "DELETE")
                    || (record == "old" && self.trigger.op == "INSERT");
                if !missing {
                    return Ok(WalkControl::Continue);
                }
                null()
            }
            _ => return Ok(WalkControl::Continue),
        };
        *expr = replacement;
        Ok(WalkControl::SkipChildren)
    }

    fn trigger_argument(&self, index: ast::Expr) -> ast::Expr {
        if let ast::Expr::Literal(ast::Literal::Numeric(index)) = &index {
            return match index
                .parse::<usize>()
                .ok()
                .and_then(|i| self.trigger.args.get(i))
            {
                Some(arg) => text_literal(arg),
                None => null(),
            };
        }
        ast::Expr::Case {
            base: Some(Box::new(index)),
            when_then_pairs: self
                .trigger
                .args
                .iter()
                .enumerate()
                .map(|(i, arg)| {
                    (
                        Box::new(ast::Expr::Literal(ast::Literal::Numeric(i.to_string()))),
                        Box::new(text_literal(arg)),
                    )
                })
                .collect(),
            else_expr: None,
        }
    }

    fn with_context(&self, error: LimboError, line: u64, kind: &str) -> LimboError {
        match error {
            LimboError::ParseError(message) if !message.contains("PL/pgSQL function") => {
                LimboError::ParseError(format!(
                    "{message} (PL/pgSQL function {}() line {line} at {})",
                    self.function_name,
                    statement_description(kind)
                ))
            }
            error => error,
        }
    }
}

/// The NEW and OLD records of a trigger function, and the record fields that
/// its assignments name.
struct Records<'a> {
    datums: &'a [Json],
    new_varno: Option<u64>,
    old_varno: Option<u64>,
}

enum Target<'a> {
    NewField(&'a str),
    OldField,
    Variable,
}

impl<'a> Records<'a> {
    fn new(function: &'a Json) -> Self {
        Self {
            datums: function
                .get("datums")
                .and_then(Json::as_array)
                .map_or(&[], Vec::as_slice),
            new_varno: function.get("new_varno").and_then(Json::as_u64),
            old_varno: function.get("old_varno").and_then(Json::as_u64),
        }
    }

    fn assignment_target(&self, varno: u64) -> Target<'a> {
        let Some(field) = usize::try_from(varno)
            .ok()
            .and_then(|varno| self.datums.get(varno))
            .and_then(|datum| datum.get("PLpgSQL_recfield"))
        else {
            return Target::Variable;
        };
        let parent = field.get("recparentno").and_then(Json::as_u64);
        match field.get("fieldname").and_then(Json::as_str) {
            Some(name) if parent.is_some() && parent == self.new_varno => Target::NewField(name),
            Some(_) if parent.is_some() && parent == self.old_varno => Target::OldField,
            _ => Target::Variable,
        }
    }
}

#[derive(Clone, Copy)]
enum Record {
    New,
    Old,
    Null,
}

/// `__pl_set(key, value)` stores a value and returns it. `__pl_get(key)`
/// returns the stored value, or NULL.
#[derive(Debug, Clone, Copy)]
enum TriggerVariableFunction {
    Set,
    Get,
}

impl ScalarFunction for TriggerVariableFunction {
    type Call = Self;

    fn create_call(&self) -> Result<Self::Call> {
        Ok(*self)
    }
}

impl ScalarCall for TriggerVariableFunction {
    fn step(&mut self, connection: &Arc<Connection>, args: &[Register]) -> IOResultOr<Value> {
        let Value::Text(key) = args[0].get_value() else {
            return Err(LimboError::InvalidArgument(
                "the key of a trigger variable must be text".to_string(),
            )
            .into());
        };
        let value = match self {
            Self::Set => {
                let value = args[1].get_value().clone();
                set_trigger_variable(connection, key.as_str(), value.clone())?;
                value
            }
            Self::Get => trigger_variable(connection, key.as_str())?,
        };
        Ok(IOResult::Done(value))
    }
}

fn statement_description(kind: &str) -> &str {
    match kind {
        "PLpgSQL_stmt_block" => "statement block",
        "PLpgSQL_stmt_assign" => "assignment",
        "PLpgSQL_stmt_if" => "IF",
        "PLpgSQL_stmt_case" => "CASE",
        "PLpgSQL_stmt_loop" => "LOOP",
        "PLpgSQL_stmt_while" => "WHILE",
        "PLpgSQL_stmt_fori" | "PLpgSQL_stmt_fors" | "PLpgSQL_stmt_forc" => "FOR",
        "PLpgSQL_stmt_foreach_a" => "FOREACH",
        "PLpgSQL_stmt_exit" => "EXIT",
        "PLpgSQL_stmt_return" => "RETURN",
        "PLpgSQL_stmt_return_next" => "RETURN NEXT",
        "PLpgSQL_stmt_return_query" => "RETURN QUERY",
        "PLpgSQL_stmt_raise" => "RAISE",
        "PLpgSQL_stmt_assert" => "ASSERT",
        "PLpgSQL_stmt_execsql" => "SQL statement",
        "PLpgSQL_stmt_dynexecute" => "EXECUTE",
        "PLpgSQL_stmt_dynfors" => "FOR over EXECUTE statement",
        "PLpgSQL_stmt_getdiag" => "GET DIAGNOSTICS",
        "PLpgSQL_stmt_open" => "OPEN",
        "PLpgSQL_stmt_fetch" => "FETCH",
        "PLpgSQL_stmt_close" => "CLOSE",
        "PLpgSQL_stmt_perform" => "PERFORM",
        "PLpgSQL_stmt_call" => "CALL",
        "PLpgSQL_stmt_commit" => "COMMIT",
        "PLpgSQL_stmt_rollback" => "ROLLBACK",
        kind => kind,
    }
}

fn json_array<'a>(value: &'a Json, key: &str) -> impl Iterator<Item = &'a Json> {
    value
        .get(key)
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
}

/// Whether a statement of this kind has statements inside it that the
/// compiler compiles. An error of another statement becomes an error when the
/// statement runs, as in PostgreSQL, so that it does not stop a trigger
/// whose branch with that statement does not run.
fn has_nested_statements(kind: &str) -> bool {
    matches!(
        kind,
        "PLpgSQL_stmt_block" | "PLpgSQL_stmt_if" | "PLpgSQL_stmt_case"
    )
}

fn expression_query(expression: Option<&Json>) -> Result<&str> {
    expression
        .and_then(|expression| expression.get("PLpgSQL_expr"))
        .and_then(|expression| expression.get("query"))
        .and_then(Json::as_str)
        .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL expression".into()))
}

/// The text after the target of an assignment such as `NEW.x := value` or
/// `NEW."X" = value`.
fn assigned_value(assignment: &str) -> Result<&str> {
    let unsupported =
        || LimboError::ParseError(format!("assignment \"{assignment}\" is not supported yet"));
    let mut rest = assignment;
    loop {
        rest = rest.trim_start();
        rest = match rest.strip_prefix('"') {
            Some(quoted) => {
                let mut end = 0;
                let bytes = quoted.as_bytes();
                loop {
                    match bytes.get(end) {
                        Some(b'"') if bytes.get(end + 1) == Some(&b'"') => end += 2,
                        Some(b'"') => break,
                        Some(_) => end += 1,
                        None => return Err(unsupported()),
                    }
                }
                &quoted[end + 1..]
            }
            None => {
                let end = rest
                    .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
                    .unwrap_or(rest.len());
                if end == 0 {
                    return Err(unsupported());
                }
                &rest[end..]
            }
        };
        rest = rest.trim_start();
        match rest.strip_prefix('.') {
            Some(after_dot) => rest = after_dot,
            None => break,
        }
    }
    rest.strip_prefix(":=")
        .or_else(|| rest.strip_prefix('='))
        .ok_or_else(unsupported)
}

fn translate_expression(expression: &str) -> Result<ast::Expr> {
    PostgreSQLTranslator::new()
        .translate_expression(&parse_expression(expression)?)
        .map_err(|e| LimboError::ParseError(e.to_string()))
}

fn parse_expression(expression: &str) -> Result<pg_query::protobuf::Node> {
    let parsed = turso_pg_parser::parse(&format!("SELECT {expression}"))
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    let invalid = || LimboError::ParseError(format!("expected one expression: {expression}"));
    let [statement] = parsed.protobuf.stmts.as_slice() else {
        return Err(invalid());
    };
    let Some(Node::SelectStmt(select)) = statement
        .stmt
        .as_ref()
        .and_then(|statement| statement.node.as_ref())
    else {
        return Err(invalid());
    };
    let [target] = select.target_list.as_slice() else {
        return Err(invalid());
    };
    match &target.node {
        Some(Node::ResTarget(target)) => target.val.as_deref().cloned().ok_or_else(invalid),
        _ => Err(invalid()),
    }
}

/// Functions that give the same value each time in one statement.
const FUNCTIONS_WITH_ONE_VALUE: &[&str] = &[
    "now",
    "transaction_timestamp",
    "statement_timestamp",
    "coalesce",
    "nullif",
    "lower",
    "upper",
    "trim",
    "btrim",
    "ltrim",
    "rtrim",
    "length",
    "abs",
    "round",
    "concat",
    "concat_ws",
    "replace",
    "substr",
    "substring",
    "greatest",
    "least",
];

/// Whether the value can be calculated again in each place that reads it:
/// it reads no table, and it calls only functions that give the same value
/// each time.
fn can_be_calculated_again(expr: &ast::Expr) -> bool {
    !contains(expr, |expr| match expr {
        ast::Expr::Subquery(_)
        | ast::Expr::Exists(_)
        | ast::Expr::InSelect { .. }
        | ast::Expr::FunctionCallStar { .. }
        | ast::Expr::Raise(..) => true,
        ast::Expr::FunctionCall { name, .. } => !FUNCTIONS_WITH_ONE_VALUE
            .iter()
            .any(|function| name.as_str().eq_ignore_ascii_case(function)),
        _ => false,
    })
}

fn contains(expr: &ast::Expr, matches: impl Fn(&ast::Expr) -> bool) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |expr| -> Result<WalkControl> {
        if matches(expr) {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found
}

enum Constant {
    Null,
    Integer(i64),
    Text(String),
}

impl Constant {
    /// Whether a branch with this condition runs. NULL is false, as in
    /// PL/pgSQL.
    fn is_true(&self) -> bool {
        matches!(self, Constant::Integer(value) if *value != 0)
    }
}

/// The value of an expression that has only constants, as after the
/// replacement of `TG_OP` and the other trigger values.
fn constant_value(expr: &ast::Expr) -> Option<Constant> {
    match expr {
        ast::Expr::Parenthesized(exprs) => match exprs.as_slice() {
            [expr] => constant_value(expr),
            _ => None,
        },
        ast::Expr::Literal(ast::Literal::Null) => Some(Constant::Null),
        ast::Expr::Literal(ast::Literal::True) => Some(Constant::Integer(1)),
        ast::Expr::Literal(ast::Literal::False) => Some(Constant::Integer(0)),
        ast::Expr::Literal(ast::Literal::Numeric(value)) => {
            value.parse().ok().map(Constant::Integer)
        }
        ast::Expr::Literal(ast::Literal::String(value)) => {
            Some(Constant::Text(value[1..value.len() - 1].replace("''", "'")))
        }
        ast::Expr::Unary(ast::UnaryOperator::Not, expr) => match constant_value(expr)? {
            Constant::Integer(value) => Some(Constant::Integer(i64::from(value == 0))),
            Constant::Null => Some(Constant::Null),
            Constant::Text(_) => None,
        },
        ast::Expr::Binary(left, operator, right) => {
            let (left, right) = (constant_value(left)?, constant_value(right)?);
            match operator {
                ast::Operator::And => match (left, right) {
                    (Constant::Integer(0), _) | (_, Constant::Integer(0)) => {
                        Some(Constant::Integer(0))
                    }
                    (Constant::Integer(_), Constant::Integer(_)) => Some(Constant::Integer(1)),
                    (Constant::Text(_), _) | (_, Constant::Text(_)) => None,
                    _ => Some(Constant::Null),
                },
                ast::Operator::Or => match (left, right) {
                    (Constant::Integer(value), _) | (_, Constant::Integer(value)) if value != 0 => {
                        Some(Constant::Integer(1))
                    }
                    (Constant::Integer(_), Constant::Integer(_)) => Some(Constant::Integer(0)),
                    (Constant::Text(_), _) | (_, Constant::Text(_)) => None,
                    _ => Some(Constant::Null),
                },
                ast::Operator::Equals | ast::Operator::NotEquals => {
                    let equal = match (left, right) {
                        (Constant::Null, _) | (_, Constant::Null) => return Some(Constant::Null),
                        (Constant::Integer(left), Constant::Integer(right)) => left == right,
                        (Constant::Text(left), Constant::Text(right)) => left == right,
                        _ => return None,
                    };
                    let equal = equal == matches!(operator, ast::Operator::Equals);
                    Some(Constant::Integer(i64::from(equal)))
                }
                _ => None,
            }
        }
        ast::Expr::InList { lhs, not, rhs } => {
            let left = constant_value(lhs)?;
            let mut found = false;
            for item in rhs {
                found |= match (&left, constant_value(item)?) {
                    (Constant::Integer(left), Constant::Integer(right)) => *left == right,
                    (Constant::Text(left), Constant::Text(right)) => *left == right,
                    _ => return None,
                };
            }
            Some(Constant::Integer(i64::from(found != *not)))
        }
        _ => None,
    }
}

/// Make a statement of a branch run only when `condition` is true.
fn with_condition(stmt: ast::Stmt, condition: Option<ast::Expr>) -> Result<ast::Stmt> {
    let Some(condition) = condition else {
        return Ok(stmt);
    };
    Ok(match stmt {
        ast::Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        } => {
            let body = match body {
                ast::InsertBody::Select(select, upsert) => {
                    ast::InsertBody::Select(inserted_rows_with_condition(select, condition), upsert)
                }
                ast::InsertBody::DefaultValues => {
                    return Err(LimboError::ParseError(
                        "INSERT ... DEFAULT VALUES in a branch is not supported yet".into(),
                    ))
                }
            };
            ast::Stmt::Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            }
        }
        ast::Stmt::Update(mut update) => {
            update.where_clause = Some(Box::new(and(
                update.where_clause.map(|expr| *expr),
                condition,
            )));
            ast::Stmt::Update(update)
        }
        ast::Stmt::Delete {
            with,
            tbl_name,
            indexed,
            where_clause,
            returning,
        } => ast::Stmt::Delete {
            with,
            tbl_name,
            indexed,
            where_clause: Some(Box::new(and(where_clause.map(|expr| *expr), condition))),
            returning,
        },
        _ => {
            return Err(LimboError::ParseError(
                "this SQL statement in a branch is not supported yet".into(),
            ))
        }
    })
}

/// The rows of `INSERT ... <select>` when `condition` is true, and no row
/// otherwise. One row of VALUES becomes `SELECT values WHERE condition`, so
/// that the values are calculated only when the condition is true.
fn inserted_rows_with_condition(select: ast::Select, condition: ast::Expr) -> ast::Select {
    if select.with.is_none()
        && select.body.compounds.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
    {
        if let ast::OneSelect::Values(rows) = &select.body.select {
            if let [row] = rows.as_slice() {
                return one_select(
                    row.iter()
                        .map(|value| ast::ResultColumn::Expr(value.clone(), None))
                        .collect(),
                    None,
                    Some(condition),
                );
            }
        }
    }
    one_select(
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

/// A query of PERFORM that runs only when `condition` is true.
fn select_with_condition(select: ast::Select, condition: Option<ast::Expr>) -> Result<ast::Select> {
    let Some(condition) = condition else {
        return Ok(select);
    };
    let mut select = select;
    if !select.body.compounds.is_empty() {
        return Err(LimboError::ParseError(
            "PERFORM of a compound query in a branch is not supported yet".into(),
        ));
    }
    match &mut select.body.select {
        ast::OneSelect::Select { where_clause, .. } => {
            *where_clause = Some(Box::new(and(
                where_clause.take().map(|expr| *expr),
                condition,
            )));
            Ok(select)
        }
        ast::OneSelect::Values(_) => Ok(inserted_rows_with_condition(select, condition)),
    }
}

fn one_select(
    columns: Vec<ast::ResultColumn>,
    from: Option<ast::FromClause>,
    where_clause: Option<ast::Expr>,
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

fn select_statement(expr: ast::Expr) -> ast::Stmt {
    select_where(expr, None)
}

fn select_where(expr: ast::Expr, condition: Option<ast::Expr>) -> ast::Stmt {
    ast::Stmt::Select(one_select(
        vec![ast::ResultColumn::Expr(Box::new(expr), None)],
        None,
        condition,
    ))
}

fn raise_ignore() -> ast::Stmt {
    select_statement(ast::Expr::Raise(ast::ResolveType::Ignore, None))
}

fn raise_abort_expr(message: ast::Expr) -> ast::Expr {
    ast::Expr::Raise(ast::ResolveType::Abort, Some(Box::new(message)))
}

fn set_variable(key: &str, value: ast::Expr) -> ast::Expr {
    function_call(SET_VARIABLE, vec![text_literal(key), value])
}

fn get_variable(key: &str) -> ast::Expr {
    function_call(GET_VARIABLE, vec![text_literal(key)])
}

fn function_call(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::FunctionCall {
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

fn coalesce_false(condition: ast::Expr) -> ast::Expr {
    function_call(
        "coalesce",
        vec![condition, ast::Expr::Literal(ast::Literal::False)],
    )
}

fn case_when(condition: ast::Expr, then: ast::Expr, otherwise: ast::Expr) -> ast::Expr {
    ast::Expr::Case {
        base: None,
        when_then_pairs: vec![(Box::new(condition), Box::new(then))],
        else_expr: Some(Box::new(otherwise)),
    }
}

fn cast_to_text(value: ast::Expr) -> ast::Expr {
    ast::Expr::Cast {
        expr: Box::new(value),
        type_name: Some(ast::Type {
            name: "TEXT".to_string(),
            size: None,
            array_dimensions: 0,
        }),
    }
}

fn and(left: Option<ast::Expr>, right: ast::Expr) -> ast::Expr {
    match left {
        Some(left) => ast::Expr::Binary(Box::new(left), ast::Operator::And, Box::new(right)),
        None => right,
    }
}

fn and_all(first: Option<ast::Expr>, rest: impl Iterator<Item = ast::Expr>) -> Option<ast::Expr> {
    rest.fold(first, |all, next| Some(and(all, next)))
}

fn not(expr: &ast::Expr) -> ast::Expr {
    ast::Expr::Unary(ast::UnaryOperator::Not, Box::new(expr.clone()))
}

fn null() -> ast::Expr {
    ast::Expr::Literal(ast::Literal::Null)
}

fn record_field(record: &str, field: &str) -> ast::Expr {
    ast::Expr::Qualified(
        ast::Name::exact(record.to_string()),
        ast::Name::exact(field.to_string()),
    )
}

fn text_literal(text: &str) -> ast::Expr {
    ast::Expr::Literal(ast::Literal::String(format!(
        "'{}'",
        text.replace('\'', "''")
    )))
}
