use crate::plpgsql_names::{NameResolver, Variables};
use crate::session::{set_trigger_variable, trigger_variable};
use crate::trigger::{parse_trigger_definition, postgres_schema_name, trigger_definition};
use crate::user_function::{Language, UserFunction};
use pg_query::protobuf::node::Node;
use serde_json::Value as Json;
use std::collections::HashMap;
use std::sync::Arc;
use turso_core::dialect::{ChangedColumn, FunctionCatalog, TableCatalog, TriggerBody};
use turso_core::native_ext::{FunctionArity, ScalarCall, ScalarFunction};
use turso_core::schema::{BTreeTable, Trigger};
use turso_core::types::IOResultOr;
use turso_core::{
    walk_expr, walk_expr_mut, Connection, ExternalFunc, IOResult, LimboError, OpenOptions,
    Register, Result, Value, WalkControl,
};
use turso_parser::ast;
use turso_pg_parser::translator::{call_name, PostgreSQLTranslator};

pub(crate) fn compile_trigger(
    trigger: &Trigger,
    table: &BTreeTable,
    functions: &dyn FunctionCatalog,
    tables: &dyn TableCatalog,
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
    let datums = Datums::new(plpgsql_function);
    let block_variables = block_variables(action, &datums)?;
    let found = datums.found();
    let uses_found = found.is_some()
        && user_function
            .body_text()
            .is_some_and(|body| mentions_found(&body));
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
        tables,
        datums,
        block_variables,
        scopes: vec![BlockScope {
            label: Some(function.name.clone()),
            variables: found.into_iter().collect(),
        }],
        uses_found,
        variable_count: 0,
        statements: Vec::new(),
        new_values: Vec::new(),
        branch_condition: None,
        return_variable: None,
        conditional_returns: Vec::new(),
        changed_columns: Vec::new(),
    };
    if let Some(found) = compiler.found_key() {
        compiler.statements.push(select_statement(set_variable(
            &found,
            ast::Expr::Literal(ast::Literal::False),
        )));
    }
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
    tables: &'a dyn TableCatalog,
    datums: Datums<'a>,
    /// The variables that each block declares, by the address of the block.
    block_variables: HashMap<usize, Vec<usize>>,
    /// The blocks that enclose the statement that compiles now, the
    /// innermost last. The first scope has FOUND.
    scopes: Vec<BlockScope>,
    /// Whether the function reads FOUND, so that the statements that change
    /// FOUND must set it.
    uses_found: bool,
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

/// The variables of a block that a name can read.
struct BlockScope {
    label: Option<String>,
    /// The datums of the variables, in the order of their declaration.
    variables: Vec<usize>,
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
    condition: Condition,
    statements: Vec<&'j Json>,
}

/// The condition of a branch, or the name that is both a column and a
/// variable in it.
enum Condition {
    Value(ast::Expr),
    Ambiguous(String),
}

/// What happens when no branch of IF or CASE runs.
enum NoBranch<'j> {
    Else(Vec<&'j Json>),
    CaseNotFound,
}

impl<'a> Compiler<'a> {
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
            "PLpgSQL_stmt_execsql" => self.compile_sql_statement(body),
            "PLpgSQL_stmt_assign" => self.compile_assignment(body),
            "PLpgSQL_stmt_if" => self.compile_if(body),
            "PLpgSQL_stmt_case" => self.compile_case(body),
            "PLpgSQL_stmt_raise" => self.compile_raise(body),
            "PLpgSQL_stmt_perform" => self.compile_perform(body),
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
        let variables = self
            .block_variables
            .get(&block_address(block))
            .cloned()
            .unwrap_or_default();
        self.scopes.push(BlockScope {
            label: block
                .get("label")
                .and_then(Json::as_str)
                .map(str::to_string),
            variables: Vec::new(),
        });
        let mut flow = Flow::Continues;
        for dno in variables {
            flow = self.declare_variable(dno)?;
            if flow == Flow::Returns {
                break;
            }
            self.scopes
                .last_mut()
                .expect("the scope of the block is on the stack")
                .variables
                .push(dno);
        }
        if flow == Flow::Continues {
            flow = self.compile_statements(json_array(block, "body"))?;
        }
        self.scopes.pop();
        Ok(flow)
    }

    /// Set a variable of a block to its default value, as PL/pgSQL does each
    /// time that the block starts.
    fn declare_variable(&mut self, dno: usize) -> Result<Flow> {
        let declaration = self.datums.declaration(dno)?;
        let value = match declaration.default {
            Some(default) => match self.compile_expression(default)? {
                Some(value) => cast(value, declaration.cast),
                None => return Ok(self.flow_after_error()),
            },
            None => null(),
        };
        let key = self.variable_key(dno);
        let condition = self.statement_condition();
        self.statements
            .push(select_where(set_variable(&key, value), condition.clone()));
        if declaration.not_null {
            self.check_not_null(&key, declaration.name, condition);
        }
        Ok(Flow::Continues)
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

    fn compile_sql_statement(&mut self, body: &Json) -> Result<Flow> {
        let query = expression_query(body.get("sqlstmt"))?;
        let stmt = self.translate_statement(query)?;
        let into = body.get("into").and_then(Json::as_bool) == Some(true);
        if into && body.get("strict").and_then(Json::as_bool) == Some(true) {
            return Err(LimboError::ParseError(
                "INTO STRICT is not supported yet".into(),
            ));
        }
        match (stmt, into) {
            (ast::Stmt::Select(_), false) => Err(LimboError::ParseError(
                "query has no destination for result data".into(),
            )),
            (ast::Stmt::Select(select), true) => {
                let targets = self.targets_of_into(body.get("target"))?;
                self.compile_select_into(select, targets)
            }
            (_, true) => Err(LimboError::ParseError(
                "RETURNING INTO is not supported yet".into(),
            )),
            (stmt, false) => self.compile_write(stmt),
        }
    }

    /// Compile INSERT, UPDATE or DELETE.
    fn compile_write(&mut self, mut stmt: ast::Stmt) -> Result<Flow> {
        if let Some(name) = self.resolve_statement(&mut stmt)? {
            return Ok(self.raise_ambiguous(&name));
        }
        let stmt = with_condition(stmt, self.statement_condition())?;
        self.statements.push(stmt);
        self.set_found_from_changes();
        Ok(Flow::Continues)
    }

    /// The variables and fields of NEW that INTO assigns, in order.
    fn targets_of_into(&self, target: Option<&Json>) -> Result<Vec<Target<'a>>> {
        let malformed = || LimboError::Corrupt("malformed PL/pgSQL INTO target".into());
        let target = target.ok_or_else(malformed)?;
        if let Some(row) = target.get("PLpgSQL_row") {
            return json_array(row, "fields")
                .map(|field| {
                    let varno = field
                        .get("varno")
                        .and_then(Json::as_u64)
                        .ok_or_else(malformed)?;
                    Ok(self.datums.assignment_target(varno))
                })
                .collect();
        }
        let record = target.get("PLpgSQL_rec").ok_or_else(malformed)?;
        if record.get("dno").and_then(Json::as_u64) != self.datums.new_varno {
            let name = record.get("refname").and_then(Json::as_str).unwrap_or("");
            return Err(LimboError::ParseError(format!(
                "INTO the record variable \"{name}\" is not supported yet"
            )));
        }
        Ok(self
            .table
            .columns()
            .iter()
            .filter(|column| !column.hidden())
            .filter_map(|column| column.name.as_deref())
            .map(Target::NewField)
            .collect())
    }

    /// Compile `SELECT ... INTO targets` as one statement that assigns all
    /// targets from the first row of the query, or NULL to each target when
    /// the query gives no row:
    ///
    /// ```sql
    /// WITH __into(__c1, ..., __found) AS
    ///     (SELECT *, 1 FROM (<query>) WHERE <condition> LIMIT 1)
    /// SELECT __pl_set('<target>', __into.__c1), ..., __pl_set('<found>', __into.__found IS NOT NULL)
    /// FROM (SELECT 1) LEFT JOIN __into ON 1 WHERE <condition>
    /// ```
    fn compile_select_into(
        &mut self,
        mut select: ast::Select,
        targets: Vec<Target>,
    ) -> Result<Flow> {
        let (ambiguous, count) = self.resolve_query(&mut select)?;
        if let Some(name) = ambiguous {
            return Ok(self.raise_ambiguous(&name));
        }
        let count = count.ok_or_else(|| {
            LimboError::ParseError(
                "SELECT INTO from a query whose columns are not known is not supported yet".into(),
            )
        })?;
        let condition = self.statement_condition();
        let mut assignments = Vec::new();
        let mut not_null = Vec::new();
        for (position, target) in targets.into_iter().enumerate() {
            let value = if position < count {
                record_field(INTO_TABLE, &format!("__c{}", position + 1))
            } else {
                null()
            };
            match target {
                Target::Variable(dno) => {
                    let declaration = self.datums.declaration(dno)?;
                    let key = self.variable_key(dno);
                    assignments.push(set_variable(&key, cast(value, declaration.cast)));
                    if declaration.not_null {
                        not_null.push((key, declaration.name));
                    }
                }
                Target::NewField(field) => {
                    if self.new_field_exists(field, condition.clone()) {
                        let key = self.new_field_variable(field)?;
                        assignments.push(set_variable(&key, value));
                    }
                }
                Target::OldField => {
                    return Err(LimboError::ParseError(
                        "assignment to OLD is not supported".into(),
                    ))
                }
                Target::Other => {
                    return Err(LimboError::ParseError(
                        "INTO this variable is not supported yet".into(),
                    ))
                }
            }
        }
        if let Some(found) = self.found_key() {
            let found_row = record_field(INTO_TABLE, "__found");
            assignments.push(set_variable(
                &found,
                ast::Expr::NotNull(Box::new(found_row)),
            ));
        }
        if assignments.is_empty() {
            assignments.push(ast::Expr::Literal(ast::Literal::Numeric("1".into())));
        }
        self.statements.push(select_into_statement(
            select,
            count,
            assignments,
            condition.clone(),
        ));
        for (key, name) in not_null {
            self.check_not_null(&key, name, condition.clone());
        }
        Ok(Flow::Continues)
    }

    fn compile_perform(&mut self, body: &Json) -> Result<Flow> {
        let query = expression_query(body.get("expr"))?;
        let mut stmt = self.translate_statement(query)?;
        if let Some(name) = self.resolve_statement(&mut stmt)? {
            return Ok(self.raise_ambiguous(&name));
        }
        let ast::Stmt::Select(mut select) = stmt else {
            return Err(LimboError::Corrupt("PERFORM without a query".into()));
        };
        if let Some(found) = self.found_key() {
            self.statements.push(select_where(
                set_variable(&found, ast::Expr::Literal(ast::Literal::False)),
                self.statement_condition(),
            ));
            add_result_column(
                &mut select,
                set_variable(&found, ast::Expr::Literal(ast::Literal::True)),
            );
        }
        let select = select_with_condition(select, self.statement_condition())?;
        self.statements.push(ast::Stmt::Select(select));
        Ok(Flow::Continues)
    }

    fn compile_assignment(&mut self, body: &Json) -> Result<Flow> {
        let varno = body
            .get("varno")
            .and_then(Json::as_u64)
            .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL assignment".into()))?;
        let query = expression_query(body.get("expr"))?;
        let Some(value) = self.compile_expression(assigned_value(query)?)? else {
            return Ok(self.flow_after_error());
        };
        match self.datums.assignment_target(varno) {
            Target::NewField(field) => self.assign_new_field(field, value)?,
            Target::Variable(dno) => self.assign_variable(dno, value)?,
            Target::OldField => {
                return Err(LimboError::ParseError(
                    "assignment to OLD is not supported".into(),
                ))
            }
            Target::Other => {
                return Err(LimboError::ParseError(
                    "assignment to this variable is not supported yet".into(),
                ))
            }
        }
        Ok(Flow::Continues)
    }

    fn assign_variable(&mut self, dno: usize, value: ast::Expr) -> Result<()> {
        let declaration = self.datums.declaration(dno)?;
        let key = self.variable_key(dno);
        let condition = self.statement_condition();
        self.statements.push(select_where(
            set_variable(&key, cast(value, declaration.cast)),
            condition.clone(),
        ));
        if declaration.not_null {
            self.check_not_null(&key, declaration.name, condition);
        }
        Ok(())
    }

    fn check_not_null(&mut self, key: &str, name: &str, condition: Option<ast::Expr>) {
        let is_null = ast::Expr::IsNull(Box::new(get_variable(key)));
        self.statements.push(select_where(
            raise_abort_expr(text_literal(&format!(
                "null value cannot be assigned to variable \"{name}\" declared NOT NULL"
            ))),
            Some(and(condition, is_null)),
        ));
    }

    fn assign_new_field(&mut self, field: &str, value: ast::Expr) -> Result<()> {
        let condition = self.statement_condition();
        if !self.new_field_exists(field, condition.clone()) {
            return Ok(());
        }
        let position = self.new_value_position(field);
        let has_variable = position
            .is_some_and(|position| matches!(self.new_values[position].1, NewValue::Variable(_)));
        if condition.is_none() && !has_variable && can_be_calculated_again(&value) {
            match position {
                Some(position) => self.new_values[position].1 = NewValue::Expression(value),
                None => self
                    .new_values
                    .push((field.to_string(), NewValue::Expression(value))),
            }
            return Ok(());
        }
        let key = self.new_field_variable(field)?;
        self.statements
            .push(select_where(set_variable(&key, value), condition));
        Ok(())
    }

    /// Whether an assignment to `NEW.<field>` changes the row. A field that
    /// is not a column gives the error of PostgreSQL when the assignment
    /// runs, and a generated column ignores the assignment.
    fn new_field_exists(&mut self, field: &str, condition: Option<ast::Expr>) -> bool {
        match self.table.get_column(field) {
            None => {
                self.statements.push(select_where(
                    raise_abort_expr(text_literal(&format!(
                        "record \"new\" has no field \"{field}\""
                    ))),
                    condition,
                ));
                false
            }
            Some((_, column)) => !column.is_generated(),
        }
    }

    /// The variable that has the value of `NEW.<field>` from here on. A new
    /// variable starts with the current value of the field.
    fn new_field_variable(&mut self, field: &str) -> Result<String> {
        let position = self.new_value_position(field);
        if let Some(NewValue::Variable(key)) = position.map(|position| &self.new_values[position].1)
        {
            return Ok(key.clone());
        }
        let key = self.new_variable(&format!("new.{field}"));
        let current = match position {
            Some(position) => self.read_new_value(position),
            None => {
                let mut current = record_field("new", field);
                self.replace_trigger_value(&mut current)?;
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
        Ok(key)
    }

    fn new_value_position(&self, field: &str) -> Option<usize> {
        self.new_values
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case(field))
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
                let Some(value) = self.compile_expression(expression_query(Some(value))?)? else {
                    return Ok(self.flow_after_error());
                };
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
            let condition = match branch.condition {
                Condition::Value(condition) => condition,
                Condition::Ambiguous(name) => {
                    let reached_when = and_all(outer.clone(), earlier.iter().map(not));
                    let always = reached_when.is_none();
                    self.statements
                        .push(raise_ambiguous_statement(&name, reached_when));
                    if always {
                        return Ok(Flow::Returns);
                    }
                    continue;
                }
            };
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
    ) -> Result<Condition> {
        let mut condition = translate_expression(expression_query(condition)?)?;
        if let Some((name, value)) = case_value {
            walk_expr_mut(&mut condition, &mut |expr| {
                if matches!(expr, ast::Expr::Id(id) if id.as_str() == name) {
                    *expr = value.clone();
                    return Ok(WalkControl::SkipChildren);
                }
                Ok(WalkControl::Continue)
            })?;
        }
        Ok(match self.resolve_expression(&mut condition)? {
            None => Condition::Value(condition),
            Some(name) => Condition::Ambiguous(name),
        })
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
            Some(format) => match self.raise_message(format, json_array(body, "params"))? {
                Some(message) => Some(message),
                None => return Ok(self.flow_after_error()),
            },
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
            let Some(value) = self.compile_expression(expression_query(option.get("expr"))?)?
            else {
                return Ok(self.flow_after_error());
            };
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
    /// parameter, and `%%` is one `%`. `None` when a name in a parameter is
    /// ambiguous.
    fn raise_message<'j>(
        &mut self,
        format: &str,
        params: impl Iterator<Item = &'j Json>,
    ) -> Result<Option<ast::Expr>> {
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
            let Some(value) = self.compile_expression(expression_query(Some(param))?)? else {
                return Ok(None);
            };
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
        Ok(Some(
            parts
                .into_iter()
                .reduce(|left, right| {
                    ast::Expr::Binary(Box::new(left), ast::Operator::Concat, Box::new(right))
                })
                .expect("the message has at least one part"),
        ))
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

    /// Translate a PL/pgSQL expression and replace the names of variables in
    /// it. When a name is both a column and a variable, the compiler adds a
    /// statement that gives the error, and the result is `None`.
    fn compile_expression(&mut self, query: &str) -> Result<Option<ast::Expr>> {
        let mut expr = translate_expression(query)?;
        match self.resolve_expression(&mut expr)? {
            None => Ok(Some(expr)),
            Some(name) => {
                self.raise_ambiguous(&name);
                Ok(None)
            }
        }
    }

    /// Replace the names of variables in `expr`, and give the first name that
    /// is both a column and a variable.
    fn resolve_expression(&self, expr: &mut ast::Expr) -> Result<Option<String>> {
        let mut resolver = NameResolver::new(self, self.tables);
        resolver.expression(expr)?;
        Ok(resolver.ambiguous)
    }

    fn resolve_statement(&self, stmt: &mut ast::Stmt) -> Result<Option<String>> {
        let mut resolver = NameResolver::new(self, self.tables);
        resolver.statement(stmt)?;
        Ok(resolver.ambiguous)
    }

    /// Replace the names of variables in a query, and give the first name
    /// that is both a column and a variable, and the number of columns.
    fn resolve_query(&self, select: &mut ast::Select) -> Result<(Option<String>, Option<usize>)> {
        let mut resolver = NameResolver::new(self, self.tables);
        let count = resolver.query(select)?;
        Ok((resolver.ambiguous, count))
    }

    /// Set FOUND after a statement that changes rows: FOUND is true when the
    /// statement changed a row.
    fn set_found_from_changes(&mut self) {
        let Some(found) = self.found_key() else {
            return;
        };
        let changed = ast::Expr::Binary(
            Box::new(function_call("changes", vec![])),
            ast::Operator::Greater,
            Box::new(ast::Expr::Literal(ast::Literal::Numeric("0".into()))),
        );
        self.statements.push(select_where(
            set_variable(&found, changed),
            self.statement_condition(),
        ));
    }

    /// The key of FOUND, or `None` when the function does not read FOUND.
    fn found_key(&self) -> Option<String> {
        let found = self.datums.found().filter(|_| self.uses_found)?;
        Some(self.variable_key(found))
    }

    /// Add the statement that gives the error of PostgreSQL for a name that
    /// is both a column and a variable. As in PostgreSQL, the error comes
    /// only when the statement runs.
    fn raise_ambiguous(&mut self, name: &str) -> Flow {
        self.statements
            .push(raise_ambiguous_statement(name, self.statement_condition()));
        self.flow_after_error()
    }

    /// The flow after a statement that always gives an error when it runs.
    fn flow_after_error(&self) -> Flow {
        match self.statement_condition() {
            None => Flow::Returns,
            Some(_) => Flow::Continues,
        }
    }

    fn variable_key(&self, dno: usize) -> String {
        format!("{}.variable.{dno}", self.variable_prefix)
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

impl Variables for Compiler<'_> {
    fn variable(&self, qualifier: Option<&str>, name: &str) -> Result<Option<ast::Expr>> {
        for scope in self.scopes.iter().rev() {
            if qualifier.is_some_and(|label| scope.label.as_deref() != Some(label)) {
                continue;
            }
            if let Some(dno) = scope
                .variables
                .iter()
                .rev()
                .copied()
                .find(|&dno| self.datums.name(dno) == Some(name))
            {
                return Ok(Some(get_variable(&self.variable_key(dno))));
            }
        }
        if qualifier.is_some_and(|label| label != self.function_name) {
            return Ok(None);
        }
        self.trigger_constant(name)
    }

    fn replace_trigger_value(&self, expr: &mut ast::Expr) -> Result<WalkControl> {
        match expr {
            ast::Expr::FunctionCall { name, args, .. }
                if name.as_str().eq_ignore_ascii_case("array_element")
                    && args.len() == 2
                    && matches!(args[0].as_ref(), ast::Expr::Id(array) if array.as_str().eq_ignore_ascii_case("tg_argv")) =>
            {
                let index = args[1].as_ref().clone();
                *expr = self.trigger_argument(index);
                Ok(WalkControl::Continue)
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
                *expr = null();
                Ok(WalkControl::SkipChildren)
            }
            _ => Ok(WalkControl::Continue),
        }
    }
}

impl Compiler<'_> {
    /// The value of `TG_OP` and the other trigger variables, which are
    /// constants for one trigger and one event.
    fn trigger_constant(&self, name: &str) -> Result<Option<ast::Expr>> {
        Ok(Some(match name.to_ascii_lowercase().as_str() {
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
            _ => return Ok(None),
        }))
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
}

/// The datums of a PL/pgSQL function: its variables, the NEW and OLD
/// records, and the record fields that its assignments name.
struct Datums<'a> {
    datums: &'a [Json],
    new_varno: Option<u64>,
    old_varno: Option<u64>,
}

enum Target<'a> {
    NewField(&'a str),
    OldField,
    Variable(usize),
    Other,
}

/// The declaration of a variable.
struct Declaration<'a> {
    name: &'a str,
    default: Option<&'a str>,
    not_null: bool,
    /// The type that an assigned value is converted to.
    cast: Option<ast::Type>,
}

impl<'a> Datums<'a> {
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
        let Some((dno, datum)) = usize::try_from(varno)
            .ok()
            .and_then(|dno| Some((dno, self.datums.get(dno)?)))
        else {
            return Target::Other;
        };
        if datum.get("PLpgSQL_var").is_some() {
            return Target::Variable(dno);
        }
        let Some(field) = datum.get("PLpgSQL_recfield") else {
            return Target::Other;
        };
        let parent = field.get("recparentno").and_then(Json::as_u64);
        match field.get("fieldname").and_then(Json::as_str) {
            Some(name) if parent.is_some() && parent == self.new_varno => Target::NewField(name),
            Some(_) if parent.is_some() && parent == self.old_varno => Target::OldField,
            _ => Target::Other,
        }
    }

    /// The datum of FOUND, which the function declares and no block does.
    fn found(&self) -> Option<usize> {
        self.datums.iter().position(|datum| {
            datum.get("PLpgSQL_var").is_some_and(|variable| {
                variable.get("refname").and_then(Json::as_str) == Some("found")
                    && variable.get("lineno").is_none()
            })
        })
    }

    /// The name of a variable or a record that a block declares.
    fn name(&self, dno: usize) -> Option<&'a str> {
        let datum = self.datums.get(dno)?;
        datum
            .get("PLpgSQL_var")
            .or_else(|| datum.get("PLpgSQL_rec"))?
            .get("refname")
            .and_then(Json::as_str)
    }

    fn declaration(&self, dno: usize) -> Result<Declaration<'a>> {
        let datum = &self.datums[dno];
        if let Some(record) = datum.get("PLpgSQL_rec") {
            let name = record.get("refname").and_then(Json::as_str).unwrap_or("");
            return Err(LimboError::ParseError(format!(
                "the record variable \"{name}\" is not supported yet"
            )));
        }
        let variable = datum
            .get("PLpgSQL_var")
            .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL variable".into()))?;
        let name = variable
            .get("refname")
            .and_then(Json::as_str)
            .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL variable".into()))?;
        if variable.get("cursor_explicit_expr").is_some() {
            return Err(LimboError::ParseError("cursors are not supported".into()));
        }
        let type_name = variable
            .get("datatype")
            .and_then(|datatype| datatype.get("PLpgSQL_type"))
            .and_then(|datatype| datatype.get("typname"))
            .and_then(Json::as_str)
            .map(str::trim)
            .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL variable".into()))?;
        if type_name.contains('%') {
            return Err(LimboError::ParseError(format!(
                "the type {type_name} of variable \"{name}\" is not supported yet"
            )));
        }
        Ok(Declaration {
            name,
            default: variable
                .get("default_val")
                .map(|default| expression_query(Some(default)))
                .transpose()?,
            not_null: variable.get("notnull").and_then(Json::as_bool) == Some(true),
            cast: cast_type(type_name)?,
        })
    }
}

/// The variables that each block declares, by the address of the block.
/// The JSON of a block does not list its variables, so a variable belongs to
/// the first block whose BEGIN is on the line of the variable or after it:
/// the declarations of a block come just before its BEGIN. The variable of
/// CASE and the variables of loops are not declarations.
fn block_variables(action: &Json, datums: &Datums) -> Result<HashMap<usize, Vec<usize>>> {
    let mut blocks = Vec::new();
    collect_blocks(std::iter::once(action), &mut blocks);
    let block_line = |block: &Json| block.get("lineno").and_then(Json::as_u64).unwrap_or(0);
    let mut variables: HashMap<usize, Vec<usize>> = HashMap::new();
    for (dno, datum) in datums.datums.iter().enumerate() {
        let Some(variable) = datum
            .get("PLpgSQL_var")
            .or_else(|| datum.get("PLpgSQL_rec"))
        else {
            continue;
        };
        let is_case_variable = variable
            .get("refname")
            .and_then(Json::as_str)
            .is_some_and(|name| name.starts_with("__Case__Variable_"));
        let Some(line) = variable.get("lineno").and_then(Json::as_u64) else {
            continue;
        };
        if is_case_variable {
            continue;
        }
        let Some(begin) = blocks
            .iter()
            .map(|block| block_line(block))
            .filter(|&begin| begin >= line)
            .min()
        else {
            continue;
        };
        let mut candidates = blocks.iter().filter(|block| block_line(block) == begin);
        let block = candidates.next().expect("a block starts on the line");
        if line == begin && candidates.next().is_some() {
            return Err(LimboError::ParseError(
                "variables of blocks that start on one line are not supported yet".into(),
            ));
        }
        variables.entry(block_address(block)).or_default().push(dno);
    }
    Ok(variables)
}

/// Add the blocks among `statements` and their nested statements to
/// `blocks`, in the order of the source.
fn collect_blocks<'j>(statements: impl IntoIterator<Item = &'j Json>, blocks: &mut Vec<&'j Json>) {
    for statement in statements {
        let Some((kind, body)) = statement
            .as_object()
            .and_then(|object| object.iter().next())
        else {
            continue;
        };
        if kind == "PLpgSQL_stmt_block" {
            blocks.push(body);
        }
        collect_blocks(json_array(body, "then_body"), blocks);
        for elsif in
            json_array(body, "elsif_list").filter_map(|elsif| elsif.get("PLpgSQL_if_elsif"))
        {
            collect_blocks(json_array(elsif, "stmts"), blocks);
        }
        collect_blocks(json_array(body, "else_body"), blocks);
        for when in
            json_array(body, "case_when_list").filter_map(|when| when.get("PLpgSQL_case_when"))
        {
            collect_blocks(json_array(when, "stmts"), blocks);
        }
        collect_blocks(json_array(body, "else_stmts"), blocks);
        collect_blocks(json_array(body, "body"), blocks);
        let exceptions = body
            .get("exceptions")
            .and_then(|exceptions| exceptions.get("PLpgSQL_exception_block"));
        for exception in exceptions
            .into_iter()
            .flat_map(|exceptions| json_array(exceptions, "exc_list"))
            .filter_map(|exception| exception.get("PLpgSQL_exception"))
        {
            collect_blocks(json_array(exception, "action"), blocks);
        }
    }
}

fn block_address(block: &Json) -> usize {
    block as *const Json as usize
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

/// Translate a PL/pgSQL expression, which PostgreSQL runs as the query
/// `SELECT <expression>`. When that query has FROM, WHERE or other clauses,
/// the expression is a subquery that gives NULL when the query gives no row.
fn translate_expression(expression: &str) -> Result<ast::Expr> {
    let query = parse_expression_query(expression)?;
    match single_expression(&query) {
        Some(value) => PostgreSQLTranslator::new()
            .translate_expression(&value)
            .map_err(|e| LimboError::ParseError(e.to_string())),
        None => one_row_value(expression),
    }
}

fn parse_expression(expression: &str) -> Result<pg_query::protobuf::Node> {
    single_expression(&parse_expression_query(expression)?)
        .ok_or_else(|| LimboError::ParseError(format!("expected one expression: {expression}")))
}

fn parse_expression_query(expression: &str) -> Result<pg_query::protobuf::SelectStmt> {
    let parsed = turso_pg_parser::parse(&format!("SELECT {expression}"))
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    let invalid = || LimboError::ParseError(format!("expected one expression: {expression}"));
    let [statement] = parsed.protobuf.stmts.as_slice() else {
        return Err(invalid());
    };
    match statement
        .stmt
        .as_ref()
        .and_then(|statement| statement.node.as_ref())
    {
        Some(Node::SelectStmt(select)) => Ok(select.as_ref().clone()),
        _ => Err(invalid()),
    }
}

/// The expression of a query that has only one expression and no clause.
fn single_expression(query: &pg_query::protobuf::SelectStmt) -> Option<pg_query::protobuf::Node> {
    let only_target = query.op() == pg_query::protobuf::SetOperation::SetopNone
        && query.from_clause.is_empty()
        && query.where_clause.is_none()
        && query.group_clause.is_empty()
        && query.having_clause.is_none()
        && query.window_clause.is_empty()
        && query.distinct_clause.is_empty()
        && query.sort_clause.is_empty()
        && query.limit_offset.is_none()
        && query.limit_count.is_none()
        && query.with_clause.is_none()
        && query.values_lists.is_empty()
        && query.into_clause.is_none()
        && query.locking_clause.is_empty();
    match query.target_list.as_slice() {
        [target] if only_target => match &target.node {
            Some(Node::ResTarget(target)) => target.val.as_deref().cloned(),
            _ => None,
        },
        _ => None,
    }
}

/// The value of the query `SELECT <expression>` with FROM or other clauses,
/// as PL/pgSQL gives it: NULL when the query gives no row, and an error when
/// it gives more than one row.
fn one_row_value(expression: &str) -> Result<ast::Expr> {
    let query = format!("SELECT {expression}");
    let parsed =
        turso_pg_parser::parse(&query).map_err(|e| LimboError::ParseError(e.to_string()))?;
    let statement = PostgreSQLTranslator::new()
        .translate(&parsed)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    let ast::Stmt::Select(mut select) = statement else {
        return Err(LimboError::ParseError(format!(
            "expected one expression: {expression}"
        )));
    };
    let ast::OneSelect::Select { columns, .. } = &mut select.body.select else {
        return Err(LimboError::ParseError(format!(
            "query \"{query}\" is not supported yet"
        )));
    };
    let [ast::ResultColumn::Expr(_, alias)] = columns.as_mut_slice() else {
        return Err(LimboError::ParseError(format!(
            "query \"{query}\" must return one column"
        )));
    };
    *alias = Some(ast::As::As(ast::Name::exact(VALUE_COLUMN.to_string())));
    let value = ast::Expr::Id(ast::Name::exact(VALUE_COLUMN.to_string()));
    let mut first_rows = one_select(
        vec![ast::ResultColumn::Expr(Box::new(value.clone()), None)],
        Some(from_subquery(select, "__query")),
        None,
    );
    first_rows.limit = Some(ast::Limit {
        expr: Box::new(ast::Expr::Literal(ast::Literal::Numeric("2".into()))),
        offset: None,
    });
    let row_count = ast::Expr::FunctionCallStar {
        name: ast::Name::exact("count".to_string()),
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    let checked_value = case_when(
        ast::Expr::Binary(
            Box::new(row_count),
            ast::Operator::Greater,
            Box::new(ast::Expr::Literal(ast::Literal::Numeric("1".into()))),
        ),
        raise_abort_expr(text_literal("query returned more than one row")),
        function_call("min", vec![value]),
    );
    Ok(ast::Expr::Subquery(Box::new(one_select(
        vec![ast::ResultColumn::Expr(Box::new(checked_value), None)],
        Some(from_subquery(first_rows, "__rows")),
        None,
    ))))
}

const VALUE_COLUMN: &str = "__value";

fn from_subquery(select: ast::Select, alias: &str) -> ast::FromClause {
    ast::FromClause {
        select: Box::new(ast::SelectTable::Select {
            select,
            alias: Some(ast::As::As(ast::Name::exact(alias.to_string()))),
            lateral: false,
        }),
        joins: vec![],
    }
}

/// Whether the body of a function names FOUND. A word in a comment or a
/// string also counts, which only adds statements that set FOUND.
fn mentions_found(body: &str) -> bool {
    body.split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .any(|word| word.eq_ignore_ascii_case("found"))
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

/// The name of the CTE that has the row of `SELECT INTO`.
const INTO_TABLE: &str = "__into";

fn select_into_statement(
    query: ast::Select,
    count: usize,
    assignments: Vec<ast::Expr>,
    condition: Option<ast::Expr>,
) -> ast::Stmt {
    let mut first_row = one_select(
        vec![
            ast::ResultColumn::Star,
            ast::ResultColumn::Expr(
                Box::new(ast::Expr::Literal(ast::Literal::Numeric("1".into()))),
                None,
            ),
        ],
        Some(ast::FromClause {
            select: Box::new(ast::SelectTable::Select {
                select: query,
                alias: Some(ast::As::As(ast::Name::exact("__query".to_string()))),
                lateral: false,
            }),
            joins: vec![],
        }),
        condition.clone(),
    );
    first_row.limit = Some(ast::Limit {
        expr: Box::new(ast::Expr::Literal(ast::Literal::Numeric("1".into()))),
        offset: None,
    });
    let columns = (1..=count)
        .map(|position| format!("__c{position}"))
        .chain(std::iter::once("__found".to_string()))
        .map(|name| ast::IndexedColumn {
            col_name: ast::Name::exact(name),
            collation_name: None,
            order: None,
        })
        .collect();
    let one_row = one_select(
        vec![ast::ResultColumn::Expr(
            Box::new(ast::Expr::Literal(ast::Literal::Numeric("1".into()))),
            None,
        )],
        None,
        None,
    );
    let mut select = one_select(
        assignments
            .into_iter()
            .map(|assignment| ast::ResultColumn::Expr(Box::new(assignment), None))
            .collect(),
        Some(ast::FromClause {
            select: Box::new(ast::SelectTable::Select {
                select: one_row,
                alias: Some(ast::As::As(ast::Name::exact("__one".to_string()))),
                lateral: false,
            }),
            joins: vec![ast::JoinedSelectTable {
                operator: ast::JoinOperator::TypedJoin(Some(
                    ast::JoinType::LEFT | ast::JoinType::OUTER,
                )),
                table: Box::new(ast::SelectTable::Table(
                    ast::QualifiedName::single(ast::Name::exact(INTO_TABLE.to_string())),
                    None,
                    None,
                )),
                constraint: Some(ast::JoinConstraint::On(Box::new(ast::Expr::Literal(
                    ast::Literal::Numeric("1".into()),
                )))),
            }],
        }),
        condition,
    );
    select.with = Some(ast::With {
        recursive: false,
        ctes: vec![ast::CommonTableExpr {
            tbl_name: ast::Name::exact(INTO_TABLE.to_string()),
            columns,
            materialized: ast::Materialized::Any,
            select: first_row,
        }],
    });
    ast::Stmt::Select(select)
}

/// Add a column to each query of a compound query.
fn add_result_column(select: &mut ast::Select, column: ast::Expr) {
    let queries = std::iter::once(&mut select.body.select).chain(
        select
            .body
            .compounds
            .iter_mut()
            .map(|compound| &mut compound.select),
    );
    for query in queries {
        match query {
            ast::OneSelect::Select { columns, .. } => {
                columns.push(ast::ResultColumn::Expr(Box::new(column.clone()), None))
            }
            ast::OneSelect::Values(rows) => {
                for row in rows {
                    row.push(Box::new(column.clone()));
                }
            }
        }
    }
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

fn raise_ambiguous_statement(name: &str, condition: Option<ast::Expr>) -> ast::Stmt {
    select_where(
        raise_abort_expr(text_literal(&format!(
            "column reference \"{name}\" is ambiguous"
        ))),
        condition,
    )
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

/// The type of a cast to the PostgreSQL type `type_name`, as the translator
/// converts it.
fn cast_type(type_name: &str) -> Result<Option<ast::Type>> {
    match translate_expression(&format!("CAST(NULL AS {type_name})"))? {
        ast::Expr::Cast { type_name, .. } => Ok(type_name),
        _ => Err(LimboError::ParseError(format!(
            "type {type_name} is not supported yet"
        ))),
    }
}

fn cast(value: ast::Expr, type_name: Option<ast::Type>) -> ast::Expr {
    match type_name {
        Some(type_name) => ast::Expr::Cast {
            expr: Box::new(value),
            type_name: Some(type_name),
        },
        None => value,
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
