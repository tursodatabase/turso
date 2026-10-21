use crate::ast_walk::{walk_stmt_exprs_mut, walk_tree_mut};
use crate::trigger::{parse_trigger_definition, postgres_schema_name, trigger_definition};
use crate::user_function::{Language, UserFunction};
use pg_query::protobuf::node::Node;
use serde_json::Value as Json;
use turso_core::dialect::{ChangedColumn, FunctionCatalog, TriggerBody};
use turso_core::schema::{BTreeTable, Trigger};
use turso_core::{walk_expr, LimboError, Result, WalkControl};
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
    let mut compiler = Compiler {
        function_name: function.name.clone(),
        trigger: TriggerConstants {
            op: match trigger.event {
                ast::TriggerEvent::Insert => "INSERT",
                ast::TriggerEvent::Update | ast::TriggerEvent::UpdateOf(_) => "UPDATE",
                ast::TriggerEvent::Delete => "DELETE",
            },
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
        statements: Vec::new(),
        new_values: Vec::new(),
        changed_columns: Vec::new(),
    };
    if compiler.compile_statement(action)? == Flow::Continues {
        compiler.statements.push(raise_abort(
            "control reached end of trigger procedure without RETURN",
        ));
    }
    Ok(Some(TriggerBody {
        statements: compiler.statements,
        changed_columns: compiler.changed_columns,
    }))
}

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
    trigger: TriggerConstants,
    table: &'a BTreeTable,
    records: Records<'a>,
    statements: Vec<ast::Stmt>,
    /// The values that the function assigned to fields of NEW, in the order
    /// of the first assignment. Each value reads NEW as the trigger got it.
    new_values: Vec<(String, ast::Expr)>,
    changed_columns: Vec<ChangedColumn>,
}

#[derive(PartialEq, Eq)]
enum Flow {
    Continues,
    Returns,
}

impl Compiler<'_> {
    fn compile_statement(&mut self, statement: &Json) -> Result<Flow> {
        let (kind, body) = statement
            .as_object()
            .and_then(|object| object.iter().next())
            .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL statement".into()))?;
        let line = body.get("lineno").and_then(Json::as_u64).unwrap_or(0);
        let result = match kind.as_str() {
            "PLpgSQL_stmt_block" => self.compile_block(body),
            "PLpgSQL_stmt_execsql" => self.compile_sql_statement(body).map(|()| Flow::Continues),
            "PLpgSQL_stmt_assign" => self.compile_assignment(body).map(|()| Flow::Continues),
            "PLpgSQL_stmt_if" => self.compile_if(body),
            "PLpgSQL_stmt_return" if body.get("lineno").is_none() => Ok(Flow::Continues),
            "PLpgSQL_stmt_return" => self.compile_return(body).map(|()| Flow::Returns),
            kind => Err(LimboError::ParseError(format!(
                "{} is not supported yet",
                statement_description(kind)
            ))),
        };
        result.map_err(|error| self.with_context(error, line, kind))
    }

    fn compile_block(&mut self, block: &Json) -> Result<Flow> {
        if block.get("exceptions").is_some() {
            return Err(LimboError::ParseError(
                "EXCEPTION blocks are not supported".into(),
            ));
        }
        self.compile_statements(block, "body")
    }

    fn compile_statements(&mut self, parent: &Json, key: &str) -> Result<Flow> {
        for statement in json_array(parent, key) {
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
        if self.new_values.iter().any(|(_, value)| reads_table(value)) {
            return Err(LimboError::ParseError(
                "a SQL statement after an assignment to NEW that reads a table is not supported yet"
                    .into(),
            ));
        }
        walk_stmt_exprs_mut(&mut stmt, &mut |expr| self.replace_trigger_values(expr))?;
        self.statements.push(stmt);
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
        match self.table.get_column(field) {
            None => {
                self.statements.push(raise_abort(&format!(
                    "record \"new\" has no field \"{field}\""
                )));
                return Ok(());
            }
            Some((_, column)) if column.is_generated() => return Ok(()),
            Some(_) => {}
        }
        match self
            .new_values
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case(field))
        {
            Some((_, old_value)) => *old_value = value,
            None => self.new_values.push((field.to_string(), value)),
        }
        Ok(())
    }

    fn compile_if(&mut self, body: &Json) -> Result<Flow> {
        let elsif = json_array(body, "elsif_list")
            .filter_map(|elsif| elsif.get("PLpgSQL_if_elsif"))
            .map(|elsif| (elsif.get("cond"), elsif, "stmts"));
        for (condition, parent, key) in
            std::iter::once((body.get("cond"), body, "then_body")).chain(elsif)
        {
            if self.constant_condition(condition)? {
                return self.compile_statements(parent, key);
            }
        }
        self.compile_statements(body, "else_body")
    }

    fn constant_condition(&self, condition: Option<&Json>) -> Result<bool> {
        let mut condition = translate_expression(expression_query(condition)?)?;
        walk_tree_mut(&mut condition, &mut |expr| {
            self.replace_trigger_values(expr)
        })?;
        match constant_value(&condition) {
            Some(Constant::Integer(value)) => Ok(value != 0),
            Some(Constant::Null) => Ok(false),
            _ => Err(LimboError::ParseError(
                "IF with a condition that is not constant is not supported yet".into(),
            )),
        }
    }

    fn compile_return(&mut self, body: &Json) -> Result<()> {
        if self.trigger.when != "BEFORE" {
            return Ok(());
        }
        let Some(value) = body.get("expr") else {
            return Err(LimboError::ParseError(
                "RETURN in a trigger function needs a value".into(),
            ));
        };
        match (
            self.returned_record(expression_query(Some(value))?)?,
            self.trigger.op,
        ) {
            (Record::New, "INSERT" | "UPDATE") => {
                if let Some((name, _)) = self.new_values.iter().find(|(name, _)| {
                    self.table
                        .get_column(name)
                        .is_some_and(|(_, column)| column.is_rowid_alias())
                }) {
                    return Err(LimboError::ParseError(format!(
                        "a BEFORE trigger cannot change the INTEGER PRIMARY KEY column \"{name}\" yet"
                    )));
                }
                self.changed_columns = std::mem::take(&mut self.new_values)
                    .into_iter()
                    .map(|(name, value)| ChangedColumn { name, value })
                    .collect();
            }
            (Record::Old, "UPDATE") => {
                self.changed_columns = self
                    .table
                    .columns()
                    .iter()
                    .filter(|column| !column.is_generated() && !column.is_rowid_alias())
                    .filter_map(|column| column.name.clone())
                    .map(|name| ChangedColumn {
                        value: ast::Expr::Qualified(
                            ast::Name::exact("old".to_string()),
                            ast::Name::exact(name.clone()),
                        ),
                        name,
                    })
                    .collect();
            }
            (Record::Old, "DELETE") => {}
            _ => self.statements.push(raise_ignore()),
        }
        Ok(())
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
                    if let Some(value) = self.assigned_value(field.as_str())? {
                        *expr = value;
                        return Ok(WalkControl::SkipChildren);
                    }
                }
                let missing = (record == "new" && self.trigger.op == "DELETE")
                    || (record == "old" && self.trigger.op == "INSERT");
                if !missing {
                    return Ok(WalkControl::Continue);
                }
                ast::Expr::Literal(ast::Literal::Null)
            }
            _ => return Ok(WalkControl::Continue),
        };
        *expr = replacement;
        Ok(WalkControl::SkipChildren)
    }

    /// The value of `NEW.<field>` after the assignments so far. The value is
    /// calculated again at each read, so it must give the same result each time.
    fn assigned_value(&self, field: &str) -> Result<Option<ast::Expr>> {
        let Some((_, value)) = self
            .new_values
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(field))
        else {
            return Ok(None);
        };
        if !gives_same_value_each_time(value) {
            return Err(LimboError::ParseError(format!(
                "a read of NEW.{field} after an assignment of a function result or a query result is not supported yet"
            )));
        }
        Ok(Some(value.clone()))
    }

    fn trigger_argument(&self, index: ast::Expr) -> ast::Expr {
        if let ast::Expr::Literal(ast::Literal::Numeric(index)) = &index {
            return match index
                .parse::<usize>()
                .ok()
                .and_then(|i| self.trigger.args.get(i))
            {
                Some(arg) => text_literal(arg),
                None => ast::Expr::Literal(ast::Literal::Null),
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

enum Record {
    New,
    Old,
    Null,
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

fn reads_table(expr: &ast::Expr) -> bool {
    contains(expr, |expr| {
        matches!(
            expr,
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. }
        )
    })
}

fn gives_same_value_each_time(expr: &ast::Expr) -> bool {
    !contains(expr, |expr| {
        matches!(
            expr,
            ast::Expr::Subquery(_)
                | ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::FunctionCall { .. }
                | ast::Expr::FunctionCallStar { .. }
                | ast::Expr::Raise(..)
        )
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
            Some(Constant::Text(value.trim_matches('\'').replace("''", "'")))
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

fn raise_abort(message: &str) -> ast::Stmt {
    select_statement(ast::Expr::Raise(
        ast::ResolveType::Abort,
        Some(Box::new(text_literal(message))),
    ))
}

fn raise_ignore() -> ast::Stmt {
    select_statement(ast::Expr::Raise(ast::ResolveType::Ignore, None))
}

fn select_statement(expr: ast::Expr) -> ast::Stmt {
    ast::Stmt::Select(ast::Select {
        with: None,
        body: ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: None,
                columns: vec![ast::ResultColumn::Expr(Box::new(expr), None)],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
        },
        order_by: vec![],
        limit: None,
    })
}

fn text_literal(text: &str) -> ast::Expr {
    ast::Expr::Literal(ast::Literal::String(format!(
        "'{}'",
        text.replace('\'', "''")
    )))
}
