use crate::ast_walk::{walk_stmt_exprs_mut, walk_tree_mut};
use crate::trigger::{parse_trigger_definition, postgres_schema_name, trigger_definition};
use crate::user_function::{Language, UserFunction};
use pg_query::protobuf::node::Node;
use serde_json::Value as Json;
use turso_core::dialect::{FunctionCatalog, TriggerBody};
use turso_core::schema::{BTreeTable, Trigger};
use turso_core::{LimboError, Result, WalkControl};
use turso_parser::ast;
use turso_pg_parser::translator::{call_name, PostgreSQLTranslator};

pub(crate) fn compile_trigger(
    trigger: &Trigger,
    _table: &BTreeTable,
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
    let Some(action) = parsed
        .get(0)
        .and_then(|function| function.get("PLpgSQL_function"))
        .and_then(|function| function.get("action"))
    else {
        return Err(LimboError::Corrupt(format!(
            "malformed PL/pgSQL function {}",
            function.name
        )));
    };
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
        statements: Vec::new(),
    };
    if compiler.trigger.when == "BEFORE" {
        return Err(LimboError::ParseError(format!(
            "cannot execute function {}(): BEFORE triggers are not supported yet",
            function.name
        )));
    }
    compiler.compile_statement(action)?;
    Ok(Some(TriggerBody {
        statements: compiler.statements,
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

struct Compiler {
    function_name: String,
    trigger: TriggerConstants,
    statements: Vec<ast::Stmt>,
}

#[derive(PartialEq, Eq)]
enum Flow {
    Continues,
    Returns,
}

impl Compiler {
    fn compile_statement(&mut self, statement: &Json) -> Result<Flow> {
        let (kind, body) = statement
            .as_object()
            .and_then(|object| object.iter().next())
            .ok_or_else(|| LimboError::Corrupt("malformed PL/pgSQL statement".into()))?;
        let line = body.get("lineno").and_then(Json::as_u64).unwrap_or(0);
        let result = match kind.as_str() {
            "PLpgSQL_stmt_block" => self.compile_block(body),
            "PLpgSQL_stmt_execsql" => self.compile_sql_statement(body).map(|()| Flow::Continues),
            "PLpgSQL_stmt_return" => Ok(Flow::Returns),
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
        for statement in json_array(block, "body") {
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
        self.statements.push(stmt);
        Ok(())
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
            ast::Expr::Qualified(record, _) => {
                let record = record.as_str().to_ascii_lowercase();
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

fn text_literal(text: &str) -> ast::Expr {
    ast::Expr::Literal(ast::Literal::String(format!(
        "'{}'",
        text.replace('\'', "''")
    )))
}
