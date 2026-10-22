//! The parts of a `CREATE TRIGGER` statement that the frontend reads.
//!
//! The engine stores the statement text as written, and reads it again
//! through this module when it loads the schema: one engine trigger for each
//! event of the statement.

use crate::translator::PostgreSQLTranslator;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self, CreateTrigStmt};
use turso_parser::ast;

/// Bits of `CreateTrigStmt::timing` and `CreateTrigStmt::events`, from
/// PostgreSQL's trigger.h.
const TRIGGER_TYPE_BEFORE: i32 = 1 << 1;
const TRIGGER_TYPE_INSERT: i32 = 1 << 2;
const TRIGGER_TYPE_DELETE: i32 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i32 = 1 << 4;
const TRIGGER_TYPE_TRUNCATE: i32 = 1 << 5;
const TRIGGER_TYPE_INSTEAD: i32 = 1 << 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTriggerTiming {
    Before,
    After,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgTriggerEvent {
    Insert,
    /// `UPDATE` or `UPDATE OF columns`.
    Update(Vec<String>),
    Delete,
}

/// A trigger that `CREATE TRIGGER` declares.
#[derive(Debug, Clone)]
pub struct PgTrigger {
    pub name: String,
    pub table: ast::QualifiedName,
    pub timing: PgTriggerTiming,
    /// The events in the order of the statement.
    pub events: Vec<PgTriggerEvent>,
    pub when: Option<ast::Expr>,
    pub function: ast::QualifiedName,
    pub args: Vec<String>,
}

impl PgTrigger {
    /// Parse the text of a `CREATE TRIGGER` statement.
    pub fn parse(sql: &str) -> Result<PgTrigger, ParseError> {
        let parse_result = crate::parse(sql)?;
        let node = parse_result
            .protobuf
            .nodes()
            .into_iter()
            .next()
            .ok_or_else(|| ParseError::ParseError("No statements found".to_string()))?;
        match node.0 {
            pg_query::NodeRef::CreateTrigStmt(stmt) => PgTrigger::from_stmt(stmt),
            _ => Err(ParseError::ParseError(
                "expected a CREATE TRIGGER statement".to_string(),
            )),
        }
    }

    pub fn from_stmt(stmt: &CreateTrigStmt) -> Result<PgTrigger, ParseError> {
        if stmt.replace {
            return Err(ParseError::ParseError(
                "CREATE OR REPLACE TRIGGER is not supported".to_string(),
            ));
        }
        if stmt.isconstraint {
            return Err(ParseError::ParseError(
                "constraint triggers are not supported".to_string(),
            ));
        }
        if !stmt.row {
            return Err(ParseError::ParseError(
                "statement-level triggers (FOR EACH STATEMENT) are not supported".to_string(),
            ));
        }
        if !stmt.transition_rels.is_empty() {
            return Err(ParseError::ParseError(
                "transition tables (REFERENCING) are not supported".to_string(),
            ));
        }
        if stmt.timing & TRIGGER_TYPE_INSTEAD != 0 {
            return Err(ParseError::ParseError(
                "INSTEAD OF triggers are not supported".to_string(),
            ));
        }
        if stmt.events & TRIGGER_TYPE_TRUNCATE != 0 {
            return Err(ParseError::ParseError(
                "TRUNCATE triggers are not supported".to_string(),
            ));
        }
        let timing = if stmt.timing & TRIGGER_TYPE_BEFORE != 0 {
            PgTriggerTiming::Before
        } else {
            PgTriggerTiming::After
        };
        let translator = PostgreSQLTranslator::new();
        let relation = stmt
            .relation
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("CREATE TRIGGER missing table name".into()))?;
        let table = translator.qualified_name_from_range_var(relation);

        let mut events = Vec::new();
        if stmt.events & TRIGGER_TYPE_INSERT != 0 {
            events.push(PgTriggerEvent::Insert);
        }
        if stmt.events & TRIGGER_TYPE_DELETE != 0 {
            events.push(PgTriggerEvent::Delete);
        }
        if stmt.events & TRIGGER_TYPE_UPDATE != 0 {
            events.push(PgTriggerEvent::Update(string_nodes(&stmt.columns)));
        }
        if events.is_empty() {
            return Err(ParseError::ParseError(
                "CREATE TRIGGER missing event".to_string(),
            ));
        }

        let when = stmt
            .when_clause
            .as_deref()
            .map(|when| translator.translate_expr(when))
            .transpose()?;

        let function_names = string_nodes(&stmt.funcname);
        let function = match function_names.as_slice() {
            [name] => ast::QualifiedName::single(ast::Name::exact(name.clone())),
            [schema, name] => ast::QualifiedName::fullname(
                ast::Name::exact(schema.clone()),
                ast::Name::exact(name.clone()),
            ),
            _ => {
                return Err(ParseError::ParseError(
                    "CREATE TRIGGER missing function name".to_string(),
                ))
            }
        };

        Ok(PgTrigger {
            name: stmt.trigname.clone(),
            table,
            timing,
            events,
            when,
            function,
            args: string_nodes(&stmt.args),
        })
    }

    /// One engine `CREATE TRIGGER` statement for each event. Each has the
    /// `EXECUTE FUNCTION` body: the engine asks the dialect for the commands
    /// when it compiles the trigger.
    pub fn to_turso_statements(&self) -> Vec<ast::Stmt> {
        let time = match self.timing {
            PgTriggerTiming::Before => ast::TriggerTime::Before,
            PgTriggerTiming::After => ast::TriggerTime::After,
        };
        self.events
            .iter()
            .map(|event| {
                let event = match event {
                    PgTriggerEvent::Insert => ast::TriggerEvent::Insert,
                    PgTriggerEvent::Delete => ast::TriggerEvent::Delete,
                    PgTriggerEvent::Update(columns) if columns.is_empty() => {
                        ast::TriggerEvent::Update
                    }
                    PgTriggerEvent::Update(columns) => ast::TriggerEvent::UpdateOf(
                        columns
                            .iter()
                            .map(|column| ast::Name::exact(column.clone()))
                            .collect(),
                    ),
                };
                ast::Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: ast::QualifiedName::single(ast::Name::exact(self.name.clone())),
                    time: Some(time),
                    event,
                    tbl_name: self.table.clone(),
                    for_each_row: true,
                    when_clause: self.when.clone().map(Box::new),
                    commands: vec![ast::TriggerCmd::ExecuteFunction {
                        name: self.function.clone(),
                        args: self.args.clone(),
                    }],
                }
            })
            .collect()
    }
}

fn string_nodes(nodes: &[protobuf::Node]) -> Vec<String> {
    nodes
        .iter()
        .filter_map(|node| match &node.node {
            Some(Node::String(s)) => Some(s.sval.clone()),
            Some(Node::Integer(i)) => Some(i.ival.to_string()),
            Some(Node::Float(f)) => Some(f.fval.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_events_timing_and_arguments() {
        let trigger = PgTrigger::parse(
            "CREATE TRIGGER trg BEFORE INSERT OR UPDATE OF a, b OR DELETE ON public.t \
             FOR EACH ROW WHEN (NEW.x > 1) EXECUTE FUNCTION public.f('a', 1)",
        )
        .unwrap();
        assert_eq!(trigger.name, "trg");
        assert_eq!(trigger.table.name.as_str(), "t");
        assert_eq!(trigger.timing, PgTriggerTiming::Before);
        assert_eq!(
            trigger.events,
            vec![
                PgTriggerEvent::Insert,
                PgTriggerEvent::Delete,
                PgTriggerEvent::Update(vec!["a".to_string(), "b".to_string()]),
            ]
        );
        assert_eq!(trigger.when.as_ref().unwrap().to_string(), "new.x > 1");
        assert_eq!(trigger.function.name.as_str(), "f");
        assert_eq!(trigger.args, vec!["a", "1"]);

        let statements = trigger.to_turso_statements();
        assert_eq!(statements.len(), 3);
        let ast::Stmt::CreateTrigger {
            event, commands, ..
        } = &statements[2]
        else {
            panic!("CREATE TRIGGER");
        };
        assert_eq!(
            *event,
            ast::TriggerEvent::UpdateOf(vec![
                ast::Name::exact("a".into()),
                ast::Name::exact("b".into())
            ])
        );
        assert_eq!(
            commands[0].to_string(),
            "EXECUTE FUNCTION public.f ('a', '1')"
        );
    }

    #[test]
    fn execute_procedure_is_the_same_as_execute_function() {
        let trigger = PgTrigger::parse(
            "CREATE TRIGGER on_auth_user_created AFTER INSERT ON users \
             FOR EACH ROW EXECUTE PROCEDURE public.handle_new_user()",
        )
        .unwrap();
        assert_eq!(trigger.timing, PgTriggerTiming::After);
        assert_eq!(trigger.events, vec![PgTriggerEvent::Insert]);
        assert!(trigger.args.is_empty());
        assert_eq!(trigger.function.name.as_str(), "handle_new_user");
    }

    #[test]
    fn rejects_unsupported_trigger_forms() {
        for (sql, message) in [
            (
                "CREATE TRIGGER t AFTER INSERT ON x FOR EACH STATEMENT EXECUTE FUNCTION f()",
                "statement-level triggers",
            ),
            (
                "CREATE TRIGGER t AFTER INSERT ON x EXECUTE FUNCTION f()",
                "statement-level triggers",
            ),
            (
                "CREATE OR REPLACE TRIGGER t AFTER INSERT ON x FOR EACH ROW EXECUTE FUNCTION f()",
                "CREATE OR REPLACE TRIGGER",
            ),
            (
                "CREATE TRIGGER t INSTEAD OF INSERT ON x FOR EACH ROW EXECUTE FUNCTION f()",
                "INSTEAD OF",
            ),
            (
                "CREATE TRIGGER t AFTER TRUNCATE ON x FOR EACH ROW EXECUTE FUNCTION f()",
                "TRUNCATE",
            ),
            (
                "CREATE CONSTRAINT TRIGGER t AFTER INSERT ON x FOR EACH ROW EXECUTE FUNCTION f()",
                "constraint triggers",
            ),
        ] {
            let err = PgTrigger::parse(sql).unwrap_err();
            assert!(err.to_string().contains(message), "{sql}: {err}");
        }
    }
}
