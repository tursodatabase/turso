use crate::user_function::{Language, UserFunction};
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{CreateTrigStmt, DropStmt, RangeVar};
use pg_query::NodeRef;
use std::sync::Arc;
use turso_core::schema::{Schema, Trigger};
use turso_core::{Connection, LimboError, Result};
use turso_parser::ast;
use turso_pg_parser::translator::{call_name, PostgreSQLTranslator};

pub(crate) const TRIGGER_DEFINITION_FUNCTION: &str = "__turso_postgres_trigger";

pub(crate) const TRIGGER_TYPE_BEFORE: i32 = 1 << 1;
pub(crate) const TRIGGER_TYPE_INSERT: i32 = 1 << 2;
pub(crate) const TRIGGER_TYPE_DELETE: i32 = 1 << 3;
pub(crate) const TRIGGER_TYPE_UPDATE: i32 = 1 << 4;
const TRIGGER_TYPE_TRUNCATE: i32 = 1 << 5;
const TRIGGER_TYPE_INSTEAD: i32 = 1 << 6;

pub(crate) fn create_trigger(
    conn: &Arc<Connection>,
    statement: &CreateTrigStmt,
    search_path: &[String],
) -> Result<()> {
    let unsupported = |what: &str| Err(LimboError::ParseError(format!("{what} are not supported")));
    if !statement.row {
        return unsupported("statement-level triggers");
    }
    if statement.isconstraint {
        return unsupported("constraint triggers");
    }
    if statement.timing & TRIGGER_TYPE_INSTEAD != 0 {
        return unsupported("INSTEAD OF triggers");
    }
    if statement.events & TRIGGER_TYPE_TRUNCATE != 0 {
        return unsupported("TRUNCATE triggers");
    }
    if !statement.transition_rels.is_empty() {
        return unsupported("transition tables");
    }
    let relation = statement
        .relation
        .as_ref()
        .ok_or_else(|| LimboError::ParseError("CREATE TRIGGER missing table name".into()))?;
    let (database, schema) = table_database(conn, relation, search_path)?;
    check_trigger_function(conn, &statement.funcname, search_path)?;

    let existing = core_triggers_named(&schema, &relation.relname, &statement.trigname);
    if !existing.is_empty() {
        if !statement.replace {
            return Err(LimboError::ParseError(format!(
                "trigger \"{}\" for relation \"{}\" already exists",
                statement.trigname, relation.relname
            )));
        }
        drop_core_triggers(conn, &database, existing)?;
    }

    let mut stored = statement.clone();
    stored.replace = false;
    if let Some(relation) = stored.relation.as_mut() {
        relation.schemaname = postgres_schema_name(&database).to_string();
    }
    let definition = NodeRef::CreateTrigStmt(&stored)
        .deparse()
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    let when_clause = statement
        .when_clause
        .as_deref()
        .map(|when| PostgreSQLTranslator::new().translate_expression(when))
        .transpose()
        .map_err(|e| LimboError::ParseError(e.to_string()))?
        .map(Box::new);
    let time = if statement.timing & TRIGGER_TYPE_BEFORE != 0 {
        ast::TriggerTime::Before
    } else {
        ast::TriggerTime::After
    };
    let update_columns: Vec<ast::Name> = statement
        .columns
        .iter()
        .filter_map(|column| match &column.node {
            Some(Node::String(column)) => Some(ast::Name::exact(column.sval.clone())),
            _ => None,
        })
        .collect();
    for (event_flag, event) in [
        (TRIGGER_TYPE_INSERT, ast::TriggerEvent::Insert),
        (TRIGGER_TYPE_UPDATE, ast::TriggerEvent::Update),
        (TRIGGER_TYPE_DELETE, ast::TriggerEvent::Delete),
    ] {
        if statement.events & event_flag == 0 {
            continue;
        }
        let event = match event {
            ast::TriggerEvent::Update if !update_columns.is_empty() => {
                ast::TriggerEvent::UpdateOf(update_columns.clone())
            }
            event => event,
        };
        let core_name = core_trigger_name(&statement.trigname, &relation.relname, &event);
        let create = ast::Stmt::CreateTrigger {
            temporary: false,
            if_not_exists: false,
            trigger_name: ast::QualifiedName::fullname(
                ast::Name::exact(database.clone()),
                ast::Name::exact(core_name),
            ),
            time: Some(time),
            event,
            tbl_name: ast::QualifiedName::single(ast::Name::exact(relation.relname.clone())),
            for_each_row: true,
            when_clause: when_clause.clone(),
            commands: vec![ast::TriggerCmd::Select(definition_call(&definition))],
        };
        let input = create.to_string();
        conn.prepare_translated_stmt(create, &input)?
            .run_ignore_rows()?;
    }
    Ok(())
}

pub(crate) fn drop_trigger(
    conn: &Arc<Connection>,
    drop: &DropStmt,
    search_path: &[String],
) -> Result<()> {
    for object in &drop.objects {
        let Some(Node::List(names)) = &object.node else {
            return Err(LimboError::ParseError(
                "DROP TRIGGER: unexpected object format".into(),
            ));
        };
        let names: Vec<&str> = names
            .items
            .iter()
            .filter_map(|name| match &name.node {
                Some(Node::String(name)) => Some(name.sval.as_str()),
                _ => None,
            })
            .collect();
        let (schemaname, relname, trigger_name) = match names.as_slice() {
            [table, trigger] => ("", *table, *trigger),
            [schema, table, trigger] => (*schema, *table, *trigger),
            _ => {
                return Err(LimboError::ParseError(
                    "DROP TRIGGER: unexpected object format".into(),
                ))
            }
        };
        let relation = RangeVar {
            schemaname: schemaname.to_string(),
            relname: relname.to_string(),
            ..Default::default()
        };
        let table = table_database(conn, &relation, search_path);
        let (database, schema) = match table {
            Ok(table) => table,
            Err(_) if drop.missing_ok => continue,
            Err(error) => return Err(error),
        };
        let existing = core_triggers_named(&schema, relname, trigger_name);
        if existing.is_empty() {
            if drop.missing_ok {
                continue;
            }
            return Err(LimboError::ParseError(format!(
                "trigger \"{trigger_name}\" for table \"{relname}\" does not exist"
            )));
        }
        drop_core_triggers(conn, &database, existing)?;
    }
    Ok(())
}

pub(crate) fn trigger_definition(trigger: &Trigger) -> Option<String> {
    let [ast::TriggerCmd::Select(select)] = trigger.commands.as_slice() else {
        return None;
    };
    let ast::OneSelect::Select { columns, .. } = &select.body.select else {
        return None;
    };
    let [ast::ResultColumn::Expr(call, _)] = columns.as_slice() else {
        return None;
    };
    let ast::Expr::FunctionCall { name, args, .. } = call.as_ref() else {
        return None;
    };
    if !name
        .as_str()
        .eq_ignore_ascii_case(TRIGGER_DEFINITION_FUNCTION)
    {
        return None;
    }
    let [definition] = args.as_slice() else {
        return None;
    };
    let ast::Expr::Literal(ast::Literal::String(definition)) = definition.as_ref() else {
        return None;
    };
    let definition = definition.strip_prefix('\'')?.strip_suffix('\'')?;
    Some(definition.replace("''", "'"))
}

pub(crate) fn parse_trigger_definition(definition: &str) -> Result<CreateTrigStmt> {
    let parsed = turso_pg_parser::parse(definition)
        .map_err(|e| LimboError::Corrupt(format!("malformed trigger definition: {e}")))?;
    match parsed
        .protobuf
        .stmts
        .first()
        .and_then(|stmt| stmt.stmt.as_deref())
    {
        Some(pg_query::protobuf::Node {
            node: Some(Node::CreateTrigStmt(statement)),
        }) => Ok(statement.as_ref().clone()),
        _ => Err(LimboError::Corrupt(format!(
            "malformed trigger definition: {definition}"
        ))),
    }
}

pub(crate) fn postgres_schema_name(database: &str) -> &str {
    match database {
        "main" => "public",
        database => database,
    }
}

fn check_trigger_function(
    conn: &Connection,
    funcname: &[pg_query::protobuf::Node],
    search_path: &[String],
) -> Result<()> {
    let call_name = call_name(funcname).map_err(|e| LimboError::ParseError(e.to_string()))?;
    let (databases, name) = match call_name.split_once('.') {
        Some((database, name)) => (vec![database.to_string()], name),
        None => (search_order(conn, search_path), call_name.as_str()),
    };
    let function = databases.iter().find_map(|database| {
        conn.database_schema(database)?
            .get_function(name, 0)
            .cloned()
    });
    let Some(function) = function else {
        return Err(LimboError::ParseError(format!(
            "function {name}() does not exist"
        )));
    };
    let user_function = UserFunction::parse(&function)?;
    let returns_trigger = user_function.return_type().is_some_and(|return_type| {
        return_type.names.last().is_some_and(|last| {
            matches!(&last.node, Some(Node::String(type_name)) if type_name.sval == "trigger")
        })
    });
    if !returns_trigger || user_function.language != Language::Plpgsql {
        return Err(LimboError::ParseError(format!(
            "function {name} must return type trigger"
        )));
    }
    Ok(())
}

fn table_database(
    conn: &Connection,
    relation: &RangeVar,
    search_path: &[String],
) -> Result<(String, Arc<Schema>)> {
    let databases = if relation.schemaname.is_empty() {
        search_order(conn, search_path)
    } else {
        vec![database_name(&relation.schemaname).to_string()]
    };
    databases
        .into_iter()
        .find_map(|database| {
            let schema = conn.database_schema(&database)?;
            schema
                .get_btree_table(&relation.relname)
                .is_some()
                .then_some((database, schema))
        })
        .ok_or_else(|| {
            LimboError::ParseError(format!("relation \"{}\" does not exist", relation.relname))
        })
}

fn search_order(conn: &Connection, search_path: &[String]) -> Vec<String> {
    if search_path.is_empty() {
        let mut databases = vec!["main".to_string()];
        databases.extend(conn.attached_database_names());
        return databases;
    }
    search_path
        .iter()
        .filter(|schema| schema.as_str() != "$user" && schema.as_str() != "pg_catalog")
        .map(|schema| database_name(schema).to_string())
        .collect()
}

fn database_name(schema: &str) -> &str {
    match schema {
        "public" => "main",
        schema => schema,
    }
}

fn core_triggers_named(schema: &Schema, table: &str, trigger_name: &str) -> Vec<String> {
    schema
        .get_triggers_for_table(table)
        .filter(|trigger| {
            trigger_definition(trigger)
                .and_then(|definition| parse_trigger_definition(&definition).ok())
                .is_some_and(|definition| definition.trigname == trigger_name)
        })
        .map(|trigger| trigger.name.clone())
        .collect()
}

fn drop_core_triggers(conn: &Arc<Connection>, database: &str, names: Vec<String>) -> Result<()> {
    for name in names {
        let drop = ast::Stmt::DropTrigger {
            if_exists: false,
            trigger_name: ast::QualifiedName::fullname(
                ast::Name::exact(database.to_string()),
                ast::Name::exact(name),
            ),
        };
        let input = drop.to_string();
        conn.prepare_translated_stmt(drop, &input)?
            .run_ignore_rows()?;
    }
    Ok(())
}

fn core_trigger_name(trigger_name: &str, table: &str, event: &ast::TriggerEvent) -> String {
    let event = match event {
        ast::TriggerEvent::Insert => "INSERT",
        ast::TriggerEvent::Update | ast::TriggerEvent::UpdateOf(_) => "UPDATE",
        ast::TriggerEvent::Delete => "DELETE",
    };
    format!("{trigger_name} ON {table} {event}")
}

fn definition_call(definition: &str) -> ast::Select {
    ast::Select {
        with: None,
        body: ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: None,
                columns: vec![ast::ResultColumn::Expr(
                    Box::new(ast::Expr::FunctionCall {
                        name: ast::Name::exact(TRIGGER_DEFINITION_FUNCTION.to_string()),
                        distinctness: None,
                        args: vec![Box::new(ast::Expr::Literal(ast::Literal::String(format!(
                            "'{}'",
                            definition.replace('\'', "''")
                        ))))],
                        order_by: vec![],
                        within_group: vec![],
                        filter_over: ast::FunctionTail {
                            filter_clause: None,
                            over_clause: None,
                        },
                    }),
                    None,
                )],
                from: None,
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
        },
        order_by: vec![],
        limit: None,
    }
}
