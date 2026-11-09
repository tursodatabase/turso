//! CREATE, ALTER and DROP TRIGGER, the trigger actions of ALTER TABLE, and
//! event triggers.
//!
//! tursopg has no triggers. The only trigger functions are the ones that
//! PostgreSQL has built in, such as suppress_redundant_updates_trigger().

use super::function::SQLSTATE_INVALID_OBJECT_DEFINITION;
use super::{
    check_schema, error, permission_denied, qualified_name, strings, Relation, RelationKind,
    RoutineCatalog,
};
use crate::builtins;
use crate::sqlstate::{
    SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_SYNTAX_ERROR, SQLSTATE_UNDEFINED_COLUMN,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_OBJECT, SQLSTATE_UNDEFINED_TABLE,
    SQLSTATE_WRONG_OBJECT_TYPE,
};
use crate::translator::not_supported;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self as pb, AlterTableType, CreateTrigStmt, RangeVar};
use pg_query::NodeRef;

/// SQLSTATE 42701: a column that a list names twice.
const SQLSTATE_DUPLICATE_COLUMN: &str = "42701";

// The flags of PostgreSQL's trigger.h in `timing` and `events`.
const TRIGGER_TYPE_INSERT: i32 = 1 << 2;
const TRIGGER_TYPE_DELETE: i32 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i32 = 1 << 4;
const TRIGGER_TYPE_TRUNCATE: i32 = 1 << 5;
const TRIGGER_TYPE_INSTEAD: i32 = 1 << 6;

/// Checks CREATE TRIGGER in the order of PostgreSQL's
/// CreateTriggerFiringOn().
pub(super) fn check_create_trigger(
    stmt: &CreateTrigStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let range = stmt
        .relation
        .as_ref()
        .ok_or_else(|| not_supported("CREATE TRIGGER"))?;
    let relation = find_relation(range, catalog)?;
    let name = &range.relname;
    let instead = stmt.timing & TRIGGER_TYPE_INSTEAD != 0;
    let truncate = stmt.events & TRIGGER_TYPE_TRUNCATE != 0;
    match relation.kind {
        RelationKind::Table if instead => {
            return Err(error(
                SQLSTATE_WRONG_OBJECT_TYPE,
                format!("\"{name}\" is a table"),
            ))
        }
        RelationKind::Table => {}
        RelationKind::View if !instead && stmt.row => {
            return Err(error(
                SQLSTATE_WRONG_OBJECT_TYPE,
                format!("\"{name}\" is a view"),
            ))
        }
        RelationKind::View if truncate => {
            return Err(error(
                SQLSTATE_WRONG_OBJECT_TYPE,
                format!("\"{name}\" is a view"),
            ))
        }
        RelationKind::View => {}
        RelationKind::MaterializedView | RelationKind::Sequence => {
            return Err(error(
                SQLSTATE_WRONG_OBJECT_TYPE,
                format!("relation \"{name}\" cannot have triggers"),
            ))
        }
        RelationKind::Index => {
            return Err(error(
                SQLSTATE_WRONG_OBJECT_TYPE,
                format!("cannot open relation \"{name}\""),
            ))
        }
    }
    if is_system_catalog(range) {
        return Err(permission_denied(format!(
            "permission denied: \"{name}\" is a system catalog"
        )));
    }
    if let Some(referenced) = &stmt.constrrel {
        find_relation(referenced, catalog)?;
    }
    if !catalog.is_superuser() {
        let what = match relation.kind {
            RelationKind::View => "view",
            _ => "table",
        };
        return Err(permission_denied(format!(
            "permission denied for {what} {name}"
        )));
    }
    if stmt.row && truncate {
        return Err(not_supported_as_postgres(
            "TRUNCATE FOR EACH ROW triggers are not supported",
        ));
    }
    if instead {
        if !stmt.row {
            return Err(not_supported_as_postgres(
                "INSTEAD OF triggers must be FOR EACH ROW",
            ));
        }
        if stmt.when_clause.is_some() {
            return Err(not_supported_as_postgres(
                "INSTEAD OF triggers cannot have WHEN conditions",
            ));
        }
        if !stmt.columns.is_empty() {
            return Err(not_supported_as_postgres(
                "INSTEAD OF triggers cannot have column lists",
            ));
        }
    }
    check_transition_tables(stmt, &relation, name)?;
    if let Some(when) = stmt.when_clause.as_deref() {
        check_when(stmt, range, when, catalog)?;
    }
    check_trigger_function(
        &stmt.funcname,
        "trigger",
        SQLSTATE_INVALID_OBJECT_DEFINITION,
        catalog,
    )?;
    let mut columns: Vec<&str> = Vec::new();
    for column in strings(&stmt.columns) {
        if !relation.columns.iter().any(|(name, _)| name == column) {
            return Err(error(
                SQLSTATE_UNDEFINED_COLUMN,
                format!("column \"{column}\" of relation \"{name}\" does not exist"),
            ));
        }
        if columns.contains(&column) {
            return Err(error(
                SQLSTATE_DUPLICATE_COLUMN,
                format!("column \"{column}\" specified more than once"),
            ));
        }
        columns.push(column);
    }
    Err(unsupported_feature(stmt))
}

fn check_transition_tables(
    stmt: &CreateTrigStmt,
    relation: &Relation,
    name: &str,
) -> Result<(), ParseError> {
    let mut new_table: Option<&str> = None;
    let mut old_table: Option<&str> = None;
    for transition in &stmt.transition_rels {
        let Some(Node::TriggerTransition(transition)) = &transition.node else {
            continue;
        };
        if !transition.is_table {
            return Err(not_supported_as_postgres(
                "ROW variable naming in the REFERENCING clause is not supported",
            ));
        }
        if relation.kind == RelationKind::View {
            return Err(error(
                SQLSTATE_WRONG_OBJECT_TYPE,
                format!("\"{name}\" is a view"),
            ));
        }
        if stmt.timing != 0 {
            return Err(invalid_definition(
                "transition table name can only be specified for an AFTER trigger",
            ));
        }
        if stmt.events & TRIGGER_TYPE_TRUNCATE != 0 {
            return Err(invalid_definition(
                "TRUNCATE triggers with transition tables are not supported",
            ));
        }
        let events = [
            TRIGGER_TYPE_INSERT,
            TRIGGER_TYPE_UPDATE,
            TRIGGER_TYPE_DELETE,
        ]
        .iter()
        .filter(|event| stmt.events & **event != 0)
        .count();
        if events != 1 {
            return Err(not_supported_as_postgres(
                "transition tables cannot be specified for triggers with more than one event",
            ));
        }
        if !stmt.columns.is_empty() {
            return Err(not_supported_as_postgres(
                "transition tables cannot be specified for triggers with column lists",
            ));
        }
        if stmt.isconstraint {
            return Err(not_supported_as_postgres(
                "transition tables cannot be specified for constraint triggers",
            ));
        }
        let changes = |event: i32| stmt.events & (event | TRIGGER_TYPE_UPDATE) != 0;
        if transition.is_new {
            if !changes(TRIGGER_TYPE_INSERT) {
                return Err(invalid_definition(
                    "NEW TABLE can only be specified for an INSERT or UPDATE trigger",
                ));
            }
            if new_table.is_some() {
                return Err(invalid_definition(
                    "NEW TABLE cannot be specified multiple times",
                ));
            }
            new_table = Some(&transition.name);
        } else {
            if !changes(TRIGGER_TYPE_DELETE) {
                return Err(invalid_definition(
                    "OLD TABLE can only be specified for a DELETE or UPDATE trigger",
                ));
            }
            if old_table.is_some() {
                return Err(invalid_definition(
                    "OLD TABLE cannot be specified multiple times",
                ));
            }
            old_table = Some(&transition.name);
        }
    }
    if new_table.is_some() && new_table == old_table {
        return Err(invalid_definition(
            "OLD TABLE name and NEW TABLE name cannot be the same",
        ));
    }
    Ok(())
}

/// Checks the WHEN condition. PostgreSQL reads it with the table as `old`
/// and as `new`, and then checks the references to them.
fn check_when(
    stmt: &CreateTrigStmt,
    range: &RangeVar,
    when: &pb::Node,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let sql = select_with_old_and_new(range, when)?;
    super::prepare(catalog, &sql)?;
    if let Some(ty) = super::expr::expression_type(when) {
        if ty != "bool" && ty != "unknown" {
            return Err(error(
                SQLSTATE_DATATYPE_MISMATCH,
                format!(
                    "argument of WHEN must be type boolean, not type {}",
                    builtins::type_display_name(ty)
                ),
            ));
        }
    }
    let Some(when) = &when.node else {
        return Ok(());
    };
    let mut references: Vec<(i32, &str)> = when
        .nodes()
        .into_iter()
        .filter_map(|(node, ..)| match node {
            NodeRef::ColumnRef(column) => {
                let names = strings(&column.fields);
                let qualifier = *names.first()?;
                (column.fields.len() > 1 && matches!(qualifier, "old" | "new"))
                    .then_some((column.location, qualifier))
            }
            _ => None,
        })
        .collect();
    references.sort_unstable();
    for (_, qualifier) in references {
        if !stmt.row {
            return Err(invalid_definition(
                "statement trigger's WHEN condition cannot reference column values",
            ));
        }
        if qualifier == "old" && stmt.events & TRIGGER_TYPE_INSERT != 0 {
            return Err(invalid_definition(
                "INSERT trigger's WHEN condition cannot reference OLD values",
            ));
        }
        if qualifier == "new" && stmt.events & TRIGGER_TYPE_DELETE != 0 {
            return Err(invalid_definition(
                "DELETE trigger's WHEN condition cannot reference NEW values",
            ));
        }
    }
    Ok(())
}

/// `SELECT 1 FROM <table> AS old, <table> AS new WHERE <condition>`.
fn select_with_old_and_new(range: &RangeVar, condition: &pb::Node) -> Result<String, ParseError> {
    let table = |alias: &str| pb::Node {
        node: Some(Node::RangeVar(RangeVar {
            alias: Some(pb::Alias {
                aliasname: alias.to_string(),
                colnames: Vec::new(),
            }),
            ..range.clone()
        })),
    };
    let one = pb::Node {
        node: Some(Node::ResTarget(Box::new(pb::ResTarget {
            val: Some(Box::new(pb::Node {
                node: Some(Node::AConst(pb::AConst {
                    isnull: false,
                    val: Some(pb::a_const::Val::Ival(pb::Integer { ival: 1 })),
                    location: 0,
                })),
            })),
            ..Default::default()
        }))),
    };
    let select = pb::SelectStmt {
        target_list: vec![one],
        from_clause: vec![table("old"), table("new")],
        where_clause: Some(Box::new(condition.clone())),
        limit_option: pb::LimitOption::Default.into(),
        op: pb::SetOperation::SetopNone.into(),
        ..Default::default()
    };
    Node::SelectStmt(Box::new(select))
        .deparse()
        .map_err(|error| ParseError::ParseError(error.to_string()))
}

/// Checks the function of a trigger or an event trigger as PostgreSQL does:
/// a function without arguments that returns `result_type`.
fn check_trigger_function(
    names: &[pb::Node],
    result_type: &str,
    wrong_result_sqlstate: &'static str,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let names = strings(names);
    let (schema, name) = qualified_name(&names)?;
    if let Some(schema) = schema {
        check_schema(schema, catalog)?;
    }
    let function = match schema {
        None | Some("pg_catalog") => builtins::functions(name)
            .iter()
            .find(|function| function.parameters.is_empty()),
        Some(_) => None,
    };
    let written = names.join(".");
    let Some(function) = function else {
        return Err(error(
            SQLSTATE_UNDEFINED_FUNCTION,
            format!("function {written}() does not exist"),
        ));
    };
    if function.result_type != result_type {
        return Err(error(
            wrong_result_sqlstate,
            format!("function {written} must return type {result_type}"),
        ));
    }
    Ok(())
}

/// The error for the first feature of a valid trigger that tursopg does not
/// support.
fn unsupported_feature(stmt: &CreateTrigStmt) -> ParseError {
    let clause = if stmt.isconstraint {
        return not_supported("CREATE CONSTRAINT TRIGGER");
    } else if stmt.replace {
        return not_supported("CREATE OR REPLACE TRIGGER");
    } else if stmt.timing & TRIGGER_TYPE_INSTEAD != 0 {
        "INSTEAD OF"
    } else if stmt.events & TRIGGER_TYPE_TRUNCATE != 0 {
        "TRUNCATE"
    } else if !stmt.columns.is_empty() {
        "UPDATE OF"
    } else if stmt.when_clause.is_some() {
        "WHEN"
    } else if !stmt.transition_rels.is_empty() {
        "REFERENCING"
    } else if !stmt.args.is_empty() {
        return not_supported("an argument to a trigger function");
    } else if !stmt.row {
        return not_supported("a statement-level trigger (a trigger without FOR EACH ROW)");
    } else {
        return not_supported("CREATE TRIGGER");
    };
    not_supported(format!("CREATE TRIGGER ... {clause}"))
}

/// DROP TRIGGER: the table must exist, and tursopg has no triggers.
pub(super) fn check_drop_trigger(
    objects: &[pb::Node],
    missing_ok: bool,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    for object in objects {
        let Some(Node::List(names)) = &object.node else {
            continue;
        };
        match check_trigger_names(&strings(&names.items), catalog) {
            Err(_) if missing_ok => continue,
            Err(error) => return Err(error),
            Ok(()) => {}
        }
    }
    Ok(())
}

/// COMMENT ON TRIGGER: the table must exist, and tursopg has no triggers.
pub(super) fn check_trigger_reference(
    object: &Node,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    match object {
        Node::List(names) => check_trigger_names(&strings(&names.items), catalog),
        _ => Err(not_supported("COMMENT ON TRIGGER")),
    }
}

/// The error for a trigger named by the names of its table and its own
/// name, as in `public.items.t`.
fn check_trigger_names(names: &[&str], catalog: &dyn RoutineCatalog) -> Result<(), ParseError> {
    let [table @ .., trigger] = names else {
        return Ok(());
    };
    let (schema, table) = qualified_name(table)?;
    if let Some(schema) = schema {
        check_schema(schema, catalog)?;
    }
    if catalog.relation(schema, table).is_none() {
        return Err(missing_relation(schema, table));
    }
    Err(missing_trigger(trigger, table))
}

/// ALTER TRIGGER: the table must exist, and tursopg has no triggers.
pub(super) fn check_alter_trigger(
    range: &RangeVar,
    catalog: &dyn RoutineCatalog,
    trigger: &str,
) -> Result<(), ParseError> {
    find_relation(range, catalog)?;
    Err(missing_trigger(trigger, &range.relname))
}

/// The trigger actions of ALTER TABLE, such as ENABLE TRIGGER. None when
/// the statement has none, or when it has other actions too and does not
/// fail here.
pub(super) fn check_alter_table_triggers(
    stmt: &pb::AlterTableStmt,
    catalog: &dyn RoutineCatalog,
) -> Option<Result<(), ParseError>> {
    let commands: Vec<&pb::AlterTableCmd> = stmt
        .cmds
        .iter()
        .filter_map(|command| match &command.node {
            Some(Node::AlterTableCmd(command)) => Some(command.as_ref()),
            _ => None,
        })
        .collect();
    let triggers: Vec<(&pb::AlterTableCmd, &str)> = commands
        .iter()
        .filter_map(|command| Some((*command, trigger_action(command.subtype())?)))
        .collect();
    let (_, first_action) = triggers.first()?;
    let range = stmt.relation.as_ref()?;
    let relation = match find_relation(range, catalog) {
        Ok(relation) => relation,
        Err(_) if stmt.missing_ok => return Some(Ok(())),
        Err(error) => return Some(Err(error)),
    };
    if relation.kind != RelationKind::Table {
        return Some(Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!(
                "ALTER action {first_action} cannot be performed on relation \"{}\"",
                range.relname
            ),
        )));
    }
    if !catalog.is_superuser() {
        return Some(Err(permission_denied(format!(
            "must be owner of table {}",
            range.relname
        ))));
    }
    let named = triggers.iter().find(|(command, _)| {
        matches!(
            command.subtype(),
            AlterTableType::AtEnableTrig
                | AlterTableType::AtEnableAlwaysTrig
                | AlterTableType::AtEnableReplicaTrig
                | AlterTableType::AtDisableTrig
        )
    });
    if let Some((command, _)) = named {
        return Some(Err(missing_trigger(&command.name, &range.relname)));
    }
    // PostgreSQL checks a foreign key with system triggers on both of its
    // tables, and DISABLE TRIGGER ALL turns them off. tursopg cannot stop
    // the checks of one table.
    let disables_all = triggers
        .iter()
        .any(|(command, _)| command.subtype() == AlterTableType::AtDisableTrigAll);
    if disables_all && relation.foreign_keys_checked {
        return Some(Err(not_supported(
            "ALTER TABLE ... DISABLE TRIGGER ALL on a table with foreign key checks",
        )));
    }
    // Without triggers, ENABLE and DISABLE TRIGGER ALL or USER do nothing.
    if triggers.len() == commands.len() {
        return Some(Ok(()));
    }
    Some(Err(not_supported(format!(
        "ALTER TABLE ... {first_action} with other actions"
    ))))
}

/// The name of a trigger action of ALTER TABLE, as PostgreSQL's
/// alter_table_type_to_string() gives it.
fn trigger_action(action: AlterTableType) -> Option<&'static str> {
    Some(match action {
        AlterTableType::AtEnableTrig => "ENABLE TRIGGER",
        AlterTableType::AtEnableAlwaysTrig => "ENABLE ALWAYS TRIGGER",
        AlterTableType::AtEnableReplicaTrig => "ENABLE REPLICA TRIGGER",
        AlterTableType::AtDisableTrig => "DISABLE TRIGGER",
        AlterTableType::AtEnableTrigAll => "ENABLE TRIGGER ALL",
        AlterTableType::AtDisableTrigAll => "DISABLE TRIGGER ALL",
        AlterTableType::AtEnableTrigUser => "ENABLE TRIGGER USER",
        AlterTableType::AtDisableTrigUser => "DISABLE TRIGGER USER",
        _ => return None,
    })
}

/// Checks CREATE EVENT TRIGGER in the order of PostgreSQL's
/// CreateEventTrigger(). It always fails, because no function that
/// PostgreSQL has built in returns event_trigger.
pub(super) fn check_create_event_trigger(
    stmt: &pb::CreateEventTrigStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    if !catalog.is_superuser() {
        return Err(permission_denied(format!(
            "permission denied to create event trigger \"{}\"",
            stmt.trigname
        )));
    }
    let event = stmt.eventname.as_str();
    if !matches!(
        event,
        "ddl_command_start" | "ddl_command_end" | "sql_drop" | "login" | "table_rewrite"
    ) {
        return Err(error(
            SQLSTATE_SYNTAX_ERROR,
            format!("unrecognized event name \"{event}\""),
        ));
    }
    let mut tags: Option<Vec<&str>> = None;
    for filter in &stmt.whenclause {
        let Some(Node::DefElem(filter)) = &filter.node else {
            continue;
        };
        if filter.defname != "tag" {
            return Err(error(
                SQLSTATE_SYNTAX_ERROR,
                format!("unrecognized filter variable \"{}\"", filter.defname),
            ));
        }
        if tags.is_some() {
            return Err(error(
                SQLSTATE_SYNTAX_ERROR,
                "filter variable \"tag\" specified more than once",
            ));
        }
        tags = Some(
            match filter.arg.as_deref().and_then(|arg| arg.node.as_ref()) {
                Some(Node::List(list)) => strings(&list.items),
                _ => Vec::new(),
            },
        );
    }
    if let Some(tags) = tags {
        check_tags(event, &tags)?;
    }
    check_trigger_function(
        &stmt.funcname,
        "event_trigger",
        SQLSTATE_INVALID_OBJECT_DEFINITION,
        catalog,
    )?;
    Err(not_supported("CREATE EVENT TRIGGER"))
}

/// Checks the command tags of the WHEN TAG IN filter of an event trigger.
fn check_tags(event: &str, tags: &[&str]) -> Result<(), ParseError> {
    if event == "login" {
        return Err(not_supported_as_postgres(
            "tag filtering is not supported for login event triggers",
        ));
    }
    for tag in tags {
        let found = builtins::command_tag(tag);
        let supported = if event == "table_rewrite" {
            found.is_some_and(|found| found.table_rewrite_ok)
        } else {
            let Some(found) = found else {
                return Err(error(
                    SQLSTATE_SYNTAX_ERROR,
                    format!("filter value \"{tag}\" not recognized for filter variable \"tag\""),
                ));
            };
            found.event_trigger_ok
        };
        if !supported {
            return Err(not_supported_as_postgres(format!(
                "event triggers are not supported for {tag}"
            )));
        }
    }
    Ok(())
}

pub(super) fn check_drop_event_trigger(
    objects: &[pb::Node],
    missing_ok: bool,
) -> Result<(), ParseError> {
    match objects.iter().find_map(|object| object.node.as_ref()) {
        Some(Node::String(name)) if !missing_ok => Err(missing_event_trigger(&name.sval)),
        _ => Ok(()),
    }
}

pub(super) fn missing_event_trigger(name: &str) -> ParseError {
    error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!("event trigger \"{name}\" does not exist"),
    )
}

fn missing_trigger(trigger: &str, table: &str) -> ParseError {
    error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!("trigger \"{trigger}\" for table \"{table}\" does not exist"),
    )
}

/// The relation that a RangeVar names.
fn find_relation(range: &RangeVar, catalog: &dyn RoutineCatalog) -> Result<Relation, ParseError> {
    if !range.catalogname.is_empty() {
        return Err(not_supported_as_postgres(format!(
            "cross-database references are not implemented: {}.{}.{}",
            range.catalogname, range.schemaname, range.relname
        )));
    }
    let schema = (!range.schemaname.is_empty()).then_some(range.schemaname.as_str());
    if let Some(schema) = schema {
        check_schema(schema, catalog)?;
    }
    catalog
        .relation(schema, &range.relname)
        .ok_or_else(|| missing_relation(schema, &range.relname))
}

fn missing_relation(schema: Option<&str>, name: &str) -> ParseError {
    let name = match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    };
    error(
        SQLSTATE_UNDEFINED_TABLE,
        format!("relation \"{name}\" does not exist"),
    )
}

/// Whether the relation is a table of pg_catalog.
fn is_system_catalog(range: &RangeVar) -> bool {
    matches!(range.schemaname.as_str(), "" | "pg_catalog")
        && crate::translator::is_catalog_table_name(&range.relname)
}

fn invalid_definition(message: &str) -> ParseError {
    error(SQLSTATE_INVALID_OBJECT_DEFINITION, message)
}

fn not_supported_as_postgres(message: impl Into<String>) -> ParseError {
    super::not_supported_as_postgres(message)
}
