use crate::schema::{Schema, PG_STORAGE_GATE_TABLE_NAME};
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::emitter::Resolver;
use crate::translate::expr::WalkControl;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::translate::ProgramBuilder;
use crate::util::{walk_expr_with_subqueries, walk_select_expressions};
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{Insn, RegisterOrLiteral};
use crate::{Result, TEMP_DB_ID};
use turso_parser::ast;

/// Older versions do not know the `pg_` types. They read a cast to one, or
/// to a domain over one, as a cast to NUMERIC, and a column of such a domain
/// as a column without a type. They refuse a file with a table that has the
/// PGSTORAGE option, so a file that stores such a cast or type outside a
/// PGSTORAGE table gets an empty PGSTORAGE table.
pub(crate) fn emit_pg_storage_gate(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
) -> Result<()> {
    if database_id == TEMP_DB_ID
        || resolver.with_schema(database_id, |s| {
            s.get_btree_table(PG_STORAGE_GATE_TABLE_NAME).is_some()
        })
    {
        return Ok(());
    }
    let root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: database_id,
        root: root_reg,
        flags: CreateBTreeFlags::new_table(),
    });
    let schema_table = resolver
        .with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID))
        .expect("every database has sqlite_schema");
    let schema_root_page = schema_table.root_page;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(schema_root_page),
        db: database_id,
    });
    emit_schema_entry(
        program,
        resolver,
        cursor_id,
        None,
        SchemaEntryType::Table,
        PG_STORAGE_GATE_TABLE_NAME,
        PG_STORAGE_GATE_TABLE_NAME,
        root_reg,
        Some(format!(
            "CREATE TABLE {PG_STORAGE_GATE_TABLE_NAME} (x TEXT) STRICT, PGSTORAGE"
        )),
    )?;
    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(format!(
            "name = '{PG_STORAGE_GATE_TABLE_NAME}' AND type = 'table'"
        )),
        trigger_target_database_id: None,
    });
    Ok(())
}

pub(crate) fn type_needs_pg_storage(body: &ast::CreateTypeBody, schema: &Schema) -> bool {
    match body {
        ast::CreateTypeBody::CustomType {
            encode,
            decode,
            default,
            ..
        } => [encode, decode, default]
            .into_iter()
            .flatten()
            .any(|expr| casts_to_pg_type(expr, schema)),
        ast::CreateTypeBody::Struct(fields) | ast::CreateTypeBody::Union(fields) => fields
            .iter()
            .any(|field| names_pg_type(&field.field_type.name, schema)),
    }
}

pub(crate) fn domain_needs_pg_storage(
    base_type: &str,
    constraints: &[ast::DomainConstraint],
    default: Option<&ast::Expr>,
    schema: &Schema,
) -> bool {
    names_pg_type(base_type, schema)
        || constraints
            .iter()
            .any(|constraint| casts_to_pg_type(&constraint.check, schema))
        || default.is_some_and(|default| casts_to_pg_type(default, schema))
}

pub(crate) fn table_casts_to_pg_type(body: &ast::CreateTableBody, schema: &Schema) -> bool {
    let ast::CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        ..
    } = body
    else {
        return false;
    };
    columns
        .iter()
        .any(|column| column_casts_to_pg_type(column, schema))
        || constraints
            .iter()
            .any(|constraint| match &constraint.constraint {
                ast::TableConstraint::Check { expr, .. } => casts_to_pg_type(expr, schema),
                _ => false,
            })
}

pub(crate) fn column_casts_to_pg_type(column: &ast::ColumnDefinition, schema: &Schema) -> bool {
    column
        .constraints
        .iter()
        .any(|constraint| match &constraint.constraint {
            ast::ColumnConstraint::Check { expr, .. }
            | ast::ColumnConstraint::Default(expr)
            | ast::ColumnConstraint::Generated { expr, .. } => casts_to_pg_type(expr, schema),
            _ => false,
        })
}

pub(crate) fn trigger_casts_to_pg_type(
    commands: &[ast::TriggerCmd],
    when_clause: Option<&ast::Expr>,
    schema: &Schema,
) -> bool {
    when_clause.is_some_and(|when| casts_to_pg_type(when, schema))
        || commands.iter().any(|command| match command {
            ast::TriggerCmd::Update {
                sets,
                from,
                where_clause,
                ..
            } => {
                sets.iter().any(|set| casts_to_pg_type(&set.expr, schema))
                    || where_clause
                        .as_deref()
                        .is_some_and(|expr| casts_to_pg_type(expr, schema))
                    || from
                        .as_ref()
                        .is_some_and(|from| from_casts_to_pg_type(from, schema))
            }
            ast::TriggerCmd::Insert {
                select,
                upsert,
                returning,
                ..
            } => {
                select_casts_to_pg_type(select, schema)
                    || upsert
                        .as_deref()
                        .is_some_and(|upsert| upsert_casts_to_pg_type(upsert, schema))
                    || returning.iter().any(|column| match column {
                        ast::ResultColumn::Expr(expr, _) => casts_to_pg_type(expr, schema),
                        _ => false,
                    })
            }
            ast::TriggerCmd::Delete { where_clause, .. } => where_clause
                .as_deref()
                .is_some_and(|expr| casts_to_pg_type(expr, schema)),
            ast::TriggerCmd::Select(select) => select_casts_to_pg_type(select, schema),
        })
}

fn from_casts_to_pg_type(from: &ast::FromClause, schema: &Schema) -> bool {
    let select = ast::Select {
        with: None,
        body: ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: None,
                columns: vec![],
                from: Some(from.clone()),
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
        },
        order_by: vec![],
        limit: None,
    };
    select_casts_to_pg_type(&select, schema)
}

fn upsert_casts_to_pg_type(upsert: &ast::Upsert, schema: &Schema) -> bool {
    let target_casts = upsert.index.as_ref().is_some_and(|index| {
        index
            .targets
            .iter()
            .any(|target| casts_to_pg_type(&target.expr, schema))
            || index
                .where_clause
                .as_deref()
                .is_some_and(|expr| casts_to_pg_type(expr, schema))
    });
    let action_casts = match &upsert.do_clause {
        ast::UpsertDo::Set { sets, where_clause } => {
            sets.iter().any(|set| casts_to_pg_type(&set.expr, schema))
                || where_clause
                    .as_deref()
                    .is_some_and(|expr| casts_to_pg_type(expr, schema))
        }
        ast::UpsertDo::Nothing => false,
    };
    target_casts
        || action_casts
        || upsert
            .next
            .as_deref()
            .is_some_and(|next| upsert_casts_to_pg_type(next, schema))
}

pub(crate) fn select_casts_to_pg_type(select: &ast::Select, schema: &Schema) -> bool {
    let mut casts = false;
    walk_select_expressions(select, &mut |expr| {
        casts = casts || is_cast_to_pg_type(expr, schema);
        Ok(WalkControl::Continue)
    })
    .expect("the walk callback returns no error");
    casts
}

pub(crate) fn casts_to_pg_type(expr: &ast::Expr, schema: &Schema) -> bool {
    let mut casts = false;
    walk_expr_with_subqueries(expr, &mut |expr| {
        casts = casts || is_cast_to_pg_type(expr, schema);
        Ok(WalkControl::Continue)
    })
    .expect("the walk callback returns no error");
    casts
}

fn is_cast_to_pg_type(expr: &ast::Expr, schema: &Schema) -> bool {
    match expr {
        ast::Expr::Cast {
            type_name: Some(type_name),
            ..
        } => names_pg_type(&type_name.name, schema),
        _ => false,
    }
}

fn names_pg_type(type_name: &str, schema: &Schema) -> bool {
    matches!(
        schema.resolve_type_unchecked(type_name),
        Ok(Some(resolved)) if resolved.needs_pg_storage()
    )
}
