use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::translate::ProgramBuilder;
use crate::translate::ProgramBuilderOpts;
use crate::util::normalize_ident;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{Cookie, Insn};
use crate::{bail_parse_error, Result};
use turso_parser::ast::{self, QualifiedName};

/// Reconstruct SQL string from CREATE TRIGGER AST
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_trigger_to_sql(
    temporary: bool,
    if_not_exists: bool,
    trigger_name: &QualifiedName,
    time: Option<ast::TriggerTime>,
    event: &ast::TriggerEvent,
    tbl_name: &QualifiedName,
    for_each_row: bool,
    when_clause: Option<&ast::Expr>,
    commands: &[ast::TriggerCmd],
) -> String {
    let mut sql = String::new();
    sql.push_str("CREATE");
    if temporary {
        sql.push_str(" TEMP");
    }
    sql.push_str(" TRIGGER");
    if if_not_exists {
        sql.push_str(" IF NOT EXISTS");
    }
    sql.push(' ');
    sql.push_str(trigger_name.name.as_str());
    sql.push(' ');

    if let Some(t) = time {
        match t {
            ast::TriggerTime::Before => sql.push_str("BEFORE "),
            ast::TriggerTime::After => sql.push_str("AFTER "),
            ast::TriggerTime::InsteadOf => sql.push_str("INSTEAD OF "),
        }
    }

    match event {
        ast::TriggerEvent::Delete => sql.push_str("DELETE"),
        ast::TriggerEvent::Insert => sql.push_str("INSERT"),
        ast::TriggerEvent::Update => sql.push_str("UPDATE"),
        ast::TriggerEvent::UpdateOf(cols) => {
            sql.push_str("UPDATE OF ");
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(col.as_str());
            }
        }
    }

    sql.push_str(" ON ");
    sql.push_str(tbl_name.name.as_str());
    if for_each_row {
        sql.push_str(" FOR EACH ROW");
    }

    if let Some(when) = when_clause {
        sql.push_str(" WHEN ");
        sql.push_str(&when.to_string());
    }

    sql.push_str(" BEGIN");
    for cmd in commands {
        sql.push(' ');
        sql.push_str(&cmd.to_string());
        sql.push(';');
    }
    sql.push_str(" END");

    sql
}

/// Translate CREATE TRIGGER statement
#[allow(clippy::too_many_arguments)]
pub fn translate_create_trigger(
    trigger_name: QualifiedName,
    resolver: &Resolver,
    temporary: bool,
    if_not_exists: bool,
    time: Option<ast::TriggerTime>,
    tbl_name: QualifiedName,
    mut program: ProgramBuilder,
    sql: String,
) -> Result<ProgramBuilder> {
    program.begin_write_operation();
    let normalized_trigger_name = normalize_ident(trigger_name.name.as_str());
    let normalized_table_name = normalize_ident(tbl_name.name.as_str());

    if crate::schema::is_system_table(&normalized_table_name) {
        bail_parse_error!("cannot create trigger on system table");
    }

    // Check if trigger already exists
    if resolver
        .schema
        .get_trigger_for_table(&normalized_table_name, &normalized_trigger_name)
        .is_some()
    {
        if if_not_exists {
            return Ok(program);
        }
        bail_parse_error!("Trigger {} already exists", normalized_trigger_name);
    }

    // Verify the table exists
    if resolver.schema.get_table(&normalized_table_name).is_none() {
        bail_parse_error!("no such table: {}", normalized_table_name);
    }

    if time
        .as_ref()
        .is_some_and(|t| *t == ast::TriggerTime::InsteadOf)
    {
        bail_parse_error!("INSTEAD OF triggers are not supported yet");
    }

    if temporary {
        bail_parse_error!("TEMPORARY triggers are not supported yet");
    }

    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 1,
    };
    program.extend(&opts);

    // Open cursor to sqlite_schema table
    let table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: 0,
    });

    // Add the trigger entry to sqlite_schema
    emit_schema_entry(
        &mut program,
        resolver,
        sqlite_schema_cursor_id,
        None, // cdc_table_cursor_id, no cdc for triggers
        SchemaEntryType::Trigger,
        &normalized_trigger_name,
        &normalized_table_name,
        0, // triggers don't have a root page
        Some(sql.clone()),
    )?;

    // Update schema version
    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema.schema_version + 1) as i32,
        p5: 0,
    });

    // Parse schema to load the new trigger
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(format!("name = '{normalized_trigger_name}'")),
    });

    Ok(program)
}

/// Translate DROP TRIGGER statement
pub fn translate_drop_trigger(
    schema: &crate::schema::Schema,
    trigger_name: &str,
    if_exists: bool,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.begin_write_operation();
    let normalized_trigger_name = normalize_ident(trigger_name);

    // Check if trigger exists
    if schema.get_trigger(&normalized_trigger_name).is_none() {
        if if_exists {
            return Ok(program);
        }
        bail_parse_error!("no such trigger: {}", normalized_trigger_name);
    }

    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 1,
    };
    program.extend(&opts);

    // Open cursor to sqlite_schema table
    let table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: 0,
    });

    let search_loop_label = program.allocate_label();
    let skip_non_trigger_label = program.allocate_label();
    let done_label = program.allocate_label();
    let rewind_done_label = program.allocate_label();

    // Find and delete the trigger from sqlite_schema
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: rewind_done_label,
    });

    program.preassign_label_to_next_insn(search_loop_label);

    // Check if this is the trigger we're looking for
    // sqlite_schema columns: type, name, tbl_name, rootpage, sql
    // Column 0: type (should be "trigger")
    // Column 1: name (should match trigger_name)
    let type_reg = program.alloc_register();
    let name_reg = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 0,
        dest: type_reg,
        default: None,
    });
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 1,
        dest: name_reg,
        default: None,
    });

    // Check if type == "trigger"
    let type_str_reg = program.emit_string8_new_reg("trigger".to_string());
    program.emit_insn(Insn::Ne {
        lhs: type_reg,
        rhs: type_str_reg,
        target_pc: skip_non_trigger_label,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    // Check if name matches
    let trigger_name_str_reg = program.emit_string8_new_reg(normalized_trigger_name.clone());
    program.emit_insn(Insn::Ne {
        lhs: name_reg,
        rhs: trigger_name_str_reg,
        target_pc: skip_non_trigger_label,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    // Found it! Delete the row
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
        table_name: SQLITE_TABLEID.to_string(),
        is_part_of_update: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: done_label,
    });

    program.preassign_label_to_next_insn(skip_non_trigger_label);
    // Continue to next row
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: search_loop_label,
    });

    program.preassign_label_to_next_insn(done_label);

    program.preassign_label_to_next_insn(rewind_done_label);

    // Update schema version
    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: (schema.schema_version + 1) as i32,
        p5: 0,
    });

    program.emit_insn(Insn::DropTrigger {
        db: 0,
        trigger_name: normalized_trigger_name.clone(),
    });

    Ok(program)
}
