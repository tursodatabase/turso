use crate::schema::{BTreeTable, FunctionDef, TURSO_FUNCTIONS_TABLE_NAME};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::translate::ProgramBuilder;
use crate::util::normalize_ident;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{to_u32, CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral};
use crate::{bail_parse_error, Result};
use turso_parser::ast;

pub fn translate_create_function(
    or_replace: bool,
    func_name: &ast::QualifiedName,
    arg_count: usize,
    definition: String,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let database_id = resolver.resolve_database_id_for_create(func_name)?;
    let name = normalize_ident(func_name.name.as_str());
    let exists = resolver.with_schema(database_id, |schema| {
        schema.get_function(&name, arg_count).is_some()
    });
    if exists && !or_replace {
        bail_parse_error!("function \"{name}\" already exists with same argument types");
    }
    let schema_cookie = resolver.with_schema(database_id, |schema| schema.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let table = open_functions_table(database_id, resolver, program)?;
    let cursor_id = table.cursor_id;
    if !table.created_by_this_statement {
        emit_delete_function_rows(cursor_id, &name, arg_count, program);
    }
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    let first_column_reg = program.emit_string8_new_reg(name.clone());
    let arg_count_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: arg_count as i64,
        dest: arg_count_reg,
    });
    program.emit_string8_new_reg(definition.clone());
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(3),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: TURSO_FUNCTIONS_TABLE_NAME.to_string(),
    });
    program.emit_insn(Insn::AddFunction {
        db: database_id,
        function: Box::new(FunctionDef {
            name,
            arg_count,
            definition,
        }),
    });
    emit_schema_version_increment(database_id, resolver, program);
    Ok(())
}

pub fn translate_drop_function(
    if_exists: bool,
    func_name: &ast::QualifiedName,
    arg_count: Option<usize>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let name = normalize_ident(func_name.name.as_str());
    let database_id = if func_name.namespace.is_some() {
        Some(resolver.resolve_database_id(func_name)?)
    } else {
        resolver.resolve_function_database_id(&name, arg_count)
    };
    let overloads = database_id
        .map(|database_id| {
            resolver.with_schema(database_id, |schema| {
                schema
                    .functions_named(&name)
                    .filter(|function| arg_count.is_none_or(|count| function.arg_count == count))
                    .map(|function| function.arg_count)
                    .collect::<Vec<_>>()
            })
        })
        .unwrap_or_default();
    let (database_id, arg_count) = match (database_id, overloads.as_slice()) {
        (Some(database_id), [arg_count]) => (database_id, *arg_count),
        (_, []) if if_exists => return Ok(()),
        (_, []) => match arg_count {
            Some(_) => bail_parse_error!("function {name} does not exist"),
            None => bail_parse_error!("could not find a function named \"{name}\""),
        },
        _ => bail_parse_error!("function name \"{name}\" is not unique"),
    };
    let schema_cookie = resolver.with_schema(database_id, |schema| schema.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let table = open_functions_table(database_id, resolver, program)?;
    emit_delete_function_rows(table.cursor_id, &name, arg_count, program);
    program.emit_insn(Insn::DropFunction {
        db: database_id,
        name,
        arg_count,
    });
    emit_schema_version_increment(database_id, resolver, program);
    Ok(())
}

struct FunctionsTable {
    cursor_id: usize,
    created_by_this_statement: bool,
}

fn open_functions_table(
    database_id: usize,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<FunctionsTable> {
    let existing = resolver.with_schema(database_id, |schema| {
        schema.get_btree_table(TURSO_FUNCTIONS_TABLE_NAME)
    });
    let (table, root_page, created_by_this_statement) = match existing {
        Some(table) => {
            let root_page = RegisterOrLiteral::Literal(table.root_page);
            (table, root_page, false)
        }
        None => {
            let root_reg = program.alloc_register();
            program.emit_insn(Insn::CreateBtree {
                db: database_id,
                root: root_reg,
                flags: CreateBTreeFlags::new_table(),
            });
            let sql = format!(
                "CREATE TABLE {TURSO_FUNCTIONS_TABLE_NAME}(name TEXT, arg_count INTEGER, definition TEXT)"
            );
            let table = Arc::new(BTreeTable::from_sql(&sql, 0)?);
            let schema_table = resolver
                .schema()
                .get_btree_table(SQLITE_TABLEID)
                .expect("sqlite_schema is always in the schema");
            let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
            program.emit_insn(Insn::OpenWrite {
                cursor_id: schema_cursor_id,
                root_page: 1i64.into(),
                db: database_id,
            });
            emit_schema_entry(
                program,
                resolver,
                schema_cursor_id,
                None,
                SchemaEntryType::Table,
                TURSO_FUNCTIONS_TABLE_NAME,
                TURSO_FUNCTIONS_TABLE_NAME,
                root_reg,
                Some(sql),
            )?;
            program.emit_insn(Insn::ParseSchema {
                db: database_id,
                where_clause: Some(format!(
                    "tbl_name = '{TURSO_FUNCTIONS_TABLE_NAME}' AND type != 'trigger'"
                )),
                trigger_target_database_id: None,
            });
            (table, RegisterOrLiteral::Register(root_reg), true)
        }
    };
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page,
        db: database_id,
    });
    Ok(FunctionsTable {
        cursor_id,
        created_by_this_statement,
    })
}

fn emit_delete_function_rows(
    cursor_id: usize,
    name: &str,
    arg_count: usize,
    program: &mut ProgramBuilder,
) {
    let name_reg = program.emit_string8_new_reg(name.to_string());
    let arg_count_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: arg_count as i64,
        dest: arg_count_reg,
    });
    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    let column_reg = program.alloc_register();
    program.emit_column_or_rowid(cursor_id, 0, column_reg);
    program.emit_insn(Insn::Ne {
        lhs: column_reg,
        rhs: name_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_column_or_rowid(cursor_id, 1, column_reg);
    program.emit_insn(Insn::Ne {
        lhs: column_reg,
        rhs: arg_count_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::Delete {
        cursor_id,
        table_name: TURSO_FUNCTIONS_TABLE_NAME.to_string(),
        is_part_of_update: false,
    });
    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id,
        pc_if_next: loop_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_label);
}

fn emit_schema_version_increment(
    database_id: usize,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) {
    let schema_version = resolver.with_schema(database_id, |schema| schema.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });
}
