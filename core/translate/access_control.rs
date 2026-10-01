use std::sync::Arc;

use turso_parser::ast;

use crate::{
    access_control::{
        AccessControlCatalog, AccessControlChange, ACCESS_CONTROL_TABLE_NAME,
        ACCESS_CONTROL_TABLE_SQL,
    },
    bail_parse_error,
    schema::BTreeTable,
    storage::pager::CreateBTreeFlags,
    translate::{
        emitter::Resolver,
        schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID},
    },
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral},
    },
    Result, MAIN_DB_ID,
};

pub fn translate_create_role(
    role_name: &ast::Name,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let role = normalize_ident(role_name.as_str());
    if role == "public" {
        bail_parse_error!("role name \"public\" is reserved");
    }
    if catalog(resolver).has_role(&role) {
        bail_parse_error!("role \"{role}\" already exists");
    }
    let rows = AccessControlRows::open(resolver, program)?;
    rows.emit_insert(program, &["role", &role, "", "", ""]);
    emit_catalog_update(program, resolver, AccessControlChange::CreateRole(role));
    Ok(())
}

pub fn translate_drop_role(
    role_name: &ast::Name,
    if_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let role = normalize_ident(role_name.as_str());
    if !catalog(resolver).has_role(&role) {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!("role \"{role}\" does not exist");
    }
    let rows = AccessControlRows::open(resolver, program)?;
    rows.emit_delete(program, &[("role", 0), (&role, 1)]);
    emit_catalog_update(program, resolver, AccessControlChange::DropRole(role));
    Ok(())
}

fn catalog(resolver: &Resolver) -> Arc<AccessControlCatalog> {
    resolver.with_schema(MAIN_DB_ID, |schema| schema.access_control.clone())
}

/// Bumps the schema cookie and then applies `change` to the in-memory catalog.
/// The cookie goes first because writing it is what fails when the
/// transaction cannot change the schema, and a failed statement must not
/// leave the in-memory catalog changed.
fn emit_catalog_update(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    change: AccessControlChange,
) {
    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });
    program.emit_insn(Insn::UpdateAccessControl {
        db: MAIN_DB_ID,
        change: Box::new(change),
    });
}

/// Write cursor on `__turso_internal_access_control`, which is created on
/// first use.
struct AccessControlRows {
    cursor_id: usize,
}

impl AccessControlRows {
    fn open(resolver: &Resolver, program: &mut ProgramBuilder) -> Result<Self> {
        let (table, root_page) = match resolver.schema().get_btree_table(ACCESS_CONTROL_TABLE_NAME)
        {
            Some(table) => {
                let root_page = RegisterOrLiteral::Literal(table.root_page);
                (table, root_page)
            }
            None => Self::emit_create_table(resolver, program)?,
        };
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page,
            db: MAIN_DB_ID,
        });
        Ok(Self { cursor_id })
    }

    fn emit_create_table(
        resolver: &Resolver,
        program: &mut ProgramBuilder,
    ) -> Result<(Arc<BTreeTable>, RegisterOrLiteral<i64>)> {
        let root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: MAIN_DB_ID,
            root: root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let schema_table = resolver
            .schema()
            .get_btree_table(SQLITE_TABLEID)
            .expect("sqlite_schema exists");
        let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: schema_cursor_id,
            root_page: 1i64.into(),
            db: MAIN_DB_ID,
        });
        emit_schema_entry(
            program,
            resolver,
            schema_cursor_id,
            None,
            SchemaEntryType::Table,
            ACCESS_CONTROL_TABLE_NAME,
            ACCESS_CONTROL_TABLE_NAME,
            root_reg,
            Some(ACCESS_CONTROL_TABLE_SQL.to_string()),
        )?;
        program.emit_insn(Insn::ParseSchema {
            db: schema_cursor_id,
            where_clause: Some(format!(
                "tbl_name = '{ACCESS_CONTROL_TABLE_NAME}' AND type != 'trigger'"
            )),
            trigger_target_database_id: None,
        });
        let table = Arc::new(BTreeTable::from_sql(ACCESS_CONTROL_TABLE_SQL, 0)?);
        Ok((table, RegisterOrLiteral::Register(root_reg)))
    }

    fn emit_insert(&self, program: &mut ProgramBuilder, values: &[&str]) {
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::NewRowid {
            cursor: self.cursor_id,
            rowid_reg,
            prev_largest_reg: 0,
        });
        let first_reg = program.alloc_registers(values.len());
        for (i, value) in values.iter().enumerate() {
            program.emit_insn(Insn::String8 {
                dest: first_reg + i,
                value: value.to_string(),
            });
        }
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(first_reg),
            count: to_u32(values.len()),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: self.cursor_id,
            key_reg: rowid_reg,
            record_reg,
            flag: InsertFlags::new(),
            table_name: ACCESS_CONTROL_TABLE_NAME.to_string(),
        });
    }

    /// Deletes every row whose columns equal the given `(value, column)` pairs.
    fn emit_delete(&self, program: &mut ProgramBuilder, matches: &[(&str, usize)]) {
        let done = program.allocate_label();
        let loop_start = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.cursor_id,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_start);
        let next = program.allocate_label();
        for (value, column) in matches {
            let column_reg = program.alloc_register();
            program.emit_column_or_rowid(self.cursor_id, *column, column_reg);
            let value_reg = program.emit_string8_new_reg(value.to_string());
            program.emit_insn(Insn::Ne {
                lhs: column_reg,
                rhs: value_reg,
                target_pc: next,
                flags: CmpInsFlags::default(),
                collation: None,
            });
        }
        program.emit_insn(Insn::Delete {
            cursor_id: self.cursor_id,
            table_name: ACCESS_CONTROL_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });
        program.preassign_label_to_next_insn(next);
        program.emit_insn(Insn::Next {
            cursor_id: self.cursor_id,
            pc_if_next: loop_start,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(done);
    }
}
