//! Bytecode that writes rows of `sqlite_schema`.

use crate::namespace::NamespaceId;
use crate::schema::BTreeTable;
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::emitter::{
    emit_cdc_autocommit_commit, emit_cdc_insns, OperationMode, Resolver,
};
use crate::translate::schema::{emit_schema_cookie, SQLITE_TABLEID};
use crate::translate::ProgramBuilder;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{to_u32, InsertFlags, Insn, RegisterOrLiteral};
use crate::{CaptureDataChangesExt, Result, MAIN_DB_ID};

/// Creates one of the engine's own tables in the main database the first
/// time it is used: a new B-tree, its row in `sqlite_schema`, and the table
/// in the in-memory schema. Returns the table and the root page that the new
/// B-tree will have.
///
/// `SetCookie` is what marks the transaction as having changed the schema,
/// so that a rollback restores the connection schema (see
/// `Pager::rollback`). It has to precede `ParseSchema`: if a later
/// instruction fails, the schema would otherwise keep the table while the
/// rollback removed its page.
pub(crate) fn emit_create_internal_table(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_name: &str,
    create_sql: &str,
) -> Result<InternalTable> {
    let root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: MAIN_DB_ID,
        root: root_reg,
        flags: CreateBTreeFlags::new_table(),
    });
    let schema_table = resolver.with_schema(MAIN_DB_ID, |s| {
        s.get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
            .expect("every database has sqlite_schema")
    });
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
        table_name,
        table_name,
        root_reg,
        Some(create_sql.to_string()),
    )?;
    let table = Arc::new(BTreeTable::from_sql(create_sql, 0)?);
    emit_schema_cookie(resolver, program);
    program.emit_insn(Insn::ParseSchema {
        db: MAIN_DB_ID,
        where_clause: Some(format!("tbl_name = '{table_name}' AND type != 'trigger'")),
        trigger_target_database_id: None,
    });
    Ok(InternalTable {
        table,
        root_page: RegisterOrLiteral::Register(root_reg),
    })
}

/// A table that [`emit_create_internal_table`] creates.
pub(crate) struct InternalTable {
    pub(crate) table: Arc<BTreeTable>,
    pub(crate) root_page: RegisterOrLiteral<i64>,
}

#[allow(clippy::too_many_arguments)]
pub fn emit_schema_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    sqlite_schema_cursor_id: usize,
    cdc_table_cursor_id: Option<usize>,
    entry_type: SchemaEntryType,
    name: &str,
    tbl_name: &str,
    root_page_reg: usize,
    sql: Option<String>,
) -> Result<()> {
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sqlite_schema_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let type_reg = program.emit_string8_new_reg(entry_type.as_str().to_string());
    program.emit_string8_new_reg(name.to_string());
    program.emit_string8_new_reg(tbl_name.to_string());

    let table_root_reg = program.alloc_register();
    if root_page_reg == 0 {
        program.emit_insn(Insn::Integer {
            dest: table_root_reg,
            value: 0, // virtual tables in sqlite always have rootpage=0
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: root_page_reg,
            dst_reg: table_root_reg,
            extra_amount: 0,
        });
    }

    let sql_reg = program.alloc_register();
    if let Some(sql) = sql {
        program.emit_string8(sql, sql_reg);
    } else {
        program.emit_null(sql_reg, None);
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(type_reg),
        count: to_u32(5),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: sqlite_schema_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: tbl_name.to_string(),
    });

    if let Some(cdc_table_cursor_id) = cdc_table_cursor_id {
        let after_record_reg = if program.capture_data_changes_info().has_after() {
            Some(record_reg)
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::INSERT,
            cdc_table_cursor_id,
            rowid_reg,
            None,
            after_record_reg,
            None,
            SQLITE_TABLEID,
        )?;
        emit_cdc_autocommit_commit(program, resolver, Some(cdc_table_cursor_id))?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaEntryType {
    Table,
    Index,
    View,
    Trigger,
}

impl SchemaEntryType {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Table => "table",
            SchemaEntryType::Index => "index",
            SchemaEntryType::View => "view",
            SchemaEntryType::Trigger => "trigger",
        }
    }
}
