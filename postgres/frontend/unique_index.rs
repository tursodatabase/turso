//! Bytecode for a catalog table of the frontend that has a unique index:
//! opening both for writing, and adding a row's key to the index.
//!
//! The frontend writes its catalog tables with bytecode of its own, which
//! does not maintain their indexes as an INSERT does, so each insert adds
//! the key itself. The in-memory catalogs already reject a duplicate they
//! know, so the index only catches one in a stored table that the catalog
//! does not know about.

use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::{
    to_u32, CursorType, IdxInsertFlags, Insn, LimboError, ProgramBuilder, RegisterOrLiteral,
    Result, MAIN_DB_ID, SQLITE_CONSTRAINT_UNIQUE,
};

/// The cursors of a catalog table and of its unique index, open for writing.
pub(crate) struct Cursors {
    pub(crate) table: usize,
    pub(crate) index: usize,
}

/// Opens table `table_name` of the main database and its index `index_name`
/// for writing. None when the database has no such table. A table without
/// the index is corrupt.
pub(crate) fn open_table_and_index(
    program: &mut ProgramBuilder,
    schema: &Schema,
    table_name: &str,
    index_name: &str,
) -> Result<Option<Cursors>> {
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, table_name) else {
        return Ok(None);
    };
    let index = schema
        .get_indices(NamespaceId::ROOT, table_name)
        .find(|index| index.name == index_name)
        .ok_or_else(|| LimboError::Corrupt(format!("{table_name} has no index {index_name}")))?;
    let root_page = table.root_page;
    let table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: table_cursor_id,
        root_page: RegisterOrLiteral::Literal(root_page),
        db: MAIN_DB_ID,
    });
    let index_cursor_id = program.alloc_cursor_index(None, index)?;
    program.emit_insn(Insn::OpenWrite {
        cursor_id: index_cursor_id,
        root_page: RegisterOrLiteral::Literal(index.root_page),
        db: MAIN_DB_ID,
    });
    Ok(Some(Cursors {
        table: table_cursor_id,
        index: index_cursor_id,
    }))
}

/// Adds the key in the `key_len` registers from `key_reg` to the unique
/// index `index_name`, open for writing on `cursor_id`. The key's last
/// register is the rowid of the row, and the ones before it are the unique
/// columns. Fails with `error` when the index already has a row with the
/// same unique columns.
pub(crate) fn emit_unique_index_insert(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    index_name: &str,
    key_reg: usize,
    key_len: usize,
    error: String,
) {
    let no_conflict = program.allocate_label();
    program.emit_insn(Insn::NoConflict {
        cursor_id,
        target_pc: no_conflict,
        record_reg: key_reg,
        num_regs: key_len - 1,
    });
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: SQLITE_CONSTRAINT_UNIQUE,
        description: error,
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(no_conflict);
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(key_reg),
        count: to_u32(key_len),
        dest_reg: to_u32(record_reg),
        index_name: Some(index_name.to_string()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id,
        record_reg,
        unpacked_start: Some(key_reg),
        unpacked_count: Some(to_u32(key_len)),
        flags: IdxInsertFlags::new(),
    });
}
