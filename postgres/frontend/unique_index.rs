//! Bytecode that adds a row's key to a unique index of one of the
//! frontend's catalog tables.
//!
//! The frontend writes its catalog tables with bytecode of its own, which
//! does not maintain their indexes as an INSERT does, so each insert adds
//! the key itself. The in-memory catalogs already reject a duplicate they
//! know, so the index only catches one in a stored table that the catalog
//! does not know about.

use turso_core::{to_u32, IdxInsertFlags, Insn, ProgramBuilder, SQLITE_CONSTRAINT_UNIQUE};

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
