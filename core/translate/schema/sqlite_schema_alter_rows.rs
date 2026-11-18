//! Bytecode that rewrites stored rows for `ALTER TABLE`: the rows of
//! `sqlite_schema`, and the rows of engine tables that name a user table.

use crate::namespace::NamespaceId;
use crate::sync::Arc;
use crate::translate::alter::database_uses_mvcc;
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::Resolver;
use crate::translate::schema::sqlite_schema_rows::open_sqlite_schema_for_write;
use crate::translate::schema::SQLITE_TABLEID;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, CmpInsFlags, Insn, RegisterOrLiteral};

/// Rewrite the sqlite_schema row for the AUTOINCREMENT backing table when
/// the parent table is renamed. The backing table is keyed by name
/// `__turso_internal_seq___turso_internal_autoincrement_<table>`, so its
/// row is not touched by the general `Func::AlterTable::RenameTable` sweep
/// (which only matches rows whose `tbl_name` column equals the renamed
/// user table). Without this rewrite, the persistent sqlite_schema row
/// keeps the old backing-table name and SQL, so the next process to open
/// the DB rebuilds the in-memory schema with the stale backing table —
/// and an INSERT into the renamed table fails with "missing implicit
/// sequence for AUTOINCREMENT table".
///
/// Mirrors the existing `emit_rename_sqlite_sequence_entry` helper for
/// the `sqlite_sequence` row; the in-memory rename of `schema.sequences`
/// and `schema.tables` happens in `op_rename_table`.
pub(crate) fn emit_rename_autoincrement_backing_table_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    old_table_name_norm: &str,
    new_table_name_norm: &str,
) {
    use crate::schema::autoincrement_sequence_name;
    use crate::translate::sequence::{sequence_backing_table_name, sequence_backing_table_sql};

    let old_backing_name =
        sequence_backing_table_name(&autoincrement_sequence_name(old_table_name_norm));
    let new_seq_name = autoincrement_sequence_name(new_table_name_norm);
    let new_backing_name = sequence_backing_table_name(&new_seq_name);
    let new_backing_sql = sequence_backing_table_sql(&new_seq_name);

    let cursor_id = open_sqlite_schema_for_write(program, resolver, database_id);

    let old_backing_reg = program.emit_string8_new_reg(old_backing_name);
    program.mark_last_insn_constant();
    let new_backing_reg = program.emit_string8_new_reg(new_backing_name);
    program.mark_last_insn_constant();
    let new_sql_reg = program.emit_string8_new_reg(new_backing_sql);
    program.mark_last_insn_constant();

    program.cursor_loop(cursor_id, |program, rowid| {
        let name_reg = program.alloc_register();
        program.emit_column_or_rowid(cursor_id, 1, name_reg);

        let continue_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: name_reg,
            rhs: old_backing_reg,
            target_pc: continue_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });

        // Preserve type (col 0) and rootpage (col 3); overwrite name/tbl_name
        // (cols 1, 2) and sql (col 4). The backing table is INTERNAL — its
        // sql is regenerated from `sequence_backing_table_sql(new_seq_name)`
        // and never altered by user DDL, so we don't need to AST-rewrite it.
        let rec_start = program.alloc_registers(5);
        program.emit_column_or_rowid(cursor_id, 0, rec_start); // type
        program.emit_insn(Insn::Copy {
            src_reg: new_backing_reg,
            dst_reg: rec_start + 1, // name
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new_backing_reg,
            dst_reg: rec_start + 2, // tbl_name
            extra_amount: 0,
        });
        program.emit_column_or_rowid(cursor_id, 3, rec_start + 3); // rootpage
        program.emit_insn(Insn::Copy {
            src_reg: new_sql_reg,
            dst_reg: rec_start + 4, // sql
            extra_amount: 0,
        });

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(rec_start),
            count: to_u32(5),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });

        // MVCC: end the old version before writing the new one
        // (Hekaton-style UPDATE = DELETE + INSERT), mirroring the
        // pattern used by `emit_rename_sqlite_sequence_entry` and the
        // main-table rewrite cursor_loop.
        if database_uses_mvcc(connection, database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id,
                table_name: SQLITE_TABLEID.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid,
            record_reg,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: SQLITE_TABLEID.to_string(),
        });

        program.preassign_label_to_next_insn(continue_label);
    });
}

pub(crate) fn emit_rename_sqlite_sequence_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    old_table_name_norm: &str,
    new_table_name_norm: &str,
) {
    let Some(sqlite_sequence) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(NamespaceId::ROOT, crate::schema::SQLITE_SEQUENCE_TABLE_NAME)
    }) else {
        return;
    };

    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_sequence.clone()));
    let sequence_name_reg = program.alloc_register();
    let sequence_value_reg = program.alloc_register();
    let row_name_to_replace_reg = program.emit_string8_new_reg(old_table_name_norm.to_string());
    program.mark_last_insn_constant();
    let replacement_row_name_reg = program.emit_string8_new_reg(new_table_name_norm.to_string());
    program.mark_last_insn_constant();

    let affinity_str = sqlite_sequence
        .columns()
        .iter()
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_sequence.root_page),
        db: database_id,
    });

    program.cursor_loop(seq_cursor_id, |program, rowid| {
        program.emit_column_or_rowid(seq_cursor_id, 0, sequence_name_reg);

        let continue_loop_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: sequence_name_reg,
            rhs: row_name_to_replace_reg,
            target_pc: continue_loop_label,
            flags: CmpInsFlags::default().jump_if_null(),
            collation: CollationSeq::for_internal_values(),
        });

        program.emit_column_or_rowid(seq_cursor_id, 1, sequence_value_reg);

        let record_start_reg = program.alloc_registers(2);
        let record_reg = program.alloc_register();

        program.emit_insn(Insn::Copy {
            src_reg: replacement_row_name_reg,
            dst_reg: record_start_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: sequence_value_reg,
            dst_reg: record_start_reg + 1,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(record_start_reg),
            count: to_u32(2),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: Some(affinity_str.clone()),
        });

        // In MVCC mode, we need to delete before insert to properly
        // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
        if database_uses_mvcc(connection, database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id: seq_cursor_id,
                table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: seq_cursor_id,
            key_reg: rowid,
            record_reg,
            flag: crate::translate::insert::autoincrement_bookkeeping_insert_flags(),
            table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        });

        program.preassign_label_to_next_insn(continue_loop_label);
    });
}
