//! VDBE bytecode generation for `PRAGMA foreign_key_check`.

use super::fkeys::{
    copy_with_affinity, emit_skip_if_any_null, index_probe, open_read_index, open_read_table,
};
use crate::{
    schema::{BTreeTable, ResolvedFkRef, Schema},
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Result,
};

/// Translate `PRAGMA foreign_key_check` and its single-table form
/// `PRAGMA foreign_key_check(table-name)`.
///
/// For every table with foreign keys (or just `table_name`, when given), scans
/// every row and emits one result row — `(table, rowid, parent, fkid)` — for
/// each child row whose foreign key value has no matching row in the
/// referenced parent table. Rows with a NULL foreign key column are not
/// checked, matching SQLite's behavior of treating NULL as "no reference".
///
/// `fkid` is numbered the same way `PRAGMA foreign_key_list` numbers its `id`
/// column (reverse declaration order), so the two pragmas agree on which
/// constraint a row refers to.
pub fn translate_foreign_key_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    database_id: usize,
    table_name: Option<&str>,
    base_reg: usize,
) -> Result<()> {
    let tables: Vec<_> = match table_name {
        Some(name) => schema.get_btree_table(name).into_iter().collect(),
        None => schema.tables.values().filter_map(|t| t.btree()).collect(),
    };

    for child_tbl in &tables {
        let mut fks = schema.resolved_fks_for_child(&child_tbl.name)?;
        if fks.is_empty() {
            continue;
        }
        fks.sort_by_key(|r| std::cmp::Reverse(r.fk.decl_order));

        let ccur = open_read_table(program, child_tbl, database_id);
        let table_done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: ccur,
            pc_if_empty: table_done,
        });

        let loop_top = program.allocate_label();
        program.preassign_label_to_next_insn(loop_top);

        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: ccur,
            dest: rowid_reg,
        });

        for (fkid, fk_ref) in fks.iter().enumerate() {
            emit_fk_row_check(
                program,
                schema,
                database_id,
                child_tbl,
                fk_ref,
                fkid,
                ccur,
                rowid_reg,
                base_reg,
            )?;
        }

        program.emit_insn(Insn::Next {
            cursor_id: ccur,
            pc_if_next: loop_top,
        });
        program.preassign_label_to_next_insn(table_done);
        program.emit_insn(Insn::Close { cursor_id: ccur });
    }

    Ok(())
}

/// Check a single foreign key constraint against the row currently under the
/// child cursor, emitting a `(table, rowid, parent, fkid)` result row if the
/// referenced parent key is missing.
#[allow(clippy::too_many_arguments)]
fn emit_fk_row_check(
    program: &mut ProgramBuilder,
    schema: &Schema,
    database_id: usize,
    child_tbl: &BTreeTable,
    fk_ref: &ResolvedFkRef,
    fkid: usize,
    child_cursor_id: usize,
    rowid_reg: usize,
    base_reg: usize,
) -> Result<()> {
    let ncols = fk_ref.child_pos.len();
    let key_start = program.alloc_registers(ncols);
    for (i, &pos) in fk_ref.child_pos.iter().enumerate() {
        program.emit_column_or_rowid(child_cursor_id, pos, key_start + i);
    }

    let skip_fk = program.allocate_label();
    emit_skip_if_any_null(program, key_start, ncols, skip_fk);

    let emit_violation = |p: &mut ProgramBuilder| -> Result<()> {
        p.emit_string8(child_tbl.name.clone(), base_reg);
        p.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: base_reg + 1,
            extra_amount: 0,
        });
        p.emit_string8(fk_ref.fk.parent_table.clone(), base_reg + 2);
        p.emit_int(fkid as i64, base_reg + 3);
        p.emit_result_row(base_reg, 4);
        Ok(())
    };

    if fk_ref.parent_uses_rowid {
        let parent_tbl = schema
            .get_btree_table(&fk_ref.fk.parent_table)
            .expect("resolved_fks_for_child guarantees the parent table exists");
        let pcur = open_read_table(program, &parent_tbl, database_id);

        let missing = program.allocate_label();
        program.emit_insn(Insn::MustBeInt {
            reg: key_start,
            target_pc: Some(missing),
        });
        program.emit_insn(Insn::NotExists {
            cursor: pcur,
            rowid_reg: key_start,
            target_pc: missing,
        });

        let found = program.allocate_label();
        program.emit_insn(Insn::Close { cursor_id: pcur });
        program.emit_insn(Insn::Goto { target_pc: found });

        program.preassign_label_to_next_insn(missing);
        program.emit_insn(Insn::Close { cursor_id: pcur });
        emit_violation(program)?;

        program.preassign_label_to_next_insn(found);
    } else {
        let idx = fk_ref
            .parent_unique_index
            .as_ref()
            .expect("resolved_fks_for_child guarantees a unique index for non-rowid parents");
        let parent_tbl = schema
            .get_btree_table(&fk_ref.fk.parent_table)
            .expect("resolved_fks_for_child guarantees the parent table exists");

        let icur = open_read_index(program, idx, database_id);
        let probe = copy_with_affinity(program, key_start, ncols, idx, &parent_tbl);
        index_probe(program, icur, probe, ncols, |_p| Ok(()), emit_violation)?;
    }

    program.preassign_label_to_next_insn(skip_fk);
    Ok(())
}
