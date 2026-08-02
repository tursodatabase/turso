//! Foreign-key probes driven only by identities and positions frozen in HIR.

use std::{fmt, num::NonZeroUsize};

use crate::{
    schema::{BTreeTable, Index, Table},
    sync::Arc,
    translate::semantic::hir::ResolvedForeignKey,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
    },
};

use super::{RegisterId, RegisterRange};

#[derive(Debug)]
pub(crate) enum PhysicalForeignKeyError {
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalForeignKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "invalid frozen foreign key: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "foreign key is not emitted yet: {message}")
            }
        }
    }
}

impl std::error::Error for PhysicalForeignKeyError {}

type ForeignKeyResult<T> = std::result::Result<T, PhysicalForeignKeyError>;

pub(crate) fn emit_insert_child_checks(
    program: &mut ProgramBuilder,
    foreign_keys: &[ResolvedForeignKey],
    child_table: &BTreeTable,
    columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys {
        emit_new_child_check(program, foreign_key, child_table, columns, rowid)?;
    }
    Ok(())
}

pub(crate) fn emit_insert_parent_repairs(
    program: &mut ProgramBuilder,
    foreign_keys: &[ResolvedForeignKey],
    parent_table: &BTreeTable,
    columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.declaration.deferred)
    {
        emit_new_parent_repair(program, foreign_key, parent_table, columns, rowid)?;
    }
    Ok(())
}

pub(crate) fn emit_update_child_checks(
    program: &mut ProgramBuilder,
    foreign_keys: &[ResolvedForeignKey],
    child_table: &BTreeTable,
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys {
        if foreign_key.declaration.deferred {
            emit_old_child_repair(program, foreign_key, child_table, old_columns, rowid)?;
        }
        emit_new_child_check(program, foreign_key, child_table, new_columns, rowid)?;
    }
    Ok(())
}

fn emit_old_child_repair(
    program: &mut ProgramBuilder,
    foreign_key: &ResolvedForeignKey,
    child_table: &BTreeTable,
    columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    let Table::BTree(parent_table) = foreign_key.parent_table.value() else {
        return Err(PhysicalForeignKeyError::Unsupported(
            "non-B-tree parent table",
        ));
    };
    let database = foreign_key
        .parent_table
        .database()
        .ok_or(PhysicalForeignKeyError::Invalid(
            "parent table has no database identity",
        ))?
        .index();
    let complete = program.allocate_label();
    let missing = program.allocate_label();
    let key = program.alloc_registers(foreign_key.child_positions.len());
    for (offset, position) in foreign_key.child_positions.iter().copied().enumerate() {
        let source = child_register(child_table, columns, rowid, position)?;
        program.emit_insn(Insn::Copy {
            src_reg: source,
            dst_reg: key + offset,
            extra_amount: 0,
        });
        program.emit_insn(Insn::IsNull {
            reg: key + offset,
            target_pc: complete,
        });
    }
    if foreign_key.parent_uses_rowid {
        let cursor = open_parent_table(program, parent_table, database);
        program.emit_insn(Insn::MustBeInt {
            reg: key,
            target_pc: Some(missing),
        });
        program.emit_insn(Insn::NotExists {
            cursor,
            rowid_reg: key,
            target_pc: missing,
        });
        program.emit_insn(Insn::Close { cursor_id: cursor });
        program.emit_insn(Insn::Goto {
            target_pc: complete,
        });
        program.preassign_label_to_next_insn(missing);
        program.emit_insn(Insn::Close { cursor_id: cursor });
    } else {
        let resolved_index =
            foreign_key
                .parent_unique_index
                .as_ref()
                .ok_or(PhysicalForeignKeyError::Invalid(
                    "parent unique index is missing",
                ))?;
        let index = resolved_index.handle();
        if let Some(count) = NonZeroUsize::new(index.columns.len()) {
            program.emit_insn(Insn::Affinity {
                start_reg: key,
                count,
                affinities: index_affinities(&index, parent_table),
            });
        }
        let cursor = open_parent_index(program, index, database);
        let found = program.allocate_label();
        program.emit_insn(Insn::Found {
            cursor_id: cursor,
            target_pc: found,
            record_reg: key,
            num_regs: foreign_key.child_positions.len(),
        });
        program.emit_insn(Insn::Close { cursor_id: cursor });
        program.emit_insn(Insn::Goto { target_pc: missing });
        program.preassign_label_to_next_insn(found);
        program.emit_insn(Insn::Close { cursor_id: cursor });
        program.emit_insn(Insn::Goto {
            target_pc: complete,
        });
        program.preassign_label_to_next_insn(missing);
    }
    program.emit_insn(Insn::FkIfZero {
        deferred: true,
        target_pc: complete,
    });
    program.emit_insn(Insn::FkCounter {
        increment_value: -1,
        deferred: true,
    });
    program.preassign_label_to_next_insn(complete);
    Ok(())
}

fn emit_new_parent_repair(
    program: &mut ProgramBuilder,
    foreign_key: &ResolvedForeignKey,
    parent_table: &BTreeTable,
    columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    let Table::BTree(child_table) = foreign_key.child_table.value() else {
        return Err(PhysicalForeignKeyError::Unsupported(
            "non-B-tree child table",
        ));
    };
    let database = foreign_key
        .child_table
        .database()
        .ok_or(PhysicalForeignKeyError::Invalid(
            "child table has no database identity",
        ))?
        .index();
    let key = program.alloc_registers(foreign_key.parent_positions.len());
    let complete = program.allocate_label();
    for (offset, position) in foreign_key.parent_positions.iter().copied().enumerate() {
        let source = child_register(parent_table, columns, rowid, position)?;
        program.emit_insn(Insn::Copy {
            src_reg: source,
            dst_reg: key + offset,
            extra_amount: 0,
        });
        program.emit_insn(Insn::IsNull {
            reg: key + offset,
            target_pc: complete,
        });
    }

    let cursor = open_parent_table(program, child_table, database);
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let scan_done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: scan_done,
    });
    program.preassign_label_to_next_insn(loop_start);
    for (offset, position) in foreign_key.child_positions.iter().copied().enumerate() {
        let value = program.alloc_register();
        program.emit_column_or_rowid(cursor, position, value);
        program.emit_insn(Insn::IsNull {
            reg: value,
            target_pc: next,
        });
        program.emit_insn(Insn::Ne {
            lhs: value,
            rhs: key + offset,
            target_pc: next,
            flags: CmpInsFlags::default().jump_if_null(),
            collation: None,
        });
    }
    program.emit_insn(Insn::FkIfZero {
        deferred: true,
        target_pc: next,
    });
    program.emit_insn(Insn::FkCounter {
        increment_value: -1,
        deferred: true,
    });
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(scan_done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    program.preassign_label_to_next_insn(complete);
    Ok(())
}

fn emit_new_child_check(
    program: &mut ProgramBuilder,
    foreign_key: &ResolvedForeignKey,
    child_table: &BTreeTable,
    columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    if foreign_key.child_positions.len() != foreign_key.parent_positions.len()
        || foreign_key.child_positions.is_empty()
    {
        return Err(PhysicalForeignKeyError::Invalid(
            "child and parent key widths differ",
        ));
    }
    let Table::BTree(parent_table) = foreign_key.parent_table.value() else {
        return Err(PhysicalForeignKeyError::Unsupported(
            "non-B-tree parent table",
        ));
    };
    let database = foreign_key
        .parent_table
        .database()
        .ok_or(PhysicalForeignKeyError::Invalid(
            "parent table has no database identity",
        ))?
        .index();
    let complete = program.allocate_label();
    let key = program.alloc_registers(foreign_key.child_positions.len());
    for (offset, position) in foreign_key.child_positions.iter().copied().enumerate() {
        let source = child_register(child_table, columns, rowid, position)?;
        program.emit_insn(Insn::Copy {
            src_reg: source,
            dst_reg: key + offset,
            extra_amount: 0,
        });
        program.emit_insn(Insn::IsNull {
            reg: key + offset,
            target_pc: complete,
        });
    }

    if same_table(foreign_key) {
        let different = program.allocate_label();
        for (offset, position) in foreign_key.parent_positions.iter().copied().enumerate() {
            let parent = child_register(child_table, columns, rowid, position)?;
            program.emit_insn(Insn::Ne {
                lhs: key + offset,
                rhs: parent,
                target_pc: different,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: None,
            });
        }
        program.emit_insn(Insn::Goto {
            target_pc: complete,
        });
        program.preassign_label_to_next_insn(different);
    }

    if foreign_key.parent_uses_rowid {
        if foreign_key.child_positions.len() != 1 {
            return Err(PhysicalForeignKeyError::Invalid(
                "rowid parent key is not scalar",
            ));
        }
        let cursor = open_parent_table(program, parent_table, database);
        let missing = program.allocate_label();
        program.emit_insn(Insn::MustBeInt {
            reg: key,
            target_pc: Some(missing),
        });
        program.emit_insn(Insn::NotExists {
            cursor,
            rowid_reg: key,
            target_pc: missing,
        });
        program.emit_insn(Insn::Close { cursor_id: cursor });
        program.emit_insn(Insn::Goto {
            target_pc: complete,
        });
        program.preassign_label_to_next_insn(missing);
        program.emit_insn(Insn::Close { cursor_id: cursor });
    } else {
        let resolved_index =
            foreign_key
                .parent_unique_index
                .as_ref()
                .ok_or(PhysicalForeignKeyError::Invalid(
                    "parent unique index is missing",
                ))?;
        let index = resolved_index.handle();
        if index.columns.len() != foreign_key.child_positions.len() {
            return Err(PhysicalForeignKeyError::Invalid(
                "parent unique index width differs from the child key",
            ));
        }
        if let Some(count) = NonZeroUsize::new(index.columns.len()) {
            program.emit_insn(Insn::Affinity {
                start_reg: key,
                count,
                affinities: index_affinities(&index, parent_table),
            });
        }
        let cursor = open_parent_index(program, index, database);
        let found = program.allocate_label();
        program.emit_insn(Insn::Found {
            cursor_id: cursor,
            target_pc: found,
            record_reg: key,
            num_regs: foreign_key.child_positions.len(),
        });
        program.emit_insn(Insn::Close { cursor_id: cursor });
        let missing = program.allocate_label();
        program.emit_insn(Insn::Goto { target_pc: missing });
        program.preassign_label_to_next_insn(found);
        program.emit_insn(Insn::Close { cursor_id: cursor });
        program.emit_insn(Insn::Goto {
            target_pc: complete,
        });
        program.preassign_label_to_next_insn(missing);
    }
    program.emit_insn(Insn::FkCounter {
        increment_value: 1,
        deferred: foreign_key.declaration.deferred,
    });
    program.preassign_label_to_next_insn(complete);
    Ok(())
}

fn child_register(
    table: &BTreeTable,
    columns: RegisterRange,
    rowid: RegisterId,
    position: usize,
) -> ForeignKeyResult<usize> {
    let column = table
        .columns()
        .get(position)
        .ok_or(PhysicalForeignKeyError::Invalid(
            "child key position is outside the row",
        ))?;
    if column.is_rowid_alias() {
        Ok(rowid.0)
    } else {
        columns.register(position).map(|register| register.0).ok_or(
            PhysicalForeignKeyError::Invalid("child key register is outside the row"),
        )
    }
}

fn same_table(foreign_key: &ResolvedForeignKey) -> bool {
    foreign_key.child_table.id() == foreign_key.parent_table.id()
        && foreign_key.child_table.database() == foreign_key.parent_table.database()
}

fn open_parent_table(program: &mut ProgramBuilder, table: &Arc<BTreeTable>, db: usize) -> usize {
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: cursor,
        root_page: table.root_page,
        db,
    });
    cursor
}

fn open_parent_index(program: &mut ProgramBuilder, index: Arc<Index>, db: usize) -> usize {
    let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: cursor,
        root_page: index.root_page,
        db,
    });
    cursor
}

fn index_affinities(index: &Index, table: &BTreeTable) -> String {
    index
        .columns
        .iter()
        .map(|column| {
            table.columns()[column.pos_in_table]
                .affinity_with_strict(table.is_strict)
                .aff_mask()
        })
        .collect()
}
