//! Catalog-free CDC emission from facts frozen in HIR.

use crate::{
    function::{Func, FuncCtx, ScalarFunc},
    schema::{BTreeTable, Table},
    translate::semantic::hir::{CdcPlan, HirDocument, SequenceOperationKind},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, InsertFlags, Insn, RegisterOrLiteral},
    },
    CdcVersion, LimboError, Result, MAIN_DB_ID,
};

use super::{RegisterId, RegisterRange};

#[derive(Clone, Copy, Debug)]
pub(crate) enum CdcChange {
    Insert,
    Update,
    Delete,
}

impl CdcChange {
    const fn code(self) -> i64 {
        match self {
            Self::Insert => 1,
            Self::Update => 0,
            Self::Delete => -1,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedCdc<'document> {
    plan: &'document CdcPlan,
    cursor: usize,
}

impl<'document> PreparedCdc<'document> {
    pub(crate) fn open(
        program: &mut ProgramBuilder,
        document: &'document HirDocument,
    ) -> Result<Option<Self>> {
        let Some(plan) = document.cdc.as_ref() else {
            return Ok(None);
        };
        let Table::BTree(table) = plan.table.value() else {
            return Err(LimboError::InternalError(
                "frozen CDC target is not a B-tree table".to_string(),
            ));
        };
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: RegisterOrLiteral::Literal(table.root_page),
            db: MAIN_DB_ID,
        });
        Ok(Some(Self { plan, cursor }))
    }

    pub(crate) fn has_before(self) -> bool {
        self.plan.info.has_before()
    }

    pub(crate) fn has_after(self) -> bool {
        self.plan.info.has_after()
    }

    pub(crate) fn has_updates(self) -> bool {
        self.plan.info.has_updates()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn emit_change(
        self,
        program: &mut ProgramBuilder,
        change: CdcChange,
        rowid: RegisterId,
        before: Option<usize>,
        after: Option<usize>,
        updates: Option<usize>,
        table_name: &str,
    ) -> Result<()> {
        match self.plan.info.cdc_version() {
            CdcVersion::V1 => {
                self.emit_v1(program, change, rowid, before, after, updates, table_name)
            }
            CdcVersion::V2 => {
                self.emit_v2(program, change, rowid, before, after, updates, table_name)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_v1(
        self,
        program: &mut ProgramBuilder,
        change: CdcChange,
        rowid: RegisterId,
        before: Option<usize>,
        after: Option<usize>,
        updates: Option<usize>,
        table_name: &str,
    ) -> Result<()> {
        let fields = program.alloc_registers(8);
        program.emit_null(fields, None);
        emit_unixepoch(program, fields + 1);
        program.emit_int(change.code(), fields + 2);
        program.emit_string8(table_name.to_string(), fields + 3);
        program.emit_insn(Insn::Copy {
            src_reg: rowid.0,
            dst_reg: fields + 4,
            extra_amount: 0,
        });
        copy_or_null(program, before, fields + 5);
        copy_or_null(program, after, fields + 6);
        copy_or_null(program, updates, fields + 7);
        let change_id = program.alloc_register();
        program.emit_insn(Insn::NewRowid {
            cursor: self.cursor,
            rowid_reg: change_id,
            prev_largest_reg: 0,
        });
        emit_cdc_insert(program, self.cursor, fields, 8, change_id);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_v2(
        self,
        program: &mut ProgramBuilder,
        change: CdcChange,
        rowid: RegisterId,
        before: Option<usize>,
        after: Option<usize>,
        updates: Option<usize>,
        table_name: &str,
    ) -> Result<()> {
        let fields = program.alloc_registers(9);
        program.emit_null(fields, None);
        emit_unixepoch(program, fields + 1);
        let change_id = program.alloc_register();
        self.emit_change_id(program, change_id)?;
        emit_conn_txn_id(program, change_id, fields + 2);
        program.emit_int(change.code(), fields + 3);
        program.emit_string8(table_name.to_string(), fields + 4);
        program.emit_insn(Insn::Copy {
            src_reg: rowid.0,
            dst_reg: fields + 5,
            extra_amount: 0,
        });
        copy_or_null(program, before, fields + 6);
        copy_or_null(program, after, fields + 7);
        copy_or_null(program, updates, fields + 8);
        emit_cdc_insert(program, self.cursor, fields, 9, change_id);
        Ok(())
    }

    fn emit_change_id(self, program: &mut ProgramBuilder, destination: usize) -> Result<()> {
        if !program.is_mvcc_enabled() {
            program.emit_insn(Insn::NewRowid {
                cursor: self.cursor,
                rowid_reg: destination,
                prev_largest_reg: 0,
            });
            return Ok(());
        }
        let operation = self.plan.sequence.as_ref().ok_or_else(|| {
            LimboError::InternalError("frozen MVCC CDC plan has no sequence".to_string())
        })?;
        if operation.kind != SequenceOperationKind::NextValue {
            return Err(LimboError::InternalError(
                "frozen CDC sequence is not a next-value operation".to_string(),
            ));
        }
        let Table::BTree(backing_table) = operation.backing_table.value() else {
            return Err(LimboError::InternalError(
                "frozen CDC sequence backing object is not a B-tree table".to_string(),
            ));
        };
        let sqlite_sequence = operation
            .sqlite_sequence
            .as_ref()
            .map(|table| match table.value() {
                Table::BTree(table) => Ok(table.clone()),
                _ => Err(LimboError::InternalError(
                    "frozen CDC sqlite_sequence object is not a B-tree table".to_string(),
                )),
            })
            .transpose()?;
        crate::translate::sequence::emit_disk_read_nextval_from_resolved(
            program,
            operation.database.index(),
            &operation.normalized_name,
            &operation.sequence,
            backing_table.clone(),
            sqlite_sequence,
            destination,
            None,
        )
    }

    pub(crate) fn emit_autocommit_commit(self, program: &mut ProgramBuilder) -> Result<()> {
        if program.flags.is_subprogram() || !self.plan.info.cdc_version().has_commit_record() {
            return Ok(());
        }
        let is_autocommit = program.alloc_register();
        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: 0,
            dest: is_autocommit,
            func: FuncCtx {
                func: Func::Scalar(ScalarFunc::IsAutocommit),
                arg_count: 0,
            },
        });
        let skip = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: is_autocommit,
            target_pc: skip,
            jump_if_null: true,
        });
        self.emit_commit(program)?;
        program.preassign_label_to_next_insn(skip);
        Ok(())
    }

    fn emit_commit(self, program: &mut ProgramBuilder) -> Result<()> {
        let fields = program.alloc_registers(9);
        program.emit_null(fields, None);
        emit_unixepoch(program, fields + 1);
        let minus_one = program.alloc_register();
        program.emit_int(-1, minus_one);
        emit_conn_txn_id(program, minus_one, fields + 2);
        program.emit_int(2, fields + 3);
        program.emit_insn(Insn::Null {
            dest: fields + 4,
            dest_end: Some(fields + 8),
        });
        let change_id = program.alloc_register();
        self.emit_change_id(program, change_id)?;
        emit_cdc_insert(program, self.cursor, fields, 9, change_id);
        Ok(())
    }

    pub(crate) fn close(self, program: &mut ProgramBuilder) {
        program.emit_insn(Insn::Close {
            cursor_id: self.cursor,
        });
    }
}

pub(crate) fn record_from_registers(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    logical: RegisterRange,
    rowid: RegisterId,
) -> usize {
    let stored = table
        .columns()
        .iter()
        .filter(|column| !column.is_virtual_generated())
        .count();
    let values = program.alloc_registers(stored);
    let mut output = 0;
    for (position, column) in table.columns().iter().enumerate() {
        if column.is_virtual_generated() {
            continue;
        }
        let source = if column.is_rowid_alias() {
            rowid.0
        } else {
            logical.first.0 + position
        };
        program.emit_insn(Insn::Copy {
            src_reg: source,
            dst_reg: values + output,
            extra_amount: 0,
        });
        output += 1;
    }
    make_record(program, table, values, stored)
}

pub(crate) fn record_from_cursor(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    cursor: usize,
    rowid: RegisterId,
) -> usize {
    let stored = table
        .columns()
        .iter()
        .filter(|column| !column.is_virtual_generated())
        .count();
    let values = program.alloc_registers(stored);
    let mut output = 0;
    for (position, column) in table.columns().iter().enumerate() {
        if column.is_virtual_generated() {
            continue;
        }
        if column.is_rowid_alias() {
            program.emit_insn(Insn::Copy {
                src_reg: rowid.0,
                dst_reg: values + output,
                extra_amount: 0,
            });
        } else {
            program.emit_column_or_rowid(cursor, position, values + output);
        }
        output += 1;
    }
    make_record(program, table, values, stored)
}

pub(crate) fn update_record(
    program: &mut ProgramBuilder,
    width: usize,
    assignments: &[crate::translate::semantic::hir::Assignment],
    logical: RegisterRange,
    value_override: Option<(usize, &str)>,
) -> usize {
    let fields = program.alloc_registers(width * 2);
    for position in 0..width {
        program.emit_bool(false, fields + position);
        program.emit_null(fields + width + position, None);
    }
    for assignment in assignments {
        for target in &assignment.columns {
            let crate::translate::semantic::hir::TargetColumn::Column(position) = target else {
                continue;
            };
            program.emit_bool(true, fields + position);
            if let Some((_, value)) =
                value_override.filter(|(override_position, _)| override_position == position)
            {
                program.emit_string8(value.to_string(), fields + width + position);
            } else {
                program.emit_insn(Insn::Copy {
                    src_reg: logical.first.0 + position,
                    dst_reg: fields + width + position,
                    extra_amount: 0,
                });
            }
        }
    }
    let record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(fields),
        count: to_u32(width * 2),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: None,
    });
    record
}

fn make_record(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    values: usize,
    stored: usize,
) -> usize {
    let record = program.alloc_register();
    let affinity_str = table
        .columns()
        .iter()
        .filter(|column| !column.is_virtual_generated())
        .map(|column| column.affinity_with_strict(table.is_strict).aff_mask())
        .collect::<String>();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(values),
        count: to_u32(stored),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    record
}

fn emit_unixepoch(program: &mut ProgramBuilder, destination: usize) {
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: destination,
        func: FuncCtx {
            func: Func::Scalar(ScalarFunc::UnixEpoch),
            arg_count: 0,
        },
    });
}

fn emit_conn_txn_id(program: &mut ProgramBuilder, argument: usize, destination: usize) {
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: argument,
        dest: destination,
        func: FuncCtx {
            func: Func::Scalar(ScalarFunc::ConnTxnId),
            arg_count: 1,
        },
    });
}

fn copy_or_null(program: &mut ProgramBuilder, source: Option<usize>, destination: usize) {
    if let Some(source) = source {
        program.emit_insn(Insn::Copy {
            src_reg: source,
            dst_reg: destination,
            extra_amount: 0,
        });
    } else {
        program.emit_null(destination, None);
    }
}

fn emit_cdc_insert(
    program: &mut ProgramBuilder,
    cursor: usize,
    fields: usize,
    count: usize,
    change_id: usize,
) {
    let record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(fields),
        count: to_u32(count),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor,
        key_reg: change_id,
        record_reg: record,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: String::new(),
    });
}
