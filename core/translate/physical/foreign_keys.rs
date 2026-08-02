//! Foreign-key probes driven only by identities and positions frozen in HIR.

use std::{fmt, num::NonZeroUsize};

use crate::{
    error::SQLITE_CONSTRAINT_FOREIGNKEY,
    schema::{BTreeTable, Index, Table},
    sync::Arc,
    translate::semantic::hir::{Expr, ResolvedForeignKey},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
    },
};

use super::{
    CursorId, ExpressionEmitter, ForeignKeyParentChange, PhysicalExpressionError, PreparedTriggers,
    RegisterId, RegisterRange, RuntimeBindingError, RuntimeBindings, SourceRuntime,
};

#[derive(Debug)]
pub(crate) enum PhysicalForeignKeyError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalForeignKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid frozen foreign key: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "foreign key is not emitted yet: {message}")
            }
        }
    }
}

impl std::error::Error for PhysicalForeignKeyError {}

impl From<RuntimeBindingError> for PhysicalForeignKeyError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalForeignKeyError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

type ForeignKeyResult<T> = std::result::Result<T, PhysicalForeignKeyError>;

fn bind_child_scan_cursor(
    bindings: &mut RuntimeBindings<'_>,
    source: crate::translate::semantic::hir::SourceId,
    cursor: usize,
) -> ForeignKeyResult<Option<SourceRuntime>> {
    let runtime = SourceRuntime::Cursor(CursorId(cursor));
    match bindings.source(source) {
        Ok(previous) => {
            bindings.replace_source(source, runtime)?;
            Ok(Some(previous))
        }
        Err(RuntimeBindingError::WrongScope("unbound source")) => {
            bindings.bind_source(source, runtime)?;
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn restore_child_scan_cursor(
    bindings: &mut RuntimeBindings<'_>,
    source: crate::translate::semantic::hir::SourceId,
    previous: Option<SourceRuntime>,
) -> ForeignKeyResult<()> {
    if let Some(previous) = previous {
        bindings.replace_source(source, previous)?;
    }
    Ok(())
}

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
    bindings: &mut RuntimeBindings<'_>,
    foreign_keys: &[ResolvedForeignKey],
    parent_table: &BTreeTable,
    columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.declaration.deferred)
    {
        emit_new_parent_repair(program, bindings, foreign_key, parent_table, columns, rowid)?;
    }
    Ok(())
}

pub(crate) fn emit_update_parent_repairs(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    foreign_keys: &[ResolvedForeignKey],
    parent_table: &BTreeTable,
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.declaration.deferred)
    {
        let complete = program.allocate_label();
        let changed = program.allocate_label();
        emit_key_change_branch(
            program,
            foreign_key,
            parent_table,
            &foreign_key.parent_positions,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
            changed,
            complete,
        )?;
        program.preassign_label_to_next_insn(changed);
        emit_new_parent_repair(
            program,
            bindings,
            foreign_key,
            parent_table,
            new_columns,
            new_rowid,
        )?;
        program.preassign_label_to_next_insn(complete);
    }
    Ok(())
}

pub(crate) fn emit_update_child_checks(
    program: &mut ProgramBuilder,
    foreign_keys: &[ResolvedForeignKey],
    child_table: &BTreeTable,
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys {
        let complete = program.allocate_label();
        let changed = program.allocate_label();
        emit_key_change_branch(
            program,
            foreign_key,
            child_table,
            &foreign_key.child_positions,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
            changed,
            complete,
        )?;
        program.preassign_label_to_next_insn(changed);
        if foreign_key.declaration.deferred {
            emit_old_child_repair(program, foreign_key, child_table, old_columns, old_rowid)?;
        }
        // An ON UPDATE CASCADE action assigns the NEW key supplied by the
        // parent mutation. That parent row already exists, even when a fresh
        // cursor in a self-referential action cannot observe the outer write.
        if !foreign_key.parent_action_guarantees_new_parent {
            emit_new_child_check(program, foreign_key, child_table, new_columns, new_rowid)?;
        }
        program.preassign_label_to_next_insn(complete);
    }
    Ok(())
}

pub(crate) fn emit_delete_child_repairs(
    program: &mut ProgramBuilder,
    foreign_keys: &[ResolvedForeignKey],
    child_table: &BTreeTable,
    old_columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.declaration.deferred)
    {
        emit_old_child_repair(program, foreign_key, child_table, old_columns, rowid)?;
    }
    Ok(())
}

pub(crate) fn emit_delete_parent_checks(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    foreign_keys: &[ResolvedForeignKey],
    parent_table: &BTreeTable,
    old_columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys.iter().filter(|foreign_key| {
        matches!(
            foreign_key.declaration.on_delete,
            turso_parser::ast::RefAct::NoAction | turso_parser::ast::RefAct::Restrict
        )
    }) {
        emit_delete_parent_check(
            program,
            bindings,
            foreign_key,
            parent_table,
            old_columns,
            rowid,
        )?;
    }
    Ok(())
}

pub(crate) fn emit_replace_parent_checks(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    foreign_keys: &[ResolvedForeignKey],
    parent_table: &BTreeTable,
    old_columns: RegisterRange,
    replacement_columns: RegisterRange,
    old_rowid: RegisterId,
    replacement_rowid: RegisterId,
) -> ForeignKeyResult<()> {
    use turso_parser::ast::RefAct;

    for foreign_key in foreign_keys.iter().filter(|foreign_key| {
        matches!(
            foreign_key.declaration.on_delete,
            RefAct::NoAction | RefAct::Restrict
        )
    }) {
        if !can_skip_transient_replace_check(
            foreign_key.declaration.on_delete,
            foreign_key.declaration.deferred,
        ) {
            emit_delete_parent_check(
                program,
                bindings,
                foreign_key,
                parent_table,
                old_columns,
                old_rowid,
            )?;
            continue;
        }

        // An immediate NO ACTION check may ignore the transient delete only
        // when this replacement restores the exact parent key. Deferred
        // checks instead record and later repair debt in the shared counter.
        let changed = program.allocate_label();
        let complete = program.allocate_label();
        let (old_key, replacement_key) = copy_parent_change_keys(
            program,
            parent_table,
            &foreign_key.parent_positions,
            old_columns,
            replacement_columns,
            old_rowid,
            replacement_rowid,
        )?;
        for offset in 0..foreign_key.parent_positions.len() {
            program.emit_insn(Insn::Ne {
                lhs: old_key + offset,
                rhs: replacement_key + offset,
                target_pc: changed,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: foreign_key
                    .parent_unique_index
                    .as_ref()
                    .and_then(|index| index.value().columns.get(offset))
                    .and_then(|column| column.collation),
            });
        }
        program.emit_insn(Insn::Goto {
            target_pc: complete,
        });
        program.preassign_label_to_next_insn(changed);
        emit_delete_parent_check(
            program,
            bindings,
            foreign_key,
            parent_table,
            old_columns,
            old_rowid,
        )?;
        program.preassign_label_to_next_insn(complete);
    }
    Ok(())
}

fn can_skip_transient_replace_check(action: turso_parser::ast::RefAct, deferred: bool) -> bool {
    action == turso_parser::ast::RefAct::NoAction && !deferred
}

pub(crate) fn emit_update_parent_checks(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    foreign_keys: &[ResolvedForeignKey],
    parent_table: &BTreeTable,
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
) -> ForeignKeyResult<()> {
    for foreign_key in foreign_keys.iter().filter(|foreign_key| {
        matches!(
            foreign_key.declaration.on_update,
            turso_parser::ast::RefAct::NoAction | turso_parser::ast::RefAct::Restrict
        )
    }) {
        emit_update_parent_check(
            program,
            bindings,
            foreign_key,
            parent_table,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
        )?;
    }
    Ok(())
}

pub(crate) fn emit_delete_parent_actions(
    program: &mut ProgramBuilder,
    foreign_keys: &[ResolvedForeignKey],
    parent_table: &BTreeTable,
    old_columns: RegisterRange,
    rowid: RegisterId,
    prepared: &PreparedTriggers,
) -> ForeignKeyResult<()> {
    use turso_parser::ast::RefAct;

    for foreign_key in foreign_keys.iter().filter(|foreign_key| {
        matches!(
            foreign_key.declaration.on_delete,
            RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault
        )
    }) {
        let action = prepared
            .foreign_key_action(
                foreign_key.child_table.id(),
                &foreign_key.declaration,
                ForeignKeyParentChange::Delete,
            )
            .ok_or(PhysicalForeignKeyError::Invalid(
                "mutating ON DELETE action has no prepared HIR program",
            ))?;
        let complete = program.allocate_label();
        let key = program.alloc_registers(foreign_key.parent_positions.len());
        for (offset, position) in foreign_key.parent_positions.iter().copied().enumerate() {
            let source = child_register(parent_table, old_columns, rowid, position)?;
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
        program.emit_insn(Insn::Program {
            param_registers: (key..key + foreign_key.parent_positions.len()).collect(),
            program: action.program.clone(),
            ignore_jump_target: complete,
        });
        program.preassign_label_to_next_insn(complete);
    }
    Ok(())
}

pub(crate) fn emit_update_parent_actions(
    program: &mut ProgramBuilder,
    foreign_keys: &[ResolvedForeignKey],
    parent_table: &BTreeTable,
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
    prepared: &PreparedTriggers,
) -> ForeignKeyResult<()> {
    use turso_parser::ast::RefAct;

    for foreign_key in foreign_keys.iter().filter(|foreign_key| {
        matches!(
            foreign_key.declaration.on_update,
            RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault
        )
    }) {
        let action = prepared
            .foreign_key_action(
                foreign_key.child_table.id(),
                &foreign_key.declaration,
                ForeignKeyParentChange::Update,
            )
            .ok_or(PhysicalForeignKeyError::Invalid(
                "mutating ON UPDATE action has no prepared HIR program",
            ))?;
        let complete = program.allocate_label();
        let changed = program.allocate_label();
        let width = foreign_key.parent_positions.len();
        let (old_key, new_key) = copy_parent_change_keys(
            program,
            parent_table,
            &foreign_key.parent_positions,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
        )?;
        for offset in 0..width {
            let next_equal = (offset + 1 != width).then(|| program.allocate_label());
            program.emit_insn(Insn::Eq {
                lhs: old_key + offset,
                rhs: new_key + offset,
                target_pc: next_equal.unwrap_or(complete),
                flags: CmpInsFlags::default().null_eq(),
                collation: foreign_key
                    .parent_unique_index
                    .as_ref()
                    .and_then(|index| index.value().columns.get(offset))
                    .and_then(|column| column.collation),
            });
            program.emit_insn(Insn::Goto { target_pc: changed });
            if let Some(next_equal) = next_equal {
                program.preassign_label_to_next_insn(next_equal);
            }
        }
        program.preassign_label_to_next_insn(changed);
        let mut parameters = (old_key..old_key + width).collect::<Vec<_>>();
        if foreign_key.declaration.on_update == RefAct::Cascade {
            parameters.extend(new_key..new_key + width);
        }
        program.emit_insn(Insn::Program {
            param_registers: parameters,
            program: action.program.clone(),
            ignore_jump_target: complete,
        });
        program.preassign_label_to_next_insn(complete);
    }
    Ok(())
}

fn emit_update_parent_check(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    foreign_key: &ResolvedForeignKey,
    parent_table: &BTreeTable,
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
) -> ForeignKeyResult<()> {
    use turso_parser::ast::RefAct;

    if !matches!(
        foreign_key.declaration.on_update,
        RefAct::NoAction | RefAct::Restrict
    ) {
        return Err(PhysicalForeignKeyError::Unsupported(
            "mutating ON UPDATE action",
        ));
    }
    if foreign_key.child_positions.len() != foreign_key.parent_positions.len()
        || foreign_key.child_positions.is_empty()
    {
        return Err(PhysicalForeignKeyError::Invalid(
            "child and parent key widths differ",
        ));
    }
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
    let complete = program.allocate_label();
    let changed = program.allocate_label();
    let (old_key, new_key) = copy_parent_change_keys(
        program,
        parent_table,
        &foreign_key.parent_positions,
        old_columns,
        new_columns,
        old_rowid,
        new_rowid,
    )?;
    for offset in 0..foreign_key.parent_positions.len() {
        program.emit_insn(Insn::IsNull {
            reg: old_key + offset,
            target_pc: complete,
        });
    }
    for offset in 0..foreign_key.parent_positions.len() {
        let collation = foreign_key
            .parent_unique_index
            .as_ref()
            .and_then(|index| index.value().columns.get(offset))
            .and_then(|column| column.collation);
        program.emit_insn(Insn::Ne {
            lhs: old_key + offset,
            rhs: new_key + offset,
            target_pc: changed,
            flags: CmpInsFlags::default().jump_if_null(),
            collation,
        });
    }
    program.emit_insn(Insn::Goto {
        target_pc: complete,
    });
    program.preassign_label_to_next_insn(changed);

    let cursor = open_parent_table(program, child_table, database);
    let previous_child = bind_child_scan_cursor(bindings, foreign_key.child_source, cursor)?;
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
        ExpressionEmitter::new(program, bindings).emit_into(
            &Expr::column(foreign_key.child_source, position),
            RegisterRange::new(value, 1),
        )?;
        program.emit_insn(Insn::IsNull {
            reg: value,
            target_pc: next,
        });
        let collation = foreign_key
            .parent_unique_index
            .as_ref()
            .and_then(|index| index.value().columns.get(offset))
            .and_then(|column| column.collation);
        program.emit_insn(Insn::Ne {
            lhs: value,
            rhs: old_key + offset,
            target_pc: next,
            flags: CmpInsFlags::default().jump_if_null(),
            collation,
        });
    }
    if same_table(foreign_key) {
        let child_rowid = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: cursor,
            dest: child_rowid,
        });
        let not_current = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: child_rowid,
            rhs: old_rowid.0,
            target_pc: not_current,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        for (offset, position) in foreign_key.child_positions.iter().copied().enumerate() {
            let new_child = child_register(parent_table, new_columns, new_rowid, position)?;
            program.emit_insn(Insn::Ne {
                lhs: new_child,
                rhs: old_key + offset,
                target_pc: next,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: None,
            });
        }
        program.preassign_label_to_next_insn(not_current);
    }
    match foreign_key.declaration.on_update {
        RefAct::Restrict => program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_FOREIGNKEY,
            description: "FOREIGN KEY constraint failed".to_string(),
            on_error: None,
            description_reg: None,
        }),
        RefAct::NoAction => program.emit_insn(Insn::FkCounter {
            increment_value: 1,
            deferred: foreign_key.declaration.deferred,
        }),
        RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault => unreachable!(),
    }
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(scan_done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    restore_child_scan_cursor(bindings, foreign_key.child_source, previous_child)?;
    program.preassign_label_to_next_insn(complete);
    Ok(())
}

fn emit_delete_parent_check(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    foreign_key: &ResolvedForeignKey,
    parent_table: &BTreeTable,
    old_columns: RegisterRange,
    rowid: RegisterId,
) -> ForeignKeyResult<()> {
    use turso_parser::ast::RefAct;

    if !matches!(
        foreign_key.declaration.on_delete,
        RefAct::NoAction | RefAct::Restrict
    ) {
        return Err(PhysicalForeignKeyError::Unsupported(
            "mutating ON DELETE action",
        ));
    }
    if foreign_key.child_positions.len() != foreign_key.parent_positions.len()
        || foreign_key.child_positions.is_empty()
    {
        return Err(PhysicalForeignKeyError::Invalid(
            "child and parent key widths differ",
        ));
    }
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
    let complete = program.allocate_label();
    let key = program.alloc_registers(foreign_key.parent_positions.len());
    for (offset, position) in foreign_key.parent_positions.iter().copied().enumerate() {
        let source = child_register(parent_table, old_columns, rowid, position)?;
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
    let previous_child = bind_child_scan_cursor(bindings, foreign_key.child_source, cursor)?;
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
        ExpressionEmitter::new(program, bindings).emit_into(
            &Expr::column(foreign_key.child_source, position),
            RegisterRange::new(value, 1),
        )?;
        program.emit_insn(Insn::IsNull {
            reg: value,
            target_pc: next,
        });
        let collation = foreign_key
            .parent_unique_index
            .as_ref()
            .and_then(|index| index.value().columns.get(offset))
            .and_then(|column| column.collation);
        program.emit_insn(Insn::Ne {
            lhs: value,
            rhs: key + offset,
            target_pc: next,
            flags: CmpInsFlags::default().jump_if_null(),
            collation,
        });
    }
    if same_table(foreign_key) {
        let child_rowid = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: cursor,
            dest: child_rowid,
        });
        program.emit_insn(Insn::Eq {
            lhs: child_rowid,
            rhs: rowid.0,
            target_pc: next,
            flags: CmpInsFlags::default(),
            collation: None,
        });
    }
    match foreign_key.declaration.on_delete {
        RefAct::Restrict => program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_FOREIGNKEY,
            description: "FOREIGN KEY constraint failed".to_string(),
            on_error: None,
            description_reg: None,
        }),
        RefAct::NoAction => program.emit_insn(Insn::FkCounter {
            increment_value: 1,
            deferred: foreign_key.declaration.deferred,
        }),
        RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault => unreachable!(),
    }
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(scan_done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    restore_child_scan_cursor(bindings, foreign_key.child_source, previous_child)?;
    program.preassign_label_to_next_insn(complete);
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
    bindings: &mut RuntimeBindings<'_>,
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
    let previous_child = bind_child_scan_cursor(bindings, foreign_key.child_source, cursor)?;
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
        ExpressionEmitter::new(program, bindings).emit_into(
            &Expr::column(foreign_key.child_source, position),
            RegisterRange::new(value, 1),
        )?;
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
    restore_child_scan_cursor(bindings, foreign_key.child_source, previous_child)?;
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

    let invalid_rowid = foreign_key.parent_uses_rowid.then(|| {
        let invalid = program.allocate_label();
        program.emit_insn(Insn::MustBeInt {
            reg: key,
            target_pc: Some(invalid),
        });
        invalid
    });

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
        if let Some(invalid_rowid) = invalid_rowid {
            program.preassign_label_to_next_insn(invalid_rowid);
        }
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

#[allow(clippy::too_many_arguments)]
fn emit_key_change_branch(
    program: &mut ProgramBuilder,
    foreign_key: &ResolvedForeignKey,
    table: &BTreeTable,
    positions: &[usize],
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
    changed: crate::vdbe::BranchOffset,
    unchanged: crate::vdbe::BranchOffset,
) -> ForeignKeyResult<()> {
    if positions.is_empty() {
        return Err(PhysicalForeignKeyError::Invalid(
            "foreign key has no key positions",
        ));
    }
    for (offset, position) in positions.iter().copied().enumerate() {
        let next_equal = (offset + 1 != positions.len()).then(|| program.allocate_label());
        program.emit_insn(Insn::Eq {
            lhs: child_register(table, old_columns, old_rowid, position)?,
            rhs: child_register(table, new_columns, new_rowid, position)?,
            target_pc: next_equal.unwrap_or(unchanged),
            flags: CmpInsFlags::default().null_eq(),
            collation: foreign_key
                .parent_unique_index
                .as_ref()
                .and_then(|index| index.value().columns.get(offset))
                .and_then(|column| column.collation),
        });
        program.emit_insn(Insn::Goto { target_pc: changed });
        if let Some(next_equal) = next_equal {
            program.preassign_label_to_next_insn(next_equal);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn copy_parent_change_keys(
    program: &mut ProgramBuilder,
    parent_table: &BTreeTable,
    parent_positions: &[usize],
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
) -> ForeignKeyResult<(usize, usize)> {
    let old_key = program.alloc_registers(parent_positions.len());
    let new_key = program.alloc_registers(parent_positions.len());
    for (offset, position) in parent_positions.iter().copied().enumerate() {
        let old = child_register(parent_table, old_columns, old_rowid, position)?;
        let new = child_register(parent_table, new_columns, new_rowid, position)?;
        program.emit_insn(Insn::Copy {
            src_reg: old,
            dst_reg: old_key + offset,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new,
            dst_reg: new_key + offset,
            extra_amount: 0,
        });
    }
    Ok((old_key, new_key))
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

#[cfg(test)]
mod properties {
    use hegel::generators;

    use super::*;
    use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};

    // Examples: changing parent key `(a, b)` from `(1, 2)` to `(10, 20)`
    // must copy all four values before either component can branch to an
    // `ON UPDATE` check or CASCADE program. The same rule holds for wider
    // keys: an early difference in `a` cannot leave the later `b` parameter
    // uninitialized in `UPDATE parent SET a = 10, b = 20`.
    #[hegel::test]
    fn parent_update_copies_the_complete_old_and_new_keys_before_comparing(tc: hegel::TestCase) {
        let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(12)));
        let columns = (0..width)
            .map(|position| format!("c{position} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ");
        let table = BTreeTable::from_sql(&format!("CREATE TABLE parent({columns})"), 2)
            .expect("generated parent table is valid");
        let mut program = ProgramBuilder::new(
            QueryMode::Normal,
            None,
            ProgramBuilderOpts::new(0, width * 2, 0),
        );
        let old_columns = RegisterRange::new(program.alloc_registers(width), width);
        let new_columns = RegisterRange::new(program.alloc_registers(width), width);
        let old_rowid = RegisterId(program.alloc_register());
        let new_rowid = RegisterId(program.alloc_register());
        let positions = (0..width).collect::<Vec<_>>();

        let (old_key, new_key) = copy_parent_change_keys(
            &mut program,
            &table,
            &positions,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
        )
        .expect("generated positions belong to the parent row");

        assert_eq!(program.insns.len(), width * 2);
        for (offset, pair) in program.insns.chunks_exact(2).enumerate() {
            assert!(matches!(
                pair[0].0,
                Insn::Copy {
                    src_reg,
                    dst_reg,
                    extra_amount: 0,
                } if src_reg == old_columns.first.0 + offset && dst_reg == old_key + offset
            ));
            assert!(matches!(
                pair[1].0,
                Insn::Copy {
                    src_reg,
                    dst_reg,
                    extra_amount: 0,
                } if src_reg == new_columns.first.0 + offset && dst_reg == new_key + offset
            ));
        }
    }

    // Examples:
    // - immediate `INSERT OR REPLACE parent VALUES (1)` may skip the transient
    //   NO ACTION violation when the replacement restores parent key `1`;
    // - deferred NO ACTION must record the delete and let the later insert
    //   repair it, so an unrelated violation already in the counter survives;
    // - `ON DELETE RESTRICT` is immediate and can never be skipped.
    #[hegel::test]
    fn only_immediate_no_action_can_skip_a_restored_replace_key(tc: hegel::TestCase) {
        let action = match tc.draw(generators::integers::<u8>().max_value(4)) {
            0 => turso_parser::ast::RefAct::NoAction,
            1 => turso_parser::ast::RefAct::Restrict,
            2 => turso_parser::ast::RefAct::Cascade,
            3 => turso_parser::ast::RefAct::SetNull,
            _ => turso_parser::ast::RefAct::SetDefault,
        };
        let deferred = tc.draw(generators::integers::<u8>().max_value(1)) == 1;

        assert_eq!(
            can_skip_transient_replace_check(action, deferred),
            action == turso_parser::ast::RefAct::NoAction && !deferred
        );
    }
}
