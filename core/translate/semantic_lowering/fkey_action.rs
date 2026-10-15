//! Foreign-key actions compiled from HIR: ON DELETE and ON UPDATE CASCADE,
//! SET NULL and SET DEFAULT.
//!
//! Like legacy, an action is the SQL statement it stands for, on the child
//! table, matching the child key against the old parent key:
//!
//! - CASCADE on delete: `DELETE FROM child WHERE c1 = ?1 AND ...`
//! - CASCADE on update: `UPDATE child SET c1 = ?n+1, ... WHERE c1 = ?1 ...`
//! - SET NULL: `UPDATE child SET c1 = NULL, ... WHERE c1 = ?1 ...`
//! - SET DEFAULT: `UPDATE child SET c1 = <default>, ... WHERE c1 = ?1 ...`
//!
//! That statement is analyzed and lowered through HIR, with its own triggers
//! and actions, into a subprogram that the statement changing the parent row
//! runs with the parent key as parameters. An action that fires itself again,
//! such as a CASCADE on a table that references itself, calls the program
//! still being compiled.

use std::cell::RefCell;

use turso_parser::ast::{self, Expr, Literal, Name, RefAct};

use crate::{
    schema::ForeignKey,
    sync::{Arc, OnceLock, Weak},
    translate::{
        semantic::{hir, SemanticRootInput},
        semantic_to_plan::HirRootPlan,
    },
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode},
        insn::{Insn, Subprogram},
        PreparedProgram,
    },
    LimboError, Result,
};

use super::{dml::Subprograms, HirSession};

/// What happened to the parent row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ParentChange {
    Delete,
    Update,
}

/// The actions whose programs are being compiled, innermost last, so that
/// an action reached again calls the program being built.
#[derive(Default)]
pub(super) struct ActionStack(RefCell<Vec<ActionInProgress>>);

struct ActionInProgress {
    foreign_key: Arc<ForeignKey>,
    change: ParentChange,
    /// Filled with the program once it is built.
    slot: Arc<OnceLock<Weak<PreparedProgram>>>,
}

/// One foreign-key action, compiled.
pub(super) struct CompiledAction<'a> {
    pub(super) foreign_key: &'a hir::ResolvedForeignKey,
    action: RefAct,
    program: Subprogram,
    /// The databases the action reads or writes, with their schema versions
    /// and whether it writes them, which the firing statement must open
    /// transactions on.
    databases: Vec<(usize, u32, bool)>,
}

/// The action of `foreign_key` for `change`, if it changes child rows.
fn action_of(foreign_key: &hir::ResolvedForeignKey, change: ParentChange) -> Option<RefAct> {
    let action = match change {
        ParentChange::Delete => foreign_key.declaration.on_delete,
        ParentChange::Update => foreign_key.declaration.on_update,
    };
    matches!(
        action,
        RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault
    )
    .then_some(action)
}

/// Whether `foreign_key` has an action that changes child rows for `change`.
pub(super) fn has_action(foreign_key: &hir::ResolvedForeignKey, change: ParentChange) -> bool {
    action_of(foreign_key, change).is_some()
}

/// Compile the actions of the foreign keys whose parent is the target, for
/// `change`. For an UPDATE, only keys among the columns at `assigned` count.
/// `None` means an action needs legacy, so the whole statement must use it.
pub(super) fn compile_actions<'a>(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    foreign_keys: &'a hir::DmlForeignKeys,
    change: ParentChange,
    assigned: &[usize],
) -> Result<Option<Vec<CompiledAction<'a>>>> {
    let mut compiled = Vec::new();
    for foreign_key in &foreign_keys.incoming {
        let Some(action) = action_of(foreign_key, change) else {
            continue;
        };
        if change == ParentChange::Update
            && !super::fkey::parent_key_may_change(foreign_key, assigned)
        {
            continue;
        }
        match compile_action(session, parent, foreign_key, change, action)? {
            Some(action) => compiled.push(action),
            None => return Ok(None),
        }
    }
    Ok(Some(compiled))
}

fn compile_action<'a>(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    foreign_key: &'a hir::ResolvedForeignKey,
    change: ParentChange,
    action: RefAct,
) -> Result<Option<CompiledAction<'a>>> {
    let declaration = &foreign_key.declaration;
    // An action reached again while its program is being built calls it.
    let in_progress = session
        .actions
        .0
        .borrow()
        .iter()
        .find(|entry| entry.change == change && Arc::ptr_eq(&entry.foreign_key, declaration))
        .map(|entry| entry.slot.clone());
    if let Some(slot) = in_progress {
        return Ok(Some(CompiledAction {
            foreign_key,
            action,
            program: Subprogram::Pending(slot),
            databases: Vec::new(),
        }));
    }

    let statement = action_statement(session, foreign_key, change, action)?;
    let slot = Arc::new(OnceLock::new());
    session.actions.0.borrow_mut().push(ActionInProgress {
        foreign_key: declaration.clone(),
        change,
        slot: slot.clone(),
    });
    struct Pop<'s>(&'s ActionStack);
    impl Drop for Pop<'_> {
        fn drop(&mut self) {
            self.0 .0.borrow_mut().pop();
        }
    }
    let _pop = Pop(&session.actions);

    let plan = session.plan(SemanticRootInput::Statement(&statement))?;
    let connection = session.connection;
    let mut builder = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        parent.capture_data_changes_info().clone(),
        ProgramBuilderOpts::new(2, 32, 4),
    );
    match (&plan.document.root, &plan.root) {
        (hir::HirRoot::Delete(delete), HirRootPlan::Delete(access)) => {
            if super::delete::needs_legacy(&plan.document, delete, access, &builder)? {
                return Ok(None);
            }
            let Some(subprograms) = Subprograms::for_delete(session, &builder, &plan, delete)?
            else {
                return Ok(None);
            };
            builder.prologue();
            builder.set_hir_plan(plan.clone());
            super::delete::emit_delete(&mut builder, &plan, delete, access, &subprograms)?;
        }
        (hir::HirRoot::Update(_), HirRootPlan::Update(_)) => {
            let (update, access) = super::update::update_root(&plan)?;
            if super::update::needs_legacy(&plan, update, access, &builder)? {
                return Ok(None);
            }
            let Some(subprograms) = Subprograms::for_update(session, &builder, &plan, update)?
            else {
                return Ok(None);
            };
            let mvcc = connection
                .mv_store_for_db(database_index(foreign_key)?)
                .is_some();
            builder.prologue();
            builder.set_hir_plan(plan.clone());
            super::update::emit_update(
                &mut builder,
                &plan,
                update,
                access,
                &subprograms,
                mvcc,
                !connection.check_constraints_ignored(),
            )?;
        }
        _ => {
            return Err(LimboError::InternalError(
                "HIR foreign-key action is not a DELETE or UPDATE".into(),
            ))
        }
    }
    builder.epilogue(session.schema);
    let built = builder.build(connection.clone(), true, "fk action")?;
    let prepared = built.prepared().clone();
    slot.set(Arc::downgrade(&prepared))
        .map_err(|_| LimboError::InternalError("HIR foreign-key action built twice".into()))?;
    let mut databases = Vec::new();
    for (database, write) in prepared
        .write_databases
        .iter()
        .map(|database| (database, true))
        .chain(
            prepared
                .read_databases
                .iter()
                .filter(|database| !prepared.write_databases.get(*database))
                .map(|database| (database, false)),
        )
    {
        let schema_version = plan
            .document
            .database(hir::DatabaseId::new(database))
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR foreign-key action reads unknown database {database}"
                ))
            })?
            .schema_version;
        databases.push((database, schema_version, write));
    }
    Ok(Some(CompiledAction {
        foreign_key,
        action,
        program: Subprogram::PreparedProgram(prepared),
        databases,
    }))
}

/// Compile the ON DELETE actions a DELETE fires.
pub(super) fn delete_actions<'a>(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    delete: &'a hir::Delete,
) -> Result<Option<Vec<CompiledAction<'a>>>> {
    match &delete.target_kind {
        hir::DeleteTargetKind::BTree { foreign_keys, .. } => {
            compile_actions(session, parent, foreign_keys, ParentChange::Delete, &[])
        }
        hir::DeleteTargetKind::Virtual => Ok(Some(Vec::new())),
    }
}

/// Compile the ON UPDATE actions an UPDATE fires.
pub(super) fn update_actions<'a>(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &super::HirPlan,
    update: &'a hir::Update,
) -> Result<Option<Vec<CompiledAction<'a>>>> {
    match &update.target_kind {
        hir::UpdateTargetKind::BTree { foreign_keys, .. } => {
            let assigned = super::update::changed_positions(plan, update)?;
            compile_actions(
                session,
                parent,
                foreign_keys,
                ParentChange::Update,
                &assigned,
            )
        }
        hir::UpdateTargetKind::Virtual => Ok(Some(Vec::new())),
    }
}

/// Compile the ON UPDATE actions of each UPSERT clause's DO UPDATE, in
/// clause order; a DO NOTHING clause gets none.
pub(super) fn upsert_actions<'a>(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &super::HirPlan,
    insert: &'a hir::Insert,
) -> Result<Option<Vec<Vec<CompiledAction<'a>>>>> {
    let none = || Ok(Some(insert.upserts.iter().map(|_| Vec::new()).collect()));
    let hir::InsertTargetKind::BTree { foreign_keys, .. } = &insert.target_kind else {
        return none();
    };
    let source = plan.document.source(insert.target).ok_or_else(|| {
        LimboError::InternalError(format!("HIR INSERT target {} is missing", insert.target))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return none();
    };
    let table = table.value().require_btree()?;
    let mut compiled = Vec::with_capacity(insert.upserts.len());
    for upsert in &insert.upserts {
        let hir::UpsertAction::Update { assignments, .. } = &upsert.action else {
            compiled.push(Vec::new());
            continue;
        };
        let assigned = super::insert::column_positions(&super::insert::do_update_columns(
            &table,
            assignments,
        )?);
        let Some(actions) = compile_actions(
            session,
            parent,
            foreign_keys,
            ParentChange::Update,
            &assigned,
        )?
        else {
            return Ok(None);
        };
        compiled.push(actions);
    }
    Ok(Some(compiled))
}

fn database_index(foreign_key: &hir::ResolvedForeignKey) -> Result<usize> {
    foreign_key
        .child_table
        .database()
        .map(|database| database.index())
        .ok_or_else(|| LimboError::InternalError("HIR foreign key has no database".into()))
}

/// The statement an action stands for, as legacy builds it.
fn action_statement(
    session: &HirSession<'_>,
    foreign_key: &hir::ResolvedForeignKey,
    change: ParentChange,
    action: RefAct,
) -> Result<ast::Stmt> {
    let child = foreign_key.child_table.value();
    let database = database_index(foreign_key)?;
    let table = ast::QualifiedName {
        db_name: (database != crate::MAIN_DB_ID)
            .then(|| session.connection.get_database_name_by_index(database))
            .flatten()
            .map(Name::from_string),
        name: Name::from_string(child.get_name()),
        alias: None,
    };
    let columns = &foreign_key.declaration.child_columns;
    let parameter = |index: usize| {
        Expr::Variable(ast::Variable::indexed(
            u32::try_from(index)
                .ok()
                .and_then(std::num::NonZeroU32::new)
                .expect("foreign-key parameter index fits in u32"),
        ))
    };
    // c1 = ?1 AND c2 = ?2 ...
    let matches_old_key = columns
        .iter()
        .enumerate()
        .map(|(offset, column)| {
            Expr::Binary(
                Box::new(Expr::Id(Name::from_string(column))),
                ast::Operator::Equals,
                Box::new(parameter(offset + 1)),
            )
        })
        .reduce(|all, term| Expr::Binary(Box::new(all), ast::Operator::And, Box::new(term)))
        .ok_or_else(|| LimboError::InternalError("HIR foreign key has no columns".into()))?;
    if change == ParentChange::Delete && action == RefAct::Cascade {
        return Ok(ast::Stmt::Delete {
            with: None,
            tbl_name: table,
            indexed: None,
            where_clause: Some(Box::new(matches_old_key)),
            returning: vec![],
        });
    }
    let child_table = child.btree();
    let sets = columns
        .iter()
        .enumerate()
        .map(|(offset, column)| {
            let value = match action {
                RefAct::Cascade => parameter(columns.len() + offset + 1),
                RefAct::SetNull => Expr::Literal(Literal::Null),
                // Without a DEFAULT, the column becomes NULL.
                RefAct::SetDefault => child_table
                    .as_ref()
                    .and_then(|table| table.get_column(column))
                    .and_then(|(_, column)| column.default.as_deref().cloned())
                    .unwrap_or(Expr::Literal(Literal::Null)),
                RefAct::NoAction | RefAct::Restrict => {
                    unreachable!("only actions that change child rows are compiled")
                }
            };
            ast::Set {
                col_names: vec![Name::from_string(column)],
                expr: Box::new(value),
            }
        })
        .collect();
    Ok(ast::Stmt::Update(ast::Update {
        with: None,
        or_conflict: None,
        tbl_name: table,
        indexed: None,
        sets,
        from: None,
        where_clause: Some(Box::new(matches_old_key)),
        returning: vec![],
    }))
}

impl CompiledAction<'_> {
    /// Run the action for one parent row: `old_key` holds the parent key
    /// before the change, `new_key` after it, for an UPDATE.
    pub(super) fn emit_fire(
        &self,
        program: &mut ProgramBuilder,
        old_key: usize,
        new_key: Option<usize>,
    ) -> Result<()> {
        let width = self.foreign_key.parent_positions.len();
        let mut param_registers = (old_key..old_key + width).collect::<Vec<_>>();
        if self.action == RefAct::Cascade {
            if let Some(new_key) = new_key {
                param_registers.extend(new_key..new_key + width);
            }
        }
        // A subprogram has no Transaction instructions, so the parent takes
        // the transactions the action needs.
        for &(database, schema_version, write) in &self.databases {
            match (write, database == crate::MAIN_DB_ID) {
                (true, true) => program.begin_write_operation()?,
                (true, false) => program.begin_write_on_database(database, schema_version)?,
                (false, true) => program.begin_read_operation()?,
                (false, false) => program.begin_read_on_database(database, schema_version)?,
            }
        }
        // An action cannot RAISE(IGNORE), so that target is the next
        // instruction.
        let next = program.allocate_label();
        program.emit_insn(Insn::Program {
            param_registers,
            program: self.program.clone(),
            ignore_jump_target: next,
        });
        program.preassign_label_to_next_insn(next);
        Ok(())
    }
}
