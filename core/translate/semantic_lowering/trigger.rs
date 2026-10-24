//! Row triggers compiled from HIR.
//!
//! Each trigger is analyzed into its own HIR document and lowered into its
//! own subprogram. The subprogram reads the OLD and NEW rows
//! from its parameters into registers and binds the trigger's OLD and NEW
//! sources to them; nothing in the trigger body is rewritten. The firing
//! statement passes the registers holding its rows with `Insn::Program`.

use std::num::NonZeroU32;

use super::dml::Subprograms;

use turso_parser::ast::{ResolveType, TriggerTime};

use crate::{
    parameters::ParameterSpelling,
    schema::BTreeTable,
    sync::Arc,
    translate::{
        expr::ConditionMetadata,
        plan::QueryDestination,
        semantic::{hir, SemanticRootInput},
        semantic_to_plan::{HirPlan, HirRootPlan, HirTriggerCommandPlan, HirTriggerPlan},
    },
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode, SourceBinding},
        insn::{Insn, Subprogram},
        BranchOffset, PreparedProgram,
    },
    LimboError, Result,
};

use super::HirSession;

/// One trigger compiled into a subprogram.
pub(super) struct CompiledTrigger {
    pub(super) time: TriggerTime,
    program: Arc<PreparedProgram>,
    /// The trigger reads OLD and NEW from parameters in this order: each
    /// OLD column then the OLD rowid, then each NEW column then the NEW
    /// rowid. Only the rows its event has are present.
    has_old: bool,
    has_new: bool,
    /// The databases the trigger body reads or writes, with their schema
    /// versions, which the firing statement must open transactions on.
    databases: Vec<TriggerDatabase>,
}

struct TriggerDatabase {
    database: usize,
    schema_version: u32,
    write: bool,
}

/// The registers of one row a trigger can read: one per table column, and
/// the rowid.
#[derive(Clone, Copy)]
pub(super) struct TriggerRow {
    pub(super) columns: usize,
    pub(super) rowid: usize,
}

/// Compile `triggers`, the triggers on `table` in `database` that the
/// statement fires. A trigger that is already being compiled further up is
/// left out.
pub(super) fn compile_triggers(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    table: &BTreeTable,
    database: usize,
    triggers: &[hir::ResolvedTrigger],
    conflict_override: Option<ResolveType>,
) -> Result<Vec<CompiledTrigger>> {
    let mut compiled = Vec::with_capacity(triggers.len());
    for trigger in triggers {
        match compile_trigger(session, parent, table, database, trigger, conflict_override)? {
            Compiled::Trigger(trigger) => compiled.push(trigger),
            Compiled::AlreadyCompiling => {}
        }
    }
    Ok(compiled)
}

enum Compiled {
    Trigger(CompiledTrigger),
    AlreadyCompiling,
}

fn compile_trigger(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    table: &BTreeTable,
    database: usize,
    trigger: &hir::ResolvedTrigger,
    conflict_override: Option<ResolveType>,
) -> Result<Compiled> {
    let connection = session.connection;
    let handle = trigger.handle();
    if connection.trigger_is_compiling(&handle) {
        return Ok(Compiled::AlreadyCompiling);
    }
    connection.start_trigger_compilation(handle.clone());
    struct CompilationGuard<'a>(&'a crate::Connection);
    impl Drop for CompilationGuard<'_> {
        fn drop(&mut self) {
            self.0.end_trigger_compilation();
        }
    }
    let _guard = CompilationGuard(connection);

    let definition = trigger.value();
    let plan = session.plan(SemanticRootInput::TriggerProgram {
        database: hir::DatabaseId::new(database),
        // Triggers stored in the temp schema (TEMP triggers, and triggers on
        // TEMP tables) look up their command tables by the normal rules.
        command_database: (!definition.temporary && database != crate::TEMP_DB_ID)
            .then(|| hir::DatabaseId::new(database)),
        table_name: &table.name,
        event: definition.event.clone(),
        new_is_decoded: definition.time == TriggerTime::Before,
        predicate: definition.when_clause.as_ref(),
        commands: &definition.commands,
        conflict_override,
    })?;
    let (environment, root, commands) = trigger_program(&plan)?;
    let mut builder = ProgramBuilder::new_for_trigger(
        QueryMode::Normal,
        parent.capture_data_changes_info().clone(),
        ProgramBuilderOpts::new(1, 32, 2),
        handle,
        session.connection.mvcc_enabled(),
    );
    if let Some(conflict) = conflict_override {
        builder.set_trigger_conflict_override(conflict);
    }
    // What the body's own DML fires, per command.
    let mut nested = Vec::with_capacity(commands.len());
    for (command, command_plan) in root.commands.iter().zip(commands) {
        let compiled = match (command, command_plan) {
            (hir::TriggerCommand::Select(_), HirTriggerCommandPlan::Select(_)) => {
                Subprograms::default()
            }
            (hir::TriggerCommand::Insert(insert), HirTriggerCommandPlan::Insert) => {
                super::insert::check(&plan, insert, &builder)?;
                Subprograms::for_insert(session, &builder, &plan, insert)?
            }
            (hir::TriggerCommand::Update(update), HirTriggerCommandPlan::Update(access)) => {
                super::update::check(&plan, update, access)?;
                Subprograms::for_update(session, &builder, &plan, update)?
            }
            (hir::TriggerCommand::Delete(delete), HirTriggerCommandPlan::Delete(access)) => {
                super::delete::check(&plan.document, delete, access)?;
                Subprograms::for_delete(session, &builder, &plan, delete)?
            }
            _ => {
                return Err(LimboError::InternalError(
                    "HIR trigger command and its plan differ".into(),
                ));
            }
        };
        nested.push(compiled);
    }

    builder.prologue();
    builder.set_hir_plan(plan.clone());
    let columns = table.columns().len();
    let mut next_parameter = 1u32;
    for source in [environment.old_source, environment.new_source]
        .into_iter()
        .flatten()
    {
        let start = builder.alloc_registers(columns + 1);
        for offset in 0..=columns {
            let index = NonZeroU32::new(next_parameter).expect("parameters start at 1");
            next_parameter += 1;
            let index = builder.register_parameter(index, &ParameterSpelling::Anonymous);
            builder.emit_insn(Insn::Variable {
                index,
                dest: start + offset,
            });
        }
        builder.bind_source(
            source,
            SourceBinding::Registers {
                start,
                rowid: Some(start + columns),
            },
        );
    }
    let done = builder.allocate_label();
    if let Some(predicate) = &root.predicate {
        let passed = builder.allocate_label();
        super::expr::translate_condition_expr(
            &mut builder,
            &plan.document,
            predicate,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: passed,
                jump_target_when_false: done,
            },
        )?;
        builder.preassign_label_to_next_insn(passed);
    }
    for ((command, command_plan), subprograms) in root.commands.iter().zip(commands).zip(&nested) {
        match (command, command_plan) {
            (hir::TriggerCommand::Select(_), HirTriggerCommandPlan::Select(query)) => {
                // The subprogram's result rows are discarded.
                super::query::emit_planned_query_body(
                    &mut builder,
                    &plan,
                    *query,
                    &QueryDestination::ResultRows,
                )?;
            }
            (hir::TriggerCommand::Insert(insert), HirTriggerCommandPlan::Insert) => {
                super::insert::emit_insert(
                    &mut builder,
                    &plan,
                    insert,
                    subprograms,
                    super::uses_mvcc(connection, &plan.document, insert.target),
                    !connection.check_constraints_ignored(),
                    connection
                        .dialect()
                        .upsert_rejects_row_written_by_same_statement(),
                )?;
                builder.emit_insn(Insn::ResetCount);
            }
            (hir::TriggerCommand::Update(update), HirTriggerCommandPlan::Update(access)) => {
                let mvcc = super::uses_mvcc(connection, &plan.document, update.target);
                super::update::emit_update(
                    &mut builder,
                    &plan,
                    update,
                    access,
                    subprograms,
                    mvcc,
                    !connection.check_constraints_ignored(),
                    None,
                )?;
                builder.emit_insn(Insn::ResetCount);
            }
            (hir::TriggerCommand::Delete(delete), HirTriggerCommandPlan::Delete(access)) => {
                super::delete::emit_delete(&mut builder, &plan, delete, access, subprograms)?;
                builder.emit_insn(Insn::ResetCount);
            }
            _ => {
                return Err(LimboError::InternalError(
                    "HIR trigger command and its plan differ".into(),
                ));
            }
        }
    }
    builder.preassign_label_to_next_insn(done);
    builder.epilogue(session.context.main_schema());
    let built = builder.build(connection.clone(), true, "trigger subprogram")?;
    let prepared = built.prepared().clone();
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
                LimboError::InternalError(format!("HIR trigger reads unknown database {database}"))
            })?
            .schema_version;
        databases.push(TriggerDatabase {
            database,
            schema_version,
            write,
        });
    }
    Ok(Compiled::Trigger(CompiledTrigger {
        time: definition.time,
        program: prepared,
        has_old: environment.old_source.is_some(),
        has_new: environment.new_source.is_some(),
        databases,
    }))
}

/// Compile the triggers an INSERT fires.
/// Like SQLite, an INSERT's own OR clause, ABORT included, also applies
/// inside its triggers; without one, each trigger step keeps its own.
pub(super) fn insert_triggers(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &HirPlan,
    insert: &hir::Insert,
) -> Result<Vec<CompiledTrigger>> {
    let hir::InsertTargetKind::BTree { triggers, .. } = &insert.target_kind else {
        return Ok(Vec::new());
    };
    let conflict_override = parent.trigger_conflict_override.or(insert.conflict);
    target_triggers(
        session,
        parent,
        plan,
        insert.target,
        &triggers.insert,
        conflict_override,
    )
}

/// Compile the UPDATE triggers an UPSERT's DO UPDATE fires. An OR IGNORE or
/// OR REPLACE inside them does not hide an error: they run with ABORT.
pub(super) fn upsert_update_triggers(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &HirPlan,
    insert: &hir::Insert,
) -> Result<Vec<CompiledTrigger>> {
    let hir::InsertTargetKind::BTree { triggers, .. } = &insert.target_kind else {
        return Ok(Vec::new());
    };
    target_triggers(
        session,
        parent,
        plan,
        insert.target,
        &triggers.upsert_update,
        Some(ResolveType::Abort),
    )
}

/// Compile the triggers an UPDATE fires.
/// The conflict clause applies inside them as for INSERT.
pub(super) fn update_triggers(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &HirPlan,
    update: &hir::Update,
) -> Result<Vec<CompiledTrigger>> {
    let hir::UpdateTargetKind::BTree { triggers, .. } = &update.target_kind else {
        return Ok(Vec::new());
    };
    let conflict_override = parent.trigger_conflict_override.or(update.conflict);
    target_triggers(
        session,
        parent,
        plan,
        update.target,
        triggers,
        conflict_override,
    )
}

/// Compile the triggers a DELETE fires.
pub(super) fn delete_triggers(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &HirPlan,
    delete: &hir::Delete,
) -> Result<Vec<CompiledTrigger>> {
    let hir::DeleteTargetKind::BTree { triggers, .. } = &delete.target_kind else {
        return Ok(Vec::new());
    };
    target_triggers(
        session,
        parent,
        plan,
        delete.target,
        triggers,
        // Like SQLite, a DELETE fires its triggers with the
        // default conflict policy, even inside a trigger fired by an OR
        // REPLACE statement.
        None,
    )
}

/// Compile `triggers`, the triggers on the table `target` writes.
fn target_triggers(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &HirPlan,
    target: hir::SourceId,
    triggers: &[hir::ResolvedTrigger],
    conflict_override: Option<ResolveType>,
) -> Result<Vec<CompiledTrigger>> {
    if triggers.is_empty() {
        return Ok(Vec::new());
    }
    let source = plan
        .document
        .source(target)
        .ok_or_else(|| LimboError::InternalError("HIR DML target is missing".into()))?;
    let (hir::SourceKind::Table(table), Some(database)) = (&source.kind, source.database) else {
        return Err(LimboError::InternalError(
            "HIR DML target with triggers is not a table".into(),
        ));
    };
    let Some(table) = table.value().btree() else {
        return Err(LimboError::InternalError(
            "HIR DML target with triggers is not a B-tree table".into(),
        ));
    };
    compile_triggers(
        session,
        parent,
        &table,
        database.index(),
        triggers,
        conflict_override,
    )
}

fn trigger_program(
    plan: &HirPlan,
) -> Result<(
    &hir::TriggerEnvironment,
    &hir::TriggerProgram,
    &[HirTriggerCommandPlan],
)> {
    match (&plan.document.root, &plan.root) {
        (
            hir::HirRoot::Trigger(hir::TriggerRoot {
                environment,
                body: hir::TriggerBody::Program(program),
            }),
            HirRootPlan::Trigger(HirTriggerPlan::Program(commands)),
        ) if program.commands.len() == commands.len() => Ok((environment, program, commands)),
        _ => Err(LimboError::InternalError(
            "HIR trigger analysis did not produce a trigger program".into(),
        )),
    }
}

impl CompiledTrigger {
    /// Run the trigger for one row. `skip_row` is where RAISE(IGNORE) goes.
    pub(super) fn emit_fire(
        &self,
        program: &mut ProgramBuilder,
        old: Option<TriggerRow>,
        new: Option<TriggerRow>,
        columns: usize,
        skip_row: BranchOffset,
    ) -> Result<()> {
        let mut param_registers = Vec::new();
        for (present, row) in [(self.has_old, old), (self.has_new, new)] {
            if !present {
                continue;
            }
            let row = row.ok_or_else(|| {
                LimboError::InternalError("HIR trigger fired without a row it reads".into())
            })?;
            param_registers.extend(row.columns..row.columns + columns);
            param_registers.push(row.rowid);
        }
        // A subprogram has no Transaction instructions, so the parent takes
        // the transactions the trigger body needs.
        for database in &self.databases {
            match (database.write, database.database == crate::MAIN_DB_ID) {
                (true, true) => program.begin_write_operation()?,
                (true, false) => {
                    program.begin_write_on_database(database.database, database.schema_version)?
                }
                (false, true) => program.begin_read_operation()?,
                (false, false) => {
                    program.begin_read_on_database(database.database, database.schema_version)?
                }
            }
        }
        program.emit_insn(Insn::Program {
            param_registers,
            program: Subprogram::PreparedProgram(self.program.clone()),
            ignore_jump_target: skip_row,
        });
        Ok(())
    }
}
