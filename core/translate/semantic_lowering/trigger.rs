//! Row triggers compiled from HIR.
//!
//! Each trigger is analyzed into its own HIR document and lowered into its
//! own subprogram, like legacy's. The subprogram reads the OLD and NEW rows
//! from its parameters into registers and binds the trigger's OLD and NEW
//! sources to them; nothing in the trigger body is rewritten. The firing
//! statement passes the registers holding its rows with `Insn::Program`.

use std::num::NonZeroU32;

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
/// statement fires. `None` means a trigger needs legacy, so the whole
/// statement must use legacy. A trigger that is already being compiled
/// further up is left out, as legacy does.
pub(super) fn compile_triggers(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    table: &BTreeTable,
    database: usize,
    triggers: &[hir::ResolvedTrigger],
    conflict_override: Option<ResolveType>,
) -> Result<Option<Vec<CompiledTrigger>>> {
    let mut compiled = Vec::with_capacity(triggers.len());
    for trigger in triggers {
        match compile_trigger(session, parent, table, database, trigger, conflict_override)? {
            Compiled::Trigger(trigger) => compiled.push(trigger),
            Compiled::AlreadyCompiling => {}
            Compiled::NeedsLegacy => return Ok(None),
        }
    }
    Ok(Some(compiled))
}

enum Compiled {
    Trigger(CompiledTrigger),
    AlreadyCompiling,
    NeedsLegacy,
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
        table_name: &table.name,
        event: definition.event.clone(),
        predicate: definition.when_clause.as_ref(),
        commands: &definition.commands,
        conflict_override,
    })?;
    let (environment, root, commands) = trigger_program(&plan)?;
    let foreign_keys_enabled = connection.foreign_keys_enabled();
    let mut builder = ProgramBuilder::new_for_trigger(
        QueryMode::Normal,
        parent.capture_data_changes_info().clone(),
        ProgramBuilderOpts::new(1, 32, 2),
        handle,
    );
    // Triggers that the body's own DELETEs fire, per command.
    let mut nested = Vec::with_capacity(commands.len());
    for (command, command_plan) in root.commands.iter().zip(commands) {
        let mut command_triggers = Vec::new();
        let needs_legacy = match (command, command_plan) {
            (hir::TriggerCommand::Select(_), HirTriggerCommandPlan::Select(_)) => false,
            (hir::TriggerCommand::Insert(insert), HirTriggerCommandPlan::Insert) => {
                super::insert::needs_legacy(&plan, insert, foreign_keys_enabled, &builder)?
            }
            (hir::TriggerCommand::Update(update), HirTriggerCommandPlan::Update(access)) => {
                super::update::needs_legacy(&plan, update, access, foreign_keys_enabled, &builder)?
            }
            (hir::TriggerCommand::Delete(delete), HirTriggerCommandPlan::Delete(access)) => {
                if super::delete::needs_legacy(delete, access, foreign_keys_enabled, &builder)? {
                    true
                } else {
                    match delete_triggers(session, &builder, &plan, delete, conflict_override)? {
                        Some(triggers) => {
                            command_triggers = triggers;
                            false
                        }
                        None => true,
                    }
                }
            }
            _ => {
                return Err(LimboError::InternalError(
                    "HIR trigger command and its plan differ".into(),
                ));
            }
        };
        if needs_legacy {
            return Ok(Compiled::NeedsLegacy);
        }
        nested.push(command_triggers);
    }

    if let Some(conflict) = conflict_override {
        builder.set_trigger_conflict_override(conflict);
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
                jump_target_when_null: done,
            },
        )?;
        builder.preassign_label_to_next_insn(passed);
    }
    for ((command, command_plan), command_triggers) in
        root.commands.iter().zip(commands).zip(&nested)
    {
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
                super::insert::emit_insert(&mut builder, &plan, insert)?;
                builder.emit_insn(Insn::ResetCount);
            }
            (hir::TriggerCommand::Update(update), HirTriggerCommandPlan::Update(access)) => {
                let mvcc = plan
                    .document
                    .source(update.target)
                    .and_then(|source| source.database)
                    .is_some_and(|database| connection.mv_store_for_db(database.index()).is_some());
                super::update::emit_update(&mut builder, &plan, update, access, mvcc)?;
                builder.emit_insn(Insn::ResetCount);
            }
            (hir::TriggerCommand::Delete(delete), HirTriggerCommandPlan::Delete(access)) => {
                super::delete::emit_delete(&mut builder, &plan, delete, access, command_triggers)?;
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
    builder.epilogue(session.schema);
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

/// Compile the triggers a DELETE fires. `None` means one needs legacy.
pub(super) fn delete_triggers(
    session: &HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &HirPlan,
    delete: &hir::Delete,
    conflict_override: Option<ResolveType>,
) -> Result<Option<Vec<CompiledTrigger>>> {
    let hir::DeleteTargetKind::BTree { triggers, .. } = &delete.target_kind else {
        return Ok(Some(Vec::new()));
    };
    if triggers.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let source = plan
        .document
        .source(delete.target)
        .ok_or_else(|| LimboError::InternalError("HIR DELETE target is missing".into()))?;
    let (hir::SourceKind::Table(table), Some(database)) = (&source.kind, source.database) else {
        return Err(LimboError::InternalError(
            "HIR DELETE target with triggers is not a table".into(),
        ));
    };
    let Some(table) = table.value().btree() else {
        return Err(LimboError::InternalError(
            "HIR DELETE target with triggers is not a B-tree table".into(),
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
