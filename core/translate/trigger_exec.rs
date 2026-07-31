use crate::schema::{BTreeTable, Trigger};
use crate::sync::Arc;
use crate::translate::plan::{
    ColumnMask, PlanRuntimeBindings, RuntimeRowBinding, RuntimeValueBinding, SourceReadPrograms,
};
use crate::translate::plan_expr::{PlanIdentityMap, PlanParameter};
use crate::translate::semantic::hir::{HirDocument, TriggerEnvironment, TypeFact};
use crate::translate::subquery::emit_non_from_clause_subquery;
use crate::translate::{
    emitter::Resolver, expr::emit_plan_column_value_decode, ProgramBuilder, ProgramBuilderOpts,
};
use crate::util::normalize_ident;
use crate::vdbe::insn::{Insn, Subprogram};
use crate::vdbe::BranchOffset;
use crate::{QueryMode, Result};
use std::num::NonZeroU32;
use turso_parser::ast::{self, TriggerEvent, TriggerTime};

/// Context for trigger execution
#[derive(Debug)]
pub struct TriggerContext {
    /// Table the trigger is attached to
    pub table: Arc<BTreeTable>,
    /// Frozen programs for reading values from the firing statement's target.
    pub read_programs: Arc<SourceReadPrograms>,
    /// NEW row registers (for INSERT/UPDATE). The last element is always the rowid.
    pub new_registers: Option<Vec<usize>>,
    /// OLD row registers (for UPDATE/DELETE). The last element is always the rowid.
    pub old_registers: Option<Vec<usize>>,
    /// Override conflict resolution for statements within this trigger.
    /// When set, all INSERT/UPDATE statements in the trigger will use this
    /// conflict resolution instead of their specified OR clause.
    /// This is needed for UPSERT DO UPDATE triggers where SQLite requires
    /// that nested OR IGNORE/REPLACE clauses do not suppress errors.
    pub override_conflict: Option<ast::ResolveType>,
    /// Whether NEW registers contain encoded custom type values that need decoding.
    /// True for AFTER triggers (values have been encoded for storage).
    /// False for BEFORE triggers (values are still user-facing).
    pub new_encoded: bool,
}

impl TriggerContext {
    pub fn new(
        table: Arc<BTreeTable>,
        read_programs: Arc<SourceReadPrograms>,
        new_registers: Option<Vec<usize>>,
        old_registers: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table,
            read_programs,
            new_registers,
            old_registers,
            override_conflict: None,
            new_encoded: false,
        }
    }

    /// Create a trigger context for AFTER triggers where NEW values are encoded.
    pub fn new_after(
        table: Arc<BTreeTable>,
        read_programs: Arc<SourceReadPrograms>,
        new_registers: Option<Vec<usize>>,
        old_registers: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table,
            read_programs,
            new_registers,
            old_registers,
            override_conflict: None,
            new_encoded: true,
        }
    }

    /// Create a trigger context with a conflict resolution override.
    /// Used for UPSERT DO UPDATE triggers where nested OR IGNORE/REPLACE
    /// clauses should not suppress errors.
    pub fn new_with_override_conflict(
        table: Arc<BTreeTable>,
        read_programs: Arc<SourceReadPrograms>,
        new_registers: Option<Vec<usize>>,
        old_registers: Option<Vec<usize>>,
        override_conflict: ast::ResolveType,
    ) -> Self {
        Self {
            table,
            read_programs,
            new_registers,
            old_registers,
            override_conflict: Some(override_conflict),
            new_encoded: false,
        }
    }

    /// Create a trigger context with a conflict resolution override for AFTER triggers.
    pub fn new_after_with_override_conflict(
        table: Arc<BTreeTable>,
        read_programs: Arc<SourceReadPrograms>,
        new_registers: Option<Vec<usize>>,
        old_registers: Option<Vec<usize>>,
        override_conflict: ast::ResolveType,
    ) -> Self {
        Self {
            table,
            read_programs,
            new_registers,
            old_registers,
            override_conflict: Some(override_conflict),
            new_encoded: true,
        }
    }
}

/// Bind semantic NEW/OLD sources to the fixed parameter layout used by a
/// trigger subprogram. Every visible column gets a slot, followed by that row
/// image's rowid slot. NEW always comes before OLD.
pub(crate) fn runtime_bindings_for_environment(
    document: &HirDocument,
    environment: &TriggerEnvironment,
    identities: &PlanIdentityMap,
) -> Result<PlanRuntimeBindings> {
    fn next_parameter(next: &mut u32, type_fact: TypeFact) -> Result<PlanParameter> {
        let index = NonZeroU32::new(*next).ok_or_else(|| {
            crate::LimboError::InternalError(
                "trigger parameter indices must start at one".to_string(),
            )
        })?;
        *next = next.checked_add(1).ok_or_else(|| {
            crate::LimboError::InternalError("trigger parameter index overflow".to_string())
        })?;
        Ok(PlanParameter {
            index,
            name: None,
            type_fact,
        })
    }

    fn bind_row(
        document: &HirDocument,
        identities: &PlanIdentityMap,
        bindings: &mut PlanRuntimeBindings,
        source: super::semantic::hir::SourceId,
        next: &mut u32,
    ) -> Result<()> {
        let source_definition = document.source(source).ok_or_else(|| {
            crate::LimboError::InternalError(format!("missing trigger pseudo-source {source}"))
        })?;
        let plan_source = identities.source(source).ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing plan identity for trigger pseudo-source {source}"
            ))
        })?;
        let columns = source_definition
            .columns
            .iter()
            .map(|column| {
                Ok(RuntimeValueBinding::Parameter(next_parameter(
                    next,
                    column.type_fact.clone(),
                )?))
            })
            .collect::<Result<Vec<_>>>()?;
        let rowid = RuntimeValueBinding::Parameter(next_parameter(
            next,
            TypeFact::known(crate::schema::Type::Integer),
        )?);
        if bindings
            .bind_row(
                plan_source,
                RuntimeRowBinding {
                    columns,
                    rowid: Some(rowid),
                    read_programs: None,
                },
            )
            .is_some()
        {
            return Err(crate::LimboError::InternalError(format!(
                "trigger pseudo-source {source} was bound more than once"
            )));
        }
        Ok(())
    }

    let mut bindings = PlanRuntimeBindings::default();
    let mut next = 1;
    if let Some(source) = environment.new_source {
        bind_row(document, identities, &mut bindings, source, &mut next)?;
    }
    if let Some(source) = environment.old_source {
        bind_row(document, identities, &mut bindings, source, &mut next)?;
    }
    Ok(bindings)
}

/// Bind a trigger predicate's semantic row images directly to the decoded
/// parent registers. Predicates run in the parent program, before OP_Program
/// enters the compiled trigger body, so they must not use subprogram
/// parameters.
fn runtime_register_bindings_for_environment(
    document: &HirDocument,
    environment: &TriggerEnvironment,
    identities: &PlanIdentityMap,
    ctx: &TriggerContext,
) -> Result<PlanRuntimeBindings> {
    fn bind_row(
        document: &HirDocument,
        identities: &PlanIdentityMap,
        bindings: &mut PlanRuntimeBindings,
        source: super::semantic::hir::SourceId,
        registers: Option<&[usize]>,
        image: &str,
    ) -> Result<()> {
        let source_definition = document.source(source).ok_or_else(|| {
            crate::LimboError::InternalError(format!("missing trigger pseudo-source {source}"))
        })?;
        let registers = registers.ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "{image} is visible to trigger analysis but has no runtime registers"
            ))
        })?;
        let expected = source_definition
            .columns
            .len()
            .checked_add(1)
            .ok_or_else(|| {
                crate::LimboError::InternalError("trigger register count overflow".to_string())
            })?;
        if registers.len() != expected {
            return Err(crate::LimboError::InternalError(format!(
                "{image} trigger row has {} registers, expected {expected}",
                registers.len()
            )));
        }
        let plan_source = identities.source(source).ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing plan identity for trigger pseudo-source {source}"
            ))
        })?;
        let rowid = *registers
            .last()
            .expect("a validated trigger row includes its rowid register");
        let row = RuntimeRowBinding {
            columns: registers[..source_definition.columns.len()]
                .iter()
                .map(|register| RuntimeValueBinding::Register {
                    register: *register,
                    needs_decode: false,
                })
                .collect(),
            rowid: Some(RuntimeValueBinding::Register {
                register: rowid,
                needs_decode: false,
            }),
            read_programs: None,
        };
        if bindings.bind_row(plan_source, row).is_some() {
            return Err(crate::LimboError::InternalError(format!(
                "trigger pseudo-source {source} was bound more than once"
            )));
        }
        Ok(())
    }

    let mut bindings = PlanRuntimeBindings::default();
    if let Some(source) = environment.new_source {
        bind_row(
            document,
            identities,
            &mut bindings,
            source,
            ctx.new_registers.as_deref(),
            "NEW",
        )?;
    }
    if let Some(source) = environment.old_source {
        bind_row(
            document,
            identities,
            &mut bindings,
            source,
            ctx.old_registers.as_deref(),
            "OLD",
        )?;
    }
    Ok(bindings)
}

/// Parent registers corresponding to [`runtime_bindings_for_environment`].
/// Keeping this complete and positional makes command compilation independent
/// of which NEW/OLD fields happen to be referenced by its syntax.
pub(crate) fn trigger_parameter_registers(ctx: &TriggerContext) -> Result<Vec<usize>> {
    fn append_row(
        output: &mut Vec<usize>,
        registers: &[usize],
        column_count: usize,
        image: &str,
    ) -> Result<()> {
        let expected = column_count.checked_add(1).ok_or_else(|| {
            crate::LimboError::InternalError("trigger register count overflow".to_string())
        })?;
        if registers.len() != expected {
            return Err(crate::LimboError::InternalError(format!(
                "{image} trigger row has {} registers, expected {expected}",
                registers.len()
            )));
        }
        output.extend_from_slice(registers);
        Ok(())
    }

    let column_count = ctx.table.columns().len();
    let capacity = (usize::from(ctx.new_registers.is_some())
        + usize::from(ctx.old_registers.is_some()))
    .checked_mul(column_count + 1)
    .ok_or_else(|| {
        crate::LimboError::InternalError("trigger parameter count overflow".to_string())
    })?;
    let mut registers = Vec::with_capacity(capacity);
    if let Some(new_registers) = ctx.new_registers.as_deref() {
        append_row(&mut registers, new_registers, column_count, "NEW")?;
    }
    if let Some(old_registers) = ctx.old_registers.as_deref() {
        append_row(&mut registers, old_registers, column_count, "OLD")?;
    }
    Ok(registers)
}

/// Execute trigger commands by compiling them as a subprogram and emitting Program instruction
/// Returns true if there are triggers that will fire.
#[turso_macros::trace_stack(detail = trigger_event_kind(&trigger.event))]
fn execute_trigger_commands(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    trigger: &Arc<Trigger>,
    ctx: &TriggerContext,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    ignore_jump_target: BranchOffset,
) -> Result<bool> {
    struct TriggerCompilationGuard {
        connection: Arc<crate::Connection>,
    }

    impl Drop for TriggerCompilationGuard {
        fn drop(&mut self) {
            self.connection.end_trigger_compilation();
        }
    }

    if connection.trigger_is_compiling(trigger) {
        // Do not recursively compile the same trigger
        return Ok(false);
    }
    connection.start_trigger_compilation(trigger.clone());
    let _trigger_compilation_guard = TriggerCompilationGuard {
        connection: connection.clone(),
    };

    let mut subprogram_builder = ProgramBuilder::new_for_trigger(
        QueryMode::Normal,
        program.capture_data_changes_info().clone(),
        ProgramBuilderOpts::new(1, 32, 2),
        trigger.clone(),
    );
    // If we have an override_conflict (e.g. from UPSERT DO UPDATE context),
    // propagate it to the subprogram so that nested trigger firing will also use it.
    if let Some(override_conflict) = ctx.override_conflict {
        subprogram_builder.set_trigger_conflict_override(override_conflict);
    }
    // Restrict table resolution to the trigger's database during subprogram compilation.
    // Temp triggers live in TEMP_DB_ID, regardless of which database the target table is in.
    let trigger_database_id = if trigger.temporary {
        crate::TEMP_DB_ID
    } else {
        database_id
    };
    let trigger_input = super::semantic::TriggerAnalysisInput {
        database_id,
        table: Arc::new(crate::schema::Table::BTree(ctx.table.clone())),
        new_visible: ctx.new_registers.is_some(),
        old_visible: ctx.old_registers.is_some(),
        override_conflict: ctx.override_conflict,
    };
    let semantic_context = resolver
        .semantic_context()
        .for_trigger(trigger_database_id, trigger.name.clone())
        .with_dml_policy(super::semantic::context::DmlPolicy::new(
            connection.is_nested_stmt(),
            connection.is_mvcc_bootstrap_connection(),
            false,
            connection.check_constraints_ignored(),
        ));
    let prev_trigger_context = resolver.trigger_context.clone();
    resolver.set_trigger_context(trigger_database_id, trigger.name.clone());
    let compile_result = (|| -> Result<()> {
        for command in trigger.commands.iter() {
            let document = super::semantic::analyze(
                &semantic_context,
                super::semantic::AnalyzeInput::TriggerCommand {
                    syntax: command,
                    trigger: &trigger_input,
                },
            )?;
            let resets_change_count = matches!(
                &document.root,
                super::semantic::hir::HirRoot::Insert(_)
                    | super::semantic::hir::HirRoot::Update(_)
                    | super::semantic::hir::HirRoot::Delete(_)
            );
            subprogram_builder.prologue();
            super::translate_hir_document(
                document,
                resolver,
                &mut subprogram_builder,
                connection,
                super::plan::QueryDestination::ResultRows,
            )?;
            if resets_change_count {
                subprogram_builder.emit_insn(Insn::ResetCount);
            }
        }
        Ok(())
    })();
    // Restore previous trigger context (supports nested triggers).
    resolver.trigger_context = prev_trigger_context;
    compile_result?;
    subprogram_builder.epilogue(resolver.schema());
    let built_subprogram =
        subprogram_builder.build(connection.clone(), true, "trigger subprogram")?;
    let subprogram_prepared = built_subprogram.prepared();

    // Trigger subprograms do not emit Transaction opcodes, so the parent statement
    // must acquire any attached/temp database transactions the trigger body needs
    // before OP_Program enters the subprogram.
    for db_id in &subprogram_prepared.write_databases {
        if db_id == crate::MAIN_DB_ID {
            program.begin_write_operation()?;
        } else {
            let schema_cookie = resolver.with_schema(db_id, |s| s.schema_version);
            program.begin_write_on_database(db_id, schema_cookie)?;
        }
    }
    for db_id in &subprogram_prepared.read_databases {
        if subprogram_prepared.write_databases.get(db_id) {
            continue;
        }
        if db_id == crate::MAIN_DB_ID {
            program.begin_read_operation()?;
        } else {
            let schema_cookie = resolver.with_schema(db_id, |s| s.schema_version);
            program.begin_read_on_database(db_id, schema_cookie)?;
        }
    }

    let param_registers = trigger_parameter_registers(ctx)?;

    program.emit_insn(Insn::Program {
        param_registers,
        program: Subprogram::PreparedProgram(built_subprogram.prepared().clone()),
        ignore_jump_target,
    });

    Ok(true)
}

/// Check if there are any triggers for a given event (regardless of time).
/// This is used during plan preparation to determine if materialization is needed.
pub fn has_relevant_triggers_type_only(
    schema: &crate::schema::Schema,
    event: TriggerEvent,
    updated_column_indices: Option<&ColumnMask>,
    table: &BTreeTable,
) -> bool {
    let mut triggers = schema.get_triggers_for_table(table.name.as_str());

    // Filter triggers by event
    triggers.any(|trigger| {
        // Check event matches
        let event_matches = match (&trigger.event, &event) {
            (TriggerEvent::Delete, TriggerEvent::Delete) => true,
            (TriggerEvent::Insert, TriggerEvent::Insert) => true,
            (TriggerEvent::Update, TriggerEvent::Update) => true,
            (TriggerEvent::UpdateOf(trigger_cols), TriggerEvent::Update) => {
                // For UPDATE OF, we need to check if any of the specified columns
                // are in the UPDATE SET clause
                let updated_cols =
                    updated_column_indices.expect("UPDATE should contain some updated columns");
                // Check if any of the trigger's specified columns are being updated
                trigger_cols.iter().any(|col_name| {
                    let normalized_col = normalize_ident(col_name.as_str());
                    if let Some((col_idx, _)) = table.get_column(&normalized_col) {
                        updated_cols.get(col_idx)
                    } else {
                        // Column doesn't exist - according to SQLite docs, unrecognized
                        // column names in UPDATE OF are silently ignored
                        false
                    }
                })
            }
            _ => false,
        };

        event_matches
    })
}

/// Check if there are any triggers for a given event (regardless of time).
/// This is used during plan preparation to determine if materialization is needed.
pub fn get_relevant_triggers_type_and_time<'a>(
    schema: &'a crate::schema::Schema,
    event: TriggerEvent,
    time: TriggerTime,
    updated_column_indices: Option<ColumnMask>,
    table: &'a BTreeTable,
) -> impl Iterator<Item = Arc<Trigger>> + 'a + Clone {
    let triggers = schema.get_triggers_for_table(table.name.as_str());

    // Filter triggers by event
    triggers
        .filter(move |trigger| -> bool {
            // Check event matches
            let event_matches = match (&trigger.event, &event) {
                (TriggerEvent::Delete, TriggerEvent::Delete) => true,
                (TriggerEvent::Insert, TriggerEvent::Insert) => true,
                (TriggerEvent::Update, TriggerEvent::Update) => true,
                (TriggerEvent::UpdateOf(trigger_cols), TriggerEvent::Update) => {
                    // For UPDATE OF, we need to check if any of the specified columns
                    // are in the UPDATE SET clause
                    if let Some(ref updated_cols) = updated_column_indices {
                        // Check if any of the trigger's specified columns are being updated
                        trigger_cols.iter().any(|col_name| {
                            let normalized_col = normalize_ident(col_name.as_str());
                            if let Some((col_idx, _)) = table.get_column(&normalized_col) {
                                updated_cols.get(col_idx)
                            } else {
                                // Column doesn't exist - according to SQLite docs, unrecognized
                                // column names in UPDATE OF are silently ignored
                                false
                            }
                        })
                    } else {
                        false
                    }
                }
                _ => false,
            };

            if !event_matches {
                return false;
            }

            trigger.time == time
        })
        .cloned()
}

/// Like [`get_relevant_triggers_type_and_time`], but also searches the temp
/// schema when `database_id != TEMP_DB_ID`.  Temp triggers on a non-temp
/// table are stored in the temp schema, so both schemas must be consulted
/// for DML on any table.  Returns a combined, de-duplicated list.
pub fn get_triggers_including_temp(
    resolver: &Resolver,
    database_id: usize,
    event: TriggerEvent,
    time: TriggerTime,
    updated_column_indices: Option<ColumnMask>,
    table: &BTreeTable,
) -> Vec<Arc<Trigger>> {
    let mut triggers: Vec<Arc<Trigger>> = resolver.with_schema(database_id, |s| {
        get_relevant_triggers_type_and_time(
            s,
            event.clone(),
            time,
            updated_column_indices.clone(),
            table,
        )
        .filter(|trigger| {
            // In the temp schema, triggers may target a different database.
            // Only include triggers whose target matches this database.
            match trigger.target_database_id {
                Some(target_db) => target_db == database_id,
                None => true, // unqualified → targets this schema's own table
            }
        })
        .collect()
    });
    if database_id != crate::TEMP_DB_ID && resolver.has_temp_database() {
        let temp_triggers: Vec<Arc<Trigger>> = resolver.with_schema(crate::TEMP_DB_ID, |s| {
            get_relevant_triggers_type_and_time(s, event, time, updated_column_indices, table)
                .filter(|trigger| match trigger.target_database_id {
                    // Explicit qualifier: include if it matches this database.
                    Some(target_db) => target_db == database_id,
                    // Unqualified: the trigger targets the temp schema's table if one
                    // exists, otherwise it targets main/attached. Include it only when
                    // no temp table with that name shadows it.
                    None => s.get_table(&trigger.table_name).is_none(),
                })
                .collect()
        });
        triggers.extend(temp_triggers);
    }
    triggers
}

/// Like [`has_relevant_triggers_type_only`], but also checks the temp schema.
pub fn has_triggers_including_temp(
    resolver: &Resolver,
    database_id: usize,
    event: TriggerEvent,
    updated_column_indices: Option<&ColumnMask>,
    table: &BTreeTable,
) -> bool {
    let found = resolver.with_schema(database_id, |s| {
        has_relevant_triggers_type_only(s, event.clone(), updated_column_indices, table)
    });
    if found {
        return true;
    }
    if database_id != crate::TEMP_DB_ID && resolver.has_temp_database() {
        // Check temp schema for triggers that target this database.
        let has_temp = resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_triggers_for_table(table.name.as_str())
                .any(|trigger| match trigger.target_database_id {
                    Some(target_db) => target_db == database_id,
                    None => s.get_table(&trigger.table_name).is_none(),
                })
        });
        if has_temp {
            return true;
        }
    }
    false
}

#[turso_macros::trace_stack(detail = trigger_event_kind(&trigger.event))]
pub fn fire_trigger(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    trigger: Arc<Trigger>,
    ctx: &TriggerContext,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    ignore_jump_target: BranchOffset,
) -> Result<()> {
    // Decode custom type registers so trigger bodies see user-facing values,
    // not raw encoded blobs from disk.
    // - OLD registers always come from cursor reads → always encoded → always decode
    // - NEW registers are only encoded for AFTER triggers (post-encode) → decode when new_encoded
    //
    // Column affinity for NEW registers is handled by the parent statement:
    // - Non-STRICT tables: INSERT/UPDATE emit Insn::Affinity before any trigger fires
    // - STRICT tables: no column affinity needed (apply_new_column_affinity was a no-op)
    // So we can use the decoded registers directly, skipping N Copy + 1 Affinity per fire.
    let ctx = &decode_trigger_registers(program, resolver, ctx)?;

    let result = (|| -> Result<()> {
        // Evaluate WHEN clause if present.
        if let Some(when_expr) = trigger.when_clause.as_ref() {
            crate::stack::trace_stack!("when_clause");
            let trigger_database_id = if trigger.temporary {
                crate::TEMP_DB_ID
            } else {
                database_id
            };
            let trigger_input = super::semantic::TriggerAnalysisInput {
                database_id,
                table: Arc::new(crate::schema::Table::BTree(ctx.table.clone())),
                new_visible: ctx.new_registers.is_some(),
                old_visible: ctx.old_registers.is_some(),
                override_conflict: ctx.override_conflict,
            };
            let semantic_context = resolver
                .semantic_context()
                .for_trigger(trigger_database_id, trigger.name.clone())
                .with_dml_policy(super::semantic::context::DmlPolicy::new(
                    connection.is_nested_stmt(),
                    connection.is_mvcc_bootstrap_connection(),
                    false,
                    connection.check_constraints_ignored(),
                ));
            let document = super::semantic::analyze(
                &semantic_context,
                super::semantic::AnalyzeInput::TriggerPredicate {
                    syntax: when_expr,
                    trigger: &trigger_input,
                },
            )?;
            let identities = program.allocate_plan_identities(&document);
            let super::semantic::hir::HirRoot::TriggerPredicate(predicate) = &document.root else {
                return Err(crate::LimboError::InternalError(
                    "trigger predicate analysis returned the wrong HIR root".to_string(),
                ));
            };
            let when_expr = super::plan_expr::lower_hir_expr(&predicate.expression, &identities)
                .map_err(|error| crate::LimboError::InternalError(error.to_string()))?;
            let runtime_bindings = runtime_register_bindings_for_environment(
                &document,
                &predicate.environment,
                &identities,
                ctx,
            )?;
            let skip_label = program.allocate_label();
            resolver.with_plan_runtime_bindings(runtime_bindings, |resolver| {
                let mut table_references = super::plan::TableReferences::new_empty();
                let mut subqueries = Vec::new();
                let mut hir_ctx =
                    super::planner::HirPlanContext::new(&document, &identities, program);
                super::subquery::prepare_hir_expression_subqueries(
                    &mut hir_ctx,
                    &mut table_references,
                    &[&when_expr],
                    super::plan::SubqueryOrigin::TriggerWhen,
                    &mut subqueries,
                )?;
                drop(hir_ctx);
                resolver.bind_plan_subqueries(&subqueries);

                // A trigger WHEN predicate is evaluated for each candidate row.
                // Do not let an uncorrelated subquery acquire a cross-row Once cache:
                // an earlier trigger body may have changed the tables it reads.
                for subquery in &mut subqueries {
                    subquery.correlated = true;
                    let plan = subquery.consume_plan(super::plan::EvalAt::BeforeLoop);
                    emit_non_from_clause_subquery(
                        program,
                        resolver,
                        *plan,
                        &subquery.query_type,
                        true,
                        false,
                    )?;
                }
                super::expr::translate_plan_condition_expr(
                    program,
                    None,
                    &when_expr,
                    super::expr::ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true: skip_label,
                        jump_target_when_false: skip_label,
                        jump_target_when_null: skip_label,
                    },
                    resolver,
                )
            })?;

            // Execute trigger commands if WHEN clause is true
            execute_trigger_commands(
                program,
                resolver,
                &trigger,
                ctx,
                connection,
                database_id,
                ignore_jump_target,
            )?;

            program.preassign_label_to_next_insn(skip_label);
        } else {
            // No WHEN clause - always execute
            execute_trigger_commands(
                program,
                resolver,
                &trigger,
                ctx,
                connection,
                database_id,
                ignore_jump_target,
            )?;
        }

        Ok(())
    })();
    result
}

fn trigger_event_kind(event: &TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Delete => "delete",
        TriggerEvent::Insert => "insert",
        TriggerEvent::Update => "update",
        TriggerEvent::UpdateOf(_) => "update_of",
    }
}

/// Decode one encoded trigger row with the firing target's frozen read programs.
/// Only scalar columns with an actual decoder need a copied register. Arrays
/// remain in their plan-native record representation.
fn decode_trigger_row_registers(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    ctx: &TriggerContext,
    registers: &[usize],
    image: &str,
) -> Result<Vec<usize>> {
    let column_count = ctx.table.columns().len();
    let expected = column_count.checked_add(1).ok_or_else(|| {
        crate::LimboError::InternalError("trigger register count overflow".to_string())
    })?;
    if registers.len() != expected {
        return Err(crate::LimboError::InternalError(format!(
            "{image} trigger row has {} registers, expected {expected}",
            registers.len()
        )));
    }
    if ctx.read_programs.column_type_programs.len() != column_count {
        return Err(crate::LimboError::InternalError(format!(
            "trigger target read programs have {} columns, expected {column_count}",
            ctx.read_programs.column_type_programs.len()
        )));
    }

    let mut decoded = Vec::with_capacity(expected);
    for (column, source_register) in registers[..column_count].iter().copied().enumerate() {
        let Some(programs) = &ctx.read_programs.column_type_programs[column] else {
            decoded.push(source_register);
            continue;
        };
        if programs.array.is_some() || programs.decode.is_empty() {
            decoded.push(source_register);
            continue;
        }

        let decoded_register = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: source_register,
            dst_reg: decoded_register,
            extra_amount: 0,
        });
        emit_plan_column_value_decode(program, None, programs, decoded_register, resolver)?;
        decoded.push(decoded_register);
    }
    decoded.push(registers[column_count]);
    Ok(decoded)
}

/// Decode encoded custom type registers in a TriggerContext.
/// OLD registers are always decoded (they always come from cursor reads on disk).
/// NEW registers are decoded only when `ctx.new_encoded` is true (AFTER triggers).
fn decode_trigger_registers(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    ctx: &TriggerContext,
) -> Result<TriggerContext> {
    let decoded_new = if ctx.new_encoded {
        if let Some(new_regs) = &ctx.new_registers {
            Some(decode_trigger_row_registers(
                program, resolver, ctx, new_regs, "NEW",
            )?)
        } else {
            None
        }
    } else {
        ctx.new_registers.clone()
    };

    let decoded_old = if let Some(old_regs) = &ctx.old_registers {
        Some(decode_trigger_row_registers(
            program, resolver, ctx, old_regs, "OLD",
        )?)
    } else {
        None
    };

    Ok(TriggerContext {
        table: ctx.table.clone(),
        read_programs: Arc::clone(&ctx.read_programs),
        new_registers: decoded_new,
        old_registers: decoded_old,
        override_conflict: ctx.override_conflict,
        new_encoded: false, // decoded now
    })
}
