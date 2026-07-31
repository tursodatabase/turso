use crate::schema::{BTreeTable, Trigger};
use crate::sync::Arc;
use crate::translate::bind::TriggerProgramBinder;
use crate::translate::plan::ColumnMask;
use crate::translate::subquery::{
    emit_non_from_clause_subquery, plan_subqueries_from_trigger_when_clause,
};
use crate::translate::{
    emitter::Resolver,
    expr::{self, translate_expr},
    translate_inner, ProgramBuilder, ProgramBuilderOpts,
};
use crate::util::normalize_ident;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::insn::{Insn, Subprogram};
use crate::vdbe::BranchOffset;
use crate::{QueryMode, Result};
use turso_parser::ast::{self, TriggerEvent, TriggerTime};

/// Context for trigger execution
#[derive(Debug)]
pub struct TriggerContext {
    /// Table the trigger is attached to
    pub table: Arc<BTreeTable>,
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
        new_registers: Option<Vec<usize>>,
        old_registers: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table,
            new_registers,
            old_registers,
            override_conflict: None,
            new_encoded: false,
        }
    }

    /// Create a trigger context for AFTER triggers where NEW values are encoded.
    pub fn new_after(
        table: Arc<BTreeTable>,
        new_registers: Option<Vec<usize>>,
        old_registers: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table,
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
        new_registers: Option<Vec<usize>>,
        old_registers: Option<Vec<usize>>,
        override_conflict: ast::ResolveType,
    ) -> Self {
        Self {
            table,
            new_registers,
            old_registers,
            override_conflict: Some(override_conflict),
            new_encoded: false,
        }
    }

    /// Create a trigger context with a conflict resolution override for AFTER triggers.
    pub fn new_after_with_override_conflict(
        table: Arc<BTreeTable>,
        new_registers: Option<Vec<usize>>,
        old_registers: Option<Vec<usize>>,
        override_conflict: ast::ResolveType,
    ) -> Self {
        Self {
            table,
            new_registers,
            old_registers,
            override_conflict: Some(override_conflict),
            new_encoded: true,
        }
    }
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

    let has_new = ctx.new_registers.is_some();
    let has_old = ctx.old_registers.is_some();

    // Ordinary non-main triggers need unqualified DML targets rewritten into the
    // trigger's schema. Temp-backed triggers intentionally keep unqualified names
    // unresolved so they can follow SQLite's normal temp/main lookup rules.
    let db_name = if database_id == crate::MAIN_DB_ID || database_id == crate::TEMP_DB_ID {
        None
    } else {
        resolver
            .get_database_name_by_index(database_id)
            .map(ast::Name::exact)
    };
    let trigger_binder = TriggerProgramBinder::new(
        ctx.table.clone(),
        has_new,
        has_old,
        ctx.override_conflict,
        db_name,
    );
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
    let prev_trigger_context = resolver.trigger_context.clone();
    resolver.set_trigger_context(trigger_database_id, trigger.name.clone());
    let compile_result = (|| -> Result<()> {
        for command in trigger.commands.iter() {
            let stmt = trigger_binder.bind_command(command)?;
            subprogram_builder.prologue();
            translate_inner(
                stmt,
                resolver,
                &mut subprogram_builder,
                connection,
                "trigger subprogram",
            )?;
            if matches!(
                command,
                ast::TriggerCmd::Insert { .. }
                    | ast::TriggerCmd::Update { .. }
                    | ast::TriggerCmd::Delete { .. }
            ) {
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

    let param_registers = trigger_binder
        .parameter_registers(ctx.new_registers.as_deref(), ctx.old_registers.as_deref());

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

    let saved_register_affinities = std::mem::take(&mut resolver.register_affinities);
    let saved_register_collations = std::mem::take(&mut resolver.register_collations);
    populate_trigger_register_affinities(resolver, ctx);
    let result = (|| -> Result<()> {
        // Evaluate WHEN clause if present
        if let Some(mut when_expr) = trigger.when_clause.clone() {
            crate::stack::trace_stack!("when_clause");
            let mut bound_subqueries = crate::translate::bind::bind_trigger_when_clause(
                &mut when_expr,
                ctx.table.clone(),
                ctx.new_registers.as_deref(),
                ctx.old_registers.as_deref(),
                resolver,
                program,
            )?;

            // Plan and emit any subqueries in the WHEN clause (e.g. IN (SELECT ...), EXISTS, scalar subqueries).
            let mut subqueries = Vec::new();
            plan_subqueries_from_trigger_when_clause(
                program,
                &mut subqueries,
                &mut when_expr,
                resolver,
                connection,
                &mut bound_subqueries,
            )?;
            // Emit the planned subqueries so their results are available when we evaluate the WHEN expression.
            // Always treat these as correlated (no `Once` caching) because the WHEN clause is evaluated
            // per-row, and trigger bodies may modify the tables referenced by the subquery between evaluations.
            for subquery in &mut subqueries {
                let plan = subquery.consume_plan(crate::translate::plan::EvalAt::BeforeLoop);
                emit_non_from_clause_subquery(
                    program,
                    resolver,
                    *plan,
                    &subquery.query_type,
                    true, // always re-evaluate: trigger WHEN is checked per-row
                    false,
                )?;
            }

            let when_reg = program.alloc_register();
            translate_expr(program, None, &when_expr, when_reg, resolver)?;

            let skip_label = program.allocate_label();
            program.emit_insn(Insn::IfNot {
                reg: when_reg,
                jump_if_null: true,
                target_pc: skip_label,
            });

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
    resolver.register_affinities = saved_register_affinities;
    resolver.register_collations = saved_register_collations;
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

/// Decode encoded custom type registers in a TriggerContext.
/// OLD registers are always decoded (they always come from cursor reads on disk).
/// NEW registers are decoded only when `ctx.new_encoded` is true (AFTER triggers).
fn decode_trigger_registers(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    ctx: &TriggerContext,
) -> Result<TriggerContext> {
    if !ctx.table.is_strict {
        // Non-STRICT tables never have custom type encoding
        return Ok(TriggerContext {
            table: ctx.table.clone(),
            new_registers: ctx.new_registers.clone(),
            old_registers: ctx.old_registers.clone(),
            override_conflict: ctx.override_conflict,
            new_encoded: false,
        });
    }

    let columns = ctx.table.columns();

    let decoded_new = if ctx.new_encoded {
        if let Some(new_regs) = &ctx.new_registers {
            let rowid_reg = *new_regs.last().expect("NEW registers must include rowid");
            Some(expr::emit_trigger_decode_registers(
                program,
                resolver,
                columns,
                &|i| new_regs[i],
                rowid_reg,
                true, // is_strict
            )?)
        } else {
            None
        }
    } else {
        ctx.new_registers.clone()
    };

    let decoded_old = if let Some(old_regs) = &ctx.old_registers {
        let rowid_reg = *old_regs.last().expect("OLD registers must include rowid");
        Some(expr::emit_trigger_decode_registers(
            program,
            resolver,
            columns,
            &|i| old_regs[i],
            rowid_reg,
            true, // is_strict
        )?)
    } else {
        None
    };

    Ok(TriggerContext {
        table: ctx.table.clone(),
        new_registers: decoded_new,
        old_registers: decoded_old,
        override_conflict: ctx.override_conflict,
        new_encoded: false, // decoded now
    })
}

fn populate_trigger_register_affinities(resolver: &mut Resolver, ctx: &TriggerContext) {
    populate_trigger_row_register_affinities(resolver, &ctx.table, ctx.new_registers.as_deref());
    populate_trigger_row_register_affinities(resolver, &ctx.table, ctx.old_registers.as_deref());
}

// NEW/OLD columns don't have affinities, except for rowids and rowid aliases,
// which have INTEGER affinity. See https://www.sqlite.org/forum/forumpost/819f2d6627
fn populate_trigger_row_register_affinities(
    resolver: &mut Resolver,
    table: &BTreeTable,
    row_registers: Option<&[usize]>,
) {
    let Some(registers) = row_registers else {
        return;
    };

    for (idx, column) in table.columns().iter().enumerate() {
        if !column.is_rowid_alias() {
            continue;
        }
        if let Some(&register) = registers.get(idx) {
            resolver
                .register_affinities
                .insert(register, Affinity::Integer);
        }
    }

    if let Some(&rowid_register) = registers.last() {
        resolver
            .register_affinities
            .insert(rowid_register, Affinity::Integer);
    }
}
