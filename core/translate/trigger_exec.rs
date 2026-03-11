use crate::schema::{BTreeTable, Trigger};
use crate::sync::Arc;
use crate::translate::expr::WalkControl;
use crate::translate::{
    emitter::Resolver,
    expr::{self, translate_expr, walk_expr_mut},
    planner::ROWID_STRS,
    translate_inner, ProgramBuilder, ProgramBuilderOpts,
};
use crate::util::normalize_ident;
use crate::vdbe::insn::Insn;
use crate::vdbe::BranchOffset;
use crate::HashSet;
use crate::{bail_parse_error, QueryMode, Result};
use std::num::NonZero;
use turso_parser::ast::{self, Expr, TriggerEvent, TriggerTime};

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

#[derive(Debug)]
struct ParamMap(Vec<NonZero<usize>>);

impl ParamMap {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Context for compiling trigger subprograms - maps NEW/OLD to parameter indices
#[derive(Debug)]
struct TriggerSubprogramContext {
    /// Map from column index to parameter index for NEW values (1-indexed)
    new_param_map: Option<ParamMap>,
    /// Map from column index to parameter index for OLD values (1-indexed)
    old_param_map: Option<ParamMap>,
    table: Arc<BTreeTable>,
    /// Override conflict resolution for statements within this trigger.
    override_conflict: Option<ast::ResolveType>,
    /// Database name for the trigger's database (used to qualify unqualified table names in body)
    db_name: Option<ast::Name>,
}

impl TriggerSubprogramContext {
    pub fn get_new_param(&self, idx: usize) -> Option<NonZero<usize>> {
        self.new_param_map
            .as_ref()
            .and_then(|map| map.0.get(idx).copied())
    }

    pub fn get_new_rowid_param(&self) -> Option<NonZero<usize>> {
        self.new_param_map
            .as_ref()
            .and_then(|map| map.0.last().copied())
    }

    pub fn get_old_param(&self, idx: usize) -> Option<NonZero<usize>> {
        self.old_param_map
            .as_ref()
            .and_then(|map| map.0.get(idx).copied())
    }

    pub fn get_old_rowid_param(&self) -> Option<NonZero<usize>> {
        self.old_param_map
            .as_ref()
            .and_then(|map| map.0.last().copied())
    }
}

/// Rewrite NEW and OLD references in trigger expressions to use Variable instructions (parameters)
fn rewrite_trigger_expr_for_subprogram(
    expr: &mut ast::Expr,
    ctx: &TriggerSubprogramContext,
) -> Result<()> {
    walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
        rewrite_trigger_expr_single_for_subprogram(e, ctx)?;
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Rewrite NEW/OLD references in all expressions within an Upsert clause for subprogram
fn rewrite_upsert_exprs_for_subprogram(
    upsert: &mut Option<Box<ast::Upsert>>,
    ctx: &TriggerSubprogramContext,
) -> Result<()> {
    let mut current = upsert.as_mut();
    while let Some(u) = current {
        if let ast::UpsertDo::Set {
            ref mut sets,
            ref mut where_clause,
        } = u.do_clause
        {
            for set in sets.iter_mut() {
                rewrite_trigger_expr_for_subprogram(&mut set.expr, ctx)?;
            }
            if let Some(ref mut wc) = where_clause {
                rewrite_trigger_expr_for_subprogram(wc, ctx)?;
            }
        }
        if let Some(ref mut idx) = u.index {
            if let Some(ref mut wc) = idx.where_clause {
                rewrite_trigger_expr_for_subprogram(wc, ctx)?;
            }
        }
        current = u.next.as_mut();
    }
    Ok(())
}

/// Convert TriggerCmd to Stmt, rewriting NEW/OLD to Variable expressions (for subprogram compilation)
fn trigger_cmd_to_stmt_for_subprogram(
    cmd: &ast::TriggerCmd,
    subprogram_ctx: &TriggerSubprogramContext,
) -> Result<ast::Stmt> {
    use ast::{InsertBody, QualifiedName};

    match cmd {
        ast::TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            col_names,
            select,
            upsert,
            returning,
        } => {
            // Rewrite NEW/OLD references in the SELECT
            let mut select_clone = select.clone();
            rewrite_expressions_in_select_for_subprogram(&mut select_clone, subprogram_ctx)?;

            // Rewrite NEW/OLD references in the UPSERT clause (if present)
            let mut upsert_clone = upsert.clone();
            rewrite_upsert_exprs_for_subprogram(&mut upsert_clone, subprogram_ctx)?;

            let body = InsertBody::Select(select_clone, upsert_clone);
            // If override_conflict is set (e.g., in UPSERT DO UPDATE context),
            // use it instead of the command's or_conflict to ensure errors propagate.
            let effective_or_conflict = subprogram_ctx.override_conflict.or(*or_conflict);
            Ok(ast::Stmt::Insert {
                with: None,
                or_conflict: effective_or_conflict,
                tbl_name: QualifiedName {
                    db_name: subprogram_ctx.db_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                columns: col_names.clone(),
                body,
                returning: returning.clone(),
            })
        }
        ast::TriggerCmd::Update {
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause,
        } => {
            // Rewrite NEW/OLD references in SET clauses and WHERE clause
            let mut sets_clone = sets.clone();
            for set in &mut sets_clone {
                rewrite_trigger_expr_for_subprogram(&mut set.expr, subprogram_ctx)?;
            }

            let mut where_clause_clone = where_clause.clone();
            if let Some(ref mut where_expr) = where_clause_clone {
                rewrite_trigger_expr_for_subprogram(where_expr, subprogram_ctx)?;
            }

            // If override_conflict is set (e.g., in UPSERT DO UPDATE context),
            // use it instead of the command's or_conflict to ensure errors propagate.
            let effective_or_conflict = subprogram_ctx.override_conflict.or(*or_conflict);
            Ok(ast::Stmt::Update(ast::Update {
                with: None,
                or_conflict: effective_or_conflict,
                tbl_name: QualifiedName {
                    db_name: subprogram_ctx.db_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                indexed: None,
                sets: sets_clone,
                from: from.clone(),
                where_clause: where_clause_clone,
                returning: vec![],
                order_by: vec![],
                limit: None,
            }))
        }
        ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => {
            // Rewrite NEW/OLD references in WHERE clause
            let mut where_clause_clone = where_clause.clone();
            if let Some(ref mut where_expr) = where_clause_clone {
                rewrite_trigger_expr_for_subprogram(where_expr, subprogram_ctx)?;
            }

            Ok(ast::Stmt::Delete {
                tbl_name: QualifiedName {
                    db_name: subprogram_ctx.db_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                where_clause: where_clause_clone,
                limit: None,
                returning: vec![],
                indexed: None,
                order_by: vec![],
                with: None,
            })
        }
        ast::TriggerCmd::Select(select) => {
            // Rewrite NEW/OLD references in the SELECT
            let mut select_clone = select.clone();
            rewrite_expressions_in_select_for_subprogram(&mut select_clone, subprogram_ctx)?;
            Ok(ast::Stmt::Select(select_clone))
        }
    }
}

/// Rewrite NEW/OLD references in all expressions within a SELECT statement for subprogram
fn rewrite_expressions_in_select_for_subprogram(
    select: &mut ast::Select,
    ctx: &TriggerSubprogramContext,
) -> Result<()> {
    rewrite_select_expressions(select, &mut |e: &mut ast::Expr| {
        rewrite_trigger_expr_single_for_subprogram(e, ctx)
    })
}

/// Rewrite a single NEW/OLD reference for subprogram (called from walk_expr_mut)
fn rewrite_trigger_expr_single_for_subprogram(
    e: &mut ast::Expr,
    ctx: &TriggerSubprogramContext,
) -> Result<()> {
    match e {
        Expr::Exists(select) | Expr::Subquery(select) => {
            rewrite_expressions_in_select_for_subprogram(select, ctx)?;
            return Ok(());
        }
        Expr::InSelect { rhs, .. } => {
            rewrite_expressions_in_select_for_subprogram(rhs, ctx)?;
            return Ok(());
        }
        Expr::Qualified(ns, col) | Expr::DoublyQualified(_, ns, col) => {
            let ns = normalize_ident(ns.as_str());
            let col = normalize_ident(col.as_str());

            // Handle NEW.column references
            if ns.eq_ignore_ascii_case("new") {
                if let Some(new_params) = &ctx.new_param_map {
                    if let Some((idx, col_def)) = ctx.table.get_column(&col) {
                        if col_def.is_rowid_alias() {
                            *e = Expr::Variable(format!(
                                "{}",
                                ctx.get_new_rowid_param()
                                    .expect("NEW parameters must be provided")
                            ));
                            return Ok(());
                        }
                        if idx < new_params.len() {
                            *e = Expr::Variable(format!(
                                "{}",
                                ctx.get_new_param(idx)
                                    .expect("NEW parameters must be provided")
                                    .get()
                            ));
                            return Ok(());
                        } else {
                            crate::bail_parse_error!("no such column in NEW: {}", col);
                        }
                    }
                    // Handle NEW.rowid
                    if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&col)) {
                        *e = Expr::Variable(format!(
                            "{}",
                            ctx.get_new_rowid_param()
                                .expect("NEW parameters must be provided")
                        ));
                        return Ok(());
                    }
                    bail_parse_error!("no such column in NEW: {}", col);
                } else {
                    bail_parse_error!(
                        "NEW references are only valid in INSERT and UPDATE triggers"
                    );
                }
            }

            // Handle OLD.column references
            if ns.eq_ignore_ascii_case("old") {
                if let Some(old_params) = &ctx.old_param_map {
                    if let Some((idx, col_def)) = ctx.table.get_column(&col) {
                        if col_def.is_rowid_alias() {
                            *e = Expr::Variable(format!(
                                "{}",
                                ctx.get_old_rowid_param()
                                    .expect("OLD parameters must be provided")
                            ));
                            return Ok(());
                        }
                        if idx < old_params.len() {
                            *e = Expr::Variable(format!(
                                "{}",
                                ctx.get_old_param(idx)
                                    .expect("OLD parameters must be provided")
                                    .get()
                            ));
                            return Ok(());
                        } else {
                            crate::bail_parse_error!("no such column in OLD: {}", col)
                        }
                    }
                    // Handle OLD.rowid
                    if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&col)) {
                        *e = Expr::Variable(format!(
                            "{}",
                            ctx.get_old_rowid_param()
                                .expect("OLD parameters must be provided")
                        ));
                        return Ok(());
                    }
                    bail_parse_error!("no such column in OLD: {}", col);
                } else {
                    bail_parse_error!(
                        "OLD references are only valid in UPDATE and DELETE triggers"
                    );
                }
            }

            // If the namespace is neither NEW nor OLD, this can be a regular
            // table-qualified reference inside the SELECT statement of the
            // trigger subprogram. Leave it untouched so the normal SELECT
            // binding/resolution phase can handle it.
            return Ok(());
        }
        _ => {}
    }
    Ok(())
}

/// Execute trigger commands by compiling them as a subprogram and emitting Program instruction
/// Returns true if there are triggers that will fire.
fn execute_trigger_commands(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    trigger: &Arc<Trigger>,
    ctx: &TriggerContext,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    ignore_jump_target: BranchOffset,
) -> Result<bool> {
    if connection.trigger_is_compiling(trigger) {
        // Do not recursively compile the same trigger
        return Ok(false);
    }
    connection.start_trigger_compilation(trigger.clone());
    // Build parameter mapping: parameters are 1-indexed and sequential
    // Order: [NEW values..., OLD values..., rowid]
    // So if we have 2 NEW columns, 2 OLD columns: NEW params are 1,2; OLD params are 3,4; rowid is 5
    let num_new = ctx.new_registers.as_ref().map(|r| r.len()).unwrap_or(0);

    let new_param_map = ctx
        .new_registers
        .as_ref()
        .map(|new_regs| {
            (1..=new_regs.len())
                .map(|i| NonZero::new(i).unwrap())
                .collect()
        })
        .map(ParamMap);

    let old_param_map = ctx
        .old_registers
        .as_ref()
        .map(|old_regs| {
            (1..=old_regs.len())
                .map(|i| NonZero::new(i + num_new).unwrap())
                .collect()
        })
        .map(ParamMap);

    // For triggers on attached databases, resolve the database name so unqualified
    // table references in the trigger body are correctly qualified to the trigger's database.
    let db_name = if database_id == crate::MAIN_DB_ID {
        None
    } else {
        resolver
            .get_database_name_by_index(database_id)
            .map(ast::Name::exact)
    };
    let subprogram_ctx = TriggerSubprogramContext {
        new_param_map,
        old_param_map,
        table: ctx.table.clone(),
        override_conflict: ctx.override_conflict,
        db_name,
    };
    let mut subprogram_builder = ProgramBuilder::new_for_trigger(
        QueryMode::Normal,
        program.capture_data_changes_info().clone(),
        ProgramBuilderOpts {
            num_cursors: 1,
            approx_num_insns: 32,
            approx_num_labels: 2,
        },
        trigger.clone(),
    );
    // If we have an override_conflict (e.g. from UPSERT DO UPDATE context),
    // propagate it to the subprogram so that nested trigger firing will also use it.
    if let Some(override_conflict) = ctx.override_conflict {
        subprogram_builder.set_trigger_conflict_override(override_conflict);
    }
    // Restrict table resolution to the trigger's database during subprogram compilation.
    let prev_trigger_context = resolver.trigger_context.clone();
    resolver.set_trigger_context(database_id, trigger.name.clone());
    let compile_result = (|| -> Result<()> {
        for command in trigger.commands.iter() {
            let stmt = trigger_cmd_to_stmt_for_subprogram(command, &subprogram_ctx)?;
            subprogram_builder.prologue();
            translate_inner(
                stmt,
                resolver,
                &mut subprogram_builder,
                connection,
                "trigger subprogram",
            )?;
        }
        Ok(())
    })();
    // Restore previous trigger context (supports nested triggers).
    resolver.trigger_context = prev_trigger_context;
    compile_result?;
    subprogram_builder.epilogue(resolver.schema());
    let built_subprogram =
        subprogram_builder.build(connection.clone(), true, "trigger subprogram")?;

    let mut params = Vec::with_capacity(
        ctx.new_registers.as_ref().map(|r| r.len()).unwrap_or(0)
            + ctx.old_registers.as_ref().map(|r| r.len()).unwrap_or(0),
    );
    if let Some(new_regs) = &ctx.new_registers {
        params.extend(
            new_regs
                .iter()
                .copied()
                .map(|reg_idx| crate::types::Value::from_i64(reg_idx as i64)),
        );
    }
    if let Some(old_regs) = &ctx.old_registers {
        params.extend(
            old_regs
                .iter()
                .copied()
                .map(|reg_idx| crate::types::Value::from_i64(reg_idx as i64)),
        );
    }

    program.emit_insn(Insn::Program {
        params,
        program: built_subprogram.prepared().clone(),
        ignore_jump_target,
    });
    connection.end_trigger_compilation();

    Ok(true)
}

/// Check if there are any triggers for a given event (regardless of time).
/// This is used during plan preparation to determine if materialization is needed.
pub fn has_relevant_triggers_type_only(
    schema: &crate::schema::Schema,
    event: TriggerEvent,
    updated_column_indices: Option<&HashSet<usize>>,
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
                        updated_cols.contains(&col_idx)
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
    updated_column_indices: Option<HashSet<usize>>,
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
                                updated_cols.contains(&col_idx)
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
    let decoded_ctx = decode_trigger_registers(program, resolver, ctx)?;
    let ctx = &decoded_ctx;

    // Evaluate WHEN clause if present
    if let Some(mut when_expr) = trigger.when_clause.clone() {
        // Rewrite NEW/OLD references in WHEN clause to use registers
        rewrite_trigger_expr_for_when_clause(&mut when_expr, &ctx.table, ctx)?;

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

    let columns = &ctx.table.columns;

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

/// Rewrite NEW/OLD references in WHEN clause expressions (uses Register expressions, not Variable)
fn rewrite_trigger_expr_for_when_clause(
    expr: &mut ast::Expr,
    table: &BTreeTable,
    ctx: &TriggerContext,
) -> Result<()> {
    walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
        rewrite_trigger_expr_single_for_when_clause(e, table, ctx, false)?;
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Rewrite NEW/OLD references in all expressions within a SELECT statement for trigger WHEN clauses.
fn rewrite_expressions_in_select_for_when_clause(
    select: &mut ast::Select,
    table: &BTreeTable,
    ctx: &TriggerContext,
) -> Result<()> {
    rewrite_select_expressions(select, &mut |e: &mut ast::Expr| {
        rewrite_trigger_expr_single_for_when_clause(e, table, ctx, true)
    })
}

/// Rewrite all expressions in a SELECT tree, including CTEs, compounds, ORDER BY,
/// LIMIT/OFFSET, FROM/JOIN subqueries, and window clauses.
fn rewrite_select_expressions<F>(select: &mut ast::Select, rewrite_expr: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    // Rewrite WITH clause (CTEs)
    if let Some(with_clause) = &mut select.with {
        for cte in &mut with_clause.ctes {
            rewrite_select_expressions(&mut cte.select, rewrite_expr)?;
        }
    }

    rewrite_one_select_expressions(&mut select.body.select, rewrite_expr)?;

    // Rewrite compound SELECT arms (UNION/EXCEPT/INTERSECT)
    for compound in &mut select.body.compounds {
        rewrite_one_select_expressions(&mut compound.select, rewrite_expr)?;
    }

    // Rewrite top-level ORDER BY
    for sorted_col in &mut select.order_by {
        rewrite_expression_tree(&mut sorted_col.expr, rewrite_expr)?;
    }

    // Rewrite top-level LIMIT/OFFSET
    if let Some(limit) = &mut select.limit {
        rewrite_expression_tree(&mut limit.expr, rewrite_expr)?;
        if let Some(offset) = &mut limit.offset {
            rewrite_expression_tree(offset, rewrite_expr)?;
        }
    }

    Ok(())
}

fn rewrite_one_select_expressions<F>(
    one_select: &mut ast::OneSelect,
    rewrite_expr: &mut F,
) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            for col in columns {
                if let ast::ResultColumn::Expr(expr, _) = col {
                    rewrite_expression_tree(expr, rewrite_expr)?;
                }
            }

            if let Some(from_clause) = from {
                rewrite_from_clause_expressions(from_clause, rewrite_expr)?;
            }

            if let Some(where_expr) = where_clause {
                rewrite_expression_tree(where_expr, rewrite_expr)?;
            }

            if let Some(group_by) = group_by {
                for expr in &mut group_by.exprs {
                    rewrite_expression_tree(expr, rewrite_expr)?;
                }
                if let Some(having_expr) = &mut group_by.having {
                    rewrite_expression_tree(having_expr, rewrite_expr)?;
                }
            }

            for window_def in window_clause {
                rewrite_window_expressions(&mut window_def.window, rewrite_expr)?;
            }
        }
        ast::OneSelect::Values(values) => {
            for row in values {
                for expr in row {
                    rewrite_expression_tree(expr, rewrite_expr)?;
                }
            }
        }
    }

    Ok(())
}

fn rewrite_from_clause_expressions<F>(
    from_clause: &mut ast::FromClause,
    rewrite_expr: &mut F,
) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    rewrite_select_table_expressions(&mut from_clause.select, rewrite_expr)?;

    for join in &mut from_clause.joins {
        rewrite_select_table_expressions(&mut join.table, rewrite_expr)?;
        if let Some(ast::JoinConstraint::On(expr)) = &mut join.constraint {
            rewrite_expression_tree(expr, rewrite_expr)?;
        }
    }

    Ok(())
}

fn rewrite_select_table_expressions<F>(
    select_table: &mut ast::SelectTable,
    rewrite_expr: &mut F,
) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    match select_table {
        ast::SelectTable::Table(..) => {}
        ast::SelectTable::TableCall(_, args, _) => {
            for arg in args {
                rewrite_expression_tree(arg, rewrite_expr)?;
            }
        }
        ast::SelectTable::Select(select, _) => {
            rewrite_select_expressions(select, rewrite_expr)?;
        }
        ast::SelectTable::Sub(from_clause, _) => {
            rewrite_from_clause_expressions(from_clause, rewrite_expr)?;
        }
    }
    Ok(())
}

fn rewrite_window_expressions<F>(window: &mut ast::Window, rewrite_expr: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    for expr in &mut window.partition_by {
        rewrite_expression_tree(expr, rewrite_expr)?;
    }

    for sorted_col in &mut window.order_by {
        rewrite_expression_tree(&mut sorted_col.expr, rewrite_expr)?;
    }

    if let Some(frame_clause) = &mut window.frame_clause {
        rewrite_frame_bound_expressions(&mut frame_clause.start, rewrite_expr)?;
        if let Some(end) = &mut frame_clause.end {
            rewrite_frame_bound_expressions(end, rewrite_expr)?;
        }
    }

    Ok(())
}

fn rewrite_frame_bound_expressions<F>(
    frame_bound: &mut ast::FrameBound,
    rewrite_expr: &mut F,
) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    match frame_bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            rewrite_expression_tree(expr, rewrite_expr)?;
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => {}
    }
    Ok(())
}

fn rewrite_expression_tree<F>(expr: &mut ast::Expr, rewrite_expr: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    walk_expr_mut(
        expr,
        &mut |e: &mut ast::Expr| -> Result<expr::WalkControl> {
            rewrite_expr(e)?;
            Ok(WalkControl::Continue)
        },
    )?;

    Ok(())
}

fn rewrite_trigger_expr_single_for_when_clause(
    expr: &mut ast::Expr,
    table: &BTreeTable,
    ctx: &TriggerContext,
    allow_non_trigger_qualified: bool,
) -> Result<()> {
    match expr {
        // Bare column references are not valid in trigger WHEN clauses.
        // Per SQLite docs, columns must be qualified with NEW or OLD.
        Expr::Id(name) if !allow_non_trigger_qualified => {
            let ident = normalize_ident(name.as_str());
            if table.get_column(&ident).is_some()
                || ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&ident))
            {
                crate::bail_parse_error!("no such column: {}", ident);
            }
            return Ok(());
        }
        Expr::Exists(select) | Expr::Subquery(select) => {
            rewrite_expressions_in_select_for_when_clause(select, table, ctx)?;
            return Ok(());
        }
        Expr::InSelect { rhs, .. } => {
            rewrite_expressions_in_select_for_when_clause(rhs, table, ctx)?;
            return Ok(());
        }
        Expr::Qualified(ns, col) | Expr::DoublyQualified(_, ns, col) => {
            let ns = normalize_ident(ns.as_str());
            let col = normalize_ident(col.as_str());

            // Handle NEW.column references
            if ns.eq_ignore_ascii_case("new") {
                if let Some(new_regs) = &ctx.new_registers {
                    if let Some((idx, col_def)) = table.get_column(&col) {
                        if col_def.is_rowid_alias() {
                            // Rowid alias columns map to the rowid register (last element)
                            *expr = Expr::Register(
                                *new_regs.last().expect("NEW registers must be provided"),
                            );
                            return Ok(());
                        }
                        if idx < new_regs.len() {
                            *expr = Expr::Register(new_regs[idx]);
                            return Ok(());
                        }
                    }
                    // Handle NEW.rowid
                    if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&col)) {
                        *expr = Expr::Register(
                            *ctx.new_registers
                                .as_ref()
                                .expect("NEW registers must be provided")
                                .last()
                                .expect("NEW registers must be provided"),
                        );
                        return Ok(());
                    }
                    bail_parse_error!("no such column in NEW: {}", col);
                } else {
                    bail_parse_error!(
                        "NEW references are only valid in INSERT and UPDATE triggers"
                    );
                }
            }

            // Handle OLD.column references
            if ns.eq_ignore_ascii_case("old") {
                if let Some(old_regs) = &ctx.old_registers {
                    if let Some((idx, _)) = table.get_column(&col) {
                        if idx < old_regs.len() {
                            *expr = Expr::Register(old_regs[idx]);
                            return Ok(());
                        }
                    }
                    // Handle OLD.rowid
                    if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&col)) {
                        *expr = Expr::Register(
                            *ctx.old_registers
                                .as_ref()
                                .expect("OLD registers must be provided")
                                .last()
                                .expect("OLD registers must be provided"),
                        );
                        return Ok(());
                    }
                    bail_parse_error!("no such column in OLD: {}", col);
                } else {
                    bail_parse_error!(
                        "OLD references are only valid in UPDATE and DELETE triggers"
                    );
                }
            }

            if !allow_non_trigger_qualified {
                bail_parse_error!("no such column: {ns}.{col}");
            }
        }
        _ => {}
    }

    Ok(())
}
