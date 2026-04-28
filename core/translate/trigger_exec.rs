use crate::schema::{BTreeTable, Trigger};
use crate::sync::Arc;
use crate::translate::expr::WalkControl;
use crate::translate::plan::ColumnMask;
use crate::translate::subquery::{
    emit_non_from_clause_subquery, plan_subqueries_from_trigger_when_clause,
};
use crate::translate::{
    emitter::Resolver,
    expr::{self, translate_expr, walk_expr_mut},
    planner::ROWID_STRS,
    translate_inner, ProgramBuilder, ProgramBuilderOpts,
};
use crate::util::normalize_ident;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::insn::Insn;
use crate::vdbe::BranchOffset;
use crate::{bail_parse_error, QueryMode, Result};
use std::cell::RefCell;
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

/// Allocates parameter indices on demand for NEW/OLD column references.
/// Only columns actually referenced in the trigger body get assigned a parameter,
/// reducing bind_at calls from N (total columns) to K (referenced columns).
#[derive(Debug)]
struct ParamAllocator {
    /// For each column index, the assigned 1-based parameter index, or None if unreferenced.
    new_entries: Vec<Option<NonZero<usize>>>,
    /// Parameter index for NEW.rowid, if referenced.
    new_rowid: Option<NonZero<usize>>,
    /// For each column index, the assigned 1-based parameter index, or None if unreferenced.
    old_entries: Vec<Option<NonZero<usize>>>,
    /// Parameter index for OLD.rowid, if referenced.
    old_rowid: Option<NonZero<usize>>,
    /// Next parameter index to assign (starts at 1).
    next_param: usize,
}

impl ParamAllocator {
    fn new(num_cols: usize, has_new: bool, has_old: bool) -> Self {
        Self {
            new_entries: if has_new {
                vec![None; num_cols]
            } else {
                vec![]
            },
            new_rowid: None,
            old_entries: if has_old {
                vec![None; num_cols]
            } else {
                vec![]
            },
            old_rowid: None,
            next_param: 1,
        }
    }

    fn alloc_new(&mut self, col_idx: usize) -> NonZero<usize> {
        *self.new_entries[col_idx].get_or_insert_with(|| {
            let p = NonZero::new(self.next_param).unwrap();
            self.next_param += 1;
            p
        })
    }

    fn alloc_new_rowid(&mut self) -> NonZero<usize> {
        *self.new_rowid.get_or_insert_with(|| {
            let p = NonZero::new(self.next_param).unwrap();
            self.next_param += 1;
            p
        })
    }

    fn alloc_old(&mut self, col_idx: usize) -> NonZero<usize> {
        *self.old_entries[col_idx].get_or_insert_with(|| {
            let p = NonZero::new(self.next_param).unwrap();
            self.next_param += 1;
            p
        })
    }

    fn alloc_old_rowid(&mut self) -> NonZero<usize> {
        *self.old_rowid.get_or_insert_with(|| {
            let p = NonZero::new(self.next_param).unwrap();
            self.next_param += 1;
            p
        })
    }

    /// Total number of parameters allocated so far.
    fn num_params(&self) -> usize {
        self.next_param - 1
    }
}

/// Context for compiling trigger subprograms - maps NEW/OLD to parameter indices
#[derive(Debug)]
struct TriggerSubprogramContext {
    /// Sparse parameter allocator (allocates on demand during AST rewrite)
    param_alloc: RefCell<ParamAllocator>,
    /// Whether this trigger has NEW registers
    has_new: bool,
    /// Whether this trigger has OLD registers
    has_old: bool,
    table: Arc<BTreeTable>,
    /// Override conflict resolution for statements within this trigger.
    override_conflict: Option<ast::ResolveType>,
    /// Database name for the trigger's database (used to qualify unqualified table names in body)
    db_name: Option<ast::Name>,
}

fn variable_from_parameter_index(index: NonZero<usize>, col_type: Option<&str>) -> Expr {
    let nz = u32::try_from(index.get())
        .ok()
        .and_then(std::num::NonZeroU32::new)
        .expect("trigger parameter index must fit into NonZeroU32");
    match col_type {
        Some(ty) => Expr::Variable(ast::Variable::indexed_typed(nz, ty)),
        None => Expr::Variable(ast::Variable::indexed(nz)),
    }
}

impl TriggerSubprogramContext {
    pub fn get_new_param(&self, idx: usize) -> Option<NonZero<usize>> {
        if !self.has_new {
            return None;
        }
        Some(self.param_alloc.borrow_mut().alloc_new(idx))
    }

    pub fn get_new_rowid_param(&self) -> Option<NonZero<usize>> {
        if !self.has_new {
            return None;
        }
        Some(self.param_alloc.borrow_mut().alloc_new_rowid())
    }

    pub fn get_old_param(&self, idx: usize) -> Option<NonZero<usize>> {
        if !self.has_old {
            return None;
        }
        Some(self.param_alloc.borrow_mut().alloc_old(idx))
    }

    pub fn get_old_rowid_param(&self) -> Option<NonZero<usize>> {
        if !self.has_old {
            return None;
        }
        Some(self.param_alloc.borrow_mut().alloc_old_rowid())
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
            // Rewrite NEW/OLD references anywhere an UPDATE trigger body can
            // legally read them: SET, FROM-derived sources, and WHERE.
            let mut sets_clone = sets.clone();
            for set in &mut sets_clone {
                rewrite_trigger_expr_for_subprogram(&mut set.expr, subprogram_ctx)?;
            }

            let mut from_clone = from.clone();
            if let Some(ref mut from_clause) = from_clone {
                rewrite_from_clause_expressions(from_clause, &mut |e: &mut ast::Expr| {
                    rewrite_trigger_expr_single_for_subprogram(e, subprogram_ctx)
                })?;
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
                from: from_clone,
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
                if ctx.has_new {
                    let num_cols = ctx.table.columns().len();
                    if let Some((idx, col_def)) = ctx.table.get_column(&col) {
                        let ty = Some(col_def.ty_str.as_str());
                        if col_def.is_rowid_alias() {
                            *e = variable_from_parameter_index(
                                ctx.get_new_rowid_param()
                                    .expect("NEW parameters must be provided"),
                                ty,
                            );
                            return Ok(());
                        }
                        if idx < num_cols {
                            *e = variable_from_parameter_index(
                                ctx.get_new_param(idx)
                                    .expect("NEW parameters must be provided"),
                                ty,
                            );
                            return Ok(());
                        } else {
                            crate::bail_parse_error!("no such column: {}.{}", ns, col);
                        }
                    }
                    // Handle NEW.rowid
                    if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&col)) {
                        *e = variable_from_parameter_index(
                            ctx.get_new_rowid_param()
                                .expect("NEW parameters must be provided"),
                            None,
                        );
                        return Ok(());
                    }
                    bail_parse_error!("no such column: {}.{}", ns, col);
                } else {
                    bail_parse_error!(
                        "NEW references are only valid in INSERT and UPDATE triggers"
                    );
                }
            }

            // Handle OLD.column references
            if ns.eq_ignore_ascii_case("old") {
                if ctx.has_old {
                    let num_cols = ctx.table.columns().len();
                    if let Some((idx, col_def)) = ctx.table.get_column(&col) {
                        let ty = Some(col_def.ty_str.as_str());
                        if col_def.is_rowid_alias() {
                            *e = variable_from_parameter_index(
                                ctx.get_old_rowid_param()
                                    .expect("OLD parameters must be provided"),
                                ty,
                            );
                            return Ok(());
                        }
                        if idx < num_cols {
                            *e = variable_from_parameter_index(
                                ctx.get_old_param(idx)
                                    .expect("OLD parameters must be provided"),
                                ty,
                            );
                            return Ok(());
                        } else {
                            crate::bail_parse_error!("no such column: {}.{}", ns, col)
                        }
                    }
                    // Handle OLD.rowid
                    if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&col)) {
                        *e = variable_from_parameter_index(
                            ctx.get_old_rowid_param()
                                .expect("OLD parameters must be provided"),
                            None,
                        );
                        return Ok(());
                    }
                    bail_parse_error!("no such column: {}.{}", ns, col);
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
    let num_cols = ctx.table.columns().len();

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
    // Parameter indices are allocated on demand during the AST rewrite.
    // Only columns actually referenced in the trigger body get a parameter,
    // reducing bind_at calls from N (all columns) to K (referenced columns).
    let subprogram_ctx = TriggerSubprogramContext {
        param_alloc: RefCell::new(ParamAllocator::new(num_cols, has_new, has_old)),
        has_new,
        has_old,
        table: ctx.table.clone(),
        override_conflict: ctx.override_conflict,
        db_name,
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
    let prev_trigger_context = resolver.trigger_context.clone();
    resolver.set_trigger_context(trigger_database_id, trigger.name.clone());
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
            program.begin_write_operation();
        } else {
            let schema_cookie = resolver.with_schema(db_id, |s| s.schema_version);
            program.begin_write_on_database(db_id, schema_cookie);
        }
    }
    for db_id in &subprogram_prepared.read_databases {
        if subprogram_prepared.write_databases.get(db_id) {
            continue;
        }
        if db_id == crate::MAIN_DB_ID {
            program.begin_read_operation();
        } else {
            let schema_cookie = resolver.with_schema(db_id, |s| s.schema_version);
            program.begin_read_on_database(db_id, schema_cookie);
        }
    }

    // Build the param_registers Vec from the sparse allocator: maps each parameter
    // index to the parent register that holds the value.
    let alloc = subprogram_ctx.param_alloc.borrow();
    let total_params = alloc.num_params();
    let mut param_registers = vec![0usize; total_params];

    if let Some(new_regs) = &ctx.new_registers {
        for (col_idx, opt_param) in alloc.new_entries.iter().enumerate() {
            if let Some(param_idx) = opt_param {
                param_registers[param_idx.get() - 1] = new_regs[col_idx];
            }
        }
        if let Some(param_idx) = alloc.new_rowid {
            param_registers[param_idx.get() - 1] = *new_regs.last().unwrap();
        }
    }
    if let Some(old_regs) = &ctx.old_registers {
        for (col_idx, opt_param) in alloc.old_entries.iter().enumerate() {
            if let Some(param_idx) = opt_param {
                param_registers[param_idx.get() - 1] = old_regs[col_idx];
            }
        }
        if let Some(param_idx) = alloc.old_rowid {
            param_registers[param_idx.get() - 1] = *old_regs.last().unwrap();
        }
    }
    drop(alloc);

    program.emit_insn(Insn::Program {
        param_registers,
        program: built_subprogram.prepared().clone(),
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
    populate_trigger_register_affinities(resolver, ctx);
    let result = (|| -> Result<()> {
        // Evaluate WHEN clause if present
        if let Some(mut when_expr) = trigger.when_clause.clone() {
            // Rewrite NEW/OLD references in WHEN clause to use registers
            rewrite_trigger_expr_for_when_clause(&mut when_expr, &ctx.table, ctx)?;

            // Plan and emit any subqueries in the WHEN clause (e.g. IN (SELECT ...), EXISTS, scalar subqueries).
            // This transforms InSelect/Exists/Subquery nodes into SubqueryResult nodes that translate_expr can handle.
            let mut subqueries = Vec::new();
            plan_subqueries_from_trigger_when_clause(
                program,
                &mut subqueries,
                &mut when_expr,
                resolver,
                connection,
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
    result
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

fn populate_trigger_row_register_affinities(
    resolver: &mut Resolver,
    table: &BTreeTable,
    row_registers: Option<&[usize]>,
) {
    let Some(registers) = row_registers else {
        return;
    };

    for (idx, column) in table.columns().iter().enumerate() {
        let affinity = if column.is_rowid_alias() {
            Affinity::Integer
        } else {
            column.affinity_with_strict(table.is_strict)
        };
        if let Some(&register) = registers.get(idx) {
            resolver.register_affinities.insert(register, affinity);
        }
    }

    if let Some(&rowid_register) = registers.last() {
        resolver
            .register_affinities
            .insert(rowid_register, Affinity::Integer);
    }
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
