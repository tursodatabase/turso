use crate::schema::ColumnLayout;
use crate::translate::emitter::{
    emit_index_column_value_new_image, emit_index_column_value_old_image, gencol,
};
use crate::turso_debug_assert;
use crate::{
    error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE},
    schema::{
        self, BTreeTable, ColDef, Column, Index, ResolvedFkRef, Table, SQLITE_SEQUENCE_TABLE_NAME,
    },
    sync::Arc,
    translate::{
        emitter::{
            delete::emit_fk_child_decrement_on_delete, emit_cdc_autocommit_commit,
            emit_cdc_full_record, emit_cdc_insns, emit_cdc_patch_record, emit_check_constraints,
            emit_make_record, prepare_cdc_if_necessary, OperationMode, Resolver,
        },
        expr::{
            emit_plan_column_value_decode, emit_plan_source_encode_columns, emit_returning_results,
            emit_returning_scan_back, restore_returning_row_image_in_cache,
            seed_returning_row_image_in_cache, translate_plan_condition_expr, translate_plan_expr,
            translate_plan_expr_no_constant_opt, ConditionMetadata, NoConstantOptReason,
            ReturningBufferCtx,
        },
        fkeys::{
            build_index_affinity_string, emit_fk_restrict_halt, emit_fk_violation,
            emit_guarded_fk_decrement, emit_skip_if_any_null, index_probe, index_scan_match_any,
            open_read_index, open_read_table, ForeignKeyActions,
        },
        plan::{
            EphemeralRowidMode, EvalAt, JoinedTable, NonFromClauseSubquery, Plan,
            PlanRuntimeBindings, QueryDestination, ResultSetColumn, RuntimeRowBinding,
            RuntimeValueBinding, SubqueryEvalPhase, TableReferences,
        },
        plan_expr::{PlanExpr, PlanIdentityMap, PlanSourceId},
        planner::ROWID_STRS,
        stmt_journal::{any_index_or_ipk_has_replace, set_insert_stmt_journal_flags},
        subquery::{
            emit_non_from_clause_subqueries_for_eval_at, emit_non_from_clause_subqueries_for_phase,
            emit_non_from_clause_subquery,
        },
        trigger_exec::{fire_trigger, get_triggers_including_temp, TriggerContext},
        upsert::{
            emit_upsert, resolved_upsert_target, PlannedUpsertAction, PlannedUpsertDo,
            ResolvedUpsertTarget,
        },
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorKey, CursorType, DmlColumnContext, ProgramBuilder, ProgramBuilderOpts},
        insn::{to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral},
        BranchOffset,
    },
    CaptureDataChangesExt, Connection, LimboError, Result, VirtualTable,
};
use gencol::compute_planned_virtual_columns;
use std::num::NonZeroUsize;
use turso_macros::{turso_assert, turso_assert_eq};
use turso_parser::ast::{self, ResolveType, TriggerEvent, TriggerTime};

pub struct TempTableCtx {
    cursor_id: usize,
    loop_start_label: BranchOffset,
    loop_end_label: BranchOffset,
}

/// Labels for INSERT loop control flow
pub struct InsertLoopLabels {
    /// Beginning of the loop for multiple-row inserts
    pub loop_start: BranchOffset,
    /// Label to jump to when a row is done processing (either inserted or upserted)
    pub row_done: BranchOffset,
    /// Jump here at the complete end of the statement
    pub stmt_epilogue: BranchOffset,
    /// Jump here when the insert value SELECT source has been fully exhausted
    pub select_exhausted: Option<BranchOffset>,
}

/// Labels for rowid/key generation flow
pub struct InsertKeyLabels {
    /// Label to jump to when a generated key is ready for uniqueness check
    pub key_ready_for_check: BranchOffset,
    /// Label to jump to when no key is provided and one must be generated
    pub key_generation: BranchOffset,
}

#[allow(dead_code)]
pub struct InsertEmitCtx<'a> {
    /// Frozen semantic target metadata for this statement.
    pub target: &'a JoinedTable,
    /// Parent table being inserted into
    pub table: &'a Arc<BTreeTable>,

    /// Frozen indexes belonging to the target table.
    pub indexes: Vec<crate::translate::semantic::hir::ResolvedIndex>,

    /// Index cursors we need to populate for this table
    /// (idx name, root_page, idx cursor id)
    pub idx_cursors: Vec<(String, i64, usize)>,

    /// Context for if the insert values are materialized first
    /// into a temporary table
    pub temp_table_ctx: Option<TempTableCtx>,
    /// on conflict, default to ABORT
    pub on_conflict: ResolveType,
    /// The original statement-level ON CONFLICT clause (None = no explicit clause)
    pub statement_on_conflict: Option<ResolveType>,
    /// Arity of the insert values
    pub num_values: usize,
    /// The yield register, if a coroutine is used to yield multiple rows
    pub yield_reg_opt: Option<usize>,
    /// The register to hold the rowid of a conflicting row
    pub conflict_rowid_reg: usize,
    /// The cursor id of the table being inserted into
    pub cursor_id: usize,

    /// Label to jump to on HALT
    pub halt_label: BranchOffset,
    /// Labels for loop control flow
    pub loop_labels: InsertLoopLabels,
    /// Labels for key generation flow
    pub key_labels: InsertKeyLabels,

    /// CDC table info
    pub cdc_table: Option<(usize, Arc<BTreeTable>)>,
    /// Autoincrement sequence table info
    pub autoincrement_meta: Option<AutoincMeta>,
    /// The database index (0 = main, 1 = temp, 2+ = attached)
    pub database_id: usize,
    /// Ephemeral table for buffering RETURNING results.
    /// When present, RETURNING rows are buffered into an ephemeral table during the DML loop,
    /// then scanned back and yielded to the caller after all DML is complete.
    pub returning_buffer: Option<ReturningBufferCtx>,
}

impl<'a> InsertEmitCtx<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        program: &mut ProgramBuilder,
        target: &'a JoinedTable,
        on_conflict: Option<ResolveType>,
        cdc_table: Option<(usize, Arc<BTreeTable>)>,
        num_values: usize,
        temp_table_ctx: Option<TempTableCtx>,
        database_id: usize,
    ) -> Result<Self> {
        let Table::BTree(table) = &target.table else {
            return Err(LimboError::InternalError(
                "INSERT target is not a B-tree table".to_string(),
            ));
        };
        // allocate cursor id's for each btree index cursor we'll need to populate the indexes
        let indices: Vec<_> = target
            .index_expressions
            .iter()
            .map(|planned| planned.index.clone())
            .collect();
        let mut idx_cursors = Vec::new();
        for idx in &indices {
            idx_cursors.push((
                idx.value().name.clone(),
                idx.value().root_page,
                program.alloc_cursor_index(None, &idx.handle())?,
            ));
        }
        let loop_labels = InsertLoopLabels {
            loop_start: program.allocate_label(),
            row_done: program.allocate_label(),
            stmt_epilogue: program.allocate_label(),
            select_exhausted: None,
        };
        let key_labels = InsertKeyLabels {
            key_ready_for_check: program.allocate_label(),
            key_generation: program.allocate_label(),
        };
        Ok(Self {
            target,
            table,
            indexes: indices,
            idx_cursors,
            temp_table_ctx,
            on_conflict: on_conflict.unwrap_or(ResolveType::Abort),
            statement_on_conflict: on_conflict,
            yield_reg_opt: None,
            conflict_rowid_reg: program.alloc_register(),
            cursor_id: 0, // set later in emit_source_emission
            halt_label: program.allocate_label(),
            loop_labels,
            key_labels,
            cdc_table,
            num_values,
            autoincrement_meta: None,
            database_id,
            returning_buffer: None,
        })
    }
}

enum PlannedInsertSource {
    Single(Vec<PlanExpr>),
    MaterializedValues {
        rows: Vec<Vec<PlanExpr>>,
        cursor_id: usize,
    },
    MaterializedQuery {
        plan: Plan,
        cursor_id: usize,
    },
}

impl PlannedInsertSource {
    fn is_multiple(&self) -> bool {
        !matches!(self, Self::Single(_))
    }

    fn width(&self) -> usize {
        match self {
            Self::Single(values) => values.len(),
            Self::MaterializedValues { rows, .. } => rows.first().map_or(0, Vec::len),
            Self::MaterializedQuery { plan, .. } => plan.select_result_columns().len(),
        }
    }
}

fn lower_insert_expr(
    expr: &super::semantic::hir::Expr,
    identities: &PlanIdentityMap,
) -> Result<PlanExpr> {
    super::plan_expr::lower_hir_expr(expr, identities)
        .map_err(|error| LimboError::InternalError(error.to_string()))
}

fn lower_insert_defaults(
    statement: &super::semantic::hir::Insert,
    identities: &PlanIdentityMap,
    column_count: usize,
) -> Result<Vec<Option<PlanExpr>>> {
    let mut defaults = vec![None; column_count];
    for default in &statement.defaults {
        let slot = defaults.get_mut(default.column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "INSERT default column {} is outside the target table",
                default.column
            ))
        })?;
        *slot = Some(lower_insert_expr(&default.value, identities)?);
    }
    Ok(defaults)
}

fn default_for_insert_target(
    target: super::semantic::hir::TargetColumn,
    defaults: &[Option<PlanExpr>],
) -> Result<PlanExpr> {
    match target {
        super::semantic::hir::TargetColumn::RowId => Ok(PlanExpr::Literal(ast::Literal::Null)),
        super::semantic::hir::TargetColumn::Column(column) => defaults
            .get(column)
            .and_then(Option::as_ref)
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "INSERT destination column {column} has no planned default"
                ))
            }),
    }
}

fn lower_upsert_actions(
    statement: &super::semantic::hir::Insert,
    identities: &PlanIdentityMap,
    program: &mut ProgramBuilder,
) -> Result<Vec<PlannedUpsertAction>> {
    statement
        .upserts
        .iter()
        .map(|upsert| {
            let action = match &upsert.action {
                super::semantic::hir::UpsertAction::Nothing => PlannedUpsertDo::Nothing,
                super::semantic::hir::UpsertAction::Update {
                    assignments,
                    predicate,
                } => {
                    let sets = super::update::collect_update_set_clauses(assignments, identities)?
                        .into_iter()
                        .map(|set| (set.column_index, set.expr))
                        .collect();
                    PlannedUpsertDo::Update {
                        sets,
                        predicate: predicate
                            .as_ref()
                            .map(|expr| lower_insert_expr(expr, identities))
                            .transpose()?,
                    }
                }
            };
            Ok((
                resolved_upsert_target(upsert),
                program.allocate_label(),
                action,
            ))
        })
        .collect()
}

#[turso_macros::trace_stack]
pub fn translate_insert(
    document: super::semantic::hir::HirDocument,
    identities: &PlanIdentityMap,
    resolver: &mut Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let opts = ProgramBuilderOpts::new(1, 30, 5);
    program.extend(&opts);

    let super::semantic::hir::HirRoot::Insert(statement) = &document.root else {
        return Err(LimboError::InternalError(
            "INSERT translator received a non-INSERT HIR root".to_string(),
        ));
    };
    let target_definition = document.source(statement.target).ok_or_else(|| {
        LimboError::InternalError(format!("missing INSERT target source {}", statement.target))
    })?;
    let database_id = target_definition
        .database
        .map_or(crate::MAIN_DB_ID, |database| database.index());
    let resolved_table = match &target_definition.kind {
        super::semantic::hir::SourceKind::Table(table) => table.clone(),
        _ => {
            return Err(LimboError::InternalError(
                "INSERT target is not a resolved table".to_string(),
            ));
        }
    };
    let table = resolved_table.handle();
    let table_name = table.get_name().to_string();
    let defaults = lower_insert_defaults(statement, identities, table.columns().len())?;

    let needs_materialization = matches!(
        &statement.source,
        super::semantic::hir::InsertSource::Query(_)
    ) || matches!(
        &statement.source,
        super::semantic::hir::InsertSource::Values(rows) if rows.len() > 1
    );
    let source_temp_cursor = if needs_materialization {
        let btree = table.btree().ok_or_else(|| {
            LimboError::ParseError(
                "virtual tables only support a single VALUES row in INSERT".to_string(),
            )
        })?;
        Some(program.alloc_cursor_id(CursorType::BTreeTable(btree)))
    } else {
        None
    };

    let mut hir_ctx = super::planner::HirPlanContext::new(&document, identities, program);
    let target = super::planner::prepare_hir_source(&mut hir_ctx, statement.target, None)?;
    let mut table_references = hir_ctx.new_table_references(vec![target.clone()], vec![])?;
    let mut source_subqueries = Vec::new();
    let mut source = match &statement.source {
        super::semantic::hir::InsertSource::DefaultValues => PlannedInsertSource::Single(
            statement
                .columns
                .iter()
                .copied()
                .map(|column| default_for_insert_target(column, &defaults))
                .collect::<Result<Vec<_>>>()?,
        ),
        super::semantic::hir::InsertSource::Values(rows) => {
            let rows = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| lower_insert_expr(expr, identities))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            let expressions = rows.iter().flatten().collect::<Vec<_>>();
            super::subquery::prepare_hir_expression_subqueries(
                &mut hir_ctx,
                &mut table_references,
                &expressions,
                super::plan::SubqueryOrigin::DmlSet,
                &mut source_subqueries,
            )?;
            if rows.len() == 1 {
                PlannedInsertSource::Single(rows.into_iter().next().unwrap())
            } else {
                PlannedInsertSource::MaterializedValues {
                    rows,
                    cursor_id: source_temp_cursor.expect("multiple VALUES has a temp cursor"),
                }
            }
        }
        super::semantic::hir::InsertSource::Query(query) => {
            let btree_table = table
                .btree()
                .expect("query INSERT target is a B-tree table");
            let cursor_id = source_temp_cursor.expect("query INSERT has a temp cursor");
            let plan = super::planner::prepare_hir_query_plan(
                &mut hir_ctx,
                *query,
                QueryDestination::EphemeralTable {
                    cursor_id,
                    table: btree_table,
                    rowid_mode: EphemeralRowidMode::Auto,
                },
            )?;
            PlannedInsertSource::MaterializedQuery { plan, cursor_id }
        }
    };
    if source.width() != statement.columns.len() {
        return Err(LimboError::InternalError(format!(
            "planned INSERT source has {} columns for {} destinations",
            source.width(),
            statement.columns.len()
        )));
    }

    let mut result_columns = statement
        .returning
        .as_ref()
        .map(|returning| {
            returning
                .outputs
                .iter()
                .map(|output| super::update::lower_output(output, identities))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    for output in &result_columns {
        table_references.register_plan_expr_usage(&output.expr)?;
    }
    let mut returning_subqueries = Vec::new();
    let returning_expressions = result_columns
        .iter()
        .map(|output| &output.expr)
        .collect::<Vec<_>>();
    super::subquery::prepare_hir_expression_subqueries(
        &mut hir_ctx,
        &mut table_references,
        &returning_expressions,
        super::plan::SubqueryOrigin::DmlReturning,
        &mut returning_subqueries,
    )?;
    drop(hir_ctx);

    let mut upsert_actions = lower_upsert_actions(statement, identities, program)?;
    let mut upsert_subqueries = Vec::with_capacity(upsert_actions.len());
    let mut upsert_hir_ctx = super::planner::HirPlanContext::new(&document, identities, program);
    for (_, _, action) in &upsert_actions {
        let expressions = match action {
            PlannedUpsertDo::Nothing => Vec::new(),
            PlannedUpsertDo::Update { sets, predicate } => sets
                .iter()
                .map(|(_, expr)| expr)
                .chain(predicate.iter())
                .collect::<Vec<_>>(),
        };
        let mut subqueries = Vec::new();
        super::subquery::prepare_hir_expression_subqueries(
            &mut upsert_hir_ctx,
            &mut table_references,
            &expressions,
            super::plan::SubqueryOrigin::DmlSet,
            &mut subqueries,
        )?;
        upsert_subqueries.push(subqueries);
    }
    drop(upsert_hir_ctx);

    // INSERT does not have one enclosing Plan node, so its VALUES and RETURNING
    // subqueries share the statement resolver scope. Each UPSERT action adds its
    // own bindings later, alongside that action's CURRENT/EXCLUDED row images.
    resolver.bind_plan_subqueries(&source_subqueries);
    resolver.bind_plan_subqueries(&returning_subqueries);

    if let PlannedInsertSource::MaterializedQuery { plan, .. } = &mut source {
        super::optimizer::optimize_plan(program, plan, resolver)?;
    }

    let excluded_source = statement
        .excluded_source
        .map(|source| {
            identities.source(source).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "missing plan identity for EXCLUDED source {source}"
                ))
            })
        })
        .transpose()?;
    let on_conflict = statement.conflict;
    let inserting_multiple_rows = source.is_multiple();

    let fk_enabled = connection.foreign_keys_enabled();
    if let Some(virtual_table) = &table.virtual_table() {
        emit_non_from_clause_subqueries_for_phase(
            program,
            resolver,
            &mut source_subqueries,
            &[],
            Some(&table_references),
            SubqueryEvalPhase::PreWrite,
            |_| true,
        )?;
        translate_virtual_table_insert(
            program,
            virtual_table.clone(),
            &statement.columns,
            &defaults,
            source,
            on_conflict,
            resolver,
            connection,
        )?;
        return Ok(());
    }

    let Some(btree_table) = table.btree() else {
        crate::bail_parse_error!("no such table: {}", table_name);
    };

    let is_mvcc = connection.mv_store_for_db(database_id).is_some();

    if inserting_multiple_rows && btree_table.has_autoincrement && !is_mvcc {
        ensure_sequence_initialized(program, resolver, &btree_table, database_id)?;
    }

    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), Some(table.get_name()))?;

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let has_fks = fk_enabled
        && (resolver.with_schema(database_id, |s| s.has_child_fks(table_name.as_str()))
            || resolver.with_schema(database_id, |s| {
                s.any_resolved_fks_referencing(table_name.as_str())
            }));
    if !btree_table.has_rowid {
        if has_fks {
            crate::bail_parse_error!("foreign keys on WITHOUT ROWID tables are not supported");
        }
        if on_conflict == Some(ResolveType::Replace) || !upsert_actions.is_empty() {
            crate::bail_parse_error!(
                "UPSERT and REPLACE on WITHOUT ROWID tables are not supported"
            );
        }
        if cdc_table.is_some() {
            crate::bail_parse_error!("CDC on WITHOUT ROWID tables is not supported");
        }
        if !resolver
            .schema()
            .get_dependent_materialized_views(table_name.as_str())
            .is_empty()
        {
            crate::bail_parse_error!(
                "materialized views on WITHOUT ROWID tables are not supported"
            );
        }
        if !target.index_expressions.is_empty() {
            crate::bail_parse_error!("secondary indexes on WITHOUT ROWID tables are not supported");
        }
    }

    let mut ctx = InsertEmitCtx::new(
        program,
        &target,
        on_conflict,
        cdc_table,
        source.width(),
        None,
        database_id,
    )?;
    program
        .flags
        .set_has_statement_conflict(on_conflict.is_some());

    // Open an ephemeral table for buffering RETURNING results.
    // All DML completes before any RETURNING rows are yielded to the caller.
    if !result_columns.is_empty() {
        let ret_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone()));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: ret_cursor_id,
            is_table: true,
        });
        ctx.returning_buffer = Some(ReturningBufferCtx {
            cursor_id: ret_cursor_id,
            result_columns: result_columns.clone(),
        });
    }

    emit_non_from_clause_subqueries_for_phase(
        program,
        resolver,
        &mut source_subqueries,
        &[],
        Some(&table_references),
        SubqueryEvalPhase::PreWrite,
        |_| true,
    )?;
    let single_values = init_source_emission(program, connection, &mut ctx, resolver, source)?;
    let has_upsert = !upsert_actions.is_empty();

    // Set up the program to return result columns if RETURNING is specified
    if !result_columns.is_empty() {
        program.result_columns.clone_from(&result_columns);
    }
    let insertion = build_insertion(
        program,
        &table,
        &statement.columns,
        ctx.num_values,
        defaults,
    )?;

    translate_rows_and_open_tables(
        program,
        resolver,
        &insertion,
        &ctx,
        single_values.as_deref(),
    )?;

    // Emit subqueries for RETURNING clause (uncorrelated subqueries are evaluated once)
    emit_non_from_clause_subqueries_for_eval_at(
        program,
        resolver,
        &mut returning_subqueries,
        &[],
        Some(&table_references),
        EvalAt::BeforeLoop,
        |_| true,
    )?;

    let has_user_provided_rowid = ctx.table.has_rowid && insertion.key.is_provided_by_user();

    if ctx.table.has_autoincrement && !is_mvcc {
        init_autoincrement(program, &mut ctx, resolver)?;
    }

    // For non-STRICT tables, apply column affinity to the values early.
    // This must happen before BEFORE triggers (matching SQLite's order) so that
    // trigger bodies see affinity-applied values. Affinity is idempotent, so
    // applying it here means we can skip the per-trigger Copy+Affinity in fire_trigger.
    if !ctx.table.is_strict {
        let affinity = insertion
            .col_mappings
            .iter()
            .filter(|cm| !cm.column.is_virtual_generated())
            .map(|col_mapping| col_mapping.column.affinity());

        // Only emit Affinity if there's meaningful affinity to apply
        // (i.e., not all BLOB/NONE affinity)
        if affinity.clone().any(|a| a != Affinity::Blob) {
            if let Ok(count) = NonZeroUsize::try_from(insertion.num_non_virtual_cols) {
                program.emit_insn(Insn::Affinity {
                    start_reg: insertion.first_col_register(),
                    count,
                    affinities: affinity.map(|a| a.aff_mask()).collect(),
                });
            }
        }
    }

    // Fire BEFORE INSERT triggers

    let relevant_before_triggers = get_triggers_including_temp(
        resolver,
        database_id,
        TriggerEvent::Insert,
        TriggerTime::Before,
        None,
        &btree_table,
    );

    let dml_ctx =
        DmlColumnContext::from_column_reg_mapping(insertion.col_mappings.iter().map(|cm| {
            (
                cm.column,
                if cm.column.is_rowid_alias() {
                    insertion.key_register()
                } else {
                    cm.register
                },
            )
        }));

    let has_before_triggers = !relevant_before_triggers.is_empty();
    if has_before_triggers {
        compute_planned_virtual_columns(program, ctx.target, &dml_ctx, resolver)?;

        // In SQLite, NEW.<rowid_alias> returns -1 in BEFORE INSERT triggers when the rowid
        // hasn't been assigned yet (i.e., it's NULL). We need to temporarily set the key
        // register to -1 so the trigger sees the correct value.
        let saved_key_reg = if has_user_provided_rowid {
            // User provided a value that might be NULL. Save original value, replace NULL with -1.
            let save_reg = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: insertion.key_register(),
                dst_reg: save_reg,
                extra_amount: 0,
            });
            let skip_label = program.allocate_label();
            program.emit_insn(Insn::NotNull {
                reg: insertion.key_register(),
                target_pc: skip_label,
            });
            program.emit_insn(Insn::Integer {
                value: -1,
                dest: insertion.key_register(),
            });
            program.preassign_label_to_next_insn(skip_label);
            Some(save_reg)
        } else {
            // Key is auto-generated, register is uninitialized. Set to -1.
            program.emit_insn(Insn::Integer {
                value: -1,
                dest: insertion.key_register(),
            });
            None
        };
        // Build NEW registers: for rowid alias columns, use the rowid register; otherwise use column register
        let new_registers: Vec<usize> = insertion
            .col_mappings
            .iter()
            .map(|col_mapping| {
                if col_mapping.column.is_rowid_alias() {
                    insertion.key_register()
                } else {
                    col_mapping.register
                }
            })
            .chain(std::iter::once(insertion.key_register()))
            .collect();
        // Determine the conflict resolution to propagate to triggers:
        // 1. If there's already an override from UPSERT DO UPDATE context, use it (forces ABORT)
        // 2. If the INSERT uses an explicit ON CONFLICT clause (not default ABORT),
        //    propagate it to trigger statements (per SQLite semantics, the outer
        //    statement's conflict resolution overrides the trigger body's)
        // 3. Otherwise, don't override (use statement's own conflict resolution)
        let trigger_ctx = if let Some(override_conflict) = program.trigger_conflict_override {
            TriggerContext::new_with_override_conflict(
                btree_table.clone(),
                Arc::clone(&ctx.target.read_programs),
                Some(new_registers),
                None, // No OLD for INSERT
                override_conflict,
            )
        } else if !matches!(ctx.on_conflict, ResolveType::Abort) {
            TriggerContext::new_with_override_conflict(
                btree_table.clone(),
                Arc::clone(&ctx.target.read_programs),
                Some(new_registers),
                None, // No OLD for INSERT
                ctx.on_conflict,
            )
        } else {
            TriggerContext::new(
                btree_table.clone(),
                Arc::clone(&ctx.target.read_programs),
                Some(new_registers),
                None, // No OLD for INSERT
            )
        };
        for trigger in relevant_before_triggers {
            fire_trigger(
                program,
                resolver,
                trigger,
                &trigger_ctx,
                connection,
                database_id,
                ctx.loop_labels.row_done,
            )?;
        }
        // Restore the original key register value so the post-trigger NotNull check
        // correctly routes NULL keys to NewRowid generation.
        if let Some(save_reg) = saved_key_reg {
            program.emit_insn(Insn::Copy {
                src_reg: save_reg,
                dst_reg: insertion.key_register(),
                extra_amount: 0,
            });
        }
    }

    if has_user_provided_rowid {
        emit_check_for_user_provided_rowid(program, &insertion, &ctx);
    }

    program.preassign_label_to_next_insn(ctx.key_labels.key_generation);

    if ctx.table.has_rowid {
        emit_rowid_generation(program, &ctx, &insertion, resolver, is_mvcc)?;
    }

    program.preassign_label_to_next_insn(ctx.key_labels.key_ready_for_check);

    if ctx.table.is_strict {
        // Pre-encode TypeCheck: validate input types match the custom type's
        // declared value type BEFORE encoding. This catches type mismatches
        // (e.g. TEXT into an INTEGER-based custom type) that would otherwise
        // be silently converted by the encode expression.
        program.emit_insn(Insn::TypeCheck {
            start_reg: insertion.first_col_register(),
            count: insertion.num_non_virtual_cols,
            check_generated: true,
            table_reference: BTreeTable::input_type_check_table_ref(
                ctx.table,
                resolver.schema(),
                None,
            )?,
        });

        // Encode values for columns with custom types.
        emit_plan_source_encode_columns(
            program,
            Some(&table_references),
            ctx.target,
            insertion.first_col_register(),
            None,
            &ctx.table.column_layout()?,
            resolver,
        )?;

        // Post-encode TypeCheck: validate that encode produced the correct
        // storage type (BASE).
        program.emit_insn(Insn::TypeCheck {
            start_reg: insertion.first_col_register(),
            count: insertion.num_non_virtual_cols,
            check_generated: true,
            table_reference: BTreeTable::type_check_table_ref(ctx.table, resolver.schema()),
        });
    }
    // Non-STRICT tables: Affinity was already emitted earlier (before BEFORE triggers).

    // For AUTOINCREMENT tables with an explicit rowid, update sqlite_sequence
    // before CHECK constraints. SQLite updates sqlite_sequence even when
    // INSERT OR IGNORE skips the row due to a CHECK failure.
    if has_user_provided_rowid {
        if is_mvcc && ctx.table.has_autoincrement {
            // MVCC mode: advance the implicit sequence's disk watermark past
            // the user-supplied rowid so the next auto-generated rowid is
            // strictly greater. The helper is a no-op when the explicit
            // rowid is already <= current watermark.
            let seq_name = crate::schema::autoincrement_sequence_name(&ctx.table.name);
            let seq = resolver
                .with_schema(ctx.database_id, |s| s.get_sequence(&seq_name).cloned())
                .ok_or_else(|| {
                    crate::LimboError::InternalError(format!(
                        "missing implicit sequence for AUTOINCREMENT table \"{}\"",
                        ctx.table.name
                    ))
                })?;
            crate::translate::sequence::emit_disk_advance_past(
                program,
                resolver,
                ctx.database_id,
                &seq_name,
                &seq,
                insertion.key_register(),
            )?;
        } else if let Some(AutoincMeta {
            seq_cursor_id,
            r_seq,
            r_seq_rowid,
            table_name_reg,
        }) = ctx.autoincrement_meta
        {
            turso_assert!(ctx.table.has_autoincrement);
            reload_autoincrement_state(
                program,
                AutoincMeta {
                    seq_cursor_id,
                    r_seq,
                    r_seq_rowid,
                    table_name_reg,
                },
            );
            // Existing sqlite_sequence row: update only when explicit key advances seq.
            let missing_row_label = program.allocate_label();
            let explicit_done_label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg: r_seq_rowid,
                target_pc: missing_row_label,
            });

            let skip_seq_update_label = program.allocate_label();
            program.emit_insn(Insn::Le {
                lhs: insertion.key_register(),
                rhs: r_seq,
                target_pc: skip_seq_update_label,
                flags: Default::default(),
                collation: None,
            });

            emit_update_sqlite_sequence(
                program,
                resolver,
                ctx.database_id,
                seq_cursor_id,
                r_seq_rowid,
                table_name_reg,
                insertion.key_register(),
            )?;
            program.emit_insn(Insn::Goto {
                target_pc: explicit_done_label,
            });

            // SQLite leaves sqlite_sequence unchanged when the explicit key
            // does not advance seq.
            program.preassign_label_to_next_insn(skip_seq_update_label);
            program.emit_insn(Insn::Goto {
                target_pc: explicit_done_label,
            });

            // If sqlite_sequence has no row yet, write max(0, explicit_key).
            program.preassign_label_to_next_insn(missing_row_label);
            let seq_to_write_reg = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: r_seq,
                dst_reg: seq_to_write_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MemMax {
                dest_reg: seq_to_write_reg,
                src_reg: insertion.key_register(),
            });
            emit_update_sqlite_sequence(
                program,
                resolver,
                ctx.database_id,
                seq_cursor_id,
                r_seq_rowid,
                table_name_reg,
                seq_to_write_reg,
            )?;
            program.preassign_label_to_next_insn(explicit_done_label);
        }
    }

    // Make computed virtual columns accessible to CHECK and NOT NULL constraint evaluation
    if insertion.has_virtual_columns() {
        //TODO only compute the necessary virtual columns for CHECK and NOT NULL evaluation
        compute_planned_virtual_columns(program, ctx.target, &dml_ctx, resolver)?;
    }

    // Evaluate CHECK constraints after type affinity/TypeCheck but before other constraints
    emit_check_constraints(
        program,
        ctx.target,
        &ctx.target.check_constraints,
        &dml_ctx,
        insertion.key_register(),
        resolver,
        connection,
        ctx.on_conflict,
        ctx.loop_labels.row_done,
    )?;

    // Build a list of upsert constraints/indexes we need to run preflight
    // checks against, in the proper order of evaluation,
    let constraints = build_constraints_to_check(
        &upsert_actions,
        &ctx.indexes,
        ctx.table.has_rowid,
        has_user_provided_rowid,
        ctx.table.rowid_alias_conflict_clause,
        ctx.statement_on_conflict.is_some(),
    );

    // We need to separate index handling and insertion into a `preflight` and a
    // `commit` phase, because in UPSERT mode we might need to skip the actual insertion, as we can
    // have a naked ON CONFLICT DO NOTHING, so if we eagerly insert any indexes, we could insert
    // invalid index entries before we hit a conflict down the line.
    //
    // REPLACE (whether statement-level OR REPLACE or constraint-level ON CONFLICT REPLACE)
    // inserts eagerly in preflight because it needs to delete-then-insert per index.
    // When there's no statement-level override (e.g. INSERT OR ...) and no UPSERT,
    // individual constraints keep their DDL modes. If some use REPLACE and others
    // don't, the preflight eagerly handles REPLACE indexes (delete+reinsert) while
    // deferring non-REPLACE indexes to the commit phase (skip_replace_indexes).
    // This mixed-mode detection is unnecessary when a statement override exists,
    // because the override applies uniformly to all constraints.
    let has_ddl_replace = ctx.statement_on_conflict.is_none()
        && upsert_actions.is_empty()
        && any_index_or_ipk_has_replace(
            ctx.table.rowid_alias_conflict_clause,
            ctx.indexes.iter().map(|index| index.value().on_conflict),
        );
    let on_replace = (matches!(ctx.on_conflict, ResolveType::Replace) && upsert_actions.is_empty())
        || has_ddl_replace;
    let mut preflight_ctx = PreflightCtx {
        upsert_actions: &upsert_actions,
        on_replace,
        effective_on_conflict: ctx.on_conflict,
        connection,
        table_references: &mut table_references,
    };
    // NOT NULL default substitution must happen before index key registers are
    // copied in preflight constraint checks. Otherwise the index entry gets NULL
    // while the table row gets the default value, causing integrity_check failures.
    emit_notnulls(
        program,
        &ctx,
        &insertion,
        &*preflight_ctx.table_references,
        resolver,
    )?;

    emit_preflight_constraint_checks(
        program,
        &mut ctx,
        resolver,
        &insertion,
        &constraints,
        &mut preflight_ctx,
    )?;

    // Create and insert the record
    emit_make_record(
        program,
        insertion.col_mappings.iter().map(|m| m.column),
        insertion.base_reg,
        insertion.record_register(),
        ctx.table.is_strict,
    );

    if has_fks {
        // Child-side FK check must run before any writes (IdxInsert / Insert).
        // For immediate FKs this emits a direct Halt, so no index entry is written
        // when the parent is missing — matching SQLite's bytecode order.
        let fk_layout = btree_table.column_layout()?;
        emit_fk_child_insert_checks(
            program,
            &btree_table,
            insertion.first_col_register(),
            insertion.key_register(),
            resolver,
            database_id,
            &fk_layout,
        )?;
    }

    // Emit deferred index inserts for cases where preflight only checked constraints
    // but didn't insert. This covers UPSERT and non-REPLACE conflict types (ABORT/FAIL/
    // IGNORE/ROLLBACK). REPLACE inserts eagerly in the preflight phase because it needs
    // to delete-then-insert per index.
    //
    // When statement-level REPLACE is active, ALL indexes use REPLACE and are eagerly
    // inserted in preflight, so the commit phase can be skipped entirely. But when only
    // some constraints have REPLACE (mixed mode via DDL), non-REPLACE indexes still need
    // their entries committed here.
    // Pure statement-level REPLACE (no upsert). When upsert actions exist,
    // ON CONFLICT DO UPDATE takes precedence over REPLACE for matching
    // constraints, so we can't skip the commit phase.
    let statement_replace = matches!(ctx.on_conflict, ResolveType::Replace);
    let skip_replace_indexes = has_ddl_replace && !statement_replace;
    if has_upsert || !statement_replace {
        emit_commit_phase(program, resolver, &insertion, &ctx, skip_replace_indexes)?;
    }

    let mut insert_flags = InsertFlags::new();

    // For REPLACE (statement-level or constraint-level), we need to force a seek on the
    // insert, as we may have already deleted the conflicting row and the cursor is not
    // guaranteed to be positioned.
    if matches!(ctx.on_conflict, ResolveType::Replace) || has_ddl_replace {
        insert_flags = insert_flags.require_seek();
    }
    program.emit_insn(Insn::Insert {
        cursor: ctx.cursor_id,
        key_reg: insertion.key_register(),
        record_reg: insertion.record_register(),
        flag: insert_flags,
        table_name: table_name.to_string(),
    });

    // Fire AFTER INSERT triggers
    let relevant_after_triggers = get_triggers_including_temp(
        resolver,
        database_id,
        TriggerEvent::Insert,
        TriggerTime::After,
        None,
        &btree_table,
    );
    let has_after_triggers = !relevant_after_triggers.is_empty();
    if has_after_triggers {
        compute_planned_virtual_columns(program, ctx.target, &dml_ctx, resolver)?;

        // Build raw NEW registers for AFTER triggers. Values are encoded at this point;
        // fire_trigger will decode them via decode_trigger_registers.
        let key_reg = insertion.key_register();
        let new_registers_after: Vec<usize> = insertion
            .col_mappings
            .iter()
            .map(|cm| {
                if cm.column.is_rowid_alias() {
                    key_reg
                } else {
                    cm.register
                }
            })
            .chain(std::iter::once(key_reg))
            .collect();
        // Determine the conflict resolution to propagate to AFTER triggers (same logic as BEFORE)
        let trigger_ctx_after = if let Some(override_conflict) = program.trigger_conflict_override {
            TriggerContext::new_after_with_override_conflict(
                btree_table.clone(),
                Arc::clone(&ctx.target.read_programs),
                Some(new_registers_after),
                None,
                override_conflict,
            )
        } else if !matches!(ctx.on_conflict, ResolveType::Abort) {
            TriggerContext::new_after_with_override_conflict(
                btree_table.clone(),
                Arc::clone(&ctx.target.read_programs),
                Some(new_registers_after),
                None,
                ctx.on_conflict,
            )
        } else {
            TriggerContext::new_after(
                btree_table.clone(),
                Arc::clone(&ctx.target.read_programs),
                Some(new_registers_after),
                None,
            )
        };
        // RAISE(IGNORE) in an AFTER trigger should only abort the trigger body,
        // not skip post-row work (FK counters, autoincrement, CDC, RETURNING).
        // Use a label that falls through to the next instruction after the trigger loop.
        let after_trigger_done = program.allocate_label();
        for trigger in relevant_after_triggers {
            fire_trigger(
                program,
                resolver,
                trigger,
                &trigger_ctx_after,
                connection,
                database_id,
                after_trigger_done,
            )?;
        }
        program.preassign_label_to_next_insn(after_trigger_done);
    }

    if has_fks {
        // After the row is actually present, repair deferred counters for children referencing this NEW parent key.
        // For REPLACE: delete increments counters above; the insert path should try to repay
        // them, even for immediate/self-ref FKs.
        emit_parent_side_fk_decrement_on_insert(
            program,
            &btree_table,
            &insertion,
            on_replace,
            resolver,
            database_id,
        )?;
    }

    if !is_mvcc {
        if let Some(AutoincMeta {
            seq_cursor_id,
            r_seq,
            r_seq_rowid,
            table_name_reg,
        }) = ctx.autoincrement_meta
        {
            reload_autoincrement_state(
                program,
                AutoincMeta {
                    seq_cursor_id,
                    r_seq,
                    r_seq_rowid,
                    table_name_reg,
                },
            );
            let no_update_needed_label = program.allocate_label();
            program.emit_insn(Insn::Le {
                lhs: insertion.key_register(),
                rhs: r_seq,
                target_pc: no_update_needed_label,
                flags: Default::default(),
                collation: None,
            });

            emit_update_sqlite_sequence(
                program,
                resolver,
                ctx.database_id,
                seq_cursor_id,
                r_seq_rowid,
                table_name_reg,
                insertion.key_register(),
            )?;

            program.preassign_label_to_next_insn(no_update_needed_label);
            program.emit_insn(Insn::Close {
                cursor_id: seq_cursor_id,
            });
        }
    }

    // Emit update in the CDC table if necessary (after the INSERT updated the table)
    if let Some((cdc_cursor_id, _)) = &ctx.cdc_table {
        let cdc_has_after = program.capture_data_changes_info().has_after();
        let after_record_reg = if cdc_has_after {
            Some(emit_cdc_patch_record(
                program,
                &table,
                insertion.first_col_register(),
                insertion.record_register(),
                insertion.key_register(),
                &ColumnLayout::from_table(&table)?,
            ))
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::INSERT,
            *cdc_cursor_id,
            insertion.key_register(),
            None,
            after_record_reg,
            None,
            table_name.as_str(),
        )?;
    }

    if !returning_subqueries.is_empty() {
        let target_table = table_references
            .joined_tables()
            .first()
            .expect("INSERT RETURNING target table must exist");
        let cache_state = seed_returning_row_image_in_cache(
            &table_references,
            &result_columns,
            &returning_subqueries,
            insertion.first_col_register(),
            insertion.key_register(),
            resolver,
            &btree_table.column_layout()?,
        )?;
        let result: Result<()> = (|| {
            for subquery in returning_subqueries
                .iter_mut()
                .filter(|s| !s.has_been_evaluated())
            {
                let rerun_for_target_scan =
                    subquery.reads_table(target_table.database_id, target_table.table.get_name());
                let subquery_plan = subquery.consume_plan(EvalAt::Loop(0));
                emit_non_from_clause_subquery(
                    program,
                    resolver,
                    *subquery_plan,
                    &subquery.query_type,
                    subquery.correlated || rerun_for_target_scan,
                    true,
                )?;
            }
            Ok(())
        })();
        restore_returning_row_image_in_cache(resolver, cache_state);
        result?;
    }

    // Emit RETURNING results if specified
    if !result_columns.is_empty() {
        emit_returning_results(
            program,
            &table_references,
            &result_columns,
            insertion.first_col_register(),
            insertion.key_register(),
            resolver,
            ctx.returning_buffer.as_ref(),
            &btree_table.column_layout()?,
        )?;
    }
    program.emit_insn(Insn::Goto {
        target_pc: ctx.loop_labels.row_done,
    });
    if !upsert_actions.is_empty() {
        resolve_upserts(
            program,
            resolver,
            &mut upsert_actions,
            &mut upsert_subqueries,
            &ctx,
            &insertion,
            &table,
            excluded_source.expect("UPSERT has an EXCLUDED source"),
            &mut result_columns,
            connection,
            &mut table_references,
        )?;
    }

    emit_epilogue(
        program,
        resolver,
        &ctx,
        inserting_multiple_rows,
        &table_references,
    )?;

    {
        let has_statement_conflict = ctx.statement_on_conflict.is_some();
        let notnull_col_exists = insertion
            .col_mappings
            .iter()
            .any(|m| m.column.notnull() && !m.column.is_rowid_alias());
        let has_unique = !constraints.constraints_to_check.is_empty();
        let has_triggers = has_before_triggers || has_after_triggers;
        let has_upsert_do_update = upsert_actions
            .iter()
            .any(|(_, _, action)| matches!(action, PlannedUpsertDo::Update { .. }));
        set_insert_stmt_journal_flags(
            program,
            resolver,
            database_id,
            ctx.table,
            has_statement_conflict,
            ctx.on_conflict,
            inserting_multiple_rows,
            has_triggers,
            has_fks,
            has_upsert,
            has_upsert_do_update,
            btree_table.has_autoincrement,
            notnull_col_exists,
            has_unique,
        );
    }

    program.result_columns = result_columns;
    program.table_references.extend(table_references);
    Ok(())
}

/// If the user provided an explicit rowid for this insert, we must validate that it is an Integer and non-null
fn emit_check_for_user_provided_rowid(
    program: &mut ProgramBuilder,
    insertion: &Insertion,
    ctx: &InsertEmitCtx,
) {
    let must_be_int_label = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: insertion.key_register(),
        target_pc: must_be_int_label,
    });

    program.emit_insn(Insn::Goto {
        target_pc: ctx.key_labels.key_generation,
    });

    program.preassign_label_to_next_insn(must_be_int_label);
    program.emit_insn(Insn::MustBeInt {
        reg: insertion.key_register(),
        target_pc: None,
    });

    program.emit_insn(Insn::Goto {
        target_pc: ctx.key_labels.key_ready_for_check,
    });
}

fn emit_epilogue(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    ctx: &InsertEmitCtx,
    inserting_multiple_rows: bool,
    table_references: &TableReferences,
) -> Result<()> {
    if inserting_multiple_rows {
        if let Some(temp_table_ctx) = &ctx.temp_table_ctx {
            program.preassign_label_to_next_insn(ctx.loop_labels.row_done);

            program.emit_insn(Insn::Next {
                cursor_id: temp_table_ctx.cursor_id,
                pc_if_next: temp_table_ctx.loop_start_label,
            });
            program.preassign_label_to_next_insn(temp_table_ctx.loop_end_label);

            program.emit_insn(Insn::Close {
                cursor_id: temp_table_ctx.cursor_id,
            });
            program.emit_insn(Insn::Goto {
                target_pc: ctx.loop_labels.stmt_epilogue,
            });
        } else {
            // For multiple rows which not require a temp table, loop back
            program.preassign_label_to_next_insn(ctx.loop_labels.row_done);
            program.emit_insn(Insn::Goto {
                target_pc: ctx.loop_labels.loop_start,
            });
            if let Some(sel_eof) = ctx.loop_labels.select_exhausted {
                program.preassign_label_to_next_insn(sel_eof);
                program.emit_insn(Insn::Goto {
                    target_pc: ctx.loop_labels.stmt_epilogue,
                });
            }
        }
    } else {
        program.preassign_label_to_next_insn(ctx.loop_labels.row_done);
        // single-row falls through to epilogue
        program.emit_insn(Insn::Goto {
            target_pc: ctx.loop_labels.stmt_epilogue,
        });
    }
    program.preassign_label_to_next_insn(ctx.loop_labels.stmt_epilogue);
    if let Some((cdc_cursor_id, _)) = &ctx.cdc_table {
        emit_cdc_autocommit_commit(program, resolver, *cdc_cursor_id)?;
    }
    // Emit scan-back loop for buffered RETURNING results.
    // All DML is complete at this point; now yield the buffered rows to the caller.
    // FkCheck must come before the scan-back so that FK violations prevent
    // RETURNING rows from being emitted (matching SQLite behavior).
    if let Some(ref buf) = ctx.returning_buffer {
        program.emit_insn(Insn::FkCheck { deferred: false });
        emit_returning_scan_back(program, buf, &table_references, resolver)?;
    }
    program.preassign_label_to_next_insn(ctx.halt_label);
    Ok(())
}

/// Evaluates a partial index WHERE clause and emits code to skip if the predicate is false.
/// Returns Some(label) if there was a WHERE clause (label should be resolved after the guarded code),
/// or None if there was no WHERE clause.
fn emit_partial_index_check(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    index: &crate::translate::semantic::hir::ResolvedIndex,
    insertion: &Insertion,
    ctx: &InsertEmitCtx,
) -> Result<Option<BranchOffset>> {
    let Some(predicate) = ctx.target.partial_index_predicate(index.value()) else {
        return Ok(None);
    };
    let skip_label = program.allocate_label();
    let predicate_true = program.allocate_label();
    let bindings = insert_row_bindings(ctx, insertion);
    resolver.with_plan_runtime_bindings(bindings, |resolver| {
        translate_plan_condition_expr(
            program,
            None,
            predicate,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: predicate_true,
                jump_target_when_false: skip_label,
                jump_target_when_null: skip_label,
            },
            resolver,
        )
    })?;
    program.preassign_label_to_next_insn(predicate_true);
    Ok(Some(skip_label))
}

// COMMIT PHASE: no preflight jumps happened; emit the actual index writes now
// We re-check partial-index predicates against the NEW image, produce packed records,
// and insert into all applicable indexes, we do not re-probe uniqueness here, as preflight
// already guaranteed non-conflict.
fn emit_commit_phase(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    insertion: &Insertion,
    ctx: &InsertEmitCtx,
    skip_replace_indexes: bool,
) -> Result<()> {
    for index in &ctx.indexes {
        let index_value = index.value();
        // In mixed mode (some constraints REPLACE, some not), REPLACE indexes
        // were already eagerly inserted in the preflight phase. Skip them here
        // to avoid double-insertion.
        if skip_replace_indexes && index_value.on_conflict == Some(ResolveType::Replace) {
            continue;
        }
        let idx_cursor_id = ctx
            .idx_cursors
            .iter()
            .find(|(name, _, _)| name == &index_value.name)
            .map(|(_, _, c_id)| *c_id)
            .expect("no cursor found for index");

        // Re-evaluate partial predicate on the would-be inserted image
        let commit_skip_label = emit_partial_index_check(program, resolver, index, insertion, ctx)?;

        let num_cols = index_value.columns.len();
        let idx_start_reg = program.alloc_registers(num_cols + 1);

        // Build [key cols..., rowid] from insertion registers
        for i in 0..index_value.columns.len() {
            emit_index_column_value_for_insert(
                program,
                resolver,
                insertion,
                ctx,
                index,
                i,
                idx_start_reg + i,
            )?;
        }
        program.emit_insn(Insn::Copy {
            src_reg: insertion.key_register(),
            dst_reg: idx_start_reg + num_cols,
            extra_amount: 0,
        });

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(idx_start_reg),
            count: to_u32(num_cols + 1),
            dest_reg: to_u32(record_reg),
            index_name: Some(index_value.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: idx_cursor_id,
            record_reg,
            unpacked_start: Some(idx_start_reg),
            unpacked_count: Some((num_cols + 1) as u32),
            flags: IdxInsertFlags::new().nchange(true),
        });

        if let Some(lbl) = commit_skip_label {
            program.preassign_label_to_next_insn(lbl);
        }
    }
    Ok(())
}

#[turso_macros::trace_stack]
fn translate_rows_and_open_tables(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    insertion: &Insertion,
    ctx: &InsertEmitCtx,
    single_values: Option<&[PlanExpr]>,
) -> Result<()> {
    program.emit_insn(Insn::OpenWrite {
        cursor_id: ctx.cursor_id,
        root_page: RegisterOrLiteral::Literal(ctx.table.root_page),
        db: ctx.database_id,
    });

    if ctx.temp_table_ctx.is_some() {
        translate_rows_multiple(program, insertion, 0, resolver, &ctx.temp_table_ctx)?;
    } else {
        translate_rows_single(
            program,
            single_values.expect("single-row INSERT has planned values"),
            insertion,
            resolver,
        )?;
    }

    // Open all the index btrees for writing
    for idx_cursor in ctx.idx_cursors.iter() {
        program.emit_insn(Insn::OpenWrite {
            cursor_id: idx_cursor.2,
            root_page: idx_cursor.1.into(),
            db: ctx.database_id,
        });
    }
    Ok(())
}

fn emit_rowid_generation(
    program: &mut ProgramBuilder,
    ctx: &InsertEmitCtx,
    insertion: &Insertion,
    resolver: &Resolver,
    is_mvcc: bool,
) -> Result<()> {
    if ctx.table.has_autoincrement && is_mvcc {
        let seq_name = crate::schema::autoincrement_sequence_name(&ctx.table.name);
        let seq = resolver
            .with_schema(ctx.database_id, |s| s.get_sequence(&seq_name).cloned())
            .ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "missing implicit sequence for AUTOINCREMENT table \"{}\"",
                    ctx.table.name
                ))
            })?;
        crate::translate::sequence::emit_disk_read_nextval(
            program,
            resolver,
            ctx.database_id,
            &seq_name,
            &seq,
            insertion.key_register(),
            None,
        )?;
    } else if let Some(AutoincMeta {
        r_seq,
        seq_cursor_id,
        r_seq_rowid,
        table_name_reg,
        ..
    }) = ctx.autoincrement_meta
    {
        reload_autoincrement_state(
            program,
            AutoincMeta {
                seq_cursor_id,
                r_seq,
                r_seq_rowid,
                table_name_reg,
            },
        );
        let r_max = program.alloc_register();

        let dummy_reg = program.alloc_register();

        program.emit_insn(Insn::NewRowid {
            cursor: ctx.cursor_id,
            rowid_reg: dummy_reg,
            prev_largest_reg: r_max,
        });

        program.emit_insn(Insn::Copy {
            src_reg: r_seq,
            dst_reg: insertion.key_register(),
            extra_amount: 0,
        });
        program.emit_insn(Insn::MemMax {
            dest_reg: insertion.key_register(),
            src_reg: r_max,
        });

        let no_overflow_label = program.allocate_label();
        let max_i64_reg = program.alloc_register();
        program.emit_insn(Insn::Integer {
            dest: max_i64_reg,
            value: i64::MAX,
        });
        program.emit_insn(Insn::Ne {
            lhs: insertion.key_register(),
            rhs: max_i64_reg,
            target_pc: no_overflow_label,
            flags: Default::default(),
            collation: None,
        });

        program.emit_insn(Insn::Halt {
            err_code: crate::error::SQLITE_FULL,
            description: "database or disk is full".to_string(),
            on_error: None,
            description_reg: None,
        });

        program.preassign_label_to_next_insn(no_overflow_label);

        program.emit_insn(Insn::AddImm {
            register: insertion.key_register(),
            value: 1,
        });

        emit_update_sqlite_sequence(
            program,
            resolver,
            ctx.database_id,
            seq_cursor_id,
            r_seq_rowid,
            table_name_reg,
            insertion.key_register(),
        )?;
    } else {
        program.emit_insn(Insn::NewRowid {
            cursor: ctx.cursor_id,
            rowid_reg: insertion.key_register(),
            prev_largest_reg: 0,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn resolve_upserts(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    upsert_actions: &mut [PlannedUpsertAction],
    upsert_subqueries: &mut [Vec<NonFromClauseSubquery>],
    ctx: &InsertEmitCtx,
    insertion: &Insertion,
    table: &Table,
    excluded_source: PlanSourceId,
    result_columns: &mut [ResultSetColumn],
    connection: &Arc<crate::Connection>,
    table_references: &mut TableReferences,
) -> Result<()> {
    turso_assert_eq!(upsert_actions.len(), upsert_subqueries.len());
    for ((_, label, action), subqueries) in upsert_actions.iter_mut().zip(upsert_subqueries) {
        program.preassign_label_to_next_insn(*label);

        if let PlannedUpsertDo::Update { sets, predicate } = action {
            emit_upsert(
                program,
                table,
                ctx,
                insertion,
                sets,
                predicate.as_ref(),
                subqueries,
                excluded_source,
                resolver,
                result_columns,
                connection,
                table_references,
            )?;
        } else {
            // DO NOTHING case
            program.emit_insn(Insn::Goto {
                target_pc: ctx.loop_labels.row_done,
            });
        }
    }
    Ok(())
}

fn get_valid_sqlite_sequence_table(
    resolver: &Resolver,
    database_id: usize,
) -> Result<Arc<BTreeTable>> {
    let Some(seq_table) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(SQLITE_SEQUENCE_TABLE_NAME)
    }) else {
        crate::bail_corrupt_error!("missing sqlite_sequence table");
    };

    if !seq_table.has_rowid {
        crate::bail_corrupt_error!("malformed sqlite_sequence: table must have rowid");
    }

    if seq_table.columns().len() != 2 {
        crate::bail_corrupt_error!(
            "malformed sqlite_sequence: expected 2 columns, got {}",
            seq_table.columns().len()
        );
    }

    let col0_name = seq_table.columns()[0].name.as_deref();
    let col1_name = seq_table.columns()[1].name.as_deref();
    if !matches!(col0_name, Some(name) if name.eq_ignore_ascii_case("name"))
        || !matches!(col1_name, Some(name) if name.eq_ignore_ascii_case("seq"))
    {
        crate::bail_corrupt_error!("malformed sqlite_sequence: expected columns (name, seq)");
    }

    Ok(seq_table)
}

fn init_autoincrement(
    program: &mut ProgramBuilder,
    ctx: &mut InsertEmitCtx,
    resolver: &Resolver,
) -> Result<()> {
    open_autoincrement_state(program, ctx, resolver)?;
    reload_autoincrement_state(
        program,
        ctx.autoincrement_meta
            .expect("AUTOINCREMENT metadata should be initialized"),
    );
    Ok(())
}

fn open_autoincrement_state(
    program: &mut ProgramBuilder,
    ctx: &mut InsertEmitCtx,
    resolver: &Resolver,
) -> Result<()> {
    let seq_table = get_valid_sqlite_sequence_table(resolver, ctx.database_id)?;
    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: seq_table.root_page.into(),
        db: ctx.database_id,
    });

    let table_name_reg = program.emit_string8_new_reg(ctx.table.name.clone());
    let r_seq = program.alloc_register();
    let r_seq_rowid = program.alloc_register();

    ctx.autoincrement_meta = Some(AutoincMeta {
        seq_cursor_id,
        r_seq,
        r_seq_rowid,
        table_name_reg,
    });

    program.emit_insn(Insn::Integer {
        dest: r_seq,
        value: 0,
    });
    program.emit_insn(Insn::Null {
        dest: r_seq_rowid,
        dest_end: None,
    });
    Ok(())
}

fn reload_autoincrement_state(program: &mut ProgramBuilder, meta: AutoincMeta) {
    let AutoincMeta {
        seq_cursor_id,
        r_seq,
        r_seq_rowid,
        table_name_reg,
    } = meta;

    program.emit_insn(Insn::Integer {
        dest: r_seq,
        value: 0,
    });
    program.emit_insn(Insn::Null {
        dest: r_seq_rowid,
        dest_end: None,
    });

    let loop_start_label = program.allocate_label();
    let loop_end_label = program.allocate_label();
    let found_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: seq_cursor_id,
        pc_if_empty: loop_end_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    let name_col_reg = program.alloc_register();
    program.emit_column_or_rowid(seq_cursor_id, 0, name_col_reg);
    program.emit_insn(Insn::Ne {
        lhs: table_name_reg,
        rhs: name_col_reg,
        target_pc: found_label,
        flags: Default::default(),
        collation: None,
    });

    program.emit_column_or_rowid(seq_cursor_id, 1, r_seq);
    // SQLite emits AddImm r[seq], 0 here. OP_AddImm always leaves an integer.
    program.emit_insn(Insn::AddImm {
        register: r_seq,
        value: 0,
    });
    program.emit_insn(Insn::RowId {
        cursor_id: seq_cursor_id,
        dest: r_seq_rowid,
    });
    program.emit_insn(Insn::Goto {
        target_pc: loop_end_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_insn(Insn::Next {
        cursor_id: seq_cursor_id,
        pc_if_next: loop_start_label,
    });
    program.preassign_label_to_next_insn(loop_end_label);
}

fn emit_notnulls(
    program: &mut ProgramBuilder,
    ctx: &InsertEmitCtx,
    insertion: &Insertion,
    table_references: &TableReferences,
    resolver: &Resolver,
) -> Result<()> {
    let column_programs = &ctx.target.read_programs.column_type_programs;
    if column_programs.len() != insertion.col_mappings.len() {
        return Err(LimboError::InternalError(format!(
            "INSERT target {} has {} column type program slots for {} columns",
            ctx.target.internal_id,
            column_programs.len(),
            insertion.col_mappings.len()
        )));
    }

    for (column_index, column_mapping) in insertion
        .col_mappings
        .iter()
        .enumerate()
        .filter(|(_, column_mapping)| column_mapping.column.notnull())
    {
        // if this is rowid alias - turso-db will emit NULL as a column value and always use rowid for the row as a column value
        if column_mapping.column.is_rowid_alias() {
            continue;
        }

        // Compute effective conflict for this NOT NULL constraint:
        // Statement-level OR clause overrides; otherwise use column's clause.
        let effective = if ctx.statement_on_conflict.is_some() {
            ctx.on_conflict
        } else {
            column_mapping
                .column
                .notnull_conflict_clause
                .unwrap_or(ResolveType::Abort)
        };
        let on_replace = matches!(effective, ResolveType::Replace);
        let on_ignore = matches!(effective, ResolveType::Ignore);

        // If a NOT NULL constraint violation occurs, the REPLACE conflict resolution replaces the NULL value with the default value for that column,
        // or if the column has no default value, then the ABORT algorithm is used
        if on_replace {
            if let Some(default_expr) = insertion.default_for_column(column_mapping.column) {
                let skip_label = program.allocate_label();

                program.emit_insn(Insn::NotNull {
                    reg: column_mapping.register,
                    target_pc: skip_label,
                });

                // Evaluate default expression into the column register.
                translate_plan_expr_no_constant_opt(
                    program,
                    None,
                    default_expr,
                    column_mapping.register,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;

                // The statement-level Affinity insn already ran on the
                // original (NULL) value, so the substituted default needs the
                // column affinity applied here or index keys copied from this
                // register keep the default's literal type (e.g. text '5' for
                // an INT column) while MakeRecord converts the table row.
                let affinity = column_mapping.column.affinity();
                if !ctx.table.is_strict && affinity != Affinity::Blob {
                    program.emit_insn(Insn::Affinity {
                        start_reg: column_mapping.register,
                        count: NonZeroUsize::MIN,
                        affinities: affinity.aff_mask().to_string(),
                    });
                }

                program.preassign_label_to_next_insn(skip_label);
            }
            // OR REPLACE but no DEFAULT, fall through to ABORT behavior
        }

        // Determine which register to check: for custom type columns with
        // a DECODE expression, decode the encoded value into a temp register
        // and check the *decoded* value. This prevents "ghost NULLs" where
        // ENCODE produces a non-NULL value but DECODE returns NULL.
        let check_reg = match &column_programs[column_index] {
            Some(programs) if programs.array.is_none() && !programs.decode.is_empty() => {
                let decoded_reg = program.alloc_register();
                program.emit_insn(Insn::Copy {
                    src_reg: column_mapping.register,
                    dst_reg: decoded_reg,
                    extra_amount: 0,
                });
                emit_plan_column_value_decode(
                    program,
                    Some(table_references),
                    programs,
                    decoded_reg,
                    resolver,
                )?;
                decoded_reg
            }
            _ => column_mapping.register,
        };

        // For IGNORE, skip to the next row if NULL
        if on_ignore {
            program.emit_insn(Insn::IsNull {
                reg: check_reg,
                target_pc: ctx.loop_labels.row_done,
            });
        } else {
            program.emit_insn(Insn::HaltIfNull {
                target_reg: check_reg,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: {
                    let mut description = String::with_capacity(
                        ctx.table.name.as_str().len()
                            + column_mapping
                                .column
                                .name
                                .as_ref()
                                .expect("Column name must be present")
                                .len()
                            + 2,
                    );
                    description.push_str(ctx.table.name.as_str());
                    description.push('.');
                    description.push_str(
                        column_mapping
                            .column
                            .name
                            .as_ref()
                            .expect("Column name must be present"),
                    );
                    description
                },
            });
        }
    }
    Ok(())
}

/// Materialize multi-row sources before opening the target for writes. This
/// gives INSERT one source shape regardless of whether HIR contained VALUES or
/// a query, and avoids keeping parser SELECT syntax alive during emission.
#[turso_macros::trace_stack]
fn init_source_emission<'a>(
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    ctx: &mut InsertEmitCtx<'a>,
    resolver: &Resolver,
    source: PlannedInsertSource,
) -> Result<Option<Vec<PlanExpr>>> {
    ctx.cursor_id = program.alloc_cursor_id_keyed(
        CursorKey::table(ctx.target.internal_id),
        CursorType::BTreeTable(ctx.table.clone()),
    );
    ctx.num_values = source.width();

    match source {
        PlannedInsertSource::Single(values) => Ok(Some(values)),
        PlannedInsertSource::MaterializedValues { rows, cursor_id } => {
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            });
            let width = rows.first().map_or(0, Vec::len);
            let values_start = program.alloc_registers(width);
            for row in rows {
                for (offset, expr) in row.iter().enumerate() {
                    translate_plan_expr_no_constant_opt(
                        program,
                        None,
                        expr,
                        values_start + offset,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                }
                let record_reg = program.alloc_register();
                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(values_start),
                    count: to_u32(width),
                    dest_reg: to_u32(record_reg),
                    index_name: None,
                    affinity_str: None,
                });
                let rowid_reg = program.alloc_register();
                program.emit_insn(Insn::NewRowid {
                    cursor: cursor_id,
                    rowid_reg,
                    prev_largest_reg: 0,
                });
                program.emit_insn(Insn::Insert {
                    cursor: cursor_id,
                    key_reg: rowid_reg,
                    record_reg,
                    flag: InsertFlags::new()
                        .require_seek()
                        .is_ephemeral_table_insert(),
                    table_name: String::new(),
                });
            }
            ctx.temp_table_ctx = Some(TempTableCtx {
                cursor_id,
                loop_start_label: program.allocate_label(),
                loop_end_label: program.allocate_label(),
            });
            Ok(None)
        }
        PlannedInsertSource::MaterializedQuery { plan, cursor_id } => {
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            });
            program.nested(|program| {
                super::emitter::emit_program(connection, resolver, program, plan, |_| {})
            })?;
            ctx.temp_table_ctx = Some(TempTableCtx {
                cursor_id,
                loop_start_label: program.allocate_label(),
                loop_end_label: program.allocate_label(),
            });
            Ok(None)
        }
    }
}

#[derive(Clone, Copy)]
pub struct AutoincMeta {
    seq_cursor_id: usize,
    r_seq: usize,
    r_seq_rowid: usize,
    table_name_reg: usize,
}

pub static ROWID_COLUMN: std::sync::LazyLock<Column> = std::sync::LazyLock::new(|| {
    Column::new(
        None,          // name
        String::new(), // type string
        None,          // default
        None,          // generated
        schema::Type::Integer,
        None,
        ColDef {
            primary_key: true,
            rowid_alias: true,
            notnull: true,
            explicit_notnull: false,
            hidden: false,
            unique: false,
            notnull_conflict_clause: None,
        },
    )
});

/// Represents how a table should be populated during an INSERT.
#[derive(Debug)]
pub struct Insertion<'a> {
    /// The integer key ("rowid") provided to the VDBE.
    key: InsertionKey<'a>,
    /// The column values that will be fed to the MakeRecord instruction to insert the row.
    /// If the table has a rowid alias column, it will also be included in this record,
    /// but a NULL will be stored for it.
    col_mappings: Vec<ColMapping<'a>>,
    /// The register that will contain the record built using the MakeRecord instruction.
    record_reg: usize,
    /// Base register of the contiguous column block. Non-virtual columns occupy
    /// `base_reg..base_reg + non_virtual_col_count`, virtual columns follow after.
    base_reg: usize,
    /// Number of non-virtual (storable) columns.
    num_non_virtual_cols: usize,
    /// Defaults resolved and lowered with the INSERT statement.
    defaults: Vec<Option<PlanExpr>>,
}

impl<'a> Insertion<'a> {
    /// Return the register that contains the rowid.
    pub fn key_register(&self) -> usize {
        self.key.register()
    }

    pub fn first_col_register(&self) -> usize {
        self.base_reg
    }

    /// Return the register that contains the record built using the MakeRecord instruction.
    pub fn record_register(&self) -> usize {
        self.record_reg
    }

    fn has_virtual_columns(&self) -> bool {
        self.col_mappings.len() - self.num_non_virtual_cols > 0
    }

    fn default_for_column(&self, column: &Column) -> Option<&PlanExpr> {
        self.col_mappings
            .iter()
            .position(|mapping| std::ptr::eq(mapping.column, column))
            .and_then(|index| self.defaults.get(index))
            .and_then(Option::as_ref)
    }

    /// Returns the column mapping for a given column name.
    pub fn get_col_mapping_by_name(&self, name: &str) -> Option<&ColMapping<'a>> {
        if let InsertionKey::RowidAlias(mapping) = &self.key {
            // If the key is a rowid alias, a NULL is emitted as the column value,
            // so we need to return the key mapping instead so that the non-NULL rowid is used
            // for the index insert.
            if mapping
                .column
                .name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
            {
                return Some(mapping);
            }
        }
        self.col_mappings.iter().find(|col| {
            col.column
                .name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
        })
    }
}

#[derive(Debug)]
enum InsertionKey<'a> {
    /// Rowid is not provided by user and will be autogenerated.
    Autogenerated { register: usize },
    /// Rowid is provided via the 'rowid' keyword.
    LiteralRowid {
        value_index: Option<usize>,
        register: usize,
    },
    /// Rowid is provided via a rowid alias column.
    RowidAlias(ColMapping<'a>),
}

impl InsertionKey<'_> {
    fn register(&self) -> usize {
        match self {
            InsertionKey::Autogenerated { register } => *register,
            InsertionKey::LiteralRowid { register, .. } => *register,
            InsertionKey::RowidAlias(x) => x.register,
        }
    }
    fn is_provided_by_user(&self) -> bool {
        !matches!(self, InsertionKey::Autogenerated { .. })
    }

    fn column_name(&self) -> &str {
        match self {
            InsertionKey::RowidAlias(x) => x
                .column
                .name
                .as_ref()
                .expect("rowid alias column must be present")
                .as_str(),
            InsertionKey::LiteralRowid { .. } => ROWID_STRS[0],
            InsertionKey::Autogenerated { .. } => ROWID_STRS[0],
        }
    }
}

/// Represents how a column in a table should be populated during an INSERT.
/// In a vector of [ColMapping], the index of a given [ColMapping] is
/// the position of the column in the table.
#[derive(Debug)]
pub struct ColMapping<'a> {
    /// Column definition
    pub column: &'a Column,
    /// Index of the value to use from a tuple in the insert statement.
    /// This is needed because the values in the insert statement are not necessarily
    /// in the same order as the columns in the table, nor do they necessarily contain
    /// all of the columns in the table.
    /// If None, a NULL will be emitted for the column, unless it has a default value.
    /// A NULL rowid alias column's value will be autogenerated.
    pub value_index: Option<usize>,
    /// Register where the value will be stored for insertion into the table.
    pub register: usize,
}

/// Resolves how each column in a table should be populated during an INSERT.
/// Returns an [Insertion] struct that contains the key and record for the insertion.
fn build_insertion<'a>(
    program: &mut ProgramBuilder,
    table: &'a Table,
    columns: &[super::semantic::hir::TargetColumn],
    num_values: usize,
    defaults: Vec<Option<PlanExpr>>,
) -> Result<Insertion<'a>> {
    let table_columns = table.columns();
    let num_cols = table_columns.len();
    let rowid_register = program.alloc_register();
    let mut insertion_key = InsertionKey::Autogenerated {
        register: rowid_register,
    };
    let layout = table
        .btree()
        .map(|bt| bt.column_layout())
        .transpose()?
        .unwrap_or(ColumnLayout::Identity {
            column_count: num_cols,
        });

    let base_reg = program.alloc_registers(num_cols);
    let mut column_mappings = table_columns
        .iter()
        .enumerate()
        .map(|(i, c)| ColMapping {
            column: c,
            value_index: None,
            register: layout.to_register(base_reg, i),
        })
        .collect::<Vec<_>>();

    if num_values != columns.len() {
        return Err(LimboError::InternalError(format!(
            "planned INSERT has {num_values} values for {} destinations",
            columns.len()
        )));
    }
    for (value_index, target) in columns.iter().copied().enumerate() {
        match target {
            super::semantic::hir::TargetColumn::Column(column_index) => {
                let column = table_columns.get(column_index).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "INSERT destination column {column_index} is outside {}",
                        table.get_name()
                    ))
                })?;
                if column.is_rowid_alias() {
                    insertion_key = InsertionKey::RowidAlias(ColMapping {
                        column,
                        value_index: Some(value_index),
                        register: rowid_register,
                    });
                } else if column_mappings[column_index].value_index.is_none() {
                    column_mappings[column_index].value_index = Some(value_index);
                }
            }
            super::semantic::hir::TargetColumn::RowId => {
                if let Some(column) = table_columns.iter().find(|column| column.is_rowid_alias()) {
                    insertion_key = InsertionKey::RowidAlias(ColMapping {
                        column,
                        value_index: Some(value_index),
                        register: rowid_register,
                    });
                } else {
                    insertion_key = InsertionKey::LiteralRowid {
                        value_index: Some(value_index),
                        register: rowid_register,
                    };
                }
            }
        }
    }

    Ok(Insertion {
        key: insertion_key,
        col_mappings: column_mappings,
        record_reg: program.alloc_register(),
        base_reg,
        num_non_virtual_cols: layout.num_non_virtual_cols(),
        defaults,
    })
}

/// Populates the column registers with values for multiple rows.
/// This is used for INSERT INTO <table> VALUES (...), (...), ... or INSERT INTO <table> SELECT ...
/// which use either a coroutine or an ephemeral table as the value source.
fn translate_rows_multiple<'short, 'long: 'short>(
    program: &mut ProgramBuilder,
    insertion: &'short Insertion<'long>,
    yield_reg: usize,
    resolver: &Resolver,
    temp_table_ctx: &Option<TempTableCtx>,
) -> Result<()> {
    if let Some(ref temp_table_ctx) = temp_table_ctx {
        // Rewind loop to read from ephemeral table
        program.emit_insn(Insn::Rewind {
            cursor_id: temp_table_ctx.cursor_id,
            pc_if_empty: temp_table_ctx.loop_end_label,
        });
        program.preassign_label_to_next_insn(temp_table_ctx.loop_start_label);
    }
    let translate_value_fn =
        |prg: &mut ProgramBuilder, value_index: usize, column_register: usize| {
            if let Some(temp_table_ctx) = temp_table_ctx {
                prg.emit_insn(Insn::Column {
                    cursor_id: temp_table_ctx.cursor_id,
                    column: value_index,
                    dest: column_register,
                    default: None,
                });
            } else {
                prg.emit_insn(Insn::Copy {
                    src_reg: yield_reg + value_index,
                    dst_reg: column_register,
                    extra_amount: 0,
                });
            }
            Ok(())
        };
    translate_rows_base(program, insertion, translate_value_fn, resolver)
}
/// Populates the column registers with values for a single row
fn translate_rows_single(
    program: &mut ProgramBuilder,
    value: &[PlanExpr],
    insertion: &Insertion,
    resolver: &Resolver,
) -> Result<()> {
    let translate_value_fn =
        |prg: &mut ProgramBuilder, value_index: usize, column_register: usize| -> Result<()> {
            translate_plan_expr_no_constant_opt(
                prg,
                None,
                value.get(value_index).unwrap_or_else(|| {
                    panic!("value index out of bounds: {value_index} for value: {value:?}")
                }),
                column_register,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            Ok(())
        };
    translate_rows_base(program, insertion, translate_value_fn, resolver)
}

/// Translate the key and the columns of the insertion.
/// This function is called by both [translate_rows_single] and [translate_rows_multiple],
/// each providing a different [translate_value_fn] implementation, because for multiple rows
/// we need to emit the values in a loop, from either an ephemeral table or a coroutine,
/// whereas for the single row the translation happens in a single pass without looping.
fn translate_rows_base<'short, 'long: 'short>(
    program: &mut ProgramBuilder,
    insertion: &'short Insertion<'long>,
    mut translate_value_fn: impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    translate_key(program, insertion, &mut translate_value_fn, resolver)?;
    for col in insertion.col_mappings.iter() {
        translate_column(
            program,
            col.column,
            col.register,
            col.value_index,
            insertion.default_for_column(col.column),
            &mut translate_value_fn,
            resolver,
        )?;
    }

    Ok(())
}

/// Translate the [InsertionKey].
fn translate_key(
    program: &mut ProgramBuilder,
    insertion: &Insertion,
    mut translate_value_fn: impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    match &insertion.key {
        InsertionKey::RowidAlias(rowid_alias_column) => translate_column(
            program,
            rowid_alias_column.column,
            rowid_alias_column.register,
            rowid_alias_column.value_index,
            insertion.default_for_column(rowid_alias_column.column),
            &mut translate_value_fn,
            resolver,
        ),
        InsertionKey::LiteralRowid {
            value_index,
            register,
        } => translate_column(
            program,
            &ROWID_COLUMN,
            *register,
            *value_index,
            None,
            &mut translate_value_fn,
            resolver,
        ),
        InsertionKey::Autogenerated { .. } => Ok(()), // will be populated later
    }
}

fn translate_column(
    program: &mut ProgramBuilder,
    column: &Column,
    column_register: usize,
    value_index: Option<usize>,
    default: Option<&PlanExpr>,
    translate_value_fn: &mut impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    if let Some(value_index) = value_index {
        translate_value_fn(program, value_index, column_register)?;
    } else if column.is_rowid_alias() {
        // Although a non-NULL integer key is used for the insertion key,
        // the rowid alias column is emitted as NULL.
        program.emit_insn(Insn::SoftNull {
            reg: column_register,
        });
    } else if column.is_virtual_generated() {
        // virtual columns are computed in a separate pass in compute_planned_virtual_columns
    } else if column.hidden() {
        program.emit_insn(Insn::Null {
            dest: column_register,
            dest_end: None,
        });
    } else if let Some(default) = default {
        translate_plan_expr(program, None, default, column_register, resolver)?;
    } else {
        program.emit_insn(Insn::Null {
            dest: column_register,
            dest_end: None,
        });
    }
    Ok(())
}

/// Emit bytecode to check PRIMARY KEY uniqueness constraint.
/// Handles ON REPLACE (delete conflicting row) and UPSERT routing.
fn emit_pk_uniqueness_check(
    program: &mut ProgramBuilder,
    ctx: &mut InsertEmitCtx,
    resolver: &mut Resolver,
    insertion: &Insertion,
    position: Option<usize>,
    upsert_catch_all: Option<usize>,
    preflight: &mut PreflightCtx,
) -> Result<()> {
    if !ctx.table.has_rowid {
        let pk_regs = program.alloc_registers(ctx.table.primary_key_columns.len());
        let pk_affinities = ctx
            .table
            .primary_key_columns
            .iter()
            .map(|(name, _)| {
                let col = insertion
                    .get_col_mapping_by_name(name)
                    .unwrap_or_else(|| panic!("primary key column missing from insertion: {name}"));
                col.column
                    .affinity_with_strict(ctx.table.is_strict)
                    .aff_mask()
            })
            .collect::<String>();
        for (i, (name, _)) in ctx.table.primary_key_columns.iter().enumerate() {
            let src_reg = insertion
                .get_col_mapping_by_name(name)
                .unwrap_or_else(|| panic!("primary key column missing from insertion: {name}"))
                .register;
            program.emit_insn(Insn::Copy {
                src_reg,
                dst_reg: pk_regs + i,
                extra_amount: 0,
            });
        }
        program.emit_insn(Insn::Affinity {
            start_reg: pk_regs,
            count: NonZeroUsize::new(ctx.table.primary_key_columns.len())
                .expect("WITHOUT ROWID tables must have a primary key"),
            affinities: pk_affinities,
        });
        let no_conflict = program.allocate_label();
        program.emit_insn(Insn::NoConflict {
            cursor_id: ctx.cursor_id,
            target_pc: no_conflict,
            record_reg: pk_regs,
            num_regs: ctx.table.primary_key_columns.len(),
        });
        if let Some(position) = position.or(upsert_catch_all) {
            program.emit_insn(Insn::Goto {
                target_pc: preflight.upsert_actions[position].1,
            });
        } else if matches!(preflight.effective_on_conflict, ResolveType::Ignore) {
            program.emit_insn(Insn::Goto {
                target_pc: ctx.loop_labels.row_done,
            });
        } else {
            let raw_desc = ctx
                .table
                .primary_key_columns
                .iter()
                .map(|(name, _)| format!("{}.{}", ctx.table.name, name))
                .collect::<Vec<_>>()
                .join(", ");
            let (description, on_error) = halt_desc_and_on_error(
                &raw_desc,
                preflight.effective_on_conflict,
                program.flags.has_statement_conflict(),
            );
            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                description,
                on_error,
                description_reg: None,
            });
        }
        program.preassign_label_to_next_insn(no_conflict);
        return Ok(());
    }

    let make_record_label = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: ctx.cursor_id,
        rowid_reg: insertion.key_register(),
        target_pc: make_record_label,
    });
    let rowid_column_name = insertion.key.column_name();

    // Conflict on rowid: attempt to route through UPSERT if it targets the PK, otherwise raise constraint.
    // emit Halt for every case *except* when upsert handles the conflict
    'emit_halt: {
        if preflight.on_replace {
            // copy the conflicting rowid into the key register and delete the existing row inline
            program.emit_insn(Insn::Copy {
                src_reg: insertion.key_register(),
                dst_reg: ctx.conflict_rowid_reg,
                extra_amount: 0,
            });
            emit_replace_delete_conflicting_row(
                program,
                resolver,
                preflight.connection,
                ctx,
                preflight.table_references,
            )?;
            program.emit_insn(Insn::Goto {
                target_pc: make_record_label,
            });
            break 'emit_halt;
        }
        if let Some(position) = position.or(upsert_catch_all) {
            // PK conflict: the conflicting rowid is exactly the attempted key.
            // Upsert clause takes precedence over column-level ON CONFLICT.
            program.emit_insn(Insn::Copy {
                src_reg: insertion.key_register(),
                dst_reg: ctx.conflict_rowid_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Goto {
                target_pc: preflight.upsert_actions[position].1,
            });
            break 'emit_halt;
        }
        if matches!(preflight.effective_on_conflict, ResolveType::Ignore) {
            // IGNORE: skip this row entirely on PK conflict
            program.emit_insn(Insn::Goto {
                target_pc: ctx.loop_labels.row_done,
            });
            break 'emit_halt;
        }
        let raw_desc = format!("{}.{}", ctx.table.name, rowid_column_name);
        let (description, on_error) = halt_desc_and_on_error(
            &raw_desc,
            preflight.effective_on_conflict,
            program.flags.has_statement_conflict(),
        );
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description,
            on_error,
            description_reg: None,
        });
    }
    program.preassign_label_to_next_insn(make_record_label);
    Ok(())
}

/// Emit bytecode to check index uniqueness constraint.
/// Handles partial index predicates, ON REPLACE, UPSERT routing, and non-unique indexes.
#[allow(clippy::too_many_arguments)]
fn emit_index_uniqueness_check(
    program: &mut ProgramBuilder,
    ctx: &mut InsertEmitCtx,
    resolver: &mut Resolver,
    insertion: &Insertion,
    index: &crate::translate::semantic::hir::ResolvedIndex,
    position: Option<usize>,
    upsert_catch_all: Option<usize>,
    preflight: &mut PreflightCtx,
) -> Result<()> {
    let index_value = index.value();
    // find which cursor we opened earlier for this index
    let idx_cursor_id = ctx
        .idx_cursors
        .iter()
        .find(|(name, _, _)| name == &index_value.name)
        .map(|(_, _, c_id)| *c_id)
        .expect("no cursor found for index");

    // For partial indexes, evaluate the WHERE clause and skip if false
    let maybe_skip_probe_label =
        emit_partial_index_check(program, resolver, index, insertion, ctx)?;

    let num_cols = index_value.columns.len();
    // allocate scratch registers for the index columns plus rowid
    let idx_start_reg = program.alloc_registers(num_cols + 1);

    // build unpacked key [idx_start_reg .. idx_start_reg+num_cols-1], and rowid in last reg,
    // copy each index column from the table's column registers into these scratch regs
    for i in 0..index_value.columns.len() {
        emit_index_column_value_for_insert(
            program,
            resolver,
            insertion,
            ctx,
            index,
            i,
            idx_start_reg + i,
        )?;
    }
    // last register is the rowid
    program.emit_insn(Insn::Copy {
        src_reg: insertion.key_register(),
        dst_reg: idx_start_reg + num_cols,
        extra_amount: 0,
    });

    if index_value.unique {
        emit_unique_index_check(
            program,
            ctx,
            resolver,
            index_value,
            idx_cursor_id,
            idx_start_reg,
            num_cols,
            position,
            upsert_catch_all,
            preflight,
        )?;
    } else {
        // Non-unique index: insert eagerly only for REPLACE (which doesn't use commit phase).
        // For UPSERT and ABORT/FAIL/IGNORE/ROLLBACK, defer to commit phase.
        if preflight.on_replace {
            let record_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(idx_start_reg),
                count: to_u32(num_cols + 1),
                dest_reg: to_u32(record_reg),
                index_name: Some(index_value.name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: idx_cursor_id,
                record_reg,
                unpacked_start: Some(idx_start_reg),
                unpacked_count: Some((num_cols + 1) as u32),
                flags: IdxInsertFlags::new().nchange(true),
            });
        }
    }

    // Close the partial-index skip (preflight)
    if let Some(lbl) = maybe_skip_probe_label {
        program.preassign_label_to_next_insn(lbl);
    }
    Ok(())
}

/// Emit bytecode for unique index conflict detection and handling.
#[allow(clippy::too_many_arguments)]
fn emit_unique_index_check(
    program: &mut ProgramBuilder,
    ctx: &mut InsertEmitCtx,
    resolver: &mut Resolver,
    index: &Index,
    idx_cursor_id: usize,
    idx_start_reg: usize,
    num_cols: usize,
    position: Option<usize>,
    upsert_catch_all: Option<usize>,
    preflight: &mut PreflightCtx,
) -> Result<()> {
    let aff = index
        .columns
        .iter()
        .map(|ic| {
            if ic.expr.is_some() {
                Affinity::Blob.aff_mask()
            } else {
                ctx.table.columns()[ic.pos_in_table]
                    .affinity_with_strict(ctx.table.is_strict)
                    .aff_mask()
            }
        })
        .collect::<String>();
    program.emit_insn(Insn::Affinity {
        start_reg: idx_start_reg,
        count: NonZeroUsize::new(num_cols).expect("nonzero col count"),
        affinities: aff,
    });

    if !preflight.upsert_actions.is_empty() {
        let next_check = program.allocate_label();
        program.emit_insn(Insn::NoConflict {
            cursor_id: idx_cursor_id,
            target_pc: next_check,
            record_reg: idx_start_reg,
            num_regs: num_cols,
        });
        // Conflict detected, figure out if this UPSERT handles the conflict
        if let Some(position) = position.or(upsert_catch_all) {
            match &preflight.upsert_actions[position].2 {
                PlannedUpsertDo::Nothing => {
                    // Bail out without writing anything
                    program.emit_insn(Insn::Goto {
                        target_pc: ctx.loop_labels.row_done,
                    });
                }
                PlannedUpsertDo::Update { .. } => {
                    // Route to DO UPDATE: capture conflicting rowid then jump
                    program.emit_insn(Insn::IdxRowId {
                        cursor_id: idx_cursor_id,
                        dest: ctx.conflict_rowid_reg,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: preflight.upsert_actions[position].1,
                    });
                }
            }
        }
        // No matching UPSERT handler so we emit constraint error
        // (if conflict clause matched - VM will jump to later instructions and skip halt)
        let raw_desc = format_unique_violation_desc(ctx.table.name.as_str(), index);
        let (description, on_error) = halt_desc_and_on_error(
            &raw_desc,
            preflight.effective_on_conflict,
            program.flags.has_statement_conflict(),
        );
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_UNIQUE,
            description,
            on_error,
            description_reg: None,
        });

        // continue preflight with next constraint
        program.preassign_label_to_next_insn(next_check);
    } else {
        // No UPSERT: probe for conflicts.
        let ok = program.allocate_label();
        program.emit_insn(Insn::NoConflict {
            cursor_id: idx_cursor_id,
            target_pc: ok,
            record_reg: idx_start_reg,
            num_regs: num_cols,
        });
        if preflight.on_replace {
            // REPLACE: delete conflicting row immediately, then insert eagerly.
            program.emit_insn(Insn::IdxRowId {
                cursor_id: idx_cursor_id,
                dest: ctx.conflict_rowid_reg,
            });
            emit_replace_delete_conflicting_row(
                program,
                resolver,
                preflight.connection,
                ctx,
                preflight.table_references,
            )?;
            program.emit_insn(Insn::Goto { target_pc: ok });
        } else if matches!(preflight.effective_on_conflict, ResolveType::Ignore) {
            // IGNORE: skip this row entirely on unique conflict.
            program.emit_insn(Insn::Goto {
                target_pc: ctx.loop_labels.row_done,
            });
        } else {
            // ABORT/FAIL/ROLLBACK: halt on conflict.
            let raw_desc = format_unique_violation_desc(ctx.table.name.as_str(), index);
            let (description, on_error) = halt_desc_and_on_error(
                &raw_desc,
                preflight.effective_on_conflict,
                program.flags.has_statement_conflict(),
            );
            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_UNIQUE,
                description,
                on_error,
                description_reg: None,
            });
        }
        program.preassign_label_to_next_insn(ok);

        if preflight.on_replace {
            // REPLACE: insert index entry eagerly (right after delete).
            // IdxDelete repositions the cursor, so we must NOT use USE_SEEK.
            let record_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(idx_start_reg),
                count: to_u32(num_cols + 1),
                dest_reg: to_u32(record_reg),
                index_name: Some(index.name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: idx_cursor_id,
                record_reg,
                unpacked_start: Some(idx_start_reg),
                unpacked_count: Some((num_cols + 1) as u32),
                flags: IdxInsertFlags::new().nchange(true),
            });
        }
        // For non-REPLACE cases (ABORT/FAIL/IGNORE/ROLLBACK), index inserts are
        // deferred to the commit phase after all constraint checks pass.
        // This prevents stale index entries when a later constraint check fails.
    }
    Ok(())
}

// Preflight phase: evaluate each applicable UNIQUE constraint and probe with NoConflict.
// If any probe hits:
// DO NOTHING -> jump to row_done_label.
//
// DO UPDATE (matching target) -> fetch conflicting rowid and jump to `upsert_entry`.
//
// otherwise, raise SQLITE_CONSTRAINT_UNIQUE
fn emit_preflight_constraint_checks(
    program: &mut ProgramBuilder,
    ctx: &mut InsertEmitCtx,
    resolver: &mut Resolver,
    insertion: &Insertion,
    constraints: &ConstraintsToCheck,
    preflight: &mut PreflightCtx,
) -> Result<()> {
    let mut seen_replace = false;
    for (constraint, position) in &constraints.constraints_to_check {
        // Compute per-constraint effective conflict resolution:
        // Statement-level OR clause overrides; otherwise use constraint's clause.
        let effective = if ctx.statement_on_conflict.is_some() {
            ctx.on_conflict
        } else {
            match constraint {
                ResolvedUpsertTarget::PrimaryKey => {
                    // Use rowid_alias_conflict_clause from BTreeTable, which is preserved
                    // even for rowid-alias PKs (whose UniqueSet is removed).
                    ctx.table
                        .rowid_alias_conflict_clause
                        .unwrap_or(ResolveType::Abort)
                }
                ResolvedUpsertTarget::Index(index) => {
                    index.value().on_conflict.unwrap_or(ResolveType::Abort)
                }
                ResolvedUpsertTarget::CatchAll => unreachable!(),
            }
        };
        // REPLACE constraints must sort after all non-REPLACE ones
        // (schema.rs:add_index + IPK deferral ensure this).
        if effective == ResolveType::Replace {
            seen_replace = true;
        } else {
            turso_assert!(
                !seen_replace,
                "non-REPLACE constraint after REPLACE constraint — sort order invariant violated"
            );
        }

        let effective_on_replace =
            matches!(effective, ResolveType::Replace) && preflight.upsert_actions.is_empty();
        preflight.on_replace = effective_on_replace;
        preflight.effective_on_conflict = effective;

        match constraint {
            ResolvedUpsertTarget::PrimaryKey => {
                emit_pk_uniqueness_check(
                    program,
                    ctx,
                    resolver,
                    insertion,
                    *position,
                    constraints.upsert_catch_all_position,
                    preflight,
                )?;
            }
            ResolvedUpsertTarget::Index(index) => {
                emit_index_uniqueness_check(
                    program,
                    ctx,
                    resolver,
                    insertion,
                    index,
                    *position,
                    constraints.upsert_catch_all_position,
                    preflight,
                )?;
            }
            ResolvedUpsertTarget::CatchAll => unreachable!(),
        }
    }
    Ok(())
}

// TODO: comeback here later to apply the same improvements on select
fn translate_virtual_table_insert(
    program: &mut ProgramBuilder,
    virtual_table: Arc<VirtualTable>,
    columns: &[super::semantic::hir::TargetColumn],
    defaults: &[Option<PlanExpr>],
    source: PlannedInsertSource,
    on_conflict: Option<ResolveType>,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    #[cfg(not(feature = "cli_only"))]
    let _ = connection;
    let allow_dbpage_write = {
        #[cfg(feature = "cli_only")]
        {
            virtual_table.name == crate::dbpage::DBPAGE_TABLE_NAME
                && connection.db.opts.unsafe_testing
        }
        #[cfg(not(feature = "cli_only"))]
        {
            false
        }
    };
    if virtual_table.readonly() && !allow_dbpage_write {
        crate::bail_constraint_error!("Table is read-only: {}", virtual_table.name);
    }
    let value = match source {
        PlannedInsertSource::Single(value) => value,
        PlannedInsertSource::MaterializedValues { .. }
        | PlannedInsertSource::MaterializedQuery { .. } => {
            crate::bail_parse_error!("Virtual tables only support one VALUES row in INSERT")
        }
    };
    let table = Table::Virtual(virtual_table.clone());
    let cursor_id = program.alloc_cursor_id(CursorType::VirtualTable(virtual_table));
    program.emit_insn(Insn::VOpen { cursor_id });
    if !allow_dbpage_write {
        program.emit_insn(Insn::VBegin { cursor_id });
    }
    /* *
     * Inserts for virtual tables are done in a single step.
     * argv[0] = (NULL for insert)
     */
    let registers_start = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: registers_start,
        dest_end: None,
    });
    /* *
     * argv[1] = (rowid for insert - NULL in most cases)
     * argv[2..] = column values
     * */
    let insertion = build_insertion(program, &table, columns, value.len(), defaults.to_vec())?;

    translate_rows_single(program, &value, &insertion, resolver)?;
    let conflict_action = on_conflict.as_ref().map(|c| c.bit_value()).unwrap_or(0) as u16;

    program.emit_insn(Insn::VUpdate {
        cursor_id,
        arg_count: insertion.col_mappings.len() + 2, // +1 for NULL, +1 for rowid
        start_reg: registers_start,
        conflict_action,
    });

    program.emit_insn(Insn::Close { cursor_id });

    let halt_label = program.allocate_label();
    program.preassign_label_to_next_insn(halt_label);

    Ok(())
}

///  makes sure that an AUTOINCREMENT table has a sequence row in `sqlite_sequence`, inserting one with 0 if missing.
fn ensure_sequence_initialized(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table: &schema::BTreeTable,
    database_id: usize,
) -> Result<()> {
    let seq_table = get_valid_sqlite_sequence_table(resolver, database_id)?;

    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));

    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: seq_table.root_page.into(),
        db: database_id,
    });

    let table_name_reg = program.emit_string8_new_reg(table.name.clone());

    let loop_start_label = program.allocate_label();
    let entry_exists_label = program.allocate_label();
    let insert_new_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: seq_cursor_id,
        pc_if_empty: insert_new_label,
    });

    program.preassign_label_to_next_insn(loop_start_label);

    let name_col_reg = program.alloc_register();

    program.emit_column_or_rowid(seq_cursor_id, 0, name_col_reg);

    program.emit_insn(Insn::Eq {
        lhs: table_name_reg,
        rhs: name_col_reg,
        target_pc: entry_exists_label,
        flags: Default::default(),
        collation: None,
    });

    program.emit_insn(Insn::Next {
        cursor_id: seq_cursor_id,
        pc_if_next: loop_start_label,
    });

    program.preassign_label_to_next_insn(insert_new_label);

    let record_reg = program.alloc_register();
    let record_start_reg = program.alloc_registers(2);
    let zero_reg = program.alloc_register();

    program.emit_insn(Insn::Integer {
        dest: zero_reg,
        value: 0,
    });

    program.emit_insn(Insn::Copy {
        src_reg: table_name_reg,
        dst_reg: record_start_reg,
        extra_amount: 0,
    });

    program.emit_insn(Insn::Copy {
        src_reg: zero_reg,
        dst_reg: record_start_reg + 1,
        extra_amount: 0,
    });

    let affinity_str = seq_table
        .columns()
        .iter()
        .map(|c| c.affinity().aff_mask())
        .collect();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(record_start_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    let new_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: seq_cursor_id,
        rowid_reg: new_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: seq_cursor_id,
        key_reg: new_rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
    });

    program.preassign_label_to_next_insn(entry_exists_label);
    program.emit_insn(Insn::Close {
        cursor_id: seq_cursor_id,
    });

    Ok(())
}
#[inline]
/// Build the UNIQUE constraint error description to match sqlite
/// single column: `t.c1`
/// multi-column:  `t.(k, c1)`
/// For constraint-level FAIL/ROLLBACK ON CONFLICT, pre-format the description
/// and set on_error so the VM's halt() produces Raise(rt, msg) with correct semantics.
/// `has_statement_conflict` should be true when the statement has its own OR clause
/// (e.g. INSERT OR FAIL), in which case program.resolve_type already handles it.
pub(crate) fn halt_desc_and_on_error(
    raw_desc: &str,
    effective: ResolveType,
    has_statement_conflict: bool,
) -> (String, Option<ResolveType>) {
    if has_statement_conflict {
        return (raw_desc.to_string(), None);
    }
    match effective {
        ResolveType::Fail | ResolveType::Rollback => (
            format!("UNIQUE constraint failed: {raw_desc} (19)"),
            Some(effective),
        ),
        _ => (raw_desc.to_string(), None),
    }
}

pub fn format_unique_violation_desc(table_name: &str, index: &Index) -> String {
    if index.columns.len() == 1 {
        let mut s = String::with_capacity(table_name.len() + 1 + index.columns[0].name.len());
        s.push_str(table_name);
        s.push('.');
        s.push_str(&index.columns[0].name);
        s
    } else {
        let mut s = String::with_capacity(table_name.len() + 3 + 4 * index.columns.len());
        s.push_str(table_name);
        s.push_str(".(");
        s.push_str(
            &index
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        );
        s.push(')');
        s
    }
}

fn insert_row_bindings(ctx: &InsertEmitCtx, insertion: &Insertion) -> PlanRuntimeBindings {
    let mut bindings = PlanRuntimeBindings::default();
    bindings.bind_row(
        ctx.target.internal_id,
        RuntimeRowBinding {
            columns: insertion
                .col_mappings
                .iter()
                .map(|mapping| RuntimeValueBinding::Register {
                    register: if mapping.column.is_rowid_alias() {
                        insertion.key_register()
                    } else {
                        mapping.register
                    },
                    needs_decode: true,
                })
                .collect(),
            rowid: Some(RuntimeValueBinding::Register {
                register: insertion.key_register(),
                needs_decode: false,
            }),
            read_programs: Some(Arc::clone(&ctx.target.read_programs)),
        },
    );
    bindings
}

fn emit_index_column_value_for_insert(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    insertion: &Insertion,
    ctx: &InsertEmitCtx,
    index: &crate::translate::semantic::hir::ResolvedIndex,
    index_column: usize,
    dest_reg: usize,
) -> Result<()> {
    let bindings = insert_row_bindings(ctx, insertion);
    resolver.with_plan_runtime_bindings(bindings, |resolver| {
        emit_index_column_value_new_image(
            program,
            resolver,
            ctx.target,
            index,
            index_column,
            dest_reg,
        )
    })
}

struct ConstraintsToCheck {
    constraints_to_check: Vec<(ResolvedUpsertTarget, Option<usize>)>,
    upsert_catch_all_position: Option<usize>,
}

/// Context for preflight constraint checks
struct PreflightCtx<'a, 'b> {
    /// UPSERT action handlers (target, label, upsert clause)
    upsert_actions: &'a [PlannedUpsertAction],
    /// Whether ON CONFLICT REPLACE is active globally (without UPSERT).
    /// This is true when the statement has INSERT OR REPLACE (applies to all constraints).
    on_replace: bool,
    /// The effective conflict resolution for the current constraint being checked.
    /// Updated per-constraint in emit_preflight_constraint_checks.
    effective_on_conflict: ResolveType,
    /// Database connection for FK checks
    connection: &'a Arc<Connection>,
    /// Table references for expression evaluation
    table_references: &'b mut TableReferences,
}

fn build_constraints_to_check(
    upsert_actions: &[PlannedUpsertAction],
    indexes: &[crate::translate::semantic::hir::ResolvedIndex],
    has_rowid: bool,
    has_user_provided_rowid: bool,
    rowid_alias_conflict_clause: Option<ResolveType>,
    has_statement_conflict: bool,
) -> ConstraintsToCheck {
    let mut constraints_to_check = Vec::new();
    if !has_rowid || has_user_provided_rowid {
        // Check uniqueness constraint for rowid if it was provided by user.
        // When the DB allocates it there are no need for separate uniqueness checks.
        let position = upsert_actions
            .iter()
            .position(|(target, ..)| matches!(target, ResolvedUpsertTarget::PrimaryKey));
        constraints_to_check.push((ResolvedUpsertTarget::PrimaryKey, position));
    }
    for index in indexes {
        let position = upsert_actions
            .iter()
            .position(|(target, ..)| matches!(target, ResolvedUpsertTarget::Index(candidate) if candidate == index));
        constraints_to_check.push((ResolvedUpsertTarget::Index(index.clone()), position));
    }

    constraints_to_check.sort_by(|(_, p1), (_, p2)| match (p1, p2) {
        (Some(p1), Some(p2)) => p1.cmp(p2),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });

    // Defer INTEGER PRIMARY KEY REPLACE to run after all other constraint checks.
    // When IPK has REPLACE and other
    // constraints exist with potentially different modes, the IPK check
    // runs last to avoid premature row deletion before FAIL/IGNORE fires.
    let defer_ipk_replace_after_other_checks = !has_statement_conflict
        && rowid_alias_conflict_clause == Some(ResolveType::Replace)
        && constraints_to_check.len() > 1
        && !upsert_actions
            .iter()
            .any(|(t, ..)| matches!(t, ResolvedUpsertTarget::PrimaryKey));
    if defer_ipk_replace_after_other_checks {
        if let Some(pos) = constraints_to_check
            .iter()
            .position(|(c, _)| matches!(c, ResolvedUpsertTarget::PrimaryKey))
        {
            let pk = constraints_to_check.remove(pos);
            constraints_to_check.push(pk);
        }
    }

    // Post-condition: when no statement-level override exists, all REPLACE
    // constraints (by DDL mode) must form a contiguous suffix. When a statement
    // override exists, all constraints get the same effective mode, so the DDL
    // ordering is irrelevant.
    turso_debug_assert!(
        has_statement_conflict || {
            let mut saw_replace = false;
            constraints_to_check.iter().all(|(c, _)| {
                let mode = match c {
                    ResolvedUpsertTarget::PrimaryKey => {
                        rowid_alias_conflict_clause.unwrap_or(ResolveType::Abort)
                    }
                    ResolvedUpsertTarget::Index(idx) => {
                        idx.value().on_conflict.unwrap_or(ResolveType::Abort)
                    }
                    ResolvedUpsertTarget::CatchAll => return true,
                };
                if mode == ResolveType::Replace {
                    saw_replace = true;
                    true
                } else {
                    !saw_replace
                }
            })
        },
        "constraints must have all REPLACE entries at the end"
    );

    let upsert_catch_all_position =
        if let Some((ResolvedUpsertTarget::CatchAll, ..)) = upsert_actions.last() {
            Some(upsert_actions.len() - 1)
        } else {
            None
        };
    ConstraintsToCheck {
        constraints_to_check,
        upsert_catch_all_position,
    }
}

fn emit_update_sqlite_sequence(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    seq_cursor_id: usize,
    r_seq_rowid: usize,
    table_name_reg: usize,
    new_key_reg: usize,
) -> Result<()> {
    let record_reg = program.alloc_register();
    let record_start_reg = program.alloc_registers(2);
    program.emit_insn(Insn::Copy {
        src_reg: table_name_reg,
        dst_reg: record_start_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: new_key_reg,
        dst_reg: record_start_reg + 1,
        extra_amount: 0,
    });

    let seq_table = get_valid_sqlite_sequence_table(resolver, database_id)?;
    let affinity_str = seq_table
        .columns()
        .iter()
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(record_start_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    let update_existing_label = program.allocate_label();
    let end_update_label = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: r_seq_rowid,
        target_pc: update_existing_label,
    });

    program.emit_insn(Insn::NewRowid {
        cursor: seq_cursor_id,
        rowid_reg: r_seq_rowid,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: seq_cursor_id,
        key_reg: r_seq_rowid,
        record_reg,
        flag: InsertFlags::new(),
        table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
    });
    program.emit_insn(Insn::Goto {
        target_pc: end_update_label,
    });

    program.preassign_label_to_next_insn(update_existing_label);
    program.emit_insn(Insn::Insert {
        cursor: seq_cursor_id,
        key_reg: r_seq_rowid,
        record_reg,
        flag: InsertFlags(turso_parser::ast::ResolveType::Replace.bit_value() as u8),
        table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
    });

    program.preassign_label_to_next_insn(end_update_label);

    Ok(())
}

fn emit_replace_delete_conflicting_row(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    connection: &Arc<Connection>,
    ctx: &mut InsertEmitCtx,
    table_references: &mut TableReferences,
) -> Result<()> {
    program.emit_insn(Insn::SeekRowid {
        cursor_id: ctx.cursor_id,
        src_reg: ctx.conflict_rowid_reg,
        target_pc: ctx.halt_label,
    });

    // Phase 1: Before Delete - build parent key registers and handle NoAction/Restrict.
    // CASCADE/SetNull/SetDefault actions are prepared but deferred until after Delete.
    let prepared_fk_actions = if connection.foreign_keys_enabled() {
        let prepared = ForeignKeyActions::prepare_fk_delete_actions(
            program,
            resolver,
            ctx.target,
            ctx.cursor_id,
            ctx.conflict_rowid_reg,
            None,
        )?;
        if resolver.schema().has_child_fks(ctx.table.name.as_str()) {
            emit_fk_child_decrement_on_delete(
                program,
                ctx.table.as_ref(),
                ctx.table.name.as_str(),
                ctx.cursor_id,
                ctx.conflict_rowid_reg,
                ctx.database_id,
                resolver,
            )?;
        }
        prepared
    } else {
        ForeignKeyActions::default()
    };

    let table = &ctx.table;
    let table_name = table.name.as_str();
    let main_cursor_id = ctx.cursor_id;

    for index in &ctx.indexes {
        let index_value = index.value();
        let index_cursor_id = ctx
            .idx_cursors
            .iter()
            .find(|(name, _, _)| name == &index_value.name)
            .map(|(_, _, cursor)| *cursor)
            .expect("planned INSERT index has a cursor");
        let skip_delete_label =
            if let Some(predicate) = ctx.target.partial_index_predicate(index_value) {
                let skip_label = program.allocate_label();
                let reg = program.alloc_register();
                translate_plan_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    predicate,
                    reg,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                program.emit_insn(Insn::IfNot {
                    reg,
                    jump_if_null: true,
                    target_pc: skip_label,
                });
                Some(skip_label)
            } else {
                None
            };

        let num_regs = index_value.columns.len() + 1;
        let start_reg = program.alloc_registers(num_regs);

        for reg_offset in 0..index_value.columns.len() {
            emit_index_column_value_old_image(
                program,
                resolver,
                table_references,
                ctx.target,
                index,
                reg_offset,
                main_cursor_id,
                start_reg + reg_offset,
            )?;
        }
        program.emit_insn(Insn::Copy {
            src_reg: ctx.conflict_rowid_reg,
            dst_reg: start_reg + num_regs - 1,
            extra_amount: 0,
        });
        program.emit_insn(Insn::IdxDelete {
            start_reg,
            num_regs,
            cursor_id: index_cursor_id,
            raise_error_if_no_matching_entry: index_value.where_clause.is_none(),
        });

        if let Some(label) = skip_delete_label {
            program.preassign_label_to_next_insn(label);
        }
    }

    // CDC BEFORE, using rowid_reg
    if let Some(cdc_cursor_id) = ctx.cdc_table.as_ref().map(|(id, _tbl)| *id) {
        let cdc_has_before = program.capture_data_changes_info().has_before();
        let before_record_reg = if cdc_has_before {
            Some(emit_cdc_full_record(
                program,
                table.columns(),
                main_cursor_id,
                ctx.conflict_rowid_reg,
                table.is_strict,
            ))
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::DELETE,
            cdc_cursor_id,
            ctx.conflict_rowid_reg,
            before_record_reg,
            None,
            None,
            table_name,
        )?;
    }
    program.emit_insn(Insn::Delete {
        cursor_id: main_cursor_id,
        table_name: table_name.to_string(),
        is_part_of_update: true,
    });

    // Phase 2: After Delete - fire CASCADE/SetNull/SetDefault FK actions.
    prepared_fk_actions.fire_prepared_fk_delete_actions(
        program,
        resolver,
        connection,
        ctx.database_id,
    )?;

    Ok(())
}

/// Child-side FK checks for INSERT of a single row:
/// For each outgoing FK on `child_tbl`, if the NEW tuple's FK columns are all non-NULL,
/// verify that the referenced parent key exists.
pub fn emit_fk_child_insert_checks(
    program: &mut ProgramBuilder,
    child_tbl: &BTreeTable,
    new_start_reg: usize,
    new_rowid_reg: usize,
    resolver: &Resolver,
    database_id: usize,
    layout: &ColumnLayout,
) -> crate::Result<()> {
    for fk_ref in
        resolver.with_schema(database_id, |s| s.resolved_fks_for_child(&child_tbl.name))?
    {
        let is_self_ref = fk_ref.fk.parent_table.eq_ignore_ascii_case(&child_tbl.name);

        // Short-circuit if any NEW component is NULL
        let fk_ok = program.allocate_label();
        for cname in &fk_ref.fk.child_columns {
            let (i, col) = child_tbl.get_column(cname).unwrap();
            let src = if col.is_rowid_alias() {
                new_rowid_reg
            } else {
                layout.to_register(new_start_reg, i)
            };
            program.emit_insn(Insn::IsNull {
                reg: src,
                target_pc: fk_ok,
            });
        }
        let parent_tbl = resolver
            .with_schema(database_id, |s| s.get_btree_table(&fk_ref.fk.parent_table))
            .expect("parent btree");
        if fk_ref.parent_uses_rowid {
            let pcur = open_read_table(program, &parent_tbl, database_id);

            // first child col carries rowid
            let (i_child, col_child) = child_tbl.get_column(&fk_ref.fk.child_columns[0]).unwrap();
            let val_reg = if col_child.is_rowid_alias() {
                new_rowid_reg
            } else {
                layout.to_register(new_start_reg, i_child)
            };

            // Normalize rowid to integer for both the probe and the same-row fast path.
            let tmp = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: val_reg,
                dst_reg: tmp,
                extra_amount: 0,
            });
            let violation = program.allocate_label();
            program.emit_insn(Insn::MustBeInt {
                reg: tmp,
                target_pc: Some(violation),
            });

            // If this is a self-reference *and* the child FK equals NEW rowid,
            // the constraint will be satisfied once this row is inserted
            if is_self_ref {
                program.emit_insn(Insn::Eq {
                    lhs: tmp,
                    rhs: new_rowid_reg,
                    target_pc: fk_ok,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
            }

            program.emit_insn(Insn::NotExists {
                cursor: pcur,
                rowid_reg: tmp,
                target_pc: violation,
            });
            program.emit_insn(Insn::Close { cursor_id: pcur });
            program.emit_insn(Insn::Goto { target_pc: fk_ok });

            // Missing parent: immediate → Halt before Insert; deferred → counter
            program.preassign_label_to_next_insn(violation);
            program.emit_insn(Insn::Close { cursor_id: pcur });
            if fk_ref.fk.deferred {
                emit_fk_violation(program, &fk_ref.fk)?;
            } else {
                emit_fk_restrict_halt(program)?;
            }
            program.preassign_label_to_next_insn(fk_ok);
        } else {
            let idx = fk_ref
                .parent_unique_index
                .as_ref()
                .expect("parent unique index required");
            let ncols = fk_ref.fk.child_columns.len();

            if is_self_ref {
                // A self-referential INSERT is checked before the new row has
                // been written to the parent index. Without a shortcut, even
                // an exact self-reference would look like a missing parent.
                // SQLite handles that by comparing the pending INSERT registers
                // directly before it builds the affinity-coerced index probe.
                //
                //   CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER UNIQUE,
                //                  pk TEXT REFERENCES t(k));
                //   INSERT INTO t(id, k, pk) VALUES (1, 1, '1');
                //
                // In that INSERT image, the child key is pk=TEXT '1' and this
                // row's parent key is k=INTEGER 1. Those stored values are not
                // the same, so the same-row shortcut must not fire.
                //
                // The normal parent-index probe asks a different question:
                // "after applying the parent key affinity, does this child key
                // match some parent row that is already in the index?" That is
                // why cross-row checks may coerce TEXT '1' to INTEGER 1 before
                // seeking t(k). For this same-row case there is no parent index
                // entry yet, so reusing the coerced probe would invent a match
                // that SQLite rejects.
                let mismatch = program.allocate_label();
                for (i, &child_pos) in fk_ref.child_pos.iter().enumerate() {
                    let child_reg = if child_tbl.columns()[child_pos].is_rowid_alias() {
                        new_rowid_reg
                    } else {
                        layout.to_register(new_start_reg, child_pos)
                    };
                    let parent_pos = fk_ref.parent_pos[i];
                    let parent_reg = if child_tbl.columns()[parent_pos].is_rowid_alias() {
                        new_rowid_reg
                    } else {
                        layout.to_register(new_start_reg, parent_pos)
                    };
                    program.emit_insn(Insn::Ne {
                        lhs: child_reg,
                        rhs: parent_reg,
                        target_pc: mismatch,
                        flags: CmpInsFlags::default().jump_if_null(),
                        // Keep this as BINARY. Even with
                        // `k TEXT COLLATE NOCASE UNIQUE, pk TEXT REFERENCES t(k)`,
                        // SQLite rejects same-row INSERT `(k, pk)=('A', 'a')`;
                        // NOCASE applies to the later parent-index lookup only.
                        collation: Some(super::collate::CollationSeq::Binary),
                    });
                }
                // All equal: same-row OK
                program.emit_insn(Insn::Goto { target_pc: fk_ok });
                program.preassign_label_to_next_insn(mismatch);
            }

            let icur = open_read_index(program, idx, database_id);

            // Build NEW child probe from child NEW values, apply parent-index affinities.
            let probe = {
                let start = program.alloc_registers(ncols);
                for (k, cname) in fk_ref.fk.child_columns.iter().enumerate() {
                    let (i, col) = child_tbl.get_column(cname).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: if col.is_rowid_alias() {
                            new_rowid_reg
                        } else {
                            layout.to_register(new_start_reg, i)
                        },
                        dst_reg: start + k,
                        extra_amount: 0,
                    });
                }
                if let Some(cnt) = NonZeroUsize::new(ncols) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: start,
                        count: cnt,
                        affinities: build_index_affinity_string(idx, &parent_tbl),
                    });
                }
                start
            };
            index_probe(
                program,
                icur,
                probe,
                ncols,
                // on_found: parent exists, FK satisfied
                |_p| Ok(()),
                // on_not_found: immediate → Halt; deferred → counter
                |p| {
                    if fk_ref.fk.deferred {
                        emit_fk_violation(p, &fk_ref.fk)?;
                    } else {
                        emit_fk_restrict_halt(p)?;
                    }
                    Ok(())
                },
            )?;
            program.emit_insn(Insn::Goto { target_pc: fk_ok });
            program.preassign_label_to_next_insn(fk_ok);
        }
    }
    Ok(())
}

/// Build NEW parent key image in FK parent-column order into a contiguous register block.
/// Handles 3 shapes:
/// - parent_uses_rowid: single "rowid" component
/// - explicit fk.parent_columns
/// - fk.parent_columns empty => use parent's declared PK columns (order-preserving)
fn build_parent_key_image_for_insert(
    program: &mut ProgramBuilder,
    parent_table: &BTreeTable,
    pref: &ResolvedFkRef,
    insertion: &Insertion,
) -> crate::Result<(usize, usize)> {
    // Decide column list. When the parent is the rowid we force the literal name
    // "rowid" so the loop below routes through `insertion.key_register()`; otherwise
    // we reuse the already-resolved columns that `ResolvedFkRef` carries.
    let rowid_slot: [String; 1];
    let parent_cols: &[String] = if pref.parent_uses_rowid {
        rowid_slot = ["rowid".to_string()];
        &rowid_slot
    } else {
        &pref.parent_cols
    };

    let ncols = parent_cols.len();
    let start = program.alloc_registers(ncols);
    // Copy from the would-be parent insertion
    for (i, pname) in parent_cols.iter().enumerate() {
        let src = if pname.eq_ignore_ascii_case("rowid") {
            insertion.key_register()
        } else {
            // For rowid-alias parents, get_col_mapping_by_name will return the key mapping,
            // not the NULL placeholder in col_mappings.
            insertion
                .get_col_mapping_by_name(pname)
                .ok_or_else(|| {
                    crate::LimboError::PlanningError(format!(
                        "Column '{}' not present in INSERT image for parent {}",
                        pname, parent_table.name
                    ))
                })?
                .register
        };
        program.emit_insn(Insn::Copy {
            src_reg: src,
            dst_reg: start + i,
            extra_amount: 0,
        });
    }

    // Apply affinities of the parent columns (or integer for rowid)
    let aff: String = if pref.parent_uses_rowid {
        "i".to_string()
    } else {
        parent_cols
            .iter()
            .map(|name| {
                let (_, col) = parent_table.get_column(name).ok_or_else(|| {
                    crate::LimboError::InternalError(format!("parent col {name} missing"))
                })?;
                Ok::<_, crate::LimboError>(
                    col.affinity_with_strict(parent_table.is_strict).aff_mask(),
                )
            })
            .collect::<Result<String, _>>()?
    };
    if let Some(count) = NonZeroUsize::new(ncols) {
        program.emit_insn(Insn::Affinity {
            start_reg: start,
            count,
            affinities: aff,
        });
    }

    Ok((start, ncols))
}

/// Parent-side: when inserting into the parent, decrement the counter
/// if any child rows reference the NEW parent key.
/// We *always* do this for deferred FKs, and we *also* do it for
/// self-referential FKs (even if immediate) because the insert can
/// “repair” a prior child-insert count recorded earlier in the same statement.
pub fn emit_parent_side_fk_decrement_on_insert(
    program: &mut ProgramBuilder,
    parent_table: &BTreeTable,
    insertion: &Insertion,
    force_immediate: bool,
    resolver: &Resolver,
    database_id: usize,
) -> crate::Result<()> {
    for pref in resolver.with_schema(database_id, |s| {
        s.resolved_fks_referencing(&parent_table.name)
    })? {
        let is_self_ref = pref
            .child_table
            .name
            .eq_ignore_ascii_case(&parent_table.name);
        // Skip only when it cannot repair anything: non-deferred and not self-referencing
        if !force_immediate && !pref.fk.deferred && !is_self_ref {
            continue;
        }
        // Nothing to do if the parent counter is 0
        let skip_fk = program.allocate_label();
        program.emit_insn(Insn::FkIfZero {
            deferred: pref.fk.deferred,
            target_pc: skip_fk,
        });

        let (new_pk_start, n_cols) =
            build_parent_key_image_for_insert(program, parent_table, &pref, insertion)?;

        // Nothing to do if the key contains NULLs, because a NULL parent key
        // never matches any child row (SQL NULL semantics)
        emit_skip_if_any_null(program, new_pk_start, n_cols, skip_fk);

        let child_tbl = &pref.child_table;
        let child_cols = &pref.fk.child_columns;
        let indices: Vec<_> = resolver.with_schema(database_id, |s| {
            s.get_indices(&child_tbl.name).cloned().collect()
        });
        let idx = indices.iter().find(|ix| {
            ix.columns.len() == child_cols.len()
                && ix
                    .columns
                    .iter()
                    .zip(child_cols.iter())
                    .all(|(ic, cc)| ic.name.eq_ignore_ascii_case(cc))
        });

        if let Some(ix) = idx {
            let icur = open_read_index(program, ix, database_id);
            // Copy key into probe regs and apply child-index affinities
            let probe_start = program.alloc_registers(n_cols);
            for i in 0..n_cols {
                program.emit_insn(Insn::Copy {
                    src_reg: new_pk_start + i,
                    dst_reg: probe_start + i,
                    extra_amount: 0,
                });
            }
            if let Some(count) = NonZeroUsize::new(n_cols) {
                program.emit_insn(Insn::Affinity {
                    start_reg: probe_start,
                    count,
                    affinities: build_index_affinity_string(ix, child_tbl),
                });
            }

            // Decrement once per matching child row
            index_scan_match_any(program, icur, probe_start, n_cols, None, |p| {
                let next = p.allocate_label();
                emit_guarded_fk_decrement(p, next, pref.fk.deferred);
                p.preassign_label_to_next_insn(next);
                Ok(())
            })?;
        } else {
            // fallback scan :(
            let ccur = open_read_table(program, child_tbl, database_id);
            let done = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: ccur,
                pc_if_empty: done,
            });
            let loop_top = program.allocate_label();
            let next_row = program.allocate_label();
            program.preassign_label_to_next_insn(loop_top);

            for (i, child_name) in child_cols.iter().enumerate() {
                let (pos, _) = child_tbl.get_column(child_name).ok_or_else(|| {
                    crate::LimboError::InternalError(format!("child col {child_name} missing"))
                })?;
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: ccur,
                    column: pos,
                    dest: tmp,
                    default: None,
                });

                program.emit_insn(Insn::IsNull {
                    reg: tmp,
                    target_pc: next_row,
                });

                let cont = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: tmp,
                    rhs: new_pk_start + i,
                    target_pc: cont,
                    flags: CmpInsFlags::default().jump_if_null(),
                    collation: Some(super::collate::CollationSeq::Binary),
                });
                program.emit_insn(Insn::Goto {
                    target_pc: next_row,
                });
                program.preassign_label_to_next_insn(cont);
            }
            // Matched one child row: guarded decrement of counter
            emit_guarded_fk_decrement(program, next_row, pref.fk.deferred);
            program.preassign_label_to_next_insn(next_row);
            program.emit_insn(Insn::Next {
                cursor_id: ccur,
                pc_if_next: loop_top,
            });
            program.preassign_label_to_next_insn(done);
            program.emit_insn(Insn::Close { cursor_id: ccur });
        }
        program.preassign_label_to_next_insn(skip_fk);
    }
    Ok(())
}
