use crate::turso_assert;
// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use crate::sync::Arc;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::borrow::Cow;
use std::num::NonZeroUsize;
use turso_macros::match_ignore_ascii_case;

use tracing::{instrument, Level};
use turso_parser::ast::{
    self, Expr, Literal, ResolveType, SubqueryType, TableInternalId, TriggerEvent, TriggerTime,
};

use super::aggregation::emit_ungrouped_aggregation;
use super::expr::translate_expr;
use super::group_by::{
    group_by_agg_phase, group_by_emit_row_phase, init_group_by, GroupByMetadata, GroupByRowSource,
};
use super::main_loop::{
    close_loop, emit_loop, init_distinct, init_loop, open_loop, LeftJoinMetadata, LoopLabels,
};
use super::order_by::{emit_order_by, init_order_by, SortMetadata};
use super::plan::{
    Distinctness, JoinOrderMember, Operation, Scan, SeekKeyComponent, SelectPlan, TableReferences,
    UpdatePlan,
};
use super::select::emit_simple_count;
use super::subquery::emit_from_clause_subqueries;
use crate::error::{
    SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE,
};
use crate::function::Func;
use crate::schema::{
    BTreeTable, CheckConstraint, Column, Index, IndexColumn, Schema, Table, ROWID_SENTINEL,
};
use crate::translate::compound_select::emit_program_for_compound_select;
use crate::translate::expr::{
    bind_and_rewrite_expr, emit_returning_results, rewrite_between_expr,
    translate_expr_no_constant_opt, walk_expr, walk_expr_mut, BindingBehavior, NoConstantOptReason,
    WalkControl,
};
use crate::translate::fkeys::{
    build_index_affinity_string, emit_fk_child_update_counters, emit_fk_update_parent_actions,
    emit_guarded_fk_decrement, fire_fk_delete_actions, fire_fk_update_actions, open_read_index,
    open_read_table, stabilize_new_row_for_fk,
};
use crate::translate::plan::{
    DeletePlan, EphemeralRowidMode, EvalAt, IndexMethodQuery, JoinedTable, NonFromClauseSubquery,
    Plan, QueryDestination, ResultSetColumn, Search,
};
use crate::translate::planner::ROWID_STRS;
use crate::translate::planner::{table_mask_from_expr, TableMask};
use crate::translate::subquery::emit_non_from_clause_subquery;
use crate::translate::trigger_exec::{
    fire_trigger, get_relevant_triggers_type_and_time, has_relevant_triggers_type_only,
    TriggerContext,
};
use crate::translate::values::emit_values;
use crate::translate::window::{emit_window_results, init_window, WindowMetadata};
use crate::util::{
    check_expr_references_column, exprs_are_equivalent, normalize_ident, parse_numeric_literal,
};
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u16, {CmpInsFlags, IdxInsertFlags, InsertFlags, RegisterOrLiteral},
};
use crate::vdbe::{insn::Insn, BranchOffset, CursorID};
use crate::{
    bail_parse_error, emit_explain, Database, DatabaseCatalog, LimboError, Result, RwLock,
    SymbolTable,
};
use crate::{CaptureDataChangesExt, Connection, QueryMode};

/// Initialize EXISTS subquery result registers to 0, but only for subqueries that haven't
/// been evaluated yet (i.e., correlated subqueries that will be evaluated in the loop).
/// Non-correlated EXISTS subqueries are evaluated before the loop and their result_reg
/// is already properly initialized and populated by emit_non_from_clause_subquery.
fn init_exists_result_regs(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    non_from_clause_subqueries: &[NonFromClauseSubquery],
) {
    let _ = walk_expr(expr, &mut |e| {
        if let ast::Expr::SubqueryResult {
            subquery_id,
            query_type: SubqueryType::Exists { result_reg },
            ..
        } = e
        {
            // Only initialize if the subquery hasn't been evaluated yet.
            // Non-correlated EXISTS subqueries are evaluated before the loop and their
            // result_reg is already set correctly. Initializing them here would overwrite
            // the correct result with 0.
            let already_evaluated = non_from_clause_subqueries
                .iter()
                .find(|s| s.internal_id == *subquery_id)
                .is_some_and(|s| s.has_been_evaluated());
            if !already_evaluated {
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
            }
        }
        Ok(WalkControl::Continue)
    });
}

// Would make more sense to not have RwLock for the attached databases and get all the schemas on prepare,
// because there could be some data race where at 1 point you check the attached db, it has a table,
// but after some write it could not be there anymore. However, leaving it as it is to avoid more complicated logic on something that is experimental
pub struct Resolver<'a> {
    schema: &'a Schema,
    database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
    attached_databases: &'a RwLock<DatabaseCatalog>,
    pub symbol_table: &'a SymbolTable,
    pub expr_to_reg_cache_enabled: bool,
    pub expr_to_reg_cache: Vec<(Cow<'a, ast::Expr>, usize)>,
    /// Maps register indices to column affinities for expression index evaluation.
    /// Populated temporarily during UPDATE new-image expression index key computation,
    /// where column references have been rewritten to Expr::Register and comparison
    /// operators need the original column affinity. Analogous to SQLite's iSelfTab
    /// mechanism, but operates as a side-channel since limbo rewrites the AST rather
    /// than redirecting column reads at codegen time.
    pub register_affinities: HashMap<usize, Affinity>,
}

impl<'a> Resolver<'a> {
    const MAIN_DB: &'static str = "main";
    const TEMP_DB: &'static str = "temp";

    const MAIN_DB_ID: usize = 0;
    const TEMP_DB_ID: usize = 1;

    pub(crate) fn new(
        schema: &'a Schema,
        database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
        attached_databases: &'a RwLock<DatabaseCatalog>,
        symbol_table: &'a SymbolTable,
    ) -> Self {
        Self {
            schema,
            database_schemas,
            attached_databases,
            symbol_table,
            expr_to_reg_cache_enabled: false,
            expr_to_reg_cache: Vec::new(),
            register_affinities: HashMap::default(),
        }
    }

    pub fn schema(&self) -> &Schema {
        self.schema
    }

    pub fn fork(&self) -> Resolver<'a> {
        Resolver {
            schema: self.schema,
            database_schemas: self.database_schemas,
            attached_databases: self.attached_databases,
            symbol_table: self.symbol_table,
            expr_to_reg_cache_enabled: false,
            expr_to_reg_cache: Vec::new(),
            register_affinities: HashMap::default(),
        }
    }

    pub fn resolve_function(&self, func_name: &str, arg_count: usize) -> Option<Func> {
        match Func::resolve_function(func_name, arg_count).ok() {
            Some(func) => Some(func),
            None => self
                .symbol_table
                .resolve_function(func_name, arg_count)
                .map(Func::External),
        }
    }

    pub(crate) fn enable_expr_to_reg_cache(&mut self) {
        self.expr_to_reg_cache_enabled = true;
    }

    /// Returns the register for a previously translated expression, if caching is enabled.
    ///
    /// We scan from newest to oldest so later translations win when equivalent
    /// expressions are seen multiple times in the same translation pass.
    pub fn resolve_cached_expr_reg(&self, expr: &ast::Expr) -> Option<usize> {
        if self.expr_to_reg_cache_enabled {
            self.expr_to_reg_cache
                .iter()
                .rev()
                .find(|(e, _)| exprs_are_equivalent(expr, e))
                .map(|(_, reg)| *reg)
        } else {
            None
        }
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        if database_id == Self::MAIN_DB_ID {
            // Main database - use connection's schema which should be kept in sync
            f(self.schema)
        } else if database_id == Self::TEMP_DB_ID {
            // Temp database - uses same schema as main for now, but this will change later.
            f(self.schema)
        } else {
            // Attached database - check cache first, then load from database
            let mut schemas = self.database_schemas.write();

            if let Some(cached_schema) = schemas.get(&database_id) {
                return f(cached_schema);
            }

            // Schema not cached, load it lazily from the attached database
            let attached_dbs = self.attached_databases.read();
            let (db, _pager) = attached_dbs
                .index_to_data
                .get(&database_id)
                .expect("Database ID should be valid after resolve_database_id");

            let schema = db.schema.lock().clone();

            // Cache the schema for future use
            schemas.insert(database_id, schema.clone());

            f(&schema)
        }
    }

    /// Resolve database ID from a qualified name
    pub(crate) fn resolve_database_id(&self, qualified_name: &ast::QualifiedName) -> Result<usize> {
        use crate::util::normalize_ident;

        // Check if this is a qualified name (database.table) or unqualified
        if let Some(db_name) = &qualified_name.db_name {
            let db_name_normalized = normalize_ident(db_name.as_str());
            let name_bytes = db_name_normalized.as_bytes();
            match_ignore_ascii_case!(match name_bytes {
                b"main" => Ok(0),
                b"temp" => Ok(1),
                _ => {
                    // Look up attached database
                    if let Some((idx, _attached_db)) =
                        self.get_attached_database(&db_name_normalized)
                    {
                        Ok(idx)
                    } else {
                        Err(LimboError::InvalidArgument(format!(
                            "no such database: {db_name_normalized}"
                        )))
                    }
                }
            })
        } else {
            // Unqualified table name - use main database
            Ok(0)
        }
    }

    // Get an attached database by alias name
    pub(crate) fn get_attached_database(&self, alias: &str) -> Option<(usize, Arc<Database>)> {
        self.attached_databases.read().get_database_by_name(alias)
    }

    /// Get the database name for a given database index.
    /// Returns "main" for index 0, "temp" for index 1, and the alias for attached databases.
    pub(crate) fn get_database_name_by_index(&self, index: usize) -> Option<String> {
        match index {
            0 => Some(Self::MAIN_DB.to_string()),
            1 => Some(Self::TEMP_DB.to_string()),
            _ => self.attached_databases.read().get_name_by_index(index),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LimitCtx {
    /// Register holding the LIMIT value (e.g. LIMIT 5)
    pub reg_limit: usize,
    /// Whether to initialize the LIMIT counter to the LIMIT value;
    /// There are cases like compound SELECTs where all the sub-selects
    /// utilize the same limit register, but it is initialized only once.
    pub initialize_counter: bool,
}

/// Identifies a value stored in a materialized hash-build input.
///
/// These references are used to map payload registers back to the original
/// table expressions during hash-probe evaluation. They are deliberately
/// table-qualified so payloads can span multiple tables when the build input
/// is derived from a join prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MaterializedColumnRef {
    /// A concrete column from a specific table, including rowid alias metadata.
    Column {
        table_id: TableInternalId,
        column_idx: usize,
        is_rowid_alias: bool,
    },
    /// The implicit rowid (or integer primary key) of a specific table.
    RowId { table_id: TableInternalId },
}

/// Describes how a hash-join build input was materialized.
///
/// Rowid-only materialization preserves prior join constraints while keeping
/// the hash table payload small, but requires `SeekRowid` into the build table
/// during probing. Key+payload materialization stores the join keys and needed
/// payload columns directly so the hash build can operate without seeking.
#[derive(Debug, Clone)]
pub enum MaterializedBuildInputMode {
    /// Ephemeral table contains only build-side rowids.
    RowidOnly,
    /// Ephemeral table contains join keys followed by payload columns.
    KeyPayload {
        /// Number of join keys stored at the start of each row.
        num_keys: usize,
        /// Payload columns (after the keys) in ephemeral-table order.
        payload_columns: Vec<MaterializedColumnRef>,
    },
}

/// Metadata for a materialized build input keyed by build table index.
///
/// The cursor refers to the ephemeral table containing the materialized rows.
/// `prefix_tables` tracks which join-prefix tables were captured so we can
/// prune redundant scans from downstream join orders.
#[derive(Debug, Clone)]
pub struct MaterializedBuildInput {
    /// Cursor id for the ephemeral table holding the materialized rows.
    pub cursor_id: CursorID,
    /// Encoding mode for the materialized rows.
    pub mode: MaterializedBuildInputMode,
    /// Join-prefix table indices folded into this materialization.
    pub prefix_tables: Vec<usize>,
}

impl LimitCtx {
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            reg_limit: program.alloc_register(),
            initialize_counter: true,
        }
    }

    pub fn new_shared(reg_limit: usize) -> Self {
        Self {
            reg_limit,
            initialize_counter: false,
        }
    }
}
#[derive(Debug, Clone)]
pub struct HashCtx {
    pub match_reg: usize,
    pub hash_table_reg: usize,
    /// Label for hash join match processing (points to just after HashProbe instruction)
    /// Used by HashNext to jump back to process additional matches without re-probing
    pub match_found_label: BranchOffset,
    /// Label for advancing to the next hash match (points to HashNext instruction).
    /// When conditions fail within a hash join, they should jump here to try the next
    /// hash match, rather than jumping to the outer loop's next label.
    pub hash_next_label: BranchOffset,
    /// Starting register where payload columns are stored after HashProbe/HashNext.
    /// None if payload optimization is not used for this hash join.
    pub payload_start_reg: Option<usize>,
    /// Column references stored in payload, in order.
    /// `payload_start_reg + i` contains the value for `payload_columns[i]`.
    /// These references may point at multiple tables when a build input was
    /// materialized from a join prefix.
    pub payload_columns: Vec<MaterializedColumnRef>,
}

/// The TranslateCtx struct holds various information and labels used during bytecode generation.
/// It is used for maintaining state and control flow during the bytecode
/// generation process.
pub struct TranslateCtx<'a> {
    // A typical query plan is a nested loop. Each loop has its own LoopLabels (see the definition of LoopLabels for more details)
    pub labels_main_loop: Vec<LoopLabels>,
    // label for the instruction that jumps to the next phase of the query after the main loop
    // we don't know ahead of time what that is (GROUP BY, ORDER BY, etc.)
    pub label_main_loop_end: Option<BranchOffset>,
    // First register of the aggregation results
    pub reg_agg_start: Option<usize>,
    // In non-group-by statements with aggregations (e.g. SELECT foo, bar, sum(baz) FROM t),
    // we want to emit the non-aggregate columns (foo and bar) only once.
    // This register is a flag that tracks whether we have already done that.
    pub reg_nonagg_emit_once_flag: Option<usize>,
    // First register of the result columns of the query
    pub reg_result_cols_start: Option<usize>,
    pub limit_ctx: Option<LimitCtx>,
    // The register holding the offset value, if any.
    pub reg_offset: Option<usize>,
    // The register holding the limit+offset value, if any.
    pub reg_limit_offset_sum: Option<usize>,
    // metadata for the group by operator
    pub meta_group_by: Option<GroupByMetadata>,
    // metadata for the order by operator
    pub meta_sort: Option<SortMetadata>,
    /// mapping between table loop index and associated metadata (for left joins only)
    /// this metadata exists for the right table in a given left join
    pub meta_left_joins: Vec<Option<LeftJoinMetadata>>,
    pub resolver: Resolver<'a>,
    /// Hash table contexts for hash joins, keyed by build table index.
    pub hash_table_contexts: HashMap<usize, HashCtx>,
    /// Materialized build inputs for hash joins, keyed by build table index.
    /// These entries are reused during nested materialization so we avoid
    /// re-scanning prefix tables and preserve prior join constraints.
    pub materialized_build_inputs: HashMap<usize, MaterializedBuildInput>,
    /// A list of expressions that are not aggregates, along with a flag indicating
    /// whether the expression should be included in the output for each group.
    ///
    /// Each entry is a tuple:
    /// - `&'ast Expr`: the expression itself
    /// - `bool`: `true` if the expression should be included in the output for each group, `false` otherwise.
    ///
    /// The order of expressions is **significant**:
    /// - First: all `GROUP BY` expressions, in the order they appear in the `GROUP BY` clause.
    /// - Then: remaining non-aggregate expressions that are not part of `GROUP BY`.
    pub non_aggregate_expressions: Vec<(&'a Expr, bool)>,
    /// Cursor id for cdc table (if capture_data_changes PRAGMA is set and query can modify the data)
    pub cdc_cursor_id: Option<usize>,
    pub meta_window: Option<WindowMetadata<'a>>,
}

impl<'a> TranslateCtx<'a> {
    pub fn new(program: &mut ProgramBuilder, resolver: Resolver<'a>, table_count: usize) -> Self {
        TranslateCtx {
            labels_main_loop: (0..table_count).map(|_| LoopLabels::new(program)).collect(),
            label_main_loop_end: None,
            reg_agg_start: None,
            reg_nonagg_emit_once_flag: None,
            limit_ctx: None,
            reg_offset: None,
            reg_limit_offset_sum: None,
            reg_result_cols_start: None,
            meta_group_by: None,
            meta_left_joins: (0..table_count).map(|_| None).collect(),
            meta_sort: None,
            hash_table_contexts: HashMap::default(),
            materialized_build_inputs: HashMap::default(),
            resolver,
            non_aggregate_expressions: Vec::new(),
            cdc_cursor_id: None,
            meta_window: None,
        }
    }
}

#[derive(Debug, Clone)]
/// Update row source for UPDATE statements
/// `Normal` is the default mode, it will iterate either the table itself or an index on the table.
/// `PrebuiltEphemeralTable` is used when an ephemeral table containing the target rowids to update has
/// been built and it is being used for iteration.
pub enum UpdateRowSource {
    /// Iterate over the table itself or an index on the table
    Normal,
    /// Iterate over an ephemeral table containing the target rowids to update
    PrebuiltEphemeralTable {
        /// The cursor id of the ephemeral table that is being used to iterate the target rowids to update.
        ephemeral_table_cursor_id: usize,
        /// The table that is being updated.
        target_table: Arc<JoinedTable>,
    },
}

/// Used to distinguish database operations
#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug, Clone)]
pub enum OperationMode {
    SELECT,
    INSERT,
    UPDATE(UpdateRowSource),
    DELETE,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Sqlite always considers Read transactions implicit
pub enum TransactionMode {
    None,
    Read,
    Write,
    Concurrent,
}

/// Main entry point for emitting bytecode for a SQL query
/// Takes a query plan and generates the corresponding bytecode program
#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    plan: Plan,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    match plan {
        Plan::Select(plan) => emit_program_for_select(program, resolver, plan),
        Plan::Delete(plan) => emit_program_for_delete(connection, resolver, program, plan),
        Plan::Update(plan) => emit_program_for_update(connection, resolver, program, plan, after),
        Plan::CompoundSelect { .. } => {
            emit_program_for_compound_select(program, resolver, plan).map(|_| ())
        }
    }
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program_for_select(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut plan: SelectPlan,
) -> Result<()> {
    let materialized_build_inputs = emit_materialized_build_inputs(program, resolver, &mut plan)?;
    emit_program_for_select_with_inputs(program, resolver, plan, materialized_build_inputs)
}

/// Returns the single-column schema used by rowid-only hash build inputs.
fn build_rowid_column() -> Column {
    Column::new_default_integer(Some("build_rowid".to_string()), "INTEGER".to_string(), None)
}

fn emit_program_for_select_with_inputs(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut plan: SelectPlan,
    materialized_build_inputs: HashMap<usize, MaterializedBuildInput>,
) -> Result<()> {
    let mut t_ctx = TranslateCtx::new(
        program,
        resolver.fork(),
        plan.table_references.joined_tables().len(),
    );
    t_ctx.materialized_build_inputs = materialized_build_inputs;

    // Emit main parts of query
    let result_cols_start = emit_query(program, &mut plan, &mut t_ctx)?;
    // TODO: This solution works but it's just a hack/quick fix.
    // Ideally we should do some refactor on how we scope queries, subqueries, and registers so that we don't have to do these ad-hoc/unintuitive adjustments.

    // Restore reg_result_cols_start after emit_query, because nested subqueries
    // (e.g. correlated scalar subqueries in WHERE) can overwrite
    // program.reg_result_cols_start with their own result column registers.
    program.reg_result_cols_start = Some(result_cols_start);

    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    Ok(())
}

#[derive(Debug, Clone)]
/// Captures the parameters needed to materialize one hash-build input.
struct MaterializationSpec {
    build_table_idx: usize,
    probe_table_idx: usize,
    mode: MaterializedBuildInputMode,
    prefix_tables: Vec<usize>,
    key_exprs: Vec<Expr>,
    payload_columns: Vec<MaterializedColumnRef>,
}

/// Build materialized hash-build inputs for hash joins that depend on prior joins.
///
/// A materialized build input is an ephemeral table that captures the rows
/// a hash join is allowed to build from after earlier joins and filters have
/// been applied. This prevents the build side from being re-scanned in its
/// full, unfiltered form when prior join constraints must be respected.
///
/// The materialization uses a join-prefix: all tables that appear before the
/// probe table in the join order, plus the build table itself. This prefix
/// represents the minimal context needed to evaluate build-side constraints.
/// For probe->build chaining we store join keys and payload columns directly
/// in the ephemeral table; otherwise we only store rowids and `SeekRowid`
/// during probing when needed.
fn emit_materialized_build_inputs(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: &mut SelectPlan,
) -> Result<HashMap<usize, MaterializedBuildInput>> {
    let mut build_inputs: HashMap<usize, MaterializedBuildInput> = HashMap::default();
    let mut materializations: Vec<MaterializationSpec> = Vec::new();
    let mut hash_tables_to_keep_open: HashSet<usize> = HashSet::default();

    // Keep hash tables open while running materialization subplans so we can reuse them.
    // A build table may appear in multiple hash joins when chaining, so we do not
    // treat repeated build tables as an error.
    for table in plan.table_references.joined_tables().iter() {
        if let Operation::HashJoin(hash_join_op) = &table.op {
            let build_table = &plan.table_references.joined_tables()[hash_join_op.build_table_idx];
            hash_tables_to_keep_open.insert(build_table.internal_id.into());
        }
    }

    let mut seen_build_tables: HashSet<usize> = HashSet::default();

    // decide per-hash-join materialization mode (rowid-only vs key+payload).
    for member in plan.join_order.iter() {
        let table = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table.op {
            if !hash_join_op.materialize_build_input
                || !seen_build_tables.insert(hash_join_op.build_table_idx)
            {
                continue;
            }

            let probe_table_idx = hash_join_op.probe_table_idx;
            let probe_pos = plan
                .join_order
                .iter()
                .position(|member| member.original_idx == probe_table_idx)
                .unwrap_or(plan.join_order.len());
            let build_table_was_prior_probe = plan.join_order[..probe_pos].iter().any(|member| {
                let table_ref = &plan.table_references.joined_tables()[member.original_idx];
                matches!(
                    table_ref.op,
                    Operation::HashJoin(ref hj) if hj.probe_table_idx == hash_join_op.build_table_idx
                )
            });

            // The join prefix is the set of tables we include when building this hash
            // input (all tables before the probe + the build table). If the prefix
            // has *any* table besides the build table, then rowid-only materialization
            // is unsafe. Here's why:
            //
            // Rowid-only keeps each build-table rowid at most once. That throws away
            // which prefix row it came from, so we lose the one-to-one link between
            // a prefix match and a build row.
            //
            // Example (t1 is a left-side table earlier in the join order):
            //   t1 rows:     t1_1(c=1), t1_2(c=2)
            //   t2 rows:     t2_7(c=1), t2_8(c=2)   (build table)
            //   t3 rows:     one row per t2 row
            //
            // Correct result after joining:
            //   t1_1 + t2_7 + t2_7's t3 row
            //   t1_2 + t2_8 + t2_8's t3 row   (2 rows)
            //
            // Key+payload materialization lets us PRUNE the prefix tables (like t1)
            // from the main join order, because their needed columns now live in
            // the payload. So the main plan does NOT loop t1 again.
            //
            // However, rowid-only materialization keeps just {t2_7, t2_8} with no link to t1_1/t1_2.
            // Since t1 stays in the main join loop, each t1 row joins against the
            // materialized t2 set. With no t1→t2 correlation, every t1 row matches
            // both t2 rows, incorrectly producing 4 rows (a cross product).
            //
            // Therefore: if the prefix has other tables, we must store key+payload
            // rows so each prefix match stays distinct and the main plan can drop
            // the prefix loops.
            let (_, included_tables) =
                materialization_prefix(plan, hash_join_op.build_table_idx, probe_table_idx)?;
            let prefix_has_other_tables = included_tables
                .iter()
                .any(|table_idx| *table_idx != hash_join_op.build_table_idx);

            if build_table_was_prior_probe || prefix_has_other_tables {
                // Prior probe -> build chaining OR any multi-table prefix requires keys+payload
                // so we do not lose multiplicity or correlation.
                let payload_columns = collect_materialized_payload_columns(plan, &included_tables)?;
                let key_exprs: Vec<Expr> = hash_join_op
                    .join_keys
                    .iter()
                    .map(|key| key.get_build_expr(&plan.where_clause).clone())
                    .collect();
                let mode = MaterializedBuildInputMode::KeyPayload {
                    num_keys: key_exprs.len(),
                    payload_columns: payload_columns.clone(),
                };
                materializations.push(MaterializationSpec {
                    build_table_idx: hash_join_op.build_table_idx,
                    probe_table_idx,
                    mode,
                    prefix_tables: included_tables,
                    key_exprs,
                    payload_columns,
                });
            } else {
                // Single-table prefix: a rowid list preserves the build-side filters
                // without losing multiplicity (as explained in the comment above).
                materializations.push(MaterializationSpec {
                    build_table_idx: hash_join_op.build_table_idx,
                    probe_table_idx,
                    mode: MaterializedBuildInputMode::RowidOnly,
                    prefix_tables: Vec::new(),
                    key_exprs: Vec::new(),
                    payload_columns: Vec::new(),
                });
            }
        }
    }

    // Now we emit each of the materialization subplans into an ephemeral table.
    for spec in materializations.iter() {
        let build_table = &plan.table_references.joined_tables()[spec.build_table_idx];
        let build_table_name = if build_table.table.get_name() == build_table.identifier {
            build_table.identifier.clone()
        } else {
            format!(
                "{} AS {}",
                build_table.table.get_name(),
                build_table.identifier
            )
        };
        let internal_id = program.table_reference_counter.next();
        let columns = match &spec.mode {
            MaterializedBuildInputMode::RowidOnly => vec![build_rowid_column()],
            MaterializedBuildInputMode::KeyPayload {
                num_keys,
                payload_columns,
            } => build_materialized_input_columns(*num_keys, payload_columns),
        };
        let ephemeral_table = Arc::new(BTreeTable {
            root_page: 0,
            name: format!("hash_build_input_{internal_id}"),
            has_rowid: true,
            has_autoincrement: false,
            primary_key_columns: vec![],
            columns,
            is_strict: false,
            unique_sets: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        });
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ephemeral_table.clone()));

        // Build a plan that emits only rowids for the build table using the join prefix
        // that makes the hash join legal (including any earlier hash joins).
        let materialize_plan = build_materialized_build_input_plan(
            plan,
            spec.build_table_idx,
            spec.probe_table_idx,
            cursor_id,
            ephemeral_table,
            &spec.mode,
            &spec.key_exprs,
            &spec.payload_columns,
            &build_inputs,
        )?;

        // Make the materialization plan show up as a subtree in EXPLAIN QUERY PLAN output.
        emit_explain!(
            program,
            true,
            format!("MATERIALIZE hash build input for {build_table_name}")
        );
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id,
            is_table: true,
        });
        program.nested(|program| -> Result<()> {
            program.set_hash_tables_to_keep_open(&hash_tables_to_keep_open);
            emit_program_for_select_with_inputs(
                program,
                resolver,
                materialize_plan,
                build_inputs.clone(),
            )?;
            program.clear_hash_tables_to_keep_open();
            Ok(())
        })?;
        program.pop_current_parent_explain();

        build_inputs.insert(
            spec.build_table_idx,
            MaterializedBuildInput {
                cursor_id,
                mode: spec.mode.clone(),
                prefix_tables: spec.prefix_tables.clone(),
            },
        );
    }

    // Drop any join-prefix tables already captured by key+payload materializations.
    prune_join_order_for_materialized_inputs(plan, &build_inputs)?;

    #[cfg(debug_assertions)]
    turso_assert!(
        {
            let join_order_tables: HashSet<_> = plan
                .join_order
                .iter()
                .map(|member| member.original_idx)
                .collect();
            let build_tables_in_plan: HashSet<_> = plan
                .join_order
                .iter()
                .filter_map(|member| {
                    let table = &plan.table_references.joined_tables()[member.original_idx];
                    if let Operation::HashJoin(hash_join_op) = &table.op {
                        Some(hash_join_op.build_table_idx)
                    } else {
                        None
                    }
                })
                .collect();
            build_inputs.iter().all(|(build_table_idx, input)| {
                if !build_tables_in_plan.contains(build_table_idx) {
                    return true;
                }
                if !matches!(input.mode, MaterializedBuildInputMode::KeyPayload { .. }) {
                    return true;
                }
                input
                    .prefix_tables
                    .iter()
                    .all(|table_idx| !join_order_tables.contains(table_idx))
            })
        },
        "materialized build input prefix table still present in join order"
    );
    Ok(build_inputs)
}

/// Remove join-order entries already satisfied by key+payload materializations.
///
/// This prevents redundant scans (and cross products) when a hash-build input
/// already captures a join prefix. It also marks fully covered WHERE terms as
/// consumed so they are not re-applied later in the main plan.
fn prune_join_order_for_materialized_inputs(
    plan: &mut SelectPlan,
    build_inputs: &HashMap<usize, MaterializedBuildInput>,
) -> Result<()> {
    if build_inputs.is_empty() {
        return Ok(());
    }

    let mut build_tables_in_plan = HashSet::default();
    for member in plan.join_order.iter() {
        let table = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table.op {
            build_tables_in_plan.insert(hash_join_op.build_table_idx);
        }
    }

    let mut tables_to_remove: HashSet<usize> = HashSet::default();
    for (build_table_idx, input) in build_inputs.iter() {
        if !build_tables_in_plan.contains(build_table_idx) {
            continue;
        }
        if matches!(input.mode, MaterializedBuildInputMode::KeyPayload { .. }) {
            tables_to_remove.extend(input.prefix_tables.iter().copied());
        }
    }

    if tables_to_remove.is_empty() {
        return Ok(());
    }

    let prefix_mask = TableMask::from_table_number_iter(tables_to_remove.iter().copied());
    for term in plan.where_clause.iter_mut() {
        if term.consumed {
            continue;
        }
        let mask = table_mask_from_expr(
            &term.expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        if prefix_mask.contains_all(&mask) {
            term.consumed = true;
        }
    }
    plan.join_order
        .retain(|member| !tables_to_remove.contains(&member.original_idx));
    Ok(())
}

/// Compute the join-prefix used to materialize a hash-build input.
///
/// The prefix consists of all tables before the probe table plus the build
/// table itself (if not already present). The returned `included_tables`
/// list also includes build tables of earlier hash joins so payload collection
/// can capture all referenced columns.
fn materialization_prefix(
    plan: &SelectPlan,
    build_table_idx: usize,
    probe_table_idx: usize,
) -> Result<(Vec<JoinOrderMember>, Vec<usize>)> {
    let mut join_order = plan.join_order.clone();
    if join_order
        .iter()
        .all(|member| member.original_idx != probe_table_idx)
    {
        let probe_table = &plan.table_references.joined_tables()[probe_table_idx];
        join_order.push(JoinOrderMember {
            table_id: probe_table.internal_id,
            original_idx: probe_table_idx,
            is_outer: probe_table
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.outer),
        });
    }
    let probe_pos = join_order
        .iter()
        .position(|m| m.original_idx == probe_table_idx)
        .expect("probe table just ensured in join order");

    // Only include tables prior to the probe table. The materialization subplan
    // should filter the build table using prior join constraints, not scan the probe.
    let mut prefix_join_order = join_order[..probe_pos].to_vec();
    if prefix_join_order
        .iter()
        .all(|member| member.original_idx != build_table_idx)
    {
        let build_table = &plan.table_references.joined_tables()[build_table_idx];
        prefix_join_order.push(JoinOrderMember {
            table_id: build_table.internal_id,
            original_idx: build_table_idx,
            is_outer: build_table
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.outer),
        });
    }

    let mut included_tables: Vec<usize> =
        prefix_join_order.iter().map(|m| m.original_idx).collect();
    for member in prefix_join_order.iter() {
        let table_ref = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table_ref.op {
            included_tables.push(hash_join_op.build_table_idx);
        }
    }
    included_tables.sort_unstable();
    included_tables.dedup();

    Ok((prefix_join_order, included_tables))
}

/// Collect the payload columns needed for a materialized build input.
///
/// This gathers referenced columns from the included tables and always adds
/// rowids for tables that have them so probe-side expressions can be satisfied
/// without seeking back into base tables.
fn collect_materialized_payload_columns(
    plan: &SelectPlan,
    included_tables: &[usize],
) -> Result<Vec<MaterializedColumnRef>> {
    let mut payload_columns: Vec<MaterializedColumnRef> = Vec::new();
    let mut seen: HashSet<MaterializedColumnRef> = HashSet::default();
    for table_idx in included_tables.iter().copied() {
        let table = &plan.table_references.joined_tables()[table_idx];
        for col_idx in table.col_used_mask.iter() {
            let is_rowid_alias = table
                .columns()
                .get(col_idx)
                .is_some_and(|col| col.is_rowid_alias());
            let col_ref = MaterializedColumnRef::Column {
                table_id: table.internal_id,
                column_idx: col_idx,
                is_rowid_alias,
            };
            if seen.insert(col_ref.clone()) {
                payload_columns.push(col_ref);
            }
        }
        if table.btree().is_some_and(|btree| btree.has_rowid) {
            let rowid_ref = MaterializedColumnRef::RowId {
                table_id: table.internal_id,
            };
            if seen.insert(rowid_ref.clone()) {
                payload_columns.push(rowid_ref);
            }
        }
    }
    Ok(payload_columns)
}

/// Build the ephemeral-table schema for key+payload materializations.
///
/// Keys are stored first (typed as BLOB for join-key affinity handling),
/// followed by payload columns with integer or blob affinity.
fn build_materialized_input_columns(
    num_keys: usize,
    payload_columns: &[MaterializedColumnRef],
) -> Vec<Column> {
    let mut columns = Vec::with_capacity(num_keys + payload_columns.len());
    for i in 0..num_keys {
        columns.push(Column::new_default_text(
            Some(format!("key_{i}")),
            "BLOB".to_string(),
            None,
        ));
    }
    for (i, payload) in payload_columns.iter().enumerate() {
        let name = Some(format!("payload_{i}"));
        let column = match payload {
            MaterializedColumnRef::RowId { .. } => {
                Column::new_default_integer(name, "INTEGER".to_string(), None)
            }
            MaterializedColumnRef::Column { .. } => {
                Column::new_default_text(name, "BLOB".to_string(), None)
            }
        };
        columns.push(column);
    }
    columns
}

/// Construct a SELECT plan that materializes build-side inputs into an ephemeral table.
/// This plan is separate from the main query plan and is exclusively used for the materialization.
/// process.
///
/// The join order is the original prefix up to (but excluding) the probe table, plus
/// the build table itself. This filters build rows using only prior join constraints
/// and then prunes any tables already captured by earlier key+payload materializations.
#[allow(clippy::too_many_arguments)]
fn build_materialized_build_input_plan(
    plan: &SelectPlan,
    build_table_idx: usize,
    probe_table_idx: usize,
    cursor_id: CursorID,
    table: Arc<BTreeTable>,
    mode: &MaterializedBuildInputMode,
    key_exprs: &[Expr],
    payload_columns: &[MaterializedColumnRef],
    materialized_build_inputs: &HashMap<usize, MaterializedBuildInput>,
) -> Result<SelectPlan> {
    // Build a materialization subplan that only includes the join prefix
    // (all tables prior to the probe + the build table). The resulting plan
    // is smaller than the original select plan, so any access methods or
    // predicates that depend on tables outside this prefix must be dropped.
    let (join_order, included_tables) =
        materialization_prefix(plan, build_table_idx, probe_table_idx)?;
    // Bitmask of tables that are actually in the prefix join order for
    // this materialization subplan. Anything that depends on other tables
    // cannot be evaluated during those table scans.
    let join_prefix_mask =
        TableMask::from_table_number_iter(join_order.iter().map(|m| m.original_idx));
    // Expressions can also reference build tables of earlier hash joins in this subplan,
    // because those tables are available during probe loops. Use the broader "included"
    // set when deciding which WHERE terms can be evaluated inside the materialization.
    let eval_prefix_mask = TableMask::from_table_number_iter(included_tables.iter().copied());

    // Clone WHERE terms but mark as "consumed" any term that needs tables
    // outside the prefix. This prevents the subplan from trying to evaluate
    // predicates it doesn't have access to (e.g. autoindex lookups that
    // would require non-prefix tables to bind parameters).
    let mut where_clause = plan.where_clause.clone();
    for term in where_clause.iter_mut() {
        let mask = table_mask_from_expr(
            &term.expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        let outside_prefix = !eval_prefix_mask.contains_all(&mask);
        // Preserve consumed terms that the optimizer already suppressed, and
        // additionally consume any term that depends on tables outside the prefix.
        term.consumed |= outside_prefix;
    }

    // Clone table references and then "sanitize" each access method so that
    // the materialization subplan does not try to use an access path that
    // requires tables outside the prefix. If it does, we fall back to a scan.
    let mut table_references = plan.table_references.clone();
    for joined_table in table_references.joined_tables_mut().iter_mut() {
        if let Operation::HashJoin(hash_join_op) = &mut joined_table.op {
            if hash_join_op.build_table_idx == build_table_idx {
                // Avoid recursive materialization and disable the hash join for the build table
                // so it can be accessed using the join constraints.
                hash_join_op.materialize_build_input = false;
                joined_table.op = Operation::default_scan_for(&joined_table.table);
            } else if hash_join_op.probe_table_idx == probe_table_idx {
                // The probe table is not part of the materialization prefix, so
                // disable hash joins anchored on it.
                joined_table.op = Operation::default_scan_for(&joined_table.table);
            }
        }
    }

    // Helper to decide whether an expression depends on tables outside
    // the prefix. If it does, any access method that relies on that
    // expression must be invalidated for the materialization subplan.
    let expr_depends_outside_prefix = |expr: &Expr| -> Result<bool> {
        let mask = table_mask_from_expr(
            expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        Ok(!join_prefix_mask.contains_all(&mask))
    };

    // Walk each table in the cloned plan and ensure its access method is
    // valid within the prefix. If the access method depends on tables
    // outside the prefix, downgrade to a plain scan.
    for (table_idx, joined_table) in table_references.joined_tables_mut().iter_mut().enumerate() {
        if !join_prefix_mask.contains_table(table_idx) {
            continue;
        }

        let mut reset_op = false;
        match &joined_table.op {
            Operation::Search(Search::RowidEq { cmp_expr }) => {
                // Rowid equality searches may depend on other tables (e.g. column = other.col).
                reset_op = expr_depends_outside_prefix(cmp_expr)?;
            }
            Operation::Search(Search::Seek { seek_def, .. }) => {
                // Seek keys can include expressions bound by other tables. If so,
                // the seek is not valid in the prefix-only subplan.
                for component in seek_def.iter(&seek_def.start) {
                    if let SeekKeyComponent::Expr(expr) = component {
                        if expr_depends_outside_prefix(expr)? {
                            reset_op = true;
                            break;
                        }
                    }
                }
                if !reset_op {
                    for component in seek_def.iter(&seek_def.end) {
                        if let SeekKeyComponent::Expr(expr) = component {
                            if expr_depends_outside_prefix(expr)? {
                                reset_op = true;
                                break;
                            }
                        }
                    }
                }
            }
            Operation::IndexMethodQuery(IndexMethodQuery { arguments, .. }) => {
                // Index method queries are driven by argument expressions.
                // If any argument depends on non-prefix tables, we cannot use it.
                for expr in arguments {
                    if expr_depends_outside_prefix(expr)? {
                        reset_op = true;
                        break;
                    }
                }
            }
            Operation::Scan(Scan::VirtualTable { constraints, .. }) => {
                // Virtual table constraints are evaluated against expressions.
                // If any constraint depends on non-prefix tables, drop the scan
                // specialization and fall back to a full scan.
                for expr in constraints {
                    if expr_depends_outside_prefix(expr)? {
                        reset_op = true;
                        break;
                    }
                }
            }
            Operation::HashJoin(hash_join_op) => {
                // Hash joins are driven by the probe table's loop. That probe table
                // must be in the prefix; otherwise the hash join cannot be evaluated
                // inside this subplan. The build table may live outside the prefix
                // because the hash build phase scans it independently.
                if !join_prefix_mask.contains_table(hash_join_op.probe_table_idx) {
                    reset_op = true;
                }
            }
            _ => {}
        }

        if reset_op {
            // Downgrade to a default scan. This ensures the subplan only uses
            // access paths that are valid within the prefix join order.
            joined_table.op = Operation::default_scan_for(&joined_table.table);
        }
    }

    let build_internal_id = plan.table_references.joined_tables()[build_table_idx].internal_id;
    let result_columns = match mode {
        MaterializedBuildInputMode::RowidOnly => vec![ResultSetColumn {
            expr: Expr::RowId {
                database: None,
                table: build_internal_id,
            },
            alias: None,
            contains_aggregates: false,
        }],
        MaterializedBuildInputMode::KeyPayload { num_keys, .. } => {
            turso_assert!(
                *num_keys == key_exprs.len(),
                "materialized hash build input key count mismatch"
            );
            let mut result_columns: Vec<ResultSetColumn> = Vec::new();
            for expr in key_exprs.iter() {
                result_columns.push(ResultSetColumn {
                    expr: expr.clone(),
                    alias: None,
                    contains_aggregates: false,
                });
            }
            for payload in payload_columns.iter() {
                let expr = match payload {
                    MaterializedColumnRef::Column {
                        table_id,
                        column_idx,
                        is_rowid_alias,
                    } => Expr::Column {
                        database: None,
                        table: *table_id,
                        column: *column_idx,
                        is_rowid_alias: *is_rowid_alias,
                    },
                    MaterializedColumnRef::RowId { table_id } => Expr::RowId {
                        database: None,
                        table: *table_id,
                    },
                };
                result_columns.push(ResultSetColumn {
                    expr,
                    alias: None,
                    contains_aggregates: false,
                });
            }
            result_columns
        }
    };

    let mut materialize_plan = SelectPlan {
        table_references,
        join_order,
        result_columns,
        where_clause,
        group_by: None,
        order_by: vec![],
        aggregates: vec![],
        limit: None,
        offset: None,
        contains_constant_false_condition: false,
        query_destination: QueryDestination::EphemeralTable {
            cursor_id,
            table,
            rowid_mode: match mode {
                MaterializedBuildInputMode::RowidOnly => EphemeralRowidMode::FromResultColumns,
                MaterializedBuildInputMode::KeyPayload { .. } => EphemeralRowidMode::Auto,
            },
        },
        distinctness: Distinctness::NonDistinct,
        values: vec![],
        window: None,
        non_from_clause_subqueries: plan.non_from_clause_subqueries.clone(),
    };

    prune_join_order_for_materialized_inputs(&mut materialize_plan, materialized_build_inputs)?;

    Ok(materialize_plan)
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_query<'a>(
    program: &mut ProgramBuilder,
    plan: &'a mut SelectPlan,
    t_ctx: &mut TranslateCtx<'a>,
) -> Result<usize> {
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    // Evaluate uncorrelated subqueries as early as possible, because even LIMIT can reference a subquery.
    // This must happen before VALUES emission since VALUES expressions may contain scalar subqueries.
    for subquery in plan
        .non_from_clause_subqueries
        .iter_mut()
        .filter(|s| !s.has_been_evaluated())
    {
        let eval_at = subquery.get_eval_at(&plan.join_order, Some(&plan.table_references))?;
        if eval_at != EvalAt::BeforeLoop {
            continue;
        }
        let plan = subquery.consume_plan(EvalAt::BeforeLoop);

        emit_non_from_clause_subquery(
            program,
            &t_ctx.resolver,
            *plan,
            &subquery.query_type,
            subquery.correlated,
        )?;
    }

    // Handle VALUES clause - emit values after subqueries are prepared
    if !plan.values.is_empty() {
        let reg_result_cols_start = emit_values(program, plan, t_ctx)?;
        program.preassign_label_to_next_insn(after_main_loop_label);
        return Ok(reg_result_cols_start);
    }

    // Emit FROM clause subqueries first so the results can be read in the main query loop.
    emit_from_clause_subqueries(program, t_ctx, &mut plan.table_references, &plan.join_order)?;

    // For non-grouped aggregation queries that also have non-aggregate columns,
    // we need to ensure non-aggregate columns are only emitted once.
    // This flag helps track whether we've already emitted these columns.
    let has_ungrouped_nonagg_cols = !plan.aggregates.is_empty()
        && plan.group_by.is_none()
        && plan.result_columns.iter().any(|c| !c.contains_aggregates);

    if has_ungrouped_nonagg_cols {
        let flag = program.alloc_register();
        program.emit_int(0, flag); // Initialize flag to 0 (not yet emitted)
        t_ctx.reg_nonagg_emit_once_flag = Some(flag);
    }

    // Allocate registers for result columns
    if t_ctx.reg_result_cols_start.is_none() {
        t_ctx.reg_result_cols_start = Some(program.alloc_registers(plan.result_columns.len()));
        program.reg_result_cols_start = t_ctx.reg_result_cols_start
    }

    // For ungrouped aggregates with non-aggregate columns, initialize EXISTS subquery
    // result_regs to 0. EXISTS returns 0 (not NULL) when the subquery is never evaluated
    // (correlated EXISTS in empty loop). Non-aggregate columns themselves are evaluated
    // after the loop in emit_ungrouped_aggregation if the loop never ran.
    // We only initialize EXISTS subqueries that haven't been evaluated yet (correlated ones).
    if has_ungrouped_nonagg_cols {
        for rc in plan.result_columns.iter() {
            if !rc.contains_aggregates {
                init_exists_result_regs(program, &rc.expr, &plan.non_from_clause_subqueries);
            }
        }
    }

    let has_group_by_exprs = plan
        .group_by
        .as_ref()
        .is_some_and(|gb| !gb.exprs.is_empty());

    // Initialize cursors and other resources needed for query execution
    if !plan.order_by.is_empty() {
        init_order_by(
            program,
            t_ctx,
            &plan.result_columns,
            &plan.order_by,
            &plan.table_references,
            has_group_by_exprs,
            plan.distinctness != Distinctness::NonDistinct,
            &plan.aggregates,
        )?;
    }

    if has_group_by_exprs {
        if let Some(ref group_by) = plan.group_by {
            init_group_by(
                program,
                t_ctx,
                group_by,
                plan,
                &plan.result_columns,
                &plan.order_by,
            )?;
        }
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY (or HAVING without GROUP BY)
        // Aggregate registers need to be NULLed at the start because the same registers might be reused on another invocation of a subquery,
        // and if they are not NULLed, the 2nd invocation of the same subquery will have values left over from the first invocation.
        t_ctx.reg_agg_start = Some(program.alloc_registers_and_init_w_null(plan.aggregates.len()));
    } else if let Some(window) = &plan.window {
        init_window(
            program,
            t_ctx,
            window,
            plan,
            &plan.result_columns,
            &plan.order_by,
        )?;
    }

    let distinct_ctx = if let Distinctness::Distinct { .. } = &plan.distinctness {
        Some(init_distinct(program, plan)?)
    } else {
        None
    };
    if let Distinctness::Distinct { ctx } = &mut plan.distinctness {
        *ctx = distinct_ctx
    }
    if let Distinctness::Distinct { ctx: Some(ctx) } = &plan.distinctness {
        program.emit_insn(Insn::HashClear {
            hash_table_id: ctx.hash_table_id,
        });
    }

    init_limit(program, t_ctx, &plan.limit, &plan.offset)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    // however an aggregation might still happen,
    // e.g. SELECT COUNT(*) WHERE 0 returns a row with 0, not an empty result set.
    // This Goto must be placed AFTER all initialization (cursors, sorters, etc.) so that
    // resources like the GROUP BY sorter are properly opened before we skip to the aggregation phase.
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    init_loop(
        program,
        t_ctx,
        &plan.table_references,
        &mut plan.aggregates,
        OperationMode::SELECT,
        &plan.where_clause,
        &plan.join_order,
        &mut plan.non_from_clause_subqueries,
    )?;

    if plan.is_simple_count() {
        emit_simple_count(program, t_ctx, plan)?;
        return Ok(t_ctx.reg_result_cols_start.unwrap());
    }

    // Set up main query execution loop
    open_loop(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        &plan.where_clause,
        None,
        OperationMode::SELECT,
        &mut plan.non_from_clause_subqueries,
    )?;

    // Process result columns and expressions in the inner loop
    emit_loop(program, t_ctx, plan)?;

    // Clean up and close the main execution loop
    close_loop(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        OperationMode::SELECT,
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);

    let order_by_necessary = !plan.order_by.is_empty() && !plan.contains_constant_false_condition;
    let order_by = &plan.order_by;

    // Handle GROUP BY and aggregation processing
    if has_group_by_exprs {
        let row_source = &t_ctx
            .meta_group_by
            .as_ref()
            .expect("group by metadata not found")
            .row_source;
        if matches!(row_source, GroupByRowSource::Sorter { .. }) {
            group_by_agg_phase(program, t_ctx, plan)?;
        }
        group_by_emit_row_phase(program, t_ctx, plan)?;
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY (or HAVING without GROUP BY)
        emit_ungrouped_aggregation(program, t_ctx, plan)?;
    } else if plan.window.is_some() {
        emit_window_results(program, t_ctx, plan)?;
    }

    // Process ORDER BY results if needed
    if !order_by.is_empty() && order_by_necessary {
        emit_order_by(program, t_ctx, plan)?;
    }

    Ok(t_ctx.reg_result_cols_start.unwrap())
}

#[instrument(skip_all, level = Level::DEBUG)]
fn emit_program_for_delete(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    mut plan: DeletePlan,
) -> Result<()> {
    let mut t_ctx = TranslateCtx::new(
        program,
        resolver.fork(),
        plan.table_references.joined_tables().len(),
    );

    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    init_limit(program, &mut t_ctx, &plan.limit, &None)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    let join_order = plan
        .table_references
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: false,
        })
        .collect::<Vec<_>>();

    // Evaluate uncorrelated subqueries as early as possible (only for normal path without rowset)
    // For the rowset path, subqueries are handled by emit_program_for_select on the rowset_plan.
    if plan.rowset_plan.is_none() {
        for subquery in plan
            .non_from_clause_subqueries
            .iter_mut()
            .filter(|s| !s.has_been_evaluated())
        {
            let eval_at = subquery.get_eval_at(&join_order, Some(&plan.table_references))?;
            if eval_at != EvalAt::BeforeLoop {
                continue;
            }
            let subquery_plan = subquery.consume_plan(EvalAt::BeforeLoop);

            emit_non_from_clause_subquery(
                program,
                &t_ctx.resolver,
                *subquery_plan,
                &subquery.query_type,
                subquery.correlated,
            )?;
        }
    }

    // Initialize cursors and other resources needed for query execution
    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &mut [],
        OperationMode::DELETE,
        &plan.where_clause,
        &join_order,
        &mut plan.non_from_clause_subqueries,
    )?;

    // If there's a rowset_plan, materialize rowids into a RowSet first and then iterate the RowSet
    // to delete the rows.
    if let Some(rowset_plan) = plan.rowset_plan.take() {
        let rowset_reg = plan
            .rowset_reg
            .expect("rowset_reg must be Some if rowset_plan is Some");

        // Initialize the RowSet register with NULL (RowSet will be created on first RowSetAdd)
        program.emit_insn(Insn::Null {
            dest: rowset_reg,
            dest_end: None,
        });

        // Execute the rowset SELECT plan to populate the rowset.
        program.nested(|program| emit_program_for_select(program, resolver, rowset_plan))?;

        // Close the read cursor(s) opened by the rowset plan before opening for writing
        let table_ref = plan.table_references.joined_tables().first().unwrap();
        let table_cursor_id_read =
            program.resolve_cursor_id(&CursorKey::table(table_ref.internal_id));
        program.emit_insn(Insn::Close {
            cursor_id: table_cursor_id_read,
        });

        // Open the table cursor for writing
        let table_cursor_id = table_cursor_id_read;

        if let Some(btree_table) = table_ref.table.btree() {
            program.emit_insn(Insn::OpenWrite {
                cursor_id: table_cursor_id,
                root_page: RegisterOrLiteral::Literal(btree_table.root_page),
                db: table_ref.database_id,
            });

            // Open all indexes for writing (needed for DELETE)
            let write_indices: Vec<_> = resolver.with_schema(table_ref.database_id, |s| {
                s.get_indices(table_ref.table.get_name()).cloned().collect()
            });
            for index in &write_indices {
                let index_cursor_id = program.alloc_cursor_index(
                    Some(CursorKey::index(table_ref.internal_id, index.clone())),
                    index,
                )?;
                program.emit_insn(Insn::OpenWrite {
                    cursor_id: index_cursor_id,
                    root_page: RegisterOrLiteral::Literal(index.root_page),
                    db: table_ref.database_id,
                });
            }
        }

        // Now iterate over the RowSet and delete each rowid
        let rowset_loop_start = program.allocate_label();
        let rowset_loop_end = program.allocate_label();
        let rowid_reg = program.alloc_register();
        if table_ref.table.virtual_table().is_some() {
            // VUpdate requires a NULL second argument ("new rowid") for deletion
            let new_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: new_rowid_reg,
                dest_end: None,
            });
        }

        program.preassign_label_to_next_insn(rowset_loop_start);

        // Read next rowid from RowSet
        // Note: rowset_loop_end will be resolved later when we assign it
        program.emit_insn(Insn::RowSetRead {
            rowset_reg,
            pc_if_empty: rowset_loop_end,
            dest_reg: rowid_reg,
        });

        emit_delete_insns_when_triggers_present(
            connection,
            program,
            &mut t_ctx,
            &mut plan.table_references,
            &plan.result_columns,
            rowid_reg,
            table_cursor_id,
            resolver,
        )?;

        // Continue loop
        program.emit_insn(Insn::Goto {
            target_pc: rowset_loop_start,
        });

        // Assign the end label here, after all loop body code
        program.preassign_label_to_next_insn(rowset_loop_end);
    } else {
        // Normal DELETE path without RowSet

        // Set up main query execution loop
        open_loop(
            program,
            &mut t_ctx,
            &plan.table_references,
            &join_order,
            &plan.where_clause,
            None,
            OperationMode::DELETE,
            &mut plan.non_from_clause_subqueries,
        )?;

        emit_delete_insns(
            connection,
            program,
            &mut t_ctx,
            &mut plan.table_references,
            &plan.result_columns,
            resolver,
        )?;

        // Clean up and close the main execution loop
        close_loop(
            program,
            &mut t_ctx,
            &plan.table_references,
            &join_order,
            OperationMode::DELETE,
        )?;
    }
    program.preassign_label_to_next_insn(after_main_loop_label);
    if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
        emit_cdc_autocommit_commit(program, resolver, cdc_cursor_id)?;
    }
    // Finalize program
    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn emit_fk_child_decrement_on_delete(
    program: &mut ProgramBuilder,
    child_tbl: &BTreeTable,
    child_table_name: &str,
    child_cursor_id: usize,
    child_rowid_reg: usize,
    database_id: usize,
    resolver: &Resolver,
) -> crate::Result<()> {
    for fk_ref in
        resolver.with_schema(database_id, |s| s.resolved_fks_for_child(child_table_name))?
    {
        if !fk_ref.fk.deferred {
            continue;
        }
        // Fast path: if any FK column is NULL can't be a violation
        let null_skip = program.allocate_label();
        for cname in &fk_ref.child_cols {
            let (pos, col) = child_tbl.get_column(cname).unwrap();
            let src = if col.is_rowid_alias() {
                child_rowid_reg
            } else {
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: child_cursor_id,
                    column: pos,
                    dest: tmp,
                    default: None,
                });
                tmp
            };
            program.emit_insn(Insn::IsNull {
                reg: src,
                target_pc: null_skip,
            });
        }

        if fk_ref.parent_uses_rowid {
            // Probe parent table by rowid
            let parent_tbl = resolver
                .with_schema(database_id, |s| s.get_btree_table(&fk_ref.fk.parent_table))
                .expect("parent btree");
            let pcur = open_read_table(program, &parent_tbl, database_id);

            let (pos, col) = child_tbl.get_column(&fk_ref.child_cols[0]).unwrap();
            let val = if col.is_rowid_alias() {
                child_rowid_reg
            } else {
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: child_cursor_id,
                    column: pos,
                    dest: tmp,
                    default: None,
                });
                tmp
            };
            let tmpi = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: val,
                dst_reg: tmpi,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MustBeInt { reg: tmpi });

            // NotExists jumps when the parent key is missing, so we decrement there
            let missing = program.allocate_label();
            let done = program.allocate_label();

            program.emit_insn(Insn::NotExists {
                cursor: pcur,
                rowid_reg: tmpi,
                target_pc: missing,
            });

            // Parent FOUND, no decrement
            program.emit_insn(Insn::Close { cursor_id: pcur });
            program.emit_insn(Insn::Goto { target_pc: done });

            // Parent MISSING, decrement is guarded by FkIfZero to avoid underflow
            program.preassign_label_to_next_insn(missing);
            program.emit_insn(Insn::Close { cursor_id: pcur });
            emit_guarded_fk_decrement(program, done, true);
            program.preassign_label_to_next_insn(done);
        } else {
            // Probe parent unique index
            let parent_tbl = resolver
                .with_schema(database_id, |s| s.get_btree_table(&fk_ref.fk.parent_table))
                .expect("parent btree");
            let idx = fk_ref.parent_unique_index.as_ref().expect("unique index");
            let icur = open_read_index(program, idx, database_id);

            // Build probe from current child row
            let n = fk_ref.child_cols.len();
            let probe = program.alloc_registers(n);
            for (i, cname) in fk_ref.child_cols.iter().enumerate() {
                let (pos, col) = child_tbl.get_column(cname).unwrap();
                let src = if col.is_rowid_alias() {
                    child_rowid_reg
                } else {
                    let r = program.alloc_register();
                    program.emit_insn(Insn::Column {
                        cursor_id: child_cursor_id,
                        column: pos,
                        dest: r,
                        default: None,
                    });
                    r
                };
                program.emit_insn(Insn::Copy {
                    src_reg: src,
                    dst_reg: probe + i,
                    extra_amount: 0,
                });
            }
            program.emit_insn(Insn::Affinity {
                start_reg: probe,
                count: std::num::NonZeroUsize::new(n).unwrap(),
                affinities: build_index_affinity_string(idx, &parent_tbl),
            });

            let ok = program.allocate_label();
            program.emit_insn(Insn::Found {
                cursor_id: icur,
                target_pc: ok,
                record_reg: probe,
                num_regs: n,
            });
            program.emit_insn(Insn::Close { cursor_id: icur });
            emit_guarded_fk_decrement(program, ok, true);
            program.preassign_label_to_next_insn(ok);
            program.emit_insn(Insn::Close { cursor_id: icur });
        }
        program.preassign_label_to_next_insn(null_skip);
    }
    Ok(())
}

fn emit_delete_insns<'a>(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    table_references: &mut TableReferences,
    result_columns: &'a [super::plan::ResultSetColumn],
    resolver: &Resolver,
) -> Result<()> {
    // we can either use this obviously safe raw pointer or we can clone it
    let table_reference: *const JoinedTable = table_references.joined_tables().first().unwrap();
    if unsafe { &*table_reference }
        .virtual_table()
        .is_some_and(|t| t.readonly())
    {
        return Err(crate::LimboError::ReadOnly);
    }
    let internal_id = unsafe { (*table_reference).internal_id };

    let cursor_id = match unsafe { &(*table_reference).op } {
        Operation::Scan { .. } => program.resolve_cursor_id(&CursorKey::table(internal_id)),
        Operation::Search(search) => match search {
            Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                program.resolve_cursor_id(&CursorKey::table(internal_id))
            }
            Search::Seek {
                index: Some(index), ..
            } => program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
        },
        Operation::IndexMethodQuery(_) => {
            return Err(crate::LimboError::InternalError(
                "IndexMethod access is not supported for DELETE statements".to_string(),
            ));
        }
        Operation::HashJoin(_) => {
            unreachable!("access through HashJoin is not supported for delete statements")
        }
        Operation::MultiIndexScan(_) => {
            unreachable!("access through MultiIndexScan is not supported for delete statements")
        }
    };
    let btree_table = unsafe { &*table_reference }.btree();
    let database_id = unsafe { (*table_reference).database_id };
    let main_table_cursor_id = program.resolve_cursor_id(&CursorKey::table(internal_id));
    let has_returning = !result_columns.is_empty();
    let has_delete_triggers = if let Some(btree_table) = btree_table {
        t_ctx.resolver.with_schema(database_id, |s| {
            has_relevant_triggers_type_only(s, TriggerEvent::Delete, None, &btree_table)
        })
    } else {
        false
    };
    let cols_len = unsafe { &*table_reference }.columns().len();
    let (columns_start_reg, rowid_reg): (Option<usize>, usize) = {
        // Get rowid for RETURNING
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: main_table_cursor_id,
            dest: rowid_reg,
        });
        if unsafe { &*table_reference }.virtual_table().is_some() {
            // VUpdate requires a NULL second argument ("new rowid") for deletion
            let new_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: new_rowid_reg,
                dest_end: None,
            });
        }

        if !has_returning && !has_delete_triggers {
            (None, rowid_reg)
        } else {
            // Allocate registers for column values
            let columns_start_reg = program.alloc_registers(cols_len);

            // Read all column values from the row to be deleted
            for (i, _column) in unsafe { &*table_reference }.columns().iter().enumerate() {
                program.emit_column_or_rowid(main_table_cursor_id, i, columns_start_reg + i);
            }

            (Some(columns_start_reg), rowid_reg)
        }
    };

    // Get the index that is being used to iterate the deletion loop, if there is one.
    let iteration_index = unsafe { &*table_reference }.op.index();

    // Capture iteration index key values BEFORE deleting the main table row,
    // since the main table cursor will be invalidated after deletion.
    let iteration_idx_delete_ctx = if let Some(index) = iteration_index {
        let iteration_index_cursor =
            program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone()));
        let num_regs = index.columns.len() + 1;
        let start_reg = program.alloc_registers(num_regs);
        for (reg_offset, column_index) in index.columns.iter().enumerate() {
            emit_index_column_value_old_image(
                program,
                &t_ctx.resolver,
                table_references,
                main_table_cursor_id,
                column_index,
                start_reg + reg_offset,
            )?;
        }
        program.emit_insn(Insn::RowId {
            cursor_id: main_table_cursor_id,
            dest: start_reg + num_regs - 1,
        });
        Some((iteration_index_cursor, start_reg, num_regs, index))
    } else {
        None
    };

    emit_delete_row_common(
        connection,
        program,
        t_ctx,
        table_references,
        result_columns,
        table_reference,
        rowid_reg,
        columns_start_reg,
        main_table_cursor_id,
        iteration_index,
        Some(cursor_id), // Use the cursor_id from the operation for virtual tables
        resolver,
    )?;

    // Delete from the iteration index after deleting from the main table,
    // using the key values captured above.
    if let Some((iteration_index_cursor, start_reg, num_regs, index)) = iteration_idx_delete_ctx {
        program.emit_insn(Insn::IdxDelete {
            start_reg,
            num_regs,
            cursor_id: iteration_index_cursor,
            raise_error_if_no_matching_entry: index.where_clause.is_none(),
        });
    }
    if let Some(limit_ctx) = t_ctx.limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }

    Ok(())
}

/// Common deletion logic shared between normal DELETE and RowSet-based DELETE.
///
/// Parameters:
/// - `rowid_reg`: Register containing the rowid of the row to delete
/// - `columns_start_reg`: Start register containing column values (already read)
/// - `skip_iteration_index`: If Some(index), skip deleting from this index (used when iterating over an index)
/// - `virtual_table_cursor_id`: If Some, use this cursor for virtual table deletion
#[allow(clippy::too_many_arguments)]
fn emit_delete_row_common(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &mut TableReferences,
    result_columns: &[super::plan::ResultSetColumn],
    table_reference: *const JoinedTable,
    rowid_reg: usize,
    columns_start_reg: Option<usize>, // must be provided when there are triggers or RETURNING
    main_table_cursor_id: usize,
    skip_iteration_index: Option<&Arc<crate::schema::Index>>,
    virtual_table_cursor_id: Option<usize>,
    resolver: &Resolver,
) -> Result<()> {
    let internal_id = unsafe { (*table_reference).internal_id };
    let table_name = unsafe { &*table_reference }.table.get_name();

    if connection.foreign_keys_enabled() {
        let delete_db_id = unsafe { (*table_reference).database_id };
        if let Some(table) = unsafe { &*table_reference }.btree() {
            if t_ctx
                .resolver
                .with_schema(delete_db_id, |s| s.any_resolved_fks_referencing(table_name))
            {
                // Use sub-program based FK actions (CASCADE, SET NULL, SET DEFAULT, and NO ACTION)
                fire_fk_delete_actions(
                    program,
                    &mut t_ctx.resolver,
                    table_name,
                    main_table_cursor_id,
                    rowid_reg,
                    connection,
                    delete_db_id,
                )?;
            }
            if t_ctx
                .resolver
                .with_schema(delete_db_id, |s| s.has_child_fks(table_name))
            {
                emit_fk_child_decrement_on_delete(
                    program,
                    &table,
                    table_name,
                    main_table_cursor_id,
                    rowid_reg,
                    delete_db_id,
                    &t_ctx.resolver,
                )?;
            }
        }
    }

    if unsafe { &*table_reference }.virtual_table().is_some() {
        let conflict_action = 0u16;
        let cursor_id = virtual_table_cursor_id.unwrap_or(main_table_cursor_id);

        program.emit_insn(Insn::VUpdate {
            cursor_id,
            arg_count: 2,
            start_reg: rowid_reg,
            conflict_action,
        });
    } else {
        // Delete from all indexes before deleting from the main table.
        let db_id = unsafe { (*table_reference).database_id };
        let all_indices: Vec<_> = t_ctx
            .resolver
            .with_schema(db_id, |s| s.get_indices(table_name).cloned().collect());

        // Get indexes to delete from (skip the iteration index if specified)
        let indexes_to_delete = all_indices
            .iter()
            .filter(|index| {
                skip_iteration_index
                    .as_ref()
                    .is_none_or(|skip_idx| !Arc::ptr_eq(skip_idx, index))
            })
            .map(|index| {
                (
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )
            })
            .collect::<Vec<_>>();

        for (index, index_cursor_id) in indexes_to_delete {
            let skip_delete_label = if index.where_clause.is_some() {
                let where_copy = index
                    .bind_where_expr(Some(table_references), resolver)
                    .expect("where clause to exist");
                let skip_label = program.allocate_label();
                let reg = program.alloc_register();
                translate_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    &where_copy,
                    reg,
                    &t_ctx.resolver,
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
            let num_regs = index.columns.len() + 1;
            let start_reg = program.alloc_registers(num_regs);
            for (reg_offset, column_index) in index.columns.iter().enumerate() {
                emit_index_column_value_old_image(
                    program,
                    &t_ctx.resolver,
                    table_references,
                    main_table_cursor_id,
                    column_index,
                    start_reg + reg_offset,
                )?;
            }
            program.emit_insn(Insn::RowId {
                cursor_id: main_table_cursor_id,
                dest: start_reg + num_regs - 1,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg,
                num_regs,
                cursor_id: index_cursor_id,
                raise_error_if_no_matching_entry: index.where_clause.is_none(),
            });
            if let Some(label) = skip_delete_label {
                program.resolve_label(label, program.offset());
            }
        }

        // Emit update in the CDC table if necessary (before DELETE updated the table)
        if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
            let cdc_has_before = program.capture_data_changes_info().has_before();
            let before_record_reg = if cdc_has_before {
                let table_reference = unsafe { &*table_reference };
                Some(emit_cdc_full_record(
                    program,
                    table_reference.table.columns(),
                    main_table_cursor_id,
                    rowid_reg,
                    table_reference
                        .table
                        .btree()
                        .is_some_and(|btree| btree.is_strict),
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                &t_ctx.resolver,
                OperationMode::DELETE,
                cdc_cursor_id,
                rowid_reg,
                before_record_reg,
                None,
                None,
                table_name,
            )?;
        }

        // Emit RETURNING results if specified (must be before DELETE)
        if !result_columns.is_empty() {
            let columns_start_reg = columns_start_reg
                .expect("columns_start_reg must be provided when there are triggers or RETURNING");
            // Emit RETURNING results using the values we just read
            emit_returning_results(
                program,
                table_references,
                result_columns,
                columns_start_reg,
                rowid_reg,
                &mut t_ctx.resolver,
            )?;
        }

        program.emit_insn(Insn::Delete {
            cursor_id: main_table_cursor_id,
            table_name: table_name.to_string(),
            is_part_of_update: false,
        });
    }

    Ok(())
}

#[expect(clippy::too_many_arguments)]
/// Helper function to delete a row when we've already seeked to it (e.g., from a RowSet).
/// This is similar to emit_delete_insns but assumes the cursor is already positioned at the row.
fn emit_delete_insns_when_triggers_present(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &mut TableReferences,
    result_columns: &[super::plan::ResultSetColumn],
    rowid_reg: usize,
    main_table_cursor_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    // Seek to the rowid and delete it
    let skip_not_found_label = program.allocate_label();

    // Skip if row with rowid pulled from the rowset does not exist in the table.
    program.emit_insn(Insn::NotExists {
        cursor: main_table_cursor_id,
        rowid_reg,
        target_pc: skip_not_found_label,
    });

    let table_reference: *const JoinedTable = table_references.joined_tables().first().unwrap();
    if unsafe { &*table_reference }
        .virtual_table()
        .is_some_and(|t| t.readonly())
    {
        return Err(crate::LimboError::ReadOnly);
    }
    let btree_table = unsafe { &*table_reference }.btree();
    let database_id = unsafe { (*table_reference).database_id };
    let has_returning = !result_columns.is_empty();
    let has_delete_triggers = if let Some(btree_table) = btree_table {
        t_ctx.resolver.with_schema(database_id, |s| {
            has_relevant_triggers_type_only(s, TriggerEvent::Delete, None, &btree_table)
        })
    } else {
        false
    };
    let cols_len = unsafe { &*table_reference }.columns().len();

    let columns_start_reg = if !has_returning && !has_delete_triggers {
        None
    } else {
        let columns_start_reg = program.alloc_registers(cols_len);
        for (i, _column) in unsafe { &*table_reference }.columns().iter().enumerate() {
            program.emit_column_or_rowid(main_table_cursor_id, i, columns_start_reg + i);
        }
        Some(columns_start_reg)
    };

    let cols_len = unsafe { &*table_reference }.columns().len();

    // Fire BEFORE DELETE triggers
    if let Some(btree_table) = unsafe { &*table_reference }.btree() {
        let relevant_triggers: Vec<_> = t_ctx.resolver.with_schema(database_id, |s| {
            get_relevant_triggers_type_and_time(
                s,
                TriggerEvent::Delete,
                TriggerTime::Before,
                None,
                &btree_table,
            )
            .collect()
        });
        if !relevant_triggers.is_empty() {
            let columns_start_reg = columns_start_reg
                .expect("columns_start_reg must be provided when there are triggers or RETURNING");
            let old_registers = (0..cols_len)
                .map(|i| columns_start_reg + i)
                .chain(std::iter::once(rowid_reg))
                .collect::<Vec<_>>();
            // If the program has a trigger_conflict_override, propagate it to the trigger context.
            let trigger_ctx = if let Some(override_conflict) = program.trigger_conflict_override {
                TriggerContext::new_with_override_conflict(
                    btree_table,
                    None, // No NEW for DELETE
                    Some(old_registers),
                    override_conflict,
                )
            } else {
                TriggerContext::new(
                    btree_table,
                    None, // No NEW for DELETE
                    Some(old_registers),
                )
            };

            for trigger in relevant_triggers {
                fire_trigger(
                    program,
                    &mut t_ctx.resolver,
                    trigger,
                    &trigger_ctx,
                    connection,
                    database_id,
                )?;
            }
        }
    }

    // BEFORE DELETE Triggers may have altered the btree so we need to seek again.
    program.emit_insn(Insn::NotExists {
        cursor: main_table_cursor_id,
        rowid_reg,
        target_pc: skip_not_found_label,
    });

    emit_delete_row_common(
        connection,
        program,
        t_ctx,
        table_references,
        result_columns,
        table_reference,
        rowid_reg,
        columns_start_reg,
        main_table_cursor_id,
        None, // Don't skip any indexes when deleting from RowSet
        None, // Use main_table_cursor_id for virtual tables
        resolver,
    )?;

    // Fire AFTER DELETE triggers
    if let Some(btree_table) = unsafe { &*table_reference }.btree() {
        let relevant_triggers: Vec<_> = t_ctx.resolver.with_schema(database_id, |s| {
            get_relevant_triggers_type_and_time(
                s,
                TriggerEvent::Delete,
                TriggerTime::After,
                None,
                &btree_table,
            )
            .collect()
        });
        if !relevant_triggers.is_empty() {
            let columns_start_reg = columns_start_reg
                .expect("columns_start_reg must be provided when there are triggers or RETURNING");
            let old_registers = (0..cols_len)
                .map(|i| columns_start_reg + i)
                .chain(std::iter::once(rowid_reg))
                .collect::<Vec<_>>();
            // If the program has a trigger_conflict_override, propagate it to the trigger context.
            let trigger_ctx_after =
                if let Some(override_conflict) = program.trigger_conflict_override {
                    TriggerContext::new_with_override_conflict(
                        btree_table,
                        None, // No NEW for DELETE
                        Some(old_registers),
                        override_conflict,
                    )
                } else {
                    TriggerContext::new(
                        btree_table,
                        None, // No NEW for DELETE
                        Some(old_registers),
                    )
                };

            for trigger in relevant_triggers {
                fire_trigger(
                    program,
                    &mut t_ctx.resolver,
                    trigger,
                    &trigger_ctx_after,
                    connection,
                    database_id,
                )?;
            }
        }
    }

    program.preassign_label_to_next_insn(skip_not_found_label);

    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
fn emit_program_for_update(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    mut plan: UpdatePlan,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    program.set_resolve_type(plan.or_conflict.unwrap_or(ResolveType::Abort));

    let mut t_ctx = TranslateCtx::new(
        program,
        resolver.fork(),
        plan.table_references.joined_tables().len(),
    );

    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    init_limit(program, &mut t_ctx, &plan.limit, &plan.offset)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    let ephemeral_plan = plan.ephemeral_plan.take();
    let temp_cursor_id = ephemeral_plan.as_ref().map(|plan| {
        let QueryDestination::EphemeralTable { cursor_id, .. } = &plan.query_destination else {
            unreachable!()
        };
        *cursor_id
    });
    let has_ephemeral_table = ephemeral_plan.is_some();

    let target_table = if let Some(ephemeral_plan) = ephemeral_plan {
        let table = ephemeral_plan
            .table_references
            .joined_tables()
            .first()
            .unwrap()
            .clone();
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: temp_cursor_id.unwrap(),
            is_table: true,
        });
        program.nested(|program| emit_program_for_select(program, resolver, ephemeral_plan))?;
        Arc::new(table)
    } else {
        Arc::new(
            plan.table_references
                .joined_tables()
                .first()
                .unwrap()
                .clone(),
        )
    };

    let mode = OperationMode::UPDATE(if has_ephemeral_table {
        UpdateRowSource::PrebuiltEphemeralTable {
            ephemeral_table_cursor_id: temp_cursor_id.expect(
                "ephemeral table cursor id is always allocated if has_ephemeral_table is true",
            ),
            target_table: target_table.clone(),
        }
    } else {
        UpdateRowSource::Normal
    });

    let join_order = plan
        .table_references
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: false,
        })
        .collect::<Vec<_>>();

    // Evaluate uncorrelated subqueries as early as possible (only for normal path without ephemeral table)
    // For the ephemeral path, subqueries are handled by emit_program_for_select on the ephemeral_plan.
    if !has_ephemeral_table {
        for subquery in plan
            .non_from_clause_subqueries
            .iter_mut()
            .filter(|s| !s.has_been_evaluated())
        {
            let eval_at = subquery.get_eval_at(&join_order, Some(&plan.table_references))?;
            if eval_at != EvalAt::BeforeLoop {
                continue;
            }
            let subquery_plan = subquery.consume_plan(EvalAt::BeforeLoop);

            emit_non_from_clause_subquery(
                program,
                &t_ctx.resolver,
                *subquery_plan,
                &subquery.query_type,
                subquery.correlated,
            )?;
        }
    }

    // Initialize the main loop
    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &mut [],
        mode.clone(),
        &plan.where_clause,
        &join_order,
        &mut plan.non_from_clause_subqueries,
    )?;

    // Prepare index cursors
    // Use target_table.database_id instead of plan.table_references because when the UPDATE
    // uses an ephemeral table, plan.table_references.first() points to the ephemeral scratch
    // table (db=0) rather than the actual target table (which may be in an attached database).
    let target_database_id = target_table.database_id;
    let mut index_cursors = Vec::with_capacity(plan.indexes_to_update.len());
    for index in &plan.indexes_to_update {
        let index_cursor = if let Some(cursor) = program.resolve_cursor_id_safe(&CursorKey::index(
            plan.table_references
                .joined_tables()
                .first()
                .unwrap()
                .internal_id,
            index.clone(),
        )) {
            cursor
        } else {
            let cursor = program.alloc_cursor_index(None, index)?;
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(index.root_page),
                db: target_database_id,
            });
            cursor
        };
        let record_reg = program.alloc_register();
        index_cursors.push((index_cursor, record_reg));
    }

    // Open the main loop
    open_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &join_order,
        &plan.where_clause,
        temp_cursor_id,
        mode.clone(),
        &mut plan.non_from_clause_subqueries,
    )?;

    let target_table_cursor_id =
        program.resolve_cursor_id(&CursorKey::table(target_table.internal_id));

    let iteration_cursor_id = if has_ephemeral_table {
        temp_cursor_id.unwrap()
    } else {
        target_table_cursor_id
    };

    // For REPLACE mode, we need cursors for ALL indexes because when we delete a
    // conflicting row, we must delete from all indexes, not just those being updated.
    // We construct this AFTER open_loop so we can determine which index is used for
    // iteration and reuse that cursor instead of opening a new one.
    let all_index_cursors = if matches!(program.resolve_type, ResolveType::Replace) {
        let table_name = target_table.table.get_name();
        let all_indexes: Vec<_> = resolver.with_schema(target_database_id, |s| {
            s.get_indices(table_name).cloned().collect()
        });
        let source_table = plan
            .table_references
            .joined_tables()
            .first()
            .expect("UPDATE must have a joined table");
        let internal_id = source_table.internal_id;

        // Determine which index (if any) is being used for iteration
        // We need to reuse that cursor to avoid corruption when deleting from it
        let iteration_index_name = match &source_table.op {
            Operation::Scan(Scan::BTreeTable { index, .. }) => index.as_ref().map(|i| &i.name),
            Operation::Search(Search::Seek {
                index: Some(index), ..
            }) => Some(&index.name),
            _ => None,
        };

        all_indexes
            .into_iter()
            .map(|index| {
                // Check if this index already has a cursor opened (from indexes_to_update)
                let existing_cursor = plan
                    .indexes_to_update
                    .iter()
                    .zip(&index_cursors)
                    .find(|(idx, _)| idx.name == index.name)
                    .map(|(_, (cursor_id, _))| *cursor_id);

                let cursor = if let Some(cursor) = existing_cursor {
                    cursor
                } else if iteration_index_name == Some(&index.name) {
                    // This index is being used for iteration - reuse that cursor
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone()))
                } else {
                    // This index is not in indexes_to_update and not used for iteration
                    // Open a new cursor
                    let cursor = program
                        .alloc_cursor_index(None, &index)
                        .expect("to allocate index cursor");
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id: cursor,
                        root_page: RegisterOrLiteral::Literal(index.root_page),
                        db: target_database_id,
                    });
                    cursor
                };
                (index, cursor)
            })
            .collect::<Vec<(Arc<Index>, usize)>>()
    } else {
        Vec::new()
    };

    // Emit update instructions
    emit_update_insns(
        connection,
        &mut plan.table_references,
        &plan.set_clauses,
        plan.cdc_update_alter_statement.as_deref(),
        &plan.indexes_to_update,
        plan.returning.as_ref(),
        plan.ephemeral_plan.as_ref(),
        &mut t_ctx,
        program,
        &index_cursors,
        &all_index_cursors,
        iteration_cursor_id,
        target_table_cursor_id,
        target_table,
        resolver,
    )?;

    // Close the main loop
    close_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &join_order,
        mode,
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);
    if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
        emit_cdc_autocommit_commit(program, resolver, cdc_cursor_id)?;
    }
    after(program);

    program.result_columns = plan.returning.unwrap_or_default();
    program.table_references.extend(plan.table_references);
    Ok(())
}

/// Helper function to evaluate SET expressions and read column values for UPDATE.
/// This is invoked once for every UPDATE, but will be invoked again if there are
/// any BEFORE UPDATE triggers that fired, because the triggers may have modified the row,
/// in which case the previously read values are stale.
#[allow(clippy::too_many_arguments)]
fn emit_update_column_values<'a>(
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    set_clauses: &[(usize, Box<ast::Expr>)],
    cdc_update_alter_statement: Option<&str>,
    target_table: &Arc<JoinedTable>,
    target_table_cursor_id: usize,
    start: usize,
    col_len: usize,
    table_name: &str,
    has_direct_rowid_update: bool,
    has_user_provided_rowid: bool,
    rowid_set_clause_reg: Option<usize>,
    is_virtual: bool,
    index: &Option<(Arc<Index>, usize)>,
    cdc_updates_register: Option<usize>,
    t_ctx: &mut TranslateCtx<'a>,
    skip_set_clauses: bool,
    skip_row_label: BranchOffset,
) -> crate::Result<()> {
    let or_conflict = program.resolve_type;
    if has_direct_rowid_update {
        if let Some((_, expr)) = set_clauses.iter().find(|(i, _)| *i == ROWID_SENTINEL) {
            if !skip_set_clauses {
                let rowid_set_clause_reg = rowid_set_clause_reg.unwrap();
                translate_expr(
                    program,
                    Some(table_references),
                    expr,
                    rowid_set_clause_reg,
                    &t_ctx.resolver,
                )?;
                program.emit_insn(Insn::MustBeInt {
                    reg: rowid_set_clause_reg,
                });
            }
        }
    }
    for (idx, table_column) in target_table.table.columns().iter().enumerate() {
        let target_reg = start + idx;
        if let Some((col_idx, expr)) = set_clauses.iter().find(|(i, _)| *i == idx) {
            if !skip_set_clauses {
                // Skip if this is the sentinel value
                if *col_idx == ROWID_SENTINEL {
                    continue;
                }
                if has_user_provided_rowid
                    && (table_column.primary_key() || table_column.is_rowid_alias())
                    && !is_virtual
                {
                    let rowid_set_clause_reg = rowid_set_clause_reg.unwrap();
                    translate_expr(
                        program,
                        Some(table_references),
                        expr,
                        rowid_set_clause_reg,
                        &t_ctx.resolver,
                    )?;

                    program.emit_insn(Insn::MustBeInt {
                        reg: rowid_set_clause_reg,
                    });

                    program.emit_null(target_reg, None);
                } else {
                    translate_expr(
                        program,
                        Some(table_references),
                        expr,
                        target_reg,
                        &t_ctx.resolver,
                    )?;
                    if table_column.notnull() {
                        match or_conflict {
                            ResolveType::Ignore => {
                                // For IGNORE, skip this row on NOT NULL violation
                                program.emit_insn(Insn::IsNull {
                                    reg: target_reg,
                                    target_pc: skip_row_label,
                                });
                            }
                            ResolveType::Replace => {
                                // For REPLACE with NOT NULL, use default value if available
                                if let Some(default_expr) = table_column.default.as_ref() {
                                    let continue_label = program.allocate_label();

                                    // If not null, skip to continue
                                    program.emit_insn(Insn::NotNull {
                                        reg: target_reg,
                                        target_pc: continue_label,
                                    });

                                    // Value is null, use default.
                                    translate_expr_no_constant_opt(
                                        program,
                                        Some(table_references),
                                        default_expr,
                                        target_reg,
                                        &t_ctx.resolver,
                                        NoConstantOptReason::RegisterReuse,
                                    )?;

                                    program.preassign_label_to_next_insn(continue_label);
                                } else {
                                    // No default value, fall through to ABORT behavior
                                    use crate::error::SQLITE_CONSTRAINT_NOTNULL;
                                    program.emit_insn(Insn::HaltIfNull {
                                        target_reg,
                                        err_code: SQLITE_CONSTRAINT_NOTNULL,
                                        description: format!(
                                            "{}.{}",
                                            table_name,
                                            table_column
                                                .name
                                                .as_ref()
                                                .expect("Column name must be present")
                                        ),
                                    });
                                }
                            }
                            _ => {
                                // Default ABORT behavior
                                use crate::error::SQLITE_CONSTRAINT_NOTNULL;
                                program.emit_insn(Insn::HaltIfNull {
                                    target_reg,
                                    err_code: SQLITE_CONSTRAINT_NOTNULL,
                                    description: format!(
                                        "{}.{}",
                                        table_name,
                                        table_column
                                            .name
                                            .as_ref()
                                            .expect("Column name must be present")
                                    ),
                                });
                            }
                        }
                    }
                }

                if let Some(cdc_updates_register) = cdc_updates_register {
                    let change_reg = cdc_updates_register + idx;
                    let value_reg = cdc_updates_register + col_len + idx;
                    program.emit_bool(true, change_reg);
                    program.mark_last_insn_constant();
                    let mut updated = false;
                    if let Some(ddl_query_for_cdc_update) = cdc_update_alter_statement {
                        if table_column.name.as_deref() == Some("sql") {
                            program.emit_string8(ddl_query_for_cdc_update.to_string(), value_reg);
                            updated = true;
                        }
                    }
                    if !updated {
                        program.emit_insn(Insn::Copy {
                            src_reg: target_reg,
                            dst_reg: value_reg,
                            extra_amount: 0,
                        });
                    }
                }
            }
        } else {
            // Column is not being updated, read it from the table
            let column_idx_in_index = index.as_ref().and_then(|(idx, _)| {
                idx.columns
                    .iter()
                    .position(|c| Some(&c.name) == table_column.name.as_ref())
            });

            // don't emit null for pkey of virtual tables. they require first two args
            // before the 'record' to be explicitly non-null
            if table_column.is_rowid_alias() && !is_virtual {
                program.emit_null(target_reg, None);
            } else if is_virtual {
                program.emit_insn(Insn::VColumn {
                    cursor_id: target_table_cursor_id,
                    column: idx,
                    dest: target_reg,
                });
            } else {
                let cursor_id = *index
                    .as_ref()
                    .and_then(|(_, id)| {
                        if column_idx_in_index.is_some() {
                            Some(id)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(&target_table_cursor_id);
                program.emit_column_or_rowid(
                    cursor_id,
                    column_idx_in_index.unwrap_or(idx),
                    target_reg,
                );
            }

            if let Some(cdc_updates_register) = cdc_updates_register {
                let change_bit_reg = cdc_updates_register + idx;
                let value_reg = cdc_updates_register + col_len + idx;
                program.emit_bool(false, change_bit_reg);
                program.mark_last_insn_constant();
                program.emit_null(value_reg, None);
                program.mark_last_insn_constant();
            }
        }
    }
    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
#[allow(clippy::too_many_arguments)]
/// Emits the instructions for the UPDATE loop.
///
/// `iteration_cursor_id` is the cursor id of the table that is being iterated over. This can be either the table itself, an index, or an ephemeral table (see [crate::translate::plan::UpdatePlan]).
///
/// `target_table_cursor_id` is the cursor id of the table that is being updated.
///
/// `target_table` is the table that is being updated.
///
/// `or_conflict` specifies the conflict resolution strategy (IGNORE, REPLACE, ABORT).
///
/// `all_index_cursors` contains cursors for ALL indexes on the table (used for REPLACE to delete
/// conflicting rows from all indexes, not just those being updated).
fn emit_update_insns<'a>(
    connection: &Arc<Connection>,
    table_references: &mut TableReferences,
    set_clauses: &[(usize, Box<ast::Expr>)],
    cdc_update_alter_statement: Option<&str>,
    indexes_to_update: &[Arc<Index>],
    returning: Option<&'a Vec<ResultSetColumn>>,
    ephemeral_plan: Option<&SelectPlan>,
    t_ctx: &mut TranslateCtx<'a>,
    program: &mut ProgramBuilder,
    index_cursors: &[(usize, usize)],
    all_index_cursors: &[(Arc<Index>, usize)],
    iteration_cursor_id: usize,
    target_table_cursor_id: usize,
    target_table: Arc<JoinedTable>,
    resolver: &Resolver,
) -> crate::Result<()> {
    let or_conflict = program.resolve_type;
    let internal_id = target_table.internal_id;
    // Copy loop labels early to avoid borrow conflicts with mutable t_ctx borrow later
    let loop_labels = *t_ctx
        .labels_main_loop
        .first()
        .expect("loop labels to exist");
    // Label to skip to the next row on conflict (for IGNORE mode)
    let skip_row_label = loop_labels.next;
    let source_table = table_references
        .joined_tables()
        .first()
        .expect("UPDATE must have a source table");
    let (index, is_virtual) = match &source_table.op {
        Operation::Scan(Scan::BTreeTable { index, .. }) => (
            index.as_ref().map(|index| {
                (
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )
            }),
            false,
        ),
        Operation::Scan(_) => (None, target_table.virtual_table().is_some()),
        Operation::Search(search) => match search {
            &Search::RowidEq { .. } | Search::Seek { index: None, .. } => (None, false),
            Search::Seek {
                index: Some(index), ..
            } => (
                Some((
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )),
                false,
            ),
        },
        Operation::IndexMethodQuery(_) => {
            return Err(crate::LimboError::InternalError(
                "IndexMethod access is not supported for UPDATE operations".to_string(),
            ));
        }
        Operation::HashJoin(_) => {
            unreachable!("access through HashJoin is not supported for update operations")
        }
        Operation::MultiIndexScan(_) => {
            unreachable!("access through MultiIndexScan is not supported for update operations")
        }
    };

    let beg = program.alloc_registers(
        target_table.table.columns().len()
            + if is_virtual {
                2 // two args before the relevant columns for VUpdate
            } else {
                1 // rowid reg
            },
    );
    program.emit_insn(Insn::RowId {
        cursor_id: iteration_cursor_id,
        dest: beg,
    });

    // Check if rowid was provided (through INTEGER PRIMARY KEY as a rowid alias)
    let rowid_alias_index = target_table
        .table
        .columns()
        .iter()
        .position(|c| c.is_rowid_alias());

    let has_direct_rowid_update = set_clauses.iter().any(|(idx, _)| *idx == ROWID_SENTINEL);

    let has_user_provided_rowid = if let Some(index) = rowid_alias_index {
        set_clauses.iter().any(|(idx, _)| *idx == index)
    } else {
        has_direct_rowid_update
    };

    let rowid_set_clause_reg = if has_user_provided_rowid {
        Some(program.alloc_register())
    } else {
        None
    };

    let not_exists_check_required =
        has_user_provided_rowid || iteration_cursor_id != target_table_cursor_id;

    let check_rowid_not_exists_label = if not_exists_check_required {
        Some(program.allocate_label())
    } else {
        None
    };

    if not_exists_check_required {
        program.emit_insn(Insn::NotExists {
            cursor: target_table_cursor_id,
            rowid_reg: beg,
            target_pc: check_rowid_not_exists_label.unwrap(),
        });
    } else {
        // if no rowid, we're done
        program.emit_insn(Insn::IsNull {
            reg: beg,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        });
    }

    if is_virtual {
        program.emit_insn(Insn::Copy {
            src_reg: beg,
            dst_reg: beg + 1,
            extra_amount: 0,
        })
    }

    if let Some(offset) = t_ctx.reg_offset {
        program.emit_insn(Insn::IfPos {
            reg: offset,
            target_pc: loop_labels.next,
            decrement_by: 1,
        });
    }
    let col_len = target_table.table.columns().len();

    // we scan a column at a time, loading either the column's values, or the new value
    // from the Set expression, into registers so we can emit a MakeRecord and update the row.

    // we allocate 2C registers for "updates" as the structure of this column for CDC table is following:
    // [C boolean values where true set for changed columns] [C values with updates where NULL is set for not-changed columns]
    let cdc_updates_register = if program.capture_data_changes_info().has_updates() {
        Some(program.alloc_registers(2 * col_len))
    } else {
        None
    };
    let table_name = target_table.table.get_name();

    let start = if is_virtual { beg + 2 } else { beg + 1 };

    let skip_set_clauses = false;

    emit_update_column_values(
        program,
        table_references,
        set_clauses,
        cdc_update_alter_statement,
        &target_table,
        target_table_cursor_id,
        start,
        col_len,
        table_name,
        has_direct_rowid_update,
        has_user_provided_rowid,
        rowid_set_clause_reg,
        is_virtual,
        &index,
        cdc_updates_register,
        t_ctx,
        skip_set_clauses,
        skip_row_label,
    )?;

    // For non-STRICT tables, apply column affinity to the NEW values early.
    // This must happen before index operations and triggers so that all operations
    // use the converted values.
    if let Some(btree_table) = target_table.table.btree() {
        if !btree_table.is_strict {
            let affinity = btree_table.columns.iter().map(|c| c.affinity());

            // Only emit Affinity if there's meaningful affinity to apply
            if affinity.clone().any(|a| a != Affinity::Blob) {
                if let Ok(count) = std::num::NonZeroUsize::try_from(col_len) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: start,
                        count,
                        affinities: affinity.map(|a| a.aff_mask()).collect(),
                    });
                }
            }
        }
    }

    // Fire BEFORE UPDATE triggers and preserve old_registers for AFTER triggers
    let update_database_id = target_table.database_id;
    let preserved_old_registers: Option<Vec<usize>> = if let Some(btree_table) =
        target_table.table.btree()
    {
        let updated_column_indices: HashSet<usize> =
            set_clauses.iter().map(|(col_idx, _)| *col_idx).collect();
        let relevant_before_update_triggers: Vec<_> =
            t_ctx.resolver.with_schema(update_database_id, |s| {
                get_relevant_triggers_type_and_time(
                    s,
                    TriggerEvent::Update,
                    TriggerTime::Before,
                    Some(updated_column_indices.clone()),
                    &btree_table,
                )
                .collect()
            });
        // Read OLD row values for trigger context
        let old_registers: Vec<usize> = (0..col_len)
            .map(|i| {
                let reg = program.alloc_register();
                program.emit_column_or_rowid(target_table_cursor_id, i, reg);
                reg
            })
            .chain(std::iter::once(beg))
            .collect();
        if relevant_before_update_triggers.is_empty() {
            Some(old_registers)
        } else {
            // NEW row values are already in 'start' registers
            let new_registers = (0..col_len)
                .map(|i| start + i)
                .chain(std::iter::once(beg))
                .collect();

            // If the program has a trigger_conflict_override, propagate it to the trigger context.
            let trigger_ctx = if let Some(override_conflict) = program.trigger_conflict_override {
                TriggerContext::new_with_override_conflict(
                    btree_table.clone(),
                    Some(new_registers),
                    Some(old_registers.clone()), // Clone for AFTER trigger
                    override_conflict,
                )
            } else {
                TriggerContext::new(
                    btree_table.clone(),
                    Some(new_registers),
                    Some(old_registers.clone()), // Clone for AFTER trigger
                )
            };

            for trigger in relevant_before_update_triggers {
                fire_trigger(
                    program,
                    &mut t_ctx.resolver,
                    trigger,
                    &trigger_ctx,
                    connection,
                    update_database_id,
                )?;
            }

            // BEFORE UPDATE Triggers may have altered the btree so we need to seek again.
            program.emit_insn(Insn::NotExists {
                cursor: target_table_cursor_id,
                rowid_reg: beg,
                target_pc: check_rowid_not_exists_label.expect(
                    "check_rowid_not_exists_label must be set if there are BEFORE UPDATE triggers",
                ),
            });

            let has_relevant_after_triggers = t_ctx.resolver.with_schema(update_database_id, |s| {
                get_relevant_triggers_type_and_time(
                    s,
                    TriggerEvent::Update,
                    TriggerTime::After,
                    Some(updated_column_indices),
                    &btree_table,
                )
                .count()
                    > 0
            });
            if has_relevant_after_triggers {
                // Preserve pseudo-row 'OLD' for AFTER triggers by copying to new registers
                // (since registers might be overwritten during trigger execution)
                let preserved: Vec<usize> = old_registers
                    .iter()
                    .map(|old_reg| {
                        let preserved_reg = program.alloc_register();
                        program.emit_insn(Insn::Copy {
                            src_reg: *old_reg,
                            dst_reg: preserved_reg,
                            extra_amount: 0,
                        });
                        preserved_reg
                    })
                    .collect();
                Some(preserved)
            } else {
                Some(old_registers)
            }
        }
    } else {
        None
    };

    // If BEFORE UPDATE triggers fired, they may have modified the row being updated.
    // According to the SQLite documentation, the behavior in these cases is undefined:
    // https://sqlite.org/lang_createtrigger.html
    // However, based on fuzz testing and observations, the logic seems to be:
    // The values that are NOT referred to in SET clauses will be evaluated again,
    // and values in SET clauses are evaluated using the old values.
    // sqlite> create table t(c0,c1,c2);
    // sqlite> create trigger tu before update on t begin update t set c1=666, c2=666; end;
    // sqlite> insert into t values (1,1,1);
    // sqlite> update t set c0 = c1+1;
    // sqlite> select * from t;
    // 2|666|666
    if target_table.table.btree().is_some() {
        let before_update_triggers_fired = preserved_old_registers.is_some();
        let skip_set_clauses = true;
        if before_update_triggers_fired {
            emit_update_column_values(
                program,
                table_references,
                set_clauses,
                cdc_update_alter_statement,
                &target_table,
                target_table_cursor_id,
                start,
                col_len,
                table_name,
                has_direct_rowid_update,
                has_user_provided_rowid,
                rowid_set_clause_reg,
                is_virtual,
                &index,
                cdc_updates_register,
                t_ctx,
                skip_set_clauses,
                skip_row_label,
            )?;
        }
    }

    if connection.foreign_keys_enabled() {
        let rowid_new_reg = rowid_set_clause_reg.unwrap_or(beg);
        if let Some(table_btree) = target_table.table.btree() {
            stabilize_new_row_for_fk(
                program,
                &table_btree,
                set_clauses,
                target_table_cursor_id,
                start,
                rowid_new_reg,
            )?;
            if t_ctx
                .resolver
                .with_schema(update_database_id, |s| s.has_child_fks(table_name))
            {
                // Child-side checks:
                // this ensures updated row still satisfies child FKs that point OUT from this table
                emit_fk_child_update_counters(
                    program,
                    &table_btree,
                    table_name,
                    target_table_cursor_id,
                    start,
                    rowid_new_reg,
                    &set_clauses.iter().map(|(i, _)| *i).collect::<HashSet<_>>(),
                    update_database_id,
                    &t_ctx.resolver,
                )?;
            }
            // Parent-side NO ACTION/RESTRICT checks must happen BEFORE the update.
            // This checks that no child rows reference the old parent key values.
            // CASCADE/SET NULL actions are fired AFTER the update (see below after Insert).
            if t_ctx.resolver.with_schema(update_database_id, |s| {
                s.any_resolved_fks_referencing(table_name)
            }) {
                emit_fk_update_parent_actions(
                    program,
                    &table_btree,
                    indexes_to_update.iter(),
                    target_table_cursor_id,
                    beg,
                    start,
                    rowid_new_reg,
                    rowid_set_clause_reg,
                    set_clauses,
                    update_database_id,
                    &t_ctx.resolver,
                )?;
            }
        }
    }

    // Populate register-to-affinity map for expression index evaluation.
    // When column references are rewritten to Expr::Register during UPDATE, comparison
    // operators need the original column affinity. This is set once here and cleared at
    // the end of the function.
    {
        let rowid_reg = rowid_set_clause_reg.unwrap_or(beg);
        for (idx, col) in target_table.table.columns().iter().enumerate() {
            t_ctx
                .resolver
                .register_affinities
                .insert(start + idx, col.affinity());
        }
        t_ctx
            .resolver
            .register_affinities
            .insert(rowid_reg, Affinity::Integer);
    }
    let target_is_strict = target_table
        .table
        .btree()
        .is_some_and(|btree| btree.is_strict);

    // For IGNORE, FAIL, and ROLLBACK modes, we need to do a preflight check for unique
    // constraint violations BEFORE deleting any old index entries. This ensures that:
    // - IGNORE: We can skip the row without corrupting it by partially deleting index entries
    // - FAIL/ROLLBACK: We can halt without partial state (index deleted but not re-inserted)
    // Note: ABORT is not included because the NotExists instruction used for rowid conflict
    // checking repositions the table cursor, which would corrupt subsequent Column reads.
    // ABORT halts the entire operation anyway, so preflight checking isn't strictly needed.
    if matches!(
        or_conflict,
        ResolveType::Ignore | ResolveType::Fail | ResolveType::Rollback
    ) {
        let rowid_reg = rowid_set_clause_reg.unwrap_or(beg);
        for (index, (idx_cursor_id, _record_reg)) in indexes_to_update.iter().zip(index_cursors) {
            if !index.unique {
                continue;
            }

            // Build the new index key for conflict checking
            let num_cols = index.columns.len();
            let idx_start_reg = program.alloc_registers(num_cols);

            for (i, col) in index.columns.iter().enumerate() {
                emit_index_column_value_new_image(
                    program,
                    &t_ctx.resolver,
                    target_table.table.columns(),
                    start,
                    rowid_reg,
                    col,
                    idx_start_reg + i,
                )?;
            }

            // Apply affinity for proper comparison
            let aff = index
                .columns
                .iter()
                .map(|ic| {
                    if ic.expr.is_some() {
                        Affinity::Blob.aff_mask()
                    } else {
                        target_table.table.columns()[ic.pos_in_table]
                            .affinity_with_strict(target_is_strict)
                            .aff_mask()
                    }
                })
                .collect::<String>();
            program.emit_insn(Insn::Affinity {
                start_reg: idx_start_reg,
                count: NonZeroUsize::new(num_cols).expect("nonzero col count"),
                affinities: aff,
            });

            // Check for conflicts - NoConflict jumps if no conflict
            let no_conflict_label = program.allocate_label();
            program.emit_insn(Insn::NoConflict {
                cursor_id: *idx_cursor_id,
                target_pc: no_conflict_label,
                record_reg: idx_start_reg,
                num_regs: num_cols,
            });

            // A conflict was found - check if it's the same row we're updating
            let idx_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::IdxRowId {
                cursor_id: *idx_cursor_id,
                dest: idx_rowid_reg,
            });

            // If the conflicting row is the one we're updating, that's not actually a conflict
            program.emit_insn(Insn::Eq {
                lhs: beg,
                rhs: idx_rowid_reg,
                target_pc: no_conflict_label,
                flags: CmpInsFlags::default(),
                collation: program.curr_collation(),
            });

            // Conflict with a different row - handle based on conflict resolution mode
            match or_conflict {
                ResolveType::Ignore => {
                    // Skip this row's update and continue with the next row
                    program.emit_insn(Insn::Goto {
                        target_pc: skip_row_label,
                    });
                }
                ResolveType::Fail | ResolveType::Rollback => {
                    // Halt with UNIQUE constraint error
                    let column_names = index.columns.iter().enumerate().fold(
                        String::with_capacity(50),
                        |mut accum, (idx, col)| {
                            if idx > 0 {
                                accum.push_str(", ");
                            }
                            accum.push_str(table_name);
                            accum.push('.');
                            accum.push_str(&col.name);
                            accum
                        },
                    );
                    program.emit_insn(Insn::Halt {
                        err_code: SQLITE_CONSTRAINT_UNIQUE,
                        description: column_names,
                    });
                }
                _ => unreachable!("Only IGNORE, FAIL, and ROLLBACK should reach preflight check"),
            }

            program.preassign_label_to_next_insn(no_conflict_label);
        }

        // Also check for rowid conflict in preflight
        if has_user_provided_rowid {
            let target_reg = rowid_set_clause_reg.unwrap();
            let no_rowid_conflict_label = program.allocate_label();

            // If the new rowid equals the old rowid, no conflict
            program.emit_insn(Insn::Eq {
                lhs: target_reg,
                rhs: beg,
                target_pc: no_rowid_conflict_label,
                flags: CmpInsFlags::default(),
                collation: program.curr_collation(),
            });

            // If a row with the new rowid doesn't exist, no conflict
            program.emit_insn(Insn::NotExists {
                cursor: target_table_cursor_id,
                rowid_reg: target_reg,
                target_pc: no_rowid_conflict_label,
            });

            // Conflict found - handle based on conflict resolution mode
            match or_conflict {
                ResolveType::Ignore => {
                    // Skip this row's update and continue with the next row
                    program.emit_insn(Insn::Goto {
                        target_pc: skip_row_label,
                    });
                }
                ResolveType::Fail | ResolveType::Rollback => {
                    // Halt with PRIMARY KEY constraint error
                    let description = if let Some(idx) = rowid_alias_index {
                        String::from(table_name)
                            + "."
                            + target_table
                                .table
                                .columns()
                                .get(idx)
                                .expect("column to exist")
                                .name
                                .as_ref()
                                .map_or("", |v| v)
                    } else {
                        String::from(table_name) + ".rowid"
                    };
                    program.emit_insn(Insn::Halt {
                        err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                        description,
                    });
                }
                _ => unreachable!("Only IGNORE, FAIL, and ROLLBACK should reach preflight check"),
            }

            program.preassign_label_to_next_insn(no_rowid_conflict_label);
        }

        if has_user_provided_rowid {
            if let Some(label) = check_rowid_not_exists_label {
                // Important: the cursor was repositioned in the previous conflict checks,
                // so if we didn't conflict above, we need to re-seek to the row under update,
                // so that the old index row images (see below) read from the correct row.
                program.emit_insn(Insn::NotExists {
                    cursor: target_table_cursor_id,
                    rowid_reg: beg,
                    target_pc: label,
                });
            }
        }
    }

    // Evaluate STRICT type checks and CHECK constraints before any index mutations.
    // This ensures that if a constraint fails, indexes remain consistent.
    if let Some(btree_table) = target_table.table.btree() {
        if btree_table.is_strict {
            program.emit_insn(Insn::TypeCheck {
                start_reg: start,
                count: col_len,
                check_generated: true,
                table_reference: Arc::clone(&btree_table),
            });
        }

        if !btree_table.check_constraints.is_empty() {
            // SQLite only evaluates CHECK constraints that reference at least one
            // column in the SET clause. Build a set of updated column names to filter.
            let mut updated_col_names: HashSet<String> = set_clauses
                .iter()
                .filter_map(|(col_idx, _)| {
                    btree_table
                        .columns
                        .get(*col_idx)
                        .and_then(|c| c.name.as_deref())
                        .map(normalize_ident)
                })
                .collect();

            // If the rowid is being updated (either directly via ROWID_SENTINEL or
            // through a rowid alias column), also include the rowid pseudo-column
            // names so that CHECK(rowid > 0) etc. are properly triggered.
            let rowid_updated = set_clauses.iter().any(|(idx, _)| *idx == ROWID_SENTINEL)
                || btree_table.columns.iter().enumerate().any(|(i, c)| {
                    c.is_rowid_alias() && set_clauses.iter().any(|(idx, _)| *idx == i)
                });
            if rowid_updated {
                for name in ROWID_STRS {
                    updated_col_names.insert(name.to_string());
                }
            }

            let relevant_checks: Vec<CheckConstraint> = btree_table
                .check_constraints
                .iter()
                .filter(|cc| check_expr_references_columns(&cc.expr, &updated_col_names))
                .cloned()
                .collect();

            let check_constraint_tables =
                TableReferences::new(vec![target_table.as_ref().clone()], vec![]);
            emit_check_constraints(
                program,
                &relevant_checks,
                &mut t_ctx.resolver,
                &btree_table.name,
                rowid_set_clause_reg.unwrap_or(beg),
                btree_table
                    .columns
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, col)| {
                        col.name.as_deref().map(|n| {
                            if col.is_rowid_alias() {
                                (n, rowid_set_clause_reg.unwrap_or(beg))
                            } else {
                                (n, start + idx)
                            }
                        })
                    }),
                connection,
                or_conflict,
                skip_row_label,
                Some(&check_constraint_tables),
            )?;
        }
    }

    for (index, (idx_cursor_id, record_reg)) in indexes_to_update.iter().zip(index_cursors) {
        // We need to know whether or not the OLD values satisfied the predicate on the
        // partial index, so we can know whether or not to delete the old index entry,
        // as well as whether or not the NEW values satisfy the predicate, to determine whether
        // or not to insert a new index entry for a partial index
        let (old_satisfies_where, new_satisfies_where) = if index.where_clause.is_some() {
            // This means that we need to bind the column references to a copy of the index Expr,
            // so we can emit Insn::Column instructions and refer to the old values.
            let where_clause = index
                .bind_where_expr(Some(table_references), resolver)
                .expect("where clause to exist");
            let old_satisfied_reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                Some(table_references),
                &where_clause,
                old_satisfied_reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;

            // grab a new copy of the original where clause from the index
            let mut new_where = index
                .where_clause
                .as_ref()
                .expect("checked where clause to exist")
                .clone();
            // Now we need to rewrite the Expr::Id and Expr::Qualified/Expr::RowID (from a copy of the original, un-bound `where` expr),
            // to refer to the new values, which are already loaded into registers starting at `start`.
            rewrite_where_for_update_registers(
                &mut new_where,
                target_table.table.columns(),
                start,
                rowid_set_clause_reg.unwrap_or(beg),
            )?;

            let new_satisfied_reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                None,
                &new_where,
                new_satisfied_reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;

            // now we have two registers that tell us whether or not the old and new values satisfy
            // the partial index predicate, and we can use those to decide whether or not to
            // delete/insert a new index entry for this partial index.
            (Some(old_satisfied_reg), Some(new_satisfied_reg))
        } else {
            (None, None)
        };

        let mut skip_delete_label = None;
        let mut skip_insert_label = None;

        // Handle deletion for partial indexes
        if let Some(old_satisfied) = old_satisfies_where {
            skip_delete_label = Some(program.allocate_label());
            // If the old values don't satisfy the WHERE clause, skip the delete
            program.emit_insn(Insn::IfNot {
                reg: old_satisfied,
                target_pc: skip_delete_label.unwrap(),
                jump_if_null: true,
            });
        }

        // Delete old index entry
        let num_regs = index.columns.len() + 1;
        let delete_start_reg = program.alloc_registers(num_regs);
        for (reg_offset, column_index) in index.columns.iter().enumerate() {
            emit_index_column_value_old_image(
                program,
                &t_ctx.resolver,
                table_references,
                target_table_cursor_id,
                column_index,
                delete_start_reg + reg_offset,
            )?;
        }
        program.emit_insn(Insn::RowId {
            cursor_id: target_table_cursor_id,
            dest: delete_start_reg + num_regs - 1,
        });
        program.emit_insn(Insn::IdxDelete {
            start_reg: delete_start_reg,
            num_regs,
            cursor_id: *idx_cursor_id,
            raise_error_if_no_matching_entry: true,
        });

        // Resolve delete skip label if it exists
        if let Some(label) = skip_delete_label {
            program.resolve_label(label, program.offset());
        }

        // Check if we should insert into partial index
        if let Some(new_satisfied) = new_satisfies_where {
            skip_insert_label = Some(program.allocate_label());
            // If the new values don't satisfy the WHERE clause, skip the idx insert
            program.emit_insn(Insn::IfNot {
                reg: new_satisfied,
                target_pc: skip_insert_label.unwrap(),
                jump_if_null: true,
            });
        }

        // Build new index entry
        let num_cols = index.columns.len();
        let idx_start_reg = program.alloc_registers(num_cols + 1);
        let rowid_reg = rowid_set_clause_reg.unwrap_or(beg);

        for (i, col) in index.columns.iter().enumerate() {
            emit_index_column_value_new_image(
                program,
                &t_ctx.resolver,
                target_table.table.columns(),
                start,
                rowid_reg,
                col,
                idx_start_reg + i,
            )?;
        }
        // last register is the rowid
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: idx_start_reg + num_cols,
            extra_amount: 0,
        });

        // Apply affinity BEFORE MakeRecord so the index record has correctly converted values.
        // This is needed for all indexes (not just unique) because the index should store
        // values with proper affinity conversion.
        let aff = index
            .columns
            .iter()
            .map(|ic| {
                if ic.expr.is_some() {
                    Affinity::Blob.aff_mask()
                } else {
                    target_table.table.columns()[ic.pos_in_table]
                        .affinity_with_strict(target_is_strict)
                        .aff_mask()
                }
            })
            .collect::<String>();
        program.emit_insn(Insn::Affinity {
            start_reg: idx_start_reg,
            count: NonZeroUsize::new(num_cols).expect("nonzero col count"),
            affinities: aff,
        });

        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(idx_start_reg),
            count: to_u16(num_cols + 1),
            dest_reg: to_u16(*record_reg),
            index_name: Some(index.name.clone()),
            affinity_str: None,
        });

        // Handle unique constraint
        if index.unique {
            let constraint_check = program.allocate_label();
            // check if the record already exists in the index for unique indexes and abort if so
            program.emit_insn(Insn::NoConflict {
                cursor_id: *idx_cursor_id,
                target_pc: constraint_check,
                record_reg: idx_start_reg,
                num_regs: num_cols,
            });

            let idx_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::IdxRowId {
                cursor_id: *idx_cursor_id,
                dest: idx_rowid_reg,
            });

            // Skip over the UNIQUE constraint failure if the existing row is the one that we are currently changing
            program.emit_insn(Insn::Eq {
                lhs: beg,
                rhs: idx_rowid_reg,
                target_pc: constraint_check,
                flags: CmpInsFlags::default(),
                collation: program.curr_collation(),
            });

            match or_conflict {
                ResolveType::Ignore => {
                    // For IGNORE, skip this row's update but continue with other rows
                    program.emit_insn(Insn::Goto {
                        target_pc: skip_row_label,
                    });
                }
                ResolveType::Replace => {
                    // For REPLACE with unique constraint, delete the conflicting row
                    // Save original rowid before seeking to conflicting row
                    let original_rowid_reg = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: beg,
                        dst_reg: original_rowid_reg,
                        extra_amount: 0,
                    });

                    // Seek to the conflicting row
                    let after_delete_label = program.allocate_label();
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: target_table_cursor_id,
                        src_reg: idx_rowid_reg,
                        target_pc: after_delete_label, // Skip if row doesn't exist
                    });

                    // Delete from ALL indexes for the conflicting row
                    // We must delete from all indexes, not just indexes_to_update,
                    // because the conflicting row may have entries in indexes
                    // whose columns are not being modified by this UPDATE.
                    for (other_index, other_idx_cursor_id) in all_index_cursors {
                        // Build index key for the conflicting row
                        let other_num_regs = other_index.columns.len() + 1;
                        let other_start_reg = program.alloc_registers(other_num_regs);

                        for (reg_offset, column_index) in other_index.columns.iter().enumerate() {
                            emit_index_column_value_old_image(
                                program,
                                &t_ctx.resolver,
                                table_references,
                                target_table_cursor_id,
                                column_index,
                                other_start_reg + reg_offset,
                            )?;
                        }

                        // Add the conflicting rowid
                        program.emit_insn(Insn::Copy {
                            src_reg: idx_rowid_reg,
                            dst_reg: other_start_reg + other_num_regs - 1,
                            extra_amount: 0,
                        });

                        program.emit_insn(Insn::IdxDelete {
                            start_reg: other_start_reg,
                            num_regs: other_num_regs,
                            cursor_id: *other_idx_cursor_id,
                            raise_error_if_no_matching_entry: other_index.where_clause.is_none(),
                        });
                    }

                    // Delete the conflicting row from the main table
                    program.emit_insn(Insn::Delete {
                        cursor_id: target_table_cursor_id,
                        table_name: table_name.to_string(),
                        is_part_of_update: false,
                    });

                    program.preassign_label_to_next_insn(after_delete_label);

                    // Seek back to the original row we're updating
                    let continue_label = program.allocate_label();
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: target_table_cursor_id,
                        src_reg: original_rowid_reg,
                        target_pc: continue_label, // Should always succeed
                    });
                    program.preassign_label_to_next_insn(continue_label);
                }
                _ => {
                    // Default ABORT behavior
                    let column_names = index.columns.iter().enumerate().fold(
                        String::with_capacity(50),
                        |mut accum, (idx, col)| {
                            if idx > 0 {
                                accum.push_str(", ");
                            }
                            accum.push_str(table_name);
                            accum.push('.');
                            accum.push_str(&col.name);
                            accum
                        },
                    );

                    program.emit_insn(Insn::Halt {
                        err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                        description: column_names,
                    });
                }
            }

            program.preassign_label_to_next_insn(constraint_check);
        }

        // Insert the index entry
        program.emit_insn(Insn::IdxInsert {
            cursor_id: *idx_cursor_id,
            record_reg: *record_reg,
            unpacked_start: Some(idx_start_reg),
            unpacked_count: Some((num_cols + 1) as u16),
            flags: IdxInsertFlags::new().nchange(true),
        });

        // Resolve insert skip label if it exists
        if let Some(label) = skip_insert_label {
            program.resolve_label(label, program.offset());
        }
    }

    if target_table.table.btree().is_some() {
        if has_user_provided_rowid {
            let record_label = program.allocate_label();
            let target_reg = rowid_set_clause_reg.unwrap();

            // If the new rowid equals the old rowid, no conflict
            program.emit_insn(Insn::Eq {
                lhs: target_reg,
                rhs: beg,
                target_pc: record_label,
                flags: CmpInsFlags::default(),
                collation: program.curr_collation(),
            });

            // If a row with the new rowid doesn't exist, no conflict
            program.emit_insn(Insn::NotExists {
                cursor: target_table_cursor_id,
                rowid_reg: target_reg,
                target_pc: record_label,
            });

            // Handle conflict resolution for rowid/primary key conflict
            match or_conflict {
                ResolveType::Ignore => {
                    // For IGNORE, skip this row's update but continue with other rows
                    program.emit_insn(Insn::Goto {
                        target_pc: skip_row_label,
                    });
                }
                ResolveType::Replace => {
                    // For REPLACE with rowid conflict, delete the conflicting row
                    // The conflicting row is at the new target rowid position
                    // Seek to the conflicting row (target_reg has the new rowid)
                    let after_delete_label = program.allocate_label();
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: target_table_cursor_id,
                        src_reg: target_reg,
                        target_pc: after_delete_label, // Skip if row doesn't exist
                    });

                    // Delete from ALL indexes for the conflicting row
                    // We must delete from all indexes, not just indexes_to_update,
                    // because the conflicting row may have entries in indexes
                    // whose columns are not being modified by this UPDATE.
                    for (other_index, other_idx_cursor_id) in all_index_cursors {
                        // Build index key for the conflicting row
                        let other_num_regs = other_index.columns.len() + 1;
                        let other_start_reg = program.alloc_registers(other_num_regs);

                        for (reg_offset, column_index) in other_index.columns.iter().enumerate() {
                            emit_index_column_value_old_image(
                                program,
                                &t_ctx.resolver,
                                table_references,
                                target_table_cursor_id,
                                column_index,
                                other_start_reg + reg_offset,
                            )?;
                        }

                        // Add the conflicting rowid (target_reg has the new/conflicting rowid)
                        program.emit_insn(Insn::Copy {
                            src_reg: target_reg,
                            dst_reg: other_start_reg + other_num_regs - 1,
                            extra_amount: 0,
                        });

                        program.emit_insn(Insn::IdxDelete {
                            start_reg: other_start_reg,
                            num_regs: other_num_regs,
                            cursor_id: *other_idx_cursor_id,
                            raise_error_if_no_matching_entry: other_index.where_clause.is_none(),
                        });
                    }

                    // Delete the conflicting row from the main table
                    program.emit_insn(Insn::Delete {
                        cursor_id: target_table_cursor_id,
                        table_name: table_name.to_string(),
                        is_part_of_update: false,
                    });

                    program.preassign_label_to_next_insn(after_delete_label);
                }
                _ => {
                    // Default ABORT behavior
                    let description = if let Some(idx) = rowid_alias_index {
                        String::from(table_name)
                            + "."
                            + target_table
                                .table
                                .columns()
                                .get(idx)
                                .unwrap()
                                .name
                                .as_ref()
                                .map_or("", |v| v)
                    } else {
                        String::from(table_name) + ".rowid"
                    };

                    program.emit_insn(Insn::Halt {
                        err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                        description,
                    });
                }
            }

            program.preassign_label_to_next_insn(record_label);
        }

        let record_reg = program.alloc_register();

        let is_strict = target_table
            .table
            .btree()
            .is_some_and(|btree| btree.is_strict);
        let affinity_str = target_table
            .table
            .columns()
            .iter()
            .map(|col| col.affinity_with_strict(is_strict).aff_mask())
            .collect::<String>();

        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(start),
            count: to_u16(col_len),
            dest_reg: to_u16(record_reg),
            index_name: None,
            affinity_str: Some(affinity_str),
        });

        if not_exists_check_required {
            program.emit_insn(Insn::NotExists {
                cursor: target_table_cursor_id,
                rowid_reg: beg,
                target_pc: check_rowid_not_exists_label.unwrap(),
            });
        }

        // create alias for CDC rowid after the change (will differ from cdc_rowid_before_reg only in case of UPDATE with change in rowid alias)
        let cdc_rowid_after_reg = rowid_set_clause_reg.unwrap_or(beg);

        // create separate register with rowid before UPDATE for CDC
        let cdc_rowid_before_reg = if t_ctx.cdc_cursor_id.is_some() {
            let cdc_rowid_before_reg = program.alloc_register();
            if has_user_provided_rowid {
                program.emit_insn(Insn::RowId {
                    cursor_id: target_table_cursor_id,
                    dest: cdc_rowid_before_reg,
                });
                Some(cdc_rowid_before_reg)
            } else {
                Some(cdc_rowid_after_reg)
            }
        } else {
            None
        };

        // create full CDC record before update if necessary
        let cdc_before_reg = if program.capture_data_changes_info().has_before() {
            Some(emit_cdc_full_record(
                program,
                target_table.table.columns(),
                target_table_cursor_id,
                cdc_rowid_before_reg.expect("cdc_rowid_before_reg must be set"),
                target_table
                    .table
                    .btree()
                    .is_some_and(|btree| btree.is_strict),
            ))
        } else {
            None
        };

        // If we are updating the rowid, we cannot rely on overwrite on the
        // Insert instruction to update the cell. We need to first delete the current cell
        // and later insert the updated record.
        // In MVCC mode, we also need DELETE+INSERT to properly version the row (Hekaton model).
        let needs_delete = not_exists_check_required || connection.mvcc_enabled();
        if needs_delete {
            program.emit_insn(Insn::Delete {
                cursor_id: target_table_cursor_id,
                table_name: table_name.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: target_table_cursor_id,
            key_reg: rowid_set_clause_reg.unwrap_or(beg),
            record_reg,
            flag: if not_exists_check_required {
                // The previous Insn::NotExists and Insn::Delete seek to the old rowid,
                // so to insert a new user-provided rowid, we need to seek to the correct place.
                InsertFlags::new()
                    .require_seek()
                    .update_rowid_change()
                    .skip_last_rowid()
            } else {
                InsertFlags::new().skip_last_rowid()
            },
            table_name: target_table.identifier.clone(),
        });

        // Fire FK CASCADE/SET NULL actions AFTER the parent row is updated
        // This ensures the new parent key exists when cascade actions update child rows
        if connection.foreign_keys_enabled()
            && t_ctx.resolver.with_schema(update_database_id, |s| {
                s.any_resolved_fks_referencing(table_name)
            })
        {
            let new_rowid_reg = rowid_set_clause_reg.unwrap_or(beg);
            // OLD column values are stored in preserved_old_registers (contiguous registers)
            let old_values_start = preserved_old_registers
                .as_ref()
                .expect("FK check requires OLD values")[0];
            fire_fk_update_actions(
                program,
                &mut t_ctx.resolver,
                table_name,
                beg, // old_rowid_reg
                old_values_start,
                start, // new_values_start
                new_rowid_reg,
                connection,
                update_database_id,
            )?;
        }

        // Fire AFTER UPDATE triggers
        if let Some(btree_table) = target_table.table.btree() {
            let updated_column_indices: HashSet<usize> =
                set_clauses.iter().map(|(col_idx, _)| *col_idx).collect();
            let relevant_triggers: Vec<_> = t_ctx.resolver.with_schema(update_database_id, |s| {
                get_relevant_triggers_type_and_time(
                    s,
                    TriggerEvent::Update,
                    TriggerTime::After,
                    Some(updated_column_indices),
                    &btree_table,
                )
                .collect()
            });
            if !relevant_triggers.is_empty() {
                let new_rowid_reg = rowid_set_clause_reg.unwrap_or(beg);
                let new_registers_after = (0..col_len)
                    .map(|i| start + i)
                    .chain(std::iter::once(new_rowid_reg))
                    .collect();

                // Use preserved OLD registers from BEFORE trigger
                let old_registers_after = preserved_old_registers;

                // If the program has a trigger_conflict_override, propagate it to the trigger context.
                let trigger_ctx_after =
                    if let Some(override_conflict) = program.trigger_conflict_override {
                        TriggerContext::new_with_override_conflict(
                            btree_table,
                            Some(new_registers_after),
                            old_registers_after, // OLD values preserved from BEFORE trigger
                            override_conflict,
                        )
                    } else {
                        TriggerContext::new(
                            btree_table,
                            Some(new_registers_after),
                            old_registers_after, // OLD values preserved from BEFORE trigger
                        )
                    };

                for trigger in relevant_triggers {
                    fire_trigger(
                        program,
                        &mut t_ctx.resolver,
                        trigger,
                        &trigger_ctx_after,
                        connection,
                        update_database_id,
                    )?;
                }
            }
        }

        // Emit RETURNING results if specified
        if let Some(returning_columns) = &returning {
            if !returning_columns.is_empty() {
                emit_returning_results(
                    program,
                    table_references,
                    returning_columns,
                    start,
                    rowid_set_clause_reg.unwrap_or(beg),
                    &mut t_ctx.resolver,
                )?;
            }
        }

        // create full CDC record after update if necessary
        let cdc_after_reg = if program.capture_data_changes_info().has_after() {
            Some(emit_cdc_patch_record(
                program,
                &target_table.table,
                start,
                record_reg,
                cdc_rowid_after_reg,
            ))
        } else {
            None
        };

        let cdc_updates_record = if let Some(cdc_updates_register) = cdc_updates_register {
            let record_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u16(cdc_updates_register),
                count: to_u16(2 * col_len),
                dest_reg: to_u16(record_reg),
                index_name: None,
                affinity_str: None,
            });
            Some(record_reg)
        } else {
            None
        };

        // emit actual CDC instructions for write to the CDC table
        if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
            let cdc_rowid_before_reg =
                cdc_rowid_before_reg.expect("cdc_rowid_before_reg must be set");
            if has_user_provided_rowid {
                emit_cdc_insns(
                    program,
                    &t_ctx.resolver,
                    OperationMode::DELETE,
                    cdc_cursor_id,
                    cdc_rowid_before_reg,
                    cdc_before_reg,
                    None,
                    None,
                    table_name,
                )?;
                emit_cdc_insns(
                    program,
                    &t_ctx.resolver,
                    OperationMode::INSERT,
                    cdc_cursor_id,
                    cdc_rowid_after_reg,
                    cdc_after_reg,
                    None,
                    None,
                    table_name,
                )?;
            } else {
                emit_cdc_insns(
                    program,
                    &t_ctx.resolver,
                    OperationMode::UPDATE(if ephemeral_plan.is_some() {
                        UpdateRowSource::PrebuiltEphemeralTable {
                            ephemeral_table_cursor_id: iteration_cursor_id,
                            target_table: target_table.clone(),
                        }
                    } else {
                        UpdateRowSource::Normal
                    }),
                    cdc_cursor_id,
                    cdc_rowid_before_reg,
                    cdc_before_reg,
                    cdc_after_reg,
                    cdc_updates_record,
                    table_name,
                )?;
            }
        }
    } else if target_table.virtual_table().is_some() {
        let arg_count = col_len + 2;
        program.emit_insn(Insn::VUpdate {
            cursor_id: target_table_cursor_id,
            arg_count,
            start_reg: beg,
            conflict_action: 0u16,
        });
    }

    if let Some(limit_ctx) = t_ctx.limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }
    // TODO(pthorpe): handle RETURNING clause

    if let Some(label) = check_rowid_not_exists_label {
        program.preassign_label_to_next_insn(label);
    }

    t_ctx.resolver.register_affinities.clear();
    Ok(())
}

pub fn prepare_cdc_if_necessary(
    program: &mut ProgramBuilder,
    schema: &Schema,
    changed_table_name: &str,
) -> Result<Option<(usize, Arc<BTreeTable>)>> {
    let mode = program.capture_data_changes_info();
    let cdc_table = mode.table();
    let Some(cdc_table) = cdc_table else {
        return Ok(None);
    };
    if changed_table_name == cdc_table
        || changed_table_name == crate::translate::pragma::TURSO_CDC_VERSION_TABLE_NAME
    {
        return Ok(None);
    }
    let Some(turso_cdc_table) = schema.get_table(cdc_table) else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let Some(cdc_btree) = turso_cdc_table.btree() else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(cdc_btree.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: cdc_btree.root_page.into(),
        db: 0, // CDC table always lives in the main database
    });
    Ok(Some((cursor_id, cdc_btree)))
}

pub fn emit_cdc_patch_record(
    program: &mut ProgramBuilder,
    table: &Table,
    columns_reg: usize,
    record_reg: usize,
    rowid_reg: usize,
) -> usize {
    let columns = table.columns();
    let rowid_alias_position = columns.iter().position(|x| x.is_rowid_alias());
    if let Some(rowid_alias_position) = rowid_alias_position {
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: columns_reg + rowid_alias_position,
            extra_amount: 0,
        });
        let is_strict = table.btree().is_some_and(|btree| btree.is_strict);
        let affinity_str = table
            .columns()
            .iter()
            .map(|col| col.affinity_with_strict(is_strict).aff_mask())
            .collect::<String>();

        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(columns_reg),
            count: to_u16(table.columns().len()),
            dest_reg: to_u16(record_reg),
            index_name: None,
            affinity_str: Some(affinity_str),
        });
        record_reg
    } else {
        record_reg
    }
}

pub fn emit_cdc_full_record(
    program: &mut ProgramBuilder,
    columns: &[Column],
    table_cursor_id: usize,
    rowid_reg: usize,
    is_strict: bool,
) -> usize {
    let columns_reg = program.alloc_registers(columns.len() + 1);
    for (i, column) in columns.iter().enumerate() {
        if column.is_rowid_alias() {
            program.emit_insn(Insn::Copy {
                src_reg: rowid_reg,
                dst_reg: columns_reg + 1 + i,
                extra_amount: 0,
            });
        } else {
            program.emit_column_or_rowid(table_cursor_id, i, columns_reg + 1 + i);
        }
    }
    let affinity_str = columns
        .iter()
        .map(|col| col.affinity_with_strict(is_strict).aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(columns_reg + 1),
        count: to_u16(columns.len()),
        dest_reg: to_u16(columns_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    columns_reg
}

#[allow(clippy::too_many_arguments)]
pub fn emit_cdc_insns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    let cdc_info = program.capture_data_changes_info().as_ref();
    match cdc_info.map(|info| info.cdc_version()) {
        Some(crate::CdcVersion::V2) => emit_cdc_insns_v2(
            program,
            resolver,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        ),
        Some(crate::CdcVersion::V1) => emit_cdc_insns_v1(
            program,
            resolver,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        ),
        None => Err(crate::LimboError::InternalError(
            "cdc info not set".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_cdc_insns_v1(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // v1: (change_id, change_time, change_type, table_name, id, before, after, updates)
    let turso_cdc_registers = program.alloc_registers(8);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    let Some(unixepoch_fn) = resolver.resolve_function("unixepoch", 0) else {
        bail_parse_error!("no function {}", "unixepoch");
    };
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: unixepoch_fn,
        arg_count: 0,
    };

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE { .. } | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 2);
    program.mark_last_insn_constant();

    program.emit_string8(table_name.to_string(), turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 4,
        extra_amount: 0,
    });

    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 5,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 5, None);
        program.mark_last_insn_constant();
    }

    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0, // todo(sivukhin): properly set value here from sqlite_sequence table when AUTOINCREMENT will be properly implemented in Turso
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(turso_cdc_registers),
        count: to_u16(8),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: "".to_string(),
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_cdc_insns_v2(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // v2: (change_id, change_time, change_txn_id, change_type, table_name, id, before, after, updates)
    let turso_cdc_registers = program.alloc_registers(9);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    // change_time = unixepoch()
    let Some(unixepoch_fn) = resolver.resolve_function("unixepoch", 0) else {
        bail_parse_error!("no function {}", "unixepoch");
    };
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: unixepoch_fn,
        arg_count: 0,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    // change_txn_id = conn_txn_id(new_rowid)
    // First generate a candidate rowid, then pass it to conn_txn_id for get-or-set.
    let candidate_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg: candidate_reg,
        prev_largest_reg: 0,
    });
    let Some(conn_txn_id_fn) = resolver.resolve_function("conn_txn_id", 1) else {
        bail_parse_error!("no function {}", "conn_txn_id");
    };
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: conn_txn_id_fn,
        arg_count: 1,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: candidate_reg,
        dest: turso_cdc_registers + 2,
        func: conn_txn_id_fn_ctx,
    });

    // change_type
    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE { .. } | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    // table_name
    program.emit_string8(table_name.to_string(), turso_cdc_registers + 4);
    program.mark_last_insn_constant();

    // id
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 5,
        extra_amount: 0,
    });

    // before
    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    // after
    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    // updates
    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 8,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 8, None);
        program.mark_last_insn_constant();
    }

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(turso_cdc_registers),
        count: to_u16(9),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: "".to_string(),
    });
    Ok(())
}

/// Emit a COMMIT record into the CDC table (v2 only).
/// change_type=2, all other data fields NULL.
pub fn emit_cdc_commit_insns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    cdc_cursor_id: usize,
) -> Result<()> {
    // v2 COMMIT record: (NULL, unixepoch(), conn_txn_id(-1), 2, NULL, NULL, NULL, NULL, NULL)
    let regs = program.alloc_registers(9);
    // reg+0: NULL (change_id, autoincrement)
    program.emit_insn(Insn::Null {
        dest: regs,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    // reg+1: change_time = unixepoch()
    let Some(unixepoch_fn) = resolver.resolve_function("unixepoch", 0) else {
        bail_parse_error!("no function {}", "unixepoch");
    };
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: unixepoch_fn,
        arg_count: 0,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: regs + 1,
        func: unixepoch_fn_ctx,
    });

    // reg+2: change_txn_id = conn_txn_id(-1)
    // Pass -1 as candidate: if a txn_id exists, return it; if not, -1 is stored (and will be reset).
    let minus_one_reg = program.alloc_register();
    program.emit_int(-1, minus_one_reg);
    let Some(conn_txn_id_fn) = resolver.resolve_function("conn_txn_id", 1) else {
        bail_parse_error!("no function {}", "conn_txn_id");
    };
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: conn_txn_id_fn,
        arg_count: 1,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: minus_one_reg,
        dest: regs + 2,
        func: conn_txn_id_fn_ctx,
    });

    // reg+3: change_type = 2 (COMMIT)
    program.emit_int(2, regs + 3);
    program.mark_last_insn_constant();

    // reg+4..8: NULL (table_name, id, before, after, updates)
    program.emit_insn(Insn::Null {
        dest: regs + 4,
        dest_end: Some(regs + 8),
    });
    program.mark_last_insn_constant();

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(regs),
        count: to_u16(9),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: "".to_string(),
    });
    Ok(())
}

/// Emit a CDC COMMIT record at end-of-statement when in autocommit mode (v2 only).
/// This should be called once per statement, after the main loop, not per-row.
pub fn emit_cdc_autocommit_commit(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    cdc_cursor_id: usize,
) -> Result<()> {
    let cdc_info = program.capture_data_changes_info().as_ref();
    if cdc_info.is_some_and(|info| info.cdc_version().has_commit_record()) {
        // Check if we're in autocommit mode; if so, emit a COMMIT record.
        let Some(is_autocommit_fn) = resolver.resolve_function("is_autocommit", 0) else {
            bail_parse_error!("no function {}", "is_autocommit");
        };
        let is_autocommit_fn_ctx = crate::function::FuncCtx {
            func: is_autocommit_fn,
            arg_count: 0,
        };
        let autocommit_reg = program.alloc_register();
        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: 0,
            dest: autocommit_reg,
            func: is_autocommit_fn_ctx,
        });

        // IfNot jumps when reg == 0 (not autocommit). Skip the COMMIT in that case.
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: autocommit_reg,
            target_pc: skip_label,
            jump_if_null: true,
        });

        emit_cdc_commit_insns(program, resolver, cdc_cursor_id)?;

        program.resolve_label(skip_label, program.offset());
    }

    Ok(())
}
/// Initialize the limit/offset counters and registers.
/// In case of compound SELECTs, the limit counter is initialized only once,
/// hence [LimitCtx::initialize_counter] being false in those cases.
fn init_limit(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    limit: &Option<Box<Expr>>,
    offset: &Option<Box<Expr>>,
) -> Result<()> {
    if t_ctx.limit_ctx.is_none() && limit.is_some() {
        t_ctx.limit_ctx = Some(LimitCtx::new(program));
    }
    let Some(limit_ctx) = &t_ctx.limit_ctx else {
        return Ok(());
    };

    if limit_ctx.initialize_counter {
        if let Some(expr) = limit {
            match expr.as_ref() {
                Expr::Literal(Literal::Numeric(n)) => match parse_numeric_literal(n)? {
                    crate::types::Value::Numeric(crate::Numeric::Integer(value)) => {
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::Integer {
                            value,
                            dest: limit_ctx.reg_limit,
                        });
                    }
                    crate::types::Value::Numeric(crate::Numeric::Float(value)) => {
                        program.emit_insn(Insn::Real {
                            value: value.into(),
                            dest: limit_ctx.reg_limit,
                        });
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::MustBeInt {
                            reg: limit_ctx.reg_limit,
                        });
                    }
                    _ => unreachable!("parse_numeric_literal only returns Integer or Float"),
                },
                _ => {
                    let r = limit_ctx.reg_limit;

                    _ = translate_expr(program, None, expr, r, &t_ctx.resolver)?;
                    program.emit_insn(Insn::MustBeInt { reg: r });
                }
            }
        }
    }

    if t_ctx.reg_offset.is_none() {
        if let Some(expr) = offset {
            let offset_reg = program.alloc_register();
            t_ctx.reg_offset = Some(offset_reg);
            match expr.as_ref() {
                Expr::Literal(Literal::Numeric(n)) => match parse_numeric_literal(n)? {
                    crate::types::Value::Numeric(crate::Numeric::Integer(value)) => {
                        program.emit_insn(Insn::Integer {
                            value,
                            dest: offset_reg,
                        });
                    }
                    crate::types::Value::Numeric(crate::Numeric::Float(value)) => {
                        program.emit_insn(Insn::Real {
                            value: value.into(),
                            dest: offset_reg,
                        });
                        program.emit_insn(Insn::MustBeInt { reg: offset_reg });
                    }
                    _ => unreachable!("parse_numeric_literal only returns Integer or Float"),
                },
                _ => {
                    _ = translate_expr(program, None, expr, offset_reg, &t_ctx.resolver)?;
                }
            }
            program.add_comment(program.offset(), "OFFSET counter");
            program.emit_insn(Insn::MustBeInt { reg: offset_reg });

            let combined_reg = program.alloc_register();
            t_ctx.reg_limit_offset_sum = Some(combined_reg);
            program.add_comment(program.offset(), "OFFSET + LIMIT");
            program.emit_insn(Insn::OffsetLimit {
                limit_reg: limit_ctx.reg_limit,
                offset_reg,
                combined_reg,
            });
        }
    }

    // exit early if LIMIT 0
    let main_loop_end = t_ctx
        .label_main_loop_end
        .expect("label_main_loop_end must be set before init_limit");
    program.emit_insn(Insn::IfNot {
        reg: limit_ctx.reg_limit,
        target_pc: main_loop_end,
        jump_if_null: false,
    });

    Ok(())
}

/// We have `Expr`s which have *not* had column references bound to them,
/// so they are in the state of Expr::Id/Expr::Qualified, etc, and instead of binding Expr::Column
/// we need to bind Expr::Register, as we have already loaded the *new* column values from the
/// UPDATE statement into registers starting at `columns_start_reg`, which we want to reference.
fn rewrite_where_for_update_registers(
    expr: &mut Expr,
    columns: &[Column],
    columns_start_reg: usize,
    rowid_reg: usize,
) -> Result<WalkControl> {
    rewrite_between_expr(expr);
    walk_expr_mut(expr, &mut |e: &mut Expr| -> Result<WalkControl> {
        match e {
            Expr::Qualified(_, col) | Expr::DoublyQualified(_, _, col) => {
                let normalized = normalize_ident(col.as_str());
                if let Some((idx, c)) = columns.iter().enumerate().find(|(_, c)| {
                    c.name
                        .as_ref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(&normalized))
                }) {
                    if c.is_rowid_alias() {
                        *e = Expr::Register(rowid_reg);
                    } else {
                        *e = Expr::Register(columns_start_reg + idx);
                    }
                }
            }
            Expr::Id(name) => {
                let normalized = normalize_ident(name.as_str());
                if ROWID_STRS
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(&normalized))
                {
                    *e = Expr::Register(rowid_reg);
                } else if let Some((idx, c)) = columns.iter().enumerate().find(|(_, c)| {
                    c.name
                        .as_ref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(&normalized))
                }) {
                    if c.is_rowid_alias() {
                        *e = Expr::Register(rowid_reg);
                    } else {
                        *e = Expr::Register(columns_start_reg + idx);
                    }
                }
            }
            Expr::RowId { .. } => {
                *e = Expr::Register(rowid_reg);
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
}

/// Emit code to load the value of an IndexColumn from the OLD image of the row being updated.
/// Handling expression indexes and regular columns
fn emit_index_column_value_old_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: &mut TableReferences,
    table_cursor_id: usize,
    idx_col: &IndexColumn,
    dest_reg: usize,
) -> Result<()> {
    if let Some(expr) = &idx_col.expr {
        let mut expr = expr.as_ref().clone();
        bind_and_rewrite_expr(
            &mut expr,
            Some(table_references),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        translate_expr_no_constant_opt(
            program,
            Some(table_references),
            &expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    } else {
        program.emit_column_or_rowid(table_cursor_id, idx_col.pos_in_table, dest_reg);
    }
    Ok(())
}

/// Emit code to load the value of an IndexColumn from the NEW image of the row being updated.
/// Handling expression indexes and regular columns
fn emit_index_column_value_new_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    columns_start_reg: usize,
    rowid_reg: usize,
    idx_col: &IndexColumn,
    dest_reg: usize,
) -> Result<()> {
    if let Some(expr) = &idx_col.expr {
        let mut expr = expr.as_ref().clone();
        rewrite_where_for_update_registers(&mut expr, columns, columns_start_reg, rowid_reg)?;
        // The caller must have populated resolver.register_affinities so that
        // comparison instructions in the expression get the correct column
        // affinity even though column references have been rewritten to
        // Expr::Register.
        translate_expr_no_constant_opt(
            program,
            None,
            &expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    } else {
        let col_in_table = columns
            .get(idx_col.pos_in_table)
            .expect("column index out of bounds");
        let src_reg = if col_in_table.is_rowid_alias() {
            rowid_reg
        } else {
            columns_start_reg + idx_col.pos_in_table
        };
        program.emit_insn(Insn::Copy {
            src_reg,
            dst_reg: dest_reg,
            extra_amount: 0,
        });
    }
    Ok(())
}

/// Emit bytecode for evaluating CHECK constraints.
/// Assumes the resolver cache is already populated with column-to-register mappings.
fn emit_check_constraint_bytecode(
    program: &mut ProgramBuilder,
    check_constraints: &[CheckConstraint],
    resolver: &mut Resolver,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
    referenced_tables: Option<&TableReferences>,
    table_name: &str,
) -> Result<()> {
    for check_constraint in check_constraints {
        let expr_result_reg = program.alloc_register();

        let mut rewritten_expr = check_constraint.expr.clone();
        rewrite_between_expr(&mut rewritten_expr);
        if let Some(referenced_tables) = referenced_tables {
            let mut binding_tables = referenced_tables.clone();
            if let Some(joined_table) = binding_tables.joined_tables_mut().first_mut() {
                // CHECK expressions come from schema SQL and may use the base table name
                // even when the query references the table through an alias.
                joined_table.identifier = table_name.to_string();
            }
            bind_and_rewrite_expr(
                &mut rewritten_expr,
                Some(&mut binding_tables),
                None,
                resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
        }

        translate_expr_no_constant_opt(
            program,
            referenced_tables,
            &rewritten_expr,
            expr_result_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;

        // CHECK constraint passes if the result is NULL or non-zero (truthy)
        let constraint_passed_label = program.allocate_label();

        // NULL means unknown, which passes CHECK constraints in SQLite
        program.emit_insn(Insn::IsNull {
            reg: expr_result_reg,
            target_pc: constraint_passed_label,
        });

        program.emit_insn(Insn::If {
            reg: expr_result_reg,
            target_pc: constraint_passed_label,
            jump_if_null: false,
        });

        let constraint_name = match &check_constraint.name {
            Some(name) => name.clone(),
            None => format!("{}", check_constraint.expr),
        };

        match or_conflict {
            ResolveType::Ignore => {
                program.emit_insn(Insn::Goto {
                    target_pc: skip_row_label,
                });
            }
            // In SQLite, REPLACE does not apply to CHECK constraints — it aborts,
            // same as Abort/Fail/Rollback.
            ResolveType::Abort
            | ResolveType::Fail
            | ResolveType::Rollback
            | ResolveType::Replace => {
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_CHECK,
                    description: constraint_name.to_string(),
                });
            }
        }

        program.preassign_label_to_next_insn(constraint_passed_label);
    }
    Ok(())
}

/// Returns true if the CHECK constraint expression references any column whose
/// normalized name is in `column_names`. This is used during UPDATE to skip
/// CHECK constraints that only reference columns not in the SET clause, matching
/// SQLite's optimization behavior.
fn check_expr_references_columns(expr: &ast::Expr, column_names: &HashSet<String>) -> bool {
    column_names
        .iter()
        .any(|name| check_expr_references_column(expr, name))
}

/// Emit CHECK constraint evaluation with resolver cache setup and teardown.
/// Takes column-to-register mappings as an iterator to avoid heap allocation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_check_constraints<'a>(
    program: &mut ProgramBuilder,
    check_constraints: &[CheckConstraint],
    resolver: &mut Resolver,
    table_name: &str,
    rowid_reg: usize,
    column_mappings: impl Iterator<Item = (&'a str, usize)>,
    connection: &Arc<Connection>,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
    referenced_tables: Option<&TableReferences>,
) -> Result<()> {
    if connection.check_constraints_ignored() || check_constraints.is_empty() {
        return Ok(());
    }

    let column_mappings: Vec<(&str, usize)> = column_mappings.collect();
    let initial_cache_size = resolver.expr_to_reg_cache.len();

    // Map rowid aliases to the actual rowid register.
    // We cache both unqualified (Expr::Id) and qualified (Expr::Qualified) forms
    // so that CHECK expressions like `CHECK(rowid > 0)` and `CHECK(t.rowid > 0)` both resolve.
    for rowid_name in ROWID_STRS {
        let rowid_expr = ast::Expr::Id(ast::Name::exact(rowid_name.to_string()));
        resolver
            .expr_to_reg_cache
            .push((Cow::Owned(rowid_expr), rowid_reg));
        let qualified_expr = ast::Expr::Qualified(
            ast::Name::exact(table_name.to_string()),
            ast::Name::exact(rowid_name.to_string()),
        );
        resolver
            .expr_to_reg_cache
            .push((Cow::Owned(qualified_expr), rowid_reg));
    }

    // Map each column to its register (both unqualified and qualified forms).
    for (col_name, register) in column_mappings.iter().copied() {
        let column_expr = ast::Expr::Id(ast::Name::exact(col_name.to_string()));
        resolver
            .expr_to_reg_cache
            .push((Cow::Owned(column_expr), register));
        let qualified_expr = ast::Expr::Qualified(
            ast::Name::exact(table_name.to_string()),
            ast::Name::exact(col_name.to_string()),
        );
        resolver
            .expr_to_reg_cache
            .push((Cow::Owned(qualified_expr), register));
    }

    if let Some(joined_table) = referenced_tables.and_then(|tables| tables.joined_tables().first())
    {
        resolver.expr_to_reg_cache.push((
            Cow::Owned(ast::Expr::RowId {
                database: None,
                table: joined_table.internal_id,
            }),
            rowid_reg,
        ));

        for (col_name, register) in column_mappings.iter().copied() {
            if let Some((idx, col)) = joined_table.columns().iter().enumerate().find(|(_, c)| {
                c.name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(col_name))
            }) {
                resolver.expr_to_reg_cache.push((
                    Cow::Owned(ast::Expr::Column {
                        database: None,
                        table: joined_table.internal_id,
                        column: idx,
                        is_rowid_alias: col.is_rowid_alias(),
                    }),
                    register,
                ));
            }
        }
    }

    resolver.enable_expr_to_reg_cache();

    let result = emit_check_constraint_bytecode(
        program,
        check_constraints,
        resolver,
        or_conflict,
        skip_row_label,
        referenced_tables,
        table_name,
    );

    // Always restore resolver state, even on error.
    resolver.expr_to_reg_cache.truncate(initial_cache_size);
    resolver.expr_to_reg_cache_enabled = false;

    result
}
