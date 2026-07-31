use crate::{
    alloc::{self, TursoIteratorExt, TursoVecExt},
    function::{AccumulatorFunc, AggFunc},
    schema::{
        BTreeTable, ColDef, Column, FromClauseSubquery, Index, PseudoCursorType, RecursiveCteInput,
        Schema, Table, Type, ROWID_SENTINEL,
    },
    translate::{
        collate::CollationSeq,
        emitter::UpdateRowSource,
        optimizer::constraints::{BinaryExprSide, SeekRangeConstraint},
        plan_expr::{
            plan_expr_dependencies, plan_exprs_are_equivalent, walk_plan_expr,
            walk_plan_expr_dependencies, PlanColumnRef, PlanColumnTypePrograms, PlanColumnUse,
            PlanCteId, PlanExpr as Expr, PlanExprAffinity, PlanExprFactSource, PlanOrderTerm,
            PlanOutputId, PlanParameter, PlanSourceId, PlanSubqueryId, PlanWalkControl,
        },
        planner::{determine_where_to_eval_term, source_loop_index},
        semantic::hir::{
            OutputNameKind, ResolvedCollation, ResolvedIndex, ResolvedTable, TypeFact,
        },
    },
    types::SeekOp,
    vdbe::{
        affinity::{self, Affinity},
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{HashDistinctData, Insn},
        BranchOffset, CursorID,
    },
    Result, VirtualTable, MAIN_DB_ID,
};
use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use std::{cmp::Ordering, marker::PhantomData, sync::Arc};
use turso_parser::ast::{self, ResolveType, SortOrder};

use super::emitter::OperationMode;

/// Computes the affinity of column `i` of a compound (UNION/INTERSECT/EXCEPT)
/// subquery, matching SQLite's `sqlite3SubqueryColumnTypes` (select.c).
///
/// Scanning the arms left-to-right, the affinity is the first arm's affinity,
/// skipping leading arms that have no affinity (adopting the next arm's). If
/// every arm has no affinity the result has no affinity. Otherwise the column
/// keeps that affinity unless a later arm yields a conflicting datatype class
/// (TEXT affinity + a numeric arm, or numeric affinity + a text arm), in which
/// case it is downgraded to no affinity so the column is compared by storage
/// class.
fn compound_column_affinity(arms: &[&SelectPlan], i: usize) -> PlanExprAffinity {
    if arms.is_empty() {
        return PlanExprAffinity::no_affinity();
    }
    let col_affinity = |arm: &SelectPlan| {
        arm.result_columns
            .get(i)
            .map(|column| column.affinity)
            .unwrap_or_else(PlanExprAffinity::no_affinity)
    };
    let col_data_type = |arm: &SelectPlan| {
        arm.result_columns
            .get(i)
            .map(ResultSetColumn::storage_classes)
            .unwrap_or(StorageClassSet::EMPTY)
    };

    let mut affinity = col_affinity(arms[0]);
    let mut data_types = StorageClassSet::EMPTY;
    let mut idx = 0;
    // Skip leading arms with no affinity, adopting the next arm's affinity.
    while !affinity.has_affinity && idx + 1 < arms.len() {
        data_types |= col_data_type(arms[idx]);
        idx += 1;
        affinity = col_affinity(arms[idx]);
    }
    if !affinity.has_affinity {
        return PlanExprAffinity::no_affinity();
    }
    // Accumulate the remaining arms' classes. A real BLOB affinity stays
    // distinct from the absence of affinity even though neither conflicts
    // with TEXT or numeric storage classes here.
    for &arm in &arms[idx + 1..] {
        data_types |= col_data_type(arm);
    }
    match affinity.affinity {
        Affinity::Text if data_types.has_numeric() => PlanExprAffinity::no_affinity(),
        value if value.is_numeric() && data_types.has_text() => PlanExprAffinity::no_affinity(),
        _ => affinity,
    }
}

/// Storage classes a result expression can produce. This mirrors the small
/// bitset SQLite uses while choosing compound-select affinity, but is derived
/// once from semantic output facts rather than by walking parser syntax.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageClassSet(u8);

impl StorageClassSet {
    const EMPTY: Self = Self(0);
    const NUMERIC: Self = Self(1 << 0);
    const TEXT: Self = Self(1 << 1);
    const BLOB: Self = Self(1 << 2);
    const ALL: Self = Self(Self::NUMERIC.0 | Self::TEXT.0 | Self::BLOB.0);

    const fn has_numeric(self) -> bool {
        self.0 & Self::NUMERIC.0 != 0
    }

    const fn has_text(self) -> bool {
        self.0 & Self::TEXT.0 != 0
    }
}

impl std::ops::BitOrAssign for StorageClassSet {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[derive(Debug, Clone)]
pub struct ResultSetColumn {
    pub id: PlanOutputId,
    /// Final SQLite result name, decided by semantic analysis.
    pub name: String,
    pub name_kind: OutputNameKind,
    /// Resolved origin used by full/short-column-name metadata without
    /// retaining a parser expression or consulting a live schema.
    pub origin: Option<ResultColumnOrigin>,
    pub type_fact: TypeFact,
    /// SQLite expression affinity, kept separate from storage type because a
    /// literal has a known storage class but deliberately has no affinity.
    pub affinity: PlanExprAffinity,
    /// Effective output collation fixed during semantic lowering.
    pub collation: Option<ResolvedCollation>,
    /// Number of array dimensions represented by the value. Kept as output
    /// metadata so derived tables do not have to rediscover it from syntax.
    pub array_dimensions: u32,
    /// Resolved expression in this plan's identity space.
    pub expr: Expr,
    // TODO: encode which aggregates (e.g. index bitmask of plan.aggregates) are present in this column
    pub contains_aggregates: bool,
}

/// Runtime storage for a semantic row image such as NEW, OLD, or EXCLUDED.
/// Ordinary table sources are read through `TableReferences`; only sources
/// whose values live outside a cursor are present here.
#[derive(Debug, Clone)]
pub enum RuntimeValueBinding {
    Register { register: usize, needs_decode: bool },
    Parameter(PlanParameter),
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeRowBinding {
    pub columns: Vec<RuntimeValueBinding>,
    pub rowid: Option<RuntimeValueBinding>,
    /// Source-local decode programs for encoded register values. Fully decoded
    /// rows and parameter-only schema-program inputs keep this empty.
    pub read_programs: Option<Arc<SourceReadPrograms>>,
}

/// Materialized value and semantic facts for one plan output.
///
/// Unlike source columns, an output reference does not carry its facts inside
/// `PlanExpr`, so both pieces are installed together when the output acquires
/// a stable runtime location.
#[derive(Debug, Clone)]
pub struct RuntimeOutputBinding {
    pub value: RuntimeValueBinding,
    pub fact: PlanOutputFact,
    pub definition: RuntimeOutputDefinition,
}

#[derive(Debug, Clone)]
pub enum RuntimeOutputDefinition {
    /// The owning SELECT's current expression, after physical rewrites.
    Plan(Expr),
    /// A value injected by an API boundary rather than computed by a SELECT.
    ExternalInput,
}

/// Runtime storage chosen for one non-FROM subquery plus the output facts that
/// remain valid after its physical plan has been consumed.
#[derive(Debug, Clone)]
pub struct RuntimeSubqueryBinding {
    pub query_type: PlanSubqueryType,
    pub output_facts: Vec<PlanOutputFact>,
}

#[derive(Debug, Clone, Default)]
pub struct PlanRuntimeBindings {
    rows: HashMap<PlanSourceId, RuntimeRowBinding>,
    outputs: HashMap<PlanOutputId, RuntimeOutputBinding>,
    subqueries: HashMap<PlanSubqueryId, RuntimeSubqueryBinding>,
}

impl PlanRuntimeBindings {
    pub(crate) fn has_value_bindings(&self) -> bool {
        !self.rows.is_empty() || !self.outputs.is_empty()
    }

    pub fn bind_row(
        &mut self,
        source: PlanSourceId,
        binding: RuntimeRowBinding,
    ) -> Option<RuntimeRowBinding> {
        let has_encoded_register =
            binding
                .columns
                .iter()
                .chain(binding.rowid.iter())
                .any(|value| {
                    matches!(
                        value,
                        RuntimeValueBinding::Register {
                            needs_decode: true,
                            ..
                        }
                    )
                });
        assert!(
            !has_encoded_register || binding.read_programs.is_some(),
            "runtime row binding for {source} has encoded registers without source read programs"
        );
        self.rows.insert(source, binding)
    }

    pub fn row(&self, source: PlanSourceId) -> Option<&RuntimeRowBinding> {
        self.rows.get(&source)
    }

    pub fn value(&self, source: PlanSourceId, column: usize) -> Option<&RuntimeValueBinding> {
        self.row(source)?.columns.get(column)
    }

    pub fn rowid(&self, source: PlanSourceId) -> Option<&RuntimeValueBinding> {
        self.row(source)?.rowid.as_ref()
    }

    pub fn read_programs(&self, source: PlanSourceId) -> Option<Arc<SourceReadPrograms>> {
        self.row(source)?.read_programs.as_ref().map(Arc::clone)
    }

    pub fn bind_output(
        &mut self,
        output: PlanOutputId,
        binding: RuntimeOutputBinding,
    ) -> Option<RuntimeOutputBinding> {
        self.outputs.insert(output, binding)
    }

    pub fn output(&self, output: PlanOutputId) -> Option<&RuntimeOutputBinding> {
        self.outputs.get(&output)
    }

    pub fn bind_subquery(
        &mut self,
        subquery: PlanSubqueryId,
        binding: RuntimeSubqueryBinding,
    ) -> Option<RuntimeSubqueryBinding> {
        self.subqueries.insert(subquery, binding)
    }

    pub fn subquery(&self, subquery: PlanSubqueryId) -> Option<&RuntimeSubqueryBinding> {
        self.subqueries.get(&subquery)
    }

    pub fn extend(&mut self, other: Self) {
        self.rows.extend(other.rows);
        self.outputs.extend(other.outputs);
        self.subqueries.extend(other.subqueries);
    }
}

impl PlanExprFactSource for PlanRuntimeBindings {
    fn output_type_fact(&self, output: PlanOutputId) -> Option<TypeFact> {
        self.output(output)
            .map(|binding| binding.fact.type_fact.clone())
    }

    fn subquery_output_type_fact(&self, query: PlanSubqueryId, output: usize) -> Option<TypeFact> {
        self.subquery(query)?
            .output_facts
            .get(output)
            .map(|fact| fact.type_fact.clone())
    }

    fn subquery_width(&self, query: PlanSubqueryId) -> Option<usize> {
        self.subquery(query)
            .map(|binding| binding.output_facts.len())
    }

    fn output_affinity(&self, output: PlanOutputId) -> Option<PlanExprAffinity> {
        self.output(output).map(|binding| binding.fact.affinity)
    }

    fn subquery_output_affinity(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<PlanExprAffinity> {
        self.subquery(query)?
            .output_facts
            .get(output)
            .map(|fact| fact.affinity)
    }

    fn output_collation(&self, output: PlanOutputId) -> Option<CollationSeq> {
        self.output(output)
            .and_then(|binding| binding.fact.collation.as_ref())
            .map(|collation| *collation.value())
    }

    fn subquery_output_collation(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<CollationSeq> {
        self.subquery(query)?
            .output_facts
            .get(output)
            .and_then(|fact| fact.collation.as_ref())
            .map(|collation| *collation.value())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultColumnOrigin {
    Column { source: PlanSourceId, column: usize },
    RowId { source: PlanSourceId },
}

impl ResultSetColumn {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn name_or_expr(&self) -> String {
        self.name.clone()
    }

    /// Returns the canonical short type name for this column's affinity,
    /// matching SQLite's `azType[]` in `createTableStmt()` (build.c).
    pub fn declared_type(&self) -> &'static str {
        // CTAS propagates the affinity of a declared source column or CAST,
        // but it does not invent a declaration from a literal, function, or
        // arithmetic result merely because its storage class is known.
        match self
            .type_fact
            .declared
            .as_ref()
            .map(|declared| declared.storage)
        {
            Some(Type::Text) => "TEXT",
            Some(Type::Numeric) => "NUM",
            Some(Type::Integer) => "INT",
            Some(Type::Real) => "REAL",
            Some(Type::Null | Type::Blob) | None => "",
        }
    }

    fn storage_classes(&self) -> StorageClassSet {
        if self.affinity.has_affinity {
            return if self.affinity.affinity.is_numeric() {
                StorageClassSet(StorageClassSet::NUMERIC.0 | StorageClassSet::BLOB.0)
            } else if self.affinity.affinity == Affinity::Text {
                StorageClassSet(StorageClassSet::TEXT.0 | StorageClassSet::BLOB.0)
            } else {
                StorageClassSet::ALL
            };
        }

        match self.type_fact.storage {
            Some(Type::Null) => StorageClassSet::EMPTY,
            Some(Type::Text) => StorageClassSet::TEXT,
            Some(Type::Blob) => StorageClassSet::BLOB,
            Some(Type::Numeric | Type::Integer | Type::Real) => StorageClassSet::NUMERIC,
            None => StorageClassSet::ALL,
        }
    }
}

/// Output metadata that must outlive the physical plan which produced it.
/// Non-FROM subquery plans are consumed during emission, but outer expression
/// lowering still needs these facts afterward.
#[derive(Debug, Clone)]
pub struct PlanOutputFact {
    pub type_fact: TypeFact,
    pub affinity: PlanExprAffinity,
    pub collation: Option<ResolvedCollation>,
    pub array_dimensions: u32,
}

/// One result value read by a nested query from the query block that owns it.
///
/// The defining expression remains in the owning block. A child plan keeps the
/// frozen facts and source dependencies needed for planning, then reads the
/// owner's materialized register at runtime.
#[derive(Debug, Clone)]
pub struct PlanOuterOutputReference {
    pub output: PlanOutputId,
    pub definition: Expr,
    pub fact: PlanOutputFact,
    pub source_dependencies: Vec<PlanSourceId>,
}

impl From<&ResultSetColumn> for PlanOutputFact {
    fn from(column: &ResultSetColumn) -> Self {
        Self {
            type_fact: column.type_fact.clone(),
            affinity: column.affinity,
            collation: column.collation.clone(),
            array_dimensions: column.array_dimensions,
        }
    }
}

impl PlanOutputFact {
    pub fn for_plan(plan: &Plan) -> Vec<Self> {
        match plan {
            Plan::Select(select) => select
                .result_columns
                .iter()
                .map(PlanOutputFact::from)
                .collect(),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                let mut arms = left.iter().map(|(select, _)| select).collect::<Vec<_>>();
                arms.push(right_most);
                let first = left
                    .first()
                    .map(|(select, _)| &select.result_columns)
                    .unwrap_or(&right_most.result_columns);
                first
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        let mut fact = PlanOutputFact::from(column);
                        fact.affinity = compound_column_affinity(&arms, index);
                        fact
                    })
                    .collect()
            }
            Plan::RecursiveCte(recursive) => recursive
                .result_columns
                .iter()
                .map(PlanOutputFact::from)
                .collect(),
            Plan::Delete(_) | Plan::Update(_) => {
                panic!("DML plan cannot provide query output facts")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupBy {
    pub exprs: Vec<Expr>,
    /// Sort direction for each GROUP BY key column. Always present once
    /// `compute_group_by_sort_order` has run; the outer optimizer reads
    /// this to derive the materialized CTE's output order.
    pub sort_order: Vec<SortOrder>,
    /// NULLS ordering for each GROUP BY key column. Populated when ORDER BY
    /// with explicit NULLS FIRST/LAST is merged into GROUP BY.
    pub nulls_order: Vec<Option<ast::NullsOrder>>,
    /// When true the scan already provides the GROUP BY order and no
    /// sorter is emitted. The `sort_order` is kept so that the outer
    /// query can still read the effective output order.
    pub sort_elided: bool,
    /// having clause split into a vec at 'AND' boundaries.
    pub having: Option<Vec<Expr>>,
}

/// In a query plan, WHERE clause conditions and JOIN conditions are all folded into a vector of WhereTerm.
/// This is done so that we can evaluate the conditions at the correct loop depth.
/// We also need to keep track of whether the condition came from an OUTER JOIN. Take this example:
/// SELECT * FROM users u LEFT JOIN products p ON u.id = 5.
/// Even though the condition only refers to 'u', we CANNOT evaluate it at the users loop, because we need to emit NULL
/// values for the columns of 'p', for EVERY row in 'u', instead of completely skipping any rows in 'u' where the condition is false.
#[derive(Debug, Clone)]
pub struct WhereTerm {
    /// The original condition expression.
    pub expr: Expr,
    /// For normal JOIN conditions (ON or WHERE clauses), we break them up into individual [WhereTerm] conditions
    /// and let the optimizer determine when each should be evaluated based on the tables they reference.
    /// See e.g. [EvalAt].
    /// For example, in "SELECT * FROM x JOIN y WHERE x.a = 2", we want to evaluate x.a = 2 right after opening x
    /// since it only depends on x.
    ///
    /// However, OUTER JOIN conditions require special handling. Consider:
    ///   SELECT * FROM t LEFT JOIN s ON t.a = 2
    ///
    /// Even though t.a = 2 only references t, we cannot evaluate it during t's loop and skip rows where t.a != 2.
    /// Instead, we must:
    /// 1. Process ALL rows from t
    /// 2. For each t row where t.a != 2, emit NULL values for s's columns
    /// 3. For each t row where t.a = 2, emit the actual s values
    ///
    /// This means the condition must be evaluated during s's loop, regardless of which tables it references.
    /// We track this requirement using [WhereTerm::from_outer_join], which contains the [PlanSourceId] of the
    /// right-side table of the OUTER JOIN (in this case, s). When evaluating conditions, if [WhereTerm::from_outer_join]
    /// is set, we force evaluation to happen during that table's loop.
    pub from_outer_join: Option<PlanSourceId>,
    /// Whether the condition has been consumed by the optimizer in some way, and it should not be evaluated
    /// in the normal place where WHERE terms are evaluated.
    /// A term may have been consumed e.g. if:
    /// - it has been converted into a constraint in a seek key
    /// - it has been removed due to being trivially true or false
    pub consumed: bool,
}

impl WhereTerm {
    pub fn should_eval_before_loop(
        &self,
        join_order: &[JoinOrderMember],
        subqueries: &[NonFromClauseSubquery],
        table_references: Option<&TableReferences>,
    ) -> bool {
        if self.consumed {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order, subqueries, table_references) else {
            return false;
        };
        eval_at == EvalAt::BeforeLoop
    }

    pub fn should_eval_at_loop(
        &self,
        loop_idx: usize,
        join_order: &[JoinOrderMember],
        subqueries: &[NonFromClauseSubquery],
        table_references: Option<&TableReferences>,
    ) -> bool {
        if self.consumed {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order, subqueries, table_references) else {
            return false;
        };
        eval_at == EvalAt::Loop(loop_idx)
    }

    fn eval_at(
        &self,
        join_order: &[JoinOrderMember],
        subqueries: &[NonFromClauseSubquery],
        table_references: Option<&TableReferences>,
    ) -> Result<EvalAt> {
        determine_where_to_eval_term(self, join_order, subqueries, table_references)
    }
}

impl From<Expr> for WhereTerm {
    fn from(value: Expr) -> Self {
        Self {
            expr: value,
            from_outer_join: None,
            consumed: false,
        }
    }
}

/// The loop index where to evaluate the condition.
/// For example, in `SELECT * FROM u JOIN p WHERE u.id = 5`, the condition can already be evaluated at the first loop (idx 0),
/// because that is the rightmost table that it references.
///
/// Conditions like 1=2 can be evaluated before the main loop is opened, because they are constant.
/// In theory we should be able to statically analyze them all and reduce them to a single boolean value,
/// but that is not implemented yet.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum EvalAt {
    Loop(usize),
    BeforeLoop,
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for EvalAt {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (EvalAt::Loop(a), EvalAt::Loop(b)) => a.partial_cmp(b),
            (EvalAt::BeforeLoop, EvalAt::BeforeLoop) => Some(Ordering::Equal),
            (EvalAt::BeforeLoop, _) => Some(Ordering::Less),
            (_, EvalAt::BeforeLoop) => Some(Ordering::Greater),
        }
    }
}

impl Ord for EvalAt {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other)
            .expect("total ordering not implemented for EvalAt")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryEvalPhase {
    BeforeLoop,
    Loop(usize),
    GroupedOutput,
    UngroupedAggregateOutput,
    WindowOutput,
    PreWrite,
    PostWriteReturning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryOrigin {
    SelectList,
    SelectWhere,
    SelectGroupBy,
    SelectHaving,
    SelectOrderBy,
    SelectLimitOffset,
    DmlWhere,
    DmlSet,
    DmlReturning,
    TriggerWhen,
}

impl SubqueryOrigin {
    pub fn phase_floor(self) -> SubqueryEvalPhase {
        match self {
            SubqueryOrigin::SelectList
            | SubqueryOrigin::SelectWhere
            | SubqueryOrigin::SelectGroupBy
            | SubqueryOrigin::SelectHaving
            | SubqueryOrigin::SelectOrderBy
            | SubqueryOrigin::SelectLimitOffset
            | SubqueryOrigin::TriggerWhen => SubqueryEvalPhase::BeforeLoop,
            SubqueryOrigin::DmlWhere => SubqueryEvalPhase::BeforeLoop,
            SubqueryOrigin::DmlSet => SubqueryEvalPhase::PreWrite,
            SubqueryOrigin::DmlReturning => SubqueryEvalPhase::PostWriteReturning,
        }
    }

    pub fn is_post_write_returning(self) -> bool {
        matches!(self, SubqueryOrigin::DmlReturning)
    }
}

/// One ORDER BY key of a compound SELECT:
/// `(result_column_index, sort_order, nulls_order, explicit_collation)`.
/// The column index is 0-based into the result set. The explicit collation is
/// set when the term carries a COLLATE override and otherwise the referenced
/// column's own collation is used.
pub(crate) type CompoundOrderByKey = (
    usize,
    SortOrder,
    Option<ast::NullsOrder>,
    Option<CollationSeq>,
);

/// A query plan is either a SELECT or a DELETE (for now)
/// Variants are boxed so that moving a `Plan` around the prepare path
/// (returns from plan builders, argument to emitters) costs a pointer
/// move rather than ~880 B on the stack.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Plan {
    Select(Box<SelectPlan>),
    CompoundSelect {
        left: Vec<(SelectPlan, ast::CompoundOperator)>,
        right_most: Box<SelectPlan>,
        limit: Option<Expr>,
        offset: Option<Expr>,
        /// Resolved ORDER BY terms. Compound restrictions were validated in HIR.
        order_by: Vec<PlanOrderTerm>,
    },
    /// Runs the initial query once, then runs the recursive query for each queued row.
    RecursiveCte(Box<RecursiveCtePlan>),
    Delete(Box<DeletePlan>),
    Update(Box<UpdatePlan>),
}

#[derive(Debug, Clone)]
/// Everything needed to emit one self-referencing CTE.
pub struct RecursiveCtePlan {
    pub name: String,
    pub initial_query: Box<Plan>,
    pub recursive_query: Box<Plan>,
    /// Canonical outward-facing columns fixed by semantic analysis after the
    /// seed and every recursive arm have been reconciled. The two child plans
    /// are execution inputs, not competing metadata sources.
    pub result_columns: Vec<ResultSetColumn>,
    /// Collations used by the recursive queue and UNION deduplication. These
    /// follow compound comparison precedence and may differ from the seed's
    /// outward-facing column collation.
    pub comparison_collations: Vec<Option<ResolvedCollation>>,
    /// One physical source identity for each syntactic self-reference in the
    /// recursive term, in stable HIR source order.
    pub input_table_ids: Vec<PlanSourceId>,
    pub union_all: bool,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub queue_order: Vec<RecursiveCteOrderTerm>,
    pub query_destination: QueryDestination,
}

/// Exact row-image values read from one source anywhere inside a physical
/// plan. Column entries retain their semantic facts, so callers can bind raw
/// registers without reopening the schema to recover type or collation data.
#[derive(Clone, Debug, Default)]
pub struct PlanRowDependency {
    pub columns: Vec<PlanColumnRef>,
    pub rowid: bool,
}

impl Plan {
    /// Returns true if this SELECT plan contains a reference to the given table.
    /// For compound selects, checks all component selects.
    /// Returns false for Delete/Update plans.
    pub fn select_contains_table(&self, table: &Table) -> bool {
        match self {
            Plan::Select(select_plan) => select_plan.table_references.contains_table(table),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                right_most.table_references.contains_table(table)
                    || left
                        .iter()
                        .any(|(plan, _)| plan.table_references.contains_table(table))
            }
            Plan::RecursiveCte(plan) => {
                plan.initial_query.select_contains_table(table)
                    || plan.recursive_query.select_contains_table(table)
            }
            Plan::Delete(_) | Plan::Update(_) => false,
        }
    }

    /// Returns the query destination for Select/CompoundSelect plans.
    /// Returns None for Delete/Update plans.
    pub fn select_query_destination(&self) -> Option<&QueryDestination> {
        match self {
            Plan::Select(select_plan) => Some(&select_plan.query_destination),
            Plan::CompoundSelect { right_most, .. } => Some(&right_most.query_destination),
            Plan::RecursiveCte(plan) => Some(&plan.query_destination),
            Plan::Delete(_) | Plan::Update(_) => None,
        }
    }

    /// Returns a mutable reference to the query destination for Select/CompoundSelect plans.
    /// Returns None for Delete/Update plans.
    pub fn select_query_destination_mut(&mut self) -> Option<&mut QueryDestination> {
        match self {
            Plan::Select(select_plan) => Some(&mut select_plan.query_destination),
            Plan::CompoundSelect { right_most, .. } => Some(&mut right_most.query_destination),
            Plan::RecursiveCte(plan) => Some(&mut plan.query_destination),
            Plan::Delete(_) | Plan::Update(_) => None,
        }
    }

    /// Returns the result columns of a SELECT or compound SELECT plan. For a
    /// compound SELECT the columns of the right-most component are returned,
    /// since every component must agree on column count and naming.
    ///
    /// # Panics
    ///
    /// Panics if called on a DELETE or UPDATE plan, which have no result
    /// columns.
    pub fn select_result_columns(&self) -> &[ResultSetColumn] {
        match self {
            Plan::Select(select_plan) => &select_plan.result_columns,
            Plan::CompoundSelect { right_most, .. } => &right_most.result_columns,
            Plan::RecursiveCte(plan) => &plan.result_columns,
            Plan::Delete(_) | Plan::Update(_) => {
                panic!("select_result_columns called on a non-SELECT plan")
            }
        }
    }

    /// Returns the table references of a SELECT or compound SELECT plan. For
    /// a compound SELECT the references of the right-most component are
    /// returned.
    ///
    /// # Panics
    ///
    /// Panics if called on a DELETE or UPDATE plan.
    pub fn select_table_references(&self) -> &TableReferences {
        match self {
            Plan::Select(select_plan) => &select_plan.table_references,
            Plan::CompoundSelect { right_most, .. } => &right_most.table_references,
            Plan::RecursiveCte(plan) => plan.initial_query.select_table_references(),
            Plan::Delete(_) | Plan::Update(_) => {
                panic!("select_table_references called on a non-SELECT plan")
            }
        }
    }

    /// Returns the IDs of every outer-query reference that this plan actually
    /// uses. For a compound SELECT, the result spans all of its component
    /// SELECTs. DELETE and UPDATE plans have no outer-query references and
    /// always return an empty vector.
    pub fn used_outer_query_ref_ids(&self) -> Vec<PlanSourceId> {
        fn collect_from_select(plan: &SelectPlan, out: &mut Vec<PlanSourceId>) {
            for outer_ref in plan.table_references.outer_query_refs().iter() {
                if outer_ref.is_used() {
                    out.push(outer_ref.internal_id);
                }
            }
        }
        let mut ids = Vec::new();
        match self {
            Plan::Select(plan) => collect_from_select(plan, &mut ids),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                for (plan, _) in left {
                    collect_from_select(plan, &mut ids);
                }
                collect_from_select(right_most, &mut ids);
            }
            Plan::RecursiveCte(plan) => {
                ids.extend(plan.initial_query.used_outer_query_ref_ids());
                ids.extend(plan.recursive_query.used_outer_query_ref_ids());
            }
            Plan::Delete(_) | Plan::Update(_) => {}
        }
        ids
    }

    /// Collect every column and rowid read from `source`, including reads in
    /// compound arms, recursive terms, and still-unemitted nested queries.
    /// This is used by post-write RETURNING, where the target cursor no longer
    /// represents the OLD/NEW row but correlated child plans still need it.
    pub fn source_row_dependency(&self, source: PlanSourceId) -> Result<PlanRowDependency> {
        let mut dependency = PlanRowDependency::default();
        collect_plan_row_dependency(self, source, &mut dependency)?;
        Ok(dependency)
    }

    /// Returns true if this plan or any of its subplans read from the given table.
    /// (Not for Delete/Update plans)
    fn reads_table(&self, database_id: usize, table_name: &str) -> bool {
        match self {
            Plan::Select(select_plan) => select_plan.reads_table(database_id, table_name),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                left.iter()
                    .any(|(select_plan, _)| select_plan.reads_table(database_id, table_name))
                    || right_most.reads_table(database_id, table_name)
            }
            Plan::RecursiveCte(plan) => {
                plan.initial_query.reads_table(database_id, table_name)
                    || plan.recursive_query.reads_table(database_id, table_name)
            }
            Plan::Delete(_) | Plan::Update(_) => false,
        }
    }
}

fn collect_expr_row_dependency(
    expr: &Expr,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    walk_plan_expr(expr, &mut |expr| {
        match expr {
            Expr::Column(column) if column.source == source => {
                if !dependency
                    .columns
                    .iter()
                    .any(|existing| existing.column == column.column)
                {
                    dependency.columns.push(column.clone());
                }
            }
            Expr::MergedColumn(column) if column.right.source == source => {
                if !dependency
                    .columns
                    .iter()
                    .any(|existing| existing.column == column.right.column)
                {
                    dependency.columns.push(column.right.clone());
                }
            }
            Expr::RowId(expr_source) if *expr_source == source => dependency.rowid = true,
            _ => {}
        }
        Ok(PlanWalkControl::Continue)
    })?;
    Ok(())
}

fn collect_exprs_row_dependency<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    for expr in exprs {
        collect_expr_row_dependency(expr, source, dependency)?;
    }
    Ok(())
}

fn collect_order_terms_row_dependency(
    terms: &[PlanOrderTerm],
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    collect_exprs_row_dependency(terms.iter().map(|term| &term.expr), source, dependency)
}

fn collect_seek_row_dependency(
    seek: &SeekDef,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    for constraint in &seek.prefix {
        if let Some((_, expr, _)) = &constraint.eq {
            collect_expr_row_dependency(expr, source, dependency)?;
        }
        if let Some((_, expr, _)) = &constraint.lower_bound {
            collect_expr_row_dependency(expr, source, dependency)?;
        }
        if let Some((_, expr, _)) = &constraint.upper_bound {
            collect_expr_row_dependency(expr, source, dependency)?;
        }
    }
    for key in [&seek.start, &seek.end] {
        if let SeekKeyComponent::Expr(expr) = &key.last_component {
            collect_expr_row_dependency(expr, source, dependency)?;
        }
    }
    Ok(())
}

fn collect_in_seek_row_dependency(
    input: &InSeekSource,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    if let InSeekSource::LiteralList { values, .. } = input {
        collect_exprs_row_dependency(values, source, dependency)?;
    }
    Ok(())
}

fn collect_operation_row_dependency(
    operation: &Operation,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    match operation {
        Operation::Scan(Scan::VirtualTable { constraints, .. }) => {
            collect_exprs_row_dependency(constraints, source, dependency)?;
        }
        Operation::Search(Search::RowidEq { cmp_expr }) => {
            collect_expr_row_dependency(cmp_expr, source, dependency)?;
        }
        Operation::Search(Search::Seek { seek_def, .. }) => {
            collect_seek_row_dependency(seek_def, source, dependency)?;
        }
        Operation::Search(Search::InSeek { source: input, .. }) => {
            collect_in_seek_row_dependency(input, source, dependency)?;
        }
        Operation::IndexMethodQuery(query) => {
            collect_exprs_row_dependency(&query.arguments, source, dependency)?;
        }
        Operation::MultiIndexScan(scan) => {
            for branch in &scan.branches {
                match &branch.access {
                    MultiIndexBranchAccess::Seek { seek_def } => {
                        collect_seek_row_dependency(seek_def, source, dependency)?;
                    }
                    MultiIndexBranchAccess::InSeek { source: input } => {
                        collect_in_seek_row_dependency(input, source, dependency)?;
                    }
                }
                if let Some(residuals) = &branch.union_residuals {
                    collect_exprs_row_dependency(
                        residuals
                            .pre_filter_exprs
                            .iter()
                            .chain(&residuals.post_filter_exprs),
                        source,
                        dependency,
                    )?;
                }
            }
        }
        Operation::Scan(
            Scan::BTreeTable { .. } | Scan::Subquery { .. } | Scan::RecursiveCteInput,
        )
        | Operation::HashJoin(_) => {}
    }
    Ok(())
}

fn collect_table_references_row_dependency(
    tables: &TableReferences,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    for table in tables.joined_tables() {
        collect_operation_row_dependency(&table.op, source, dependency)?;
        collect_source_read_programs_row_dependency(&table.read_programs, source, dependency)?;
        collect_exprs_row_dependency(
            table
                .check_constraints
                .iter()
                .map(|check| &check.expression),
            source,
            dependency,
        )?;
        for pattern in &table.index_method_patterns {
            collect_exprs_row_dependency(
                pattern.outputs.iter().map(|output| &output.expr),
                source,
                dependency,
            )?;
            if let Some(predicate) = &pattern.predicate {
                collect_expr_row_dependency(predicate, source, dependency)?;
            }
            collect_order_terms_row_dependency(&pattern.order_by, source, dependency)?;
            collect_exprs_row_dependency(
                pattern.limit.iter().chain(pattern.offset.iter()),
                source,
                dependency,
            )?;
        }
        for expressions in &table.index_expressions {
            collect_exprs_row_dependency(
                expressions.columns.iter().filter_map(Option::as_ref),
                source,
                dependency,
            )?;
            if let Some(predicate) = &expressions.predicate {
                collect_expr_row_dependency(predicate, source, dependency)?;
            }
        }
        collect_exprs_row_dependency(
            table
                .expression_index_usages
                .iter()
                .map(|usage| &usage.expr),
            source,
            dependency,
        )?;
        if let Table::FromClauseSubquery(subquery) = &table.table {
            collect_plan_row_dependency(&subquery.plan, source, dependency)?;
        }
    }
    for table in tables.outer_query_refs() {
        collect_source_read_programs_row_dependency(&table.read_programs, source, dependency)?;
    }
    Ok(())
}

fn collect_source_read_programs_row_dependency(
    programs: &SourceReadPrograms,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    collect_exprs_row_dependency(
        programs
            .generated_expressions
            .iter()
            .chain(&programs.default_expressions)
            .filter_map(Option::as_ref),
        source,
        dependency,
    )?;
    for column_programs in programs.column_type_programs.iter().flatten() {
        for call in column_programs.encode.iter().chain(&column_programs.decode) {
            collect_exprs_row_dependency(&call.arguments, source, dependency)?;
        }
    }
    Ok(())
}

fn collect_frame_boundary_row_dependency(
    boundary: &FrameBoundary,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    match boundary {
        FrameBoundary::Preceding(expr) | FrameBoundary::Following(expr) => {
            collect_expr_row_dependency(expr, source, dependency)
        }
        FrameBoundary::UnboundedPreceding
        | FrameBoundary::CurrentRow
        | FrameBoundary::UnboundedFollowing => Ok(()),
    }
}

fn collect_select_row_dependency(
    plan: &SelectPlan,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    collect_table_references_row_dependency(&plan.table_references, source, dependency)?;
    collect_exprs_row_dependency(
        plan.result_columns.iter().map(|column| &column.expr),
        source,
        dependency,
    )?;
    collect_exprs_row_dependency(
        plan.where_clause.iter().map(|term| &term.expr),
        source,
        dependency,
    )?;
    if let Some(group_by) = &plan.group_by {
        collect_exprs_row_dependency(&group_by.exprs, source, dependency)?;
        if let Some(having) = &group_by.having {
            collect_exprs_row_dependency(having, source, dependency)?;
        }
    }
    collect_order_terms_row_dependency(&plan.order_by, source, dependency)?;
    for aggregate in &plan.aggregates {
        collect_exprs_row_dependency(&aggregate.args, source, dependency)?;
        collect_expr_row_dependency(&aggregate.original_expr, source, dependency)?;
        if let Some(filter) = &aggregate.filter_expr {
            collect_expr_row_dependency(filter, source, dependency)?;
        }
    }
    collect_exprs_row_dependency(
        plan.limit.iter().chain(plan.offset.iter()),
        source,
        dependency,
    )?;
    for row in &plan.values {
        collect_exprs_row_dependency(row, source, dependency)?;
    }
    if let Some(window) = &plan.window {
        collect_exprs_row_dependency(&window.partition_by, source, dependency)?;
        collect_exprs_row_dependency(
            window.order_by.iter().map(|(expr, _, _)| expr),
            source,
            dependency,
        )?;
        collect_frame_boundary_row_dependency(&window.frame.start, source, dependency)?;
        collect_frame_boundary_row_dependency(&window.frame.end, source, dependency)?;
        for function in &window.functions {
            collect_expr_row_dependency(&function.original_expr, source, dependency)?;
            if let Some(rewritten) = &function.rewritten {
                collect_expr_row_dependency(&rewritten.expr, source, dependency)?;
                if let Some(filter) = &rewritten.filter_expr {
                    collect_expr_row_dependency(filter, source, dependency)?;
                }
            }
        }
    }
    if let Some(SimpleAggregate::MinMax(definition)) = &plan.simple_aggregate {
        collect_expr_row_dependency(&definition.argument, source, dependency)?;
    }
    for subquery in &plan.non_from_clause_subqueries {
        if let SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &subquery.state
        {
            collect_plan_row_dependency(subquery_plan, source, dependency)?;
        }
    }
    Ok(())
}

fn collect_plan_row_dependency(
    plan: &Plan,
    source: PlanSourceId,
    dependency: &mut PlanRowDependency,
) -> Result<()> {
    match plan {
        Plan::Select(select) => collect_select_row_dependency(select, source, dependency),
        Plan::CompoundSelect {
            left,
            right_most,
            limit,
            offset,
            order_by,
        } => {
            for (select, _) in left {
                collect_select_row_dependency(select, source, dependency)?;
            }
            collect_select_row_dependency(right_most, source, dependency)?;
            collect_exprs_row_dependency(limit.iter().chain(offset.iter()), source, dependency)?;
            collect_order_terms_row_dependency(order_by, source, dependency)
        }
        Plan::RecursiveCte(recursive) => {
            collect_plan_row_dependency(&recursive.initial_query, source, dependency)?;
            collect_plan_row_dependency(&recursive.recursive_query, source, dependency)?;
            collect_exprs_row_dependency(
                recursive.limit.iter().chain(recursive.offset.iter()),
                source,
                dependency,
            )
        }
        Plan::Delete(delete) => {
            collect_table_references_row_dependency(&delete.table_references, source, dependency)?;
            collect_exprs_row_dependency(
                delete.result_columns.iter().map(|column| &column.expr),
                source,
                dependency,
            )?;
            collect_exprs_row_dependency(
                delete.where_clause.iter().map(|term| &term.expr),
                source,
                dependency,
            )?;
            collect_order_terms_row_dependency(&delete.order_by, source, dependency)?;
            collect_exprs_row_dependency(
                delete.limit.iter().chain(delete.offset.iter()),
                source,
                dependency,
            )?;
            if let Some(rowset) = &delete.rowset_plan {
                collect_select_row_dependency(rowset, source, dependency)?;
            }
            for subquery in &delete.non_from_clause_subqueries {
                if let SubqueryState::Unevaluated {
                    plan: Some(subquery_plan),
                } = &subquery.state
                {
                    collect_plan_row_dependency(subquery_plan, source, dependency)?;
                }
            }
            Ok(())
        }
        Plan::Update(update) => {
            collect_operation_row_dependency(&update.target_table.op, source, dependency)?;
            collect_table_references_row_dependency(&update.from_tables, source, dependency)?;
            for set in &update.set_clauses {
                collect_expr_row_dependency(&set.expr, source, dependency)?;
                if let Some(rewritten) = &set.update_from_result {
                    collect_expr_row_dependency(rewritten, source, dependency)?;
                }
            }
            collect_exprs_row_dependency(
                update.defaults.iter().map(|(_, expr)| expr),
                source,
                dependency,
            )?;
            collect_exprs_row_dependency(
                update.where_clause.iter().map(|term| &term.expr),
                source,
                dependency,
            )?;
            collect_exprs_row_dependency(
                update.limit.iter().chain(update.offset.iter()),
                source,
                dependency,
            )?;
            if let Some(returning) = &update.returning {
                collect_exprs_row_dependency(
                    returning.iter().map(|column| &column.expr),
                    source,
                    dependency,
                )?;
            }
            if let Some(write_set) = &update.write_set_plan {
                collect_select_row_dependency(&write_set.select, source, dependency)?;
            }
            for subquery in &update.non_from_clause_subqueries {
                if let SubqueryState::Unevaluated {
                    plan: Some(subquery_plan),
                } = &subquery.state
                {
                    collect_plan_row_dependency(subquery_plan, source, dependency)?;
                }
            }
            Ok(())
        }
    }
}

/// The destination of the results of a query.
/// Typically, the results of a query are returned to the caller.
/// However, there are some cases where the results are not returned to the caller,
/// but rather are yielded to a parent query via coroutine, or stored in a temp table,
/// later used by the parent query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EphemeralRowidMode {
    /// The last result column is used as the rowid key.
    FromResultColumns,
    /// Generate a fresh rowid for each inserted row.
    Auto,
}

#[derive(Debug, Clone)]
pub enum QueryDestination {
    /// The results of the query are returned to the caller.
    ResultRows,
    /// The results of the query are yielded to a parent query via coroutine.
    CoroutineYield {
        /// The register that holds the program offset that handles jumping to/from the coroutine.
        yield_reg: usize,
        /// The index of the first instruction in the bytecode that implements the coroutine.
        coroutine_implementation_start: BranchOffset,
    },
    /// The results of the query are stored in an ephemeral index,
    /// later used by the parent query.
    EphemeralIndex {
        /// The cursor ID of the ephemeral index that will be used to store the results.
        cursor_id: CursorID,
        /// The index that will be used to store the results.
        index: Arc<Index>,
        /// Optional MakeRecord affinity string to apply before inserting keys.
        /// For `IN (SELECT ...)` this must match the left-hand side expression affinity.
        affinity_str: Option<Arc<String>>,
        /// Whether this is a delete operation that will remove the index entries
        is_delete: bool,
    },
    /// The results of the query are stored in an ephemeral table,
    /// later used by the parent query.
    EphemeralTable {
        /// The cursor ID of the ephemeral table that will be used to store the results.
        cursor_id: CursorID,
        /// The table that will be used to store the results.
        table: Arc<BTreeTable>,
        /// How to determine the rowid key for inserts.
        rowid_mode: EphemeralRowidMode,
    },
    /// Insert rows produced by a recursive CTE into its work queue.
    RecursiveCteQueue {
        cursor_id: CursorID,
        index: Arc<Index>,
        /// Result columns that determine which queued row is read next.
        sort_keys: alloc::Vec<RecursiveCteQueueKey>,
        /// Index of rows already produced by a recursive `UNION`.
        seen_rows: Option<(CursorID, Arc<Index>)>,
    },
    /// The result of an EXISTS subquery are stored in a single register.
    ExistsSubqueryResult {
        /// The register that holds the result of the EXISTS subquery.
        result_reg: usize,
    },
    /// The results of a subquery that is neither 'EXISTS' nor 'IN' are stored in a range of registers.
    RowValueSubqueryResult {
        /// The start register of the range that holds the result of the subquery.
        result_reg_start: usize,
        /// The number of registers that hold the result of the subquery.
        num_regs: usize,
    },
    /// The results of the query are stored in a RowSet (for DELETE operations with triggers).
    /// Rowids are added to the RowSet using RowSetAdd, then read back using RowSetRead.
    RowSet {
        /// The register that holds the RowSet object.
        rowset_reg: usize,
    },
    /// Decision made at some point after query plan construction.
    Unset,
}

#[derive(Debug, Clone, Copy)]
/// One result column used to order the recursive CTE work queue.
pub struct RecursiveCteQueueKey {
    pub result_column_index: usize,
    /// `None` when the index sort order already puts NULLs in the requested
    /// position.
    pub nulls_override: Option<ast::NullsOrder>,
}

#[derive(Debug, Clone)]
/// One priority expression resolved to a canonical recursive output column.
pub struct RecursiveCteOrderTerm {
    pub result_column_index: usize,
    pub order: SortOrder,
    pub nulls: Option<ast::NullsOrder>,
    pub explicit_collation: Option<ResolvedCollation>,
}

impl QueryDestination {
    pub fn placeholder_for_subquery() -> Self {
        QueryDestination::CoroutineYield {
            yield_reg: usize::MAX, // will be set later in bytecode emission
            coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JoinOrderMember {
    /// The internal ID of the[TableReference]
    pub table_id: PlanSourceId,
    /// The index of the table in the original join order.
    /// This is used to index into e.g. [TableReferences::joined_tables()]
    pub original_idx: usize,
    /// Whether this member is the right side of an OUTER JOIN
    pub is_outer: bool,
}

#[derive(Debug, Clone, PartialEq)]

/// Whether a column is DISTINCT or not.
pub enum Distinctness {
    /// The column is not a DISTINCT column.
    NonDistinct,
    /// The column is a DISTINCT column,
    /// and includes a translation context for handling duplicates.
    Distinct { ctx: Option<DistinctCtx> },
}

impl Distinctness {
    pub fn from_ast(distinctness: Option<&ast::Distinctness>) -> Self {
        match distinctness {
            Some(ast::Distinctness::Distinct) => Self::Distinct { ctx: None },
            Some(ast::Distinctness::All) => Self::NonDistinct,
            None => Self::NonDistinct,
        }
    }
    pub fn is_distinct(&self) -> bool {
        matches!(self, Distinctness::Distinct { .. })
    }
}

/// Translation context for handling DISTINCT columns.
#[derive(Debug, Clone, PartialEq)]
pub struct DistinctCtx {
    /// Hash table id used to deduplicate results.
    pub hash_table_id: usize,
    /// Collations for each distinct key column.
    pub collations: Vec<CollationSeq>,
    /// The label for the on conflict branch.
    /// When a duplicate is found, the program will jump to the offset this label points to.
    pub label_on_conflict: BranchOffset,
}

impl DistinctCtx {
    pub fn emit_deduplication_insns(
        &self,
        program: &mut ProgramBuilder,
        num_regs: usize,
        start_reg: usize,
    ) {
        program.emit_insn(Insn::HashDistinct {
            data: Box::new(HashDistinctData {
                hash_table_id: self.hash_table_id,
                key_start_reg: start_reg,
                num_keys: num_regs,
                collations: self.collations.clone(),
                target_pc: self.label_on_conflict,
            }),
        });
    }
}

/// Detected simple-aggregate optimization.
///
/// Analogous to SQLite's `isSimpleCount()` / `minMaxQuery()`. When set on a
/// `SelectPlan`, the emitter can use a specialised fast path instead of a full
/// scan + accumulate loop.
#[derive(Debug, Clone)]
pub struct MinMaxDef {
    pub func: AggFunc,
    pub argument: Expr,
    pub order: SortOrder,
    /// Explicit COLLATE override, if any. `None` means use the column default.
    pub collation: Option<CollationSeq>,
}

#[derive(Debug, Clone)]
pub enum SimpleAggregate {
    /// `SELECT count(*) FROM <tbl>` — uses the `Insn::Count` opcode directly.
    Count,
    /// `SELECT min(expr) FROM …` or `SELECT max(expr) FROM …` — the optimizer
    /// will pick an index that delivers rows in the right order so the emitter
    /// only needs to read the first (non-NULL for MIN) row.
    MinMax(Box<MinMaxDef>),
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub table_references: TableReferences,
    /// The order in which the tables are joined. Tables have usize Ids (their index in joined_tables)
    pub join_order: Vec<JoinOrderMember>,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries. all join conditions also get shoved in here,
    /// and we keep track of which join they came from (mainly for OUTER JOIN processing)
    pub where_clause: Vec<WhereTerm>,
    /// group by clause
    pub group_by: Option<GroupBy>,
    /// order by clause
    pub order_by: Vec<PlanOrderTerm>,
    /// all the aggregates collected from the result columns, order by, and (TODO) having clauses
    pub aggregates: Vec<Aggregate>,
    /// limit clause
    pub limit: Option<Expr>,
    /// offset clause
    pub offset: Option<Expr>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
    /// the destination of the resulting rows from this plan.
    pub query_destination: QueryDestination,
    /// whether the query is DISTINCT
    pub distinctness: Distinctness,
    /// values: https://sqlite.org/syntax/select-core.html
    pub values: Vec<Vec<Expr>>,
    /// The window definition and all window functions associated with it. There is at most one
    /// window per SELECT. If the original query contains more, they are pushed down into subqueries.
    pub window: Option<Window>,
    /// Subqueries that appear in any part of the query apart from the FROM clause
    pub non_from_clause_subqueries: Vec<NonFromClauseSubquery>,
    /// Estimated number of times this SELECT will be invoked by its parent scope.
    ///
    /// Top-level queries and standalone FROM-subqueries default to 1. Correlated
    /// non-FROM subqueries may be re-optimized after their parent join order is
    /// known so their inner FROM-subqueries can cost repeated probes correctly.
    pub input_cardinality_hint: Option<f64>,
    /// Estimated output rows from the optimizer's join order computation.
    /// Used to propagate cardinality estimates for CTE/subquery tables.
    pub estimated_output_rows: Option<f64>,
    /// When set, this query is a simple aggregate (COUNT(*), MIN, or MAX)
    /// that can be satisfied without a full table scan.
    pub simple_aggregate: Option<SimpleAggregate>,
    /// Parameters from EXISTS subquery result columns that were dropped during
    /// semi/anti-join unnesting. These need to be registered in the program's
    /// parameter list even though no code is emitted for them, so that final
    /// program validation (`has_slot`) succeeds.
    pub phantom_params: Vec<PlanParameter>,
}

impl SelectPlan {
    pub fn joined_tables(&self) -> &[JoinedTable] {
        self.table_references.joined_tables()
    }

    pub fn agg_args_count(&self) -> usize {
        self.aggregates.iter().map(|agg| agg.args.len()).sum()
    }

    /// Whether this query or any of its subqueries reference columns from the outer query.
    pub fn is_correlated(&self) -> bool {
        self.table_references
            .outer_query_refs()
            .iter()
            .any(|t| t.is_used())
            || !self.table_references.outer_outputs().is_empty()
            || self.non_from_clause_subqueries.iter().any(|s| s.correlated)
            || self
                .table_references
                .joined_tables()
                .iter()
                .any(|t| match &t.table {
                    Table::FromClauseSubquery(subquery) => plan_is_correlated(&subquery.plan),
                    _ => false,
                })
    }

    fn reads_table(&self, database_id: usize, table_name: &str) -> bool {
        self.table_references.joined_tables().iter().any(|table| {
            table.matches(database_id, table_name)
                || match &table.table {
                    Table::FromClauseSubquery(subquery) => {
                        subquery.plan.reads_table(database_id, table_name)
                    }
                    Table::BTree(_) | Table::Virtual(_) | Table::RecursiveCteInput(_) => false,
                }
        }) || self
            .non_from_clause_subqueries
            .iter()
            .any(|subquery| subquery.reads_table(database_id, table_name))
    }
}

impl SelectPlan {
    /// Collect every column and rowid this SELECT reads from `source`, including
    /// reads made by its still-unemitted nested queries.
    pub fn source_row_dependency(&self, source: PlanSourceId) -> Result<PlanRowDependency> {
        let mut dependency = PlanRowDependency::default();
        collect_select_row_dependency(self, source, &mut dependency)?;
        Ok(dependency)
    }

    fn output_fact(&self, output: PlanOutputId) -> Option<PlanOutputFact> {
        self.result_columns
            .iter()
            .find(|column| column.id == output)
            .map(PlanOutputFact::from)
            .or_else(|| {
                self.table_references
                    .outer_output(output)
                    .map(|reference| reference.fact.clone())
            })
    }

    fn subquery_output_fact(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<&PlanOutputFact> {
        self.non_from_clause_subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)?
            .output_facts
            .get(output)
    }
}

impl PlanExprFactSource for SelectPlan {
    fn output_type_fact(&self, output: PlanOutputId) -> Option<TypeFact> {
        self.output_fact(output).map(|fact| fact.type_fact)
    }

    fn subquery_output_type_fact(&self, query: PlanSubqueryId, output: usize) -> Option<TypeFact> {
        self.subquery_output_fact(query, output)
            .map(|fact| fact.type_fact.clone())
    }

    fn subquery_width(&self, query: PlanSubqueryId) -> Option<usize> {
        self.non_from_clause_subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)
            .map(|subquery| subquery.output_facts.len())
    }

    fn output_affinity(&self, output: PlanOutputId) -> Option<PlanExprAffinity> {
        self.output_fact(output).map(|fact| fact.affinity)
    }

    fn subquery_output_affinity(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<PlanExprAffinity> {
        self.subquery_output_fact(query, output)
            .map(|fact| fact.affinity)
    }

    fn output_collation(&self, output: PlanOutputId) -> Option<CollationSeq> {
        self.output_fact(output)
            .and_then(|fact| fact.collation.map(|collation| *collation.value()))
    }

    fn subquery_output_collation(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<CollationSeq> {
        self.subquery_output_fact(query, output)
            .and_then(|fact| fact.collation.as_ref().map(|collation| *collation.value()))
    }
}

/// Why an UPDATE/DELETE must gather target rowids first, then apply writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlSafetyReason {
    /// UPDATE ... FROM computes writes from the materialized result of the FROM clause.
    UpdateFrom,
    /// Triggers exist, so we lock in target rows before writing.
    Trigger,
    /// WHERE has a subquery, so we lock in target rows before writing.
    SubqueryInWhere,
    /// The plan reads rowids from multiple index branches (multi-index scan).
    MultiIndexScan,
    /// REPLACE may delete conflicting rows while we are scanning.
    ReplaceMode,
    /// The statement updates key columns used by the scan itself.
    KeyMutation,
    /// The index method cursor does not materialize results up front,
    /// so writes could invalidate the live iterator.
    IndexMethodNotMaterialized,
    /// The UPDATE changes a column referenced by an FK with a cascading
    /// action (CASCADE / SET NULL / SET DEFAULT). The cascade can fire
    /// triggers on referencing tables that write back to the target,
    /// which would invalidate the live scan iterator.
    FkCascade,
}

/// Safety decisions made while planning UPDATE/DELETE.
#[derive(Debug, Clone, Default)]
pub struct DmlSafety {
    /// Why the safer "collect first, write later" mode was enabled.
    pub reasons: SmallVec<[DmlSafetyReason; 2]>,
}

impl DmlSafety {
    pub fn requires_stable_write_set(&self) -> bool {
        !self.reasons.is_empty()
    }

    pub fn require(&mut self, reason: DmlSafetyReason) {
        if !self.reasons.contains(&reason) {
            self.reasons.push(reason);
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table_references: TableReferences,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries.
    pub where_clause: Vec<WhereTerm>,
    /// order by clause
    pub order_by: Vec<PlanOrderTerm>,
    /// limit clause
    pub limit: Option<Expr>,
    /// offset clause
    pub offset: Option<Expr>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
    /// Indexes that must be updated by the delete operation.
    pub indexes: Vec<ResolvedIndex>,
    /// When DELETE cannot safely write while scanning, we first collect rowids into a RowSet.
    pub rowset_plan: Option<SelectPlan>,
    /// Register ID for the RowSet (if rowset_plan is Some)
    pub rowset_reg: Option<usize>,
    /// Subqueries that appear in the WHERE clause (for non-rowset path)
    pub non_from_clause_subqueries: Vec<NonFromClauseSubquery>,
    /// Whether this DELETE plan uses the safer pre-materialization path, and why.
    pub safety: DmlSafety,
}

#[derive(Debug, Clone)]
pub struct UpdateSetClause {
    pub column_index: usize,
    /// Original user-visible SET expression.
    pub expr: Expr,
    /// In UPDATE FROM, SET clause expressions are rewritten to read from the
    /// scratch table populated before the write loop.
    ///
    /// For example, `UPDATE t SET a = s.x + 1 FROM s WHERE t.id = s.id` rewrites
    /// the SET expression `s.x + 1` (a column reference into the FROM table + a literal 1) into a
    /// `Column` read from the ephemeral scratch table that was populated during
    /// the collection phase. That column in the scratch table contains the evaluated result
    /// of s.x + 1.
    pub update_from_result: Option<Expr>,
}

impl UpdateSetClause {
    pub fn new(column_index: usize, expr: Expr) -> Self {
        Self {
            column_index,
            expr,
            update_from_result: None,
        }
    }

    /// If UPDATE ... FROM, the this is the materialized result of a SET clause expression derived from the FROM clause;
    /// otherwise, it is the original expression.
    pub fn emitted_expr(&self) -> &Expr {
        self.update_from_result.as_ref().unwrap_or(&self.expr)
    }
}

/// The SELECT plan that is used for either a) UPDATE...FROM or b) a normal UPDATE where the write set must be prematerialized;
/// see [crate::translate::plan::DmlSafety].
#[derive(Debug, Clone)]
pub struct WriteSetPlan {
    pub select: SelectPlan,
    pub scratch_table_id: PlanSourceId,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    /// The table whose rows this UPDATE mutates.
    pub target_table: JoinedTable,
    /// The read-side FROM graph for `UPDATE ... FROM`.
    ///
    /// Plain UPDATE statements keep this empty except for any outer-query
    /// references (for example preplanned CTE definitions) that are still needed
    /// when emitting planned subqueries later in the pipeline.
    pub from_tables: TableReferences,
    /// Conflict resolution strategy (e.g., OR IGNORE, OR REPLACE)
    pub or_conflict: Option<ResolveType>,
    /// SET clause assignments
    pub set_clauses: Vec<UpdateSetClause>,
    /// Target-column defaults instantiated during semantic analysis, used by
    /// conflict handling without reopening stored schema expressions.
    pub defaults: Vec<(usize, Expr)>,
    pub where_clause: Vec<WhereTerm>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    /// Optional RETURNING clause.
    pub returning: Option<Vec<ResultSetColumn>>,
    /// Whether the WHERE clause is always false.
    pub contains_constant_false_condition: bool,
    pub indexes_to_update: Vec<ResolvedIndex>,
    /// Prebuilt write-set SELECT for Halloween protection / UPDATE FROM.
    pub write_set_plan: Option<WriteSetPlan>,
    /// For ALTER TABLE turso-db emits appropriate DDL statement in the "updates"
    /// cell of CDC table. This field is present only for update plans created for
    /// ALTER TABLE when CDC mode has "updates" values.
    pub cdc_update_alter_statement: Option<String>,
    /// Subqueries that appear in the WHERE clause (for non-ephemeral path)
    pub non_from_clause_subqueries: Vec<NonFromClauseSubquery>,
    /// Whether this UPDATE plan uses the safer pre-materialization path, and why.
    pub safety: DmlSafety,
}

impl UpdatePlan {
    /// Build the target-only scope used while choosing an UPDATE access path.
    /// Runtime rows such as trigger NEW/OLD remain visible even though they do
    /// not own a FROM cursor.
    pub fn build_target_scope_tables(&self) -> TableReferences {
        let mut target_tables = TableReferences::new(
            vec![self.target_table.clone()],
            self.from_tables.outer_query_refs().to_vec(),
        );
        target_tables.extend_runtime_sources_from(&self.from_tables);
        target_tables
    }

    /// Combine the UPDATE target (always first) and the `FROM`-clause tables
    /// into one `TableReferences` — the read-side scope used for planning
    /// outer-`WHERE` subqueries, `EXPLAIN QUERY PLAN`, and rendering the plan
    /// back to SQL text via `ToTokens`.
    /// The plan stores the two separately because the write-side emitter
    /// treats the target table specially; this helper rejoins them for readers.
    pub fn build_read_scope_tables(&self) -> TableReferences {
        let mut read_scope_tables = TableReferences::new(vec![self.target_table.clone()], vec![]);
        if self.from_tables.right_join_swapped() {
            read_scope_tables.set_right_join_swapped();
        }
        read_scope_tables.extend(self.from_tables.clone());
        read_scope_tables
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
}

/// The type of join between two tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    FullOuter,
    /// Semi-join: keep outer row if inner match found (EXISTS).
    Semi,
    /// Anti-join: keep outer row if NO inner match found (NOT EXISTS).
    Anti,
}

/// Join information for a table reference.
#[derive(Debug, Clone)]
pub struct JoinInfo {
    /// The type of join.
    pub join_type: JoinType,
    /// The USING clause for the join, if any. NATURAL JOIN is transformed into USING (col1, col2, ...).
    pub using: Vec<ast::Name>,
    /// When true, the optimizer must not reorder this table relative to its
    /// neighbors. Set for CROSS JOIN to match SQLite semantics.
    pub no_reorder: bool,
}

impl JoinInfo {
    /// Whether this is an OUTER JOIN (LEFT OUTER or FULL OUTER).
    pub fn is_outer(&self) -> bool {
        matches!(self.join_type, JoinType::LeftOuter | JoinType::FullOuter)
    }

    /// Whether this is a FULL OUTER JOIN.
    pub fn is_full_outer(&self) -> bool {
        self.join_type == JoinType::FullOuter
    }

    /// Whether this is a semi-join (EXISTS).
    pub fn is_semi(&self) -> bool {
        self.join_type == JoinType::Semi
    }

    /// Whether this is an anti-join (NOT EXISTS).
    pub fn is_anti(&self) -> bool {
        self.join_type == JoinType::Anti
    }

    /// Whether this is a semi-join or anti-join (EXISTS/NOT EXISTS).
    pub fn is_semi_or_anti(&self) -> bool {
        matches!(self.join_type, JoinType::Semi | JoinType::Anti)
    }

    /// Whether the optimizer must preserve this table's position in the join order.
    pub fn is_ordering_constrained(&self) -> bool {
        self.is_outer() || self.is_semi_or_anti() || self.no_reorder
    }
}

/// Source-local programs needed whenever a logical column is read. The same
/// allocation is shared by a joined table and every correlated outer
/// reference derived from it.
#[derive(Debug, Clone)]
pub struct SourceReadPrograms {
    pub generated_expressions: Vec<Option<Expr>>,
    pub default_expressions: Vec<Option<Expr>>,
    pub column_type_programs: Vec<Option<PlanColumnTypePrograms>>,
}

impl SourceReadPrograms {
    pub fn none(column_count: usize) -> Self {
        Self {
            generated_expressions: vec![None; column_count],
            default_expressions: vec![None; column_count],
            column_type_programs: vec![None; column_count],
        }
    }
}

/// A joined table in the query plan.
/// For example,
/// ```sql
/// SELECT * FROM users u JOIN products p JOIN (SELECT * FROM users) sub;
/// ```
/// has three table references where
/// - all have [Operation::Scan]
/// - identifiers are `t`, `p`, `sub`
/// - `t` and `p` are [Table::BTree] while `sub` is [Table::FromClauseSubquery]
/// - join_info is None for the first table reference, and Some(JoinInfo { join_type: JoinType::Inner, using: vec![] }) for the second and third table references
#[derive(Debug, Clone)]
pub struct JoinedTable {
    /// The operation that this table reference performs.
    pub op: Operation,
    /// Table object, which contains metadata about the table, e.g. columns.
    pub table: Table,
    /// Catalog identity retained for real table sources. Synthetic subquery,
    /// CTE, and scratch tables have no catalog object.
    pub resolved_table: Option<ResolvedTable>,
    /// The name of the table as referred to in the query, either the literal name or an alias e.g. "users" or "u"
    pub identifier: String,
    /// Plan identity used by [`Expr::Column`] to refer to this source.
    pub internal_id: PlanSourceId,
    /// The join info for this table reference, if it is the right side of a join (which all except the first table reference have)
    pub join_info: Option<JoinInfo>,
    /// Bitmask of columns that are referenced in the query.
    /// Used to decide whether a covering index can be used.
    pub col_used_mask: ColumnUsedMask,
    /// Count of how many times each column is referenced.
    ///
    /// Expression indexes can satisfy a column requirement if the column is
    /// only used to build the expression itself. Tracking counts lets us
    /// subtract a column from the covering set only when every usage is
    /// accounted for by an expression index.
    pub column_use_counts: Vec<usize>,
    /// Expressions referencing this table that may be satisfied by an expression index.
    ///
    /// Each entry stores the planned query expression and the columns it
    /// needs. During covering checks we ask: does an index contain this
    /// expression? If yes, all columns that *only* feed this expression can be
    /// removed from the required-column set.
    pub expression_index_usages: Vec<ExpressionIndexUsage>,
    /// The index of the database. "main" is always zero.
    pub database_id: usize,
    /// INDEXED BY / NOT INDEXED hint from the SQL statement.
    pub index_hint: PlanIndexHint,
    /// Custom index-method patterns resolved to this source identity.
    /// The optimizer only matches these planned expressions; it never resolves
    /// names from an index method's raw SQL pattern.
    pub index_method_patterns: Vec<PlanIndexMethodPattern>,
    /// Schema index expressions instantiated for this source during semantic analysis.
    pub index_expressions: Vec<PlanIndexExpressions>,
    /// Generated/default/custom-type programs shared with correlated reads of
    /// this same source identity.
    pub read_programs: Arc<SourceReadPrograms>,
    /// CHECK constraints resolved and lowered for this exact DML target.
    pub check_constraints: Vec<PlanCheckConstraint>,
}

#[derive(Debug, Clone)]
pub enum PlanIndexHint {
    None,
    NotIndexed,
    Indexed(ResolvedIndex),
}

/// An index selected by physical planning. Catalog indexes retain their
/// semantic identity and snapshot; planner-created ephemeral indexes are
/// explicitly distinguished instead of masquerading as catalog objects.
#[derive(Debug, Clone)]
pub enum PlanIndex {
    Catalog(ResolvedIndex),
    Ephemeral(Arc<Index>),
}

impl PlanIndex {
    pub fn value(&self) -> &Index {
        match self {
            Self::Catalog(index) => index.value(),
            Self::Ephemeral(index) => index,
        }
    }

    pub fn handle(&self) -> Arc<Index> {
        match self {
            Self::Catalog(index) => index.handle(),
            Self::Ephemeral(index) => index.clone(),
        }
    }

    pub fn resolved(&self) -> Option<&ResolvedIndex> {
        match self {
            Self::Catalog(index) => Some(index),
            Self::Ephemeral(_) => None,
        }
    }
}

impl std::ops::Deref for PlanIndex {
    type Target = Index;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl AsRef<Index> for PlanIndex {
    fn as_ref(&self) -> &Index {
        self.value()
    }
}

impl Default for PlanIndexHint {
    fn default() -> Self {
        Self::None
    }
}

/// A custom index-method query pattern lowered from Semantic HIR.
#[derive(Debug, Clone)]
pub struct PlanIndexMethodPattern {
    pub index: ResolvedIndex,
    pub pattern_idx: usize,
    pub outputs: Vec<ResultSetColumn>,
    pub predicate: Option<Expr>,
    pub order_by: Vec<PlanOrderTerm>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct PlanIndexExpressions {
    pub index: ResolvedIndex,
    pub columns: Vec<Option<Expr>>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct PlanCheckConstraint {
    pub expression: Expr,
    pub description: String,
}

impl JoinedTable {
    pub fn using_dedup_hidden_cols(&self) -> Result<ColumnMask> {
        let Some(join_info) = self.join_info.as_ref() else {
            return Ok(ColumnMask::default());
        };
        let col_mask = self
            .table
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                let col_name = col.name.as_deref()?;
                join_info
                    .using
                    .iter()
                    .any(|using_col| using_col.as_str().eq_ignore_ascii_case(col_name))
                    .then_some(idx)
            })
            .try_collect()?;
        Ok(col_mask)
    }
}

#[derive(Debug, Clone)]
pub struct OuterQueryReference {
    /// The name of the table as referred to in the query, either the literal name or an alias e.g. "users" or "u"
    pub identifier: String,
    /// Plan identity used by [`Expr::Column`] to refer to this source.
    pub internal_id: PlanSourceId,
    /// Table object, which contains metadata about the table, e.g. columns.
    pub table: Table,
    /// The exact source read programs owned by the outer source.
    pub read_programs: Arc<SourceReadPrograms>,
    /// Columns hidden by USING/NATURAL deduplication in the outer scope.
    pub using_dedup_hidden_cols: ColumnMask,
    /// Bitmask of columns that are referenced in the query.
    /// Used to track dependencies, so that it can be resolved
    /// when a WHERE clause subquery should be evaluated;
    /// i.e., if the subquery depends on tables T and U,
    /// then both T and U need to be in scope for the subquery to be evaluated.
    pub col_used_mask: ColumnUsedMask,
    /// When true, this entry is only for CTE definition lookup in subquery
    /// FROM clauses, not for column resolution. This is set when the CTE
    /// has been consumed by a FROM clause (with or without an alias), so
    /// column resolution goes through the joined_table instead.
    pub cte_definition_only: bool,
    /// Whether the rowid of this table is referenced. Tracked separately from
    /// col_used_mask because rowid is not a real column and setting a fake
    /// column index in col_used_mask could mislead covering index decisions.
    pub rowid_referenced: bool,
    /// Scope depth for this outer reference. 0 = immediate outer scope,
    /// 1 = grandparent scope, etc. Used to avoid false "ambiguous column"
    /// errors when the same column name exists at different nesting depths.
    pub scope_depth: usize,
}

impl OuterQueryReference {
    /// A definition-only entry: it lets a subquery's FROM clause find a CTE
    /// by name, but its columns are not visible for column resolution.
    pub fn cte_definition_only(
        identifier: String,
        internal_id: PlanSourceId,
        table: Table,
    ) -> Self {
        let read_programs = Arc::new(SourceReadPrograms::none(table.columns().len()));
        Self {
            identifier,
            internal_id,
            table,
            read_programs,
            using_dedup_hidden_cols: ColumnMask::default(),
            col_used_mask: ColumnUsedMask::default(),
            cte_definition_only: true,
            rowid_referenced: false,
            scope_depth: 0,
        }
    }

    /// Returns the columns of the table that this outer query reference refers to.
    pub fn columns(&self) -> &[Column] {
        self.table.columns()
    }

    /// Marks a column as used; used means that the column is referenced in the query.
    pub fn mark_column_used(&mut self, column_index: usize) -> Result<()> {
        self.col_used_mask.set(column_index)?;
        Ok(())
    }

    /// Whether the OuterQueryReference is used by the current query scope.
    /// This is used primarily to determine at what loop depth a subquery should be evaluated.
    pub fn is_used(&self) -> bool {
        !self.col_used_mask.is_empty() || self.rowid_referenced
    }
}

/// How a resolved plan source participates in one expression scope.
///
/// Runtime sources are valid resolved values, but they have no FROM cursor and
/// therefore contribute no bit to optimizer table masks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanSourceScope {
    Joined(usize),
    OuterQuery,
    Runtime,
}

#[derive(Debug, Clone)]
/// A collection of table references in a given SQL statement.
///
/// `TableReferences::joined_tables` is the list of tables that are joined together.
/// Example: SELECT * FROM t JOIN u JOIN v -- the joined tables are t, u and v.
///
/// `TableReferences::outer_query_refs` are references to tables outside the current scope.
/// Example: SELECT * FROM t WHERE EXISTS (SELECT * FROM u WHERE u.foo = t.foo)
/// -- here, 'u' is an outer query reference for the subquery (SELECT * FROM u WHERE u.foo = t.foo),
/// since that query does not declare 't' in its FROM clause.
///
///
/// Typically a query will only have joined tables, but the following may have outer query references:
/// - CTEs that refer to other preceding CTEs
/// - Correlated subqueries, i.e. subqueries that depend on the outer scope
pub struct TableReferences {
    /// Tables that are joined together in this query scope.
    joined_tables: Vec<JoinedTable>,
    /// Tables from outer scopes that are referenced in this query scope.
    outer_query_refs: Vec<OuterQueryReference>,
    /// Resolved cursorless sources whose values are supplied by runtime
    /// bindings, such as trigger NEW/OLD and schema-expression inputs.
    runtime_sources: Vec<PlanSourceId>,
    /// Result values supplied by enclosing query blocks. These are registers,
    /// not table cursors, but their source dependencies still decide when a
    /// correlated child may run.
    outer_outputs: Vec<PlanOuterOutputReference>,
    /// Set when a RIGHT JOIN is rewritten as LEFT JOIN by swapping the two tables,
    /// so `select_star` emits columns in the original user-visible order.
    right_join_swapped: bool,
}

impl Default for TableReferences {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl TableReferences {
    /// The maximum number of tables that can be joined together in a query.
    /// This limit is arbitrary, although we currently use a u128 to represent the [crate::translate::planner::TableMask],
    /// which can represent up to 128 tables.
    /// Even at 63 tables we currently cannot handle the optimization performantly, hence the arbitrary cap.
    pub const MAX_JOINED_TABLES: usize = 63;
    pub const fn new(
        joined_tables: Vec<JoinedTable>,
        outer_query_refs: Vec<OuterQueryReference>,
    ) -> Self {
        Self {
            joined_tables,
            outer_query_refs,
            runtime_sources: Vec::new(),
            outer_outputs: Vec::new(),
            right_join_swapped: false,
        }
    }

    pub const fn new_empty() -> Self {
        Self {
            joined_tables: Vec::new(),
            outer_query_refs: Vec::new(),
            runtime_sources: Vec::new(),
            outer_outputs: Vec::new(),
            right_join_swapped: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.joined_tables.is_empty()
            && self.outer_query_refs.is_empty()
            && self.runtime_sources.is_empty()
    }

    /// Mark that tables were swapped for a RIGHT-to-LEFT JOIN rewrite.
    pub const fn set_right_join_swapped(&mut self) {
        self.right_join_swapped = true;
    }

    /// Whether tables were swapped for a RIGHT JOIN rewrite.
    pub const fn right_join_swapped(&self) -> bool {
        self.right_join_swapped
    }

    /// Add a new [JoinedTable] to the query plan.
    pub fn add_joined_table(&mut self, joined_table: JoinedTable) {
        self.joined_tables.push(joined_table);
    }

    /// Add a new [OuterQueryReference] to the query plan.
    pub fn add_outer_query_reference(&mut self, outer_query_reference: OuterQueryReference) {
        self.outer_query_refs.push(outer_query_reference);
    }

    /// Add one source whose values come from runtime bindings instead of a
    /// table cursor.
    pub(crate) fn add_runtime_source(&mut self, source: PlanSourceId) {
        assert!(
            self.find_table_by_internal_id(source).is_none(),
            "runtime source {source} also appears as a table source"
        );
        if !self.runtime_sources.contains(&source) {
            self.runtime_sources.push(source);
        }
    }

    pub(crate) fn extend_runtime_sources_from(&mut self, other: &Self) {
        for source in &other.runtime_sources {
            self.add_runtime_source(*source);
        }
    }

    /// Register an enclosing output that is valid in this expression scope.
    pub(crate) fn add_outer_output(&mut self, reference: PlanOuterOutputReference) {
        if self
            .outer_outputs
            .iter()
            .any(|candidate| candidate.output == reference.output)
        {
            return;
        }
        self.outer_outputs.push(reference);
        self.outer_outputs.sort_by_key(|reference| reference.output);
    }

    pub(crate) fn outer_outputs(&self) -> &[PlanOuterOutputReference] {
        &self.outer_outputs
    }

    pub(crate) fn outer_output(&self, output: PlanOutputId) -> Option<&PlanOuterOutputReference> {
        self.outer_outputs
            .iter()
            .find(|reference| reference.output == output)
    }

    /// Classify a resolved source in this exact plan scope.
    pub(crate) fn source_scope(&self, source: PlanSourceId) -> Option<PlanSourceScope> {
        self.joined_tables
            .iter()
            .position(|table| table.internal_id == source)
            .map(PlanSourceScope::Joined)
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .any(|table| table.internal_id == source)
                    .then_some(PlanSourceScope::OuterQuery)
            })
            .or_else(|| {
                self.runtime_sources
                    .contains(&source)
                    .then_some(PlanSourceScope::Runtime)
            })
    }

    /// Returns an immutable reference to the [JoinedTable]s in the query plan.
    pub fn joined_tables(&self) -> &[JoinedTable] {
        &self.joined_tables
    }

    /// Returns a mutable reference to the [JoinedTable]s in the query plan.
    pub const fn joined_tables_mut(&mut self) -> &mut Vec<JoinedTable> {
        &mut self.joined_tables
    }

    /// Resets the expression index usages for all joined tables.
    pub fn reset_expression_index_usages(&mut self) {
        for table in self.joined_tables.iter_mut() {
            table.clear_expression_index_usages();
        }
    }

    /// Called before optimization so we can reuse the same registration
    /// for result columns, ORDER BY, and GROUP BY expressions. If a
    /// SELECT lists `LOWER(name)` and an index exists on `LOWER(name)`, we
    /// can plan a covering scan because the expression value lives inside
    /// the index key.
    pub fn register_plan_expr_usage(&mut self, expr: &Expr) -> Result<()> {
        let dependencies = plan_expr_dependencies(expr)?;
        let mut usages = Vec::new();
        walk_plan_expr_dependencies(expr, &mut |candidate, dependencies| {
            if !dependencies.outputs.is_empty() || !dependencies.subqueries.is_empty() {
                return Ok(PlanWalkControl::Continue);
            }

            let mut source = None;
            let mut columns = ColumnUsedMask::default();
            for (candidate_source, use_kind) in &dependencies.source_uses {
                if source.is_some_and(|source| source != *candidate_source) {
                    return Ok(PlanWalkControl::Continue);
                }
                source = Some(*candidate_source);
                match use_kind {
                    PlanColumnUse::Column(column) => columns.set(*column)?,
                    PlanColumnUse::RowId => return Ok(PlanWalkControl::Continue),
                }
            }

            if let Some(source) = source {
                usages.push((source, candidate.clone(), columns));
            }
            Ok(PlanWalkControl::Continue)
        })?;

        for (source, candidate, columns) in usages {
            if let Some(table) = self
                .joined_tables_mut()
                .iter_mut()
                .find(|table| table.internal_id == source)
            {
                table.register_expression_index_usage(candidate, columns);
            }
        }

        for (source, usage) in dependencies.source_uses {
            match self.source_scope(source) {
                Some(PlanSourceScope::Runtime) => continue,
                Some(PlanSourceScope::Joined(_) | PlanSourceScope::OuterQuery) => {}
                None => {
                    return Err(crate::LimboError::InternalError(format!(
                        "expression references plan source {source} outside its query scope"
                    )));
                }
            }
            match usage {
                PlanColumnUse::Column(column) => self.mark_column_used(source, column),
                PlanColumnUse::RowId => self.mark_rowid_referenced(source),
            }
        }
        for output in dependencies.outputs {
            if self.outer_output(output).is_none() {
                return Err(crate::LimboError::InternalError(format!(
                    "expression references plan output {output} outside its query scope"
                )));
            }
        }
        Ok(())
    }

    /// Returns an immutable reference to the [OuterQueryReference]s in the query plan.
    pub fn outer_query_refs(&self) -> &[OuterQueryReference] {
        &self.outer_query_refs
    }

    /// Returns an immutable reference to the [OuterQueryReference] with the given internal ID.
    pub fn find_outer_query_ref_by_internal_id(
        &self,
        internal_id: PlanSourceId,
    ) -> Option<&OuterQueryReference> {
        self.outer_query_refs
            .iter()
            .find(|t| t.internal_id == internal_id)
    }

    /// Returns a mutable reference to the [OuterQueryReference] with the given internal ID.
    pub fn find_outer_query_ref_by_internal_id_mut(
        &mut self,
        internal_id: PlanSourceId,
    ) -> Option<&mut OuterQueryReference> {
        self.outer_query_refs
            .iter_mut()
            .find(|t| t.internal_id == internal_id)
    }

    /// Returns an immutable reference to the [Table] with the given internal ID,
    /// plus a boolean indicating whether the table is a joined table from the current query scope (false),
    /// or an outer query reference (true).
    pub fn find_table_by_internal_id(&self, internal_id: PlanSourceId) -> Option<(bool, &Table)> {
        self.joined_tables
            .iter()
            .find(|t| t.internal_id == internal_id)
            .map(|t| (false, &t.table))
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|t| t.internal_id == internal_id)
                    .map(|t| (true, &t.table))
            })
    }

    /// Return the source-local read programs for either a current-scope table
    /// or a correlated outer reference.
    pub fn find_source_read_programs_by_internal_id(
        &self,
        internal_id: PlanSourceId,
    ) -> Option<&SourceReadPrograms> {
        self.joined_tables
            .iter()
            .find(|table| table.internal_id == internal_id)
            .map(|table| table.read_programs.as_ref())
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|table| table.internal_id == internal_id)
                    .map(|table| table.read_programs.as_ref())
            })
    }

    /// Returns an immutable reference to the [Table] with the given identifier,
    /// where identifier is either the literal name of the table or an alias.
    pub fn find_table_by_identifier(&self, identifier: &str) -> Option<&Table> {
        self.joined_tables
            .iter()
            .find(|t| t.identifier == identifier)
            .map(|t| &t.table)
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|t| t.identifier == identifier)
                    .map(|t| &t.table)
            })
    }

    /// Returns an immutable reference to the first [Table] whose underlying
    /// table name matches `name`. Unlike [find_table_by_identifier], this
    /// searches by the base table name (e.g. "t1") rather than the alias
    /// (e.g. "a"). This is needed when looking up column metadata for
    /// ephemeral auto-indexes, whose `table_name` field stores the base name
    /// while the table reference may be aliased.
    pub fn find_table_by_table_name(&self, name: &str) -> Option<&Table> {
        self.joined_tables
            .iter()
            .find(|t| t.table.get_name() == name)
            .map(|t| &t.table)
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|t| t.table.get_name() == name)
                    .map(|t| &t.table)
            })
    }

    /// Returns an immutable reference to the [OuterQueryReference] with the given identifier,
    /// where identifier is either the literal name of the table or an alias.
    pub fn find_outer_query_ref_by_identifier(
        &self,
        identifier: &str,
    ) -> Option<&OuterQueryReference> {
        self.outer_query_refs
            .iter()
            .find(|t| t.identifier == identifier)
    }

    /// Returns `(internal_id, &Table)` for the table with the given identifier.
    /// Searches `joined_tables` first, then visible `outer_query_refs`
    /// (excluding CTE-definition-only entries).
    pub fn find_table_and_internal_id_by_identifier(
        &self,
        identifier: &str,
    ) -> Option<(PlanSourceId, &Table)> {
        self.joined_tables
            .iter()
            .find(|t| t.identifier == identifier)
            .map(|t| (t.internal_id, &t.table))
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|t| t.identifier == identifier && !t.cte_definition_only)
                    .map(|t| (t.internal_id, &t.table))
            })
    }

    /// Returns an immutable reference to the [JoinedTable] with the given internal ID.
    pub fn find_joined_table_by_internal_id(
        &self,
        internal_id: PlanSourceId,
    ) -> Option<&JoinedTable> {
        self.joined_tables
            .iter()
            .find(|t| t.internal_id == internal_id)
    }

    /// Returns a mutable reference to the [JoinedTable] with the given internal ID.
    pub fn find_joined_table_by_internal_id_mut(
        &mut self,
        internal_id: PlanSourceId,
    ) -> Option<&mut JoinedTable> {
        self.joined_tables
            .iter_mut()
            .find(|t| t.internal_id == internal_id)
    }

    /// Marks a column as used; used means that the column is referenced in the query.
    pub fn mark_column_used(&mut self, internal_id: PlanSourceId, column_index: usize) {
        if let Some(joined_table) = self.find_joined_table_by_internal_id_mut(internal_id) {
            joined_table.mark_column_used(column_index);
        } else if let Some(outer_query_ref) =
            self.find_outer_query_ref_by_internal_id_mut(internal_id)
        {
            outer_query_ref
                .mark_column_used(column_index)
                .expect("TODO: alloc error");
        } else {
            panic!("table with internal id {internal_id} not found in table references");
        }
    }

    /// Marks the rowid of a table as referenced. This is tracked separately
    /// from column usage because rowid is not a real column.
    pub fn mark_rowid_referenced(&mut self, internal_id: PlanSourceId) {
        if let Some(outer_query_ref) = self.find_outer_query_ref_by_internal_id_mut(internal_id) {
            outer_query_ref.rowid_referenced = true;
        }
        // For joined tables, rowid references don't need special tracking
        // since correlated subquery detection only looks at outer_query_refs.
    }

    pub fn contains_table(&self, table: &Table) -> bool {
        self.joined_tables
            .iter()
            .map(|t| &t.table)
            .chain(self.outer_query_refs.iter().map(|t| &t.table))
            .any(|t| match t {
                Table::FromClauseSubquery(subquery_table) => {
                    subquery_table.plan.select_contains_table(table)
                }
                _ => t == table,
            })
    }

    pub fn extend(&mut self, other: TableReferences) {
        fn take_or_append<T>(dst: &mut Vec<T>, mut src: Vec<T>) {
            if dst.is_empty() {
                *dst = src;
            } else if !src.is_empty() {
                dst.append(&mut src);
            }
        }

        let TableReferences {
            joined_tables,
            outer_query_refs,
            runtime_sources,
            outer_outputs,
            right_join_swapped: _,
        } = other;

        // Avoid `Vec::extend` here: `JoinedTable` is large, and many prepare
        // paths append into an empty `TableReferences`. Taking ownership of the
        // source vectors lets us reuse their allocation instead of reallocating
        // and copying every element into a fresh buffer.
        take_or_append(&mut self.joined_tables, joined_tables);
        take_or_append(&mut self.outer_query_refs, outer_query_refs);
        for source in runtime_sources {
            self.add_runtime_source(source);
        }
        for output in outer_outputs {
            self.add_outer_output(output);
        }
    }
}

/// Tracks which columns are used in a query.
pub type ColumnUsedMask = BitSet;

/// ColumnMask wraps [BitSet] and adds a special-case so that it can store [ROWID_SENTINEL]
/// in `O(1)` space
//TODO instead of carrying naked usize's around, we should ideally have a `ColumnID` type alias,
// just like we have `CursorID`, so that we can make [ColumnMask] type-safe.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ColumnMask {
    bitset: BitSet,
    has_rowid_sentinel: bool,
}

impl ColumnMask {
    pub fn set(&mut self, idx: usize) -> Result<(), alloc::TryReserveError> {
        if idx == ROWID_SENTINEL {
            self.has_rowid_sentinel = true;
        } else {
            self.bitset.set(idx)?;
        }
        Ok(())
    }

    pub fn union_with(&mut self, other: &ColumnMask) -> Result<(), alloc::TryReserveError> {
        self.bitset.union_with(&other.bitset)?;
        self.has_rowid_sentinel |= other.has_rowid_sentinel;
        Ok(())
    }

    pub fn get(&self, idx: usize) -> bool {
        if idx == ROWID_SENTINEL {
            self.has_rowid_sentinel
        } else {
            self.bitset.get(idx)
        }
    }

    pub fn count(&self) -> usize {
        self.bitset.count() + self.has_rowid_sentinel as usize
    }

    pub fn is_empty(&self) -> bool {
        self.bitset.is_empty() && !self.has_rowid_sentinel
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        let rowid_sentinel = self.has_rowid_sentinel.then_some(ROWID_SENTINEL);
        self.bitset.iter().chain(rowid_sentinel)
    }
}

impl std::ops::SubAssign<&Self> for ColumnMask {
    fn sub_assign(&mut self, rhs: &Self) {
        self.bitset -= &rhs.bitset;
        self.has_rowid_sentinel &= !rhs.has_rowid_sentinel;
    }
}

impl alloc::TursoFromIterator<usize> for ColumnMask {
    fn try_from_iter<I: IntoIterator<Item = usize>>(
        iter: I,
    ) -> Result<Self, alloc::TryReserveError> {
        let mut mask = ColumnMask::default();
        mask.try_extend(iter)?;
        Ok(mask)
    }

    fn try_extend<I: IntoIterator<Item = usize>>(
        &mut self,
        iter: I,
    ) -> Result<(), alloc::TryReserveError> {
        for idx in iter {
            self.set(idx)?;
        }
        Ok(())
    }
}

pub struct ColumnMaskIter<B: std::borrow::Borrow<BitSet>> {
    inner: BitSetIter<usize, B>,
    pending_rowid: bool,
}

impl<B: std::borrow::Borrow<BitSet>> Iterator for ColumnMaskIter<B> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(v) = self.inner.next() {
            return Some(v);
        }
        if self.pending_rowid {
            self.pending_rowid = false;
            return Some(ROWID_SENTINEL);
        }
        None
    }
}

impl<'a> IntoIterator for &'a ColumnMask {
    type Item = usize;
    type IntoIter = ColumnMaskIter<&'a BitSet>;

    fn into_iter(self) -> Self::IntoIter {
        ColumnMaskIter {
            inner: (&self.bitset).into_iter(),
            pending_rowid: self.has_rowid_sentinel,
        }
    }
}

impl IntoIterator for ColumnMask {
    type Item = usize;
    type IntoIter = ColumnMaskIter<BitSet>;

    fn into_iter(self) -> Self::IntoIter {
        ColumnMaskIter {
            inner: self.bitset.into_iter(),
            pending_rowid: self.has_rowid_sentinel,
        }
    }
}

impl alloc::TryClone for ColumnMask {
    type Error = alloc::TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            bitset: self.bitset.try_clone()?,
            has_rowid_sentinel: self.has_rowid_sentinel,
        })
    }
}

/// Dense bitset optimized for the common case where all elements ≤64, with heap-allocated overflow.
///
/// *WARNING*: This bitset occupies `O(max_num)` space when `max_num > 64`,
/// so it is best used for smaller numbers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BitSet<T = usize> {
    inline: u64,
    /// invariant: `overflow` is `None` iff no bits ≥ 64 are set.
    overflow: Option<alloc::Vec<u64>>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Default for BitSet<T> {
    fn default() -> Self {
        Self {
            inline: 0,
            overflow: None,
            _phantom: PhantomData,
        }
    }
}

impl<T> alloc::TryClone for BitSet<T> {
    type Error = alloc::TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            inline: self.inline,
            overflow: self.overflow.try_clone()?,
            _phantom: PhantomData,
        })
    }
}

/// This iterator, inspired by Kernighan's bit-counting algorighm, is `O(num_words + popcount)`
/// for the whole bitset.
pub struct BitSetIter<T, B: std::borrow::Borrow<BitSet<T>>> {
    bitset: B,
    /// Remaining bits to drain from the word currently pointed at by `word`.
    current: u64,
    /// `0` = inline word, `1..=overflow.len()` = `overflow[word - 1]`.
    word: usize,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: From<usize>, B: std::borrow::Borrow<BitSet<T>>> Iterator for BitSetIter<T, B> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current != 0 {
                let bit = self.current.trailing_zeros() as usize;
                self.current &= self.current - 1;
                let base = if self.word == 0 {
                    0
                } else {
                    BitSet::<T>::INLINE_BITS + (self.word - 1) * 64
                };
                return Some(T::from(base + bit));
            }
            self.word += 1;
            let overflow = self.bitset.borrow().overflow.as_ref()?;
            self.current = *overflow.get(self.word - 1)?;
        }
    }
}

impl<'a, T: From<usize>> IntoIterator for &'a BitSet<T> {
    type Item = T;
    type IntoIter = BitSetIter<T, &'a BitSet<T>>;

    fn into_iter(self) -> Self::IntoIter {
        BitSetIter {
            current: self.inline,
            bitset: self,
            word: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: From<usize>> IntoIterator for BitSet<T> {
    type Item = T;
    type IntoIter = BitSetIter<T, BitSet<T>>;

    fn into_iter(self) -> Self::IntoIter {
        BitSetIter {
            current: self.inline,
            bitset: self,
            word: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T> BitSet<T> {
    const INLINE_BITS: usize = 64;
}

impl<T: From<usize>> BitSet<T>
where
    usize: From<T>,
{
    pub fn set(&mut self, index: T) -> Result<(), alloc::TryReserveError> {
        let index: usize = index.into();
        if index < Self::INLINE_BITS {
            self.inline |= 1 << index;
        } else {
            let overflow_idx = (index - Self::INLINE_BITS) / 64;
            let bit = (index - Self::INLINE_BITS) % 64;
            let overflow = self.overflow.get_or_insert_with(|| alloc::vec![]);
            if overflow_idx >= overflow.len() {
                overflow.try_reserve(overflow_idx + 1 - overflow.len())?;
                overflow.resize(overflow_idx + 1, 0);
            }
            overflow[overflow_idx] |= 1 << bit;
        }
        Ok(())
    }

    pub fn get(&self, index: T) -> bool {
        let index: usize = index.into();
        if index < Self::INLINE_BITS {
            (self.inline >> index) & 1 != 0
        } else {
            let Some(overflow) = &self.overflow else {
                return false;
            };
            let overflow_idx = (index - Self::INLINE_BITS) / 64;
            let bit = (index - Self::INLINE_BITS) % 64;
            overflow
                .get(overflow_idx)
                .is_some_and(|word| (word >> bit) & 1 != 0)
        }
    }

    pub fn clear(&mut self, index: T) {
        let index: usize = index.into();
        if index < Self::INLINE_BITS {
            self.inline &= !(1 << index);
        } else if let Some(overflow) = &mut self.overflow {
            let overflow_idx = (index - Self::INLINE_BITS) / 64;
            let bit = (index - Self::INLINE_BITS) % 64;
            if let Some(word) = overflow.get_mut(overflow_idx) {
                *word &= !(1 << bit);
            }
            self.trim_overflow();
        }
    }

    pub fn contains_all_set_bits_of(&self, other: &Self) -> bool {
        if (self.inline & other.inline) != other.inline {
            return false;
        }
        match (&self.overflow, &other.overflow) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(self_ov), Some(other_ov)) => {
                if other_ov.len() > self_ov.len() {
                    return false;
                }
                self_ov
                    .iter()
                    .zip(other_ov.iter())
                    .all(|(&s, &o)| (s & o) == o)
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inline == 0 && self.overflow.is_none()
    }

    pub fn is_only(&self, index: T) -> bool {
        let index: usize = index.into();
        if index < Self::INLINE_BITS {
            self.inline == (1 << index)
                && self
                    .overflow
                    .as_ref()
                    .is_none_or(|ov| ov.iter().all(|&w| w == 0))
        } else {
            if self.inline != 0 {
                return false;
            }
            let Some(overflow) = &self.overflow else {
                return false;
            };
            let overflow_idx = (index - Self::INLINE_BITS) / 64;
            let bit = (index - Self::INLINE_BITS) % 64;
            // The overflow vector must be long enough to contain the target index
            if overflow_idx >= overflow.len() {
                return false;
            }
            overflow.iter().enumerate().all(|(i, &w)| {
                if i == overflow_idx {
                    w == (1 << bit)
                } else {
                    w == 0
                }
            })
        }
    }

    pub fn subtract(&mut self, other: &Self) {
        self.inline &= !other.inline;
        if let (Some(self_ov), Some(other_ov)) = (&mut self.overflow, &other.overflow) {
            for (s, &o) in self_ov.iter_mut().zip(other_ov.iter()) {
                *s &= !o;
            }
            self.trim_overflow();
        }
    }

    pub fn union_with(&mut self, other: &Self) -> Result<(), alloc::TryReserveError> {
        self.inline |= other.inline;
        if let Some(other_ov) = &other.overflow {
            let self_ov = self.overflow.get_or_insert_with(|| alloc::vec![]);
            if self_ov.len() < other_ov.len() {
                self_ov.try_reserve(other_ov.len() - self_ov.len())?;
                self_ov.resize(other_ov.len(), 0);
            }
            for (s, &o) in self_ov.iter_mut().zip(other_ov.iter()) {
                *s |= o;
            }
        }
        Ok(())
    }

    pub fn iter(&self) -> BitSetIter<T, &Self> {
        BitSetIter {
            current: self.inline,
            bitset: self,
            word: 0,
            _phantom: PhantomData,
        }
    }

    /// returns the number of set bits
    pub fn count(&self) -> usize {
        let mut count = self.inline.count_ones() as usize;
        if let Some(ref ov) = self.overflow {
            for &word in ov {
                count += word.count_ones() as usize;
            }
        }
        count
    }

    /// Returns the number of set bits strictly below `index`.
    pub fn rank(&self, index: T) -> usize {
        let index: usize = index.into();
        if index == 0 {
            return 0;
        }
        if index <= Self::INLINE_BITS {
            let mask = if index < 64 {
                (1u64 << index) - 1
            } else {
                u64::MAX
            };
            return (self.inline & mask).count_ones() as usize;
        }
        let mut count = self.inline.count_ones() as usize;
        let Some(ref ov) = self.overflow else {
            return count;
        };
        let remaining = index - Self::INLINE_BITS;
        let full_words = remaining / 64;
        let extra_bits = remaining % 64;
        for &word in ov.iter().take(full_words) {
            count += word.count_ones() as usize;
        }
        if extra_bits > 0 {
            if let Some(&word) = ov.get(full_words) {
                count += (word & ((1u64 << extra_bits) - 1)).count_ones() as usize;
            }
        }
        count
    }

    pub(crate) fn intersects(&self, other: &Self) -> bool {
        if (self.inline & other.inline) != 0 {
            return true;
        }
        match (&self.overflow, &other.overflow) {
            (Some(self_ov), Some(other_ov)) => self_ov
                .iter()
                .zip(other_ov.iter())
                .any(|(&a, &b)| (a & b) != 0),
            _ => false,
        }
    }

    fn trim_overflow(&mut self) {
        if let Some(overflow) = &mut self.overflow {
            while overflow.last() == Some(&0) {
                overflow.pop();
            }
            if overflow.is_empty() {
                self.overflow = None;
            }
        }
    }
}

impl<T: From<usize>> std::ops::SubAssign<&Self> for BitSet<T>
where
    usize: From<T>,
{
    fn sub_assign(&mut self, rhs: &Self) {
        self.subtract(rhs);
    }
}

impl<T: From<usize>> alloc::TursoFromIterator<T> for BitSet<T>
where
    usize: From<T>,
{
    fn try_from_iter<I: IntoIterator<Item = T>>(iter: I) -> Result<Self, alloc::TryReserveError> {
        let mut set = Self::default();
        set.try_extend(iter)?;
        Ok(set)
    }

    fn try_extend<I: IntoIterator<Item = T>>(
        &mut self,
        iter: I,
    ) -> Result<(), alloc::TryReserveError> {
        for index in iter {
            self.set(index)?;
        }
        Ok(())
    }
}

impl<T> TryFrom<u128> for BitSet<T> {
    type Error = alloc::TryReserveError;

    fn try_from(from: u128) -> Result<Self, Self::Error> {
        let high = (from >> 64) as u64;
        let overflow = match high != 0 {
            true => Some(alloc::try_vec![high]?),
            false => None,
        };
        Ok(Self {
            inline: from as u64,
            overflow,
            _phantom: PhantomData,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ExpressionIndexUsage {
    /// Query expression with column references fixed by semantic analysis.
    /// Example: `lower(name)` for INDEX ON t(lower(name)).
    pub expr: Expr,
    /// Columns required to compute the expression. Helps decide whether using
    /// the expression value from the index fully covers those column reads.
    pub columns_mask: ColumnUsedMask,
}

/// Represents one key pair in a hash join equality condition.
/// For `expr1 = expr2`, this tracks which WHERE term contains the equality
/// and which side of the equality belongs to the build table.
#[derive(Debug, Clone, Copy)]
pub struct HashJoinKey {
    /// Index into the where_clause vector
    pub where_clause_idx: usize,
    /// Which side of the binary equality expression belongs to the build table.
    /// The other side belongs to the probe table.
    pub build_side: BinaryExprSide,
}

impl HashJoinKey {
    /// Get the build table's expression from the WHERE clause.
    pub fn get_build_expr<'a>(&self, where_clause: &'a [WhereTerm]) -> &'a Expr {
        let where_term = &where_clause[self.where_clause_idx];
        let Expr::Binary { lhs, rhs, .. } = &where_term.expr else {
            panic!("HashJoinKey: expected a valid binary expression");
        };
        if self.build_side == BinaryExprSide::Lhs {
            lhs
        } else {
            rhs
        }
    }

    /// Get the probe table's expression from the WHERE clause.
    pub fn get_probe_expr<'a>(&self, where_clause: &'a [WhereTerm]) -> &'a Expr {
        let where_term = &where_clause[self.where_clause_idx];
        let Expr::Binary { lhs, rhs, .. } = &where_term.expr else {
            panic!("HashJoinKey: expected a valid binary expression");
        };
        if self.build_side == BinaryExprSide::Lhs {
            rhs // probe is the opposite side
        } else {
            lhs
        }
    }
}

/// Hash join semantics. Build = LHS (populates hash table), Probe = RHS (scanned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashJoinType {
    /// Only matching rows emitted.
    Inner,
    /// All build rows appear; unmatched build rows get NULLs for the probe side.
    LeftOuter,
    /// Like LeftOuter, plus unmatched probe rows get NULLs for the build side.
    FullOuter,
}

/// Hash join operation metadata
#[derive(Debug, Clone)]
pub struct HashJoinOp {
    /// Index of the build table in the join order
    pub build_table_idx: usize,
    /// Index of the probe table in the join order
    pub probe_table_idx: usize,
    /// Join key references, each entry points to an equality condition in the [WhereTerm]
    /// and indicates which side of the equality belongs to the build table.
    pub join_keys: Vec<HashJoinKey>,
    /// Memory budget for hash table
    pub mem_budget: usize,
    /// Whether the build input should be materialized as a rowid list before hash build.
    pub materialize_build_input: bool,
    /// Whether to use a bloom filter on the probe side.
    pub use_bloom_filter: bool,
    /// Join semantics (inner, left outer, or full outer).
    pub join_type: HashJoinType,
}

/// Distinguishes union (OR) from intersection (AND) operations for multi-index scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetOperation {
    /// Union: rowid appears in result if it's in ANY branch (OR)
    Union,
    /// Intersection: rowid appears in result only if it's in ALL branches (AND).
    /// Carries the indices of additional WHERE terms consumed beyond the primary one.
    Intersection { additional_consumed_terms: BitSet },
}

/// Multi-index scan operation metadata for OR-by-union or AND-by-intersection optimization.
///
/// When a WHERE clause contains an OR of terms that can each use a different index,
/// we can scan each index separately and combine the results using a RowSet for deduplication.
/// For example: `WHERE a = 1 OR b = 2` with indexes on `a` and `b`.
///
/// Similarly, when a WHERE clause contains AND terms on different indexed columns,
/// we can scan each index and intersect the results to reduce the number of table fetches.
/// For example: `WHERE a = 1 AND b = 2` with separate indexes on `a` and `b`.
#[derive(Debug, Clone)]
pub struct MultiIndexScanOp {
    /// Each branch represents one term with its own index access
    pub branches: Vec<MultiIndexBranch>,
    /// Index of the primary WHERE term.
    /// For Union: the index of the OR expression.
    /// For Intersection: the index of the first AND term consumed.
    pub where_term_idx: usize,
    /// The set operation to perform when combining branches
    pub set_op: SetOperation,
}

/// Residual filters that apply only to union (OR) branches.
///
/// Each OR disjunct may be a compound expression (e.g. `a = 1 AND c > 5`), so
/// after the index seek satisfies the indexable part, these residuals filter
/// the remaining conditions.
#[derive(Debug, Clone)]
pub struct UnionBranchPrePostFilters {
    /// Outer-table-only residuals evaluated before the branch's index seek.
    /// These reference only tables from earlier (outer) loops, so they can
    /// short-circuit the entire branch without touching the index.
    pub pre_filter_exprs: Vec<Expr>,
    /// Residual filter expressions that could not be satisfied by the index seek.
    /// Applied within the branch loop after positioning on the table row.
    pub post_filter_exprs: Vec<Expr>,
    /// Whether residual evaluation needs the scanned table cursor positioned.
    pub requires_table_cursor: bool,
}

/// A single branch of a multi-index scan, representing one disjunct of an OR expression.
#[derive(Debug, Clone)]
pub struct MultiIndexBranch {
    /// The index to use for this branch, or None for rowid access
    pub index: Option<PlanIndex>,
    /// How this branch probes the table/index.
    pub access: MultiIndexBranchAccess,
    /// Estimated number of rows from this branch
    pub estimated_rows: f64,
    /// Residual filters for union (OR) branches. `None` for intersection branches.
    pub union_residuals: Option<UnionBranchPrePostFilters>,
}

/// Access shape for a single multi-index branch.
#[derive(Debug, Clone)]
#[expect(clippy::large_enum_variant)]
pub enum MultiIndexBranchAccess {
    /// Ordinary seek/range scan on either the rowid btree or a secondary index.
    Seek { seek_def: SeekDef },
    /// Repeated equality seeks driven by an IN-list or IN-subquery RHS.
    InSeek { source: InSeekSource },
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Operation {
    // Scan operation
    // This operation is used to scan a table.
    Scan(Scan),
    // Search operation
    // This operation is used to search for a row in a table using an index
    // (i.e. a primary key or a secondary index)
    Search(Search),
    // Access through custom index method query
    IndexMethodQuery(IndexMethodQuery),
    // Hash join operation
    // This operation is used on the probe side of a hash join.
    // The build table is accessed normally (via Scan), and the probe table
    // uses this operation to indicate it should probe the hash table.
    HashJoin(HashJoinOp),
    // Multi-index scan operation for OR-by-union optimization.
    // This operation scans multiple indexes (one per OR branch) and combines
    // results using RowSet deduplication.
    MultiIndexScan(MultiIndexScanOp),
}

impl Operation {
    pub fn default_scan_for(table: &Table) -> Self {
        match table {
            Table::BTree(_) => Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            Table::Virtual(_) => Operation::Scan(Scan::VirtualTable {
                idx_num: -1,
                idx_str: None,
                constraints: Vec::new(),
            }),
            Table::FromClauseSubquery(_) => Operation::Scan(Scan::Subquery {
                iter_dir: IterationDirection::Forwards,
            }),
            Table::RecursiveCteInput(_) => Operation::Scan(Scan::RecursiveCteInput),
        }
    }

    pub fn index(&self) -> Option<&PlanIndex> {
        match self {
            Operation::Scan(Scan::BTreeTable { index, .. }) => index.as_ref(),
            Operation::Search(Search::Seek { index, .. })
            | Operation::Search(Search::InSeek { index, .. }) => index.as_ref(),
            Operation::IndexMethodQuery(IndexMethodQuery { index, .. }) => Some(index),
            Operation::Scan(_) => None,
            Operation::Search(Search::RowidEq { .. }) => None,
            Operation::HashJoin(_) => None,
            // Multi-index scan uses multiple indexes; return None as there's no single index
            Operation::MultiIndexScan(_) => None,
        }
    }

    /// Returns true if this operation is guaranteed to access at most one row.
    /// Used to determine whether UPDATE/DELETE is single-write.
    ///
    /// Conservative: returns false when unsure (e.g. table scans, range seeks,
    /// non-unique index seeks).
    pub fn affects_max_1_row(&self) -> bool {
        match self {
            // RowidEq is always a single-row point lookup.
            Operation::Search(Search::RowidEq { .. }) => true,
            // Seek on a unique index with all columns equality-constrained.
            Operation::Search(Search::Seek { index, seek_def }) => {
                let Some(idx) = index else {
                    // Seek on rowid (no index): check if the seek is an equality
                    // point lookup. This happens when prefix has one eq constraint
                    // and no range component.
                    return seek_def.prefix.len() == 1
                        && seek_def.prefix[0].eq.is_some()
                        && matches!(seek_def.start.last_component, SeekKeyComponent::None);
                };
                if !idx.unique {
                    return false;
                }
                // All index columns must have equality constraints.
                let num_index_cols = idx.columns.len();
                let num_eq_prefix = seek_def.prefix.iter().filter(|c| c.eq.is_some()).count();
                num_eq_prefix == num_index_cols
            }
            // Table scans, hash joins, multi-index scans, etc. are not single-row.
            _ => false,
        }
    }
}

fn query_output_columns(
    plan: &Plan,
    explicit_columns: Option<&[String]>,
) -> Result<alloc::Vec<Column>> {
    let result_columns: &[ResultSetColumn] = match plan {
        Plan::Select(select_plan) => &select_plan.result_columns,
        Plan::CompoundSelect {
            left, right_most, ..
        } => left
            .first()
            .map(|(select, _)| &select.result_columns[..])
            .unwrap_or(&right_most.result_columns),
        Plan::RecursiveCte(recursive_cte) => &recursive_cte.result_columns,
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE/UPDATE plans cannot define query output columns")
        }
    };

    let compound_arms = match plan {
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut arms = left
                .iter()
                .map(|(select, _)| select)
                .try_collect::<alloc::Vec<_>>()?;
            arms.try_push(right_most)?;
            Some(arms)
        }
        _ => None,
    };

    let mut columns = result_columns
        .iter()
        .enumerate()
        .map(|(column_index, result_column)| {
            let name = explicit_columns
                .and_then(|names| names.get(column_index).cloned())
                .unwrap_or_else(|| result_column.name.clone());
            let column_type = compound_arms
                .as_ref()
                .map(|arms| {
                    compound_column_affinity(arms, column_index)
                        .affinity
                        .to_type()
                })
                .unwrap_or_else(|| result_column.affinity.affinity.to_type());
            Column::new(
                Some(name),
                column_type.to_string(),
                None,
                None,
                column_type,
                None,
                ColDef::default(),
            )
        })
        .try_collect::<alloc::Vec<_>>()?;

    for (column_index, column) in columns.iter_mut().enumerate() {
        let result_column = &result_columns[column_index];
        column.set_array_dimensions(result_column.array_dimensions);
        column.set_collation(
            result_column
                .collation
                .as_ref()
                .map(|collation| *collation.value()),
        );
    }
    Ok(columns)
}

impl JoinedTable {
    /// Returns the btree table for this table reference, if it is a BTreeTable.
    pub fn btree(&self) -> Option<Arc<BTreeTable>> {
        match &self.table {
            Table::BTree(_) => self.table.btree(),
            _ => None,
        }
    }
    pub fn virtual_table(&self) -> Option<Arc<VirtualTable>> {
        match &self.table {
            Table::Virtual(_) => self.table.virtual_table(),
            _ => None,
        }
    }

    fn matches(&self, database_id: usize, table_name: &str) -> bool {
        self.database_id == database_id
            && matches!(self.table, Table::BTree(_) | Table::Virtual(_))
            && self.table.get_name().eq_ignore_ascii_case(table_name)
    }

    /// Creates a new TableReference for a subquery from a SelectPlan.
    pub fn new_subquery(
        identifier: String,
        plan: SelectPlan,
        join_info: Option<JoinInfo>,
        internal_id: PlanSourceId,
    ) -> Result<Self> {
        let mut columns = plan
            .result_columns
            .iter()
            .map(|rc| {
                let col_type = rc.affinity.affinity.to_type();
                let type_name = col_type.to_string();
                Column::new(
                    Some(rc.name().to_string()),
                    type_name,
                    None,
                    None,
                    col_type,
                    None,
                    ColDef::default(),
                )
            })
            .try_collect::<alloc::Vec<_>>()?;

        for (i, column) in columns.iter_mut().enumerate() {
            let result_column = &plan.result_columns[i];
            column.set_array_dimensions(result_column.array_dimensions);
            column.set_collation(
                result_column
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value()),
            );
        }

        let table = Table::FromClauseSubquery(Arc::new(FromClauseSubquery {
            name: identifier.clone(),
            plan: Box::new(Plan::Select(Box::new(plan))),
            columns,
            result_columns_start_reg: None,
            materialized_cursor_id: None,
            cte: None,
        }));
        let read_programs = Arc::new(SourceReadPrograms::none(table.columns().len()));
        Ok(Self {
            op: Operation::default_scan_for(&table),
            table,
            resolved_table: None,
            identifier,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            index_hint: PlanIndexHint::None,
            index_method_patterns: Vec::new(),
            index_expressions: Vec::new(),
            read_programs,
            check_constraints: Vec::new(),
        })
    }

    /// Creates a new TableReference for a subquery from a Plan (either SelectPlan or CompoundSelect).
    /// If `explicit_columns` is provided, those names override the derived column names from the SELECT.
    /// If `cte_id` is provided, this subquery is a CTE reference that can share materialized data.
    /// If `materialize_hint` is true, the CTE was declared with AS MATERIALIZED and should always
    /// be materialized regardless of reference count.
    pub fn new_subquery_from_plan(
        identifier: String,
        plan: Plan,
        join_info: Option<JoinInfo>,
        internal_id: PlanSourceId,
        explicit_columns: Option<&[String]>,
        cte_id: Option<PlanCteId>,
        materialize_hint: bool,
    ) -> Result<Self> {
        let columns = query_output_columns(&plan, explicit_columns)?;
        // Get result columns and table references from the plan
        // materialize_hint is set true for explicit WITH ... AS MATERIALIZED hint.
        // Multi-reference CTEs are also detected at emission time via reference counting,
        // and they may be materialized regardless of explicit keyword usage.
        let cte = cte_id.map(|id| crate::schema::FromClauseSubqueryCteMetadata {
            id,
            shared_materialization: false,
            materialize_hint,
        });
        let table = Table::FromClauseSubquery(Arc::new(FromClauseSubquery {
            name: identifier.clone(),
            plan: Box::new(plan),
            columns,
            result_columns_start_reg: None,
            materialized_cursor_id: None,
            cte,
        }));
        let read_programs = Arc::new(SourceReadPrograms::none(table.columns().len()));
        Ok(Self {
            op: Operation::default_scan_for(&table),
            table,
            resolved_table: None,
            identifier,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            index_hint: PlanIndexHint::None,
            index_method_patterns: Vec::new(),
            index_expressions: Vec::new(),
            read_programs,
            check_constraints: Vec::new(),
        })
    }

    pub fn new_recursive_cte_input(
        identifier: String,
        result_columns: &[ResultSetColumn],
        internal_id: PlanSourceId,
        explicit_columns: Option<&[String]>,
    ) -> Result<Self> {
        let mut columns = result_columns
            .iter()
            .enumerate()
            .map(|(column_index, result_column)| {
                let name = explicit_columns
                    .and_then(|names| names.get(column_index).cloned())
                    .unwrap_or_else(|| result_column.name.clone());
                let column_type = result_column.affinity.affinity.to_type();
                let mut column = Column::new(
                    Some(name),
                    column_type.to_string(),
                    None,
                    None,
                    column_type,
                    None,
                    ColDef::default(),
                );
                column.set_array_dimensions(result_column.array_dimensions);
                column.set_collation(
                    result_column
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value()),
                );
                column
            })
            .try_collect::<alloc::Vec<_>>()?;
        // The recursive self-reference reads SQLite's queue table, whose
        // columns have no declared type: comparisons in the recursive term
        // see the stored value without the anchor query's affinity. Only the
        // outer read of the CTE keeps the derived affinity.
        for column in columns.iter_mut() {
            column.set_base_affinity(Affinity::Blob);
        }
        let table = Table::RecursiveCteInput(Arc::new(RecursiveCteInput {
            name: identifier.clone(),
            columns,
        }));
        let read_programs = Arc::new(SourceReadPrograms::none(table.columns().len()));
        Ok(Self {
            op: Operation::default_scan_for(&table),
            table,
            resolved_table: None,
            identifier,
            internal_id,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            index_hint: PlanIndexHint::None,
            index_method_patterns: Vec::new(),
            index_expressions: Vec::new(),
            read_programs,
            check_constraints: Vec::new(),
        })
    }

    pub fn columns(&self) -> &[Column] {
        self.table.columns()
    }

    pub fn plan_index_expressions(&self, index: &Index) -> Option<&PlanIndexExpressions> {
        self.index_expressions
            .iter()
            .find(|planned| std::ptr::eq(planned.index.value(), index))
    }

    pub fn expression_index_pos(&self, index: &Index, expr: &Expr) -> Option<usize> {
        self.plan_index_expressions(index)?
            .columns
            .iter()
            .enumerate()
            .position(|(position, candidate)| {
                index.columns[position].pos_in_table == crate::schema::EXPR_INDEX_SENTINEL
                    && candidate
                        .as_ref()
                        .is_some_and(|candidate| plan_exprs_are_equivalent(candidate, expr))
            })
    }

    pub fn partial_index_predicate(&self, index: &Index) -> Option<&Expr> {
        self.plan_index_expressions(index)?.predicate.as_ref()
    }

    /// Mark a column as used in the query.
    /// This is used to determine whether a covering index can be used.
    pub fn mark_column_used(&mut self, index: usize) {
        if index >= self.column_use_counts.len() {
            self.column_use_counts.resize(index + 1, 0);
        }
        self.column_use_counts[index] += 1;
        self.col_used_mask.set(index).expect("TODO: alloc error");
    }

    /// Clear any previously registered expression index usages.
    pub fn clear_expression_index_usages(&mut self) {
        self.expression_index_usages.clear();
    }

    /// Example: SELECT a+b FROM t WHERE a+b=5 with INDEX ON t(a+b)
    /// We want to remember that (a+b) is available on an index key and that
    /// columns a and b are only needed to produce that expression. Later we
    /// can avoid opening the table cursor if all column references are
    /// covered by expression keys.
    pub fn register_expression_index_usage(&mut self, expr: Expr, columns_mask: ColumnUsedMask) {
        if columns_mask.is_empty() {
            return;
        }
        if self
            .expression_index_usages
            .iter()
            .any(|usage| plan_exprs_are_equivalent(&usage.expr, &expr))
        {
            return;
        }
        self.expression_index_usages
            .push(ExpressionIndexUsage { expr, columns_mask });
    }

    /// Provided an index that may contain expression keys, remove any
    /// columns from `required_columns` that are fully covered by expression index values.
    fn apply_expression_index_coverage(
        &self,
        index: &Index,
        required_columns: &mut ColumnUsedMask,
    ) {
        let mut coverage_counts = vec![0usize; self.column_use_counts.len()];
        let mut any_covered = false;
        for usage in &self.expression_index_usages {
            // If the index stores the expression (e.g. idx on lower(name)), all
            // columns needed *solely* for that expression can be treated as
            // covered by the index key. Example:
            //   CREATE INDEX idx ON t(lower(name));
            //   SELECT lower(name) FROM t;
            // Column `name` is not otherwise needed, so we can rely on the
            // expression value from the index and drop the table cursor.
            if self.expression_index_pos(index, &usage.expr).is_some() {
                any_covered = true;
                for col_idx in usage.columns_mask.iter() {
                    if col_idx >= coverage_counts.len() {
                        coverage_counts.resize(col_idx + 1, 0);
                    }
                    coverage_counts[col_idx] += 1;
                }
            }
        }
        if !any_covered {
            return;
        }
        for (col_idx, &covered) in coverage_counts.iter().enumerate() {
            if covered == 0 {
                continue;
            }
            // Only drop the requirement if *all* references to this column are
            // satisfied by expression-index values. If the column is also
            // selected or filtered directly, the table data is still needed.
            if self.column_use_counts.get(col_idx).copied().unwrap_or(0) == covered {
                required_columns.clear(col_idx);
            }
        }
    }

    /// Open the necessary cursors for this table reference.
    /// Generally a table cursor is always opened unless a SELECT query can use a covering index.
    /// An index cursor is opened if an index is used in any way for reading data from the table.
    pub fn open_cursors(
        &self,
        program: &mut ProgramBuilder,
        mode: OperationMode,
        schema: &Schema,
    ) -> Result<(Option<CursorID>, Option<CursorID>)> {
        let index = self.op.index();
        match &self.table {
            Table::BTree(btree) => {
                let use_covering_index = self.utilizes_covering_index();
                let index_is_ephemeral = matches!(index, Some(PlanIndex::Ephemeral(_)));
                let table_not_required = matches!(mode, OperationMode::SELECT)
                    && use_covering_index
                    && !index_is_ephemeral;
                let table_cursor_id = if table_not_required {
                    None
                } else if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
                    target_table,
                    ..
                }) = &mode
                {
                    // The cursor for the ephemeral table was already allocated earlier. Let's allocate one for the target table,
                    // in case it wasn't already allocated when populating the ephemeral table.
                    Some(program.alloc_cursor_id_keyed_if_not_exists(
                        CursorKey::table(target_table.internal_id),
                        match &target_table.table {
                            Table::BTree(btree) => CursorType::BTreeTable(btree.clone()),
                            Table::Virtual(virtual_table) => {
                                CursorType::VirtualTable(virtual_table.clone())
                            }
                            _ => unreachable!("target table must be a btree or virtual table"),
                        },
                    ))
                } else {
                    // Check if this is a materialized view
                    let cursor_type =
                        if let Some(view_mutex) = schema.get_materialized_view(&btree.name) {
                            CursorType::MaterializedView(btree.clone(), view_mutex)
                        } else {
                            CursorType::BTreeTable(btree.clone())
                        };
                    Some(program.alloc_cursor_id_keyed_if_not_exists(
                        CursorKey::table(self.internal_id),
                        cursor_type,
                    ))
                };

                let index_cursor_id = index
                    .map(|index| {
                        let index_handle = index.handle();
                        program.alloc_cursor_index_if_not_exists(
                            CursorKey::index(self.internal_id, index_handle.clone()),
                            &index_handle,
                        )
                    })
                    .transpose()?;
                Ok((table_cursor_id, index_cursor_id))
            }
            Table::Virtual(virtual_table) => {
                let table_cursor_id = Some(program.alloc_cursor_id_keyed(
                    CursorKey::table(self.internal_id),
                    CursorType::VirtualTable(virtual_table.clone()),
                ));
                let index_cursor_id = None;
                Ok((table_cursor_id, index_cursor_id))
            }
            Table::FromClauseSubquery(..) => {
                let index_cursor_id = index
                    .map(|index| {
                        let index_handle = index.handle();
                        program.alloc_cursor_index_if_not_exists(
                            CursorKey::index(self.internal_id, index_handle.clone()),
                            &index_handle,
                        )
                    })
                    .transpose()?;
                Ok((None, index_cursor_id))
            }
            Table::RecursiveCteInput(input) => {
                let cursor_id = program.alloc_cursor_id_keyed_if_not_exists(
                    CursorKey::table(self.internal_id),
                    CursorType::Pseudo(PseudoCursorType::new_with_columns(&input.columns)),
                );
                Ok((Some(cursor_id), None))
            }
        }
    }

    /// Resolve the already opened cursors for this table reference.
    pub fn resolve_cursors(
        &self,
        program: &mut ProgramBuilder,
        mode: OperationMode,
    ) -> Result<(Option<CursorID>, Option<CursorID>)> {
        let index = self.op.index();
        let table_cursor_id = if let Table::FromClauseSubquery(from_clause_subquery) = &self.table {
            from_clause_subquery.materialized_cursor_id
        } else if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
            target_table,
            ..
        }) = &mode
        {
            program.resolve_cursor_id_safe(&CursorKey::table(target_table.internal_id))
        } else {
            program.resolve_cursor_id_safe(&CursorKey::table(self.internal_id))
        };
        let index_cursor_id = index.map(|index| {
            program.resolve_cursor_id(&CursorKey::index(self.internal_id, index.handle()))
        });
        Ok((table_cursor_id, index_cursor_id))
    }

    /// Returns true if a given index is a covering index for this [TableReference].
    pub fn index_is_covering(&self, index: &Index) -> bool {
        let Table::BTree(btree) = &self.table else {
            return false;
        };
        if index.index_method.is_some() {
            return false;
        }
        if self.col_used_mask.is_empty() {
            // With no referenced columns, a complete index can provide the row-producing
            // scan without opening the table. Partial-index completeness depends on the
            // query predicate, so keep this path conservative.
            return index.where_clause.is_none();
        }

        if self.expression_index_usages.is_empty() {
            Self::index_covers_columns(index, btree, &self.col_used_mask)
        } else {
            let mut required_columns = self.col_used_mask.clone();
            self.apply_expression_index_coverage(index, &mut required_columns);
            if required_columns.is_empty() {
                return true;
            }
            Self::index_covers_columns(index, btree, &required_columns)
        }
    }

    fn index_covers_columns(
        index: &Index,
        btree: &BTreeTable,
        required_columns: &ColumnUsedMask,
    ) -> bool {
        // If a table has a rowid, the index is guaranteed to contain it as well.
        let rowid_alias_pos = if btree.has_rowid {
            btree.get_rowid_alias_column().map(|(pos, _)| pos)
        } else {
            None
        };

        if let Some(pos) = rowid_alias_pos {
            if required_columns.is_only(pos) {
                // If the index would be ONLY used for the rowid, don't bother.
                // Example: SELECT id FROM t where id is a rowid alias - just scan the table.
                return false;
            }
        }

        // Check that every required column is covered by the index
        for required_col in required_columns.iter() {
            if rowid_alias_pos == Some(required_col) {
                // rowid is always implicitly covered by the index
                continue;
            }
            let covered_by_index = index
                .columns
                .iter()
                .filter(|c| c.pos_in_table == required_col)
                .any(|c| {
                    // SQLite doesn't consider fulfill covering indexes with virtual columns,
                    // see `recomputeColumnsNotIndexed` in `build.c`. We might be able to improve this
                    // in the future, but for now we do this to ensure correctness.
                    !btree
                        .columns()
                        .get(c.pos_in_table)
                        .expect("column should be in table")
                        .is_virtual_generated()
                });
            if !covered_by_index {
                return false;
            }
        }
        true
    }

    /// Returns true if the index selected for use with this [TableReference] is a covering index,
    /// meaning that it contains all the columns that are referenced in the query.
    pub fn utilizes_covering_index(&self) -> bool {
        let Some(index) = self.op.index() else {
            return false;
        };
        self.index_is_covering(index.as_ref())
    }

    pub fn column_is_used(&self, index: usize) -> bool {
        self.col_used_mask.get(index)
    }
}

/// A definition of a rowid/index search.
///
/// [SeekKey] is the condition that is used to seek to a specific row in a table/index.
/// [SeekKey] also used to represent range scan termination condition.
#[derive(Debug, Clone)]
pub struct SeekDef {
    /// Common prefix of the key which is shared between start/end fields
    /// For example, given:
    /// - CREATE INDEX i ON t (x, y desc)
    /// - SELECT * FROM t WHERE x = 1 AND y >= 30
    ///
    /// Then, prefix=[(eq=1, ASC)], start=Some((ge, Expr(30))), end=Some((gt, Sentinel))
    pub prefix: Vec<SeekRangeConstraint>,
    /// The condition to use when seeking. See [SeekKey] for more details.
    pub start: SeekKey,
    /// The condition to use when terminating the scan that follows the seek. See [SeekKey] for more details.
    pub end: SeekKey,
    /// The direction of the scan that follows the seek.
    pub iter_dir: IterationDirection,
}

pub struct SeekDefKeyIterator<'a, T> {
    seek_def: &'a SeekDef,
    seek_key: &'a SeekKey,
    pos: usize,
    _t: PhantomData<T>,
}

impl<'a> Iterator for SeekDefKeyIterator<'a, SeekKeyComponent<&'a Expr>> {
    type Item = SeekKeyComponent<&'a Expr>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.pos < self.seek_def.prefix.len() {
            Some(SeekKeyComponent::Expr(
                &self.seek_def.prefix[self.pos].eq.as_ref().unwrap().1,
            ))
        } else if self.pos == self.seek_def.prefix.len() {
            match &self.seek_key.last_component {
                SeekKeyComponent::Expr(expr) => Some(SeekKeyComponent::Expr(expr)),
                SeekKeyComponent::Null => Some(SeekKeyComponent::Null),
                SeekKeyComponent::None => None,
            }
        } else {
            None
        };
        self.pos += 1;
        result
    }
}

impl<'a> Iterator for SeekDefKeyIterator<'a, Affinity> {
    type Item = Affinity;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.pos < self.seek_def.prefix.len() {
            Some(self.seek_def.prefix[self.pos].eq.as_ref().unwrap().2)
        } else if self.pos == self.seek_def.prefix.len() {
            match &self.seek_key.last_component {
                SeekKeyComponent::Expr(..) => Some(self.seek_key.affinity),
                // NULL sentinel does not require conversion; use NONE affinity so width matches.
                SeekKeyComponent::Null => Some(Affinity::Blob),
                SeekKeyComponent::None => None,
            }
        } else {
            None
        };
        self.pos += 1;
        result
    }
}

impl SeekDef {
    /// returns amount of values in the given seek key
    /// - so, for SELECT * FROM t WHERE x = 10 AND y = 20 AND y >= 30 there will be 3 values (10, 20, 30)
    pub fn size(&self, key: &SeekKey) -> usize {
        self.prefix.len()
            + match key.last_component {
                SeekKeyComponent::Expr(_) => 1,
                SeekKeyComponent::Null => 1,
                SeekKeyComponent::None => 0,
            }
    }
    /// iterate over value expressions in the given seek key
    pub fn iter<'a>(
        &'a self,
        key: &'a SeekKey,
    ) -> SeekDefKeyIterator<'a, SeekKeyComponent<&'a Expr>> {
        SeekDefKeyIterator {
            seek_def: self,
            seek_key: key,
            pos: 0,
            _t: PhantomData,
        }
    }

    /// iterate over affinity in the given seek key
    pub fn iter_affinity<'a>(&'a self, key: &'a SeekKey) -> SeekDefKeyIterator<'a, Affinity> {
        SeekDefKeyIterator {
            seek_def: self,
            seek_key: key,
            pos: 0,
            _t: PhantomData,
        }
    }
}

/// Build the affinity string for a synthesized ephemeral seek index.
///
/// The seek key only constrains the leading key prefix, but the backing record
/// stored in the ephemeral index still includes the remaining payload columns
/// (and possibly a synthetic rowid). Pad those trailing slots with NONE affinity
/// so MakeRecord sees the same layout the index insert path produced.
pub fn synthesized_seek_affinity_str(index: &Index, seek_def: &SeekDef) -> Option<Arc<String>> {
    let num_key_cols = seek_def.size(&seek_def.start);
    let total_cols = index.columns.len() + if index.has_rowid { 1 } else { 0 };
    let mut aff: String = seek_def
        .iter_affinity(&seek_def.start)
        .map(|a| a.aff_mask())
        .collect();
    for _ in num_key_cols..total_cols {
        aff.push(affinity::SQLITE_AFF_NONE);
    }
    aff.chars()
        .any(|c| c != affinity::SQLITE_AFF_NONE)
        .then(|| Arc::new(aff))
}

/// [SeekKeyComponent] represents the optional trailing component of a seek key.
/// Besides user-provided expressions, planner logic may inject a synthetic NULL sentinel
/// to encode SQLite-compatible boundary behavior on composite indexes.
/// This enum accepts generic argument E so we can use both
/// SeekKeyComponent<Expr> and SeekKeyComponent<&Expr>.
#[derive(Debug, Clone)]
pub enum SeekKeyComponent<E> {
    Expr(E),
    Null,
    None,
}

/// A condition to use when seeking.
#[derive(Debug, Clone)]
pub struct SeekKey {
    /// Complete key must be constructed from common [SeekDef::prefix] and optional last_component
    pub last_component: SeekKeyComponent<Expr>,

    /// The comparison operator to use when seeking.
    pub op: SeekOp,

    /// Affinity of the comparison
    pub affinity: Affinity,
}

/// Represents the type of table scan performed during query execution.
#[derive(Clone, Debug)]
pub enum Scan {
    /// A scan of a B-tree–backed table, optionally using an index, and with an iteration direction.
    BTreeTable {
        /// The iter_dir is used to indicate the direction of the iterator.
        iter_dir: IterationDirection,
        /// The index that we are using to scan the table, if any.
        index: Option<PlanIndex>,
    },
    /// A scan of a virtual table, delegated to the table’s `filter` and related methods.
    VirtualTable {
        /// Index identifier returned by the table's `best_index` method.
        idx_num: i32,
        /// Optional index name returned by the table’s `best_index` method.
        idx_str: Option<String>,
        /// Constraining expressions to be passed to the table’s `filter` method.
        /// The order of expressions matches the argument order expected by the virtual table.
        constraints: Vec<Expr>,
    },
    /// A scan of a subquery in the `FROM` clause.
    Subquery {
        /// Coroutine-backed scans run forwards. Materialized subqueries may
        /// also be scanned backwards when the planner relies on intrinsic
        /// subquery order for an extremum fast path.
        iter_dir: IterationDirection,
    },
    /// The one-row input consumed by the recursive part of a recursive CTE.
    RecursiveCteInput,
}

/// An enum that represents a search operation that can be used to search for a row in a table using an index
/// (i.e. a primary key or a secondary index)
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum Search {
    /// A rowid equality point lookup. This is a special case that uses the SeekRowid bytecode instruction and does not loop.
    RowidEq { cmp_expr: Expr },
    /// A search on a table btree (via `rowid`) or a secondary index search. Uses bytecode instructions like SeekGE, SeekGT etc.
    Seek {
        index: Option<PlanIndex>,
        seek_def: SeekDef,
    },
    /// An IN-driven index seek. Iterates an ephemeral B-tree of IN values and
    /// for each value seeks into the real index (or table, if seek by rowid).
    InSeek {
        index: Option<PlanIndex>,
        source: InSeekSource,
    },
}

/// Where IN-seek values come from.
#[derive(Clone, Debug)]
pub enum InSeekSource {
    /// Literal values to materialize into a new ephemeral index at open_loop time.
    LiteralList {
        values: Vec<Expr>,
        affinity: Affinity,
    },
    /// Subquery already materialized by emit_non_from_clause_subquery;
    /// open_loop reuses the existing ephemeral cursor.
    Subquery { cursor_id: CursorID },
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub struct IndexMethodQuery {
    /// index method to use
    pub index: PlanIndex,
    /// idx of the pattern from [crate::index_method::IndexMethodAttachment::definition] which planner chose to use for the access
    pub pattern_idx: usize,
    /// captured arguments for the pattern chosen by the planner
    pub arguments: Vec<Expr>,
    /// Mapping from the index of a [`Expr::Column`] argument to the column
    /// index returned by the index method.
    pub covered_columns: HashMap<usize, usize>,
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    pub func: AggFunc,
    pub args: Vec<Expr>,
    pub original_expr: Expr,
    pub distinctness: Distinctness,
    pub filter_expr: Option<Expr>,
    /// For `percentile_cont`/`percentile_disc`: register holding the fraction
    /// after it has been evaluated and range-checked once per invocation,
    /// before the aggregate row loop. Populated by `InitLoop::emit`.
    pub fraction_reg: Option<usize>,
}

impl Aggregate {
    pub fn new(
        func: AggFunc,
        args: &[Box<Expr>],
        expr: &Expr,
        distinctness: Distinctness,
        filter_expr: Option<Expr>,
    ) -> Self {
        Aggregate {
            func,
            args: args.iter().map(|arg| *arg.clone()).collect(),
            original_expr: expr.clone(),
            distinctness,
            filter_expr,
            fraction_reg: None,
        }
    }

    pub fn is_distinct(&self) -> bool {
        self.distinctness.is_distinct()
    }
}

/// Represents the window definition and all window functions associated with a single SELECT.
///
/// All functions in a single `Window` share the same partition/order/frame. When
/// a user OVER clause is referenced by functions with different coerced frames
/// (e.g. `row_number() OVER w` coerces to `ROWS UNBOUNDED..CURRENT` while
/// `rank() OVER w` coerces to `RANGE UNBOUNDED..CURRENT`), the planner splits
/// them into separate `Window` instances with identical partition/order but
/// distinct frames. The existing nested-subquery rewrite in
/// `prepare_window_subquery` then produces one ephemeral-table pass per
/// `Window` — matching SQLite's behaviour where mixed-frame queries compile to
/// nested coroutine layers.
#[derive(Debug, Clone)]
pub struct Window {
    /// The window name, either provided in the original statement or synthetically generated by
    /// the planner. This is optional because it can be assigned at different stages of query
    /// processing, but it should eventually always be set.
    pub name: Option<String>,
    /// Expressions from the PARTITION BY clause.
    pub partition_by: Vec<Expr>,
    /// The number of unique expressions in the PARTITION BY clause. This determines how many of
    /// the leftmost columns in the subquery output make up the partition key.
    pub deduplicated_partition_by_len: Option<usize>,
    /// Expressions from the ORDER BY clause.
    pub order_by: Vec<(Expr, SortOrder, Option<ast::NullsOrder>)>,
    /// The single coerced frame shared by every function in this window.
    /// Mirrors SQLite's assert at `window.c:1679` ("All OVER clauses in the
    /// same window function aggregate step must be the same"). Functions
    /// whose coerced frames disagree are split into separate `Window`
    /// instances by the planner.
    pub frame: Frame,
    /// All window functions associated with this window.
    pub functions: Vec<WindowFunction>,
}

impl Window {
    /// Build an unnamed window from partition/order expressions inherited from
    /// a named base window.
    pub fn from_planned_spec(spec: PlannedWindowSpec, frame: Frame) -> Self {
        Window {
            name: None,
            partition_by: spec.partition_by,
            deduplicated_partition_by_len: None,
            order_by: spec.order_by,
            frame,
            functions: vec![],
        }
    }

    pub fn is_equivalent_to_spec(&self, spec: &PlannedWindowSpec, frame: &Frame) -> bool {
        if !frames_are_equivalent(&self.frame, frame)
            || self.partition_by.len() != spec.partition_by.len()
        {
            return false;
        }
        if !self
            .partition_by
            .iter()
            .zip(&spec.partition_by)
            .all(|(a, b)| plan_exprs_are_equivalent(a, b))
        {
            return false;
        }
        if self.order_by.len() != spec.order_by.len() {
            return false;
        }
        self.order_by.iter().zip(&spec.order_by).all(
            |((expr_a, order_a, nulls_a), (expr_b, order_b, nulls_b))| {
                plan_exprs_are_equivalent(expr_a, expr_b)
                    && order_a == order_b
                    && nulls_a == nulls_b
            },
        )
    }
}

fn frames_are_equivalent(lhs: &Frame, rhs: &Frame) -> bool {
    lhs.mode == rhs.mode
        && frame_boundaries_are_equivalent(&lhs.start, &rhs.start)
        && frame_boundaries_are_equivalent(&lhs.end, &rhs.end)
}

fn frame_boundaries_are_equivalent(lhs: &FrameBoundary, rhs: &FrameBoundary) -> bool {
    match (lhs, rhs) {
        (FrameBoundary::UnboundedPreceding, FrameBoundary::UnboundedPreceding)
        | (FrameBoundary::CurrentRow, FrameBoundary::CurrentRow)
        | (FrameBoundary::UnboundedFollowing, FrameBoundary::UnboundedFollowing) => true,
        (FrameBoundary::Preceding(lhs), FrameBoundary::Preceding(rhs))
        | (FrameBoundary::Following(lhs), FrameBoundary::Following(rhs)) => {
            plan_exprs_are_equivalent(lhs, rhs)
        }
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct PlannedWindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<(Expr, SortOrder, Option<ast::NullsOrder>)>,
}

/// One bound of a window function's effective frame.
#[derive(Debug, Clone)]
pub enum FrameBoundary {
    UnboundedPreceding,
    Preceding(Box<Expr>),
    CurrentRow,
    Following(Box<Expr>),
    UnboundedFollowing,
}

/// A window function's effective frame. The bounds are interpreted per `mode`:
/// `Rows` counts physical rows, `Range` and `Groups` work over peer groups of
/// the window's ORDER BY values.
///
/// Example: `<mode: RANGE> <start: UNBOUNDED PRECEDING> TO <end: CURRENT ROW>`
#[derive(Debug, Clone)]
pub struct Frame {
    pub mode: ast::FrameMode,
    pub start: FrameBoundary,
    pub end: FrameBoundary,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            mode: ast::FrameMode::Range,
            start: FrameBoundary::UnboundedPreceding,
            end: FrameBoundary::CurrentRow,
        }
    }
}

/// One window function call belonging to a `Window`.
///
/// Window queries are planned by wrapping the original FROM/WHERE in a
/// subquery, pushing each window function's arguments and FILTER predicate
/// into that subquery as new output columns, and rewriting the call to read
/// those columns instead of the original tables. See `plan_windows` in
/// `translate/window.rs` for the full rewrite, including worked examples.
/// "Source subquery" below refers to that wrapper subquery.
#[derive(Debug, Clone)]
pub struct WindowFunction {
    /// The resolved function. Aggregate window functions and specialized window
    /// functions such as ROW_NUMBER() are supported.
    pub func: AccumulatorFunc,
    /// The expression from which the function was resolved. Used as the lookup
    /// key when matching SQL occurrences back to this entry during rewriting.
    pub original_expr: Expr,
    /// Populated the first time `rewrite_terminal_expr` matches this function.
    /// Later occurrences of the same call reuse this cached rewrite so they
    /// resolve to the same result register.
    pub rewritten: Option<RewrittenWindowCall>,
}

/// The rewritten form of a window function call, populated once `WindowFunction`
/// has been mapped onto its source subquery.
#[derive(Debug, Clone)]
pub struct RewrittenWindowCall {
    /// `WindowFunction::original_expr` with its arguments, FILTER predicate, and
    /// OVER clause rewritten to reference the source subquery.
    pub expr: Expr,
    /// The FILTER predicate, rewritten to reference the source subquery's
    /// output columns. AggStep evaluates this once per input row and skips
    /// the step when it is false. A copy of the predicate already inside
    /// `expr.filter_over`, lifted to a bare `Expr` so AggStep doesn't have to
    /// pattern-match it back out on every row.
    pub filter_expr: Option<Expr>,
}

impl WindowFunction {
    /// The expression that downstream lookups should match against: the
    /// rewritten form once available, otherwise the original.
    pub fn current_expr(&self) -> &Expr {
        self.rewritten
            .as_ref()
            .map(|r| &r.expr)
            .unwrap_or(&self.original_expr)
    }
}

#[derive(Debug, Clone)]
pub enum SubqueryState {
    /// The subquery has not been evaluated yet.
    /// The 'plan' field is only optional because it is .take()'d when the the subquery
    /// is translated into bytecode.
    Unevaluated { plan: Option<Box<Plan>> },
    /// The subquery has been evaluated.
    /// The [evaluated_at] field contains the loop index where the subquery was evaluated.
    /// The query plan struct no longer exists because translating the plan currently
    /// requires an ownership transfer. We retain the outer table references so
    /// later masking/evaluation logic can still reason about dependencies.
    Evaluated {
        /// Join-loop position where the subquery was emitted into bytecode.
        evaluated_at: EvalAt,
        /// Outer table ids referenced by the subquery when it was planned.
        /// We keep these so later analysis can still understand dependencies
        /// even after the plan is consumed.
        outer_ref_ids: Vec<PlanSourceId>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryPosition {
    ResultColumn,
    Where,
    GroupBy,
    Having,
    OrderBy,
    LimitOffset,
}

/// Runtime shape chosen for a planned non-FROM subquery. Semantic HIR records
/// only scalar/EXISTS/IN meaning; register and cursor allocation lives here.
#[derive(Debug, Clone)]
pub enum PlanSubqueryType {
    Exists {
        result_reg: usize,
    },
    RowValue {
        result_reg_start: usize,
        num_regs: usize,
    },
    In {
        cursor_id: CursorID,
        affinity_str: Arc<String>,
    },
}

impl SubqueryPosition {
    /// Returns true if a subquery in this position of the SELECT can be correlated, i.e. if it can reference columns from the outer query.
    pub fn allow_correlated(&self) -> bool {
        matches!(
            self,
            SubqueryPosition::ResultColumn
                | SubqueryPosition::Where
                | SubqueryPosition::GroupBy
                | SubqueryPosition::OrderBy
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            SubqueryPosition::ResultColumn => "SELECT list",
            SubqueryPosition::Where => "WHERE",
            SubqueryPosition::GroupBy => "GROUP BY",
            SubqueryPosition::Having => "HAVING",
            SubqueryPosition::OrderBy => "ORDER BY",
            SubqueryPosition::LimitOffset => "LIMIT/OFFSET",
        }
    }
}

#[derive(Debug, Clone)]
/// A subquery that is not part of the `FROM` clause.
/// This is used for subqueries in the WHERE clause, HAVING clause, ORDER BY clause, LIMIT clause, OFFSET clause, etc.
/// Currently only subqueries in the WHERE clause are supported.
pub struct NonFromClauseSubquery {
    pub internal_id: PlanSubqueryId,
    pub query_type: PlanSubqueryType,
    /// Result metadata retained independently of `state.plan`, which is
    /// consumed when the subquery is emitted.
    pub output_facts: Vec<PlanOutputFact>,
    /// Values this child reads from result registers owned by enclosing query
    /// blocks. The definitions stay with those owners.
    pub outer_outputs: Vec<PlanOuterOutputReference>,
    pub state: SubqueryState,
    pub correlated: bool,
    pub origin: SubqueryOrigin,
    pub eval_phase: SubqueryEvalPhase,
}

impl PlanExprFactSource for &[NonFromClauseSubquery] {
    fn subquery_output_type_fact(&self, query: PlanSubqueryId, output: usize) -> Option<TypeFact> {
        self.iter()
            .find(|subquery| subquery.internal_id == query)?
            .output_facts
            .get(output)
            .map(|fact| fact.type_fact.clone())
    }

    fn subquery_width(&self, query: PlanSubqueryId) -> Option<usize> {
        self.iter()
            .find(|subquery| subquery.internal_id == query)
            .map(|subquery| subquery.output_facts.len())
    }

    fn subquery_output_affinity(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<PlanExprAffinity> {
        self.iter()
            .find(|subquery| subquery.internal_id == query)?
            .output_facts
            .get(output)
            .map(|fact| fact.affinity)
    }

    fn subquery_output_collation(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<CollationSeq> {
        self.iter()
            .find(|subquery| subquery.internal_id == query)?
            .output_facts
            .get(output)
            .and_then(|fact| fact.collation.as_ref())
            .map(|collation| *collation.value())
    }
}

impl NonFromClauseSubquery {
    /// Returns true if the subquery has been evaluated (translated into bytecode).
    pub fn has_been_evaluated(&self) -> bool {
        matches!(self.state, SubqueryState::Evaluated { .. })
    }

    pub fn is_post_write_returning(&self) -> bool {
        self.origin.is_post_write_returning()
            && matches!(self.eval_phase, SubqueryEvalPhase::PostWriteReturning)
    }

    pub fn reads_table(&self, database_id: usize, table_name: &str) -> bool {
        match &self.state {
            SubqueryState::Unevaluated { plan: Some(plan) } => {
                Plan::reads_table(plan, database_id, table_name)
            }
            _ => false,
        }
    }

    /// Returns the loop index where the subquery should be evaluated in this join order.
    ///
    /// If the subquery references tables from the parent query, it is evaluated at
    /// the right-most loop that makes those tables available. For hash joins, this
    /// may map a build-table reference to the probe loop where its rows are produced.
    pub fn get_eval_at(
        &self,
        join_order: &[JoinOrderMember],
        table_references: Option<&TableReferences>,
    ) -> Result<EvalAt> {
        let plan = match &self.state {
            SubqueryState::Unevaluated { plan } => plan.as_ref().unwrap(),
            SubqueryState::Evaluated { evaluated_at, .. } => {
                return Ok(*evaluated_at);
            }
        };
        let mut eval_at = eval_at_for_plan(plan, join_order, table_references)?;
        for output in &self.outer_outputs {
            for source in &output.source_dependencies {
                if let Some(loop_idx) = source_loop_index(*source, join_order, table_references)? {
                    eval_at = eval_at.max(EvalAt::Loop(loop_idx));
                }
            }
        }
        Ok(eval_at)
    }

    /// Consumes the plan and returns it, and sets the subquery to the evaluated state.
    ///
    /// This captures any outer references before the plan is moved so later
    /// phases can still reason about dependencies.
    pub fn consume_plan(&mut self, evaluated_at: EvalAt) -> Box<Plan> {
        match &mut self.state {
            SubqueryState::Unevaluated { plan } => {
                let outer_ref_ids = plan
                    .as_ref()
                    .map(|plan| plan.used_outer_query_ref_ids())
                    .unwrap_or_default();
                let plan = plan.take().unwrap();
                self.state = SubqueryState::Evaluated {
                    evaluated_at,
                    outer_ref_ids,
                };
                plan
            }
            SubqueryState::Evaluated { .. } => {
                panic!("subquery has already been evaluated");
            }
        }
    }
}

/// Determine the earliest evaluation point for a nested plan by walking all SELECT components.
fn eval_at_for_plan(
    plan: &Plan,
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    match plan {
        Plan::Select(select_plan) => {
            eval_at_for_select_plan(select_plan, join_order, table_references)
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut eval_at = EvalAt::BeforeLoop;
            for (select_plan, _) in left.iter() {
                eval_at = eval_at.max(eval_at_for_select_plan(
                    select_plan,
                    join_order,
                    table_references,
                )?);
            }
            eval_at = eval_at.max(eval_at_for_select_plan(
                right_most,
                join_order,
                table_references,
            )?);
            Ok(eval_at)
        }
        Plan::RecursiveCte(recursive_cte) => {
            let initial_query =
                eval_at_for_plan(&recursive_cte.initial_query, join_order, table_references)?;
            let recursive_query =
                eval_at_for_plan(&recursive_cte.recursive_query, join_order, table_references)?;
            Ok(initial_query.max(recursive_query))
        }
        Plan::Delete(_) | Plan::Update(_) => Ok(EvalAt::BeforeLoop),
    }
}

/// Returns true if a plan (including compound SELECTs) references outer-scope tables.
pub fn plan_is_correlated(plan: &Plan) -> bool {
    match plan {
        Plan::Select(select_plan) => select_plan.is_correlated(),
        Plan::CompoundSelect {
            left, right_most, ..
        } => left.iter().any(|(plan, _)| plan.is_correlated()) || right_most.is_correlated(),
        Plan::RecursiveCte(recursive_cte) => {
            plan_is_correlated(&recursive_cte.initial_query)
                || plan_is_correlated(&recursive_cte.recursive_query)
        }
        Plan::Delete(_) | Plan::Update(_) => false,
    }
}

fn select_plan_has_outer_scope_dependency_with_tables(
    plan: &SelectPlan,
    accessible_table_ids: &mut Vec<PlanSourceId>,
) -> bool {
    let outer_scope_base_len = accessible_table_ids.len();
    accessible_table_ids.extend(
        plan.table_references
            .joined_tables()
            .iter()
            .map(|table| table.internal_id),
    );

    let has_outer_scope_dependency =
        plan.table_references
            .outer_query_refs()
            .iter()
            .any(|outer_ref| {
                outer_ref.is_used() && !accessible_table_ids.contains(&outer_ref.internal_id)
            })
            || plan.non_from_clause_subqueries.iter().any(|subquery| {
                !subquery.outer_outputs.is_empty()
                    || match &subquery.state {
                        SubqueryState::Unevaluated {
                            plan: Some(subquery_plan),
                        } => plan_has_outer_scope_dependency_with_tables(
                            subquery_plan,
                            accessible_table_ids,
                        ),
                        SubqueryState::Unevaluated { plan: None } => false,
                        SubqueryState::Evaluated { outer_ref_ids, .. } => outer_ref_ids
                            .iter()
                            .any(|outer_ref_id| !accessible_table_ids.contains(outer_ref_id)),
                    }
            })
            || plan
                .table_references
                .joined_tables()
                .iter()
                .any(|table| match &table.table {
                    Table::FromClauseSubquery(subquery) => {
                        plan_has_outer_scope_dependency_with_tables(
                            subquery.plan.as_ref(),
                            accessible_table_ids,
                        )
                    }
                    _ => false,
                });

    accessible_table_ids.truncate(outer_scope_base_len);
    has_outer_scope_dependency
}

fn plan_has_outer_scope_dependency_with_tables(
    plan: &Plan,
    accessible_table_ids: &mut Vec<PlanSourceId>,
) -> bool {
    match plan {
        Plan::Select(select_plan) => {
            select_plan_has_outer_scope_dependency_with_tables(select_plan, accessible_table_ids)
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            left.iter().any(|(select_plan, _)| {
                select_plan_has_outer_scope_dependency_with_tables(
                    select_plan,
                    accessible_table_ids,
                )
            }) || select_plan_has_outer_scope_dependency_with_tables(
                right_most,
                accessible_table_ids,
            )
        }
        Plan::RecursiveCte(recursive_cte) => {
            plan_has_outer_scope_dependency_with_tables(
                &recursive_cte.initial_query,
                accessible_table_ids,
            ) || plan_has_outer_scope_dependency_with_tables(
                &recursive_cte.recursive_query,
                accessible_table_ids,
            )
        }
        Plan::Delete(_) | Plan::Update(_) => false,
    }
}

/// Returns true when evaluating this plan depends on table values from an
/// enclosing query scope outside the plan itself.
///
/// This is narrower than [`plan_is_correlated()`]: a plan may contain
/// internally correlated scalar subqueries (for example, a scalar subquery that
/// references another table in the same CTE) without depending on an enclosing
/// query row. Those plans are still safe to materialize once and reuse.
pub fn plan_has_outer_scope_dependency(plan: &Plan) -> bool {
    plan_has_outer_scope_dependency_with_tables(plan, &mut Vec::new())
}

pub fn select_plan_has_outer_scope_dependency(plan: &SelectPlan) -> bool {
    select_plan_has_outer_scope_dependency_with_tables(plan, &mut Vec::new())
}

/// Determine when a SELECT plan can be evaluated, including nested non-FROM and FROM-clause subqueries.
fn eval_at_for_select_plan(
    plan: &SelectPlan,
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    let mut eval_at = EvalAt::BeforeLoop;
    let used_outer_refs = plan
        .table_references
        .outer_query_refs()
        .iter()
        .filter(|t| t.is_used());

    for outer_ref in used_outer_refs {
        if let Some(loop_idx) =
            resolve_outer_ref_loop(outer_ref.internal_id, join_order, table_references)
        {
            eval_at = eval_at.max(EvalAt::Loop(loop_idx));
        }
    }
    for subquery in plan.non_from_clause_subqueries.iter() {
        let eval_at_inner = subquery.get_eval_at(join_order, table_references)?;
        eval_at = eval_at.max(eval_at_inner);
    }
    for joined_table in plan.table_references.joined_tables().iter() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &joined_table.table {
            eval_at = eval_at.max(eval_at_for_plan(
                from_clause_subquery.plan.as_ref(),
                join_order,
                table_references,
            )?);
        }
    }
    Ok(eval_at)
}

/// Resolves the loop index for an outer-table reference.
///
/// If the table is not present in the join order, we look for a hash join
/// where that table is the build side and map it to the probe loop.
fn resolve_outer_ref_loop(
    table_id: PlanSourceId,
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
) -> Option<usize> {
    if let Some(loop_idx) = join_order.iter().position(|t| t.table_id == table_id) {
        return Some(loop_idx);
    }
    let tables = table_references?;
    for (probe_idx, member) in join_order.iter().enumerate() {
        let probe_table = &tables.joined_tables()[member.original_idx];
        if let Operation::HashJoin(ref hj) = probe_table.op {
            let build_table = &tables.joined_tables()[hj.build_table_idx];
            if build_table.internal_id == table_id {
                return Some(probe_idx);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use crate::alloc::TursoFromIterator;

    use super::*;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    type TestResult = std::result::Result<(), alloc::TryReserveError>;

    #[test]
    fn test_column_used_mask_empty() -> TestResult {
        let mask = ColumnUsedMask::default();
        assert!(mask.is_empty());

        let mut mask2 = ColumnUsedMask::default();
        mask2.set(0)?;
        assert!(!mask2.is_empty());
        Ok(())
    }

    #[test]
    fn test_column_used_mask_set_and_get() -> TestResult {
        let mut mask = ColumnUsedMask::default();

        let max_columns = 10000;
        let mut set_indices = Vec::new();
        let mut rng = ChaCha8Rng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        for i in 0..max_columns {
            if rng.next_u32() % 3 == 0 {
                set_indices.push(i);
                mask.set(i)?;
            }
        }

        // Verify set bits are present
        for &i in &set_indices {
            assert!(mask.get(i), "Expected bit {i} to be set");
        }

        // Verify unset bits are not present
        for i in 0..max_columns {
            if !set_indices.contains(&i) {
                assert!(!mask.get(i), "Expected bit {i} to not be set");
            }
        }
        Ok(())
    }

    #[test]
    fn test_column_used_mask_subset_relationship() -> TestResult {
        let mut full_mask = ColumnUsedMask::default();
        let mut subset_mask = ColumnUsedMask::default();

        let max_columns = 5000;
        let mut rng = ChaCha8Rng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        // Create a pattern where subset has fewer bits
        for i in 0..max_columns {
            if rng.next_u32() % 5 == 0 {
                full_mask.set(i)?;
                if i % 2 == 0 {
                    subset_mask.set(i)?;
                }
            }
        }

        // full_mask contains all bits of subset_mask
        assert!(full_mask.contains_all_set_bits_of(&subset_mask));

        // subset_mask does not contain all bits of full_mask
        assert!(!subset_mask.contains_all_set_bits_of(&full_mask));

        // A mask contains itself
        assert!(full_mask.contains_all_set_bits_of(&full_mask));
        assert!(subset_mask.contains_all_set_bits_of(&subset_mask));
        Ok(())
    }

    #[test]
    fn test_column_used_mask_empty_subset() -> TestResult {
        let mut mask = ColumnUsedMask::default();
        for i in (0..1000).step_by(7) {
            mask.set(i)?;
        }

        let empty_mask = ColumnUsedMask::default();

        // Empty mask is subset of everything
        assert!(mask.contains_all_set_bits_of(&empty_mask));
        assert!(empty_mask.contains_all_set_bits_of(&empty_mask));
        Ok(())
    }

    #[test]
    fn test_column_used_mask_sparse_indices() -> TestResult {
        let mut sparse_mask = ColumnUsedMask::default();

        // Test with very sparse, large indices
        let sparse_indices = vec![0, 137, 1042, 5389, 10000, 50000, 100000, 500000, 1000000];

        for &idx in &sparse_indices {
            sparse_mask.set(idx)?;
        }

        for &idx in &sparse_indices {
            assert!(sparse_mask.get(idx), "Expected bit {idx} to be set");
        }

        // Check some indices that shouldn't be set
        let unset_indices = vec![1, 100, 1000, 5000, 25000, 75000, 250000, 750000];
        for &idx in &unset_indices {
            assert!(!sparse_mask.get(idx), "Expected bit {idx} to not be set");
        }

        assert!(!sparse_mask.is_empty());
        Ok(())
    }

    #[test]
    fn test_column_used_mask_clear() -> TestResult {
        let mut mask = ColumnUsedMask::default();

        // Test inline clear
        mask.set(5)?;
        mask.set(10)?;
        assert!(mask.get(5));
        mask.clear(5);
        assert!(!mask.get(5));
        assert!(mask.get(10));

        // Test overflow clear
        mask.set(100)?;
        mask.set(200)?;
        assert!(mask.get(100));
        mask.clear(100);
        assert!(!mask.get(100));
        assert!(mask.get(200));

        // Clear non-existent bit should be no-op
        mask.clear(999);
        assert!(!mask.get(999));
        Ok(())
    }

    #[test]
    fn test_column_used_mask_is_only() -> TestResult {
        // Test inline is_only
        let mut mask = ColumnUsedMask::default();
        mask.set(5)?;
        assert!(mask.is_only(5));
        assert!(!mask.is_only(0));
        assert!(!mask.is_only(100));

        mask.set(10)?;
        assert!(!mask.is_only(5));
        assert!(!mask.is_only(10));

        // Test overflow is_only
        let mut mask2 = ColumnUsedMask::default();
        mask2.set(100)?;
        assert!(mask2.is_only(100));
        assert!(!mask2.is_only(0));
        assert!(!mask2.is_only(50));

        mask2.set(200)?;
        assert!(!mask2.is_only(100));

        // Test empty mask
        let empty = ColumnUsedMask::default();
        assert!(!empty.is_only(0));
        assert!(!empty.is_only(100));
        Ok(())
    }

    #[test]
    fn test_column_used_mask_subtract() -> TestResult {
        let mut mask1 = ColumnUsedMask::default();
        let mut mask2 = ColumnUsedMask::default();

        // Set up mask1 with inline and overflow bits
        for i in [1, 5, 10, 63, 64, 100, 200] {
            mask1.set(i)?;
        }

        // Set up mask2 with some overlapping bits
        for i in [5, 10, 100] {
            mask2.set(i)?;
        }

        mask1.subtract(&mask2);

        // Should remain
        assert!(mask1.get(1));
        assert!(mask1.get(63));
        assert!(mask1.get(64));
        assert!(mask1.get(200));

        // Should be cleared
        assert!(!mask1.get(5));
        assert!(!mask1.get(10));
        assert!(!mask1.get(100));
        Ok(())
    }

    #[test]
    fn test_column_used_mask_iter() -> TestResult {
        let mut mask = ColumnUsedMask::default();
        let indices = vec![0, 5, 63, 64, 65, 127, 128, 200, 1000];

        for &i in &indices {
            mask.set(i)?;
        }

        let collected: Vec<usize> = mask.iter().collect();
        assert_eq!(collected, indices);

        // Empty mask iter
        let empty = ColumnUsedMask::default();
        assert_eq!(empty.iter().count(), 0);
        Ok(())
    }

    #[test]
    fn test_column_used_mask_bitor_assign() -> TestResult {
        let mut mask1 = ColumnUsedMask::default();
        let mut mask2 = ColumnUsedMask::default();

        // Inline bits
        mask1.set(1)?;
        mask1.set(5)?;
        mask2.set(5)?;
        mask2.set(10)?;

        // Overflow bits
        mask1.set(100)?;
        mask2.set(200)?;

        mask1.union_with(&mask2)?;

        assert!(mask1.get(1));
        assert!(mask1.get(5));
        assert!(mask1.get(10));
        assert!(mask1.get(100));
        assert!(mask1.get(200));

        // mask2 should be unchanged
        assert!(!mask2.get(1));
        assert!(mask2.get(5));
        assert!(mask2.get(10));
        assert!(!mask2.get(100));
        assert!(mask2.get(200));
        Ok(())
    }

    #[test]
    fn test_column_used_mask_boundary_conditions() -> TestResult {
        let mut mask = ColumnUsedMask::default();

        // Test at inline/overflow boundary
        mask.set(63)?; // last inline bit
        mask.set(64)?; // first overflow bit

        assert!(mask.get(63));
        assert!(mask.get(64));
        assert!(!mask.get(62));
        assert!(!mask.get(65));

        // Test is_only at boundary
        let mut mask2 = ColumnUsedMask::default();
        mask2.set(63)?;
        assert!(mask2.is_only(63));

        let mut mask3 = ColumnUsedMask::default();
        mask3.set(64)?;
        assert!(mask3.is_only(64));
        Ok(())
    }

    #[test]
    fn test_column_mask_rowid_sentinel() -> TestResult {
        // ColumnMask stores `usize::MAX` (ROWID_SENTINEL) in an out-of-band bool
        // so that the underlying dense BitSet never sees it. The small API surface
        // that ColumnMask exposes must all honor the sentinel consistently.

        // set / get round-trip on the sentinel alone
        let mut mask = ColumnMask::default();
        assert!(!mask.get(usize::MAX));
        mask.set(usize::MAX)?;
        assert!(mask.get(usize::MAX));
        assert_eq!(mask.count(), 1);

        // sentinel coexists with dense bits
        let mut mixed = ColumnMask::default();
        mixed.set(0)?;
        mixed.set(63)?;
        mixed.set(64)?; // crosses into overflow
        mixed.set(500)?;
        mixed.set(usize::MAX)?;
        assert!(mixed.get(0));
        assert!(mixed.get(63));
        assert!(mixed.get(64));
        assert!(mixed.get(500));
        assert!(mixed.get(usize::MAX));
        assert_eq!(mixed.count(), 5);

        // iter yields dense positions in ascending order, then usize::MAX at the end
        let collected: Vec<usize> = (&mixed).into_iter().collect();
        assert_eq!(collected, vec![0, 63, 64, 500, usize::MAX]);
        // count() and iter().count() must agree
        assert_eq!(mixed.count(), (&mixed).into_iter().count());

        // fallible collection round-trip through the sentinel
        let built = ColumnMask::try_from_iter([0usize, 63, 64, 500, usize::MAX])?;
        assert_eq!(built, mixed);
        let round = ColumnMask::try_from_iter(&mixed)?;
        assert_eq!(round, mixed);
        let mut extended = ColumnMask::default();
        extended.try_extend([0usize, 63, 64, 500, usize::MAX])?;
        assert_eq!(extended, mixed);

        // owned IntoIterator (used by flat_map in the UPDATE emitter)
        let mixed_owned: Vec<usize> = mixed.clone().into_iter().collect();
        assert_eq!(mixed_owned, vec![0, 63, 64, 500, usize::MAX]);
        Ok(())
    }

    fn rng_from_env_or_time() -> (ChaCha8Rng, u64) {
        let seed = std::env::var("TEST_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        (ChaCha8Rng::seed_from_u64(seed), seed)
    }

    /// Reference implementation using BTreeSet for correctness comparison
    struct ReferenceMask(std::collections::BTreeSet<usize>);

    impl ReferenceMask {
        fn new() -> Self {
            Self(std::collections::BTreeSet::new())
        }
        fn set(&mut self, index: usize) {
            self.0.insert(index);
        }
        fn get(&self, index: usize) -> bool {
            self.0.contains(&index)
        }
        fn clear(&mut self, index: usize) {
            self.0.remove(&index);
        }
        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
        fn is_only(&self, index: usize) -> bool {
            self.0.len() == 1 && self.0.contains(&index)
        }
        fn contains_all_set_bits_of(&self, other: &Self) -> bool {
            other.0.is_subset(&self.0)
        }
        fn subtract(&mut self, other: &Self) {
            for &idx in &other.0 {
                self.0.remove(&idx);
            }
        }
        fn bitor_assign(&mut self, other: &Self) {
            for &idx in &other.0 {
                self.0.insert(idx);
            }
        }
    }

    #[test]
    fn test_column_used_mask_fuzz() -> TestResult {
        fn pick_index(rng: &mut ChaCha8Rng, max_index: u32) -> usize {
            (rng.next_u32() % max_index) as usize
        }

        let (mut rng, seed) = rng_from_env_or_time();
        eprintln!("test_column_used_mask_random_ops seed: {seed}");

        let mut mask = ColumnUsedMask::default();
        let mut reference = ReferenceMask::new();

        let num_ops = 100000;
        let max_index = 4096;

        for _ in 0..num_ops {
            let op = rng.next_u32() % 10;
            let idx = pick_index(&mut rng, max_index);

            match op {
                0..=2 => {
                    // Set (more frequent)
                    mask.set(idx)?;
                    reference.set(idx);
                }
                3 => {
                    // Get
                    assert_eq!(
                        mask.get(idx),
                        reference.get(idx),
                        "get({idx}) mismatch, seed={seed}"
                    );
                }
                4 => {
                    // Clear
                    mask.clear(idx);
                    reference.clear(idx);
                }
                5 => {
                    // IsEmpty
                    assert_eq!(
                        mask.is_empty(),
                        reference.is_empty(),
                        "is_empty mismatch, seed={seed}"
                    );
                }
                6 => {
                    // IsOnly
                    assert_eq!(
                        mask.is_only(idx),
                        reference.is_only(idx),
                        "is_only({idx}) mismatch, seed={seed}"
                    );
                }
                7 => {
                    // ContainsAllSetBitsOf with random other mask
                    let mut other_mask = ColumnUsedMask::default();
                    let mut other_ref = ReferenceMask::new();
                    for _ in 0..(rng.next_u32() % 20) {
                        let other_idx = pick_index(&mut rng, max_index);
                        other_mask.set(other_idx)?;
                        other_ref.set(other_idx);
                    }
                    assert_eq!(
                        mask.contains_all_set_bits_of(&other_mask),
                        reference.contains_all_set_bits_of(&other_ref),
                        "contains_all_set_bits_of mismatch, seed={seed}"
                    );
                }
                8 => {
                    // BitOrAssign with random other mask
                    let mut other_mask = ColumnUsedMask::default();
                    let mut other_ref = ReferenceMask::new();
                    for _ in 0..(rng.next_u32() % 20) {
                        let other_idx = pick_index(&mut rng, max_index);
                        other_mask.set(other_idx)?;
                        other_ref.set(other_idx);
                    }
                    mask.union_with(&other_mask)?;
                    reference.bitor_assign(&other_ref);
                }
                9 => {
                    // Subtract with random other mask
                    let mut other_mask = ColumnUsedMask::default();
                    let mut other_ref = ReferenceMask::new();
                    for _ in 0..(rng.next_u32() % 20) {
                        let other_idx = pick_index(&mut rng, max_index);
                        other_mask.set(other_idx)?;
                        other_ref.set(other_idx);
                    }
                    mask.subtract(&other_mask);
                    reference.subtract(&other_ref);
                }
                _ => unreachable!(),
            }
        }

        // Final verification: iter should produce same results
        let mask_set: std::collections::BTreeSet<usize> = mask.iter().collect();
        assert_eq!(mask_set, reference.0, "final iter mismatch, seed={seed}");
        Ok(())
    }

    #[test]
    fn test_bitset_properties_fuzz() -> TestResult {
        fn sample_other(
            rng: &mut ChaCha8Rng,
            max_index: usize,
        ) -> Result<(BitSet, std::collections::BTreeSet<usize>), alloc::TryReserveError> {
            let mut m = BitSet::default();
            let mut r = std::collections::BTreeSet::new();
            for _ in 0..(rng.next_u32() % 20) {
                let i = (rng.next_u32() as usize) % max_index;
                m.set(i)?;
                r.insert(i);
            }
            Ok((m, r))
        }

        let (mut rng, seed) = rng_from_env_or_time();
        eprintln!("test_bitset_properties_fuzz seed: {seed}");

        let mut mask = BitSet::default();
        let mut reference = std::collections::BTreeSet::<usize>::new();
        let max_index: usize = 2048;
        let num_ops = 30_000;

        for step in 0..num_ops {
            let op = rng.next_u32() % 16;
            let idx = (rng.next_u32() as usize) % max_index;

            match op {
                0..=3 => {
                    // Set (weighted to grow the set)
                    mask.set(idx)?;
                    reference.insert(idx);
                }
                4 => {
                    // Clear
                    mask.clear(idx);
                    reference.remove(&idx);
                }
                5 => {
                    // count() agrees with reference size
                    assert_eq!(
                        mask.count(),
                        reference.len(),
                        "step={step} seed={seed} op=count"
                    );
                }
                6 => {
                    // rank(k) agrees with |{x in ref : x < k}|
                    let expected = reference.range(..idx).count();
                    assert_eq!(
                        mask.rank(idx),
                        expected,
                        "step={step} seed={seed} op=rank({idx})"
                    );
                }
                7 => {
                    // intersects() agrees with BTreeSet intersection
                    let (other_mask, other_ref) = sample_other(&mut rng, max_index)?;
                    let expected = reference.intersection(&other_ref).next().is_some();
                    assert_eq!(
                        mask.intersects(&other_mask),
                        expected,
                        "step={step} seed={seed} op=intersects"
                    );
                    // Symmetry: intersects is commutative
                    assert_eq!(
                        other_mask.intersects(&mask),
                        expected,
                        "step={step} seed={seed} op=intersects-symmetric"
                    );
                }
                8 => {
                    // Fallible collection: building a fresh BitSet from the reference
                    // must compare equal to the mask.
                    let built = BitSet::try_from_iter(reference.iter().copied())?;
                    assert_eq!(built, mask, "step={step} seed={seed} op=try_from_iter");
                }
                9 => {
                    // iter() -> try_from_iter() round trip is the identity
                    let round = BitSet::try_from_iter(mask.iter())?;
                    assert_eq!(round, mask, "step={step} seed={seed} op=iter-roundtrip");

                    // iter() yields bits in strictly increasing order, matching the reference
                    let collected: Vec<usize> = mask.iter().collect();
                    for w in collected.windows(2) {
                        assert!(
                            w[0] < w[1],
                            "step={step} seed={seed} iter not strictly increasing"
                        );
                    }
                    let ref_vec: Vec<usize> = reference.iter().copied().collect();
                    assert_eq!(
                        collected, ref_vec,
                        "step={step} seed={seed} iter contents vs ref"
                    );
                }
                10 => {
                    // TryFrom<u128>: sample a random u128, verify per-bit and count
                    let val = ((rng.next_u32() as u128) << 96)
                        | ((rng.next_u32() as u128) << 64)
                        | ((rng.next_u32() as u128) << 32)
                        | (rng.next_u32() as u128);
                    let bs = BitSet::try_from(val)?;
                    assert_eq!(
                        bs.count(),
                        val.count_ones() as usize,
                        "step={step} seed={seed} TryFrom<u128>({val:#x}) count"
                    );
                    for i in 0..128 {
                        let expected = (val >> i) & 1 != 0;
                        assert_eq!(
                            bs.get(i),
                            expected,
                            "step={step} seed={seed} TryFrom<u128>({val:#x}) get({i})"
                        );
                    }
                    // Path equivalence: same bits via set() must compare equal
                    let mut manual = BitSet::default();
                    for i in 0..128 {
                        if (val >> i) & 1 != 0 {
                            manual.set(i)?;
                        }
                    }
                    assert_eq!(
                        bs, manual,
                        "step={step} seed={seed} TryFrom<u128>({val:#x}) vs manual"
                    );
                    // TryFrom<u128>(0) must equal default (equality anchor)
                    assert_eq!(
                        BitSet::<usize>::try_from(0u128)?,
                        BitSet::<usize>::default(),
                        "step={step} seed={seed} TryFrom<u128>(0) != default"
                    );
                }
                11 => {
                    // SubAssign (delegates to subtract)
                    let (other_mask, other_ref) = sample_other(&mut rng, max_index)?;
                    mask -= &other_mask;
                    for i in &other_ref {
                        reference.remove(i);
                    }
                }
                12 => {
                    // union_with
                    let (other_mask, other_ref) = sample_other(&mut rng, max_index)?;
                    mask.union_with(&other_mask)?;
                    for i in other_ref {
                        reference.insert(i);
                    }
                }
                13 => {
                    // Cross-method: count() == iter().count() == rank(usize::MAX)
                    let c = mask.count();
                    assert_eq!(
                        c,
                        mask.iter().count(),
                        "step={step} seed={seed} count vs iter().count()"
                    );
                    assert_eq!(
                        c,
                        mask.rank(usize::MAX),
                        "step={step} seed={seed} count vs rank(MAX)"
                    );
                }
                14 => {
                    // Cross-method: contains_all(other) && !other.is_empty() => intersects(other)
                    let (other_mask, other_ref) = sample_other(&mut rng, max_index)?;
                    if mask.contains_all_set_bits_of(&other_mask) && !other_ref.is_empty() {
                        assert!(
                            mask.intersects(&other_mask),
                            "step={step} seed={seed} contains_all should imply intersects"
                        );
                    }
                }
                15 => {
                    // Cross-method: is_empty() iff count() == 0
                    assert_eq!(
                        mask.is_empty(),
                        mask.count() == 0,
                        "step={step} seed={seed} is_empty vs count==0"
                    );
                    assert_eq!(
                        mask.is_empty(),
                        reference.is_empty(),
                        "step={step} seed={seed} is_empty vs ref"
                    );
                }
                _ => unreachable!(),
            }
        }

        // Final verification: complete iter vs reference, and count agreement
        let collected: std::collections::BTreeSet<usize> = mask.iter().collect();
        assert_eq!(collected, reference, "final iter mismatch, seed={seed}");
        assert_eq!(
            mask.count(),
            reference.len(),
            "final count mismatch, seed={seed}"
        );
        Ok(())
    }

    #[test]
    fn test_bitset_with_table_internal_id() -> TestResult {
        let a = PlanSourceId::new(3);
        let b = PlanSourceId::new(70); // exercises overflow path
        let c = PlanSourceId::new(200);

        let mut mask: BitSet<PlanSourceId> = BitSet::default();
        mask.set(a)?;
        mask.set(b)?;
        mask.set(c)?;

        assert!(mask.get(a));
        assert!(mask.get(b));
        assert!(mask.get(c));
        assert!(!mask.get(PlanSourceId::new(4)));
        assert_eq!(mask.count(), 3);

        mask.clear(b);
        assert!(!mask.get(b));
        assert_eq!(mask.count(), 2);

        // Iterator yields PlanSourceId, not usize.
        let collected: Vec<PlanSourceId> = (&mask).into_iter().collect();
        assert_eq!(collected, vec![a, c]);

        // Fallible collection preserves PlanSourceId.
        let rebuilt = BitSet::<PlanSourceId>::try_from_iter([a, c])?;
        assert_eq!(rebuilt, mask);
        let mut extended = BitSet::<PlanSourceId>::default();
        extended.try_extend([a, c])?;
        assert_eq!(extended, mask);
        Ok(())
    }

    #[test]
    fn test_column_mask_sub_assign() -> TestResult {
        let mut a = ColumnMask::try_from_iter([1, 3, ROWID_SENTINEL])?;
        let b = ColumnMask::try_from_iter([3, ROWID_SENTINEL])?;
        a -= &b;
        assert!(a.get(1));
        assert!(!a.get(3));
        assert!(!a.get(ROWID_SENTINEL));
        assert_eq!(a.count(), 1);

        // Subtracting without rowid sentinel leaves it intact
        let mut a = ColumnMask::try_from_iter([2, 4, ROWID_SENTINEL])?;
        let b = ColumnMask::try_from_iter([2])?;
        a -= &b;
        assert!(!a.get(2));
        assert!(a.get(4));
        assert!(a.get(ROWID_SENTINEL));
        assert_eq!(a.count(), 2);
        Ok(())
    }
}
