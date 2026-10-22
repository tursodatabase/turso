use crate::schema::Table;
use crate::{
    schema::{Index, Schema},
    translate::{
        collate::CollationSeq,
        optimizer::access_method::AccessMethodParams,
        optimizer::constraints::RangeConstraintRef,
        plan::{HirPlanSource, HirPlannedSource, IterationDirection},
        planner::TableMask,
        semantic::hir,
    },
    LimboError, Result,
};
use turso_parser::ast::{self, SortOrder};

use super::{access_method::AccessMethod, cost::index_access_is_unique_point_lookup, join::JoinN};

/// Target component in an ORDER BY/GROUP BY that may be a plain column or an expression.
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnTarget<E> {
    Column(usize),
    RowId,
    Expr(E),
}

/// A convenience struct for representing a (table_no, column_target, [SortOrder]) tuple.
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnOrder<I, E> {
    pub source: I,
    pub target: ColumnTarget<E>,
    pub order: SortOrder,
    pub collation: CollationSeq,
    pub nulls_order: Option<ast::NullsOrder>,
}

impl<I, E> ColumnOrder<I, E> {
    pub fn effective_nulls_order_when_iterated(
        &self,
        iter_dir: IterationDirection,
    ) -> ast::NullsOrder {
        match iter_dir {
            IterationDirection::Forwards => self.effective_nulls_order(),
            IterationDirection::Backwards => self.effective_nulls_order().reverse(),
        }
    }

    pub fn effective_nulls_order(&self) -> ast::NullsOrder {
        self.nulls_order
            .unwrap_or_else(|| ast::NullsOrder::default_for(self.order))
    }
}

#[derive(Debug, PartialEq, Clone)]
/// If an [OrderTarget] is satisfied, then [EliminatesSort] describes which part
/// of the query no longer requires sorting.
pub enum EliminatesSortBy {
    Group,
    Order,
    GroupByAndOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderTargetPurpose {
    /// Matching this target lets the planner eliminate a later ORDER BY and/or
    /// GROUP BY sort step.
    EliminatesSort(EliminatesSortBy),
    /// Matching this target enables an extremum fast path, analogous to
    /// SQLite's WHERE_ORDERBY_MIN/MAX planning mode.
    Extremum,
}

#[derive(Debug, PartialEq, Clone)]
/// An [OrderTarget] is considered in join optimization and index selection,
/// so that if a given join ordering and its access methods satisfy the [OrderTarget],
/// then the join ordering and its access methods are preferred, all other things being equal.
pub struct OrderTarget<I, E> {
    pub columns: Vec<ColumnOrder<I, E>>,
    pub purpose: OrderTargetPurpose,
}

pub type HirOrderTarget<'a> = OrderTarget<
    crate::translate::semantic::hir::SourceId,
    &'a crate::translate::semantic::hir::Expr,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqualityPrefixScope {
    /// Candidate scoring may skip any equality-constrained seek prefix because
    /// it only reasons about the order within that specific seek.
    AnyEquality,
    /// Final ORDER BY / GROUP BY elimination may only skip globally constant
    /// equality prefixes. Join-dependent equalities vary per outer row and do
    /// not guarantee a globally ordered concatenation of inner scans.
    ConstantEquality,
}

impl<I, E> OrderTarget<I, E> {
    pub fn is_extremum(&self) -> bool {
        matches!(self.purpose, OrderTargetPurpose::Extremum)
    }
}

/// Prove that the selected HIR B-tree loops supply the complete requested
/// order. Plans with
/// an outer join or a loop that is not a plain B-tree access keep their
/// sorter.
pub(crate) fn hir_plan_satisfies_order_target(
    plan: &JoinN,
    access_methods: &[AccessMethod],
    sources: &[HirPlanSource],
    document: &hir::HirDocument,
    order_target: &HirOrderTarget<'_>,
    schema: &Schema,
) -> Result<bool> {
    // A hash build is read into an unordered hash table, so its scan order
    // never orders the output; the loops before the build keep theirs. A
    // build input folds the loops before the probe into the table, so none
    // of them orders the output, and an outer hash join emits its unmatched
    // build rows in hash order.
    let mut hash_build_sources = TableMask::default();
    for (_, method) in &plan.data {
        if let Some(AccessMethodParams::HashJoin {
            build_table_idx,
            join_type,
            materialize_build_input,
            ..
        }) = access_methods.get(*method).map(|method| &method.params)
        {
            hash_build_sources.set(*build_table_idx)?;
            if *materialize_build_input || join_type.keeps_unmatched_build_rows() {
                return Ok(false);
            }
        }
    }
    // A RIGHT or FULL join adds its unmatched rows after every loop ends.
    let join_kind = |source_position: usize| {
        sources
            .get(source_position)
            .and_then(|source| source.join_info())
            .map(|join| join.kind)
    };
    if plan.data.iter().any(|(source_position, _)| {
        matches!(
            join_kind(*source_position),
            Some(hir::JoinKind::Right | hir::JoinKind::Full)
        )
    }) {
        return Ok(false);
    }
    let columns = order_target.columns.len();
    let mut consumed_columns = 0;
    for (loop_position, (source_position, access_method_position)) in plan.data.iter().enumerate() {
        if hash_build_sources.get(*source_position) {
            return Ok(false);
        }
        // A LEFT JOIN adds a NULL row after its scan or seek, so
        // its order cannot give the terms that read its own columns. The
        // terms before them keep the order of the loops before it.
        if join_kind(*source_position) == Some(hir::JoinKind::Left)
            && order_target.columns[consumed_columns..]
                .iter()
                .any(|column| {
                    sources
                        .get(*source_position)
                        .is_some_and(|source| source.source() == column.source)
                })
        {
            return Ok(false);
        }
        let source = match sources.get(*source_position) {
            Some(HirPlanSource::BTree(source)) => source,
            // A FROM subquery or CTE that is scanned returns its rows in its
            // own order.
            Some(
                HirPlanSource::Derived {
                    source: subquery_source,
                    output_order,
                    ..
                }
                | HirPlanSource::Cte {
                    source: subquery_source,
                    output_order,
                    ..
                },
            ) => {
                let Some(access_method) = access_methods.get(*access_method_position) else {
                    return Ok(false);
                };
                let consumed = match &access_method.params {
                    AccessMethodParams::Subquery { iter_dir } => OrderConsumption {
                        consumed: hir_subquery_intrinsic_order_consumed(
                            *subquery_source,
                            output_order,
                            *iter_dir,
                            &order_target.columns[consumed_columns..],
                        ),
                        includes_rowid: false,
                    },
                    // Read through its temporary index, the subquery's rows
                    // come out in the index's order after the seek prefix.
                    AccessMethodParams::MaterializedSubquery {
                        index,
                        constraint_refs,
                        iter_dir,
                    } => hir_materialized_subquery_order_consumed(
                        *subquery_source,
                        index,
                        constraint_refs,
                        *iter_dir,
                        order_target,
                        consumed_columns,
                        schema,
                        EqualityPrefixScope::ConstantEquality,
                    ),
                    _ => return Ok(false),
                };
                if consumed.consumed == 0 {
                    return Ok(false);
                }
                consumed_columns += consumed.consumed;
                if consumed_columns == columns {
                    return Ok(true);
                }
                // The loop may repeat the values ordered so far, unless it
                // finds at most one row.
                let next_comes_from_later_loop = order_target
                    .columns
                    .get(consumed_columns)
                    .is_some_and(|column| {
                        plan.data[loop_position + 1..].iter().any(|(later, _)| {
                            sources
                                .get(*later)
                                .is_some_and(|later| later.source() == column.source)
                        })
                    });
                if next_comes_from_later_loop
                    && !access_method_emits_unique_order_prefix(access_method, &consumed)
                {
                    return Ok(false);
                }
                continue;
            }
            _ => return Ok(false),
        };
        let Some(definition) = document.source(source.internal_id) else {
            return Err(LimboError::InternalError(format!(
                "HIR order proof references missing source {}",
                source.internal_id
            )));
        };
        let Some(access_method) = access_methods.get(*access_method_position) else {
            return Err(LimboError::InternalError(format!(
                "HIR order proof references missing access method {access_method_position}"
            )));
        };
        let AccessMethodParams::BTreeTable {
            iter_dir,
            index,
            build_index: false,
            constraint_refs,
        } = &access_method.params
        else {
            return Ok(false);
        };
        let consumed = hir_btree_access_order_consumed(
            &HirOrderSource::new(source, definition),
            *iter_dir,
            index.as_deref(),
            constraint_refs,
            order_target,
            consumed_columns,
            schema,
            EqualityPrefixScope::ConstantEquality,
        )?;
        if consumed.consumed == 0 {
            return Ok(false);
        }
        consumed_columns += consumed.consumed;
        if consumed_columns == columns {
            return Ok(true);
        }
        // A later loop can supply the next ORDER BY term only
        // if this loop never repeats the values ordered so far, or the inner
        // loop would restart its order for each repeat.
        let next_comes_from_later_loop =
            order_target
                .columns
                .get(consumed_columns)
                .is_some_and(|column| {
                    plan.data[loop_position + 1..].iter().any(|(later, _)| {
                        sources
                            .get(*later)
                            .is_some_and(|later| later.source() == column.source)
                    })
                });
        if next_comes_from_later_loop
            && !access_method_emits_unique_order_prefix(access_method, &consumed)
        {
            return Ok(false);
        }
    }
    Ok(consumed_columns == columns)
}

fn access_method_emits_unique_order_prefix(
    access_method: &AccessMethod,
    order_consumption: &OrderConsumption,
) -> bool {
    // Rows always differ in rowid, so a consumed prefix that includes the
    // rowid never repeats.
    if order_consumption.includes_rowid {
        return true;
    }
    // A complete lookup on a UNIQUE index also returns at most one row.
    // Each key column must use `=`. `IS` can match many NULL entries.
    match &access_method.params {
        AccessMethodParams::BTreeTable {
            index,
            build_index,
            constraint_refs,
            ..
        } => {
            !*build_index && index_access_is_unique_point_lookup(index.as_deref(), constraint_refs)
        }
        AccessMethodParams::MaterializedSubquery {
            index,
            constraint_refs,
            ..
        } => index_access_is_unique_point_lookup(Some(index.as_ref()), constraint_refs),
        AccessMethodParams::Subquery { .. }
        | AccessMethodParams::HashJoin { .. }
        | AccessMethodParams::VirtualTable { .. }
        | AccessMethodParams::InSeek { .. } => false,
    }
}

/// One output column of a planned HIR query, in the order the query returns
/// its rows: rows are ordered by the first entry, then the second, and so on.
/// `nulls` is the effective NULLS placement, so a later reversal flips it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HirOutputOrder {
    pub(crate) column: usize,
    pub(crate) order: SortOrder,
    pub(crate) collation: CollationSeq,
    pub(crate) nulls: ast::NullsOrder,
}

/// How many leading `target` columns a HIR FROM subquery or CTE returns its
/// rows in when read in `iter_dir`. `output_order` is the order the
/// subquery's plan returns its rows in; see `HirPlannedQuery`.
pub(crate) fn hir_subquery_intrinsic_order_consumed(
    source: hir::SourceId,
    output_order: &[HirOutputOrder],
    iter_dir: IterationDirection,
    target: &[ColumnOrder<hir::SourceId, &hir::Expr>],
) -> usize {
    let intrinsic = output_order
        .iter()
        .map(|column| ColumnOrder {
            source,
            target: ColumnTarget::<()>::Column(column.column),
            order: column.order,
            collation: column.collation,
            nulls_order: Some(column.nulls),
        })
        .collect::<Vec<_>>();
    match_intrinsic_order_by(&intrinsic, iter_dir, target, |intrinsic, target| {
        intrinsic.source == target.source
            && matches!(
                (&intrinsic.target, &target.target),
                (ColumnTarget::Column(a), ColumnTarget::Column(b)) if a == b
            )
    })
}

/// [match_intrinsic_order] for any column representation: `same_column`
/// decides whether an intrinsic column is the target column; direction,
/// collation and NULLS order are compared the same way for every caller.
fn match_intrinsic_order_by<I, E, J, F>(
    intrinsic: &[ColumnOrder<J, F>],
    iter_dir: IterationDirection,
    target: &[ColumnOrder<I, E>],
    same_column: impl Fn(&ColumnOrder<J, F>, &ColumnOrder<I, E>) -> bool,
) -> usize {
    let target_len = target.len().min(intrinsic.len());
    for (intrinsic_col, target_col) in intrinsic.iter().zip(target.iter()).take(target_len) {
        if !same_column(intrinsic_col, target_col)
            || intrinsic_col.collation != target_col.collation
        {
            return 0;
        }
        let expected_order = match iter_dir {
            IterationDirection::Forwards => intrinsic_col.order,
            IterationDirection::Backwards => match intrinsic_col.order {
                SortOrder::Asc => SortOrder::Desc,
                SortOrder::Desc => SortOrder::Asc,
            },
        };
        if expected_order != target_col.order {
            return 0;
        }
        // If the target requests an explicit NULLS ordering, the intrinsic
        // order must provide the same.
        let requested_nulls = target_col.effective_nulls_order();
        let intrinsic_nulls = intrinsic_col.effective_nulls_order_when_iterated(iter_dir);
        if requested_nulls != intrinsic_nulls {
            return 0;
        }
    }
    target_len
}

trait OrderedSource<I, E> {
    fn id(&self) -> I;
    /// Whether column `col_no` has a custom type. Such a column stores
    /// encoded blobs, whose byte order is not the type's order.
    fn column_has_custom_type(&self, col_no: usize, schema: &Schema) -> bool;
    /// Whether column `col_no` may hold NULL. False for a NOT NULL column.
    fn column_can_be_null(&self, col_no: usize) -> bool;
    fn expressions_match(&self, target: E, indexed: E) -> bool;
}

pub(crate) struct HirOrderSource<'a> {
    planned: &'a HirPlannedSource,
    definition: &'a hir::Source,
}

impl<'a> HirOrderSource<'a> {
    pub(crate) fn new(planned: &'a HirPlannedSource, definition: &'a hir::Source) -> Self {
        debug_assert_eq!(planned.internal_id, definition.id);
        Self {
            planned,
            definition,
        }
    }

    fn table(&self) -> &Table {
        &self.planned.table
    }

    fn index_expressions(&self, index: &Index) -> Result<&'a hir::IndexExpressions> {
        self.definition
            .index_expressions
            .iter()
            .find(|expressions| std::ptr::eq(expressions.index.value(), index))
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR source {} has no frozen expressions for index {}",
                    self.definition.id, index.name
                ))
            })
    }
}

impl<'a> OrderedSource<hir::SourceId, &'a hir::Expr> for HirOrderSource<'a> {
    fn id(&self) -> hir::SourceId {
        self.definition.id
    }

    fn column_has_custom_type(&self, col_no: usize, schema: &Schema) -> bool {
        self.table().columns().get(col_no).is_some_and(|column| {
            schema
                .get_type_def(&column.ty_str, self.table().is_strict())
                .is_some()
        })
    }

    fn column_can_be_null(&self, col_no: usize) -> bool {
        self.table()
            .columns()
            .get(col_no)
            .is_none_or(|column| !column.notnull())
    }

    fn expressions_match(&self, target: &'a hir::Expr, indexed: &'a hir::Expr) -> bool {
        target.equivalent(indexed)
    }
}

/// A FROM subquery or CTE read through the temporary index over its stored
/// rows. Its columns are the subquery's outputs: none has a custom type the
/// index would store encoded, and the index has no expression columns.
struct SubqueryOrderSource(hir::SourceId);

impl<'a> OrderedSource<hir::SourceId, &'a hir::Expr> for SubqueryOrderSource {
    fn id(&self) -> hir::SourceId {
        self.0
    }

    fn column_has_custom_type(&self, _col_no: usize, _schema: &Schema) -> bool {
        false
    }

    fn column_can_be_null(&self, _col_no: usize) -> bool {
        true
    }

    fn expressions_match(&self, _target: &'a hir::Expr, _indexed: &'a hir::Expr) -> bool {
        false
    }
}

/// How many leading `order_target` columns from `start_col` a FROM subquery
/// or CTE supplies when its stored rows are read through `index`, the
/// temporary index [`crate::translate::optimizer::access_method::find_best_hir_access_method_for_subquery`]
/// builds over them, seeking it with `constraint_refs` in `iter_dir`. The
/// index holds the key columns first and then every other column, all
/// ascending, so after an equality-pinned prefix the rows come out in the
/// order of the remaining index columns.
#[allow(clippy::too_many_arguments)]
pub(super) fn hir_materialized_subquery_order_consumed<'a>(
    source: hir::SourceId,
    index: &Index,
    constraint_refs: &[RangeConstraintRef],
    iter_dir: IterationDirection,
    order_target: &HirOrderTarget<'a>,
    start_col: usize,
    schema: &Schema,
    equality_prefix_scope: EqualityPrefixScope,
) -> OrderConsumption {
    let target_columns = &order_target.columns[start_col..];
    if target_columns.is_empty() {
        return OrderConsumption::NONE;
    }
    index_columns_order_consumed(
        &SubqueryOrderSource(source),
        iter_dir,
        constraint_refs,
        order_target,
        target_columns,
        schema,
        equality_prefix_scope,
        index.columns.iter().map(|column| IndexOrderColumn {
            pos_in_table: column.pos_in_table,
            order: column.order,
            nulls_order: column.nulls_order,
            collation: column.collation,
            expr: None,
        }),
        index.columns.len(),
        false,
        None,
    )
}

/// The index column values needed to check its row order.
#[derive(Clone, Copy)]
struct IndexOrderColumn<E> {
    pos_in_table: usize,
    order: SortOrder,
    nulls_order: Option<ast::NullsOrder>,
    collation: Option<CollationSeq>,
    expr: Option<E>,
}

impl<E: Copy> IndexOrderColumn<E> {
    /// Return the NULL order for this scan direction.
    fn nulls_order(self, iter_dir: IterationDirection) -> ast::NullsOrder {
        let nulls_order = self
            .nulls_order
            .unwrap_or_else(|| ast::NullsOrder::default_for(self.order));
        match iter_dir {
            IterationDirection::Forwards => nulls_order,
            IterationDirection::Backwards => nulls_order.reverse(),
        }
    }
}

/// Return true when an order term names this index column.
fn target_matches_order_column<I, E>(
    target_col: &ColumnOrder<I, E>,
    idx_col: IndexOrderColumn<E>,
    source: &impl OrderedSource<I, E>,
) -> bool
where
    I: Copy + Eq,
    E: Copy,
{
    if target_col.source != source.id() {
        return false;
    }
    match (&target_col.target, idx_col.expr) {
        (ColumnTarget::Column(col_no), _) => idx_col.pos_in_table == *col_no,
        (ColumnTarget::Expr(expr), Some(indexed)) => source.expressions_match(*expr, indexed),
        _ => false,
    }
}

/// What a single-table btree access path contributes to an ORDER BY / GROUP BY
/// target. See [`btree_access_order_consumed`].
pub(super) struct OrderConsumption {
    /// How many leading order-target columns the access path satisfies.
    pub consumed: usize,
    /// Whether one of those consumed columns is the table's rowid (directly,
    /// through a rowid alias, or through the implicit rowid suffix of a
    /// secondary index). Every row has a different rowid, so a consumed prefix
    /// that includes the rowid never repeats across the rows this loop emits.
    pub includes_rowid: bool,
}

impl OrderConsumption {
    pub const NONE: OrderConsumption = OrderConsumption {
        consumed: 0,
        includes_rowid: false,
    };
}

fn rowid_order_consumed<I, E>(
    source: &impl OrderedSource<I, E>,
    first_target: &ColumnOrder<I, E>,
    iter_dir: IterationDirection,
    rowid_alias_col: Option<usize>,
) -> OrderConsumption
where
    I: Copy + Eq,
    E: Copy,
{
    if first_target.source != source.id() {
        return OrderConsumption::NONE;
    }
    match first_target.target {
        ColumnTarget::RowId => {}
        ColumnTarget::Column(column) if rowid_alias_col == Some(column) => {}
        ColumnTarget::Column(_) | ColumnTarget::Expr(_) => return OrderConsumption::NONE,
    }
    let correct_order = if iter_dir == IterationDirection::Forwards {
        first_target.order == SortOrder::Asc
    } else {
        first_target.order == SortOrder::Desc
    };
    OrderConsumption {
        consumed: usize::from(correct_order),
        includes_rowid: correct_order,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn hir_btree_access_order_consumed<'a>(
    source: &HirOrderSource<'a>,
    iter_dir: IterationDirection,
    index: Option<&Index>,
    constraint_refs: &[RangeConstraintRef],
    order_target: &HirOrderTarget<'a>,
    start_col: usize,
    schema: &Schema,
    equality_prefix_scope: EqualityPrefixScope,
) -> Result<OrderConsumption> {
    let target_columns = &order_target.columns[start_col..];
    let Some(first_target) = target_columns.first() else {
        return Ok(OrderConsumption::NONE);
    };
    let rowid_alias_col = source
        .table()
        .columns()
        .iter()
        .position(|column| column.is_rowid_alias());

    let Some(index) = index else {
        return Ok(rowid_order_consumed(
            source,
            first_target,
            iter_dir,
            rowid_alias_col,
        ));
    };
    let expressions = source.index_expressions(index)?;
    if expressions.columns.len() != index.columns.len() {
        return Err(LimboError::InternalError(format!(
            "HIR index {} has {} physical columns but {} frozen expressions",
            index.name,
            index.columns.len(),
            expressions.columns.len()
        )));
    }

    Ok(index_columns_order_consumed(
        source,
        iter_dir,
        constraint_refs,
        order_target,
        target_columns,
        schema,
        equality_prefix_scope,
        index
            .columns
            .iter()
            .zip(&expressions.columns)
            .map(|(column, expression)| IndexOrderColumn {
                pos_in_table: column.pos_in_table,
                order: column.order,
                nulls_order: column.nulls_order,
                collation: column.collation,
                expr: expression.as_ref(),
            }),
        index.columns.len(),
        index.has_rowid,
        rowid_alias_col,
    ))
}

/// Return how many order terms these index columns supply.
#[allow(clippy::too_many_arguments)]
fn index_columns_order_consumed<I, E>(
    source: &impl OrderedSource<I, E>,
    iter_dir: IterationDirection,
    constraint_refs: &[RangeConstraintRef],
    order_target: &OrderTarget<I, E>,
    target_columns: &[ColumnOrder<I, E>],
    schema: &Schema,
    equality_prefix_scope: EqualityPrefixScope,
    index_columns: impl Iterator<Item = IndexOrderColumn<E>>,
    column_count: usize,
    has_rowid: bool,
    rowid_alias_col: Option<usize>,
) -> OrderConsumption
where
    I: Copy + Eq,
    E: Copy,
{
    let mut col_idx = 0;
    let mut matched_columns = 0;
    let mut index_columns = index_columns.enumerate();
    let mut includes_rowid = false;
    let target_is_rowid = |target_col: &ColumnOrder<I, E>| match target_col.target {
        ColumnTarget::RowId => true,
        ColumnTarget::Column(col_no) => rowid_alias_col == Some(col_no),
        ColumnTarget::Expr(_) => false,
    };
    while col_idx < target_columns.len() {
        let Some((idx_pos, idx_col)) = index_columns.next() else {
            break;
        };
        let target_col = &target_columns[col_idx];
        if target_col.source != source.id() {
            break;
        }
        let eq_prefix_usable = constraint_refs.iter().any(|constraint| {
            constraint.index_col_pos == idx_pos
                && constraint.eq.as_ref().is_some_and(|eq| {
                    equality_prefix_scope == EqualityPrefixScope::AnyEquality || eq.is_const
                })
        });
        if eq_prefix_usable {
            if target_matches_order_column(target_col, idx_col, source) {
                if target_col.collation != idx_col.collation.unwrap_or_default() {
                    break;
                }
                col_idx += 1;
            }
            matched_columns += 1;
            continue;
        }

        if !target_matches_order_column(target_col, idx_col, source) {
            break;
        }

        // Custom type columns store encoded blobs. The B-tree's byte order
        // does not match the custom type's order.
        if let ColumnTarget::Column(col_no) = &target_col.target {
            if source.column_has_custom_type(*col_no, schema) {
                break;
            }
        }

        if target_col.collation != idx_col.collation.unwrap_or_default() {
            break;
        }
        let correct_order = if iter_dir == IterationDirection::Forwards {
            target_col.order == idx_col.order
        } else {
            target_col.order != idx_col.order
        };
        if !correct_order {
            break;
        }
        if !order_target.is_extremum()
            && target_can_be_null(target_col, source)
            && target_col.effective_nulls_order() != idx_col.nulls_order(iter_dir)
        {
            break;
        }
        if target_is_rowid(target_col) {
            includes_rowid = true;
        }
        col_idx += 1;
        matched_columns += 1;
    }

    // Rowid tables keep equal secondary-index keys ordered by rowid.
    if col_idx < target_columns.len() && matched_columns == column_count && has_rowid {
        let target_col = &target_columns[col_idx];
        let correct_order = if iter_dir == IterationDirection::Forwards {
            target_col.order == SortOrder::Asc
        } else {
            target_col.order == SortOrder::Desc
        };
        if target_col.source == source.id() && target_is_rowid(target_col) && correct_order {
            col_idx += 1;
            includes_rowid = true;
        }
    }

    OrderConsumption {
        consumed: col_idx,
        includes_rowid,
    }
}

/// Whether an order term may be NULL, so that its NULLS placement must
/// match the index's. A rowid and a NOT NULL column never are.
fn target_can_be_null<I, E>(
    target_col: &ColumnOrder<I, E>,
    source: &impl OrderedSource<I, E>,
) -> bool {
    match target_col.target {
        ColumnTarget::RowId => false,
        ColumnTarget::Column(col_no) => source.column_can_be_null(col_no),
        ColumnTarget::Expr(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, IndexColumn, Type},
        sync::Arc,
        translate::{
            plan::{ColumnUsedMask, HirPlannedSource},
            semantic::hir::{
                CatalogObject, CatalogObjectId, CatalogSnapshot, ColumnReadExpression, DatabaseId,
                Expr, IndexCoverage, IndexExpressions, IndexHint, Source, SourceColumn, SourceId,
                SourceKind, SourceOwner, TypeFact,
            },
        },
        vdbe::affinity::Affinity,
    };
    use turso_parser::ast::Operator;

    #[test]
    fn hir_order_target_borrows_resolved_expression() {
        let source = SourceId::new(3);
        let expression = Expr::column(source, 1);
        let target: HirOrderTarget<'_> = OrderTarget {
            columns: vec![ColumnOrder {
                source,
                target: ColumnTarget::Expr(&expression),
                order: SortOrder::Asc,
                collation: CollationSeq::Binary,
                nulls_order: None,
            }],
            purpose: OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
        };

        let ColumnTarget::Expr(stored) = target.columns[0].target else {
            panic!("HIR expression target is preserved");
        };
        assert!(std::ptr::eq(stored, &expression));
        assert_eq!(target.columns[0].source, source);
    }

    #[test]
    fn hir_index_order_matches_frozen_expression() {
        let source_id = SourceId::new(3);
        let table = Table::BTree(Arc::new(BTreeTable::new(
            2,
            "items".to_string(),
            vec![],
            vec![Column::new(
                Some("value".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            )],
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        )));
        let resolved_table = CatalogObject::new(
            CatalogObjectId::new(1),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(table.clone()),
        );
        let indexed_expression = Expr::Binary {
            lhs: Box::new(Expr::column(source_id, 0)),
            operator: Operator::Add,
            rhs: Box::new(Expr::Literal(ast::Literal::Numeric("1".to_string()))),
            array_concat: false,
            custom: None,
            comparison: None,
        };
        let index = CatalogObject::new(
            CatalogObjectId::new(2),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(Index {
                name: "items_plus_one".to_string(),
                table_name: "items".to_string(),
                root_page: 3,
                columns: vec![IndexColumn::new("value + 1", 0)],
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }),
        );
        let definition = Source {
            id: source_id,
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(0)),
            name: "items".to_string(),
            alias: None,
            kind: SourceKind::Table(resolved_table),
            columns: vec![SourceColumn {
                name: "value".to_string(),
                type_fact: TypeFact::known(Type::Integer),
                affinity: Affinity::Integer,
                has_affinity: true,
                collation: None,
                hidden: false,
                rowid_alias: false,
            }],
            generated_expressions: vec![ColumnReadExpression::Absent],
            default_expressions: vec![ColumnReadExpression::Absent],
            column_type_programs: vec![None],
            check_constraints: None,
            rowid_available: true,
            index_hint: IndexHint::None,
            index_expressions: vec![IndexExpressions {
                index: index.clone(),
                columns: vec![Some(indexed_expression.clone())],
                predicate: None,
            }],
            index_coverage: IndexCoverage::Complete {
                indexes: vec![index.id()],
            },
            index_method_patterns: Vec::new(),
        };
        let planned = HirPlannedSource {
            table,
            internal_id: source_id,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            indexed: IndexHint::None,
        };
        let target_expression = indexed_expression;
        let target = OrderTarget {
            columns: vec![ColumnOrder {
                source: source_id,
                target: ColumnTarget::Expr(&target_expression),
                order: SortOrder::Asc,
                collation: CollationSeq::Binary,
                nulls_order: None,
            }],
            purpose: OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
        };

        let consumption = hir_btree_access_order_consumed(
            &HirOrderSource::new(&planned, &definition),
            IterationDirection::Forwards,
            Some(index.value()),
            &[],
            &target,
            0,
            &Schema::new(),
            EqualityPrefixScope::ConstantEquality,
        )
        .expect("frozen HIR expression metadata is complete");

        assert_eq!(consumption.consumed, 1);
        assert!(!consumption.includes_rowid);
    }
}
