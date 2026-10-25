use crate::{
    alloc::{self},
    schema::{BTreeTable, Index, Table, ROWID_SENTINEL},
    translate::{
        optimizer::constraints::{BinaryExprSide, SeekRangeConstraint},
        planner::{table_mask_from_hir_scope, HirFromScope, TableMask},
        semantic::hir,
    },
    types::SeekOp,
    vdbe::{
        affinity::{self, Affinity},
        CursorID,
    },
    Result,
};
use std::{marker::PhantomData, ops::Range, sync::Arc};
use turso_parser::ast::{self};

/// The most sources one FROM clause may join: a join bitmask fits in 64 bits.
pub(crate) const MAX_JOINED_TABLES: usize = 63;

/// In a query plan, WHERE and JOIN conditions are split into predicate terms
/// so each condition can run at the correct loop depth.
///
/// Outer joins require the originating join source to be retained. For:
///
/// `SELECT * FROM users u LEFT JOIN products p ON u.id = 5`
///
/// `u.id = 5` cannot run in the `users` loop and skip rows. Every `users` row
/// must reach the join so `products` can be NULL-extended when the condition
/// is false.
#[derive(Debug, Clone)]
pub struct PredicateTerm<E, S> {
    /// The original condition expression.
    pub expr: E,

    /// For ordinary conditions, the optimizer chooses the earliest loop
    /// containing every referenced source.
    ///
    /// Outer-join conditions cannot always run at that earliest loop. For:
    ///
    /// `SELECT * FROM t LEFT JOIN s ON t.a = 2`
    ///
    /// the engine must:
    ///
    /// 1. Process every row from `t`.
    /// 2. Emit NULL values for `s` when `t.a != 2`.
    /// 3. Emit the matching `s` values when `t.a = 2`.
    ///
    /// This field forces evaluation at the outer join's source loop. `S` is
    /// the source identity, `SourceId` for HIR.
    pub from_outer_join: Option<S>,

    /// Whether the optimizer already incorporated this condition elsewhere.
    ///
    /// A term may be consumed because:
    ///
    /// - it became a seek constraint;
    /// - it was proven trivially true or false.
    pub consumed: bool,
}

/// Predicate term used by HIR planning without parser expressions or planner IDs.
pub(crate) type HirWhereTerm = PredicateTerm<hir::ExprId, hir::SourceId>;

impl From<hir::ExprId> for HirWhereTerm {
    fn from(expr: hir::ExprId) -> Self {
        Self {
            expr,
            from_outer_join: None,
            consumed: false,
        }
    }
}

/// Build the planning terms of every predicate of `document`: the
/// conjuncts each WHERE, ON, DML and partial-index predicate splits into,
/// the `col IN (...)` terms their ORs imply, and the equality each
/// `x IN (SELECT y ...)` subquery can run with. A predicate whose terms are
/// already built is left alone, so this runs again after a rewrite adds
/// predicates.
pub(crate) fn prepare_planning_terms(document: &mut hir::HirDocument) {
    let mut predicates = Vec::new();
    let mut filters = Vec::new();
    for query in &document.queries {
        for block in &query.blocks {
            if let Some(from) = &block.from {
                collect_join_predicates(from, &mut predicates);
            }
            if let hir::QueryBlockBody::Select {
                filter: Some(filter),
                ..
            } = &block.body
            {
                predicates.push(*filter);
                filters.push(*filter);
            }
        }
    }
    for source in &document.sources {
        if let hir::SourceKind::FromGroup(group) = &source.kind {
            collect_join_predicates(&group.from, &mut predicates);
        }
        predicates.extend(
            source
                .index_expressions
                .iter()
                .filter_map(|index| index.predicate),
        );
    }
    collect_root_predicates(&document.root, &mut predicates);
    for predicate in predicates {
        add_planning_terms(&mut document.exprs, &mut document.planning, predicate);
    }
    for filter in filters {
        add_in_probes(document, filter);
    }
    if document.planning.always_true.is_none() {
        document.planning.always_true = Some(
            document
                .exprs
                .push(hir::Expr::Literal(ast::Literal::Numeric("1".to_string()))),
        );
        document.planning.null = Some(document.exprs.push(hir::Expr::Literal(ast::Literal::Null)));
    }
}

fn collect_join_predicates(from: &hir::From, predicates: &mut Vec<hir::ExprId>) {
    for join in &from.joins {
        if let hir::JoinConstraint::On(expression) = join.constraint {
            predicates.push(expression);
        }
    }
}

fn collect_root_predicates(root: &hir::HirRoot, predicates: &mut Vec<hir::ExprId>) {
    let add_update = |update: &hir::Update, predicates: &mut Vec<hir::ExprId>| {
        predicates.extend(update.predicate);
        if let Some(from) = &update.from {
            collect_join_predicates(from, predicates);
        }
    };
    let add_command = |command: &hir::TriggerCommand| match command {
        hir::TriggerCommand::Update(update) => add_update(update, predicates),
        hir::TriggerCommand::Delete(delete) => predicates.extend(delete.predicate),
        hir::TriggerCommand::Select(_) | hir::TriggerCommand::Insert(_) => {}
    };
    match root {
        hir::HirRoot::Update(update) => add_update(update, predicates),
        hir::HirRoot::Delete(delete) => predicates.extend(delete.predicate),
        hir::HirRoot::Trigger(trigger) => match &trigger.body {
            #[cfg(test)]
            hir::TriggerBody::Predicate(_) => {}
            #[cfg(test)]
            hir::TriggerBody::Command(command) => std::iter::once(command).for_each(add_command),
            hir::TriggerBody::Program(program) => program.commands.iter().for_each(add_command),
        },
        hir::HirRoot::Query(_) | hir::HirRoot::Insert(_) | hir::HirRoot::SchemaExpressions(_) => {}
    }
}

/// The conjuncts of an HIR predicate for planning, rewritten so that each
/// can drive an index seek and be consumed on its own:
///
/// - `x BETWEEN a AND b` on a column becomes `x >= a` and `x <= b`, as
///   SQLite also does;
/// - a conjunct every branch of an OR has is lifted out of it:
///   `(x = 1 AND a) OR (x = 1 AND b)` becomes `x = 1` and `a OR b`, and
///   `x = 1 OR (x = 1 AND b)` becomes `x = 1`.
fn add_planning_terms(
    exprs: &mut hir::ExprArena,
    planning: &mut hir::PlanningTerms,
    predicate: hir::ExprId,
) {
    if planning.conjuncts.contains_key(&predicate) {
        return;
    }
    let written = exprs
        .at(predicate)
        .conjuncts()
        .map(|conjunct| conjunct.id())
        .collect::<Vec<_>>();
    let mut lifted = Vec::with_capacity(written.len());
    for conjunct in written {
        match lift_or_common_conjuncts(exprs, conjunct) {
            Some((common, rest)) => {
                lifted.extend(common);
                lifted.extend(rest);
            }
            None => lifted.push(conjunct),
        }
    }
    let mut conjuncts = Vec::with_capacity(lifted.len());
    for conjunct in lifted {
        split_column_between(exprs, conjunct, &mut conjuncts);
    }
    for conjunct in &conjuncts {
        let implied = implied_in_terms(exprs, *conjunct);
        if !implied.is_empty() {
            planning.implied_in.insert(*conjunct, implied);
        }
    }
    planning.conjuncts.insert(predicate, conjuncts);
}

fn hir_operands(
    expression: hir::ExprRef<'_>,
    operator: ast::Operator,
    operands: &mut Vec<hir::ExprId>,
) {
    match expression.node() {
        hir::Expr::Binary {
            lhs,
            operator: found,
            rhs,
            custom: None,
            ..
        } if *found == operator => {
            hir_operands(expression.at(*lhs), operator, operands);
            hir_operands(expression.at(*rhs), operator, operands);
        }
        _ => operands.push(expression.id()),
    }
}

fn hir_chain(
    exprs: &mut hir::ExprArena,
    operands: Vec<hir::ExprId>,
    operator: ast::Operator,
) -> hir::ExprId {
    let mut operands = operands.into_iter();
    let first = operands.next().expect("a chain has at least one operand");
    operands.fold(first, |lhs, rhs| {
        exprs.push(hir::Expr::Binary {
            lhs,
            operator,
            rhs,
            array_concat: false,
            custom: None,
            comparison: None,
        })
    })
}

/// The conjuncts every branch of an OR shares, and the OR without them, or
/// none when it is no longer needed. `None` when nothing is shared.
fn lift_or_common_conjuncts(
    exprs: &mut hir::ExprArena,
    expression: hir::ExprId,
) -> Option<(Vec<hir::ExprId>, Option<hir::ExprId>)> {
    let mut disjuncts = Vec::new();
    hir_operands(exprs.at(expression), ast::Operator::Or, &mut disjuncts);
    if disjuncts.len() < 2 {
        return None;
    }
    let branches = disjuncts
        .into_iter()
        .map(|disjunct| {
            let mut conjuncts = Vec::new();
            hir_operands(exprs.at(disjunct), ast::Operator::And, &mut conjuncts);
            conjuncts
        })
        .collect::<Vec<_>>();
    let same = |left: hir::ExprId, right: hir::ExprId| exprs.at(left).equivalent(exprs.at(right));
    let mut common: Vec<hir::ExprId> = Vec::new();
    for candidate in &branches[0] {
        if !common.iter().any(|found| same(*found, *candidate))
            && branches[1..]
                .iter()
                .all(|branch| branch.iter().any(|other| same(*other, *candidate)))
        {
            common.push(*candidate);
        }
    }
    if common.is_empty() {
        return None;
    }
    let mut remaining_branches = Vec::with_capacity(branches.len());
    for branch in &branches {
        let remaining = branch
            .iter()
            .copied()
            .filter(|conjunct| !common.iter().any(|found| same(*found, *conjunct)))
            .collect::<Vec<_>>();
        if remaining.is_empty() {
            // One branch is the shared conjuncts alone, so the OR adds
            // nothing to them.
            return Some((common, None));
        }
        remaining_branches.push(remaining);
    }
    let rest = remaining_branches
        .into_iter()
        .map(|remaining| hir_chain(exprs, remaining, ast::Operator::And))
        .collect();
    Some((common, Some(hir_chain(exprs, rest, ast::Operator::Or))))
}

/// `col IN (v1, v2, ...)` terms an OR term implies: every branch of the
/// OR, its ANDs flattened, compares `col` with a literal by `=`. The OR
/// stays, since the IN only narrows the rows it is checked on, but the IN
/// can drive an index seek the OR cannot. The planner keeps only some of
/// them; see [`hir_implied_in_terms`].
fn implied_in_terms(
    exprs: &mut hir::ExprArena,
    expression: hir::ExprId,
) -> Vec<(hir::ColumnRef, hir::ExprId)> {
    let mut disjuncts = Vec::new();
    hir_operands(exprs.at(expression), ast::Operator::Or, &mut disjuncts);
    if disjuncts.len() < 2 {
        return Vec::new();
    }
    let branches = disjuncts
        .into_iter()
        .map(|disjunct| {
            let mut conjuncts = Vec::new();
            hir_operands(exprs.at(disjunct), ast::Operator::And, &mut conjuncts);
            conjuncts
        })
        .collect::<Vec<_>>();
    // A filter true for every branch names a column of the first branch.
    let mut candidates: Vec<hir::ColumnRef> = Vec::new();
    for term in &branches[0] {
        if let Some((column, _, _)) = hir_column_literal_equality(exprs.at(*term)) {
            if !candidates.contains(&column) {
                candidates.push(column);
            }
        }
    }
    let mut filters = Vec::new();
    'candidates: for column in candidates {
        let mut values: Vec<hir::ExprId> = Vec::new();
        let mut comparisons = Vec::new();
        for branch in &branches {
            let mut found = false;
            for term in branch {
                let Some((other, value, comparison)) = hir_column_literal_equality(exprs.at(*term))
                else {
                    continue;
                };
                if other != column {
                    continue;
                }
                found = true;
                if !values
                    .iter()
                    .any(|known| exprs.at(*known).equivalent(exprs.at(value)))
                {
                    values.push(value);
                    comparisons.push(comparison.clone());
                }
            }
            if !found {
                // This branch does not restrict the column, so a filter on
                // it could reject rows of this branch.
                continue 'candidates;
            }
        }
        // One value for every branch is a conjunct the OR shares, which
        // `add_planning_terms` lifts out of it.
        if values.len() > 1 {
            let lhs = exprs.push(hir::Expr::Column(column));
            let filter = exprs.push(hir::Expr::InList {
                lhs,
                negated: false,
                values,
                comparisons,
            });
            filters.push((column, filter));
        }
    }
    filters
}

/// Of the `col IN (...)` terms `implied` an OR term implies, the ones to
/// plan: with several such columns of one table, only the one that alone
/// starts an index is used, so that two seeks do not compete for the same
/// table.
pub(crate) fn hir_implied_in_terms(
    implied: &[(hir::ColumnRef, hir::ExprId)],
    sole_indexed_column: &dyn Fn(hir::SourceId, &[usize]) -> Option<usize>,
) -> Vec<hir::ExprId> {
    implied
        .iter()
        .filter_map(|(column, filter)| {
            let same_source = implied
                .iter()
                .filter(|(other, _)| other.source == column.source)
                .map(|(other, _)| other.column)
                .collect::<Vec<_>>();
            (same_source.len() == 1
                || sole_indexed_column(column.source, &same_source) == Some(column.column))
            .then_some(*filter)
        })
        .collect()
}

/// The column and literal of `col = literal` or `literal = col`, with the
/// comparison's semantics.
fn hir_column_literal_equality(
    expression: hir::ExprRef<'_>,
) -> Option<(hir::ColumnRef, hir::ExprId, &hir::ComparisonSemantics)> {
    let hir::Expr::Binary {
        lhs,
        operator: ast::Operator::Equals,
        rhs,
        custom: None,
        comparison: Some(comparison),
        ..
    } = expression.node()
    else {
        return None;
    };
    match (expression.at(*lhs).node(), expression.at(*rhs)) {
        (hir::Expr::Column(column), value) if hir_literal_can_be_copied(value) => {
            Some((*column, *rhs, comparison))
        }
        _ => match (expression.at(*lhs), expression.at(*rhs).node()) {
            (value, hir::Expr::Column(column)) if hir_literal_can_be_copied(value) => {
                Some((*column, *lhs, comparison))
            }
            _ => None,
        },
    }
}

/// A number, string, blob or boolean, possibly signed. NULL never satisfies
/// `=`, so it is left out.
fn hir_literal_can_be_copied(expression: hir::ExprRef<'_>) -> bool {
    use turso_parser::ast::{Literal, UnaryOperator};
    match expression.node() {
        hir::Expr::Literal(
            Literal::Numeric(_)
            | Literal::String(_)
            | Literal::Blob(_)
            | Literal::True
            | Literal::False,
        ) => true,
        hir::Expr::Unary {
            operator: UnaryOperator::Negative | UnaryOperator::Positive,
            expr,
        } => matches!(
            expression.at(*expr).node(),
            hir::Expr::Literal(Literal::Numeric(_))
        ),
        _ => false,
    }
}

fn split_column_between(
    exprs: &mut hir::ExprArena,
    conjunct: hir::ExprId,
    conjuncts: &mut Vec<hir::ExprId>,
) {
    let hir::Expr::Between {
        expr,
        negated: false,
        start,
        end,
        start_comparison,
        end_comparison,
    } = &exprs[conjunct]
    else {
        conjuncts.push(conjunct);
        return;
    };
    if !matches!(exprs[*expr], hir::Expr::Column(_) | hir::Expr::RowId(_)) {
        conjuncts.push(conjunct);
        return;
    }
    let bounds = [
        (
            ast::Operator::GreaterEquals,
            *start,
            start_comparison.clone(),
        ),
        (ast::Operator::LessEquals, *end, end_comparison.clone()),
    ];
    let lhs = *expr;
    for (operator, rhs, comparison) in bounds {
        conjuncts.push(exprs.push(hir::Expr::Binary {
            lhs,
            operator,
            rhs,
            array_concat: false,
            custom: None,
            comparison: Some(comparison),
        }));
    }
}

/// Build the `x = y` term of each `x IN (SELECT y ...)` conjunct of
/// `filter` whose subquery has one block with one output.
fn add_in_probes(document: &mut hir::HirDocument, filter: hir::ExprId) {
    let mut probes = Vec::new();
    for conjunct in document.expr(filter).conjuncts() {
        let hir::Expr::Subquery(hir::SubqueryExpr::In {
            lhs,
            query,
            negated: false,
            comparison,
        }) = conjunct.node()
        else {
            continue;
        };
        if comparison.components.len() != 1 || document.planning.in_probe(*query).is_some() {
            continue;
        }
        let Some(definition) = document.query(*query) else {
            continue;
        };
        let [block] = definition.blocks.as_slice() else {
            continue;
        };
        let [output] = block.outputs.as_slice() else {
            continue;
        };
        probes.push((*query, *lhs, output.expr, comparison.clone()));
    }
    for (query, lhs, rhs, comparison) in probes {
        let probe = document.exprs.push(hir::Expr::Binary {
            lhs,
            operator: ast::Operator::Equals,
            rhs,
            array_concat: false,
            custom: None,
            comparison: Some(comparison),
        });
        add_planning_terms(&mut document.exprs, &mut document.planning, probe);
        document.planning.in_probes.insert(query, probe);
    }
}

/// The destination of the results of a query.
/// Typically, the results of a query are returned to the caller.
/// However, there are some cases where the results are not returned to the caller,
/// but rather are yielded to a parent query via coroutine, or stored in a temp table,
/// later used by the parent query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EphemeralRowidMode {
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
        /// Outputs computed from custom-typed columns as stored, not decoded:
        /// the values a schema statement compares with an index entry or a
        /// CHECK constraint, which DML computes the same way.
        stored_outputs: alloc::Vec<usize>,
        /// Whether array outputs are turned into text. A FROM subquery or
        /// CTE keeps them as arrays, so the outer query can still read their
        /// elements; the outer query turns the arrays it returns into text.
        decode_arrays: bool,
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
}

#[derive(Debug, Clone, Copy)]
/// One result column used to order the recursive CTE work queue.
pub struct RecursiveCteQueueKey {
    pub result_column_index: usize,
    /// `None` when the index sort order already puts NULLs in the requested
    /// position.
    pub nulls_override: Option<ast::NullsOrder>,
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
    RightOuter,
    FullOuter,
    /// Semi-join: keep outer row if inner match found (EXISTS).
    Semi,
    /// Anti-join: keep outer row if NO inner match found (NOT EXISTS).
    Anti,
}

/// Join information for a planned source.
#[derive(Debug, Clone)]
pub struct JoinInfo<N = ast::Name> {
    /// The type of join.
    pub join_type: JoinType,
    /// The USING clause for the join, if any. NATURAL JOIN is transformed into USING (col1, col2, ...).
    pub using: Vec<N>,
    /// When true, the optimizer must not reorder this table relative to its
    /// neighbors. Set for CROSS JOIN to match SQLite semantics.
    pub no_reorder: bool,
}

impl JoinType {
    pub fn from_join_operator(operator: &ast::JoinOperator) -> Self {
        let ast::JoinOperator::TypedJoin(Some(join_type)) = operator else {
            return Self::Inner;
        };
        let has_left = join_type.contains(ast::JoinType::LEFT);
        let has_right = join_type.contains(ast::JoinType::RIGHT);
        if has_left && has_right {
            Self::FullOuter
        } else if has_right {
            Self::RightOuter
        } else if has_left {
            Self::LeftOuter
        } else {
            Self::Inner
        }
    }

    /// Whether the join keeps the rows of its right side that match no row
    /// of the left side.
    pub fn keeps_right_rows(self) -> bool {
        matches!(self, Self::RightOuter | Self::FullOuter)
    }
}

impl JoinInfo {
    /// Whether `column_name` is one of the join's USING columns.
    pub fn merges_column(&self, column_name: &str) -> bool {
        self.using
            .iter()
            .any(|name| name.as_str().eq_ignore_ascii_case(column_name))
    }
}

/// Return true when SQLite resolves a qualified star column as an unqualified name.
///
/// A later USING clause and a later RIGHT or FULL JOIN are both required.
/// They can belong to different joins.
pub fn star_column_uses_merged_value<'a>(
    later_joins: impl Iterator<Item = &'a JoinInfo>,
    column_name: &str,
) -> bool {
    let mut has_later_using = false;
    let mut has_later_right_or_full_join = false;
    for join_info in later_joins {
        has_later_using |= join_info.merges_column(column_name);
        has_later_right_or_full_join |= join_info.keeps_right_rows();
    }
    has_later_using && has_later_right_or_full_join
}

/// The FROM sources that supply an unqualified column.
pub struct UnqualifiedColumnSources {
    /// `(source index, column index)` pairs, first source first. Empty when no
    /// source has the column.
    pub sources: smallvec::SmallVec<[(usize, usize); 4]>,
}

/// The result of looking up an unqualified column name in one FROM source.
pub enum ColumnLookup {
    Found(usize),
    Missing,
}

/// Find the FROM sources that supply an unqualified column, from left to right.
///
/// INNER and LEFT JOIN USING keep the copy found so far. RIGHT JOIN USING
/// replaces it, because the left copy is NULL in an unmatched right row. FULL
/// JOIN USING adds a fallback, because either copy can be NULL.
///
/// View metadata uses this to name a view's columns the way the query
/// binder resolves them.
pub fn unqualified_column_sources<'a, S>(
    sources: &'a [S],
    column_name: &str,
    join_info: impl Fn(&'a S) -> Option<&'a JoinInfo>,
    lookup: impl Fn(&'a S) -> ColumnLookup,
) -> UnqualifiedColumnSources {
    let mut found = smallvec::SmallVec::new();
    for (source_index, source) in sources.iter().enumerate() {
        let already_found = !found.is_empty();
        let using_join_type = join_info(source)
            .filter(|join_info| join_info.merges_column(column_name))
            .map(|join_info| join_info.join_type);
        if already_found && matches!(using_join_type, Some(JoinType::Inner | JoinType::LeftOuter)) {
            continue;
        }
        let column_index = match lookup(source) {
            ColumnLookup::Found(column_index) => column_index,
            ColumnLookup::Missing => continue,
        };

        if !already_found {
            found.push((source_index, column_index));
            continue;
        }

        match using_join_type {
            Some(JoinType::RightOuter) => {
                found.clear();
                found.push((source_index, column_index));
            }
            Some(JoinType::FullOuter) => found.push((source_index, column_index)),
            // A repeated name that no USING merged is ambiguous; the binder
            // reports that, this only lists the copies in order.
            None
            | Some(JoinType::Inner | JoinType::LeftOuter | JoinType::Semi | JoinType::Anti) => {}
        }
    }
    UnqualifiedColumnSources { sources: found }
}

/// Find the earlier FROM sources that a USING or NATURAL join compares with.
///
/// Returns `(source index, column index)` pairs; an empty list means no
/// earlier source has the column. Without a RIGHT or FULL JOIN in the FROM
/// list, SQLite compares only the first copy. With one, every copy after the
/// first must have been merged by an earlier USING, as in
/// `sqlite3ProcessJoin` (select.c):
///
/// ```sql
/// SELECT * FROM t1 JOIN t2 USING(a) JOIN t3 USING(a) RIGHT JOIN t4 ON 1;  -- ok
/// SELECT * FROM t1 JOIN t2 ON 1 JOIN t3 USING(a) RIGHT JOIN t4 ON 1;
/// -- ambiguous reference to a in USING()
/// ```
///
/// The planner and view metadata both use this, so they report the same errors.
pub fn left_using_column_sources<'a, S>(
    sources: &'a [S],
    column_name: &str,
    has_right_or_full_join: bool,
    join_info: impl Fn(&'a S) -> Option<&'a JoinInfo>,
    column_index: impl Fn(&'a S) -> Option<usize>,
) -> Result<smallvec::SmallVec<[(usize, usize); 4]>> {
    let mut found = smallvec::SmallVec::<[(usize, usize); 4]>::new();
    for (source_index, source) in sources.iter().enumerate() {
        let Some(column_index) = column_index(source) else {
            continue;
        };
        if !found.is_empty() && !has_right_or_full_join {
            break;
        }
        if !found.is_empty()
            && !join_info(source).is_some_and(|join_info| join_info.merges_column(column_name))
        {
            crate::bail_parse_error!("ambiguous reference to {} in USING()", column_name);
        }
        found.push((source_index, column_index));
    }
    Ok(found)
}

impl<N> JoinInfo<N> {
    /// Whether this is an OUTER JOIN (LEFT OUTER or FULL OUTER).
    pub fn is_outer(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter
        )
    }

    /// Whether the join keeps the rows of its left side that match no row
    /// of the right side.
    pub fn keeps_left_rows(&self) -> bool {
        matches!(self.join_type, JoinType::LeftOuter | JoinType::FullOuter)
    }

    /// Whether the join keeps the rows of its right side that match no row
    /// of the left side.
    pub fn keeps_right_rows(&self) -> bool {
        self.join_type.keeps_right_rows()
    }

    /// Whether this is a FULL OUTER JOIN.
    pub fn is_full_outer(&self) -> bool {
        self.join_type == JoinType::FullOuter
    }

    /// Whether this is a semi-join or anti-join (EXISTS/NOT EXISTS).
    pub fn is_semi_or_anti(&self) -> bool {
        matches!(self.join_type, JoinType::Semi | JoinType::Anti)
    }
}

/// A source the planner chooses an access path for: a table of the FROM
/// clause with the columns and expressions the query reads from it.
#[derive(Debug, Clone)]
pub struct PlannedSource<I, H, E, J> {
    /// Table object, which contains metadata about the table, e.g. columns.
    pub table: Table,
    /// Identity used by expressions and planner metadata.
    pub internal_id: I,
    /// Join info when this source is the right side of a join.
    pub join_info: Option<J>,
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
    /// Each entry stores the expression shape used by this planning path and
    /// the columns it needs. During covering checks we ask: does an index contain this
    /// expression? If yes, all columns that *only* feed this expression can be
    /// removed from the required-column set.
    pub expression_index_usages: Vec<ExpressionIndexUsage<E>>,
    /// Index-selection hint already represented for this planning path.
    pub indexed: H,
}

/// Resolved join metadata used by HIR planning without rewriting RIGHT JOIN.
#[derive(Debug, Clone)]
pub(crate) struct HirJoinInfo {
    pub(crate) kind: crate::translate::semantic::hir::JoinKind,
}

/// Structural boundary for a parenthesized FROM group. Physical leaves remain
/// in the flat source list; this records their nesting and join position.
pub(crate) struct HirFromGroupBoundary {
    pub(crate) source: crate::translate::semantic::hir::SourceId,
    pub(crate) parent: Option<crate::translate::semantic::hir::SourceId>,
    pub(crate) source_range: Range<usize>,
    pub(crate) join_info: Option<HirJoinInfo>,
}

/// Flat physical source positions plus parenthesized join boundaries for one
/// resolved FROM clause.
pub(crate) struct HirFromLayout<'a> {
    sources: &'a [HirPlanSource],
    groups: &'a [HirFromGroupBoundary],
}

impl<'a> HirFromLayout<'a> {
    pub(crate) fn new(sources: &'a [HirPlanSource], groups: &'a [HirFromGroupBoundary]) -> Self {
        Self { sources, groups }
    }

    pub(crate) fn source_position(&self, source: hir::SourceId) -> Option<usize> {
        self.sources
            .iter()
            .position(|candidate| candidate.source() == source)
    }

    pub(crate) fn table_mask(
        &self,
        document: &hir::HirDocument,
        expression: hir::ExprId,
    ) -> Result<TableMask> {
        table_mask_from_hir_scope(document, self, expression)
    }

    pub(crate) fn outer_join_may_null_extend(&self, source: hir::SourceId) -> bool {
        self.join_may_null_extend(source, false)
    }

    pub(crate) fn full_join_may_null_extend(&self, source: hir::SourceId) -> bool {
        self.join_may_null_extend(source, true)
    }

    /// An outer-join predicate owned by a group can run only when the current
    /// source completes that group. Requiring the current source to be inside
    /// the group prevents the predicate from becoming ready again later.
    pub(crate) fn predicate_owner_ready(
        &self,
        owner: hir::SourceId,
        joined: &TableMask,
        current: usize,
    ) -> bool {
        if self.source_position(owner) == Some(current) {
            return true;
        }

        self.groups
            .iter()
            .find(|group| group.source == owner)
            .is_some_and(|group| {
                group.source_range.contains(&current)
                    && group
                        .source_range
                        .clone()
                        .all(|position| joined.get(position))
            })
    }

    fn join_may_null_extend(&self, source: hir::SourceId, full_only: bool) -> bool {
        let Some(position) = self.source_position(source) else {
            return false;
        };
        self.sources.iter().enumerate().any(|(right, candidate)| {
            candidate.join_info().is_some_and(|join| {
                (!full_only || join.kind == hir::JoinKind::Full)
                    && join_kind_may_null_extend(
                        join.kind,
                        position,
                        self.innermost_group_start(right),
                        right..right + 1,
                    )
            })
        }) || self.groups.iter().any(|group| {
            group.join_info.as_ref().is_some_and(|join| {
                (!full_only || join.kind == hir::JoinKind::Full)
                    && join_kind_may_null_extend(
                        join.kind,
                        position,
                        self.parent_group_start(group),
                        group.source_range.clone(),
                    )
            })
        })
    }

    fn innermost_group_start(&self, position: usize) -> usize {
        self.groups
            .iter()
            .filter(|group| group.source_range.contains(&position))
            .min_by_key(|group| group.source_range.len())
            .map_or(0, |group| group.source_range.start)
    }

    fn parent_group_start(&self, group: &HirFromGroupBoundary) -> usize {
        group
            .parent
            .and_then(|parent| self.groups.iter().find(|group| group.source == parent))
            .map_or(0, |parent| parent.source_range.start)
    }
}

impl HirFromScope for HirFromLayout<'_> {
    fn source_position(&self, source: hir::SourceId) -> Option<usize> {
        HirFromLayout::source_position(self, source)
    }

    fn outer_join_may_null_extend(&self, source: hir::SourceId) -> bool {
        HirFromLayout::outer_join_may_null_extend(self, source)
    }

    fn full_join_may_null_extend(&self, source: hir::SourceId) -> bool {
        HirFromLayout::full_join_may_null_extend(self, source)
    }
}

fn join_kind_may_null_extend(
    kind: hir::JoinKind,
    source_position: usize,
    left_start: usize,
    right: Range<usize>,
) -> bool {
    match kind {
        hir::JoinKind::Left => right.contains(&source_position),
        hir::JoinKind::Right => (left_start..right.start).contains(&source_position),
        hir::JoinKind::Full => (left_start..right.end).contains(&source_position),
        hir::JoinKind::Comma
        | hir::JoinKind::Inner
        | hir::JoinKind::Cross
        | hir::JoinKind::Semi
        | hir::JoinKind::Anti => false,
    }
}

/// Source used by HIR planning without parser expressions or planner IDs.
pub(crate) type HirPlannedSource = PlannedSource<
    crate::translate::semantic::hir::SourceId,
    crate::translate::semantic::hir::IndexHint,
    hir::ExprId,
    HirJoinInfo,
>;

/// Resolved reuse policy for a non-recursive CTE scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HirCteMaterialization {
    PerReference,
    Shared,
    Explicit,
}

/// One resolved HIR source as seen by join planning.
///
/// B-tree sources carry physical access metadata. Query-backed sources point
/// at their planned query, so they do not need a synthetic table.
pub(crate) enum HirPlanSource {
    BTree(HirPlannedSource),
    Derived {
        source: crate::translate::semantic::hir::SourceId,
        query: crate::translate::semantic::hir::QueryId,
        join_info: Option<HirJoinInfo>,
        /// The order the subquery's plan returns its rows in.
        output_order: Vec<crate::translate::optimizer::order::HirOutputOrder>,
        /// A LATERAL subquery that reads sources of the same FROM clause:
        /// it runs again for each of their rows, after all of them.
        lateral: bool,
    },
    Cte {
        source: crate::translate::semantic::hir::SourceId,
        cte: crate::translate::semantic::hir::CteId,
        query: crate::translate::semantic::hir::QueryId,
        materialization: HirCteMaterialization,
        join_info: Option<HirJoinInfo>,
        /// The order the CTE body's plan returns its rows in.
        output_order: Vec<crate::translate::optimizer::order::HirOutputOrder>,
    },
    RecursiveCte {
        source: crate::translate::semantic::hir::SourceId,
        cte: crate::translate::semantic::hir::CteId,
        join_info: Option<HirJoinInfo>,
    },
    RecursiveInput {
        source: crate::translate::semantic::hir::SourceId,
        cte: crate::translate::semantic::hir::CteId,
        join_info: Option<HirJoinInfo>,
    },
}

impl HirPlanSource {
    pub(crate) fn source(&self) -> crate::translate::semantic::hir::SourceId {
        match self {
            Self::BTree(source) => source.internal_id,
            Self::Derived { source, .. }
            | Self::Cte { source, .. }
            | Self::RecursiveCte { source, .. }
            | Self::RecursiveInput { source, .. } => *source,
        }
    }

    /// Whether this is a LATERAL subquery that reads sources of its own
    /// FROM clause.
    pub(crate) fn is_lateral(&self) -> bool {
        matches!(self, Self::Derived { lateral: true, .. })
    }

    pub(crate) fn join_info_mut(&mut self) -> Option<&mut HirJoinInfo> {
        match self {
            Self::BTree(source) => source.join_info.as_mut(),
            Self::Derived { join_info, .. }
            | Self::Cte { join_info, .. }
            | Self::RecursiveCte { join_info, .. }
            | Self::RecursiveInput { join_info, .. } => join_info.as_mut(),
        }
    }

    pub(crate) fn join_info(&self) -> Option<&HirJoinInfo> {
        match self {
            Self::BTree(source) => source.join_info.as_ref(),
            Self::Derived { join_info, .. }
            | Self::Cte { join_info, .. }
            | Self::RecursiveCte { join_info, .. }
            | Self::RecursiveInput { join_info, .. } => join_info.as_ref(),
        }
    }

    pub(crate) fn btree(&self) -> Option<&HirPlannedSource> {
        match self {
            Self::BTree(source) => Some(source),
            Self::Derived { .. }
            | Self::Cte { .. }
            | Self::RecursiveCte { .. }
            | Self::RecursiveInput { .. } => None,
        }
    }

    pub(crate) fn btree_mut(&mut self) -> Option<&mut HirPlannedSource> {
        match self {
            Self::BTree(source) => Some(source),
            Self::Derived { .. }
            | Self::Cte { .. }
            | Self::RecursiveCte { .. }
            | Self::RecursiveInput { .. } => None,
        }
    }
}

impl<I, H, E, J> PlannedSource<I, H, E, J> {
    /// Apply shared covering-index rules while letting each expression
    /// representation decide whether an expression key matches.
    fn index_is_covering_with(
        &self,
        index: &Index,
        expression_in_index: impl Fn(&E, &Index) -> bool,
    ) -> bool {
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
            self.apply_expression_index_coverage(index, &mut required_columns, expression_in_index);
            if required_columns.is_empty() {
                return true;
            }
            Self::index_covers_columns(index, btree, &required_columns)
        }
    }

    /// Whether the index holds every column the query reads from the table,
    /// without counting the expressions it keys. A row the join
    /// NULL-extends reads NULL from every column, so an expression the index
    /// supplies must be computed from the NULL columns instead: `coalesce(v,
    /// -1)` is -1 on that row, not the stored key.
    pub(crate) fn index_covers_columns_without_expressions(&self, index: &Index) -> bool {
        let Table::BTree(btree) = &self.table else {
            return false;
        };
        if index.index_method.is_some() {
            return false;
        }
        if self.col_used_mask.is_empty() {
            return index.where_clause.is_none();
        }
        Self::index_covers_columns(index, btree, &self.col_used_mask)
    }

    /// Remove columns fully supplied by expression keys in this index.
    fn apply_expression_index_coverage(
        &self,
        index: &Index,
        required_columns: &mut ColumnUsedMask,
        expression_in_index: impl Fn(&E, &Index) -> bool,
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
            if expression_in_index(&usage.normalized_expr, index) {
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
}

impl
    PlannedSource<
        crate::translate::semantic::hir::SourceId,
        crate::translate::semantic::hir::IndexHint,
        hir::ExprId,
        HirJoinInfo,
    >
{
    pub(crate) fn register_expression_index_usage(
        &mut self,
        expression: hir::ExprId,
        columns_mask: ColumnUsedMask,
        exprs: &hir::ExprArena,
    ) {
        if columns_mask.is_empty() {
            return;
        }
        if self.expression_index_usages.iter().any(|usage| {
            exprs
                .at(*usage.normalized_expr)
                .equivalent(exprs.at(expression))
        }) {
            return;
        }
        self.expression_index_usages.push(ExpressionIndexUsage {
            normalized_expr: Box::new(expression),
            columns_mask,
        });
    }

    /// Whether the index stores every column the query reads from this
    /// source as a column. An index storing only an expression of a column
    /// does not count: a hash build input copies the column itself.
    pub(crate) fn index_stores_read_columns(&self, index: &Index) -> bool {
        self.index_is_covering_with(index, |_, _| false)
    }

    pub(crate) fn index_is_covering(
        &self,
        source: &crate::translate::semantic::hir::Source,
        index: &Index,
        exprs: &hir::ExprArena,
    ) -> bool {
        self.index_is_covering_with(index, |expression, index| {
            let expression = exprs.at(*expression);
            source
                .index_expressions
                .iter()
                .find(|expressions| std::ptr::eq(expressions.index.value(), index))
                .is_some_and(|expressions| {
                    expressions
                        .columns
                        .iter()
                        .flatten()
                        .any(|indexed| expression.equivalent(exprs.at(*indexed)))
                        || expressions
                            .predicate
                            .is_some_and(|predicate| expression.equivalent(exprs.at(predicate)))
                })
        })
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
pub struct ExpressionIndexUsage<E = ast::Expr> {
    /// Normalized expression as stored on an index column.
    /// Example: `lower(name)` for INDEX ON t(lower(name)).
    pub normalized_expr: Box<E>,
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

/// Hash join semantics. Build = LHS (populates hash table), Probe = RHS (scanned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashJoinType {
    /// Only matching rows emitted.
    Inner,
    /// All build rows appear; unmatched build rows get NULLs for the probe side.
    LeftOuter,
    /// Like LeftOuter, plus unmatched probe rows get NULLs for the build side.
    FullOuter,
    /// Only the build rows no probe row matched appear: the probe side is
    /// the inner table of a NOT EXISTS.
    LeftAnti,
}

impl HashJoinType {
    /// Whether the build rows that no probe row matched are emitted after
    /// the probe loop.
    pub fn keeps_unmatched_build_rows(self) -> bool {
        matches!(self, Self::LeftOuter | Self::FullOuter | Self::LeftAnti)
    }
}

/// The planner's estimate for one table access, shown in JSON query plans.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TablePlanEstimate {
    /// Rows from the join prefix before this table access.
    pub input_rows: f64,
    /// Rows that this access returns for each input row.
    pub rows_per_input: f64,
    /// Rows from the join prefix after this table access.
    pub output_rows: f64,
    /// Cost of this table access for all input rows.
    pub access_cost: f64,
    /// Cost of the join prefix through this table access.
    pub total_cost: f64,
}

/// A definition of a rowid/index search.
///
/// [SeekKey] is the condition that is used to seek to a specific row in a table/index.
/// [SeekKey] also used to represent range scan termination condition.
#[derive(Debug, Clone)]
pub struct SeekDef<E = ast::Expr> {
    /// Common prefix of the key which is shared between start/end fields
    /// For example, given:
    /// - CREATE INDEX i ON t (x, y desc)
    /// - SELECT * FROM t WHERE x = 1 AND y >= 30
    ///
    /// Then, prefix=[(eq=1, ASC)], start=Some((ge, Expr(30))), end=Some((gt, Sentinel))
    pub prefix: Vec<SeekRangeConstraint<E>>,
    /// The condition to use when seeking. See [SeekKey] for more details.
    pub start: SeekKey<E>,
    /// The condition to use when terminating the scan that follows the seek. See [SeekKey] for more details.
    pub end: SeekKey<E>,
    /// The direction of the scan that follows the seek.
    pub iter_dir: IterationDirection,
}

pub(crate) type HirSeekDef = SeekDef<hir::ExprId>;

pub struct SeekDefKeyIterator<'a, E, T> {
    seek_def: &'a SeekDef<E>,
    seek_key: &'a SeekKey<E>,
    pos: usize,
    _t: PhantomData<T>,
}

impl<'a, E> Iterator for SeekDefKeyIterator<'a, E, SeekKeyComponent<&'a E>> {
    type Item = SeekKeyComponent<&'a E>;

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

impl<E> Iterator for SeekDefKeyIterator<'_, E, Affinity> {
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

impl<E> SeekDef<E> {
    /// returns amount of values in the given seek key
    /// - so, for SELECT * FROM t WHERE x = 10 AND y = 20 AND y >= 30 there will be 3 values (10, 20, 30)
    pub fn size(&self, key: &SeekKey<E>) -> usize {
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
        key: &'a SeekKey<E>,
    ) -> SeekDefKeyIterator<'a, E, SeekKeyComponent<&'a E>> {
        SeekDefKeyIterator {
            seek_def: self,
            seek_key: key,
            pos: 0,
            _t: PhantomData,
        }
    }

    /// iterate over affinity in the given seek key
    pub fn iter_affinity<'a>(&'a self, key: &'a SeekKey<E>) -> SeekDefKeyIterator<'a, E, Affinity> {
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
pub fn synthesized_seek_affinity_str<E>(
    index: &Index,
    seek_def: &SeekDef<E>,
) -> Option<Arc<String>> {
    let num_key_cols = seek_def.size(&seek_def.start);
    let total_cols = index.columns.len() + if index.has_rowid { 1 } else { 0 };
    let mut aff: String = seek_def
        .iter_affinity(&seek_def.start)
        .map(|a| a.aff_mask())
        .collect();
    for _ in num_key_cols..total_cols {
        aff.push(affinity::SQLITE_AFF_BLOB);
    }
    aff.chars()
        .any(|c| c != affinity::SQLITE_AFF_BLOB)
        .then(|| Arc::new(aff))
}

/// [SeekKeyComponent] represents the optional trailing component of a seek key.
/// Besides user-provided expressions, planner logic may inject a synthetic NULL sentinel
/// to encode SQLite-compatible boundary behavior on composite indexes.
/// This enum accepts generic argument E so we can use both
/// SeekKeyComponent<ast::Expr> and SeekKeyComponent<&ast::Expr>.
#[derive(Debug, Clone)]
pub enum SeekKeyComponent<E> {
    Expr(E),
    Null,
    None,
}

/// A condition to use when seeking.
#[derive(Debug, Clone)]
pub struct SeekKey<E = ast::Expr> {
    /// Complete key must be constructed from common [SeekDef::prefix] and optional last_component
    pub last_component: SeekKeyComponent<E>,

    /// The comparison operator to use when seeking.
    pub op: SeekOp,

    /// Affinity of the comparison
    pub affinity: Affinity,
}

/// A window function's effective frame. The bounds are interpreted per `mode`:
/// `Rows` counts physical rows, `Range` and `Groups` work over peer groups of
/// the window's ORDER BY values.
///
/// Example: `<mode: RANGE> <start: UNBOUNDED PRECEDING> TO <end: CURRENT ROW>`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub mode: ast::FrameMode,
    pub start: ast::FrameBound,
    pub end: ast::FrameBound,
    /// The EXCLUDE clause the user wrote, or `None` if they wrote none.
    ///
    /// `Some(NoOthers)` (an explicit `EXCLUDE NO OTHERS`) excludes
    /// nothing, so it returns the same rows as `None`. We keep the two
    /// apart anyway: like SQLite, any EXCLUDE clause — even NO OTHERS —
    /// switches the window to the slower path that recomputes each
    /// function over the whole frame per output row. Only `None` gets the
    /// fast streaming path.
    pub exclude: Option<ast::FrameExclude>,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            mode: ast::FrameMode::Range,
            start: ast::FrameBound::UnboundedPreceding,
            end: ast::FrameBound::CurrentRow,
            exclude: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::alloc::TursoFromIterator;
    use crate::translate::semantic::hir;
    use turso_parser::ast::SortOrder;

    use super::*;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    type TestResult = std::result::Result<(), alloc::TryReserveError>;

    fn hir_integer(exprs: &mut hir::ExprArena, value: i64) -> hir::ExprId {
        exprs.push(hir::Expr::Literal(ast::Literal::Numeric(value.to_string())))
    }

    fn hir_and(exprs: &mut hir::ExprArena, lhs: hir::ExprId, rhs: hir::ExprId) -> hir::ExprId {
        exprs.push(hir::Expr::Binary {
            lhs,
            operator: ast::Operator::And,
            rhs,
            array_concat: false,
            custom: None,
            comparison: None,
        })
    }

    #[test]
    fn hir_seek_definition_keeps_resolved_expressions() {
        let mut exprs = hir::ExprArena::default();
        let seek: HirSeekDef = SeekDef {
            prefix: vec![SeekRangeConstraint::new_eq(
                SortOrder::Asc,
                ast::NullsOrder::First,
                (
                    ast::Operator::Equals,
                    hir_integer(&mut exprs, 7),
                    Affinity::Integer,
                ),
            )],
            start: SeekKey {
                last_component: SeekKeyComponent::None,
                op: SeekOp::GE { eq_only: true },
                affinity: Affinity::Blob,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::None,
                op: SeekOp::LE { eq_only: true },
                affinity: Affinity::Blob,
            },
            iter_dir: IterationDirection::Forwards,
        };

        let values = seek.iter(&seek.start).collect::<Vec<_>>();
        assert!(matches!(
            values.as_slice(),
            [SeekKeyComponent::Expr(value)] if matches!(
                &exprs[**value],
                hir::Expr::Literal(ast::Literal::Numeric(value)) if value == "7"
            )
        ));
        assert_eq!(
            seek.iter_affinity(&seek.start).collect::<Vec<_>>(),
            [Affinity::Integer]
        );
    }

    fn hir_output(id: hir::OutputId, expr: hir::ExprId) -> hir::Output {
        hir::Output {
            id,
            name: "value".into(),
            expr,
            type_fact: hir::TypeFact::dynamic(),
            affinity: Affinity::Blob,
            schema_affinity: Affinity::Blob,
            has_affinity: false,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        }
    }

    fn hir_dependency_document(
        output_expr: hir::Expr,
        captures: Vec<hir::SourceId>,
    ) -> hir::HirDocument {
        let mut exprs = hir::ExprArena::default();
        let output_expr = exprs.push(output_expr);
        let outer_query = hir::QueryId::new(0);
        let outer_block = hir::QueryBlockId::new(outer_query, 0);
        let output = hir::OutputId::query(outer_block, 0);
        let mut block = hir::QueryBlock::new(
            outer_block,
            hir::QueryBlockBody::Values { rows: Vec::new() },
        );
        block.outputs.push(hir_output(output, output_expr));

        let inner_query = hir::QueryId::new(1);
        let inner_block = hir::QueryBlockId::new(inner_query, 0);
        hir::HirDocument {
            snapshot: hir::CatalogSnapshot::from_id(0),
            databases: Vec::new(),
            root: hir::HirRoot::Query(hir::QueryRoot { query: outer_query }),
            queries: vec![
                hir::Query {
                    id: outer_query,
                    parent: None,
                    captures: Vec::new(),
                    reachable_ctes: Vec::new(),
                    blocks: vec![block],
                    first: outer_block,
                    compounds: Vec::new(),
                    order_by: Vec::new(),
                    limit: None,
                    output: vec![output],
                },
                hir::Query {
                    id: inner_query,
                    parent: Some(outer_query),
                    captures,
                    reachable_ctes: Vec::new(),
                    blocks: vec![hir::QueryBlock::new(
                        inner_block,
                        hir::QueryBlockBody::Values { rows: Vec::new() },
                    )],
                    first: inner_block,
                    compounds: Vec::new(),
                    order_by: Vec::new(),
                    limit: None,
                    output: Vec::new(),
                },
            ],
            sources: Vec::new(),
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
            exprs,
            planning: hir::PlanningTerms::default(),
        }
    }

    #[test]
    fn hir_implied_in_terms_need_every_or_branch_to_fix_the_column() {
        use crate::translate::semantic::hir::Expr;
        let table = hir::SourceId::new(1);
        let comparison = hir::ComparisonSemantics {
            components: vec![hir::ComparisonComponent {
                affinity: Affinity::Integer,
                collation: None,
                array: false,
            }],
        };
        let mut exprs = hir::ExprArena::default();
        let equals = |exprs: &mut hir::ExprArena, column: usize, value: hir::ExprId| {
            let lhs = exprs.push(Expr::column(table, column));
            exprs.push(Expr::Binary {
                lhs,
                operator: ast::Operator::Equals,
                rhs: value,
                array_concat: false,
                custom: None,
                comparison: Some(comparison.clone()),
            })
        };
        let or = |exprs: &mut hir::ExprArena, lhs: hir::ExprId, rhs: hir::ExprId| {
            exprs.push(Expr::Binary {
                lhs,
                operator: ast::Operator::Or,
                rhs,
                array_concat: false,
                custom: None,
                comparison: None,
            })
        };
        let negative = |exprs: &mut hir::ExprArena, value: i64| {
            let expr = hir_integer(exprs, value);
            exprs.push(Expr::Unary {
                operator: ast::UnaryOperator::Negative,
                expr,
            })
        };
        let in_values = |exprs: &hir::ExprArena, term: hir::ExprId| match &exprs[term] {
            Expr::InList {
                lhs,
                negated: false,
                values,
                comparisons,
            } => {
                assert_eq!(values.len(), comparisons.len());
                let Expr::Column(column) = &exprs[*lhs] else {
                    panic!("an implied IN tests a column");
                };
                (column.column, values.clone())
            }
            _ => panic!("an implied term is an IN list"),
        };
        let implied =
            |exprs: &mut hir::ExprArena,
             expression: hir::ExprId,
             starts_index: &dyn Fn(hir::SourceId, &[usize]) -> Option<usize>| {
                hir_implied_in_terms(&implied_in_terms(exprs, expression), starts_index)
            };

        // Both columns are fixed by every branch; only the one that starts
        // an index gets a filter. A repeated value is listed once.
        let one = hir_integer(&mut exprs, 1);
        let ten = hir_integer(&mut exprs, 10);
        let first_left = equals(&mut exprs, 0, one);
        let first_right = equals(&mut exprs, 1, ten);
        let first = hir_and(&mut exprs, first_left, first_right);
        let twenty = hir_integer(&mut exprs, 20);
        let minus_two = negative(&mut exprs, 2);
        let second_left = equals(&mut exprs, 1, twenty);
        let second_right = equals(&mut exprs, 0, minus_two);
        let second = hir_and(&mut exprs, second_left, second_right);
        let one = hir_integer(&mut exprs, 1);
        let thirty = hir_integer(&mut exprs, 30);
        let third_left = equals(&mut exprs, 0, one);
        let third_right = equals(&mut exprs, 1, thirty);
        let third = hir_and(&mut exprs, third_left, third_right);
        let first_two = or(&mut exprs, first, second);
        let expression = or(&mut exprs, first_two, third);
        let starts_index = |_: hir::SourceId, columns: &[usize]| {
            assert_eq!(columns, [0, 1]);
            Some(1)
        };
        let same_values =
            |exprs: &hir::ExprArena, values: &[hir::ExprId], expected: &[hir::ExprId]| {
                values.len() == expected.len()
                    && values
                        .iter()
                        .zip(expected)
                        .all(|(value, expected)| exprs.at(*value).equivalent(exprs.at(*expected)))
            };
        let terms = implied(&mut exprs, expression, &starts_index);
        assert_eq!(terms.len(), 1);
        let (column, values) = in_values(&exprs, terms[0]);
        assert_eq!(column, 1);
        let expected = [
            hir_integer(&mut exprs, 10),
            hir_integer(&mut exprs, 20),
            hir_integer(&mut exprs, 30),
        ];
        assert!(same_values(&exprs, &values, &expected));

        // With one fixed column, no index is needed.
        let one = hir_integer(&mut exprs, 1);
        let left = equals(&mut exprs, 0, one);
        let minus_two = negative(&mut exprs, 2);
        let right = equals(&mut exprs, 0, minus_two);
        let expression = or(&mut exprs, left, right);
        let terms = implied(&mut exprs, expression, &|_, _| None);
        assert_eq!(terms.len(), 1);
        let (column, values) = in_values(&exprs, terms[0]);
        assert_eq!(column, 0);
        let expected = [hir_integer(&mut exprs, 1), negative(&mut exprs, 2)];
        assert!(same_values(&exprs, &values, &expected));

        // A branch that does not fix the column, or fixes it to a non-literal
        // or to NULL, gives no filter.
        let other_column_value = hir_integer(&mut exprs, 2);
        let other_column = equals(&mut exprs, 1, other_column_value);
        let column_value = exprs.push(Expr::column(table, 2));
        let non_literal = equals(&mut exprs, 0, column_value);
        let null_value = exprs.push(Expr::Literal(ast::Literal::Null));
        let null = equals(&mut exprs, 0, null_value);
        for second in [other_column, non_literal, null] {
            let one = hir_integer(&mut exprs, 1);
            let first = equals(&mut exprs, 0, one);
            let expression = or(&mut exprs, first, second);
            assert!(implied(&mut exprs, expression, &|_, _| None).is_empty());
        }
    }

    #[test]
    fn hir_predicates_split_without_ast_conversion() {
        let mut exprs = hir::ExprArena::default();
        let one = hir_integer(&mut exprs, 1);
        let two = hir_integer(&mut exprs, 2);
        let three = hir_integer(&mut exprs, 3);
        let rest = hir_and(&mut exprs, two, three);
        let expression = hir_and(&mut exprs, one, rest);

        let terms: Vec<HirWhereTerm> = exprs
            .at(expression)
            .conjuncts()
            .map(|conjunct| conjunct.id().into())
            .collect();

        let values = terms
            .iter()
            .map(|term| match &exprs[term.expr] {
                crate::translate::semantic::hir::Expr::Literal(ast::Literal::Numeric(value)) => {
                    value.as_str()
                }
                _ => panic!("conjunct remains an HIR numeric literal"),
            })
            .collect::<Vec<_>>();
        assert_eq!(values, ["1", "2", "3"]);
        assert!(terms.iter().all(|term| term.from_outer_join.is_none()));
    }

    #[test]
    fn hir_expression_dependencies_map_to_local_from_positions() -> Result<()> {
        let first = hir::SourceId::new(10);
        let joined = hir::SourceId::new(20);
        let outside = hir::SourceId::new(30);
        let from = hir::From {
            first,
            joins: vec![hir::Join {
                right: joined,
                kind: hir::JoinKind::Inner,
                constraint: hir::JoinConstraint::None,
            }],
        };
        let output = hir::OutputId::query(hir::QueryBlockId::new(hir::QueryId::new(0), 0), 0);
        let mut document =
            hir_dependency_document(hir::Expr::column(joined, 0), vec![joined, outside]);
        let exprs = &mut document.exprs;

        let first_column = exprs.push(hir::Expr::column(first, 0));
        let joined_rowid = exprs.push(hir::Expr::rowid(joined));
        let direct = hir_and(exprs, first_column, joined_rowid);

        let merged_left = exprs.push(hir::Expr::column(first, 0));
        let merged_right = exprs.push(hir::Expr::Column(hir::ColumnRef {
            source: joined,
            column: 0,
        }));
        let merged = exprs.push(hir::Expr::MergedColumn(Box::new(hir::MergedColumn {
            left: merged_left,
            right: merged_right,
            value: hir::MergedColumnValue::Coalesce,
            type_fact: hir::TypeFact::dynamic(),
            affinity: Affinity::Blob,
            has_affinity: false,
            collation: None,
        })));

        let output_expr = exprs.push(hir::Expr::output(output));

        let subquery_lhs = exprs.push(hir::Expr::column(first, 0));
        let subquery = exprs.push(hir::Expr::Subquery(hir::SubqueryExpr::In {
            lhs: subquery_lhs,
            query: hir::QueryId::new(1),
            negated: false,
            comparison: hir::ComparisonSemantics {
                components: vec![hir::ComparisonComponent {
                    affinity: Affinity::Blob,
                    collation: None,
                    array: false,
                }],
            },
        }));

        let duplicate_left = exprs.push(hir::Expr::column(first, 0));
        let duplicate_right = exprs.push(hir::Expr::column(first, 1));
        let duplicate = hir_and(exprs, duplicate_left, duplicate_right);

        let direct_mask =
            crate::translate::planner::table_mask_from_hir_expr(&document, Some(&from), direct)?;
        assert!(direct_mask.get(0));
        assert!(direct_mask.get(1));

        let merged_mask =
            crate::translate::planner::table_mask_from_hir_expr(&document, Some(&from), merged)?;
        assert!(merged_mask.get(0));
        assert!(merged_mask.get(1));

        let output_mask = crate::translate::planner::table_mask_from_hir_expr(
            &document,
            Some(&from),
            output_expr,
        )?;
        assert!(!output_mask.get(0));
        assert!(output_mask.get(1));

        let subquery_mask =
            crate::translate::planner::table_mask_from_hir_expr(&document, Some(&from), subquery)?;
        assert!(subquery_mask.get(0));
        assert!(subquery_mask.get(1));
        assert_eq!(subquery_mask.iter().count(), 2);

        let duplicate_mask =
            crate::translate::planner::table_mask_from_hir_expr(&document, Some(&from), duplicate)?;
        assert_eq!(duplicate_mask.iter().collect::<Vec<_>>(), [0]);

        Ok(())
    }

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
        use turso_parser::ast::TableInternalId;
        let a = TableInternalId::from(3);
        let b = TableInternalId::from(70); // exercises overflow path
        let c = TableInternalId::from(200);

        let mut mask: BitSet<TableInternalId> = BitSet::default();
        mask.set(a)?;
        mask.set(b)?;
        mask.set(c)?;

        assert!(mask.get(a));
        assert!(mask.get(b));
        assert!(mask.get(c));
        assert!(!mask.get(TableInternalId::from(4)));
        assert_eq!(mask.count(), 3);

        mask.clear(b);
        assert!(!mask.get(b));
        assert_eq!(mask.count(), 2);

        // Iterator yields TableInternalId, not usize.
        let collected: Vec<TableInternalId> = (&mask).into_iter().collect();
        assert_eq!(collected, vec![a, c]);

        // Fallible collection preserves TableInternalId.
        let rebuilt = BitSet::<TableInternalId>::try_from_iter([a, c])?;
        assert_eq!(rebuilt, mask);
        let mut extended = BitSet::<TableInternalId>::default();
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
