//! A correctness-first physical view of one closed HIR document.
//!
//! This initial planner deliberately chooses scans unless SQL contains a
//! resolved `INDEXED BY` requirement. Cost-based choices can be added here
//! after the HIR execution path is complete and measured.

use std::{borrow::Cow, collections::BTreeMap, fmt};

use rustc_hash::FxHashMap;
use turso_parser::ast::Operator;

use crate::translate::semantic::hir::{
    self, CteId, Expr, HirDocument, IndexHint, IndexMethodPattern, Output, OutputId, Query,
    QueryBlock, QueryBlockId, QueryId, ResolvedIndex, ResolvedTable, SourceId, SourceKind,
    SourceOwner,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PhysicalPlanError {
    InvalidDocument(String),
    UnsupportedQuery(&'static str),
}

impl fmt::Display for PhysicalPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDocument(message) => write!(formatter, "invalid HIR document: {message}"),
            Self::UnsupportedQuery(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for PhysicalPlanError {}

#[derive(Debug)]
pub(crate) struct PhysicalPlan<'hir> {
    pub(crate) document: &'hir HirDocument,
    pub(crate) root: PhysicalRoot<'hir>,
    pub(crate) queries: Vec<PhysicalQuery<'hir>>,
    pub(crate) sources: Vec<PhysicalSource<'hir>>,
}

#[derive(Debug)]
pub(crate) enum PhysicalRoot<'hir> {
    Query(QueryId),
    Insert(&'hir hir::Insert),
    Update(&'hir hir::Update),
    Delete(&'hir hir::Delete),
    TriggerPredicate(&'hir hir::TriggerPredicate),
    SchemaExpressions(&'hir hir::SchemaExpressionRoot),
}

#[derive(Debug)]
pub(crate) struct PhysicalQuery<'hir> {
    pub(crate) id: QueryId,
    pub(crate) hir: &'hir Query,
    pub(crate) blocks: Vec<PhysicalQueryBlock<'hir>>,
}

#[derive(Debug)]
pub(crate) struct PhysicalQueryBlock<'hir> {
    pub(crate) id: QueryBlockId,
    pub(crate) hir: &'hir QueryBlock,
    /// Sources in the order in which a scan-only nested-loop plan visits them.
    pub(crate) source_order: Vec<SourceId>,
    /// These are the original resolved HIR outputs, not copied expressions.
    pub(crate) outputs: &'hir [Output],
    /// WHERE after removing the one term consumed by a custom index method.
    pub(crate) filter: Option<Cow<'hir, Expr>>,
    /// Pattern outputs that replace computed query outputs, aligned with
    /// `outputs`. Ordinary outputs contain `None`.
    pub(crate) covered_outputs: Vec<Option<OutputId>>,
    /// Aggregate calls indexed by their stable block-local HIR identity.
    pub(crate) aggregates: Vec<PhysicalAggregate<'hir>>,
    /// Window calls indexed by their stable block-local HIR identity.
    pub(crate) window_functions: Vec<PhysicalWindowFunction<'hir>>,
}

#[derive(Debug)]
pub(crate) struct PhysicalAggregate<'hir> {
    pub(crate) id: hir::AggregateId,
    pub(crate) call: &'hir hir::FunctionCall,
}

#[derive(Debug)]
pub(crate) struct PhysicalWindowFunction<'hir> {
    pub(crate) id: hir::WindowFunctionId,
    pub(crate) call: &'hir hir::FunctionCall,
}

#[derive(Debug)]
pub(crate) struct PhysicalSource<'hir> {
    pub(crate) id: SourceId,
    pub(crate) width: usize,
    pub(crate) kind: PhysicalSourceKind<'hir>,
}

#[derive(Debug)]
pub(crate) enum PhysicalSourceKind<'hir> {
    CatalogTable {
        table: &'hir ResolvedTable,
        access: TableAccess<'hir>,
    },
    TableFunction {
        table: &'hir ResolvedTable,
        arguments: &'hir [hir::Expr],
    },
    Cte(CteId),
    Derived(QueryId),
    RecursiveInput(CteId),
    Pseudo {
        table: &'hir ResolvedTable,
        kind: hir::PseudoSource,
    },
    SchemaExpression,
}

#[derive(Debug)]
pub(crate) enum TableAccess<'hir> {
    Scan,
    ForcedIndex(&'hir ResolvedIndex),
    IndexMethod(IndexMethodAccess<'hir>),
}

#[derive(Debug)]
pub(crate) struct IndexMethodAccess<'hir> {
    pub(crate) pattern: &'hir IndexMethodPattern,
    pub(crate) arguments: Vec<&'hir Expr>,
}

impl<'hir> PhysicalPlan<'hir> {
    pub(crate) fn new(document: &'hir HirDocument) -> Result<Self, PhysicalPlanError> {
        document
            .validate()
            .map_err(|error| PhysicalPlanError::InvalidDocument(error.to_string()))?;
        for query in &document.queries {
            for block in &query.blocks {
                reject_unsupported_full_join_constraint(block)?;
                reject_correlated_subquery_with_full_join(document, query, block)?;
            }
        }
        let root = match &document.root {
            hir::HirRoot::Query(root) => PhysicalRoot::Query(root.query),
            hir::HirRoot::Insert(insert) => PhysicalRoot::Insert(insert),
            hir::HirRoot::Update(update) => PhysicalRoot::Update(update),
            hir::HirRoot::Delete(delete) => PhysicalRoot::Delete(delete),
            hir::HirRoot::TriggerPredicate(predicate) => PhysicalRoot::TriggerPredicate(predicate),
            hir::HirRoot::SchemaExpressions(expressions) => {
                PhysicalRoot::SchemaExpressions(expressions)
            }
        };
        let mut index_method_accesses = FxHashMap::default();
        let queries = document
            .queries
            .iter()
            .map(|query| {
                let blocks = query
                    .blocks
                    .iter()
                    .map(|block| {
                        let (aggregates, window_functions) = collect_block_functions(query, block)?;
                        let index_method = choose_index_method(document, query, block);
                        let (filter, covered_outputs) = if let Some(selection) = index_method {
                            index_method_accesses.insert(selection.source, selection.access);
                            (selection.filter, selection.covered_outputs)
                        } else {
                            (
                                block_filter(block).map(Cow::Borrowed),
                                vec![None; block.outputs.len()],
                            )
                        };
                        Ok(PhysicalQueryBlock {
                            id: block.id,
                            hir: block,
                            source_order: block.from.as_ref().map_or_else(Vec::new, |from| {
                                std::iter::once(from.first)
                                    .chain(from.joins.iter().map(|join| join.right))
                                    .collect()
                            }),
                            outputs: &block.outputs,
                            filter,
                            covered_outputs,
                            aggregates,
                            window_functions,
                        })
                    })
                    .collect::<Result<Vec<_>, PhysicalPlanError>>()?;
                Ok(PhysicalQuery {
                    id: query.id,
                    hir: query,
                    blocks,
                })
            })
            .collect::<Result<Vec<_>, PhysicalPlanError>>()?;
        let sources = document
            .sources
            .iter()
            .map(|source| PhysicalSource {
                id: source.id,
                width: source.columns.len(),
                kind: match &source.kind {
                    SourceKind::SchemaExpression => PhysicalSourceKind::SchemaExpression,
                    SourceKind::Table(table) => PhysicalSourceKind::CatalogTable {
                        table,
                        access: index_method_accesses
                            .remove(&source.id)
                            .map(TableAccess::IndexMethod)
                            .unwrap_or_else(|| match &source.index_hint {
                                IndexHint::Indexed(index) => TableAccess::ForcedIndex(index),
                                IndexHint::None | IndexHint::NotIndexed => TableAccess::Scan,
                            }),
                    },
                    SourceKind::TableFunction { table, arguments } => {
                        PhysicalSourceKind::TableFunction { table, arguments }
                    }
                    SourceKind::Cte(cte) => PhysicalSourceKind::Cte(*cte),
                    SourceKind::Derived(query) => PhysicalSourceKind::Derived(*query),
                    SourceKind::RecursiveInput(cte) => PhysicalSourceKind::RecursiveInput(*cte),
                    SourceKind::Pseudo { kind, table } => {
                        PhysicalSourceKind::Pseudo { table, kind: *kind }
                    }
                },
            })
            .collect();
        Ok(Self {
            document,
            root,
            queries,
            sources,
        })
    }

    pub(crate) fn query(&self, id: QueryId) -> Option<&PhysicalQuery<'hir>> {
        self.queries.get(id.index()).filter(|query| query.id == id)
    }

    pub(crate) fn source(&self, id: SourceId) -> Option<&PhysicalSource<'hir>> {
        self.sources
            .get(id.index())
            .filter(|source| source.id == id)
    }
}

fn reject_unsupported_full_join_constraint(block: &QueryBlock) -> Result<(), PhysicalPlanError> {
    let Some(from) = &block.from else {
        return Ok(());
    };
    for join in &from.joins {
        let has_using_columns = match &join.constraint {
            hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                !columns.is_empty()
            }
            hir::JoinConstraint::None | hir::JoinConstraint::On(_) => false,
        };
        if join.kind == hir::JoinKind::Full && has_using_columns {
            return Err(PhysicalPlanError::UnsupportedQuery(
                "FULL OUTER JOIN requires an equality condition in the ON clause",
            ));
        }
    }
    Ok(())
}

fn reject_correlated_subquery_with_full_join(
    document: &HirDocument,
    query: &Query,
    block: &QueryBlock,
) -> Result<(), PhysicalPlanError> {
    let has_full_join = block.from.as_ref().is_some_and(|from| {
        from.joins
            .iter()
            .any(|join| join.kind == hir::JoinKind::Full)
    });
    if !has_full_join {
        return Ok(());
    }

    let mut has_correlated_subquery = false;
    let mut inspect = |expression: &hir::Expr| {
        expression.walk(&mut |expression| {
            let hir::Expr::Subquery(subquery) = expression else {
                return;
            };
            let query_id = match subquery {
                hir::SubqueryExpr::Scalar { query, .. }
                | hir::SubqueryExpr::Exists(query)
                | hir::SubqueryExpr::In { query, .. } => *query,
            };
            has_correlated_subquery |= query_tree_has_outer_dependency(document, query_id);
        });
    };

    for output in &block.outputs {
        inspect(&output.expr);
    }
    if let Some(from) = &block.from {
        for join in &from.joins {
            match &join.constraint {
                hir::JoinConstraint::On(expression) => inspect(expression),
                hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                    for column in columns {
                        inspect(&column.left);
                    }
                }
                hir::JoinConstraint::None => {}
            }
        }
    }
    match &block.body {
        hir::QueryBlockBody::Select {
            filter,
            grouping,
            windows,
            ..
        } => {
            if let Some(filter) = filter {
                inspect(filter);
            }
            if let Some(grouping) = grouping {
                for key in &grouping.keys {
                    inspect(key);
                }
                if let Some(having) = &grouping.having {
                    inspect(having);
                }
            }
            for window in windows {
                for expression in &window.spec.partition_by {
                    inspect(expression);
                }
                for term in &window.spec.order_by {
                    inspect(&term.expr);
                }
            }
        }
        hir::QueryBlockBody::Values { rows } => {
            for expression in rows.iter().flatten() {
                inspect(expression);
            }
        }
    }
    if block.id == query.first {
        for term in &query.order_by {
            inspect(&term.expr);
        }
        if let Some(limit) = &query.limit {
            inspect(&limit.limit);
            if let Some(offset) = &limit.offset {
                inspect(offset);
            }
        }
    }

    if has_correlated_subquery {
        return Err(PhysicalPlanError::UnsupportedQuery(
            "FULL OUTER JOIN is not supported with correlated subqueries that reference the joined tables",
        ));
    }
    Ok(())
}

/// Return whether this query tree reads a source owned outside the tree.
/// Direct captures are not enough: a capture-free scalar query can contain a
/// derived query that reaches through it to the scalar query's parent.
pub(super) fn query_tree_has_outer_dependency(document: &HirDocument, root: QueryId) -> bool {
    let mut tree = rustc_hash::FxHashSet::default();
    let mut pending = vec![root];
    while let Some(query_id) = pending.pop() {
        if !tree.insert(query_id) {
            continue;
        }
        pending.extend(
            document
                .queries
                .iter()
                .filter(|query| query.parent == Some(query_id))
                .map(|query| query.id),
        );
    }
    tree.iter().any(|query_id| {
        document.query(*query_id).is_some_and(|query| {
            query.captures.iter().any(|source_id| {
                !matches!(
                    document.source(*source_id).map(|source| source.owner),
                    Some(hir::SourceOwner::QueryBlock(block)) if tree.contains(&block.query)
                )
            })
        })
    })
}

fn collect_block_functions<'hir>(
    query: &'hir Query,
    block: &'hir QueryBlock,
) -> Result<
    (
        Vec<PhysicalAggregate<'hir>>,
        Vec<PhysicalWindowFunction<'hir>>,
    ),
    PhysicalPlanError,
> {
    let mut aggregates = vec![None; block.aggregate_count];
    let mut window_functions = vec![None; block.window_function_count];
    let mut collect = |expression: &'hir hir::Expr| -> Result<(), PhysicalPlanError> {
        let mut error = None;
        expression.walk(&mut |expression| {
            if error.is_some() {
                return;
            }
            let hir::Expr::Function(call) = expression else {
                return;
            };
            match call.evaluation {
                hir::FunctionEvaluation::Scalar => {}
                hir::FunctionEvaluation::Aggregate(id) => {
                    if id.block != block.id {
                        error = Some(PhysicalPlanError::InvalidDocument(format!(
                            "aggregate {id:?} appears in block {:?}",
                            block.id
                        )));
                        return;
                    }
                    let Some(slot) = aggregates.get_mut(id.index) else {
                        error = Some(PhysicalPlanError::InvalidDocument(format!(
                            "aggregate {id:?} is outside its block"
                        )));
                        return;
                    };
                    if slot.replace(call).is_some() {
                        error = Some(PhysicalPlanError::InvalidDocument(format!(
                            "aggregate {id:?} is defined more than once"
                        )));
                    }
                }
                hir::FunctionEvaluation::Window(id) => {
                    if id.block != block.id {
                        error = Some(PhysicalPlanError::InvalidDocument(format!(
                            "window function {id:?} appears in block {:?}",
                            block.id
                        )));
                        return;
                    }
                    let Some(slot) = window_functions.get_mut(id.index) else {
                        error = Some(PhysicalPlanError::InvalidDocument(format!(
                            "window function {id:?} is outside its block"
                        )));
                        return;
                    };
                    if slot.replace(call).is_some() {
                        error = Some(PhysicalPlanError::InvalidDocument(format!(
                            "window function {id:?} is defined more than once"
                        )));
                    }
                }
            }
        });
        match error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    };

    for output in &block.outputs {
        collect(&output.expr)?;
    }
    if let Some(from) = &block.from {
        for join in &from.joins {
            match &join.constraint {
                hir::JoinConstraint::None => {}
                hir::JoinConstraint::On(expression) => collect(expression)?,
                hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                    for column in columns {
                        collect(&column.left)?;
                    }
                }
            }
        }
    }
    match &block.body {
        hir::QueryBlockBody::Select {
            filter,
            grouping,
            windows,
            ..
        } => {
            if let Some(filter) = filter {
                collect(filter)?;
            }
            if let Some(grouping) = grouping {
                for key in &grouping.keys {
                    collect(key)?;
                }
                if let Some(having) = &grouping.having {
                    collect(having)?;
                }
            }
            for window in windows {
                for expression in &window.spec.partition_by {
                    collect(expression)?;
                }
                for term in &window.spec.order_by {
                    collect(&term.expr)?;
                }
            }
        }
        hir::QueryBlockBody::Values { rows } => {
            for row in rows {
                for expression in row {
                    collect(expression)?;
                }
            }
        }
    }
    if block.id == query.first {
        for term in &query.order_by {
            collect(&term.expr)?;
        }
    }

    let aggregates = aggregates
        .into_iter()
        .enumerate()
        .map(|(index, call)| {
            call.map(|call| PhysicalAggregate {
                id: hir::AggregateId::new(block.id, index),
                call,
            })
            .ok_or_else(|| {
                PhysicalPlanError::InvalidDocument(format!(
                    "aggregate slot {index} in block {:?} has no definition",
                    block.id
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let window_functions = window_functions
        .into_iter()
        .enumerate()
        .map(|(index, call)| {
            call.map(|call| PhysicalWindowFunction {
                id: hir::WindowFunctionId::new(block.id, index),
                call,
            })
            .ok_or_else(|| {
                PhysicalPlanError::InvalidDocument(format!(
                    "window function slot {index} in block {:?} has no definition",
                    block.id
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((aggregates, window_functions))
}

struct IndexMethodSelection<'hir> {
    source: SourceId,
    access: IndexMethodAccess<'hir>,
    filter: Option<Cow<'hir, Expr>>,
    covered_outputs: Vec<Option<OutputId>>,
}

fn block_filter(block: &hir::QueryBlock) -> Option<&Expr> {
    match &block.body {
        hir::QueryBlockBody::Select { filter, .. } => filter.as_ref(),
        hir::QueryBlockBody::Values { .. } => None,
    }
}

/// Pick the first bound custom-index pattern that can answer this block.
/// This is deliberately correctness-first: it preserves source order and does
/// no cost comparison. The semantic layer has already resolved every name and
/// catalog object in both the query and the patterns.
fn choose_index_method<'hir>(
    document: &'hir HirDocument,
    query: &'hir Query,
    block: &'hir QueryBlock,
) -> Option<IndexMethodSelection<'hir>> {
    if query.blocks.len() != 1 || !query.compounds.is_empty() || block.id != query.first {
        return None;
    }
    let from = block.from.as_ref()?;
    let source_ids = std::iter::once(from.first).chain(from.joins.iter().map(|join| join.right));
    for source_id in source_ids {
        let source = document.source(source_id)?;
        if source.owner != SourceOwner::QueryBlock(block.id) {
            continue;
        }
        for pattern in &source.index_method_patterns {
            if let Some(selection) = match_index_method_pattern(document, query, block, pattern) {
                return Some(IndexMethodSelection {
                    source: source_id,
                    access: IndexMethodAccess {
                        pattern,
                        arguments: selection.arguments,
                    },
                    filter: selection.filter,
                    covered_outputs: selection.covered_outputs,
                });
            }
        }
    }
    None
}

struct PatternMatch<'hir> {
    arguments: Vec<&'hir Expr>,
    filter: Option<Cow<'hir, Expr>>,
    covered_outputs: Vec<Option<OutputId>>,
}

fn match_index_method_pattern<'hir>(
    document: &'hir HirDocument,
    query: &'hir Query,
    block: &'hir QueryBlock,
    pattern: &'hir IndexMethodPattern,
) -> Option<PatternMatch<'hir>> {
    let mut parameters = BTreeMap::new();
    let original_filter = block_filter(block);
    let mut filter_terms = Vec::new();
    if let Some(filter) = original_filter {
        split_and_terms(filter, &mut filter_terms);
    }

    let consumed_filter = if let Some(pattern_filter) = pattern.predicate.as_ref() {
        let mut matched = None;
        for (position, query_filter) in filter_terms.iter().enumerate() {
            let mut candidate = parameters.clone();
            if match_expr(document, pattern_filter, query_filter, &mut candidate, true) {
                parameters = candidate;
                matched = Some(position);
                break;
            }
        }
        Some(matched?)
    } else {
        None
    };

    let where_is_fully_consumed = filter_terms.is_empty()
        || consumed_filter.is_some_and(|position| filter_terms.len() == 1 && position == 0);
    if !where_is_fully_consumed && (!pattern.order_by.is_empty() || pattern.limit.is_some()) {
        return None;
    }

    if !pattern.order_by.is_empty() {
        if pattern.order_by.len() != query.order_by.len() {
            return None;
        }
        for (pattern_term, query_term) in pattern.order_by.iter().zip(&query.order_by) {
            if pattern_term.order != query_term.order
                || query_term.nulls.is_some()
                || !match_expr(
                    document,
                    &pattern_term.expr,
                    &query_term.expr,
                    &mut parameters,
                    true,
                )
            {
                return None;
            }
        }
    }

    if let Some(pattern_limit) = pattern.limit.as_ref() {
        let query_limit = query.limit.as_ref()?;
        if !match_expr(
            document,
            &pattern_limit.limit,
            &query_limit.limit,
            &mut parameters,
            true,
        ) {
            return None;
        }
        if let Some(pattern_offset) = pattern_limit.offset.as_ref() {
            let query_offset = query_limit.offset.as_ref()?;
            if !match_expr(
                document,
                pattern_offset,
                query_offset,
                &mut parameters,
                true,
            ) {
                return None;
            }
        }
    }

    let mut covered_outputs = vec![None; block.outputs.len()];
    for pattern_output in &pattern.outputs {
        if matches!(
            resolve_output_expr(document, &pattern_output.expr),
            Expr::Column(_) | Expr::RowId(_)
        ) {
            continue;
        }
        for (position, query_output) in block.outputs.iter().enumerate() {
            let mut candidate = parameters.clone();
            if match_expr(
                document,
                &pattern_output.expr,
                &query_output.expr,
                &mut candidate,
                true,
            ) {
                parameters = candidate;
                covered_outputs[position] = Some(pattern_output.id);
            }
        }
    }
    // A pattern with no predicate must be anchored by one of its computed
    // outputs (for example the score-only FTS pattern).
    if pattern.predicate.is_none() && covered_outputs.iter().all(Option::is_none) {
        return None;
    }

    let filter = match consumed_filter {
        None => original_filter.map(Cow::Borrowed),
        Some(consumed) => rebuild_and_filter(&filter_terms, consumed).map(Cow::Owned),
    };
    Some(PatternMatch {
        arguments: parameters.into_values().collect(),
        filter,
        covered_outputs,
    })
}

fn split_and_terms<'hir>(expression: &'hir Expr, terms: &mut Vec<&'hir Expr>) {
    if let Expr::Binary {
        lhs,
        operator: Operator::And,
        rhs,
        ..
    } = expression
    {
        split_and_terms(lhs, terms);
        split_and_terms(rhs, terms);
    } else {
        terms.push(expression);
    }
}

fn rebuild_and_filter(terms: &[&Expr], consumed: usize) -> Option<Expr> {
    terms
        .iter()
        .enumerate()
        .filter(|(position, _)| *position != consumed)
        .map(|(_, expression)| (*expression).clone())
        .reduce(|lhs, rhs| Expr::Binary {
            lhs: Box::new(lhs),
            operator: Operator::And,
            rhs: Box::new(rhs),
            array_concat: false,
            custom: None,
            comparison: None,
        })
}

fn resolve_output_expr<'hir>(document: &'hir HirDocument, expression: &'hir Expr) -> &'hir Expr {
    if let Expr::Output(output) = expression {
        document
            .output(*output)
            .map_or(expression, |output| &output.expr)
    } else {
        expression
    }
}

fn match_expr<'hir>(
    document: &'hir HirDocument,
    pattern: &'hir Expr,
    query: &'hir Expr,
    parameters: &mut BTreeMap<u32, &'hir Expr>,
    capture_parameters: bool,
) -> bool {
    let pattern = resolve_output_expr(document, pattern);
    let query = resolve_output_expr(document, query);
    if let Expr::Parameter(parameter) = pattern {
        if capture_parameters && parameter.name.is_none() {
            let index = parameter.index.get();
            if let Some(captured) = parameters.get(&index) {
                let mut ignored = BTreeMap::new();
                return match_expr(document, captured, query, &mut ignored, false);
            }
            parameters.insert(index, query);
            return true;
        }
    }
    match (pattern, query) {
        (Expr::Literal(left), Expr::Literal(right)) => {
            crate::util::check_literal_equivalency(left, right)
        }
        (Expr::Parameter(left), Expr::Parameter(right)) => {
            left.index == right.index && left.name == right.name
        }
        (Expr::Column(left), Expr::Column(right)) => left == right,
        (Expr::RowId(left), Expr::RowId(right)) => left == right,
        (Expr::Function(left), Expr::Function(right)) => {
            if left.function != right.function
                || left.star != right.star
                || left.arguments.len() != right.arguments.len()
            {
                return false;
            }
            let function_name = left.function.value().to_string();
            let unordered_columns = match function_name.to_ascii_lowercase().as_str() {
                "fts_match" | "fts_score" => left.arguments.len().saturating_sub(1),
                "fts_highlight" => left.arguments.len().saturating_sub(3),
                _ => 0,
            };
            if unordered_columns > 0 {
                let mut matched = vec![false; unordered_columns];
                for query_column in &right.arguments[..unordered_columns] {
                    let Some(position) = left.arguments[..unordered_columns]
                        .iter()
                        .enumerate()
                        .position(|(position, pattern_column)| {
                            !matched[position] && {
                                let mut ignored = BTreeMap::new();
                                match_expr(
                                    document,
                                    pattern_column,
                                    query_column,
                                    &mut ignored,
                                    false,
                                )
                            }
                        })
                    else {
                        return false;
                    };
                    matched[position] = true;
                }
            }
            left.arguments[unordered_columns..]
                .iter()
                .zip(&right.arguments[unordered_columns..])
                .all(|(left, right)| {
                    match_expr(document, left, right, parameters, capture_parameters)
                })
        }
        (
            Expr::Unary {
                operator: left_operator,
                expr: left,
            },
            Expr::Unary {
                operator: right_operator,
                expr: right,
            },
        ) => {
            left_operator == right_operator
                && match_expr(document, left, right, parameters, capture_parameters)
        }
        (
            Expr::Binary {
                lhs: left_lhs,
                operator: left_operator,
                rhs: left_rhs,
                ..
            },
            Expr::Binary {
                lhs: right_lhs,
                operator: right_operator,
                rhs: right_rhs,
                ..
            },
        ) => {
            left_operator == right_operator
                && match_expr(
                    document,
                    left_lhs,
                    right_lhs,
                    parameters,
                    capture_parameters,
                )
                && match_expr(
                    document,
                    left_rhs,
                    right_rhs,
                    parameters,
                    capture_parameters,
                )
        }
        (Expr::IsNull(left), Expr::IsNull(right)) | (Expr::NotNull(left), Expr::NotNull(right)) => {
            match_expr(document, left, right, parameters, capture_parameters)
        }
        (
            Expr::Collate {
                expr: left,
                collation: left_collation,
            },
            Expr::Collate {
                expr: right,
                collation: right_collation,
            },
        ) => {
            left_collation == right_collation
                && match_expr(document, left, right, parameters, capture_parameters)
        }
        _ => false,
    }
}
