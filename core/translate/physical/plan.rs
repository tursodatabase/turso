//! A correctness-first physical view of one closed HIR document.
//!
//! This initial planner deliberately chooses scans unless SQL contains a
//! resolved `INDEXED BY` requirement. Cost-based choices can be added here
//! after the HIR execution path is complete and measured.

use std::fmt;

use crate::translate::semantic::hir::{
    self, CteId, HirDocument, IndexHint, Output, Query, QueryBlock, QueryBlockId, QueryId,
    ResolvedIndex, ResolvedTable, SourceId, SourceKind,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PhysicalPlanError {
    InvalidDocument(String),
}

impl fmt::Display for PhysicalPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDocument(message) => write!(formatter, "invalid HIR document: {message}"),
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
}

impl<'hir> PhysicalPlan<'hir> {
    pub(crate) fn new(document: &'hir HirDocument) -> Result<Self, PhysicalPlanError> {
        document
            .validate()
            .map_err(|error| PhysicalPlanError::InvalidDocument(error.to_string()))?;
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
        let queries = document
            .queries
            .iter()
            .map(|query| {
                let blocks = query
                    .blocks
                    .iter()
                    .map(|block| {
                        let (aggregates, window_functions) = collect_block_functions(query, block)?;
                        Ok(PhysicalQueryBlock {
                            id: block.id,
                            hir: block,
                            source_order: block.from.as_ref().map_or_else(Vec::new, |from| {
                                std::iter::once(from.first)
                                    .chain(from.joins.iter().map(|join| join.right))
                                    .collect()
                            }),
                            outputs: &block.outputs,
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
                        access: match &source.index_hint {
                            IndexHint::Indexed(index) => TableAccess::ForcedIndex(index),
                            IndexHint::None | IndexHint::NotIndexed => TableAccess::Scan,
                        },
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
