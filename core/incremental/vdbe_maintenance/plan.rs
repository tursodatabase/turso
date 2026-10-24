use rustc_hash::FxHashSet;
use turso_parser::ast;

use super::binding::{replace_expression, visit_expression};
use super::output::NodeOutputContract;
use super::stream::{binding_rowid_metadata_width, DeltaIdentity};
use crate::alloc::TursoIteratorExt;
use crate::function::{AggFunc, Deterministic, Func};
use crate::incremental::dag::{self, Aggregate};
use crate::schema::BTreeTable;
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::translate::semantic::context::SemanticContext;
use crate::translate::semantic::hir::{self, HirDocument, QueryBlockId, QueryId, SourceId};
use crate::translate::semantic::{analyze_root, SemanticRootInput};
use crate::vdbe::affinity::Affinity;
use crate::{LimboError, Result};

/// Whether an aggregate's accumulator must see each distinct argument value
/// exactly once, tracked through the value multiset table. MIN/MAX see each
/// value once by definition, so DISTINCT changes nothing for them — their
/// multiset participation is about extremes, not distinctness.
pub(super) fn tracks_distinct_values(agg: &Aggregate) -> bool {
    agg.distinct && !matches!(agg.func, AggFunc::Min | AggFunc::Max)
}

fn unsupported<T>(what: &str) -> Result<T> {
    Err(LimboError::ParseError(format!(
        "materialized views with {what} are not yet supported",
    )))
}

/// Whether `expression` is one of the values `schema` publishes.
fn is_published(schema: &dag::StreamSchema, expression: &hir::Expr) -> bool {
    schema.columns.iter().any(|column| {
        column
            .as_ref()
            .is_some_and(|published| published.equivalent(expression))
    })
}

/// Validate an expression against the values and rowids published by one
/// operator edge.
///
/// Downstream expression compilation binds exactly these expressions to
/// registers. Validating the same contract here keeps composition
/// independent of the upstream operator kind: an aggregate result remains
/// usable through any number of linear operators, while a source value that
/// was not published cannot leak across the boundary.
fn validate_stream_expr(
    document: &HirDocument,
    expr: &hir::Expr,
    schema: &dag::StreamSchema,
    binding_rowids: &[bool],
    unavailable_value: &str,
) -> Result<()> {
    visit_expression(document, expr, &mut |node: &hir::Expr| {
        let published = is_published(schema, node);
        match node {
            hir::Expr::Subquery(_) => Err(LimboError::ParseError(
                "materialized views with subqueries are not yet supported".to_string(),
            )),
            hir::Expr::Function(call)
                if matches!(
                    call.evaluation,
                    hir::FunctionEvaluation::Aggregate { .. }
                        | hir::FunctionEvaluation::Window { .. }
                ) =>
            {
                if published {
                    Ok(false)
                } else {
                    unsupported("expressions over aggregate results")
                }
            }
            hir::Expr::Parameter(_) => Err(LimboError::ParseError(
                "materialized views with bound parameters are not supported".to_string(),
            )),
            hir::Expr::Column(_) => {
                if published {
                    Ok(false)
                } else {
                    unsupported(unavailable_value)
                }
            }
            hir::Expr::RowId(source) => {
                let available = published
                    || schema
                        .bindings
                        .iter()
                        .zip(binding_rowids.iter())
                        .any(|(binding, available)| binding.source == *source && *available);
                if available {
                    Ok(false)
                } else {
                    unsupported("rowids not published by their input stream")
                }
            }
            _ => Ok(!published),
        }
    })
}

/// Aggregate recognition and deduplication use the analyzed query's own
/// aggregate identities, so this only decides what maintenance can do with
/// each aggregate.
fn validate_supported_aggregate(agg: &Aggregate) -> Result<()> {
    match agg.func {
        AggFunc::Count0
        | AggFunc::Count
        | AggFunc::Sum
        | AggFunc::Total
        | AggFunc::Avg
        | AggFunc::Min
        | AggFunc::Max => {}
        // GROUP_CONCAT and friends have no inverse and no multiset-based
        // retraction strategy.
        ref other => return unsupported(&format!("the {} aggregate", other.as_str())),
    }
    if agg.args.len() > 1 {
        return unsupported("multi-argument aggregates");
    }
    Ok(())
}

/// Lower the analyzed query of a view into the maintenance DAG. Names,
/// stars, USING columns, ordinal GROUP BY terms and compound arity were
/// resolved by semantic analysis; correlated subqueries that can become
/// joins already have.
fn build_dag(document: &HirDocument) -> Result<dag::MaintenanceDag> {
    let hir::HirRoot::Query(root) = &document.root else {
        return Err(LimboError::InternalError(
            "materialized view analysis did not produce a query".to_string(),
        ));
    };
    let mut builder = dag::DagBuilder::new();
    let root = build_query_dag(&mut builder, document, root.query)?;
    builder.finish(root)
}

fn query(document: &HirDocument, id: QueryId) -> Result<&hir::Query> {
    document
        .query(id)
        .ok_or_else(|| LimboError::InternalError(format!("HIR query {id:?} is missing")))
}

fn block(document: &HirDocument, id: QueryBlockId) -> Result<&hir::QueryBlock> {
    document
        .query_block(id)
        .ok_or_else(|| LimboError::InternalError(format!("HIR query block {id:?} is missing")))
}

fn source(document: &HirDocument, id: SourceId) -> Result<&hir::Source> {
    document
        .source(id)
        .ok_or_else(|| LimboError::InternalError(format!("HIR source {id:?} is missing")))
}

fn build_query_dag(
    builder: &mut dag::DagBuilder,
    document: &HirDocument,
    query_id: QueryId,
) -> Result<dag::NodeId> {
    let query = query(document, query_id)?;
    if !query.order_by.is_empty() {
        return unsupported("ORDER BY");
    }
    if query.limit.is_some() {
        return unsupported("LIMIT or OFFSET");
    }
    if query.compounds.is_empty() {
        return build_block_dag(builder, document, query.first);
    }
    let blocks = std::iter::once(query.first)
        .chain(query.compounds.iter().map(|arm| arm.block))
        .collect::<Vec<_>>();
    let mut inputs = Vec::with_capacity(blocks.len());
    for block_id in &blocks {
        inputs.push(build_block_dag(builder, document, *block_id)?);
    }
    let operators: Vec<_> = query.compounds.iter().map(|arm| arm.operator).collect();
    let prefix_len = operators
        .iter()
        .rposition(|operator| *operator != ast::CompoundOperator::UnionAll)
        .map_or(0, |last_dedup| last_dedup + 2);
    let arity = block(document, query.first)?.outputs.len();
    let key_columns = if prefix_len == 0 {
        Vec::new()
    } else {
        let branches = blocks
            .iter()
            .take(prefix_len)
            .map(|block_id| block(document, *block_id))
            .collect::<Result<Vec<_>>>()?;
        (0..arity)
            .map(|index| set_op_key_column(document, &branches, index))
            .collect::<Result<Vec<_>>>()?
    };
    builder.push(dag::OpNode::SetOp {
        inputs,
        operators,
        arity,
        prefix_len,
        key_columns,
    })
}

/// The join kind of one FROM step, with `Inner` for the first source.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StepJoin {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// `expression` with every reference to an output of the query replaced by
/// the output's expression, as SQLite expands an alias in GROUP BY or
/// HAVING, and every column of a parenthesized join group replaced by the
/// value the group's own sources supply for it.
fn expand_outputs(document: &HirDocument, expression: &hir::Expr) -> hir::Expr {
    let mut expression = expression.clone();
    replace_expression(&mut expression, &|node| match node {
        hir::Expr::Output(output) => document
            .output(*output)
            .map(|output| expand_outputs(document, &output.expr)),
        hir::Expr::Column(column) => match document.source(column.source).map(|s| &s.kind) {
            Some(hir::SourceKind::FromGroup(group)) => group
                .columns
                .get(column.column)
                .map(|value| expand_outputs(document, value)),
            _ => None,
        },
        _ => None,
    });
    expression
}

fn synthesized_derived_table(source: &hir::Source) -> Result<BTreeTable> {
    let columns: crate::alloc::Vec<crate::schema::Column> = source
        .columns
        .iter()
        .map(|column| {
            crate::schema::Column::new_default_text(
                Some(column.name().to_string()),
                String::new(),
                None,
            )
        })
        .try_collect()?;
    Ok(BTreeTable::new(
        0,
        source.alias.clone().unwrap_or_else(|| source.name.clone()),
        crate::alloc::vec![],
        columns,
        crate::schema::BTreeCharacteristics::empty(),
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ))
}

/// The DAG input of one FROM source: a scan of its table, or the aliased
/// DAG of the query it stands for.
fn build_source_dag(
    builder: &mut dag::DagBuilder,
    document: &HirDocument,
    source_id: SourceId,
) -> Result<dag::NodeId> {
    let source = source(document, source_id)?;
    match &source.kind {
        hir::SourceKind::Table(table) => match table.value() {
            crate::schema::Table::BTree(table) => builder.push(dag::OpNode::Scan {
                table: table.clone(),
                source: source_id,
            }),
            crate::schema::Table::Virtual(_) => unsupported("virtual-table FROM sources"),
            _ => unsupported("this kind of FROM source"),
        },
        hir::SourceKind::Derived(query_id) => {
            let input = build_query_dag(builder, document, *query_id)?;
            builder.push(dag::OpNode::Alias {
                input,
                table: Arc::new(synthesized_derived_table(source)?),
                source: source_id,
            })
        }
        hir::SourceKind::Cte(cte_id) => {
            let cte = document.cte(*cte_id).ok_or_else(|| {
                LimboError::InternalError(format!("HIR CTE {cte_id:?} is missing"))
            })?;
            let hir::CteBody::Query(query_id) = &cte.body else {
                return unsupported("recursive CTEs");
            };
            let input = build_query_dag(builder, document, *query_id)?;
            builder.push(dag::OpNode::Alias {
                input,
                table: Arc::new(synthesized_derived_table(source)?),
                source: source_id,
            })
        }
        hir::SourceKind::TableFunction { .. } => unsupported("table-valued function FROM sources"),
        hir::SourceKind::RecursiveInput(_) => unsupported("recursive CTE inputs"),
        // A parenthesized join is joined on its own. Its stream keeps the
        // columns of the sources inside it, which the rest of the query
        // reads directly; a read through the group is expanded to the value
        // the group supplies for it (see `expand_outputs`).
        hir::SourceKind::FromGroup(group) => build_from_dag(builder, document, &group.from, None),
        hir::SourceKind::Pseudo { .. } | hir::SourceKind::SchemaExpression => {
            unsupported("this kind of FROM source")
        }
    }
}

/// The ON predicates of one join: its ON expression, or one equality per
/// USING or NATURAL column.
fn join_predicates(document: &HirDocument, constraint: &hir::JoinConstraint) -> Vec<hir::Expr> {
    match constraint {
        hir::JoinConstraint::None => Vec::new(),
        hir::JoinConstraint::On(expression) => vec![expand_outputs(document, expression)],
        hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => columns
            .iter()
            .map(|column| hir::Expr::Binary {
                lhs: Box::new(expand_outputs(document, &column.left)),
                operator: ast::Operator::Equals,
                rhs: Box::new(hir::Expr::Column(column.right)),
                array_concat: false,
                custom: None,
                comparison: Some(hir::ComparisonSemantics {
                    components: vec![hir::ComparisonComponent {
                        affinity: column.affinity,
                        collation: column.collation.clone(),
                        array: false,
                    }],
                }),
            })
            .collect(),
    }
}

/// The conjuncts of a WHERE or HAVING expression.
fn conjuncts(expression: &hir::Expr, into: &mut Vec<hir::Expr>) {
    match expression {
        hir::Expr::Binary {
            lhs,
            operator: ast::Operator::And,
            rhs,
            ..
        } => {
            conjuncts(lhs, into);
            conjuncts(rhs, into);
        }
        other => into.push(other.clone()),
    }
}

/// Every aggregate call of a block, each once, in the order of its identity.
fn block_aggregates(document: &HirDocument, block: &hir::QueryBlock) -> Result<Vec<Aggregate>> {
    let mut calls: Vec<(hir::AggregateId, Aggregate)> = Vec::new();
    let mut collect = |expression: &hir::Expr| -> Result<()> {
        visit_expression(document, expression, &mut |node| {
            let hir::Expr::Function(call) = node else {
                return Ok(true);
            };
            let hir::FunctionEvaluation::Aggregate { id, filter } = &call.evaluation else {
                return Ok(true);
            };
            if calls.iter().any(|(known, _)| known == id) {
                return Ok(false);
            }
            let func = match call.function.value() {
                Func::Agg(func) => func.clone(),
                Func::External(_) => return unsupported("extension aggregates"),
                _ => {
                    return Err(LimboError::InternalError(
                        "aggregate call resolves to a non-aggregate function".to_string(),
                    ))
                }
            };
            let (args, distinct) = match &call.arguments {
                hir::FunctionArguments::Star => (Vec::new(), false),
                hir::FunctionArguments::Expressions { order_by, .. } if !order_by.is_empty() => {
                    return unsupported("ORDER BY in aggregates")
                }
                hir::FunctionArguments::Expressions {
                    values,
                    distinctness,
                    ..
                } => (
                    values
                        .iter()
                        .map(|value| expand_outputs(document, value))
                        .collect(),
                    matches!(distinctness, Some(ast::Distinctness::Distinct)),
                ),
                hir::FunctionArguments::OrderedSet { .. } => {
                    return unsupported("ordered-set aggregates")
                }
            };
            calls.push((
                *id,
                Aggregate {
                    func,
                    args,
                    distinct,
                    filter: filter
                        .as_deref()
                        .map(|filter| expand_outputs(document, filter)),
                    published: Some(expand_outputs(document, node)),
                },
            ));
            Ok(false)
        })
    };
    for output in &block.outputs {
        collect(&output.expr)?;
    }
    if let hir::QueryBlockBody::Select {
        grouping: Some(grouping),
        ..
    } = &block.body
    {
        if let Some(having) = &grouping.having {
            collect(having)?;
        }
    }
    calls.sort_by_key(|(id, _)| id.index);
    Ok(calls.into_iter().map(|(_, aggregate)| aggregate).collect())
}

fn hidden_count_aggregate() -> Aggregate {
    Aggregate {
        func: AggFunc::Count0,
        args: Vec::new(),
        distinct: false,
        filter: None,
        published: None,
    }
}

/// Build the DAG of one FROM clause: its sources joined in order, with the
/// WHERE terms of `filter` placed at the lowest join they may run below.
fn build_from_dag(
    builder: &mut dag::DagBuilder,
    document: &HirDocument,
    from: &hir::From,
    filter: Option<&hir::Expr>,
) -> Result<dag::NodeId> {
    // The first source has no join; it behaves as the left side of the rest.
    let mut sources = vec![from.first];
    let mut join_types = vec![StepJoin::Inner];
    let mut on_by_step: Vec<Vec<hir::Expr>> = vec![Vec::new()];
    for join in &from.joins {
        sources.push(join.right);
        join_types.push(match join.kind {
            hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross => StepJoin::Inner,
            hir::JoinKind::Left => StepJoin::Left,
            hir::JoinKind::Right => StepJoin::Right,
            hir::JoinKind::Full => StepJoin::Full,
            hir::JoinKind::Semi => StepJoin::Semi,
            hir::JoinKind::Anti => StepJoin::Anti,
        });
        on_by_step.push(join_predicates(document, &join.constraint));
    }
    let mut inputs = Vec::with_capacity(sources.len());
    for source_id in &sources {
        inputs.push(build_source_dag(builder, document, *source_id)?);
    }
    let position_of = |source: SourceId| sources.iter().position(|known| *known == source);

    // The sources whose columns the join at `step` fills with NULLs.
    let nulls_filled_by = |step: usize| match join_types[step] {
        StepJoin::Left => step..step + 1,
        StepJoin::Right => 0..step,
        StepJoin::Full => 0..step + 1,
        StepJoin::Inner | StepJoin::Semi | StepJoin::Anti => 0..0,
    };
    // ON terms belong to their join. A WHERE term may run below a join only
    // if no join from there on fills one of its sources with NULLs: in
    // `a RIGHT JOIN b ... WHERE a.x > b.y`, running the term on `a` first
    // would let `b` rows find no match and be padded instead of being
    // removed. Otherwise it runs after the last such join. On an inner
    // join, running a term after it and adding it to ON are the same.
    //
    // The source of a semi or anti join is invisible after its join, so
    // every term that reads it is part of that join's ON.
    let mut filters_by_step: Vec<Vec<hir::Expr>> = vec![Vec::new(); inputs.len()];
    let mut where_terms = Vec::new();
    if let Some(filter) = filter {
        conjuncts(&expand_outputs(document, filter), &mut where_terms);
    }
    for term in where_terms {
        let positions = referenced_positions(document, &term, &position_of)?;
        let mut step = positions.iter().copied().max().unwrap_or(0);
        let reads_hidden_source =
            |position: usize| matches!(join_types[position], StepJoin::Semi | StepJoin::Anti);
        if positions
            .iter()
            .any(|position| reads_hidden_source(*position))
        {
            // Its source must be the last one the term reads, or a later
            // join would need a column that is already gone.
            if positions
                .iter()
                .any(|position| *position != step && reads_hidden_source(*position))
                || !reads_hidden_source(step)
            {
                return Err(LimboError::InternalError(
                    "a predicate reads a semi or anti join source outside that join".to_string(),
                ));
            }
            on_by_step[step].push(term);
            continue;
        }
        for later in step + 1..inputs.len() {
            let nulls = nulls_filled_by(later);
            if positions.iter().any(|position| nulls.contains(position)) {
                step = later;
            }
        }
        if step > 0 && join_types[step] == StepJoin::Inner {
            on_by_step[step].push(term);
        } else {
            filters_by_step[step].push(term);
        }
    }

    let mut node = inputs[0];
    for step in 0..inputs.len() {
        if step > 0 {
            let pair = [node, inputs[step]];
            let on = std::mem::take(&mut on_by_step[step]);
            node = match join_types[step] {
                StepJoin::Inner => builder.push(dag::OpNode::Join {
                    inputs: pair,
                    on,
                    kind: dag::JoinKind::Inner,
                })?,
                StepJoin::Left => push_outer_join(builder, pair, on, true, false)?,
                StepJoin::Right => push_outer_join(builder, pair, on, false, true)?,
                StepJoin::Full => push_outer_join(builder, pair, on, true, true)?,
                StepJoin::Semi => builder.push(dag::OpNode::Join {
                    inputs: pair,
                    on,
                    kind: dag::JoinKind::Semi,
                })?,
                StepJoin::Anti => builder.push(dag::OpNode::Join {
                    inputs: pair,
                    on,
                    kind: dag::JoinKind::Anti,
                })?,
            };
        }
        if let Some(predicate) = combine_predicates(std::mem::take(&mut filters_by_step[step])) {
            node = builder.push(dag::OpNode::Filter {
                input: node,
                predicate,
            })?;
        }
    }

    Ok(node)
}

fn build_block_dag(
    builder: &mut dag::DagBuilder,
    document: &HirDocument,
    block_id: QueryBlockId,
) -> Result<dag::NodeId> {
    let block = block(document, block_id)?;
    if !block.windows.is_empty() || block.window_function_count != 0 {
        return unsupported("window functions");
    }
    let hir::QueryBlockBody::Select {
        distinctness,
        filter,
        grouping,
    } = &block.body
    else {
        return unsupported("VALUES");
    };
    let Some(from) = &block.from else {
        return unsupported("SELECT without a FROM clause");
    };

    let mut node = build_from_dag(builder, document, from, filter.as_ref())?;

    let aggregates = block_aggregates(document, block)?;
    if grouping.is_some() || !aggregates.is_empty() {
        let (group_exprs, group_keys, scalar) = match grouping {
            Some(grouping) if !grouping.keys.is_empty() => {
                let mut exprs = Vec::with_capacity(grouping.keys.len());
                let mut keys = Vec::with_capacity(grouping.keys.len());
                for (key, collation) in grouping.keys.iter().zip(&grouping.key_collations) {
                    let expr = expand_outputs(document, key);
                    keys.push(key_column(
                        document,
                        &expr,
                        collation.as_ref().map(|collation| *collation.value()),
                    )?);
                    exprs.push(expr);
                }
                (exprs, keys, false)
            }
            _ => (
                // The one group of a scalar aggregate is the constant 0.
                vec![hir::Expr::Literal(ast::Literal::Numeric("0".to_string()))],
                vec![dag::KeyColumn {
                    collation: CollationSeq::Binary,
                    ambiguous: false,
                }],
                true,
            ),
        };

        let mut all_aggregates = vec![hidden_count_aggregate()];
        all_aggregates.extend(aggregates);
        let mut multiset_keys = Vec::with_capacity(all_aggregates.len());
        for aggregate in &all_aggregates {
            if !uses_multiset(aggregate) {
                multiset_keys.push(None);
                continue;
            }
            let argument = aggregate.args.first().ok_or_else(|| {
                LimboError::InternalError(
                    "a MIN/MAX or DISTINCT aggregate has no argument".to_string(),
                )
            })?;
            let collation = aggregate_argument_collation(aggregate);
            multiset_keys.push(Some(key_column(document, argument, collation)?));
        }
        node = builder.push(dag::OpNode::Aggregate {
            input: node,
            group_exprs,
            group_keys,
            aggregates: all_aggregates,
            multiset_keys,
            scalar,
        })?;

        if let Some(having) = grouping
            .as_ref()
            .and_then(|grouping| grouping.having.as_ref())
        {
            let mut terms = Vec::new();
            conjuncts(&expand_outputs(document, having), &mut terms);
            if let Some(predicate) = combine_predicates(terms) {
                node = builder.push(dag::OpNode::Filter {
                    input: node,
                    predicate,
                })?;
            }
        }
    }

    let result_exprs = block
        .outputs
        .iter()
        .map(|output| expand_outputs(document, &output.expr))
        .collect::<Vec<_>>();
    node = builder.push(dag::OpNode::Project {
        input: node,
        projections: result_exprs.clone(),
    })?;

    if distinctness.is_some() {
        let group_keys = result_exprs
            .iter()
            .zip(&block.outputs)
            .map(|(expr, output)| {
                key_column(
                    document,
                    expr,
                    output
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value()),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        node = builder.push(dag::OpNode::Aggregate {
            input: node,
            group_exprs: result_exprs.clone(),
            group_keys,
            aggregates: vec![hidden_count_aggregate()],
            multiset_keys: vec![None],
            scalar: false,
        })?;
        node = builder.push(dag::OpNode::Project {
            input: node,
            projections: result_exprs,
        })?;
    }

    Ok(node)
}

/// The collation of a MIN/MAX or DISTINCT aggregate's argument, as the
/// analyzer resolved it.
fn aggregate_argument_collation(aggregate: &Aggregate) -> Option<CollationSeq> {
    let hir::Expr::Function(call) = aggregate.published.as_ref()? else {
        return None;
    };
    let hir::FunctionArguments::Expressions { facts, .. } = &call.arguments else {
        return None;
    };
    facts
        .first()?
        .collation
        .as_ref()
        .map(|collation| *collation.value())
}

/// Build an outer join from simpler operators: the inner join, plus the
/// NULL-padded anti join of each side whose unmatched rows are kept.
///
/// ```text
/// A LEFT JOIN B  = (A JOIN B) UNION ALL pad(A ANTI JOIN B)
/// A RIGHT JOIN B = (A JOIN B) UNION ALL pad(B ANTI JOIN A)
/// A FULL JOIN B  = both padded branches
/// ```
fn push_outer_join(
    builder: &mut dag::DagBuilder,
    inputs: [dag::NodeId; 2],
    on: Vec<hir::Expr>,
    keep_left_rows: bool,
    keep_right_rows: bool,
) -> Result<dag::NodeId> {
    let no_columns = Arc::new(dag::StreamSchema {
        columns: Vec::new(),
        bindings: Vec::new(),
    });
    let mut branches = vec![builder.push(dag::OpNode::Join {
        inputs,
        on: on.clone(),
        kind: dag::JoinKind::Inner,
    })?];
    for (keep, side) in [(keep_left_rows, 0), (keep_right_rows, 1)] {
        if !keep {
            continue;
        }
        let other = inputs[1 - side];
        let unmatched = builder.push(dag::OpNode::Join {
            inputs: [inputs[side], other],
            on: on.clone(),
            kind: dag::JoinKind::Anti,
        })?;
        // The NULL columns take the other side's place in the joined row.
        let other_columns = builder.output_schema(other).clone();
        let (before, after) = if side == 0 {
            (no_columns.clone(), other_columns)
        } else {
            (other_columns, no_columns.clone())
        };
        branches.push(builder.push(dag::OpNode::Pad {
            input: unmatched,
            before,
            after,
        })?);
    }
    let arity = builder.output_schema(branches[0]).len();
    builder.push(dag::OpNode::SetOp {
        operators: vec![ast::CompoundOperator::UnionAll; branches.len() - 1],
        inputs: branches,
        arity,
        prefix_len: 0,
        key_columns: Vec::new(),
    })
}

fn combine_predicates(mut predicates: Vec<hir::Expr>) -> Option<hir::Expr> {
    let first = predicates.pop()?;
    Some(
        predicates
            .into_iter()
            .fold(first, |right, left| hir::Expr::Binary {
                lhs: Box::new(left),
                operator: ast::Operator::And,
                rhs: Box::new(right),
                array_concat: false,
                custom: None,
                comparison: None,
            }),
    )
}

/// The FROM positions of the sources an expression reads.
fn referenced_positions(
    document: &HirDocument,
    expr: &hir::Expr,
    position_of: &impl Fn(SourceId) -> Option<usize>,
) -> Result<Vec<usize>> {
    let mut positions = Vec::new();
    visit_expression(document, expr, &mut |node| {
        let source = match node {
            hir::Expr::Column(column) => Some(column.source),
            hir::Expr::RowId(source) => Some(*source),
            _ => None,
        };
        if let Some(source) = source {
            let position = position_of(source).ok_or_else(|| {
                LimboError::InternalError(
                    "maintenance predicate references a source outside its FROM clause".to_string(),
                )
            })?;
            positions.push(position);
        }
        Ok(true)
    })?;
    Ok(positions)
}

/// The key column a stateful operator uses for `expr`, which compares under
/// `collation`.
fn key_column(
    document: &HirDocument,
    expr: &hir::Expr,
    collation: Option<CollationSeq>,
) -> Result<dag::KeyColumn> {
    let collation = match collation.unwrap_or(CollationSeq::Binary) {
        CollationSeq::Unset => CollationSeq::Binary,
        collation if collation.is_custom() => {
            return unsupported("custom collations on grouping or dedup keys")
        }
        collation => collation,
    };
    Ok(dag::KeyColumn {
        collation,
        ambiguous: collation != CollationSeq::Binary || stored_affinity(document, expr).is_none(),
    })
}

/// The key column of compound result column `index` over the deduplicated
/// `branches`. The collation is the first explicit one, as in SQLite. Under
/// BINARY a class holds one exact value only when no branch can produce an
/// integer that another branch produces as an equal real.
fn set_op_key_column(
    document: &HirDocument,
    branches: &[&hir::QueryBlock],
    index: usize,
) -> Result<dag::KeyColumn> {
    let mut collation = None;
    let mut affinities = Vec::with_capacity(branches.len());
    for branch in branches {
        let output = branch.outputs.get(index).ok_or_else(|| {
            LimboError::InternalError("compound arm has too few result columns".to_string())
        })?;
        if collation.is_none() {
            collation = output
                .collation
                .as_ref()
                .map(|collation| *collation.value());
        }
        affinities.push(stored_affinity(document, &output.expr));
    }
    let collation = match collation.unwrap_or(CollationSeq::Binary) {
        CollationSeq::Unset => CollationSeq::Binary,
        collation if collation.is_custom() => {
            return unsupported("custom collations on grouping or dedup keys")
        }
        collation => collation,
    };
    let mixes_integers_and_reals =
        affinities.contains(&Some(Affinity::Integer)) && affinities.contains(&Some(Affinity::Real));
    Ok(dag::KeyColumn {
        collation,
        ambiguous: collation != CollationSeq::Binary
            || affinities.contains(&None)
            || mixes_integers_and_reals,
    })
}

/// The affinity of the base-table column `expr` reads, when that affinity
/// keeps BINARY-equal stored values identical: INTEGER and NUMERIC store an
/// integral number as an integer (reported as INTEGER), REAL stores every
/// number as a real, and TEXT stores numbers as text. `None` for anything
/// else (an expression, a derived-table column, or a column without such an
/// affinity), which may hold both the integer 2 and the real 2.0.
fn stored_affinity(document: &HirDocument, expr: &hir::Expr) -> Option<Affinity> {
    match expr {
        hir::Expr::RowId(_) => Some(Affinity::Integer),
        hir::Expr::Output(output) => stored_affinity(document, &document.output(*output)?.expr),
        hir::Expr::Column(column) => {
            let source = document.source(column.source)?;
            let hir::SourceKind::Table(table) = &source.kind else {
                return None;
            };
            let crate::schema::Table::BTree(table) = table.value() else {
                return None;
            };
            match table.columns().get(column.column)?.affinity() {
                Affinity::Integer | Affinity::Numeric => Some(Affinity::Integer),
                Affinity::Real => Some(Affinity::Real),
                Affinity::Text => Some(Affinity::Text),
                Affinity::Blob | Affinity::None => None,
            }
        }
        _ => None,
    }
}

/// A hidden state/multiset table a view needs, fully named with its CREATE
/// SQL. The single source of truth for the DDL the CREATE program emits.
#[derive(Debug, Clone)]
pub(crate) struct HiddenTableDef {
    pub(crate) table_name: String,
    pub(crate) create_sql: String,
    pub(crate) primary_key_index: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuxiliaryStatePurpose {
    AggregateMultiset { aggregate_index: usize },
    AggregateGroupMembers,
    MatchCounts,
    SetOpIdentity,
    SetOpMembers,
}

#[derive(Debug, Clone)]
struct AuxiliaryTableDef {
    purpose: AuxiliaryStatePurpose,
    table: HiddenTableDef,
}

/// Persistent storage and output contract assigned to one DAG node.
///
/// `state_table` is the operator's primary integral. Auxiliary tables have an
/// explicit operator-local purpose: each aggregate value multiset owns its
/// own collation-bearing index, while semi/anti joins and mixed set operations each
/// own one bookkeeping table. `arrangement_table` is the node's explicitly
/// materialized output integral when a downstream join or terminal sink needs
/// a one-rowid identity and the operator has no native arrangement.
#[derive(Debug, Clone)]
pub(super) struct OperatorStateDef {
    pub(super) node_id: dag::NodeId,
    pub(super) output: NodeOutputContract,
    pub(super) state_table: Option<HiddenTableDef>,
    auxiliary_tables: Vec<AuxiliaryTableDef>,
    pub(super) arrangement_table: Option<HiddenTableDef>,
}

impl OperatorStateDef {
    fn exposes_arrangement(&self, node: &dag::OpNode) -> bool {
        node_has_native_arrangement(node) || self.arrangement_table.is_some()
    }

    pub(super) fn state_table_name(&self) -> Result<&str> {
        self.state_table
            .as_ref()
            .map(|table| table.table_name.as_str())
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "maintenance DAG node {} has no primary state table",
                    self.node_id
                ))
            })
    }

    fn auxiliary_table_name(&self, purpose: AuxiliaryStatePurpose) -> Result<&str> {
        self.auxiliary_tables
            .iter()
            .find(|table| table.purpose == purpose)
            .map(|table| table.table.table_name.as_str())
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "maintenance DAG node {} has no {purpose:?} table",
                    self.node_id
                ))
            })
    }

    pub(super) fn aggregate_multiset_table_name(&self, aggregate_index: usize) -> Result<&str> {
        self.auxiliary_table_name(AuxiliaryStatePurpose::AggregateMultiset { aggregate_index })
    }

    pub(super) fn aggregate_group_members_table_name(&self) -> Result<&str> {
        self.auxiliary_table_name(AuxiliaryStatePurpose::AggregateGroupMembers)
    }

    pub(super) fn match_counts_table_name(&self) -> Result<&str> {
        self.auxiliary_table_name(AuxiliaryStatePurpose::MatchCounts)
    }

    pub(super) fn set_op_identity_table_name(&self) -> Result<&str> {
        self.auxiliary_table_name(AuxiliaryStatePurpose::SetOpIdentity)
    }

    pub(super) fn set_op_members_table_name(&self) -> Result<&str> {
        self.auxiliary_table_name(AuxiliaryStatePurpose::SetOpMembers)
    }

    fn hidden_tables(&self) -> impl Iterator<Item = &HiddenTableDef> {
        self.state_table
            .iter()
            .chain(self.auxiliary_tables.iter().map(|table| &table.table))
            .chain(self.arrangement_table.iter())
    }
}

/// Node-indexed inventory shared by CREATE and maintenance code generation.
///
/// The vector has exactly one entry per DAG node, so codegen cannot recover
/// storage by reclassifying a branch or reconstructing a name.
#[derive(Debug, Clone)]
pub(super) struct OperatorStateCatalog {
    nodes: Vec<OperatorStateDef>,
}

impl OperatorStateCatalog {
    pub(super) fn for_node(&self, node_id: dag::NodeId) -> Result<&OperatorStateDef> {
        let state = self.nodes.get(node_id).ok_or_else(|| {
            LimboError::InternalError(format!(
                "maintenance state catalog has no DAG node {node_id}"
            ))
        })?;
        if state.node_id != node_id {
            return Err(LimboError::InternalError(format!(
                "maintenance state catalog entry {} is stored at node {node_id}",
                state.node_id
            )));
        }
        Ok(state)
    }

    fn hidden_tables(&self) -> impl Iterator<Item = &HiddenTableDef> {
        self.nodes.iter().flat_map(OperatorStateDef::hidden_tables)
    }
}

/// The single CREATE/compile-time description of an incremental view.
///
/// Validation builds the DAG once, storage is derived from that DAG, and
/// codegen consumes the same representation. Keeping these together prevents
/// the accepted-query set, hidden schema, and bytecode dispatcher from
/// evolving as three independent shape classifiers.
pub(crate) struct MaintenancePlan {
    /// The analyzed query every DAG expression refers to.
    document: Arc<HirDocument>,
    dag: dag::MaintenanceDag,
    operator_states: OperatorStateCatalog,
    output_arity: usize,
    version_marker: HiddenTableDef,
}

impl MaintenancePlan {
    pub(super) fn document(&self) -> &HirDocument {
        &self.document
    }

    pub(super) fn dag(&self) -> &dag::MaintenanceDag {
        &self.dag
    }

    pub(super) fn operator_states(&self) -> &OperatorStateCatalog {
        &self.operator_states
    }

    pub(crate) fn output_arity(&self) -> usize {
        self.output_arity
    }

    pub(crate) fn hidden_tables(&self) -> impl Iterator<Item = &HiddenTableDef> {
        std::iter::once(&self.version_marker).chain(self.operator_states.hidden_tables())
    }

    /// For each view column: the collation under which the value it shows
    /// was chosen among several values that compare equal, or `None` when
    /// the value is exact. These are the values SQL leaves open, such as
    /// which of 'a' and 'A' a NOCASE GROUP BY key or MIN shows (see
    /// `members`). A column is chosen when it is an ambiguous key column of
    /// a GROUP BY, DISTINCT or deduplicating compound operator, or the
    /// result of MIN/MAX over an ambiguous value, reached through operators
    /// that pass the value on unchanged. A value computed from a chosen
    /// value is not reported.
    #[cfg(feature = "test_helper")]
    pub(crate) fn chosen_value_collations(&self) -> Vec<Option<CollationSeq>> {
        let dag = &self.dag;
        let mut chosen: Vec<Vec<Option<CollationSeq>>> = Vec::with_capacity(dag.nodes.len());
        // The collation of input column `expr` when it passes on a chosen
        // value unchanged.
        let passed_on = |chosen: &[Vec<Option<CollationSeq>>], input: dag::NodeId, expr| {
            dag.output_schema(input)
                .columns
                .iter()
                .position(|column| {
                    column
                        .as_ref()
                        .is_some_and(|column| column.equivalent(expr))
                })
                .and_then(|position| chosen[input][position])
        };
        for (node_id, node) in dag.nodes.iter().enumerate() {
            let width = dag.output_schema(node_id).len();
            let columns = match node {
                dag::OpNode::Scan { .. } => vec![None; width],
                dag::OpNode::Filter { input, .. } | dag::OpNode::Alias { input, .. } => {
                    chosen[*input].clone()
                }
                dag::OpNode::Project { input, projections } => projections
                    .iter()
                    .map(|projection| passed_on(&chosen, *input, projection))
                    .collect(),
                dag::OpNode::Join {
                    inputs,
                    kind: dag::JoinKind::Inner,
                    ..
                } => [&chosen[inputs[0]], &chosen[inputs[1]]]
                    .into_iter()
                    .flatten()
                    .copied()
                    .collect(),
                dag::OpNode::Join { inputs, .. } => chosen[inputs[0]].clone(),
                dag::OpNode::Pad {
                    input,
                    before,
                    after,
                } => std::iter::repeat_n(None, before.len())
                    .chain(chosen[*input].iter().copied())
                    .chain(std::iter::repeat_n(None, after.len()))
                    .collect(),
                dag::OpNode::Aggregate {
                    input,
                    group_exprs,
                    group_keys,
                    aggregates,
                    multiset_keys,
                    ..
                } => {
                    // A key or MIN/MAX value is chosen here when its class can
                    // hold several exact values, and otherwise stays chosen
                    // when its input column was.
                    let chosen_value = |expr, key: dag::KeyColumn| {
                        if key.ambiguous {
                            Some(key.collation)
                        } else {
                            passed_on(&chosen, *input, expr)
                        }
                    };
                    group_exprs
                        .iter()
                        .zip(group_keys)
                        .map(|(expr, key)| chosen_value(expr, *key))
                        .chain(
                            aggregates
                                .iter()
                                .zip(multiset_keys)
                                .map(|(aggregate, key)| {
                                    match (&aggregate.func, key, aggregate.args.first()) {
                                        (
                                            AggFunc::Min | AggFunc::Max,
                                            Some(key),
                                            Some(argument),
                                        ) => chosen_value(argument, *key),
                                        _ => None,
                                    }
                                }),
                        )
                        .collect()
                }
                dag::OpNode::SetOp {
                    inputs,
                    key_columns,
                    ..
                } => (0..width)
                    .map(|index| {
                        key_columns
                            .get(index)
                            .filter(|key| key.ambiguous)
                            .map(|key| key.collation)
                            .or_else(|| inputs.iter().find_map(|input| chosen[*input][index]))
                    })
                    .collect(),
            };
            chosen.push(columns);
        }
        chosen.swap_remove(dag.root)
    }
}

/// Validate a view and build its executable maintenance plan.
///
/// The view's unqualified table names resolve within the main database,
/// where every materialized view lives, never through a TEMP table that
/// happens to share a source table's name. Every correlated subquery the
/// planner can turn into a join becomes one (EXISTS and IN into semi joins,
/// NOT EXISTS into anti joins), whatever its cost: maintenance cannot run a
/// correlated subquery, and the view's plan is rebuilt whenever the schema
/// changes, so the choice must not depend on cost.
pub(crate) fn plan_view(
    view_name: &str,
    select: &ast::Select,
    catalog: &SemanticContext<'_>,
) -> Result<MaintenancePlan> {
    let context = catalog.scoped_to_database(hir::DatabaseId::new(crate::MAIN_DB_ID));
    let statement = ast::Stmt::Select(select.clone());
    let document = analyze_root(&context, SemanticRootInput::Statement(&statement))?;
    let (document, _) = crate::translate::semantic_unnest::unnest_correlated_subqueries(
        Arc::new(document),
        context.main_schema(),
        crate::translate::optimizer::cost_model_params(),
        crate::SubqueryUnnestingMode::Forced,
    )?;
    let dag = build_dag(&document)?;
    let operator_states = plan_operator_states(view_name, &document, &dag)?;
    let output_arity = dag.root_schema().len();
    let version_marker = version_marker_table(view_name);
    Ok(MaintenancePlan {
        document,
        dag,
        operator_states,
        output_arity,
        version_marker,
    })
}

fn version_marker_table(view_name: &str) -> HiddenTableDef {
    let table_name = crate::incremental::view::dbsp_version_marker_table_name(view_name);
    let table_ident = crate::util::quote_identifier(&table_name);
    HiddenTableDef {
        create_sql: format!("CREATE TABLE {table_ident} (version INTEGER)"),
        table_name,
        primary_key_index: false,
    }
}

/// Validate one fully planned node.
///
/// Planning visits nodes in topological order and calls this immediately
/// after assigning the node's identity, state, and arrangement contract.
/// Validation therefore cannot accept a shape that has no physical contract,
/// and it may inspect only contracts already established for declared inputs.
fn validate_planned_node(
    document: &HirDocument,
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    operator_states: &OperatorStateCatalog,
) -> Result<()> {
    let node = &dag.nodes[node_id];
    let check_expr =
        |expr: &hir::Expr, input: dag::NodeId, unavailable_value: &str| -> Result<()> {
            if changes_between_statements(document, expr)? {
                return unsupported("non-deterministic expressions");
            }
            let input = &operator_states.for_node(input)?.output;
            validate_stream_expr(
                document,
                expr,
                &input.schema,
                &input.binding_rowids,
                unavailable_value,
            )
        };
    match node {
        dag::OpNode::Scan { table, .. } => {
            if !table.has_rowid {
                return unsupported("WITHOUT ROWID base tables");
            }
            if table.has_virtual_columns {
                return unsupported("base tables with virtual generated columns");
            }
        }
        dag::OpNode::Filter { input, predicate } => {
            // HAVING is the only filter placed directly on an aggregate.
            let unavailable_value = if matches!(dag.nodes[*input], dag::OpNode::Aggregate { .. }) {
                "HAVING referencing columns outside GROUP BY"
            } else {
                "filters reading columns that their input does not provide"
            };
            check_expr(predicate, *input, unavailable_value)?
        }
        dag::OpNode::Project { input, projections } => {
            for expr in projections {
                check_expr(expr, *input, "non-aggregate result columns not in GROUP BY")?;
            }
        }
        dag::OpNode::Alias { .. } | dag::OpNode::Pad { .. } => {}
        dag::OpNode::Join { inputs, on, kind } => {
            for &input in inputs {
                if !operator_states
                    .for_node(input)?
                    .exposes_arrangement(&dag.nodes[input])
                {
                    return Err(LimboError::InternalError(format!(
                        "join input DAG node {input} has no declared arrangement"
                    )));
                }
            }
            let input_contracts = [
                &operator_states.for_node(inputs[0])?.output,
                &operator_states.for_node(inputs[1])?.output,
            ];
            if *kind != dag::JoinKind::Inner
                && !matches!(
                    input_contracts[0].published_identity,
                    DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid
                )
            {
                return Err(LimboError::InternalError(format!(
                    "semi or anti join input DAG node {} is not identified by one rowid",
                    inputs[0]
                )));
            }
            // ON reads both inputs, also when the join outputs only the first.
            let joined_schema = dag::StreamSchema {
                columns: input_contracts
                    .iter()
                    .flat_map(|input| input.schema.columns.iter().cloned())
                    .collect(),
                bindings: input_contracts
                    .iter()
                    .flat_map(|input| input.schema.bindings.iter().cloned())
                    .collect(),
            };
            let joined_rowids = input_contracts
                .iter()
                .flat_map(|input| input.binding_rowids.iter().copied())
                .collect::<Vec<_>>();
            for expr in on {
                if changes_between_statements(document, expr)? {
                    return unsupported("non-deterministic expressions");
                }
                validate_stream_expr(
                    document,
                    expr,
                    &joined_schema,
                    &joined_rowids,
                    "join predicates referencing values outside their input stream",
                )?;
            }
        }
        dag::OpNode::SetOp {
            inputs,
            operators,
            arity,
            prefix_len,
            key_columns,
        } => {
            if inputs.len() != operators.len() + 1
                || *prefix_len > inputs.len()
                || *prefix_len == 1
                || inputs
                    .iter()
                    .any(|input| dag.output_schema(*input).len() != *arity)
            {
                return Err(LimboError::InternalError(
                    "composable set-op has inconsistent physical metadata".to_string(),
                ));
            }
            if (*prefix_len == 0 && !key_columns.is_empty())
                || (*prefix_len > 0 && key_columns.len() != *arity)
            {
                return Err(LimboError::InternalError(
                    "composable set-op has an invalid collation layout".to_string(),
                ));
            }
            if key_columns.iter().any(|key| key.collation.is_custom()) {
                return unsupported("custom collations on grouping or dedup keys");
            }
            if *prefix_len == 0
                && operators
                    .iter()
                    .any(|operator| *operator != ast::CompoundOperator::UnionAll)
            {
                return Err(LimboError::InternalError(
                    "pure UNION ALL DAG has a non-UNION ALL operator".to_string(),
                ));
            }
        }
        dag::OpNode::Aggregate {
            input,
            group_exprs,
            group_keys,
            aggregates,
            multiset_keys,
            ..
        } => {
            if group_exprs.len() != group_keys.len()
                || aggregates.len() != multiset_keys.len()
                || aggregates
                    .iter()
                    .zip(multiset_keys)
                    .any(|(aggregate, key)| uses_multiset(aggregate) != key.is_some())
            {
                return Err(LimboError::InternalError(
                    "aggregate DAG has inconsistent collation metadata".to_string(),
                ));
            }
            if group_keys
                .iter()
                .chain(multiset_keys.iter().flatten())
                .any(|key| key.collation.is_custom())
            {
                return unsupported("custom collations on aggregate state keys");
            }
            for expr in group_exprs {
                check_expr(
                    expr,
                    *input,
                    "grouping expressions referencing values outside their input stream",
                )?;
            }
            for aggregate in aggregates {
                validate_supported_aggregate(aggregate)?;
                for expr in &aggregate.args {
                    check_expr(
                        expr,
                        *input,
                        "aggregate arguments referencing values outside their input stream",
                    )?;
                }
                if let Some(expr) = &aggregate.filter {
                    check_expr(
                        expr,
                        *input,
                        "aggregate filters referencing values outside their input stream",
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Whether `expr` can give a different value in a later statement than it gave
/// when a row was added to the view: a non-deterministic function, or
/// `CURRENT_DATE`, `CURRENT_TIME` or `CURRENT_TIMESTAMP`. Those literals are
/// fixed within one statement, but a view keeps its rows across statements.
fn changes_between_statements(document: &HirDocument, expr: &hir::Expr) -> Result<bool> {
    let mut found = false;
    visit_expression(document, expr, &mut |node| {
        match node {
            hir::Expr::Function(call) => {
                let function = call.function.value();
                let aggregate_or_window = matches!(function, Func::Agg(_) | Func::Window(_))
                    || matches!(
                        function,
                        Func::External(external)
                            if matches!(external.func, crate::function::ExtFunc::Aggregate { .. })
                    );
                found |= !aggregate_or_window && !function.is_deterministic();
            }
            hir::Expr::Literal(
                ast::Literal::CurrentDate
                | ast::Literal::CurrentTime
                | ast::Literal::CurrentTimestamp,
            ) => found = true,
            _ => {}
        }
        Ok(true)
    })?;
    Ok(found)
}

fn validate_terminal_delta_identity(
    operator_states: &OperatorStateCatalog,
    node_id: dag::NodeId,
) -> Result<()> {
    match operator_states.for_node(node_id)?.output.published_identity {
        DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid => Ok(()),
        DeltaIdentity::BindingRowids(_) | DeltaIdentity::OperatorKey(_) => unsupported(
            "a root delta with composite identity without an explicit materialized sink",
        ),
    }
}

fn plan_node_output(
    schema: Arc<dag::StreamSchema>,
    node: &dag::OpNode,
    prior_states: &[OperatorStateDef],
) -> Result<NodeOutputContract> {
    let prior_output = |input: dag::NodeId| {
        prior_states
            .get(input)
            .map(|state| &state.output)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "maintenance DAG input node {input} has no output contract"
                ))
            })
    };
    let (emitted_identity, binding_rowids): (DeltaIdentity, Arc<[bool]>) = match node {
        dag::OpNode::Scan { table, .. } => (
            DeltaIdentity::BindingRowids(1),
            vec![table.has_rowid].into(),
        ),
        dag::OpNode::Filter { input, .. } | dag::OpNode::Project { input, .. } => {
            let upstream = prior_output(*input)?;
            (upstream.published_identity, upstream.binding_rowids.clone())
        }
        // A derived-table alias introduces one new SQL namespace. The source
        // transport key still distinguishes rows, but it is not the alias's
        // SQL rowid.
        dag::OpNode::Alias { input, .. } => {
            let upstream = prior_output(*input)?;
            let identity = match upstream.published_identity {
                DeltaIdentity::BindingRowids(width) => DeltaIdentity::OperatorKey(width),
                identity => identity,
            };
            (identity, vec![false].into())
        }
        dag::OpNode::Join {
            inputs,
            kind: dag::JoinKind::Inner,
            ..
        } => {
            let inputs = [prior_output(inputs[0])?, prior_output(inputs[1])?];
            let identity = if inputs
                .iter()
                .all(|input| matches!(input.published_identity, DeltaIdentity::BindingRowids(_)))
            {
                DeltaIdentity::BindingRowids(
                    inputs
                        .iter()
                        .map(|input| input.published_identity.width())
                        .sum(),
                )
            } else {
                // Opaque pair for non-binding inputs.
                DeltaIdentity::OperatorKey(2)
            };
            let mut provenance = Vec::new();
            for input in &inputs {
                provenance.extend_from_slice(&input.binding_rowids);
            }
            (identity, provenance.into())
        }
        // A semi or anti join outputs rows of its first input unchanged, so
        // they keep that input's identity.
        dag::OpNode::Join {
            inputs,
            kind: dag::JoinKind::Semi | dag::JoinKind::Anti,
            ..
        } => {
            let rows = prior_output(inputs[0])?;
            (rows.published_identity, rows.binding_rowids.clone())
        }
        // The NULL columns publish a NULL rowid for each binding they stand for.
        dag::OpNode::Pad {
            input,
            before,
            after,
        } => {
            let upstream = prior_output(*input)?;
            let identity = match upstream.published_identity {
                DeltaIdentity::BindingRowids(width) => DeltaIdentity::OperatorKey(width),
                identity => identity,
            };
            let rowids = std::iter::repeat_n(true, before.bindings.len())
                .chain(upstream.binding_rowids.iter().copied())
                .chain(std::iter::repeat_n(true, after.bindings.len()))
                .collect::<Vec<_>>();
            (identity, rowids.into())
        }
        // These operators may retain source expressions as output values, but
        // they do not preserve a one-to-one source-row namespace.
        dag::OpNode::Aggregate { input, .. } => (
            DeltaIdentity::OperatorRowid,
            vec![false; prior_output(*input)?.binding_rowids.len()].into(),
        ),
        dag::OpNode::SetOp {
            inputs, prefix_len, ..
        } => {
            let first = inputs.first().ok_or_else(|| {
                LimboError::InternalError("set-op DAG must have at least one input".to_string())
            })?;
            if *prefix_len > 0 {
                (
                    DeltaIdentity::OperatorRowid,
                    vec![false; prior_output(*first)?.binding_rowids.len()].into(),
                )
            } else {
                // Pure UNION ALL namespaces every branch's packed identity. A
                // binding's rowid survives when every branch publishes it for
                // the same binding, as the branches of an outer join do.
                let mut rowids = vec![true; schema.bindings.len()];
                for input in inputs {
                    let branch = prior_output(*input)?;
                    if branch.schema.bindings.len() != schema.bindings.len() {
                        rowids.fill(false);
                        continue;
                    }
                    for (index, available) in rowids.iter_mut().enumerate() {
                        *available &= branch.binding_rowids[index]
                            && branch.schema.bindings[index].source
                                == schema.bindings[index].source;
                    }
                }
                (DeltaIdentity::OperatorKey(2), rowids.into())
            }
        }
    };
    if binding_rowids.len() != schema.bindings.len() {
        return Err(LimboError::InternalError(
            "maintenance output rowid provenance does not match its bindings".to_string(),
        ));
    }
    Ok(NodeOutputContract {
        schema,
        binding_rowids,
        emitted_identity,
        published_identity: emitted_identity,
    })
}

fn node_has_native_arrangement(node: &dag::OpNode) -> bool {
    matches!(
        node,
        dag::OpNode::Scan { .. } | dag::OpNode::Aggregate { .. }
    )
}

fn node_is_join_input(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> bool {
    dag.nodes.iter().any(|node| {
        matches!(
            node,
            dag::OpNode::Join { inputs, .. } if inputs.contains(&node_id)
        )
    })
}

/// Whether a semi or anti join keeps or drops this node's rows. It looks each
/// row up by rowid, so the node must publish a one-rowid identity.
fn node_rows_are_looked_up_by_rowid(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> bool {
    dag.nodes.iter().any(|node| {
        matches!(
            node,
            dag::OpNode::Join {
                inputs,
                kind: dag::JoinKind::Semi | dag::JoinKind::Anti,
                ..
            } if inputs[0] == node_id
        )
    })
}

/// Whether this node must persist its output integral for a downstream join
/// or terminal sink.
///
/// This demand is derived from DAG edges once and shared by validation,
/// hidden-table creation, and codegen. Pure UNION ALL uses a branch id plus
/// packed upstream identity, so even branches with different identity widths
/// expose one fixed arrangement key.
fn node_requires_output_arrangement(
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    emitted_identity: DeltaIdentity,
) -> bool {
    if matches!(dag.nodes[node_id], dag::OpNode::SetOp { prefix_len: 0, .. }) {
        return true;
    }
    // Preserve binding rowids through consumers that may reference them.
    // Composite identities are normalized only when they reach a terminal
    // sink or must back a downstream join arrangement.
    if node_id == dag.root
        && !matches!(
            emitted_identity,
            DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid
        )
    {
        return true;
    }
    if !node_is_join_input(dag, node_id) || node_has_native_arrangement(&dag.nodes[node_id]) {
        return false;
    }
    true
}

/// Build the persistent-state inventory in DAG order.
///
/// Aggregate and deduplicating set operators own their native integral.
/// Joins and pure UNION ALL publish through ordinary output arrangements, so
/// storage selection depends only on node and edge contracts.
fn plan_operator_states(
    view_name: &str,
    document: &HirDocument,
    dag: &dag::MaintenanceDag,
) -> Result<OperatorStateCatalog> {
    use crate::incremental::view::DBSP_CIRCUIT_VERSION;

    let mut catalog = OperatorStateCatalog {
        nodes: Vec::with_capacity(dag.nodes.len()),
    };
    let mut hidden_names = FxHashSet::default();
    for (node_id, node) in dag.nodes.iter().enumerate() {
        let mut output =
            plan_node_output(dag.output_schema(node_id).clone(), node, &catalog.nodes)?;
        let needs_state = node_requires_primary_state(dag, node_id);
        let state_table = if needs_state {
            let table_name = format!(
                "{}{DBSP_CIRCUIT_VERSION}_{view_name}__n{node_id}",
                crate::schema::DBSP_TABLE_PREFIX,
            );
            Some(HiddenTableDef {
                create_sql: state_table_sql(&table_name, node)?,
                table_name,
                primary_key_index: true,
            })
        } else {
            None
        };
        let auxiliary_tables = plan_auxiliary_tables(
            view_name,
            node_id,
            node,
            dag.output_schema(node_id).len(),
            &output.binding_rowids,
        );
        let arrangement_table =
            if node_requires_output_arrangement(dag, node_id, output.emitted_identity) {
                let identity_width = output.emitted_identity.width();
                let table_name = format!(
                    "{}{DBSP_CIRCUIT_VERSION}_{view_name}__n{node_id}",
                    crate::schema::DBSP_ARRANGEMENT_TABLE_PREFIX,
                );
                Some(HiddenTableDef {
                    create_sql: arrangement_table_sql(
                        &table_name,
                        identity_width,
                        dag.output_schema(node_id).len(),
                        binding_rowid_metadata_width(
                            output.emitted_identity,
                            &output.binding_rowids,
                        ),
                    )?,
                    table_name,
                    primary_key_index: true,
                })
            } else {
                None
            };
        if arrangement_table.is_some()
            && (node_id == dag.root || node_rows_are_looked_up_by_rowid(dag, node_id))
        {
            output.published_identity = DeltaIdentity::OperatorRowid;
        }
        let state = OperatorStateDef {
            node_id,
            output,
            state_table,
            auxiliary_tables,
            arrangement_table,
        };
        for table in state.hidden_tables() {
            if !hidden_names.insert(table.table_name.clone()) {
                return Err(LimboError::InternalError(format!(
                    "duplicate maintenance state table {}",
                    table.table_name
                )));
            }
        }
        catalog.nodes.push(state);
        validate_planned_node(document, dag, node_id, &catalog)?;
    }
    validate_terminal_delta_identity(&catalog, dag.root)?;
    Ok(catalog)
}

fn plan_auxiliary_tables(
    view_name: &str,
    node_id: dag::NodeId,
    node: &dag::OpNode,
    output_width: usize,
    binding_rowids: &[bool],
) -> Vec<AuxiliaryTableDef> {
    use crate::incremental::view::DBSP_CIRCUIT_VERSION;

    let table_name = |suffix: &str| {
        format!(
            "{}{DBSP_CIRCUIT_VERSION}_{view_name}__n{node_id}{suffix}",
            crate::schema::DBSP_MULTISET_TABLE_PREFIX,
        )
    };
    let table = |purpose, table_name: String, create_sql| AuxiliaryTableDef {
        purpose,
        table: HiddenTableDef {
            table_name,
            create_sql,
            primary_key_index: true,
        },
    };

    match node {
        dag::OpNode::Aggregate {
            group_keys,
            multiset_keys,
            ..
        } => {
            let mut tables = multiset_keys
                .iter()
                .enumerate()
                .filter_map(|(aggregate_index, value_key)| {
                    value_key.map(|value_key| {
                        let table_name = table_name(&format!("__a{aggregate_index}"));
                        let create_sql =
                            aggregate_multiset_table_sql(&table_name, group_keys, value_key);
                        table(
                            AuxiliaryStatePurpose::AggregateMultiset { aggregate_index },
                            table_name,
                            create_sql,
                        )
                    })
                })
                .collect::<Vec<_>>();
            if group_keys.iter().any(|key| key.ambiguous) {
                let table_name = table_name("__g");
                let create_sql = class_members_table_sql(&table_name, group_keys);
                tables.push(table(
                    AuxiliaryStatePurpose::AggregateGroupMembers,
                    table_name,
                    create_sql,
                ));
            }
            tables
        }
        dag::OpNode::Join {
            kind: dag::JoinKind::Semi | dag::JoinKind::Anti,
            ..
        } => {
            let table_name = table_name("");
            let payload_width = output_width
                + binding_rowids
                    .iter()
                    .filter(|available| **available)
                    .count();
            vec![AuxiliaryTableDef {
                purpose: AuxiliaryStatePurpose::MatchCounts,
                table: HiddenTableDef {
                    create_sql: match_counts_table_sql(&table_name, payload_width),
                    table_name,
                    // Rows are keyed by the input row's rowid, which is also
                    // this table's rowid.
                    primary_key_index: false,
                },
            }]
        }
        dag::OpNode::SetOp {
            operators,
            prefix_len,
            key_columns,
            ..
        } if *prefix_len > 0 => {
            let mut tables = Vec::new();
            if *prefix_len < operators.len() + 1 {
                let table_name = table_name("");
                tables.push(table(
                    AuxiliaryStatePurpose::SetOpIdentity,
                    table_name.clone(),
                    set_op_identity_table_sql(&table_name),
                ));
            }
            if key_columns.iter().any(|key| key.ambiguous) {
                let table_name = table_name("__r");
                tables.push(table(
                    AuxiliaryStatePurpose::SetOpMembers,
                    table_name.clone(),
                    class_members_table_sql(&table_name, key_columns),
                ));
            }
            tables
        }
        _ => Vec::new(),
    }
}

fn arrangement_table_sql(
    table_name: &str,
    identity_width: usize,
    value_width: usize,
    binding_rowid_width: usize,
) -> Result<String> {
    if identity_width == 0 {
        return Err(LimboError::InternalError(
            "an output arrangement requires a non-empty stable identity".to_string(),
        ));
    }
    let table_ident = crate::util::quote_identifier(table_name);
    let mut columns = (0..identity_width)
        .map(|i| format!("i{i}"))
        .collect::<Vec<_>>();
    columns.extend((0..value_width).map(|i| format!("c{i}")));
    // Rows are keyed by their identity and the exact record of their values
    // (see `members::emit_exact_value_record`), so the integer 2 and the
    // real 2.0 are two rows, as in the z-set.
    let mut key = columns[..identity_width].to_vec();
    key.push("exact".to_string());
    columns.extend((0..binding_rowid_width).map(|i| format!("r{i}")));
    columns.push("mult".to_string());
    columns.push("exact".to_string());
    Ok(format!(
        "CREATE TABLE {table_ident} ({}, PRIMARY KEY ({}))",
        columns.join(", "),
        key.join(", ")
    ))
}

fn node_requires_primary_state(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> bool {
    match &dag.nodes[node_id] {
        dag::OpNode::Aggregate { .. } => true,
        dag::OpNode::SetOp { prefix_len, .. } => *prefix_len > 0,
        _ => false,
    }
}

/// The CREATE TABLE statement for a view's internal state table.
///
/// For a GROUP BY view: one row per live group, holding the group-key values
/// and each payload aggregate's persisted state, with the PRIMARY KEY over
/// the group columns giving the automatic group-lookup index. MIN/MAX
/// aggregates contribute no columns here — their state is the value multiset
/// table.
///
/// For a compound view with a dedup prefix: one row per distinct content
/// key, holding one signed count per prefix branch (the visibility fold
/// reads them). A trailing UNION ALL suffix keeps its
/// (branch, source-identity, multiplicity) rows in its identity table; pure
/// UNION ALL is stateless branch composition.
///
/// In each case the state row's rowid is the operator output identity.
///
/// Declared column types are empty so nothing coerces the stored values.
fn state_table_sql(state_table_name: &str, node: &dag::OpNode) -> Result<String> {
    let state_table_ident = crate::util::quote_identifier(state_table_name);
    match node {
        dag::OpNode::Aggregate {
            group_exprs,
            group_keys,
            aggregates,
            ..
        } => {
            let mut columns = Vec::new();
            for (i, key) in group_keys.iter().enumerate() {
                columns.push(format!("g{i}{}", collate_clause(key.collation)));
            }
            for i in 0..aggregates.len() {
                columns.push(format!("v{i}"));
            }
            for (i, agg) in aggregates.iter().enumerate() {
                if let Some(width) = super::aggregate::ivm_payload_width(&agg.func) {
                    for j in 0..width {
                        columns.push(format!("a{i}_{j}"));
                    }
                }
            }
            let key: Vec<String> = (0..group_exprs.len()).map(|i| format!("g{i}")).collect();
            Ok(format!(
                "CREATE TABLE {state_table_ident} ({}, PRIMARY KEY ({}))",
                columns.join(", "),
                key.join(", ")
            ))
        }
        dag::OpNode::SetOp {
            arity,
            prefix_len,
            key_columns,
            ..
        } => {
            if *prefix_len == 0 {
                return Err(LimboError::InternalError(
                    "pure UNION ALL is a stateless branch composition".to_string(),
                ));
            }
            let mut columns: Vec<String> = (0..*arity)
                .map(|i| format!("c{i}{}", collate_clause(key_columns[i].collation)))
                .collect();
            for i in 0..*prefix_len {
                columns.push(format!("cnt{i}"));
            }
            let key: Vec<String> = (0..*arity).map(|i| format!("c{i}")).collect();
            Ok(format!(
                "CREATE TABLE {state_table_ident} ({}, PRIMARY KEY ({}))",
                columns.join(", "),
                key.join(", ")
            ))
        }
        _ => Err(LimboError::InternalError(
            "this DAG operator has no state table".to_string(),
        )),
    }
}

/// Whether an aggregate keeps per-value state in the multiset table: MIN/MAX
/// (to find the next extreme after a retraction) and DISTINCT aggregates (to
/// step the accumulator only on first occurrence of a value).
pub(super) fn uses_multiset(agg: &Aggregate) -> bool {
    matches!(agg.func, AggFunc::Min | AggFunc::Max) || tracks_distinct_values(agg)
}

/// One aggregate's value multiset: one row per (group, value) with its
/// multiplicity. A table per aggregate lets the value-key index carry that
/// argument's own collation. For MIN/MAX the key ordering makes a group's
/// extreme an index seek; for DISTINCT aggregates the rows decide which
/// values the aggregate sees.
///
/// A value whose class can hold several exact values (see
/// [`dag::KeyColumn::ambiguous`]) is keyed by its exact value: the `val`
/// class column is followed by the member columns (`member_column_names`).
fn aggregate_multiset_table_sql(
    multiset_table_name: &str,
    group_keys: &[dag::KeyColumn],
    value_key: dag::KeyColumn,
) -> String {
    let multiset_table_ident = crate::util::quote_identifier(multiset_table_name);
    let mut key = Vec::with_capacity(group_keys.len() + 4);
    let mut columns = Vec::with_capacity(group_keys.len() + 5);
    for (i, group_key) in group_keys.iter().enumerate() {
        columns.push(format!("g{i}{}", collate_clause(group_key.collation)));
        key.push(format!("g{i}"));
    }
    columns.push(format!("val{}", collate_clause(value_key.collation)));
    key.push("val".to_string());
    if value_key.ambiguous {
        let members = member_column_names(1);
        columns.extend(members.iter().cloned());
        key.extend(members);
    }
    columns.push("mult".to_string());
    format!(
        "CREATE TABLE {multiset_table_ident} ({}, PRIMARY KEY ({}))",
        columns.join(", "),
        key.join(", ")
    )
}

/// The exact values in each class of an operator's collated key, with their
/// counts: `(class.., member columns.., mult)`.
///
/// `class` columns compare under the key's collations and hold the member's
/// own values. Index order lists each class's members smallest-first, which
/// is the member the operator shows (see `vdbe_maintenance::members`).
fn class_members_table_sql(table_name: &str, keys: &[dag::KeyColumn]) -> String {
    let table_ident = crate::util::quote_identifier(table_name);
    let mut key = (0..keys.len()).map(|i| format!("c{i}")).collect::<Vec<_>>();
    let mut columns = keys
        .iter()
        .enumerate()
        .map(|(i, column)| format!("c{i}{}", collate_clause(column.collation)))
        .collect::<Vec<_>>();
    let members = member_column_names(keys.len());
    columns.extend(members.iter().cloned());
    key.extend(members);
    columns.push("mult".to_string());
    format!(
        "CREATE TABLE {table_ident} ({}, PRIMARY KEY ({}))",
        columns.join(", "),
        key.join(", ")
    )
}

/// The member columns that follow the class columns of `width` values in a
/// member table: `raw..`, the values compared under BINARY; `type..`, their
/// `typeof()`; and `exact`, the record of the values. See
/// `members::emit_member_columns`.
fn member_column_names(width: usize) -> Vec<String> {
    (0..width)
        .map(|i| format!("raw{i}"))
        .chain((0..width).map(|i| format!("type{i}")))
        .chain(std::iter::once("exact".to_string()))
        .collect()
}

/// A compound view's trailing UNION ALL branches: a multiset of
/// (branch, packed source identity), each row's rowid doubling as its view
/// rowid. Packing keeps the table independent of each branch's identity width.
fn set_op_identity_table_sql(table_name: &str) -> String {
    let table_ident = crate::util::quote_identifier(table_name);
    format!(
        "CREATE TABLE {table_ident} (branch, source_identity, mult, PRIMARY KEY (branch, source_identity))"
    )
}

/// A semi or anti join's state, one row per input row, stored under that row's
/// rowid: the row's multiplicity, its number of ON matches, the weight it
/// currently has in the output, and the output image published with that
/// weight (needed to retract it after the input row is gone).
fn match_counts_table_sql(table_name: &str, payload_width: usize) -> String {
    let table_ident = crate::util::quote_identifier(table_name);
    let payload = (0..payload_width)
        .map(|i| format!(", c{i}"))
        .collect::<String>();
    format!("CREATE TABLE {table_ident} (present, matches, published{payload})")
}

/// The `COLLATE` clause for a hidden-table key column, empty for the
/// default binary comparison. The name round-trips through
/// [`CollationSeq::new`] when the generated DDL is parsed back; custom
/// collations (which do not) are rejected at classify time.
fn collate_clause(collation: CollationSeq) -> String {
    match collation {
        CollationSeq::Binary | CollationSeq::Unset => String::new(),
        other => format!(" COLLATE \"{}\"", other.name()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_table() -> Arc<BTreeTable> {
        Arc::new(BTreeTable::new(
            1,
            "t".to_string(),
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::schema::BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ))
    }

    fn empty_document() -> HirDocument {
        HirDocument {
            snapshot: hir::CatalogSnapshot::from_id(0),
            databases: Vec::new(),
            root: hir::HirRoot::Query(hir::QueryRoot {
                query: QueryId::new(0),
            }),
            queries: Vec::new(),
            sources: Vec::new(),
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        }
    }

    fn plus_one(expr: hir::Expr) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(expr),
            operator: ast::Operator::Add,
            rhs: Box::new(hir::Expr::Literal(ast::Literal::Numeric("1".to_string()))),
            array_concat: false,
            custom: None,
            comparison: None,
        }
    }

    #[test]
    fn expression_validation_depends_only_on_published_values() {
        let document = empty_document();
        let source = SourceId::new(1);
        let published = hir::Expr::column(source, 0);
        let input = NodeOutputContract {
            schema: Arc::new(dag::StreamSchema {
                columns: vec![Some(published.clone())],
                bindings: Vec::new(),
            }),
            binding_rowids: Vec::<bool>::new().into(),
            emitted_identity: DeltaIdentity::OperatorRowid,
            published_identity: DeltaIdentity::OperatorRowid,
        };

        validate_stream_expr(
            &document,
            &plus_one(published),
            &input.schema,
            &input.binding_rowids,
            "unpublished value",
        )
        .unwrap();

        let error = validate_stream_expr(
            &document,
            &hir::Expr::column(source, 1),
            &input.schema,
            &input.binding_rowids,
            "unpublished value",
        )
        .unwrap_err();
        assert!(error.to_string().contains("unpublished value"));
    }

    #[test]
    fn expression_validation_stops_at_published_expressions() {
        let document = empty_document();
        let published = plus_one(hir::Expr::column(SourceId::new(1), 0));
        let input = NodeOutputContract {
            schema: Arc::new(dag::StreamSchema {
                columns: vec![Some(published.clone())],
                bindings: Vec::new(),
            }),
            binding_rowids: Vec::<bool>::new().into(),
            emitted_identity: DeltaIdentity::OperatorRowid,
            published_identity: DeltaIdentity::OperatorRowid,
        };

        // The column inside the published expression is not published on
        // its own; reading the whole expression is what the stream offers.
        validate_stream_expr(
            &document,
            &published,
            &input.schema,
            &input.binding_rowids,
            "unpublished value",
        )
        .unwrap();
    }

    #[test]
    fn node_output_contract_reuses_dag_schema() {
        let table = test_table();
        let source = SourceId::new(1);
        let schema = Arc::new(dag::StreamSchema {
            columns: Vec::new(),
            bindings: vec![dag::StreamBinding { source }],
        });
        let node = dag::OpNode::Scan { table, source };

        let output = plan_node_output(schema.clone(), &node, &[]).unwrap();

        assert!(Arc::ptr_eq(&output.schema, &schema));
        assert_eq!(output.emitted_identity, DeltaIdentity::BindingRowids(1));
        assert_eq!(output.binding_rowids.as_ref(), &[true]);
    }
}
