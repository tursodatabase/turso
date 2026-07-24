use rustc_hash::{FxHashMap, FxHashSet};
use turso_parser::ast::{self, TableInternalId};

use super::output::NodeOutputContract;
use super::stream::{binding_rowid_metadata_width, DeltaIdentity};
use crate::function::{AggFunc, Func};
use crate::incremental::dag;
use crate::schema::BTreeTable;
use crate::sync::Arc;
use crate::translate::collate::{get_collseq_from_expr_with_symbols, CollationSeq};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{expr_contains_nondeterministic_scalar_function, WalkControl};
use crate::translate::plan::{Aggregate, Plan, QueryDestination, SelectPlan, TableReferences};
use crate::translate::planner::resolve_window_and_aggregate_functions;
use crate::translate::select::prepare_select_plan;
use crate::util::walk_expr_with_subqueries;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::{Connection, LimboError, QueryMode, Result};

/// Whether an aggregate's accumulator must see each distinct argument value
/// exactly once, tracked through the value multiset table. MIN/MAX see each
/// value once by definition, so DISTINCT changes nothing for them — their
/// multiset participation is about extremes, not distinctness.
pub(super) fn tracks_distinct_values(agg: &Aggregate) -> bool {
    agg.distinctness.is_distinct() && !matches!(agg.func, AggFunc::Min | AggFunc::Max)
}

fn unsupported<T>(what: &str) -> Result<T> {
    Err(LimboError::ParseError(format!(
        "materialized views with {what} are not yet supported",
    )))
}

/// Validate an expression against the values and rowids published by one
/// operator edge.
///
/// Downstream expression compilation seeds exactly these expressions into
/// the translator's register cache. Validating the same contract here keeps
/// composition independent of the upstream operator kind: an aggregate
/// result remains usable through any number of linear operators, while a
/// source value that was not published cannot leak across the boundary.
fn validate_stream_expr(
    expr: &ast::Expr,
    input: &NodeOutputContract,
    unavailable_value: &str,
) -> Result<()> {
    walk_expr_with_subqueries(expr, &mut |e: &ast::Expr| {
        let published_value = input.schema.columns.iter().any(|column| {
            column
                .as_ref()
                .is_some_and(|published| crate::util::exprs_are_equivalent(published, e))
        });
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                return Err(LimboError::ParseError(
                    "materialized views with subqueries are not yet supported".to_string(),
                ));
            }
            ast::Expr::FunctionCall { name, args, .. } => {
                if matches!(
                    Func::resolve_function(name.as_str(), args.len()),
                    Ok(Some(Func::Agg(_)))
                ) {
                    if published_value {
                        return Ok(WalkControl::SkipChildren);
                    }
                    return unsupported("expressions over aggregate results");
                }
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                if matches!(
                    Func::resolve_function(name.as_str(), 0),
                    Ok(Some(Func::Agg(_)))
                ) {
                    if published_value {
                        return Ok(WalkControl::SkipChildren);
                    }
                    return unsupported("expressions over aggregate results");
                }
            }
            ast::Expr::Variable(_) => {
                return Err(LimboError::ParseError(
                    "materialized views with bound parameters are not supported".to_string(),
                ));
            }
            ast::Expr::Id(_) | ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                return Err(LimboError::InternalError(
                    "maintenance expression contains an unresolved identifier".to_string(),
                ));
            }
            ast::Expr::Column { .. } => {
                if !published_value {
                    return unsupported(unavailable_value);
                }
                return Ok(WalkControl::SkipChildren);
            }
            ast::Expr::RowId { table, .. } => {
                let available = published_value
                    || input
                        .schema
                        .bindings
                        .iter()
                        .zip(input.binding_rowids.iter())
                        .any(|(binding, available)| binding.logical_id == *table && *available);
                if !available {
                    return unsupported("rowids not published by their input stream");
                }
                return Ok(WalkControl::SkipChildren);
            }
            _ if published_value => return Ok(WalkControl::SkipChildren),
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
}

/// Reject collected aggregates the maintenance codegen cannot invert.
///
/// Aggregate recognition and deduplication use the planner's own collector
/// ([`resolve_window_and_aggregate_functions`]); this is only the IVM gate on
/// top of it.
fn validate_supported_aggregate(agg: &Aggregate) -> Result<()> {
    if agg.filter_expr.is_some() {
        return unsupported("FILTER clauses on aggregates");
    }
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

/// Lower the main query planner's bound relational plan into the maintenance
/// DAG. Parser spelling (aliases, stars, USING expansion, ordinal GROUP BY,
/// and compound arity) has already been resolved by this point.
fn build_dag_from_plan(plan: &Plan, resolver: &Resolver) -> Result<dag::MaintenanceDag> {
    let mut builder = dag::DagBuilder::new();
    let root = build_plan_dag_node(&mut builder, plan, resolver)?;
    builder.finish(root)
}

fn build_plan_dag_node(
    builder: &mut dag::DagBuilder,
    plan: &Plan,
    resolver: &Resolver,
) -> Result<dag::NodeId> {
    let root = match plan {
        Plan::Select(select) => build_select_plan_dag(builder, select, resolver)?,
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut inputs = Vec::with_capacity(left.len() + 1);
            for (branch, _) in left {
                inputs.push(build_select_plan_dag(builder, branch, resolver)?);
            }
            inputs.push(build_select_plan_dag(builder, right_most, resolver)?);

            let operators: Vec<_> = left.iter().map(|(_, operator)| *operator).collect();
            let prefix_len = operators
                .iter()
                .rposition(|operator| *operator != ast::CompoundOperator::UnionAll)
                .map_or(0, |last_dedup| last_dedup + 2);
            let arity = right_most.result_columns.len();
            let key_collations = if prefix_len == 0 {
                Vec::new()
            } else {
                let mut folded = vec![None; arity];
                for branch in left
                    .iter()
                    .map(|(branch, _)| branch)
                    .chain(std::iter::once(right_most.as_ref()))
                    .take(prefix_len)
                {
                    for (index, column) in branch.result_columns.iter().enumerate() {
                        if folded[index].is_none() {
                            folded[index] = plan_expr_collation(
                                &column.expr,
                                &branch.table_references,
                                resolver,
                            )?;
                        }
                    }
                }
                folded
                    .into_iter()
                    .map(|collation| collation.unwrap_or(CollationSeq::Binary))
                    .collect()
            };
            builder.push(dag::OpNode::SetOp {
                inputs,
                operators,
                arity,
                prefix_len,
                key_collations,
            })?
        }
        Plan::Delete(_) | Plan::Update(_) => {
            return Err(LimboError::InternalError(
                "materialized view planner produced a DML plan".to_string(),
            ));
        }
    };
    Ok(root)
}

fn synthesized_derived_table(name: &str, columns: Vec<crate::schema::Column>) -> BTreeTable {
    BTreeTable::new(
        0,
        name.to_string(),
        Vec::new(),
        columns,
        crate::schema::BTreeCharacteristics::empty(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    )
}

fn build_select_plan_dag(
    builder: &mut dag::DagBuilder,
    plan: &SelectPlan,
    resolver: &Resolver,
) -> Result<dag::NodeId> {
    if !plan.order_by.is_empty() {
        return unsupported("ORDER BY");
    }
    if plan.limit.is_some() || plan.offset.is_some() {
        return unsupported("LIMIT or OFFSET");
    }
    if !plan.values.is_empty() {
        return unsupported("VALUES");
    }
    if plan.window.is_some() {
        return unsupported("window functions");
    }
    if !plan.non_from_clause_subqueries.is_empty() {
        return unsupported("subqueries");
    }

    let joined_tables = plan.table_references.joined_tables();
    if joined_tables.is_empty() {
        return unsupported("SELECT without a FROM clause");
    }
    for joined in joined_tables {
        if let Some(join) = &joined.join_info {
            match join.join_type {
                crate::translate::plan::JoinType::Inner
                | crate::translate::plan::JoinType::LeftOuter => {}
                crate::translate::plan::JoinType::FullOuter => {
                    // FULL must eventually lower to the arrangement/set-op
                    // composition. Treating it as the existing LEFT flag
                    // loses the right-side padded delta and is incorrect.
                    return unsupported("FULL OUTER joins");
                }
                crate::translate::plan::JoinType::Semi | crate::translate::plan::JoinType::Anti => {
                    return unsupported("semi or anti joins");
                }
            }
        }
    }

    let mut inputs = Vec::with_capacity(joined_tables.len());
    for joined in joined_tables {
        let input = match &joined.table {
            crate::schema::Table::BTree(table) => builder.push(dag::OpNode::Scan {
                table: table.clone(),
                identifier: joined.identifier.clone(),
                logical_id: joined.internal_id,
            })?,
            crate::schema::Table::FromClauseSubquery(subquery) => {
                let input = build_plan_dag_node(builder, &subquery.plan, resolver)?;
                let table = Arc::new(synthesized_derived_table(
                    &subquery.name,
                    subquery.columns.clone(),
                ));
                builder.push(dag::OpNode::Alias {
                    input,
                    table,
                    identifier: joined.identifier.clone(),
                    logical_id: joined.internal_id,
                })?
            }
            crate::schema::Table::Virtual(_) => return unsupported("virtual-table FROM sources"),
        };
        inputs.push(input);
    }

    let binding_positions = joined_tables
        .iter()
        .enumerate()
        .map(|(position, table)| (table.internal_id, position))
        .collect::<FxHashMap<_, _>>();
    let mut on_by_step = vec![Vec::new(); inputs.len()];
    let mut filters_by_step = vec![Vec::new(); inputs.len()];
    for term in &plan.where_clause {
        if let Some(right_binding) = term.from_outer_join {
            let step = *binding_positions.get(&right_binding).ok_or_else(|| {
                LimboError::InternalError(
                    "outer-join predicate references a table outside the FROM clause".to_string(),
                )
            })?;
            if step == 0
                || !joined_tables[step].join_info.as_ref().is_some_and(|join| {
                    join.join_type == crate::translate::plan::JoinType::LeftOuter
                })
            {
                return Err(LimboError::InternalError(
                    "outer-join predicate is not owned by a LEFT JOIN step".to_string(),
                ));
            }
            on_by_step[step].push(term.expr.clone());
        } else {
            let step = max_referenced_binding_position(&term.expr, &binding_positions)?;
            filters_by_step[step].push(term.expr.clone());
        }
    }

    let mut node = inputs[0];
    if let Some(predicate) = combine_predicates(std::mem::take(&mut filters_by_step[0])) {
        node = builder.push(dag::OpNode::Filter {
            input: node,
            predicate,
        })?;
    }
    for step in 1..inputs.len() {
        let kind = match joined_tables[step]
            .join_info
            .as_ref()
            .map(|join| join.join_type)
            .unwrap_or(crate::translate::plan::JoinType::Inner)
        {
            crate::translate::plan::JoinType::Inner => dag::JoinKind::Inner,
            crate::translate::plan::JoinType::LeftOuter => dag::JoinKind::LeftOuter,
            _ => unreachable!("unsupported join kinds were rejected before DAG lowering"),
        };
        node = builder.push(dag::OpNode::Join {
            inputs: [node, inputs[step]],
            on: std::mem::take(&mut on_by_step[step]),
            kind,
        })?;
        if let Some(predicate) = combine_predicates(std::mem::take(&mut filters_by_step[step])) {
            node = builder.push(dag::OpNode::Filter {
                input: node,
                predicate,
            })?;
        }
    }

    let is_distinct = plan.distinctness.is_distinct();
    if is_distinct && (plan.group_by.is_some() || !plan.aggregates.is_empty()) {
        return unsupported("DISTINCT over aggregates");
    }
    let is_aggregate = plan.group_by.is_some() || !plan.aggregates.is_empty() || is_distinct;
    if is_aggregate {
        let (group_exprs, group_collations, scalar) = if is_distinct {
            let mut exprs = Vec::with_capacity(plan.result_columns.len());
            let mut collations = Vec::with_capacity(plan.result_columns.len());
            for column in &plan.result_columns {
                collations.push(
                    plan_expr_collation(&column.expr, &plan.table_references, resolver)?
                        .unwrap_or(CollationSeq::Binary),
                );
                exprs.push(column.expr.clone());
            }
            (exprs, collations, false)
        } else if plan
            .group_by
            .as_ref()
            .is_some_and(|group_by| !group_by.exprs.is_empty())
        {
            let group_by = plan.group_by.as_ref().unwrap();
            let mut exprs = Vec::with_capacity(group_by.exprs.len());
            let mut collations = Vec::with_capacity(group_by.exprs.len());
            for expr in &group_by.exprs {
                collations.push(
                    plan_expr_collation(expr, &plan.table_references, resolver)?
                        .unwrap_or(CollationSeq::Binary),
                );
                exprs.push(expr.clone());
            }
            (exprs, collations, false)
        } else {
            (
                vec![ast::Expr::Literal(ast::Literal::Numeric("0".to_string()))],
                vec![CollationSeq::Binary],
                true,
            )
        };

        let mut aggregates = hidden_count_aggregate(resolver)?;
        for aggregate in &plan.aggregates {
            let mut normalized = aggregate.clone();
            normalized.fraction_reg = None;
            if !aggregates.iter().any(|existing| {
                crate::util::exprs_are_equivalent(
                    &existing.original_expr,
                    &normalized.original_expr,
                )
            }) {
                aggregates.push(normalized);
            }
        }
        let mut multiset_collations = Vec::with_capacity(aggregates.len());
        for aggregate in &aggregates {
            if !uses_multiset(aggregate) {
                multiset_collations.push(None);
                continue;
            }
            let collation = aggregate
                .args
                .first()
                .map(|argument| plan_expr_collation(argument, &plan.table_references, resolver))
                .transpose()?
                .flatten()
                .unwrap_or(CollationSeq::Binary);
            multiset_collations.push(Some(collation));
        }
        node = builder.push(dag::OpNode::Aggregate {
            input: node,
            group_exprs,
            group_collations,
            aggregates,
            multiset_collations,
            scalar,
        })?;

        if let Some(having) = plan
            .group_by
            .as_ref()
            .and_then(|group_by| group_by.having.as_ref())
        {
            let normalized = having.to_vec();
            if let Some(predicate) = combine_predicates(normalized) {
                node = builder.push(dag::OpNode::Filter {
                    input: node,
                    predicate,
                })?;
            }
        }
    }

    let projections = plan
        .result_columns
        .iter()
        .map(|column| column.expr.clone())
        .collect();
    builder.push(dag::OpNode::Project {
        input: node,
        projections,
    })
}

fn combine_predicates(mut predicates: Vec<ast::Expr>) -> Option<ast::Expr> {
    let first = predicates.pop()?;
    Some(predicates.into_iter().fold(first, |right, left| {
        ast::Expr::Binary(Box::new(left), ast::Operator::And, Box::new(right))
    }))
}

fn max_referenced_binding_position(
    expr: &ast::Expr,
    binding_positions: &FxHashMap<TableInternalId, usize>,
) -> Result<usize> {
    let mut max_position = 0;
    walk_expr_with_subqueries(expr, &mut |node| {
        let binding = match node {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => Some(*table),
            ast::Expr::Id(_) | ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                return Err(LimboError::InternalError(
                    "maintenance predicate contains an unresolved identifier".to_string(),
                ));
            }
            _ => None,
        };
        if let Some(binding) = binding {
            let position = binding_positions.get(&binding).ok_or_else(|| {
                LimboError::InternalError(
                    "maintenance predicate references a binding outside its FROM clause"
                        .to_string(),
                )
            })?;
            max_position = max_position.max(*position);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(max_position)
}

fn hidden_count_aggregate(resolver: &Resolver) -> Result<Vec<Aggregate>> {
    let count_star = ast::Expr::FunctionCallStar {
        name: ast::Name::exact("count".to_string()),
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    let mut aggregates = Vec::new();
    resolve_window_and_aggregate_functions(&count_star, resolver, &mut aggregates, None, &mut [])?;
    Ok(aggregates)
}

fn plan_expr_collation(
    expr: &ast::Expr,
    tables: &TableReferences,
    resolver: &Resolver,
) -> Result<Option<CollationSeq>> {
    let collation = get_collseq_from_expr_with_symbols(expr, tables, Some(resolver.symbol_table))?;
    if collation.is_some_and(|collation| collation.is_custom()) {
        return unsupported("custom collations on grouping or dedup keys");
    }
    Ok(collation.map(|collation| match collation {
        CollationSeq::Unset => CollationSeq::Binary,
        other => other,
    }))
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
    LeftJoinMatches,
    SetOpIdentity,
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
/// own collation-bearing index, while LEFT joins and mixed set operations each
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

    pub(super) fn left_join_matches_table_name(&self) -> Result<&str> {
        self.auxiliary_table_name(AuxiliaryStatePurpose::LeftJoinMatches)
    }

    pub(super) fn set_op_identity_table_name(&self) -> Result<&str> {
        self.auxiliary_table_name(AuxiliaryStatePurpose::SetOpIdentity)
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
    dag: dag::MaintenanceDag,
    operator_states: OperatorStateCatalog,
    output_arity: usize,
    version_marker: HiddenTableDef,
}

impl MaintenancePlan {
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
}

/// Validate a view and build its executable maintenance plan.
pub(crate) fn plan_view(
    view_name: &str,
    select: &ast::Select,
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<MaintenancePlan> {
    let mut planner_program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts::new(8, 32, 8),
    );
    let relational_plan = prepare_select_plan(
        select.clone(),
        resolver,
        &mut planner_program,
        &[],
        QueryDestination::ResultRows,
        connection,
    )?;
    let dag = build_dag_from_plan(&relational_plan, resolver)?;
    let operator_states = plan_operator_states(view_name, &dag, resolver)?;
    let output_arity = dag.root_schema().len();
    let version_marker = version_marker_table(view_name);
    Ok(MaintenancePlan {
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
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    operator_states: &OperatorStateCatalog,
    resolver: &Resolver,
) -> Result<()> {
    let node = &dag.nodes[node_id];
    let check_expr =
        |expr: &ast::Expr, input: dag::NodeId, unavailable_value: &str| -> Result<()> {
            if expr_contains_nondeterministic_scalar_function(expr, resolver)? {
                return unsupported("non-deterministic expressions");
            }
            validate_stream_expr(
                expr,
                &operator_states.for_node(input)?.output,
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
        dag::OpNode::Filter { input, predicate } => check_expr(
            predicate,
            *input,
            "HAVING referencing columns outside GROUP BY",
        )?,
        dag::OpNode::Project { input, projections } => {
            for expr in projections {
                check_expr(expr, *input, "non-aggregate result columns not in GROUP BY")?;
            }
        }
        dag::OpNode::Alias { .. } => {}
        dag::OpNode::Join { inputs, on, .. } => {
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
            let output = &operator_states.for_node(node_id)?.output;
            for expr in on {
                if expr_contains_nondeterministic_scalar_function(expr, resolver)? {
                    return unsupported("non-deterministic expressions");
                }
                validate_stream_expr(
                    expr,
                    output,
                    "join predicates referencing values outside their input stream",
                )?;
            }
        }
        dag::OpNode::SetOp {
            inputs,
            operators,
            arity,
            prefix_len,
            key_collations,
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
            if (*prefix_len == 0 && !key_collations.is_empty())
                || (*prefix_len > 0 && key_collations.len() != *arity)
            {
                return Err(LimboError::InternalError(
                    "composable set-op has an invalid collation layout".to_string(),
                ));
            }
            if key_collations.iter().any(|collation| collation.is_custom()) {
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
            group_collations,
            aggregates,
            multiset_collations,
            ..
        } => {
            if group_exprs.len() != group_collations.len()
                || aggregates.len() != multiset_collations.len()
                || aggregates
                    .iter()
                    .zip(multiset_collations)
                    .any(|(aggregate, collation)| uses_multiset(aggregate) != collation.is_some())
            {
                return Err(LimboError::InternalError(
                    "aggregate DAG has inconsistent collation metadata".to_string(),
                ));
            }
            if group_collations
                .iter()
                .chain(multiset_collations.iter().flatten())
                .any(|collation| collation.is_custom())
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
                if let Some(expr) = &aggregate.filter_expr {
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

fn declared_emitted_identity(
    node: &dag::OpNode,
    prior_states: &[OperatorStateDef],
) -> Result<DeltaIdentity> {
    let identity = match node {
        dag::OpNode::Scan { .. } => DeltaIdentity::BindingRowids(1),
        dag::OpNode::Filter { input, .. } | dag::OpNode::Project { input, .. } => {
            prior_states
                .get(*input)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "maintenance DAG input node {input} has no identity contract"
                    ))
                })?
                .output
                .published_identity
        }
        dag::OpNode::Alias { input, .. } => {
            let upstream = prior_states
                .get(*input)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "maintenance DAG input node {input} has no identity contract"
                    ))
                })?
                .output
                .published_identity;
            alias_identity(upstream)
        }
        dag::OpNode::Join {
            inputs,
            kind: dag::JoinKind::Inner,
            ..
        } => {
            let input_identities = inputs
                .iter()
                .map(|input| {
                    prior_states
                        .get(*input)
                        .map(|state| state.output.published_identity)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "maintenance DAG input node {input} has no identity contract"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            if input_identities
                .iter()
                .all(|identity| matches!(identity, DeltaIdentity::BindingRowids(_)))
            {
                DeltaIdentity::BindingRowids(
                    input_identities
                        .iter()
                        .map(|identity| identity.width())
                        .sum(),
                )
            } else {
                DeltaIdentity::OperatorKey(2)
            }
        }
        // Matched and NULL-padded rows share `(kind, packed source identity)`.
        dag::OpNode::Join {
            kind: dag::JoinKind::LeftOuter,
            ..
        } => DeltaIdentity::OperatorKey(2),
        dag::OpNode::Aggregate { .. } => DeltaIdentity::OperatorRowid,
        dag::OpNode::SetOp { prefix_len, .. } if *prefix_len > 0 => DeltaIdentity::OperatorRowid,
        // Pure UNION ALL namespaces every branch's packed source identity.
        dag::OpNode::SetOp { .. } => DeltaIdentity::OperatorKey(2),
    };
    Ok(identity)
}

fn declared_binding_rowids(
    node: &dag::OpNode,
    prior_states: &[OperatorStateDef],
) -> Result<Arc<[bool]>> {
    let provenance = match node {
        dag::OpNode::Scan { table, .. } => vec![table.has_rowid],
        dag::OpNode::Filter { input, .. } | dag::OpNode::Project { input, .. } => prior_states
            .get(*input)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "maintenance DAG input node {input} has no rowid provenance contract"
                ))
            })?
            .output
            .binding_rowids
            .to_vec(),
        // A derived-table alias introduces one new SQL namespace. The source
        // transport key still distinguishes rows, but it is not the alias's
        // SQL rowid.
        dag::OpNode::Alias { .. } => vec![false],
        dag::OpNode::Join { inputs, .. } => {
            let mut provenance = Vec::new();
            for input in inputs {
                provenance.extend_from_slice(
                    &prior_states
                        .get(*input)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "maintenance DAG input node {input} has no rowid provenance contract"
                            ))
                        })?
                        .output
                        .binding_rowids,
                );
            }
            provenance
        }
        // These operators may retain source expressions as output values, but
        // they do not preserve a one-to-one source-row namespace.
        dag::OpNode::Aggregate { input, .. } => vec![
            false;
            prior_states
                .get(*input)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "maintenance DAG input node {input} has no rowid provenance contract"
                    ))
                })?
                .output
                .binding_rowids
                .len()
        ],
        dag::OpNode::SetOp { inputs, .. } => vec![
            false;
            prior_states
                .get(inputs[0])
                .ok_or_else(|| {
                    LimboError::InternalError(
                        "set-op input has no rowid provenance contract".to_string(),
                    )
                })?
                .output
                .binding_rowids
                .len()
        ],
    };
    Ok(provenance.into())
}

/// A derived-table alias replaces all of its input bindings with one new SQL
/// namespace. The physical source key remains stable, but its slots can no
/// longer be interpreted as one rowid per output binding.
fn alias_identity(upstream: DeltaIdentity) -> DeltaIdentity {
    match upstream {
        DeltaIdentity::BindingRowids(width) => DeltaIdentity::OperatorKey(width),
        identity => identity,
    }
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

fn node_is_left_join_input(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> bool {
    dag.nodes.iter().any(|node| {
        matches!(
            node,
            dag::OpNode::Join {
                inputs,
                kind: dag::JoinKind::LeftOuter,
                ..
            } if inputs.contains(&node_id)
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
    if matches!(
        dag.nodes[node_id],
        dag::OpNode::Join {
            kind: dag::JoinKind::LeftOuter,
            ..
        } | dag::OpNode::SetOp { prefix_len: 0, .. }
    ) {
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
    dag: &dag::MaintenanceDag,
    resolver: &Resolver,
) -> Result<OperatorStateCatalog> {
    use crate::incremental::view::DBSP_CIRCUIT_VERSION;

    let mut catalog = OperatorStateCatalog {
        nodes: Vec::with_capacity(dag.nodes.len()),
    };
    let mut hidden_names = FxHashSet::default();
    for (node_id, node) in dag.nodes.iter().enumerate() {
        let emitted_identity = declared_emitted_identity(node, &catalog.nodes)?;
        let binding_rowids = declared_binding_rowids(node, &catalog.nodes)?;
        if binding_rowids.len() != dag.output_schema(node_id).bindings.len() {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} rowid provenance does not match its output bindings"
            )));
        }
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
            &binding_rowids,
        );
        let arrangement_table = if node_requires_output_arrangement(dag, node_id, emitted_identity)
        {
            let identity_width = emitted_identity.width();
            let table_name = format!(
                "{}{DBSP_CIRCUIT_VERSION}_{view_name}__n{node_id}",
                crate::schema::DBSP_ARRANGEMENT_TABLE_PREFIX,
            );
            Some(HiddenTableDef {
                create_sql: arrangement_table_sql(
                    &table_name,
                    identity_width,
                    dag.output_schema(node_id).len(),
                    binding_rowid_metadata_width(emitted_identity, &binding_rowids),
                )?,
                table_name,
                primary_key_index: true,
            })
        } else {
            None
        };
        let published_identity = if arrangement_table.is_some()
            && (node_id == dag.root || node_is_left_join_input(dag, node_id))
        {
            DeltaIdentity::OperatorRowid
        } else {
            emitted_identity
        };
        let state = OperatorStateDef {
            node_id,
            output: NodeOutputContract {
                schema: Arc::new(dag.output_schema(node_id).clone()),
                binding_rowids,
                emitted_identity,
                published_identity,
            },
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
        validate_planned_node(dag, node_id, &catalog, resolver)?;
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
            group_collations,
            multiset_collations,
            ..
        } => multiset_collations
            .iter()
            .enumerate()
            .filter_map(|(aggregate_index, collation)| {
                collation.map(|collation| {
                    let table_name = table_name(&format!("__a{aggregate_index}"));
                    let create_sql =
                        aggregate_multiset_table_sql(&table_name, group_collations, collation);
                    table(
                        AuxiliaryStatePurpose::AggregateMultiset { aggregate_index },
                        table_name,
                        create_sql,
                    )
                })
            })
            .collect(),
        dag::OpNode::Join {
            kind: dag::JoinKind::LeftOuter,
            ..
        } => {
            let table_name = table_name("");
            let payload_width = output_width
                + binding_rowids
                    .iter()
                    .filter(|available| **available)
                    .count();
            vec![table(
                AuxiliaryStatePurpose::LeftJoinMatches,
                table_name.clone(),
                left_join_matches_table_sql(&table_name, payload_width),
            )]
        }
        dag::OpNode::SetOp {
            operators,
            prefix_len,
            ..
        } if *prefix_len > 0 && *prefix_len < operators.len() + 1 => {
            let table_name = table_name("");
            vec![table(
                AuxiliaryStatePurpose::SetOpIdentity,
                table_name.clone(),
                set_op_identity_table_sql(&table_name),
            )]
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
    let key = columns[..identity_width + value_width].to_vec();
    columns.extend((0..binding_rowid_width).map(|i| format!("r{i}")));
    columns.push("mult".to_string());
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
            group_collations,
            aggregates,
            ..
        } => {
            let mut columns = Vec::new();
            for (i, collation) in group_collations.iter().enumerate() {
                columns.push(format!("g{i}{}", collate_clause(*collation)));
            }
            for i in 0..aggregates.len() {
                columns.push(format!("v{i}"));
            }
            for (i, agg) in aggregates.iter().enumerate() {
                if let Some(width) = crate::vdbe::execute::agg_payload_width(&agg.func) {
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
            key_collations,
            ..
        } => {
            if *prefix_len == 0 {
                return Err(LimboError::InternalError(
                    "pure UNION ALL is a stateless branch composition".to_string(),
                ));
            }
            let mut columns: Vec<String> = (0..*arity)
                .map(|i| format!("c{i}{}", collate_clause(key_collations[i])))
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

/// One aggregate's value multiset: one row per (group, distinct value) with
/// its multiplicity. A table per aggregate lets the value-key index carry
/// that argument's own collation. For MIN/MAX the key ordering makes a
/// group's extreme an index seek; for DISTINCT aggregates the multiplicity
/// decides when a value enters or leaves the accumulator.
fn aggregate_multiset_table_sql(
    multiset_table_name: &str,
    group_collations: &[CollationSeq],
    value_collation: CollationSeq,
) -> String {
    let multiset_table_ident = crate::util::quote_identifier(multiset_table_name);
    let mut columns = Vec::with_capacity(group_collations.len() + 2);
    let mut key = Vec::with_capacity(group_collations.len() + 1);
    for (i, collation) in group_collations.iter().enumerate() {
        columns.push(format!("g{i}{}", collate_clause(*collation)));
        key.push(format!("g{i}"));
    }
    columns.push(format!("val{}", collate_clause(value_collation)));
    key.push("val".to_string());
    columns.push("mult".to_string());
    format!(
        "CREATE TABLE {multiset_table_ident} ({}, PRIMARY KEY ({}))",
        columns.join(", "),
        key.join(", ")
    )
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

/// A LEFT JOIN's per-left-row presence, ON-match count, padded-row publication
/// flag, and preserved output image.
fn left_join_matches_table_sql(table_name: &str, payload_width: usize) -> String {
    let table_ident = crate::util::quote_identifier(table_name);
    let payload = (0..payload_width)
        .map(|i| format!(", c{i}"))
        .collect::<String>();
    format!(
        "CREATE TABLE {table_ident} (l_rid, present, matches, padded{payload}, PRIMARY KEY (l_rid))"
    )
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

    fn sum_expr(column: usize) -> ast::Expr {
        ast::Expr::FunctionCall {
            name: ast::Name::exact("sum".to_string()),
            distinctness: None,
            args: vec![Box::new(ast::Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column,
                is_rowid_alias: false,
            })],
            order_by: Vec::new(),
            within_group: Vec::new(),
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        }
    }

    #[test]
    fn expression_validation_depends_only_on_published_values() {
        let sum = sum_expr(0);
        let input = NodeOutputContract {
            schema: Arc::new(dag::StreamSchema {
                columns: vec![Some(sum.clone())],
                bindings: Vec::new(),
            }),
            binding_rowids: Vec::<bool>::new().into(),
            emitted_identity: DeltaIdentity::OperatorRowid,
            published_identity: DeltaIdentity::OperatorRowid,
        };
        let expression = ast::Expr::Binary(
            Box::new(sum),
            ast::Operator::Add,
            Box::new(ast::Expr::Literal(ast::Literal::Numeric("1".to_string()))),
        );

        validate_stream_expr(&expression, &input, "unpublished value").unwrap();

        let error = validate_stream_expr(&sum_expr(1), &input, "unpublished value").unwrap_err();
        assert!(error
            .to_string()
            .contains("expressions over aggregate results"));
    }
}
