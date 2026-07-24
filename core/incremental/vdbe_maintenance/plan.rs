use super::*;

pub(super) fn tracks_distinct_values(agg: &Aggregate) -> bool {
    agg.distinctness.is_distinct() && !matches!(agg.func, AggFunc::Min | AggFunc::Max)
}

fn unsupported<T>(what: &str) -> Result<T> {
    Err(LimboError::ParseError(format!(
        "materialized views with {what} are not yet supported",
    )))
}

/// Reject subqueries, bound parameters, and (unless the expression IS a
/// supported aggregate handled by the caller) aggregate function calls.
fn check_scalar_expr(expr: &ast::Expr) -> Result<()> {
    walk_expr_with_subqueries(expr, &mut |e: &ast::Expr| {
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
                    return unsupported("expressions over aggregate results");
                }
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                if matches!(
                    Func::resolve_function(name.as_str(), 0),
                    Ok(Some(Func::Agg(_)))
                ) {
                    return unsupported("expressions over aggregate results");
                }
            }
            ast::Expr::Variable(_) => {
                return Err(LimboError::ParseError(
                    "materialized views with bound parameters are not supported".to_string(),
                ));
            }
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
    for arg in &agg.args {
        check_scalar_expr(arg)?;
    }
    Ok(())
}

/// Validate that a HAVING expression evaluates only group expressions and
/// aggregate results.
///
/// The aggregates themselves were already collected into `aggregates` by the
/// planner's collector; this walk rejects what remains outside them: bare
/// column references (SQLite's arbitrary-row semantics cannot be maintained
/// incrementally), subqueries, and bound parameters.
fn check_group_row_expr(
    expr: &ast::Expr,
    group_exprs: &[ast::Expr],
    aggregates: &[Aggregate],
    bare_column_error: &str,
) -> Result<()> {
    walk_expr_with_subqueries(expr, &mut |e: &ast::Expr| {
        if group_exprs
            .iter()
            .any(|g| crate::util::exprs_are_equivalent(g, e))
            || aggregates
                .iter()
                .any(|a| crate::util::exprs_are_equivalent(&a.original_expr, e))
        {
            return Ok(WalkControl::SkipChildren);
        }
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                Err(LimboError::ParseError(
                    "materialized views with subqueries are not yet supported".to_string(),
                ))
            }
            ast::Expr::Variable(_) => Err(LimboError::ParseError(
                "materialized views with bound parameters are not supported".to_string(),
            )),
            ast::Expr::Id(_)
            | ast::Expr::Qualified(_, _)
            | ast::Expr::DoublyQualified(_, _, _)
            | ast::Expr::Column { .. }
            | ast::Expr::RowId { .. } => unsupported(bare_column_error),
            _ => Ok(WalkControl::Continue),
        }
    })
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

        let mut multiset_collation = None;
        for aggregate in plan
            .aggregates
            .iter()
            .filter(|aggregate| uses_multiset(aggregate))
        {
            let collation = aggregate
                .args
                .first()
                .map(|argument| {
                    plan_expr_collation(argument, &plan.table_references, resolver)
                        .map(|collation| collation.unwrap_or(CollationSeq::Binary))
                })
                .transpose()?
                .unwrap_or(CollationSeq::Binary);
            if multiset_collation.is_some_and(|existing| existing != collation) {
                return unsupported(
                    "different collations across MIN/MAX or DISTINCT aggregate arguments",
                );
            }
            multiset_collation = Some(collation);
        }
        let multiset_collation = multiset_collation.unwrap_or(CollationSeq::Binary);
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
        node = builder.push(dag::OpNode::Aggregate {
            input: node,
            group_exprs,
            group_collations,
            aggregates,
            multiset_collation,
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
        .map(|column| Ok((column.expr.clone(), column.alias.clone())))
        .collect::<Result<Vec<_>>>()?;
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
pub struct HiddenTableDef {
    pub table_name: String,
    pub create_sql: String,
}

/// The complete physical output contract of one DAG node.
///
/// Planning derives this once from the logical node and its already-planned
/// inputs. Bytecode emission consumes it verbatim; emitters must not infer an
/// identity from an operator shape or reconstruct a schema from physical
/// inputs.
#[derive(Debug, Clone)]
pub(super) struct NodeOutputContract {
    pub(super) schema: Arc<dag::StreamSchema>,
    /// Whether each logical binding's SQL rowid remains available after this
    /// operator. Transport identity is planned separately below.
    pub(super) binding_rowids: Arc<[bool]>,
    /// Identity emitted by the relational operator before an optional output
    /// arrangement.
    pub(super) emitted_identity: DeltaIdentity,
    /// Identity published on the node's DAG edge. An explicit arrangement
    /// replaces the producer identity with its own stable rowid.
    pub(super) published_identity: DeltaIdentity,
}

/// Persistent storage and output contract assigned to one DAG node.
///
/// `state_table` is the operator's primary integral. `auxiliary_table` is the
/// aggregate value multiset, the LEFT-join unmatched-row bookkeeping, or the
/// trailing-UNION-ALL identity multiset associated with that same node.
/// `arrangement_table` is the node's explicitly materialized output integral
/// when a downstream join or terminal sink needs a one-rowid identity and the
/// operator has no native arrangement.
#[derive(Debug, Clone)]
pub(super) struct OperatorStateDef {
    pub(super) node_id: dag::NodeId,
    pub(super) output: NodeOutputContract,
    pub(super) state_table: Option<HiddenTableDef>,
    pub(super) auxiliary_table: Option<HiddenTableDef>,
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

    pub(super) fn auxiliary_table_name(&self) -> Result<&str> {
        self.auxiliary_table
            .as_ref()
            .map(|table| table.table_name.as_str())
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "maintenance DAG node {} has no auxiliary state table",
                    self.node_id
                ))
            })
    }
}

/// Node-indexed inventory shared by CREATE and maintenance code generation.
///
/// The vector has exactly one entry per DAG node, so codegen cannot recover
/// storage by reclassifying a branch or reconstructing a name.
#[derive(Debug, Clone)]
pub struct OperatorStateCatalog {
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

    pub(super) fn output_for_node(&self, node_id: dag::NodeId) -> Result<&NodeOutputContract> {
        Ok(&self.for_node(node_id)?.output)
    }

    pub fn hidden_tables(&self) -> impl Iterator<Item = &HiddenTableDef> {
        self.nodes.iter().flat_map(|state| {
            state
                .state_table
                .iter()
                .chain(state.auxiliary_table.iter())
                .chain(state.arrangement_table.iter())
        })
    }
}

/// The single CREATE/compile-time description of an incremental view.
///
/// Validation builds the DAG once, storage is derived from that DAG, and
/// codegen consumes the same representation. Keeping these together prevents
/// the accepted-query set, hidden schema, and bytecode dispatcher from
/// evolving as three independent shape classifiers.
pub struct MaintenancePlan {
    pub dag: dag::MaintenanceDag,
    pub operator_states: OperatorStateCatalog,
    pub output_arity: usize,
}

/// Validate a view and build its executable maintenance plan.
pub fn plan_view(
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
    Ok(MaintenancePlan {
        dag,
        operator_states,
        output_arity,
    })
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
    validate_output_schema(dag, node_id)?;
    if let dag::OpNode::Scan { table, .. } = node {
        if !table.has_rowid {
            return unsupported("WITHOUT ROWID base tables");
        }
        if table.has_virtual_columns {
            return unsupported("base tables with virtual generated columns");
        }
    }
    let check_expr = |expr: &ast::Expr| -> Result<()> {
        if expr_contains_nondeterministic_scalar_function(expr, resolver)? {
            return unsupported("non-deterministic expressions");
        }
        Ok(())
    };
    match node {
        dag::OpNode::Filter { predicate, .. } => check_expr(predicate)?,
        dag::OpNode::Project { projections, .. } => {
            for (expr, _) in projections {
                check_expr(expr)?;
            }
        }
        dag::OpNode::Join { on, .. } => {
            for expr in on {
                check_expr(expr)?;
            }
        }
        dag::OpNode::Aggregate {
            group_exprs,
            aggregates,
            ..
        } => {
            for expr in group_exprs {
                check_expr(expr)?;
            }
            for aggregate in aggregates {
                for expr in &aggregate.args {
                    check_expr(expr)?;
                }
                if let Some(expr) = &aggregate.filter_expr {
                    check_expr(expr)?;
                }
            }
        }
        dag::OpNode::Scan { .. } | dag::OpNode::Alias { .. } | dag::OpNode::SetOp { .. } => {}
    }
    validate_composable_delta_node(dag, node_id, operator_states)
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

fn validate_output_schema(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> Result<()> {
    let node = &dag.nodes[node_id];
    let schema = dag.output_schema(node_id);
    let valid = match node {
        dag::OpNode::Scan {
            table,
            identifier,
            logical_id,
        } => {
            schema.len() == table.columns().len()
                && schema.bindings.len() == 1
                && schema.bindings[0].table.root_page == table.root_page
                && schema.bindings[0].identifier == *identifier
                && schema.bindings[0].logical_id == *logical_id
                && schema.columns.iter().enumerate().zip(table.columns()).all(
                    |((column_index, stream), column)| {
                        stream.name == column.name
                            && match (&stream.expr, &column.name) {
                                (
                                    Some(ast::Expr::Column {
                                        table: expr_table,
                                        column: expr_column,
                                        is_rowid_alias,
                                        ..
                                    }),
                                    Some(_),
                                ) => {
                                    *expr_table == *logical_id
                                        && *expr_column == column_index
                                        && *is_rowid_alias == column.is_rowid_alias()
                                }
                                (None, None) => true,
                                _ => false,
                            }
                    },
                )
        }
        dag::OpNode::Filter { input, .. } => {
            schema.columns.len() == dag.output_schema(*input).columns.len()
                && stream_bindings_match(schema, dag.output_schema(*input))
                && schema
                    .columns
                    .iter()
                    .zip(&dag.output_schema(*input).columns)
                    .all(|(left, right)| {
                        left.name == right.name
                            && match (&left.expr, &right.expr) {
                                (Some(left), Some(right)) => {
                                    crate::util::exprs_are_equivalent(left, right)
                                }
                                (None, None) => true,
                                _ => false,
                            }
                    })
        }
        dag::OpNode::Project { input, projections } => {
            schema.len() == projections.len()
                && stream_bindings_match(schema, dag.output_schema(*input))
                && schema
                    .columns
                    .iter()
                    .zip(projections)
                    .all(|(stream, (expr, name))| {
                        stream.name == *name
                            && stream.expr.as_ref().is_some_and(|stream_expr| {
                                crate::util::exprs_are_equivalent(stream_expr, expr)
                            })
                    })
        }
        dag::OpNode::Alias {
            input,
            table,
            identifier,
            logical_id,
        } => {
            schema.len() == dag.output_schema(*input).len()
                && schema.len() == table.columns().len()
                && schema.bindings.len() == 1
                && Arc::ptr_eq(&schema.bindings[0].table, table)
                && schema.bindings[0].identifier == *identifier
                && schema.bindings[0].logical_id == *logical_id
                && schema.columns.iter().enumerate().zip(table.columns()).all(
                    |((column_index, stream), column)| {
                        stream.name == column.name
                            && match (&stream.expr, &column.name) {
                                (
                                    Some(ast::Expr::Column {
                                        table: expr_table,
                                        column: expr_column,
                                        is_rowid_alias,
                                        ..
                                    }),
                                    Some(_),
                                ) => {
                                    *expr_table == *logical_id
                                        && *expr_column == column_index
                                        && *is_rowid_alias == column.is_rowid_alias()
                                }
                                (None, None) => true,
                                _ => false,
                            }
                    },
                )
        }
        dag::OpNode::Join { inputs, .. } => {
            schema.len()
                == inputs
                    .iter()
                    .map(|input| dag.output_schema(*input).len())
                    .sum::<usize>()
                && schema.bindings.len()
                    == inputs
                        .iter()
                        .map(|input| dag.output_schema(*input).bindings.len())
                        .sum::<usize>()
        }
        dag::OpNode::Aggregate {
            input,
            group_exprs,
            aggregates,
            ..
        } => {
            schema.len() == group_exprs.len() + aggregates.len()
                && stream_bindings_match(schema, dag.output_schema(*input))
        }
        dag::OpNode::SetOp { arity, .. } => schema.len() == *arity,
    };
    if !valid {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} has an inconsistent output schema"
        )));
    }
    Ok(())
}

fn stream_bindings_match(left: &dag::StreamSchema, right: &dag::StreamSchema) -> bool {
    left.bindings.len() == right.bindings.len()
        && left
            .bindings
            .iter()
            .zip(&right.bindings)
            .all(|(left, right)| {
                left.table.root_page == right.table.root_page
                    && left.identifier == right.identifier
                    && left.logical_id == right.logical_id
            })
}

fn validate_composable_delta_node(
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    operator_states: &OperatorStateCatalog,
) -> Result<()> {
    match &dag.nodes[node_id] {
        dag::OpNode::Scan { .. } => Ok(()),
        dag::OpNode::Filter { input, predicate } => {
            if let dag::OpNode::Aggregate {
                group_exprs,
                aggregates,
                ..
            } = &dag.nodes[*input]
            {
                check_group_row_expr(
                    predicate,
                    group_exprs,
                    aggregates,
                    "HAVING referencing columns outside GROUP BY",
                )?;
            } else {
                check_scalar_expr(predicate)?;
            }
            Ok(())
        }
        dag::OpNode::Project { input, projections } => {
            let (aggregate, output_filter) = match &dag.nodes[*input] {
                dag::OpNode::Aggregate { .. } => (Some(*input), None),
                dag::OpNode::Filter {
                    input: aggregate,
                    predicate,
                } if matches!(dag.nodes[*aggregate], dag::OpNode::Aggregate { .. }) => {
                    (Some(*aggregate), Some(predicate))
                }
                _ => (None, None),
            };
            if let Some(aggregate) = aggregate {
                let dag::OpNode::Aggregate {
                    group_exprs,
                    aggregates,
                    ..
                } = &dag.nodes[aggregate]
                else {
                    unreachable!()
                };
                if let Some(predicate) = output_filter {
                    check_group_row_expr(
                        predicate,
                        group_exprs,
                        aggregates,
                        "HAVING referencing columns outside GROUP BY",
                    )?;
                }
                for (expr, _) in projections {
                    check_group_row_expr(
                        expr,
                        group_exprs,
                        aggregates,
                        "non-aggregate result columns not in GROUP BY",
                    )?;
                }
                return Ok(());
            }
            for (expr, _) in projections {
                check_scalar_expr(expr)?;
            }
            Ok(())
        }
        dag::OpNode::Alias { .. } => Ok(()),
        dag::OpNode::Join { inputs, on, .. } => {
            validate_join_node(dag, inputs, on, operator_states)?;
            Ok(())
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
            Ok(())
        }
        dag::OpNode::Aggregate {
            group_exprs,
            group_collations,
            aggregates,
            ..
        } => {
            if group_exprs.len() != group_collations.len() {
                return Err(LimboError::InternalError(
                    "aggregate DAG has inconsistent group collation layout".to_string(),
                ));
            }
            if group_collations
                .iter()
                .any(|collation| collation.is_custom())
            {
                return unsupported("custom collations on grouping or dedup keys");
            }
            for expr in group_exprs {
                check_scalar_expr(expr)?;
            }
            for aggregate in aggregates {
                validate_supported_aggregate(aggregate)?;
            }
            Ok(())
        }
    }
}

fn validate_join_node(
    dag: &dag::MaintenanceDag,
    inputs: &[dag::NodeId; 2],
    predicates: &[ast::Expr],
    operator_states: &OperatorStateCatalog,
) -> Result<()> {
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
    for predicate in predicates {
        check_scalar_expr(predicate)?;
    }
    Ok(())
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
            })
        } else {
            None
        };
        let auxiliary_table = if node_needs_auxiliary_state(node) {
            let table_name = format!(
                "{}{DBSP_CIRCUIT_VERSION}_{view_name}__n{node_id}",
                crate::schema::DBSP_MULTISET_TABLE_PREFIX,
            );
            Some(HiddenTableDef {
                create_sql: multiset_table_sql(
                    &table_name,
                    node,
                    dag.output_schema(node_id).len()
                        + if matches!(
                            node,
                            dag::OpNode::Join {
                                kind: dag::JoinKind::LeftOuter,
                                ..
                            }
                        ) {
                            binding_rowids
                                .iter()
                                .filter(|available| **available)
                                .count()
                        } else {
                            0
                        },
                )?,
                table_name,
            })
        } else {
            None
        };
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
            auxiliary_table,
            arrangement_table,
        };
        for table in state
            .state_table
            .iter()
            .chain(state.auxiliary_table.iter())
            .chain(state.arrangement_table.iter())
        {
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
/// (branch, source-identity, multiplicity) rows in the auxiliary table (see
/// [`multiset_table_sql`]); pure UNION ALL is stateless branch composition.
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

/// Whether this operator needs auxiliary state.
///
/// Aggregate value multisets are part of aggregate semantics. A mixed set-op's
/// trailing UNION ALL identity map is also part of its output contract: the
/// operator must publish one stable identity whether its consumer is another
/// operator or the terminal view.
fn node_needs_auxiliary_state(node: &dag::OpNode) -> bool {
    match node {
        dag::OpNode::Aggregate { aggregates, .. } => aggregates.iter().any(uses_multiset),
        dag::OpNode::SetOp {
            operators,
            prefix_len,
            ..
        } => *prefix_len > 0 && *prefix_len < operators.len() + 1,
        dag::OpNode::Join { kind, .. } => *kind == dag::JoinKind::LeftOuter,
        _ => false,
    }
}

/// The CREATE TABLE statement for a GROUP BY view's value multiset: one row
/// per (aggregate, group, distinct value) with its multiplicity. For MIN/MAX
/// the PRIMARY KEY ordering makes a group's extreme value an index seek, so
/// retracting the current extreme never needs a rescan in Rust; for DISTINCT
/// aggregates the multiplicity decides when a value enters or leaves the
/// accumulator.
fn multiset_table_sql(
    multiset_table_name: &str,
    node: &dag::OpNode,
    payload_width: usize,
) -> Result<String> {
    let multiset_table_ident = crate::util::quote_identifier(multiset_table_name);
    match node {
        dag::OpNode::Aggregate {
            group_collations,
            multiset_collation,
            ..
        } => {
            let mut columns = vec!["agg_id".to_string()];
            let mut key = vec!["agg_id".to_string()];
            for (i, collation) in group_collations.iter().enumerate() {
                columns.push(format!("g{i}{}", collate_clause(*collation)));
                key.push(format!("g{i}"));
            }
            columns.push(format!("val{}", collate_clause(*multiset_collation)));
            key.push("val".to_string());
            Ok(format!(
                "CREATE TABLE {multiset_table_ident} ({}, mult, PRIMARY KEY ({}))",
                columns.join(", "),
                key.join(", ")
            ))
        }
        // A compound view's trailing UNION ALL branches: a multiset of
        // (branch, packed source identity), each row's rowid doubling as its
        // view rowid (disambiguated from content-row rowids by the caller).
        // Packing makes the contract independent of whether a branch carries
        // one scan rowid or a composed join identity tuple.
        dag::OpNode::SetOp { .. } => Ok(format!(
            "CREATE TABLE {multiset_table_ident} (branch, source_identity, mult, PRIMARY KEY (branch, source_identity))"
        )),
        // A LEFT JOIN view's per-left-row bookkeeping: signed presence,
        // count of ON-matches, and whether its NULL-padded row is currently
        // published. The payload preserves that image and its binding-rowid
        // provenance so downstream operators can consume its retraction after
        // the left row departs.
        dag::OpNode::Join { .. } => {
            let payload = (0..payload_width)
                .map(|i| format!(", c{i}"))
                .collect::<String>();
            Ok(format!(
                "CREATE TABLE {multiset_table_ident} (l_rid, present, matches, padded{payload}, PRIMARY KEY (l_rid))"
            ))
        }
        _ => Err(LimboError::InternalError(
            "only GROUP BY, compound, and LEFT JOIN views have multiset tables".to_string(),
        )),
    }
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
