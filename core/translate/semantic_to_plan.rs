//! Adapts resolved semantic sources to planner structures.

use super::{
    optimizer::{
        apply_hir_selected_btree_access, apply_hir_selected_virtual_access, base_row_estimate,
        constraints::{hir_constraints_for_source, HirConstraintSource, HirTableConstraints},
        cost::{Cost, RowCountEstimate},
        join::compute_hir_greedy_join_order,
        order::{
            hir_plan_satisfies_order_target, ColumnOrder, ColumnTarget, EliminatesSortBy,
            HirOrderTarget, OrderTarget, OrderTargetPurpose,
        },
        CostModelParams, HirBtreeOperation, HirVirtualTableOperation,
    },
    plan::{
        ColumnUsedMask, HirCteMaterialization, HirFromGroupBoundary, HirFromLayout, HirJoinInfo,
        HirPlanSource, HirPlannedSource, HirWhereTerm, PredicateExpr,
    },
    semantic::hir::{self, ColumnUsage, HirDocument, QueryBlockId, QueryId, SourceId},
};
use crate::{
    schema::{Index, Schema, Table},
    sync::Arc,
    LimboError, Result,
};
use rustc_hash::FxHashSet as HashSet;

enum HirFromItem<'a> {
    Source(SourceId),
    Group {
        source: SourceId,
        from: &'a hir::From,
    },
}

#[derive(Clone, Copy, Default)]
struct HirFromContext {
    parent_group: Option<SourceId>,
    outer_join: Option<SourceId>,
}

/// Planned form of one resolved HIR document. The semantic document stays
/// alive so source and output identities remain resolvable without copying
/// definitions.
pub(crate) struct HirPlan {
    pub(crate) document: Arc<HirDocument>,
    pub(crate) root: HirRootPlan,
    pub(crate) queries: Vec<HirPlannedQuery>,
}

/// Planner facts owned by the semantic root. Variants without a source scan
/// need no additional access data before lowering.
pub(crate) enum HirRootPlan {
    Query,
    Insert,
    Update(HirAccessPlan),
    Delete(HirAccessPlan),
    Trigger(HirTriggerPlan),
    SchemaExpressions,
}

/// Planner facts for one trigger body. Expressions remain owned by HIR; this
/// records only command access plans and their source order.
pub(crate) enum HirTriggerPlan {
    Predicate,
    Command(HirTriggerCommandPlan),
    Program(Vec<HirTriggerCommandPlan>),
}

/// Planner facts for one command inside a trigger body.
pub(crate) enum HirTriggerCommandPlan {
    Select(QueryId),
    Insert,
    Update(HirAccessPlan),
    Delete(HirAccessPlan),
}

/// Access plans and estimates for one query in the owned HIR document.
pub(crate) struct HirPlannedQuery {
    pub(crate) query: QueryId,
    pub(crate) blocks: Vec<HirQueryBlockPlan>,
    pub(crate) output_cardinality: f64,
    pub(crate) cost: Cost,
}

impl HirPlan {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn build(
        document: Arc<HirDocument>,
        schema: &Schema,
        params: &CostModelParams,
    ) -> Result<Self> {
        let mut queries = Vec::new();
        let mut active = HashSet::default();
        let context = HirPlanContext::new(&document);
        for query in &document.queries {
            context.plan_query_tree(query.id, schema, params, &mut queries, &mut active)?;
        }
        queries.sort_unstable_by_key(|plan| plan.query.index());
        let query_estimate = |query| {
            queries
                .iter()
                .find(|plan| plan.query == query)
                .map(|plan| HirQueryEstimate {
                    output_cardinality: plan.output_cardinality,
                    cost: plan.cost,
                })
        };
        let root = context.plan_root(schema, params, &query_estimate)?;
        Ok(Self {
            document,
            root,
            queries,
        })
    }

    pub(crate) fn planned_query(&self, query: QueryId) -> Option<&HirPlannedQuery> {
        self.queries.iter().find(|plan| plan.query == query)
    }
}

/// Resolved source and predicate input for one query block.
pub(crate) struct HirQueryBlockPlanInput {
    pub(crate) sources: Vec<HirPlanSource>,
    pub(crate) groups: Vec<HirFromGroupBoundary>,
    pub(crate) predicates: Vec<HirWhereTerm>,
}

/// Access choices for one resolved HIR query block.
pub(crate) struct HirQueryBlockPlan {
    pub(crate) block: QueryBlockId,
    pub(crate) groups: Vec<HirFromGroupBoundary>,
    pub(crate) loops: Vec<HirPlannedLoop>,
    pub(crate) predicates: Vec<HirWhereTerm>,
    pub(crate) group_sort_elided: bool,
    pub(crate) output_cardinality: f64,
    pub(crate) cost: Cost,
}

/// Access choices and estimates shared by query blocks and DML scans.
pub(crate) struct HirAccessPlan {
    pub(crate) groups: Vec<HirFromGroupBoundary>,
    pub(crate) loops: Vec<HirPlannedLoop>,
    pub(crate) predicates: Vec<HirWhereTerm>,
    pub(crate) group_sort_elided: bool,
    pub(crate) output_cardinality: f64,
    pub(crate) cost: Cost,
}

/// One selected source loop in physical join order, still using HIR
/// expressions and document-local source identity.
pub(crate) struct HirPlannedLoop {
    pub(crate) source: SourceId,
    pub(crate) source_position: usize,
    pub(crate) access: HirSourceAccess,
}

/// Whether an index scan can supply every referenced value itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BtreeTableLookup {
    ScanOnly,
    TableRequired,
}

/// Selected access for one resolved HIR source.
pub(crate) enum HirSourceAccess {
    BTree {
        operation: HirBtreeOperation,
        table_lookup: BtreeTableLookup,
    },
    Virtual(HirVirtualTableOperation),
    Derived {
        query: QueryId,
    },
    Cte {
        cte: hir::CteId,
        query: QueryId,
        materialization: HirCteMaterialization,
    },
    RecursiveCte {
        cte: hir::CteId,
    },
    RecursiveInput {
        cte: hir::CteId,
    },
}

#[derive(Clone, Copy)]
struct HirQueryEstimate {
    output_cardinality: f64,
    cost: Cost,
}

/// State shared while one HIR document is converted into plan nodes.
pub(crate) struct HirPlanContext<'a> {
    document: &'a HirDocument,
}

impl<'a> HirPlanContext<'a> {
    pub(crate) fn new(document: &'a HirDocument) -> Self {
        Self { document }
    }

    fn definition(&self, id: SourceId) -> &'a hir::Source {
        self.document
            .source(id)
            .expect("validated HIR contains referenced source")
    }

    fn plan_root(
        &self,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirRootPlan> {
        match &self.document.root {
            hir::HirRoot::Query(_) => Ok(HirRootPlan::Query),
            hir::HirRoot::Insert(_) => Ok(HirRootPlan::Insert),
            hir::HirRoot::Update(update) => self
                .plan_update(update, schema, params, query_estimate)
                .map(HirRootPlan::Update),
            hir::HirRoot::Delete(delete) => self
                .plan_delete(delete, schema, params, query_estimate)
                .map(HirRootPlan::Delete),
            hir::HirRoot::Trigger(trigger) => self
                .plan_trigger(trigger, schema, params, query_estimate)
                .map(HirRootPlan::Trigger),
            hir::HirRoot::SchemaExpressions(_) => Ok(HirRootPlan::SchemaExpressions),
        }
    }

    fn plan_trigger(
        &self,
        trigger: &hir::TriggerRoot,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirTriggerPlan> {
        match &trigger.body {
            hir::TriggerBody::Predicate(_) => Ok(HirTriggerPlan::Predicate),
            hir::TriggerBody::Command(command) => self
                .plan_trigger_command(command, schema, params, query_estimate)
                .map(HirTriggerPlan::Command),
            hir::TriggerBody::Program(program) => program
                .commands
                .iter()
                .map(|command| self.plan_trigger_command(command, schema, params, query_estimate))
                .collect::<Result<Vec<_>>>()
                .map(HirTriggerPlan::Program),
        }
    }

    fn plan_trigger_command(
        &self,
        command: &hir::TriggerCommand,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirTriggerCommandPlan> {
        match command {
            hir::TriggerCommand::Select(query) => Ok(HirTriggerCommandPlan::Select(*query)),
            hir::TriggerCommand::Insert(_) => Ok(HirTriggerCommandPlan::Insert),
            hir::TriggerCommand::Update(update) => self
                .plan_update(update, schema, params, query_estimate)
                .map(HirTriggerCommandPlan::Update),
            hir::TriggerCommand::Delete(delete) => self
                .plan_delete(delete, schema, params, query_estimate)
                .map(HirTriggerCommandPlan::Delete),
        }
    }

    fn plan_update(
        &self,
        update: &hir::Update,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirAccessPlan> {
        let usage = update.direct_column_usage(|source| self.document.source(source));
        let mut input = HirQueryBlockPlanInput {
            sources: vec![self.plan_source(update.target, None, &usage)?],
            groups: Vec::new(),
            predicates: Vec::new(),
        };
        if let Some(from) = &update.from {
            self.append_from(from, HirFromContext::default(), &mut input, &usage)?;
        }
        if let Some(predicate) = &update.predicate {
            append_predicates(&mut input.predicates, predicate, None);
        }

        self.plan_source_access(input, None, 1.0, schema, params, query_estimate)?
            .ok_or_else(|| {
                LimboError::InternalError("UPDATE target produced no access plan".to_string())
            })
    }

    fn plan_delete(
        &self,
        delete: &hir::Delete,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirAccessPlan> {
        let usage = delete.direct_column_usage();
        let mut input = HirQueryBlockPlanInput {
            sources: vec![self.plan_source(delete.target, None, &usage)?],
            groups: Vec::new(),
            predicates: Vec::new(),
        };
        if let Some(predicate) = &delete.predicate {
            append_predicates(&mut input.predicates, predicate, None);
        }
        for term in &delete.order_by {
            self.register_expression_index_usage(&mut input.sources, &term.expr)?;
        }
        if let Some(returning) = &delete.returning {
            for output in &returning.outputs {
                self.register_expression_index_usage(&mut input.sources, &output.expr)?;
            }
        }

        let order_target = self.order_target(
            &delete.order_by,
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
        );
        self.plan_source_access(
            input,
            order_target.as_ref(),
            1.0,
            schema,
            params,
            query_estimate,
        )?
        .ok_or_else(|| {
            LimboError::InternalError("DELETE target produced no access plan".to_string())
        })
    }

    /// Plan query-backed dependencies before their owning query so source
    /// scans can use the child's output and work estimates.
    fn plan_query_tree(
        &self,
        query_id: QueryId,
        schema: &Schema,
        params: &CostModelParams,
        planned: &mut Vec<HirPlannedQuery>,
        active: &mut HashSet<QueryId>,
    ) -> Result<HirQueryEstimate> {
        if let Some(plan) = planned.iter().find(|plan| plan.query == query_id) {
            return Ok(HirQueryEstimate {
                output_cardinality: plan.output_cardinality,
                cost: plan.cost,
            });
        }
        if !active.insert(query_id) {
            return Err(LimboError::InternalError(format!(
                "HIR query dependency cycle at {query_id}"
            )));
        }

        let query = self
            .document
            .query(query_id)
            .expect("validated HIR query exists");
        for dependency in self.document.direct_query_dependencies(query_id) {
            self.plan_query_tree(dependency, schema, params, planned, active)?;
        }

        let block_order_by = query
            .compounds
            .is_empty()
            .then_some(query.order_by.as_slice())
            .unwrap_or_default();
        let query_estimate = |query| {
            planned
                .iter()
                .find(|plan| plan.query == query)
                .map(|plan| HirQueryEstimate {
                    output_cardinality: plan.output_cardinality,
                    cost: plan.cost,
                })
        };
        let blocks = query
            .blocks
            .iter()
            .map(|block| {
                self.plan_query_block(block, block_order_by, 1.0, schema, params, &query_estimate)
            })
            .collect::<Result<Vec<_>>>()?;
        let estimate = query_estimate_from_blocks(query, &blocks, params);
        planned.push(HirPlannedQuery {
            query: query_id,
            blocks,
            output_cardinality: estimate.output_cardinality,
            cost: estimate.cost,
        });
        active.remove(&query_id);
        Ok(estimate)
    }

    /// Build planner source metadata without resolving names, allocating a
    /// second identity, or reading the catalog again.
    pub(crate) fn source(
        &self,
        source: SourceId,
        join_info: Option<HirJoinInfo>,
        usage: &[ColumnUsage],
    ) -> Result<HirPlannedSource> {
        planned_source_from_definition(self.definition(source), join_info, usage)
    }

    fn cte_materialization(
        &self,
        owner: QueryId,
        cte: hir::CteId,
        query: QueryId,
    ) -> HirCteMaterialization {
        let definition = self
            .document
            .cte(cte)
            .expect("validated HIR contains referenced CTE");
        if matches!(
            definition.materialized,
            turso_parser::ast::Materialized::Yes
        ) {
            return HirCteMaterialization::Explicit;
        }

        if self.cte_reference_count(owner, cte) > 1 && !self.query_tree_has_outer_dependency(query)
        {
            HirCteMaterialization::Shared
        } else {
            HirCteMaterialization::PerReference
        }
    }

    /// Count references in one query plan. A CTE body is a separate plan tree:
    /// when reached through a CTE source, its nested references are classified
    /// while that body is planned instead of being multiplied by its callers.
    fn cte_reference_count(&self, root: QueryId, target: hir::CteId) -> usize {
        self.document
            .queries
            .iter()
            .filter(|query| self.query_belongs_to_tree(query.id, root, true))
            .flat_map(|query| &query.blocks)
            .filter_map(|block| block.from.as_ref())
            .map(|from| self.count_cte_references_in_from(from, target))
            .sum()
    }

    fn count_cte_references_in_from(&self, from: &hir::From, target: hir::CteId) -> usize {
        core::iter::once(from.first)
            .chain(from.joins.iter().map(|join| join.right))
            .map(|source| self.count_cte_references_in_source(source, target))
            .sum()
    }

    fn count_cte_references_in_source(&self, source: SourceId, target: hir::CteId) -> usize {
        match &self.definition(source).kind {
            hir::SourceKind::Cte(cte) => usize::from(*cte == target),
            hir::SourceKind::FromGroup(group) => {
                self.count_cte_references_in_from(&group.from, target)
            }
            _ => 0,
        }
    }

    fn query_tree_has_outer_dependency(&self, root: QueryId) -> bool {
        let queries = self
            .document
            .queries
            .iter()
            .filter(|query| self.query_belongs_to_tree(query.id, root, false))
            .map(|query| query.id)
            .collect::<HashSet<_>>();

        self.document.queries.iter().any(|query| {
            queries.contains(&query.id)
                && query.captures.iter().any(|source| {
                    let source = self.definition(*source);
                    match source.owner {
                        hir::SourceOwner::QueryBlock(block) => !queries.contains(&block.query),
                        hir::SourceOwner::Cte(cte) => !self.cte_belongs_to_queries(cte, &queries),
                        hir::SourceOwner::Root => true,
                    }
                })
        })
    }

    fn query_belongs_to_tree(
        &self,
        mut query: QueryId,
        root: QueryId,
        stop_at_cte_body: bool,
    ) -> bool {
        loop {
            if query == root {
                return true;
            }
            if stop_at_cte_body && self.is_cte_body_query(query) {
                return false;
            }
            let Some(parent) = self
                .document
                .query(query)
                .expect("validated HIR query exists")
                .parent
            else {
                return false;
            };
            query = parent;
        }
    }

    fn is_cte_body_query(&self, query: QueryId) -> bool {
        self.document.ctes.iter().any(|cte| match &cte.body {
            hir::CteBody::Query(body) => *body == query,
            hir::CteBody::Recursive(recursive) => {
                recursive.seed == query || recursive.arms.iter().any(|arm| arm.query == query)
            }
        })
    }

    fn cte_belongs_to_queries(&self, cte: hir::CteId, queries: &HashSet<QueryId>) -> bool {
        let definition = self
            .document
            .cte(cte)
            .expect("validated HIR contains captured CTE source");
        match &definition.body {
            hir::CteBody::Query(query) => queries.contains(query),
            hir::CteBody::Recursive(recursive) => {
                queries.contains(&recursive.seed)
                    || recursive
                        .arms
                        .iter()
                        .any(|arm| queries.contains(&arm.query))
            }
        }
    }

    fn plan_source(
        &self,
        source: SourceId,
        join_info: Option<HirJoinInfo>,
        usage: &[ColumnUsage],
    ) -> Result<HirPlanSource> {
        match &self.definition(source).kind {
            hir::SourceKind::Table(_) | hir::SourceKind::TableFunction { .. } => self
                .source(source, join_info, usage)
                .map(HirPlanSource::BTree),
            hir::SourceKind::Derived(query) => Ok(HirPlanSource::Derived {
                source,
                query: *query,
                join_info,
            }),
            hir::SourceKind::Cte(cte) => {
                let definition = self
                    .document
                    .cte(*cte)
                    .expect("validated HIR contains referenced CTE");
                match &definition.body {
                    hir::CteBody::Query(query) => {
                        let owner = match self.definition(source).owner {
                            hir::SourceOwner::QueryBlock(block) => block.query,
                            owner => {
                                return Err(LimboError::InternalError(format!(
                                    "CTE HIR source {source} has non-query owner {owner:?}"
                                )));
                            }
                        };
                        Ok(HirPlanSource::Cte {
                            source,
                            cte: *cte,
                            query: *query,
                            materialization: self.cte_materialization(owner, *cte, *query),
                            join_info,
                        })
                    }
                    hir::CteBody::Recursive(_) => Ok(HirPlanSource::RecursiveCte {
                        source,
                        cte: *cte,
                        join_info,
                    }),
                }
            }
            hir::SourceKind::RecursiveInput(cte) => Ok(HirPlanSource::RecursiveInput {
                source,
                cte: *cte,
                join_info,
            }),
            _ => Err(LimboError::InternalError(format!(
                "source {source} cannot be planned as a query source"
            ))),
        }
    }

    fn from_item(&self, source: SourceId) -> HirFromItem<'_> {
        match &self.definition(source).kind {
            hir::SourceKind::FromGroup(group) => HirFromItem::Group {
                source,
                from: &group.from,
            },
            _ => HirFromItem::Source(source),
        }
    }

    fn append_from(
        &self,
        from: &hir::From,
        context: HirFromContext,
        input: &mut HirQueryBlockPlanInput,
        usage: &[ColumnUsage],
    ) -> Result<()> {
        self.append_from_item(from.first, None, context, input, usage)?;

        for join in &from.joins {
            let local_outer_join = match join.kind {
                hir::JoinKind::Left | hir::JoinKind::Right | hir::JoinKind::Full => {
                    Some(join.right)
                }
                hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross => None,
            };
            let predicate_owner = local_outer_join.or(context.outer_join);
            self.append_from_item(
                join.right,
                Some(HirJoinInfo { kind: join.kind }),
                HirFromContext {
                    outer_join: predicate_owner,
                    ..context
                },
                input,
                usage,
            )?;
            self.append_join_predicates(&mut input.predicates, join, predicate_owner);
        }
        Ok(())
    }

    fn append_from_item(
        &self,
        source: SourceId,
        join_info: Option<HirJoinInfo>,
        context: HirFromContext,
        input: &mut HirQueryBlockPlanInput,
        usage: &[ColumnUsage],
    ) -> Result<()> {
        match self.from_item(source) {
            HirFromItem::Source(source) => {
                input
                    .sources
                    .push(self.plan_source(source, join_info, usage)?);
                self.append_table_function_predicates(
                    &mut input.predicates,
                    source,
                    context.outer_join,
                );
            }
            HirFromItem::Group { source, from } => {
                let start = input.sources.len();
                self.append_from(
                    from,
                    HirFromContext {
                        parent_group: Some(source),
                        ..context
                    },
                    input,
                    usage,
                )?;
                input.groups.push(HirFromGroupBoundary {
                    source,
                    parent: context.parent_group,
                    source_range: start..input.sources.len(),
                    join_info,
                });
            }
        }
        Ok(())
    }

    fn append_join_predicates(
        &self,
        predicates: &mut Vec<HirWhereTerm>,
        join: &hir::Join,
        from_outer_join: Option<SourceId>,
    ) {
        match &join.constraint {
            hir::JoinConstraint::None => {}
            hir::JoinConstraint::On(expression) => {
                append_predicates(predicates, expression, from_outer_join)
            }
            hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                for column in columns {
                    predicates.push(HirWhereTerm {
                        expr: using_equality(column),
                        from_outer_join,
                        consumed: false,
                    });
                }
            }
        }
    }

    /// Assemble planner input from resolved HIR without repeating binding.
    pub(crate) fn query_block_input(
        &self,
        block: &hir::QueryBlock,
        order_by: &[hir::OrderTerm],
        usage: &[ColumnUsage],
    ) -> Result<HirQueryBlockPlanInput> {
        let mut input = HirQueryBlockPlanInput {
            sources: Vec::new(),
            groups: Vec::new(),
            predicates: Vec::new(),
        };

        if let Some(from) = &block.from {
            self.append_from(from, HirFromContext::default(), &mut input, usage)?;
        }

        if let hir::QueryBlockBody::Select {
            filter: Some(filter),
            ..
        } = &block.body
        {
            append_predicates(&mut input.predicates, filter, None);
        }

        for output in &block.outputs {
            self.register_expression_index_usage(&mut input.sources, &output.expr)?;
        }
        if let hir::QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } = &block.body
        {
            for key in &grouping.keys {
                self.register_expression_index_usage(&mut input.sources, key)?;
            }
            if let Some(having) = &grouping.having {
                self.register_expression_index_usage(&mut input.sources, having)?;
            }
        }
        for term in order_by {
            self.register_expression_index_usage(&mut input.sources, &term.expr)?;
        }

        Ok(input)
    }

    fn append_table_function_predicates(
        &self,
        predicates: &mut Vec<HirWhereTerm>,
        source: SourceId,
        from_outer_join: Option<SourceId>,
    ) {
        let hir::SourceKind::TableFunction {
            argument_predicates,
            ..
        } = &self.definition(source).kind
        else {
            return;
        };
        predicates.extend(
            argument_predicates
                .iter()
                .cloned()
                .map(|expr| HirWhereTerm {
                    expr,
                    from_outer_join,
                    consumed: false,
                }),
        );
    }

    /// Collect physical constraints for every source while keeping the source
    /// and predicate order established by `query_block_input`.
    pub(crate) fn constraints(
        &self,
        input: &HirQueryBlockPlanInput,
        base_rows: &[RowCountEstimate],
        schema: &Schema,
        params: &CostModelParams,
        query_rows: &dyn Fn(QueryId) -> Option<f64>,
    ) -> Result<Vec<HirTableConstraints>> {
        assert_eq!(
            input.sources.len(),
            base_rows.len(),
            "every HIR plan source must have a base-row estimate"
        );
        let layout = HirFromLayout::new(&input.sources, &input.groups);
        input
            .sources
            .iter()
            .zip(base_rows)
            .map(|(source, row_count)| {
                let source = match source {
                    HirPlanSource::BTree(source) => HirConstraintSource::BTree(source),
                    HirPlanSource::Derived { source, .. } => HirConstraintSource::Derived {
                        source: *source,
                        row_count: *row_count,
                    },
                    HirPlanSource::Cte { source, .. } => HirConstraintSource::Cte {
                        source: *source,
                        row_count: *row_count,
                    },
                    HirPlanSource::RecursiveCte { source, .. }
                    | HirPlanSource::RecursiveInput { source, .. } => HirConstraintSource::Cte {
                        source: *source,
                        row_count: *row_count,
                    },
                };
                hir_constraints_for_source(
                    self.document,
                    &layout,
                    &input.predicates,
                    source,
                    schema,
                    params,
                    query_rows,
                )
            })
            .collect()
    }

    /// Plan one resolved query block without exposing mutable planner input.
    #[allow(clippy::too_many_arguments)]
    fn plan_query_block(
        &self,
        block: &hir::QueryBlock,
        order_by: &[hir::OrderTerm],
        initial_cardinality: f64,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirQueryBlockPlan> {
        let query = self
            .document
            .query(block.id.query)
            .expect("validated HIR query block has owning query");
        let usage = query.direct_column_usage(|source| self.document.source(source));
        if block.from.is_none() {
            let input = self.query_block_input(block, order_by, &usage)?;
            let output_cardinality = match &block.body {
                hir::QueryBlockBody::Values { rows } => initial_cardinality * rows.len() as f64,
                hir::QueryBlockBody::Select { .. } => initial_cardinality,
            };
            return Ok(HirQueryBlockPlan {
                block: block.id,
                groups: input.groups,
                loops: Vec::new(),
                predicates: input.predicates,
                group_sort_elided: false,
                output_cardinality,
                cost: Cost(0.0),
            });
        }

        let input = self.query_block_input(block, order_by, &usage)?;
        let order_target = if order_by.is_empty() {
            self.grouping_target(block)
        } else {
            self.order_target(
                order_by,
                OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
            )
        };
        let access = self
            .plan_source_access(
                input,
                order_target.as_ref(),
                initial_cardinality,
                schema,
                params,
                query_estimate,
            )?
            .ok_or_else(|| {
                LimboError::InternalError(
                    "query block with FROM produced no B-tree access plan".to_string(),
                )
            })?;
        Ok(HirQueryBlockPlan {
            block: block.id,
            groups: access.groups,
            loops: access.loops,
            predicates: access.predicates,
            group_sort_elided: access.group_sort_elided,
            output_cardinality: access.output_cardinality,
            cost: access.cost,
        })
    }

    /// Choose source access methods and a complete join order directly from
    /// resolved HIR. This keeps the planning boundary free of parser-era table
    /// identities, synthetic subquery tables, and bound AST expressions.
    #[allow(clippy::too_many_arguments)]
    fn plan_source_access(
        &self,
        mut input: HirQueryBlockPlanInput,
        order_target: Option<&HirOrderTarget<'_>>,
        initial_cardinality: f64,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<Option<HirAccessPlan>> {
        let query_rows = |query| query_estimate(query).map(|estimate| estimate.output_cardinality);
        let mut base_rows = Vec::with_capacity(input.sources.len());
        let mut source_costs = Vec::with_capacity(input.sources.len());
        for source in &input.sources {
            match source {
                HirPlanSource::BTree(source) => {
                    base_rows.push(base_row_estimate(schema, &source.table, params));
                    source_costs.push(Cost(0.0));
                }
                HirPlanSource::Derived { source, query, .. } => {
                    let child = query_estimate(*query).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "derived HIR source {source} references unplanned query {query}"
                        ))
                    })?;
                    base_rows.push(RowCountEstimate::AnalyzeStats(
                        child.output_cardinality.max(1.0),
                    ));
                    source_costs.push(child.cost);
                }
                HirPlanSource::Cte {
                    source, cte, query, ..
                } => {
                    let child = query_estimate(*query).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "CTE HIR source {source} for {cte} references unplanned query {query}"
                        ))
                    })?;
                    base_rows.push(RowCountEstimate::AnalyzeStats(
                        child.output_cardinality.max(1.0),
                    ));
                    source_costs.push(child.cost);
                }
                HirPlanSource::RecursiveCte { .. } => {
                    base_rows.push(RowCountEstimate::hardcoded_fallback(params));
                    source_costs.push(Cost(0.0));
                }
                HirPlanSource::RecursiveInput { .. } => {
                    base_rows.push(RowCountEstimate::AnalyzeStats(1.0));
                    source_costs.push(Cost(0.0));
                }
            }
        }
        let constraints = self.constraints(&input, &base_rows, schema, params, &query_rows)?;
        let mut access_methods = Vec::new();
        let result = compute_hir_greedy_join_order(
            self.document,
            &input.sources,
            &input.groups,
            &constraints,
            &input.predicates,
            &base_rows,
            &source_costs,
            order_target,
            initial_cardinality,
            &mut access_methods,
            schema,
            &schema.analyze_stats,
            params,
        )?;

        let Some(result) = result else {
            return Ok(None);
        };
        let group_sort_elided = if order_target.is_some_and(|target| {
            matches!(
                &target.purpose,
                OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Group)
            )
        }) {
            hir_plan_satisfies_order_target(
                &result.best_plan,
                &access_methods,
                &input.sources,
                self.document,
                order_target.expect("GROUP BY order target exists"),
                schema,
            )?
        } else {
            false
        };
        let mut prior_sources = crate::translate::planner::TableMask::default();
        let mut loops = Vec::with_capacity(input.sources.len());
        for (source_position, access_method_position) in result.best_plan.data.iter().copied() {
            let access = match &input.sources[source_position] {
                HirPlanSource::BTree(source) => match &source.table {
                    Table::BTree(_) => {
                        let operation = apply_hir_selected_btree_access(
                            source,
                            &constraints[source_position],
                            &mut input.predicates,
                            &mut access_methods[access_method_position],
                            &prior_sources,
                            source_position,
                        )?;
                        let definition = self.definition(source.internal_id);
                        let table_lookup = selected_btree_index(&operation)
                            .filter(|index| !source.index_is_covering(definition, index))
                            .map_or(BtreeTableLookup::ScanOnly, |_| {
                                BtreeTableLookup::TableRequired
                            });
                        HirSourceAccess::BTree {
                            operation,
                            table_lookup,
                        }
                    }
                    Table::Virtual(_) => {
                        HirSourceAccess::Virtual(apply_hir_selected_virtual_access(
                            &constraints[source_position],
                            &mut input.predicates,
                            &access_methods[access_method_position],
                        )?)
                    }
                    _ => {
                        return Err(LimboError::InternalError(format!(
                            "HIR catalog source {} is not a table",
                            source.internal_id
                        )))
                    }
                },
                HirPlanSource::Derived { query, .. } => {
                    if !matches!(
                        access_methods[access_method_position].params,
                        super::optimizer::access_method::AccessMethodParams::Subquery { .. }
                    ) {
                        return Err(LimboError::InternalError(
                            "derived HIR source selected a non-subquery access method".to_string(),
                        ));
                    }
                    HirSourceAccess::Derived { query: *query }
                }
                HirPlanSource::Cte {
                    cte,
                    query,
                    materialization,
                    ..
                } => {
                    if !matches!(
                        access_methods[access_method_position].params,
                        super::optimizer::access_method::AccessMethodParams::Subquery { .. }
                    ) {
                        return Err(LimboError::InternalError(
                            "CTE HIR source selected a non-subquery access method".to_string(),
                        ));
                    }
                    HirSourceAccess::Cte {
                        cte: *cte,
                        query: *query,
                        materialization: *materialization,
                    }
                }
                HirPlanSource::RecursiveCte { cte, .. } => {
                    if !matches!(
                        access_methods[access_method_position].params,
                        super::optimizer::access_method::AccessMethodParams::Subquery { .. }
                    ) {
                        return Err(LimboError::InternalError(
                            "recursive CTE HIR source selected a non-subquery access method"
                                .to_string(),
                        ));
                    }
                    HirSourceAccess::RecursiveCte { cte: *cte }
                }
                HirPlanSource::RecursiveInput { cte, .. } => {
                    if !matches!(
                        access_methods[access_method_position].params,
                        super::optimizer::access_method::AccessMethodParams::Subquery { .. }
                    ) {
                        return Err(LimboError::InternalError(
                            "recursive CTE input selected a non-subquery access method".to_string(),
                        ));
                    }
                    HirSourceAccess::RecursiveInput { cte: *cte }
                }
            };
            loops.push(HirPlannedLoop {
                source: input.sources[source_position].source(),
                source_position,
                access,
            });
            prior_sources.set(source_position)?;
        }

        Ok(Some(HirAccessPlan {
            groups: input.groups,
            loops,
            predicates: input.predicates,
            group_sort_elided,
            output_cardinality: result.best_plan.output_cardinality,
            cost: result.best_plan.cost,
        }))
    }

    /// Build an ordering requirement from resolved HIR terms without reopening
    /// name resolution or copying expressions out of the document.
    pub(crate) fn order_target<'term>(
        &self,
        terms: &'term [hir::OrderTerm],
        purpose: OrderTargetPurpose,
    ) -> Option<HirOrderTarget<'term>>
    where
        'a: 'term,
    {
        let columns = terms
            .iter()
            .map(|term| self.order_column(term))
            .collect::<Option<Vec<_>>>()?;
        (!columns.is_empty()).then_some(OrderTarget { columns, purpose })
    }

    fn grouping_target<'term>(
        &self,
        block: &'term hir::QueryBlock,
    ) -> Option<HirOrderTarget<'term>>
    where
        'a: 'term,
    {
        let hir::QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } = &block.body
        else {
            return None;
        };
        let columns = grouping
            .keys
            .iter()
            .zip(&grouping.key_collations)
            .map(|(expression, collation)| {
                self.ordering_column(
                    expression,
                    turso_parser::ast::SortOrder::Asc,
                    collation
                        .as_ref()
                        .map(|collation| *collation.value())
                        .unwrap_or_default(),
                    None,
                )
            })
            .collect::<Option<Vec<_>>>()?;
        (!columns.is_empty()).then_some(OrderTarget {
            columns,
            purpose: OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Group),
        })
    }

    fn order_column<'term>(
        &self,
        term: &'term hir::OrderTerm,
    ) -> Option<ColumnOrder<SourceId, &'term hir::Expr>>
    where
        'a: 'term,
    {
        self.ordering_column(
            &term.expr,
            term.order,
            term.collation
                .as_ref()
                .map(|collation| *collation.value())
                .unwrap_or_default(),
            term.nulls,
        )
    }

    fn ordering_column<'term>(
        &self,
        mut expression: &'term hir::Expr,
        order: turso_parser::ast::SortOrder,
        collation: crate::translate::collate::CollationSeq,
        nulls_order: Option<turso_parser::ast::NullsOrder>,
    ) -> Option<ColumnOrder<SourceId, &'term hir::Expr>>
    where
        'a: 'term,
    {
        while let hir::Expr::Output(output) = expression {
            expression = &self.document.output(*output)?.expr;
        }

        let (source, target) = match expression {
            hir::Expr::Column(column) => (column.source, ColumnTarget::Column(column.column)),
            hir::Expr::RowId(source) => (*source, ColumnTarget::RowId),
            expression => {
                let (source, _) = expression.single_source_column_usage()?;
                (source, ColumnTarget::Expr(expression))
            }
        };

        Some(ColumnOrder {
            source,
            target,
            order,
            collation,
            nulls_order,
        })
    }

    fn register_expression_index_usage(
        &self,
        sources: &mut [HirPlanSource],
        expression: &hir::Expr,
    ) -> Result<()> {
        let Some((source, columns)) = expression.single_source_column_usage() else {
            return Ok(());
        };
        let Some(planned) = sources
            .iter_mut()
            .find(|planned| planned.source() == source)
            .and_then(HirPlanSource::btree_mut)
        else {
            // Outer-query reads are planned by their owning query.
            return Ok(());
        };
        let has_expression_key = self
            .definition(source)
            .index_expressions
            .iter()
            .any(|index| index.columns.iter().any(Option::is_some));
        if !has_expression_key {
            return Ok(());
        }

        let mut columns_mask = ColumnUsedMask::default();
        for column in columns {
            columns_mask.set(column)?;
        }
        planned.register_expression_index_usage(expression.clone(), columns_mask);
        Ok(())
    }
}

fn selected_btree_index(operation: &HirBtreeOperation) -> Option<&Index> {
    match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_deref(),
        HirBtreeOperation::RowidEq { .. } => None,
    }
}

fn query_estimate_from_blocks(
    query: &hir::Query,
    blocks: &[HirQueryBlockPlan],
    params: &CostModelParams,
) -> HirQueryEstimate {
    let fallback = *RowCountEstimate::hardcoded_fallback(params);
    let block_rows = |block| {
        blocks
            .iter()
            .find(|plan| plan.block == block)
            .map_or(fallback, |plan| plan.output_cardinality)
    };
    let mut output_cardinality = block_rows(query.first);
    for arm in &query.compounds {
        let rhs = block_rows(arm.block);
        output_cardinality = match arm.operator {
            turso_parser::ast::CompoundOperator::Union
            | turso_parser::ast::CompoundOperator::UnionAll => output_cardinality + rhs,
            turso_parser::ast::CompoundOperator::Except => output_cardinality,
            turso_parser::ast::CompoundOperator::Intersect => output_cardinality.min(rhs),
        };
    }

    HirQueryEstimate {
        output_cardinality,
        cost: blocks
            .iter()
            .fold(Cost(0.0), |cost, block| cost + block.cost),
    }
}

fn append_predicates(
    predicates: &mut Vec<HirWhereTerm>,
    expression: &hir::Expr,
    from_outer_join: Option<SourceId>,
) {
    predicates.extend(expression.conjuncts().map(|expression| HirWhereTerm {
        expr: expression.clone(),
        from_outer_join,
        consumed: false,
    }));
}

fn using_equality(column: &hir::UsingColumn) -> hir::Expr {
    hir::Expr::Binary {
        lhs: column.left.clone(),
        operator: turso_parser::ast::Operator::Equals,
        rhs: Box::new(hir::Expr::Column(column.right)),
        array_concat: false,
        custom: None,
        comparison: Some(column.comparison.clone()),
    }
}

fn planned_source_from_definition(
    source: &hir::Source,
    join_info: Option<HirJoinInfo>,
    usage: &[ColumnUsage],
) -> Result<HirPlannedSource> {
    let table = match &source.kind {
        hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => table,
        _ => {
            return Err(LimboError::InternalError(format!(
                "source {} is not a table source",
                source.id
            )))
        }
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!("table source {} has no database", source.id))
    })?;
    let (col_used_mask, column_use_counts) = column_usage(source.id, usage)?;

    Ok(HirPlannedSource {
        op: (),
        table: table.value().clone(),
        identifier: (),
        internal_id: source.id,
        join_info,
        col_used_mask,
        column_use_counts,
        expression_index_usages: Vec::new(),
        database_id: database.index(),
        indexed: source.index_hint.clone(),
    })
}

fn column_usage(source: SourceId, usage: &[ColumnUsage]) -> Result<(ColumnUsedMask, Vec<usize>)> {
    let mut mask = ColumnUsedMask::default();
    let mut counts = Vec::new();
    for usage in usage
        .iter()
        .filter(|usage| usage.reference.source == source)
    {
        mask.set(usage.reference.column)?;
        if counts.len() <= usage.reference.column {
            counts.resize(usage.reference.column + 1, 0);
        }
        counts[usage.reference.column] = usage.count;
    }
    Ok((mask, counts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, Index, Schema, Table, Type},
        sync::Arc,
        translate::collate::CollationSeq,
        translate::{
            optimizer::cost::RowCountEstimate,
            semantic::hir::{
                CatalogObject, CatalogObjectId, CatalogSnapshot, ColumnReadExpression,
                ComparisonComponent, ComparisonSemantics, CompoundArm, Cte, CteBody, CteColumn,
                CteId, DatabaseId, DatabaseSnapshot, IndexCoverage, Join, JoinConstraint, JoinKind,
                OrderTerm, Output, OutputId, Query, QueryBlock, QueryBlockBody, QueryBlockId,
                QueryId, QueryRoot, RecursiveArm, RecursiveCte, SourceColumn, SourceKind,
                SourceOwner, SubqueryExpr, TypeFact, UsingColumn,
            },
        },
        vdbe::affinity::Affinity,
    };
    use turso_parser::ast::{CompoundOperator, Materialized, NullsOrder, Operator, SortOrder};

    fn resolved_table(name: &str) -> hir::ResolvedTable {
        let columns = vec![
            Column::new(
                Some("first".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            ),
            Column::new(
                Some("second".to_string()),
                "TEXT".to_string(),
                None,
                None,
                Type::Text,
                None,
                ColDef::default(),
            ),
        ];
        let table = Table::BTree(Arc::new(BTreeTable::new(
            2,
            name.to_string(),
            vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        )));
        CatalogObject::new(
            CatalogObjectId::new(10),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(table),
        )
    }

    fn source() -> hir::Source {
        hir::Source {
            id: SourceId::new(0),
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(0)),
            name: "items".to_string(),
            alias: Some("i".to_string()),
            kind: SourceKind::Table(resolved_table("items")),
            columns: vec![
                SourceColumn {
                    name: "first".to_string(),
                    type_fact: TypeFact::known(Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: false,
                },
                SourceColumn {
                    name: "second".to_string(),
                    type_fact: TypeFact::known(Type::Text),
                    affinity: Affinity::Text,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: false,
                },
            ],
            generated_expressions: vec![ColumnReadExpression::Absent; 2],
            default_expressions: vec![ColumnReadExpression::Absent; 2],
            column_type_programs: vec![None; 2],
            check_constraints: None,
            rowid_available: true,
            index_hint: hir::IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    fn source_with_id(id: usize, name: &str) -> hir::Source {
        let mut source = source();
        source.id = SourceId::new(id);
        source.name = name.to_string();
        source.alias = None;
        source.kind = SourceKind::Table(resolved_table(name));
        source
    }

    fn group_source(id: usize, from: hir::From) -> hir::Source {
        hir::Source {
            id: SourceId::new(id),
            owner: SourceOwner::Root,
            database: None,
            name: format!("group-{id}"),
            alias: None,
            kind: SourceKind::FromGroup(hir::FromGroup {
                from: Box::new(from),
                columns: Vec::new(),
            }),
            columns: Vec::new(),
            generated_expressions: Vec::new(),
            default_expressions: Vec::new(),
            column_type_programs: Vec::new(),
            check_constraints: None,
            rowid_available: false,
            index_hint: hir::IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    fn document(sources: Vec<hir::Source>) -> hir::HirDocument {
        hir::HirDocument {
            snapshot: CatalogSnapshot::from_id(1),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source: sources[0].id,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources,
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        }
    }

    fn add_query(document: &mut hir::HirDocument, block: &QueryBlock) {
        let query = block.id.query;
        document.root = hir::HirRoot::Query(QueryRoot { query });
        document.queries.push(Query {
            id: query,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![block.clone()],
            first: block.id,
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: block.outputs.iter().map(|output| output.id).collect(),
        });
    }

    fn binary(lhs: hir::Expr, operator: Operator, rhs: hir::Expr) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(lhs),
            operator,
            rhs: Box::new(rhs),
            array_concat: false,
            custom: None,
            comparison: None,
        }
    }

    #[test]
    fn hir_order_target_resolves_outputs_and_borrows_expressions() {
        let source = SourceId::new(0);
        let query = QueryId::new(0);
        let block_id = QueryBlockId::new(query, 0);
        let output_id = OutputId::query(block_id, 0);
        let mut block = QueryBlock::new(
            block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.outputs.push(Output {
            id: output_id,
            name: "first".to_string(),
            expr: hir::Expr::column(source, 0),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let mut document = document(vec![source_with_id(0, "items")]);
        document.root = hir::HirRoot::Query(QueryRoot { query });
        document.queries.push(Query {
            id: query,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![block],
            first: block_id,
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: vec![output_id],
        });

        let computed = binary(
            hir::Expr::column(source, 0),
            Operator::Add,
            hir::Expr::column(source, 1),
        );
        let collation = CatalogObject::new(
            CatalogObjectId::new(20),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(CollationSeq::NoCase),
        );
        let terms = [
            OrderTerm {
                expr: hir::Expr::output(output_id),
                order: SortOrder::Desc,
                nulls: Some(NullsOrder::First),
                type_fact: TypeFact::known(Type::Integer),
                collation: None,
            },
            OrderTerm {
                expr: computed,
                order: SortOrder::Asc,
                nulls: None,
                type_fact: TypeFact::known(Type::Integer),
                collation: Some(collation),
            },
        ];
        let target = HirPlanContext::new(&document)
            .order_target(
                &terms,
                OrderTargetPurpose::EliminatesSort(
                    crate::translate::optimizer::order::EliminatesSortBy::Order,
                ),
            )
            .expect("resolved terms form one-source order target");

        assert!(matches!(target.columns[0].target, ColumnTarget::Column(0)));
        assert_eq!(target.columns[0].source, source);
        assert_eq!(target.columns[0].order, SortOrder::Desc);
        assert_eq!(target.columns[0].nulls_order, Some(NullsOrder::First));
        let ColumnTarget::Expr(expression) = target.columns[1].target else {
            panic!("computed HIR order term remains an expression");
        };
        assert!(std::ptr::eq(expression, &terms[1].expr));
        assert_eq!(target.columns[1].collation, CollationSeq::NoCase);
    }

    #[test]
    fn hir_source_reuses_identity_and_table_metadata() {
        let source = source();
        let SourceKind::Table(resolved) = &source.kind else {
            unreachable!();
        };
        let document = hir::HirDocument {
            snapshot: CatalogSnapshot::from_id(1),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source: source.id,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources: vec![source.clone()],
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        };
        let context = HirPlanContext::new(&document);
        let joined = context
            .source(
                source.id,
                Some(HirJoinInfo {
                    kind: hir::JoinKind::Inner,
                }),
                &[],
            )
            .expect("table source converts");

        assert_eq!(joined.internal_id, source.id);
        assert_eq!(
            joined.join_info.as_ref().unwrap().kind,
            hir::JoinKind::Inner
        );
        assert_eq!(joined.database_id, 0);
        let (Table::BTree(expected), Table::BTree(actual)) = (resolved.value(), &joined.table)
        else {
            panic!("resolved and planned tables are btree tables");
        };
        assert!(Arc::ptr_eq(expected, actual));
    }

    #[test]
    fn table_source_uses_hir_column_counts() {
        let source = source();
        let usage = [ColumnUsage {
            reference: hir::ColumnRef {
                source: source.id,
                column: 1,
            },
            count: 3,
        }];
        let joined =
            planned_source_from_definition(&source, None, &usage).expect("table source converts");

        assert_eq!(
            (&joined.col_used_mask).into_iter().collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(joined.column_use_counts, vec![0, 3]);
    }

    #[test]
    fn table_source_preserves_index_hints_without_lookup() {
        let mut source = source();
        source.index_hint = hir::IndexHint::NotIndexed;
        let joined =
            planned_source_from_definition(&source, None, &[]).expect("table source converts");
        assert!(matches!(joined.indexed, hir::IndexHint::NotIndexed));

        let index = Index {
            name: "items_second".to_string(),
            table_name: "items".to_string(),
            root_page: 3,
            columns: Vec::new(),
            unique: false,
            ephemeral: false,
            has_rowid: true,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        };
        source.index_hint = hir::IndexHint::Indexed(CatalogObject::new(
            CatalogObjectId::new(11),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(index),
        ));
        let joined =
            planned_source_from_definition(&source, None, &[]).expect("table source converts");
        assert!(matches!(
            joined.indexed,
            hir::IndexHint::Indexed(index) if index.value().name == "items_second"
        ));
    }

    #[test]
    fn hir_covering_index_matches_frozen_expression_keys() {
        let mut source = source();
        let indexed_expression = binary(
            hir::Expr::column(source.id, 0),
            Operator::Add,
            hir::Expr::column(source.id, 1),
        );
        let index = CatalogObject::new(
            CatalogObjectId::new(11),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(Index {
                name: "items_sum".to_string(),
                table_name: "items".to_string(),
                root_page: 3,
                columns: Vec::new(),
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }),
        );
        source.index_expressions.push(hir::IndexExpressions {
            index: index.clone(),
            columns: vec![Some(indexed_expression.clone())],
            predicate: None,
        });

        let usage = [
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: source.id,
                    column: 0,
                },
                count: 1,
            },
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: source.id,
                    column: 1,
                },
                count: 1,
            },
        ];
        let block_id = QueryBlockId::new(QueryId::new(0), 0);
        let mut block = QueryBlock::new(
            block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: source.id,
            joins: Vec::new(),
        });
        block.outputs.push(hir::Output {
            id: hir::OutputId::query(block_id, 0),
            name: "sum".to_string(),
            expr: indexed_expression.clone(),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let order_by = [hir::OrderTerm {
            expr: indexed_expression,
            order: turso_parser::ast::SortOrder::Asc,
            nulls: None,
            type_fact: TypeFact::known(Type::Integer),
            collation: None,
        }];
        let document = document(vec![source.clone()]);
        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &order_by, &usage)
            .expect("query block input converts");
        let mut planned = input.sources.into_iter().next().expect("source is planned");
        let HirPlanSource::BTree(planned) = &mut planned else {
            panic!("table source carries B-tree planning metadata");
        };

        // Output and ORDER BY use the same expression, so registration deduplicates them.
        assert_eq!(planned.expression_index_usages.len(), 1);

        assert!(planned.index_is_covering(&source, index.value()));

        planned.expression_index_usages[0].normalized_expr =
            Box::new(hir::Expr::column(source.id, 0));
        assert!(!planned.index_is_covering(&source, index.value()));
    }

    #[test]
    fn non_table_source_is_rejected() {
        let mut source = source();
        source.kind = SourceKind::SchemaExpression;
        let error = planned_source_from_definition(&source, None, &[])
            .expect_err("schema namespace has no planner table");
        assert_eq!(
            error.to_string(),
            "Internal error: source s0 is not a table source"
        );
    }

    #[test]
    fn query_block_input_preserves_source_order_and_right_join_predicates() {
        let left = SourceId::new(0);
        let right = SourceId::new(1);
        let document = document(vec![
            source_with_id(0, "left_items"),
            source_with_id(1, "right_items"),
        ]);
        let on = binary(
            binary(
                hir::Expr::column(left, 0),
                Operator::Equals,
                hir::Expr::column(right, 0),
            ),
            Operator::And,
            binary(
                hir::Expr::column(left, 1),
                Operator::Equals,
                hir::Expr::column(right, 1),
            ),
        );
        let filter = binary(
            hir::Expr::column(right, 0),
            Operator::NotEquals,
            hir::Expr::column(right, 1),
        );
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(filter),
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: left,
            joins: vec![Join {
                right,
                kind: JoinKind::Right,
                constraint: JoinConstraint::On(on),
            }],
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("query block input converts");

        assert_eq!(
            input
                .sources
                .iter()
                .map(HirPlanSource::source)
                .collect::<Vec<_>>(),
            vec![left, right]
        );
        assert!(input.sources[0].join_info().is_none());
        let join = input.sources[1].join_info().unwrap();
        assert_eq!(join.kind, JoinKind::Right);
        assert_eq!(input.predicates.len(), 3);
        assert!(input.predicates[..2]
            .iter()
            .all(|predicate| predicate.from_outer_join == Some(right)));
        assert_eq!(input.predicates[2].from_outer_join, None);
        assert!(matches!(
            &input.predicates[0].expr,
            hir::Expr::Binary {
                operator: Operator::Equals,
                ..
            }
        ));
        assert!(matches!(
            &input.predicates[1].expr,
            hir::Expr::Binary {
                operator: Operator::Equals,
                ..
            }
        ));
        assert!(matches!(
            &input.predicates[2].expr,
            hir::Expr::Binary {
                operator: Operator::NotEquals,
                ..
            }
        ));
    }

    #[test]
    fn query_block_input_flattens_group_leaves_and_records_the_boundary() {
        let outer = SourceId::new(0);
        let inner_left = SourceId::new(1);
        let inner_right = SourceId::new(2);
        let group = SourceId::new(3);
        let inner_predicate = binary(
            hir::Expr::column(inner_left, 0),
            Operator::Equals,
            hir::Expr::column(inner_right, 0),
        );
        let outer_predicate = binary(
            hir::Expr::column(outer, 0),
            Operator::Equals,
            hir::Expr::column(inner_right, 0),
        );
        let group_from = hir::From {
            first: inner_left,
            joins: vec![Join {
                right: inner_right,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(inner_predicate),
            }],
        };
        let document = document(vec![
            source_with_id(0, "outer_items"),
            source_with_id(1, "inner_left"),
            source_with_id(2, "inner_right"),
            group_source(3, group_from),
        ]);
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: outer,
            joins: vec![Join {
                right: group,
                kind: JoinKind::Left,
                constraint: JoinConstraint::On(outer_predicate),
            }],
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("FROM group produces recursive planner input");

        assert_eq!(
            input
                .sources
                .iter()
                .map(HirPlanSource::source)
                .collect::<Vec<_>>(),
            vec![outer, inner_left, inner_right]
        );
        let [boundary] = input.groups.as_slice() else {
            panic!("one FROM-group boundary is recorded");
        };
        assert_eq!(boundary.source, group);
        assert_eq!(boundary.parent, None);
        assert_eq!(boundary.source_range, 1..3);
        assert_eq!(boundary.join_info.as_ref().unwrap().kind, JoinKind::Left);
        assert!(input.sources[1].join_info().is_none());
        assert_eq!(input.sources[2].join_info().unwrap().kind, JoinKind::Inner);
        assert_eq!(input.predicates.len(), 2);
        assert!(input
            .predicates
            .iter()
            .all(|predicate| predicate.from_outer_join == Some(group)));

        let layout = HirFromLayout::new(&input.sources, &input.groups);
        let mask = layout
            .table_mask(&document, &input.predicates[0].expr)
            .expect("group predicate has a physical source mask");
        assert!(!mask.get(0));
        assert!(mask.get(1));
        assert!(mask.get(2));
        assert!(!layout.outer_join_may_null_extend(outer));
        assert!(layout.outer_join_may_null_extend(inner_left));
        assert!(layout.outer_join_may_null_extend(inner_right));
        assert!(!layout.full_join_may_null_extend(inner_left));
    }

    #[test]
    fn owned_hir_plan_plans_parenthesized_from_group() {
        let outer = SourceId::new(0);
        let inner_left = SourceId::new(1);
        let inner_right = SourceId::new(2);
        let group = SourceId::new(3);
        let group_from = hir::From {
            first: inner_left,
            joins: vec![Join {
                right: inner_right,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            }],
        };
        let mut document = document(vec![
            source_with_id(0, "outer_items"),
            source_with_id(1, "inner_left"),
            source_with_id(2, "inner_right"),
            group_source(3, group_from),
        ]);
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: outer,
            joins: vec![Join {
                right: group,
                kind: JoinKind::Left,
                constraint: JoinConstraint::On(binary(
                    hir::Expr::column(outer, 0),
                    Operator::Equals,
                    hir::Expr::column(inner_right, 0),
                )),
            }],
        });
        add_query(&mut document, &block);

        let plan = HirPlan::build(
            Arc::new(document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("parenthesized FROM group plans from HIR");
        let [block_plan] = plan
            .planned_query(block.id.query)
            .expect("query is planned")
            .blocks
            .as_slice()
        else {
            panic!("query has one block");
        };

        assert_eq!(block_plan.loops.len(), 3);
        assert_eq!(block_plan.groups.len(), 1);
        assert_eq!(block_plan.groups[0].source, group);
        assert_eq!(block_plan.groups[0].source_range, 1..3);
        assert_eq!(block_plan.loops[0].source, outer);
        assert!(block_plan.loops[1..]
            .iter()
            .all(|planned| planned.source == inner_left || planned.source == inner_right));
        assert_eq!(block_plan.predicates.len(), 1);
        assert_eq!(block_plan.predicates[0].from_outer_join, Some(group));
        assert!(!block_plan.predicates[0].consumed);
    }

    #[test]
    fn query_block_input_records_nested_group_ranges() {
        let last = SourceId::new(0);
        let first = SourceId::new(1);
        let second = SourceId::new(2);
        let inner_group = SourceId::new(3);
        let outer_group = SourceId::new(4);
        let inner_from = hir::From {
            first,
            joins: vec![Join {
                right: second,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            }],
        };
        let outer_from = hir::From {
            first: inner_group,
            joins: vec![Join {
                right: last,
                kind: JoinKind::Cross,
                constraint: JoinConstraint::None,
            }],
        };
        let document = document(vec![
            source_with_id(0, "last"),
            source_with_id(1, "first"),
            source_with_id(2, "second"),
            group_source(3, inner_from),
            group_source(4, outer_from),
        ]);
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: outer_group,
            joins: Vec::new(),
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("nested FROM groups produce recursive planner input");

        assert_eq!(
            input
                .sources
                .iter()
                .map(HirPlanSource::source)
                .collect::<Vec<_>>(),
            vec![first, second, last]
        );
        let inner = input
            .groups
            .iter()
            .find(|boundary| boundary.source == inner_group)
            .expect("inner boundary exists");
        assert_eq!(inner.parent, Some(outer_group));
        assert_eq!(inner.source_range, 0..2);
        let outer = input
            .groups
            .iter()
            .find(|boundary| boundary.source == outer_group)
            .expect("outer boundary exists");
        assert_eq!(outer.parent, None);
        assert_eq!(outer.source_range, 0..3);
    }

    #[test]
    fn hir_from_layout_keeps_nested_right_and_full_join_scope_local() {
        let sources = (0..4)
            .map(|id| {
                HirPlanSource::BTree(
                    planned_source_from_definition(
                        &source_with_id(id, &format!("source_{id}")),
                        None,
                        &[],
                    )
                    .expect("table source converts"),
                )
            })
            .collect::<Vec<_>>();
        let outer_group = SourceId::new(10);
        let nested_group = SourceId::new(11);
        let boundaries = |kind| {
            vec![
                HirFromGroupBoundary {
                    source: nested_group,
                    parent: Some(outer_group),
                    source_range: 2..4,
                    join_info: Some(HirJoinInfo { kind }),
                },
                HirFromGroupBoundary {
                    source: outer_group,
                    parent: None,
                    source_range: 1..4,
                    join_info: None,
                },
            ]
        };

        let right_groups = boundaries(JoinKind::Right);
        let right = HirFromLayout::new(&sources, &right_groups);
        assert!(!right.outer_join_may_null_extend(SourceId::new(0)));
        assert!(right.outer_join_may_null_extend(SourceId::new(1)));
        assert!(!right.outer_join_may_null_extend(SourceId::new(2)));
        assert!(!right.outer_join_may_null_extend(SourceId::new(3)));
        assert!(!(0..4).any(|id| right.full_join_may_null_extend(SourceId::new(id))));

        let full_groups = boundaries(JoinKind::Full);
        let full = HirFromLayout::new(&sources, &full_groups);
        assert!(!full.full_join_may_null_extend(SourceId::new(0)));
        for id in 1..4 {
            assert!(full.full_join_may_null_extend(SourceId::new(id)));
        }
    }

    #[test]
    fn query_block_input_adds_table_function_predicates_with_join_ownership() {
        let left = SourceId::new(0);
        let function = SourceId::new(1);
        let mut function_source = source_with_id(1, "table_function");
        let argument_predicate = binary(
            hir::Expr::column(function, 1),
            Operator::Equals,
            hir::Expr::column(left, 0),
        );
        let SourceKind::Table(table) = function_source.kind else {
            unreachable!();
        };
        function_source.kind = SourceKind::TableFunction {
            table,
            argument_predicates: vec![argument_predicate],
        };
        let document = document(vec![source_with_id(0, "items"), function_source]);
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: left,
            joins: vec![Join {
                right: function,
                kind: JoinKind::Left,
                constraint: JoinConstraint::None,
            }],
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("table-function source converts");

        assert_eq!(input.sources.len(), 2);
        assert_eq!(input.sources[1].source(), function);
        assert_eq!(input.predicates.len(), 1);
        assert!(matches!(
            &input.predicates[0].expr,
            hir::Expr::Binary { lhs, operator: Operator::Equals, rhs, .. }
                if matches!(lhs.as_ref(), hir::Expr::Column(column) if *column == hir::ColumnRef { source: function, column: 1 })
                    && matches!(rhs.as_ref(), hir::Expr::Column(column) if *column == hir::ColumnRef { source: left, column: 0 })
        ));
        assert_eq!(input.predicates[0].from_outer_join, Some(function));
        assert!(!input.predicates[0].consumed);
    }

    #[test]
    fn query_block_input_builds_using_equality_from_resolved_columns() {
        let left = SourceId::new(0);
        let right = SourceId::new(1);
        let mut document = document(vec![
            source_with_id(0, "left_items"),
            source_with_id(1, "right_items"),
        ]);
        let comparison = ComparisonSemantics {
            components: vec![ComparisonComponent {
                affinity: Affinity::Integer,
                collation: None,
                array: false,
            }],
        };
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: left,
            joins: vec![Join {
                right,
                kind: JoinKind::Left,
                constraint: JoinConstraint::Using(vec![UsingColumn {
                    name: "first".to_string(),
                    left: Box::new(hir::Expr::column(left, 0)),
                    right: hir::ColumnRef {
                        source: right,
                        column: 0,
                    },
                    value: hir::MergedColumnValue::Left,
                    type_fact: TypeFact::known(Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                    comparison: comparison.clone(),
                }]),
            }],
        });

        let usage = [
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: left,
                    column: 0,
                },
                count: 1,
            },
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: right,
                    column: 0,
                },
                count: 1,
            },
        ];

        add_query(&mut document, &block);

        let context = HirPlanContext::new(&document);
        let input = context
            .query_block_input(&block, &[], &usage)
            .expect("query block input converts");

        assert_eq!(input.sources[1].join_info().unwrap().kind, JoinKind::Left);
        assert_eq!(input.predicates.len(), 1);
        assert_eq!(input.predicates[0].from_outer_join, Some(right));
        let hir::Expr::Binary {
            lhs,
            operator,
            rhs,
            comparison: actual_comparison,
            ..
        } = &input.predicates[0].expr
        else {
            panic!("USING becomes equality predicate");
        };
        assert_eq!(*operator, Operator::Equals);
        assert!(matches!(
            lhs.as_ref(),
            hir::Expr::Column(column) if *column == hir::ColumnRef { source: left, column: 0 }
        ));
        assert!(matches!(
            rhs.as_ref(),
            hir::Expr::Column(column) if *column == hir::ColumnRef { source: right, column: 0 }
        ));
        assert_eq!(actual_comparison.as_ref(), Some(&comparison));

        let params = CostModelParams::default();
        let mut schema = Schema::default();
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        let base_rows = input
            .sources
            .iter()
            .map(|source| {
                base_row_estimate(
                    &schema,
                    &source.btree().expect("test source is a table").table,
                    &params,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            base_rows,
            [
                RowCountEstimate::AnalyzeStats(1_000.0),
                RowCountEstimate::AnalyzeStats(1.0),
            ]
        );
        let query_id = block.id.query;
        let document = Arc::new(document);
        let query_plan = HirPlan::build(document.clone(), &schema, &params)
            .expect("HIR query planning succeeds");
        assert!(Arc::ptr_eq(&query_plan.document, &document));
        let planned_query = query_plan
            .planned_query(query_id)
            .expect("root query is planned");
        let [plan] = planned_query.blocks.as_slice() else {
            panic!("ordinary SELECT has one planned block");
        };
        assert_eq!(plan.block, block.id);
        assert!(plan.output_cardinality > 0.0);
        assert!(plan.cost.0 >= 0.0);
        let [left_loop, right_loop] = plan.loops.as_slice() else {
            panic!("two sources produce two planned loops");
        };
        assert_eq!(left_loop.source, left);
        assert_eq!(left_loop.source_position, 0);
        assert_eq!(right_loop.source, right);
        assert_eq!(right_loop.source_position, 1);
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index),
                    seek_def,
                },
            ..
        } = &right_loop.access
        else {
            panic!("right source uses its selected automatic-index seek");
        };
        assert!(index.ephemeral);
        assert_eq!(
            index
                .columns
                .iter()
                .map(|column| column.pos_in_table)
                .collect::<Vec<_>>(),
            [0]
        );
        let Some((operator, expression, affinity)) = &seek_def.prefix[0].eq else {
            panic!("join equality becomes the seek prefix");
        };
        assert_eq!(*operator, Operator::Equals);
        assert_eq!(*affinity, Affinity::Integer);
        assert!(matches!(
            expression,
            hir::Expr::Column(column) if *column == hir::ColumnRef { source: left, column: 0 }
        ));
        assert!(plan.predicates[0].consumed);
    }

    #[test]
    fn source_less_query_block_keeps_predicates_without_access_loops() {
        let mut document = document(vec![source()]);
        let block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                    "1".into(),
                ))),
                grouping: None,
            },
        );
        add_query(&mut document, &block);
        let plan = HirPlanContext::new(&document)
            .plan_query_block(
                &block,
                &[],
                7.0,
                &Schema::default(),
                &CostModelParams::default(),
                &|_| None,
            )
            .expect("source-less query block plans");

        assert!(plan.loops.is_empty());
        assert_eq!(plan.predicates.len(), 1);
        assert!(!plan.predicates[0].consumed);
        assert_eq!(plan.output_cardinality, 7.0);
        assert_eq!(plan.cost, Cost(0.0));
    }

    #[test]
    fn owned_hir_plan_plans_values_block() {
        let mut document = document(vec![source()]);
        let block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Values {
                rows: vec![vec![hir::Expr::Literal(
                    turso_parser::ast::Literal::Numeric("1".into()),
                )]],
            },
        );
        add_query(&mut document, &block);
        let document = Arc::new(document);

        let plan = HirPlan::build(
            document.clone(),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("VALUES query plans from HIR");

        assert!(Arc::ptr_eq(&plan.document, &document));
        let query = plan
            .planned_query(block.id.query)
            .expect("root query is planned");
        assert_eq!(query.blocks.len(), 1);
        assert_eq!(query.blocks[0].block, block.id);
        assert!(query.blocks[0].loops.is_empty());
        assert!(query.blocks[0].predicates.is_empty());
        assert_eq!(query.blocks[0].output_cardinality, 1.0);
        assert_eq!(query.blocks[0].cost, Cost(0.0));
    }

    #[test]
    fn owned_hir_plan_plans_compound_arms_without_arm_ordering() {
        let query_id = QueryId::new(0);
        let first_id = QueryBlockId::new(query_id, 0);
        let second_id = QueryBlockId::new(query_id, 1);
        let value = || QueryBlockBody::Values {
            rows: vec![vec![hir::Expr::Literal(
                turso_parser::ast::Literal::Numeric("1".into()),
            )]],
        };
        let first = QueryBlock::new(first_id, value());
        let second = QueryBlock::new(second_id, value());
        let mut document = document(vec![source()]);
        document.root = hir::HirRoot::Query(QueryRoot { query: query_id });
        document.queries.push(Query {
            id: query_id,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![first, second],
            first: first_id,
            compounds: vec![CompoundArm {
                operator: CompoundOperator::UnionAll,
                block: second_id,
            }],
            order_by: vec![OrderTerm {
                expr: hir::Expr::Literal(turso_parser::ast::Literal::Numeric("1".into())),
                order: SortOrder::Desc,
                nulls: None,
                type_fact: TypeFact::known(Type::Integer),
                collation: None,
            }],
            limit: None,
            output: Vec::new(),
        });
        let document = Arc::new(document);

        let plan = HirPlan::build(
            document.clone(),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("compound query arms plan from HIR");

        let planned_query = plan.planned_query(query_id).expect("root query is planned");
        assert_eq!(
            planned_query
                .blocks
                .iter()
                .map(|block| block.block)
                .collect::<Vec<_>>(),
            [first_id, second_id]
        );
        assert_eq!(planned_query.output_cardinality, 2.0);
        let query = plan.document.query(query_id).expect("query remains owned");
        assert_eq!(query.compounds.len(), 1);
        assert_eq!(query.order_by.len(), 1);
    }

    #[test]
    fn owned_hir_plan_plans_query_dependencies_before_their_owner() {
        let root_query = QueryId::new(0);
        let child_query = QueryId::new(1);
        let root_block_id = QueryBlockId::new(root_query, 0);
        let child_block_id = QueryBlockId::new(child_query, 0);
        let source_id = SourceId::new(0);

        let mut root_block = QueryBlock::new(
            root_block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::column(source_id, 0)),
                    operator: Operator::Equals,
                    rhs: Box::new(hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "1".into(),
                    ))),
                    array_concat: false,
                    custom: None,
                    comparison: Some(ComparisonSemantics {
                        components: vec![ComparisonComponent {
                            affinity: Affinity::Integer,
                            collation: None,
                            array: false,
                        }],
                    }),
                }),
                grouping: None,
            },
        );
        root_block.from = Some(hir::From {
            first: source_id,
            joins: Vec::new(),
        });
        let mut child_block = QueryBlock::new(
            child_block_id,
            QueryBlockBody::Values {
                rows: vec![
                    vec![hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "1".into(),
                    ))],
                    vec![hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "2".into(),
                    ))],
                ],
            },
        );
        child_block.outputs.push(Output {
            id: OutputId::query(child_block_id, 0),
            name: "column1".to_string(),
            expr: hir::Expr::Literal(turso_parser::ast::Literal::Numeric("1".into())),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: false,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let query = |id, parent, block: QueryBlock| {
            let output = block.outputs.iter().map(|output| output.id).collect();
            Query {
                id,
                parent,
                captures: Vec::new(),
                reachable_ctes: Vec::new(),
                first: block.id,
                blocks: vec![block],
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output,
            }
        };
        let mut derived_source = source();
        derived_source.id = source_id;
        derived_source.owner = SourceOwner::QueryBlock(root_block_id);
        derived_source.database = None;
        derived_source.kind = SourceKind::Derived(child_query);
        derived_source.columns.truncate(1);
        derived_source.generated_expressions.truncate(1);
        derived_source.default_expressions.truncate(1);
        derived_source.column_type_programs.truncate(1);
        let mut document = document(vec![derived_source]);
        document.root = hir::HirRoot::Query(QueryRoot { query: root_query });
        document.queries = vec![
            query(root_query, None, root_block),
            query(child_query, Some(root_query), child_block),
        ];
        document.validate().expect("derived query HIR is valid");
        let plan = HirPlan::build(
            Arc::new(document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("derived query plans from HIR");

        let child = plan
            .planned_query(child_query)
            .expect("child query is planned");
        assert_eq!(child.output_cardinality, 2.0);
        let root = plan
            .planned_query(root_query)
            .expect("root query is planned");
        assert_eq!(root.output_cardinality, 0.2);
        let [source_loop] = root.blocks[0].loops.as_slice() else {
            panic!("derived source produces one scan loop");
        };
        assert_eq!(source_loop.source, source_id);
        assert_eq!(source_loop.source_position, 0);
        assert!(matches!(
            source_loop.access,
            HirSourceAccess::Derived { query } if query == child_query
        ));
        assert!(root.cost.0 > child.cost.0);

        let mut subquery_document = plan.document.as_ref().clone();
        let subquery = QueryId::new(subquery_document.queries.len());
        let subquery_block = QueryBlockId::new(subquery, 0);
        let mut subquery_definition = subquery_document.queries[child_query.index()].clone();
        subquery_definition.id = subquery;
        subquery_definition.parent = Some(root_query);
        subquery_definition.first = subquery_block;
        subquery_definition.blocks[0].id = subquery_block;
        subquery_definition.blocks[0].outputs[0].id = OutputId::query(subquery_block, 0);
        subquery_definition.output = vec![OutputId::query(subquery_block, 0)];
        subquery_document.queries.push(subquery_definition);
        let QueryBlockBody::Select { filter, .. } =
            &mut subquery_document.queries[root_query.index()].blocks[0].body
        else {
            panic!("root query is SELECT");
        };
        *filter = Some(hir::Expr::Subquery(SubqueryExpr::Exists(subquery)));
        subquery_document
            .validate()
            .expect("expression subquery HIR is valid");
        let subquery_plan = HirPlan::build(
            Arc::new(subquery_document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("expression subquery plans from HIR");
        assert!(subquery_plan.planned_query(subquery).is_some());
        assert!(subquery_plan.planned_query(root_query).is_some());

        let cte_id = CteId::new(0);
        let mut cte_document = plan.document.as_ref().clone();
        cte_document.sources[source_id.index()].kind = SourceKind::Cte(cte_id);
        cte_document.queries[root_query.index()].reachable_ctes = vec![cte_id];
        let source_column = &cte_document.sources[source_id.index()].columns[0];
        cte_document.ctes.push(Cte {
            id: cte_id,
            name: "numbers".to_string(),
            columns: vec![CteColumn {
                name: source_column.name.clone(),
                type_fact: source_column.type_fact.clone(),
                affinity: source_column.affinity,
                has_affinity: source_column.has_affinity,
                collation: source_column.collation.clone(),
            }],
            materialized: Materialized::Yes,
            body: CteBody::Query(child_query),
        });
        cte_document
            .validate()
            .expect("non-recursive CTE query HIR is valid");
        let materialization =
            HirPlanContext::new(&cte_document).cte_materialization(root_query, cte_id, child_query);
        assert_eq!(materialization, HirCteMaterialization::Explicit);
        let cte_plan = HirPlan::build(
            Arc::new(cte_document.clone()),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("non-recursive CTE source plans from HIR");
        let cte_root = cte_plan
            .planned_query(root_query)
            .expect("CTE root query is planned");
        assert_eq!(cte_root.output_cardinality, 0.2);
        let [cte_loop] = cte_root.blocks[0].loops.as_slice() else {
            panic!("CTE source produces one scan loop");
        };
        assert!(matches!(
            cte_loop.access,
            HirSourceAccess::Cte {
                cte,
                query,
                materialization: HirCteMaterialization::Explicit,
            } if cte == cte_id && query == child_query
        ));

        let mut recursive_document = cte_plan.document.as_ref().clone();
        let arm_query = QueryId::new(recursive_document.queries.len());
        let arm_block_id = QueryBlockId::new(arm_query, 0);
        let input_source = SourceId::new(recursive_document.sources.len());
        let mut input_definition = recursive_document.sources[source_id.index()].clone();
        input_definition.id = input_source;
        input_definition.owner = SourceOwner::QueryBlock(arm_block_id);
        input_definition.name = "numbers".to_string();
        input_definition.kind = SourceKind::RecursiveInput(cte_id);
        recursive_document.sources.push(input_definition);

        let mut arm_block = QueryBlock::new(
            arm_block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        arm_block.from = Some(hir::From {
            first: input_source,
            joins: Vec::new(),
        });
        let mut arm_output =
            recursive_document.queries[child_query.index()].blocks[0].outputs[0].clone();
        arm_output.id = OutputId::query(arm_block_id, 0);
        arm_output.expr = hir::Expr::column(input_source, 0);
        arm_block.outputs.push(arm_output);
        recursive_document.queries.push(Query {
            id: arm_query,
            parent: Some(root_query),
            captures: Vec::new(),
            reachable_ctes: vec![cte_id],
            first: arm_block_id,
            blocks: vec![arm_block],
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: vec![OutputId::query(arm_block_id, 0)],
        });
        recursive_document.ctes[cte_id.index()].body = CteBody::Recursive(RecursiveCte {
            seed: child_query,
            arms: vec![RecursiveArm {
                operator: CompoundOperator::UnionAll,
                query: arm_query,
            }],
            input_sources: vec![input_source],
            comparison_collations: vec![None],
            queue_order: Vec::new(),
            limit: None,
        });
        recursive_document
            .validate()
            .expect("recursive CTE source HIR is valid");
        let recursive_plan = HirPlan::build(
            Arc::new(recursive_document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("recursive CTE source plans from HIR");
        assert!(recursive_plan.planned_query(child_query).is_some());
        let recursive_arm = recursive_plan
            .planned_query(arm_query)
            .expect("recursive arm query is planned");
        assert!(matches!(
            recursive_arm.blocks[0].loops[0].access,
            HirSourceAccess::RecursiveInput { cte } if cte == cte_id
        ));
        let recursive_root = recursive_plan
            .planned_query(root_query)
            .expect("recursive CTE owner query is planned");
        assert!(matches!(
            recursive_root.blocks[0].loops[0].access,
            HirSourceAccess::RecursiveCte { cte } if cte == cte_id
        ));

        cte_document.ctes[cte_id.index()].materialized = Materialized::Any;
        let materialization =
            HirPlanContext::new(&cte_document).cte_materialization(root_query, cte_id, child_query);
        assert_eq!(materialization, HirCteMaterialization::PerReference);

        let second_cte_source = SourceId::new(cte_document.sources.len());
        let mut second_reference = cte_document.sources[source_id.index()].clone();
        second_reference.id = second_cte_source;
        cte_document.sources.push(second_reference);
        cte_document.queries[root_query.index()].blocks[0]
            .from
            .as_mut()
            .expect("root query has a CTE source")
            .joins
            .push(Join {
                right: second_cte_source,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            });
        cte_document.ctes[cte_id.index()].materialized = Materialized::No;
        cte_document
            .validate()
            .expect("two references to a NOT MATERIALIZED CTE are valid");

        let mut correlated_document = cte_document.clone();
        let outer_source = SourceId::new(correlated_document.sources.len());
        let mut outer_definition = source_with_id(outer_source.index(), "outer_items");
        outer_definition.owner = SourceOwner::QueryBlock(root_block_id);
        correlated_document.sources.push(outer_definition);
        correlated_document.databases.push(DatabaseSnapshot {
            database: DatabaseId::new(0),
            schema_version: 0,
        });
        correlated_document.queries[root_query.index()].blocks[0]
            .from
            .as_mut()
            .expect("root query has CTE references")
            .joins
            .push(Join {
                right: outer_source,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            });
        correlated_document.queries[child_query.index()].blocks[0].outputs[0].expr =
            hir::Expr::column(outer_source, 0);
        correlated_document.queries[child_query.index()].captures = vec![outer_source];
        correlated_document
            .validate()
            .expect("correlated CTE body HIR is valid");
        assert_eq!(
            HirPlanContext::new(&correlated_document).cte_materialization(
                root_query,
                cte_id,
                child_query,
            ),
            HirCteMaterialization::PerReference,
        );

        let shared_plan = HirPlan::build(
            Arc::new(cte_document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("repeated CTE references plan from HIR");
        assert_eq!(
            shared_plan
                .queries
                .iter()
                .filter(|planned| planned.query == child_query)
                .count(),
            1
        );
        assert!(shared_plan
            .planned_query(root_query)
            .expect("shared CTE root query is planned")
            .blocks[0]
            .loops
            .iter()
            .all(|source_loop| matches!(
                source_loop.access,
                HirSourceAccess::Cte {
                    materialization: HirCteMaterialization::Shared,
                    ..
                }
            )));

        let table_source_id = SourceId::new(1);
        let mut table_source = source_with_id(1, "items");
        table_source.owner = SourceOwner::QueryBlock(root_block_id);
        let mut mixed_document = plan.document.as_ref().clone();
        mixed_document.databases.push(DatabaseSnapshot {
            database: DatabaseId::new(0),
            schema_version: 0,
        });
        mixed_document.sources.push(table_source);
        let root_block = &mut mixed_document.queries[root_query.index()].blocks[0];
        root_block.from = Some(hir::From {
            first: table_source_id,
            joins: vec![Join {
                right: source_id,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::column(table_source_id, 0)),
                    operator: Operator::Equals,
                    rhs: Box::new(hir::Expr::column(source_id, 0)),
                    array_concat: false,
                    custom: None,
                    comparison: Some(ComparisonSemantics {
                        components: vec![ComparisonComponent {
                            affinity: Affinity::Integer,
                            collation: None,
                            array: false,
                        }],
                    }),
                }),
            }],
        });
        mixed_document
            .validate()
            .expect("mixed table and derived query HIR is valid");
        let mixed_plan = HirPlan::build(
            Arc::new(mixed_document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("table and derived source join plans from HIR");
        let mixed_root = mixed_plan
            .planned_query(root_query)
            .expect("mixed root query is planned");
        assert_eq!(mixed_root.blocks[0].loops.len(), 2);
        assert!(mixed_root.blocks[0].loops.iter().any(|source_loop| {
            source_loop.source == table_source_id
                && matches!(source_loop.access, HirSourceAccess::BTree { .. })
        }));
        assert!(mixed_root.blocks[0].loops.iter().any(|source_loop| {
            source_loop.source == source_id
                && matches!(
                    source_loop.access,
                    HirSourceAccess::Derived { query } if query == child_query
                )
        }));
    }
}
