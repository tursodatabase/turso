//! Derived dependency summaries for resolved queries.

use rustc_hash::FxHashSet as HashSet;

use super::*;
use crate::function::Deterministic;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ColumnUsage {
    pub(crate) reference: ColumnRef,
    pub(crate) count: usize,
}

/// The table columns each query reads directly, in query order.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct QueryColumnReads {
    reads: Vec<ColumnRef>,
    ends: Vec<usize>,
}

impl QueryColumnReads {
    pub(crate) fn collect<'query, 'source>(
        queries: impl IntoIterator<Item = &'query Query>,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
        exprs: &ExprArena,
    ) -> Self {
        let mut query_reads = Self::default();
        for query in queries {
            query.direct_column_reads(source_by_id, exprs, &mut query_reads.reads);
            query_reads.ends.push(query_reads.reads.len());
        }
        query_reads
    }

    pub(crate) fn all(&self) -> &[ColumnRef] {
        &self.reads
    }

    fn by_query(&self) -> impl Iterator<Item = &[ColumnRef]> {
        let starts = core::iter::once(0).chain(self.ends.iter().copied());
        starts
            .zip(self.ends.iter().copied())
            .map(|(start, end)| &self.reads[start..end])
    }
}

#[derive(Default)]
struct ColumnUsageCollector {
    reads: Vec<ColumnUsage>,
}

trait RecordColumnRead {
    fn record(&mut self, reference: ColumnRef);
}

impl RecordColumnRead for ColumnUsageCollector {
    fn record(&mut self, reference: ColumnRef) {
        self.add(ColumnUsage {
            reference,
            count: 1,
        });
    }
}

impl RecordColumnRead for Vec<ColumnRef> {
    fn record(&mut self, reference: ColumnRef) {
        self.push(reference);
    }
}

impl ColumnUsageCollector {
    fn add(&mut self, usage: ColumnUsage) {
        self.reads.push(usage);
    }

    fn into_usage(mut self) -> Vec<ColumnUsage> {
        let key = |usage: &ColumnUsage| (usage.reference.source.index(), usage.reference.column);
        if !self.reads.is_sorted_by_key(key) {
            self.reads.sort_by_key(key);
        }
        let mut usage: Vec<ColumnUsage> = Vec::with_capacity(self.reads.len());
        for read in self.reads {
            match usage.last_mut() {
                Some(last) if last.reference == read.reference => last.count += read.count,
                _ => usage.push(read),
            }
        }
        usage
    }
}

impl HirDocument {
    /// Column reads of a DML statement: `direct`, its own reads, plus the
    /// reads of every subquery that reads a source the statement owns, such
    /// as `t.c` in `DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.x = t.c)`.
    pub(crate) fn root_column_usage(&self, direct: Vec<ColumnUsage>) -> Vec<ColumnUsage> {
        let owns = |source: SourceId| {
            self.source(source)
                .is_some_and(|source| matches!(source.owner, SourceOwner::Root))
        };
        let mut counts = ColumnUsageCollector::default();
        for usage in direct {
            counts.add(usage);
        }
        for query in &self.queries {
            for usage in query.direct_column_usage(|source| self.source(source), &self.exprs) {
                if owns(usage.reference.source) {
                    counts.add(usage);
                }
            }
        }
        counts.into_usage()
    }

    /// Sources of enclosing queries that `query`, or any query it runs
    /// (subqueries in its expressions, FROM subqueries and CTEs), reads. A
    /// query that reads any must run again whenever the outer row changes,
    /// even when its own expressions read no outer column.
    pub(crate) fn outer_sources(&self, query: QueryId) -> Vec<SourceId> {
        let mut pending = vec![query];
        let mut nested = HashSet::default();
        while let Some(id) = pending.pop() {
            if nested.insert(id) {
                pending.extend(self.direct_query_dependencies(id));
            }
        }
        self.outer_sources_of(&nested)
    }

    fn outer_sources_of(&self, nested: &HashSet<QueryId>) -> Vec<SourceId> {
        let mut outer = nested
            .iter()
            .filter_map(|id| self.query(*id))
            .flat_map(|query| query.captures.iter().copied())
            .filter(|source| {
                !matches!(
                    self.source(*source).map(|source| source.owner),
                    Some(SourceOwner::QueryBlock(block)) if nested.contains(&block.query)
                )
            })
            .collect::<Vec<_>>();
        outer.sort_unstable();
        outer.dedup();
        outer
    }

    pub(crate) fn reads_outer_sources(&self, query: QueryId) -> bool {
        !self.outer_sources(query).is_empty()
    }

    /// Return every query whose plan is needed directly by this query. This
    /// includes query-backed FROM sources and subqueries in expressions, but
    /// does not descend into those query definitions.
    /// Whether a FROM clause of `query` itself names `table`, without
    /// looking into the queries it runs.
    pub(crate) fn query_reads_table_directly(&self, query: QueryId, table: &ResolvedTable) -> bool {
        let definition = self.query(query).expect("validated HIR query must exist");
        let mut reads = false;
        visit_query_parts(self, definition, &mut |part| {
            if let QueryPart::Source(source) = part {
                if let SourceKind::Table(source_table) = &source.kind {
                    reads |= source_table == table;
                }
            }
        });
        reads
    }

    /// Whether `query`, or any query it runs, reads `table`.
    pub(crate) fn query_reads_table(&self, query: QueryId, table: &ResolvedTable) -> bool {
        self.query_reads_source(
            query,
            |kind| matches!(kind, SourceKind::Table(source_table) if source_table == table),
        )
    }

    /// Whether `query`, or any query it runs, has a FROM source of a kind
    /// `matches` accepts.
    fn query_reads_source(&self, query: QueryId, matches: impl Fn(&SourceKind) -> bool) -> bool {
        let mut pending = vec![query];
        let mut seen = HashSet::default();
        while let Some(query) = pending.pop() {
            if !seen.insert(query) {
                continue;
            }
            let definition = self.query(query).expect("validated HIR query must exist");
            let mut reads = false;
            visit_query_parts(self, definition, &mut |part| {
                if let QueryPart::Source(source) = part {
                    reads |= matches(&source.kind);
                }
            });
            if reads {
                return true;
            }
            pending.extend(self.direct_query_dependencies(query));
        }
        false
    }

    /// The scalar, row, IN and EXISTS subqueries written in `query`'s own
    /// clauses, without the queries of its FROM subqueries and CTEs.
    pub(crate) fn expression_subqueries(&self, id: QueryId) -> Vec<QueryId> {
        let Some(query) = self.query(id) else {
            return Vec::new();
        };
        self.direct_query_links(query).0
    }

    pub(crate) fn direct_query_dependencies(&self, id: QueryId) -> Vec<QueryId> {
        let Some(query) = self.query(id) else {
            return Vec::new();
        };
        self.direct_query_links(query).1
    }

    pub(crate) fn query_facts(&self, query_reads: &QueryColumnReads) -> Vec<QueryFacts> {
        #[cfg(debug_assertions)]
        assert_eq!(
            query_reads,
            &QueryColumnReads::collect(&self.queries, |source| self.source(source), &self.exprs),
            "shared column reads must match the reads of the finished HIR"
        );
        let mut facts = self
            .queries
            .iter()
            .map(|query| {
                let (subqueries, dependencies) = self.direct_query_links(query);
                QueryFacts {
                    subqueries,
                    dependencies,
                    ..QueryFacts::default()
                }
            })
            .collect::<Vec<_>>();
        let mut usage = (0..self.queries.len())
            .map(|_| ColumnUsageCollector::default())
            .collect::<Vec<_>>();
        for (query, reads) in self.queries.iter().zip(query_reads.by_query()) {
            usage[query.id.index()].reads.reserve(reads.len());
            for &reference in reads {
                usage[query.id.index()].record(reference);
                if let Some(SourceOwner::QueryBlock(block)) =
                    self.source(reference.source).map(|source| source.owner)
                {
                    if block.query != query.id {
                        usage[block.query.index()].record(reference);
                    }
                }
            }
        }
        for (index, usage) in usage.into_iter().enumerate() {
            facts[index].source_column_usage = usage.into_usage();
        }
        for index in 0..facts.len() {
            let mut pending = vec![self.queries[index].id];
            let mut nested = HashSet::default();
            while let Some(id) = pending.pop() {
                if nested.insert(id) {
                    pending.extend(facts[id.index()].dependencies.iter().copied());
                }
            }
            facts[index].outer_sources = self.outer_sources_of(&nested);
        }
        facts
    }

    fn direct_query_links(&self, query: &Query) -> (Vec<QueryId>, Vec<QueryId>) {
        let mut subqueries = HashSet::default();
        let mut dependencies = HashSet::default();
        visit_query_parts(self, query, &mut |part| match part {
            QueryPart::Source(source) => match &source.kind {
                SourceKind::Derived(query) => {
                    dependencies.insert(*query);
                }
                SourceKind::Cte(cte) => {
                    let cte = self.cte(*cte).expect("validated HIR query CTE must exist");
                    match &cte.body {
                        CteBody::Query(query) => {
                            dependencies.insert(*query);
                        }
                        CteBody::Recursive(recursive) => {
                            dependencies.insert(recursive.seed);
                            dependencies.extend(recursive.arms.iter().map(|arm| arm.query));
                        }
                    }
                }
                _ => {}
            },
            QueryPart::Expression(expression) => {
                expression.for_each(&mut |expression| {
                    let Expr::Subquery(subquery) = expression.node() else {
                        return;
                    };
                    let query = match subquery {
                        SubqueryExpr::Scalar { query, .. }
                        | SubqueryExpr::Row { query }
                        | SubqueryExpr::In { query, .. }
                        | SubqueryExpr::Exists(query) => *query,
                    };
                    subqueries.insert(query);
                    dependencies.insert(query);
                });
            }
        });

        let mut subqueries = subqueries.into_iter().collect::<Vec<_>>();
        subqueries.sort_unstable();
        let mut dependencies = dependencies.into_iter().collect::<Vec<_>>();
        dependencies.sort_unstable();
        (subqueries, dependencies)
    }

    /// The outer values that `query`, or any query it runs, reads: one
    /// `Column` or `RowId` expression per outer column, so the same values
    /// always give the same result.
    ///
    /// `None` when that is not true or not known: an outer value is read some
    /// other way, such as through an outer aggregate or output; a function
    /// is not deterministic; or a source is a virtual table or table
    /// function, whose rows may change between calls.
    pub(crate) fn repeatable_outer_values(&self, query: QueryId) -> Option<Vec<Expr>> {
        let mut pending = vec![query];
        let mut tree = HashSet::default();
        while let Some(id) = pending.pop() {
            if tree.insert(id) {
                pending.extend(self.direct_query_dependencies(id));
            }
        }
        let in_tree = |source: SourceId| {
            matches!(
                self.source(source).map(|source| source.owner),
                Some(SourceOwner::QueryBlock(block)) if tree.contains(&block.query)
            )
        };
        let mut values = Vec::new();
        let mut repeatable = true;
        for id in &tree {
            let definition = self.query(*id)?;
            visit_query_parts(self, definition, &mut |part| match part {
                QueryPart::Source(source) => {
                    repeatable &= match &source.kind {
                        SourceKind::Table(table) => {
                            !matches!(table.value(), crate::schema::Table::Virtual(_))
                        }
                        kind => matches!(
                            kind,
                            SourceKind::Cte(_)
                                | SourceKind::Derived(_)
                                | SourceKind::FromGroup(_)
                                | SourceKind::RecursiveInput(_)
                        ),
                    };
                }
                QueryPart::Expression(expression) => {
                    expression.for_each(&mut |expression| {
                        match expression.node() {
                        Expr::Column(reference) if !in_tree(reference.source) => {
                            if !values.iter().any(|known: &Expr| {
                                matches!(known, Expr::Column(known) if known == reference)
                            }) {
                                values.push(Expr::Column(*reference));
                            }
                        }
                        Expr::RowId(source) if !in_tree(*source) => {
                            if !values.iter().any(|known: &Expr| {
                                matches!(known, Expr::RowId(known) if known == source)
                            }) {
                                values.push(Expr::RowId(*source));
                            }
                        }
                        Expr::Function(call) => {
                            // Built-in aggregates call themselves not
                            // deterministic because their result depends on
                            // the rows they read; the same rows give the
                            // same result.
                            let function = call.function.value();
                            repeatable &= (function.is_deterministic()
                                || matches!(
                                    function,
                                    crate::function::Func::Agg(_)
                                        | crate::function::Func::Window(_)
                                ))
                                && matches!(call.operation, FunctionOperation::Ordinary);
                            match &call.evaluation {
                                FunctionEvaluation::Aggregate { id, .. } => {
                                    repeatable &= tree.contains(&id.block.query);
                                }
                                FunctionEvaluation::Window { id, .. } => {
                                    repeatable &= tree.contains(&id.block.query);
                                }
                                FunctionEvaluation::Scalar => {}
                            }
                        }
                        Expr::Output(id) => {
                            repeatable &= matches!(
                                id.owner,
                                OutputOwner::QueryBlock(block) if tree.contains(&block.query)
                            );
                        }
                        _ => {}
                    }
                    });
                }
            });
        }
        repeatable.then_some(values)
    }

    /// How many subquery expressions of `parent` itself run `child`.
    pub(crate) fn subquery_use_count(&self, parent: QueryId, child: QueryId) -> usize {
        let Some(parent) = self.query(parent) else {
            return 0;
        };
        let mut uses = 0;
        visit_query_parts(self, parent, &mut |part| {
            let QueryPart::Expression(expression) = part else {
                return;
            };
            expression.for_each(&mut |expression| {
                if let Expr::Subquery(
                    SubqueryExpr::Scalar { query, .. }
                    | SubqueryExpr::Row { query }
                    | SubqueryExpr::In { query, .. }
                    | SubqueryExpr::Exists(query),
                ) = expression.node()
                {
                    uses += usize::from(*query == child);
                }
            });
        });
        uses
    }

    /// Visit every source needed to evaluate one expression in its owning
    /// query. Output references follow their resolved expression; subqueries
    /// contribute their validated outer captures rather than their local
    /// sources.
    pub(crate) fn visit_expr_sources(&self, expression: ExprId, visit: &mut impl FnMut(SourceId)) {
        self.expr(expression)
            .for_each(&mut |expression| match expression.node() {
                Expr::Column(reference) => visit(reference.source),
                Expr::RowId(source) => visit(*source),
                Expr::Output(id) => {
                    let output = self
                        .output(*id)
                        .expect("validated HIR output reference must exist");
                    self.visit_expr_sources(output.expr, visit);
                }
                Expr::Subquery(subquery) => {
                    let query = match subquery {
                        SubqueryExpr::Scalar { query, .. }
                        | SubqueryExpr::Row { query }
                        | SubqueryExpr::In { query, .. }
                        | SubqueryExpr::Exists(query) => query,
                    };
                    for source in self.planning.outer_sources(*query) {
                        visit(*source);
                    }
                }
                _ => {}
            });
    }

    /// Visit every expression written in `query`'s own clauses, without the
    /// expressions of the queries it runs.
    pub(crate) fn for_each_query_expression(
        &self,
        query: QueryId,
        visit: &mut impl FnMut(ExprRef<'_>),
    ) {
        let Some(query) = self.query(query) else {
            return;
        };
        visit_query_parts(self, query, &mut |part| {
            if let QueryPart::Expression(expression) = part {
                visit(expression);
            }
        });
    }

    /// Recompute the exact external source set read directly by one query.
    /// Nested queries own their own capture summaries and are not traversed.
    pub(crate) fn direct_query_captures(&self, id: QueryId) -> Vec<SourceId> {
        let Some(query) = self.query(id) else {
            return Vec::new();
        };
        query.direct_captures(|source| self.source(source), &self.exprs)
    }
}

enum QueryPart<'hir> {
    Source(&'hir Source),
    Expression(ExprRef<'hir>),
}

fn visit_query_parts<'hir>(
    document: &'hir HirDocument,
    query: &'hir Query,
    visit: &mut impl FnMut(QueryPart<'hir>),
) {
    for block in &query.blocks {
        if let Some(from) = &block.from {
            visit_from_parts(document, from, visit);
        }
        for output in &block.outputs {
            visit(QueryPart::Expression(document.expr(output.expr)));
        }
        match &block.body {
            QueryBlockBody::Select {
                filter, grouping, ..
            } => {
                if let Some(filter) = filter {
                    visit(QueryPart::Expression(document.expr(*filter)));
                }
                if let Some(grouping) = grouping {
                    for key in &grouping.keys {
                        visit(QueryPart::Expression(document.expr(*key)));
                    }
                    if let Some(having) = &grouping.having {
                        visit(QueryPart::Expression(document.expr(*having)));
                    }
                }
            }
            QueryBlockBody::Values { rows } => {
                for expression in rows.iter().flatten() {
                    visit(QueryPart::Expression(document.expr(*expression)));
                }
            }
        }
        for window in &block.windows {
            for term in &window.partition_by {
                visit(QueryPart::Expression(document.expr(term.expr)));
            }
            for term in &window.order_by {
                visit(QueryPart::Expression(document.expr(term.expr)));
            }
            visit_window_bound_part(document, &window.frame.start, visit);
            if let Some(end) = &window.frame.end {
                visit_window_bound_part(document, end, visit);
            }
        }
    }
    for term in &query.order_by {
        visit(QueryPart::Expression(document.expr(term.expr)));
    }
    if let Some(limit) = &query.limit {
        visit(QueryPart::Expression(document.expr(limit.limit)));
        if let Some(offset) = &limit.offset {
            visit(QueryPart::Expression(document.expr(*offset)));
        }
    }
}

fn visit_from_parts<'hir>(
    document: &'hir HirDocument,
    from: &'hir From,
    visit: &mut impl FnMut(QueryPart<'hir>),
) {
    let mut pending = vec![from];
    while let Some(from) = pending.pop() {
        for id in core::iter::once(from.first).chain(from.joins.iter().map(|join| join.right)) {
            let source = document
                .source(id)
                .expect("validated HIR query source must exist");
            visit(QueryPart::Source(source));
            match &source.kind {
                SourceKind::TableFunction {
                    argument_predicates,
                    ..
                } => {
                    for predicate in argument_predicates {
                        visit(QueryPart::Expression(document.expr(*predicate)));
                    }
                }
                SourceKind::FromGroup(group) => {
                    pending.push(&group.from);
                    for column in &group.columns {
                        visit(QueryPart::Expression(document.expr(*column)));
                    }
                }
                _ => {}
            }
        }
        for join in &from.joins {
            match &join.constraint {
                JoinConstraint::None => {}
                JoinConstraint::On(expression) => {
                    visit(QueryPart::Expression(document.expr(*expression)))
                }
                JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
                    for column in columns {
                        visit(QueryPart::Expression(document.expr(column.left)));
                    }
                }
            }
        }
    }
}

fn visit_window_bound_part<'hir>(
    document: &'hir HirDocument,
    bound: &'hir WindowFrameBound,
    visit: &mut impl FnMut(QueryPart<'hir>),
) {
    if let WindowFrameBound::Following(expression) | WindowFrameBound::Preceding(expression) = bound
    {
        visit(QueryPart::Expression(document.expr(*expression)));
    }
}

impl ExprRef<'_> {
    /// Return distinct column positions when this expression reads exactly
    /// one source. Output definitions and nested queries remain separate work.
    pub(crate) fn single_source_column_usage(self) -> Option<(SourceId, Vec<usize>)> {
        let mut reads = ColumnUsageCollector::default();
        collect_expr_column_reads(self.arena(), self.id(), &mut reads);
        let usage = reads.into_usage();
        let source = usage.first()?.reference.source;
        if usage.iter().any(|usage| usage.reference.source != source) {
            return None;
        }
        Some((
            source,
            usage
                .into_iter()
                .map(|usage| usage.reference.column)
                .collect(),
        ))
    }
}

impl Query {
    /// Derive the external sources read directly by this query from resolved
    /// HIR. Nested queries own their own capture summaries.
    pub(crate) fn direct_captures<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
        exprs: &ExprArena,
    ) -> Vec<SourceId> {
        let mut references = Vec::new();
        for block in &self.blocks {
            if let Some(from) = &block.from {
                collect_from_references(exprs, from, &mut references, source_by_id);
            }
            for output in &block.outputs {
                collect_expr_references(exprs, output.expr, &mut references);
            }
            match &block.body {
                QueryBlockBody::Select {
                    filter, grouping, ..
                } => {
                    collect_optional_expr_references(exprs, *filter, &mut references);
                    if let Some(grouping) = grouping {
                        collect_exprs_references(exprs, &grouping.keys, &mut references);
                        collect_optional_expr_references(exprs, grouping.having, &mut references);
                    }
                }
                QueryBlockBody::Values { rows } => {
                    for row in rows {
                        collect_exprs_references(exprs, row, &mut references);
                    }
                }
            }
            for window in &block.windows {
                collect_window_references(exprs, window, &mut references);
            }
        }
        collect_order_references(exprs, &self.order_by, &mut references);
        if let Some(limit) = &self.limit {
            collect_expr_references(exprs, limit.limit, &mut references);
            collect_optional_expr_references(exprs, limit.offset, &mut references);
        }

        references.sort_unstable();
        references.dedup();
        references.retain(|source| {
            !matches!(
                source_by_id(*source).map(|source| source.owner),
                Some(SourceOwner::QueryBlock(block)) if block.query == self.id
            )
        });
        references
    }

    /// Count every table column read directly by this query. Nested queries
    /// own their usage and are visited separately by later phases.
    pub(crate) fn direct_column_usage<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
        exprs: &ExprArena,
    ) -> Vec<ColumnUsage> {
        let mut reads = ColumnUsageCollector::default();
        self.record_direct_column_reads(source_by_id, exprs, &mut reads);
        reads.into_usage()
    }

    /// Append every table column read directly by this query, once per
    /// read. Nested queries own their reads and are visited separately by
    /// the analyzer.
    pub(crate) fn direct_column_reads<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
        exprs: &ExprArena,
        reads: &mut Vec<ColumnRef>,
    ) {
        self.record_direct_column_reads(source_by_id, exprs, reads);
    }

    fn record_direct_column_reads<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
        exprs: &ExprArena,
        reads: &mut impl RecordColumnRead,
    ) {
        for block in &self.blocks {
            if let Some(from) = &block.from {
                collect_from_column_reads(exprs, from, reads, source_by_id);
            }
            for output in &block.outputs {
                collect_expr_column_reads(exprs, output.expr, reads);
            }
            match &block.body {
                QueryBlockBody::Select {
                    filter, grouping, ..
                } => {
                    collect_optional_expr_column_reads(exprs, *filter, reads);
                    if let Some(grouping) = grouping {
                        collect_exprs_column_reads(exprs, &grouping.keys, reads);
                        collect_optional_expr_column_reads(exprs, grouping.having, reads);
                    }
                }
                QueryBlockBody::Values { rows } => {
                    for row in rows {
                        collect_exprs_column_reads(exprs, row, reads);
                    }
                }
            }
            for window in &block.windows {
                collect_window_column_reads(exprs, window, reads);
            }
        }
        collect_order_column_reads(exprs, &self.order_by, reads);
        if let Some(limit) = &self.limit {
            collect_expr_column_reads(exprs, limit.limit, reads);
            collect_optional_expr_column_reads(exprs, limit.offset, reads);
        }
    }
}

impl Update {
    /// Count every table column read directly by the UPDATE root. Nested
    /// queries own their usage and are visited separately by later phases.
    pub(crate) fn direct_column_usage<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
        exprs: &ExprArena,
    ) -> Vec<ColumnUsage> {
        let mut reads = ColumnUsageCollector::default();
        if let Some(from) = &self.from {
            collect_from_column_reads(exprs, from, &mut reads, source_by_id);
        }
        if let UpdateTargetKind::BTree { defaults, .. } = &self.target_kind {
            for default in defaults {
                collect_expr_column_reads(exprs, default.value, &mut reads);
            }
        }
        for assignment in &self.assignments {
            collect_expr_column_reads(exprs, assignment.value, &mut reads);
        }
        collect_optional_expr_column_reads(exprs, self.predicate, &mut reads);
        collect_order_column_reads(exprs, &self.order_by, &mut reads);
        if let Some(limit) = &self.limit {
            collect_expr_column_reads(exprs, limit.limit, &mut reads);
            collect_optional_expr_column_reads(exprs, limit.offset, &mut reads);
        }
        if let Some(returning) = &self.returning {
            for output in &returning.outputs {
                collect_expr_column_reads(exprs, output.expr, &mut reads);
            }
        }

        reads.into_usage()
    }

    /// Return every table column read directly by the UPDATE root. Nested
    /// queries own their reads and are visited separately by the analyzer.
    pub(crate) fn direct_column_reads<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
        exprs: &ExprArena,
    ) -> impl Iterator<Item = ColumnRef> {
        self.direct_column_usage(source_by_id, exprs)
            .into_iter()
            .map(|usage| usage.reference)
    }
}

impl Delete {
    /// Count every table column read directly by the DELETE root. Nested
    /// queries own their usage and are visited separately by later phases.
    pub(crate) fn direct_column_usage(&self, exprs: &ExprArena) -> Vec<ColumnUsage> {
        let mut reads = ColumnUsageCollector::default();
        collect_optional_expr_column_reads(exprs, self.predicate, &mut reads);
        collect_order_column_reads(exprs, &self.order_by, &mut reads);
        if let Some(limit) = &self.limit {
            collect_expr_column_reads(exprs, limit.limit, &mut reads);
            collect_optional_expr_column_reads(exprs, limit.offset, &mut reads);
        }
        if let Some(returning) = &self.returning {
            for output in &returning.outputs {
                collect_expr_column_reads(exprs, output.expr, &mut reads);
            }
        }

        reads.into_usage()
    }

    /// Return every table column read directly by the DELETE root. Nested
    /// queries own their reads and are visited separately by the analyzer.
    pub(crate) fn direct_column_reads(&self, exprs: &ExprArena) -> impl Iterator<Item = ColumnRef> {
        self.direct_column_usage(exprs)
            .into_iter()
            .map(|usage| usage.reference)
    }
}

fn collect_from_column_reads<'source>(
    exprs: &ExprArena,
    from: &From,
    reads: &mut impl RecordColumnRead,
    source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
) {
    collect_source_argument_column_reads(exprs, from.first, reads, source_by_id);
    for join in &from.joins {
        collect_source_argument_column_reads(exprs, join.right, reads, source_by_id);
        match &join.constraint {
            JoinConstraint::None => {}
            JoinConstraint::On(expression) => collect_expr_column_reads(exprs, *expression, reads),
            JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
                for column in columns {
                    collect_expr_column_reads(exprs, column.left, reads);
                    reads.record(column.right);
                }
            }
        }
    }
}

fn collect_source_argument_column_reads<'source>(
    exprs: &ExprArena,
    id: SourceId,
    reads: &mut impl RecordColumnRead,
    source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
) {
    let Some(source) = source_by_id(id) else {
        return;
    };
    match &source.kind {
        SourceKind::TableFunction {
            argument_predicates,
            ..
        } => {
            collect_exprs_column_reads(exprs, argument_predicates, reads);
        }
        SourceKind::FromGroup(group) => {
            collect_from_column_reads(exprs, &group.from, reads, source_by_id);
            collect_exprs_column_reads(exprs, &group.columns, reads);
        }
        _ => {}
    }
}

fn collect_expr_column_reads(
    exprs: &ExprArena,
    expression: ExprId,
    reads: &mut impl RecordColumnRead,
) {
    exprs.at(expression).for_each(&mut |expression| {
        if let Expr::Column(reference) = expression.node() {
            reads.record(*reference);
        }
    });
}

fn collect_exprs_column_reads(
    exprs: &ExprArena,
    expressions: &[ExprId],
    reads: &mut impl RecordColumnRead,
) {
    for expression in expressions {
        collect_expr_column_reads(exprs, *expression, reads);
    }
}

fn collect_optional_expr_column_reads(
    exprs: &ExprArena,
    expression: Option<ExprId>,
    reads: &mut impl RecordColumnRead,
) {
    if let Some(expression) = expression {
        collect_expr_column_reads(exprs, expression, reads);
    }
}

fn collect_order_column_reads(
    exprs: &ExprArena,
    terms: &[OrderTerm],
    reads: &mut impl RecordColumnRead,
) {
    for term in terms {
        collect_expr_column_reads(exprs, term.expr, reads);
    }
}

fn collect_window_column_reads(
    exprs: &ExprArena,
    window: &ResolvedWindow,
    reads: &mut impl RecordColumnRead,
) {
    for term in &window.partition_by {
        collect_expr_column_reads(exprs, term.expr, reads);
    }
    collect_order_column_reads(exprs, &window.order_by, reads);
    let frame = &window.frame;
    collect_window_bound_column_reads(exprs, &frame.start, reads);
    if let Some(end) = &frame.end {
        collect_window_bound_column_reads(exprs, end, reads);
    }
}

fn collect_window_bound_column_reads(
    exprs: &ExprArena,
    bound: &WindowFrameBound,
    reads: &mut impl RecordColumnRead,
) {
    if let WindowFrameBound::Following(expression) | WindowFrameBound::Preceding(expression) = bound
    {
        collect_expr_column_reads(exprs, *expression, reads);
    }
}

fn collect_from_references<'source>(
    exprs: &ExprArena,
    from: &From,
    references: &mut Vec<SourceId>,
    source: impl Fn(SourceId) -> Option<&'source Source> + Copy,
) {
    collect_source_arguments(exprs, from.first, references, source);
    for join in &from.joins {
        collect_source_arguments(exprs, join.right, references, source);
        match &join.constraint {
            JoinConstraint::None => {}
            JoinConstraint::On(expression) => {
                collect_expr_references(exprs, *expression, references);
            }
            JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
                for column in columns {
                    collect_expr_references(exprs, column.left, references);
                    add_reference(references, column.right.source);
                }
            }
        }
    }
}

fn collect_source_arguments<'source>(
    exprs: &ExprArena,
    id: SourceId,
    references: &mut Vec<SourceId>,
    source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
) {
    let Some(source) = source_by_id(id) else {
        return;
    };
    match &source.kind {
        SourceKind::TableFunction {
            argument_predicates,
            ..
        } => {
            collect_exprs_references(exprs, argument_predicates, references);
        }
        SourceKind::FromGroup(group) => {
            collect_from_references(exprs, &group.from, references, source_by_id);
            collect_exprs_references(exprs, &group.columns, references);
        }
        _ => {}
    }
}

fn collect_expr_references(exprs: &ExprArena, expression: ExprId, references: &mut Vec<SourceId>) {
    match &exprs[expression] {
        Expr::Literal(_) | Expr::Parameter(_) | Expr::Output(_) => {}
        Expr::Column(reference) => {
            add_reference(references, reference.source);
        }
        Expr::MergedColumn(column) => match column.value {
            MergedColumnValue::Left => collect_expr_references(exprs, column.left, references),
            MergedColumnValue::Right => collect_expr_references(exprs, column.right, references),
            MergedColumnValue::Coalesce => {
                collect_expr_references(exprs, column.left, references);
                collect_expr_references(exprs, column.right, references);
            }
        },
        Expr::RowId(source) => {
            add_reference(references, *source);
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::TruthTest { expr, .. } => {
            collect_expr_references(exprs, *expr, references);
        }
        Expr::Binary {
            lhs, rhs, custom, ..
        } => {
            collect_expr_references(exprs, *lhs, references);
            collect_expr_references(exprs, *rhs, references);
            if let Some(call) = custom
                .as_ref()
                .and_then(|custom| custom.literal_encoding.as_ref())
                .and_then(|encoding| encoding.encoder.as_ref())
            {
                collect_exprs_references(exprs, &call.arguments, references);
            }
        }
        Expr::Between {
            expr, start, end, ..
        } => {
            collect_expr_references(exprs, *expr, references);
            collect_expr_references(exprs, *start, references);
            collect_expr_references(exprs, *end, references);
        }
        Expr::Case {
            base,
            when_then,
            else_expr,
            ..
        } => {
            collect_optional_expr_references(exprs, *base, references);
            for (when, then) in when_then {
                collect_expr_references(exprs, *when, references);
                collect_expr_references(exprs, *then, references);
            }
            collect_optional_expr_references(exprs, *else_expr, references);
        }
        Expr::Cast { expr, target } => {
            collect_expr_references(exprs, *expr, references);
            collect_exprs_references(exprs, &target.parameters, references);
            for call in &target.programs.encode {
                collect_exprs_references(exprs, &call.arguments, references);
            }
            if let Some(domain) = &target.programs.domain {
                for check in &domain.checks {
                    collect_exprs_references(exprs, &check.call.arguments, references);
                }
            }
        }
        Expr::Collate { expr, .. } => collect_expr_references(exprs, *expr, references),
        Expr::Function(function) => {
            collect_exprs_references(exprs, function.arguments.expressions(), references);
            collect_order_references(exprs, function.arguments.order_terms(), references);
            collect_optional_expr_references(exprs, function.evaluation.filter(), references);
        }
        Expr::InList { lhs, values, .. } => {
            collect_expr_references(exprs, *lhs, references);
            collect_exprs_references(exprs, values, references);
        }
        Expr::Subquery(SubqueryExpr::In { lhs, .. }) => {
            collect_expr_references(exprs, *lhs, references);
        }
        Expr::Subquery(
            SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. } | SubqueryExpr::Exists(_),
        ) => {}
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            collect_expr_references(exprs, *lhs, references);
            collect_expr_references(exprs, *rhs, references);
            collect_optional_expr_references(exprs, *escape, references);
        }
        Expr::Row(expressions) | Expr::Array(expressions) => {
            collect_exprs_references(exprs, expressions, references);
        }
        Expr::Subscript { base, index } => {
            collect_expr_references(exprs, *base, references);
            collect_expr_references(exprs, *index, references);
        }
        Expr::FieldAccess(access) => collect_expr_references(exprs, access.base, references),
        Expr::Raise { message, .. } => {
            collect_optional_expr_references(exprs, *message, references);
        }
    }
}

fn add_reference(references: &mut Vec<SourceId>, source: SourceId) {
    if references.last() != Some(&source) {
        references.push(source);
    }
}

fn collect_exprs_references(
    exprs: &ExprArena,
    expressions: &[ExprId],
    references: &mut Vec<SourceId>,
) {
    for expression in expressions {
        collect_expr_references(exprs, *expression, references);
    }
}

fn collect_optional_expr_references(
    exprs: &ExprArena,
    expression: Option<ExprId>,
    references: &mut Vec<SourceId>,
) {
    if let Some(expression) = expression {
        collect_expr_references(exprs, expression, references);
    }
}

fn collect_order_references(
    exprs: &ExprArena,
    terms: &[OrderTerm],
    references: &mut Vec<SourceId>,
) {
    for term in terms {
        collect_expr_references(exprs, term.expr, references);
    }
}

fn collect_window_references(
    exprs: &ExprArena,
    window: &ResolvedWindow,
    references: &mut Vec<SourceId>,
) {
    for term in &window.partition_by {
        collect_expr_references(exprs, term.expr, references);
    }
    collect_order_references(exprs, &window.order_by, references);
    let frame = &window.frame;
    collect_window_bound_references(exprs, &frame.start, references);
    if let Some(end) = &frame.end {
        collect_window_bound_references(exprs, end, references);
    }
}

fn collect_window_bound_references(
    exprs: &ExprArena,
    bound: &WindowFrameBound,
    references: &mut Vec<SourceId>,
) {
    if let WindowFrameBound::Following(expression) | WindowFrameBound::Preceding(expression) = bound
    {
        collect_expr_references(exprs, *expression, references);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(filter: ExprId, from: Option<From>) -> Query {
        let query = QueryId::new(0);
        let block = QueryBlockId::new(query, 0);
        let mut block_value = QueryBlock::new(
            block,
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(filter),
                grouping: None,
            },
        );
        block_value.from = from;
        Query {
            id: query,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![block_value],
            first: block,
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: Vec::new(),
        }
    }

    fn no_source(_: SourceId) -> Option<&'static Source> {
        None
    }

    #[test]
    fn repeated_column_reads_are_counted() {
        let source = SourceId::new(0);
        let mut exprs = ExprArena::default();
        let columns = (0..3)
            .map(|_| exprs.push(Expr::column(source, 0)))
            .collect();
        let row = exprs.push(Expr::Row(columns));
        let query = query(row, None);

        assert_eq!(
            query.direct_column_usage(no_source, &exprs),
            vec![ColumnUsage {
                reference: ColumnRef { source, column: 0 },
                count: 3,
            }]
        );
        let mut reads = Vec::new();
        query.direct_column_reads(no_source, &exprs, &mut reads);
        assert_eq!(reads, vec![ColumnRef { source, column: 0 }; 3]);
    }

    #[test]
    fn expression_usage_requires_exactly_one_source() {
        let first = SourceId::new(0);
        let second = SourceId::new(1);
        let mut exprs = ExprArena::default();
        let single_source = vec![
            exprs.push(Expr::column(first, 1)),
            exprs.push(Expr::column(first, 0)),
            exprs.push(Expr::column(first, 1)),
        ];
        let single_source = exprs.push(Expr::Row(single_source));
        let multiple_sources = vec![
            exprs.push(Expr::column(first, 0)),
            exprs.push(Expr::column(second, 0)),
        ];
        let multiple_sources = exprs.push(Expr::Row(multiple_sources));
        let null = exprs.push(Expr::Literal(turso_parser::ast::Literal::Null));
        assert_eq!(
            exprs.at(single_source).single_source_column_usage(),
            Some((first, vec![0, 1]))
        );

        assert_eq!(
            exprs.at(multiple_sources).single_source_column_usage(),
            None
        );
        assert_eq!(exprs.at(null).single_source_column_usage(), None);
    }

    #[test]
    fn join_constraint_reads_are_counted() {
        let left = SourceId::new(0);
        let right = SourceId::new(1);
        let mut exprs = ExprArena::default();
        let constraint = vec![
            exprs.push(Expr::column(left, 0)),
            exprs.push(Expr::column(right, 1)),
        ];
        let constraint = exprs.push(Expr::Row(constraint));
        let filter = exprs.push(Expr::Row(Vec::new()));
        let from = From {
            first: left,
            joins: vec![Join {
                right,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(constraint),
            }],
        };
        let query = query(filter, Some(from));

        assert_eq!(
            query.direct_column_usage(no_source, &exprs),
            vec![
                ColumnUsage {
                    reference: ColumnRef {
                        source: left,
                        column: 0,
                    },
                    count: 1,
                },
                ColumnUsage {
                    reference: ColumnRef {
                        source: right,
                        column: 1,
                    },
                    count: 1,
                },
            ]
        );
    }

    #[test]
    fn nested_query_references_are_not_counted_by_parent() {
        let source = SourceId::new(0);
        let mut exprs = ExprArena::default();
        let values = vec![
            exprs.push(Expr::column(source, 0)),
            exprs.push(Expr::Subquery(SubqueryExpr::Exists(QueryId::new(1)))),
        ];
        let row = exprs.push(Expr::Row(values));
        let query = query(row, None);

        assert_eq!(
            query.direct_column_usage(no_source, &exprs),
            vec![ColumnUsage {
                reference: ColumnRef { source, column: 0 },
                count: 1,
            }]
        );
    }
}
