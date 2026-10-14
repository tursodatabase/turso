//! Derived dependency summaries for resolved queries.

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ColumnUsage {
    pub(crate) reference: ColumnRef,
    pub(crate) count: usize,
}

#[derive(Default)]
struct ColumnUsageCollector {
    counts: HashMap<ColumnRef, usize>,
}

impl ColumnUsageCollector {
    fn record(&mut self, reference: ColumnRef) {
        *self.counts.entry(reference).or_default() += 1;
    }

    fn into_usage(self) -> Vec<ColumnUsage> {
        let mut usage = self
            .counts
            .into_iter()
            .map(|(reference, count)| ColumnUsage { reference, count })
            .collect::<Vec<_>>();
        usage
            .sort_unstable_by_key(|usage| (usage.reference.source.index(), usage.reference.column));
        usage
    }
}

impl HirDocument {
    /// Return every query whose plan is needed directly by this query. This
    /// includes query-backed FROM sources and subqueries in expressions, but
    /// does not descend into those query definitions.
    pub(crate) fn direct_query_dependencies(&self, id: QueryId) -> Vec<QueryId> {
        let Some(query) = self.query(id) else {
            return Vec::new();
        };
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
                    let Expr::Subquery(subquery) = expression else {
                        return;
                    };
                    let query = match subquery {
                        SubqueryExpr::Scalar { query, .. }
                        | SubqueryExpr::Row { query }
                        | SubqueryExpr::In { query, .. }
                        | SubqueryExpr::Exists(query) => *query,
                    };
                    dependencies.insert(query);
                });
            }
        });

        let mut dependencies = dependencies.into_iter().collect::<Vec<_>>();
        dependencies.sort_unstable();
        dependencies
    }

    /// Visit every source needed to evaluate one expression in its owning
    /// query. Output references follow their resolved expression; subqueries
    /// contribute their validated outer captures rather than their local
    /// sources.
    pub(crate) fn visit_expr_sources(&self, expression: &Expr, visit: &mut impl FnMut(SourceId)) {
        expression.for_each(&mut |expression| match expression {
            Expr::Column(reference) => visit(reference.source),
            Expr::RowId(source) => visit(*source),
            Expr::Output(id) => {
                let output = self
                    .output(*id)
                    .expect("validated HIR output reference must exist");
                self.visit_expr_sources(&output.expr, visit);
            }
            Expr::Subquery(subquery) => {
                let query = match subquery {
                    SubqueryExpr::Scalar { query, .. }
                    | SubqueryExpr::Row { query }
                    | SubqueryExpr::In { query, .. }
                    | SubqueryExpr::Exists(query) => query,
                };
                let query = self
                    .query(*query)
                    .expect("validated HIR subquery reference must exist");
                for source in &query.captures {
                    visit(*source);
                }
            }
            _ => {}
        });
    }

    /// Recompute the exact external source set read directly by one query.
    /// Nested queries own their own capture summaries and are not traversed.
    pub(crate) fn direct_query_captures(&self, id: QueryId) -> Vec<SourceId> {
        let Some(query) = self.query(id) else {
            return Vec::new();
        };
        query.direct_captures(|source| self.source(source))
    }
}

enum QueryPart<'hir> {
    Source(&'hir Source),
    Expression(&'hir Expr),
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
            visit(QueryPart::Expression(&output.expr));
        }
        match &block.body {
            QueryBlockBody::Select {
                filter, grouping, ..
            } => {
                if let Some(filter) = filter {
                    visit(QueryPart::Expression(filter));
                }
                if let Some(grouping) = grouping {
                    for key in &grouping.keys {
                        visit(QueryPart::Expression(key));
                    }
                    if let Some(having) = &grouping.having {
                        visit(QueryPart::Expression(having));
                    }
                }
            }
            QueryBlockBody::Values { rows } => {
                for expression in rows.iter().flatten() {
                    visit(QueryPart::Expression(expression));
                }
            }
        }
        for window in &block.windows {
            for term in &window.partition_by {
                visit(QueryPart::Expression(&term.expr));
            }
            for term in &window.order_by {
                visit(QueryPart::Expression(&term.expr));
            }
            visit_window_bound_part(&window.frame.start, visit);
            if let Some(end) = &window.frame.end {
                visit_window_bound_part(end, visit);
            }
        }
    }
    for term in &query.order_by {
        visit(QueryPart::Expression(&term.expr));
    }
    if let Some(limit) = &query.limit {
        visit(QueryPart::Expression(&limit.limit));
        if let Some(offset) = &limit.offset {
            visit(QueryPart::Expression(offset));
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
                        visit(QueryPart::Expression(predicate));
                    }
                }
                SourceKind::FromGroup(group) => {
                    pending.push(&group.from);
                    for column in &group.columns {
                        visit(QueryPart::Expression(column));
                    }
                }
                _ => {}
            }
        }
        for join in &from.joins {
            match &join.constraint {
                JoinConstraint::None => {}
                JoinConstraint::On(expression) => visit(QueryPart::Expression(expression)),
                JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
                    for column in columns {
                        visit(QueryPart::Expression(&column.left));
                    }
                }
            }
        }
    }
}

fn visit_window_bound_part<'hir>(
    bound: &'hir WindowFrameBound,
    visit: &mut impl FnMut(QueryPart<'hir>),
) {
    if let WindowFrameBound::Following(expression) | WindowFrameBound::Preceding(expression) = bound
    {
        visit(QueryPart::Expression(expression));
    }
}

impl Expr {
    /// Return distinct column positions when this expression reads exactly
    /// one source. Output definitions and nested queries remain separate work.
    pub(crate) fn single_source_column_usage(&self) -> Option<(SourceId, Vec<usize>)> {
        let mut reads = ColumnUsageCollector::default();
        collect_expr_column_reads(self, &mut reads);
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
    ) -> Vec<SourceId> {
        let mut references = HashSet::default();
        for block in &self.blocks {
            if let Some(from) = &block.from {
                collect_from_references(from, &mut references, source_by_id);
            }
            for output in &block.outputs {
                collect_expr_references(&output.expr, &mut references);
            }
            match &block.body {
                QueryBlockBody::Select {
                    filter, grouping, ..
                } => {
                    collect_optional_expr_references(filter.as_ref(), &mut references);
                    if let Some(grouping) = grouping {
                        collect_exprs_references(&grouping.keys, &mut references);
                        collect_optional_expr_references(grouping.having.as_ref(), &mut references);
                    }
                }
                QueryBlockBody::Values { rows } => {
                    for row in rows {
                        collect_exprs_references(row, &mut references);
                    }
                }
            }
            for window in &block.windows {
                collect_window_references(window, &mut references);
            }
        }
        collect_order_references(&self.order_by, &mut references);
        if let Some(limit) = &self.limit {
            collect_expr_references(&limit.limit, &mut references);
            collect_optional_expr_references(limit.offset.as_ref(), &mut references);
        }

        let mut captures = references
            .into_iter()
            .filter(|source| {
                !matches!(
                    source_by_id(*source).map(|source| source.owner),
                    Some(SourceOwner::QueryBlock(block)) if block.query == self.id
                )
            })
            .collect::<Vec<_>>();
        captures.sort_unstable();
        captures
    }

    /// Count every table column read directly by this query. Nested queries
    /// own their usage and are visited separately by later phases.
    pub(crate) fn direct_column_usage<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
    ) -> Vec<ColumnUsage> {
        let mut reads = ColumnUsageCollector::default();
        for block in &self.blocks {
            if let Some(from) = &block.from {
                collect_from_column_reads(from, &mut reads, source_by_id);
            }
            for output in &block.outputs {
                collect_expr_column_reads(&output.expr, &mut reads);
            }
            match &block.body {
                QueryBlockBody::Select {
                    filter, grouping, ..
                } => {
                    collect_optional_expr_column_reads(filter.as_ref(), &mut reads);
                    if let Some(grouping) = grouping {
                        collect_exprs_column_reads(&grouping.keys, &mut reads);
                        collect_optional_expr_column_reads(grouping.having.as_ref(), &mut reads);
                    }
                }
                QueryBlockBody::Values { rows } => {
                    for row in rows {
                        collect_exprs_column_reads(row, &mut reads);
                    }
                }
            }
            for window in &block.windows {
                collect_window_column_reads(window, &mut reads);
            }
        }
        collect_order_column_reads(&self.order_by, &mut reads);
        if let Some(limit) = &self.limit {
            collect_expr_column_reads(&limit.limit, &mut reads);
            collect_optional_expr_column_reads(limit.offset.as_ref(), &mut reads);
        }

        reads.into_usage()
    }

    /// Return every table column read directly by this query. Nested queries
    /// own their reads and are visited separately by the analyzer.
    pub(crate) fn direct_column_reads<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
    ) -> impl Iterator<Item = ColumnRef> {
        self.direct_column_usage(source_by_id)
            .into_iter()
            .map(|usage| usage.reference)
    }
}

impl Update {
    /// Count every table column read directly by the UPDATE root. Nested
    /// queries own their usage and are visited separately by later phases.
    pub(crate) fn direct_column_usage<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
    ) -> Vec<ColumnUsage> {
        let mut reads = ColumnUsageCollector::default();
        if let Some(from) = &self.from {
            collect_from_column_reads(from, &mut reads, source_by_id);
        }
        if let UpdateTargetKind::BTree { defaults, .. } = &self.target_kind {
            for default in defaults {
                collect_expr_column_reads(&default.value, &mut reads);
            }
        }
        for assignment in &self.assignments {
            collect_expr_column_reads(&assignment.value, &mut reads);
        }
        collect_optional_expr_column_reads(self.predicate.as_ref(), &mut reads);
        collect_order_column_reads(&self.order_by, &mut reads);
        if let Some(limit) = &self.limit {
            collect_expr_column_reads(&limit.limit, &mut reads);
            collect_optional_expr_column_reads(limit.offset.as_ref(), &mut reads);
        }
        if let Some(returning) = &self.returning {
            for output in &returning.outputs {
                collect_expr_column_reads(&output.expr, &mut reads);
            }
        }

        reads.into_usage()
    }

    /// Return every table column read directly by the UPDATE root. Nested
    /// queries own their reads and are visited separately by the analyzer.
    pub(crate) fn direct_column_reads<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
    ) -> impl Iterator<Item = ColumnRef> {
        self.direct_column_usage(source_by_id)
            .into_iter()
            .map(|usage| usage.reference)
    }
}

impl Delete {
    /// Count every table column read directly by the DELETE root. Nested
    /// queries own their usage and are visited separately by later phases.
    pub(crate) fn direct_column_usage(&self) -> Vec<ColumnUsage> {
        let mut reads = ColumnUsageCollector::default();
        collect_optional_expr_column_reads(self.predicate.as_ref(), &mut reads);
        collect_order_column_reads(&self.order_by, &mut reads);
        if let Some(limit) = &self.limit {
            collect_expr_column_reads(&limit.limit, &mut reads);
            collect_optional_expr_column_reads(limit.offset.as_ref(), &mut reads);
        }
        if let Some(returning) = &self.returning {
            for output in &returning.outputs {
                collect_expr_column_reads(&output.expr, &mut reads);
            }
        }

        reads.into_usage()
    }

    /// Return every table column read directly by the DELETE root. Nested
    /// queries own their reads and are visited separately by the analyzer.
    pub(crate) fn direct_column_reads(&self) -> impl Iterator<Item = ColumnRef> {
        self.direct_column_usage()
            .into_iter()
            .map(|usage| usage.reference)
    }
}

fn collect_from_column_reads<'source>(
    from: &From,
    reads: &mut ColumnUsageCollector,
    source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
) {
    collect_source_argument_column_reads(from.first, reads, source_by_id);
    for join in &from.joins {
        collect_source_argument_column_reads(join.right, reads, source_by_id);
        match &join.constraint {
            JoinConstraint::None => {}
            JoinConstraint::On(expression) => collect_expr_column_reads(expression, reads),
            JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
                for column in columns {
                    collect_expr_column_reads(&column.left, reads);
                    reads.record(column.right);
                }
            }
        }
    }
}

fn collect_source_argument_column_reads<'source>(
    id: SourceId,
    reads: &mut ColumnUsageCollector,
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
            collect_exprs_column_reads(argument_predicates, reads);
        }
        SourceKind::FromGroup(group) => {
            collect_from_column_reads(&group.from, reads, source_by_id);
            collect_exprs_column_reads(&group.columns, reads);
        }
        _ => {}
    }
}

fn collect_expr_column_reads(expression: &Expr, reads: &mut ColumnUsageCollector) {
    expression.for_each(&mut |expression| match expression {
        Expr::Column(reference) => {
            reads.record(*reference);
        }
        _ => {}
    });
}

fn collect_exprs_column_reads(expressions: &[Expr], reads: &mut ColumnUsageCollector) {
    for expression in expressions {
        collect_expr_column_reads(expression, reads);
    }
}

fn collect_optional_expr_column_reads(expression: Option<&Expr>, reads: &mut ColumnUsageCollector) {
    if let Some(expression) = expression {
        collect_expr_column_reads(expression, reads);
    }
}

fn collect_order_column_reads(terms: &[OrderTerm], reads: &mut ColumnUsageCollector) {
    for term in terms {
        collect_expr_column_reads(&term.expr, reads);
    }
}

fn collect_window_column_reads(window: &ResolvedWindow, reads: &mut ColumnUsageCollector) {
    for term in &window.partition_by {
        collect_expr_column_reads(&term.expr, reads);
    }
    collect_order_column_reads(&window.order_by, reads);
    let frame = &window.frame;
    collect_window_bound_column_reads(&frame.start, reads);
    if let Some(end) = &frame.end {
        collect_window_bound_column_reads(end, reads);
    }
}

fn collect_window_bound_column_reads(bound: &WindowFrameBound, reads: &mut ColumnUsageCollector) {
    if let WindowFrameBound::Following(expression) | WindowFrameBound::Preceding(expression) = bound
    {
        collect_expr_column_reads(expression, reads);
    }
}

fn collect_from_references<'source>(
    from: &From,
    references: &mut HashSet<SourceId>,
    source: impl Fn(SourceId) -> Option<&'source Source> + Copy,
) {
    collect_source_arguments(from.first, references, source);
    for join in &from.joins {
        collect_source_arguments(join.right, references, source);
        match &join.constraint {
            JoinConstraint::None => {}
            JoinConstraint::On(expression) => {
                collect_expr_references(expression, references);
            }
            JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
                for column in columns {
                    collect_expr_references(&column.left, references);
                    references.insert(column.right.source);
                }
            }
        }
    }
}

fn collect_source_arguments<'source>(
    id: SourceId,
    references: &mut HashSet<SourceId>,
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
            collect_exprs_references(argument_predicates, references);
        }
        SourceKind::FromGroup(group) => {
            collect_from_references(&group.from, references, source_by_id);
            collect_exprs_references(&group.columns, references);
        }
        _ => {}
    }
}

fn collect_expr_references(expression: &Expr, references: &mut HashSet<SourceId>) {
    match expression {
        Expr::Literal(_) | Expr::Parameter(_) | Expr::Output(_) => {}
        Expr::Column(reference) => {
            references.insert(reference.source);
        }
        Expr::MergedColumn(column) => match column.value {
            MergedColumnValue::Left => collect_expr_references(&column.left, references),
            MergedColumnValue::Right => collect_expr_references(&column.right, references),
            MergedColumnValue::Coalesce => {
                collect_expr_references(&column.left, references);
                collect_expr_references(&column.right, references);
            }
        },
        Expr::RowId(source) => {
            references.insert(*source);
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::TruthTest { expr, .. } => {
            collect_expr_references(expr, references);
        }
        Expr::Binary {
            lhs, rhs, custom, ..
        } => {
            collect_expr_references(lhs, references);
            collect_expr_references(rhs, references);
            if let Some(call) = custom
                .as_ref()
                .and_then(|custom| custom.literal_encoding.as_ref())
                .and_then(|encoding| encoding.encoder.as_ref())
            {
                collect_exprs_references(&call.arguments, references);
            }
        }
        Expr::Between {
            expr, start, end, ..
        } => {
            collect_expr_references(expr, references);
            collect_expr_references(start, references);
            collect_expr_references(end, references);
        }
        Expr::Case {
            base,
            when_then,
            else_expr,
            ..
        } => {
            collect_optional_expr_references(base.as_deref(), references);
            for (when, then) in when_then {
                collect_expr_references(when, references);
                collect_expr_references(then, references);
            }
            collect_optional_expr_references(else_expr.as_deref(), references);
        }
        Expr::Cast { expr, target } => {
            collect_expr_references(expr, references);
            collect_exprs_references(&target.parameters, references);
            for call in &target.programs.encode {
                collect_exprs_references(&call.arguments, references);
            }
            if let Some(domain) = &target.programs.domain {
                for check in &domain.checks {
                    collect_exprs_references(&check.call.arguments, references);
                }
            }
        }
        Expr::Collate { expr, .. } => collect_expr_references(expr, references),
        Expr::Function(function) => {
            collect_exprs_references(function.arguments.expressions(), references);
            collect_order_references(function.arguments.order_terms(), references);
            collect_optional_expr_references(function.evaluation.filter(), references);
        }
        Expr::InList { lhs, values, .. } => {
            collect_expr_references(lhs, references);
            collect_exprs_references(values, references);
        }
        Expr::Subquery(SubqueryExpr::In { lhs, .. }) => {
            collect_expr_references(lhs, references);
        }
        Expr::Subquery(
            SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. } | SubqueryExpr::Exists(_),
        ) => {}
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            collect_expr_references(lhs, references);
            collect_expr_references(rhs, references);
            collect_optional_expr_references(escape.as_deref(), references);
        }
        Expr::Row(expressions) | Expr::Array(expressions) => {
            collect_exprs_references(expressions, references);
        }
        Expr::Subscript { base, index } => {
            collect_expr_references(base, references);
            collect_expr_references(index, references);
        }
        Expr::FieldAccess(access) => collect_expr_references(&access.base, references),
        Expr::Raise { message, .. } => {
            collect_optional_expr_references(message.as_deref(), references);
        }
    }
}

fn collect_exprs_references(expressions: &[Expr], references: &mut HashSet<SourceId>) {
    for expression in expressions {
        collect_expr_references(expression, references);
    }
}

fn collect_optional_expr_references(expression: Option<&Expr>, references: &mut HashSet<SourceId>) {
    if let Some(expression) = expression {
        collect_expr_references(expression, references);
    }
}

fn collect_order_references(terms: &[OrderTerm], references: &mut HashSet<SourceId>) {
    for term in terms {
        collect_expr_references(&term.expr, references);
    }
}

fn collect_window_references(window: &ResolvedWindow, references: &mut HashSet<SourceId>) {
    for term in &window.partition_by {
        collect_expr_references(&term.expr, references);
    }
    collect_order_references(&window.order_by, references);
    let frame = &window.frame;
    collect_window_bound_references(&frame.start, references);
    if let Some(end) = &frame.end {
        collect_window_bound_references(end, references);
    }
}

fn collect_window_bound_references(bound: &WindowFrameBound, references: &mut HashSet<SourceId>) {
    if let WindowFrameBound::Following(expression) | WindowFrameBound::Preceding(expression) = bound
    {
        collect_expr_references(expression, references);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(filter: Expr, from: Option<From>) -> Query {
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
        let query = query(
            Expr::Row(vec![
                Expr::column(source, 0),
                Expr::column(source, 0),
                Expr::column(source, 0),
            ]),
            None,
        );

        assert_eq!(
            query.direct_column_usage(no_source),
            vec![ColumnUsage {
                reference: ColumnRef { source, column: 0 },
                count: 3,
            }]
        );
        assert_eq!(
            query.direct_column_reads(no_source).collect::<Vec<_>>(),
            vec![ColumnRef { source, column: 0 }]
        );
    }

    #[test]
    fn expression_usage_requires_exactly_one_source() {
        let first = SourceId::new(0);
        let second = SourceId::new(1);
        let single_source = Expr::Row(vec![
            Expr::column(first, 1),
            Expr::column(first, 0),
            Expr::column(first, 1),
        ]);
        assert_eq!(
            single_source.single_source_column_usage(),
            Some((first, vec![0, 1]))
        );

        let multiple_sources = Expr::Row(vec![Expr::column(first, 0), Expr::column(second, 0)]);
        assert_eq!(multiple_sources.single_source_column_usage(), None);
        assert_eq!(
            Expr::Literal(turso_parser::ast::Literal::Null).single_source_column_usage(),
            None
        );
    }

    #[test]
    fn join_constraint_reads_are_counted() {
        let left = SourceId::new(0);
        let right = SourceId::new(1);
        let from = From {
            first: left,
            joins: vec![Join {
                right,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(Expr::Row(vec![
                    Expr::column(left, 0),
                    Expr::column(right, 1),
                ])),
            }],
        };
        let query = query(Expr::Row(Vec::new()), Some(from));

        assert_eq!(
            query.direct_column_usage(no_source),
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
        let query = query(
            Expr::Row(vec![
                Expr::column(source, 0),
                Expr::Subquery(SubqueryExpr::Exists(QueryId::new(1))),
            ]),
            None,
        );

        assert_eq!(
            query.direct_column_usage(no_source),
            vec![ColumnUsage {
                reference: ColumnRef { source, column: 0 },
                count: 1,
            }]
        );
    }
}
