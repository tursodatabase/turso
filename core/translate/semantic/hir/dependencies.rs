//! Derived dependency summaries for resolved queries.

use rustc_hash::FxHashSet as HashSet;

use super::*;

impl HirDocument {
    /// Recompute the exact external source set read directly by one query.
    /// Nested queries own their own capture summaries and are not traversed.
    pub(crate) fn direct_query_captures(&self, id: QueryId) -> Vec<SourceId> {
        let Some(query) = self.query(id) else {
            return Vec::new();
        };
        query.direct_captures(|source| self.source(source))
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

    /// Return every table column read directly by this query. Nested queries
    /// own their reads and are visited separately by the analyzer.
    pub(crate) fn direct_column_reads<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
    ) -> Vec<ColumnRef> {
        let mut reads = HashSet::default();
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

        let mut reads = reads.into_iter().collect::<Vec<_>>();
        reads.sort_unstable_by_key(|read| (read.source.index(), read.column));
        reads
    }
}

impl Update {
    /// Return every table column read directly by the UPDATE root. Nested
    /// queries own their reads and are visited separately by the analyzer.
    pub(crate) fn direct_column_reads<'source>(
        &self,
        source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
    ) -> Vec<ColumnRef> {
        let mut reads = HashSet::default();
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

        let mut reads = reads.into_iter().collect::<Vec<_>>();
        reads.sort_unstable_by_key(|read| (read.source.index(), read.column));
        reads
    }
}

impl Delete {
    /// Return every table column read directly by the DELETE root. Nested
    /// queries own their reads and are visited separately by the analyzer.
    pub(crate) fn direct_column_reads(&self) -> Vec<ColumnRef> {
        let mut reads = HashSet::default();
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

        let mut reads = reads.into_iter().collect::<Vec<_>>();
        reads.sort_unstable_by_key(|read| (read.source.index(), read.column));
        reads
    }
}

fn collect_from_column_reads<'source>(
    from: &From,
    reads: &mut HashSet<ColumnRef>,
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
                    reads.insert(column.right);
                }
            }
        }
    }
}

fn collect_source_argument_column_reads<'source>(
    id: SourceId,
    reads: &mut HashSet<ColumnRef>,
    source_by_id: impl Fn(SourceId) -> Option<&'source Source> + Copy,
) {
    let Some(source) = source_by_id(id) else {
        return;
    };
    match &source.kind {
        SourceKind::TableFunction { arguments, .. } => {
            collect_exprs_column_reads(arguments, reads);
        }
        SourceKind::FromGroup(group) => {
            collect_from_column_reads(&group.from, reads, source_by_id);
            collect_exprs_column_reads(&group.columns, reads);
        }
        _ => {}
    }
}

fn collect_expr_column_reads(expression: &Expr, reads: &mut HashSet<ColumnRef>) {
    expression.walk(&mut |expression| match expression {
        Expr::Column(reference) => {
            reads.insert(*reference);
        }
        Expr::MergedColumn(column) => {
            reads.insert(column.right);
        }
        _ => {}
    });
}

fn collect_exprs_column_reads(expressions: &[Expr], reads: &mut HashSet<ColumnRef>) {
    for expression in expressions {
        collect_expr_column_reads(expression, reads);
    }
}

fn collect_optional_expr_column_reads(expression: Option<&Expr>, reads: &mut HashSet<ColumnRef>) {
    if let Some(expression) = expression {
        collect_expr_column_reads(expression, reads);
    }
}

fn collect_order_column_reads(terms: &[OrderTerm], reads: &mut HashSet<ColumnRef>) {
    for term in terms {
        collect_expr_column_reads(&term.expr, reads);
    }
}

fn collect_window_column_reads(window: &ResolvedWindow, reads: &mut HashSet<ColumnRef>) {
    collect_exprs_column_reads(&window.partition_by, reads);
    collect_order_column_reads(&window.order_by, reads);
    let frame = &window.frame;
    collect_window_bound_column_reads(&frame.start, reads);
    if let Some(end) = &frame.end {
        collect_window_bound_column_reads(end, reads);
    }
}

fn collect_window_bound_column_reads(bound: &WindowFrameBound, reads: &mut HashSet<ColumnRef>) {
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
        SourceKind::TableFunction { arguments, .. } => {
            collect_exprs_references(arguments, references);
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
        Expr::MergedColumn(column) => {
            collect_expr_references(&column.left, references);
            references.insert(column.right.source);
        }
        Expr::RowId(source) => {
            references.insert(*source);
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::NotNull(expr) => {
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
    collect_exprs_references(&window.partition_by, references);
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
