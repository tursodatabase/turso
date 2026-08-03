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
        let mut references = HashSet::default();
        for block in &query.blocks {
            if let Some(from) = &block.from {
                self.collect_from_references(from, &mut references);
            }
            for output in &block.outputs {
                collect_expr_references(&output.expr, &mut references);
            }
            match &block.body {
                QueryBlockBody::Select {
                    filter,
                    grouping,
                    windows,
                    ..
                } => {
                    collect_optional_expr_references(filter.as_ref(), &mut references);
                    if let Some(grouping) = grouping {
                        collect_exprs_references(&grouping.keys, &mut references);
                        collect_optional_expr_references(grouping.having.as_ref(), &mut references);
                    }
                    for window in windows {
                        collect_window_references(&window.spec, &mut references);
                    }
                }
                QueryBlockBody::Values { rows } => {
                    for row in rows {
                        collect_exprs_references(row, &mut references);
                    }
                }
            }
        }
        collect_order_references(&query.order_by, &mut references);
        if let Some(limit) = &query.limit {
            collect_expr_references(&limit.limit, &mut references);
            collect_optional_expr_references(limit.offset.as_ref(), &mut references);
        }

        let mut captures = references
            .into_iter()
            .filter(|source| {
                !matches!(
                    self.source(*source).map(|source| source.owner),
                    Some(SourceOwner::QueryBlock(block)) if block.query == id
                )
            })
            .collect::<Vec<_>>();
        captures.sort_unstable();
        captures
    }

    fn collect_from_references(&self, from: &From, references: &mut HashSet<SourceId>) {
        self.collect_source_arguments(from.first, references);
        for join in &from.joins {
            self.collect_source_arguments(join.right, references);
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

    fn collect_source_arguments(&self, source: SourceId, references: &mut HashSet<SourceId>) {
        let Some(source) = self.source(source) else {
            return;
        };
        if let SourceKind::TableFunction { arguments, .. } = &source.kind {
            collect_exprs_references(arguments, references);
        }
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
            collect_exprs_references(&function.arguments, references);
            collect_order_references(&function.argument_order, references);
            collect_order_references(&function.within_group, references);
            collect_optional_expr_references(function.filter.as_deref(), references);
            if let Some(window) = &function.window {
                collect_window_references(window, references);
            }
        }
        Expr::InList { lhs, values, .. } => {
            collect_expr_references(lhs, references);
            collect_exprs_references(values, references);
        }
        Expr::Subquery(SubqueryExpr::In { lhs, .. }) => {
            collect_expr_references(lhs, references);
        }
        Expr::Subquery(SubqueryExpr::Scalar { .. } | SubqueryExpr::Exists(_)) => {}
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

fn collect_window_references(window: &WindowSpec, references: &mut HashSet<SourceId>) {
    collect_exprs_references(&window.partition_by, references);
    collect_order_references(&window.order_by, references);
    let Some(frame) = &window.frame else {
        return;
    };
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
