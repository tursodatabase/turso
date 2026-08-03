//! CTE reference counting and recursive-query placement rules.

use turso_parser::ast;

use crate::{
    translate::expr::{walk_expr, WalkControl},
    LimboError, Result,
};

pub(super) fn cte_self_reference_info(cte_name: &str, select: &ast::Select) -> (bool, bool) {
    let counter = RecursiveRefCounter {
        cte_name,
        count_table_calls: true,
    };
    let references_itself = counter.count_select(select, &mut RecursiveRefScope::new()) > 0;
    if !references_itself {
        return (false, false);
    }
    let mut scope = RecursiveRefScope::new();
    counter.push_nested_ctes(select.with.as_ref(), &mut scope);
    let (_, first_arm_count) = counter.count_arm(&select.body.select, &mut scope);
    (true, first_arm_count > 0)
}

pub(super) fn validate_recursive_cte_structure(
    cte_name: &str,
    select: &ast::Select,
) -> Result<usize> {
    let mut first_recursive_query_index = None;
    let counter = RecursiveRefCounter {
        cte_name,
        count_table_calls: true,
    };
    let mut scope = RecursiveRefScope::new();
    counter.push_nested_ctes(select.with.as_ref(), &mut scope);
    for (query_index, query) in std::iter::once(&select.body.select)
        .chain(
            select
                .body
                .compounds
                .iter()
                .map(|compound| &compound.select),
        )
        .enumerate()
    {
        let (top_level_from_count, total_count) = counter.count_arm(query, &mut scope);
        if first_recursive_query_index.is_none() && total_count == 0 {
            continue;
        }
        if query_index == 0 {
            crate::bail_parse_error!("circular reference: {}", cte_name);
        }
        first_recursive_query_index.get_or_insert(query_index);
        if top_level_from_count == 0 {
            crate::bail_parse_error!("circular reference: {}", cte_name);
        }
        if top_level_from_count > 1 {
            crate::bail_parse_error!("multiple references to recursive table: {}", cte_name);
        }
        if total_count > top_level_from_count {
            crate::bail_parse_error!("multiple recursive references: {}", cte_name);
        }
    }
    first_recursive_query_index.ok_or_else(|| {
        LimboError::InternalError(format!("recursive CTE {cte_name} has no recursive query"))
    })
}

pub(super) struct RecursiveRefCounter<'a> {
    pub(super) cte_name: &'a str,
    pub(super) count_table_calls: bool,
}

pub(super) type RecursiveRefScope = Vec<(String, usize)>;
const MANY_REFERENCES: usize = 2;

impl RecursiveRefCounter<'_> {
    fn name_weight(&self, name: &str, scope: &RecursiveRefScope) -> usize {
        scope
            .iter()
            .rev()
            .find(|(scope_name, _)| scope_name == name)
            .map_or_else(|| usize::from(name == self.cte_name), |(_, weight)| *weight)
    }

    fn push_nested_ctes(&self, with: Option<&ast::With>, scope: &mut RecursiveRefScope) {
        let Some(with) = with else {
            return;
        };

        // Every name in one WITH clause shadows outer names throughout that
        // clause, including inside definitions written before it. Weights are
        // capped at two because callers only distinguish none, one, or many.
        let base = scope.len();
        for cte in &with.ctes {
            let name = crate::util::normalize_ident(cte.tbl_name.as_str());
            scope.push((name, 0));
        }
        loop {
            let mut changed = false;
            for (index, cte) in with.ctes.iter().enumerate() {
                let weight = self.count_select(&cte.select, scope).min(MANY_REFERENCES);
                let entry = &mut scope[base + index];
                if weight > entry.1 {
                    entry.1 = weight;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    pub(super) fn count_select(
        &self,
        select: &ast::Select,
        scope: &mut RecursiveRefScope,
    ) -> usize {
        let base = scope.len();
        self.push_nested_ctes(select.with.as_ref(), scope);
        let mut count = self.count_one_select(&select.body.select, scope);
        for compound in &select.body.compounds {
            count += self.count_one_select(&compound.select, scope);
        }
        for sorted in &select.order_by {
            count += self.count_expr(&sorted.expr, scope);
        }
        if let Some(limit) = &select.limit {
            count += self.count_expr(&limit.expr, scope);
            if let Some(offset) = &limit.offset {
                count += self.count_expr(offset, scope);
            }
        }
        scope.truncate(base);
        count
    }

    fn count_one_select(&self, one: &ast::OneSelect, scope: &mut RecursiveRefScope) -> usize {
        match one {
            ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
                ..
            } => {
                let mut count = 0;
                if let Some(from) = from {
                    count += self.count_from_table(&from.select, scope);
                    for join in &from.joins {
                        count += self.count_from_table(&join.table, scope);
                        if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                            count += self.count_expr(expr, scope);
                        }
                    }
                }
                for column in columns {
                    if let ast::ResultColumn::Expr(expr, _) = column {
                        count += self.count_expr(expr, scope);
                    }
                }
                if let Some(expr) = where_clause {
                    count += self.count_expr(expr, scope);
                }
                if let Some(group_by) = group_by {
                    for expr in &group_by.exprs {
                        count += self.count_expr(expr, scope);
                    }
                    if let Some(having) = &group_by.having {
                        count += self.count_expr(having, scope);
                    }
                }
                for window in window_clause {
                    count += self.count_window(&window.window, scope);
                }
                count
            }
            ast::OneSelect::Values(rows) => rows
                .iter()
                .flatten()
                .map(|expr| self.count_expr(expr, scope))
                .sum(),
        }
    }

    fn count_from_table(&self, table: &ast::SelectTable, scope: &mut RecursiveRefScope) -> usize {
        match table {
            ast::SelectTable::Table(name, _, _) => {
                if name.db_name.is_none() {
                    self.name_weight(&crate::util::normalize_ident(name.name.as_str()), scope)
                } else {
                    0
                }
            }
            ast::SelectTable::TableCall(name, args, _) => {
                let mut count = if self.count_table_calls && name.db_name.is_none() {
                    self.name_weight(&crate::util::normalize_ident(name.name.as_str()), scope)
                } else {
                    0
                };
                count += args
                    .iter()
                    .map(|expr| self.count_expr(expr, scope))
                    .sum::<usize>();
                count
            }
            ast::SelectTable::Select(select, _) => self.count_select(select, scope),
            ast::SelectTable::Sub(from, _) => {
                self.count_from_table(&from.select, scope)
                    + from
                        .joins
                        .iter()
                        .map(|join| {
                            self.count_from_table(&join.table, scope)
                                + match &join.constraint {
                                    Some(ast::JoinConstraint::On(expr)) => {
                                        self.count_expr(expr, scope)
                                    }
                                    _ => 0,
                                }
                        })
                        .sum::<usize>()
            }
        }
    }

    fn count_window(&self, window: &ast::Window, scope: &mut RecursiveRefScope) -> usize {
        let mut count = window
            .partition_by
            .iter()
            .map(|expr| self.count_expr(expr, scope))
            .sum::<usize>();
        count += window
            .order_by
            .iter()
            .map(|sorted| self.count_expr(&sorted.expr, scope))
            .sum::<usize>();
        if let Some(frame) = &window.frame_clause {
            for bound in std::iter::once(&frame.start).chain(frame.end.as_ref()) {
                if let ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) = bound {
                    count += self.count_expr(expr, scope);
                }
            }
        }
        count
    }

    fn count_expr(&self, expr: &ast::Expr, scope: &mut RecursiveRefScope) -> usize {
        let mut count = 0;
        let _ = walk_expr(expr, &mut |node: &ast::Expr| -> Result<WalkControl> {
            match node {
                ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
                    count += self.count_select(select, scope);
                    Ok(WalkControl::SkipChildren)
                }
                ast::Expr::InSelect { rhs, .. } => {
                    count += self.count_select(rhs, scope);
                    Ok(WalkControl::Continue)
                }
                ast::Expr::InTable { rhs, args, .. }
                    if !self.count_table_calls && rhs.db_name.is_none() && args.is_empty() =>
                {
                    count +=
                        self.name_weight(&crate::util::normalize_ident(rhs.name.as_str()), scope);
                    Ok(WalkControl::Continue)
                }
                _ => Ok(WalkControl::Continue),
            }
        });
        count
    }

    fn count_arm(&self, one: &ast::OneSelect, scope: &mut RecursiveRefScope) -> (usize, usize) {
        fn count_direct(
            counter: &RecursiveRefCounter<'_>,
            table: &ast::SelectTable,
            scope: &RecursiveRefScope,
        ) -> usize {
            match table {
                ast::SelectTable::Table(name, _, _) | ast::SelectTable::TableCall(name, _, _) => {
                    if name.db_name.is_some() {
                        return 0;
                    }
                    let name = crate::util::normalize_ident(name.name.as_str());
                    usize::from(
                        name == counter.cte_name
                            && !scope.iter().any(|(scope_name, _)| *scope_name == name),
                    )
                }
                ast::SelectTable::Select(_, _) => 0,
                ast::SelectTable::Sub(from, _) => {
                    count_direct(counter, &from.select, scope)
                        + from
                            .joins
                            .iter()
                            .map(|join| count_direct(counter, &join.table, scope))
                            .sum::<usize>()
                }
            }
        }

        let top_level = if let ast::OneSelect::Select {
            from: Some(from), ..
        } = one
        {
            count_direct(self, &from.select, scope)
                + from
                    .joins
                    .iter()
                    .map(|join| count_direct(self, &join.table, scope))
                    .sum::<usize>()
        } else {
            0
        };
        (top_level, self.count_one_select(one, scope))
    }
}
