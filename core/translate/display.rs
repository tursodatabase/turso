use core::fmt;
use std::fmt::{Display, Formatter};
use turso_parser::{
    ast::{
        self,
        fmt::{BlankContext, ToSqlContext, ToTokens, TokenStream},
        SortOrder, TableInternalId,
    },
    token::TokenType,
};

use crate::{schema::Table, translate::plan::TableReferences};

use super::plan::{
    Aggregate, JoinedTable, Operation, Plan, ResultSetColumn, Scan, Search, SeekDef, SelectPlan,
    SetOperation,
};

fn fmt_order_by_item(
    f: &mut fmt::Formatter<'_>,
    expr: &impl fmt::Display,
    dir: SortOrder,
    nulls: Option<turso_parser::ast::NullsOrder>,
) -> fmt::Result {
    let dir_str = match dir {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };
    match nulls {
        Some(turso_parser::ast::NullsOrder::First) => {
            writeln!(f, "  - {expr} {dir_str} NULLS FIRST")
        }
        Some(turso_parser::ast::NullsOrder::Last) => writeln!(f, "  - {expr} {dir_str} NULLS LAST"),
        None => writeln!(f, "  - {expr} {dir_str}"),
    }
}

/// Build SQLite-style constraint annotation string for an index seek.
/// e.g. "(label=? AND fromId>?)"
pub(crate) fn seek_constraint_annotation(
    index: &crate::schema::Index,
    seek_def: &SeekDef,
) -> String {
    let parts = super::eqp::seek_constraint_parts(index, seek_def);
    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" AND "))
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let args_str = self
            .args
            .iter()
            .map(|arg| arg.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{:?}({})", self.func, args_str)
    }
}

/// For EXPLAIN QUERY PLAN
impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select(select_plan) => select_plan.fmt(f),
            Self::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => {
                for (plan, operator) in left {
                    plan.fmt(f)?;
                    writeln!(f, "{operator}")?;
                }
                right_most.fmt(f)?;
                if let Some(limit) = limit {
                    writeln!(f, "LIMIT: {limit}")?;
                }
                if let Some(offset) = offset {
                    writeln!(f, "OFFSET: {offset}")?;
                }
                if let Some(order_by) = order_by {
                    writeln!(f, "ORDER BY:")?;
                    for (expr, dir, nulls, _) in order_by {
                        fmt_order_by_item(f, expr, *dir, *nulls)?;
                    }
                }
                Ok(())
            }
            Self::RecursiveCte(plan) => {
                writeln!(f, "RECURSIVE CTE {}", plan.name)?;
                writeln!(f, "INITIAL QUERY:")?;
                plan.initial_query.fmt(f)?;
                writeln!(f, "RECURSIVE QUERY:")?;
                plan.recursive_query.fmt(f)
            }
        }
    }
}

impl Display for SelectPlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        // Print each table reference with appropriate indentation based on join depth
        for (i, member) in self.join_order.iter().enumerate() {
            let reference = &self.table_references.joined_tables()[member.original_idx];
            let is_last = i == self.join_order.len() - 1;
            let indent = if i == 0 {
                if is_last { "`--" } else { "|--" }.to_string()
            } else {
                format!(
                    "   {}{}",
                    "|  ".repeat(i - 1),
                    if is_last { "`--" } else { "|--" }
                )
            };

            match &reference.op {
                Operation::Scan(scan) => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    match scan {
                        Scan::BTreeTable { index, .. } => {
                            if let Some(index) = index {
                                if reference.utilizes_covering_index() {
                                    writeln!(
                                        f,
                                        "{indent}SCAN {table_name} USING COVERING INDEX {}",
                                        index.name
                                    )?;
                                } else {
                                    writeln!(
                                        f,
                                        "{indent}SCAN {table_name} USING INDEX {}",
                                        index.name
                                    )?;
                                }
                            } else {
                                writeln!(f, "{indent}SCAN {table_name}")?;
                            }
                        }
                        Scan::VirtualTable { .. }
                        | Scan::Subquery { .. }
                        | Scan::RecursiveCteInput => {
                            writeln!(f, "{indent}SCAN {table_name}")?;
                        }
                    }
                }
                Operation::Search(search) => {
                    let left_join_suffix = if member.is_outer { " LEFT-JOIN" } else { "" };
                    match search {
                        Search::RowidEq { .. }
                        | Search::Seek { index: None, .. }
                        | Search::InSeek { index: None, .. } => {
                            writeln!(
                                f,
                                "{indent}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?){left_join_suffix}",
                                reference.identifier
                            )?;
                        }
                        Search::Seek {
                            index: Some(index),
                            seek_def,
                        } => {
                            let constraints = seek_constraint_annotation(index, seek_def);
                            let covering = if reference.utilizes_covering_index() {
                                "COVERING "
                            } else {
                                ""
                            };
                            writeln!(
                                f,
                                "{indent}SEARCH {} USING {covering}INDEX {}{constraints}{left_join_suffix}",
                                reference.identifier, index.name
                            )?;
                        }
                        Search::InSeek {
                            index: Some(index), ..
                        } => {
                            let constraint = if let Some(col) = index.columns.first() {
                                format!(" ({}=?)", col.name)
                            } else {
                                String::new()
                            };
                            let covering = if reference.utilizes_covering_index() {
                                "COVERING "
                            } else {
                                ""
                            };
                            writeln!(
                                f,
                                "{indent}SEARCH {} USING {covering}INDEX {}{constraint}{left_join_suffix}",
                                reference.identifier, index.name
                            )?;
                        }
                    }
                }
                Operation::IndexMethodQuery(query) => {
                    let index_method = query.index.index_method.as_ref().unwrap();
                    writeln!(
                        f,
                        "{}QUERY INDEX METHOD {}",
                        indent,
                        index_method.definition().method_name
                    )?;
                }
                Operation::HashJoin(_) => {
                    writeln!(f, "{indent}HASH JOIN")?;
                }
                Operation::MultiIndexScan(multi_idx) => {
                    let index_names: Vec<&str> = multi_idx
                        .branches
                        .iter()
                        .map(|b| {
                            b.index
                                .as_ref()
                                .map(|i| i.name.as_str())
                                .unwrap_or("PRIMARY KEY")
                        })
                        .collect();
                    let op_name = match multi_idx.set_op {
                        SetOperation::Union => "MULTI-INDEX OR",
                        SetOperation::Intersection { .. } => "MULTI-INDEX AND",
                    };
                    writeln!(
                        f,
                        "{indent}{op_name} {} ({}) ",
                        reference.identifier,
                        index_names.join(", ")
                    )?;
                }
            }
        }
        if self.distinctness.is_distinct() {
            writeln!(f, "USE HASH TABLE FOR DISTINCT")?;
        }
        Ok(())
    }
}

pub struct PlanContext<'a>(pub &'a [&'a TableReferences]);

// Definitely not perfect yet
impl ToSqlContext for PlanContext<'_> {
    fn get_column_name(&self, table_id: TableInternalId, col_idx: usize) -> Option<Option<&str>> {
        let (_, table) = self
            .0
            .iter()
            .find_map(|table_ref| table_ref.find_table_by_internal_id(table_id))?;
        let cols = table.columns();
        cols.get(col_idx)
            .map(|col| col.name.as_ref().map(|name| name.as_ref()))
    }

    fn get_table_name(&self, id: TableInternalId) -> Option<&str> {
        let table_ref = self
            .0
            .iter()
            .find(|table_ref| table_ref.find_table_by_internal_id(id).is_some())?;
        let joined_table = table_ref.find_joined_table_by_internal_id(id);
        let outer_query = table_ref.find_outer_query_ref_by_internal_id(id);
        match (joined_table, outer_query) {
            (Some(table), None) => Some(&table.identifier),
            (None, Some(table)) => Some(&table.identifier),
            (Some(table), Some(_)) => Some(&table.identifier),
            (None, None) => unreachable!(),
        }
    }
}

impl ToTokens for Plan {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Select(select) => {
                select.to_tokens(s, &PlanContext(&[&select.table_references]))?;
            }
            Self::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => {
                let all_refs = left
                    .iter()
                    .flat_map(|(plan, _)| std::iter::once(&plan.table_references))
                    .chain(std::iter::once(&right_most.table_references))
                    .collect::<Vec<_>>();
                let context = &PlanContext(all_refs.as_slice());

                for (plan, operator) in left {
                    plan.to_tokens(s, context)?;
                    operator.to_tokens(s, context)?;
                }

                right_most.to_tokens(s, context)?;

                if let Some(order_by) = order_by {
                    s.append(TokenType::TK_ORDER, None)?;
                    s.append(TokenType::TK_BY, None)?;

                    s.comma(
                        order_by
                            .iter()
                            .map(|(col_idx, order, nulls, _)| ast::SortedColumn {
                                expr: Box::new(ast::Expr::Literal(ast::Literal::Numeric(
                                    (col_idx + 1).to_string(),
                                ))),
                                order: Some(*order),
                                nulls: *nulls,
                            }),
                        context,
                    )?;
                }

                if let Some(limit) = &limit {
                    s.append(TokenType::TK_LIMIT, None)?;
                    s.append(TokenType::TK_FLOAT, Some(&limit.to_string()))?;
                }

                if let Some(offset) = &offset {
                    s.append(TokenType::TK_OFFSET, None)?;
                    s.append(TokenType::TK_FLOAT, Some(&offset.to_string()))?;
                }
            }
            Self::RecursiveCte(plan) => {
                plan.initial_query.to_tokens(s, context)?;
                if plan.union_all {
                    ast::CompoundOperator::UnionAll.to_tokens(s, context)?;
                } else {
                    ast::CompoundOperator::Union.to_tokens(s, context)?;
                }
                plan.recursive_query.to_tokens(s, context)?;
            }
        }

        Ok(())
    }
}

impl Display for JoinedTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.displayer(&BlankContext).fmt(f)
    }
}

impl ToTokens for JoinedTable {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _context: &C,
    ) -> Result<(), S::Error> {
        match &self.table {
            Table::BTree(..) | Table::Virtual(..) | Table::RecursiveCteInput(..) => {
                let name = self.table.get_name();
                s.append(TokenType::TK_ID, Some(name))?;
                if self.identifier != name {
                    s.append(TokenType::TK_AS, None)?;
                    s.append(TokenType::TK_ID, Some(&self.identifier))?;
                }
            }
            Table::FromClauseSubquery(from_clause_subquery) => {
                s.append(TokenType::TK_LP, None)?;
                // Plan::to_tokens creates its own context internally, so we pass BlankContext here.
                from_clause_subquery.plan.to_tokens(s, &BlankContext)?;
                s.append(TokenType::TK_RP, None)?;

                s.append(TokenType::TK_AS, None)?;
                s.append(TokenType::TK_ID, Some(&self.identifier))?;
            }
        };

        Ok(())
    }
}

// TODO: currently cannot print the original CTE as it is optimized into a subquery
impl ToTokens for SelectPlan {
    fn to_tokens<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if !self.values.is_empty() {
            ast::OneSelect::Values(
                self.values
                    .iter()
                    .map(|values| values.iter().map(|v| Box::from(v.clone())).collect())
                    .collect(),
            )
            .to_tokens(s, context)?;
        } else {
            s.append(TokenType::TK_SELECT, None)?;
            if self.distinctness.is_distinct() {
                s.append(TokenType::TK_DISTINCT, None)?;
            }

            for (i, ResultSetColumn { expr, alias, .. }) in self.result_columns.iter().enumerate() {
                if i != 0 {
                    s.append(TokenType::TK_COMMA, None)?;
                }

                expr.to_tokens(s, context)?;
                if let Some(alias) = alias {
                    s.append(TokenType::TK_AS, None)?;
                    s.append(TokenType::TK_ID, Some(alias))?;
                }
            }
            s.append(TokenType::TK_FROM, None)?;

            for (i, order) in self.join_order.iter().enumerate() {
                if i != 0 {
                    if order.is_outer {
                        s.append(TokenType::TK_ORDER, None)?;
                    }
                    s.append(TokenType::TK_JOIN, None)?;
                }

                let table_ref = self.joined_tables().get(order.original_idx).unwrap();
                table_ref.to_tokens(s, context)?;
            }

            if !self.where_clause.is_empty() {
                s.append(TokenType::TK_WHERE, None)?;

                for (i, expr) in self
                    .where_clause
                    .iter()
                    .map(|where_clause| where_clause.expr.clone())
                    .enumerate()
                {
                    if i != 0 {
                        s.append(TokenType::TK_AND, None)?;
                    }
                    expr.to_tokens(s, context)?;
                }
            }

            if let Some(group_by) = &self.group_by {
                if !group_by.exprs.is_empty() {
                    s.append(TokenType::TK_GROUP, None)?;
                    s.append(TokenType::TK_BY, None)?;

                    s.comma(group_by.exprs.iter(), context)?;
                }

                // TODO: not sure where I need to place the group_by.sort_order
                if let Some(having) = &group_by.having {
                    s.append(TokenType::TK_HAVING, None)?;

                    for (i, expr) in having.iter().enumerate() {
                        if i != 0 {
                            s.append(TokenType::TK_AND, None)?;
                        }
                        expr.to_tokens(s, context)?;
                    }
                }
            }
        }

        if let Some(window) = &self.window {
            if let Some(window_name) = &window.name {
                s.append(TokenType::TK_WINDOW, None)?;
                s.append(TokenType::TK_ID, Some(window_name))?;
                s.append(TokenType::TK_AS, None)?;

                s.append(TokenType::TK_LP, None)?;

                if !window.partition_by.is_empty() {
                    s.append(TokenType::TK_PARTITION, None)?;
                    s.append(TokenType::TK_BY, None)?;
                    s.comma(window.partition_by.iter(), context)?;
                }

                if !window.order_by.is_empty() {
                    s.append(TokenType::TK_ORDER, None)?;
                    s.append(TokenType::TK_BY, None)?;
                    s.comma(
                        window
                            .order_by
                            .iter()
                            .map(|(expr, order, nulls)| ast::SortedColumn {
                                expr: Box::new(expr.clone()),
                                order: Some(*order),
                                nulls: *nulls,
                            }),
                        context,
                    )?;
                }

                s.append(TokenType::TK_RP, None)?;
            }
        }

        if !self.order_by.is_empty() {
            s.append(TokenType::TK_ORDER, None)?;
            s.append(TokenType::TK_BY, None)?;

            s.comma(
                self.order_by
                    .iter()
                    .map(|(expr, order, nulls)| ast::SortedColumn {
                        expr: expr.clone(),
                        order: Some(*order),
                        nulls: *nulls,
                    }),
                context,
            )?;
        }

        if let Some(limit) = &self.limit {
            s.append(TokenType::TK_LIMIT, None)?;
            s.append(TokenType::TK_FLOAT, Some(&limit.to_string()))?;
        }

        if let Some(offset) = &self.offset {
            s.append(TokenType::TK_OFFSET, None)?;
            s.append(TokenType::TK_FLOAT, Some(&offset.to_string()))?;
        }

        Ok(())
    }
}
