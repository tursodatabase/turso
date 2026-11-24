use crate::ast::{
    self,
    fmt::{BlankContext, SqlWriter, ToSqlContext, WriteSql},
    SortOrder, TableInternalId,
};
use core::fmt;
use std::fmt::{Display, Formatter};

use crate::{schema::Table, translate::plan::TableReferences};

use super::plan::{
    Aggregate, DeletePlan, JoinedTable, MultiIndexScanOp, Operation, Plan, ResultSetColumn, Scan,
    Search, SeekDef, SelectPlan, SetOperation, UpdatePlan,
};

fn fmt_order_by_item(
    f: &mut fmt::Formatter<'_>,
    expr: &impl fmt::Display,
    dir: SortOrder,
    nulls: Option<crate::ast::NullsOrder>,
) -> fmt::Result {
    let dir_str = match dir {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };
    match nulls {
        Some(crate::ast::NullsOrder::First) => {
            writeln!(f, "  - {expr} {dir_str} NULLS FIRST")
        }
        Some(crate::ast::NullsOrder::Last) => writeln!(f, "  - {expr} {dir_str} NULLS LAST"),
        None => writeln!(f, "  - {expr} {dir_str}"),
    }
}

/// Build SQLite-style constraint annotation string for an index seek.
/// e.g. "(label=? AND fromId>?)"
pub(crate) fn seek_constraint_annotation(
    index: Option<&crate::schema::Index>,
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
            Self::Delete(delete_plan) => delete_plan.fmt(f),
            Self::Update(update_plan) => update_plan.fmt(f),
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
                            let constraints = match search {
                                Search::Seek { seek_def, .. } => {
                                    seek_constraint_annotation(None, seek_def)
                                }
                                _ => " (rowid=?)".to_string(),
                            };
                            writeln!(
                                f,
                                "{indent}SEARCH {} USING INTEGER PRIMARY KEY{constraints}{left_join_suffix}",
                                reference.identifier
                            )?;
                        }
                        Search::Seek {
                            index: Some(index),
                            seek_def,
                        } => {
                            let constraints = seek_constraint_annotation(Some(index), seek_def);
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
                    write_multi_index_scan(f, &indent, &reference.identifier, multi_idx)?;
                    writeln!(f, " ")?;
                }
            }
        }
        if self.distinctness.is_distinct() {
            writeln!(f, "USE HASH TABLE FOR DISTINCT")?;
        }
        Ok(())
    }
}

impl Display for DeletePlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        // Delete plan should only have one table reference
        if let Some(reference) = self.table_references.joined_tables().first() {
            let indent = "`--";

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
                                        "{indent}DELETE FROM {table_name} USING COVERING INDEX {}",
                                        index.name
                                    )?;
                                } else {
                                    writeln!(
                                        f,
                                        "{indent}DELETE FROM {table_name} USING INDEX {}",
                                        index.name
                                    )?;
                                }
                            } else {
                                writeln!(f, "{indent}DELETE FROM {table_name}")?;
                            }
                        }
                        Scan::VirtualTable { .. }
                        | Scan::Subquery { .. }
                        | Scan::RecursiveCteInput => {
                            writeln!(f, "{indent}DELETE FROM {table_name}")?;
                        }
                    }
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. }
                    | Search::Seek { index: None, .. }
                    | Search::InSeek { index: None, .. } => {
                        let constraints = match search {
                            Search::Seek { seek_def, .. } => {
                                seek_constraint_annotation(None, seek_def)
                            }
                            _ => " (rowid=?)".to_string(),
                        };
                        writeln!(
                            f,
                            "{}SEARCH {} USING INTEGER PRIMARY KEY{constraints}",
                            indent, reference.identifier
                        )?;
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}",
                            indent, reference.identifier, index.name
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
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}{constraint}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                },
                Operation::IndexMethodQuery(query) => {
                    let module = query.index.index_method.as_ref().unwrap();
                    writeln!(
                        f,
                        "{}QUERY MODULE {}",
                        indent,
                        module.definition().method_name
                    )?;
                }
                Operation::HashJoin(_) => {
                    unreachable!("Delete plan should not have hash joins");
                }
                Operation::MultiIndexScan(multi_idx) => {
                    write_multi_index_scan(f, indent, &reference.identifier, multi_idx)?;
                    writeln!(f)?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for UpdatePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        let read_scope_tables = self.build_read_scope_tables();

        for (i, reference) in read_scope_tables.joined_tables().iter().enumerate() {
            let is_last = i == read_scope_tables.joined_tables().len() - 1;
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
                            let action = if i == 0 { "UPDATE" } else { "SCAN" };
                            if let Some(index) = index {
                                if reference.utilizes_covering_index() {
                                    writeln!(
                                        f,
                                        "{indent}{action} {table_name} USING COVERING INDEX {}",
                                        index.name
                                    )?;
                                } else {
                                    writeln!(
                                        f,
                                        "{indent}{action} {table_name} USING INDEX {}",
                                        index.name
                                    )?;
                                }
                            } else {
                                writeln!(f, "{indent}{action} {table_name}")?;
                            }
                        }
                        Scan::VirtualTable { .. }
                        | Scan::Subquery { .. }
                        | Scan::RecursiveCteInput => {
                            if i == 0 {
                                writeln!(f, "{indent}UPDATE {table_name}")?;
                            } else {
                                writeln!(f, "{indent}SCAN {table_name}")?;
                            }
                        }
                    }
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. }
                    | Search::Seek { index: None, .. }
                    | Search::InSeek { index: None, .. } => {
                        let constraints = match search {
                            Search::Seek { seek_def, .. } => {
                                seek_constraint_annotation(None, seek_def)
                            }
                            _ => " (rowid=?)".to_string(),
                        };
                        writeln!(
                            f,
                            "{}SEARCH {} USING INTEGER PRIMARY KEY{constraints}",
                            indent, reference.identifier
                        )?;
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}",
                            indent, reference.identifier, index.name
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
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}{constraint}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                },
                Operation::IndexMethodQuery(query) => {
                    let module = query.index.index_method.as_ref().unwrap();
                    writeln!(
                        f,
                        "{}QUERY MODULE {}",
                        indent,
                        module.definition().method_name
                    )?;
                }
                Operation::HashJoin(_) => {
                    unreachable!("Update plan should not have hash joins");
                }
                Operation::MultiIndexScan(multi_idx) => {
                    write_multi_index_scan(f, &indent, &reference.identifier, multi_idx)?;
                    writeln!(f)?;
                }
            }
        }
        if let Some(ret) = &self.returning {
            writeln!(f, "RETURNING:")?;
            for col in ret {
                writeln!(f, "  - {}", col.expr)?;
            }
        }

        Ok(())
    }
}

fn write_multi_index_scan(
    f: &mut Formatter<'_>,
    indent: &str,
    table_identifier: &str,
    multi_idx: &MultiIndexScanOp,
) -> fmt::Result {
    let op_name = match multi_idx.set_op {
        SetOperation::Union => "MULTI-INDEX OR",
        SetOperation::Intersection { .. } => "MULTI-INDEX AND",
    };
    write!(f, "{indent}{op_name} {table_identifier} (")?;
    for (branch_idx, branch) in multi_idx.branches.iter().enumerate() {
        if branch_idx > 0 {
            f.write_str(", ")?;
        }
        let index_name = branch
            .index
            .as_ref()
            .map(|index| index.name.as_str())
            .unwrap_or("PRIMARY KEY");
        f.write_str(index_name)?;
    }
    f.write_str(")")
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

impl WriteSql for Plan {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        match self {
            Self::Select(select) => {
                select.write_sql(s, &PlanContext(&[&select.table_references]))?;
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
                    plan.write_sql(s, context)?;
                    operator.write_sql(s, context)?;
                }

                right_most.write_sql(s, context)?;

                if let Some(order_by) = order_by {
                    s.keyword("ORDER")?;
                    s.keyword("BY")?;

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
                    s.keyword("LIMIT")?;
                    s.text(&limit.to_string())?;
                }

                if let Some(offset) = &offset {
                    s.keyword("OFFSET")?;
                    s.text(&offset.to_string())?;
                }
            }
            Self::RecursiveCte(plan) => {
                plan.initial_query.write_sql(s, context)?;
                if plan.union_all {
                    ast::CompoundOperator::UnionAll.write_sql(s, context)?;
                } else {
                    ast::CompoundOperator::Union.write_sql(s, context)?;
                }
                plan.recursive_query.write_sql(s, context)?;
            }
            Self::Delete(delete) => delete.write_sql(s, context)?,
            Self::Update(update) => update.write_sql(s, context)?,
        }

        Ok(())
    }
}

impl Display for JoinedTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.displayer(&BlankContext).fmt(f)
    }
}

impl WriteSql for JoinedTable {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _context: &C) -> fmt::Result {
        match &self.table {
            Table::BTree(..) | Table::Virtual(..) | Table::RecursiveCteInput(..) => {
                let name = self.table.get_name();
                s.text(name)?;
                if self.identifier != name {
                    s.keyword("AS")?;
                    s.text(&self.identifier)?;
                }
            }
            Table::FromClauseSubquery(from_clause_subquery) => {
                s.punct("(")?;
                // Plan::write_sql creates its own context internally, so we pass BlankContext here.
                from_clause_subquery.plan.write_sql(s, &BlankContext)?;
                s.punct(")")?;

                s.keyword("AS")?;
                s.text(&self.identifier)?;
            }
        };

        Ok(())
    }
}

// TODO: currently cannot print the original CTE as it is optimized into a subquery
impl WriteSql for SelectPlan {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, context: &C) -> fmt::Result {
        if !self.values.is_empty() {
            ast::OneSelect::Values(self.values.clone()).write_sql(s, context)?;
        } else {
            s.keyword("SELECT")?;
            if self.distinctness.is_distinct() {
                s.keyword("DISTINCT")?;
            }

            for (i, ResultSetColumn { expr, alias, .. }) in self.result_columns.iter().enumerate() {
                if i != 0 {
                    s.punct(",")?;
                }

                expr.write_sql(s, context)?;
                if let Some(alias) = alias {
                    s.keyword("AS")?;
                    s.text(alias)?;
                }
            }
            s.keyword("FROM")?;

            for (i, order) in self.join_order.iter().enumerate() {
                if i != 0 {
                    if order.is_outer {
                        s.keyword("ORDER")?;
                    }
                    s.keyword("JOIN")?;
                }

                let table_ref = self.joined_tables().get(order.original_idx).unwrap();
                table_ref.write_sql(s, context)?;
            }

            if !self.where_clause.is_empty() {
                s.keyword("WHERE")?;

                for (i, expr) in self
                    .where_clause
                    .iter()
                    .map(|where_clause| where_clause.expr.clone())
                    .enumerate()
                {
                    if i != 0 {
                        s.keyword("AND")?;
                    }
                    expr.write_sql(s, context)?;
                }
            }

            if let Some(group_by) = &self.group_by {
                if !group_by.exprs.is_empty() {
                    s.keyword("GROUP")?;
                    s.keyword("BY")?;

                    s.comma(group_by.exprs.iter(), context)?;
                }

                // TODO: not sure where I need to place the group_by.sort_order
                if let Some(having) = &group_by.having {
                    s.keyword("HAVING")?;

                    for (i, expr) in having.iter().enumerate() {
                        if i != 0 {
                            s.keyword("AND")?;
                        }
                        expr.write_sql(s, context)?;
                    }
                }
            }
        }

        if let Some(window) = &self.window {
            if let Some(window_name) = &window.name {
                s.keyword("WINDOW")?;
                s.text(window_name)?;
                s.keyword("AS")?;

                s.punct("(")?;

                if !window.partition_by.is_empty() {
                    s.keyword("PARTITION")?;
                    s.keyword("BY")?;
                    s.comma(window.partition_by.iter(), context)?;
                }

                if !window.order_by.is_empty() {
                    s.keyword("ORDER")?;
                    s.keyword("BY")?;
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

                s.punct(")")?;
            }
        }

        if !self.order_by.is_empty() {
            s.keyword("ORDER")?;
            s.keyword("BY")?;

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
            s.keyword("LIMIT")?;
            s.text(&limit.to_string())?;
        }

        if let Some(offset) = &self.offset {
            s.keyword("OFFSET")?;
            s.text(&offset.to_string())?;
        }

        Ok(())
    }
}

impl WriteSql for DeletePlan {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        let table = self
            .table_references
            .joined_tables()
            .first()
            .expect("Delete Plan should have only one table reference");
        let context = &[&self.table_references];
        let context = &PlanContext(context);

        s.keyword("DELETE")?;
        s.keyword("FROM")?;
        s.text(table.table.get_name())?;

        if !self.where_clause.is_empty() {
            s.keyword("WHERE")?;

            for (i, expr) in self
                .where_clause
                .iter()
                .map(|where_clause| where_clause.expr.clone())
                .enumerate()
            {
                if i != 0 {
                    s.keyword("AND")?;
                }
                expr.write_sql(s, context)?;
            }
        }

        Ok(())
    }
}

impl WriteSql for UpdatePlan {
    fn write_sql<C: ToSqlContext>(&self, s: &mut SqlWriter<'_>, _: &C) -> fmt::Result {
        let table = &self.target_table;
        let read_scope_tables = self.build_read_scope_tables();
        let context = [&read_scope_tables];
        let context = &PlanContext(&context);

        s.keyword("UPDATE")?;
        s.text(table.table.get_name())?;
        s.keyword("SET")?;

        s.comma(
            self.set_clauses.iter().map(|set_clause| {
                let col_name = table
                    .table
                    .get_column_at(set_clause.column_index)
                    .as_ref()
                    .unwrap()
                    .name
                    .as_ref()
                    .unwrap();

                ast::Set {
                    col_names: vec![ast::Name::exact(col_name.clone())],
                    expr: set_clause.expr.clone(),
                }
            }),
            context,
        )?;

        if !self.where_clause.is_empty() {
            s.keyword("WHERE")?;

            let mut iter = self
                .where_clause
                .iter()
                .map(|where_clause| where_clause.expr.clone());
            iter.next()
                .expect("should not be empty")
                .write_sql(s, context)?;
            for expr in iter {
                s.keyword("AND")?;
                expr.write_sql(s, context)?;
            }
        }

        Ok(())
    }
}
