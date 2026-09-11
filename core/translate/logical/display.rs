//! Text form of a tree, for logs and tests.

use std::fmt::{self, Display, Formatter};

use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{
    fmt::{ToSqlContext, ToTokens},
    Expr, SortOrder, TableInternalId,
};

use crate::translate::plan::JoinType;

use super::{Block, LogicalPlan};

#[derive(Default)]
struct Names {
    tables: HashMap<TableInternalId, (String, Vec<Option<String>>)>,
}

impl Names {
    fn collect(&mut self, block: &Block) {
        for reference in &block.outer_query_refs {
            self.tables.entry(reference.internal_id).or_insert_with(|| {
                (
                    reference.identifier.clone(),
                    reference
                        .columns()
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                )
            });
        }
        self.collect_node(&block.root);
    }

    fn collect_node(&mut self, node: &LogicalPlan) {
        let mut nested = Vec::new();
        node.for_each_node(&mut |node| match node {
            LogicalPlan::Scan(scan) => {
                self.tables.insert(
                    scan.table.internal_id,
                    (
                        scan.table.identifier.clone(),
                        scan.table
                            .columns()
                            .iter()
                            .map(|column| column.name.clone())
                            .collect(),
                    ),
                );
            }
            LogicalPlan::DerivedTable(derived) => {
                let columns = match &derived.shell {
                    Some(table) => table
                        .columns()
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                    None => projected_names(&derived.block),
                };
                self.tables
                    .insert(derived.internal_id, (derived.identifier.clone(), columns));
                nested.push(&derived.block);
            }
            _ => {}
        });
        for block in nested {
            self.collect(block);
        }
    }
}

fn projected_names(block: &Block) -> Vec<Option<String>> {
    let mut node = &block.root;
    loop {
        match node {
            LogicalPlan::Project(project) => {
                return project
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        Some(
                            column
                                .alias
                                .clone()
                                .unwrap_or_else(|| format!("column{}", index + 1)),
                        )
                    })
                    .collect();
            }
            LogicalPlan::Limit(next) => node = &next.input,
            LogicalPlan::Sort(next) => node = &next.input,
            LogicalPlan::Distinct(next) => node = &next.input,
            _ => return Vec::new(),
        }
    }
}

impl ToSqlContext for Names {
    fn get_table_name(&self, id: TableInternalId) -> Option<&str> {
        self.tables.get(&id).map(|(name, _)| name.as_str())
    }

    fn get_column_name(&self, table_id: TableInternalId, col_idx: usize) -> Option<Option<&str>> {
        let (_, columns) = self.tables.get(&table_id)?;
        columns.get(col_idx).map(|name| name.as_deref())
    }
}

impl Display for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut names = Names::default();
        names.collect(self);
        write_block(f, self, &names, 0)
    }
}

fn write_block(f: &mut Formatter<'_>, block: &Block, names: &Names, depth: usize) -> fmt::Result {
    write_node(f, &block.root, names, depth)?;
    for subquery in &block.subqueries {
        let correlated = if subquery.correlated {
            "correlated"
        } else {
            "uncorrelated"
        };
        writeln!(
            f,
            "{}Subquery {} ({correlated})",
            "  ".repeat(depth),
            subquery.internal_id
        )?;
    }
    Ok(())
}

fn write_node(
    f: &mut Formatter<'_>,
    node: &LogicalPlan,
    names: &Names,
    depth: usize,
) -> fmt::Result {
    let indent = "  ".repeat(depth);
    match node {
        LogicalPlan::OneRow => writeln!(f, "{indent}OneRow"),
        LogicalPlan::Scan(scan) => {
            let table_name = scan.table.table.get_name();
            if table_name == scan.table.identifier {
                writeln!(f, "{indent}Scan {table_name}")
            } else {
                writeln!(f, "{indent}Scan {table_name} AS {}", scan.table.identifier)
            }
        }
        LogicalPlan::DerivedTable(derived) => {
            writeln!(f, "{indent}DerivedTable {}", derived.identifier)?;
            write_block(f, &derived.block, names, depth + 1)
        }
        LogicalPlan::Join(join) => {
            let kind = match join.info.join_type {
                JoinType::Inner if join.info.no_reorder => "Cross",
                JoinType::Inner => "Inner",
                JoinType::LeftOuter => "Left",
                JoinType::FullOuter => "Full",
                JoinType::Semi => "Semi",
                JoinType::Anti => "Anti",
            };
            writeln!(f, "{indent}Join {kind}")?;
            write_node(f, &join.left, names, depth + 1)?;
            write_node(f, &join.right, names, depth + 1)
        }
        LogicalPlan::Filter(filter) => {
            write!(f, "{indent}Filter [")?;
            for (index, term) in filter.terms.iter().enumerate() {
                if index > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", term.expr.displayer(names))?;
                if let Some(table_id) = term.from_outer_join {
                    let name = names.get_table_name(table_id).unwrap_or("?");
                    write!(f, " (ON {name})")?;
                }
            }
            writeln!(f, "]")?;
            write_node(f, &filter.input, names, depth + 1)
        }
        LogicalPlan::Aggregate(aggregate) => {
            write!(f, "{indent}Aggregate")?;
            if let Some(group_by) = &aggregate.group_by {
                write_exprs(f, " GROUP BY ", &group_by.exprs, names)?;
                if let Some(having) = &group_by.having {
                    write_exprs(f, " HAVING ", having, names)?;
                }
            }
            let calls: Vec<&Expr> = aggregate
                .aggregates
                .iter()
                .map(|aggregate| &aggregate.original_expr)
                .collect();
            write!(f, " [")?;
            for (index, call) in calls.iter().enumerate() {
                if index > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", call.displayer(names))?;
            }
            writeln!(f, "]")?;
            write_node(f, &aggregate.input, names, depth + 1)
        }
        LogicalPlan::Project(project) => {
            write!(f, "{indent}Project [")?;
            for (index, column) in project.columns.iter().enumerate() {
                if index > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", column.expr.displayer(names))?;
                if let Some(alias) = &column.alias {
                    write!(f, " AS {alias}")?;
                }
            }
            writeln!(f, "]")?;
            write_node(f, &project.input, names, depth + 1)
        }
        LogicalPlan::Distinct(distinct) => {
            writeln!(f, "{indent}Distinct")?;
            write_node(f, &distinct.input, names, depth + 1)
        }
        LogicalPlan::Sort(sort) => {
            write!(f, "{indent}Sort [")?;
            for (index, (expr, order, _)) in sort.keys.iter().enumerate() {
                if index > 0 {
                    write!(f, ", ")?;
                }
                let direction = match order {
                    SortOrder::Asc => "ASC",
                    SortOrder::Desc => "DESC",
                };
                write!(f, "{} {direction}", expr.displayer(names))?;
            }
            writeln!(f, "]")?;
            write_node(f, &sort.input, names, depth + 1)
        }
        LogicalPlan::Limit(limit) => {
            write!(f, "{indent}Limit")?;
            if let Some(expr) = &limit.limit {
                write!(f, " {}", expr.displayer(names))?;
            }
            if let Some(expr) = &limit.offset {
                write!(f, " OFFSET {}", expr.displayer(names))?;
            }
            writeln!(f)?;
            write_node(f, &limit.input, names, depth + 1)
        }
    }
}

fn write_exprs(f: &mut Formatter<'_>, prefix: &str, exprs: &[Expr], names: &Names) -> fmt::Result {
    if exprs.is_empty() {
        return Ok(());
    }
    write!(f, "{prefix}[")?;
    for (index, expr) in exprs.iter().enumerate() {
        if index > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{}", expr.displayer(names))?;
    }
    write!(f, "]")
}
