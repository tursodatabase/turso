use crate::Result;
use turso_parser::ast;

pub fn desugar_outer_joins(select: &mut ast::Select) -> Result<()> {
    desugar_one_select(&mut select.body.select)?;
    for compound in &mut select.body.compounds {
        desugar_one_select(&mut compound.select)?;
    }
    Ok(())
}

fn desugar_one_select(select: &mut ast::OneSelect) -> Result<()> {
    if let ast::OneSelect::Select { columns, from, .. } = select {
        if let Some(from_clause) = from {
            let has_right_join = from_clause.joins.iter().any(|j| {
                if let ast::JoinOperator::TypedJoin(Some(join_type)) = &j.operator {
                    join_type.contains(ast::JoinType::RIGHT)
                } else {
                    false
                }
            });

            if !has_right_join {
                return Ok(());
            }

            // Expand Star
            let mut new_columns = vec![];
            for col in columns.iter() {
                if let ast::ResultColumn::Star = col {
                    let names = get_top_level_table_names(from_clause);
                    for name in names {
                        new_columns.push(ast::ResultColumn::TableStar(name));
                    }
                } else {
                    new_columns.push(col.clone());
                }
            }
            *columns = new_columns;

            // Rewrite RIGHT JOIN to LEFT JOIN by folding the joins left-to-right
            let mut current_select = from_clause.select.clone();
            let mut current_joins = vec![];

            for join in from_clause.joins.drain(..) {
                let is_right = if let ast::JoinOperator::TypedJoin(Some(join_type)) = &join.operator {
                    join_type.contains(ast::JoinType::RIGHT) && !join_type.contains(ast::JoinType::LEFT)
                } else {
                    false
                };

                if is_right {
                    let new_join_type = ast::JoinType::LEFT | ast::JoinType::OUTER;
                    let new_operator = ast::JoinOperator::TypedJoin(Some(new_join_type));
                    
                    // Group everything accumulated so far
                    let lhs = if current_joins.is_empty() {
                        current_select
                    } else {
                        Box::new(ast::SelectTable::Sub(
                            ast::FromClause {
                                select: current_select,
                                joins: current_joins,
                            },
                            None,
                        ))
                    };
                    
                    // The RHS becomes the new base select, and we LEFT JOIN the LHS
                    current_select = join.table.clone();
                    current_joins = vec![ast::JoinedSelectTable {
                        operator: new_operator,
                        table: lhs,
                        constraint: join.constraint.clone(),
                    }];
                } else {
                    current_joins.push(join);
                }
            }

            from_clause.select = current_select;
            from_clause.joins = current_joins;
        }
    }
    Ok(())
}

fn get_top_level_table_names(from: &ast::FromClause) -> Vec<ast::Name> {
    let mut names = vec![];
    names.push(get_select_table_name(&from.select));
    for join in &from.joins {
        names.push(get_select_table_name(&join.table));
    }
    names
}

fn get_select_table_name(table: &ast::SelectTable) -> ast::Name {
    match table {
        ast::SelectTable::Table(qname, alias, _) => {
            alias.as_ref().map(|a| match a {
                ast::As::As(n) => n.clone(),
                ast::As::Elided(n) => n.clone(),
            }).unwrap_or_else(|| qname.name.clone())
        }
        ast::SelectTable::TableCall(qname, _, alias) => {
            alias.as_ref().map(|a| match a {
                ast::As::As(n) => n.clone(),
                ast::As::Elided(n) => n.clone(),
            }).unwrap_or_else(|| qname.name.clone())
        }
        ast::SelectTable::Select(_, alias) | ast::SelectTable::Sub(_, alias) => {
            alias.as_ref().map(|a| match a {
                ast::As::As(n) => n.clone(),
                ast::As::Elided(n) => n.clone(),
            }).unwrap_or_else(|| ast::Name::exact("subquery".to_string()))
        }
    }
}
