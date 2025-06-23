use std::fmt::Display;

use crate::{
    ast::{self, fmt::ToTokens},
    to_sql_string::{ToSqlContext, ToSqlString},
};

impl ToSqlString for ast::Select {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = Vec::new();
        if let Some(with) = &self.with {
            ret.push(with.to_sql_string(context));
        }

        ret.push(self.body.to_sql_string(context));

        if let Some(order_by) = &self.order_by {
            // TODO: SortedColumn missing collation in ast
            let joined_cols = order_by
                .iter()
                .map(|col| col.to_sql_string(context))
                .collect::<Vec<_>>()
                .join(", ");
            ret.push(format!("ORDER BY {joined_cols}"));
        }
        if let Some(limit) = &self.limit {
            ret.push(limit.to_sql_string(context));
        }
        ret.join(" ")
    }
}

impl ToSqlString for ast::SelectBody {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = self.select.to_sql_string(context);

        if let Some(compounds) = &self.compounds {
            ret.push(' ');
            let compound_selects = compounds
                .iter()
                .map(|compound_select| {
                    let mut curr = compound_select.operator.to_string();
                    curr.push(' ');
                    curr.push_str(&compound_select.select.to_sql_string(context));
                    curr
                })
                .collect::<Vec<_>>()
                .join(" ");
            ret.push_str(&compound_selects);
        }
        ret
    }
}

impl ToSqlString for ast::OneSelect {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        match self {
            ast::OneSelect::Select(select) => select.to_sql_string(context),
            ast::OneSelect::Values(values) => {
                let joined_values = values
                    .iter()
                    .map(|value| {
                        let joined_value = value
                            .iter()
                            .map(|e| e.to_sql_string(context))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({joined_value})")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("VALUES {joined_values}")
            }
        }
    }
}

impl ToSqlString for ast::SelectInner {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        dbg!(&self);
        let mut ret = Vec::with_capacity(2 + self.columns.len());
        ret.push("SELECT".to_string());
        if let Some(distinct) = self.distinctness {
            ret.push(distinct.to_string());
        }
        let joined_cols = self
            .columns
            .iter()
            .map(|col| col.to_sql_string(context))
            .collect::<Vec<_>>()
            .join(", ");
        ret.push(joined_cols);

        if let Some(from) = &self.from {
            ret.push(from.to_sql_string(context));
        }
        if let Some(where_expr) = &self.where_clause {
            ret.push("WHERE".to_string());
            ret.push(where_expr.to_sql_string(context));
        }
        if let Some(group_by) = &self.group_by {
            ret.push(group_by.to_sql_string(context));
        }
        if let Some(window_clause) = &self.window_clause {
            ret.push("WINDOW".to_string());
            let joined_window = window_clause
                .iter()
                .map(|window_def| window_def.to_sql_string(context))
                .collect::<Vec<_>>()
                .join(",");
            ret.push(joined_window);
        }

        ret.join(" ")
    }
}

impl ToSqlString for ast::FromClause {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::from("FROM");
        if let Some(select_table) = &self.select {
            ret.push(' ');
            ret.push_str(&select_table.to_sql_string(context));
        }
        if let Some(joins) = &self.joins {
            ret.push(' ');
            let joined_joins = joins
                .iter()
                .map(|join| {
                    let mut curr = join.operator.to_string();
                    curr.push(' ');
                    curr.push_str(&join.table.to_sql_string(context));
                    if let Some(join_constraint) = &join.constraint {
                        curr.push(' ');
                        curr.push_str(&join_constraint.to_sql_string(context));
                    }
                    curr
                })
                .collect::<Vec<_>>()
                .join(" ");
            ret.push_str(&joined_joins);
        }
        ret
    }
}

impl ToSqlString for ast::SelectTable {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::new();
        match self {
            Self::Table(name, alias, indexed) => {
                ret.push_str(&name.to_sql_string(context));
                if let Some(alias) = alias {
                    ret.push(' ');
                    ret.push_str(&alias.to_string());
                }
                if let Some(indexed) = indexed {
                    ret.push(' ');
                    ret.push_str(&indexed.to_string());
                }
            }
            Self::TableCall(table_func, args, alias) => {
                ret.push_str(&table_func.to_sql_string(context));
                if let Some(args) = args {
                    ret.push(' ');
                    let joined_args = args
                        .iter()
                        .map(|arg| arg.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ");
                    ret.push_str(&joined_args);
                }
                if let Some(alias) = alias {
                    ret.push(' ');
                    ret.push_str(&alias.to_string());
                }
            }
            Self::Select(select, alias) => {
                ret.push('(');
                ret.push_str(&select.to_sql_string(context));
                ret.push(')');
                if let Some(alias) = alias {
                    ret.push(' ');
                    ret.push_str(&alias.to_string());
                }
            }
            Self::Sub(from_clause, alias) => {
                ret.push('(');
                ret.push_str(&from_clause.to_sql_string(context));
                ret.push(')');
                if let Some(alias) = alias {
                    ret.push(' ');
                    ret.push_str(&alias.to_string());
                }
            }
        }
        ret
    }
}

impl ToSqlString for ast::With {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        format!(
            "WITH{} {}",
            if self.recursive { " RECURSIVE " } else { "" },
            self.ctes
                .iter()
                .map(|cte| cte.to_sql_string(context))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl ToSqlString for ast::Limit {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        format!(
            "LIMIT {}{}",
            self.expr.to_sql_string(context),
            self.offset
                .as_ref()
                .map_or("".to_string(), |offset| format!(
                    " OFFSET {}",
                    offset.to_sql_string(context)
                ))
        )
        // TODO: missing , + expr in ast
    }
}

impl ToSqlString for ast::CommonTableExpr {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = Vec::with_capacity(self.columns.as_ref().map_or(2, |cols| cols.len()));
        ret.push(self.tbl_name.0.clone());
        if let Some(cols) = &self.columns {
            let joined_cols = cols
                .iter()
                .map(|col| col.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            ret.push(format!("({joined_cols})"));
        }
        ret.push(format!(
            "AS {}({})",
            {
                let mut materialized = self.materialized.to_string();
                if !materialized.is_empty() {
                    materialized.push(' ');
                }
                materialized
            },
            self.select.to_sql_string(context)
        ));
        ret.join(" ")
    }
}

impl Display for ast::IndexedColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.col_name.0)
    }
}

impl ToSqlString for ast::SortedColumn {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut curr = self.expr.to_sql_string(context);
        if let Some(sort_order) = self.order {
            curr.push(' ');
            curr.push_str(&sort_order.to_string());
        }
        if let Some(nulls_order) = self.nulls {
            curr.push(' ');
            curr.push_str(&nulls_order.to_string());
        }
        curr
    }
}

impl Display for ast::SortOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_fmt(f)
    }
}

impl Display for ast::NullsOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_fmt(f)
    }
}

impl Display for ast::Materialized {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Any => "",
            Self::No => "NOT MATERIALIZED",
            Self::Yes => "MATERIALIZED",
        };
        write!(f, "{value}")
    }
}

impl ToSqlString for ast::ResultColumn {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::new();
        match self {
            Self::Expr(expr, alias) => {
                ret.push_str(&expr.to_sql_string(context));
                if let Some(alias) = alias {
                    ret.push(' ');
                    ret.push_str(&alias.to_string());
                }
            }
            Self::Star => {
                ret.push('*');
            }
            Self::TableStar(name) => {
                ret.push_str(&format!("{}.*", name.0));
            }
        }
        ret
    }
}

impl Display for ast::As {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::As(alias) => {
                    format!("AS {}", alias.0)
                }
                Self::Elided(alias) => alias.0.clone(),
            }
        )
    }
}

impl Display for ast::Indexed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::NotIndexed => "NOT INDEXED".to_string(),
                Self::IndexedBy(name) => format!("INDEXED BY {}", name.0),
            }
        )
    }
}

impl Display for ast::JoinOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Comma => ",".to_string(),
                Self::TypedJoin(join) => {
                    let join_keyword = "JOIN";
                    if let Some(join) = join {
                        format!("{join} {join_keyword}")
                    } else {
                        join_keyword.to_string()
                    }
                }
            }
        )
    }
}

impl Display for ast::JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = {
            let mut modifiers = Vec::new();
            if self.contains(Self::NATURAL) {
                modifiers.push("NATURAL");
            }
            if self.contains(Self::LEFT) || self.contains(Self::RIGHT) {
                // TODO: I think the parser incorrectly asigns outer to every LEFT and RIGHT query
                if self.contains(Self::LEFT | Self::RIGHT) {
                    modifiers.push("FULL");
                } else if self.contains(Self::LEFT) {
                    modifiers.push("LEFT");
                } else if self.contains(Self::RIGHT) {
                    modifiers.push("RIGHT");
                }
                // FIXME: ignore outer joins as I think they are parsed incorrectly in the bitflags
                // if self.contains(Self::OUTER) {
                //     modifiers.push("OUTER");
                // }
            }

            if self.contains(Self::INNER) {
                modifiers.push("INNER");
            }
            if self.contains(Self::CROSS) {
                modifiers.push("CROSS");
            }
            modifiers.join(" ")
        };
        write!(f, "{value}")
    }
}

impl ToSqlString for ast::JoinConstraint {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::On(expr) => {
                format!("ON {}", expr.to_sql_string(context))
            }
            Self::Using(col_names) => {
                let joined_names = col_names
                    .iter()
                    .map(|col| col.0.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                format!("USING ({joined_names})")
            }
        }
    }
}

impl ToSqlString for ast::GroupBy {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::from("GROUP BY ");
        let curr = self
            .exprs
            .iter()
            .map(|expr| expr.to_sql_string(context))
            .collect::<Vec<_>>()
            .join(",");
        ret.push_str(&curr);
        if let Some(having) = &self.having {
            ret.push_str(&format!(" HAVING {}", having.to_sql_string(context)));
        }
        ret
    }
}

impl ToSqlString for ast::WindowDef {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        format!("{} AS {}", self.name.0, self.window.to_sql_string(context))
    }
}

impl ToSqlString for ast::Window {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = Vec::new();
        if let Some(name) = &self.base {
            ret.push(name.0.clone());
        }
        if let Some(partition) = &self.partition_by {
            let joined_exprs = partition
                .iter()
                .map(|e| e.to_sql_string(context))
                .collect::<Vec<_>>()
                .join(",");
            ret.push(format!("PARTITION BY {joined_exprs}"));
        }
        if let Some(order_by) = &self.order_by {
            let joined_cols = order_by
                .iter()
                .map(|col| col.to_sql_string(context))
                .collect::<Vec<_>>()
                .join(", ");
            ret.push(format!("ORDER BY {joined_cols}"));
        }
        if let Some(frame_claue) = &self.frame_clause {
            ret.push(frame_claue.to_sql_string(context));
        }
        format!("({})", ret.join(" "))
    }
}

impl ToSqlString for ast::FrameClause {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = Vec::new();
        ret.push(self.mode.to_string());
        let start_sql = self.start.to_sql_string(context);
        if let Some(end) = &self.end {
            ret.push(format!(
                "BETWEEN {} AND {}",
                start_sql,
                end.to_sql_string(context)
            ));
        } else {
            ret.push(start_sql);
        }
        if let Some(exclude) = &self.exclude {
            ret.push(exclude.to_string());
        }

        ret.join(" ")
    }
}

impl Display for ast::FrameMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_fmt(f)
    }
}

impl ToSqlString for ast::FrameBound {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::CurrentRow => "CURRENT ROW".to_string(),
            Self::Following(expr) => format!("{} FOLLOWING", expr.to_sql_string(context)),
            Self::Preceding(expr) => format!("{} PRECEDING", expr.to_sql_string(context)),
            Self::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
            Self::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
        }
    }
}

impl Display for ast::FrameExclude {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", {
            let clause = match self {
                Self::CurrentRow => "CURRENT ROW",
                Self::Group => "GROUP",
                Self::NoOthers => "NO OTHERS",
                Self::Ties => "TIES",
            };
            format!("EXCLUDE {clause}")
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    to_sql_string_test!(test_select_basic, "SELECT 1;");

    to_sql_string_test!(test_select_table, "SELECT * FROM t;");

    to_sql_string_test!(test_select_table_2, "SELECT a FROM t;");

    to_sql_string_test!(test_select_multiple_columns, "SELECT a, b, c FROM t;");

    to_sql_string_test!(test_select_with_alias, "SELECT a AS col1 FROM t;");

    to_sql_string_test!(test_select_with_table_alias, "SELECT t1.a FROM t AS t1;");

    to_sql_string_test!(test_select_with_where, "SELECT a FROM t WHERE b = 1;");

    to_sql_string_test!(
        test_select_with_multiple_conditions,
        "SELECT a FROM t WHERE b = 1 AND c > 2;"
    );

    to_sql_string_test!(
        test_select_with_order_by,
        "SELECT a FROM t ORDER BY a DESC;"
    );

    to_sql_string_test!(test_select_with_limit, "SELECT a FROM t LIMIT 10;");

    to_sql_string_test!(
        test_select_with_offset,
        "SELECT a FROM t LIMIT 10 OFFSET 5;"
    );

    to_sql_string_test!(
        test_select_with_join,
        "SELECT a FROM t JOIN t2 ON t.b = t2.b;"
    );

    to_sql_string_test!(
        test_select_with_group_by,
        "SELECT a, COUNT(*) FROM t GROUP BY a;"
    );

    to_sql_string_test!(
        test_select_with_having,
        "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1;"
    );

    to_sql_string_test!(test_select_with_distinct, "SELECT DISTINCT a FROM t;");

    to_sql_string_test!(test_select_with_function, "SELECT COUNT(a) FROM t;");

    to_sql_string_test!(
        test_select_with_subquery,
        "SELECT a FROM (SELECT b FROM t) AS sub;"
    );

    to_sql_string_test!(
        test_select_nested_subquery,
        "SELECT a FROM (SELECT b FROM (SELECT c FROM t WHERE c > 10) AS sub1 WHERE b < 20) AS sub2;"
    );

    to_sql_string_test!(
        test_select_multiple_joins,
        "SELECT t1.a, t2.b, t3.c FROM t1 JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id;"
    );

    to_sql_string_test!(
        test_select_with_cte,
        "WITH cte AS (SELECT a FROM t WHERE b = 1) SELECT a FROM cte WHERE a > 10;"
    );

    to_sql_string_test!(
        test_select_with_window_function,
        "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) AS rn FROM t;"
    );

    to_sql_string_test!(
        test_select_with_complex_where,
        "SELECT a FROM t WHERE b IN (1, 2, 3) AND c BETWEEN 10 AND 20 OR d IS NULL;"
    );

    to_sql_string_test!(
        test_select_with_case,
        "SELECT CASE WHEN a > 0 THEN 'positive' ELSE 'non-positive' END AS result FROM t;"
    );

    to_sql_string_test!(test_select_with_aggregate_and_join, "SELECT t1.a, COUNT(t2.b) FROM t1 LEFT JOIN t2 ON t1.id = t2.id GROUP BY t1.a HAVING COUNT(t2.b) > 5;");

    to_sql_string_test!(test_select_with_multiple_ctes, "WITH cte1 AS (SELECT a FROM t WHERE b = 1), cte2 AS (SELECT c FROM t2 WHERE d = 2) SELECT cte1.a, cte2.c FROM cte1 JOIN cte2 ON cte1.a = cte2.c;");

    to_sql_string_test!(
        test_select_with_union,
        "SELECT a FROM t1 UNION SELECT b FROM t2;"
    );

    to_sql_string_test!(
        test_select_with_union_all,
        "SELECT a FROM t1 UNION ALL SELECT b FROM t2;"
    );

    to_sql_string_test!(
        test_select_with_exists,
        "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.b = t.a);"
    );

    to_sql_string_test!(
        test_select_with_correlated_subquery,
        "SELECT a, (SELECT COUNT(*) FROM t2 WHERE t2.b = t.a) AS count_b FROM t;"
    );

    to_sql_string_test!(
        test_select_with_complex_order_by,
        "SELECT a, b FROM t ORDER BY CASE WHEN a IS NULL THEN 1 ELSE 0 END, b ASC, c DESC;"
    );

    to_sql_string_test!(
        test_select_with_full_outer_join,
        "SELECT t1.a, t2.b FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;",
        ignore = "OUTER JOIN is incorrectly parsed in parser"
    );

    to_sql_string_test!(test_select_with_aggregate_window, "SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS running_sum FROM t;");

    to_sql_string_test!(
        test_select_with_exclude,
        "SELECT 
    c.name,
    o.order_id,
    o.order_amount,
    SUM(o.order_amount) OVER (PARTITION BY c.id
        ORDER BY o.order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        EXCLUDE CURRENT ROW) AS running_total_excluding_current
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE EXISTS (SELECT 1
    FROM orders o2
    WHERE o2.customer_id = c.id
    AND o2.order_amount > 1000);"
    );
}
