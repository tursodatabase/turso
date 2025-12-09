use serde::{Deserialize, Serialize};
use turso_parser::ast::{Expr, Name, QualifiedName, SortOrder, SortedColumn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CreateIndex {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<(String, SortOrder)>,
}

impl
    From<(
        bool,
        bool,
        QualifiedName,
        Name,
        Vec<SortedColumn>,
        Option<Box<Expr>>,
    )> for CreateIndex
{
    fn from(
        (unique, if_not_exists, idx_name, tbl_name, columns, where_clause): (
            bool,
            bool,
            QualifiedName,
            Name,
            Vec<SortedColumn>,
            Option<Box<Expr>>,
        ),
    ) -> Self {
        if unique {
            todo!("UNIQUE indexes are not supported");
        }

        if if_not_exists {
            todo!("IF NOT EXISTS in CREATE INDEX not supported");
        }

        if where_clause.is_some() {
            todo!("Partial indexes (WHERE clause) are not supported");
        }

        let columns = columns
            .into_iter()
            .map(|col| {
                (
                    match *col.expr {
                        Expr::Id(name) => name.to_string(),
                        _ => {
                            todo!(
                                "Only column names are supported in CREATE INDEX, got: {:?}",
                                col.expr
                            )
                        }
                    },
                    col.order.unwrap_or(SortOrder::Asc),
                )
            })
            .collect::<Vec<(String, SortOrder)>>();

        CreateIndex {
            index_name: idx_name.to_string(),
            table_name: tbl_name.to_string(),
            columns,
        }
    }
}

impl std::fmt::Display for CreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE INDEX {} ON {} ({})",
            self.index_name,
            self.table_name,
            self.columns
                .iter()
                .map(|(name, order)| format!("{name} {order}"))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
