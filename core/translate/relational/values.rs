use crate::translate::{
    emitter::Resolver,
    plan::{select_column_affinity, ResultSetColumn, SelectPlan},
};

use super::{BindError, Column, ColumnId, Scalar};

#[derive(Clone, Debug)]
pub(crate) struct Values {
    pub rows: Vec<Vec<Scalar>>,
    pub columns: Vec<Column>,
}

impl Values {
    pub(super) fn bind(
        plan: &SelectPlan,
        resolver: &Resolver,
        relation: turso_parser::ast::TableInternalId,
    ) -> std::result::Result<Self, BindError> {
        assert!(!plan.values.is_empty(), "VALUES has at least one row");
        let rows = plan
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| Scalar::bind(expr.clone(), &plan.table_references, resolver))
                    .collect::<std::result::Result<Vec<_>, _>>()
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let columns = plan
            .result_columns
            .iter()
            .enumerate()
            .map(|(position, output)| Column {
                id: ColumnId {
                    relation,
                    position: Some(position),
                },
                name: output.name_or_expr(&plan.table_references),
                nullable: rows.iter().any(|row| row[position].nullable),
                affinity: select_column_affinity(plan, position, &plan.values[0][position]),
                collation: rows[0][position].collation,
            })
            .collect();
        Ok(Self { rows, columns })
    }

    pub(super) fn lower(self, plan: &mut SelectPlan) {
        plan.values = self
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(Scalar::into_ast).collect())
            .collect();
        plan.result_columns = self
            .columns
            .into_iter()
            .zip(&plan.values[0])
            .map(|(column, expr)| ResultSetColumn {
                expr: expr.clone(),
                alias: Some(column.name),
                implicit_column_name: None,
                contains_aggregates: false,
            })
            .collect();
    }
}
