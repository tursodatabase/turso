use crate::translate::{
    collate::CollationSeq,
    plan::{compound_column_affinity, CompoundOrderByKey},
};

use super::super::SetOperation;

use super::{
    ast, bind_optional, select_outputs, BindError, Builder, Column, Expr, Relation, Scalar,
    SelectPlan,
};

impl Builder<'_, '_> {
    pub(super) fn compound(
        &mut self,
        left: &[(SelectPlan, ast::CompoundOperator)],
        right_most: &SelectPlan,
        limit: Option<&Expr>,
        offset: Option<&Expr>,
        order_by: Option<&[CompoundOrderByKey]>,
    ) -> std::result::Result<Relation, BindError> {
        assert!(!left.is_empty(), "compound query has a left input");
        let inputs: Vec<_> = left
            .iter()
            .map(|(plan, _)| plan)
            .chain(std::iter::once(right_most))
            .collect();
        let bound_inputs = inputs
            .iter()
            .map(|input| self.select(input, false))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut columns: Vec<Column> = select_outputs(&bound_inputs[0])
            .iter()
            .map(|output| output.column.clone())
            .collect();
        let comparison_collations: Vec<_> = (0..columns.len())
            .map(|position| {
                bound_inputs
                    .iter()
                    .map(|input| select_outputs(input)[position].column.collation)
                    .find(|collation| *collation != CollationSeq::Unset)
                    .unwrap_or(CollationSeq::Binary)
            })
            .collect();
        let mut bound_inputs = bound_inputs.into_iter();
        let mut result = bound_inputs
            .next()
            .expect("compound query has a left input");
        for (index, ((_, operator), right)) in left.iter().zip(bound_inputs).enumerate() {
            let right_outputs = select_outputs(&right);
            assert_eq!(columns.len(), right_outputs.len());
            let next_output = self
                .next_output
                .as_mut()
                .expect("query initialized output identities");
            let relation = (*next_output).into();
            *next_output += 1;
            for (position, column) in columns.iter_mut().enumerate() {
                column.id.relation = relation;
                column.nullable |= right_outputs[position].column.nullable;
                column.affinity = compound_column_affinity(&inputs[..index + 2], position);
            }
            result = Relation::Set {
                left: Box::new(result),
                right: Box::new(right),
                operation: Box::new(SetOperation {
                    operator: *operator,
                    outputs: columns.clone(),
                    comparison_collations: comparison_collations.clone(),
                }),
            };
        }
        if let Some(order_by) = order_by {
            let keys = order_by
                .iter()
                .map(|(position, direction, nulls, collation)| {
                    let column = columns
                        .get(*position)
                        .expect("bound compound ordering references a result column");
                    (
                        Scalar::result_column(column, *collation, comparison_collations[*position]),
                        *direction,
                        *nulls,
                    )
                })
                .collect();
            result = Relation::Sort {
                input: Box::new(result),
                keys,
            };
        }
        if limit.is_some() || offset.is_some() {
            result = Relation::Limit {
                input: Box::new(result),
                limit: bind_optional(limit, &right_most.table_references, self.resolver)?,
                offset: bind_optional(offset, &right_most.table_references, self.resolver)?,
            };
        }
        Ok(result)
    }
}
