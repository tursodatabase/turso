//! Basic UPDATE conversion into closed semantic HIR.

use turso_parser::ast;

use super::{
    analyze::Analyzer,
    dml::{trigger_matches_update, trigger_targets_database},
    expr::ExprPolicies,
    hir::{self, HirRoot, SourceOwner},
    query::FromContext,
    scope::Scope,
};
use crate::{function::ScalarFunc, schema::Table, util::normalize_ident, LimboError, Result};

#[derive(Clone, Copy)]
pub(super) struct UpdateExprContext<'scope> {
    pub(super) outer_scope: Option<&'scope Scope>,
    pub(super) policies: ExprPolicies,
}

pub(super) struct UpdateBodySyntax<'ast> {
    pub(super) conflict: Option<ast::ResolveType>,
    pub(super) assignments: &'ast [ast::Set],
    pub(super) from: Option<&'ast ast::FromClause>,
    pub(super) predicate: Option<&'ast ast::Expr>,
    pub(super) returning: &'ast [ast::ResultColumn],
}

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn analyze_update(&mut self, syntax: &'ast ast::Update) -> Result<HirRoot> {
        self.with_cte_scope(syntax.with.as_ref(), |analyzer| {
            let target = analyzer.analyze_base_table_source(
                &syntax.tbl_name,
                None,
                syntax.indexed.as_ref(),
                SourceOwner::Root,
            )?;
            analyzer.analyze_update_target(
                target,
                UpdateBodySyntax {
                    conflict: syntax.or_conflict,
                    assignments: &syntax.sets,
                    from: syntax.from.as_ref(),
                    predicate: syntax.where_clause.as_deref(),
                    returning: &syntax.returning,
                },
                UpdateExprContext {
                    outer_scope: None,
                    policies: ExprPolicies::statement(analyzer.context().dqs_dml()),
                },
            )
        })
    }

    pub(super) fn analyze_update_target(
        &mut self,
        target: hir::SourceId,
        syntax: UpdateBodySyntax<'ast>,
        expressions: UpdateExprContext<'_>,
    ) -> Result<HirRoot> {
        let table = match &self
            .source(target)
            .ok_or_else(|| LimboError::InternalError(format!("missing UPDATE target {target}")))?
            .kind
        {
            hir::SourceKind::Table(table) => table.clone(),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "UPDATE target {target} is not a catalog table"
                )));
            }
        };
        let virtual_target = table.value().virtual_table().is_some();
        if table.value().btree().is_some_and(|btree| !btree.has_rowid) {
            return Err(LimboError::ParseError(
                "UPDATE of WITHOUT ROWID tables is not supported".to_string(),
            ));
        }
        let new_source = self.create_update_new_source(target, &table)?;
        if !virtual_target {
            self.analyze_btree_write_metadata(target, &table)?;
            self.analyze_btree_write_metadata(new_source, &table)?;
        }

        let (from, from_scope) = match syntax.from {
            Some(syntax) => {
                let (from, scope) = self.analyze_from_clause(
                    syntax,
                    FromContext::Dml {
                        outer_scope: expressions.outer_scope,
                        policies: expressions.policies,
                    },
                )?;
                (Some(from), Some(scope))
            }
            None => (None, None),
        };
        let mut scope = self.update_read_scope(target)?;
        if let Some(from_scope) = from_scope {
            scope.append_local(from_scope.without_outer());
        }
        scope.set_outer(expressions.outer_scope);
        let assignments = self.analyze_update_assignments(
            syntax.assignments,
            new_source,
            table.value(),
            &scope,
            expressions.policies,
        )?;
        let target_kind = if virtual_target {
            hir::UpdateTargetKind::Virtual
        } else {
            hir::UpdateTargetKind::BTree {
                defaults: Vec::new(),
                triggers: self.analyze_update_triggers(&table, &assignments)?,
                foreign_keys: self.analyze_dml_foreign_keys(&table, new_source)?,
            }
        };
        let returning = self.analyze_dml_returning_with_policies(
            syntax.returning,
            new_source,
            expressions.policies,
        )?;
        let predicate = syntax
            .predicate
            .map(|syntax| {
                self.analyze_root_expr(syntax, &scope, expressions.policies.where_clause())
                    .map(|resolved| resolved.expr)
            })
            .transpose()?;

        Ok(HirRoot::Update(hir::Update {
            target,
            new_source,
            target_kind,
            from,
            assignments,
            predicate,
            order_by: Vec::new(),
            limit: None,
            conflict: syntax.conflict,
            returning,
            cdc_updates_override: None,
        }))
    }

    fn analyze_update_triggers(
        &mut self,
        table: &hir::ResolvedTable,
        assignments: &[hir::Assignment],
    ) -> Result<Vec<hir::ResolvedTrigger>> {
        let database = table
            .database()
            .expect("an UPDATE target table must have an owning database");
        let updated_columns = assignments
            .iter()
            .flat_map(|assignment| assignment.columns.iter())
            .filter_map(|column| match column {
                hir::TargetColumn::Column(column) => Some(*column),
                hir::TargetColumn::RowId => None,
            })
            .collect::<Vec<_>>();
        let triggers = self
            .context()
            .schema(database)?
            .get_triggers_for_table(table.value().get_name())
            .filter(|trigger| {
                trigger_targets_database(trigger, database)
                    && trigger_matches_update(trigger, table.value(), &updated_columns)
            })
            .cloned()
            .collect::<Vec<_>>();
        Ok(triggers
            .into_iter()
            .map(|trigger| self.freeze_trigger(database, trigger))
            .collect())
    }

    fn create_update_new_source(
        &mut self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
    ) -> Result<hir::SourceId> {
        let old = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing UPDATE target source {target}"))
        })?;
        let columns = old.columns.clone();
        let generated_expressions = old.generated_expressions.clone();
        let default_expressions = old.default_expressions.clone();
        let column_type_programs = old.column_type_programs.clone();
        let rowid_available = old.rowid_available;
        let new_source = self.reserve_source();
        self.insert_source(
            new_source,
            hir::Source {
                id: new_source,
                owner: SourceOwner::Root,
                database: table.database(),
                name: table.value().get_name().to_string(),
                alias: None,
                kind: hir::SourceKind::Pseudo {
                    kind: hir::PseudoSource::New,
                    table: table.clone(),
                },
                columns,
                generated_expressions,
                default_expressions,
                column_type_programs,
                check_constraints: None,
                rowid_available,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(new_source)
    }

    fn update_read_scope(&self, target: hir::SourceId) -> Result<Scope> {
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing UPDATE target source {target}"))
        })?;
        let mut scope = Scope::default();
        scope.add_source(source, true);
        Ok(scope)
    }

    fn analyze_update_assignments(
        &mut self,
        sets: &'ast [ast::Set],
        new_source: hir::SourceId,
        table: &Table,
        scope: &Scope,
        policies: ExprPolicies,
    ) -> Result<Vec<hir::Assignment>> {
        let mut assignments = Vec::<hir::Assignment>::new();
        for set in sets {
            if set.col_names.len() > 1 {
                if let Some(select) = row_assignment_subquery(&set.expr) {
                    let columns = set
                        .col_names
                        .iter()
                        .map(|name| resolve_update_target(table, name))
                        .collect::<Result<Vec<_>>>()?;
                    let expected_outputs = columns
                        .iter()
                        .map(|column| self.write_target_type(new_source, table, *column))
                        .collect::<Result<Vec<_>>>()?;
                    let query = self.analyze_subquery_with_expected_outputs(
                        select,
                        None,
                        scope,
                        &expected_outputs,
                        policies,
                    )?;
                    let output_width = self
                        .query(query)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "missing UPDATE assignment query {query}"
                            ))
                        })?
                        .output
                        .len();
                    if columns.len() != output_width {
                        crate::bail_parse_error!(
                            "{} columns assigned {} values",
                            columns.len(),
                            output_width
                        );
                    }
                    merge_update_row_assignment(&mut assignments, columns, query)?;
                    continue;
                }
            }

            let values: Vec<&ast::Expr> = match set.expr.as_ref() {
                ast::Expr::Parenthesized(values) => {
                    if set.col_names.len() != values.len() {
                        crate::bail_parse_error!(
                            "{} columns assigned {} values",
                            set.col_names.len(),
                            values.len()
                        );
                    }
                    values.iter().map(Box::as_ref).collect()
                }
                expression => {
                    if set.col_names.len() != 1 {
                        crate::bail_parse_error!(
                            "{} columns assigned 1 values",
                            set.col_names.len()
                        );
                    }
                    vec![expression]
                }
            };

            for (name, syntax) in set.col_names.iter().zip(values) {
                let column = resolve_update_target(table, name)?;
                let value = if matches!(syntax, ast::Expr::Default) {
                    self.analyze_write_default(new_source, table, column)?
                } else {
                    self.analyze_root_expr_with_expected_type(
                        syntax,
                        scope,
                        policies.update(),
                        self.write_target_type(new_source, table, column)?,
                    )?
                    .expr
                };
                merge_update_assignment(&mut assignments, column, value)?;
            }
        }
        Ok(assignments)
    }
}

fn resolve_update_target(table: &Table, name: &ast::Name) -> Result<hir::TargetColumn> {
    let normalized = normalize_ident(name.as_str());
    if let Some((column, definition)) = table.get_column_by_name(&normalized) {
        definition.ensure_not_generated("UPDATE", name.as_str())?;
        return Ok(hir::TargetColumn::Column(column));
    }
    if (table.btree().is_some_and(|btree| btree.has_rowid) || table.virtual_table().is_some())
        && ["rowid", "_rowid_", "oid"]
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&normalized))
    {
        return Ok(table
            .columns()
            .iter()
            .position(|column| column.is_rowid_alias())
            .map_or(hir::TargetColumn::RowId, hir::TargetColumn::Column));
    }
    crate::bail_parse_error!("no such column: {}.{}", table.get_name(), name)
}

fn merge_update_assignment(
    assignments: &mut Vec<hir::Assignment>,
    column: hir::TargetColumn,
    mut value: hir::Expr,
) -> Result<()> {
    split_overlapping_row_assignment(assignments, column)?;
    let Some(existing) = assignments
        .iter_mut()
        .find(|assignment| assignment.columns == [column])
    else {
        assignments.push(hir::Assignment {
            columns: vec![column],
            value,
        });
        return Ok(());
    };

    if let hir::Expr::Function(call) = &mut value {
        if matches!(
            call.function.value(),
            crate::Func::Scalar(ScalarFunc::ArraySetElement)
        ) {
            if let hir::FunctionArguments::Expressions { values, .. } = &mut call.arguments {
                if values.len() == 3 {
                    values[0] = existing.value.clone();
                }
            }
        }
    }
    existing.value = value;
    Ok(())
}

fn merge_update_row_assignment(
    assignments: &mut Vec<hir::Assignment>,
    columns: Vec<hir::TargetColumn>,
    query: hir::QueryId,
) -> Result<()> {
    if columns
        .iter()
        .enumerate()
        .any(|(index, column)| columns[..index].contains(column))
    {
        for (output, column) in columns.into_iter().enumerate() {
            merge_update_assignment(
                assignments,
                column,
                hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }),
            )?;
        }
        return Ok(());
    }

    for column in &columns {
        split_overlapping_row_assignment(assignments, *column)?;
    }
    assignments.retain(|assignment| {
        !assignment
            .columns
            .first()
            .is_some_and(|column| columns.contains(column))
    });
    assignments.push(hir::Assignment {
        columns,
        value: hir::Expr::Subquery(hir::SubqueryExpr::Row { query }),
    });
    Ok(())
}

fn split_overlapping_row_assignment(
    assignments: &mut Vec<hir::Assignment>,
    column: hir::TargetColumn,
) -> Result<()> {
    let Some(index) = assignments.iter().position(|assignment| {
        assignment.columns.len() > 1 && assignment.columns.contains(&column)
    }) else {
        return Ok(());
    };
    let assignment = assignments.remove(index);
    let hir::Expr::Subquery(hir::SubqueryExpr::Row { query }) = assignment.value else {
        return Err(LimboError::InternalError(
            "multi-column UPDATE assignment is not a row subquery".to_string(),
        ));
    };
    for (output, column) in assignment.columns.into_iter().enumerate() {
        assignments.insert(
            index + output,
            hir::Assignment {
                columns: vec![column],
                value: hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }),
            },
        );
    }
    Ok(())
}

fn row_assignment_subquery(mut expression: &ast::Expr) -> Option<&ast::Select> {
    while let ast::Expr::Parenthesized(expressions) = expression {
        let [inner] = expressions.as_slice() else {
            break;
        };
        expression = inner;
    }
    match expression {
        ast::Expr::Subquery(select) => Some(select),
        _ => None,
    }
}
