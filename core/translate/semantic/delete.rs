//! Basic DELETE conversion into closed semantic HIR.

use turso_parser::ast;

use super::{
    analyze::Analyzer,
    dml::trigger_targets_database,
    expr::ExprPolicies,
    hir::{self, HirRoot, SourceOwner},
    scope::Scope,
};
use crate::{LimboError, Result};

#[derive(Clone, Copy)]
pub(super) struct DeleteExprContext<'scope> {
    pub(super) outer_scope: Option<&'scope Scope>,
    pub(super) policies: ExprPolicies,
}

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn analyze_delete(
        &mut self,
        with: Option<&'ast ast::With>,
        table_name: &'ast ast::QualifiedName,
        indexed: Option<&'ast ast::Indexed>,
        where_clause: Option<&'ast ast::Expr>,
        returning: &'ast [ast::ResultColumn],
    ) -> Result<HirRoot> {
        self.with_cte_scope(with, |analyzer| {
            let target =
                analyzer.analyze_base_table_source(table_name, None, indexed, SourceOwner::Root)?;
            analyzer.analyze_delete_target(
                target,
                where_clause,
                returning,
                DeleteExprContext {
                    outer_scope: None,
                    policies: ExprPolicies::statement(analyzer.context().dqs_dml()),
                },
            )
        })
    }

    pub(super) fn analyze_delete_target(
        &mut self,
        target: hir::SourceId,
        where_clause: Option<&'ast ast::Expr>,
        returning: &'ast [ast::ResultColumn],
        expressions: DeleteExprContext<'_>,
    ) -> Result<HirRoot> {
        let table = match &self
            .source(target)
            .ok_or_else(|| LimboError::InternalError(format!("missing DELETE target {target}")))?
            .kind
        {
            hir::SourceKind::Table(table) => table.clone(),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "DELETE target {target} is not a catalog table"
                )));
            }
        };
        let target_kind = if let Some(btree) = table.value().btree() {
            if !btree.has_rowid {
                return Err(LimboError::ParseError(
                    "DELETE from WITHOUT ROWID tables is not supported".to_string(),
                ));
            }
            self.analyze_btree_delete_metadata(target, &table)?;
            hir::DeleteTargetKind::BTree {
                triggers: self.analyze_delete_triggers(&table)?,
                foreign_keys: self.analyze_dml_foreign_keys(&table, target)?,
            }
        } else if table.value().virtual_table().is_some() {
            hir::DeleteTargetKind::Virtual
        } else {
            return Err(LimboError::InternalError(format!(
                "DELETE target {} is not writable",
                table.value().get_name()
            )));
        };

        let mut scope = self.delete_read_scope(target)?;
        scope.set_outer(expressions.outer_scope);
        let predicate = where_clause
            .map(|syntax| {
                self.analyze_root_expr(syntax, &scope, expressions.policies.where_clause())
                    .map(|resolved| resolved.expr)
            })
            .transpose()?;
        let returning =
            self.analyze_dml_returning_with_policies(returning, target, expressions.policies)?;

        Ok(HirRoot::Delete(hir::Delete {
            target,
            target_kind,
            predicate,
            order_by: Vec::new(),
            limit: None,
            returning,
        }))
    }

    fn delete_read_scope(&self, target: hir::SourceId) -> Result<Scope> {
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing DELETE target source {target}"))
        })?;
        let mut scope = Scope::default();
        scope.add_source(source, true);
        Ok(scope)
    }

    fn analyze_delete_triggers(
        &mut self,
        table: &hir::ResolvedTable,
    ) -> Result<Vec<hir::ResolvedTrigger>> {
        let database = table
            .database()
            .expect("a DELETE target table must have an owning database");
        let triggers = self
            .context()
            .schema(database)?
            .get_triggers_for_table(table.value().get_name())
            .filter(|trigger| {
                trigger_targets_database(trigger, database)
                    && matches!(trigger.event, ast::TriggerEvent::Delete)
            })
            .cloned()
            .collect::<Vec<_>>();
        Ok(triggers
            .into_iter()
            .map(|trigger| self.freeze_trigger(database, trigger))
            .collect())
    }
}
