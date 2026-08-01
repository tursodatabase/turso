//! Trigger-specific semantic environments.

use turso_parser::ast;

use super::{
    dml::InsertSourceSyntax,
    expr::ExprPolicy,
    hir::{self, CatalogObject, DatabaseId, PseudoSource},
    scope::{QueryEnvironment, Scope},
    Analyzer, CatalogObjectKind, TriggerAnalysisInput,
};
use crate::{LimboError, Result};

const NEW_NOT_VISIBLE: &str = "NEW references are only valid in INSERT and UPDATE triggers";
const OLD_NOT_VISIBLE: &str = "OLD references are only valid in UPDATE and DELETE triggers";

pub(super) fn query_environment(environment: &hir::TriggerEnvironment) -> QueryEnvironment {
    let environment_builder = QueryEnvironment::empty().within_trigger_program();
    let environment_builder = match environment.new_source {
        Some(source) => environment_builder.with_visible_pseudo_source(PseudoSource::New, source),
        None => {
            environment_builder.with_forbidden_pseudo_source(PseudoSource::New, NEW_NOT_VISIBLE)
        }
    };
    match environment.old_source {
        Some(source) => environment_builder.with_visible_pseudo_source(PseudoSource::Old, source),
        None => {
            environment_builder.with_forbidden_pseudo_source(PseudoSource::Old, OLD_NOT_VISIBLE)
        }
    }
}

pub(super) fn expression_scope(
    analyzer: &Analyzer<'_, '_>,
    environment: &hir::TriggerEnvironment,
) -> Result<Scope> {
    analyzer.scope_for_environment(&query_environment(environment))
}

impl Analyzer<'_, '_> {
    pub(super) fn analyze_trigger_environment(
        &mut self,
        input: &TriggerAnalysisInput,
    ) -> Result<hir::TriggerEnvironment> {
        let table_name = crate::util::normalize_ident(input.table.get_name());
        let object_id = self.catalog_object_id(
            Some(input.database_id),
            CatalogObjectKind::Table,
            table_name,
        );
        let table = CatalogObject::new(
            object_id,
            self.context().snapshot(),
            Some(DatabaseId::new(input.database_id)),
            input.table.clone(),
        );
        let new_source = input
            .new_visible
            .then(|| self.analyze_trigger_pseudo_source(PseudoSource::New, table.clone()))
            .transpose()?;
        let old_source = input
            .old_visible
            .then(|| self.analyze_trigger_pseudo_source(PseudoSource::Old, table.clone()))
            .transpose()?;
        Ok(hir::TriggerEnvironment {
            table,
            new_source,
            old_source,
        })
    }

    pub(crate) fn analyze_trigger_predicate(
        &mut self,
        syntax: &ast::Expr,
        input: &TriggerAnalysisInput,
    ) -> Result<hir::HirRoot> {
        let environment = self.analyze_trigger_environment(input)?;
        let scope = expression_scope(self, &environment)?;
        let expression = self.analyze_expr(
            syntax,
            &scope,
            ExprPolicy::trigger_predicate().without_aggregate(),
        )?;
        Ok(hir::HirRoot::TriggerPredicate(hir::TriggerPredicate {
            expression,
            environment,
        }))
    }

    pub(crate) fn analyze_trigger_command(
        &mut self,
        syntax: &ast::TriggerCmd,
        input: &TriggerAnalysisInput,
    ) -> Result<hir::HirRoot> {
        let environment = self.analyze_trigger_environment(input)?;
        match syntax {
            ast::TriggerCmd::Update {
                or_conflict,
                tbl_name,
                sets,
                from,
                where_clause,
            } => self.analyze_update_parts(
                None,
                input.override_conflict.or(*or_conflict),
                &ast::QualifiedName::single(tbl_name.clone()),
                None,
                sets,
                from.as_ref(),
                where_clause.as_deref(),
                &[],
                &[],
                None,
                Some(environment),
            ),
            ast::TriggerCmd::Insert {
                or_conflict,
                tbl_name,
                col_names,
                select,
                upsert,
                returning,
            } => self.analyze_insert_parts(
                None,
                input.override_conflict.or(*or_conflict),
                &ast::QualifiedName::single(tbl_name.clone()),
                col_names,
                InsertSourceSyntax::Select {
                    select,
                    upsert: upsert.as_deref(),
                },
                returning,
                Some(environment),
            ),
            ast::TriggerCmd::Delete {
                tbl_name,
                where_clause,
            } => self.analyze_delete_parts(
                None,
                &ast::QualifiedName::single(tbl_name.clone()),
                None,
                where_clause.as_deref(),
                &[],
                &[],
                None,
                Some(environment),
            ),
            ast::TriggerCmd::Select(select) => {
                let query = self.analyze_query(select, query_environment(&environment))?;
                Ok(hir::HirRoot::Query(hir::QueryRoot {
                    query,
                    trigger: Some(environment),
                }))
            }
        }
    }

    fn analyze_trigger_pseudo_source(
        &mut self,
        kind: PseudoSource,
        table: hir::ResolvedTable,
    ) -> Result<hir::SourceId> {
        let source = self.analyze_pseudo_source(kind, table, hir::SourceOwner::Root)?;
        let definition = self.source_mut(source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "missing trigger pseudo-source immediately after creating {source}"
            ))
        })?;
        for column in &mut definition.columns {
            // SQLite treats NEW/OLD fields as register values rather than table
            // columns during comparisons. A true rowid alias is the exception:
            // it keeps INTEGER affinity just like NEW.rowid and OLD.rowid.
            column.has_affinity = column.rowid_alias;
        }
        Ok(source)
    }
}
