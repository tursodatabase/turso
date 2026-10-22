//! Shared semantic helpers for data-changing statements.

use turso_parser::ast;

use super::{
    analyze::{output_from_resolved, Analyzer, CatalogObjectKind},
    expr::ExprPolicies,
    hir,
    scope::{ExpandedColumn, Scope},
};
use crate::{sync::Arc, util::normalize_ident, LimboError, Result};

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn freeze_trigger(
        &mut self,
        database: hir::DatabaseId,
        trigger: Arc<crate::schema::Trigger>,
    ) -> hir::ResolvedTrigger {
        let id = self.catalog_object_id(
            Some(database),
            CatalogObjectKind::Trigger,
            normalize_ident(&trigger.name),
        );
        hir::CatalogObject::new(id, self.context().snapshot(), Some(database), trigger)
    }

    /// The triggers on `table` that `selected` accepts, frozen. These are
    /// the table's own database's triggers, then temp triggers for it: those
    /// that name the table's database, or name no database while temp has
    /// no table of that name.
    pub(super) fn table_triggers(
        &mut self,
        table: &hir::ResolvedTable,
        selected: impl Fn(&crate::schema::Trigger) -> bool,
    ) -> Result<Vec<hir::ResolvedTrigger>> {
        let database = table
            .database()
            .expect("a DML target table must have an owning database");
        let name = table.value().get_name();
        let mut triggers = self
            .context()
            .schema(database)?
            .get_triggers_for_table(name)
            .filter(|trigger| trigger_targets_database(trigger, database) && selected(trigger))
            .cloned()
            .map(|trigger| (database, trigger))
            .collect::<Vec<_>>();
        let temp = hir::DatabaseId::new(crate::TEMP_DB_ID);
        if database != temp {
            if let Ok(temp_schema) = self.context().schema(temp) {
                // Like SQLite, temp triggers fire first. Each one
                // found is put in front, so they end up in the reverse of
                // the order the temp schema lists them.
                let mut temp_triggers = temp_schema
                    .get_triggers_for_table(name)
                    .filter(|trigger| {
                        selected(trigger)
                            && match trigger.target_database_id {
                                Some(target) => target == database.index(),
                                None => temp_schema.get_table(&trigger.table_name).is_none(),
                            }
                    })
                    .cloned()
                    .map(|trigger| (temp, trigger))
                    .collect::<Vec<_>>();
                temp_triggers.reverse();
                temp_triggers.append(&mut triggers);
                triggers = temp_triggers;
            }
        }
        Ok(triggers
            .into_iter()
            .map(|(database, trigger)| self.freeze_trigger(database, trigger))
            .collect())
    }

    /// The table change data capture writes the target's row changes to.
    /// Changes to the capture table itself and to its version table are not
    /// captured.
    pub(super) fn analyze_capture_changes(
        &mut self,
        target: &hir::ResolvedTable,
    ) -> Result<Option<hir::CaptureChanges>> {
        let Some(name) = self.context().capture_changes_table().map(str::to_string) else {
            return Ok(None);
        };
        let target_name = target.value().get_name();
        if target_name == name || target_name == crate::cdc::TURSO_CDC_VERSION_TABLE_NAME {
            return Ok(None);
        }
        let database = hir::DatabaseId::new(crate::MAIN_DB_ID);
        let table = self
            .context()
            .schema(database)?
            .get_table(&name)
            .filter(|table| table.btree().is_some());
        let Some(table) = table else {
            crate::bail_parse_error!("no such table: {}", name);
        };
        let id = self.catalog_object_id(Some(database), CatalogObjectKind::Table, name.as_str());
        let table = hir::CatalogObject::new(id, self.context().snapshot(), Some(database), table);
        let change_ids = self
            .analyze_autoincrement(&table)?
            .and_then(|autoincrement| autoincrement.mvcc_sequence);
        Ok(Some(hir::CaptureChanges { table, change_ids }))
    }

    /// The foreign keys a write to `target` checks. For an UPDATE,
    /// `updated` is the columns it sets: a constraint whose key it leaves
    /// alone is not checked, and not even resolved, so a mismatched
    /// constraint on other columns does not fail the statement.
    pub(super) fn analyze_dml_foreign_keys(
        &mut self,
        target: &hir::ResolvedTable,
        outgoing_child_source: hir::SourceId,
        updated: Option<&crate::translate::plan::ColumnMask>,
    ) -> Result<hir::DmlForeignKeys> {
        if !self.context().foreign_keys_enabled() {
            return Ok(hir::DmlForeignKeys::default());
        }
        let database = target.database().ok_or_else(|| {
            LimboError::InternalError("DML target has no owning database".to_string())
        })?;
        let (outgoing, incoming) = {
            let schema = self.context().schema(database)?;
            let name = target.value().get_name();
            let incoming = schema.resolved_fks_referencing(name)?;
            // Like SQLite, an UPDATE resolves the constraints the table
            // declares only when it changes a key: one of their child keys,
            // or a parent key another table points at. `updated` covers the
            // generated columns the SET columns feed.
            let outgoing = match updated {
                Some(updated) => {
                    let table = target.value().require_btree()?;
                    let child_key_changed = table
                        .foreign_keys
                        .iter()
                        .any(|foreign_key| foreign_key.child_key_assigned(&table, updated));
                    let mut parent_key_changed = false;
                    for foreign_key in &incoming {
                        if foreign_key.parent_key_may_change(updated, &table)? {
                            parent_key_changed = true;
                            break;
                        }
                    }
                    if child_key_changed || parent_key_changed {
                        schema.resolved_fks_for_child(name, Some(updated))?
                    } else {
                        Vec::new()
                    }
                }
                None => schema.resolved_fks_for_child(name, None)?,
            };
            (outgoing, incoming)
        };

        let mut resolved_outgoing = Vec::with_capacity(outgoing.len());
        for foreign_key in outgoing {
            let parent_table =
                self.freeze_foreign_key_table(database, &foreign_key.fk.parent_table, "parent")?;
            resolved_outgoing.push(self.freeze_foreign_key(
                database,
                outgoing_child_source,
                target.clone(),
                parent_table,
                foreign_key,
            )?);
        }

        let mut resolved_incoming = Vec::with_capacity(incoming.len());
        for foreign_key in incoming {
            let child_name = foreign_key.child_table.name.clone();
            // The child table is always in the parent's database.
            let child_source = self.analyze_base_table_source_in_database(
                &ast::Name::exact(child_name.clone()),
                database,
                hir::SourceOwner::Root,
            )?;
            let child_table = match &self
                .source(child_source)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing foreign-key child source {child_source}"
                    ))
                })?
                .kind
            {
                hir::SourceKind::Table(table) => table.clone(),
                _ => {
                    return Err(LimboError::InternalError(format!(
                        "foreign-key child {child_name} is not a catalog table"
                    )));
                }
            };
            resolved_incoming.push(self.freeze_foreign_key(
                database,
                child_source,
                child_table,
                target.clone(),
                foreign_key,
            )?);
        }

        Ok(hir::DmlForeignKeys {
            outgoing: resolved_outgoing,
            incoming: resolved_incoming,
        })
    }

    pub(super) fn analyze_dml_returning_with_policies(
        &mut self,
        columns: &'ast [ast::ResultColumn],
        row_source: hir::SourceId,
        policies: ExprPolicies,
    ) -> Result<Option<hir::Returning>> {
        if columns.is_empty() {
            return Ok(None);
        }
        let source = self.source(row_source).ok_or_else(|| {
            LimboError::InternalError(format!("missing RETURNING row source {row_source}"))
        })?;
        let mut scope = Scope::default();
        scope.add_source(source, true);
        let policy = policies.returning();
        let mut outputs = Vec::with_capacity(columns.len());

        for column in columns {
            match column {
                ast::ResultColumn::Expr(expression, alias) => {
                    let resolved = self.analyze_root_expr(expression, &scope, policy)?;
                    let (name, name_kind) = match alias {
                        Some(alias) if alias.is_explicit() => (
                            alias.name().as_str().to_string(),
                            hir::OutputNameKind::ExplicitAlias,
                        ),
                        Some(ast::As::ImplicitColumnName(name)) => {
                            (name.as_str().to_string(), hir::OutputNameKind::Inferred)
                        }
                        None => (expression.to_string(), hir::OutputNameKind::Inferred),
                        Some(_) => unreachable!("all explicit aliases were handled"),
                    };
                    outputs.push(output_from_resolved(
                        hir::OutputId::root(outputs.len()),
                        name,
                        name_kind,
                        resolved,
                    ));
                }
                ast::ResultColumn::Star => {
                    let expanded = scope.expand_star()?;
                    self.append_returning_star_outputs(&mut outputs, expanded, &scope)?;
                }
                // Like SQLite, only a bare `*` expands here.
                ast::ResultColumn::TableStar(_) => {
                    crate::bail_parse_error!("RETURNING may not use \"TABLE.*\" wildcards");
                }
            }
        }
        Ok(Some(hir::Returning { outputs }))
    }

    fn append_returning_star_outputs(
        &self,
        outputs: &mut Vec<hir::Output>,
        expanded: Vec<ExpandedColumn>,
        scope: &Scope,
    ) -> Result<()> {
        for column in expanded {
            let resolved = self.resolve_atomic_expr(column.resolved.expr, scope)?;
            outputs.push(output_from_resolved(
                hir::OutputId::root(outputs.len()),
                column.name,
                hir::OutputNameKind::StarExpansion,
                resolved,
            ));
        }
        Ok(())
    }

    fn freeze_foreign_key_table(
        &mut self,
        database: hir::DatabaseId,
        name: &str,
        role: &str,
    ) -> Result<hir::ResolvedTable> {
        let normalized = normalize_ident(name);
        let table = self
            .context()
            .schema(database)?
            .get_table(&normalized)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "resolved foreign-key {role} table {normalized} is missing"
                ))
            })?;
        let id = self.catalog_object_id(Some(database), CatalogObjectKind::Table, normalized);
        Ok(hir::CatalogObject::new(
            id,
            self.context().snapshot(),
            Some(database),
            table,
        ))
    }

    fn freeze_foreign_key(
        &mut self,
        database: hir::DatabaseId,
        child_source: hir::SourceId,
        child_table: hir::ResolvedTable,
        parent_table: hir::ResolvedTable,
        foreign_key: crate::schema::ResolvedFkRef,
    ) -> Result<hir::ResolvedForeignKey> {
        let parent_unique_index = foreign_key
            .parent_unique_index
            .map(|index| self.freeze_foreign_key_index(database, index));
        // The child index must list exactly the child columns, in the same
        // order.
        let child_index = self
            .context()
            .table_indexes(database, child_table.value().get_name())?
            .into_iter()
            .find(|index| {
                index.columns.len() == foreign_key.fk.child_columns.len()
                    && index
                        .columns
                        .iter()
                        .zip(foreign_key.fk.child_columns.iter())
                        .all(|(column, child)| column.name.eq_ignore_ascii_case(child))
            });
        let child_index = child_index.map(|index| self.freeze_foreign_key_index(database, index));
        Ok(hir::ResolvedForeignKey {
            child_table,
            child_source,
            parent_table,
            declaration: foreign_key.fk,
            parent_columns: foreign_key.parent_cols,
            child_positions: foreign_key.child_pos,
            parent_positions: foreign_key.parent_pos,
            parent_uses_rowid: foreign_key.parent_uses_rowid,
            parent_unique_index,
            child_index,
            parent_action_guarantees_new_parent: false,
        })
    }

    fn freeze_foreign_key_index(
        &mut self,
        database: hir::DatabaseId,
        index: Arc<crate::schema::Index>,
    ) -> hir::ResolvedIndex {
        let id = self.catalog_object_id(
            Some(database),
            CatalogObjectKind::Index,
            normalize_ident(&index.name),
        );
        hir::CatalogObject::new(id, self.context().snapshot(), Some(database), index)
    }
}

pub(super) fn trigger_targets_database(
    trigger: &crate::schema::Trigger,
    database: hir::DatabaseId,
) -> bool {
    trigger
        .target_database_id
        .is_none_or(|target| target == database.index())
}

/// Whether an UPDATE that sets the columns named `updated_names` (as
/// written, normalized) fires `trigger`. Like SQLite, an `UPDATE OF`
/// trigger compares names: `SET rowid = 1` fires a trigger on `rowid`, not
/// one on the INTEGER PRIMARY KEY column, and the other way round.
pub(super) fn trigger_matches_update(
    trigger: &crate::schema::Trigger,
    updated_names: &[String],
) -> bool {
    match &trigger.event {
        ast::TriggerEvent::Update => true,
        ast::TriggerEvent::UpdateOf(columns) => columns.iter().any(|column| {
            let column = normalize_ident(column.as_str());
            updated_names
                .iter()
                .any(|updated| updated.eq_ignore_ascii_case(&column))
        }),
        ast::TriggerEvent::Delete | ast::TriggerEvent::Insert => false,
    }
}
