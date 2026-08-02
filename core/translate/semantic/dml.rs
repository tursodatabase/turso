//! Semantic analysis for INSERT, UPDATE, and DELETE.

use rustc_hash::FxHashSet as HashSet;
use turso_parser::ast;

use super::{
    context::SemanticContext,
    dml_rules::{
        add_schema_named_target, configure_target_read_scope, configure_upsert_scope,
        resolve_assignment_columns, resolve_insert_columns, DmlOperation,
    },
    expr::ExprPolicy,
    hir::{self, CatalogObject, DatabaseId, TargetColumn},
    query::IndexMetadataMode,
    scope::{QueryEnvironment, Scope},
    Analyzer, CatalogObjectKind, TriggerAnalysisInput,
};
use crate::{
    schema::{
        BTreeTable, Index, IndexColumn, ResolvedFkRef, Table, Trigger, SQLITE_SEQUENCE_TABLE_NAME,
    },
    schema_expr::SchemaExprProfile,
    sync::Arc,
    translate::expr::{walk_expr, WalkControl},
    LimboError, Result,
};

pub(super) enum InsertSourceSyntax<'a> {
    DefaultValues,
    Select {
        select: &'a ast::Select,
        upsert: Option<&'a ast::Upsert>,
    },
}

fn validate_dml_target(
    context: &SemanticContext<'_>,
    database_id: usize,
    table: &Table,
    operation: DmlOperation,
) -> Result<()> {
    let table_name = table.get_name();
    let policy = context.dml_policy();
    let internal_schema_update =
        operation == DmlOperation::Update && policy.internal_schema_change();
    if !internal_schema_update
        && !policy.nested_statement()
        && !policy.mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(table_name)
    {
        crate::bail_parse_error!("table {table_name} may not be modified");
    }

    if context.trigger().is_some() && table.virtual_table().is_some() {
        crate::bail_parse_error!("unsafe use of virtual table \"{}\"", table_name);
    }

    if matches!(operation, DmlOperation::Update | DmlOperation::Delete)
        && table.btree().is_some_and(|table| !table.has_rowid)
    {
        match operation {
            DmlOperation::Update => {
                crate::bail_parse_error!("UPDATE of WITHOUT ROWID tables is not supported")
            }
            DmlOperation::Delete => {
                crate::bail_parse_error!("DELETE from WITHOUT ROWID tables is not supported")
            }
            DmlOperation::Insert => unreachable!(),
        }
    }

    let schema = context.schema(database_id).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "database {database_id} disappeared from the semantic catalog snapshot"
        ))
    })?;
    if schema.is_materialized_view(table_name) {
        crate::bail_parse_error!("cannot modify materialized view {table_name}");
    }

    schema.with_incompatible_dependent_views(table_name, |views| {
        if views.is_empty() {
            return Ok(());
        }
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        let names = views
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        match operation {
            DmlOperation::Insert => crate::bail_parse_error!(
                "Cannot INSERT into table '{table_name}' because it has incompatible dependent materialized view(s): {names}. \n\
                 These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
                 Please DROP and recreate the view(s) before modifying this table."
            ),
            DmlOperation::Update => crate::bail_parse_error!(
                "Cannot UPDATE table '{table_name}' because it has incompatible dependent materialized view(s): {names}. \n\
                 These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
                 Please DROP and recreate the view(s) before modifying this table."
            ),
            DmlOperation::Delete => crate::bail_parse_error!(
                "Cannot DELETE from table '{table_name}' because it has incompatible dependent materialized view(s): {names}. \n\
                 These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
                 Please DROP and recreate the view(s) before modifying this table."
            ),
        }
    })
}

fn target_type_fact(target: TargetColumn, source: &hir::Source) -> hir::TypeFact {
    match target {
        TargetColumn::Column(index) => source
            .columns
            .get(index)
            .unwrap_or_else(|| {
                panic!(
                    "resolved target column {index} is outside source {}",
                    source.id
                )
            })
            .type_fact
            .clone(),
        TargetColumn::RowId => hir::TypeFact::known(crate::schema::Type::Integer),
    }
}

fn trigger_targets_database(trigger: &Trigger, database_id: usize) -> bool {
    trigger
        .target_database_id
        .map_or(true, |target| target == database_id)
}

fn trigger_matches_event(
    trigger: &Trigger,
    event: &ast::TriggerEvent,
    table: &Table,
    updated_columns: &HashSet<usize>,
) -> bool {
    match (&trigger.event, event) {
        (ast::TriggerEvent::Insert, ast::TriggerEvent::Insert)
        | (ast::TriggerEvent::Delete, ast::TriggerEvent::Delete)
        | (ast::TriggerEvent::Update, ast::TriggerEvent::Update) => true,
        (ast::TriggerEvent::UpdateOf(names), ast::TriggerEvent::Update) => {
            names.iter().any(|name| {
                table
                    .get_column_by_name(&crate::util::normalize_ident(name.as_str()))
                    .is_some_and(|(position, _)| updated_columns.contains(&position))
            })
        }
        _ => false,
    }
}

impl Analyzer<'_, '_> {
    pub(crate) fn analyze_dml_statement(
        &mut self,
        syntax: &ast::Stmt,
        trigger_input: Option<&TriggerAnalysisInput>,
    ) -> Result<hir::HirRoot> {
        let trigger = trigger_input
            .map(|input| self.analyze_trigger_environment(input))
            .transpose()?;
        match syntax {
            ast::Stmt::Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } => {
                let source = match body {
                    ast::InsertBody::DefaultValues => InsertSourceSyntax::DefaultValues,
                    ast::InsertBody::Select(select, upsert) => InsertSourceSyntax::Select {
                        select,
                        upsert: upsert.as_deref(),
                    },
                };
                self.analyze_insert_parts(
                    with.as_ref(),
                    *or_conflict,
                    tbl_name,
                    columns,
                    source,
                    returning,
                    trigger,
                )
            }
            ast::Stmt::Update(update) => self.analyze_update_parts(
                update.with.as_ref(),
                update.or_conflict,
                &update.tbl_name,
                update.indexed.as_ref(),
                &update.sets,
                update.from.as_ref(),
                update.where_clause.as_deref(),
                &update.returning,
                &update.order_by,
                update.limit.as_ref(),
                trigger,
            ),
            ast::Stmt::Delete {
                with,
                tbl_name,
                indexed,
                where_clause,
                returning,
                order_by,
                limit,
            } => self.analyze_delete_parts(
                with.as_ref(),
                tbl_name,
                indexed.as_ref(),
                where_clause.as_deref(),
                returning,
                order_by,
                limit.as_ref(),
                trigger,
            ),
            _ => Err(LimboError::InternalError(
                "DML semantic analysis requires INSERT, UPDATE, or DELETE".to_string(),
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn analyze_insert_parts(
        &mut self,
        with: Option<&ast::With>,
        conflict: Option<ast::ResolveType>,
        table_name: &ast::QualifiedName,
        columns: &[ast::Name],
        source_syntax: InsertSourceSyntax<'_>,
        returning_syntax: &[ast::ResultColumn],
        trigger: Option<hir::TriggerEnvironment>,
    ) -> Result<hir::HirRoot> {
        let environment = self.prepare_dml_environment(trigger.as_ref(), with)?;
        let target = self.analyze_base_table_source(
            table_name,
            None,
            None,
            hir::SourceOwner::Root,
            IndexMetadataMode::Dml,
        )?;
        let resolved_table = self.resolved_source_table(target)?;
        let database_id = resolved_table_database_id(&resolved_table)?;
        let table = resolved_table.handle();
        validate_dml_target(self.context(), database_id, &table, DmlOperation::Insert)?;
        self.populate_dml_check_constraints(target, &table)?;

        let columns = resolve_insert_columns(&table, columns)?;
        let autoincrement = self.resolve_autoincrement_table(database_id, &table)?;
        let autoincrement_sequence = table
            .btree()
            .filter(|table| table.has_autoincrement)
            .map(|table| crate::schema::autoincrement_sequence_name(&table.name))
            .filter(|name| {
                self.context()
                    .schema(database_id)
                    .is_some_and(|schema| schema.get_sequence(name).is_some())
            })
            .map(|name| {
                let user_name = if database_id == crate::MAIN_DB_ID {
                    name
                } else {
                    let database_name =
                        self.context().database_name(database_id).ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "database {database_id} has no name in semantic snapshot"
                            ))
                        })?;
                    format!("{database_name}.{name}")
                };
                self.resolve_sequence_catalog_operation(
                    hir::SequenceOperationKind::NextValue,
                    user_name,
                )
            })
            .transpose()?;
        let defaults = self.analyze_insert_defaults(&table, target, database_id)?;
        let expected_types = self.destination_expected_types(target, &columns)?;
        let expected_defaults = columns
            .iter()
            .copied()
            .map(|column| default_for_target(column, &defaults).map(Some))
            .collect::<Result<Vec<_>>>()?;
        let (source, upsert_syntax) = match source_syntax {
            InsertSourceSyntax::DefaultValues => (hir::InsertSource::DefaultValues, None),
            InsertSourceSyntax::Select { select, upsert } => {
                let source = self.analyze_insert_source(
                    select,
                    &environment,
                    &table,
                    &columns,
                    &defaults,
                    &expected_types,
                    &expected_defaults,
                    trigger.is_some(),
                )?;
                (source, upsert)
            }
        };

        let excluded_source = upsert_syntax
            .map(|_| {
                self.analyze_pseudo_source(
                    hir::PseudoSource::Excluded,
                    resolved_table.clone(),
                    hir::SourceOwner::Root,
                )
            })
            .transpose()?;
        let upserts = match (upsert_syntax, excluded_source) {
            (Some(syntax), Some(excluded)) => self.analyze_upserts(
                syntax,
                &environment,
                &table,
                database_id,
                target,
                excluded,
                trigger.is_some(),
            )?,
            (None, None) => Vec::new(),
            _ => unreachable!("excluded exists exactly when an UPSERT clause exists"),
        };

        let returning =
            self.analyze_dml_returning(returning_syntax, &environment, target, trigger.is_some())?;
        let triggers =
            self.resolve_dml_triggers(database_id, &table, ast::TriggerEvent::Insert, &[])?;
        let foreign_keys = self.resolve_dml_foreign_keys(database_id, table.get_name())?;
        Ok(hir::HirRoot::Insert(hir::Insert {
            target,
            autoincrement,
            autoincrement_sequence,
            columns,
            defaults,
            source,
            conflict,
            upserts,
            excluded_source,
            returning,
            trigger,
            triggers,
            foreign_keys,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn analyze_update_parts(
        &mut self,
        with: Option<&ast::With>,
        conflict: Option<ast::ResolveType>,
        table_name: &ast::QualifiedName,
        indexed: Option<&ast::Indexed>,
        sets: &[ast::Set],
        from_syntax: Option<&ast::FromClause>,
        predicate_syntax: Option<&ast::Expr>,
        returning_syntax: &[ast::ResultColumn],
        order_by_syntax: &[ast::SortedColumn],
        limit_syntax: Option<&ast::Limit>,
        trigger: Option<hir::TriggerEnvironment>,
    ) -> Result<hir::HirRoot> {
        let environment = self.prepare_dml_environment(trigger.as_ref(), with)?;
        let target = self.analyze_base_table_source(
            table_name,
            None,
            indexed,
            hir::SourceOwner::Root,
            IndexMetadataMode::Dml,
        )?;
        let resolved_table = self.resolved_source_table(target)?;
        let database_id = resolved_table_database_id(&resolved_table)?;
        let table = resolved_table.handle();
        validate_dml_target(self.context(), database_id, &table, DmlOperation::Update)?;
        self.populate_dml_check_constraints(target, &table)?;
        let defaults = self.analyze_insert_defaults(&table, target, database_id)?;
        let (from, mut read_scope) = match from_syntax {
            Some(syntax) => {
                let (from, scope) =
                    self.analyze_update_from(syntax, hir::SourceOwner::Root, &environment)?;
                self.reject_target_in_update_from(target, &from)?;
                (Some(from), scope)
            }
            None => (None, self.scope_for_environment(&environment)?),
        };
        let target_definition = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing UPDATE target source {target}"))
        })?;
        configure_target_read_scope(&mut read_scope, target_definition);

        let assignments =
            self.analyze_assignments(sets, &table, target, &read_scope, trigger.is_some(), true)?;
        let predicate = predicate_syntax
            .map(|syntax| {
                self.analyze_expr(syntax, &read_scope, scalar_expr_policy(trigger.is_some()))
            })
            .transpose()?;
        let order_by =
            self.analyze_dml_order_by(order_by_syntax, &read_scope, trigger.is_some())?;
        let limit = self.analyze_dml_limit(limit_syntax, &environment, trigger.is_some())?;
        let returning =
            self.analyze_dml_returning(returning_syntax, &environment, target, trigger.is_some())?;
        let triggers = self.resolve_dml_triggers(
            database_id,
            &table,
            ast::TriggerEvent::Update,
            &assignments,
        )?;
        let foreign_keys = self.resolve_dml_foreign_keys(database_id, table.get_name())?;
        let cdc_updates_override = self.context().internal_schema_change_sql().and_then(|sql| {
            table
                .columns()
                .iter()
                .position(|column| column.name.as_deref() == Some("sql"))
                .map(|position| (position, sql.to_string()))
        });

        Ok(hir::HirRoot::Update(hir::Update {
            target,
            defaults,
            from,
            assignments,
            predicate,
            order_by,
            limit,
            conflict,
            returning,
            trigger,
            triggers,
            foreign_keys,
            cdc_updates_override,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn analyze_delete_parts(
        &mut self,
        with: Option<&ast::With>,
        table_name: &ast::QualifiedName,
        indexed: Option<&ast::Indexed>,
        predicate_syntax: Option<&ast::Expr>,
        returning_syntax: &[ast::ResultColumn],
        order_by_syntax: &[ast::SortedColumn],
        limit_syntax: Option<&ast::Limit>,
        trigger: Option<hir::TriggerEnvironment>,
    ) -> Result<hir::HirRoot> {
        let environment = self.prepare_dml_environment(trigger.as_ref(), with)?;
        let target = self.analyze_base_table_source(
            table_name,
            None,
            indexed,
            hir::SourceOwner::Root,
            IndexMetadataMode::Dml,
        )?;
        let resolved_table = self.resolved_source_table(target)?;
        let database_id = resolved_table_database_id(&resolved_table)?;
        let table = resolved_table.handle();
        validate_dml_target(self.context(), database_id, &table, DmlOperation::Delete)?;

        let mut scope = self.scope_for_environment(&environment)?;
        let target_definition = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing DELETE target source {target}"))
        })?;
        configure_target_read_scope(&mut scope, target_definition);
        let predicate = predicate_syntax
            .map(|syntax| self.analyze_expr(syntax, &scope, scalar_expr_policy(trigger.is_some())))
            .transpose()?;
        let order_by = self.analyze_dml_order_by(order_by_syntax, &scope, trigger.is_some())?;
        let limit = self.analyze_dml_limit(limit_syntax, &environment, trigger.is_some())?;
        let returning =
            self.analyze_dml_returning(returning_syntax, &environment, target, trigger.is_some())?;
        let triggers =
            self.resolve_dml_triggers(database_id, &table, ast::TriggerEvent::Delete, &[])?;
        let foreign_keys = self.resolve_dml_foreign_keys(database_id, table.get_name())?;

        Ok(hir::HirRoot::Delete(hir::Delete {
            target,
            predicate,
            order_by,
            limit,
            returning,
            trigger,
            triggers,
            foreign_keys,
        }))
    }

    fn resolve_dml_triggers(
        &mut self,
        database_id: usize,
        table: &Table,
        event: ast::TriggerEvent,
        assignments: &[hir::Assignment],
    ) -> Result<Vec<hir::ResolvedTrigger>> {
        let updated_columns = assignments
            .iter()
            .flat_map(|assignment| assignment.columns.iter())
            .filter_map(|column| match column {
                TargetColumn::Column(position) => Some(*position),
                TargetColumn::RowId => None,
            })
            .collect::<HashSet<_>>();
        let table_name = table.get_name();
        let schema = self.context().schema(database_id).ok_or_else(|| {
            LimboError::InternalError(format!(
                "database {database_id} disappeared while resolving DML triggers"
            ))
        })?;
        let mut matches = schema
            .get_triggers_for_table(table_name)
            .filter(|trigger| {
                trigger_targets_database(trigger, database_id)
                    && trigger_matches_event(trigger, &event, table, &updated_columns)
            })
            .cloned()
            .map(|trigger| (database_id, trigger))
            .collect::<Vec<_>>();

        if database_id != crate::TEMP_DB_ID {
            if let Some(temp_schema) = self.context().schema(crate::TEMP_DB_ID) {
                let temp_shadows_target = temp_schema.get_table(table_name).is_some();
                matches.extend(
                    temp_schema
                        .get_triggers_for_table(table_name)
                        .filter(|trigger| {
                            let target_matches = match trigger.target_database_id {
                                Some(target) => target == database_id,
                                None => !temp_shadows_target,
                            };
                            target_matches
                                && trigger_matches_event(trigger, &event, table, &updated_columns)
                        })
                        .cloned()
                        .map(|trigger| (crate::TEMP_DB_ID, trigger)),
                );
            }
        }

        matches
            .into_iter()
            .map(|(owner_database, trigger)| {
                let name = crate::util::normalize_ident(&trigger.name);
                let object_id =
                    self.catalog_object_id(Some(owner_database), CatalogObjectKind::Trigger, name);
                Ok(CatalogObject::new(
                    object_id,
                    self.context().snapshot(),
                    Some(DatabaseId::new(owner_database)),
                    trigger,
                ))
            })
            .collect()
    }

    fn resolve_autoincrement_table(
        &mut self,
        database_id: usize,
        table: &Table,
    ) -> Result<Option<hir::ResolvedTable>> {
        let Some(table) = table.btree().filter(|table| table.has_autoincrement) else {
            return Ok(None);
        };
        let sequence = self
            .context()
            .schema(database_id)
            .and_then(|schema| schema.get_btree_table(SQLITE_SEQUENCE_TABLE_NAME))
            .ok_or_else(|| {
                LimboError::Corrupt(format!(
                    "missing sqlite_sequence table for AUTOINCREMENT table {}",
                    table.name
                ))
            })?;
        if !sequence.has_rowid {
            crate::bail_corrupt_error!("malformed sqlite_sequence: table must have rowid");
        }
        if sequence.columns().len() != 2 {
            crate::bail_corrupt_error!(
                "malformed sqlite_sequence: expected 2 columns, got {}",
                sequence.columns().len()
            );
        }
        let name = sequence.columns()[0].name.as_deref();
        let value = sequence.columns()[1].name.as_deref();
        if !name.is_some_and(|name| name.eq_ignore_ascii_case("name"))
            || !value.is_some_and(|name| name.eq_ignore_ascii_case("seq"))
        {
            crate::bail_corrupt_error!("malformed sqlite_sequence: expected columns (name, seq)");
        }
        let id = self.catalog_object_id(
            Some(database_id),
            CatalogObjectKind::Table,
            SQLITE_SEQUENCE_TABLE_NAME,
        );
        Ok(Some(CatalogObject::new(
            id,
            self.context().snapshot(),
            Some(DatabaseId::new(database_id)),
            Arc::new(Table::BTree(sequence)),
        )))
    }

    fn resolve_dml_foreign_keys(
        &mut self,
        database_id: usize,
        table_name: &str,
    ) -> Result<hir::DmlForeignKeys> {
        if !self.context().dml_policy().foreign_keys_enabled() {
            return Ok(hir::DmlForeignKeys::default());
        }
        let schema = self.context().schema(database_id).ok_or_else(|| {
            LimboError::InternalError(format!(
                "database {database_id} disappeared while resolving foreign keys"
            ))
        })?;
        if schema.get_btree_table(table_name).is_none() {
            return Ok(hir::DmlForeignKeys::default());
        }

        let outgoing = schema
            .resolved_fks_for_child(table_name)?
            .into_iter()
            .map(|foreign_key| {
                let parent = schema
                    .get_btree_table(&foreign_key.fk.parent_table)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "resolved foreign-key parent {} disappeared",
                            foreign_key.fk.parent_table
                        ))
                    })?;
                Ok((foreign_key, parent))
            })
            .collect::<Result<Vec<_>>>()?;
        let parent = schema.get_btree_table(table_name).ok_or_else(|| {
            LimboError::InternalError(format!(
                "resolved foreign-key parent {table_name} disappeared"
            ))
        })?;
        let incoming = schema
            .resolved_fks_referencing(table_name)?
            .into_iter()
            .map(|foreign_key| (foreign_key, parent.clone()))
            .collect::<Vec<_>>();

        Ok(hir::DmlForeignKeys {
            outgoing: outgoing
                .into_iter()
                .map(|(foreign_key, parent)| {
                    self.freeze_foreign_key(database_id, foreign_key, parent)
                })
                .collect::<Result<Vec<_>>>()?,
            incoming: incoming
                .into_iter()
                .map(|(foreign_key, parent)| {
                    self.freeze_foreign_key(database_id, foreign_key, parent)
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn freeze_foreign_key(
        &mut self,
        database_id: usize,
        foreign_key: ResolvedFkRef,
        parent: Arc<BTreeTable>,
    ) -> Result<hir::ResolvedForeignKey> {
        let child_id = self.catalog_object_id(
            Some(database_id),
            CatalogObjectKind::Table,
            crate::util::normalize_ident(&foreign_key.child_table.name),
        );
        let parent_id = self.catalog_object_id(
            Some(database_id),
            CatalogObjectKind::Table,
            crate::util::normalize_ident(&parent.name),
        );
        let parent_unique_index = foreign_key.parent_unique_index.map(|index| {
            let id = self.catalog_object_id(
                Some(database_id),
                CatalogObjectKind::Index,
                crate::util::normalize_ident(&index.name),
            );
            CatalogObject::new(
                id,
                self.context().snapshot(),
                Some(DatabaseId::new(database_id)),
                index,
            )
        });
        Ok(hir::ResolvedForeignKey {
            child_table: CatalogObject::new(
                child_id,
                self.context().snapshot(),
                Some(DatabaseId::new(database_id)),
                Arc::new(Table::BTree(foreign_key.child_table)),
            ),
            parent_table: CatalogObject::new(
                parent_id,
                self.context().snapshot(),
                Some(DatabaseId::new(database_id)),
                Arc::new(Table::BTree(parent)),
            ),
            declaration: foreign_key.fk,
            parent_columns: foreign_key.parent_cols,
            child_positions: foreign_key.child_pos,
            parent_positions: foreign_key.parent_pos,
            parent_uses_rowid: foreign_key.parent_uses_rowid,
            parent_unique_index,
        })
    }

    fn prepare_dml_environment(
        &self,
        trigger: Option<&hir::TriggerEnvironment>,
        with: Option<&ast::With>,
    ) -> Result<QueryEnvironment> {
        let environment = trigger
            .map(super::trigger_rules::query_environment)
            .unwrap_or_else(QueryEnvironment::empty);
        self.prepare_query_environment(environment, with)
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_insert_source(
        &mut self,
        syntax: &ast::Select,
        environment: &QueryEnvironment,
        table: &Table,
        columns: &[TargetColumn],
        defaults: &[hir::ResolvedDefault],
        expected_types: &[Option<hir::ResolvedType>],
        expected_defaults: &[Option<hir::Expr>],
        in_trigger: bool,
    ) -> Result<hir::InsertSource> {
        if syntax.body.compounds.is_empty() && syntax.order_by.is_empty() && syntax.limit.is_none()
        {
            if let ast::OneSelect::Values(rows) = &syntax.body.select {
                if rows.is_empty() {
                    crate::bail_parse_error!("no values to insert");
                }
                let values_environment =
                    self.prepare_query_environment(environment.clone(), syntax.with.as_ref())?;
                let scope = self.scope_for_environment(&values_environment)?;
                // SQLite treats the optimized one-row VALUES form as a
                // scope-less expression list. Only a top-level quoted
                // identifier gets the legacy string fallback; identifiers
                // nested inside functions remain column references and fail.
                // A subquery or a second row uses normal SELECT name rules.
                let scopeless_values = rows.len() == 1
                    && !rows[0]
                        .iter()
                        .any(|expression| expr_contains_subquery(expression));
                let mut resolved_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    self.validate_insert_arity(table, columns.len(), row.len())?;
                    let mut resolved = Vec::with_capacity(row.len());
                    for (index, expression) in row.iter().enumerate() {
                        if matches!(expression.as_ref(), ast::Expr::Default) {
                            resolved.push(default_for_target(columns[index], defaults)?);
                            continue;
                        }
                        let mut policy = scalar_expr_policy(in_trigger)
                            .with_expected_type(expected_types[index].clone());
                        let top_level_quoted_identifier = matches!(
                            expression.as_ref(),
                            ast::Expr::Id(name) if name.quoted_with('"')
                        );
                        if scopeless_values && !top_level_quoted_identifier {
                            policy = policy.without_dqs_fallback();
                        }
                        resolved.push(self.analyze_expr(expression, &scope, policy)?);
                    }
                    resolved_rows.push(resolved);
                }
                return Ok(hir::InsertSource::Values(resolved_rows));
            }
        }

        let query = self.analyze_query_with_inputs(
            syntax,
            environment.clone(),
            expected_types,
            expected_defaults,
        )?;
        let supplied = self
            .query(query)
            .map(|query| query.output.len())
            .ok_or_else(|| {
                LimboError::InternalError(format!("INSERT source query {query} is missing"))
            })?;
        self.validate_insert_arity(table, columns.len(), supplied)?;
        Ok(hir::InsertSource::Query(query))
    }

    fn validate_insert_arity(&self, table: &Table, expected: usize, supplied: usize) -> Result<()> {
        if expected != supplied {
            crate::bail_parse_error!(
                "table {} has {expected} columns but {supplied} values were supplied",
                table.get_name()
            );
        }
        Ok(())
    }

    fn destination_expected_types(
        &self,
        target: hir::SourceId,
        columns: &[TargetColumn],
    ) -> Result<Vec<Option<hir::ResolvedType>>> {
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing INSERT target source {target}"))
        })?;
        Ok(columns
            .iter()
            .copied()
            .map(|column| {
                target_type_fact(column, source)
                    .declared
                    .and_then(|declared| declared.custom().cloned())
            })
            .collect())
    }

    fn analyze_insert_defaults(
        &mut self,
        table: &Table,
        target: hir::SourceId,
        database_id: usize,
    ) -> Result<Vec<hir::ResolvedDefault>> {
        let mut defaults = Vec::new();
        for (column_index, column) in table.columns().iter().enumerate() {
            if column.is_generated() {
                continue;
            }
            let stored = if let Some(default) = column.default.as_deref() {
                Some(default.clone())
            } else {
                self.context()
                    .schema(database_id)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "database {database_id} disappeared while resolving INSERT defaults"
                        ))
                    })?
                    .resolve_type(&column.ty_str, table.is_strict())?
                    .and_then(|resolved| resolved.default_expr().cloned())
            };
            let value = match stored {
                Some(stored) => self.instantiate_column_schema_syntax(
                    &stored,
                    SchemaExprProfile::Default,
                    target,
                    column_index,
                )?,
                None => hir::Expr::Literal(ast::Literal::Null),
            };
            defaults.push(hir::ResolvedDefault {
                column: column_index,
                value,
            });
        }
        Ok(defaults)
    }

    fn analyze_dml_returning(
        &mut self,
        syntax: &[ast::ResultColumn],
        environment: &QueryEnvironment,
        target: hir::SourceId,
        in_trigger: bool,
    ) -> Result<Option<hir::Returning>> {
        if syntax.is_empty() {
            return Ok(None);
        }
        let mut scope = self.scope_for_environment(environment)?;
        let target_definition = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing DML target source {target}"))
        })?;
        if matches!(
            &target_definition.kind,
            hir::SourceKind::Table(table) if matches!(table.value(), Table::Virtual(_))
        ) {
            crate::bail_parse_error!("RETURNING is not supported for virtual tables");
        }
        // SQLite resolves RETURNING through the schema table name even when
        // UPDATE or INSERT uses an alias for the writable target.
        add_schema_named_target(&mut scope, target_definition);
        let policy = if in_trigger {
            ExprPolicy::trigger_predicate()
        } else {
            ExprPolicy::returning()
        };
        self.analyze_returning_with_policy(syntax, &scope, policy)
            .map(Some)
    }

    fn analyze_dml_order_by(
        &mut self,
        syntax: &[ast::SortedColumn],
        scope: &Scope,
        in_trigger: bool,
    ) -> Result<Vec<hir::OrderTerm>> {
        let mut terms = Vec::with_capacity(syntax.len());
        for term in syntax {
            let expr = self.analyze_expr(&term.expr, scope, scalar_expr_policy(in_trigger))?;
            terms.push(self.resolved_order_term(
                expr,
                term.order.unwrap_or(ast::SortOrder::Asc),
                term.nulls,
                scope,
            ));
        }
        Ok(terms)
    }

    fn analyze_dml_limit(
        &mut self,
        syntax: Option<&ast::Limit>,
        environment: &QueryEnvironment,
        in_trigger: bool,
    ) -> Result<Option<hir::Limit>> {
        let Some(syntax) = syntax else {
            return Ok(None);
        };
        let scope = self.scope_for_environment(environment)?;
        let policy = scalar_expr_policy(in_trigger);
        Ok(Some(hir::Limit {
            limit: self.analyze_expr(&syntax.expr, &scope, policy.clone())?,
            offset: syntax
                .offset
                .as_deref()
                .map(|offset| self.analyze_expr(offset, &scope, policy))
                .transpose()?,
        }))
    }

    fn reject_target_in_update_from(&self, target: hir::SourceId, from: &hir::From) -> Result<()> {
        let target = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing UPDATE target source {target}"))
        })?;
        let target_identifier = source_identifier(target);
        let target_table_name = crate::util::normalize_ident(&target.name);
        for source_id in std::iter::once(from.first).chain(from.joins.iter().map(|join| join.right))
        {
            let source = self.source(source_id).ok_or_else(|| {
                LimboError::InternalError(format!("missing UPDATE FROM source {source_id}"))
            })?;
            let hir::SourceKind::Table(source_table) = &source.kind else {
                continue;
            };
            if source.database == target.database
                && source_identifier(source) == target_identifier
                && crate::util::normalize_ident(source_table.value().get_name())
                    == target_table_name
            {
                crate::bail_parse_error!(
                    "target object/alias may not appear in FROM clause: {}",
                    target.alias.as_deref().unwrap_or(&target.name)
                );
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_upserts(
        &mut self,
        syntax: &ast::Upsert,
        environment: &QueryEnvironment,
        table: &Table,
        database_id: usize,
        target: hir::SourceId,
        excluded: hir::SourceId,
        in_trigger: bool,
    ) -> Result<Vec<hir::Upsert>> {
        let mut clauses = Vec::new();
        let mut current = Some(syntax);
        while let Some(clause) = current {
            let target_clause = clause
                .index
                .as_ref()
                .map(|syntax| {
                    self.analyze_conflict_target(syntax, environment, table, database_id, target)
                })
                .transpose()?;

            let action = match &clause.do_clause {
                ast::UpsertDo::Nothing => hir::UpsertAction::Nothing,
                ast::UpsertDo::Set { sets, where_clause } => {
                    let mut scope = self.scope_for_environment(environment)?;
                    let target_definition = self.source(target).ok_or_else(|| {
                        LimboError::InternalError(format!("missing UPSERT target source {target}"))
                    })?;
                    let excluded_definition = self.source(excluded).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "missing UPSERT excluded source {excluded}"
                        ))
                    })?;
                    configure_upsert_scope(&mut scope, target_definition, excluded_definition);
                    let assignments =
                        self.analyze_assignments(sets, table, target, &scope, in_trigger, false)?;
                    let predicate = where_clause
                        .as_deref()
                        .map(|syntax| {
                            self.analyze_expr(
                                syntax,
                                &scope,
                                scalar_expr_policy(in_trigger).without_subqueries(),
                            )
                        })
                        .transpose()?;
                    hir::UpsertAction::Update {
                        assignments,
                        predicate,
                    }
                }
            };
            clauses.push(hir::Upsert {
                target: target_clause,
                action,
            });
            current = clause.next.as_deref();
        }
        Ok(clauses)
    }

    fn analyze_conflict_target(
        &mut self,
        syntax: &ast::UpsertIndex,
        environment: &QueryEnvironment,
        table: &Table,
        database_id: usize,
        target: hir::SourceId,
    ) -> Result<hir::ConflictTarget> {
        let mut scope = self.scope_for_environment(environment)?;
        let target_definition = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing UPSERT target source {target}"))
        })?;
        // A conflict target names the schema table, even when INSERT itself
        // gave its writable target an alias.
        add_schema_named_target(&mut scope, target_definition);

        let policy = scalar_expr_policy(false).without_subqueries();
        let mut terms = Vec::with_capacity(syntax.targets.len());
        for term in &syntax.targets {
            if let Some(nulls) = term.nulls {
                crate::bail_parse_error!("unsupported use of {}", nulls);
            }
            let expr = self.analyze_expr(&term.expr, &scope, policy.clone())?;
            terms.push(hir::ConflictTerm {
                collation: explicit_collation(&expr).cloned(),
                expr,
                order: term.order.unwrap_or(ast::SortOrder::Asc),
            });
        }
        let predicate = syntax
            .where_clause
            .as_deref()
            .map(|syntax| self.analyze_expr(syntax, &scope, policy))
            .transpose()?;
        let matched_index =
            self.match_conflict_target(&terms, predicate.as_ref(), table, database_id, target)?;
        Ok(hir::ConflictTarget {
            terms,
            predicate,
            matched_index,
        })
    }

    fn match_conflict_target(
        &mut self,
        terms: &[hir::ConflictTerm],
        predicate: Option<&hir::Expr>,
        table: &Table,
        database_id: usize,
        target: hir::SourceId,
    ) -> Result<Option<hir::ResolvedIndex>> {
        if terms.len() == 1 {
            if let hir::Expr::Column(column) = without_explicit_collation(&terms[0].expr) {
                if column.source == target
                    && table
                        .columns()
                        .get(column.column)
                        .is_some_and(|column| column.is_rowid_alias())
                {
                    return Ok(None);
                }
            }
        }

        let indexes: Vec<Arc<Index>> = self
            .context()
            .schema(database_id)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "database {database_id} disappeared while matching UPSERT"
                ))
            })?
            .get_indices(table.get_name())
            .filter(|index| index.unique)
            .cloned()
            .collect();
        for index in indexes {
            if self.conflict_target_matches_index(terms, predicate, table, target, &index)? {
                let object_id = self.catalog_object_id(
                    Some(database_id),
                    CatalogObjectKind::Index,
                    crate::util::normalize_ident(&index.name),
                );
                return Ok(Some(CatalogObject::new(
                    object_id,
                    self.context().snapshot(),
                    Some(DatabaseId::new(database_id)),
                    index,
                )));
            }
        }
        crate::bail_parse_error!(
            "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"
        );
    }

    fn conflict_target_matches_index(
        &mut self,
        terms: &[hir::ConflictTerm],
        predicate: Option<&hir::Expr>,
        table: &Table,
        target: hir::SourceId,
        index: &Index,
    ) -> Result<bool> {
        if terms.len() != index.columns.len() {
            return Ok(false);
        }
        match (&index.where_clause, predicate) {
            (Some(stored), Some(predicate)) => {
                let stored = self.instantiate_table_schema_syntax(
                    stored,
                    SchemaExprProfile::PartialIndexPredicate,
                    target,
                )?;
                if !super::schema_expr::equivalent(predicate, &stored) {
                    return Ok(false);
                }
            }
            (Some(_), None) => return Ok(false),
            (None, _) => {}
        }

        let mut matched = vec![false; index.columns.len()];
        for term in terms {
            let expression = without_explicit_collation(&term.expr);
            let mut found = None;
            if let hir::Expr::Column(column) = expression {
                if column.source == target {
                    for (position, index_column) in index.columns.iter().enumerate() {
                        if matched[position]
                            || index_column.expr.is_some()
                            || index_column.pos_in_table != column.column
                            || !conflict_collation_matches(term, index_column, table)
                        {
                            continue;
                        }
                        found = Some(position);
                        break;
                    }
                }
            } else {
                for (position, index_column) in index.columns.iter().enumerate() {
                    if matched[position] || !conflict_collation_matches(term, index_column, table) {
                        continue;
                    }
                    let Some(stored) = &index_column.expr else {
                        continue;
                    };
                    let stored = self.instantiate_table_schema_syntax(
                        stored,
                        SchemaExprProfile::IndexKey,
                        target,
                    )?;
                    if super::schema_expr::equivalent(expression, &stored) {
                        found = Some(position);
                        break;
                    }
                }
            }
            let Some(position) = found else {
                return Ok(false);
            };
            matched[position] = true;
        }
        Ok(matched.into_iter().all(|matched| matched))
    }

    fn analyze_assignments(
        &mut self,
        syntax: &[ast::Set],
        table: &Table,
        target: hir::SourceId,
        scope: &Scope,
        in_trigger: bool,
        allow_subqueries: bool,
    ) -> Result<Vec<hir::Assignment>> {
        let mut assignments = Vec::with_capacity(syntax.len());
        for set in syntax {
            let columns = resolve_assignment_columns(table, &set.col_names)?;
            if columns.is_empty() {
                return Err(LimboError::InternalError(
                    "UPDATE assignment has no destination columns".to_string(),
                ));
            }

            let value = self.analyze_assignment_rhs(
                set.expr.as_ref(),
                &columns,
                target,
                scope,
                in_trigger,
                allow_subqueries,
            )?;

            let value_count = self.assignment_value_count(&value)?;
            if value_count != columns.len() {
                crate::bail_parse_error!(
                    "{} columns assigned {} values",
                    columns.len(),
                    value_count
                );
            }
            assignments.push(hir::Assignment { columns, value });
        }
        Ok(assignments)
    }

    fn analyze_assignment_rhs(
        &mut self,
        syntax: &ast::Expr,
        columns: &[TargetColumn],
        target: hir::SourceId,
        scope: &Scope,
        in_trigger: bool,
        allow_subqueries: bool,
    ) -> Result<hir::Expr> {
        if let ast::Expr::Parenthesized(values) = syntax {
            if values.len() == 1 {
                return self.analyze_assignment_rhs(
                    &values[0],
                    columns,
                    target,
                    scope,
                    in_trigger,
                    allow_subqueries,
                );
            }
            if values.len() != columns.len() {
                crate::bail_parse_error!(
                    "{} columns assigned {} values",
                    columns.len(),
                    values.len()
                );
            }
            let mut resolved = Vec::with_capacity(values.len());
            for (value, column) in values.iter().zip(columns.iter().copied()) {
                resolved.push(self.analyze_assignment_value(
                    value,
                    column,
                    target,
                    scope,
                    in_trigger,
                    allow_subqueries,
                )?);
            }
            return Ok(hir::Expr::Row(resolved));
        }
        if let ast::Expr::Subquery(select) = syntax {
            if !allow_subqueries {
                crate::bail_parse_error!("subqueries are prohibited in this expression");
            }
            let source = self.source(target).ok_or_else(|| {
                LimboError::InternalError(format!("missing DML target source {target}"))
            })?;
            let expected_types = columns
                .iter()
                .copied()
                .map(|column| {
                    target_type_fact(column, source)
                        .declared
                        .and_then(|declared| declared.custom().cloned())
                })
                .collect::<Vec<_>>();
            let environment = QueryEnvironment::for_subquery(scope);
            let query =
                self.analyze_query_with_expected_types(select, environment, &expected_types)?;
            return self.subquery_value_expr(query);
        }
        self.analyze_assignment_value(
            syntax,
            columns[0],
            target,
            scope,
            in_trigger,
            allow_subqueries,
        )
    }

    fn analyze_assignment_value(
        &mut self,
        syntax: &ast::Expr,
        column: TargetColumn,
        target: hir::SourceId,
        scope: &Scope,
        in_trigger: bool,
        allow_subqueries: bool,
    ) -> Result<hir::Expr> {
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing DML target source {target}"))
        })?;
        let expected = target_type_fact(column, source)
            .declared
            .and_then(|declared| declared.custom().cloned());
        let mut policy = scalar_expr_policy(in_trigger).with_expected_type(expected);
        if !allow_subqueries {
            policy = policy.without_subqueries();
        }
        self.analyze_expr(syntax, scope, policy)
    }

    fn assignment_value_count(&self, value: &hir::Expr) -> Result<usize> {
        match value {
            hir::Expr::Row(values) => Ok(values.len()),
            _ => Ok(1),
        }
    }
}

fn scalar_expr_policy(in_trigger: bool) -> ExprPolicy {
    if in_trigger {
        ExprPolicy::trigger_predicate().without_aggregate()
    } else {
        ExprPolicy::select().without_aggregate()
    }
}

fn expr_contains_subquery(expr: &ast::Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |expression| {
        if matches!(
            expression,
            ast::Expr::Subquery(_) | ast::Expr::InSelect { .. } | ast::Expr::Exists(_)
        ) {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found
}

fn resolved_table_database_id(table: &hir::ResolvedTable) -> Result<usize> {
    table.database().map(DatabaseId::index).ok_or_else(|| {
        LimboError::InternalError(format!(
            "resolved DML table {} has no database identity",
            table.value().get_name()
        ))
    })
}

fn default_for_target(
    target: TargetColumn,
    defaults: &[hir::ResolvedDefault],
) -> Result<hir::Expr> {
    match target {
        TargetColumn::RowId => Ok(hir::Expr::Literal(ast::Literal::Null)),
        TargetColumn::Column(column) => defaults
            .iter()
            .find(|default| default.column == column)
            .map(|default| default.value.clone())
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "INSERT destination column {column} has no resolved default"
                ))
            }),
    }
}

fn source_identifier(source: &hir::Source) -> String {
    crate::util::normalize_ident(source.alias.as_deref().unwrap_or(&source.name))
}

fn explicit_collation(expr: &hir::Expr) -> Option<&hir::ResolvedCollation> {
    match expr {
        hir::Expr::Collate { collation, .. } => Some(collation),
        _ => None,
    }
}

fn without_explicit_collation(expr: &hir::Expr) -> &hir::Expr {
    match expr {
        hir::Expr::Collate { expr, .. } => without_explicit_collation(expr),
        _ => expr,
    }
}

fn conflict_collation_matches(
    target: &hir::ConflictTerm,
    index_column: &IndexColumn,
    table: &Table,
) -> bool {
    let Some(target_collation) = &target.collation else {
        return true;
    };
    let index_collation = index_column.collation.unwrap_or_else(|| {
        table
            .get_column_by_name(&index_column.name)
            .map(|(_, column)| column.collation())
            .unwrap_or_default()
    });
    target_collation.value() == &index_collation
}
