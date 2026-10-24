//! INSERT conversion into closed semantic HIR.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    dml::trigger_matches_update,
    expr::{ExprPolicies, ExprPolicy},
    hir::{self, HirRoot, SourceOwner},
    scope::{ExprCollation, Scope},
};

#[derive(Clone, Copy)]
pub(super) struct InsertExprContext<'scope> {
    pub(super) outer_scope: &'scope Scope,
    pub(super) policies: ExprPolicies,
}

pub(super) enum InsertBodySyntax<'ast> {
    DefaultValues,
    Select {
        select: &'ast ast::Select,
        upsert: Option<&'ast ast::Upsert>,
    },
}
use crate::{
    schema::{autoincrement_sequence_name, Table, TypeDef, SQLITE_SEQUENCE_TABLE_NAME},
    sync::Arc,
    translate::expr::{walk_expr, WalkControl},
    util::normalize_ident,
    LimboError, Result,
};

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn analyze_insert(
        &mut self,
        with: Option<&'ast ast::With>,
        conflict: Option<ast::ResolveType>,
        table_name: &ast::QualifiedName,
        column_names: &[ast::Name],
        body: &'ast ast::InsertBody,
        returning: &'ast [ast::ResultColumn],
    ) -> Result<HirRoot> {
        let target = self.analyze_base_table_source(table_name, None, None, SourceOwner::Root)?;
        let body = match body {
            ast::InsertBody::DefaultValues => InsertBodySyntax::DefaultValues,
            ast::InsertBody::Select(select, upsert) => InsertBodySyntax::Select {
                select,
                upsert: upsert.as_deref(),
            },
        };
        let empty_scope = Scope::default();
        let expressions = InsertExprContext {
            outer_scope: &empty_scope,
            policies: ExprPolicies::statement(self.context().dqs_dml()),
        };
        self.analyze_insert_target(
            with,
            conflict,
            column_names,
            body,
            returning,
            target,
            expressions,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn analyze_insert_target(
        &mut self,
        with: Option<&'ast ast::With>,
        conflict: Option<ast::ResolveType>,
        column_names: &[ast::Name],
        body: InsertBodySyntax<'ast>,
        returning: &'ast [ast::ResultColumn],
        target: hir::SourceId,
        expressions: InsertExprContext<'_>,
    ) -> Result<HirRoot> {
        let table = match &self
            .source(target)
            .ok_or_else(|| {
                LimboError::InternalError(format!("missing INSERT target source {target}"))
            })?
            .kind
        {
            hir::SourceKind::Table(table) => table.clone(),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "INSERT target {target} is not a catalog table"
                )));
            }
        };
        let autoincrement = if table.value().btree().is_some() {
            self.analyze_btree_write_metadata(target, &table)?;
            self.analyze_autoincrement(&table)?
        } else {
            None
        };

        self.with_cte_scope(with, |analyzer| {
            analyzer.analyze_insert_body(
                conflict,
                column_names,
                body,
                returning,
                target,
                &table,
                autoincrement,
                expressions,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_insert_body(
        &mut self,
        conflict: Option<ast::ResolveType>,
        column_names: &[ast::Name],
        body: InsertBodySyntax<'ast>,
        returning: &'ast [ast::ResultColumn],
        target: hir::SourceId,
        table: &hir::ResolvedTable,
        autoincrement: Option<hir::ResolvedAutoincrement>,
        expressions: InsertExprContext<'_>,
    ) -> Result<HirRoot> {
        let virtual_target = table.value().virtual_table().is_some();
        let (columns, source, upserts, excluded_source) = match body {
            InsertBodySyntax::DefaultValues => {
                if !column_names.is_empty() {
                    crate::bail_parse_error!("0 values for {} columns", column_names.len());
                }
                (
                    Vec::new(),
                    hir::InsertSource::DefaultValues,
                    Vec::new(),
                    None,
                )
            }
            InsertBodySyntax::Select { select, upsert } => {
                if virtual_target
                    && (upsert.is_some()
                        || !is_simple_values(select)
                        || !matches!(select.body.select, ast::OneSelect::Values(_)))
                {
                    return unsupported_insert(
                        "UPSERT or non-VALUES sources for virtual-table targets",
                    );
                }
                let columns = resolve_insert_targets(table.value(), column_names)?;
                let expected = columns.len();
                let source = match &select.body.select {
                    ast::OneSelect::Values(rows) if is_simple_values(select) => {
                        if rows.is_empty() {
                            crate::bail_parse_error!("no values to insert");
                        }
                        let mut bound_rows = Vec::with_capacity(rows.len());
                        for row in rows {
                            if row.len() != expected {
                                crate::bail_parse_error!(
                                    "table {} has {expected} columns but {} values were supplied",
                                    table.value().get_name(),
                                    row.len()
                                );
                            }
                            let mut bound = Vec::with_capacity(row.len());
                            for (syntax, target_column) in row.iter().zip(&columns) {
                                bound.push(self.analyze_insert_value(
                                    syntax,
                                    target,
                                    table.value(),
                                    target_column.column,
                                    expressions,
                                )?);
                            }
                            bound_rows.push(bound);
                        }
                        hir::InsertSource::Values(bound_rows)
                    }
                    ast::OneSelect::Select { .. } | ast::OneSelect::Values(_) => {
                        let expected_outputs = columns
                            .iter()
                            .map(|target_column| {
                                self.write_target_type(target, table.value(), target_column.column)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let query =
                            self.analyze_insert_select(select, &expected_outputs, expressions)?;
                        let actual = self
                            .query(query)
                            .ok_or_else(|| {
                                LimboError::InternalError(format!(
                                    "missing INSERT source query {query}"
                                ))
                            })?
                            .output
                            .len();
                        if actual != expected {
                            crate::bail_parse_error!(
                                "table {} has {expected} columns but {actual} values were supplied",
                                table.value().get_name()
                            );
                        }
                        hir::InsertSource::Query(query)
                    }
                };
                let (upserts, excluded_source) =
                    self.analyze_upserts(upsert, target, table, &columns, expressions.policies)?;
                (columns, source, upserts, excluded_source)
            }
        };
        let returning =
            self.analyze_dml_returning_with_policies(returning, target, expressions.policies)?;
        let upsert_new_source = if !virtual_target
            && upserts
                .iter()
                .any(|upsert| matches!(upsert.action, hir::UpsertAction::Update { .. }))
        {
            let new_source = self.create_update_new_source(target, table)?;
            self.analyze_btree_write_metadata(new_source, table)?;
            Some(new_source)
        } else {
            None
        };
        let target_kind = if virtual_target {
            hir::InsertTargetKind::Virtual
        } else {
            hir::InsertTargetKind::BTree {
                autoincrement,
                defaults: self.analyze_insert_defaults(target, table.value(), &columns)?,
                replace_defaults: self.analyze_replace_defaults(target, table.value())?,
                triggers: self.analyze_insert_triggers(table, &upserts)?,
                foreign_keys: self.analyze_dml_foreign_keys(table, target, None)?,
                capture_changes: self.analyze_capture_changes(table)?,
            }
        };

        Ok(HirRoot::Insert(hir::Insert {
            target,
            target_kind,
            columns,
            source,
            conflict,
            upserts,
            excluded_source,
            upsert_new_source,
            returning,
        }))
    }

    fn analyze_insert_select(
        &mut self,
        select: &'ast ast::Select,
        expected_outputs: &[Option<Arc<TypeDef>>],
        expressions: InsertExprContext<'_>,
    ) -> Result<hir::QueryId> {
        self.analyze_subquery_with_expected_outputs(
            select,
            None,
            expressions.outer_scope,
            expected_outputs,
            expressions.policies,
        )
    }

    pub(super) fn analyze_autoincrement(
        &mut self,
        table: &hir::ResolvedTable,
    ) -> Result<Option<hir::ResolvedAutoincrement>> {
        if !table
            .value()
            .btree()
            .is_some_and(|table| table.has_autoincrement)
        {
            return Ok(None);
        }
        let database = table.database().ok_or_else(|| {
            LimboError::InternalError("AUTOINCREMENT target has no owning database".to_string())
        })?;
        let sequence_name = autoincrement_sequence_name(table.value().get_name());
        let backing_table_name =
            crate::translate::sequence::sequence_backing_table_name(&sequence_name);
        let (sqlite_sequence, sequence, backing_table) = {
            let schema = self.context().schema(database)?;
            let sqlite_sequence = schema
                .get_table(SQLITE_SEQUENCE_TABLE_NAME)
                .ok_or_else(|| LimboError::Corrupt("missing sqlite_sequence table".to_string()))?;
            let sequence = schema.get_sequence(&sequence_name).cloned();
            let backing_table = schema.get_table(&backing_table_name);
            (sqlite_sequence, sequence, backing_table)
        };
        let sqlite_sequence_id = self.catalog_object_id(
            Some(database),
            CatalogObjectKind::Table,
            SQLITE_SEQUENCE_TABLE_NAME,
        );
        let sqlite_sequence = hir::CatalogObject::new(
            sqlite_sequence_id,
            self.context().snapshot(),
            Some(database),
            sqlite_sequence,
        );
        let mvcc_sequence = match (sequence, backing_table) {
            (Some(sequence), Some(backing_table)) => {
                let sequence_id = self.catalog_object_id(
                    Some(database),
                    CatalogObjectKind::Sequence,
                    sequence_name.clone(),
                );
                let sequence = hir::CatalogObject::new(
                    sequence_id,
                    self.context().snapshot(),
                    Some(database),
                    sequence,
                );
                let backing_table_id = self.catalog_object_id(
                    Some(database),
                    CatalogObjectKind::Table,
                    backing_table_name,
                );
                let backing_table = hir::CatalogObject::new(
                    backing_table_id,
                    self.context().snapshot(),
                    Some(database),
                    backing_table,
                );
                Some(hir::SequenceOperation {
                    kind: hir::SequenceOperationKind::NextValue,
                    normalized_name: sequence_name,
                    sequence,
                    backing_table,
                    sqlite_sequence: Some(sqlite_sequence.clone()),
                })
            }
            _ => None,
        };
        let table_name = table.value().get_name();
        let inserts_into_table = |schema: &crate::schema::Schema| {
            schema
                .triggers
                .values()
                .flatten()
                .flat_map(|trigger| trigger.commands.iter())
                .any(|command| {
                    matches!(command, ast::TriggerCmd::Insert { tbl_name, .. }
                        if normalize_ident(tbl_name.as_str()).eq_ignore_ascii_case(table_name))
                })
        };
        let temp = hir::DatabaseId::new(crate::TEMP_DB_ID);
        let triggers_insert_into_table = inserts_into_table(self.context().schema(database)?)
            || (database != temp
                && self.context().has_temp_database()
                && inserts_into_table(self.context().schema(temp)?));
        Ok(Some(hir::ResolvedAutoincrement {
            sqlite_sequence,
            mvcc_sequence,
            triggers_insert_into_table,
        }))
    }

    fn analyze_insert_triggers(
        &mut self,
        table: &hir::ResolvedTable,
        upserts: &[hir::Upsert],
    ) -> Result<hir::InsertTriggers> {
        let mut has_upsert_update = false;
        let mut updated_names = Vec::new();
        for upsert in upserts {
            if let hir::UpsertAction::Update { assignments, .. } = &upsert.action {
                has_upsert_update = true;
                updated_names.extend(
                    assignments
                        .iter()
                        .flat_map(|assignment| assignment.names.iter().cloned()),
                );
            }
        }
        let insert = self.table_triggers(table, |trigger| {
            matches!(trigger.event, ast::TriggerEvent::Insert)
        })?;
        let upsert_update = if has_upsert_update {
            self.table_triggers(table, |trigger| {
                trigger_matches_update(trigger, &updated_names)
            })?
        } else {
            Vec::new()
        };
        Ok(hir::InsertTriggers {
            insert,
            upsert_update,
        })
    }

    pub(super) fn analyze_btree_write_metadata(
        &mut self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
    ) -> Result<()> {
        let btree = table.value().btree().ok_or_else(|| {
            LimboError::InternalError(format!(
                "write target {} stopped being a B-tree table",
                table.value().get_name()
            ))
        })?;
        let scope = {
            let source = self.source(target).ok_or_else(|| {
                LimboError::InternalError(format!("missing write target source {target}"))
            })?;
            let mut scope = Scope::default();
            scope.add_schema_source(source);
            scope
        };
        let policy = ExprPolicy::schema_expression().with_self_source(target);

        let mut check_constraints = Vec::with_capacity(btree.check_constraints.len());
        for (catalog_position, constraint) in btree.check_constraints.iter().enumerate() {
            check_constraints.push(hir::CheckConstraint {
                catalog_position,
                expression: self.analyze_expr(&constraint.expr, &scope, policy)?,
                description: constraint.error_description(),
                name: constraint.name.clone(),
            });
        }

        self.analyze_btree_index_metadata(target, table, &scope, policy)?;
        let source = self.source_mut(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing write target source {target}"))
        })?;
        source.check_constraints = Some(check_constraints);
        Ok(())
    }

    pub(super) fn analyze_btree_delete_metadata(
        &mut self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
    ) -> Result<()> {
        if table.value().btree().is_none() {
            return Err(LimboError::InternalError(format!(
                "DELETE target {} stopped being a B-tree table",
                table.value().get_name()
            )));
        }
        let scope = {
            let source = self.source(target).ok_or_else(|| {
                LimboError::InternalError(format!("missing DELETE target source {target}"))
            })?;
            let mut scope = Scope::default();
            scope.add_schema_source(source);
            scope
        };
        let policy = ExprPolicy::schema_expression().with_self_source(target);
        self.analyze_btree_index_metadata(target, table, &scope, policy)
    }

    pub(super) fn analyze_btree_index_metadata(
        &mut self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<()> {
        let database = table.database().ok_or_else(|| {
            LimboError::InternalError("B-tree target has no owning database".to_string())
        })?;
        let indexes = self
            .context()
            .table_indexes(database, table.value().get_name())?;
        let mut index_expressions = Vec::with_capacity(indexes.len());
        let mut index_ids = Vec::with_capacity(indexes.len());
        for index in indexes {
            let index_id = self.catalog_object_id(
                table.database(),
                CatalogObjectKind::Index,
                normalize_ident(&index.name),
            );
            let resolved = hir::CatalogObject::new(
                index_id,
                self.context().snapshot(),
                table.database(),
                index,
            );
            let columns = resolved
                .value()
                .columns
                .iter()
                .map(|column| {
                    column
                        .expr
                        .as_deref()
                        .map(|syntax| self.analyze_expr(syntax, scope, policy))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let predicate = resolved
                .value()
                .where_clause
                .as_deref()
                .map(|syntax| self.analyze_expr(syntax, scope, policy))
                .transpose()?;
            index_ids.push(index_id);
            index_expressions.push(hir::IndexExpressions {
                index: resolved,
                columns,
                predicate,
            });
        }

        let source = self.source_mut(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing index-metadata source {target}"))
        })?;
        source.index_expressions = index_expressions;
        source.index_coverage = hir::IndexCoverage::Complete { indexes: index_ids };
        Ok(())
    }

    fn analyze_insert_value(
        &mut self,
        syntax: &'ast ast::Expr,
        target: hir::SourceId,
        table: &Table,
        column: hir::TargetColumn,
        expressions: InsertExprContext<'_>,
    ) -> Result<hir::Expr> {
        if matches!(syntax, ast::Expr::Default) {
            return self.analyze_write_default(target, table, column);
        }
        let policy = expressions.policies.insert_values();
        Ok(self
            .analyze_root_expr_with_expected_type(
                syntax,
                expressions.outer_scope,
                policy,
                self.write_target_type(target, table, column)?,
            )?
            .expr)
    }

    fn analyze_insert_defaults(
        &mut self,
        target: hir::SourceId,
        table: &Table,
        columns: &[hir::InsertTarget],
    ) -> Result<Vec<hir::ResolvedDefault>> {
        let mut supplied = vec![false; table.columns().len()];
        for target in columns {
            if let hir::TargetColumn::Column(column) = target.column {
                supplied[column] = true;
            }
        }
        let missing = table
            .columns()
            .iter()
            .enumerate()
            .filter(|(column, definition)| {
                !definition.hidden() && !definition.is_generated() && !supplied[*column]
            })
            .map(|(column, _)| column)
            .collect::<Vec<_>>();
        let mut defaults = Vec::with_capacity(missing.len());
        for column in missing {
            defaults.push(hir::ResolvedDefault {
                column,
                value: self.analyze_write_default(
                    target,
                    table,
                    hir::TargetColumn::Column(column),
                )?,
            });
        }
        Ok(defaults)
    }

    /// The DEFAULT of each NOT NULL column that declares one, for NOT NULL
    /// ON CONFLICT REPLACE.
    pub(super) fn analyze_replace_defaults(
        &mut self,
        target: hir::SourceId,
        table: &Table,
    ) -> Result<Vec<hir::ResolvedDefault>> {
        let columns = table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                column.notnull() && !column.is_rowid_alias() && column.default.is_some()
            })
            .map(|(column, _)| column)
            .collect::<Vec<_>>();
        let mut defaults = Vec::with_capacity(columns.len());
        for column in columns {
            defaults.push(hir::ResolvedDefault {
                column,
                value: self.analyze_write_default(
                    target,
                    table,
                    hir::TargetColumn::Column(column),
                )?,
            });
        }
        Ok(defaults)
    }

    pub(super) fn analyze_write_default(
        &mut self,
        target: hir::SourceId,
        table: &Table,
        column: hir::TargetColumn,
    ) -> Result<hir::Expr> {
        let syntax = match column {
            hir::TargetColumn::RowId => ast::Expr::Literal(ast::Literal::Null),
            hir::TargetColumn::Column(column) => {
                let definition = table.columns().get(column).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "write target column {column} is outside table {}",
                        table.get_name()
                    ))
                })?;
                match &definition.default {
                    Some(default) => default.as_ref().clone(),
                    None => {
                        let database = self
                            .source(target)
                            .and_then(|source| source.database)
                            .ok_or_else(|| {
                                LimboError::InternalError(format!(
                                    "write target source {target} has no owning database"
                                ))
                            })?;
                        let type_default = self
                            .context()
                            .schema(database)?
                            .resolve_type(&definition.ty_str, table.is_strict())?
                            .and_then(|resolved| resolved.default_expr().cloned());
                        type_default.unwrap_or(ast::Expr::Literal(ast::Literal::Null))
                    }
                }
            }
        };
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing write target source {target}"))
        })?;
        let mut scope = Scope::default();
        scope.add_schema_source(source);
        self.analyze_expr_with_expected_type(
            &syntax,
            &scope,
            ExprPolicy::schema_expression().with_self_source(target),
            self.write_target_type(target, table, column)?,
        )
    }

    pub(super) fn write_target_type(
        &self,
        target: hir::SourceId,
        table: &Table,
        column: hir::TargetColumn,
    ) -> Result<Option<Arc<TypeDef>>> {
        let hir::TargetColumn::Column(column) = column else {
            return Ok(None);
        };
        let definition = table.columns().get(column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "write target column {column} is outside table {}",
                table.get_name()
            ))
        })?;
        let database = self
            .source(target)
            .and_then(|source| source.database)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "write target source {target} has no owning database"
                ))
            })?;
        Ok(self
            .context()
            .schema(database)?
            .get_type_def_unchecked(&definition.ty_str)
            .cloned())
    }

    fn analyze_upserts(
        &mut self,
        mut syntax: Option<&'ast ast::Upsert>,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
        columns: &[hir::InsertTarget],
        policies: ExprPolicies,
    ) -> Result<(Vec<hir::Upsert>, Option<hir::SourceId>)> {
        let scope = {
            let source = self.source(target).ok_or_else(|| {
                LimboError::InternalError(format!("missing INSERT target source {target}"))
            })?;
            let mut scope = Scope::default();
            scope.add_source(source, true);
            scope
        };
        let policy = policies.schema_expression().with_self_source(target);
        let mut targets = Vec::new();
        while let Some(upsert) = syntax {
            let conflict_target = upsert
                .index
                .as_ref()
                .map(|conflict| {
                    // Like SQLite, a conflict target cannot name NULLS FIRST
                    // or NULLS LAST.
                    if let Some(nulls) = conflict.targets.iter().find_map(|term| term.nulls) {
                        crate::bail_parse_error!("unsupported use of {}", nulls);
                    }
                    let terms = conflict
                        .targets
                        .iter()
                        .map(|term| {
                            let resolved =
                                self.analyze_resolved_expr(&term.expr, &scope, policy)?;
                            Ok(hir::ConflictTerm {
                                collation: match resolved.collation {
                                    ExprCollation::Explicit(collation) => Some(collation),
                                    ExprCollation::Absent
                                    | ExprCollation::Inherited(_)
                                    | ExprCollation::InheritedBinary(_) => None,
                                },
                                expr: resolved.expr,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let predicate = conflict
                        .where_clause
                        .as_deref()
                        .map(|predicate| self.analyze_expr(predicate, &scope, policy))
                        .transpose()?;
                    let matched_index =
                        self.match_conflict_target(target, table, &terms, predicate.as_ref())?;
                    Ok::<_, LimboError>(hir::ConflictTarget {
                        terms,
                        predicate,
                        matched_index,
                    })
                })
                .transpose()?;
            targets.push((conflict_target, &upsert.do_clause));
            syntax = upsert.next.as_deref();
        }
        let reachable = self.reachable_upserts(target, table, columns, &targets)?;
        let mut upserts = Vec::new();
        let mut excluded_source = None;
        for ((conflict_target, do_clause), reachable) in targets.into_iter().zip(reachable) {
            if !reachable {
                continue;
            }
            let action = self.analyze_upsert_action(
                do_clause,
                target,
                table,
                &mut excluded_source,
                policies,
            )?;
            upserts.push(hir::Upsert {
                target: conflict_target,
                action,
            });
        }
        Ok((upserts, excluded_source))
    }

    /// Like SQLite, which UPSERTs some constraint check can jump to: the
    /// first one naming the rowid when the INSERT sets the rowid, the first
    /// one naming each UNIQUE index, and the last one without a target for
    /// the other constraints. The others are never compiled, so errors in
    /// their DO UPDATE and in UPDATE triggers do not fail the INSERT.
    fn reachable_upserts(
        &self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
        columns: &[hir::InsertTarget],
        upserts: &[(Option<hir::ConflictTarget>, &ast::UpsertDo)],
    ) -> Result<Vec<bool>> {
        if table.value().btree().is_none() {
            return Ok(vec![true; upserts.len()]);
        }
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing INSERT target source {target}"))
        })?;
        let sets_rowid = columns.iter().any(|column| {
            column.uses_value
                && match column.column {
                    hir::TargetColumn::RowId => true,
                    hir::TargetColumn::Column(position) => table
                        .value()
                        .columns()
                        .get(position)
                        .is_some_and(|column| column.is_rowid_alias()),
                }
        });
        let constraints = sets_rowid
            .then_some(None)
            .into_iter()
            .chain(
                source
                    .index_expressions
                    .iter()
                    .filter(|expressions| expressions.index.value().unique)
                    .map(|expressions| Some(&expressions.index)),
            )
            .collect::<Vec<_>>();
        let named = |constraint: Option<&hir::ResolvedIndex>| {
            upserts.iter().position(|(conflict_target, _)| {
                conflict_target.as_ref().is_some_and(|conflict_target| {
                    conflict_target.matched_index.as_ref() == constraint
                })
            })
        };
        let catch_all = upserts
            .last()
            .filter(|(conflict_target, _)| conflict_target.is_none())
            .map(|_| upserts.len() - 1);
        let mut reachable = vec![false; upserts.len()];
        for constraint in constraints {
            if let Some(position) = named(constraint).or(catch_all) {
                reachable[position] = true;
            }
        }
        Ok(reachable)
    }

    /// The constraint an ON CONFLICT target names, matched as SQLite does:
    /// `None` for the rowid, named by `rowid` or the INTEGER PRIMARY KEY column
    /// without COLLATE, otherwise the
    /// first UNIQUE index whose key has exactly the target's terms, in any
    /// order. A column term matches a plain key column, an expression term
    /// an equivalent expression column, and a COLLATE on a term must be the
    /// key column's collation. A partial index also needs an equivalent
    /// target WHERE clause.
    fn match_conflict_target(
        &self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
        terms: &[hir::ConflictTerm],
        predicate: Option<&hir::Expr>,
    ) -> Result<Option<hir::ResolvedIndex>> {
        // Strip COLLATE, keeping the outermost collation.
        fn unwrap(
            term: &hir::ConflictTerm,
        ) -> (&hir::Expr, Option<crate::translate::collate::CollationSeq>) {
            let mut expression = &term.expr;
            let mut collation = term.collation.as_ref().map(|collation| *collation.value());
            while let hir::Expr::Collate {
                expr,
                collation: inner,
            } = expression
            {
                collation.get_or_insert(*inner.value());
                expression = expr;
            }
            (expression, collation)
        }
        let target_column = |expression: &hir::Expr| match expression {
            hir::Expr::Column(column) if column.source == target => Some(column.column),
            _ => None,
        };
        let columns = table.value().columns();
        if let [term] = terms {
            // SQLite takes the rowid only from a bare term, without COLLATE:
            // `rowid` or the INTEGER PRIMARY KEY column.
            let names_rowid = match &term.expr {
                hir::Expr::RowId(source) => *source == target,
                expression => target_column(expression)
                    .is_some_and(|column| columns.get(column).is_some_and(|c| c.is_rowid_alias())),
            };
            if names_rowid {
                return Ok(None);
            }
        }
        let source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing INSERT target source {target}"))
        })?;
        for expressions in &source.index_expressions {
            let index = expressions.index.value();
            let predicate_matches = match (&expressions.predicate, predicate) {
                (Some(index_predicate), Some(predicate)) => index_predicate.equivalent(predicate),
                (Some(_), None) => false,
                (None, _) => true,
            };
            if !index.unique || !predicate_matches || terms.len() != index.columns.len() {
                continue;
            }
            let key_collation = |position: usize| {
                let key_column = &index.columns[position];
                key_column.collation.unwrap_or_else(|| {
                    columns
                        .get(key_column.pos_in_table)
                        .map(|column| column.collation())
                        .unwrap_or_default()
                })
            };
            let mut matched = vec![false; index.columns.len()];
            let all_match = terms.iter().all(|term| {
                let (expression, collation) = unwrap(term);
                let found = (0..index.columns.len()).find(|&position| {
                    if matched[position] {
                        return false;
                    }
                    // A column names an index column on it, generated or
                    // not; an expression names only an expression column,
                    // never a generated column that computes it.
                    let key_column = &index.columns[position];
                    let is_expression_column =
                        key_column.pos_in_table == crate::schema::EXPR_INDEX_SENTINEL;
                    let same_key = match (target_column(expression), &expressions.columns[position])
                    {
                        (Some(column), _) => {
                            !is_expression_column && key_column.pos_in_table == column
                        }
                        (None, Some(key)) => is_expression_column && key.equivalent(expression),
                        (None, None) => false,
                    };
                    same_key
                        && collation.is_none_or(|collation| collation == key_collation(position))
                });
                if let Some(position) = found {
                    matched[position] = true;
                }
                found.is_some()
            });
            if all_match {
                return Ok(Some(expressions.index.clone()));
            }
        }
        crate::bail_parse_error!(
            "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"
        )
    }

    fn analyze_upsert_action(
        &mut self,
        syntax: &'ast ast::UpsertDo,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
        excluded_source: &mut Option<hir::SourceId>,
        policies: ExprPolicies,
    ) -> Result<hir::UpsertAction> {
        let ast::UpsertDo::Set { sets, where_clause } = syntax else {
            return Ok(hir::UpsertAction::Nothing);
        };
        let excluded = match *excluded_source {
            Some(excluded) => excluded,
            None => {
                let excluded = self.create_excluded_source(target, table)?;
                *excluded_source = Some(excluded);
                excluded
            }
        };
        let scope = {
            let mut excluded_scope = Scope::default();
            excluded_scope.add_source(
                self.source(excluded).ok_or_else(|| {
                    LimboError::InternalError(format!("missing INSERT EXCLUDED source {excluded}"))
                })?,
                false,
            );
            let mut scope = self.new_scope(Some(excluded_scope));
            scope.add_source(
                self.source(target).ok_or_else(|| {
                    LimboError::InternalError(format!("missing INSERT target source {target}"))
                })?,
                true,
            );
            scope
        };
        let policy = policies.insert_values();
        let mut assignments = Vec::<hir::Assignment>::new();
        for set in sets {
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
            for (name, value) in set.col_names.iter().zip(values) {
                let column = resolve_upsert_target(table.value(), name)?;
                if expression_contains_subquery(value) {
                    crate::bail_parse_error!("Subquery is not supported in this position");
                }
                let value = self.analyze_expr_with_expected_type(
                    value,
                    &scope,
                    policy,
                    self.write_target_type(target, table.value(), column)?,
                )?;
                match assignments
                    .iter_mut()
                    .find(|assignment| assignment.columns == [column])
                {
                    Some(existing) => {
                        // Every name the column was set under fires its
                        // `UPDATE OF` triggers.
                        let name = normalize_ident(name.as_str());
                        if !existing.names.contains(&name) {
                            existing.names.push(name);
                        }
                        existing.value = value;
                    }
                    None => assignments.push(hir::Assignment {
                        columns: vec![column],
                        names: vec![normalize_ident(name.as_str())],
                        value,
                    }),
                }
            }
        }
        let predicate = where_clause
            .as_deref()
            .map(|predicate| {
                if expression_contains_subquery(predicate) {
                    crate::bail_parse_error!("Subquery is not supported in this position");
                }
                self.analyze_expr(predicate, &scope, policy)
            })
            .transpose()?;
        Ok(hir::UpsertAction::Update {
            assignments,
            predicate,
        })
    }

    fn create_excluded_source(
        &mut self,
        target: hir::SourceId,
        table: &hir::ResolvedTable,
    ) -> Result<hir::SourceId> {
        let target_source = self.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("missing INSERT target source {target}"))
        })?;
        let columns = target_source.columns.clone();
        // EXCLUDED holds the row as it would have been written, so reading
        // it decodes custom-typed values like reading the target does.
        let column_type_programs = target_source.column_type_programs.clone();
        let rowid_available = target_source.rowid_available;
        let width = columns.len();
        let excluded = self.reserve_source();
        self.insert_source(
            excluded,
            hir::Source {
                id: excluded,
                owner: SourceOwner::Root,
                database: table.database(),
                name: "excluded".to_string(),
                alias: None,
                kind: hir::SourceKind::Pseudo {
                    kind: hir::PseudoSource::Excluded,
                    table: table.clone(),
                },
                columns,
                generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
                default_expressions: vec![hir::ColumnReadExpression::Absent; width],
                column_type_programs,
                check_constraints: None,
                rowid_available,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(excluded)
    }
}

fn is_simple_values(select: &ast::Select) -> bool {
    select.with.is_none()
        && select.body.compounds.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
}

/// The column a DO UPDATE assignment sets. Like SQLite, `rowid`, `oid` and
/// `_rowid_` name the rowid unless the table has a column of that name; the
/// rowid is the INTEGER PRIMARY KEY column when the table has one.
fn resolve_upsert_target(table: &Table, name: &ast::Name) -> Result<hir::TargetColumn> {
    let normalized = normalize_ident(name.as_str());
    if let Some((column, definition)) = table.get_column_by_name(&normalized) {
        definition.ensure_not_generated("UPDATE", name.as_str())?;
        return Ok(hir::TargetColumn::Column(column));
    }
    if table.btree().is_some_and(|btree| btree.has_rowid)
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
    crate::bail_parse_error!("no such column: {}", name)
}

fn resolve_insert_targets(table: &Table, names: &[ast::Name]) -> Result<Vec<hir::InsertTarget>> {
    if names.is_empty() {
        return Ok(table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| !column.hidden() && !column.is_generated())
            .map(|(column, _)| hir::InsertTarget {
                column: hir::TargetColumn::Column(column),
                uses_value: true,
            })
            .collect());
    }

    let mut targets = Vec::<hir::InsertTarget>::with_capacity(names.len());
    for name in names {
        let normalized = normalize_ident(name.as_str());
        let column = if let Some((column, definition)) = table.get_column_by_name(&normalized) {
            definition.ensure_not_generated("INSERT into", &normalized)?;
            hir::TargetColumn::Column(column)
        } else if is_rowid_name(&normalized) && table_has_rowid(table) {
            table
                .columns()
                .iter()
                .position(|column| column.is_rowid_alias())
                .map_or(hir::TargetColumn::RowId, hir::TargetColumn::Column)
        } else {
            crate::bail_parse_error!(
                "table {} has no column named {}",
                table.get_name(),
                normalized
            );
        };
        let rowid = is_rowid_target(table, column);
        let duplicate = targets
            .iter_mut()
            .filter(|target| target.column == column)
            .last();
        let uses_value = match duplicate {
            Some(previous) if rowid => {
                previous.uses_value = false;
                true
            }
            Some(_) => false,
            None => true,
        };
        targets.push(hir::InsertTarget { column, uses_value });
    }
    Ok(targets)
}

fn is_rowid_target(table: &Table, target: hir::TargetColumn) -> bool {
    match target {
        hir::TargetColumn::RowId => true,
        hir::TargetColumn::Column(column) => table
            .columns()
            .get(column)
            .is_some_and(|column| column.is_rowid_alias()),
    }
}

fn table_has_rowid(table: &Table) -> bool {
    table.btree().is_some_and(|table| table.has_rowid) || table.virtual_table().is_some()
}

fn is_rowid_name(name: &str) -> bool {
    ["rowid", "_rowid_", "oid"]
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(name))
}

fn expression_contains_subquery(expression: &ast::Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expression, &mut |expression| {
        if matches!(
            expression,
            ast::Expr::Subquery(_)
                | ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::InTable { .. }
        ) {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found
}

fn unsupported_insert<T>(feature: &str) -> Result<T> {
    Err(LimboError::ParseError(format!(
        "semantic INSERT does not yet accept {feature}"
    )))
}
