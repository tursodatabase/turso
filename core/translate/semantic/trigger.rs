//! Trigger expression and command conversion into resolved HIR.

use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    delete::DeleteExprContext,
    expr::ExprPolicies,
    hir::{self, CatalogObject, SourceOwner},
    insert::{InsertBodySyntax, InsertExprContext},
    query::table_has_rowid,
    scope::Scope,
    update::{UpdateBodySyntax, UpdateExprContext},
    TriggerAnalysis, TriggerDelete, TriggerInsert, TriggerProgramInput, TriggerUpdate,
};
use crate::{util::normalize_ident, LimboError, Result};

#[derive(Clone, Copy)]
enum TriggerRow {
    New,
    Old,
}

impl TriggerRow {
    fn name(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Old => "old",
        }
    }

    fn pseudo_source(self) -> hir::PseudoSource {
        match self {
            Self::New => hir::PseudoSource::New,
            Self::Old => hir::PseudoSource::Old,
        }
    }
}

impl<'ast> Analyzer<'_, '_, 'ast> {
    pub(super) fn analyze_trigger_program(
        &mut self,
        input: TriggerProgramInput<'ast>,
    ) -> Result<hir::HirRoot> {
        let database = input.command_database;
        let (environment, scope) = self.create_trigger_environment(input.context)?;
        let predicate = input
            .predicate
            .map(|expression| self.analyze_trigger_predicate_in(&environment, &scope, expression))
            .transpose()?;
        let commands = input
            .commands
            .iter()
            .map(|command| {
                self.analyze_trigger_command_in(
                    database,
                    &environment,
                    &scope,
                    command,
                    input.conflict_override,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(hir::HirRoot::Trigger(hir::TriggerRoot {
            environment,
            body: hir::TriggerBody::Program(hir::TriggerProgram {
                predicate,
                commands,
            }),
        }))
    }

    #[cfg(test)]
    pub(super) fn analyze_trigger_predicate(
        &mut self,
        context: TriggerAnalysis,
        expression: &'ast ast::Expr,
    ) -> Result<hir::HirRoot> {
        let (environment, scope) = self.create_trigger_environment(context)?;
        let expression = self.analyze_trigger_predicate_in(&environment, &scope, expression)?;

        Ok(hir::HirRoot::Trigger(hir::TriggerRoot {
            environment,
            body: hir::TriggerBody::Predicate(expression),
        }))
    }

    #[cfg(test)]
    pub(super) fn analyze_trigger_select(
        &mut self,
        context: TriggerAnalysis,
        select: &'ast ast::Select,
    ) -> Result<hir::HirRoot> {
        let (environment, scope) = self.create_trigger_environment(context)?;
        let command = self.analyze_trigger_select_in(&environment, &scope, select)?;
        Ok(hir::HirRoot::Trigger(hir::TriggerRoot {
            environment,
            body: hir::TriggerBody::Command(command),
        }))
    }

    #[cfg(test)]
    pub(super) fn analyze_trigger_insert(
        &mut self,
        context: TriggerAnalysis,
        insert: TriggerInsert<'ast>,
    ) -> Result<hir::HirRoot> {
        let database = Some(context.database);
        let (environment, scope) = self.create_trigger_environment(context)?;
        let command = self.analyze_trigger_insert_in(database, &environment, &scope, insert)?;
        Ok(hir::HirRoot::Trigger(hir::TriggerRoot {
            environment,
            body: hir::TriggerBody::Command(command),
        }))
    }

    #[cfg(test)]
    pub(super) fn analyze_trigger_update(
        &mut self,
        context: TriggerAnalysis,
        update: TriggerUpdate<'ast>,
    ) -> Result<hir::HirRoot> {
        let database = Some(context.database);
        let (environment, scope) = self.create_trigger_environment(context)?;
        let command = self.analyze_trigger_update_in(database, &environment, &scope, update)?;
        Ok(hir::HirRoot::Trigger(hir::TriggerRoot {
            environment,
            body: hir::TriggerBody::Command(command),
        }))
    }

    #[cfg(test)]
    pub(super) fn analyze_trigger_delete(
        &mut self,
        context: TriggerAnalysis,
        delete: TriggerDelete<'ast>,
    ) -> Result<hir::HirRoot> {
        let database = Some(context.database);
        let (environment, scope) = self.create_trigger_environment(context)?;
        let command = self.analyze_trigger_delete_in(database, &environment, &scope, delete)?;
        Ok(hir::HirRoot::Trigger(hir::TriggerRoot {
            environment,
            body: hir::TriggerBody::Command(command),
        }))
    }

    fn analyze_trigger_predicate_in(
        &mut self,
        environment: &hir::TriggerEnvironment,
        scope: &Scope,
        expression: &'ast ast::Expr,
    ) -> Result<hir::Expr> {
        Ok(self
            .analyze_root_expr(
                expression,
                scope,
                ExprPolicies::trigger(self.context().dqs_dml(), environment).where_clause(),
            )?
            .expr)
    }

    fn analyze_trigger_select_in(
        &mut self,
        environment: &hir::TriggerEnvironment,
        scope: &Scope,
        select: &'ast ast::Select,
    ) -> Result<hir::TriggerCommand> {
        let policies = ExprPolicies::trigger(self.context().dqs_dml(), environment);
        let first_source = self.source_count();
        let query = self.analyze_subquery(select, None, scope, policies)?;
        self.reject_unsafe_virtual_table_reads(first_source)?;
        Ok(hir::TriggerCommand::Select(query))
    }

    /// Like SQLite, a trigger may only read a virtual table whose module
    /// marks it innocuous. The sources the SELECT added, including those of
    /// its subqueries, start at `first_source`.
    fn reject_unsafe_virtual_table_reads(&self, first_source: usize) -> Result<()> {
        for source in self.sources_from(first_source) {
            let table = match &source.kind {
                hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => {
                    table
                }
                _ => continue,
            };
            if let Some(virtual_table) = table.value().virtual_table() {
                if !virtual_table.innocuous {
                    crate::bail_parse_error!("unsafe use of virtual table \"{}\"", source.name);
                }
            }
        }
        Ok(())
    }

    /// Resolve the table an INSERT, UPDATE or DELETE inside a trigger writes.
    /// `database` is the trigger's own database. TEMP triggers pass `None`
    /// and use the normal temp-then-main lookup, like SQLite.
    fn analyze_trigger_target(
        &mut self,
        table: &ast::Name,
        database: Option<hir::DatabaseId>,
    ) -> Result<hir::SourceId> {
        let source = match database {
            Some(database) => {
                self.analyze_base_table_source_in_database(table, database, SourceOwner::Root)?
            }
            None => self.analyze_base_table_source(
                &ast::QualifiedName::single(table.clone()),
                None,
                None,
                SourceOwner::Root,
            )?,
        };
        // Like SQLite, a trigger never writes a virtual table.
        let target = self.source(source).ok_or_else(|| {
            LimboError::InternalError(format!("trigger target source {source} is missing"))
        })?;
        if let hir::SourceKind::Table(table) = &target.kind {
            if table.value().virtual_table().is_some() {
                crate::bail_parse_error!("unsafe use of virtual table \"{}\"", target.name);
            }
        }
        Ok(source)
    }

    fn analyze_trigger_insert_in(
        &mut self,
        database: Option<hir::DatabaseId>,
        environment: &hir::TriggerEnvironment,
        scope: &Scope,
        insert: TriggerInsert<'ast>,
    ) -> Result<hir::TriggerCommand> {
        let policies = ExprPolicies::trigger(self.context().dqs_dml(), environment);
        let target = self.analyze_trigger_target(insert.table, database)?;
        let root = self.analyze_insert_target(
            None,
            insert.conflict_override.or(insert.command_conflict),
            insert.columns,
            InsertBodySyntax::Select {
                select: insert.select,
                upsert: insert.upsert,
            },
            insert.returning,
            target,
            InsertExprContext {
                outer_scope: scope,
                policies,
            },
        )?;
        let hir::HirRoot::Insert(insert) = root else {
            unreachable!("INSERT analyzer returns an INSERT root");
        };
        Ok(hir::TriggerCommand::Insert(insert))
    }

    fn analyze_trigger_update_in(
        &mut self,
        database: Option<hir::DatabaseId>,
        environment: &hir::TriggerEnvironment,
        scope: &Scope,
        update: TriggerUpdate<'ast>,
    ) -> Result<hir::TriggerCommand> {
        let policies = ExprPolicies::trigger(self.context().dqs_dml(), environment);
        let target = self.analyze_trigger_target(update.table, database)?;
        let root = self.analyze_update_target(
            target,
            UpdateBodySyntax {
                conflict: update.conflict_override.or(update.command_conflict),
                assignments: update.assignments,
                from: update.from,
                predicate: update.predicate,
                returning: &[],
            },
            UpdateExprContext {
                outer_scope: Some(scope),
                policies,
            },
        )?;
        let hir::HirRoot::Update(update) = root else {
            unreachable!("UPDATE analyzer returns an UPDATE root");
        };
        Ok(hir::TriggerCommand::Update(update))
    }

    fn analyze_trigger_delete_in(
        &mut self,
        database: Option<hir::DatabaseId>,
        environment: &hir::TriggerEnvironment,
        scope: &Scope,
        delete: TriggerDelete<'ast>,
    ) -> Result<hir::TriggerCommand> {
        let policies = ExprPolicies::trigger(self.context().dqs_dml(), environment);
        let target = self.analyze_trigger_target(delete.table, database)?;
        let root = self.analyze_delete_target(
            target,
            delete.predicate,
            &[],
            DeleteExprContext {
                outer_scope: Some(scope),
                policies,
            },
        )?;
        let hir::HirRoot::Delete(delete) = root else {
            unreachable!("DELETE analyzer returns a DELETE root");
        };
        Ok(hir::TriggerCommand::Delete(delete))
    }

    fn analyze_trigger_command_in(
        &mut self,
        database: Option<hir::DatabaseId>,
        environment: &hir::TriggerEnvironment,
        scope: &Scope,
        command: &'ast ast::TriggerCmd,
        conflict_override: Option<ast::ResolveType>,
    ) -> Result<hir::TriggerCommand> {
        match command {
            ast::TriggerCmd::Select(select) => {
                self.analyze_trigger_select_in(environment, scope, select)
            }
            ast::TriggerCmd::Insert {
                or_conflict,
                tbl_name,
                col_names,
                select,
                upsert,
                returning,
            } => self.analyze_trigger_insert_in(
                database,
                environment,
                scope,
                TriggerInsert {
                    command_conflict: *or_conflict,
                    conflict_override,
                    table: tbl_name,
                    columns: col_names,
                    select,
                    upsert: upsert.as_deref(),
                    returning,
                },
            ),
            ast::TriggerCmd::Update {
                or_conflict,
                tbl_name,
                sets,
                from,
                where_clause,
            } => self.analyze_trigger_update_in(
                database,
                environment,
                scope,
                TriggerUpdate {
                    command_conflict: *or_conflict,
                    conflict_override,
                    table: tbl_name,
                    assignments: sets,
                    from: from.as_ref(),
                    predicate: where_clause.as_deref(),
                },
            ),
            ast::TriggerCmd::Delete {
                tbl_name,
                where_clause,
            } => self.analyze_trigger_delete_in(
                database,
                environment,
                scope,
                TriggerDelete {
                    table: tbl_name,
                    predicate: where_clause.as_deref(),
                },
            ),
        }
    }

    fn create_trigger_environment(
        &mut self,
        context: TriggerAnalysis,
    ) -> Result<(hir::TriggerEnvironment, Scope)> {
        let table_name = normalize_ident(context.table.get_name());
        let table_id =
            self.catalog_object_id(Some(context.database), CatalogObjectKind::Table, table_name);
        let table = CatalogObject::new(
            table_id,
            self.context().snapshot(),
            Some(context.database),
            context.table,
        );
        // A BEFORE INSERT trigger reads only NEW, which it gets as SQL values,
        // so nothing decodes.
        let decodes =
            !context.new_is_decoded || !matches!(context.event, ast::TriggerEvent::Insert);
        let analyzed_columns = self.source_columns_with(&table, decodes)?;
        let columns = analyzed_columns
            .iter()
            .map(|column| column.column.clone())
            .collect::<Vec<_>>();
        let column_type_programs = analyzed_columns
            .into_iter()
            .map(|column| column.programs)
            .collect::<Vec<_>>();
        let rowid_available = table_has_rowid(table.value());

        // A BEFORE trigger gets NEW before it is encoded, so reading it does
        // not decode.
        let new_type_programs = if context.new_is_decoded {
            vec![None; column_type_programs.len()]
        } else {
            column_type_programs.clone()
        };
        let mut create_row = |row| {
            let programs = match row {
                TriggerRow::New => &new_type_programs,
                TriggerRow::Old => &column_type_programs,
            };
            self.create_trigger_row_source(row, &table, &columns, programs, rowid_available)
        };
        let (new_source, old_source) = match &context.event {
            ast::TriggerEvent::Insert => (Some(create_row(TriggerRow::New)?), None),
            ast::TriggerEvent::Update | ast::TriggerEvent::UpdateOf(_) => (
                Some(create_row(TriggerRow::New)?),
                Some(create_row(TriggerRow::Old)?),
            ),
            ast::TriggerEvent::Delete => (None, Some(create_row(TriggerRow::Old)?)),
        };
        let environment = hir::TriggerEnvironment {
            table,
            new_source,
            old_source,
        };

        let mut scope = Scope::default();
        for source in [environment.new_source, environment.old_source]
            .into_iter()
            .flatten()
        {
            scope.add_source(
                self.source(source).ok_or_else(|| {
                    LimboError::InternalError(format!("missing trigger row source {source}"))
                })?,
                false,
            );
        }
        Ok((environment, scope))
    }

    fn create_trigger_row_source(
        &mut self,
        row: TriggerRow,
        table: &hir::ResolvedTable,
        columns: &[hir::SourceColumn],
        column_type_programs: &[Option<hir::BoundColumnTypePrograms>],
        rowid_available: bool,
    ) -> Result<hir::SourceId> {
        let source = self.reserve_source();
        let name = row.name();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner: SourceOwner::Root,
                database: table.database(),
                name: name.to_string(),
                alias: None,
                kind: hir::SourceKind::Pseudo {
                    kind: row.pseudo_source(),
                    table: table.clone(),
                },
                // SQLite gives OLD.x and NEW.x no comparison affinity in
                // trigger programs, so `OLD.a = 10` on a TEXT column holding
                // '10' is false. A rowid alias reads the rowid and keeps
                // INTEGER affinity. Collation still comes from the column.
                columns: columns
                    .iter()
                    .cloned()
                    .map(|mut column| {
                        column.has_affinity &= column.rowid_alias;
                        column
                    })
                    .collect(),
                generated_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                default_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                column_type_programs: column_type_programs.to_vec(),
                check_constraints: None,
                rowid_available,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }
}
