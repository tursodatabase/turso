//! Independent structural validation for completed HIR documents.

use std::{cell::Cell, collections::HashSet, fmt, sync::Arc};

use super::*;

type ValidationResult<T = ()> = std::result::Result<T, HirValidationError>;

fn expression_width(expression: &Expr) -> usize {
    match expression {
        Expr::Row(values) => values.len(),
        _ => 1,
    }
}

/// A broken document-local identity, owner, shape, or reachability invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HirValidationError {
    message: String,
}

impl HirValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for HirValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for HirValidationError {}

impl HirDocument {
    /// Check that this document is closed and internally self-consistent.
    pub fn validate(&self) -> ValidationResult {
        HirValidator::new(self).validate()
    }
}

struct HirValidator<'document> {
    document: &'document HirDocument,
    visited_queries: Vec<Cell<bool>>,
    visited_sources: Vec<Cell<bool>>,
    visited_ctes: Vec<Cell<bool>>,
    /// 0 is unseen, 1 is on the current validation stack, and 2 is complete.
    schema_program_states: Vec<Cell<u8>>,
}

impl<'document> HirValidator<'document> {
    fn new(document: &'document HirDocument) -> Self {
        Self {
            document,
            visited_queries: (0..document.queries.len())
                .map(|_| Cell::new(false))
                .collect(),
            visited_sources: (0..document.sources.len())
                .map(|_| Cell::new(false))
                .collect(),
            visited_ctes: (0..document.ctes.len()).map(|_| Cell::new(false)).collect(),
            schema_program_states: (0..document.schema_programs.len())
                .map(|_| Cell::new(0))
                .collect(),
        }
    }

    fn validate(&self) -> ValidationResult {
        self.validate_database_snapshots()?;
        self.validate_arena_identities()?;
        self.visit_root(&self.document.root)?;
        self.visit_cdc()?;
        self.validate_reachability()
    }

    fn visit_cdc(&self) -> ValidationResult {
        let Some(cdc) = &self.document.cdc else {
            return Ok(());
        };
        self.require(
            matches!(
                self.document.root,
                HirRoot::Insert(_) | HirRoot::Update(_) | HirRoot::Delete(_)
            ),
            "CDC metadata is only valid for a DML root",
        )?;
        self.visit_catalog_object(&cdc.table, "CDC table")?;
        self.require(
            cdc.table.database().map(DatabaseId::index) == Some(crate::MAIN_DB_ID),
            "CDC table must belong to the main database",
        )?;
        self.require(
            cdc.table.value().btree().is_some(),
            "CDC target is not a B-tree table",
        )?;
        self.require(
            cdc.table
                .value()
                .get_name()
                .eq_ignore_ascii_case(&cdc.info.table),
            "CDC table identity does not match the configured table name",
        )?;
        if let Some(sequence) = &cdc.sequence {
            self.visit_sequence_operation(sequence)?;
            self.require(
                sequence.database.index() == crate::MAIN_DB_ID,
                "CDC sequence must belong to the main database",
            )?;
        }
        Ok(())
    }

    fn validate_database_snapshots(&self) -> ValidationResult {
        let mut previous = None;
        for snapshot in &self.document.databases {
            if let Some(previous) = previous {
                self.require(
                    previous < snapshot.database.index(),
                    "database snapshots must be unique and sorted by database id",
                )?;
            }
            previous = Some(snapshot.database.index());
        }
        Ok(())
    }

    fn validate_arena_identities(&self) -> ValidationResult {
        for (index, query) in self.document.queries.iter().enumerate() {
            let expected = QueryId::new(index);
            self.require(
                query.id == expected,
                format!("query arena slot {index} contains {}", query.id),
            )?;
            if let Some(parent) = query.parent {
                self.require(
                    parent != query.id,
                    format!("query {} is its own lexical parent", query.id),
                )?;
                self.query(parent)?;
                self.require(
                    self.query_parent_chain_is_acyclic(query.id)?,
                    format!("query {} has a cyclic lexical parent chain", query.id),
                )?;
            }
            for (block_index, block) in query.blocks.iter().enumerate() {
                let expected = QueryBlockId::new(expected, block_index);
                self.require(
                    block.id == expected,
                    format!(
                        "query block arena slot {expected:?} contains {:?}",
                        block.id
                    ),
                )?;
            }
        }
        for (index, source) in self.document.sources.iter().enumerate() {
            let expected = SourceId::new(index);
            self.require(
                source.id == expected,
                format!("source arena slot {index} contains {}", source.id),
            )?;
        }
        for (index, cte) in self.document.ctes.iter().enumerate() {
            let expected = CteId::new(index);
            self.require(
                cte.id == expected,
                format!("CTE arena slot {index} contains {}", cte.id),
            )?;
        }
        Ok(())
    }

    fn validate_reachability(&self) -> ValidationResult {
        for (index, visited) in self.visited_queries.iter().enumerate() {
            self.require(visited.get(), format!("query q{index} is unreachable"))?;
        }
        for (index, visited) in self.visited_sources.iter().enumerate() {
            self.require(visited.get(), format!("source s{index} is unreachable"))?;
        }
        for (index, visited) in self.visited_ctes.iter().enumerate() {
            self.require(visited.get(), format!("CTE c{index} is unreachable"))?;
        }
        for (index, state) in self.schema_program_states.iter().enumerate() {
            self.require(
                state.get() == 2,
                format!("schema program schema_program{index} is unreachable or unfinished"),
            )?;
        }
        Ok(())
    }

    fn visit_root(&self, root: &HirRoot) -> ValidationResult {
        match root {
            HirRoot::Query(root) => {
                self.visit_query(root.query)?;
                self.visit_optional_trigger_environment(root.trigger.as_ref())
            }
            HirRoot::Insert(insert) => self.visit_insert(insert),
            HirRoot::Update(update) => self.visit_update(update),
            HirRoot::Delete(delete) => self.visit_delete(delete),
            HirRoot::TriggerPredicate(predicate) => {
                self.visit_trigger_environment(&predicate.environment)?;
                self.visit_expr(&predicate.expression)
            }
            HirRoot::SchemaExpressions(root) => {
                self.visit_source(root.source, Some(SourceOwner::Root))?;
                for expression in &root.expressions {
                    self.visit_expr(expression)?;
                }
                Ok(())
            }
        }
    }

    fn visit_insert(&self, insert: &Insert) -> ValidationResult {
        self.visit_source(insert.target, Some(SourceOwner::Root))?;
        self.require_complete_row_image(insert.target)?;
        self.require_complete_index_metadata(insert.target)?;
        self.visit_dml_triggers(
            insert.target,
            &insert.triggers,
            turso_parser::ast::TriggerEvent::Insert,
            &[],
        )?;
        let upsert_assignments = insert
            .upserts
            .iter()
            .filter_map(|upsert| match &upsert.action {
                UpsertAction::Update { assignments, .. } => Some(assignments.as_slice()),
                UpsertAction::Nothing => None,
            })
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        self.visit_dml_triggers(
            insert.target,
            &insert.upsert_triggers,
            turso_parser::ast::TriggerEvent::Update,
            &upsert_assignments,
        )?;
        self.visit_dml_foreign_keys(insert.target, &insert.foreign_keys)?;
        self.visit_optional_catalog_object(
            insert.autoincrement.as_ref(),
            "AUTOINCREMENT sequence table",
        )?;
        if let Some(sequence) = &insert.autoincrement_sequence {
            self.visit_sequence_operation(sequence)?;
            self.require(
                insert.autoincrement.is_some(),
                "MVCC AUTOINCREMENT sequence has no sqlite_sequence table",
            )?;
        }
        for target in &insert.columns {
            self.validate_target_column(insert.target, target.column)?;
        }
        for default in &insert.defaults {
            self.validate_column_position(insert.target, default.column)?;
            self.visit_expr(&default.value)?;
        }
        match &insert.source {
            InsertSource::DefaultValues => {}
            InsertSource::Values(rows) => {
                for row in rows {
                    self.require(
                        row.len() == insert.columns.len(),
                        format!(
                            "INSERT row has {} values for {} target columns",
                            row.len(),
                            insert.columns.len()
                        ),
                    )?;
                    self.visit_exprs(row)?;
                }
            }
            InsertSource::Query(query) => {
                self.visit_query(*query)?;
                let width = self.query(*query)?.output.len();
                self.require(
                    width == insert.columns.len(),
                    format!(
                        "INSERT query has {width} outputs for {} target columns",
                        insert.columns.len()
                    ),
                )?;
            }
        }
        for upsert in &insert.upserts {
            if let Some(target) = &upsert.target {
                for term in &target.terms {
                    self.visit_expr(&term.expr)?;
                    self.visit_optional_catalog_object(
                        term.collation.as_ref(),
                        "conflict-target collation",
                    )?;
                }
                self.visit_optional_expr(target.predicate.as_ref())?;
                self.visit_optional_catalog_object(
                    target.matched_index.as_ref(),
                    "conflict-target index",
                )?;
            }
            if let UpsertAction::Update {
                assignments,
                predicate,
            } = &upsert.action
            {
                self.visit_assignments(insert.target, assignments)?;
                self.visit_optional_expr(predicate.as_ref())?;
            }
        }
        if let Some(excluded) = insert.excluded_source {
            self.visit_source(excluded, Some(SourceOwner::Root))?;
            self.require_pseudo_source(excluded, PseudoSource::Excluded)?;
        }
        self.visit_optional_returning(insert.returning.as_ref())?;
        self.visit_optional_trigger_environment(insert.trigger.as_ref())
    }

    fn visit_update(&self, update: &Update) -> ValidationResult {
        self.visit_source(update.target, Some(SourceOwner::Root))?;
        self.visit_source(update.new_source, Some(SourceOwner::Root))?;
        self.require(
            update.target != update.new_source,
            "UPDATE OLD and NEW rows must have distinct source identities",
        )?;
        self.require_complete_row_image(update.target)?;
        self.require_complete_row_image(update.new_source)?;
        self.require_complete_index_metadata(update.target)?;
        self.require_complete_index_metadata(update.new_source)?;
        self.visit_dml_triggers(
            update.target,
            &update.triggers,
            turso_parser::ast::TriggerEvent::Update,
            &update.assignments,
        )?;
        self.visit_dml_foreign_keys(update.target, &update.foreign_keys)?;
        for default in &update.defaults {
            self.validate_column_position(update.new_source, default.column)?;
            self.visit_expr(&default.value)?;
        }
        if let Some(from) = &update.from {
            self.visit_from(from, SourceOwner::Root)?;
        }
        self.visit_assignments(update.target, &update.assignments)?;
        self.visit_optional_expr(update.predicate.as_ref())?;
        self.visit_order_terms(&update.order_by)?;
        self.visit_optional_limit(update.limit.as_ref())?;
        self.visit_optional_returning(update.returning.as_ref())?;
        self.visit_optional_trigger_environment(update.trigger.as_ref())
    }

    fn visit_delete(&self, delete: &Delete) -> ValidationResult {
        self.visit_source(delete.target, Some(SourceOwner::Root))?;
        self.require_complete_row_image(delete.target)?;
        self.require_complete_index_metadata(delete.target)?;
        self.visit_dml_triggers(
            delete.target,
            &delete.triggers,
            turso_parser::ast::TriggerEvent::Delete,
            &[],
        )?;
        self.visit_dml_foreign_keys(delete.target, &delete.foreign_keys)?;
        self.visit_optional_expr(delete.predicate.as_ref())?;
        self.visit_order_terms(&delete.order_by)?;
        self.visit_optional_limit(delete.limit.as_ref())?;
        self.visit_optional_returning(delete.returning.as_ref())?;
        self.visit_optional_trigger_environment(delete.trigger.as_ref())
    }

    fn visit_assignments(&self, target: SourceId, assignments: &[Assignment]) -> ValidationResult {
        for assignment in assignments {
            for column in &assignment.columns {
                self.validate_target_column(target, *column)?;
            }
            self.visit_expr(&assignment.value)?;
        }
        Ok(())
    }

    fn validate_target_column(&self, target: SourceId, column: TargetColumn) -> ValidationResult {
        match column {
            TargetColumn::Column(position) => self.validate_column_position(target, position),
            TargetColumn::RowId => {
                let source = self.source(target)?;
                self.require(
                    source.rowid_available,
                    format!("source {target} has no rowid target"),
                )
            }
        }
    }

    fn visit_optional_returning(&self, returning: Option<&Returning>) -> ValidationResult {
        let Some(returning) = returning else {
            return Ok(());
        };
        for (index, output) in returning.outputs.iter().enumerate() {
            self.visit_output(output, OutputId::root(index))?;
        }
        Ok(())
    }

    fn visit_optional_trigger_environment(
        &self,
        environment: Option<&TriggerEnvironment>,
    ) -> ValidationResult {
        environment.map_or(Ok(()), |environment| {
            self.visit_trigger_environment(environment)
        })
    }

    fn visit_trigger_environment(&self, environment: &TriggerEnvironment) -> ValidationResult {
        self.visit_catalog_object(&environment.table, "trigger table")?;
        if let Some(source) = environment.new_source {
            self.visit_source(source, Some(SourceOwner::Root))?;
            self.require_pseudo_source(source, PseudoSource::New)?;
            self.require_pseudo_table(source, &environment.table)?;
        }
        if let Some(source) = environment.old_source {
            self.visit_source(source, Some(SourceOwner::Root))?;
            self.require_pseudo_source(source, PseudoSource::Old)?;
            self.require_pseudo_table(source, &environment.table)?;
        }
        Ok(())
    }

    fn visit_dml_triggers(
        &self,
        target: SourceId,
        triggers: &[ResolvedTrigger],
        event: turso_parser::ast::TriggerEvent,
        assignments: &[Assignment],
    ) -> ValidationResult {
        let target_source = self.source(target)?;
        let target_table = match &target_source.kind {
            SourceKind::Table(table) => table,
            _ if triggers.is_empty() => return Ok(()),
            _ => {
                return self.invalid(format!(
                    "non-table DML source {target} carries trigger metadata"
                ));
            }
        };
        let updated_columns = assignments
            .iter()
            .flat_map(|assignment| assignment.columns.iter())
            .filter_map(|column| match column {
                TargetColumn::Column(position) => Some(*position),
                TargetColumn::RowId => None,
            })
            .collect::<HashSet<_>>();
        let mut identities = HashSet::with_capacity(triggers.len());
        for trigger in triggers {
            self.visit_catalog_object(trigger, "DML trigger")?;
            self.require(
                identities.insert((trigger.database(), trigger.id())),
                format!("DML source {target} carries a duplicate trigger"),
            )?;
            self.require(
                trigger
                    .value()
                    .table_name
                    .eq_ignore_ascii_case(target_table.value().get_name()),
                format!("DML trigger {} targets another table", trigger.value().name),
            )?;
            if let Some(database) = trigger.value().target_database_id {
                self.require(
                    target_table.database().map(DatabaseId::index) == Some(database),
                    format!(
                        "DML trigger {} targets another database",
                        trigger.value().name
                    ),
                )?;
            }
            let event_matches = match (&trigger.value().event, &event) {
                (
                    turso_parser::ast::TriggerEvent::Insert,
                    turso_parser::ast::TriggerEvent::Insert,
                )
                | (
                    turso_parser::ast::TriggerEvent::Delete,
                    turso_parser::ast::TriggerEvent::Delete,
                )
                | (
                    turso_parser::ast::TriggerEvent::Update,
                    turso_parser::ast::TriggerEvent::Update,
                ) => true,
                (
                    turso_parser::ast::TriggerEvent::UpdateOf(columns),
                    turso_parser::ast::TriggerEvent::Update,
                ) => columns.iter().any(|column| {
                    target_table
                        .value()
                        .get_column_by_name(&crate::util::normalize_ident(column.as_str()))
                        .is_some_and(|(position, _)| updated_columns.contains(&position))
                }),
                _ => false,
            };
            self.require(
                event_matches,
                format!(
                    "DML trigger {} does not match the write event",
                    trigger.value().name
                ),
            )?;
        }
        Ok(())
    }

    fn visit_dml_foreign_keys(
        &self,
        target: SourceId,
        foreign_keys: &DmlForeignKeys,
    ) -> ValidationResult {
        let target_source = self.source(target)?;
        let SourceKind::Table(target_table) = &target_source.kind else {
            return self.require(
                foreign_keys.outgoing.is_empty() && foreign_keys.incoming.is_empty(),
                format!("non-table DML source {target} carries foreign-key metadata"),
            );
        };

        let mut outgoing_identities = HashSet::with_capacity(foreign_keys.outgoing.len());
        for foreign_key in &foreign_keys.outgoing {
            self.require(
                &foreign_key.child_table == target_table,
                format!("outgoing foreign key belongs to another DML target than {target}"),
            )?;
            self.visit_resolved_foreign_key(foreign_key, true)?;
            self.require(
                outgoing_identities.insert((
                    foreign_key.child_table.database(),
                    foreign_key.child_table.id(),
                    foreign_key.declaration.decl_order,
                )),
                format!("DML source {target} carries a duplicate foreign key"),
            )?;
        }
        let mut incoming_identities = HashSet::with_capacity(foreign_keys.incoming.len());
        for foreign_key in &foreign_keys.incoming {
            self.require(
                &foreign_key.parent_table == target_table,
                format!("incoming foreign key belongs to another DML target than {target}"),
            )?;
            self.visit_resolved_foreign_key(foreign_key, false)?;
            self.require(
                incoming_identities.insert((
                    foreign_key.child_table.database(),
                    foreign_key.child_table.id(),
                    foreign_key.declaration.decl_order,
                )),
                format!("DML source {target} carries a duplicate foreign key"),
            )?;
        }
        Ok(())
    }

    fn visit_resolved_foreign_key(
        &self,
        foreign_key: &ResolvedForeignKey,
        require_parent_index: bool,
    ) -> ValidationResult {
        self.visit_catalog_object(&foreign_key.child_table, "foreign-key child table")?;
        self.visit_catalog_object(&foreign_key.parent_table, "foreign-key parent table")?;
        self.visit_source(
            foreign_key.child_source,
            Some(crate::translate::semantic::hir::SourceOwner::Root),
        )?;
        let child_source = self.source(foreign_key.child_source)?;
        self.require(
            matches!(&child_source.kind, SourceKind::Table(table) if table == &foreign_key.child_table),
            "foreign-key child scan source belongs to another table",
        )?;
        self.require(
            foreign_key.child_table.database() == foreign_key.parent_table.database(),
            "foreign-key child and parent tables belong to different databases",
        )?;

        let Some(child_table) = foreign_key.child_table.value().btree() else {
            return self.invalid("foreign-key child is not a B-tree table");
        };
        let Some(parent_table) = foreign_key.parent_table.value().btree() else {
            return self.invalid("foreign-key parent is not a B-tree table");
        };
        self.require(
            child_table
                .foreign_keys
                .iter()
                .any(|declaration| Arc::ptr_eq(declaration, &foreign_key.declaration)),
            format!(
                "foreign key is not declared by child table {}",
                child_table.name
            ),
        )?;
        self.require(
            foreign_key
                .declaration
                .parent_table
                .eq_ignore_ascii_case(&parent_table.name),
            format!(
                "foreign key declared for parent {} was resolved to {}",
                foreign_key.declaration.parent_table, parent_table.name
            ),
        )?;

        let width = foreign_key.declaration.child_columns.len();
        self.require(width > 0, "foreign key has no child columns")?;
        self.require(
            foreign_key.parent_columns.len() == width
                && foreign_key.child_positions.len() == width
                && foreign_key.parent_positions.len() == width,
            "foreign-key names and positions have different widths",
        )?;
        if !foreign_key.declaration.parent_columns.is_empty() {
            self.require(
                foreign_key
                    .declaration
                    .parent_columns
                    .iter()
                    .zip(foreign_key.parent_columns.iter())
                    .all(|(declared, resolved)| declared.eq_ignore_ascii_case(resolved)),
                "foreign-key resolved parent columns differ from its declaration",
            )?;
        }

        for ((child_name, child_position), (parent_name, parent_position)) in foreign_key
            .declaration
            .child_columns
            .iter()
            .zip(foreign_key.child_positions.iter().copied())
            .zip(
                foreign_key
                    .parent_columns
                    .iter()
                    .zip(foreign_key.parent_positions.iter().copied()),
            )
        {
            let child_column = child_table.columns().get(child_position).ok_or_else(|| {
                HirValidationError::new(format!(
                    "foreign-key child column {child_position} is out of range"
                ))
            })?;
            self.require(
                child_column
                    .name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(child_name)),
                format!("foreign-key child position {child_position} does not name {child_name}"),
            )?;
            let parent_column = parent_table.columns().get(parent_position).ok_or_else(|| {
                HirValidationError::new(format!(
                    "foreign-key parent column {parent_position} is out of range"
                ))
            })?;
            self.require(
                parent_column
                    .name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(parent_name)),
                format!(
                    "foreign-key parent position {parent_position} does not name {parent_name}"
                ),
            )?;
        }

        let parent_uses_rowid = foreign_key.parent_columns.len() == 1
            && parent_table
                .columns()
                .get(foreign_key.parent_positions[0])
                .is_some_and(crate::schema::Column::is_rowid_alias);
        self.require(
            foreign_key.parent_uses_rowid == parent_uses_rowid,
            "foreign-key rowid lookup fact does not match the parent column",
        )?;
        self.require(
            !foreign_key.parent_action_guarantees_new_parent
                || foreign_key.declaration.on_update == turso_parser::ast::RefAct::Cascade,
            "only an UPDATE CASCADE action can guarantee the new parent",
        )?;

        match &foreign_key.parent_unique_index {
            Some(index) => {
                self.visit_catalog_object(index, "foreign-key parent index")?;
                self.require(
                    index.database() == foreign_key.parent_table.database(),
                    "foreign-key parent index belongs to another database",
                )?;
                self.require(
                    !foreign_key.parent_uses_rowid,
                    "rowid foreign key carries a parent index",
                )?;
                self.require(
                    index
                        .value()
                        .table_name
                        .eq_ignore_ascii_case(&parent_table.name)
                        && index.value().unique
                        && index.value().where_clause.is_none(),
                    "foreign-key parent index is not a full UNIQUE index on the parent table",
                )?;
                self.require(
                    index.value().columns.len() == foreign_key.parent_columns.len()
                        && index
                            .value()
                            .columns
                            .iter()
                            .zip(
                                foreign_key
                                    .parent_columns
                                    .iter()
                                    .zip(foreign_key.parent_positions.iter()),
                            )
                            .all(|(index_column, (name, position))| {
                                index_column.name.eq_ignore_ascii_case(name)
                                    && index_column.pos_in_table == *position
                            }),
                    "foreign-key parent index does not match the resolved parent key",
                )?;
            }
            None => self.require(
                foreign_key.parent_uses_rowid || !require_parent_index,
                "outgoing non-rowid foreign key has no parent UNIQUE index",
            )?,
        }
        Ok(())
    }

    fn require_pseudo_source(&self, source: SourceId, expected: PseudoSource) -> ValidationResult {
        let definition = self.source(source)?;
        self.require(
            matches!(definition.kind, SourceKind::Pseudo { kind, .. } if kind == expected),
            format!("source {source} is not the expected {expected:?} pseudo-source"),
        )
    }

    fn require_pseudo_table(&self, source: SourceId, expected: &ResolvedTable) -> ValidationResult {
        let definition = self.source(source)?;
        let SourceKind::Pseudo { table, .. } = &definition.kind else {
            return self.invalid(format!("source {source} is not a pseudo-source"));
        };
        self.require(
            table == expected,
            format!("pseudo-source {source} belongs to a different table"),
        )
    }

    fn visit_query(&self, id: QueryId) -> ValidationResult {
        let query = self.query(id)?;
        if self.visited_queries[id.index()].replace(true) {
            return Ok(());
        }
        self.require(
            !query.blocks.is_empty(),
            format!("query {id} has no blocks"),
        )?;
        self.require(
            query.first == query.blocks[0].id,
            format!("query {id} first block is not block zero"),
        )?;
        self.require(
            query.compounds.len() + 1 == query.blocks.len(),
            format!(
                "query {id} has {} blocks but {} compound arms",
                query.blocks.len(),
                query.compounds.len()
            ),
        )?;
        let expected_captures = self.document.direct_query_captures(id);
        self.require(
            query.captures == expected_captures,
            format!("query {id} has an incorrect external source capture summary"),
        )?;
        for source in &query.captures {
            self.validate_capture(id, *source)?;
        }

        for (index, block) in query.blocks.iter().enumerate() {
            let expected = QueryBlockId::new(id, index);
            self.require(
                block.id == expected,
                format!("query {id} block {index} has identity {:?}", block.id),
            )?;
            self.visit_query_block(block)?;
        }
        for (index, arm) in query.compounds.iter().enumerate() {
            let expected = query.blocks[index + 1].id;
            self.require(
                arm.block == expected,
                format!("query {id} compound arm {index} points to {:?}", arm.block),
            )?;
        }

        let first = self.query_block(query.first)?;
        let expected_outputs = first
            .outputs
            .iter()
            .map(|output| output.id)
            .collect::<Vec<_>>();
        self.require(
            query.output == expected_outputs,
            format!("query {id} output list does not match its first block"),
        )?;
        for block in query.blocks.iter().skip(1) {
            self.require(
                block.outputs.len() == first.outputs.len(),
                format!(
                    "query {id} compound block {:?} has width {}, expected {}",
                    block.id,
                    block.outputs.len(),
                    first.outputs.len()
                ),
            )?;
        }

        let expected_ctes = self.direct_query_ctes(query)?;
        self.require(
            query.reachable_ctes == expected_ctes,
            format!("query {id} has an incorrect reachable CTE summary"),
        )?;
        for cte in &query.reachable_ctes {
            self.visit_cte(*cte)?;
        }
        self.visit_order_terms(&query.order_by)?;
        self.visit_optional_limit(query.limit.as_ref())
    }

    fn direct_query_ctes(&self, query: &Query) -> ValidationResult<Vec<CteId>> {
        let mut ctes = Vec::new();
        for block in &query.blocks {
            let Some(from) = &block.from else {
                continue;
            };
            self.add_source_cte(from.first, &mut ctes)?;
            for join in &from.joins {
                self.add_source_cte(join.right, &mut ctes)?;
            }
        }
        Ok(ctes)
    }

    fn add_source_cte(&self, source: SourceId, ctes: &mut Vec<CteId>) -> ValidationResult {
        let source = self.source(source)?;
        let cte = match source.kind {
            SourceKind::Cte(cte) | SourceKind::RecursiveInput(cte) => cte,
            _ => return Ok(()),
        };
        if !ctes.contains(&cte) {
            ctes.push(cte);
        }
        Ok(())
    }

    fn visit_query_block(&self, block: &QueryBlock) -> ValidationResult {
        if let Some(from) = &block.from {
            self.visit_from(from, SourceOwner::QueryBlock(block.id))?;
        }
        for (index, output) in block.outputs.iter().enumerate() {
            self.visit_output(output, OutputId::query(block.id, index))?;
        }
        match &block.body {
            QueryBlockBody::Select {
                filter,
                grouping,
                windows,
                ..
            } => {
                self.visit_optional_expr(filter.as_ref())?;
                if let Some(grouping) = grouping {
                    self.visit_exprs(&grouping.keys)?;
                    self.require(
                        grouping.key_type_facts.len() == grouping.keys.len(),
                        format!("GROUP BY key type facts do not match block {:?}", block.id),
                    )?;
                    self.require(
                        grouping.key_collations.len() == grouping.keys.len(),
                        format!("GROUP BY key collations do not match block {:?}", block.id),
                    )?;
                    for type_fact in &grouping.key_type_facts {
                        self.visit_type_fact(type_fact)?;
                    }
                    for collation in &grouping.key_collations {
                        self.visit_optional_catalog_object(
                            collation.as_ref(),
                            "GROUP BY key collation",
                        )?;
                    }
                    self.visit_optional_expr(grouping.having.as_ref())?;
                }
                for window in windows {
                    self.visit_window_spec(&window.spec)?;
                }
            }
            QueryBlockBody::Values { rows } => {
                for row in rows {
                    self.require(
                        row.len() == block.outputs.len(),
                        format!(
                            "VALUES row in {:?} has width {}, expected {}",
                            block.id,
                            row.len(),
                            block.outputs.len()
                        ),
                    )?;
                    self.visit_exprs(row)?;
                }
            }
        }
        Ok(())
    }

    fn visit_from(&self, from: &From, owner: SourceOwner) -> ValidationResult {
        self.visit_source(from.first, Some(owner))?;
        for join in &from.joins {
            self.visit_source(join.right, Some(owner))?;
            match &join.constraint {
                JoinConstraint::None => {}
                JoinConstraint::On(expression) => self.visit_expr(expression)?,
                JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
                    for column in columns {
                        self.require(
                            column.right.source == join.right,
                            format!(
                                "join source {} has USING right column from {}",
                                join.right, column.right.source
                            ),
                        )?;
                        self.visit_expr(&column.left)?;
                        self.visit_column_ref(column.right)?;
                        self.visit_type_fact(&column.type_fact)?;
                        self.visit_optional_catalog_object(
                            column.collation.as_ref(),
                            "join collation",
                        )?;
                        self.visit_comparison(
                            &column.comparison,
                            1,
                            "USING/NATURAL join comparison",
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn visit_source(&self, id: SourceId, expected_owner: Option<SourceOwner>) -> ValidationResult {
        let source = self.source(id)?;
        if let Some(expected_owner) = expected_owner {
            self.require(
                source.owner == expected_owner,
                format!(
                    "source {id} is owned by {:?}, expected {expected_owner:?}",
                    source.owner
                ),
            )?;
        }
        if self.visited_sources[id.index()].replace(true) {
            return Ok(());
        }
        self.validate_source_owner(source.owner)?;
        if let Some(database) = source.database {
            self.require_database(database)?;
        }
        let width = source.columns.len();
        self.require(
            source.generated_expressions.len() == width,
            format!("source {id} generated-expression width does not match its columns"),
        )?;
        self.require(
            source.default_expressions.len() == width,
            format!("source {id} default-expression width does not match its columns"),
        )?;
        self.require(
            source.column_type_programs.len() == width,
            format!("source {id} type-program width does not match its columns"),
        )?;

        match &source.kind {
            SourceKind::SchemaExpression => {}
            SourceKind::Table(table) => self.visit_source_table(source, table, true)?,
            SourceKind::Pseudo { table, .. } => self.visit_source_table(source, table, false)?,
            SourceKind::TableFunction { table, arguments } => {
                self.visit_source_table(source, table, false)?;
                self.visit_exprs(arguments)?;
            }
            SourceKind::Cte(cte) | SourceKind::RecursiveInput(cte) => {
                self.visit_cte(*cte)?;
                let cte = self.cte(*cte)?;
                self.require(
                    source.columns.len() == cte.columns.len(),
                    format!("source {id} width does not match CTE {}", cte.id),
                )?;
            }
            SourceKind::Derived(query) => {
                self.visit_query(*query)?;
                let query = self.query(*query)?;
                self.require(
                    source.columns.len() == query.output.len(),
                    format!(
                        "source {id} width does not match derived query {}",
                        query.id
                    ),
                )?;
            }
        }

        for column in &source.columns {
            self.visit_type_fact(&column.type_fact)?;
            self.visit_optional_catalog_object(column.collation.as_ref(), "column collation")?;
        }
        match &source.index_hint {
            IndexHint::None | IndexHint::NotIndexed => {}
            IndexHint::Indexed(index) => self.visit_catalog_object(index, "index hint")?,
        }

        self.visit_column_read_expressions(&source.generated_expressions)?;
        self.visit_column_read_expressions(&source.default_expressions)?;
        for programs in source.column_type_programs.iter().flatten() {
            self.visit_column_type_programs(programs)?;
        }
        if let Some(constraints) = &source.check_constraints {
            for constraint in constraints {
                self.visit_expr(&constraint.expression)?;
            }
        }
        let source_table = match &source.kind {
            SourceKind::Table(table) | SourceKind::TableFunction { table, .. } => Some(table),
            _ => None,
        };
        let mut index_ids = HashSet::with_capacity(source.index_expressions.len());
        let mut resolved_index_ids = Vec::with_capacity(source.index_expressions.len());
        for expressions in &source.index_expressions {
            self.visit_catalog_object(&expressions.index, "index expression")?;
            self.require(
                index_ids.insert((expressions.index.database(), expressions.index.id())),
                format!("source {id} contains duplicate index metadata"),
            )?;
            resolved_index_ids.push(expressions.index.id());
            let table = source_table.ok_or_else(|| {
                HirValidationError::new(format!("non-table source {id} contains index metadata"))
            })?;
            self.require(
                expressions.index.database() == table.database()
                    && expressions
                        .index
                        .value()
                        .table_name
                        .eq_ignore_ascii_case(table.value().get_name()),
                format!("source {id} contains metadata for another table's index"),
            )?;
            self.require(
                expressions.columns.len() == expressions.index.value().columns.len(),
                format!("source {id} index-expression width does not match its index"),
            )?;
            for (position, (expression, column)) in expressions
                .columns
                .iter()
                .zip(&expressions.index.value().columns)
                .enumerate()
            {
                self.require(
                    expression.is_some()
                        == (column.pos_in_table == crate::schema::EXPR_INDEX_SENTINEL),
                    format!(
                        "source {id} index column {position} has incomplete expression metadata"
                    ),
                )?;
            }
            self.require(
                expressions.predicate.is_some() == expressions.index.value().where_clause.is_some(),
                format!("source {id} has incomplete partial-index metadata"),
            )?;
            for expression in expressions.columns.iter().flatten() {
                self.visit_expr(expression)?;
            }
            self.visit_optional_expr(expressions.predicate.as_ref())?;
        }
        if let IndexCoverage::Complete { indexes } = &source.index_coverage {
            self.require(
                indexes == &resolved_index_ids,
                format!("source {id} has an incorrect complete-index summary"),
            )?;
        }
        let mut pattern_ids = HashSet::with_capacity(source.index_method_patterns.len());
        for pattern in &source.index_method_patterns {
            self.visit_catalog_object(&pattern.index, "index-method pattern")?;
            self.require(
                pattern.id.source == id,
                format!("source {id} contains index-method pattern {:?}", pattern.id),
            )?;
            self.require(
                pattern.id.index == pattern.index.id(),
                format!(
                    "index-method pattern {:?} names index {:?} but contains index {:?}",
                    pattern.id,
                    pattern.id.index,
                    pattern.index.id()
                ),
            )?;
            self.require(
                pattern_ids.insert(pattern.id),
                format!(
                    "source {id} contains duplicate index-method pattern {:?}",
                    pattern.id
                ),
            )?;
            for (output_index, output) in pattern.outputs.iter().enumerate() {
                self.visit_output(
                    output,
                    OutputId::index_method_pattern(pattern.id, output_index),
                )?;
            }
            self.visit_optional_expr(pattern.predicate.as_ref())?;
            self.visit_order_terms(&pattern.order_by)?;
            self.visit_optional_limit(pattern.limit.as_ref())?;
        }
        Ok(())
    }

    fn validate_source_owner(&self, owner: SourceOwner) -> ValidationResult {
        match owner {
            SourceOwner::Root => Ok(()),
            SourceOwner::QueryBlock(block) => {
                self.query_block(block)?;
                Ok(())
            }
            SourceOwner::Cte(cte) => {
                self.cte(cte)?;
                Ok(())
            }
        }
    }

    fn validate_capture(&self, query: QueryId, source: SourceId) -> ValidationResult {
        let source = self.source(source)?;
        match source.owner {
            SourceOwner::Root => Ok(()),
            SourceOwner::QueryBlock(block) => self.require(
                self.is_query_ancestor(block.query, query)?,
                format!(
                    "query {query} captures source {} from non-ancestor query {}",
                    source.id, block.query
                ),
            ),
            SourceOwner::Cte(cte) => self.invalid(format!(
                "query {query} captures source {} owned directly by CTE {cte}",
                source.id
            )),
        }
    }

    fn is_query_ancestor(&self, ancestor: QueryId, descendant: QueryId) -> ValidationResult<bool> {
        let mut current = self.query(descendant)?.parent;
        let mut remaining = self.document.queries.len();
        while let Some(query) = current {
            if query == ancestor {
                return Ok(true);
            }
            if remaining == 0 {
                return Ok(false);
            }
            remaining -= 1;
            current = self.query(query)?.parent;
        }
        Ok(false)
    }

    fn query_parent_chain_is_acyclic(&self, query: QueryId) -> ValidationResult<bool> {
        let mut seen = HashSet::with_capacity(self.document.queries.len());
        let mut current = Some(query);
        while let Some(query) = current {
            if !seen.insert(query) {
                return Ok(false);
            }
            current = self.query(query)?.parent;
        }
        Ok(true)
    }

    fn visit_column_read_expressions(
        &self,
        expressions: &[ColumnReadExpression],
    ) -> ValidationResult {
        for expression in expressions {
            if let ColumnReadExpression::Planned(expression) = expression {
                self.visit_expr(expression)?;
            }
        }
        Ok(())
    }

    fn visit_cte(&self, id: CteId) -> ValidationResult {
        let cte = self.cte(id)?;
        if self.visited_ctes[id.index()].replace(true) {
            return Ok(());
        }
        match &cte.body {
            CteBody::Query(query) => {
                self.visit_query(*query)?;
                self.require_query_width(*query, cte.columns.len(), "CTE")?;
            }
            CteBody::Recursive(recursive) => {
                self.visit_query(recursive.seed)?;
                self.require_query_width(recursive.seed, cte.columns.len(), "recursive seed")?;
                for arm in &recursive.arms {
                    self.visit_query(arm.query)?;
                    self.require_query_width(arm.query, cte.columns.len(), "recursive arm")?;
                }
                for source in &recursive.input_sources {
                    self.visit_source(*source, None)?;
                    let definition = self.source(*source)?;
                    self.require(
                        matches!(definition.kind, SourceKind::RecursiveInput(source_cte) if source_cte == id),
                        format!("recursive CTE {id} lists non-recursive source {source}"),
                    )?;
                }
                self.require(
                    recursive.comparison_collations.len() == cte.columns.len(),
                    format!("recursive CTE {id} comparison width does not match its columns"),
                )?;
                for collation in &recursive.comparison_collations {
                    self.visit_optional_catalog_object(
                        collation.as_ref(),
                        "recursive CTE comparison collation",
                    )?;
                }
                for term in &recursive.queue_order {
                    self.require(
                        term.output < cte.columns.len(),
                        format!(
                            "recursive CTE {id} queue output {} is out of range",
                            term.output
                        ),
                    )?;
                    self.visit_optional_catalog_object(
                        term.explicit_collation.as_ref(),
                        "recursive CTE order collation",
                    )?;
                }
                self.visit_optional_limit(recursive.limit.as_ref())?;
            }
        }
        for column in &cte.columns {
            self.visit_type_fact(&column.type_fact)?;
            self.visit_optional_catalog_object(column.collation.as_ref(), "CTE collation")?;
        }
        Ok(())
    }

    fn require_query_width(&self, query: QueryId, width: usize, context: &str) -> ValidationResult {
        let actual = self.query(query)?.output.len();
        self.require(
            actual == width,
            format!("{context} query {query} has width {actual}, expected {width}"),
        )
    }

    fn visit_output(&self, output: &Output, expected: OutputId) -> ValidationResult {
        self.require(
            output.id == expected,
            format!("output {expected:?} contains identity {:?}", output.id),
        )?;
        self.visit_type_fact(&output.type_fact)?;
        self.require(
            !output.collation_is_explicit || output.collation.is_some(),
            format!("output {expected:?} marks an absent collation as explicit"),
        )?;
        self.visit_optional_catalog_object(output.collation.as_ref(), "output collation")?;
        self.visit_expr(&output.expr)
    }

    fn visit_output_reference(&self, id: OutputId) -> ValidationResult {
        self.document
            .output(id)
            .ok_or_else(|| HirValidationError::new(format!("output {id:?} does not exist")))?;
        match id.owner {
            OutputOwner::QueryBlock(block) => self.visit_query(block.query),
            OutputOwner::IndexMethodPattern(pattern) => self.visit_source(pattern.source, None),
            OutputOwner::Root => Ok(()),
        }
    }

    fn visit_schema_program(&self, id: SchemaProgramId) -> ValidationResult {
        let Some(state) = self.schema_program_states.get(id.index()) else {
            return self.invalid(format!("schema program {id} does not exist"));
        };
        match state.get() {
            1 => return self.invalid(format!("schema program {id} is recursive")),
            2 => return Ok(()),
            _ => state.set(1),
        }
        let program = self.document.schema_program(id).ok_or_else(|| {
            HirValidationError::new(format!("schema program {id} does not exist"))
        })?;
        self.visit_source(program.input_source, Some(SourceOwner::Root))?;
        self.require(
            matches!(
                self.source(program.input_source)?.kind,
                SourceKind::SchemaExpression
            ),
            format!("schema program {id} input is not a schema-expression source"),
        )?;
        self.visit_expr(&program.body)?;
        state.set(2);
        Ok(())
    }

    fn visit_schema_call(&self, call: &BoundSchemaCall) -> ValidationResult {
        self.visit_exprs(&call.arguments)?;
        self.visit_schema_program(call.program)
    }

    fn visit_column_type_programs(&self, programs: &BoundColumnTypePrograms) -> ValidationResult {
        for call in programs.encode.iter().chain(&programs.decode) {
            self.visit_schema_call(call)?;
        }
        Ok(())
    }

    fn visit_cast_programs(&self, programs: &BoundCastPrograms) -> ValidationResult {
        for call in &programs.encode {
            self.visit_schema_call(call)?;
        }
        if let Some(domain) = &programs.domain {
            for check in &domain.checks {
                self.visit_schema_call(&check.call)?;
            }
        }
        Ok(())
    }

    fn visit_expr(&self, expression: &Expr) -> ValidationResult {
        match expression {
            Expr::Literal(_) | Expr::Parameter(_) => Ok(()),
            Expr::Column(reference) => self.visit_column_ref(*reference),
            Expr::MergedColumn(column) => {
                self.visit_expr(&column.left)?;
                self.visit_column_ref(column.right)?;
                self.visit_type_fact(&column.type_fact)?;
                self.visit_optional_catalog_object(
                    column.collation.as_ref(),
                    "merged-column collation",
                )
            }
            Expr::RowId(source) => {
                self.visit_source(*source, None)?;
                self.require(
                    self.source(*source)?.rowid_available,
                    format!("rowid expression refers to WITHOUT ROWID source {source}"),
                )
            }
            Expr::Output(output) => self.visit_output_reference(*output),
            Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::NotNull(expr) => {
                self.visit_expr(expr)
            }
            Expr::Binary {
                lhs,
                operator,
                rhs,
                array_concat,
                custom,
                comparison,
            } => {
                self.visit_expr(lhs)?;
                self.visit_expr(rhs)?;
                self.require(
                    operator.is_comparison() == comparison.is_some(),
                    "binary expression has incorrect comparison metadata",
                )?;
                self.require(
                    !*array_concat || *operator == turso_parser::ast::Operator::Concat,
                    "non-concatenation expression is marked as array concatenation",
                )?;
                if let Some(comparison) = comparison {
                    self.visit_expression_comparison(comparison, lhs, rhs, "binary comparison")?;
                }
                if let Some(encoding) = custom
                    .as_ref()
                    .and_then(|custom| custom.literal_encoding.as_ref())
                    .and_then(|encoding| encoding.encoder.as_ref())
                {
                    self.visit_schema_call(encoding)?;
                }
                if let Some(custom) = custom {
                    self.visit_catalog_object(&custom.function, "custom operator function")?;
                }
                Ok(())
            }
            Expr::Between {
                expr,
                start,
                end,
                start_comparison,
                end_comparison,
                ..
            } => {
                self.visit_expr(expr)?;
                self.visit_expr(start)?;
                self.visit_expr(end)?;
                self.visit_expression_comparison(
                    start_comparison,
                    expr,
                    start,
                    "BETWEEN lower-bound comparison",
                )?;
                self.visit_expression_comparison(
                    end_comparison,
                    expr,
                    end,
                    "BETWEEN upper-bound comparison",
                )
            }
            Expr::Case {
                base,
                when_then,
                else_expr,
                base_comparisons,
            } => {
                self.visit_optional_expr(base.as_deref())?;
                for (when, then) in when_then {
                    self.visit_expr(when)?;
                    self.visit_expr(then)?;
                }
                self.visit_optional_expr(else_expr.as_deref())?;
                match base.as_deref() {
                    None => self.require(
                        base_comparisons.is_empty(),
                        "searched CASE contains base-comparison metadata",
                    ),
                    Some(base) => {
                        self.require(
                            base_comparisons.len() == when_then.len(),
                            "simple CASE comparison count does not match its WHEN count",
                        )?;
                        for ((when, _), comparison) in when_then.iter().zip(base_comparisons) {
                            self.visit_expression_comparison(
                                comparison,
                                base,
                                when,
                                "simple CASE comparison",
                            )?;
                        }
                        Ok(())
                    }
                }
            }
            Expr::Cast { expr, target } => {
                self.visit_expr(expr)?;
                self.visit_exprs(&target.parameters)?;
                self.visit_type_fact(&target.type_fact)?;
                self.visit_cast_programs(&target.programs)
            }
            Expr::Collate { expr, collation } => {
                self.visit_catalog_object(collation, "COLLATE expression")?;
                self.visit_expr(expr)
            }
            Expr::Function(function) => {
                self.visit_catalog_object(&function.function, "function call")?;
                self.visit_type_fact(&function.result_type)?;
                self.visit_function_evaluation(function)?;
                if let Some(operation) = &function.custom_type_operation {
                    self.visit_custom_type_operation(operation)?;
                }
                if let Some(operation) = &function.sequence_operation {
                    self.visit_sequence_operation(operation)?;
                }
                self.visit_exprs(&function.arguments)?;
                self.visit_order_terms(&function.argument_order)?;
                self.visit_order_terms(&function.within_group)?;
                self.visit_optional_expr(function.filter.as_deref())?;
                if let Some(window) = &function.window {
                    self.visit_window_spec(window)?;
                }
                Ok(())
            }
            Expr::InList {
                lhs,
                values,
                comparisons,
                ..
            } => {
                self.visit_expr(lhs)?;
                self.visit_exprs(values)?;
                self.require(
                    comparisons.len() == values.len(),
                    "IN-list comparison count does not match its value count",
                )?;
                for (value, comparison) in values.iter().zip(comparisons) {
                    self.visit_expression_comparison(comparison, lhs, value, "IN-list comparison")?;
                }
                Ok(())
            }
            Expr::Subquery(subquery) => self.visit_subquery(subquery),
            Expr::Like {
                lhs,
                rhs,
                escape,
                function,
                ..
            } => {
                self.visit_catalog_object(function, "LIKE function")?;
                self.visit_expr(lhs)?;
                self.visit_expr(rhs)?;
                self.visit_optional_expr(escape.as_deref())
            }
            Expr::Row(values) | Expr::Array(values) => self.visit_exprs(values),
            Expr::Subscript { base, index } => {
                self.visit_expr(base)?;
                self.visit_expr(index)
            }
            Expr::FieldAccess(access) => {
                self.visit_catalog_object(&access.container_type, "field-access type")?;
                self.visit_type_fact(&access.result_type)?;
                self.visit_expr(&access.base)
            }
            Expr::Raise { message, .. } => self.visit_optional_expr(message.as_deref()),
        }
    }

    fn visit_function_evaluation(&self, call: &FunctionCall) -> ValidationResult {
        let aggregate = match call.function.value() {
            crate::function::Func::Agg(_) => true,
            crate::function::Func::External(function) => function.func.is_aggregate(),
            _ => false,
        };
        let window = call.window.is_some()
            || matches!(call.function.value(), crate::function::Func::Window(_));
        match call.evaluation {
            FunctionEvaluation::Scalar => self.require(
                !aggregate && !window,
                "aggregate or window function has scalar evaluation identity",
            ),
            FunctionEvaluation::Aggregate(id) => {
                let block = self.query_block(id.block)?;
                self.require(
                    aggregate && !window,
                    format!("aggregate identity {id:?} belongs to a non-aggregate call"),
                )?;
                self.require(
                    id.index < block.aggregate_count,
                    format!("aggregate identity {id:?} is outside its block"),
                )
            }
            FunctionEvaluation::Window(id) => {
                let block = self.query_block(id.block)?;
                self.require(
                    window,
                    format!("window identity {id:?} belongs to a non-window call"),
                )?;
                self.require(
                    id.index < block.window_function_count,
                    format!("window identity {id:?} is outside its block"),
                )
            }
        }
    }

    fn visit_subquery(&self, subquery: &SubqueryExpr) -> ValidationResult {
        match subquery {
            SubqueryExpr::Scalar { query, output } => {
                self.visit_query(*query)?;
                let width = self.query(*query)?.output.len();
                self.require(
                    *output < width,
                    format!("scalar subquery {query} output {output} is out of range"),
                )
            }
            SubqueryExpr::Exists(query) => self.visit_query(*query),
            SubqueryExpr::In {
                lhs,
                query,
                comparison,
                ..
            } => {
                self.visit_expr(lhs)?;
                self.visit_query(*query)?;
                let lhs_width = expression_width(lhs);
                let output_width = self.query(*query)?.output.len();
                self.require(
                    output_width == lhs_width,
                    format!("IN subquery {query} has {output_width} outputs, expected {lhs_width}"),
                )?;
                self.visit_comparison(comparison, lhs_width, "IN-subquery comparison")
            }
        }
    }

    fn visit_expression_comparison(
        &self,
        comparison: &ComparisonSemantics,
        lhs: &Expr,
        rhs: &Expr,
        context: &str,
    ) -> ValidationResult {
        let lhs_width = expression_width(lhs);
        let rhs_width = expression_width(rhs);
        self.require(
            lhs_width == rhs_width,
            format!("{context} compares widths {lhs_width} and {rhs_width}"),
        )?;
        self.visit_comparison(comparison, lhs_width, context)
    }

    fn visit_comparison(
        &self,
        comparison: &ComparisonSemantics,
        expected_width: usize,
        context: &str,
    ) -> ValidationResult {
        self.require(
            comparison.components.len() == expected_width,
            format!(
                "{context} has {} components, expected {expected_width}",
                comparison.components.len()
            ),
        )?;
        for component in &comparison.components {
            self.visit_optional_catalog_object(
                component.collation.as_ref(),
                "comparison collation",
            )?;
        }
        Ok(())
    }

    fn visit_window_spec(&self, window: &WindowSpec) -> ValidationResult {
        self.visit_exprs(&window.partition_by)?;
        self.visit_order_terms(&window.order_by)?;
        let Some(frame) = &window.frame else {
            return Ok(());
        };
        self.visit_window_bound(&frame.start)?;
        if let Some(end) = &frame.end {
            self.visit_window_bound(end)?;
        }
        Ok(())
    }

    fn visit_window_bound(&self, bound: &WindowFrameBound) -> ValidationResult {
        match bound {
            WindowFrameBound::Following(expr) | WindowFrameBound::Preceding(expr) => {
                self.visit_expr(expr)
            }
            WindowFrameBound::CurrentRow
            | WindowFrameBound::UnboundedFollowing
            | WindowFrameBound::UnboundedPreceding => Ok(()),
        }
    }

    fn visit_order_terms(&self, terms: &[OrderTerm]) -> ValidationResult {
        for term in terms {
            self.visit_expr(&term.expr)?;
            self.visit_type_fact(&term.type_fact)?;
            if let Some(collation) = &term.collation {
                self.visit_catalog_object(collation, "ORDER BY collation")?;
            }
        }
        Ok(())
    }

    fn visit_optional_limit(&self, limit: Option<&Limit>) -> ValidationResult {
        let Some(limit) = limit else {
            return Ok(());
        };
        self.visit_expr(&limit.limit)?;
        self.visit_optional_expr(limit.offset.as_ref())
    }

    fn visit_exprs(&self, expressions: &[Expr]) -> ValidationResult {
        for expression in expressions {
            self.visit_expr(expression)?;
        }
        Ok(())
    }

    fn visit_optional_expr(&self, expression: Option<&Expr>) -> ValidationResult {
        expression.map_or(Ok(()), |expression| self.visit_expr(expression))
    }

    fn visit_source_table(
        &self,
        source: &Source,
        table: &ResolvedTable,
        validate_stored_programs: bool,
    ) -> ValidationResult {
        self.visit_catalog_object(table, "source table")?;
        self.require(
            source.database == table.database(),
            format!(
                "source {} and its table disagree on database identity",
                source.id
            ),
        )?;
        self.require(
            source.columns.len() == table.value().columns().len(),
            format!("source {} width differs from its catalog table", source.id),
        )?;
        if !validate_stored_programs {
            return Ok(());
        }
        for (position, catalog_column) in table.value().columns().iter().enumerate() {
            let generated = &source.generated_expressions[position];
            self.require(
                matches!(generated, ColumnReadExpression::Absent)
                    == catalog_column.generated_expr().is_none(),
                format!(
                    "source {} generated-expression slot {position} disagrees with its catalog table",
                    source.id
                ),
            )?;
            let default = &source.default_expressions[position];
            self.require(
                matches!(default, ColumnReadExpression::Absent) == catalog_column.default.is_none(),
                format!(
                    "source {} default-expression slot {position} disagrees with its catalog table",
                    source.id
                ),
            )?;
        }
        if let Some(constraints) = &source.check_constraints {
            let Some(table) = table.value().btree() else {
                return self.require(
                    constraints.is_empty(),
                    format!("non-B-tree source {} carries CHECK metadata", source.id),
                );
            };
            let require_complete = matches!(
                &self.document.root,
                HirRoot::Insert(insert) if insert.target == source.id
            );
            if require_complete {
                self.require(
                    constraints.len() == table.check_constraints.len(),
                    format!("source {} has incomplete INSERT CHECK metadata", source.id),
                )?;
            }
            let mut previous = None;
            for constraint in constraints {
                self.require(
                    constraint.catalog_position < table.check_constraints.len(),
                    format!(
                        "source {} CHECK position is outside its catalog table",
                        source.id
                    ),
                )?;
                self.require(
                    previous.is_none_or(|position| position < constraint.catalog_position),
                    format!(
                        "source {} CHECK positions are not unique and ordered",
                        source.id
                    ),
                )?;
                previous = Some(constraint.catalog_position);
                let catalog = &table.check_constraints[constraint.catalog_position];
                let expected = catalog
                    .name
                    .clone()
                    .unwrap_or_else(|| catalog.expr.to_string());
                self.require(
                    constraint.description == expected,
                    format!("source {} CHECK descriptions are out of order", source.id),
                )?;
            }
        }
        Ok(())
    }

    fn visit_custom_type_operation(&self, operation: &CustomTypeOperation) -> ValidationResult {
        let resolved_type = match operation {
            CustomTypeOperation::UnionValue { union_type, .. }
            | CustomTypeOperation::UnionTag { union_type, .. }
            | CustomTypeOperation::UnionExtract { union_type, .. } => union_type,
            CustomTypeOperation::StructExtract { struct_type, .. } => struct_type,
        };
        self.visit_catalog_object(resolved_type, "custom-type operation")?;
        match operation {
            CustomTypeOperation::UnionValue { result_type, .. }
            | CustomTypeOperation::UnionExtract { result_type, .. }
            | CustomTypeOperation::StructExtract { result_type, .. } => {
                self.visit_type_fact(result_type)
            }
            CustomTypeOperation::UnionTag { .. } => Ok(()),
        }
    }

    fn visit_sequence_operation(&self, operation: &SequenceOperation) -> ValidationResult {
        self.require_database(operation.database)?;
        self.visit_catalog_object(&operation.backing_table, "sequence backing table")?;
        if let Some(sqlite_sequence) = &operation.sqlite_sequence {
            self.visit_catalog_object(sqlite_sequence, "sqlite_sequence table")?;
        }
        let expected = self
            .document
            .databases
            .iter()
            .find(|snapshot| snapshot.database == operation.database)
            .map(|snapshot| snapshot.schema_version)
            .ok_or_else(|| {
                HirValidationError::new(format!(
                    "sequence database {} is absent from the catalog snapshot",
                    operation.database.index()
                ))
            })?;
        self.require(
            operation.schema_cookie == expected,
            format!(
                "sequence schema cookie {} does not match snapshot version {expected}",
                operation.schema_cookie
            ),
        )
    }

    fn visit_type_fact(&self, fact: &TypeFact) -> ValidationResult {
        if let Some(declared) = &fact.declared {
            for resolved_type in &declared.custom_chain {
                self.visit_catalog_object(resolved_type, "declared type")?;
            }
        }
        Ok(())
    }

    fn visit_catalog_object<T>(
        &self,
        object: &CatalogObject<T>,
        description: &str,
    ) -> ValidationResult {
        self.require(
            object.snapshot() == self.document.snapshot,
            format!("{description} belongs to a different catalog snapshot"),
        )?;
        if let Some(database) = object.database() {
            self.require_database(database)?;
        }
        Ok(())
    }

    fn visit_optional_catalog_object<T>(
        &self,
        object: Option<&CatalogObject<T>>,
        description: &str,
    ) -> ValidationResult {
        object.map_or(Ok(()), |object| {
            self.visit_catalog_object(object, description)
        })
    }

    fn require_database(&self, database: DatabaseId) -> ValidationResult {
        self.require(
            self.document
                .databases
                .iter()
                .any(|snapshot| snapshot.database == database),
            format!(
                "database {} is absent from the catalog snapshot",
                database.index()
            ),
        )
    }

    fn visit_column_ref(&self, reference: ColumnRef) -> ValidationResult {
        self.visit_source(reference.source, None)?;
        self.validate_column_position(reference.source, reference.column)?;
        self.require_column_read_programs(reference.source, reference.column)
    }

    fn require_complete_row_image(&self, source: SourceId) -> ValidationResult {
        let width = self.source(source)?.columns.len();
        for column in 0..width {
            self.require_column_read_programs(source, column)?;
        }
        Ok(())
    }

    fn require_complete_index_metadata(&self, source: SourceId) -> ValidationResult {
        self.require(
            matches!(
                &self.source(source)?.index_coverage,
                IndexCoverage::Complete { .. }
            ),
            format!("DML source {source} does not carry complete index metadata"),
        )
    }

    fn require_column_read_programs(&self, source: SourceId, column: usize) -> ValidationResult {
        let source_definition = self.source(source)?;
        self.validate_column_position(source, column)?;
        for (description, expression) in [
            (
                "generated expression",
                &source_definition.generated_expressions[column],
            ),
            (
                "default expression",
                &source_definition.default_expressions[column],
            ),
        ] {
            self.require(
                !matches!(expression, ColumnReadExpression::NotRequired),
                format!("source {source} column {column} requires an unplanned {description}"),
            )?;
        }
        let type_fact = &source_definition.columns[column].type_fact;
        let needs_type_programs = type_fact.array_dimensions > 0
            || type_fact
                .declared
                .as_ref()
                .is_some_and(|declared| !declared.custom_chain.is_empty());
        self.require(
            source_definition.column_type_programs[column].is_some() == needs_type_programs,
            format!("source {source} column {column} has incomplete type programs"),
        )?;
        if let Some(programs) = &source_definition.column_type_programs[column] {
            let chain = type_fact
                .declared
                .as_ref()
                .map_or(&[][..], |declared| declared.custom_chain.as_slice());
            self.require(
                programs.encode.len()
                    == chain
                        .iter()
                        .filter(|definition| definition.value().encode().is_some())
                        .count(),
                format!("source {source} column {column} has incomplete ENCODE programs"),
            )?;
            self.require(
                programs.decode.len()
                    == chain
                        .iter()
                        .filter(|definition| definition.value().decode().is_some())
                        .count(),
                format!("source {source} column {column} has incomplete DECODE programs"),
            )?;
            self.require(
                programs.array.as_ref().map(|array| array.dimensions)
                    == (type_fact.array_dimensions > 0).then_some(type_fact.array_dimensions),
                format!("source {source} column {column} has incorrect array storage metadata"),
            )?;
            let expected_encode_nulls = type_fact.array_dimensions == 0
                && chain.iter().any(|definition| definition.value().not_null);
            self.require(
                programs.encode_nulls == expected_encode_nulls,
                format!("source {source} column {column} has incorrect NULL encoding metadata"),
            )?;
        }
        Ok(())
    }

    fn validate_column_position(&self, source: SourceId, position: usize) -> ValidationResult {
        let width = self.source(source)?.columns.len();
        self.require(
            position < width,
            format!("source {source} column {position} is out of range for width {width}"),
        )
    }

    fn query(&self, id: QueryId) -> ValidationResult<&Query> {
        self.document
            .query(id)
            .ok_or_else(|| HirValidationError::new(format!("query {id} does not exist")))
    }

    fn query_block(&self, id: QueryBlockId) -> ValidationResult<&QueryBlock> {
        self.document
            .query_block(id)
            .ok_or_else(|| HirValidationError::new(format!("query block {id:?} does not exist")))
    }

    fn source(&self, id: SourceId) -> ValidationResult<&Source> {
        self.document
            .source(id)
            .ok_or_else(|| HirValidationError::new(format!("source {id} does not exist")))
    }

    fn cte(&self, id: CteId) -> ValidationResult<&Cte> {
        self.document
            .cte(id)
            .ok_or_else(|| HirValidationError::new(format!("CTE {id} does not exist")))
    }

    fn require(&self, condition: bool, message: impl Into<String>) -> ValidationResult {
        if condition {
            Ok(())
        } else {
            Err(HirValidationError::new(message))
        }
    }

    fn invalid<T>(&self, message: impl Into<String>) -> ValidationResult<T> {
        Err(HirValidationError::new(message))
    }
}
