//! Independent structural validation for completed HIR documents.

use std::{cell::Cell, collections::HashSet, fmt};

use super::*;

type ValidationResult<T = ()> = std::result::Result<T, HirValidationError>;

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
        self.validate_arena_identities()?;
        self.visit_root(&self.document.root)?;
        self.validate_reachability()
    }

    fn validate_arena_identities(&self) -> ValidationResult {
        for (index, query) in self.document.queries.iter().enumerate() {
            let expected = QueryId::new(index);
            self.require(
                query.id == expected,
                format!("query arena slot {index} contains {}", query.id),
            )?;
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
        }
    }

    fn visit_insert(&self, insert: &Insert) -> ValidationResult {
        self.visit_source(insert.target, Some(SourceOwner::Root))?;
        for column in &insert.columns {
            self.validate_target_column(insert.target, *column)?;
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
                }
                self.visit_optional_expr(target.predicate.as_ref())?;
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
        for default in &update.defaults {
            self.validate_column_position(update.target, default.column)?;
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
            SourceKind::SchemaExpression | SourceKind::Table(_) | SourceKind::Pseudo { .. } => {}
            SourceKind::TableFunction { arguments, .. } => self.visit_exprs(arguments)?,
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

        self.visit_column_read_expressions(&source.generated_expressions)?;
        self.visit_column_read_expressions(&source.default_expressions)?;
        for programs in source.column_type_programs.iter().flatten() {
            self.visit_column_type_programs(programs)?;
        }
        for constraint in &source.check_constraints {
            self.visit_expr(&constraint.expression)?;
        }
        for expressions in &source.index_expressions {
            for expression in expressions.columns.iter().flatten() {
                self.visit_expr(expression)?;
            }
            self.visit_optional_expr(expressions.predicate.as_ref())?;
        }
        let mut pattern_ids = HashSet::with_capacity(source.index_method_patterns.len());
        for pattern in &source.index_method_patterns {
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
                for term in &recursive.queue_order {
                    self.require(
                        term.output < cte.columns.len(),
                        format!(
                            "recursive CTE {id} queue output {} is out of range",
                            term.output
                        ),
                    )?;
                }
                self.visit_optional_limit(recursive.limit.as_ref())?;
            }
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
                self.visit_column_ref(column.right)
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
                lhs, rhs, custom, ..
            } => {
                self.visit_expr(lhs)?;
                self.visit_expr(rhs)?;
                if let Some(encoding) = custom
                    .as_ref()
                    .and_then(|custom| custom.literal_encoding.as_ref())
                    .and_then(|encoding| encoding.encoder.as_ref())
                {
                    self.visit_schema_call(encoding)?;
                }
                Ok(())
            }
            Expr::Between {
                expr, start, end, ..
            } => {
                self.visit_expr(expr)?;
                self.visit_expr(start)?;
                self.visit_expr(end)
            }
            Expr::Case {
                base,
                when_then,
                else_expr,
            } => {
                self.visit_optional_expr(base.as_deref())?;
                for (when, then) in when_then {
                    self.visit_expr(when)?;
                    self.visit_expr(then)?;
                }
                self.visit_optional_expr(else_expr.as_deref())
            }
            Expr::Cast { expr, target } => {
                self.visit_expr(expr)?;
                self.visit_exprs(&target.parameters)?;
                self.visit_cast_programs(&target.programs)
            }
            Expr::Collate { expr, .. } => self.visit_expr(expr),
            Expr::Function(function) => {
                self.visit_exprs(&function.arguments)?;
                self.visit_order_terms(&function.argument_order)?;
                self.visit_order_terms(&function.within_group)?;
                self.visit_optional_expr(function.filter.as_deref())?;
                if let Some(window) = &function.window {
                    self.visit_window_spec(window)?;
                }
                Ok(())
            }
            Expr::InList { lhs, values, .. } => {
                self.visit_expr(lhs)?;
                self.visit_exprs(values)
            }
            Expr::Subquery(subquery) => self.visit_subquery(subquery),
            Expr::Like {
                lhs, rhs, escape, ..
            } => {
                self.visit_expr(lhs)?;
                self.visit_expr(rhs)?;
                self.visit_optional_expr(escape.as_deref())
            }
            Expr::Row(values) | Expr::Array(values) => self.visit_exprs(values),
            Expr::Subscript { base, index } => {
                self.visit_expr(base)?;
                self.visit_expr(index)
            }
            Expr::FieldAccess(access) => self.visit_expr(&access.base),
            Expr::Raise { message, .. } => self.visit_optional_expr(message.as_deref()),
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
            SubqueryExpr::In { lhs, query, .. } => {
                self.visit_expr(lhs)?;
                self.visit_query(*query)?;
                self.require(
                    self.query(*query)?.output.len() == 1,
                    format!("IN subquery {query} does not have exactly one output"),
                )
            }
        }
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

    fn visit_column_ref(&self, reference: ColumnRef) -> ValidationResult {
        self.visit_source(reference.source, None)?;
        self.validate_column_position(reference.source, reference.column)
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
