//! Lazy CTE registration and recursion state.

use std::collections::HashSet;

use turso_parser::ast;

use super::{
    cte_bindings::{cte_query_environment, CteBinding, CteState},
    cte_rules::{cte_self_reference_info, validate_recursive_cte_structure},
    hir::{self, CteId, SourceId, SourceOwner},
    scope::{QueryEnvironment, Scope},
    Analyzer,
};
use crate::{vdbe::affinity::Affinity, LimboError, Result};

struct PendingCteAnalysis {
    binding: CteBinding,
    id: CteId,
    dependencies: Vec<CteBinding>,
    next_dependency: usize,
}

impl Analyzer<'_, '_> {
    pub(crate) fn analyze_cte_source(
        &mut self,
        binding: CteBinding,
        alias: Option<&ast::Name>,
        owner: SourceOwner,
        environment: &QueryEnvironment,
    ) -> Result<SourceId> {
        let cte = match binding.state() {
            CteState::Unseen => self.analyze_cte_definition(&binding, environment)?,
            CteState::Analyzing {
                id,
                recursive_columns: Some(columns),
                mut recursive_inputs,
            } => {
                let source =
                    self.register_recursive_reference(id, binding.name(), &columns, alias, owner)?;
                recursive_inputs.push(source);
                binding.set_state(CteState::Analyzing {
                    id,
                    recursive_columns: Some(columns),
                    recursive_inputs,
                });
                return Ok(source);
            }
            CteState::Analyzing { .. } => {
                crate::bail_parse_error!("circular reference: {}", binding.name());
            }
            CteState::Complete(cte) => cte,
            CteState::Failed(message) => return Err(LimboError::ParseError(message)),
        };
        self.register_cte_reference(cte, alias, owner)
    }

    fn analyze_cte_definition(
        &mut self,
        binding: &CteBinding,
        environment: &QueryEnvironment,
    ) -> Result<CteId> {
        let id = self.begin_cte_definition(binding);
        let mut prepared = vec![binding.clone()];
        let mut pending = vec![PendingCteAnalysis {
            binding: binding.clone(),
            id,
            dependencies: binding.dependencies(),
            next_dependency: 0,
        }];
        let mut order = Vec::new();

        let result = (|| {
            while !pending.is_empty() {
                let dependency = {
                    let current = pending.last_mut().expect("pending CTE exists");
                    let dependency = current.dependencies.get(current.next_dependency).cloned();
                    if dependency.is_some() {
                        current.next_dependency += 1;
                    }
                    dependency
                };
                let Some(dependency) = dependency else {
                    let complete = pending.pop().expect("pending CTE exists");
                    order.push((complete.binding, complete.id));
                    continue;
                };

                let current = &pending.last().expect("pending CTE exists").binding;
                if dependency.is_same_definition(current) {
                    // Self-references are validated while analyzing the CTE
                    // body, where recursive UNION terms have column metadata.
                    continue;
                }
                if pending
                    .iter()
                    .any(|entry| dependency.is_same_definition(&entry.binding))
                {
                    crate::bail_parse_error!("circular reference: {}", dependency.name());
                }

                match dependency.state() {
                    CteState::Unseen => {
                        let dependency_id = self.begin_cte_definition(&dependency);
                        prepared.push(dependency.clone());
                        pending.push(PendingCteAnalysis {
                            dependencies: dependency.dependencies(),
                            binding: dependency,
                            id: dependency_id,
                            next_dependency: 0,
                        });
                    }
                    CteState::Analyzing {
                        recursive_columns: Some(_),
                        ..
                    }
                    | CteState::Complete(_) => {}
                    CteState::Analyzing { .. }
                        if prepared
                            .iter()
                            .any(|entry| dependency.is_same_definition(entry)) =>
                    {
                        // This dependency was already placed earlier in the
                        // post-order worklist and will be complete first.
                    }
                    CteState::Analyzing { .. } => {
                        crate::bail_parse_error!("circular reference: {}", dependency.name());
                    }
                    CteState::Failed(message) => {
                        return Err(LimboError::ParseError(message));
                    }
                }
            }

            for (binding, id) in order {
                match binding.state() {
                    CteState::Analyzing { id: state_id, .. } if state_id == id => {}
                    CteState::Complete(state_id) if state_id == id => continue,
                    CteState::Failed(message) => {
                        return Err(LimboError::ParseError(message));
                    }
                    _ => {
                        return Err(LimboError::InternalError(format!(
                            "CTE {} lost its prepared analysis state",
                            binding.name()
                        )));
                    }
                }

                let cte = self.analyze_cte_body(id, &binding, environment)?;
                self.insert_cte(id, cte)?;
                binding.set_state(CteState::Complete(id));
            }

            match binding.state() {
                CteState::Complete(id) => Ok(id),
                _ => Err(LimboError::InternalError(format!(
                    "CTE {} was not completed by its dependency worklist",
                    binding.name()
                ))),
            }
        })();

        if let Err(error) = &result {
            let message = match error {
                LimboError::ParseError(message) => message.clone(),
                other => other.to_string(),
            };
            for binding in prepared {
                if matches!(binding.state(), CteState::Analyzing { .. }) {
                    binding.set_state(CteState::Failed(message.clone()));
                }
            }
        }
        result
    }

    fn begin_cte_definition(&mut self, binding: &CteBinding) -> CteId {
        let id = self.reserve_cte();
        binding.set_state(CteState::Analyzing {
            id,
            recursive_columns: None,
            recursive_inputs: Vec::new(),
        });
        id
    }

    fn analyze_cte_body(
        &mut self,
        id: CteId,
        binding: &CteBinding,
        environment: &QueryEnvironment,
    ) -> Result<hir::Cte> {
        let syntax = binding.syntax();
        let environment = binding.query_environment(environment);
        let (recursive, first_arm_self_reference) =
            cte_self_reference_info(binding.name(), &syntax.select);
        if first_arm_self_reference {
            crate::bail_parse_error!("circular reference: {}", binding.name());
        }
        if recursive {
            self.analyze_recursive_cte(id, binding, &environment)
        } else {
            let query = self.analyze_query(&syntax.select, environment)?;
            let columns = self.cte_columns(binding.name(), &syntax.columns, query)?;
            Ok(hir::Cte {
                id,
                name: binding.name().to_string(),
                columns,
                materialized: syntax.materialized.clone(),
                body: hir::CteBody::Query(query),
            })
        }
    }

    fn analyze_recursive_cte(
        &mut self,
        id: CteId,
        binding: &CteBinding,
        environment: &QueryEnvironment,
    ) -> Result<hir::Cte> {
        let syntax = binding.syntax();
        let first_recursive = validate_recursive_cte_structure(binding.name(), &syntax.select)?;
        let recursive_compounds = &syntax.select.body.compounds[first_recursive - 1..];
        let recursive_operator = recursive_compounds
            .first()
            .map(|compound| compound.operator)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "recursive CTE '{}' has no recursive compound arm",
                    binding.name()
                ))
            })?;
        if !matches!(
            recursive_operator,
            ast::CompoundOperator::Union | ast::CompoundOperator::UnionAll
        ) {
            crate::bail_parse_error!(
                "recursive CTEs must use UNION ALL or UNION between the initial and recursive queries"
            );
        }
        if recursive_compounds
            .iter()
            .any(|compound| compound.operator != recursive_operator)
        {
            crate::bail_parse_error!("recursive CTE queries must use the same UNION operator");
        }
        let body_environment = self.prepare_query_environment(
            cte_query_environment(environment),
            syntax.select.with.as_ref(),
        )?;
        let seed_syntax = ast::Select {
            with: None,
            body: ast::SelectBody {
                select: syntax.select.body.select.clone(),
                compounds: syntax.select.body.compounds[..first_recursive - 1].to_vec(),
            },
            order_by: Vec::new(),
            limit: None,
        };
        let seed = self.analyze_query(&seed_syntax, body_environment.clone())?;
        let mut columns = self.cte_columns(binding.name(), &syntax.columns, seed)?;
        binding.set_state(CteState::Analyzing {
            id,
            recursive_columns: Some(columns.clone()),
            recursive_inputs: Vec::new(),
        });

        let mut arms = Vec::new();
        for compound in recursive_compounds {
            let arm_syntax = ast::Select {
                with: None,
                body: ast::SelectBody {
                    select: compound.select.clone(),
                    compounds: Vec::new(),
                },
                order_by: Vec::new(),
                limit: None,
            };
            let query = self.analyze_query(&arm_syntax, body_environment.clone())?;
            let count = self.query_outputs(query)?.len();
            if count != columns.len() {
                crate::bail_parse_error!(
                    "SELECTs to the left and right of {} do not have the same number of result columns",
                    compound.operator
                );
            }
            arms.push(hir::RecursiveArm {
                operator: compound.operator,
                query,
            });
        }

        let input_sources = match binding.state() {
            CteState::Analyzing {
                id: state_id,
                recursive_inputs,
                ..
            } if state_id == id => recursive_inputs,
            _ => {
                return Err(LimboError::InternalError(format!(
                    "recursive CTE {} lost its analysis state",
                    binding.name()
                )))
            }
        };
        self.stabilize_recursive_cte_columns(
            id,
            binding.name(),
            &mut columns,
            &arms,
            &input_sources,
            true,
        )?;
        let mut queue_scope = Scope::new(None);
        queue_scope.set_ctes(body_environment.ctes.clone());
        let mut seed_outputs = self.query_outputs(seed)?;
        for (output, column) in seed_outputs.iter_mut().zip(&columns) {
            output.type_fact.clone_from(&column.type_fact);
        }
        queue_scope.set_outputs(&seed_outputs);
        let mut compound_outputs = self
            .query(seed)
            .ok_or_else(|| LimboError::InternalError(format!("missing recursive seed {seed}")))?
            .blocks
            .iter()
            .map(|block| block.outputs.clone())
            .collect::<Vec<_>>();
        for arm in &arms {
            let query = self.query(arm.query).ok_or_else(|| {
                LimboError::InternalError(format!("missing recursive arm {}", arm.query))
            })?;
            compound_outputs.extend(query.blocks.iter().map(|block| block.outputs.clone()));
        }
        let comparison_collations = (0..columns.len())
            .map(|column| {
                compound_outputs
                    .iter()
                    .find_map(|outputs| outputs.get(column)?.collation.clone())
            })
            .collect::<Vec<_>>();
        let queue_order = self.analyze_query_order_by(
            &syntax.select.order_by,
            &queue_scope,
            Some(&compound_outputs),
        )?;
        let queue_order = queue_order
            .into_iter()
            .map(|term| {
                let explicit_collation =
                    self.expression_explicit_collation(&term.expr, &queue_scope);
                let mut expression = &term.expr;
                while let hir::Expr::Collate { expr, .. } = expression {
                    expression = expr;
                }
                let hir::Expr::Output(output) = expression else {
                    return Err(LimboError::InternalError(
                        "recursive CTE ORDER BY did not resolve to an output column".to_string(),
                    ));
                };
                if output.index >= columns.len() {
                    return Err(LimboError::InternalError(format!(
                        "recursive CTE ORDER BY resolved missing output column {}",
                        output.index
                    )));
                }
                Ok(hir::RecursiveOrderTerm {
                    output: output.index,
                    order: term.order,
                    nulls: term.nulls,
                    explicit_collation,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let limit = self.analyze_query_limit(syntax.select.limit.as_ref(), &body_environment)?;

        Ok(hir::Cte {
            id,
            name: binding.name().to_string(),
            columns,
            materialized: syntax.materialized.clone(),
            body: hir::CteBody::Recursive(hir::RecursiveCte {
                seed,
                arms,
                input_sources,
                comparison_collations,
                queue_order,
                limit,
            }),
        })
    }

    fn stabilize_recursive_cte_columns(
        &mut self,
        id: CteId,
        name: &str,
        columns: &mut [hir::CteColumn],
        arms: &[hir::RecursiveArm],
        input_sources: &[SourceId],
        finalize_custom_operators: bool,
    ) -> Result<()> {
        let seed_facts = columns
            .iter()
            .map(|column| column.type_fact.clone())
            .collect::<Vec<_>>();
        let width = columns.len();
        let precise_rounds = width.saturating_mul(2).max(4);
        let mut recent_rank_growth = vec![false; width];
        let mut stable = false;

        for round in 0..precise_rounds {
            self.set_recursive_input_facts(id, name, columns, input_sources)?;
            let arm_outputs = self.refresh_recursive_arm_outputs(arms)?;
            let next = merge_recursive_column_facts(name, &seed_facts, &arm_outputs)?;
            if next
                .iter()
                .zip(columns.iter())
                .all(|(next, current)| *next == current.type_fact)
            {
                stable = true;
                break;
            }
            if round.saturating_add(width.max(1)) >= precise_rounds {
                for ((grew, next), current) in
                    recent_rank_growth.iter_mut().zip(&next).zip(columns.iter())
                {
                    *grew |= next.is_array()
                        && next.array_dimensions > current.type_fact.array_dimensions;
                }
            }
            for (column, fact) in columns.iter_mut().zip(next) {
                column.type_fact = fact;
            }
        }

        if !stable {
            let mut widened = false;
            for (((column, seed), grew), column_index) in columns
                .iter_mut()
                .zip(&seed_facts)
                .zip(&recent_rank_growth)
                .zip(0..)
            {
                if !*grew {
                    continue;
                }
                if !column.type_fact.is_array() {
                    return Err(LimboError::InternalError(format!(
                        "recursive CTE {name} column {} grew in array rank without an array fact",
                        column_index + 1
                    )));
                }
                column.type_fact.array_rank_unbounded = true;
                column.type_fact.array_dimensions = seed.array_dimensions.max(1);
                column.type_fact.declared = None;
                widened = true;
            }
            if !widened {
                return Err(LimboError::InternalError(format!(
                    "recursive CTE {name} type facts did not stabilize"
                )));
            }

            stable = false;
            for _ in 0..width.saturating_add(2).max(3) {
                self.set_recursive_input_facts(id, name, columns, input_sources)?;
                let arm_outputs = self.refresh_recursive_arm_outputs(arms)?;
                let mut next = merge_recursive_column_facts(name, &seed_facts, &arm_outputs)?;
                for ((next, current), seed) in next.iter_mut().zip(columns.iter()).zip(&seed_facts)
                {
                    if current.type_fact.array_rank_unbounded && next.is_array() {
                        next.array_rank_unbounded = true;
                        next.array_dimensions = current
                            .type_fact
                            .array_dimensions
                            .max(seed.array_dimensions)
                            .max(1);
                        next.declared = None;
                    } else if next.is_array()
                        && current.type_fact.is_array()
                        && next.array_dimensions > current.type_fact.array_dimensions
                    {
                        next.array_rank_unbounded = true;
                        next.array_dimensions = current
                            .type_fact
                            .array_dimensions
                            .max(seed.array_dimensions)
                            .max(1);
                        next.declared = None;
                    }
                }
                if next
                    .iter()
                    .zip(columns.iter())
                    .all(|(next, current)| *next == current.type_fact)
                {
                    stable = true;
                    break;
                }
                for (column, fact) in columns.iter_mut().zip(next) {
                    column.type_fact = fact;
                }
            }
            if !stable {
                return Err(LimboError::InternalError(format!(
                    "recursive CTE {name} type facts did not stabilize after array-rank widening"
                )));
            }
        }

        self.set_recursive_input_facts(id, name, columns, input_sources)?;
        self.refresh_recursive_arm_outputs(arms)?;
        if finalize_custom_operators {
            self.finalize_recursive_arm_semantics(arms)?;
        }
        Ok(())
    }

    fn set_recursive_input_facts(
        &mut self,
        id: CteId,
        name: &str,
        columns: &[hir::CteColumn],
        input_sources: &[SourceId],
    ) -> Result<()> {
        for source_id in input_sources {
            let source = self.source_mut(*source_id).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "recursive CTE {name} lost input source {source_id}"
                ))
            })?;
            if !matches!(&source.kind, hir::SourceKind::RecursiveInput(source_cte) if *source_cte == id)
            {
                return Err(LimboError::InternalError(format!(
                    "source {source_id} is not an input for recursive CTE {name}"
                )));
            }
            if source.columns.len() != columns.len() {
                return Err(LimboError::InternalError(format!(
                    "recursive CTE {name} input source {source_id} has {} columns, expected {}",
                    source.columns.len(),
                    columns.len()
                )));
            }
            for (source_column, column) in source.columns.iter_mut().zip(columns) {
                source_column.type_fact.clone_from(&column.type_fact);
            }
        }
        Ok(())
    }

    fn refresh_recursive_arm_outputs(
        &mut self,
        arms: &[hir::RecursiveArm],
    ) -> Result<Vec<Vec<hir::Output>>> {
        let mut refresh = RecursiveSemanticRefresh::default();
        arms.iter()
            .map(|arm| self.refresh_query_semantics(arm.query, &mut refresh, false))
            .collect()
    }

    fn finalize_recursive_arm_semantics(&mut self, arms: &[hir::RecursiveArm]) -> Result<()> {
        let mut refresh = RecursiveSemanticRefresh::default();
        for arm in arms {
            self.refresh_query_semantics(arm.query, &mut refresh, true)?;
        }
        Ok(())
    }

    fn refresh_query_semantics(
        &mut self,
        query_id: hir::QueryId,
        refresh: &mut RecursiveSemanticRefresh,
        finalize_custom_operators: bool,
    ) -> Result<Vec<hir::Output>> {
        if refresh.refreshed_queries.contains(&query_id) {
            return self.query_outputs(query_id);
        }
        if !refresh.refreshing_queries.insert(query_id) {
            return Err(LimboError::InternalError(format!(
                "recursive CTE contains a cyclic semantic subquery graph at {query_id}"
            )));
        }

        let query = self.query(query_id).ok_or_else(|| {
            LimboError::InternalError(format!("missing recursive query {query_id}"))
        })?;
        let mut nested_queries = Vec::new();
        let mut nested_ctes = Vec::new();
        collect_query_subqueries(query, &mut nested_queries);
        let source_ids = query_source_ids(query);
        for source_id in &source_ids {
            match self.source(*source_id).map(|source| &source.kind) {
                Some(hir::SourceKind::Derived(derived)) => nested_queries.push(*derived),
                Some(hir::SourceKind::Cte(cte)) => nested_ctes.push(*cte),
                Some(hir::SourceKind::TableFunction { arguments, .. }) => {
                    for argument in arguments {
                        collect_expression_subqueries(argument, &mut nested_queries);
                    }
                }
                Some(
                    hir::SourceKind::SchemaExpression
                    | hir::SourceKind::Table(_)
                    | hir::SourceKind::RecursiveInput(_)
                    | hir::SourceKind::Pseudo { .. },
                )
                | None => {}
            }
        }
        for nested in nested_queries {
            self.refresh_query_semantics(nested, refresh, finalize_custom_operators)?;
        }
        for cte in nested_ctes {
            self.refresh_cte_semantics(cte, refresh, finalize_custom_operators)?;
        }

        // FROM sources cache their child outputs and table-function argument
        // expressions. Refresh those copies before this query reads them.
        for source_id in source_ids {
            self.refresh_query_source_semantics(query_id, source_id, finalize_custom_operators)?;
        }

        let mut query = self.query(query_id).cloned().ok_or_else(|| {
            LimboError::InternalError(format!("missing recursive query {query_id}"))
        })?;
        for block in &mut query.blocks {
            let mut scope = Scope::new(None);
            scope.set_outputs(&block.outputs);
            if let Some(from) = &mut block.from {
                for join in &mut from.joins {
                    match &mut join.constraint {
                        hir::JoinConstraint::On(expression) => {
                            self.refresh_widened_expression(
                                expression,
                                &scope,
                                finalize_custom_operators,
                            )?;
                        }
                        hir::JoinConstraint::Using(columns)
                        | hir::JoinConstraint::Natural(columns) => {
                            for column in columns {
                                let left = self.refresh_widened_expression(
                                    &mut column.left,
                                    &scope,
                                    finalize_custom_operators,
                                )?;
                                let right = self
                                    .source(column.right.source)
                                    .and_then(|source| source.columns.get(column.right.column))
                                    .map(|column| column.type_fact.clone())
                                    .unwrap_or_default();
                                column.type_fact = match column.value {
                                    hir::MergedColumnValue::Left => left,
                                    hir::MergedColumnValue::Right => right,
                                    hir::MergedColumnValue::Coalesce => {
                                        hir::TypeFact::selected_value_result([&left, &right])
                                    }
                                };
                            }
                        }
                        hir::JoinConstraint::None => {}
                    }
                }
            }
            match &mut block.body {
                hir::QueryBlockBody::Select {
                    filter,
                    grouping,
                    windows,
                    ..
                } => {
                    for output in &mut block.outputs {
                        output.type_fact = self.refresh_widened_expression(
                            &mut output.expr,
                            &scope,
                            finalize_custom_operators,
                        )?;
                    }
                    scope.set_outputs(&block.outputs);
                    if let Some(filter) = filter {
                        self.refresh_widened_expression(filter, &scope, finalize_custom_operators)?;
                    }
                    if let Some(grouping) = grouping {
                        for key in &mut grouping.keys {
                            self.refresh_widened_expression(
                                key,
                                &scope,
                                finalize_custom_operators,
                            )?;
                        }
                        grouping.key_type_facts = grouping
                            .keys
                            .iter()
                            .map(|key| self.expression_type_fact(key, &scope))
                            .collect();
                        grouping.key_collations = grouping
                            .keys
                            .iter()
                            .map(|key| self.expression_collation_with_origin(key, &scope).0)
                            .collect();
                        if let Some(having) = &mut grouping.having {
                            self.refresh_widened_expression(
                                having,
                                &scope,
                                finalize_custom_operators,
                            )?;
                        }
                    }
                    for window in windows {
                        refresh_window_spec_semantics(
                            self,
                            &mut window.spec,
                            &scope,
                            finalize_custom_operators,
                        )?;
                    }
                }
                hir::QueryBlockBody::Values { rows } => {
                    for row in rows.iter_mut() {
                        for expression in row {
                            self.refresh_widened_expression(
                                expression,
                                &scope,
                                finalize_custom_operators,
                            )?;
                        }
                    }
                    for (column_index, output) in block.outputs.iter_mut().enumerate() {
                        if let Some(expression) = rows.first().and_then(|row| row.get(column_index))
                        {
                            output.expr.clone_from(expression);
                        }
                        let facts = rows
                            .iter()
                            .filter_map(|row| row.get(column_index))
                            .map(|expression| self.expression_type_fact(expression, &scope))
                            .collect::<Vec<_>>();
                        output.type_fact = hir::TypeFact::selected_value_result(&facts);
                    }
                    scope.set_outputs(&block.outputs);
                }
            }
        }
        if !query.compounds.is_empty() {
            let output_count = query
                .blocks
                .get(query.first.index)
                .map(|block| block.outputs.len())
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "recursive query {query_id} lost its first block"
                    ))
                })?;
            let merged = (0..output_count)
                .map(|index| {
                    super::query::compound_output_type_fact(query_id, &query.blocks, index)
                })
                .collect::<Result<Vec<_>>>()?;
            let first = query.blocks.get_mut(query.first.index).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "recursive query {query_id} lost its first block"
                ))
            })?;
            for (output, type_fact) in first.outputs.iter_mut().zip(merged) {
                output.type_fact = type_fact;
            }
        }
        let first_outputs = query
            .blocks
            .get(query.first.index)
            .map(|block| block.outputs.clone())
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "recursive query {query_id} lost its first block"
                ))
            })?;
        let mut result_scope = Scope::new(None);
        result_scope.set_outputs(&first_outputs);
        for term in &mut query.order_by {
            self.refresh_widened_expression(
                &mut term.expr,
                &result_scope,
                finalize_custom_operators,
            )?;
        }
        if let Some(limit) = &mut query.limit {
            self.refresh_widened_expression(
                &mut limit.limit,
                &result_scope,
                finalize_custom_operators,
            )?;
            if let Some(offset) = &mut limit.offset {
                self.refresh_widened_expression(offset, &result_scope, finalize_custom_operators)?;
            }
        }
        *self.query_mut(query_id).ok_or_else(|| {
            LimboError::InternalError(format!("missing recursive query slot {query_id}"))
        })? = query;
        refresh.refreshing_queries.remove(&query_id);
        refresh.refreshed_queries.insert(query_id);
        Ok(first_outputs)
    }

    fn refresh_cte_semantics(
        &mut self,
        cte_id: CteId,
        refresh: &mut RecursiveSemanticRefresh,
        finalize_custom_operators: bool,
    ) -> Result<Vec<hir::CteColumn>> {
        if refresh.refreshed_ctes.contains(&cte_id) {
            return self
                .cte(cte_id)
                .map(|cte| cte.columns.clone())
                .ok_or_else(|| LimboError::InternalError(format!("missing CTE {cte_id}")));
        }
        if !refresh.refreshing_ctes.insert(cte_id) {
            return Err(LimboError::InternalError(format!(
                "recursive CTE contains a cyclic semantic CTE graph at {cte_id}"
            )));
        }

        let mut cte = self
            .cte(cte_id)
            .cloned()
            .ok_or_else(|| LimboError::InternalError(format!("missing CTE {cte_id}")))?;
        match cte.body.clone() {
            hir::CteBody::Query(query) => {
                let outputs =
                    self.refresh_query_semantics(query, refresh, finalize_custom_operators)?;
                if cte.columns.len() != outputs.len() {
                    return Err(LimboError::InternalError(format!(
                        "CTE {} has {} columns, expected {}",
                        cte.name,
                        cte.columns.len(),
                        outputs.len()
                    )));
                }
                for (column, output) in cte.columns.iter_mut().zip(outputs) {
                    column.type_fact = output.type_fact;
                }
            }
            hir::CteBody::Recursive(recursive) => {
                self.stabilize_recursive_cte_columns(
                    cte.id,
                    &cte.name,
                    &mut cte.columns,
                    &recursive.arms,
                    &recursive.input_sources,
                    finalize_custom_operators,
                )?;
            }
        }

        let columns = cte.columns.clone();
        *self
            .cte_mut(cte_id)
            .ok_or_else(|| LimboError::InternalError(format!("missing CTE slot {cte_id}")))? = cte;
        refresh.refreshing_ctes.remove(&cte_id);
        refresh.refreshed_ctes.insert(cte_id);
        Ok(columns)
    }

    fn refresh_query_source_semantics(
        &mut self,
        query_id: hir::QueryId,
        source_id: SourceId,
        finalize_custom_operators: bool,
    ) -> Result<()> {
        enum SourceRefresh {
            Derived(hir::QueryId),
            Cte(CteId),
            TableFunction(Vec<hir::Expr>),
            None,
        }

        let action = self
            .source(source_id)
            .map(|source| match &source.kind {
                hir::SourceKind::Derived(query) => SourceRefresh::Derived(*query),
                hir::SourceKind::Cte(cte) => SourceRefresh::Cte(*cte),
                hir::SourceKind::TableFunction { arguments, .. } => {
                    SourceRefresh::TableFunction(arguments.clone())
                }
                hir::SourceKind::SchemaExpression
                | hir::SourceKind::Table(_)
                | hir::SourceKind::RecursiveInput(_)
                | hir::SourceKind::Pseudo { .. } => SourceRefresh::None,
            })
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "recursive query {query_id} lost source {source_id}"
                ))
            })?;

        match action {
            SourceRefresh::Derived(query) => {
                let facts = self
                    .query_outputs(query)?
                    .into_iter()
                    .map(|output| output.type_fact)
                    .collect::<Vec<_>>();
                let source = self.source_mut(source_id).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "recursive query {query_id} lost derived source {source_id}"
                    ))
                })?;
                if source.columns.len() != facts.len() {
                    return Err(LimboError::InternalError(format!(
                        "derived source {source_id} has {} columns, expected {}",
                        source.columns.len(),
                        facts.len()
                    )));
                }
                for (column, fact) in source.columns.iter_mut().zip(facts) {
                    column.type_fact = fact;
                }
            }
            SourceRefresh::Cte(cte) => {
                let facts = self
                    .cte(cte)
                    .ok_or_else(|| LimboError::InternalError(format!("missing CTE {cte}")))?
                    .columns
                    .iter()
                    .map(|column| column.type_fact.clone())
                    .collect::<Vec<_>>();
                let source = self.source_mut(source_id).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "recursive query {query_id} lost CTE source {source_id}"
                    ))
                })?;
                if source.columns.len() != facts.len() {
                    return Err(LimboError::InternalError(format!(
                        "CTE source {source_id} has {} columns, expected {}",
                        source.columns.len(),
                        facts.len()
                    )));
                }
                for (column, fact) in source.columns.iter_mut().zip(facts) {
                    column.type_fact = fact;
                }
            }
            SourceRefresh::TableFunction(mut arguments) => {
                let scope = Scope::new(None);
                for argument in &mut arguments {
                    self.refresh_widened_expression(argument, &scope, finalize_custom_operators)?;
                }
                let source = self.source_mut(source_id).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "recursive query {query_id} lost table-function source {source_id}"
                    ))
                })?;
                let hir::SourceKind::TableFunction {
                    arguments: stored, ..
                } = &mut source.kind
                else {
                    return Err(LimboError::InternalError(format!(
                        "source {source_id} stopped being a table function"
                    )));
                };
                *stored = arguments;
            }
            SourceRefresh::None => {}
        }
        Ok(())
    }

    fn refresh_widened_expression(
        &mut self,
        expression: &mut hir::Expr,
        scope: &Scope,
        finalize_custom_operators: bool,
    ) -> Result<hir::TypeFact> {
        if finalize_custom_operators {
            self.finalize_expression_semantics(expression, scope)
        } else {
            self.refresh_expression_type_fact(expression, scope)
        }
    }

    fn cte_columns(
        &self,
        name: &str,
        explicit: &[ast::IndexedColumn],
        query: hir::QueryId,
    ) -> Result<Vec<hir::CteColumn>> {
        let outputs = self.query_outputs(query)?;
        if !explicit.is_empty() && explicit.len() != outputs.len() {
            crate::bail_parse_error!(
                "table {} has {} values for {} columns",
                name,
                outputs.len(),
                explicit.len()
            );
        }
        Ok(outputs
            .iter()
            .enumerate()
            .map(|(index, output)| hir::CteColumn {
                name: explicit
                    .get(index)
                    .map(|column| crate::util::normalize_ident(column.col_name.as_str()))
                    .unwrap_or_else(|| output.name.clone()),
                type_fact: output.type_fact.clone(),
                affinity: output.affinity,
                has_affinity: output.has_affinity,
                collation: output.collation.clone(),
            })
            .collect())
    }

    fn register_cte_reference(
        &mut self,
        cte: CteId,
        alias: Option<&ast::Name>,
        owner: SourceOwner,
    ) -> Result<SourceId> {
        let definition = self
            .cte(cte)
            .cloned()
            .ok_or_else(|| LimboError::InternalError(format!("missing analyzed CTE {cte}")))?;
        let columns = definition
            .columns
            .into_iter()
            .map(|column| hir::SourceColumn {
                name: column.name,
                type_fact: column.type_fact,
                affinity: column.affinity,
                has_affinity: column.has_affinity,
                collation: column.collation,
                hidden: false,
                rowid_alias: false,
            })
            .collect::<Vec<_>>();
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database: None,
                name: definition.name,
                alias: alias.map(|alias| crate::util::normalize_ident(alias.as_str())),
                kind: hir::SourceKind::Cte(cte),
                generated_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                default_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                column_type_programs: vec![None; columns.len()],
                check_constraints: None,
                columns,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }

    fn register_recursive_reference(
        &mut self,
        cte: CteId,
        name: &str,
        columns: &[hir::CteColumn],
        alias: Option<&ast::Name>,
        owner: SourceOwner,
    ) -> Result<SourceId> {
        let source_columns = columns
            .iter()
            .map(|column| hir::SourceColumn {
                name: column.name.clone(),
                type_fact: column.type_fact.clone(),
                // The recursive arm reads from SQLite's queue table, not
                // from the CTE's outward-facing result. Queue columns have
                // no declared affinity, even though consumers of the
                // completed CTE retain the seed query's derived affinity.
                affinity: Affinity::Blob,
                has_affinity: false,
                collation: column.collation.clone(),
                hidden: false,
                rowid_alias: false,
            })
            .collect::<Vec<_>>();
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database: None,
                name: name.to_string(),
                alias: alias.map(|alias| crate::util::normalize_ident(alias.as_str())),
                kind: hir::SourceKind::RecursiveInput(cte),
                generated_expressions: vec![
                    hir::ColumnReadExpression::Absent;
                    source_columns.len()
                ],
                default_expressions: vec![hir::ColumnReadExpression::Absent; source_columns.len()],
                column_type_programs: vec![None; source_columns.len()],
                check_constraints: None,
                columns: source_columns,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }
}

#[derive(Default)]
struct RecursiveSemanticRefresh {
    refreshing_queries: HashSet<hir::QueryId>,
    refreshed_queries: HashSet<hir::QueryId>,
    refreshing_ctes: HashSet<CteId>,
    refreshed_ctes: HashSet<CteId>,
}

fn merge_recursive_column_facts(
    name: &str,
    seed_facts: &[hir::TypeFact],
    arm_outputs: &[Vec<hir::Output>],
) -> Result<Vec<hir::TypeFact>> {
    for outputs in arm_outputs {
        if outputs.len() != seed_facts.len() {
            return Err(LimboError::InternalError(format!(
                "recursive CTE {name} arm has {} columns, expected {}",
                outputs.len(),
                seed_facts.len()
            )));
        }
    }
    (0..seed_facts.len())
        .map(|column_index| {
            let mut facts = Vec::with_capacity(arm_outputs.len() + 1);
            facts.push(&seed_facts[column_index]);
            facts.extend(
                arm_outputs
                    .iter()
                    .map(|outputs| &outputs[column_index].type_fact),
            );
            Ok(hir::TypeFact::selected_value_result(facts))
        })
        .collect()
}

fn query_source_ids(query: &hir::Query) -> Vec<SourceId> {
    let mut sources = Vec::new();
    for block in &query.blocks {
        let Some(from) = &block.from else {
            continue;
        };
        sources.push(from.first);
        sources.extend(from.joins.iter().map(|join| join.right));
    }
    sources
}

fn collect_query_subqueries(query: &hir::Query, queries: &mut Vec<hir::QueryId>) {
    for block in &query.blocks {
        if let Some(from) = &block.from {
            for join in &from.joins {
                match &join.constraint {
                    hir::JoinConstraint::On(expression) => {
                        collect_expression_subqueries(expression, queries);
                    }
                    hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                        for column in columns {
                            collect_expression_subqueries(&column.left, queries);
                        }
                    }
                    hir::JoinConstraint::None => {}
                }
            }
        }
        for output in &block.outputs {
            collect_expression_subqueries(&output.expr, queries);
        }
        match &block.body {
            hir::QueryBlockBody::Select {
                filter,
                grouping,
                windows,
                ..
            } => {
                if let Some(filter) = filter {
                    collect_expression_subqueries(filter, queries);
                }
                if let Some(grouping) = grouping {
                    for key in &grouping.keys {
                        collect_expression_subqueries(key, queries);
                    }
                    if let Some(having) = &grouping.having {
                        collect_expression_subqueries(having, queries);
                    }
                }
                for window in windows {
                    collect_window_subqueries(&window.spec, queries);
                }
            }
            hir::QueryBlockBody::Values { rows } => {
                for expression in rows.iter().flatten() {
                    collect_expression_subqueries(expression, queries);
                }
            }
        }
    }
    for term in &query.order_by {
        collect_expression_subqueries(&term.expr, queries);
    }
    if let Some(limit) = &query.limit {
        collect_expression_subqueries(&limit.limit, queries);
        if let Some(offset) = &limit.offset {
            collect_expression_subqueries(offset, queries);
        }
    }
}

fn collect_window_subqueries(window: &hir::WindowSpec, queries: &mut Vec<hir::QueryId>) {
    for expression in &window.partition_by {
        collect_expression_subqueries(expression, queries);
    }
    for term in &window.order_by {
        collect_expression_subqueries(&term.expr, queries);
    }
    if let Some(frame) = &window.frame {
        for bound in std::iter::once(&frame.start).chain(frame.end.iter()) {
            match bound {
                hir::WindowFrameBound::Following(expression)
                | hir::WindowFrameBound::Preceding(expression) => {
                    collect_expression_subqueries(expression, queries);
                }
                hir::WindowFrameBound::CurrentRow
                | hir::WindowFrameBound::UnboundedFollowing
                | hir::WindowFrameBound::UnboundedPreceding => {}
            }
        }
    }
}

fn collect_expression_subqueries(expression: &hir::Expr, queries: &mut Vec<hir::QueryId>) {
    match expression {
        hir::Expr::MergedColumn(column) => {
            collect_expression_subqueries(&column.left, queries);
        }
        hir::Expr::Unary { expr, .. }
        | hir::Expr::Collate { expr, .. }
        | hir::Expr::IsNull(expr)
        | hir::Expr::NotNull(expr) => collect_expression_subqueries(expr, queries),
        hir::Expr::Binary { lhs, rhs, .. } => {
            collect_expression_subqueries(lhs, queries);
            collect_expression_subqueries(rhs, queries);
        }
        hir::Expr::Between {
            expr, start, end, ..
        } => {
            collect_expression_subqueries(expr, queries);
            collect_expression_subqueries(start, queries);
            collect_expression_subqueries(end, queries);
        }
        hir::Expr::Case {
            base,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(base) = base {
                collect_expression_subqueries(base, queries);
            }
            for (when, then) in when_then {
                collect_expression_subqueries(when, queries);
                collect_expression_subqueries(then, queries);
            }
            if let Some(else_expr) = else_expr {
                collect_expression_subqueries(else_expr, queries);
            }
        }
        hir::Expr::Cast { expr, target } => {
            collect_expression_subqueries(expr, queries);
            for parameter in &target.parameters {
                collect_expression_subqueries(parameter, queries);
            }
        }
        hir::Expr::Function(call) => {
            for argument in &call.arguments {
                collect_expression_subqueries(argument, queries);
            }
            for term in &call.argument_order {
                collect_expression_subqueries(&term.expr, queries);
            }
            for term in &call.within_group {
                collect_expression_subqueries(&term.expr, queries);
            }
            if let Some(filter) = &call.filter {
                collect_expression_subqueries(filter, queries);
            }
            if let Some(window) = &call.window {
                collect_window_subqueries(window, queries);
            }
        }
        hir::Expr::InList { lhs, values, .. } => {
            collect_expression_subqueries(lhs, queries);
            for value in values {
                collect_expression_subqueries(value, queries);
            }
        }
        hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, .. })
        | hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)) => queries.push(*query),
        hir::Expr::Subquery(hir::SubqueryExpr::In { lhs, query, .. }) => {
            collect_expression_subqueries(lhs, queries);
            queries.push(*query);
        }
        hir::Expr::Like {
            lhs, rhs, escape, ..
        } => {
            collect_expression_subqueries(lhs, queries);
            collect_expression_subqueries(rhs, queries);
            if let Some(escape) = escape {
                collect_expression_subqueries(escape, queries);
            }
        }
        hir::Expr::Row(expressions) | hir::Expr::Array(expressions) => {
            for expression in expressions {
                collect_expression_subqueries(expression, queries);
            }
        }
        hir::Expr::Subscript { base, index } => {
            collect_expression_subqueries(base, queries);
            collect_expression_subqueries(index, queries);
        }
        hir::Expr::FieldAccess(access) => {
            collect_expression_subqueries(&access.base, queries);
        }
        hir::Expr::Raise { message, .. } => {
            if let Some(message) = message {
                collect_expression_subqueries(message, queries);
            }
        }
        hir::Expr::Literal(_)
        | hir::Expr::Parameter(_)
        | hir::Expr::Column(_)
        | hir::Expr::RowId(_)
        | hir::Expr::Output(_) => {}
    }
}

fn refresh_window_spec_semantics(
    analyzer: &mut Analyzer<'_, '_>,
    window: &mut hir::WindowSpec,
    scope: &Scope,
    finalize_custom_operators: bool,
) -> Result<()> {
    for expression in &mut window.partition_by {
        analyzer.refresh_widened_expression(expression, scope, finalize_custom_operators)?;
    }
    for term in &mut window.order_by {
        analyzer.refresh_widened_expression(&mut term.expr, scope, finalize_custom_operators)?;
    }
    if let Some(frame) = &mut window.frame {
        for bound in std::iter::once(&mut frame.start).chain(frame.end.iter_mut()) {
            match bound {
                hir::WindowFrameBound::Following(expression)
                | hir::WindowFrameBound::Preceding(expression) => {
                    analyzer.refresh_widened_expression(
                        expression,
                        scope,
                        finalize_custom_operators,
                    )?;
                }
                hir::WindowFrameBound::CurrentRow
                | hir::WindowFrameBound::UnboundedFollowing
                | hir::WindowFrameBound::UnboundedPreceding => {}
            }
        }
    }
    Ok(())
}
