//! Lazy common-table-expression analysis.

use turso_parser::ast;

use super::{
    analyze::Analyzer,
    expr::ExprPolicies,
    hir::{
        Cte, CteBody, CteColumn, CteId, QueryId, RecursiveArm, RecursiveCte, RecursiveOrderTerm,
    },
    scope::Scope,
};
use crate::{
    translate::expr::{walk_expr, WalkControl},
    LimboError, Result,
};

pub(super) struct CteScope<'ast> {
    entries: Vec<PendingCte<'ast>>,
}

/// Lexical context available while a lazily resolved CTE is bound.
#[derive(Clone, Copy)]
pub(super) enum CteBindingContext<'scope> {
    /// `outer_scope` excludes sources owned by `parent` itself.
    Query {
        parent: QueryId,
        outer_scope: Option<&'scope Scope>,
        policies: ExprPolicies,
    },
    Root {
        policies: ExprPolicies,
    },
}

impl<'scope> CteBindingContext<'scope> {
    pub(super) fn new(
        parent: QueryId,
        outer_scope: Option<&'scope Scope>,
        policies: ExprPolicies,
    ) -> Self {
        Self::Query {
            parent,
            outer_scope,
            policies,
        }
    }

    pub(super) const fn root(policies: ExprPolicies) -> Self {
        Self::Root { policies }
    }

    fn query_parent(self) -> Option<QueryId> {
        match self {
            Self::Query {
                parent,
                outer_scope: Some(_),
                ..
            } => Some(parent),
            Self::Query {
                outer_scope: None, ..
            }
            | Self::Root { .. } => None,
        }
    }

    fn outer_scope(self) -> Option<&'scope Scope> {
        match self {
            Self::Query { outer_scope, .. } => outer_scope,
            Self::Root { .. } => None,
        }
    }

    fn policies(self) -> ExprPolicies {
        match self {
            Self::Query { policies, .. } | Self::Root { policies } => policies,
        }
    }
}

fn recursive_arm_start(syntax: &ast::CommonTableExpr) -> Result<Option<usize>> {
    let name = crate::util::normalize_ident(syntax.tbl_name.as_str());
    let counter = RecursiveRefCounter { cte_name: &name };
    let mut scope = RecursiveRefScope::new();
    counter.push_nested_ctes(syntax.select.with.as_ref(), &mut scope);
    let arms = std::iter::once(&syntax.select.body.select)
        .chain(syntax.select.body.compounds.iter().map(|arm| &arm.select));
    let mut first_recursive = None;
    for (index, arm) in arms.enumerate() {
        let (direct, total) = counter.count_arm(arm, &mut scope);
        if total == 0 {
            if first_recursive.is_some() {
                crate::bail_parse_error!("circular reference: {}", syntax.tbl_name.as_str());
            }
            continue;
        }
        if index == 0 || direct == 0 {
            crate::bail_parse_error!("circular reference: {}", syntax.tbl_name.as_str());
        }
        if direct > 1 {
            crate::bail_parse_error!(
                "multiple references to recursive table: {}",
                syntax.tbl_name.as_str()
            );
        }
        if total > direct {
            crate::bail_parse_error!(
                "multiple recursive references: {}",
                syntax.tbl_name.as_str()
            );
        }
        first_recursive.get_or_insert(index);
    }
    let Some(first_recursive) = first_recursive else {
        return Ok(None);
    };

    let operator = syntax.select.body.compounds[first_recursive - 1].operator;
    if !matches!(
        operator,
        ast::CompoundOperator::Union | ast::CompoundOperator::UnionAll
    ) {
        crate::bail_parse_error!(
            "recursive CTEs must use UNION ALL or UNION between the initial and recursive queries"
        );
    }
    if syntax.select.body.compounds[first_recursive..]
        .iter()
        .any(|arm| arm.operator != operator)
    {
        crate::bail_parse_error!("recursive CTE queries must use the same UNION operator");
    }
    Ok(Some(first_recursive))
}

struct RecursiveRefCounter<'a> {
    cte_name: &'a str,
}

/// Names visible while counting recursive references, innermost last. Weight
/// is zero for a shadowing definition, or the recursive-reference count
/// contributed when another nested CTE is used.
type RecursiveRefScope = Vec<(String, usize)>;

impl RecursiveRefCounter<'_> {
    fn name_weight(&self, name: &str, scope: &RecursiveRefScope) -> usize {
        scope
            .iter()
            .rev()
            .find_map(|(scope_name, weight)| (scope_name == name).then_some(*weight))
            .unwrap_or_else(|| usize::from(name == self.cte_name))
    }

    fn push_nested_ctes(&self, with: Option<&ast::With>, scope: &mut RecursiveRefScope) {
        let Some(with) = with else {
            return;
        };
        for cte in &with.ctes {
            let name = crate::util::normalize_ident(cte.tbl_name.as_str());
            // Its own name must shadow the outer recursive table while its
            // body is counted.
            scope.push((name, 0));
            let weight = self.count_select(&cte.select, scope);
            scope
                .last_mut()
                .expect("nested CTE scope entry was pushed")
                .1 = weight;
        }
    }

    fn count_select(&self, select: &ast::Select, scope: &mut RecursiveRefScope) -> usize {
        let scope_base = scope.len();
        self.push_nested_ctes(select.with.as_ref(), scope);
        let mut count = self.count_one_select(&select.body.select, scope);
        count += select
            .body
            .compounds
            .iter()
            .map(|arm| self.count_one_select(&arm.select, scope))
            .sum::<usize>();
        count += select
            .order_by
            .iter()
            .map(|term| self.count_expr(&term.expr, scope))
            .sum::<usize>();
        if let Some(limit) = &select.limit {
            count += self.count_expr(&limit.expr, scope);
            count += limit
                .offset
                .as_deref()
                .map_or(0, |offset| self.count_expr(offset, scope));
        }
        scope.truncate(scope_base);
        count
    }

    fn count_one_select(&self, one: &ast::OneSelect, scope: &mut RecursiveRefScope) -> usize {
        match one {
            ast::OneSelect::Values(rows) => rows
                .iter()
                .flatten()
                .map(|value| self.count_expr(value, scope))
                .sum(),
            ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
                ..
            } => {
                let mut count = from
                    .as_ref()
                    .map_or(0, |from| self.count_from_clause(from, scope));
                for column in columns {
                    if let ast::ResultColumn::Expr(value, _) = column {
                        count += self.count_expr(value, scope);
                    }
                }
                count += where_clause
                    .as_deref()
                    .map_or(0, |value| self.count_expr(value, scope));
                if let Some(grouping) = group_by {
                    count += grouping
                        .exprs
                        .iter()
                        .map(|value| self.count_expr(value, scope))
                        .sum::<usize>();
                    count += grouping
                        .having
                        .as_deref()
                        .map_or(0, |value| self.count_expr(value, scope));
                }
                for window in window_clause {
                    count += self.count_window(&window.window, scope);
                }
                count
            }
        }
    }

    fn count_from_table(&self, table: &ast::SelectTable, scope: &mut RecursiveRefScope) -> usize {
        match table {
            ast::SelectTable::Table(name, _, _) => {
                if name.db_name.is_some() {
                    return 0;
                }
                self.name_weight(&crate::util::normalize_ident(name.name.as_str()), scope)
            }
            ast::SelectTable::TableCall(name, arguments, _) => {
                let mut count = if name.db_name.is_none() {
                    self.name_weight(&crate::util::normalize_ident(name.name.as_str()), scope)
                } else {
                    0
                };
                count += arguments
                    .iter()
                    .map(|argument| self.count_expr(argument, scope))
                    .sum::<usize>();
                count
            }
            ast::SelectTable::Select(select, _) => self.count_select(select, scope),
            ast::SelectTable::Sub(from, _) => self.count_from_clause(from, scope),
        }
    }

    fn count_from_clause(&self, from: &ast::FromClause, scope: &mut RecursiveRefScope) -> usize {
        self.count_from_table(&from.select, scope)
            + from
                .joins
                .iter()
                .map(|join| {
                    self.count_from_table(&join.table, scope)
                        + match &join.constraint {
                            Some(ast::JoinConstraint::On(value)) => self.count_expr(value, scope),
                            _ => 0,
                        }
                })
                .sum::<usize>()
    }

    fn count_window(&self, window: &ast::Window, scope: &mut RecursiveRefScope) -> usize {
        let mut count = 0;
        count += window
            .partition_by
            .iter()
            .map(|value| self.count_expr(value, scope))
            .sum::<usize>();
        count += window
            .order_by
            .iter()
            .map(|term| self.count_expr(&term.expr, scope))
            .sum::<usize>();
        if let Some(frame) = &window.frame_clause {
            for bound in std::iter::once(&frame.start).chain(frame.end.as_ref()) {
                if let ast::FrameBound::Following(value) | ast::FrameBound::Preceding(value) = bound
                {
                    count += self.count_expr(value, scope);
                }
            }
        }
        count
    }

    fn count_expr(&self, expression: &ast::Expr, scope: &mut RecursiveRefScope) -> usize {
        let mut count = 0;
        walk_expr(expression, &mut |node| -> Result<WalkControl> {
            match node {
                ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
                    count += self.count_select(select, scope);
                    Ok(WalkControl::SkipChildren)
                }
                ast::Expr::InSelect { rhs, .. } => {
                    count += self.count_select(rhs, scope);
                    Ok(WalkControl::Continue)
                }
                _ => Ok(WalkControl::Continue),
            }
        })
        .expect("recursive reference visitor cannot fail");
        count
    }

    fn count_arm(&self, one: &ast::OneSelect, scope: &mut RecursiveRefScope) -> (usize, usize) {
        fn direct(
            counter: &RecursiveRefCounter<'_>,
            table: &ast::SelectTable,
            scope: &RecursiveRefScope,
        ) -> usize {
            match table {
                ast::SelectTable::Table(name, _, _) | ast::SelectTable::TableCall(name, _, _) => {
                    if name.db_name.is_some() {
                        return 0;
                    }
                    let name = crate::util::normalize_ident(name.name.as_str());
                    usize::from(
                        name == counter.cte_name
                            && !scope.iter().any(|(scope_name, _)| *scope_name == name),
                    )
                }
                ast::SelectTable::Select(_, _) => 0,
                ast::SelectTable::Sub(from, _) => {
                    direct(counter, &from.select, scope)
                        + from
                            .joins
                            .iter()
                            .map(|join| direct(counter, &join.table, scope))
                            .sum::<usize>()
                }
            }
        }

        let direct_count = if let ast::OneSelect::Select {
            from: Some(from), ..
        } = one
        {
            direct(self, &from.select, scope)
                + from
                    .joins
                    .iter()
                    .map(|join| direct(self, &join.table, scope))
                    .sum::<usize>()
        } else {
            0
        };
        (direct_count, self.count_one_select(one, scope))
    }
}

fn reject_recursive_query_functions(
    analyzer: &Analyzer<'_, '_, '_>,
    query: super::hir::QueryId,
) -> Result<()> {
    let query = analyzer
        .query(query)
        .ok_or_else(|| LimboError::InternalError(format!("missing recursive query {query}")))?;
    if query.blocks.iter().any(|block| {
        block.aggregate_count > 0
            || matches!(
                block.body,
                super::hir::QueryBlockBody::Select {
                    grouping: Some(_),
                    ..
                }
            )
    }) {
        crate::bail_parse_error!("recursive aggregate queries not supported");
    }
    if query
        .blocks
        .iter()
        .any(|block| block.window_function_count > 0)
    {
        crate::bail_parse_error!("cannot use window functions in recursive queries");
    }
    Ok(())
}

fn recursive_comparison_collations(
    analyzer: &Analyzer<'_, '_, '_>,
    seed: super::hir::QueryId,
    arms: &[RecursiveArm],
) -> Result<Vec<Option<super::hir::ResolvedCollation>>> {
    let seed = analyzer
        .query(seed)
        .ok_or_else(|| LimboError::InternalError(format!("missing recursive seed {seed}")))?;
    let mut collations = vec![None; seed.output.len()];
    let arm_queries = arms
        .iter()
        .map(|arm| {
            analyzer.query(arm.query).ok_or_else(|| {
                LimboError::InternalError(format!("missing recursive arm {}", arm.query))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    for query in std::iter::once(seed).chain(arm_queries) {
        for block in &query.blocks {
            for (collation, output) in collations.iter_mut().zip(&block.outputs) {
                if collation.is_none() {
                    *collation = output.collation.clone();
                }
            }
        }
    }
    Ok(collations)
}

struct PendingCte<'ast> {
    name: String,
    syntax: &'ast ast::CommonTableExpr,
    state: CteState,
}

enum CteState {
    Unbound,
    BindingSeed(Option<CteId>),
    BindingRecursive(RecursiveBinding),
    Bound(CteId),
    Failed(String),
}

struct RecursiveBinding {
    id: CteId,
    columns: Vec<CteColumn>,
    input_sources: Vec<super::hir::SourceId>,
}

pub(super) enum CteResolution {
    Cte(CteId),
    RecursiveInput { cte: CteId, columns: Vec<CteColumn> },
}

impl<'context, 'catalog, 'ast> Analyzer<'context, 'catalog, 'ast> {
    pub(super) fn with_cte_scope<T>(
        &mut self,
        with: Option<&'ast ast::With>,
        analyze: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let Some(with) = with else {
            return analyze(self);
        };

        let depth = self.cte_scopes.len();
        self.push_cte_scope(with)?;
        let result = analyze(self);
        assert_eq!(
            self.cte_scopes.len(),
            depth + 1,
            "nested analysis leaked a CTE scope"
        );
        self.cte_scopes.pop().expect("CTE scope was pushed");
        result
    }

    pub(super) fn push_cte_scope(&mut self, with: &'ast ast::With) -> Result<()> {
        let mut entries = Vec::with_capacity(with.ctes.len());
        for cte in &with.ctes {
            let name = crate::util::normalize_ident(cte.tbl_name.as_str());
            if entries
                .iter()
                .any(|entry: &PendingCte<'_>| entry.name == name)
            {
                crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
            }
            entries.push(PendingCte {
                name,
                syntax: cte,
                state: CteState::Unbound,
            });
        }
        self.cte_scopes.push(CteScope { entries });
        Ok(())
    }

    pub(super) fn resolve_cte(
        &mut self,
        name: &str,
        context: CteBindingContext<'_>,
    ) -> Result<Option<CteResolution>> {
        let name = crate::util::normalize_ident(name);
        let Some((scope, entry)) =
            self.cte_scopes
                .iter()
                .enumerate()
                .rev()
                .find_map(|(scope, entries)| {
                    entries
                        .entries
                        .iter()
                        .position(|entry| entry.name == name)
                        .map(|entry| (scope, entry))
                })
        else {
            return Ok(None);
        };

        match &self.cte_scopes[scope].entries[entry].state {
            CteState::Bound(id) => return Ok(Some(CteResolution::Cte(*id))),
            CteState::BindingSeed(_) => crate::bail_parse_error!("circular reference: {}", name),
            CteState::BindingRecursive(binding) => {
                return Ok(Some(CteResolution::RecursiveInput {
                    cte: binding.id,
                    columns: binding.columns.clone(),
                }));
            }
            CteState::Failed(message) => {
                return Err(LimboError::ParseError(message.clone()));
            }
            CteState::Unbound => {}
        }

        let syntax = self.cte_scopes[scope].entries[entry].syntax;
        let first_recursive_arm = match recursive_arm_start(syntax) {
            Ok(first_recursive_arm) => first_recursive_arm,
            Err(error) => {
                let message = match error {
                    LimboError::ParseError(message) => message,
                    other => other.to_string(),
                };
                self.cte_scopes[scope].entries[entry].state = CteState::Failed(message.clone());
                return Err(LimboError::ParseError(message));
            }
        };
        let recursive_id = first_recursive_arm.map(|_| self.reserve_cte());
        self.cte_scopes[scope].entries[entry].state = CteState::BindingSeed(recursive_id);
        let result = self.bind_cte(
            syntax,
            recursive_id,
            scope,
            entry,
            first_recursive_arm,
            context,
        );
        match result {
            Ok(id) => {
                self.cte_scopes[scope].entries[entry].state = CteState::Bound(id);
                Ok(Some(CteResolution::Cte(id)))
            }
            Err(error) => {
                let message = match error {
                    LimboError::ParseError(message) => message,
                    other => other.to_string(),
                };
                self.cte_scopes[scope].entries[entry].state = CteState::Failed(message.clone());
                Err(LimboError::ParseError(message))
            }
        }
    }

    fn bind_cte(
        &mut self,
        syntax: &'ast ast::CommonTableExpr,
        recursive_id: Option<CteId>,
        scope: usize,
        entry: usize,
        first_recursive_arm: Option<usize>,
        context: CteBindingContext<'_>,
    ) -> Result<CteId> {
        if let Some(first_recursive_arm) = first_recursive_arm {
            let id = recursive_id.expect("recursive CTE reserves its identity");
            return self.bind_recursive_cte(syntax, id, scope, entry, first_recursive_arm, context);
        }
        let query = self.analyze_cte_select(&syntax.select, context)?;
        let source_columns = self.query_source_columns(query)?;

        if !syntax.columns.is_empty() && syntax.columns.len() != source_columns.len() {
            crate::bail_parse_error!(
                "table {} has {} values for {} columns",
                syntax.tbl_name.as_str(),
                source_columns.len(),
                syntax.columns.len()
            );
        }

        let columns = source_columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| CteColumn {
                name: syntax
                    .columns
                    .get(index)
                    .map(|column| crate::util::normalize_ident(column.col_name.as_str()))
                    .unwrap_or(column.name),
                type_fact: column.type_fact,
                affinity: column.affinity,
                has_affinity: column.has_affinity,
                collation: column.collation,
            })
            .collect();
        // Allocate only referenced definitions. Closed HIR rejects arena
        // entries that cannot be reached from the statement root.
        let id = self.reserve_cte();
        self.insert_cte(
            id,
            Cte {
                id,
                name: crate::util::normalize_ident(syntax.tbl_name.as_str()),
                columns,
                materialized: syntax.materialized.clone(),
                body: CteBody::Query(query),
            },
        )?;
        Ok(id)
    }

    fn bind_recursive_cte(
        &mut self,
        syntax: &'ast ast::CommonTableExpr,
        id: CteId,
        scope: usize,
        entry: usize,
        first_recursive_arm: usize,
        context: CteBindingContext<'_>,
    ) -> Result<CteId> {
        let seed_compounds = &syntax.select.body.compounds[..first_recursive_arm - 1];
        let seed = self.analyze_cte_query_parts(
            &syntax.select,
            &syntax.select.body.select,
            seed_compounds,
            context,
        )?;
        let source_columns = self.query_source_columns(seed)?;
        if !syntax.columns.is_empty() && syntax.columns.len() != source_columns.len() {
            crate::bail_parse_error!(
                "table {} has {} values for {} columns",
                syntax.tbl_name.as_str(),
                source_columns.len(),
                syntax.columns.len()
            );
        }
        let columns = source_columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| CteColumn {
                name: syntax
                    .columns
                    .get(index)
                    .map(|column| crate::util::normalize_ident(column.col_name.as_str()))
                    .unwrap_or(column.name),
                type_fact: column.type_fact,
                affinity: column.affinity,
                has_affinity: column.has_affinity,
                collation: column.collation,
            })
            .collect::<Vec<_>>();

        match &self.cte_scopes[scope].entries[entry].state {
            CteState::BindingSeed(Some(binding_id)) if *binding_id == id => {}
            _ => {
                return Err(LimboError::InternalError(format!(
                    "recursive CTE {id} left seed-binding state"
                )));
            }
        }
        self.cte_scopes[scope].entries[entry].state =
            CteState::BindingRecursive(RecursiveBinding {
                id,
                columns: columns.clone(),
                input_sources: Vec::new(),
            });

        let mut arms = Vec::new();
        for compound in &syntax.select.body.compounds[first_recursive_arm - 1..] {
            let query =
                self.analyze_cte_query_parts(&syntax.select, &compound.select, &[], context)?;
            reject_recursive_query_functions(self, query)?;
            let width = self.query_source_columns(query)?.len();
            if width != columns.len() {
                crate::bail_parse_error!(
                    "SELECTs to the left and right of {} do not have the same number of result columns",
                    compound.operator
                );
            }
            arms.push(RecursiveArm {
                operator: compound.operator,
                query,
            });
        }

        let input_sources = match &mut self.cte_scopes[scope].entries[entry].state {
            CteState::BindingRecursive(binding) if binding.id == id => {
                std::mem::take(&mut binding.input_sources)
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "recursive CTE {id} left recursive-binding state"
                )));
            }
        };
        let comparison_collations = recursive_comparison_collations(self, seed, &arms)?;
        let queue_order = self.analyze_recursive_order_by(&syntax.select.order_by, seed, &arms)?;
        let limit = syntax
            .select
            .limit
            .as_ref()
            .map(|limit| self.analyze_limit(limit, seed, context.policies()))
            .transpose()?;
        self.insert_cte(
            id,
            Cte {
                id,
                name: crate::util::normalize_ident(syntax.tbl_name.as_str()),
                columns,
                materialized: syntax.materialized.clone(),
                body: CteBody::Recursive(RecursiveCte {
                    seed,
                    arms,
                    input_sources,
                    comparison_collations,
                    queue_order,
                    limit,
                }),
            },
        )?;
        Ok(id)
    }

    fn analyze_recursive_order_by(
        &mut self,
        syntax: &'ast [ast::SortedColumn],
        seed: super::hir::QueryId,
        arms: &[RecursiveArm],
    ) -> Result<Vec<RecursiveOrderTerm>> {
        let positions = {
            let seed = self.query(seed).ok_or_else(|| {
                LimboError::InternalError(format!("missing recursive seed {seed}"))
            })?;
            let arm_queries = arms
                .iter()
                .map(|arm| {
                    self.query(arm.query).ok_or_else(|| {
                        LimboError::InternalError(format!("missing recursive arm {}", arm.query))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let output_arms = std::iter::once(seed)
                .chain(arm_queries)
                .flat_map(|query| query.blocks.iter().map(|block| block.outputs.as_slice()))
                .collect::<Vec<_>>();
            syntax
                .iter()
                .enumerate()
                .map(|(index, term)| {
                    super::analyze::compound_order_by_position(&term.expr, &output_arms, index + 1)
                })
                .collect::<Result<Vec<_>>>()?
        };

        syntax
            .iter()
            .zip(positions)
            .map(|(syntax, (output, collations))| {
                let mut explicit_collation = None;
                for name in collations.iter().rev() {
                    explicit_collation = Some(self.resolve_collation(name)?);
                }
                Ok(RecursiveOrderTerm {
                    output,
                    order: syntax.order.unwrap_or(ast::SortOrder::Asc),
                    nulls: syntax.nulls,
                    explicit_collation,
                })
            })
            .collect()
    }

    fn analyze_cte_query_parts(
        &mut self,
        whole: &'ast ast::Select,
        first: &'ast ast::OneSelect,
        compounds: &'ast [ast::CompoundSelect],
        context: CteBindingContext<'_>,
    ) -> Result<super::hir::QueryId> {
        self.with_cte_scope(whole.with.as_ref(), |analyzer| {
            analyzer.analyze_select_parts(
                first,
                compounds,
                &[],
                None,
                super::analyze::SelectContext {
                    parent: context.query_parent(),
                    outer_scope: context.outer_scope(),
                    expected_outputs: None,
                    policies: context.policies(),
                },
            )
        })
    }

    fn analyze_cte_select(
        &mut self,
        select: &'ast ast::Select,
        context: CteBindingContext<'_>,
    ) -> Result<QueryId> {
        self.analyze_select_with_scope(
            select,
            context.query_parent(),
            context.outer_scope(),
            None,
            context.policies(),
        )
    }

    pub(super) fn record_recursive_input(
        &mut self,
        cte: CteId,
        source: super::hir::SourceId,
    ) -> Result<()> {
        for scope in self.cte_scopes.iter_mut().rev() {
            for entry in &mut scope.entries {
                if let CteState::BindingRecursive(binding) = &mut entry.state {
                    if binding.id == cte {
                        binding.input_sources.push(source);
                        return Ok(());
                    }
                }
            }
        }
        Err(LimboError::InternalError(format!(
            "recursive input {source} has no active CTE {cte}"
        )))
    }
}
