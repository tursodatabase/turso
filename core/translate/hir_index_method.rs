//! Matching a query block against the query patterns of an index method,
//! such as FTS, so the index answers the query itself.
//!
//! An index method lists the queries it can answer as SQL patterns, for
//! example `SELECT fts_score(title, body, ?1) AS score FROM docs WHERE
//! fts_match(title, body, ?1) ORDER BY score DESC LIMIT ?`. A pattern matches
//! when the query's WHERE term, ORDER BY and LIMIT have the same shape, with
//! each `?` standing for any expression of the query. Those expressions are
//! handed to the index when the scan starts, and the pattern's result columns
//! are read from the index instead of being computed.

use crate::schema::Index;
use crate::sync::Arc;
use crate::translate::plan::HirWhereTerm;
use crate::translate::semantic::hir::{self, HirDocument};
use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, SortOrder};

/// A scan of a table through one query pattern of an index method.
#[derive(Clone, Debug)]
pub(crate) struct HirIndexMethodAccess {
    pub(crate) index: Arc<Index>,
    /// Which of the index method's query patterns matched.
    pub(crate) pattern: usize,
    /// The expressions the pattern's placeholders stand for, in placeholder
    /// order.
    pub(crate) arguments: Vec<hir::Expr>,
    /// Query expressions the index returns, each with the column of the
    /// index cursor that holds it.
    pub(crate) covered: Vec<(hir::Expr, usize)>,
}

/// What an index method takes over from a query block.
pub(crate) struct IndexMethodMatch {
    pub(crate) access: HirIndexMethodAccess,
    /// The WHERE term the index applies, by position in the predicates.
    pub(crate) predicate: Option<usize>,
    /// The index returns rows in ORDER BY order.
    pub(crate) applies_order_by: bool,
    /// The index applies LIMIT and OFFSET.
    pub(crate) applies_limit: bool,
}

/// The query block parts a pattern is matched against.
pub(crate) struct IndexMethodQuery<'a> {
    pub(crate) document: &'a HirDocument,
    pub(crate) source: &'a hir::Source,
    pub(crate) outputs: &'a [hir::Output],
    pub(crate) predicates: &'a [HirWhereTerm],
    /// Position of the first WHERE term in `predicates`.
    pub(crate) where_start: usize,
    pub(crate) order_by: &'a [hir::OrderTerm],
    pub(crate) limit: Option<&'a hir::Limit>,
    /// Whether the index may take over ORDER BY and LIMIT: false when rows
    /// are grouped, aggregated, made DISTINCT or windowed after the scan.
    pub(crate) rows_are_output: bool,
}

/// The first query pattern of an index on the source's table that matches
/// the query, if any.
pub(crate) fn match_index_methods(query: &IndexMethodQuery<'_>) -> Option<IndexMethodMatch> {
    if !matches!(query.source.index_hint, hir::IndexHint::None) {
        return None;
    }
    let hir::SourceKind::Table(table) = &query.source.kind else {
        return None;
    };
    let table_name = table.value().get_name();
    query
        .source
        .index_expressions
        .iter()
        .map(|expressions| expressions.index.handle())
        .filter(|index| {
            index
                .index_method
                .as_ref()
                .is_some_and(|method| !method.definition().backing_btree)
        })
        .find_map(|index| {
            let method = index.index_method.as_ref()?;
            method
                .definition()
                .patterns
                .iter()
                .enumerate()
                .find_map(|(position, pattern)| {
                    match_pattern(query, table_name, &index, position, pattern)
                })
        })
}

fn match_pattern(
    query: &IndexMethodQuery<'_>,
    table_name: &str,
    index: &Arc<Index>,
    position: usize,
    pattern: &ast::Select,
) -> Option<IndexMethodMatch> {
    if pattern.with.is_some() || !pattern.body.compounds.is_empty() {
        return None;
    }
    let ast::OneSelect::Select {
        columns,
        from: Some(ast::FromClause { select, joins }),
        distinctness: None,
        where_clause,
        group_by: None,
        window_clause,
    } = &pattern.body.select
    else {
        return None;
    };
    if !joins.is_empty() || !window_clause.is_empty() {
        return None;
    }
    let ast::SelectTable::Table(name, _, _) = select.as_ref() else {
        return None;
    };
    if !crate::util::normalize_ident(name.name.as_str()).eq_ignore_ascii_case(table_name) {
        return None;
    }
    let applies_order_by = !pattern.order_by.is_empty();
    let applies_limit = pattern.limit.is_some();
    if (applies_order_by || applies_limit) && !query.rows_are_output {
        return None;
    }

    let mut matcher = Matcher {
        document: query.document,
        source: query.source,
        captures: HashMap::default(),
    };
    if applies_order_by {
        if pattern.order_by.len() != query.order_by.len() {
            return None;
        }
        for (pattern_term, query_term) in pattern.order_by.iter().zip(query.order_by) {
            if query_term.order != pattern_term.order.unwrap_or(SortOrder::Asc)
                || query_term.nulls.is_some()
                || pattern_term.nulls.is_some()
            {
                return None;
            }
            let pattern_expr = resolve_alias(columns, &pattern_term.expr);
            if !matcher.capture(pattern_expr, &query_term.expr) {
                return None;
            }
        }
    }
    if let Some(pattern_limit) = &pattern.limit {
        let limit = query.limit?;
        if !matcher.capture(&pattern_limit.expr, &limit.limit) {
            return None;
        }
        // The index only skips rows the pattern asks it to.
        match (&pattern_limit.offset, &limit.offset) {
            (Some(pattern_offset), Some(offset)) => {
                if !matcher.capture(pattern_offset, offset) {
                    return None;
                }
            }
            (None, None) => {}
            _ => return None,
        }
    }

    let where_terms = query
        .predicates
        .iter()
        .enumerate()
        .skip(query.where_start)
        .filter(|(_, term)| !term.consumed)
        .collect::<Vec<_>>();
    let predicate = match where_clause {
        Some(pattern_where) => Some(where_terms.iter().find_map(|(position, term)| {
            if term.from_outer_join.is_some() {
                return None;
            }
            let mut attempt = matcher.clone();
            attempt.capture(pattern_where, &term.expr).then(|| {
                matcher = attempt;
                *position
            })
        })?),
        None => None,
    };
    // The index cannot order or limit rows that a WHERE term it does not
    // apply still filters.
    let where_covered = where_terms.is_empty() || (predicate.is_some() && where_terms.len() == 1);
    if (applies_order_by || applies_limit) && !where_covered {
        return None;
    }
    // A predicate that reads another table is not one the index can apply.
    if query.predicates[..query.where_start]
        .iter()
        .any(|term| !term.consumed)
    {
        return None;
    }

    let mut numbers = matcher.captures.keys().copied().collect::<Vec<_>>();
    numbers.sort_unstable();
    let arguments = numbers
        .iter()
        .map(|number| matcher.captures[number].clone())
        .collect::<Vec<_>>();

    // Every query expression a result column of the pattern stands for is
    // read from the index.
    let mut covered: Vec<(hir::Expr, usize)> = Vec::new();
    for (column, result_column) in columns.iter().enumerate() {
        let ast::ResultColumn::Expr(pattern_expr, _) = result_column else {
            continue;
        };
        let mut visit = |expression: &hir::Expr| {
            if covered
                .iter()
                .any(|(existing, _)| existing.equivalent(expression))
            {
                return;
            }
            let mut attempt = matcher.clone();
            if attempt.capture(pattern_expr, expression)
                && attempt.captures.len() == matcher.captures.len()
            {
                covered.push((expression.clone(), column));
            }
        };
        for output in query.outputs {
            output.expr.for_each(&mut visit);
        }
        for term in query.order_by {
            term.expr.for_each(&mut visit);
        }
        for (position, term) in &where_terms {
            if Some(*position) != predicate {
                term.expr.for_each(&mut visit);
            }
        }
    }

    Some(IndexMethodMatch {
        access: HirIndexMethodAccess {
            index: index.clone(),
            pattern: position,
            arguments,
            covered,
        },
        predicate,
        applies_order_by,
        applies_limit,
    })
}

/// The result column a pattern's ORDER BY names by its alias, or the term
/// itself.
fn resolve_alias<'a>(columns: &'a [ast::ResultColumn], expr: &'a ast::Expr) -> &'a ast::Expr {
    let (ast::Expr::Id(name) | ast::Expr::Name(name)) = expr else {
        return expr;
    };
    columns
        .iter()
        .find_map(|column| match column {
            ast::ResultColumn::Expr(
                column_expr,
                Some(ast::As::As(alias) | ast::As::Elided(alias)),
            ) if alias.as_str().eq_ignore_ascii_case(name.as_str()) => Some(column_expr.as_ref()),
            _ => None,
        })
        .unwrap_or(expr)
}

/// Compares pattern expressions with query expressions, remembering what
/// each placeholder stands for.
#[derive(Clone)]
struct Matcher<'a> {
    document: &'a HirDocument,
    source: &'a hir::Source,
    captures: HashMap<u32, &'a hir::Expr>,
}

impl<'a> Matcher<'a> {
    /// An ORDER BY term that names a result column stands for its expression.
    fn resolve(&self, expr: &'a hir::Expr) -> &'a hir::Expr {
        match expr {
            hir::Expr::Output(id) => self
                .document
                .output(*id)
                .map_or(expr, |output| self.resolve(&output.expr)),
            _ => expr,
        }
    }

    fn capture(&mut self, pattern: &ast::Expr, query: &'a hir::Expr) -> bool {
        let query = self.resolve(query);
        match pattern {
            ast::Expr::Variable(variable) => {
                if !variable.is_positional() {
                    return false;
                }
                let number = variable.index.get();
                match self.captures.get(&number) {
                    // A placeholder used twice stands for one expression.
                    Some(previous) => previous.equivalent(query),
                    None => {
                        self.captures.insert(number, query);
                        true
                    }
                }
            }
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                let hir::Expr::Column(column) = query else {
                    return false;
                };
                column.source == self.source.id
                    && self
                        .source
                        .columns
                        .get(column.column)
                        .is_some_and(|column| {
                            column
                                .name
                                .eq_ignore_ascii_case(&crate::util::normalize_ident(name.as_str()))
                        })
            }
            ast::Expr::FunctionCall {
                name,
                distinctness: None,
                args,
                order_by,
                within_group,
                filter_over,
            } if order_by.is_empty()
                && within_group.is_empty()
                && filter_over.filter_clause.is_none()
                && filter_over.over_clause.is_none() =>
            {
                let hir::Expr::Function(call) = query else {
                    return false;
                };
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by: call_order_by,
                    ..
                } = &call.arguments
                else {
                    return false;
                };
                if !matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    || !call_order_by.is_empty()
                    || values.len() != args.len()
                    || !call
                        .function
                        .value()
                        .to_string()
                        .eq_ignore_ascii_case(name.as_str())
                {
                    return false;
                }
                let columns = fts_column_arguments(name.as_str(), args.len());
                // FTS functions accept their columns in any order.
                let mut used = vec![false; columns];
                for value in &values[..columns] {
                    let Some(position) = (0..columns).find(|&position| {
                        !used[position] && {
                            let mut attempt = self.clone();
                            attempt.capture(&args[position], value) && {
                                *self = attempt;
                                true
                            }
                        }
                    }) else {
                        return false;
                    };
                    used[position] = true;
                }
                args[columns..]
                    .iter()
                    .zip(&values[columns..])
                    .all(|(arg, value)| self.capture(arg, value))
            }
            _ => false,
        }
    }
}

/// How many leading arguments of an FTS function name columns, which may
/// come in any order; 0 for other functions.
fn fts_column_arguments(name: &str, arguments: usize) -> usize {
    if name.eq_ignore_ascii_case("fts_match") || name.eq_ignore_ascii_case("fts_score") {
        arguments.saturating_sub(1)
    } else if name.eq_ignore_ascii_case("fts_highlight") {
        arguments.saturating_sub(3)
    } else {
        0
    }
}
