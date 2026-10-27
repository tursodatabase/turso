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
    pub(crate) arguments: Vec<hir::ExprId>,
    /// Query expressions the index returns, each with the column of the
    /// index cursor that holds it.
    pub(crate) covered: Vec<(hir::ExprId, usize)>,
    /// Which of the pattern's FTS columns the query names, as a
    /// comma-separated list of positions ("0,2"); None for other methods.
    /// Passed to the index after the placeholder values.
    pub(crate) fields: Option<String>,
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
/// the query, if any. A source with an INDEXED BY or NOT INDEXED hint keeps
/// its hinted access.
pub(crate) fn match_index_methods(query: &IndexMethodQuery<'_>) -> Option<IndexMethodMatch> {
    if !matches!(query.source.index_hint, hir::IndexHint::None) {
        return None;
    }
    match_index_methods_ignoring_hint(query)
}

fn match_index_methods_ignoring_hint(query: &IndexMethodQuery<'_>) -> Option<IndexMethodMatch> {
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

/// A query pattern that applies one WHERE or ON term of `source` to a row
/// the source's own loop already found: the arguments of the term may read
/// that row, so the index cannot drive the loop, but it can say whether the
/// row is among the term's matches. Only patterns that neither sort nor
/// limit rows qualify, and the source's index hint does not matter, since
/// the hinted access still reads the rows.
pub(crate) fn match_index_method_predicate(
    document: &HirDocument,
    source: &hir::Source,
    term: &HirWhereTerm,
) -> Option<HirIndexMethodAccess> {
    let term = HirWhereTerm {
        expr: term.expr,
        from_outer_join: None,
        consumed: false,
    };
    let matched = match_index_methods_ignoring_hint(&IndexMethodQuery {
        document,
        source,
        outputs: &[],
        predicates: std::slice::from_ref(&term),
        where_start: 0,
        order_by: &[],
        limit: None,
        rows_are_output: false,
    })?;
    matched.predicate.is_some().then_some(matched.access)
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
        fields: None,
    };
    if (applies_order_by || applies_limit) && pattern.order_by.len() != query.order_by.len() {
        return None;
    }
    if applies_order_by {
        for (pattern_term, query_term) in pattern.order_by.iter().zip(query.order_by) {
            if query_term.order != pattern_term.order.unwrap_or(SortOrder::Asc)
                || query_term.nulls.is_some()
                || pattern_term.nulls.is_some()
            {
                return None;
            }
            let pattern_expr = resolve_alias(columns, &pattern_term.expr);
            if !matcher.capture(pattern_expr, query_term.expr) {
                return None;
            }
        }
    }
    if let Some(pattern_limit) = &pattern.limit {
        let limit = query.limit?;
        if !matcher.capture(&pattern_limit.expr, limit.limit) {
            return None;
        }
        // The index only skips rows the pattern asks it to.
        match (&pattern_limit.offset, &limit.offset) {
            (Some(pattern_offset), Some(offset)) => {
                if !matcher.capture(pattern_offset, *offset) {
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
            attempt.capture(pattern_where, term.expr).then(|| {
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
    let mut numbers = matcher.captures.keys().copied().collect::<Vec<_>>();
    numbers.sort_unstable();
    let arguments = numbers
        .iter()
        .map(|number| matcher.captures[number])
        .collect::<Vec<_>>();

    // Every query expression a result column of the pattern stands for is
    // read from the index.
    let exprs = &query.document.exprs;
    let mut covered: Vec<(hir::ExprId, usize)> = Vec::new();
    for (column, result_column) in columns.iter().enumerate() {
        let ast::ResultColumn::Expr(pattern_expr, _) = result_column else {
            continue;
        };
        let mut visit = |expression: hir::ExprRef<'_>| {
            if covered
                .iter()
                .any(|(existing, _)| exprs.at(*existing).equivalent(expression))
            {
                return;
            }
            let mut attempt = matcher.clone();
            if attempt.capture(pattern_expr, expression.id())
                && attempt.captures.len() == matcher.captures.len()
            {
                covered.push((expression.id(), column));
            }
        };
        for output in query.outputs {
            exprs.at(output.expr).for_each(&mut visit);
        }
        for term in query.order_by {
            exprs.at(term.expr).for_each(&mut visit);
        }
        for (position, term) in &where_terms {
            if Some(*position) != predicate {
                exprs.at(term.expr).for_each(&mut visit);
            }
        }
    }

    // The index's score for a row is the score of the search that found
    // it: a `fts_score` over the same columns and query reads it from the
    // index even when the pattern returns no column for it.
    if let Some(fts_arguments) = fts_search_arguments(pattern) {
        let score_column = fts_score_column(position);
        let mut visit = |expression: hir::ExprRef<'_>| {
            if covered
                .iter()
                .any(|(existing, _)| exprs.at(*existing).equivalent(expression))
            {
                return;
            }
            let hir::Expr::Function(call) = expression.node() else {
                return;
            };
            if !call
                .function
                .value()
                .to_string()
                .eq_ignore_ascii_case("fts_score")
            {
                return;
            }
            let mut attempt = matcher.clone();
            if attempt.capture_call("fts_score", fts_arguments, expression.id())
                && attempt.captures.len() == matcher.captures.len()
            {
                covered.push((expression.id(), score_column));
            }
        };
        for output in query.outputs {
            exprs.at(output.expr).for_each(&mut visit);
        }
        for term in query.order_by {
            exprs.at(term.expr).for_each(&mut visit);
        }
        for (position, term) in &where_terms {
            if Some(*position) != predicate {
                exprs.at(term.expr).for_each(&mut visit);
            }
        }
    }

    let fields = matcher.fields.as_ref().map(|fields| {
        fields
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    });

    Some(IndexMethodMatch {
        access: HirIndexMethodAccess {
            index: index.clone(),
            pattern: position,
            arguments,
            covered,
            fields,
        },
        predicate,
        applies_order_by,
        applies_limit,
    })
}

/// The arguments of the FTS search an FTS pattern runs, its columns then
/// the query placeholder: those of its `fts_match` WHERE term, else of its
/// `fts_score` result column. None for a pattern of another method.
fn fts_search_arguments(pattern: &ast::Select) -> Option<&[Box<ast::Expr>]> {
    let ast::OneSelect::Select {
        columns,
        where_clause,
        ..
    } = &pattern.body.select
    else {
        return None;
    };
    fn fts_call<'p>(expression: &'p ast::Expr, function: &str) -> Option<&'p [Box<ast::Expr>]> {
        match expression {
            ast::Expr::FunctionCall { name, args, .. }
                if name.as_str().eq_ignore_ascii_case(function) =>
            {
                Some(args.as_slice())
            }
            _ => None,
        }
    }
    where_clause
        .as_deref()
        .and_then(|where_clause| fts_call(where_clause, "fts_match"))
        .or_else(|| {
            columns.iter().find_map(|column| match column {
                ast::ResultColumn::Expr(expression, _) => fts_call(expression, "fts_score"),
                _ => None,
            })
        })
}

/// The column of an FTS index cursor that holds the row's score: the
/// second for the `fts_match`-only patterns, whose first column says the
/// row matched, else the first.
fn fts_score_column(pattern: usize) -> usize {
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    {
        usize::from(matches!(
            pattern as i64,
            crate::index_method::fts::FTS_PATTERN_MATCH
                | crate::index_method::fts::FTS_PATTERN_MATCH_LIMIT
        ))
    }
    #[cfg(not(all(feature = "fts", not(target_family = "wasm"))))]
    {
        let _ = pattern;
        0
    }
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
    captures: HashMap<u32, hir::ExprId>,
    /// The pattern's FTS column positions the query names, sorted. Every
    /// FTS function of one query names the same columns.
    fields: Option<Vec<usize>>,
}

impl<'a> Matcher<'a> {
    /// An ORDER BY term that names a result column stands for its expression.
    fn resolve(&self, expr: hir::ExprId) -> hir::ExprId {
        match &self.document.exprs[expr] {
            hir::Expr::Output(id) => self
                .document
                .output(*id)
                .map_or(expr, |output| self.resolve(output.expr)),
            _ => expr,
        }
    }

    fn capture(&mut self, pattern: &ast::Expr, query: hir::ExprId) -> bool {
        let query = self.resolve(query);
        let exprs = &self.document.exprs;
        match pattern {
            ast::Expr::Variable(variable) => {
                if !variable.is_positional() {
                    return false;
                }
                let number = variable.index.get();
                match self.captures.get(&number) {
                    // A placeholder used twice stands for one expression.
                    Some(previous) => exprs.at(*previous).equivalent(exprs.at(query)),
                    None => {
                        self.captures.insert(number, query);
                        true
                    }
                }
            }
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                let hir::Expr::Column(column) = &exprs[query] else {
                    return false;
                };
                column.source == self.source.id
                    && self
                        .source
                        .columns
                        .get(column.column)
                        .is_some_and(|column| {
                            column
                                .name()
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
                self.capture_call(name.as_str(), args, query)
            }
            _ => false,
        }
    }

    /// Whether `query` is a call of the function `name` with the pattern
    /// arguments `args`. An FTS function names any non-empty subset of the
    /// pattern's columns, in any order, and the same subset in every call
    /// of one query; the arguments after the columns are positional.
    /// `x MATCH y` and `(x, ...) MATCH y` are calls of `fts_match`.
    fn capture_call(&mut self, name: &str, args: &[Box<ast::Expr>], query: hir::ExprId) -> bool {
        let query = self.resolve(query);
        let exprs = &self.document.exprs;
        let mut row_values: Vec<hir::ExprId> = Vec::new();
        let values: &[hir::ExprId] = match &exprs[query] {
            hir::Expr::Function(call) => {
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
                    || !call.function.value().to_string().eq_ignore_ascii_case(name)
                {
                    return false;
                }
                row_values.extend(values.iter());
                &row_values
            }
            hir::Expr::Like {
                lhs,
                negated: false,
                operator: ast::LikeOperator::Match,
                rhs,
                escape: None,
                ..
            } if name.eq_ignore_ascii_case("fts_match") => {
                match &exprs[*lhs] {
                    hir::Expr::Row(items) => row_values.extend(items.iter()),
                    _ => row_values.push(*lhs),
                }
                row_values.push(*rhs);
                &row_values
            }
            _ => return false,
        };
        let columns = fts_column_arguments(name, args.len());
        if columns == 0 {
            return values.len() == args.len()
                && args
                    .iter()
                    .zip(values)
                    .all(|(arg, value)| self.capture(arg, *value));
        }
        let rest = args.len() - columns;
        let Some(query_columns) = values.len().checked_sub(rest) else {
            return false;
        };
        if query_columns == 0 || query_columns > columns {
            return false;
        }
        let mut used = vec![false; columns];
        for value in &values[..query_columns] {
            let Some(position) = (0..columns).find(|&position| {
                !used[position] && {
                    let mut attempt = self.clone();
                    attempt.capture(&args[position], *value) && {
                        *self = attempt;
                        true
                    }
                }
            }) else {
                return false;
            };
            used[position] = true;
        }
        let fields = (0..columns).filter(|&position| used[position]).collect();
        match &self.fields {
            Some(previous) if *previous != fields => return false,
            Some(_) => {}
            None => self.fields = Some(fields),
        }
        args[columns..]
            .iter()
            .zip(&values[query_columns..])
            .all(|(arg, value)| self.capture(arg, *value))
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
