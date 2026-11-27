use crate::ast::{self, Expr, Literal, UnaryOperator};
use crate::dialect::Dialect;
use crate::namespace::NamespaceId;
use crate::numeric::StrToF64;
use crate::schema::CheckConstraint;
use crate::security::policy::RowPolicy;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{walk_expr, walk_expr_mut, WalkControl};
use crate::translate::plan::{BitSet, JoinedTable};
use crate::translate::planner::parse_row_id;
use crate::types::IOResult;
use crate::types::IOResultOr;
use crate::IO;
use crate::{
    schema::{Column, Index, Schema, Table, Trigger, Type},
    types::{Value, ValueType},
    LimboError, OpenFlags, Result,
};
use either::Either;
use rustc_hash::FxHashMap as HashMap;
use std::future::Future;
use turso_macros::match_ignore_ascii_case;

#[macro_export]
macro_rules! io_yield_one {
    ($c:expr) => {
        return Ok(IOResult::IO(IOCompletions($c)));
    };
}

#[macro_export]
macro_rules! eq_ignore_ascii_case {
    ( $var:expr, $value:literal ) => {{
        match_ignore_ascii_case!(match $var {
            $value => true,
            _ => false,
        })
    }};
}

#[macro_export]
macro_rules! contains_ignore_ascii_case {
    ( $var:expr, $value:literal ) => {{
        let compare_to_idx = $var.len().saturating_sub($value.len());
        if $var.len() < $value.len() {
            false
        } else {
            let mut result = false;
            for i in 0..=compare_to_idx {
                if eq_ignore_ascii_case!(&$var[i..i + $value.len()], $value) {
                    result = true;
                    break;
                }
            }

            result
        }
    }};
}

#[macro_export]
macro_rules! starts_with_ignore_ascii_case {
    ( $var:expr, $value:literal ) => {{
        if $var.len() < $value.len() {
            false
        } else {
            eq_ignore_ascii_case!(&$var[..$value.len()], $value)
        }
    }};
}

#[macro_export]
macro_rules! ends_with_ignore_ascii_case {
    ( $var:expr, $value:literal ) => {{
        if $var.len() < $value.len() {
            false
        } else {
            eq_ignore_ascii_case!(&$var[$var.len() - $value.len()..], $value)
        }
    }};
}

pub trait IOExt {
    fn block<T>(&self, f: impl FnMut() -> IOResultOr<T>) -> Result<T>;
    fn wait<T, F>(&self, f: F) -> impl Future<Output = Result<T>> + Send
    where
        F: FnMut() -> IOResultOr<T> + Send,
        T: Send;
}

impl<I: ?Sized + IO> IOExt for I {
    fn block<T>(&self, mut f: impl FnMut() -> IOResultOr<T>) -> Result<T> {
        Ok(loop {
            match f()? {
                IOResult::Done(v) => break v,
                IOResult::IO(io) => io.wait(self)?,
            }
        })
    }

    async fn wait<T, F>(&self, mut f: F) -> Result<T>
    where
        F: FnMut() -> IOResultOr<T> + Send,
        T: Send,
    {
        Ok(loop {
            match f()? {
                IOResult::Done(v) => break v,
                IOResult::IO(io) => io.wait_async(self).await?,
            }
        })
    }
}

// https://sqlite.org/lang_keywords.html
const QUOTE_PAIRS: &[(char, char)] = &[
    ('"', '"'),
    ('[', ']'),
    ('`', '`'),
    ('\'', '\''), // string sometimes used as identifier quoting
];

pub fn normalize_ident(identifier: &str) -> String {
    // quotes normalization already happened in the parser layer (see Name ast node implementation)
    // so, we only need to apply SQLite's ASCII-only identifier case folding.
    identifier.to_ascii_lowercase()
}

/// Escape a SQL string literal payload for safe interpolation inside single quotes.
pub fn escape_sql_string_literal(literal: &str) -> String {
    literal.replace('\'', "''")
}

/// Format an identifier for error messages the way SQLite's `%T` does: the
/// token exactly as the user wrote it, keeping the original quote characters.
/// `CREATE TABLE "t"` reports an existing table as `table "t" already exists`.
pub fn identifier_token_for_error(name: &crate::ast::Name) -> String {
    if name.quoted() {
        name.as_ident()
    } else {
        name.as_str().to_owned()
    }
}

/// Returns a column's declared type from its type name the way SQLite's
/// `sqlite3AddColumn` does: a name that starts with a quote and has no other
/// quote characters before its last character loses its first and last
/// characters, so `[x] INT` becomes `x] IN`. Other names go through [dequote].
pub fn column_declared_type(type_name: &str) -> String {
    match strip_type_name_quotes(type_name) {
        Some(stripped) => stripped.to_string(),
        None => dequote(type_name),
    }
}

pub fn strip_type_name_quotes(type_name: &str) -> Option<&str> {
    let is_quote = |b: &u8| matches!(b, b'"' | b'\'' | b'[' | b'`');
    let bytes = type_name.as_bytes();
    if bytes.len() >= 3 && is_quote(&bytes[0]) && !bytes[1..bytes.len() - 1].iter().any(is_quote) {
        let mut chars = type_name.chars();
        chars.next();
        chars.next_back();
        return Some(chars.as_str());
    }
    None
}

/// Returns true when a STRICT table accepts the written type name without a
/// custom type. Like SQLite, quotes are only removed when they wrap the whole
/// name, so `"INT"` is built in but `"INT" X` is not.
pub fn is_builtin_strict_type(written_type_name: &str) -> bool {
    crate::types::StrictType::named(written_type_name).is_some()
}

/// Removes the quotes around the start of `source` the way SQLite's
/// `sqlite3Dequote` does: when it starts with a quote character, only the
/// first quoted token survives, so `"x" < +5` becomes just `x`.
pub fn dequote(source: &str) -> String {
    let bytes = source.as_bytes();
    let close = match bytes.first() {
        Some(&c @ (b'"' | b'\'' | b'`')) => c,
        Some(b'[') => b']',
        _ => return source.to_string(),
    };
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == close {
            // A doubled quote is an escaped quote, not the end.
            if close != b']' && bytes.get(i + 1) == Some(&close) {
                i += 2;
                continue;
            }
            break;
        }
        i += 1;
    }
    let inner = &source[1..i.min(source.len())];
    if close == b']' {
        inner.to_string()
    } else {
        let quote = (close as char).to_string();
        inner.replace(&format!("{quote}{quote}"), &quote)
    }
}

/// Format a table reference for error messages the way SQLite does: the name
/// as the user wrote it with quotes stripped, keeping any database prefix.
/// `SELECT * FROM main."T1"` reports the missing table as `main.T1`.
pub fn table_name_for_error(name: &crate::ast::QualifiedName) -> String {
    match &name.namespace {
        Some(db) => format!("{}.{}", db.as_str(), name.name.as_str()),
        None => name.name.as_str().to_owned(),
    }
}

/// Quote a SQL identifier with double quotes when necessary.
/// Always safe to call — returns the bare name when no quoting is needed.
pub fn quote_identifier(name: &str) -> String {
    ast::Name::exact(name.to_string()).as_ident()
}

pub const PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX: &str = "sqlite_autoindex_";

/// The name of automatic index `index_name` of table `old_table` once the
/// table is named `new_table`, the tables by the names their schema rows
/// have; None when the index is not an automatic index of `old_table`.
pub fn renamed_automatic_index_name(
    index_name: &str,
    old_table: &str,
    new_table: &str,
) -> Option<String> {
    let ordinal = index_name
        .strip_prefix(PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX)?
        .strip_prefix(old_table)?
        .strip_prefix('_')?;
    Some(format!(
        "{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{new_table}_{ordinal}"
    ))
}

/// Unparsed index that comes from a sql query, i.e not an automatic index
///
/// CREATE INDEX idx ON table_name(sql)
pub struct UnparsedFromSqlIndex {
    pub table_name: String,
    pub root_page: i64,
    pub sql: String,
}

fn cmp_numeric_strings(num_str: &str, other: &str) -> bool {
    // i128 rather than i64: an integer literal such as 9223372036854775808 does not
    // fit in i64 but becomes the integer i64::MIN under unary minus, so it must not
    // compare equal to the float 9.223372036854776e18.
    fn parse(s: &str) -> Option<Either<i128, f64>> {
        if let Ok(i) = s.parse::<i128>() {
            Some(Either::Left(i))
        } else if let Ok(f) = s.parse::<f64>() {
            Some(Either::Right(f))
        } else {
            None
        }
    }

    match (parse(num_str), parse(other)) {
        (Some(Either::Left(i1)), Some(Either::Left(i2))) => i1 == i2,
        (Some(Either::Right(f1)), Some(Either::Right(f2))) => f1 == f2,
        // Integer and Float are NOT equivalent even if values match,
        // because result type of operations depends on operand types
        (Some(Either::Left(_)), Some(Either::Right(_)))
        | (Some(Either::Right(_)), Some(Either::Left(_))) => false,
        _ => num_str == other,
    }
}

pub fn check_ident_equivalency(ident1: &str, ident2: &str) -> bool {
    fn strip_quotes(identifier: &str) -> &str {
        for &(start, end) in QUOTE_PAIRS {
            if identifier.starts_with(start) && identifier.ends_with(end) {
                return &identifier[1..identifier.len() - 1];
            }
        }
        identifier
    }
    strip_quotes(ident1).eq_ignore_ascii_case(strip_quotes(ident2))
}

pub fn check_literal_equivalency(lhs: &Literal, rhs: &Literal) -> bool {
    match (lhs, rhs) {
        (Literal::Numeric(n1), Literal::Numeric(n2)) => cmp_numeric_strings(n1, n2),
        (Literal::String(s1), Literal::String(s2)) => s1 == s2,
        (Literal::Blob(b1), Literal::Blob(b2)) => {
            ast::blob_literal_hex(b1).eq_ignore_ascii_case(ast::blob_literal_hex(b2))
        }
        (Literal::Keyword(k1), Literal::Keyword(k2)) => check_ident_equivalency(k1, k2),
        (Literal::Null, Literal::Null) => true,
        (Literal::True, Literal::True) => true,
        (Literal::False, Literal::False) => true,
        (Literal::CurrentDate, Literal::CurrentDate) => true,
        (Literal::CurrentTime, Literal::CurrentTime) => true,
        (Literal::CurrentTimestamp, Literal::CurrentTimestamp) => true,
        _ => false,
    }
}

/// bind AST identifiers to either Column or Rowid if possible
pub fn simple_bind_expr(
    joined_table: &JoinedTable,
    result_columns: &[ast::ResultColumn],
    expr: &mut ast::Expr,
) -> Result<()> {
    let internal_id = joined_table.internal_id;
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        #[allow(clippy::single_match)]
        match expr {
            Expr::Id(id) => {
                for result_column in result_columns.iter() {
                    if let ast::ResultColumn::Expr(result, Some(ast::As::As(alias))) = result_column
                    {
                        if alias.as_str().eq_ignore_ascii_case(id.as_str()) {
                            *expr = *result.clone();
                            return Ok(WalkControl::Continue);
                        }
                    }
                }
                let col_idx = joined_table.columns().iter().position(|c| {
                    c.name
                        .as_ref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(id.as_str()))
                });
                if let Some(col_idx) = col_idx {
                    let col = joined_table.table.columns().get(col_idx).unwrap();
                    *expr = ast::Expr::Column {
                        database: None,
                        table: internal_id,
                        column: col_idx,
                        is_rowid_alias: col.is_rowid_alias(),
                    };
                } else {
                    // only if we haven't found a match, check for explicit rowid reference
                    let is_btree_table = matches!(joined_table.table, Table::BTree(_));
                    if is_btree_table {
                        if let Some(rowid) =
                            parse_row_id(&normalize_ident(id.as_str()), internal_id, || false)?
                        {
                            *expr = rowid;
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

pub fn try_substitute_parameters(
    pattern: &Expr,
    parameters: &HashMap<i32, Expr>,
) -> Option<Box<Expr>> {
    match pattern {
        Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            within_group,
            filter_over,
        } => {
            let mut substituted = Vec::new();
            for arg in args {
                substituted.push(try_substitute_parameters(arg, parameters)?);
            }
            Some(Box::new(Expr::FunctionCall {
                args: substituted,
                distinctness: *distinctness,
                name: name.clone(),
                order_by: order_by.clone(),
                within_group: within_group.clone(),
                filter_over: filter_over.clone(),
            }))
        }
        Expr::Variable(var) => {
            // Named parameters (:x) are not placeholders; positional ones —
            // bare ? and explicit ?N — are.
            if !var.is_positional() {
                return None;
            }
            let Ok(var) = i32::try_from(var.index.get()) else {
                return None;
            };
            Some(Box::new(parameters.get(&var)?.clone()))
        }
        _ => Some(Box::new(pattern.clone())),
    }
}

pub fn try_capture_parameters(pattern: &Expr, query: &Expr) -> Option<HashMap<i32, Expr>> {
    let mut captured = HashMap::default();
    match (pattern, query) {
        (
            Expr::FunctionCall {
                name: name1,
                distinctness: distinct1,
                args: args1,
                order_by: order1,
                within_group: within1,
                filter_over: filter1,
            },
            Expr::FunctionCall {
                name: name2,
                distinctness: distinct2,
                args: args2,
                order_by: order2,
                within_group: within2,
                filter_over: filter2,
            },
        ) => {
            if !name1.as_str().eq_ignore_ascii_case(name2.as_str()) {
                return None;
            }
            if distinct1.is_some() || distinct2.is_some() {
                return None;
            }
            if !order1.is_empty() || !order2.is_empty() {
                return None;
            }
            if !within1.is_empty() || !within2.is_empty() {
                return None;
            }
            if filter1.filter_clause.is_some() || filter1.over_clause.is_some() {
                return None;
            }
            if filter2.filter_clause.is_some() || filter2.over_clause.is_some() {
                return None;
            }
            for (arg1, arg2) in args1.iter().zip(args2.iter()) {
                let result = try_capture_parameters(arg1, arg2)?;
                captured.extend(result);
            }
            Some(captured)
        }
        (Expr::Variable(var), expr) => {
            // Named parameters (:x) are not placeholders; positional ones —
            // bare ? and explicit ?N — are.
            if !var.is_positional() {
                return None;
            }
            let Ok(var) = i32::try_from(var.index.get()) else {
                return None;
            };
            captured.insert(var, expr.clone());
            Some(captured)
        }
        (
            Expr::Id(_) | Expr::Name(_) | Expr::Column { .. },
            Expr::Id(_) | Expr::Name(_) | Expr::Column { .. },
        ) => {
            if pattern == query {
                Some(captured)
            } else {
                None
            }
        }
        (_, _) => None,
    }
}

/// Returns the number of column arguments for FTS functions.
/// FTS functions have column arguments followed by non-column arguments:
/// - fts_match(col1, col2, ..., query_string) -> columns = args.len() - 1
/// - fts_score(col1, col2, ..., query_string) -> columns = args.len() - 1
/// - fts_highlight(col1, col2, ..., before_tag, after_tag, query_string) -> columns = args.len() - 3
///
/// Returns 0 for non-FTS functions.
/// Specific for FTS but cannot gate behind feature = "fts" so it must
/// live in util.rs :/
pub fn count_fts_column_args(expr: &Expr) -> usize {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            let name_lower = name.as_str().to_lowercase();
            match name_lower.as_str() {
                "fts_match" | "fts_score" => args.len().saturating_sub(1),
                "fts_highlight" => args.len().saturating_sub(3),
                _ => 0,
            }
        }
        _ => 0,
    }
}

pub const FTS_FIELD_PARAMETER: i32 = i32::MAX;

/// Match FTS function calls where column arguments can appear in any order.
///
/// FTS functions like `fts_match(col1, col2, 'query')` should match
/// `fts_match(col2, col1, 'query')` as long as the same columns are used.
///
/// Semi-specific for FTS but cannot gate behind feature = "fts" so it must
/// live in util.rs :/
pub fn try_capture_parameters_column_agnostic(
    pattern: &Expr,         // pattern expression from index definition
    query: &Expr,           // the actual query expression
    num_column_args: usize, // number of leading column arguments
) -> Option<HashMap<i32, Expr>> {
    // If not a function call or no column args, fall back to standard matching
    if num_column_args == 0 {
        return try_capture_parameters(pattern, query);
    }

    let (
        Expr::FunctionCall {
            name: pattern_name,
            distinctness: pattern_distinct,
            args: pattern_args,
            order_by: pattern_order,
            within_group: pattern_within,
            filter_over: pattern_filter,
        },
        Expr::FunctionCall {
            name: query_name,
            distinctness: query_distinct,
            args: query_args,
            order_by: query_order,
            within_group: query_within,
            filter_over: query_filter,
        },
    ) = (pattern, query)
    else {
        return try_capture_parameters(pattern, query);
    };
    // Function names must match
    if !pattern_name
        .as_str()
        .eq_ignore_ascii_case(query_name.as_str())
    {
        return None;
    }

    let suffix_len = pattern_args.len().checked_sub(num_column_args)?;
    let query_column_count = query_args.len().checked_sub(suffix_len)?;
    if query_column_count == 0 || query_column_count > num_column_args {
        return None;
    }
    // Distinctness must match (we don't support it)
    if pattern_distinct.is_some() || query_distinct.is_some() {
        return None;
    }
    // ORDER BY within function not supported
    if !pattern_order.is_empty() || !query_order.is_empty() {
        return None;
    }
    // WITHIN GROUP not supported
    if !pattern_within.is_empty() || !query_within.is_empty() {
        return None;
    }

    // Filter/over clause not supported
    if pattern_filter.filter_clause.is_some() || pattern_filter.over_clause.is_some() {
        return None;
    }
    if query_filter.filter_clause.is_some() || query_filter.over_clause.is_some() {
        return None;
    }

    let mut captured = HashMap::default();

    // Split args into column args (reorderable) and remaining args (positional)
    let pattern_col_args = &pattern_args[..num_column_args];
    let query_col_args = &query_args[..query_column_count];
    let pattern_rest = &pattern_args[num_column_args..];
    let query_rest = &query_args[query_column_count..];

    let mut matched_pattern_indices = BitSet::default();

    for query_col in query_col_args {
        let mut found_match = false;
        for (i, pattern_col) in pattern_col_args.iter().enumerate() {
            if matched_pattern_indices.get(i) {
                continue;
            }
            if exprs_are_equivalent(pattern_col, query_col) {
                matched_pattern_indices.set(i).expect("TODO: alloc error");
                found_match = true;
                break;
            }
        }
        if !found_match {
            return None;
        }
    }
    // Remaining args must match positionally (includes the query string parameter)
    for (pattern_arg, query_arg) in pattern_rest.iter().zip(query_rest.iter()) {
        let result = try_capture_parameters(pattern_arg, query_arg)?;
        captured.extend(result);
    }

    let fields = (0..num_column_args)
        .filter(|&i| matched_pattern_indices.get(i))
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    captured.insert(
        FTS_FIELD_PARAMETER,
        Expr::Literal(Literal::String(format!("'{fields}'"))),
    );

    Some(captured)
}

/// This function is used to determine whether two expressions are logically
/// equivalent in the context of queries, even if their representations
/// differ. e.g.: `SUM(x)` and `sum(x)`, `x + y` and `y + x`
pub fn exprs_are_equivalent(expr1: &Expr, expr2: &Expr) -> bool {
    exprs_match(expr1, expr2, true)
}

/// Like [exprs_are_equivalent], but `x + y` and `y + x` are different expressions.
pub fn exprs_are_equivalent_as_written(expr1: &Expr, expr2: &Expr) -> bool {
    exprs_match(expr1, expr2, false)
}

fn exprs_match(expr1: &Expr, expr2: &Expr, operands_can_swap: bool) -> bool {
    match (expr1, expr2) {
        (
            Expr::Between {
                lhs: lhs1,
                not: not1,
                start: start1,
                end: end1,
            },
            Expr::Between {
                lhs: lhs2,
                not: not2,
                start: start2,
                end: end2,
            },
        ) => {
            not1 == not2
                && exprs_match(lhs1, lhs2, operands_can_swap)
                && exprs_match(start1, start2, operands_can_swap)
                && exprs_match(end1, end2, operands_can_swap)
        }
        (Expr::Binary(lhs1, op1, rhs1), Expr::Binary(lhs2, op2, rhs2)) => {
            op1 == op2
                && ((exprs_match(lhs1, lhs2, operands_can_swap)
                    && exprs_match(rhs1, rhs2, operands_can_swap))
                    || (operands_can_swap
                        && op1.is_commutative()
                        && exprs_match(lhs1, rhs2, operands_can_swap)
                        && exprs_match(rhs1, lhs2, operands_can_swap)))
        }
        (
            Expr::Case {
                base: base1,
                when_then_pairs: pairs1,
                else_expr: else1,
            },
            Expr::Case {
                base: base2,
                when_then_pairs: pairs2,
                else_expr: else2,
            },
        ) => {
            base1 == base2
                && pairs1.len() == pairs2.len()
                && pairs1.iter().zip(pairs2).all(|((w1, t1), (w2, t2))| {
                    exprs_match(w1, w2, operands_can_swap) && exprs_match(t1, t2, operands_can_swap)
                })
                && else1 == else2
        }
        (
            Expr::Cast {
                expr: expr1,
                type_name: type1,
            },
            Expr::Cast {
                expr: expr2,
                type_name: type2,
            },
        ) => {
            exprs_match(expr1, expr2, operands_can_swap)
                && match (type1, type2) {
                    (Some(t1), Some(t2)) => t1.name.eq_ignore_ascii_case(&t2.name),
                    _ => false,
                }
        }
        (Expr::Collate(expr1, collation1), Expr::Collate(expr2, collation2)) => {
            // TODO: check correctness of comparing colation as strings
            exprs_match(expr1, expr2, operands_can_swap)
                && collation1
                    .as_str()
                    .eq_ignore_ascii_case(collation2.as_str())
        }
        (
            Expr::FunctionCall {
                name: name1,
                distinctness: distinct1,
                args: args1,
                order_by: order1,
                within_group: within1,
                filter_over: filter1,
            },
            Expr::FunctionCall {
                name: name2,
                distinctness: distinct2,
                args: args2,
                order_by: order2,
                within_group: within2,
                filter_over: filter2,
            },
        ) => {
            name1.as_str().eq_ignore_ascii_case(name2.as_str())
                && distinct1 == distinct2
                && args1 == args2
                && order1 == order2
                && within1 == within2
                && filter1 == filter2
        }
        (
            Expr::FunctionCallStar {
                name: name1,
                filter_over: filter1,
            },
            Expr::FunctionCallStar {
                name: name2,
                filter_over: filter2,
            },
        ) => {
            name1.as_str().eq_ignore_ascii_case(name2.as_str())
                && match (&filter1.filter_clause, &filter2.filter_clause) {
                    (Some(expr1), Some(expr2)) => exprs_match(expr1, expr2, operands_can_swap),
                    (None, None) => true,
                    _ => false,
                }
                && filter1.over_clause == filter2.over_clause
        }
        (Expr::NotNull(expr1), Expr::NotNull(expr2)) => {
            exprs_match(expr1, expr2, operands_can_swap)
        }
        (Expr::IsNull(expr1), Expr::IsNull(expr2)) => exprs_match(expr1, expr2, operands_can_swap),
        (Expr::Literal(lit1), Expr::Literal(lit2)) => check_literal_equivalency(lit1, lit2),
        (Expr::Id(id1), Expr::Id(id2)) => check_ident_equivalency(id1.as_str(), id2.as_str()),
        (Expr::Unary(op1, expr1), Expr::Unary(op2, expr2)) => {
            op1 == op2 && exprs_match(expr1, expr2, operands_can_swap)
        }
        (Expr::Variable(val), Expr::Variable(val2)) => val == val2,
        (Expr::Parenthesized(exprs1), Expr::Parenthesized(exprs2)) => {
            exprs1.len() == exprs2.len()
                && exprs1
                    .iter()
                    .zip(exprs2)
                    .all(|(e1, e2)| exprs_match(e1, e2, operands_can_swap))
        }
        (Expr::Parenthesized(exprs1), exprs2) | (exprs2, Expr::Parenthesized(exprs1)) => {
            exprs1.len() == 1 && exprs_match(&exprs1[0], exprs2, operands_can_swap)
        }
        (Expr::Qualified(tn1, cn1), Expr::Qualified(tn2, cn2)) => {
            check_ident_equivalency(tn1.as_str(), tn2.as_str())
                && check_ident_equivalency(cn1.as_str(), cn2.as_str())
        }
        (Expr::DoublyQualified(sn1, tn1, cn1), Expr::DoublyQualified(sn2, tn2, cn2)) => {
            check_ident_equivalency(sn1.as_str(), sn2.as_str())
                && check_ident_equivalency(tn1.as_str(), tn2.as_str())
                && check_ident_equivalency(cn1.as_str(), cn2.as_str())
        }
        (
            Expr::InList {
                lhs: lhs1,
                not: not1,
                rhs: rhs1,
            },
            Expr::InList {
                lhs: lhs2,
                not: not2,
                rhs: rhs2,
            },
        ) => {
            *not1 == *not2
                && exprs_match(lhs1, lhs2, operands_can_swap)
                && rhs1.len() == rhs2.len()
                && rhs1
                    .iter()
                    .zip(rhs2.iter())
                    .all(|(a, b)| exprs_match(a, b, operands_can_swap))
        }
        (
            Expr::Column {
                database: db1,
                is_rowid_alias: r1,
                table: tbl_1,
                column: col_1,
            },
            Expr::Column {
                database: db2,
                is_rowid_alias: r2,
                table: tbl_2,
                column: col_2,
            },
        ) => tbl_1 == tbl_2 && col_1 == col_2 && db1 == db2 && r1 == r2,
        // fall back to naive equality check
        _ => expr1 == expr2,
    }
}

/// "evaluate" an expression to determine if it contains a poisonous NULL
/// which will propagate through most expressions and result in it's evaluation
/// into NULL. This is used to prevent things like the following:
/// `ALTER TABLE t ADD COLUMN (a NOT NULL DEFAULT (NULL + 5)`
pub(crate) fn expr_contains_null(expr: &ast::Expr) -> bool {
    let mut contains_null = false;
    let _ = walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        if let ast::Expr::Literal(ast::Literal::Null) = expr {
            contains_null = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    }); // infallible
    contains_null
}

// this function returns the affinity type and whether the type name was exactly "INTEGER"
// https://www.sqlite.org/datatype3.html
pub(crate) fn type_from_name(type_name: &str) -> (Type, bool) {
    let type_name = type_name.as_bytes();
    if type_name.is_empty() {
        return (Type::Blob, false);
    }

    if eq_ignore_ascii_case!(type_name, b"INTEGER") {
        return (Type::Integer, true);
    }

    if contains_ignore_ascii_case!(type_name, b"INT") {
        return (Type::Integer, false);
    }

    if let Some(ty) = type_name.windows(4).find_map(|s| {
        match_ignore_ascii_case!(match s {
            b"CHAR" | b"CLOB" | b"TEXT" => Some(Type::Text),
            b"BLOB" => Some(Type::Blob),
            b"REAL" | b"FLOA" | b"DOUB" => Some(Type::Real),
            _ => None,
        })
    }) {
        return (ty, false);
    }

    (Type::Numeric, false)
}

/// Older Turso versions kept the quotes of `"INTEGER"`, `'INTEGER'` and
/// `` `INTEGER` `` when deciding whether an INTEGER PRIMARY KEY column is a
/// rowid alias, and only removed the brackets of `[INTEGER]`. Existing
/// databases depend on that, because a column that is not a rowid alias is
/// stored in the record and has an automatic index.
///
/// An array type such as `INTEGER[]` is never a rowid alias, even though its
/// element type name is `INTEGER`.
pub(crate) fn is_rowid_alias_type(written_type: &ast::Type) -> bool {
    !written_type.is_array()
        && (written_type.name.eq_ignore_ascii_case("INTEGER")
            || written_type.name.eq_ignore_ascii_case("[INTEGER]"))
}

#[derive(Debug, Default, PartialEq)]
pub struct OpenOptions<'a> {
    /// The authority component of the URI. may be 'localhost' or empty
    pub authority: Option<&'a str>,
    /// The normalized path to the database file
    pub path: String,
    /// The vfs query parameter causes the database connection to be opened using the VFS called NAME
    pub vfs: Option<String>,
    /// read-only, read-write, read-write and created if it does not exist, or pure in-memory database that never interacts with disk
    pub mode: OpenMode,
    /// Attempt to set the permissions of the new database file to match the existing file "filename".
    pub modeof: Option<String>,
    /// Specifies Cache mode shared | private
    pub cache: CacheMode,
    /// immutable=1|0 specifies that the database is stored on read-only media
    pub immutable: bool,
    // The encryption cipher
    pub cipher: Option<String>,
    // The encryption key in hex format
    pub hexkey: Option<String>,
}

pub const MEMORY_PATH: &str = ":memory:";

#[derive(Clone, Default, Debug, Copy, PartialEq)]
pub enum OpenMode {
    ReadOnly,
    ReadWrite,
    Memory,
    #[default]
    ReadWriteCreate,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum CacheMode {
    #[default]
    Private,
    Shared,
}

impl From<&str> for CacheMode {
    fn from(s: &str) -> Self {
        match s {
            "private" => CacheMode::Private,
            "shared" => CacheMode::Shared,
            _ => CacheMode::Private,
        }
    }
}

impl OpenMode {
    pub fn from_str(s: &str) -> Result<Self> {
        let s_bytes = s.trim().as_bytes();
        match_ignore_ascii_case!(match s_bytes {
            b"ro" => Ok(OpenMode::ReadOnly),
            b"rw" => Ok(OpenMode::ReadWrite),
            b"memory" => Ok(OpenMode::Memory),
            b"rwc" => Ok(OpenMode::ReadWriteCreate),
            _ => Err(LimboError::InvalidArgument(format!(
                "Invalid mode: '{s}'. Expected one of 'ro', 'rw', 'memory', 'rwc'"
            ))),
        })
    }
}

fn is_windows_path(path: &str) -> bool {
    path.len() >= 3
        && path.chars().nth(1) == Some(':')
        && (path.chars().nth(2) == Some('/') || path.chars().nth(2) == Some('\\'))
}

/// converts windows-style paths to forward slashes, per SQLite spec.
fn normalize_windows_path(path: &str) -> String {
    let mut normalized = path.replace("\\", "/");

    // remove duplicate slashes (`//` → `/`)
    while normalized.contains("//") {
        normalized = normalized.replace("//", "/");
    }

    // if absolute windows path (`C:/...`), ensure it starts with `/`
    if normalized.len() >= 3
        && !normalized.starts_with('/')
        && normalized.chars().nth(1) == Some(':')
        && normalized.chars().nth(2) == Some('/')
    {
        normalized.insert(0, '/');
    }
    normalized
}

/// Returns the path of the database file that the URI `uri` names.
/// `Connection::from_uri` reads `uri` in the same way, but this function does
/// not open the file. Thus a caller can examine the file before `from_uri`
/// creates it or changes it.
pub fn database_path_from_uri(uri: &str) -> Result<String> {
    Ok(OpenOptions::parse(uri)?.path)
}

impl<'a> OpenOptions<'a> {
    /// Parses a SQLite URI, handling Windows and Unix paths separately.
    pub fn parse(uri: &'a str) -> Result<OpenOptions<'a>> {
        if !uri.starts_with("file:") {
            return Ok(OpenOptions {
                path: uri.to_string(),
                ..Default::default()
            });
        }

        let mut opts = OpenOptions::default();
        let without_scheme = &uri[5..];

        let (without_fragment, _) = without_scheme
            .split_once('#')
            .unwrap_or((without_scheme, ""));

        let (without_query, query) = without_fragment
            .split_once('?')
            .unwrap_or((without_fragment, ""));
        parse_query_params(query, &mut opts)?;

        // handle authority + path separately
        if let Some(after_slashes) = without_query.strip_prefix("//") {
            let (authority, path) = match after_slashes.split_once('/') {
                Some((authority, path)) => (authority, Some(path)),
                None => (after_slashes, None),
            };

            // sqlite allows only `localhost` or empty authority.
            if !(authority.is_empty() || authority == "localhost") {
                return Err(LimboError::InvalidArgument(format!(
                    "Invalid authority '{authority}'. Only '' or 'localhost' allowed."
                )));
            }
            opts.authority = if authority.is_empty() {
                None
            } else {
                Some(authority)
            };

            opts.path = match path {
                Some(path) if is_windows_path(path) => {
                    normalize_windows_path(&decode_percent(path))
                }
                Some(path) => format!("/{}", decode_percent(path)),
                None => String::new(),
            };
        } else {
            // no authority, must be a normal absolute or relative path.
            opts.path = decode_percent(without_query);
        }

        Ok(opts)
    }

    pub fn get_flags(&self) -> Result<OpenFlags> {
        // Only use modeof if we're in a mode that can create files
        if self.mode != OpenMode::ReadWriteCreate && self.modeof.is_some() {
            return Err(LimboError::InvalidArgument(
                "modeof is not applicable without mode=rwc".to_string(),
            ));
        }
        // If modeof is not applicable or file doesn't exist, use default flags
        Ok(match self.mode {
            OpenMode::ReadWriteCreate => OpenFlags::Create,
            OpenMode::ReadOnly => OpenFlags::ReadOnly,
            _ => OpenFlags::default(),
        })
    }
}

// parses query parameters and updates OpenOptions
fn parse_query_params(query: &str, opts: &mut OpenOptions) -> Result<()> {
    for param in query.split('&') {
        if let Some((key, value)) = param.split_once('=') {
            let decoded_value = decode_percent(value);
            match key {
                "mode" => opts.mode = OpenMode::from_str(value)?,
                "modeof" => opts.modeof = Some(decoded_value),
                "cache" => opts.cache = decoded_value.as_str().into(),
                "immutable" => opts.immutable = decoded_value == "1",
                "vfs" => opts.vfs = Some(decoded_value),
                "cipher" => opts.cipher = Some(decoded_value),
                "hexkey" => opts.hexkey = Some(decoded_value),
                _ => {}
            }
        }
    }
    Ok(())
}

/// Decodes percent-encoded characters
/// this function was adapted from the 'urlencoding' crate. MIT
pub fn decode_percent(uri: &str) -> String {
    let from_hex_digit = |digit: u8| -> Option<u8> {
        match digit {
            b'0'..=b'9' => Some(digit - b'0'),
            b'A'..=b'F' => Some(digit - b'A' + 10),
            b'a'..=b'f' => Some(digit - b'a' + 10),
            _ => None,
        }
    };

    let offset = uri.chars().take_while(|&c| c != '%').count();

    if offset >= uri.len() {
        return uri.to_string();
    }

    let mut decoded: Vec<u8> = Vec::with_capacity(uri.len());
    let (ascii, mut data) = uri.as_bytes().split_at(offset);
    decoded.extend_from_slice(ascii);

    loop {
        let mut parts = data.splitn(2, |&c| c == b'%');
        let non_escaped_part = parts.next().unwrap();
        let rest = parts.next();
        if rest.is_none() && decoded.is_empty() {
            return String::from_utf8_lossy(data).to_string();
        }
        decoded.extend_from_slice(non_escaped_part);
        match rest {
            Some(rest) => match rest.get(0..2) {
                Some([first, second]) => match from_hex_digit(*first) {
                    Some(first_val) => match from_hex_digit(*second) {
                        Some(second_val) => {
                            decoded.push((first_val << 4) | second_val);
                            data = &rest[2..];
                        }
                        None => {
                            decoded.extend_from_slice(&[b'%', *first]);
                            data = &rest[1..];
                        }
                    },
                    None => {
                        decoded.push(b'%');
                        data = rest;
                    }
                },
                _ => {
                    decoded.push(b'%');
                    decoded.extend_from_slice(rest);
                    break;
                }
            },
            None => break,
        }
    }
    String::from_utf8_lossy(&decoded).to_string()
}

pub fn trim_ascii_whitespace(s: &str) -> &str {
    // SQLite's ctype table treats 0x0B (vertical tab) as whitespace, unlike
    // Rust's is_ascii_whitespace() which follows the WHATWG definition and
    // excludes it. Match SQLite here.
    let is_space = |b: u8| b.is_ascii_whitespace() || b == b'\x0b';
    let bytes = s.as_bytes();
    let start = bytes
        .iter()
        .position(|&b| !is_space(b))
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|&b| !is_space(b))
        .map(|i| i + 1)
        .unwrap_or(0);
    if start <= end {
        &s[start..end]
    } else {
        ""
    }
}

/// NUMERIC Casting a TEXT or BLOB value into NUMERIC yields either an INTEGER or a REAL result.
/// If the input text looks like an integer (there is no decimal point nor exponent) and the value
/// is small enough to fit in a 64-bit signed integer, then the result will be INTEGER.
/// Input text that looks like floating point (there is a decimal point and/or an exponent)
/// and the text describes a value that can be losslessly converted back and forth between IEEE 754
/// 64-bit float and a 51-bit signed integer, then the result is INTEGER. (In the previous sentence,
/// a 51-bit integer is specified since that is one bit less than the length of the mantissa of an
/// IEEE 754 64-bit float and thus provides a 1-bit of margin for the text-to-float conversion operation.)
/// Any text input that describes a value outside the range of a 64-bit signed integer yields a REAL result.
/// Casting a REAL or INTEGER value to NUMERIC is a no-op, even if a real value could be losslessly converted to an integer.
///
/// `lossless`: If `true`, rejects the input if any characters remain after the numeric prefix (strict / exact conversion).
pub fn checked_cast_text_to_numeric(text: &str, lossless: bool) -> std::result::Result<Value, ()> {
    // sqlite will parse the first N digits of a string to numeric value, then determine
    // whether _that_ value is more likely a real or integer value. e.g.
    // '-100234-2344.23e14' evaluates to -100234 instead of -100234.0
    let original_len = trim_ascii_whitespace(text).len();
    let (kind, text) = parse_numeric_str(text)?;

    if original_len != text.len() && lossless {
        return Err(());
    }

    // `str_to_f64` rounds the same way as sqlite3AtoF. `str::parse::<f64>` rounds
    // correctly, which gives a different double for some inputs.
    let parse_real = || {
        f64::from(
            crate::numeric::str_to_f64(text)
                .expect("parse_numeric_str returns text with at least one digit"),
        )
    };
    match kind {
        ValueType::Integer => match text.parse::<i64>() {
            Ok(i) => Ok(Value::from_i64(i)),
            Err(e) => {
                if matches!(
                    e.kind(),
                    std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow
                ) {
                    // if overflow, we return the representation as a real.
                    Ok(Value::from_f64(parse_real()))
                } else {
                    Err(())
                }
            }
        },
        ValueType::Float => Ok(real_to_numeric_value(parse_real())),
        _ => unreachable!(),
    }
}

/// Applies the same normalization strategy as SQLite:
/// converts a `f64` to an integer when it represents an exact integral value.
///
/// The conversion is restricted to 51-bit signed integers to ensure the value
/// can round-trip through a text representation without losing precision,
/// staying below the 52-bit mantissa limit of IEEE 754 `f64`.
fn real_to_numeric_value(value: f64) -> Value {
    const INT51_MAX: i64 = 1 << 51;
    if value == 0.0 {
        return Value::from_i64(0);
    }
    if value.is_finite() {
        let i = value as i64;
        if (i as f64) == value && (-INT51_MAX..INT51_MAX).contains(&i) {
            return Value::from_i64(i);
        }
    }
    Value::from_f64(value)
}

fn parse_numeric_str(text: &str) -> Result<(ValueType, &str), ()> {
    let text = trim_ascii_whitespace(text);
    let bytes = text.as_bytes();

    if matches!(
        bytes,
        [] | [b'e', ..] | [b'E', ..] | [b'.', b'e' | b'E', ..]
    ) {
        return Err(());
    }

    let mut end = 0;
    let mut has_decimal = false;
    let mut has_exponent = false;
    let mut has_mantissa_digit = false;
    if bytes[0] == b'-' || bytes[0] == b'+' {
        end = 1;
    }
    while end < bytes.len() {
        match bytes[end] {
            b'0'..=b'9' => {
                has_mantissa_digit |= !has_exponent;
                end += 1;
            }
            b'.' if !has_decimal && !has_exponent => {
                has_decimal = true;
                end += 1;
            }
            b'e' | b'E' if !has_exponent => {
                has_exponent = true;
                end += 1;
                // allow exponent sign
                if end < bytes.len() && (bytes[end] == b'+' || bytes[end] == b'-') {
                    end += 1;
                }
            }
            _ => break,
        }
    }
    // like sqlite3AtoF, a number needs at least one digit before any exponent:
    // ".", "-." and "+e5" are not numeric
    if !has_mantissa_digit {
        return Err(());
    }
    // like sqlite3AtoF, an exponent with no digits ("12e", "12e+") is not part of
    // the number, so it does not make the text a real either
    let last = bytes[end - 1];
    if last.eq_ignore_ascii_case(&b'e') {
        end -= 1;
        has_exponent = false;
    } else if has_exponent && (last == b'-' || last == b'+') {
        end -= 2;
        has_exponent = false;
    }
    Ok((
        if !has_decimal && !has_exponent {
            ValueType::Integer
        } else {
            ValueType::Float
        },
        &text[..end],
    ))
}

// Check if float can be converted to integer for INTEGER PRIMARY KEY columns.
// SQLite uses sqlite3VdbeIntegerAffinity which requires:
// 1. The float must round-trip correctly (float -> int -> float gives same value)
// 2. The integer must be strictly between i64::MIN and i64::MAX (exclusive)
//
// This matches SQLite's check: ix > SMALLEST_INT64 && ix < LARGEST_INT64
pub fn cast_real_to_integer(float: f64) -> std::result::Result<i64, ()> {
    // Must be finite and a whole number (no fractional part)
    if !float.is_finite() || float.trunc() != float {
        return Err(());
    }

    // Convert to i64, clamping to i64 range if necessary
    // Note: Rust's f64 as i64 saturates to i64::MIN/MAX for out-of-range values
    let int_val = float as i64;

    // SQLite requires the value to be STRICTLY between i64::MIN and i64::MAX
    // (i.e., ix > SMALLEST_INT64 && ix < LARGEST_INT64)
    if int_val == i64::MIN || int_val == i64::MAX {
        return Err(());
    }

    // Verify round-trip: converting back to f64 must give the same value
    // This matches SQLite's check: pMem->u.r == ix
    if (int_val as f64) != float {
        return Err(());
    }

    Ok(int_val)
}

// we don't need to verify the numeric literal here, as it is already verified by the parser
pub fn parse_numeric_literal(text: &str) -> Result<Value> {
    if let Some(value) = parse_short_decimal_integer(text) {
        return Ok(Value::from_i64(value));
    }
    // a single extra underscore ("_") character can exist between any two digits
    let text = if text.contains('_') {
        std::borrow::Cow::Owned(text.replace('_', ""))
    } else {
        std::borrow::Cow::Borrowed(text)
    };

    if text.starts_with("0x") || text.starts_with("0X") {
        let value = u64::from_str_radix(&text[2..], 16)? as i64;
        return Ok(Value::from_i64(value));
    } else if text.starts_with("-0x") || text.starts_with("-0X") {
        let value = u64::from_str_radix(&text[3..], 16)? as i64;
        if value == i64::MIN {
            // Negating i64::MIN overflows. SQLite's codeInteger rejects this at
            // prepare time with "hex literal too big"; do the same.
            return Err(LimboError::ParseError(format!(
                "hex literal too big: {text}"
            )));
        }
        return Ok(Value::from_i64(-value));
    }

    if let Ok(int_value) = text.parse::<i64>() {
        return Ok(Value::from_i64(int_value));
    }

    let Some(StrToF64::Fractional(float) | StrToF64::Decimal(float)) =
        crate::numeric::str_to_f64(text)
    else {
        unreachable!();
    };
    Ok(Value::Numeric(crate::numeric::Numeric::Float(float)))
}

/// The value of `text` if it has only ASCII digits, at most 18 of them. Such
/// a number always fits in an `i64`.
fn parse_short_decimal_integer(text: &str) -> Option<i64> {
    let digits = text.as_bytes();
    if digits.is_empty() || digits.len() > 18 {
        return None;
    }
    let mut value: i64 = 0;
    for &byte in digits {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value * 10 + i64::from(digit);
    }
    Some(value)
}

pub fn parse_signed_number(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Literal(Literal::Numeric(num)) => parse_numeric_literal(num),
        Expr::Unary(op, expr) => match (op, expr.as_ref()) {
            (UnaryOperator::Negative, Expr::Literal(Literal::Numeric(num))) => {
                let data = "-".to_owned() + &num.to_string();
                parse_numeric_literal(&data)
            }
            (UnaryOperator::Positive, Expr::Literal(Literal::Numeric(num))) => {
                parse_numeric_literal(num)
            }
            _ => Err(LimboError::InvalidArgument(
                "signed-number must follow the format: ([+|-] numeric-literal)".to_string(),
            )),
        },
        _ => Err(LimboError::InvalidArgument(
            "signed-number must follow the format: ([+|-] numeric-literal)".to_string(),
        )),
    }
}

pub fn parse_string(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Name(name) if name.quoted_with('\'') => Ok(name.as_str().to_string()),
        _ => Err(LimboError::InvalidArgument(format!(
            "string parameter expected, got {expr:?} instead"
        ))),
    }
}

#[allow(unused)]
pub fn parse_pragma_bool(expr: &Expr) -> Result<bool> {
    const TRUE_VALUES: &[&str] = &["yes", "true", "on"];
    const FALSE_VALUES: &[&str] = &["no", "false", "off"];
    if let Ok(number) = parse_signed_number(expr) {
        if let Value::Numeric(crate::numeric::Numeric::Integer(x @ (0 | 1))) = number {
            return Ok(x != 0);
        }
    } else if let Expr::Name(name) = expr {
        let ident = normalize_ident(name.as_str());
        if TRUE_VALUES.contains(&ident.as_str()) {
            return Ok(true);
        }
        if FALSE_VALUES.contains(&ident.as_str()) {
            return Ok(false);
        }
    }
    Err(LimboError::InvalidArgument(
        "boolean pragma value must be either 0|1 integer or yes|true|on|no|false|off token"
            .to_string(),
    ))
}

/// Extract column name from an expression (e.g., for SELECT clauses).
/// Like SQLite's sqlite3ColumnsFromExprList(), it looks through parentheses
/// and COLLATE, so `(x)` and `x COLLATE nocase` are named x.
pub fn extract_column_name_from_expr(expr: impl AsRef<ast::Expr>) -> Option<String> {
    let expr = expr.as_ref();
    let name = crate::translate::expr::column_reference_name(expr)?;
    match crate::translate::expr::skip_parens_and_collate(expr) {
        ast::Expr::Id(_) => Some(name.as_str().to_string()),
        _ => Some(normalize_ident(name.as_str())),
    }
}

/// What a walk over a statement, a SELECT or a trigger reports. A closure
/// over `&ast::Expr` is a visitor that only looks at expressions.
pub trait AstVisitor {
    /// Every expression, including the ones inside subqueries. `SkipChildren`
    /// leaves out its sub-expressions and the subqueries in them.
    fn expr(&mut self, expr: &ast::Expr) -> Result<WalkControl>;

    /// Every table a FROM clause names.
    fn table(&mut self, _name: &ast::QualifiedName) -> Result<()> {
        Ok(())
    }

    /// Every table-valued function a FROM clause calls.
    fn table_function(&mut self, _name: &ast::QualifiedName) -> Result<()> {
        Ok(())
    }

    /// Before the CTEs and the body of a SELECT that has a WITH clause.
    fn enter_with(&mut self, _with: &ast::With) {}

    /// After the SELECT whose WITH clause `enter_with` reported.
    fn leave_with(&mut self) {}

    /// Visit in SQLite's name resolution order: each query level's FROM
    /// clause before its SELECT list, and the left operand of `IN (SELECT ...)`
    /// before the subquery. Off by default: the walk visits in source order so
    /// that it also drives `?` / `:name` parameter numbering.
    fn resolve_from_before_columns(&self) -> bool {
        false
    }

    /// Descend into a named WINDOW clause's frame bounds. On by default.
    fn enter_window_frame_bounds(&self) -> bool {
        true
    }

    /// Report a trigger's own table and the target table of each of its
    /// INSERT, UPDATE and DELETE commands through `table()`. Off by default.
    fn report_target_tables(&self) -> bool {
        false
    }
}

impl<F> AstVisitor for F
where
    F: FnMut(&ast::Expr) -> Result<WalkControl>,
{
    fn expr(&mut self, expr: &ast::Expr) -> Result<WalkControl> {
        self(expr)
    }
}

/// The schema object an expression stored in the schema belongs to.
#[derive(Clone, Copy)]
pub enum SchemaExprOwner<'a> {
    ColumnDefault {
        table: &'a Table,
        column: &'a Column,
    },
    GeneratedColumn {
        table: &'a Table,
        column: &'a Column,
    },
    CheckConstraint {
        table: &'a Table,
        constraint: &'a CheckConstraint,
    },
    Policy {
        table: &'a Table,
        policy: &'a RowPolicy,
    },
    Index(&'a Index),
    View(&'a str),
    MaterializedView(&'a str),
    Trigger(&'a Trigger),
}

impl std::fmt::Display for SchemaExprOwner<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let column_name = |column: &Column| column.name.as_deref().unwrap_or("?").to_string();
        match self {
            Self::ColumnDefault { table, column } => write!(
                f,
                "the default of column {} in table {}",
                column_name(column),
                table.get_name()
            ),
            Self::GeneratedColumn { table, column } => write!(
                f,
                "generated column {} in table {}",
                column_name(column),
                table.get_name()
            ),
            Self::CheckConstraint { table, .. } => {
                write!(f, "a CHECK constraint in table {}", table.get_name())
            }
            Self::Policy { table, policy } => {
                write!(f, "policy {} on table {}", policy.name, table.get_name())
            }
            Self::Index(index) => write!(f, "index {}", index.name),
            Self::View(name) => write!(f, "view {name}"),
            Self::MaterializedView(name) => write!(f, "materialized view {name}"),
            Self::Trigger(trigger) => write!(f, "trigger {}", trigger.name),
        }
    }
}

/// Call `func` on every expression the schema stores, with the object it
/// belongs to and that object's namespace: column defaults, generated
/// columns, CHECK constraints, the row security policies that `dialect`
/// stores, index keys and WHERE clauses, view and materialized view bodies,
/// and trigger WHEN clauses and bodies, subqueries included.
pub fn walk_schema_expressions<F>(
    schema: &Schema,
    dialect: &dyn Dialect,
    func: &mut F,
) -> Result<()>
where
    F: FnMut(NamespaceId, SchemaExprOwner<'_>, &ast::Expr) -> Result<WalkControl>,
{
    for namespace in schema.namespace_ids() {
        for table in schema.tables(namespace) {
            let table: &Table = table;
            for column in table.columns() {
                if let Some(default) = &column.default {
                    walk_expr_with_subqueries(default, &mut |expr| {
                        func(
                            namespace,
                            SchemaExprOwner::ColumnDefault { table, column },
                            expr,
                        )
                    })?;
                }
                if let Some(expr) = column.generated_expr() {
                    walk_expr_with_subqueries(expr, &mut |expr| {
                        func(
                            namespace,
                            SchemaExprOwner::GeneratedColumn { table, column },
                            expr,
                        )
                    })?;
                }
            }
            if let Table::BTree(btree) = table {
                for constraint in &btree.check_constraints {
                    walk_expr_with_subqueries(&constraint.expr, &mut |expr| {
                        func(
                            namespace,
                            SchemaExprOwner::CheckConstraint { table, constraint },
                            expr,
                        )
                    })?;
                }
            }
            for policy in dialect.row_security_policies(schema, namespace, table.get_name()) {
                for policy_expr in [&policy.using_expr, &policy.check_expr]
                    .into_iter()
                    .flatten()
                {
                    walk_expr_with_subqueries(policy_expr, &mut |expr| {
                        func(namespace, SchemaExprOwner::Policy { table, policy }, expr)
                    })?;
                }
            }
            if let Some(view) = schema.get_materialized_view(namespace, table.get_name()) {
                walk_select_expressions(&view.lock().select_stmt, &mut |expr| {
                    func(
                        namespace,
                        SchemaExprOwner::MaterializedView(table.get_name()),
                        expr,
                    )
                })?;
            }
        }
        for index in schema.indexes(namespace) {
            let index_exprs = index
                .columns
                .iter()
                .filter_map(|column| column.expr.as_deref())
                .chain(index.where_clause.as_deref());
            for expr in index_exprs {
                walk_expr_with_subqueries(expr, &mut |expr| {
                    func(namespace, SchemaExprOwner::Index(index), expr)
                })?;
            }
        }
        for (name, view) in schema.views(namespace) {
            walk_select_expressions(&view.select_stmt, &mut |expr| {
                func(namespace, SchemaExprOwner::View(name), expr)
            })?;
        }
        for trigger in schema.triggers(namespace) {
            visit_trigger(trigger, &mut |expr: &ast::Expr| {
                func(namespace, SchemaExprOwner::Trigger(trigger), expr)
            })?;
        }
    }
    Ok(())
}

/// Walk all expressions in a statement, including subqueries.
pub fn walk_stmt_expressions<F>(stmt: &ast::Stmt, func: &mut F) -> Result<()>
where
    F: FnMut(&ast::Expr) -> Result<WalkControl>,
{
    visit_stmt(stmt, func)
}

/// Walk a statement: the tables it reads and all of its expressions,
/// including subqueries.
pub fn visit_stmt<V: AstVisitor>(stmt: &ast::Stmt, visitor: &mut V) -> Result<()> {
    match stmt {
        ast::Stmt::Select(select) => visit_select(select, visitor)?,
        ast::Stmt::Insert {
            with,
            body,
            returning,
            ..
        } => {
            visit_with(with.as_ref(), visitor)?;
            if let ast::InsertBody::Select(select, upsert) = body {
                visit_select(select, visitor)?;
                visit_upsert(upsert.as_deref(), visitor)?;
            }
            visit_result_columns(returning, visitor)?;
        }
        ast::Stmt::Update(update) => {
            visit_with(update.with.as_ref(), visitor)?;
            for set in &update.sets {
                visit_expr_with_subqueries(&set.expr, visitor)?;
            }
            if let Some(from_clause) = &update.from {
                visit_from_clause(from_clause, visitor)?;
            }
            if let Some(where_clause) = &update.where_clause {
                visit_expr_with_subqueries(where_clause, visitor)?;
            }
            visit_result_columns(&update.returning, visitor)?;
        }
        ast::Stmt::Delete {
            with,
            where_clause,
            returning,
            ..
        } => {
            visit_with(with.as_ref(), visitor)?;
            if let Some(where_clause) = where_clause {
                visit_expr_with_subqueries(where_clause, visitor)?;
            }
            visit_result_columns(returning, visitor)?;
        }
        ast::Stmt::Attach { expr, db_name, key } => {
            visit_expr_with_subqueries(expr, visitor)?;
            visit_expr_with_subqueries(db_name, visitor)?;
            if let Some(key) = key {
                visit_expr_with_subqueries(key, visitor)?;
            }
        }
        ast::Stmt::Detach { name } => visit_expr_with_subqueries(name, visitor)?,
        ast::Stmt::Vacuum { into, .. } => {
            if let Some(into) = into {
                visit_expr_with_subqueries(into, visitor)?;
            }
        }
        ast::Stmt::Pragma { body, .. } => {
            if let Some(ast::PragmaBody::Equals(value) | ast::PragmaBody::Call(value)) = body {
                visit_expr_with_subqueries(value, visitor)?;
            }
        }
        ast::Stmt::CreateTable {
            body: ast::CreateTableBody::AsSelect(select),
            ..
        } => visit_select(select, visitor)?,
        ast::Stmt::AlterTable(_)
        | ast::Stmt::Analyze { .. }
        | ast::Stmt::Begin { .. }
        | ast::Stmt::Commit { .. }
        | ast::Stmt::CreateIndex { .. }
        | ast::Stmt::CreateTable { .. }
        | ast::Stmt::CreateTrigger { .. }
        | ast::Stmt::CreateView { .. }
        | ast::Stmt::CreateMaterializedView { .. }
        | ast::Stmt::CreateVirtualTable(_)
        | ast::Stmt::CreateType { .. }
        | ast::Stmt::CreateDomain { .. }
        | ast::Stmt::DropIndex { .. }
        | ast::Stmt::DropTable { .. }
        | ast::Stmt::DropTrigger { .. }
        | ast::Stmt::DropView { .. }
        | ast::Stmt::DropType { .. }
        | ast::Stmt::DropDomain { .. }
        | ast::Stmt::Reindex { .. }
        | ast::Stmt::Release { .. }
        | ast::Stmt::Rollback { .. }
        | ast::Stmt::Savepoint { .. }
        | ast::Stmt::Optimize { .. }
        | ast::Stmt::CreateSequence { .. }
        | ast::Stmt::DropSequence { .. }
        | ast::Stmt::AlterSequenceOwnedBy { .. }
        | ast::Stmt::CreateSchema { .. }
        | ast::Stmt::CreateRole { .. }
        | ast::Stmt::DropRole { .. }
        | ast::Stmt::Grant(_)
        | ast::Stmt::CreatePolicy(_)
        | ast::Stmt::DropPolicy { .. }
        | ast::Stmt::DropSchema { .. } => {}
    }
    Ok(())
}

/// Walk a trigger: its WHEN clause and its commands, and its own table first
/// when the visitor asks for target tables.
pub fn visit_trigger<V: AstVisitor>(trigger: &Trigger, visitor: &mut V) -> Result<()> {
    if visitor.report_target_tables() {
        visitor.table(&ast::QualifiedName::single(ast::Name::exact(
            trigger.table_name.clone(),
        )))?;
    }
    if let Some(when_clause) = &trigger.when_clause {
        visit_expr_with_subqueries(when_clause, visitor)?;
    }
    for command in &trigger.commands {
        visit_trigger_cmd(command, visitor)?;
    }
    Ok(())
}

fn visit_trigger_cmd<V: AstVisitor>(cmd: &ast::TriggerCmd, visitor: &mut V) -> Result<()> {
    match cmd {
        ast::TriggerCmd::Update {
            tbl_name,
            sets,
            from,
            where_clause,
            ..
        } => {
            if visitor.report_target_tables() {
                visitor.table(&ast::QualifiedName::single(tbl_name.clone()))?;
            }
            // Name resolution sees the FROM clause before the SET expressions.
            if visitor.resolve_from_before_columns() {
                if let Some(from_clause) = from {
                    visit_from_clause(from_clause, visitor)?;
                }
            }
            for set in sets {
                visit_expr_with_subqueries(&set.expr, visitor)?;
            }
            if !visitor.resolve_from_before_columns() {
                if let Some(from_clause) = from {
                    visit_from_clause(from_clause, visitor)?;
                }
            }
            if let Some(where_clause) = where_clause {
                visit_expr_with_subqueries(where_clause, visitor)?;
            }
        }
        ast::TriggerCmd::Insert {
            tbl_name,
            select,
            upsert,
            returning,
            ..
        } => {
            if visitor.report_target_tables() {
                visitor.table(&ast::QualifiedName::single(tbl_name.clone()))?;
            }
            visit_select(select, visitor)?;
            visit_upsert(upsert.as_deref(), visitor)?;
            visit_result_columns(returning, visitor)?;
        }
        ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => {
            if visitor.report_target_tables() {
                visitor.table(&ast::QualifiedName::single(tbl_name.clone()))?;
            }
            if let Some(where_clause) = where_clause {
                visit_expr_with_subqueries(where_clause, visitor)?;
            }
        }
        ast::TriggerCmd::Select(select) => visit_select(select, visitor)?,
    }
    Ok(())
}

fn visit_upsert<V: AstVisitor>(mut upsert: Option<&ast::Upsert>, visitor: &mut V) -> Result<()> {
    while let Some(clause) = upsert {
        if let Some(index) = &clause.index {
            for target in &index.targets {
                visit_expr_with_subqueries(&target.expr, visitor)?;
            }
            if let Some(where_clause) = &index.where_clause {
                visit_expr_with_subqueries(where_clause, visitor)?;
            }
        }
        if let ast::UpsertDo::Set { sets, where_clause } = &clause.do_clause {
            for set in sets {
                visit_expr_with_subqueries(&set.expr, visitor)?;
            }
            if let Some(where_clause) = where_clause {
                visit_expr_with_subqueries(where_clause, visitor)?;
            }
        }
        upsert = clause.next.as_deref();
    }
    Ok(())
}

fn visit_with<V: AstVisitor>(with: Option<&ast::With>, visitor: &mut V) -> Result<()> {
    for cte in with.iter().flat_map(|with| &with.ctes) {
        visit_select(&cte.select, visitor)?;
    }
    Ok(())
}

fn visit_result_columns<V: AstVisitor>(
    columns: &[ast::ResultColumn],
    visitor: &mut V,
) -> Result<()> {
    for column in columns {
        if let ast::ResultColumn::Expr(expr, _) = column {
            visit_expr_with_subqueries(expr, visitor)?;
        }
    }
    Ok(())
}

/// Walk all expressions in a SELECT statement, including subqueries.
pub fn walk_select_expressions<F>(select: &ast::Select, func: &mut F) -> Result<()>
where
    F: FnMut(&ast::Expr) -> Result<WalkControl>,
{
    visit_select(select, func)
}

/// Walk a SELECT: the tables it reads and all of its expressions, including
/// subqueries.
pub fn visit_select<V: AstVisitor>(select: &ast::Select, visitor: &mut V) -> Result<()> {
    if let Some(with) = &select.with {
        visitor.enter_with(with);
    }
    visit_with(select.with.as_ref(), visitor)?;

    visit_one_select(&select.body.select, visitor)?;
    for compound in &select.body.compounds {
        visit_one_select(&compound.select, visitor)?;
    }

    for sorted_col in &select.order_by {
        visit_expr_with_subqueries(&sorted_col.expr, visitor)?;
    }

    if let Some(limit) = &select.limit {
        visit_expr_with_subqueries(&limit.expr, visitor)?;
        if let Some(offset) = &limit.offset {
            visit_expr_with_subqueries(offset, visitor)?;
        }
    }

    if select.with.is_some() {
        visitor.leave_with();
    }

    Ok(())
}

fn visit_one_select<V: AstVisitor>(one_select: &ast::OneSelect, visitor: &mut V) -> Result<()> {
    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            distinctness,
        } => {
            // Source order is SELECT list then FROM; name resolution is the
            // reverse, which a visitor can ask for.
            if visitor.resolve_from_before_columns() {
                if let Some(from_clause) = from {
                    visit_from_clause(from_clause, visitor)?;
                }
                visit_result_columns(columns, visitor)?;
            } else {
                visit_result_columns(columns, visitor)?;
                if let Some(from_clause) = from {
                    visit_from_clause(from_clause, visitor)?;
                }
            }

            if let Some(where_expr) = where_clause {
                visit_expr_with_subqueries(where_expr, visitor)?;
            }

            if let Some(group_by) = group_by {
                for expr in &group_by.exprs {
                    visit_expr_with_subqueries(expr, visitor)?;
                }
                if let Some(having_expr) = &group_by.having {
                    visit_expr_with_subqueries(having_expr, visitor)?;
                }
            }

            for window_def in window_clause {
                visit_window(&window_def.window, visitor)?;
            }

            for expr in distinctness
                .iter()
                .flat_map(|distinctness| distinctness.exprs())
            {
                visit_expr_with_subqueries(expr, visitor)?;
            }
        }
        ast::OneSelect::Values(values) => {
            for row in values {
                for expr in row {
                    visit_expr_with_subqueries(expr, visitor)?;
                }
            }
        }
    }

    Ok(())
}

fn visit_from_clause<V: AstVisitor>(from_clause: &ast::FromClause, visitor: &mut V) -> Result<()> {
    visit_select_table(&from_clause.select, visitor)?;

    for join in &from_clause.joins {
        visit_select_table(&join.table, visitor)?;

        if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
            visit_expr_with_subqueries(expr, visitor)?;
        }
    }

    Ok(())
}

fn visit_select_table<V: AstVisitor>(
    select_table: &ast::SelectTable,
    visitor: &mut V,
) -> Result<()> {
    match select_table {
        ast::SelectTable::Table(name, _, _) => visitor.table(name),
        ast::SelectTable::Select { select, .. } => visit_select(select, visitor),
        ast::SelectTable::Sub(from_clause, _) => visit_from_clause(from_clause, visitor),
        ast::SelectTable::TableCall(name, args, _) => {
            visitor.table_function(name)?;
            for arg in args {
                visit_expr_with_subqueries(arg, visitor)?;
            }
            Ok(())
        }
    }
}

fn visit_window<V: AstVisitor>(window: &ast::Window, visitor: &mut V) -> Result<()> {
    for expr in &window.partition_by {
        visit_expr_with_subqueries(expr, visitor)?;
    }

    for sorted_col in &window.order_by {
        visit_expr_with_subqueries(&sorted_col.expr, visitor)?;
    }

    if visitor.enter_window_frame_bounds() {
        if let Some(frame_clause) = &window.frame_clause {
            visit_frame_bound(&frame_clause.start, visitor)?;
            if let Some(end_bound) = &frame_clause.end {
                visit_frame_bound(end_bound, visitor)?;
            }
        }
    }

    Ok(())
}

fn visit_frame_bound<V: AstVisitor>(bound: &ast::FrameBound, visitor: &mut V) -> Result<()> {
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            visit_expr_with_subqueries(expr, visitor)
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => Ok(()),
    }
}

pub fn walk_expr_with_subqueries<F>(expr: &ast::Expr, func: &mut F) -> Result<()>
where
    F: FnMut(&ast::Expr) -> Result<WalkControl>,
{
    visit_expr_with_subqueries(expr, func)
}

/// Walk an expression and the subqueries in it.
pub fn visit_expr_with_subqueries<V: AstVisitor>(expr: &ast::Expr, visitor: &mut V) -> Result<()> {
    walk_expr(expr, &mut |e| {
        let control = visitor.expr(e)?;
        if matches!(control, WalkControl::Continue) {
            match e {
                ast::Expr::Subquery(select) | ast::Expr::Exists(select) => {
                    visit_select(select, visitor)?;
                }
                ast::Expr::InSelect { lhs, rhs, .. } => {
                    if visitor.resolve_from_before_columns() {
                        // Name resolution reaches the left operand before the
                        // IN subquery; walk_expr would visit it after.
                        visit_expr_with_subqueries(lhs, visitor)?;
                        visit_select(rhs, visitor)?;
                        return Ok(WalkControl::SkipChildren);
                    }
                    visit_select(rhs, visitor)?;
                }
                _ => {}
            }
        }
        Ok(control)
    })?;
    Ok(())
}

pub fn validate_select_for_views(
    select_stmt: &ast::Select,
    resolver: &Resolver,
    view_database_id: usize,
) -> Result<()> {
    visit_select(
        select_stmt,
        &mut CrossDbReferences {
            resolver,
            view_database_id,
        },
    )?;

    // Expression subqueries are checked here, in source order and interleaved
    // with the window ORDER BY check, so the first problem in the text wins.
    walk_select_expressions(select_stmt, &mut |expr| {
        match expr {
            ast::Expr::FunctionCall {
                name,
                order_by,
                filter_over,
                ..
            } if !order_by.is_empty() && filter_over.over_clause.is_some() => {
                crate::bail_parse_error!(
                    "ORDER BY may not be used with non-aggregate {}()",
                    name.as_str()
                );
            }
            ast::Expr::Subquery(subquery) | ast::Expr::Exists(subquery) => {
                visit_select(
                    subquery,
                    &mut CrossDbReferences {
                        resolver,
                        view_database_id,
                    },
                )?;
            }
            ast::Expr::InSelect { rhs, .. } => {
                visit_select(
                    rhs,
                    &mut CrossDbReferences {
                        resolver,
                        view_database_id,
                    },
                )?;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
}

/// Rejects every table or table-valued function of another database that a
/// view body names.
struct CrossDbReferences<'r, 'a> {
    resolver: &'r Resolver<'a>,
    view_database_id: usize,
}

impl AstVisitor for CrossDbReferences<'_, '_> {
    /// Checks one query level: its FROM clause, joins, FROM subqueries,
    /// compounds and CTEs. Expression subqueries are left to
    /// `validate_select_for_views`, which checks them in source order and
    /// interleaved with the window ORDER BY check, so the first problem in
    /// the text is the one reported.
    fn expr(&mut self, expr: &ast::Expr) -> Result<WalkControl> {
        Ok(match expr {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                WalkControl::SkipChildren
            }
            _ => WalkControl::Continue,
        })
    }

    fn table(&mut self, name: &ast::QualifiedName) -> Result<()> {
        reject_cross_db_qualified_name(name, self.resolver, self.view_database_id)
    }

    fn table_function(&mut self, name: &ast::QualifiedName) -> Result<()> {
        reject_cross_db_qualified_name(name, self.resolver, self.view_database_id)
    }
}

/// A view may read any namespace of the database it is in, so a name is
/// cross-database when its namespace is of another database. A temp view
/// may read any database, as in SQLite.
fn reject_cross_db_qualified_name(
    qualified_name: &ast::QualifiedName,
    resolver: &Resolver,
    view_database_id: usize,
) -> Result<()> {
    if view_database_id == crate::TEMP_DB_ID {
        return Ok(());
    }
    if let Some(table_db_name) = &qualified_name.namespace {
        let is_cross_db = resolver
            .catalog
            .namespace_database_id(&normalize_ident(table_db_name.as_str()))
            != Some(view_database_id);
        if is_cross_db {
            return Err(crate::LimboError::ParseError(format!(
                "view cannot reference table in attached database: {qualified_name}"
            )));
        }
    }
    Ok(())
}

/// Returns true if the expression tree references a column whose normalized
/// name equals `col_name_normalized`.
pub fn check_expr_references_column(expr: &ast::Expr, col_name_normalized: &str) -> bool {
    let mut found = false;
    // The closure is infallible, so walk_expr cannot fail.
    let _ = walk_expr(expr, &mut |e| {
        if found {
            return Ok(WalkControl::SkipChildren);
        }
        match e {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                if name.as_str().eq_ignore_ascii_case(col_name_normalized) {
                    found = true;
                    return Ok(WalkControl::SkipChildren);
                }
            }
            ast::Expr::Qualified(_, col) | ast::Expr::DoublyQualified(_, _, col) => {
                if col.as_str().eq_ignore_ascii_case(col_name_normalized) {
                    found = true;
                    return Ok(WalkControl::SkipChildren);
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    found
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::ast::{self, Expr, FunctionTail, Literal, Name, Operator::*, Type, Variable};
    use crate::schema::Type as SchemaValueType;

    #[test]
    fn test_normalize_ident() {
        assert_eq!(normalize_ident("foo"), "foo");
        assert_eq!(normalize_ident("FOO"), "foo");
        // SQLite folds only ASCII; non-ASCII bytes pass through untouched.
        assert_eq!(normalize_ident("ὈΔΥΣΣΕΎΣ"), "ὈΔΥΣΣΕΎΣ");
        assert_eq!(normalize_ident("Foo_ΔΥΣ"), "foo_ΔΥΣ");
    }

    #[test]
    fn test_indexed_variable_comparison() {
        let expr1 = Expr::Variable(Variable::indexed(1u32.try_into().unwrap()));
        let expr2 = Expr::Variable(Variable::indexed(1u32.try_into().unwrap()));
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_named_variable_comparison() {
        let expr1 = Expr::Variable(Variable::named(":a".to_string(), 1u32.try_into().unwrap()));
        let expr2 = Expr::Variable(Variable::named(":a".to_string(), 1u32.try_into().unwrap()));
        assert!(exprs_are_equivalent(&expr1, &expr2));

        let expr1 = Expr::Variable(Variable::named(":a".to_string(), 1u32.try_into().unwrap()));
        let expr2 = Expr::Variable(Variable::named(":b".to_string(), 2u32.try_into().unwrap()));
        assert!(!exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_basic_addition_exprs_are_equivalent() {
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("826".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("389".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("389".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("826".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_addition_expressions_equivalent_normalized() {
        // Same types: 123.0 + 243.0 == 243.0 + 123.0 (commutative)
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));

        // Mixed types are NOT equivalent (different result types)
        let expr3 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("243".to_string()))),
        );
        let expr4 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
        );
        assert!(!exprs_are_equivalent(&expr3, &expr4));
    }

    #[test]
    fn test_subtraction_expressions_not_equivalent() {
        let expr3 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("364".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        let expr4 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("364".to_string()))),
        );
        assert!(!exprs_are_equivalent(&expr3, &expr4));
    }

    #[test]
    fn test_subtraction_expressions_normalized() {
        // Same types: 66.0 - 22.0 == 66.0 - 22.0
        let expr3 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        let expr4 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr3, &expr4));

        // Mixed types are NOT equivalent
        let expr5 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22".to_string()))),
        );
        let expr6 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        assert!(!exprs_are_equivalent(&expr5, &expr6));
    }

    #[test]
    fn test_expressions_equivalent_case_insensitive_functioncalls() {
        let func1 = Expr::FunctionCall {
            name: Name::exact("SUM".to_string()),
            distinctness: None,
            args: vec![Expr::Id(Name::exact("x".to_string())).into()],
            order_by: vec![],
            within_group: vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let func2 = Expr::FunctionCall {
            name: Name::exact("sum".to_string()),
            distinctness: None,
            args: vec![Expr::Id(Name::exact("x".to_string())).into()],
            order_by: vec![],
            within_group: vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        assert!(exprs_are_equivalent(&func1, &func2));

        let func3 = Expr::FunctionCall {
            name: Name::exact("SUM".to_string()),
            distinctness: Some(ast::Distinctness::Distinct),
            args: vec![Expr::Id(Name::exact("x".to_string())).into()],
            order_by: vec![],
            within_group: vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        assert!(!exprs_are_equivalent(&func1, &func3));
    }

    #[test]
    fn test_expressions_equivalent_identical_fn_with_distinct() {
        let sum = Expr::FunctionCall {
            name: Name::exact("SUM".to_string()),
            distinctness: None,
            args: vec![Expr::Id(Name::exact("x".to_string())).into()],
            order_by: vec![],
            within_group: vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let sum_distinct = Expr::FunctionCall {
            name: Name::exact("SUM".to_string()),
            distinctness: Some(ast::Distinctness::Distinct),
            args: vec![Expr::Id(Name::exact("x".to_string())).into()],
            order_by: vec![],
            within_group: vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        assert!(!exprs_are_equivalent(&sum, &sum_distinct));
    }

    #[test]
    fn test_expressions_equivalent_multiplication() {
        // Same types: 42.0 * 38.0 == 38.0 * 42.0 (commutative)
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("42.0".to_string()))),
            Multiply,
            Box::new(Expr::Literal(Literal::Numeric("38.0".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("38.0".to_string()))),
            Multiply,
            Box::new(Expr::Literal(Literal::Numeric("42.0".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_expressions_both_parenthesized_equivalent() {
        // Same types: (683 + 799) == 799 + 683 (commutative, integers only)
        let expr1 = Expr::Parenthesized(vec![Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("799".to_string()))),
        )
        .into()]);
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("799".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }
    #[test]
    fn test_expressions_parenthesized_equivalent() {
        let expr7 = Expr::Parenthesized(vec![Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("6".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("7".to_string()))),
        )
        .into()]);
        let expr8 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("6".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("7".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr7, &expr8));
    }

    #[test]
    fn test_like_expressions_equivalent() {
        let expr1 = Expr::Like {
            lhs: Box::new(Expr::Id(Name::exact("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        let expr2 = Expr::Like {
            lhs: Box::new(Expr::Id(Name::exact("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_expressions_equivalent_like_escaped() {
        let expr1 = Expr::Like {
            lhs: Box::new(Expr::Id(Name::exact("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        let expr2 = Expr::Like {
            lhs: Box::new(Expr::Id(Name::exact("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("#".to_string())))),
        };
        assert!(!exprs_are_equivalent(&expr1, &expr2));
    }
    #[test]
    fn test_expressions_equivalent_between() {
        let expr1 = Expr::Between {
            lhs: Box::new(Expr::Id(Name::exact("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("18".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        let expr2 = Expr::Between {
            lhs: Box::new(Expr::Id(Name::exact("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("18".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        assert!(exprs_are_equivalent(&expr1, &expr2));

        // differing BETWEEN bounds
        let expr3 = Expr::Between {
            lhs: Box::new(Expr::Id(Name::exact("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("20".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        assert!(!exprs_are_equivalent(&expr1, &expr3));
    }
    #[test]
    fn test_cast_exprs_equivalent() {
        let cast1 = Expr::Cast {
            expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
            type_name: Some(Type {
                name: "INTEGER".to_string(),
                size: None,
                array_dimensions: 0,
            }),
        };

        let cast2 = Expr::Cast {
            expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
            type_name: Some(Type {
                name: "integer".to_string(),
                size: None,
                array_dimensions: 0,
            }),
        };
        assert!(exprs_are_equivalent(&cast1, &cast2));
    }

    #[test]
    fn test_ident_equivalency() {
        assert!(check_ident_equivalency("\"foo\"", "foo"));
        assert!(check_ident_equivalency("[foo]", "foo"));
        assert!(check_ident_equivalency("`FOO`", "foo"));
        assert!(check_ident_equivalency("\"foo\"", "`FOO`"));
        assert!(!check_ident_equivalency("\"foo\"", "[bar]"));
        assert!(!check_ident_equivalency("foo", "\"bar\""));
    }

    #[test]
    fn test_simple_uri() {
        let uri = "file:/home/user/db.sqlite";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.authority, None);
    }

    #[test]
    fn test_uri_with_authority() {
        let uri = "file://localhost/home/user/db.sqlite";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.authority, Some("localhost"));
    }

    #[test]
    fn test_uri_with_invalid_authority() {
        let uri = "file://example.com/home/user/db.sqlite";
        let result = OpenOptions::parse(uri);
        assert!(result.is_err());
    }

    #[test]
    fn test_uri_with_query_params() {
        let uri = "file:/home/user/db.sqlite?vfs=unix&mode=ro&immutable=1";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, Some("unix".to_string()));
        assert_eq!(opts.mode, OpenMode::ReadOnly);
        assert!(opts.immutable);
    }

    #[test]
    fn test_uri_with_fragment() {
        let uri = "file:/home/user/db.sqlite#section1";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
    }

    #[test]
    fn test_uri_with_percent_encoding() {
        let uri = "file:/home/user/db%20with%20spaces.sqlite?vfs=unix";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db with spaces.sqlite");
        assert_eq!(opts.vfs, Some("unix".to_string()));
    }

    #[test]
    fn test_uri_without_scheme() {
        let uri = "/home/user/db.sqlite";
        let result = OpenOptions::parse(uri);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path, "/home/user/db.sqlite");
    }

    #[test]
    fn test_uri_with_empty_query() {
        let uri = "file:/home/user/db.sqlite?";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_uri_with_partial_query() {
        let uri = "file:/home/user/db.sqlite?mode=rw";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.mode, OpenMode::ReadWrite);
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_uri_windows_style_path() {
        let uri = "file:///C:/Users/test/db.sqlite";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/C:/Users/test/db.sqlite");
    }

    #[test]
    fn test_uri_with_only_query_params() {
        let uri = "file:?mode=memory&cache=shared";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "");
        assert_eq!(opts.mode, OpenMode::Memory);
        assert_eq!(opts.cache, CacheMode::Shared);
    }

    #[test]
    fn test_uri_with_only_fragment() {
        let uri = "file:#fragment";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "");
    }

    #[test]
    fn test_uri_with_invalid_scheme() {
        let uri = "http:/home/user/db.sqlite";
        let result = OpenOptions::parse(uri);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path, "http:/home/user/db.sqlite");
    }

    #[test]
    fn test_uri_with_multiple_query_params() {
        let uri = "file:/home/user/db.sqlite?vfs=unix&mode=rw&cache=private&immutable=0";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, Some("unix".to_string()));
        assert_eq!(opts.mode, OpenMode::ReadWrite);
        assert_eq!(opts.cache, CacheMode::Private);
        assert!(!opts.immutable);
    }

    #[test]
    fn test_uri_with_unknown_query_param() {
        let uri = "file:/home/user/db.sqlite?unknown=param";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_uri_with_multiple_equal_signs() {
        let uri = "file:/home/user/db.sqlite?vfs=unix=custom";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, Some("unix=custom".to_string()));
    }

    #[test]
    fn test_uri_with_trailing_slash() {
        let uri = "file:/home/user/db.sqlite/";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite/");
    }

    #[test]
    fn test_uri_with_encoded_characters_in_query() {
        let uri = "file:/home/user/db.sqlite?vfs=unix%20mode";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/user/db.sqlite");
        assert_eq!(opts.vfs, Some("unix mode".to_string()));
    }

    #[test]
    fn test_uri_windows_network_path() {
        let uri = "file://server/share/db.sqlite";
        let result = OpenOptions::parse(uri);
        assert!(result.is_err()); // non-localhost authority should fail
    }

    #[test]
    fn test_uri_windows_drive_letter_with_slash() {
        let uri = "file:///C:/database.sqlite";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/C:/database.sqlite");
    }

    #[test]
    fn test_localhost_with_double_slash_and_no_path() {
        let uri = "file://localhost";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "");
        assert_eq!(opts.authority, Some("localhost"));
    }

    #[test]
    fn test_uri_with_authority_and_only_a_slash_names_the_root_directory() {
        for uri in ["file:///", "file://localhost/"] {
            let opts = OpenOptions::parse(uri).unwrap();
            assert_eq!(opts.path, "/", "{uri}");
        }
    }

    #[test]
    fn test_uri_windows_drive_letter_without_slash() {
        let uri = "file:///C:/database.sqlite";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/C:/database.sqlite");
    }

    #[test]
    fn test_improper_mode() {
        // any other mode but ro, rwc, rw, memory should fail per sqlite

        let uri = "file:data.db?mode=readonly";
        let res = OpenOptions::parse(uri);
        assert!(res.is_err());
        // including empty
        let uri = "file:/home/user/db.sqlite?vfs=&mode=";
        let res = OpenOptions::parse(uri);
        assert!(res.is_err());
    }

    // Some examples from https://www.sqlite.org/c3ref/open.html#urifilenameexamples
    #[test]
    fn test_simple_file_current_dir() {
        let uri = "file:data.db";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "data.db");
        assert_eq!(opts.authority, None);
        assert_eq!(opts.vfs, None);
        assert_eq!(opts.mode, OpenMode::ReadWriteCreate);
    }

    #[test]
    fn test_simple_file_three_slash() {
        let uri = "file:///home/data/data.db";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/data/data.db");
        assert_eq!(opts.authority, None);
        assert_eq!(opts.vfs, None);
        assert_eq!(opts.mode, OpenMode::ReadWriteCreate);
    }

    #[test]
    fn test_simple_file_two_slash_localhost() {
        let uri = "file://localhost/home/fred/data.db";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/home/fred/data.db");
        assert_eq!(opts.authority, Some("localhost"));
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_windows_double_invalid() {
        let uri = "file://C:/home/fred/data.db?mode=ro";
        let opts = OpenOptions::parse(uri);
        assert!(opts.is_err());
    }

    #[test]
    fn test_simple_file_two_slash() {
        let uri = "file:///C:/Documents%20and%20Settings/fred/Desktop/data.db";
        let opts = OpenOptions::parse(uri).unwrap();
        assert_eq!(opts.path, "/C:/Documents and Settings/fred/Desktop/data.db");
        assert_eq!(opts.vfs, None);
    }

    #[test]
    fn test_decode_percent_basic() {
        assert_eq!(decode_percent("hello%20world"), "hello world");
        assert_eq!(decode_percent("file%3Adata.db"), "file:data.db");
        assert_eq!(decode_percent("path%2Fto%2Ffile"), "path/to/file");
    }

    #[test]
    fn test_decode_percent_edge_cases() {
        assert_eq!(decode_percent(""), "");
        assert_eq!(decode_percent("plain_text"), "plain_text");
        assert_eq!(
            decode_percent("%2Fhome%2Fuser%2Fdb.sqlite"),
            "/home/user/db.sqlite"
        );
        // multiple percent-encoded characters in sequence
        assert_eq!(decode_percent("%41%42%43"), "ABC");
        assert_eq!(decode_percent("%61%62%63"), "abc");
    }

    #[test]
    fn test_decode_percent_invalid_sequences() {
        // invalid percent encoding (single % without two hex digits)
        assert_eq!(decode_percent("hello%"), "hello%");
        // only one hex digit after %
        assert_eq!(decode_percent("file%2"), "file%2");
        // invalid hex digits (not 0-9, A-F, a-f)
        assert_eq!(decode_percent("file%2X.db"), "file%2X.db");

        // Incomplete sequence at the end, leave untouched
        assert_eq!(decode_percent("path%2Fto%2"), "path/to%2");
    }

    #[test]
    fn test_decode_percent_mixed_valid_invalid() {
        assert_eq!(decode_percent("hello%20world%"), "hello world%");
        assert_eq!(decode_percent("%2Fpath%2Xto%2Ffile"), "/path%2Xto/file");
        assert_eq!(decode_percent("file%3Adata.db%2"), "file:data.db%2");
    }

    #[test]
    fn test_decode_percent_special_characters() {
        assert_eq!(
            decode_percent("%21%40%23%24%25%5E%26%2A%28%29"),
            "!@#$%^&*()"
        );
        assert_eq!(decode_percent("%5B%5D%7B%7D%7C%5C%3A"), "[]{}|\\:");
    }

    #[test]
    fn test_decode_percent_unmodified_valid_text() {
        // ensure already valid text remains unchanged
        assert_eq!(
            decode_percent("C:/Users/Example/Database.sqlite"),
            "C:/Users/Example/Database.sqlite"
        );
        assert_eq!(
            decode_percent("/home/user/db.sqlite"),
            "/home/user/db.sqlite"
        );
    }

    #[test]
    fn test_text_to_integer() {
        assert_eq!(
            checked_cast_text_to_numeric("1", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1823400-00000", false).unwrap(),
            Value::from_i64(1823400)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-10000000", false).unwrap(),
            Value::from_i64(-10000000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("123xxx", false).unwrap(),
            Value::from_i64(123)
        );
        assert_eq!(
            checked_cast_text_to_numeric("9223372036854775807", false).unwrap(),
            Value::from_i64(i64::MAX)
        );
        // Overflow becomes Float (different from cast_text_to_integer which returned 0)
        assert_eq!(
            checked_cast_text_to_numeric("9223372036854775808", false).unwrap(),
            Value::from_f64(9223372036854775808.0)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-9223372036854775808", false).unwrap(),
            Value::from_i64(i64::MIN)
        );
        // Overflow becomes Float (different from cast_text_to_integer which returned 0)
        assert_eq!(
            checked_cast_text_to_numeric("-9223372036854775809", false).unwrap(),
            Value::from_f64(-9223372036854775808.0)
        );
        assert!(checked_cast_text_to_numeric("-", false).is_err());
    }

    #[test]
    fn test_text_to_real() {
        assert_eq!(
            checked_cast_text_to_numeric("1", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.0", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.0", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1e10", false).unwrap(),
            Value::from_i64(10_000_000_000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1e10", false).unwrap(),
            Value::from_i64(-10_000_000_000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1e-10", false).unwrap(),
            Value::from_f64(1e-10)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1e-10", false).unwrap(),
            Value::from_f64(-1e-10)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.123e10", false).unwrap(),
            Value::from_i64(11_230_000_000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.123e10", false).unwrap(),
            Value::from_i64(-11_230_000_000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.123e-10", false).unwrap(),
            Value::from_f64(1.123e-10)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.123-e-10", false).unwrap(),
            Value::from_f64(-1.123)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1-282584294928", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.7976931348623157e309", false).unwrap(),
            Value::from_f64(f64::INFINITY),
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.7976931348623157e308", false).unwrap(),
            Value::from_f64(f64::MIN),
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.7976931348623157e309", false).unwrap(),
            Value::from_f64(f64::NEG_INFINITY),
        );
        assert_eq!(
            checked_cast_text_to_numeric("1E", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1EE", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1E", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.23E", false).unwrap(),
            Value::from_f64(1.23)
        );
        assert_eq!(
            checked_cast_text_to_numeric(".1.23E-", false).unwrap(),
            Value::from_f64(0.1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("0", false).unwrap(),
            Value::from_i64(0)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-0", false).unwrap(),
            Value::from_i64(0)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-0", false).unwrap(),
            Value::from_i64(0)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-0.0", false).unwrap(),
            Value::from_i64(0)
        );
        assert_eq!(
            checked_cast_text_to_numeric("0.0", false).unwrap(),
            Value::from_i64(0)
        );
        assert!(checked_cast_text_to_numeric("-", false).is_err());
    }

    #[test]
    fn test_text_to_numeric() {
        assert_eq!(
            checked_cast_text_to_numeric("1", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1823400-00000", false).unwrap(),
            Value::from_i64(1823400)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-10000000", false).unwrap(),
            Value::from_i64(-10000000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("123xxx", false).unwrap(),
            Value::from_i64(123)
        );
        // vertical tab (0x0B) is whitespace to SQLite, unlike Rust's
        // is_ascii_whitespace(). https://github.com/tursodatabase/turso/issues/8454
        assert_eq!(
            checked_cast_text_to_numeric("\x0b12", false).unwrap(),
            Value::from_i64(12)
        );
        assert_eq!(
            checked_cast_text_to_numeric("12\x0b", false).unwrap(),
            Value::from_i64(12)
        );
        assert_eq!(
            checked_cast_text_to_numeric("9223372036854775807", false).unwrap(),
            Value::from_i64(i64::MAX)
        );
        assert_eq!(
            checked_cast_text_to_numeric("9223372036854775808", false).unwrap(),
            Value::from_f64(9223372036854775808.0)
        ); // Exceeds i64, becomes float
        assert_eq!(
            checked_cast_text_to_numeric("-9223372036854775808", false).unwrap(),
            Value::from_i64(i64::MIN)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-9223372036854775809", false).unwrap(),
            Value::from_f64(-9223372036854775808.0)
        ); // Exceeds i64, becomes float

        assert_eq!(
            checked_cast_text_to_numeric("1.0", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.0", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1e10", false).unwrap(),
            Value::from_i64(10_000_000_000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1e10", false).unwrap(),
            Value::from_i64(-10_000_000_000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1e-10", false).unwrap(),
            Value::from_f64(1e-10)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1e-10", false).unwrap(),
            Value::from_f64(-1e-10)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.123e10", false).unwrap(),
            Value::from_i64(11_230_000_000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.123e10", false).unwrap(),
            Value::from_i64(-11_230_000_000)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.123e-10", false).unwrap(),
            Value::from_f64(1.123e-10)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.123-e-10", false).unwrap(),
            Value::from_f64(-1.123)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1-282584294928", false).unwrap(),
            Value::from_i64(1)
        );
        assert!(checked_cast_text_to_numeric("xxx", false).is_err());
        assert_eq!(
            checked_cast_text_to_numeric("1.7976931348623157e309", false).unwrap(),
            Value::from_f64(f64::INFINITY)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.7976931348623157e308", false).unwrap(),
            Value::from_f64(f64::MIN)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.7976931348623157e309", false).unwrap(),
            Value::from_f64(f64::NEG_INFINITY)
        );

        assert_eq!(
            checked_cast_text_to_numeric("1E", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1EE", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1E", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.", false).unwrap(),
            Value::from_i64(1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-1.", false).unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.23E", false).unwrap(),
            Value::from_f64(1.23)
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.23E-", false).unwrap(),
            Value::from_f64(1.23)
        );

        assert_eq!(
            checked_cast_text_to_numeric("0", false).unwrap(),
            Value::from_i64(0)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-0", false).unwrap(),
            Value::from_i64(0)
        );
        assert_eq!(
            checked_cast_text_to_numeric("-0.0", false).unwrap(),
            Value::from_i64(0)
        );
        assert_eq!(
            checked_cast_text_to_numeric("0.0", false).unwrap(),
            Value::from_i64(0)
        );
        assert!(checked_cast_text_to_numeric("-", false).is_err());
        assert!(checked_cast_text_to_numeric("-e", false).is_err());
        assert!(checked_cast_text_to_numeric("-E", false).is_err());
    }

    #[test]
    fn test_parse_numeric_str_valid_integer() {
        assert_eq!(parse_numeric_str("123"), Ok((ValueType::Integer, "123")));
        assert_eq!(parse_numeric_str("-456"), Ok((ValueType::Integer, "-456")));
        assert_eq!(parse_numeric_str("+789"), Ok((ValueType::Integer, "+789")));
        assert_eq!(
            parse_numeric_str("000789"),
            Ok((ValueType::Integer, "000789"))
        );
    }

    #[test]
    fn test_parse_numeric_str_valid_float() {
        assert_eq!(
            parse_numeric_str("123.456"),
            Ok((ValueType::Float, "123.456"))
        );
        assert_eq!(
            parse_numeric_str("-0.789"),
            Ok((ValueType::Float, "-0.789"))
        );
        assert_eq!(
            parse_numeric_str("+0.789"),
            Ok((ValueType::Float, "+0.789"))
        );
        assert_eq!(parse_numeric_str("1e10"), Ok((ValueType::Float, "1e10")));
        assert_eq!(parse_numeric_str("+1e10"), Ok((ValueType::Float, "+1e10")));
        assert_eq!(
            parse_numeric_str("-1.23e-4"),
            Ok((ValueType::Float, "-1.23e-4"))
        );
        assert_eq!(
            parse_numeric_str("1.23E+4"),
            Ok((ValueType::Float, "1.23E+4"))
        );
        assert_eq!(parse_numeric_str("1.2.3"), Ok((ValueType::Float, "1.2")))
    }

    #[test]
    fn test_parse_numeric_str_edge_cases() {
        assert_eq!(parse_numeric_str("1e"), Ok((ValueType::Integer, "1")));
        assert_eq!(parse_numeric_str("1e-"), Ok((ValueType::Integer, "1")));
        assert_eq!(parse_numeric_str("1e+"), Ok((ValueType::Integer, "1")));
        assert_eq!(parse_numeric_str("-1e"), Ok((ValueType::Integer, "-1")));
        assert_eq!(parse_numeric_str("-1e-"), Ok((ValueType::Integer, "-1")));
        assert_eq!(parse_numeric_str("1.e"), Ok((ValueType::Float, "1.")));
    }

    #[test]
    fn test_parse_numeric_str_invalid() {
        assert_eq!(parse_numeric_str(""), Err(()));
        assert_eq!(parse_numeric_str("abc"), Err(()));
        assert_eq!(parse_numeric_str("-"), Err(()));
        assert_eq!(parse_numeric_str("+"), Err(()));
        assert_eq!(parse_numeric_str("e10"), Err(()));
        assert_eq!(parse_numeric_str(".e10"), Err(()));
        assert_eq!(parse_numeric_str("."), Err(()));
        assert_eq!(parse_numeric_str("-."), Err(()));
        assert_eq!(parse_numeric_str(" . "), Err(()));
        assert_eq!(parse_numeric_str("+e5"), Err(()));
    }

    #[test]
    fn test_parse_numeric_str_with_whitespace() {
        assert_eq!(parse_numeric_str("   123"), Ok((ValueType::Integer, "123")));
        assert_eq!(
            parse_numeric_str("  -456.78  "),
            Ok((ValueType::Float, "-456.78"))
        );
        assert_eq!(
            parse_numeric_str("  1.23e4  "),
            Ok((ValueType::Float, "1.23e4"))
        );
    }

    #[test]
    fn test_parse_numeric_str_leading_zeros() {
        assert_eq!(
            parse_numeric_str("000123"),
            Ok((ValueType::Integer, "000123"))
        );
        assert_eq!(
            parse_numeric_str("000.456"),
            Ok((ValueType::Float, "000.456"))
        );
        assert_eq!(
            parse_numeric_str("0001e3"),
            Ok((ValueType::Float, "0001e3"))
        );
    }

    #[test]
    fn test_parse_numeric_str_trailing_characters() {
        assert_eq!(parse_numeric_str("123abc"), Ok((ValueType::Integer, "123")));
        assert_eq!(
            parse_numeric_str("456.78xyz"),
            Ok((ValueType::Float, "456.78"))
        );
        assert_eq!(
            parse_numeric_str("1.23e4extra"),
            Ok((ValueType::Float, "1.23e4"))
        );
    }

    #[test]
    fn test_parse_numeric_literal_hex() {
        assert_eq!(
            parse_numeric_literal("0x1234").unwrap(),
            Value::from_i64(4660)
        );
        assert_eq!(
            parse_numeric_literal("0xFFFFFFFF").unwrap(),
            Value::from_i64(4294967295)
        );
        assert_eq!(
            parse_numeric_literal("0x7FFFFFFF").unwrap(),
            Value::from_i64(2147483647)
        );
        assert_eq!(
            parse_numeric_literal("0x7FFFFFFFFFFFFFFF").unwrap(),
            Value::from_i64(9223372036854775807)
        );
        assert_eq!(
            parse_numeric_literal("0xFFFFFFFFFFFFFFFF").unwrap(),
            Value::from_i64(-1)
        );
        assert_eq!(
            parse_numeric_literal("0x8000000000000000").unwrap(),
            Value::from_i64(-9223372036854775808)
        );

        assert_eq!(
            parse_numeric_literal("-0x1234").unwrap(),
            Value::from_i64(-4660)
        );
        // too big hex: matches SQLite's prepare-time error
        assert!(parse_numeric_literal("-0x8000000000000000")
            .unwrap_err()
            .to_string()
            .contains("hex literal too big: -0x8000000000000000"));
    }

    #[test]
    fn test_parse_numeric_literal_integer() {
        assert_eq!(parse_numeric_literal("123").unwrap(), Value::from_i64(123));
        assert_eq!(parse_numeric_literal("0").unwrap(), Value::from_i64(0));
        assert_eq!(parse_numeric_literal("007").unwrap(), Value::from_i64(7));
        assert_eq!(
            parse_numeric_literal("999999999999999999").unwrap(),
            Value::from_i64(999_999_999_999_999_999)
        );
        assert_eq!(
            parse_numeric_literal("9223372036854775807").unwrap(),
            Value::from_i64(i64::MAX)
        );
        assert_eq!(
            parse_numeric_literal("-9223372036854775808").unwrap(),
            Value::from_i64(i64::MIN)
        );
        assert_eq!(
            parse_numeric_literal("9_223_372_036_854_775_807").unwrap(),
            Value::from_i64(9223372036854775807)
        );
    }

    #[test]
    fn test_parse_numeric_literal_float() {
        assert_eq!(
            parse_numeric_literal("123.456").unwrap(),
            Value::from_f64(123.456)
        );
        assert_eq!(
            parse_numeric_literal(".123").unwrap(),
            Value::from_f64(0.123)
        );
        assert_eq!(
            parse_numeric_literal("1.23e10").unwrap(),
            Value::from_f64(1.23e10)
        );
        assert_eq!(
            parse_numeric_literal("1e-10").unwrap(),
            Value::from_f64(1e-10)
        );
        assert_eq!(
            parse_numeric_literal("1.23E+10").unwrap(),
            Value::from_f64(1.23e10)
        );
        assert_eq!(
            parse_numeric_literal("1.1_1").unwrap(),
            Value::from_f64(1.11)
        );

        // > i64::MAX, convert to float
        assert_eq!(
            parse_numeric_literal("9223372036854775808").unwrap(),
            Value::from_f64(9.223_372_036_854_776e18)
        );
        // < i64::MIN, convert to float
        assert_eq!(
            parse_numeric_literal("-9223372036854775809").unwrap(),
            Value::from_f64(-9.223_372_036_854_776e18)
        );
    }

    #[test]
    fn test_parse_pragma_bool() {
        assert!(parse_pragma_bool(&Expr::Literal(Literal::Numeric("1".into()))).unwrap(),);
        assert!(parse_pragma_bool(&Expr::Name(Name::exact("true".into()))).unwrap(),);
        assert!(parse_pragma_bool(&Expr::Name(Name::exact("on".into()))).unwrap(),);
        assert!(parse_pragma_bool(&Expr::Name(Name::exact("yes".into()))).unwrap(),);

        assert!(!parse_pragma_bool(&Expr::Literal(Literal::Numeric("0".into()))).unwrap(),);
        assert!(!parse_pragma_bool(&Expr::Name(Name::exact("false".into()))).unwrap(),);
        assert!(!parse_pragma_bool(&Expr::Name(Name::exact("off".into()))).unwrap(),);
        assert!(!parse_pragma_bool(&Expr::Name(Name::exact("no".into()))).unwrap(),);

        assert!(parse_pragma_bool(&Expr::Name(Name::exact("nono".into()))).is_err());
        assert!(parse_pragma_bool(&Expr::Name(Name::exact("10".into()))).is_err());
        assert!(parse_pragma_bool(&Expr::Name(Name::exact("-1".into()))).is_err());
    }

    #[test]
    fn test_type_from_name() {
        let tc = vec![
            ("", (SchemaValueType::Blob, false)),
            ("INTEGER", (SchemaValueType::Integer, true)),
            ("INT", (SchemaValueType::Integer, false)),
            ("CHAR", (SchemaValueType::Text, false)),
            ("CLOB", (SchemaValueType::Text, false)),
            ("TEXT", (SchemaValueType::Text, false)),
            ("BLOB", (SchemaValueType::Blob, false)),
            ("REAL", (SchemaValueType::Real, false)),
            ("FLOAT", (SchemaValueType::Real, false)),
            ("DOUBLE", (SchemaValueType::Real, false)),
            ("U128", (SchemaValueType::Numeric, false)),
        ];

        for (input, expected) in tc {
            let result = type_from_name(input);
            assert_eq!(result, expected, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_checked_cast_text_to_numeric_lossless_property() {
        assert_eq!(checked_cast_text_to_numeric("1.xx", true), Err(()));
        assert_eq!(checked_cast_text_to_numeric("abc", true), Err(()));
        assert_eq!(checked_cast_text_to_numeric("--5", true), Err(()));
        assert_eq!(checked_cast_text_to_numeric("12.34.56", true), Err(()));
        assert_eq!(checked_cast_text_to_numeric("", true), Err(()));
        assert_eq!(checked_cast_text_to_numeric(" ", true), Err(()));
        assert_eq!(
            checked_cast_text_to_numeric("0", true),
            Ok(Value::from_i64(0))
        );
        assert_eq!(
            checked_cast_text_to_numeric("42", true),
            Ok(Value::from_i64(42))
        );
        assert_eq!(
            checked_cast_text_to_numeric("-42", true),
            Ok(Value::from_i64(-42))
        );
        assert_eq!(
            checked_cast_text_to_numeric("999999999999", true),
            Ok(Value::from_i64(999_999_999_999))
        );
        assert_eq!(
            checked_cast_text_to_numeric("1.0", true),
            Ok(Value::from_i64(1))
        );
        assert_eq!(
            checked_cast_text_to_numeric("-3.22", true),
            Ok(Value::from_f64(-3.22))
        );
        assert_eq!(
            checked_cast_text_to_numeric("0.001", true),
            Ok(Value::from_f64(0.001))
        );
        assert_eq!(
            checked_cast_text_to_numeric("2e3", true),
            Ok(Value::from_i64(2000))
        );
        assert_eq!(
            checked_cast_text_to_numeric("-5.5e-2", true),
            Ok(Value::from_f64(-0.055))
        );
        assert_eq!(
            checked_cast_text_to_numeric(" 123 ", true),
            Ok(Value::from_i64(123))
        );
        assert_eq!(
            checked_cast_text_to_numeric("\t-3.22\n", true),
            Ok(Value::from_f64(-3.22))
        );
        assert_eq!(
            checked_cast_text_to_numeric("\u{00A0}123\u{00A0}", true),
            Err(())
        );
    }

    #[test]
    fn test_trim_ascii_whitespace_helper() {
        assert_eq!(trim_ascii_whitespace("  hello  "), "hello");
        assert_eq!(trim_ascii_whitespace("\t\nhello\r\n"), "hello");
        assert_eq!(trim_ascii_whitespace("hello"), "hello");
        assert_eq!(trim_ascii_whitespace("   "), "");
        assert_eq!(trim_ascii_whitespace(""), "");

        // vertical tab (0x0B) is whitespace to SQLite's ctype table, unlike
        // Rust's is_ascii_whitespace(). https://github.com/tursodatabase/turso/issues/8454
        assert_eq!(trim_ascii_whitespace("\x0bhello\x0b"), "hello");

        // non-breaking space should NOT be trimmed
        assert_eq!(
            trim_ascii_whitespace("\u{00A0}hello\u{00A0}"),
            "\u{00A0}hello\u{00A0}"
        );
        assert_eq!(
            trim_ascii_whitespace("  \u{00A0}hello\u{00A0}  "),
            "\u{00A0}hello\u{00A0}"
        );
    }

    #[test]
    fn test_cast_real_to_integer_limits() {
        // Values that are exactly representable in f64 and strictly within i64 range
        let max_exact = ((1i64 << 51) - 1) as f64;
        assert_eq!(cast_real_to_integer(max_exact), Ok((1i64 << 51) - 1));
        assert_eq!(cast_real_to_integer(-max_exact), Ok(-((1i64 << 51) - 1)));

        // Values beyond 2^51 are valid if they round-trip correctly and are strictly within bounds
        assert_eq!(cast_real_to_integer((1i64 << 51) as f64), Ok(1i64 << 51));
        assert_eq!(cast_real_to_integer((1i64 << 52) as f64), Ok(1i64 << 52));

        // 2^62 round-trips correctly and is strictly between i64::MIN and i64::MAX
        assert_eq!(cast_real_to_integer((1i64 << 62) as f64), Ok(1i64 << 62));

        // The original bug's value: 426601719749026560 should work
        assert_eq!(
            cast_real_to_integer(426601719749026560.0),
            Ok(426601719749026560)
        );

        // SQLite rejects boundary values: i64::MIN and i64::MAX exactly
        // (ix > SMALLEST_INT64 && ix < LARGEST_INT64 requires STRICT inequality)
        assert_eq!(cast_real_to_integer(i64::MIN as f64), Err(()));
        assert_eq!(cast_real_to_integer(i64::MAX as f64), Err(()));

        // Values at or beyond i64::MAX + 1 (2^63) should fail
        assert_eq!(cast_real_to_integer(9223372036854775808.0), Err(()));

        // Values below i64::MIN should fail
        assert_eq!(cast_real_to_integer(-9223372036854777856.0), Err(()));

        // Non-whole numbers should fail
        assert_eq!(cast_real_to_integer(1.5), Err(()));
        assert_eq!(cast_real_to_integer(-1.5), Err(()));

        // Non-finite values should fail
        assert_eq!(cast_real_to_integer(f64::INFINITY), Err(()));
        assert_eq!(cast_real_to_integer(f64::NEG_INFINITY), Err(()));
        assert_eq!(cast_real_to_integer(f64::NAN), Err(()));
    }

    #[test]
    fn capture_parameters_treats_numbered_markers_as_placeholders() {
        use crate::ast::{Literal, Variable};
        // Index-method patterns use ?N to share one captured argument
        // across several call sites (fts_score/fts_match); a numbered
        // marker carries its "?N" spelling as its name and must still
        // capture. Named parameters are not placeholders.
        let query = Expr::Literal(Literal::String("'x'".to_string()));

        let numbered = Expr::Variable(Variable::numbered(1.try_into().unwrap()));
        let captured = try_capture_parameters(&numbered, &query).unwrap();
        assert_eq!(captured.get(&1), Some(&query));

        let anonymous = Expr::Variable(Variable::indexed(1.try_into().unwrap()));
        assert!(try_capture_parameters(&anonymous, &query)
            .unwrap()
            .contains_key(&1));

        let named = Expr::Variable(Variable::named(":x", 1.try_into().unwrap()));
        assert!(try_capture_parameters(&named, &query).is_none());
    }
}
