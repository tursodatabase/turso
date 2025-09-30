#![allow(unused)]
use crate::incremental::view::IncrementalView;
use crate::numeric::StrToF64;
use crate::translate::emitter::TransactionMode;
use crate::translate::expr::WalkControl;
use crate::types::IOResult;
use crate::{
    schema::{self, BTreeTable, Column, Schema, Table, Type, DBSP_TABLE_PREFIX},
    translate::{collate::CollationSeq, expr::walk_expr, plan::JoinOrderMember},
    types::{Value, ValueType},
    LimboError, OpenFlags, Result, Statement, StepResult, SymbolTable,
};
use crate::{Connection, MvStore, IO};
use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex},
};
use tracing::{instrument, Level};
use turso_macros::match_ignore_ascii_case;
use turso_parser::ast::{
    self, fmt::ToTokens, Cmd, CreateTableBody, Expr, FunctionTail, Literal, Stmt, UnaryOperator,
};
use turso_parser::parser::Parser;

#[macro_export]
macro_rules! io_yield_one {
    ($c:expr) => {
        return Ok(IOResult::IO(IOCompletions::Single($c)));
    };
}
#[macro_export]
macro_rules! io_yield_many {
    ($v:expr) => {
        return Ok(IOResult::IO(IOCompletions::Many($v)));
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
    fn block<T>(&self, f: impl FnMut() -> Result<IOResult<T>>) -> Result<T>;
}

impl<I: ?Sized + IO> IOExt for I {
    fn block<T>(&self, mut f: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
        Ok(loop {
            match f()? {
                IOResult::Done(v) => break v,
                IOResult::IO(io) => io.wait(self)?,
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
    // so, we only need to convert identifier string to lowercase
    identifier.to_lowercase()
}

pub const PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX: &str = "sqlite_autoindex_";

/// Unparsed index that comes from a sql query, i.e not an automatic index
///
/// CREATE INDEX idx ON table_name(sql)
pub struct UnparsedFromSqlIndex {
    pub table_name: String,
    pub root_page: i64,
    pub sql: String,
}

#[instrument(skip_all, level = Level::INFO)]
pub fn parse_schema_rows(
    mut rows: Statement,
    schema: &mut Schema,
    syms: &SymbolTable,
    mv_tx: Option<(u64, TransactionMode)>,
    mut existing_views: HashMap<String, Arc<Mutex<IncrementalView>>>,
    mv_store: Option<&Arc<MvStore>>,
) -> Result<()> {
    rows.set_mv_tx(mv_tx);
    // TODO: if we IO, this unparsed indexes is lost. Will probably need some state between
    // IO runs
    let mut from_sql_indexes = Vec::with_capacity(10);
    let mut automatic_indices = std::collections::HashMap::with_capacity(10);

    // Store DBSP state table root pages: view_name -> dbsp_state_root_page
    let mut dbsp_state_roots: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    // Store DBSP state table index root pages: view_name -> dbsp_state_index_root_page
    let mut dbsp_state_index_roots: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    // Store materialized view info (SQL and root page) for later creation
    let mut materialized_view_info: std::collections::HashMap<String, (String, i64)> =
        std::collections::HashMap::new();
    loop {
        match rows.step()? {
            StepResult::Row => {
                let row = rows.row().unwrap();
                let ty = row.get::<&str>(0)?;
                let name = row.get::<&str>(1)?;
                let table_name = row.get::<&str>(2)?;
                let root_page = row.get::<i64>(3)?;
                let sql = row.get::<&str>(4).ok();
                schema.handle_schema_row(
                    ty,
                    name,
                    table_name,
                    root_page,
                    sql,
                    syms,
                    &mut from_sql_indexes,
                    &mut automatic_indices,
                    &mut dbsp_state_roots,
                    &mut dbsp_state_index_roots,
                    &mut materialized_view_info,
                    mv_store,
                )?
            }
            StepResult::IO => {
                // TODO: How do we ensure that the I/O we submitted to
                // read the schema is actually complete?
                rows.run_once()?;
            }
            StepResult::Interrupt => break,
            StepResult::Done => break,
            StepResult::Busy => break,
        }
    }

    schema.populate_indices(from_sql_indexes, automatic_indices)?;
    schema.populate_materialized_views(
        materialized_view_info,
        dbsp_state_roots,
        dbsp_state_index_roots,
    )?;

    Ok(())
}

fn cmp_numeric_strings(num_str: &str, other: &str) -> bool {
    match (num_str.parse::<f64>(), other.parse::<f64>()) {
        (Ok(num), Ok(other)) => num == other,
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

pub fn module_name_from_sql(sql: &str) -> Result<&str> {
    if let Some(start) = sql.find("USING") {
        let start = start + 6;
        // stop at the first space, semicolon, or parenthesis
        let end = sql[start..]
            .find(|c: char| c.is_whitespace() || c == ';' || c == '(')
            .unwrap_or(sql.len() - start)
            + start;
        Ok(sql[start..end].trim())
    } else {
        Err(LimboError::InvalidArgument(
            "Expected 'USING' in module name".to_string(),
        ))
    }
}

// CREATE VIRTUAL TABLE table_name USING module_name(arg1, arg2, ...);
// CREATE VIRTUAL TABLE table_name USING module_name;
pub fn module_args_from_sql(sql: &str) -> Result<Vec<turso_ext::Value>> {
    if !sql.contains('(') {
        return Ok(vec![]);
    }
    let start = sql.find('(').ok_or_else(|| {
        LimboError::InvalidArgument("Expected '(' in module argument list".to_string())
    })? + 1;
    let end = sql.rfind(')').ok_or_else(|| {
        LimboError::InvalidArgument("Expected ')' in module argument list".to_string())
    })?;

    let mut args = Vec::new();
    let mut current_arg = String::new();
    let mut chars = sql[start..end].chars().peekable();
    let mut in_quotes = false;

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                if in_quotes {
                    if chars.peek() == Some(&'\'') {
                        // Escaped quote
                        current_arg.push('\'');
                        chars.next();
                    } else {
                        in_quotes = false;
                        args.push(turso_ext::Value::from_text(current_arg.trim().to_string()));
                        current_arg.clear();
                        // Skip until comma or end
                        while let Some(&nc) = chars.peek() {
                            if nc == ',' {
                                chars.next(); // Consume comma
                                break;
                            } else if nc.is_whitespace() {
                                chars.next();
                            } else {
                                return Err(LimboError::InvalidArgument(
                                    "Unexpected characters after quoted argument".to_string(),
                                ));
                            }
                        }
                    }
                } else {
                    in_quotes = true;
                }
            }
            ',' => {
                if !in_quotes {
                    if !current_arg.trim().is_empty() {
                        args.push(turso_ext::Value::from_text(current_arg.trim().to_string()));
                        current_arg.clear();
                    }
                } else {
                    current_arg.push(c);
                }
            }
            _ => {
                current_arg.push(c);
            }
        }
    }

    if !current_arg.trim().is_empty() && !in_quotes {
        args.push(turso_ext::Value::from_text(current_arg.trim().to_string()));
    }

    if in_quotes {
        return Err(LimboError::InvalidArgument(
            "Unterminated string literal in module arguments".to_string(),
        ));
    }

    Ok(args)
}

pub fn check_literal_equivalency(lhs: &Literal, rhs: &Literal) -> bool {
    match (lhs, rhs) {
        (Literal::Numeric(n1), Literal::Numeric(n2)) => cmp_numeric_strings(n1, n2),
        (Literal::String(s1), Literal::String(s2)) => check_ident_equivalency(s1, s2),
        (Literal::Blob(b1), Literal::Blob(b2)) => b1 == b2,
        (Literal::Keyword(k1), Literal::Keyword(k2)) => check_ident_equivalency(k1, k2),
        (Literal::Null, Literal::Null) => true,
        (Literal::CurrentDate, Literal::CurrentDate) => true,
        (Literal::CurrentTime, Literal::CurrentTime) => true,
        (Literal::CurrentTimestamp, Literal::CurrentTimestamp) => true,
        _ => false,
    }
}

/// This function is used to determine whether two expressions are logically
/// equivalent in the context of queries, even if their representations
/// differ. e.g.: `SUM(x)` and `sum(x)`, `x + y` and `y + x`
pub fn exprs_are_equivalent(expr1: &Expr, expr2: &Expr) -> bool {
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
                && exprs_are_equivalent(lhs1, lhs2)
                && exprs_are_equivalent(start1, start2)
                && exprs_are_equivalent(end1, end2)
        }
        (Expr::Binary(lhs1, op1, rhs1), Expr::Binary(lhs2, op2, rhs2)) => {
            op1 == op2
                && ((exprs_are_equivalent(lhs1, lhs2) && exprs_are_equivalent(rhs1, rhs2))
                    || (op1.is_commutative()
                        && exprs_are_equivalent(lhs1, rhs2)
                        && exprs_are_equivalent(rhs1, lhs2)))
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
                    exprs_are_equivalent(w1, w2) && exprs_are_equivalent(t1, t2)
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
            exprs_are_equivalent(expr1, expr2)
                && match (type1, type2) {
                    (Some(t1), Some(t2)) => t1.name.eq_ignore_ascii_case(&t2.name),
                    _ => false,
                }
        }
        (Expr::Collate(expr1, collation1), Expr::Collate(expr2, collation2)) => {
            // TODO: check correctness of comparing colation as strings
            exprs_are_equivalent(expr1, expr2)
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
                filter_over: filter1,
            },
            Expr::FunctionCall {
                name: name2,
                distinctness: distinct2,
                args: args2,
                order_by: order2,
                filter_over: filter2,
            },
        ) => {
            name1.as_str().eq_ignore_ascii_case(name2.as_str())
                && distinct1 == distinct2
                && args1 == args2
                && order1 == order2
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
                    (Some(expr1), Some(expr2)) => exprs_are_equivalent(expr1, expr2),
                    (None, None) => true,
                    _ => false,
                }
                && filter1.over_clause == filter2.over_clause
        }
        (Expr::NotNull(expr1), Expr::NotNull(expr2)) => exprs_are_equivalent(expr1, expr2),
        (Expr::IsNull(expr1), Expr::IsNull(expr2)) => exprs_are_equivalent(expr1, expr2),
        (Expr::Literal(lit1), Expr::Literal(lit2)) => check_literal_equivalency(lit1, lit2),
        (Expr::Id(id1), Expr::Id(id2)) => check_ident_equivalency(id1.as_str(), id2.as_str()),
        (Expr::Unary(op1, expr1), Expr::Unary(op2, expr2)) => {
            op1 == op2 && exprs_are_equivalent(expr1, expr2)
        }
        // Variables that are not bound to a specific value, are treated as NULL
        // https://sqlite.org/lang_expr.html#varparam
        (Expr::Variable(var), Expr::Variable(var2)) if var.is_empty() && var2.is_empty() => false,
        // Named variables can be compared by their name
        (Expr::Variable(val), Expr::Variable(val2)) => val == val2,
        (Expr::Parenthesized(exprs1), Expr::Parenthesized(exprs2)) => {
            exprs1.len() == exprs2.len()
                && exprs1
                    .iter()
                    .zip(exprs2)
                    .all(|(e1, e2)| exprs_are_equivalent(e1, e2))
        }
        (Expr::Parenthesized(exprs1), exprs2) | (exprs2, Expr::Parenthesized(exprs1)) => {
            exprs1.len() == 1 && exprs_are_equivalent(&exprs1[0], exprs2)
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
                && exprs_are_equivalent(lhs1, lhs2)
                && rhs1.len() == rhs2.len()
                && rhs1
                    .iter()
                    .zip(rhs2.iter())
                    .all(|(a, b)| exprs_are_equivalent(a, b))
        }
        // fall back to naive equality check
        _ => expr1 == expr2,
    }
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

pub fn columns_from_create_table_body(
    body: &turso_parser::ast::CreateTableBody,
) -> crate::Result<Vec<Column>> {
    let CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
        return Err(crate::LimboError::ParseError(
            "CREATE TABLE body must contain columns and constraints".to_string(),
        ));
    };

    use turso_parser::ast;

    Ok(columns.iter().map(Into::into).collect())
}

/// This function checks if a given expression is a constant value that can be pushed down to the database engine.
/// It is expected to be called with the other half of a binary expression with an Expr::Column
pub fn can_pushdown_predicate(
    top_level_expr: &Expr,
    table_idx: usize,
    join_order: &[JoinOrderMember],
) -> Result<bool> {
    let mut can_pushdown = true;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                let join_idx = join_order
                    .iter()
                    .position(|t| t.table_id == *table)
                    .expect("table not found in join_order");
                can_pushdown &= join_idx <= table_idx;
            }
            Expr::FunctionCall { args, name, .. } => {
                let function = crate::function::Func::resolve_function(name.as_str(), args.len())?;
                // is deterministic
                can_pushdown &= function.is_deterministic();
            }
            _ => {}
        };
        Ok(WalkControl::Continue)
    })?;

    Ok(can_pushdown)
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
            let (authority, path) = after_slashes.split_once('/').unwrap_or((after_slashes, ""));

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

            if is_windows_path(path) {
                opts.path = normalize_windows_path(&decode_percent(path));
            } else if !path.is_empty() {
                opts.path = format!("/{}", decode_percent(path));
            } else {
                opts.path = String::new();
            }
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

/// When casting a TEXT value to INTEGER, the longest possible prefix of the value that can be interpreted as an integer number
/// is extracted from the TEXT value and the remainder ignored. Any leading spaces in the TEXT value when converting from TEXT to INTEGER are ignored.
/// If there is no prefix that can be interpreted as an integer number, the result of the conversion is 0.
/// If the prefix integer is greater than +9223372036854775807 then the result of the cast is exactly +9223372036854775807.
/// Similarly, if the prefix integer is less than -9223372036854775808 then the result of the cast is exactly -9223372036854775808.
/// When casting to INTEGER, if the text looks like a floating point value with an exponent, the exponent will be ignored
/// because it is no part of the integer prefix. For example, "CAST('123e+5' AS INTEGER)" results in 123, not in 12300000.
/// The CAST operator understands decimal integers only — conversion of hexadecimal integers stops at the "x" in the "0x" prefix of the hexadecimal integer string and thus result of the CAST is always zero.
pub fn cast_text_to_integer(text: &str) -> Value {
    let text = text.trim();
    if text.is_empty() {
        return Value::Integer(0);
    }
    if let Ok(i) = text.parse::<i64>() {
        return Value::Integer(i);
    }
    let bytes = text.as_bytes();
    let mut end = 0;
    if bytes[0] == b'-' {
        end = 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    text[..end]
        .parse::<i64>()
        .map_or(Value::Integer(0), Value::Integer)
}

/// When casting a TEXT value to REAL, the longest possible prefix of the value that can be interpreted
/// as a real number is extracted from the TEXT value and the remainder ignored. Any leading spaces in
/// the TEXT value are ignored when converging from TEXT to REAL.
/// If there is no prefix that can be interpreted as a real number, the result of the conversion is 0.0.
pub fn cast_text_to_real(text: &str) -> Value {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Value::Float(0.0);
    }
    let Ok((_, text)) = parse_numeric_str(trimmed) else {
        return Value::Float(0.0);
    };
    text.parse::<f64>().map_or(Value::Float(0.0), Value::Float)
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
pub fn checked_cast_text_to_numeric(text: &str) -> std::result::Result<Value, ()> {
    // sqlite will parse the first N digits of a string to numeric value, then determine
    // whether _that_ value is more likely a real or integer value. e.g.
    // '-100234-2344.23e14' evaluates to -100234 instead of -100234.0
    let (kind, text) = parse_numeric_str(text)?;
    match kind {
        ValueType::Integer => match text.parse::<i64>() {
            Ok(i) => Ok(Value::Integer(i)),
            Err(e) => {
                if matches!(
                    e.kind(),
                    std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow
                ) {
                    // if overflow, we return the representation as a real.
                    // we have to match sqlite exactly here, so we match sqlite3AtoF
                    let value = text.parse::<f64>().unwrap_or_default();
                    let factor = 10f64.powi(15 - value.abs().log10().ceil() as i32);
                    Ok(Value::Float((value * factor).round() / factor))
                } else {
                    Err(())
                }
            }
        },
        ValueType::Float => Ok(text.parse::<f64>().map_or(Value::Float(0.0), Value::Float)),
        _ => unreachable!(),
    }
}

fn parse_numeric_str(text: &str) -> Result<(ValueType, &str), ()> {
    let text = text.trim();
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
    if bytes[0] == b'-' {
        end = 1;
    }
    while end < bytes.len() {
        match bytes[end] {
            b'0'..=b'9' => end += 1,
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
    if end == 0 || (end == 1 && bytes[0] == b'-') {
        return Err(());
    }
    // edge case: if it ends with exponent, strip and cast valid digits as float
    let last = bytes[end - 1];
    if last.eq_ignore_ascii_case(&b'e') {
        return Ok((ValueType::Float, &text[0..end - 1]));
    // edge case: ends with extponent / sign
    } else if has_exponent && (last == b'-' || last == b'+') {
        return Ok((ValueType::Float, &text[0..end - 2]));
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

pub fn cast_text_to_numeric(txt: &str) -> Value {
    checked_cast_text_to_numeric(txt).unwrap_or(Value::Integer(0))
}

// Check if float can be losslessly converted to 51-bit integer
pub fn cast_real_to_integer(float: f64) -> std::result::Result<i64, ()> {
    let i = float as i64;
    if float == i as f64 && i.abs() < (1i64 << 51) {
        return Ok(i);
    }
    Err(())
}

// we don't need to verify the numeric literal here, as it is already verified by the parser
pub fn parse_numeric_literal(text: &str) -> Result<Value> {
    // a single extra underscore ("_") character can exist between any two digits
    let text = if text.contains('_') {
        std::borrow::Cow::Owned(text.replace('_', ""))
    } else {
        std::borrow::Cow::Borrowed(text)
    };

    if text.starts_with("0x") || text.starts_with("0X") {
        let value = u64::from_str_radix(&text[2..], 16)? as i64;
        return Ok(Value::Integer(value));
    } else if text.starts_with("-0x") || text.starts_with("-0X") {
        let value = u64::from_str_radix(&text[3..], 16)? as i64;
        if value == i64::MIN {
            return Err(LimboError::IntegerOverflow);
        }
        return Ok(Value::Integer(-value));
    }

    if let Ok(int_value) = text.parse::<i64>() {
        return Ok(Value::Integer(int_value));
    }

    let Some(StrToF64::Fractional(float) | StrToF64::Decimal(float)) =
        crate::numeric::str_to_f64(text)
    else {
        unreachable!();
    };
    Ok(Value::Float(float.into()))
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
        if let Value::Integer(x @ (0 | 1)) = number {
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

/// Extract column name from an expression (e.g., for SELECT clauses)
pub fn extract_column_name_from_expr(expr: impl AsRef<ast::Expr>) -> Option<String> {
    match expr.as_ref() {
        ast::Expr::Id(name) => Some(name.as_str().to_string()),
        ast::Expr::DoublyQualified(_, _, name) | ast::Expr::Qualified(_, name) => {
            Some(normalize_ident(name.as_str()))
        }
        _ => None,
    }
}

/// Information about a table referenced in a view
#[derive(Debug, Clone)]
pub struct ViewTable {
    /// The full table name (potentially including database qualifier like "main.customers")
    pub name: String,
    /// Optional alias (e.g., "c" in "FROM customers c")
    pub alias: Option<String>,
}

/// Information about a column in the view's output
#[derive(Debug, Clone)]
pub struct ViewColumn {
    /// Index into ViewColumnSchema.tables indicating which table this column comes from
    /// For computed columns or constants, this will be usize::MAX
    pub table_index: usize,
    /// The actual column definition
    pub column: Column,
}

/// Schema information for a view, tracking which columns come from which tables
#[derive(Debug, Clone)]
pub struct ViewColumnSchema {
    /// All tables referenced by the view (in order of appearance)
    pub tables: Vec<ViewTable>,
    /// The view's output columns with their table associations
    pub columns: Vec<ViewColumn>,
}

impl ViewColumnSchema {
    /// Get all columns as a flat vector (without table association info)
    pub fn flat_columns(&self) -> Vec<Column> {
        self.columns.iter().map(|vc| vc.column.clone()).collect()
    }

    /// Get columns that belong to a specific table
    pub fn table_columns(&self, table_index: usize) -> Vec<Column> {
        self.columns
            .iter()
            .filter(|vc| vc.table_index == table_index)
            .map(|vc| vc.column.clone())
            .collect()
    }
}

/// Extract column information from a SELECT statement for view creation
pub fn extract_view_columns(
    select_stmt: &ast::Select,
    schema: &Schema,
) -> Result<ViewColumnSchema> {
    let mut tables = Vec::new();
    let mut columns = Vec::new();
    let mut column_name_counts: HashMap<String, usize> = HashMap::new();

    // Navigate to the first SELECT in the statement
    if let ast::OneSelect::Select {
        ref from,
        columns: select_columns,
        ..
    } = &select_stmt.body.select
    {
        // First, extract all tables (from FROM clause and JOINs)
        if let Some(from) = from {
            // Add the main table from FROM clause
            match from.select.as_ref() {
                ast::SelectTable::Table(qualified_name, alias, _) => {
                    let table_name = if qualified_name.db_name.is_some() {
                        // Include database qualifier if present
                        qualified_name.to_string()
                    } else {
                        normalize_ident(qualified_name.name.as_str())
                    };
                    tables.push(ViewTable {
                        name: table_name.clone(),
                        alias: alias.as_ref().map(|a| match a {
                            ast::As::As(name) => normalize_ident(name.as_str()),
                            ast::As::Elided(name) => normalize_ident(name.as_str()),
                        }),
                    });
                }
                _ => {
                    // Handle other types like subqueries if needed
                }
            }

            // Add tables from JOINs
            for join in &from.joins {
                match join.table.as_ref() {
                    ast::SelectTable::Table(qualified_name, alias, _) => {
                        let table_name = if qualified_name.db_name.is_some() {
                            // Include database qualifier if present
                            qualified_name.to_string()
                        } else {
                            normalize_ident(qualified_name.name.as_str())
                        };
                        tables.push(ViewTable {
                            name: table_name.clone(),
                            alias: alias.as_ref().map(|a| match a {
                                ast::As::As(name) => normalize_ident(name.as_str()),
                                ast::As::Elided(name) => normalize_ident(name.as_str()),
                            }),
                        });
                    }
                    _ => {
                        // Handle other types like subqueries if needed
                    }
                }
            }
        }

        // Helper function to find table index by name or alias
        let find_table_index = |name: &str| -> Option<usize> {
            tables
                .iter()
                .position(|t| t.name == name || t.alias.as_ref().is_some_and(|a| a == name))
        };

        // Process each column in the SELECT list
        for result_col in select_columns.iter() {
            match result_col {
                ast::ResultColumn::Expr(expr, alias) => {
                    // Figure out which table this expression comes from
                    let table_index = match expr.as_ref() {
                        ast::Expr::Qualified(table_ref, _col_name) => {
                            // Column qualified with table name
                            find_table_index(table_ref.as_str())
                        }
                        ast::Expr::Id(_col_name) => {
                            // Unqualified column - would need to resolve based on schema
                            // For now, assume it's from the first table if there is one
                            if !tables.is_empty() {
                                Some(0)
                            } else {
                                None
                            }
                        }
                        _ => None, // Expression, literal, etc.
                    };

                    let col_name = alias
                        .as_ref()
                        .map(|a| match a {
                            ast::As::Elided(name) => name.as_str().to_string(),
                            ast::As::As(name) => name.as_str().to_string(),
                        })
                        .or_else(|| extract_column_name_from_expr(expr))
                        .unwrap_or_else(|| {
                            // If we can't extract a simple column name, use the expression itself
                            expr.to_string()
                        });

                    columns.push(ViewColumn {
                        table_index: table_index.unwrap_or(usize::MAX),
                        column: Column {
                            name: Some(col_name),
                            ty: Type::Text, // Default to TEXT, could be refined with type analysis
                            ty_str: "TEXT".to_string(),
                            primary_key: false,
                            is_rowid_alias: false,
                            notnull: false,
                            default: None,
                            unique: false,
                            collation: None,
                            hidden: false,
                        },
                    });
                }
                ast::ResultColumn::Star => {
                    // For SELECT *, expand to all columns from all tables
                    for (table_idx, table) in tables.iter().enumerate() {
                        if let Some(table_obj) = schema.get_table(&table.name) {
                            for table_column in table_obj.columns() {
                                let col_name =
                                    table_column.name.clone().unwrap_or_else(|| "?".to_string());

                                // Handle duplicate column names by adding suffix
                                let final_name =
                                    if let Some(count) = column_name_counts.get_mut(&col_name) {
                                        *count += 1;
                                        format!("{}:{}", col_name, *count - 1)
                                    } else {
                                        column_name_counts.insert(col_name.clone(), 1);
                                        col_name.clone()
                                    };

                                columns.push(ViewColumn {
                                    table_index: table_idx,
                                    column: Column {
                                        name: Some(final_name),
                                        ty: table_column.ty,
                                        ty_str: table_column.ty_str.clone(),
                                        primary_key: false,
                                        is_rowid_alias: false,
                                        notnull: false,
                                        default: None,
                                        unique: false,
                                        collation: table_column.collation,
                                        hidden: false,
                                    },
                                });
                            }
                        }
                    }

                    // If no tables, create a placeholder
                    if tables.is_empty() {
                        columns.push(ViewColumn {
                            table_index: usize::MAX,
                            column: Column {
                                name: Some("*".to_string()),
                                ty: Type::Text,
                                ty_str: "TEXT".to_string(),
                                primary_key: false,
                                is_rowid_alias: false,
                                notnull: false,
                                default: None,
                                unique: false,
                                collation: None,
                                hidden: false,
                            },
                        });
                    }
                }
                ast::ResultColumn::TableStar(table_ref) => {
                    // For table.*, expand to all columns from the specified table
                    let table_name_str = normalize_ident(table_ref.as_str());
                    if let Some(table_idx) = find_table_index(&table_name_str) {
                        if let Some(table) = schema.get_table(&tables[table_idx].name) {
                            for table_column in table.columns() {
                                let col_name =
                                    table_column.name.clone().unwrap_or_else(|| "?".to_string());

                                // Handle duplicate column names by adding suffix
                                let final_name =
                                    if let Some(count) = column_name_counts.get_mut(&col_name) {
                                        *count += 1;
                                        format!("{}:{}", col_name, *count - 1)
                                    } else {
                                        column_name_counts.insert(col_name.clone(), 1);
                                        col_name.clone()
                                    };

                                columns.push(ViewColumn {
                                    table_index: table_idx,
                                    column: Column {
                                        name: Some(final_name),
                                        ty: table_column.ty,
                                        ty_str: table_column.ty_str.clone(),
                                        primary_key: false,
                                        is_rowid_alias: false,
                                        notnull: false,
                                        default: None,
                                        unique: false,
                                        collation: table_column.collation,
                                        hidden: false,
                                    },
                                });
                            }
                        } else {
                            // Table not found, create placeholder
                            columns.push(ViewColumn {
                                table_index: usize::MAX,
                                column: Column {
                                    name: Some(format!("{table_name_str}.*")),
                                    ty: Type::Text,
                                    ty_str: "TEXT".to_string(),
                                    primary_key: false,
                                    is_rowid_alias: false,
                                    notnull: false,
                                    default: None,
                                    unique: false,
                                    collation: None,
                                    hidden: false,
                                },
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(ViewColumnSchema { tables, columns })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::schema::Type as SchemaValueType;
    use turso_parser::ast::{self, Expr, Literal, Name, Operator::*, Type};

    #[test]
    fn test_normalize_ident() {
        assert_eq!(normalize_ident("foo"), "foo");
        assert_eq!(normalize_ident("FOO"), "foo");
        assert_eq!(normalize_ident("ὈΔΥΣΣΕΎΣ"), "ὀδυσσεύς");
    }

    #[test]
    fn test_anonymous_variable_comparison() {
        let expr1 = Expr::Variable("".to_string());
        let expr2 = Expr::Variable("".to_string());
        assert!(!exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_named_variable_comparison() {
        let expr1 = Expr::Variable("1".to_string());
        let expr2 = Expr::Variable("1".to_string());
        assert!(exprs_are_equivalent(&expr1, &expr2));

        let expr1 = Expr::Variable("1".to_string());
        let expr2 = Expr::Variable("2".to_string());
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
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("243".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
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
        let expr3 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22".to_string()))),
        );
        let expr4 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr3, &expr4));
    }

    #[test]
    fn test_expressions_equivalent_case_insensitive_functioncalls() {
        let func1 = Expr::FunctionCall {
            name: Name::exact("SUM".to_string()),
            distinctness: None,
            args: vec![Expr::Id(Name::exact("x".to_string())).into()],
            order_by: vec![],
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
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        assert!(!exprs_are_equivalent(&sum, &sum_distinct));
    }

    #[test]
    fn test_expressions_equivalent_multiplication() {
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("42.0".to_string()))),
            Multiply,
            Box::new(Expr::Literal(Literal::Numeric("38".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("38.0".to_string()))),
            Multiply,
            Box::new(Expr::Literal(Literal::Numeric("42".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_expressions_both_parenthesized_equivalent() {
        let expr1 = Expr::Parenthesized(vec![Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("799.0".to_string()))),
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
            }),
        };

        let cast2 = Expr::Cast {
            expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
            type_name: Some(Type {
                name: "integer".to_string(),
                size: None,
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
        assert_eq!(cast_text_to_integer("1"), Value::Integer(1),);
        assert_eq!(cast_text_to_integer("-1"), Value::Integer(-1),);
        assert_eq!(
            cast_text_to_integer("1823400-00000"),
            Value::Integer(1823400),
        );
        assert_eq!(cast_text_to_integer("-10000000"), Value::Integer(-10000000),);
        assert_eq!(cast_text_to_integer("123xxx"), Value::Integer(123),);
        assert_eq!(
            cast_text_to_integer("9223372036854775807"),
            Value::Integer(i64::MAX),
        );
        assert_eq!(
            cast_text_to_integer("9223372036854775808"),
            Value::Integer(0),
        );
        assert_eq!(
            cast_text_to_integer("-9223372036854775808"),
            Value::Integer(i64::MIN),
        );
        assert_eq!(
            cast_text_to_integer("-9223372036854775809"),
            Value::Integer(0),
        );
        assert_eq!(cast_text_to_integer("-"), Value::Integer(0),);
    }

    #[test]
    fn test_text_to_real() {
        assert_eq!(cast_text_to_real("1"), Value::Float(1.0));
        assert_eq!(cast_text_to_real("-1"), Value::Float(-1.0));
        assert_eq!(cast_text_to_real("1.0"), Value::Float(1.0));
        assert_eq!(cast_text_to_real("-1.0"), Value::Float(-1.0));
        assert_eq!(cast_text_to_real("1e10"), Value::Float(1e10));
        assert_eq!(cast_text_to_real("-1e10"), Value::Float(-1e10));
        assert_eq!(cast_text_to_real("1e-10"), Value::Float(1e-10));
        assert_eq!(cast_text_to_real("-1e-10"), Value::Float(-1e-10));
        assert_eq!(cast_text_to_real("1.123e10"), Value::Float(1.123e10));
        assert_eq!(cast_text_to_real("-1.123e10"), Value::Float(-1.123e10));
        assert_eq!(cast_text_to_real("1.123e-10"), Value::Float(1.123e-10));
        assert_eq!(cast_text_to_real("-1.123-e-10"), Value::Float(-1.123));
        assert_eq!(cast_text_to_real("1-282584294928"), Value::Float(1.0));
        assert_eq!(
            cast_text_to_real("1.7976931348623157e309"),
            Value::Float(f64::INFINITY),
        );
        assert_eq!(
            cast_text_to_real("-1.7976931348623157e308"),
            Value::Float(f64::MIN),
        );
        assert_eq!(
            cast_text_to_real("-1.7976931348623157e309"),
            Value::Float(f64::NEG_INFINITY),
        );
        assert_eq!(cast_text_to_real("1E"), Value::Float(1.0));
        assert_eq!(cast_text_to_real("1EE"), Value::Float(1.0));
        assert_eq!(cast_text_to_real("-1E"), Value::Float(-1.0));
        assert_eq!(cast_text_to_real("1."), Value::Float(1.0));
        assert_eq!(cast_text_to_real("-1."), Value::Float(-1.0));
        assert_eq!(cast_text_to_real("1.23E"), Value::Float(1.23));
        assert_eq!(cast_text_to_real(".1.23E-"), Value::Float(0.1));
        assert_eq!(cast_text_to_real("0"), Value::Float(0.0));
        assert_eq!(cast_text_to_real("-0"), Value::Float(0.0));
        assert_eq!(cast_text_to_real("-0"), Value::Float(0.0));
        assert_eq!(cast_text_to_real("-0.0"), Value::Float(0.0));
        assert_eq!(cast_text_to_real("0.0"), Value::Float(0.0));
        assert_eq!(cast_text_to_real("-"), Value::Float(0.0));
    }

    #[test]
    fn test_text_to_numeric() {
        assert_eq!(cast_text_to_numeric("1"), Value::Integer(1));
        assert_eq!(cast_text_to_numeric("-1"), Value::Integer(-1));
        assert_eq!(
            cast_text_to_numeric("1823400-00000"),
            Value::Integer(1823400)
        );
        assert_eq!(cast_text_to_numeric("-10000000"), Value::Integer(-10000000));
        assert_eq!(cast_text_to_numeric("123xxx"), Value::Integer(123));
        assert_eq!(
            cast_text_to_numeric("9223372036854775807"),
            Value::Integer(i64::MAX)
        );
        assert_eq!(
            cast_text_to_numeric("9223372036854775808"),
            Value::Float(9.22337203685478e18)
        ); // Exceeds i64, becomes float
        assert_eq!(
            cast_text_to_numeric("-9223372036854775808"),
            Value::Integer(i64::MIN)
        );
        assert_eq!(
            cast_text_to_numeric("-9223372036854775809"),
            Value::Float(-9.22337203685478e18)
        ); // Exceeds i64, becomes float

        assert_eq!(cast_text_to_numeric("1.0"), Value::Float(1.0));
        assert_eq!(cast_text_to_numeric("-1.0"), Value::Float(-1.0));
        assert_eq!(cast_text_to_numeric("1e10"), Value::Float(1e10));
        assert_eq!(cast_text_to_numeric("-1e10"), Value::Float(-1e10));
        assert_eq!(cast_text_to_numeric("1e-10"), Value::Float(1e-10));
        assert_eq!(cast_text_to_numeric("-1e-10"), Value::Float(-1e-10));
        assert_eq!(cast_text_to_numeric("1.123e10"), Value::Float(1.123e10));
        assert_eq!(cast_text_to_numeric("-1.123e10"), Value::Float(-1.123e10));
        assert_eq!(cast_text_to_numeric("1.123e-10"), Value::Float(1.123e-10));
        assert_eq!(cast_text_to_numeric("-1.123-e-10"), Value::Float(-1.123));
        assert_eq!(cast_text_to_numeric("1-282584294928"), Value::Integer(1));
        assert_eq!(cast_text_to_numeric("xxx"), Value::Integer(0));
        assert_eq!(
            cast_text_to_numeric("1.7976931348623157e309"),
            Value::Float(f64::INFINITY)
        );
        assert_eq!(
            cast_text_to_numeric("-1.7976931348623157e308"),
            Value::Float(f64::MIN)
        );
        assert_eq!(
            cast_text_to_numeric("-1.7976931348623157e309"),
            Value::Float(f64::NEG_INFINITY)
        );

        assert_eq!(cast_text_to_numeric("1E"), Value::Float(1.0));
        assert_eq!(cast_text_to_numeric("1EE"), Value::Float(1.0));
        assert_eq!(cast_text_to_numeric("-1E"), Value::Float(-1.0));
        assert_eq!(cast_text_to_numeric("1."), Value::Float(1.0));
        assert_eq!(cast_text_to_numeric("-1."), Value::Float(-1.0));
        assert_eq!(cast_text_to_numeric("1.23E"), Value::Float(1.23));
        assert_eq!(cast_text_to_numeric("1.23E-"), Value::Float(1.23));

        assert_eq!(cast_text_to_numeric("0"), Value::Integer(0));
        assert_eq!(cast_text_to_numeric("-0"), Value::Integer(0));
        assert_eq!(cast_text_to_numeric("-0.0"), Value::Float(0.0));
        assert_eq!(cast_text_to_numeric("0.0"), Value::Float(0.0));
        assert_eq!(cast_text_to_numeric("-"), Value::Integer(0));
        assert_eq!(cast_text_to_numeric("-e"), Value::Integer(0));
        assert_eq!(cast_text_to_numeric("-E"), Value::Integer(0));
    }

    #[test]
    fn test_parse_numeric_str_valid_integer() {
        assert_eq!(parse_numeric_str("123"), Ok((ValueType::Integer, "123")));
        assert_eq!(parse_numeric_str("-456"), Ok((ValueType::Integer, "-456")));
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
        assert_eq!(parse_numeric_str("1e10"), Ok((ValueType::Float, "1e10")));
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
        assert_eq!(parse_numeric_str("1e"), Ok((ValueType::Float, "1")));
        assert_eq!(parse_numeric_str("1e-"), Ok((ValueType::Float, "1")));
        assert_eq!(parse_numeric_str("1e+"), Ok((ValueType::Float, "1")));
        assert_eq!(parse_numeric_str("-1e"), Ok((ValueType::Float, "-1")));
        assert_eq!(parse_numeric_str("-1e-"), Ok((ValueType::Float, "-1")));
    }

    #[test]
    fn test_parse_numeric_str_invalid() {
        assert_eq!(parse_numeric_str(""), Err(()));
        assert_eq!(parse_numeric_str("abc"), Err(()));
        assert_eq!(parse_numeric_str("-"), Err(()));
        assert_eq!(parse_numeric_str("e10"), Err(()));
        assert_eq!(parse_numeric_str(".e10"), Err(()));
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
    fn test_module_name_basic() {
        let sql = "CREATE VIRTUAL TABLE x USING y;";
        assert_eq!(module_name_from_sql(sql).unwrap(), "y");
    }

    #[test]
    fn test_module_name_with_args() {
        let sql = "CREATE VIRTUAL TABLE x USING modname('a', 'b');";
        assert_eq!(module_name_from_sql(sql).unwrap(), "modname");
    }

    #[test]
    fn test_module_name_missing_using() {
        let sql = "CREATE VIRTUAL TABLE x (a, b);";
        assert!(module_name_from_sql(sql).is_err());
    }

    #[test]
    fn test_module_name_no_semicolon() {
        let sql = "CREATE VIRTUAL TABLE x USING limbo(a, b)";
        assert_eq!(module_name_from_sql(sql).unwrap(), "limbo");
    }

    #[test]
    fn test_module_name_no_semicolon_or_args() {
        let sql = "CREATE VIRTUAL TABLE x USING limbo";
        assert_eq!(module_name_from_sql(sql).unwrap(), "limbo");
    }

    #[test]
    fn test_module_args_none() {
        let sql = "CREATE VIRTUAL TABLE x USING modname;";
        let args = module_args_from_sql(sql).unwrap();
        assert_eq!(args.len(), 0);
    }

    #[test]
    fn test_module_args_basic() {
        let sql = "CREATE VIRTUAL TABLE x USING modname('arg1', 'arg2');";
        let args = module_args_from_sql(sql).unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!("arg1", args[0].to_text().unwrap());
        assert_eq!("arg2", args[1].to_text().unwrap());
        for arg in args {
            unsafe { arg.__free_internal_type() }
        }
    }

    #[test]
    fn test_module_args_with_escaped_quote() {
        let sql = "CREATE VIRTUAL TABLE x USING modname('a''b', 'c');";
        let args = module_args_from_sql(sql).unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0].to_text().unwrap(), "a'b");
        assert_eq!(args[1].to_text().unwrap(), "c");
        for arg in args {
            unsafe { arg.__free_internal_type() }
        }
    }

    #[test]
    fn test_module_args_unterminated_string() {
        let sql = "CREATE VIRTUAL TABLE x USING modname('arg1, 'arg2');";
        assert!(module_args_from_sql(sql).is_err());
    }

    #[test]
    fn test_module_args_extra_garbage_after_quote() {
        let sql = "CREATE VIRTUAL TABLE x USING modname('arg1'x);";
        assert!(module_args_from_sql(sql).is_err());
    }

    #[test]
    fn test_module_args_trailing_comma() {
        let sql = "CREATE VIRTUAL TABLE x USING modname('arg1',);";
        let args = module_args_from_sql(sql).unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!("arg1", args[0].to_text().unwrap());
        for arg in args {
            unsafe { arg.__free_internal_type() }
        }
    }

    #[test]
    fn test_parse_numeric_literal_hex() {
        assert_eq!(
            parse_numeric_literal("0x1234").unwrap(),
            Value::Integer(4660)
        );
        assert_eq!(
            parse_numeric_literal("0xFFFFFFFF").unwrap(),
            Value::Integer(4294967295)
        );
        assert_eq!(
            parse_numeric_literal("0x7FFFFFFF").unwrap(),
            Value::Integer(2147483647)
        );
        assert_eq!(
            parse_numeric_literal("0x7FFFFFFFFFFFFFFF").unwrap(),
            Value::Integer(9223372036854775807)
        );
        assert_eq!(
            parse_numeric_literal("0xFFFFFFFFFFFFFFFF").unwrap(),
            Value::Integer(-1)
        );
        assert_eq!(
            parse_numeric_literal("0x8000000000000000").unwrap(),
            Value::Integer(-9223372036854775808)
        );

        assert_eq!(
            parse_numeric_literal("-0x1234").unwrap(),
            Value::Integer(-4660)
        );
        // too big hex
        assert!(parse_numeric_literal("-0x8000000000000000").is_err());
    }

    #[test]
    fn test_parse_numeric_literal_integer() {
        assert_eq!(parse_numeric_literal("123").unwrap(), Value::Integer(123));
        assert_eq!(
            parse_numeric_literal("9_223_372_036_854_775_807").unwrap(),
            Value::Integer(9223372036854775807)
        );
    }

    #[test]
    fn test_parse_numeric_literal_float() {
        assert_eq!(
            parse_numeric_literal("123.456").unwrap(),
            Value::Float(123.456)
        );
        assert_eq!(parse_numeric_literal(".123").unwrap(), Value::Float(0.123));
        assert_eq!(
            parse_numeric_literal("1.23e10").unwrap(),
            Value::Float(1.23e10)
        );
        assert_eq!(parse_numeric_literal("1e-10").unwrap(), Value::Float(1e-10));
        assert_eq!(
            parse_numeric_literal("1.23E+10").unwrap(),
            Value::Float(1.23e10)
        );
        assert_eq!(parse_numeric_literal("1.1_1").unwrap(), Value::Float(1.11));

        // > i64::MAX, convert to float
        assert_eq!(
            parse_numeric_literal("9223372036854775808").unwrap(),
            Value::Float(9.223_372_036_854_776e18)
        );
        // < i64::MIN, convert to float
        assert_eq!(
            parse_numeric_literal("-9223372036854775809").unwrap(),
            Value::Float(-9.223_372_036_854_776e18)
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
}
