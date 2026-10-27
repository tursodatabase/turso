use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{Hash, Hasher},
    str::FromStr as _,
};

use icu_collator::{options::CollatorOptions, Collator, CollatorBorrowed};
use icu_locale::Locale;
use turso_parser::ast::{Expr, TableInternalId, UnaryOperator};

use crate::{
    sync::{LazyLock, Mutex, RwLock},
    translate::{emitter::Resolver, plan::TableReferences},
    types::ValueRef,
    vdbe::hash_table::normalized_f64_bits,
    LimboError, Numeric, Result,
};

/// **Pre defined collation sequences**\
/// Collating functions only matter when comparing string values.
/// Numeric values are always compared numerically, and BLOBs are always compared byte-by-byte using memcmp().
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum CollationSeq {
    Unset,
    Binary,
    NoCase,
    Rtrim,
    Locale(LocaleCollationId),
    /// Name/id token for a connection-owned callback. The comparison itself
    /// must be resolved through `Connection` at runtime.
    Custom(u32),
}

#[derive(Default)]
struct CustomCollationNames {
    // Custom collation callbacks are connection-local. This process-wide table
    // only interns names so bytecode can carry compact `CollationSeq::Custom`
    // tokens and resolve the callback through the active connection at runtime.
    by_name: HashMap<String, u32>,
    by_id: HashMap<u32, String>,
}

static CUSTOM_COLLATION_NAMES: LazyLock<Mutex<CustomCollationNames>> =
    LazyLock::new(|| Mutex::new(CustomCollationNames::default()));

impl CollationSeq {
    pub fn new(collation: &str) -> crate::Result<Self> {
        match crate::util::normalize_ident(collation).as_str() {
            "binary" => return Ok(Self::Binary),
            "nocase" => return Ok(Self::NoCase),
            "rtrim" => return Ok(Self::Rtrim),
            _ => {}
        }

        LocaleCollationRegistry::global()
            .get_or_register(collation)
            .map(Self::Locale)
    }

    #[inline]
    /// Returns the collation, defaulting to BINARY if unset
    pub const fn from_bits(bits: u8) -> Self {
        match bits {
            2 => Self::NoCase,
            3 => Self::Rtrim,
            _ => Self::Binary,
        }
    }

    #[inline]
    pub const fn to_bits(self) -> u16 {
        match self {
            Self::Unset => 0,
            Self::Binary => 1,
            Self::NoCase => 2,
            Self::Rtrim => 3,
            Self::Locale(id) => id.to_bits(),
            Self::Custom(_) => 0,
        }
    }

    #[inline]
    pub const fn from_storage_bits(bits: u16) -> Self {
        match bits {
            0 => Self::Unset,
            1 => Self::Binary,
            2 => Self::NoCase,
            3 => Self::Rtrim,
            bits => Self::Locale(LocaleCollationId::from_bits(bits)),
        }
    }

    #[inline]
    pub const fn id(self) -> u32 {
        match self {
            Self::Custom(id) => id,
            _ => self.to_bits() as u32,
        }
    }

    #[inline]
    pub const fn is_custom(self) -> bool {
        matches!(self, Self::Custom(_))
    }

    pub fn custom(collation: &str) -> Self {
        let normalized = crate::util::normalize_ident(collation);
        let mut registry = CUSTOM_COLLATION_NAMES.lock();
        if let Some(id) = registry.by_name.get(&normalized) {
            return Self::Custom(*id);
        }

        let mut id = custom_collation_id(&normalized);
        while id <= 3 || registry.by_id.contains_key(&id) {
            id = id.wrapping_add(1).max(4);
        }

        registry.by_name.insert(normalized, id);
        registry.by_id.insert(id, collation.to_string());
        Self::Custom(id)
    }

    pub(crate) fn known_custom(collation: &str) -> Option<Self> {
        let normalized = crate::util::normalize_ident(collation);
        CUSTOM_COLLATION_NAMES
            .lock()
            .by_name
            .get(&normalized)
            .copied()
            .map(Self::Custom)
    }

    pub fn name(self) -> String {
        match self {
            Self::Unset => "Unset".to_string(),
            Self::Binary => "Binary".to_string(),
            Self::NoCase => "NoCase".to_string(),
            Self::Rtrim => "RTrim".to_string(),
            Self::Locale(id) => LocaleCollationRegistry::global().name(id),
            Self::Custom(id) => CUSTOM_COLLATION_NAMES
                .lock()
                .by_id
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("collation_{id}")),
        }
    }

    /// Orders two values. Values of different kinds order as NULL, numbers,
    /// text, blobs; two texts compare with this collation.
    ///
    /// A custom collation is a connection callback, which is not reachable
    /// from here, so two texts under a custom collation compare as BINARY.
    /// The VDBE resolves custom collations through `Connection` before it
    /// gets here, and schema objects reject them.
    #[inline(always)]
    pub fn compare(self, lhs: ValueRef, rhs: ValueRef) -> Ordering {
        match (lhs, rhs) {
            (ValueRef::Text(lhs), ValueRef::Text(rhs)) => {
                self.compare_strings(lhs.as_str(), rhs.as_str())
            }
            _ => lhs.cmp(&rhs),
        }
    }

    /// Feeds `value` to `hasher` so that values that [Self::compare] calls
    /// equal hash the same. Integers hash like the equal REAL, so 10 and 10.0
    /// land in the same bucket.
    pub fn hash_value(self, value: ValueRef, hasher: &mut impl Hasher) {
        const NULL_HASH: u8 = 0;
        const INT_HASH: u8 = 1;
        const FLOAT_HASH: u8 = 2;
        const TEXT_HASH: u8 = 3;
        const BLOB_HASH: u8 = 4;
        match value {
            ValueRef::Null => hasher.write_u8(NULL_HASH),
            ValueRef::Numeric(Numeric::Integer(i)) => {
                let f = i as f64;
                if (f as i64) == i && f.is_finite() {
                    hasher.write_u8(FLOAT_HASH);
                    hasher.write(&normalized_f64_bits(f).to_le_bytes());
                } else {
                    // The REAL would lose precision, so no REAL can equal this integer.
                    hasher.write_u8(INT_HASH);
                    hasher.write_i64(i);
                }
            }
            ValueRef::Numeric(Numeric::Float(f)) => {
                hasher.write_u8(FLOAT_HASH);
                hasher.write(&normalized_f64_bits(f64::from(f)).to_le_bytes());
            }
            ValueRef::Text(text) => {
                hasher.write_u8(TEXT_HASH);
                self.hash_text(text.as_str(), hasher);
            }
            ValueRef::Blob(blob) => {
                hasher.write_u8(BLOB_HASH);
                hasher.write(blob);
            }
        }
    }

    fn hash_text(self, text: &str, hasher: &mut impl Hasher) {
        match self {
            Self::Unset | Self::Binary => hasher.write(text.as_bytes()),
            Self::NoCase => {
                // Stops at the first NUL like the comparison does.
                hasher.write_usize(text.len());
                for byte in text.bytes() {
                    hasher.write_u8(byte.to_ascii_lowercase());
                    if byte == 0 {
                        break;
                    }
                }
            }
            Self::Rtrim => hasher.write(text.trim_end_matches(' ').as_bytes()),
            Self::Locale(id) => hasher.write(&LocaleCollationRegistry::global().sort_key(id, text)),
            Self::Custom(_) => {
                unreachable!("custom collations are rejected before values are hashed")
            }
        }
    }

    #[inline(always)]
    fn compare_strings(&self, lhs: &str, rhs: &str) -> Ordering {
        match *self {
            Self::Unset | Self::Binary => Self::binary_cmp(lhs, rhs),
            Self::NoCase => Self::nocase_cmp(lhs, rhs),
            Self::Rtrim => Self::rtrim_cmp(lhs, rhs),
            Self::Locale(id) => LocaleCollationRegistry::global().compare(id, lhs, rhs),
            // Immutable comparison paths have no connection to fetch the external
            // callback from. Runtime VDBE paths dispatch custom collations via
            // `Connection`; schema/index paths reject them before storage.
            Self::Custom(_) => Self::binary_cmp(lhs, rhs),
        }
    }

    #[inline(always)]
    fn binary_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline(always)]
    fn nocase_cmp(lhs: &str, rhs: &str) -> Ordering {
        for (left, right) in lhs.bytes().zip(rhs.bytes()) {
            let left = left.to_ascii_lowercase();
            let right = right.to_ascii_lowercase();
            if left != right {
                return left.cmp(&right);
            }
            if left == 0 {
                return lhs.len().cmp(&rhs.len());
            }
        }
        lhs.len().cmp(&rhs.len())
    }

    #[inline(always)]
    fn rtrim_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.trim_end_matches(' ').cmp(rhs.trim_end_matches(' '))
    }
}

impl std::fmt::Display for CollationSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct LocaleCollationId(u16);

impl LocaleCollationId {
    const FIRST_STORAGE_BIT: u16 = 4;

    fn from_index(index: usize) -> Result<Self> {
        if index > (u16::MAX - Self::FIRST_STORAGE_BIT) as usize {
            return Err(crate::LimboError::ParseError(
                "too many locale collation sequences".to_string(),
            ));
        }
        Ok(Self(index as u16))
    }

    const fn from_bits(bits: u16) -> Self {
        Self(bits - Self::FIRST_STORAGE_BIT)
    }

    const fn to_bits(self) -> u16 {
        self.0 + Self::FIRST_STORAGE_BIT
    }
}

struct LocaleCollation {
    name: String,
    collator: CollatorBorrowed<'static>,
}

struct LocaleCollationRegistry {
    collations: RwLock<Vec<LocaleCollation>>,
}

impl LocaleCollationRegistry {
    fn global() -> &'static Self {
        static REGISTRY: LazyLock<LocaleCollationRegistry> =
            LazyLock::new(|| LocaleCollationRegistry {
                collations: RwLock::new(Vec::new()),
            });
        &REGISTRY
    }

    fn get_or_register(&self, name: &str) -> Result<LocaleCollationId> {
        if let Some(id) = self.find(name) {
            return Ok(id);
        }

        let locale = Locale::from_str(name).map_err(|_| {
            crate::LimboError::ParseError(format!("no such collation sequence: {name}"))
        })?;
        let collator =
            Collator::try_new(locale.into(), CollatorOptions::default()).map_err(|_| {
                crate::LimboError::ParseError(format!("no such collation sequence: {name}"))
            })?;

        let mut collations = self.collations.write();
        if let Some((idx, _)) = collations
            .iter()
            .enumerate()
            .find(|(_, collation)| collation.name.eq_ignore_ascii_case(name))
        {
            return LocaleCollationId::from_index(idx);
        }
        let id = LocaleCollationId::from_index(collations.len())?;
        collations.push(LocaleCollation {
            name: name.to_string(),
            collator,
        });
        Ok(id)
    }

    fn find(&self, name: &str) -> Option<LocaleCollationId> {
        self.collations
            .read()
            .iter()
            .enumerate()
            .find(|(_, collation)| collation.name.eq_ignore_ascii_case(name))
            .and_then(|(idx, _)| LocaleCollationId::from_index(idx).ok())
    }

    fn compare(&self, id: LocaleCollationId, lhs: &str, rhs: &str) -> Ordering {
        self.with_collation(id, |collation| collation.collator.compare(lhs, rhs))
    }

    fn sort_key(&self, id: LocaleCollationId, text: &str) -> Vec<u8> {
        self.with_collation(id, |collation| {
            let mut key = Vec::new();
            collation
                .collator
                .write_sort_key_to(text, &mut key)
                .expect("Vec collation key sink should be infallible");
            key
        })
    }

    fn name(&self, id: LocaleCollationId) -> String {
        self.with_collation(id, |collation| collation.name.clone())
    }

    fn with_collation<T>(&self, id: LocaleCollationId, f: impl FnOnce(&LocaleCollation) -> T) -> T {
        let collations = self.collations.read();
        let collation = collations
            .get(id.0 as usize)
            .expect("locale collation id should be registered");
        f(collation)
    }
}

fn custom_collation_id(name: &str) -> u32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    ((hasher.finish() as u32) & 0x7fff_fffc).max(4)
}

/// The collation a comparison between `lhs` and `rhs` uses. This and
/// [expr_collation] are the only places that decide a collation from
/// expressions; every comparison, sort key, grouping key and hash key gets its
/// collation from one of them. The rules are SQLite's
/// (`sqlite3BinaryCompareCollSeq`):
///
/// 1. If an operand contains a COLLATE operator anywhere outside a subquery,
///    the collation of that operand is used, the left operand first.
/// 2. Otherwise the collation of an operand that is a column is used, the left
///    operand first. A column behind CAST or unary `+` still counts as a
///    column; a column inside a function call, `||`, CASE or a subquery does
///    not. A column without a declared collation has BINARY.
/// 3. Otherwise BINARY.
pub fn comparison_collation(
    lhs: &Expr,
    rhs: &Expr,
    tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    comparison_collation_across_queries(lhs, tables, rhs, tables, resolver)
}

/// [comparison_collation] for `x IN (SELECT y ...)` and other comparisons
/// whose operands belong to different queries, so each operand's columns are
/// looked up in its own tables.
pub fn comparison_collation_across_queries(
    lhs: &Expr,
    lhs_tables: Option<&TableReferences>,
    rhs: &Expr,
    rhs_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    let collation = if has_collate_operator(lhs) {
        expr_collation_if_any(lhs, lhs_tables, resolver)?
    } else if has_collate_operator(rhs) {
        expr_collation_if_any(rhs, rhs_tables, resolver)?
    } else {
        match expr_collation_if_any(lhs, lhs_tables, resolver)? {
            Some(collation) => Some(collation),
            None => expr_collation_if_any(rhs, rhs_tables, resolver)?,
        }
    };
    Ok(collation.unwrap_or(CollationSeq::Binary))
}

/// The collation of one expression, BINARY when it has none. Sort, grouping,
/// DISTINCT, compound select, window partition and index keys use it, and so
/// does `x IN (y1, ..., yN)`, which compares with the collation of x alone.
pub fn expr_collation(
    expr: &Expr,
    tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    Ok(expr_collation_if_any(expr, tables, resolver)?.unwrap_or(CollationSeq::Binary))
}

/// The collation of the first expression that has one, BINARY when none has.
///
/// SQLite uses this rule in two places: `min`, `max` and `nullif` compare their
/// arguments with the collation of the first argument that has one (functions
/// marked `SQLITE_FUNC_NEEDCOLL`), and a compound SELECT compares a result
/// column with the collation of the leftmost SELECT whose column has one.
/// Each expression comes with the tables its columns refer to.
pub fn first_collation<'a>(
    exprs: impl IntoIterator<Item = (&'a Expr, Option<&'a TableReferences>)>,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    for (expr, tables) in exprs {
        if let Some(collation) = expr_collation_if_any(expr, tables, resolver)? {
            return Ok(collation);
        }
    }
    Ok(CollationSeq::Binary)
}

/// The collation of one expression, or None when it has none (SQLite's
/// `sqlite3ExprCollSeq`). A column has its declared collation, BINARY when it
/// declares none. CAST and unary `+` pass on the collation of their operand. A
/// row value has the collation of its first element. Any other expression has
/// the collation of its leftmost COLLATE operator, or none when it contains no
/// COLLATE operator.
fn expr_collation_if_any(
    expr: &Expr,
    tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<Option<CollationSeq>> {
    match expr {
        Expr::Column { table, column, .. } => column_collation(*table, *column, tables, resolver),
        Expr::RowId { .. } => Ok(None),
        Expr::Register(register) => Ok(resolver.register_collations.get(register).copied()),
        Expr::MergedColumn(columns) => {
            let first_column = columns
                .first()
                .expect("a merged column must have at least two source columns");
            expr_collation_if_any(first_column, tables, resolver)
        }
        Expr::Cast { expr, .. } | Expr::Unary(UnaryOperator::Positive, expr) => {
            expr_collation_if_any(expr, tables, resolver)
        }
        Expr::Parenthesized(exprs) => {
            let first = exprs.first().expect("parentheses must hold an expression");
            expr_collation_if_any(first, tables, resolver)
        }
        Expr::Collate(_, name) => Ok(Some(resolver.resolve_collation(name.as_str())?)),
        _ => {
            let mut operand_with_collate = None;
            for_each_operand(expr, &mut |operand| {
                if operand_with_collate.is_none() && has_collate_operator(operand) {
                    operand_with_collate = Some(operand);
                }
            });
            match operand_with_collate {
                Some(operand) => expr_collation_if_any(operand, tables, resolver),
                None => Ok(None),
            }
        }
    }
}

/// A column has its declared collation, BINARY when it declares none. A rowid
/// alias (INTEGER PRIMARY KEY) is the rowid and has none, as in SQLite.
fn column_collation(
    table: TableInternalId,
    column: usize,
    tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<Option<CollationSeq>> {
    let column_collation =
        |column: &crate::schema::Column| (!column.is_rowid_alias()).then(|| column.collation());
    if table.is_self_table() {
        let self_table = resolver
            .self_table()
            .ok_or_else(|| LimboError::InternalError("no table for a self-table column".into()))?;
        let column = self_table
            .columns()
            .get(column)
            .ok_or_else(|| LimboError::ParseError("column not found".to_string()))?;
        return Ok(column_collation(column));
    }
    let (_, table_ref) = tables
        .and_then(|tables| tables.find_table_by_internal_id(table))
        .ok_or_else(|| LimboError::ParseError("table not found".to_string()))?;
    let column = table_ref
        .get_column_at(column)
        .ok_or_else(|| LimboError::ParseError("column not found".to_string()))?;
    Ok(column_collation(column))
}

/// Whether the expression contains a COLLATE operator outside of a subquery
/// (SQLite's `EP_Collate` flag).
fn has_collate_operator(expr: &Expr) -> bool {
    if matches!(expr, Expr::Collate(..)) {
        return true;
    }
    let mut found = false;
    for_each_operand(expr, &mut |operand| {
        found = found || has_collate_operator(operand);
    });
    found
}

/// Calls `f` on the operands of `expr` that pass a COLLATE operator up to
/// `expr`, in the order SQLite searches them for one. Subqueries pass nothing
/// up, and the pattern of LIKE comes before its subject because SQLite
/// evaluates `x LIKE y` as `like(y, x)`.
fn for_each_operand<'a>(expr: &'a Expr, f: &mut impl FnMut(&'a Expr)) {
    match expr {
        Expr::Binary(lhs, _, rhs) => {
            f(lhs);
            f(rhs);
        }
        Expr::Between {
            lhs, start, end, ..
        } => {
            f(lhs);
            f(start);
            f(end);
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                f(base);
            }
            for (when, then) in when_then_pairs {
                f(when);
                f(then);
            }
            if let Some(else_expr) = else_expr {
                f(else_expr);
            }
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => f(expr),
        Expr::FieldAccess { base, .. } => f(base),
        Expr::FunctionCall { args, .. } => args.iter().for_each(|arg| f(arg)),
        Expr::InList { lhs, rhs, .. } => {
            f(lhs);
            rhs.iter().for_each(|item| f(item));
        }
        Expr::InSelect { lhs, .. } | Expr::InTable { lhs, .. } => f(lhs),
        Expr::SubqueryResult { lhs, .. } => {
            if let Some(lhs) = lhs {
                f(lhs);
            }
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            f(rhs);
            f(lhs);
            if let Some(escape) = escape {
                f(escape);
            }
        }
        Expr::Parenthesized(exprs) | Expr::Array { elements: exprs } => {
            exprs.iter().for_each(|expr| f(expr))
        }
        Expr::Subscript { base, index } => {
            f(base);
            f(index);
        }
        Expr::Raise(_, message) => {
            if let Some(message) = message {
                f(message);
            }
        }
        Expr::MergedColumn(_)
        | Expr::Column { .. }
        | Expr::RowId { .. }
        | Expr::Register(_)
        | Expr::DoublyQualified(..)
        | Expr::Exists(_)
        | Expr::FunctionCallStar { .. }
        | Expr::Id(_)
        | Expr::Literal(_)
        | Expr::Name(_)
        | Expr::Qualified(..)
        | Expr::Subquery(_)
        | Expr::Variable(_)
        | Expr::Default => {}
    }
}

#[cfg(test)]
mod tests {
    use crate::alloc::vec;
    use crate::{sync::Arc, MAIN_DB_ID};

    use turso_parser::ast::{Literal, Name, Operator, TableInternalId, UnaryOperator};

    use crate::{
        schema::{BTreeCharacteristics, BTreeTable, ColDef, ColDefFlags, Column, Table, Type},
        translate::plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan},
    };

    use super::*;

    #[test]
    fn test_locale_collation_names() {
        assert!(matches!(
            CollationSeq::new("fr-FR").unwrap(),
            CollationSeq::Locale(_)
        ));
        assert!(matches!(
            CollationSeq::new("es-u-co-trad").unwrap(),
            CollationSeq::Locale(_)
        ));
        assert!(matches!(
            CollationSeq::new("en-u-kf-upper").unwrap(),
            CollationSeq::Locale(_)
        ));
        assert!(matches!(
            CollationSeq::new("en-US-u-kf-upper").unwrap(),
            CollationSeq::Locale(_)
        ));
        assert!(CollationSeq::new("compile_options").is_err());
    }

    #[test]
    fn test_locale_collation_compare() {
        let traditional_spanish = CollationSeq::new("es-u-co-trad").unwrap();
        assert_eq!(
            traditional_spanish.compare_strings("pollo", "polvo"),
            Ordering::Greater
        );

        let upper_first = CollationSeq::new("en-u-kf-upper").unwrap();
        assert_eq!(upper_first.compare_strings("A", "a"), Ordering::Less);

        let upper_first_us = CollationSeq::new("en-US-u-kf-upper").unwrap();
        assert_eq!(upper_first_us.compare_strings("A", "a"), Ordering::Less);
    }

    #[test]
    fn expr_collation_single_table_single_column() {
        // A plain column has its declared collation, BINARY when it declares none.
        for collation in [
            None,
            Some(CollationSeq::Binary),
            Some(CollationSeq::NoCase),
            Some(CollationSeq::Rtrim),
        ] {
            let table_references =
                get_table_references_single_table_single_column_with_collation(collation);
            let expr = Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            };
            let collseq =
                expr_collation_if_any(&expr, Some(&table_references), &Resolver::empty_for_tests())
                    .unwrap();
            assert_eq!(collseq, Some(collation.unwrap_or(CollationSeq::Binary)));
        }
    }

    #[test]
    fn expr_collation_single_table_single_column_with_collate() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::Binary),
        );
        // col COLLATE RTRIM, col COLLATE NOCASE, col COLLATE BINARY
        for collation in ["RTRIM", "NOCASE", "BINARY"] {
            let expected_collation = CollationSeq::new(collation).unwrap();
            let expr = Expr::Collate(
                Box::new(Expr::Column {
                    database: None,
                    table: TableInternalId::from(1),
                    column: 0,
                    is_rowid_alias: false,
                }),
                Name::exact(collation.to_string()),
            );
            let collseq =
                expr_collation_if_any(&expr, Some(&table_references), &Resolver::empty_for_tests())
                    .unwrap();
            assert_eq!(collseq, Some(expected_collation));
        }
    }

    #[test]
    fn expr_collation_multiple_collate_leftmost_wins() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::Binary),
        );
        // (col COLLATE NOCASE) COLLATE RTRIM -- RTRIM wins as it is the leftmost AST node with a COLLATE
        let inner = Expr::Collate(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            }),
            Name::exact("NOCASE".to_string()),
        );
        let expr = Expr::Collate(
            Box::new(Expr::Parenthesized(std::vec![Box::new(inner)])),
            Name::exact("RTRIM".to_string()),
        );
        let collseq =
            expr_collation_if_any(&expr, Some(&table_references), &Resolver::empty_for_tests())
                .unwrap();
        assert_eq!(collseq, Some(CollationSeq::Rtrim));
    }

    #[test]
    fn expr_collation_unary_plus_and_cast_still_column() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::NoCase),
        );
        // Unary plus on column
        let expr_plus = Expr::unary(
            UnaryOperator::Positive,
            Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            },
        );
        let collseq_plus = expr_collation_if_any(
            &expr_plus,
            Some(&table_references),
            &Resolver::empty_for_tests(),
        )
        .unwrap();
        assert_eq!(collseq_plus, Some(CollationSeq::NoCase));

        // CAST(column AS TEXT)
        let cast_ty = Some(turso_parser::ast::Type {
            name: "TEXT".to_string(),
            size: None,
            array_dimensions: 0,
        });
        let expr_cast = Expr::cast(
            Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            },
            cast_ty,
        );
        let collseq_cast = expr_collation_if_any(
            &expr_cast,
            Some(&table_references),
            &Resolver::empty_for_tests(),
        )
        .unwrap();
        assert_eq!(collseq_cast, Some(CollationSeq::NoCase));
    }

    #[test]
    fn expr_collation_explicit_collate_anywhere_in_operand() {
        let table_references = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            None,
        );
        // RTRIM wins because it's an explicit COLLATE even though it appears on the right side of the expression
        let lhs = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let rhs = Expr::Parenthesized(std::vec![Box::new(Expr::Collate(
            Box::new(Expr::Literal(Literal::String("x".to_string()))),
            Name::exact("RTRIM".to_string()),
        ))]);
        let expr = Expr::binary(lhs, Operator::Add, rhs);
        let collseq =
            expr_collation_if_any(&expr, Some(&table_references), &Resolver::empty_for_tests())
                .unwrap();
        assert_eq!(collseq, Some(CollationSeq::Rtrim));
    }

    #[test]
    fn expr_collation_column_plus_column_has_none() {
        let table_references = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            Some(CollationSeq::Rtrim),
        );
        // col1 + col2 has no collation: as in SQLite, an operator only passes on a
        // collation from an operand that contains an explicit COLLATE.
        let lhs = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let rhs = Expr::Column {
            database: None,
            table: TableInternalId::from(2),
            column: 0,
            is_rowid_alias: false,
        };
        let expr = Expr::binary(lhs, Operator::Add, rhs);
        let collseq =
            expr_collation_if_any(&expr, Some(&table_references), &Resolver::empty_for_tests())
                .unwrap();
        assert_eq!(collseq, None);
    }

    #[test]
    fn expr_collation_collate_vs_collate_leftside_expr_wins() {
        let table_references = TableReferences::new_empty();
        // (x COLLATE NOCASE) + (y COLLATE RTRIM) -- NOCASE wins since it's on the left side
        let lhs = Expr::Collate(
            Box::new(Expr::Literal(Literal::String("x".to_string()))),
            Name::exact("NOCASE".to_string()),
        );
        let rhs = Expr::Collate(
            Box::new(Expr::Literal(Literal::String("y".to_string()))),
            Name::exact("RTRIM".to_string()),
        );
        let expr = Expr::binary(lhs, Operator::Add, rhs);
        let collseq =
            expr_collation_if_any(&expr, Some(&table_references), &Resolver::empty_for_tests())
                .unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn expr_collation_default_binary_when_no_collate_or_column() {
        let table_references = TableReferences::new_empty();
        let expr = Expr::Literal(Literal::String("abc".to_string()));
        let collseq =
            expr_collation_if_any(&expr, Some(&table_references), &Resolver::empty_for_tests())
                .unwrap();
        assert_eq!(collseq, None);
    }

    #[test]
    fn expr_collation_rowid_has_none_even_with_declared_collation() {
        let table_references = get_table_references_single_table_rowid_alias_with_collation(Some(
            CollationSeq::NoCase,
        ));
        let expr = Expr::RowId {
            database: None,
            table: TableInternalId::from(1),
        };
        // As in SQLite, the rowid has no collation even when its INTEGER PRIMARY KEY
        // alias declares one.
        let collseq =
            expr_collation_if_any(&expr, Some(&table_references), &Resolver::empty_for_tests())
                .unwrap();
        assert_eq!(collseq, None);
    }

    #[test]
    fn comparison_collation_nocase_column_vs_binary_default() {
        // LHS has NOCASE column, RHS has no collation → NOCASE
        let table_refs = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            None,
        );
        let lhs = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let rhs = Expr::Column {
            database: None,
            table: TableInternalId::from(2),
            column: 0,
            is_rowid_alias: false,
        };
        assert_eq!(
            comparison_collation(&lhs, &rhs, Some(&table_refs), &Resolver::empty_for_tests())
                .unwrap(),
            CollationSeq::NoCase
        );
        // Swapped: the LHS column is still a column, so its default BINARY
        // collation wins over the NOCASE column on the RHS.
        assert_eq!(
            comparison_collation(&rhs, &lhs, Some(&table_refs), &Resolver::empty_for_tests())
                .unwrap(),
            CollationSeq::Binary
        );
    }

    #[test]
    fn comparison_collation_function_hides_column_collation() {
        // A column inside a function call is not a column operand, so the
        // NOCASE column on the other side supplies the collation.
        let table_refs = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            None,
        );
        let nocase_column = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let function_of_binary_column = Expr::FunctionCall {
            name: Name::exact("lower".to_string()),
            distinctness: None,
            args: std::vec![Box::new(Expr::Column {
                database: None,
                table: TableInternalId::from(2),
                column: 0,
                is_rowid_alias: false,
            })],
            order_by: std::vec![],
            within_group: std::vec![],
            filter_over: turso_parser::ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        assert_eq!(
            comparison_collation(
                &function_of_binary_column,
                &nocase_column,
                Some(&table_refs),
                &Resolver::empty_for_tests()
            )
            .unwrap(),
            CollationSeq::NoCase
        );
    }

    #[test]
    fn comparison_collation_uplus_and_parens_keep_column_collation() {
        // "+column" and "(column)" still count as columns, so the BINARY
        // column on the LHS wins over the NOCASE column on the RHS.
        let table_refs = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            None,
        );
        let nocase_column = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let wrapped_binary_column = Expr::Unary(
            UnaryOperator::Positive,
            Box::new(Expr::Parenthesized(std::vec![Box::new(Expr::Column {
                database: None,
                table: TableInternalId::from(2),
                column: 0,
                is_rowid_alias: false,
            })])),
        );
        assert_eq!(
            comparison_collation(
                &wrapped_binary_column,
                &nocase_column,
                Some(&table_refs),
                &Resolver::empty_for_tests()
            )
            .unwrap(),
            CollationSeq::Binary
        );
    }

    #[test]
    fn comparison_collation_explicit_beats_column() {
        // LHS column is NOCASE, but RHS has explicit RTRIM → RTRIM wins
        let table_refs = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            None,
        );
        let lhs = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let rhs = Expr::Collate(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::from(2),
                column: 0,
                is_rowid_alias: false,
            }),
            Name::exact("RTRIM".to_string()),
        );
        assert_eq!(
            comparison_collation(&lhs, &rhs, Some(&table_refs), &Resolver::empty_for_tests())
                .unwrap(),
            CollationSeq::Rtrim
        );
    }

    #[test]
    fn comparison_collation_both_default_is_binary() {
        let table_refs = get_table_references_two_tables_single_column_with_collations(None, None);
        let lhs = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let rhs = Expr::Column {
            database: None,
            table: TableInternalId::from(2),
            column: 0,
            is_rowid_alias: false,
        };
        assert_eq!(
            comparison_collation(&lhs, &rhs, Some(&table_refs), &Resolver::empty_for_tests())
                .unwrap(),
            CollationSeq::Binary
        );
    }

    // Helpers //

    fn get_table_references_single_table_single_column_with_collation(
        collation: Option<CollationSeq>,
    ) -> TableReferences {
        let mut table_references = TableReferences::new_empty();
        let columns = vec![Column::new(
            Some("foo".to_string()),
            "text".to_string(),
            None,
            None,
            Type::Text,
            collation,
            ColDef::default(),
        )];
        let table = Table::BTree(Arc::new(BTreeTable::new(
            0,
            "foo".to_string(),
            vec![],
            columns,
            BTreeCharacteristics::empty(),
            vec![],
            vec![],
            vec![],
            None,
        )));
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            unmatched_right_rows_plan: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            identifier: "foo".to_string(),
            has_alias: false,
            internal_id: TableInternalId::from(1),
            join_info: None,
            table,
            plan_estimate: None,
            indexed: None,
        });

        table_references
    }

    fn get_table_references_two_tables_single_column_with_collations(
        left: Option<CollationSeq>,
        right: Option<CollationSeq>,
    ) -> TableReferences {
        let mut table_references = TableReferences::new_empty();
        // Left table t1(id=1)
        let columns = vec![Column::new(
            Some("a".to_string()),
            "text".to_string(),
            None,
            None,
            Type::Text,
            left,
            ColDef::default(),
        )];
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            unmatched_right_rows_plan: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            identifier: "t1".to_string(),
            has_alias: false,
            internal_id: TableInternalId::from(1),
            join_info: None,
            plan_estimate: None,
            table: Table::BTree(Arc::new(BTreeTable::new(
                0,
                "t1".to_string(),
                vec![],
                columns,
                BTreeCharacteristics::HAS_ROWID,
                vec![],
                vec![],
                vec![],
                None,
            ))),
            indexed: None,
        });
        // Right table t2(id=2)
        let columns = vec![Column::new(
            Some("b".to_string()),
            "text".to_string(),
            None,
            None,
            Type::Text,
            right,
            ColDef::default(),
        )];
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            unmatched_right_rows_plan: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            identifier: "t2".to_string(),
            has_alias: false,
            internal_id: TableInternalId::from(2),
            join_info: None,
            plan_estimate: None,
            table: Table::BTree(Arc::new(BTreeTable::new(
                0,
                "t2".to_string(),
                vec![],
                columns,
                BTreeCharacteristics::HAS_ROWID,
                vec![],
                vec![],
                vec![],
                None,
            ))),
            indexed: None,
        });
        table_references
    }

    fn get_table_references_single_table_rowid_alias_with_collation(
        collation: Option<CollationSeq>,
    ) -> TableReferences {
        use turso_parser::ast::SortOrder;
        let mut table_references = TableReferences::new_empty();
        let columns = vec![Column::new(
            Some("id".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            collation,
            ColDef {
                flags: ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias | ColDefFlags::Unique,
                ..Default::default()
            },
        )];
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            unmatched_right_rows_plan: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            identifier: "bar".to_string(),
            has_alias: false,
            internal_id: TableInternalId::from(1),
            join_info: None,
            plan_estimate: None,
            indexed: None,
            table: Table::BTree(Arc::new(BTreeTable::new(
                0,
                "bar".to_string(),
                vec![("id".to_string(), SortOrder::Asc)],
                columns,
                BTreeCharacteristics::HAS_ROWID,
                vec![],
                vec![],
                vec![],
                None,
            ))),
        });
        table_references
    }
}
