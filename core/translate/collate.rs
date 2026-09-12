use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{Hash, Hasher},
    str::FromStr as _,
};

use icu_collator::{options::CollatorOptions, Collator, CollatorBorrowed};
use icu_locale::Locale;
use turso_parser::ast::Expr;

use crate::{
    connection::SymbolTable,
    sync::{LazyLock, Mutex, RwLock},
    translate::{
        expr::{walk_expr, WalkControl},
        plan::TableReferences,
    },
    Result,
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

    #[inline(always)]
    pub fn compare_strings(&self, lhs: &str, rhs: &str) -> Ordering {
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

    pub fn hash_key(&self, text: &str) -> Vec<u8> {
        match self {
            Self::Unset | Self::Binary => text.as_bytes().to_vec(),
            Self::NoCase => text.bytes().map(|b| b.to_ascii_lowercase()).collect(),
            Self::Rtrim => text.trim_end_matches(' ').as_bytes().to_vec(),
            Self::Locale(id) => LocaleCollationRegistry::global().sort_key(*id, text),
            // Hash joins using custom collations are disabled during planning
            // because the callback is connection-owned and may define arbitrary equality.
            Self::Custom(_) => text.as_bytes().to_vec(),
        }
    }
}

fn resolve_collation_name(
    collation: &str,
    symbol_table: Option<&SymbolTable>,
) -> Result<CollationSeq> {
    if let Some(collation) = symbol_table.and_then(|syms| syms.resolve_collation(collation)) {
        return Ok(collation);
    }
    CollationSeq::new(collation)
}

impl Default for CollationSeq {
    fn default() -> Self {
        Self::Binary
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

/// Every column of every table has an associated collating function. If no collating function is explicitly defined,
/// then the collating function defaults to BINARY.
/// The COLLATE clause of the column definition is used to define alternative collating functions for a column.
///
/// The rules for determining which collating function to use for a binary comparison operator (=, <, >, <=, >=, !=, IS, and IS NOT) are as follows:
///
/// If either operand has an explicit collating function assignment using the postfix COLLATE operator,
/// then the explicit collating function is used for comparison, with precedence to the collating function of the left operand.
///
/// If either operand is a column, then the collating function of that column is used with precedence to the left operand.
/// For the purposes of the previous sentence, a column name preceded by one or more unary "+" operators and/or CAST operators is still considered a column name.
///
/// Otherwise, the BINARY collating function is used for comparison.
///
/// An operand of a comparison is considered to have an explicit collating function assignment
/// if any subexpression of the operand uses the postfix COLLATE operator.
/// Thus, if a COLLATE operator is used anywhere in a comparison expression,
/// the collating function defined by that operator is used for string comparison
/// regardless of what table columns might be a part of that expression.
/// If two or more COLLATE operator subexpressions appear anywhere in a comparison,
/// the left most explicit collating function is used regardless of how deeply
/// the COLLATE operators are nested in the expression and regardless of how
/// the expression is parenthesized.
pub fn get_collseq_from_expr(
    top_expr: &Expr,
    referenced_tables: &TableReferences,
) -> Result<Option<CollationSeq>> {
    get_collseq_from_expr_with_symbols(top_expr, referenced_tables, None)
}

pub fn get_collseq_from_expr_with_symbols(
    top_expr: &Expr,
    referenced_tables: &TableReferences,
    symbol_table: Option<&SymbolTable>,
) -> Result<Option<CollationSeq>> {
    let (explicit, column) =
        get_collseq_parts_from_expr_with_symbols(top_expr, referenced_tables, symbol_table)?;
    Ok(explicit.or(column))
}

/// Return the collation context that standalone expression translation would
/// propagate to a parent comparison when this expression is reused from cache.
///
/// This differs from `get_collseq_from_expr()` in one important way: plain
/// column references keep their default BINARY collation, because standalone
/// column translation records that fact in `ProgramBuilder::curr_collation_ctx()`.
/// Synthetic expressions such as aggregates must opt out by storing `None` in
/// the cache entry instead of calling this helper.
pub fn get_expr_collation_ctx_with_symbols(
    top_expr: &Expr,
    referenced_tables: &TableReferences,
    symbol_table: Option<&SymbolTable>,
) -> Result<Option<(CollationSeq, bool)>> {
    let mut maybe_column_collseq = None;
    let mut maybe_explicit_collseq = None;

    walk_expr(top_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Collate(_, seq) => {
                if maybe_explicit_collseq.is_none() {
                    maybe_explicit_collseq = Some(
                        resolve_collation_name(seq.as_str(), symbol_table).unwrap_or_default(),
                    );
                }
                return Ok(WalkControl::SkipChildren);
            }
            Expr::Column { table, column, .. } => {
                // generated columns (the SELF_TABLE placeholder) don't inherit an implicit
                // collation from their expression, so we skip them
                if !table.is_self_table() {
                    let (_, table_ref) = referenced_tables
                        .find_table_by_internal_id(*table)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError("table not found".to_string())
                        })?;
                    let column = table_ref.get_column_at(*column).ok_or_else(|| {
                        crate::LimboError::ParseError("column not found".to_string())
                    })?;
                    if maybe_column_collseq.is_none() {
                        maybe_column_collseq = Some(column.collation());
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(maybe_explicit_collseq
        .map(|collation| (collation, true))
        .or_else(|| maybe_column_collseq.map(|collation| (collation, false))))
}

/// Resolve the collation for a binary comparison (=, <, >, etc.) per SQLite rules:
/// 1. Explicit COLLATE operator on either side wins (LHS takes precedence)
/// 2. Column with defined collation on either side wins (LHS takes precedence)
/// 3. Otherwise BINARY
#[cfg(test)]
pub fn resolve_comparison_collseq(
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: &TableReferences,
) -> Result<CollationSeq> {
    resolve_comparison_collseq_with_symbols(lhs_expr, rhs_expr, referenced_tables, None)
}

pub fn resolve_comparison_collseq_with_symbols(
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: &TableReferences,
    symbol_table: Option<&SymbolTable>,
) -> Result<CollationSeq> {
    let (lhs_explicit, lhs_column) =
        get_collseq_parts_from_expr_with_symbols(lhs_expr, referenced_tables, symbol_table)?;
    let (rhs_explicit, rhs_column) =
        get_collseq_parts_from_expr_with_symbols(rhs_expr, referenced_tables, symbol_table)?;
    Ok(lhs_explicit
        .or(rhs_explicit)
        .or(lhs_column)
        .or(rhs_column)
        .unwrap_or(CollationSeq::Binary))
}

/// Returns (explicit_collation, column_collation) from a single expression.
/// Explicit collation comes from COLLATE operators; column collation comes from
/// column definitions. These are kept separate to allow proper precedence resolution
/// in binary comparisons.
fn get_collseq_parts_from_expr_with_symbols(
    top_expr: &Expr,
    referenced_tables: &TableReferences,
    symbol_table: Option<&SymbolTable>,
) -> Result<(Option<CollationSeq>, Option<CollationSeq>)> {
    let mut maybe_column_collseq = None;
    let mut maybe_explicit_collseq = None;

    walk_expr(top_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Collate(_, seq) => {
                // Only store the first (leftmost) COLLATE operator we find
                if maybe_explicit_collseq.is_none() {
                    maybe_explicit_collseq = Some(
                        resolve_collation_name(seq.as_str(), symbol_table).unwrap_or_default(),
                    );
                }
                // Skip children since we've found a COLLATE operator
                return Ok(WalkControl::SkipChildren);
            }
            Expr::Column { table, column, .. } => {
                let (_, table_ref) = referenced_tables
                    .find_table_by_internal_id(*table)
                    .ok_or_else(|| crate::LimboError::ParseError("table not found".to_string()))?;
                let column = table_ref
                    .get_column_at(*column)
                    .ok_or_else(|| crate::LimboError::ParseError("column not found".to_string()))?;
                if maybe_column_collseq.is_none() {
                    maybe_column_collseq = column.collation_opt();
                }
                return Ok(WalkControl::Continue);
            }
            Expr::RowId { table, .. } => {
                let (_, table_ref) = referenced_tables
                    .find_table_by_internal_id(*table)
                    .ok_or_else(|| crate::LimboError::ParseError("table not found".to_string()))?;
                if let Some(btree) = table_ref.btree() {
                    if let Some((_, rowid_alias_col)) = btree.get_rowid_alias_column() {
                        if maybe_column_collseq.is_none() {
                            maybe_column_collseq = rowid_alias_col.collation_opt();
                        }
                    }
                }
                return Ok(WalkControl::Continue);
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok((maybe_explicit_collseq, maybe_column_collseq))
}

#[cfg(test)]
#[path = "../tests/unit/translate/collate/tests.rs"]
mod tests;
