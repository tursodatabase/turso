use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{Hash, Hasher},
    str::FromStr as _,
};

use icu_collator::{options::CollatorOptions, Collator, CollatorBorrowed};
use icu_locale::Locale;
use turso_parser::ast::{Expr, Operator, UnaryOperator};

use crate::{
    connection::SymbolTable,
    sync::{LazyLock, Mutex, RwLock},
    translate::plan::TableReferences,
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
    let (explicit, column) =
        collseq_parts_recursive(top_expr, referenced_tables, symbol_table, true, false, true)?;
    Ok(explicit
        .map(|collation| (collation, true))
        .or_else(|| column.map(|collation| (collation, false))))
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
    collseq_parts_recursive(
        top_expr,
        referenced_tables,
        symbol_table,
        false,
        true,
        false,
    )
}

/// Collect the `(explicit, column)` collation parts of an expression following
/// SQLite's `sqlite3ExprCollSeq` rules.
///
/// An explicit COLLATE in a hoist position wins outright, and a column's
/// declared collation only contributes through collation-transparent
/// expressions (CAST, unary +/-, arithmetic operators, the column itself).
/// Collation-opaque expressions (CONCAT, function calls, CASE, ...) block the
/// column collation, but an explicit COLLATE on a hoist position (either
/// operand of a binary operator, any argument of a function, any THEN/ELSE
/// branch of a CASE) still carries over to the result, first one wins.
#[allow(clippy::too_many_arguments)]
fn collseq_parts_recursive(
    expr: &Expr,
    referenced_tables: &TableReferences,
    symbol_table: Option<&SymbolTable>,
    column_with_binary_default: bool,
    handle_rowid_alias: bool,
    skip_self_table_columns: bool,
) -> Result<(Option<CollationSeq>, Option<CollationSeq>)> {
    let recurse = |sub: &Expr| {
        collseq_parts_recursive(
            sub,
            referenced_tables,
            symbol_table,
            column_with_binary_default,
            handle_rowid_alias,
            skip_self_table_columns,
        )
    };
    match expr {
        Expr::Collate(_, seq) => Ok((
            Some(resolve_collation_name(seq.as_str(), symbol_table).unwrap_or_default()),
            None,
        )),
        Expr::Cast { expr, .. } | Expr::FieldAccess { base: expr, .. } => recurse(expr),
        Expr::Unary(UnaryOperator::Positive | UnaryOperator::Negative, operand) => recurse(operand),
        Expr::Parenthesized(exprs) => {
            if let [inner] = exprs.as_slice() {
                recurse(inner)
            } else {
                Ok((None, None))
            }
        }
        Expr::Binary(lhs, operator, rhs) => {
            let (lhs_explicit, lhs_column) = recurse(lhs)?;
            let (rhs_explicit, rhs_column) = recurse(rhs)?;
            if *operator == Operator::Concat {
                // CONCAT is collation-opaque. Only an explicit COLLATE on
                // either operand hoists to the whole expression (left wins);
                // a column's declared collation must not leak through.
                Ok((lhs_explicit.or(rhs_explicit), None))
            } else if operator.is_comparison() {
                // Comparison operators consume the collation of their
                // operands; the comparison result itself compares BINARY.
                Ok((None, None))
            } else if matches!(
                operator,
                Operator::Add
                    | Operator::Subtract
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::Modulus
                    | Operator::BitwiseAnd
                    | Operator::BitwiseOr
                    | Operator::LeftShift
                    | Operator::RightShift
            ) {
                // Arithmetic operators are collation-transparent.
                Ok((lhs_explicit.or(rhs_explicit), lhs_column.or(rhs_column)))
            } else {
                Ok((None, None))
            }
        }
        Expr::FunctionCall { args, .. } => {
            let mut explicit = None;
            for arg in args.iter() {
                let (arg_explicit, _) = recurse(arg)?;
                if explicit.is_none() {
                    explicit = arg_explicit;
                }
            }
            Ok((explicit, None))
        }
        Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => {
            let mut explicit = None;
            for (_, then_expr) in when_then_pairs.iter() {
                let (then_explicit, _) = recurse(then_expr)?;
                if explicit.is_none() {
                    explicit = then_explicit;
                }
            }
            if explicit.is_none() {
                if let Some(else_expr) = else_expr {
                    let (else_explicit, _) = recurse(else_expr)?;
                    explicit = else_explicit;
                }
            }
            Ok((explicit, None))
        }
        Expr::Column { table, column, .. } => {
            // Generated columns (the SELF_TABLE placeholder) don't inherit an
            // implicit collation from their expression, so we skip them.
            if skip_self_table_columns && table.is_self_table() {
                return Ok((None, None));
            }
            let (_, table_ref) = referenced_tables
                .find_table_by_internal_id(*table)
                .ok_or_else(|| crate::LimboError::ParseError("table not found".to_string()))?;
            let column = table_ref
                .get_column_at(*column)
                .ok_or_else(|| crate::LimboError::ParseError("column not found".to_string()))?;
            let collation = if column_with_binary_default {
                Some(column.collation())
            } else {
                column.collation_opt()
            };
            Ok((None, collation))
        }
        Expr::RowId { table, .. } => {
            if !handle_rowid_alias {
                return Ok((None, None));
            }
            let (_, table_ref) = referenced_tables
                .find_table_by_internal_id(*table)
                .ok_or_else(|| crate::LimboError::ParseError("table not found".to_string()))?;
            let mut collation = None;
            if let Some(btree) = table_ref.btree() {
                if let Some((_, rowid_alias_col)) = btree.get_rowid_alias_column() {
                    collation = rowid_alias_col.collation_opt();
                }
            }
            Ok((None, collation))
        }
        _ => Ok((None, None)),
    }
}

#[cfg(test)]
mod tests {
    use crate::alloc::vec;
    use crate::{sync::Arc, MAIN_DB_ID};

    use turso_parser::ast::{
        FunctionTail, Literal, Name, Operator, TableInternalId, UnaryOperator,
    };

    use crate::{
        schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, Table, Type},
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
    fn test_get_collseq_from_expr_single_table_single_column() {
        // plain column
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
            let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
            assert_eq!(collseq, collation);
        }
    }

    #[test]
    fn test_get_collseq_from_expr_single_table_single_column_with_collate() {
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
            let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
            assert_eq!(collseq, Some(expected_collation));
        }
    }

    #[test]
    fn test_get_collseq_from_expr_multiple_collate_leftmost_wins() {
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
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::Rtrim));
    }

    #[test]
    fn test_get_collseq_from_expr_unary_plus_and_cast_still_column() {
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
        let collseq_plus = get_collseq_from_expr(&expr_plus, &table_references).unwrap();
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
        let collseq_cast = get_collseq_from_expr(&expr_cast, &table_references).unwrap();
        assert_eq!(collseq_cast, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_get_collseq_from_expr_explicit_collate_anywhere_in_operand() {
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
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::Rtrim));
    }

    #[test]
    fn test_get_collseq_from_expr_column_plus_column_leftside_column_wins() {
        let table_references = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            Some(CollationSeq::Rtrim),
        );
        // col1 + col2 -- col1's NOCASE collation wins since it's on the left side
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
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_get_collseq_from_expr_collate_vs_collate_leftside_expr_wins() {
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
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_get_collseq_from_expr_default_binary_when_no_collate_or_column() {
        let table_references = TableReferences::new_empty();
        let expr = Expr::Literal(Literal::String("abc".to_string()));
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, None);
    }

    #[test]
    fn test_get_collseq_from_expr_rowid_uses_rowid_alias_collation() {
        let table_references = get_table_references_single_table_rowid_alias_with_collation(Some(
            CollationSeq::NoCase,
        ));
        let expr = Expr::RowId {
            database: None,
            table: TableInternalId::from(1),
        };
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_resolve_comparison_collseq_nocase_column_vs_binary_default() {
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
            resolve_comparison_collseq(&lhs, &rhs, &table_refs).unwrap(),
            CollationSeq::NoCase
        );
        // Swapped: RHS has NOCASE, LHS has no collation → still NOCASE
        assert_eq!(
            resolve_comparison_collseq(&rhs, &lhs, &table_refs).unwrap(),
            CollationSeq::NoCase
        );
    }

    #[test]
    fn test_resolve_comparison_collseq_explicit_beats_column() {
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
            resolve_comparison_collseq(&lhs, &rhs, &table_refs).unwrap(),
            CollationSeq::Rtrim
        );
    }

    #[test]
    fn test_resolve_comparison_collseq_both_default_is_binary() {
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
            resolve_comparison_collseq(&lhs, &rhs, &table_refs).unwrap(),
            CollationSeq::Binary
        );
    }

    #[test]
    fn test_get_collseq_from_expr_concat_is_opaque_to_column_collation() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::NoCase),
        );
        // (col || '') — CONCAT is collation-opaque, so the column's declared
        // NOCASE collation must not leak to the parent comparison.
        let lhs = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let rhs = Expr::Literal(Literal::String(String::new()));
        let expr = Expr::binary(lhs, Operator::Concat, rhs);
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, None);
    }

    #[test]
    fn test_get_collseq_from_expr_concat_hoists_explicit_collate() {
        let table_references = TableReferences::new_empty();
        // (x COLLATE NOCASE || '') — explicit COLLATE hoists through CONCAT.
        let lhs = Expr::Collate(
            Box::new(Expr::Literal(Literal::String("x".to_string()))),
            Name::exact("NOCASE".to_string()),
        );
        let rhs = Expr::Literal(Literal::String(String::new()));
        let expr = Expr::binary(lhs, Operator::Concat, rhs);
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));

        // ('' || x COLLATE NOCASE) — explicit COLLATE hoists from the right
        // operand when the left one has none.
        let lhs = Expr::Literal(Literal::String(String::new()));
        let rhs = Expr::Collate(
            Box::new(Expr::Literal(Literal::String("x".to_string()))),
            Name::exact("NOCASE".to_string()),
        );
        let expr = Expr::binary(lhs, Operator::Concat, rhs);
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_get_collseq_from_expr_function_call_is_opaque() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::NoCase),
        );
        let col = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        // trim(col) — a function call is collation-opaque.
        let expr = Expr::FunctionCall {
            name: Name::exact("trim".to_string()),
            distinctness: None,
            args: std::vec![Box::new(col.clone())],
            order_by: std::vec![],
            within_group: std::vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, None);

        // trim(col COLLATE NOCASE) — explicit COLLATE on an argument hoists
        // to the function result.
        let arg = Expr::Collate(Box::new(col), Name::exact("NOCASE".to_string()));
        let expr = Expr::FunctionCall {
            name: Name::exact("trim".to_string()),
            distinctness: None,
            args: std::vec![Box::new(arg)],
            order_by: std::vec![],
            within_group: std::vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_get_collseq_from_expr_case_is_opaque() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::NoCase),
        );
        let col = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        // CASE WHEN 1 THEN col END — a CASE expression is collation-opaque.
        let expr = Expr::Case {
            base: None,
            when_then_pairs: std::vec![(
                Box::new(Expr::Literal(Literal::Numeric(String::from("1")))),
                Box::new(col.clone()),
            )],
            else_expr: None,
        };
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, None);

        // Explicit COLLATE on a branch hoists to the CASE result.
        let then_expr = Expr::Collate(Box::new(col), Name::exact("NOCASE".to_string()));
        let expr = Expr::Case {
            base: None,
            when_then_pairs: std::vec![(
                Box::new(Expr::Literal(Literal::Numeric(String::from("1")))),
                Box::new(then_expr),
            )],
            else_expr: None,
        };
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_expr_collation_ctx_concat_is_opaque() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::NoCase),
        );
        let col = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        // Bare column keeps its (default-BINARY or declared) collation.
        let ctx = get_expr_collation_ctx_with_symbols(&col, &table_references, None).unwrap();
        assert_eq!(ctx, Some((CollationSeq::NoCase, false)));
        // Wrapped in CONCAT the implicit collation must be blocked.
        let expr = Expr::binary(
            col,
            Operator::Concat,
            Expr::Literal(Literal::String(String::new())),
        );
        let ctx = get_expr_collation_ctx_with_symbols(&expr, &table_references, None).unwrap();
        assert_eq!(ctx, None);
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
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            identifier: "foo".to_string(),
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
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            identifier: "t1".to_string(),
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
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            identifier: "t2".to_string(),
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
                primary_key: true,
                rowid_alias: true,
                notnull: false,
                explicit_notnull: false,
                unique: true,
                hidden: false,
                notnull_conflict_clause: None,
            },
        )];
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            identifier: "bar".to_string(),
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
