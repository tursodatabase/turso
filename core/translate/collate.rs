use std::{borrow::Borrow, cmp::Ordering, collections::HashMap, hash::Hasher, str::FromStr as _};

use icu_collator::{options::CollatorOptions, Collator, CollatorBorrowed};
use icu_locale::Locale;
use smallvec::SmallVec;
use turso_parser::ast::{Expr, UnaryOperator};

use crate::{
    function::ExternalCollation,
    schema::Schema,
    sync::{Arc, LazyLock, RwLock, Weak},
    translate::{
        catalog::CatalogContext,
        emitter::Resolver,
        expr::{
            expr_type::{query_column_type, type_of, type_ordering, ExprType},
            row_value_component_expr, row_value_len,
        },
        expr_context::{ExprContext, ExprProperties},
        plan::{Plan, SelectPlan, TableReferences},
    },
    type_ordering::TypeOrdering,
    types::ValueRef,
    vdbe::hash_table::normalized_f64_bits,
    LimboError, Numeric, Result,
};

/// How two values compare.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum CollationSeq {
    /// SQLite's comparison: values of different kinds order as NULL, numbers,
    /// text, blobs; numbers compare numerically, blobs byte by byte, and two
    /// texts with the text collation.
    Text(TextCollation),
    /// The order of a custom type, such as numeric's order by value. It
    /// decides how every pair of values compares. A comparison gets it when an
    /// operand's type has an order.
    Type(TypeOrdering),
}

/// How two texts compare under SQLite's comparison.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum TextCollation {
    Binary,
    NoCase,
    Rtrim,
    Locale(LocaleCollationId),
    /// A collation that an application registered on a connection, by its
    /// number in [CUSTOM_COLLATIONS]. Two registrations of the same name get
    /// different numbers.
    Custom(u32),
}

/// Every registered custom collation, by its number in [CollationSeq::Custom].
/// Comparing with a custom collation looks up its callback here, so the
/// comparison works without a connection.
///
/// An entry is removed when its [ExternalCollation] is dropped. A connection
/// keeps its custom collations alive while they are registered, and a program
/// keeps the ones of its connection alive while it exists, so every
/// `CollationSeq::Custom` that a plan or a program holds has an entry.
static CUSTOM_COLLATIONS: LazyLock<RwLock<CustomCollations>> =
    LazyLock::new(|| RwLock::new(CustomCollations::default()));

#[derive(Default)]
struct CustomCollations {
    next_number: u32,
    by_number: HashMap<u32, Weak<ExternalCollation>>,
}

impl CollationSeq {
    pub const BINARY: Self = Self::Text(TextCollation::Binary);
    pub const NOCASE: Self = Self::Text(TextCollation::NoCase);
    pub const RTRIM: Self = Self::Text(TextCollation::Rtrim);

    pub fn new(collation: &str) -> crate::Result<Self> {
        TextCollation::new(collation).map(Self::Text)
    }

    /// The collation for comparing values the engine made itself: rowids,
    /// counters, rows of sqlite_schema, encoded records. It compares bytes
    /// like BINARY. A comparison of user values gets its collation from
    /// [comparison_collation] or [expr_collation] instead, so the two kinds of
    /// comparison look different in the code.
    pub const fn for_internal_values() -> Self {
        Self::BINARY
    }

    /// The value [Self::to_bits] never returns for a built-in or locale
    /// collation, so stored bits can use it for "no collation".
    pub const NO_COLLATION_BITS: u16 = 0;

    /// A compact number for the collation, for storing it in column flags and
    /// aggregate state. A custom collation has none and gets
    /// [Self::NO_COLLATION_BITS].
    #[inline]
    pub const fn to_bits(self) -> u16 {
        match self {
            Self::Text(TextCollation::Binary) => 1,
            Self::Text(TextCollation::NoCase) => 2,
            Self::Text(TextCollation::Rtrim) => 3,
            Self::Text(TextCollation::Locale(id)) => id.to_bits(),
            Self::Text(TextCollation::Custom(_)) => Self::NO_COLLATION_BITS,
            Self::Type(ordering) => Self::FIRST_TYPE_ORDERING_BITS + ordering as u16,
        }
    }

    /// Type orderings take the numbers from here up to the first number of a
    /// locale collation.
    const FIRST_TYPE_ORDERING_BITS: u16 = 4;

    /// The collation [Self::to_bits] turned into `bits`. Any other value,
    /// including [Self::NO_COLLATION_BITS], is an error.
    pub fn from_bits(bits: u16) -> Result<Self> {
        match bits {
            1 => Ok(Self::BINARY),
            2 => Ok(Self::NOCASE),
            3 => Ok(Self::RTRIM),
            bits if (Self::FIRST_TYPE_ORDERING_BITS..LocaleCollationId::FIRST_STORAGE_BIT)
                .contains(&bits) =>
            {
                let ordering = TypeOrdering::ALL
                    .get(usize::from(bits - Self::FIRST_TYPE_ORDERING_BITS))
                    .ok_or_else(|| {
                        LimboError::InternalError(format!("no collation has the number {bits}"))
                    })?;
                Ok(Self::Type(*ordering))
            }
            bits if bits >= LocaleCollationId::FIRST_STORAGE_BIT
                && LocaleCollationRegistry::global()
                    .is_registered(LocaleCollationId::from_bits(bits)) =>
            {
                Ok(Self::Text(TextCollation::Locale(
                    LocaleCollationId::from_bits(bits),
                )))
            }
            bits => Err(LimboError::InternalError(format!(
                "no collation has the number {bits}"
            ))),
        }
    }

    #[inline]
    pub const fn is_custom(self) -> bool {
        matches!(self, Self::Text(TextCollation::Custom(_)))
    }

    /// Whether values can be hashed so that values this collation calls equal
    /// get the same hash. A custom collation is an arbitrary callback, so
    /// queries that compare with one must not use a hash table.
    #[inline]
    pub const fn can_hash(self) -> bool {
        !self.is_custom()
    }

    pub fn name(self) -> String {
        match self {
            Self::Text(collation) => collation.name(),
            Self::Type(ordering) => ordering.name().to_string(),
        }
    }

    /// Orders two values. With a type's order, as the type orders them.
    /// Otherwise values of different kinds order as NULL, numbers, text,
    /// blobs, and two texts compare with the text collation.
    #[inline(always)]
    pub fn compare(self, lhs: ValueRef, rhs: ValueRef) -> Ordering {
        match (self, lhs, rhs) {
            (Self::Type(ordering), _, _) => ordering.compare(lhs, rhs),
            (Self::Text(collation), ValueRef::Text(lhs), ValueRef::Text(rhs)) => {
                collation.compare_strings(lhs.as_str(), rhs.as_str())
            }
            (Self::Text(_), _, _) => lhs.cmp(&rhs),
        }
    }

    /// [Self::compare] for the two sides of a comparison operator, which
    /// fails where the type's [TypeOrdering::compare_operands] does.
    #[inline(always)]
    pub fn compare_operands(self, lhs: ValueRef, rhs: ValueRef) -> Result<Ordering> {
        match self {
            Self::Type(ordering) => ordering.compare_operands(lhs, rhs),
            Self::Text(_) => Ok(self.compare(lhs, rhs)),
        }
    }

    /// Feeds `value` to `hasher` so that values that [Self::compare] calls
    /// equal hash the same. Integers hash like the equal REAL, so 10 and 10.0
    /// land in the same bucket.
    pub fn hash_value(self, value: ValueRef, hasher: &mut impl Hasher) {
        let collation = match self {
            Self::Type(ordering) => return ordering.hash(value, hasher),
            Self::Text(collation) => collation,
        };
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
                collation.hash_text(text.as_str(), hasher);
            }
            ValueRef::Blob(blob) => {
                hasher.write_u8(BLOB_HASH);
                hasher.write(blob);
            }
        }
    }
}

impl TextCollation {
    /// Gives a new custom collation a number and returns the
    /// [ExternalCollation] that `new_external` makes for it. Comparisons with
    /// the collation call that ExternalCollation's callback.
    pub(crate) fn new_custom(
        new_external: impl FnOnce(Self) -> ExternalCollation,
    ) -> Arc<ExternalCollation> {
        let mut registry = CUSTOM_COLLATIONS.write();
        let number = registry.next_number;
        registry.next_number = number
            .checked_add(1)
            .expect("a process registers fewer than 2^32 custom collations");
        let external = Arc::new(new_external(Self::Custom(number)));
        registry.by_number.insert(number, Arc::downgrade(&external));
        external
    }

    pub const fn is_custom(self) -> bool {
        matches!(self, Self::Custom(_))
    }

    /// Removes a custom collation whose [ExternalCollation] is being dropped.
    pub(crate) fn remove_custom(self) {
        let Self::Custom(number) = self else {
            panic!("{self:?} is not a custom collation");
        };
        CUSTOM_COLLATIONS.write().by_number.remove(&number);
    }

    /// The collation named in a COLLATE clause.
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

    pub fn name(self) -> String {
        match self {
            Self::Binary => "Binary".to_string(),
            Self::NoCase => "NoCase".to_string(),
            Self::Rtrim => "RTrim".to_string(),
            Self::Locale(id) => LocaleCollationRegistry::global().name(id),
            Self::Custom(number) => custom_collation(number).name.clone(),
        }
    }

    fn hash_text(self, text: &str, hasher: &mut impl Hasher) {
        match self {
            Self::Binary => hasher.write(text.as_bytes()),
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
                unreachable!("hash tables reject collations that cannot hash (see can_hash)")
            }
        }
    }

    #[inline(always)]
    pub fn compare_strings(self, lhs: &str, rhs: &str) -> Ordering {
        match self {
            Self::Binary => binary_cmp(lhs, rhs),
            Self::NoCase => nocase_cmp(lhs, rhs),
            Self::Rtrim => rtrim_cmp(lhs, rhs),
            Self::Locale(id) => LocaleCollationRegistry::global().compare(id, lhs, rhs),
            Self::Custom(number) => custom_collation(number).compare(lhs, rhs),
        }
    }
}

fn custom_collation(number: u32) -> Arc<ExternalCollation> {
    // The read lock is released before the returned Arc can be dropped,
    // because dropping the last Arc takes the write lock.
    CUSTOM_COLLATIONS
        .read()
        .by_number
        .get(&number)
        .and_then(Weak::upgrade)
        .expect("a plan or program keeps its custom collations alive")
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

impl std::fmt::Display for TextCollation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name())
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
    const FIRST_STORAGE_BIT: u16 =
        CollationSeq::FIRST_TYPE_ORDERING_BITS + TypeOrdering::ALL.len() as u16;

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

    fn is_registered(&self, id: LocaleCollationId) -> bool {
        (id.0 as usize) < self.collations.read().len()
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

/// The collation a comparison between `lhs` and `rhs` uses. This and
/// [expr_collation] are the only places that decide a collation from
/// expressions; every comparison, sort key, grouping key and hash key gets its
/// collation from one of them.
///
/// When an operand's type has its own order (numeric and arrays), the
/// comparison uses that order. An
/// explicit COLLATE next to such an operand, or two operands with different
/// orders, is an error, as in PostgreSQL. Otherwise the rules are SQLite's
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
    context: &ExprContext<'_, '_>,
) -> Result<CollationSeq> {
    comparison_collation_across_queries(lhs, context, rhs, context)
}

/// [comparison_collation] for `x IN (SELECT y ...)` and other comparisons
/// whose operands belong to different queries, so each operand's columns are
/// looked up in its own tables.
pub fn comparison_collation_across_queries(
    lhs: &Expr,
    lhs_context: &ExprContext<'_, '_>,
    rhs: &Expr,
    rhs_context: &ExprContext<'_, '_>,
) -> Result<CollationSeq> {
    if let Some(ordering) = common_type_ordering([(lhs, lhs_context), (rhs, rhs_context)])? {
        return Ok(CollationSeq::Type(ordering));
    }
    let catalog = lhs_context.catalog();
    let collation = if has_collate_operator(lhs) {
        expr_collation_if_any(lhs, lhs_context, catalog)?
    } else if has_collate_operator(rhs) {
        expr_collation_if_any(rhs, rhs_context, catalog)?
    } else {
        match expr_collation_if_any(lhs, lhs_context, catalog)? {
            Some(collation) => Some(collation),
            None => expr_collation_if_any(rhs, rhs_context, catalog)?,
        }
    };
    Ok(CollationSeq::Text(
        collation.unwrap_or(TextCollation::Binary),
    ))
}

/// [comparison_collation_across_queries] for `x IN (SELECT ...)`, where the
/// right operand is column `column` of `query`. The column's type is the one
/// [query_column_type] gives it, which in a compound SELECT comes from every
/// SELECT, not only from `rhs`, the column of the rightmost SELECT. `rhs`
/// gives the text collation, as in SQLite.
pub fn in_subquery_collation(
    lhs: &Expr,
    lhs_tables: Option<&TableReferences>,
    query: &Plan,
    column: usize,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    let rhs = &query.select_result_columns()[column].expr;
    let lhs_context = ExprContext::new(lhs_tables, resolver);
    let rhs_context = ExprContext::new(Some(query.select_table_references()), resolver);
    let types = [
        type_of(lhs, &lhs_context.type_env()),
        query_column_type(query, column, resolver),
    ];
    let has_collate = has_collate_operator(lhs) || has_collate_operator(rhs);
    if let Some(ordering) = common_ordering(&types, resolver.schema(), has_collate)? {
        return Ok(CollationSeq::Type(ordering));
    }
    comparison_collation_across_queries(lhs, &lhs_context, rhs, &rhs_context)
}

/// The collation that `x IN (y1, ..., yN)` compares with. When x or any item
/// has a type with an order, every comparison uses that order, so
/// `'10.0' IN (n)` agrees with `'10.0' = n`. Otherwise the text collation is
/// the one of x alone, as in SQLite.
pub fn in_list_collation(
    lhs: &Expr,
    rhs: &[Box<Expr>],
    context: &ExprContext<'_, '_>,
) -> Result<CollationSeq> {
    let operands = std::iter::once(lhs)
        .chain(rhs.iter().map(|item| item.as_ref()))
        .map(|expr| (expr, context));
    if let Some(ordering) = common_type_ordering(operands)? {
        return Ok(CollationSeq::Type(ordering));
    }
    expr_text_collation(lhs, context, context.catalog()).map(CollationSeq::Text)
}

/// The collation of one expression, BINARY when it has none. Sort, grouping,
/// DISTINCT, compound select, window partition and index keys use it. An
/// expression whose type has an order uses that order.
pub fn expr_collation(expr: &Expr, context: &ExprContext<'_, '_>) -> Result<CollationSeq> {
    if let Some(ordering) = common_type_ordering([(expr, context)])? {
        return Ok(CollationSeq::Type(ordering));
    }
    expr_text_collation(expr, context, context.catalog()).map(CollationSeq::Text)
}

/// The text collation of an expression by SQLite's rules alone, BINARY when
/// it has none. A subquery or view column stores it apart from the order of
/// its type (see `Column::text_collation`), so that `CAST(col AS text)` in the
/// outer query, whose type has no order, compares with the text collation.
pub(crate) fn expr_text_collation(
    expr: &Expr,
    metadata: &impl ExprProperties,
    catalog: &CatalogContext<'_>,
) -> Result<TextCollation> {
    Ok(expr_collation_if_any(expr, metadata, catalog)?.unwrap_or(TextCollation::Binary))
}

/// The collation of the first expression that has one, BINARY when none has.
///
/// SQLite uses this rule in two places: `min`, `max` and `nullif` compare their
/// arguments with the collation of the first argument that has one (functions
/// marked `SQLITE_FUNC_NEEDCOLL`), and a compound SELECT compares a result
/// column with the collation of the leftmost SELECT whose column has one.
/// Each expression comes with the context its columns refer to. As in
/// [comparison_collation], an expression whose type has an order decides.
pub fn first_collation<'a, 'c, 's: 'c>(
    exprs: impl IntoIterator<Item = (&'a Expr, ExprContext<'c, 's>)> + Clone,
) -> Result<CollationSeq> {
    if let Some(ordering) = common_type_ordering(exprs.clone())? {
        return Ok(CollationSeq::Type(ordering));
    }
    first_text_collation(exprs).map(CollationSeq::Text)
}

/// The collation that column `column` of a compound SELECT compares with,
/// where `arms` are its SELECTs, by the rule of [first_collation]: a SELECT
/// whose column has an order decides, so `SELECT n FROM t UNION SELECT 10`
/// compares in numeric order and keeps one of `10.00` and `10`. Otherwise the
/// text collation of the leftmost SELECT whose column has one, as in SQLite's
/// `multiSelectCollSeq`.
pub fn compound_column_collation(
    arms: &[&SelectPlan],
    column: usize,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    first_collation(compound_column_exprs(arms, column, resolver))
}

/// The collation of a compound SELECT's ORDER BY term `column COLLATE name`,
/// where `arms` are its SELECTs. The COLLATE applies to the column of every
/// SELECT, so it is an error when any of them has a type that takes none, as
/// in a comparison.
pub fn compound_order_by_collation(
    arms: &[&SelectPlan],
    column: usize,
    explicit_collation: TextCollation,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    common_ordering_of(compound_column_exprs(arms, column, resolver), true)?;
    Ok(CollationSeq::Text(explicit_collation))
}

/// Column `column` of every SELECT in `arms`, each with the context its
/// columns refer to.
fn compound_column_exprs<'a, 's>(
    arms: &'a [&'a SelectPlan],
    column: usize,
    resolver: &'a Resolver<'s>,
) -> impl Iterator<Item = (&'a Expr, ExprContext<'a, 's>)> + Clone + 'a {
    arms.iter().map(move |select| {
        (
            &select.result_columns[column].expr,
            ExprContext::new(Some(&select.table_references), resolver),
        )
    })
}

/// The text collation of the first expression that has one, BINARY when none
/// has.
pub(crate) fn first_text_collation<'a, 'c, 's: 'c>(
    exprs: impl IntoIterator<Item = (&'a Expr, ExprContext<'c, 's>)>,
) -> Result<TextCollation> {
    for (expr, context) in exprs {
        if let Some(collation) = expr_collation_if_any(expr, &context, context.catalog())? {
            return Ok(collation);
        }
    }
    Ok(TextCollation::Binary)
}

/// The order that values compared with each other use when one of them has a
/// type with an order. Text collations do not apply to such types, so an
/// explicit COLLATE among them is an error, and so are two different orders:
/// PostgreSQL has no operator that compares numeric with an array. An
/// explicit COLLATE is also an error when any of the values has a custom type
/// that takes no COLLATE, such as `d = '2024-01-02' COLLATE nocase` for a
/// date column d.
fn common_type_ordering<'a, 'c, 's: 'c, C: Borrow<ExprContext<'c, 's>>>(
    exprs: impl IntoIterator<Item = (&'a Expr, C)> + Clone,
) -> Result<Option<TypeOrdering>> {
    let has_collate = exprs
        .clone()
        .into_iter()
        .any(|(expr, _)| has_collate_operator(expr));
    common_ordering_of(exprs, has_collate)
}

/// The one order among the types of `exprs`, as [common_type_ordering]
/// decides it. `has_collate` says whether a COLLATE applies to the values.
fn common_ordering_of<'a, 'c, 's: 'c, C: Borrow<ExprContext<'c, 's>>>(
    exprs: impl IntoIterator<Item = (&'a Expr, C)>,
    has_collate: bool,
) -> Result<Option<TypeOrdering>> {
    // Most comparisons have two operands; keep their types off the heap.
    let mut types: SmallVec<[ExprType; 2]> = SmallVec::new();
    let mut schema = None;
    for (expr, context) in exprs {
        let context: ExprContext<'c, 's> = *context.borrow();
        types.push(type_of(expr, &context.type_env()));
        schema.get_or_insert(context.schema());
    }
    match schema {
        Some(schema) => common_ordering(&types, schema, has_collate),
        None => Ok(None),
    }
}

/// The one order among values of `types`, as [common_type_ordering] decides
/// it. `has_collate` says whether a COLLATE applies to the values. Every
/// COLLATE that applies to a value is checked here, through [operand_order].
fn common_ordering(
    types: &[ExprType],
    schema: &Schema,
    has_collate: bool,
) -> Result<Option<TypeOrdering>> {
    let mut found: Option<(TypeOrdering, &ExprType)> = None;
    let mut array_of_unknown_elements = None;
    for ty in types {
        let ordering = match operand_order(ty, schema, has_collate)? {
            OperandOrder::None => continue,
            OperandOrder::ArrayOfUnknownElements => {
                array_of_unknown_elements.get_or_insert(ty);
                continue;
            }
            OperandOrder::Type(ordering) => ordering,
        };
        match found {
            None => found = Some((ordering, ty)),
            Some((other, _)) if other == ordering => found = Some((ordering, ty)),
            Some((_, other_ty)) => return Err(cannot_compare(other_ty, ty)),
        }
    }
    match (found, array_of_unknown_elements) {
        (None, Some(_)) => Ok(Some(TypeOrdering::Array)),
        (Some((other, other_ty)), Some(array_ty))
            if !matches!(other, TypeOrdering::Array | TypeOrdering::TextArray) =>
        {
            Err(cannot_compare(other_ty, array_ty))
        }
        (found, _) => Ok(found.map(|(ordering, _)| ordering)),
    }
}

/// How the values of a table or index column of type `ty` compare: the order
/// of its type when it has one, otherwise the explicit COLLATE, if any. A type
/// with its own order doesn't accept a COLLATE, and neither does a custom type
/// that isn't text.
pub(crate) fn key_collation(
    explicit_collation: Option<TextCollation>,
    ty: &ExprType,
    schema: &Schema,
) -> Result<Option<CollationSeq>> {
    let ordering = common_ordering(
        std::slice::from_ref(ty),
        schema,
        explicit_collation.is_some(),
    )?;
    Ok(match ordering {
        Some(ordering) => Some(CollationSeq::Type(ordering)),
        None => explicit_collation.map(CollationSeq::Text),
    })
}

/// Fails when `operand`, the operand of a COLLATE operator, has a type that
/// takes no COLLATE, such as `d COLLATE nocase` for a date d.
pub(crate) fn check_collate_operand(operand: &Expr, context: &ExprContext<'_, '_>) -> Result<()> {
    common_ordering_of([(operand, context)], true).map(|_| ())
}

/// Fails when `expr` itself, not counting its operands, applies a COLLATE to
/// a type that takes none or compares two types with different orders. These
/// are the errors that compiling `expr` would give, found by the same
/// checks, so that `WHERE 0 AND d = 'x' COLLATE nocase` fails like
/// `WHERE d = 'x' COLLATE nocase` for a date d.
pub(crate) fn check_expr_collation(expr: &Expr, context: &ExprContext<'_, '_>) -> Result<()> {
    match expr {
        Expr::Collate(operand, _) => check_collate_operand(operand, context),
        Expr::Binary(lhs, op, rhs) if op.is_comparison() => check_comparison(lhs, rhs, context),
        // Compiled as `lhs >= start AND lhs <= end`.
        Expr::Between {
            lhs, start, end, ..
        } => {
            check_comparison(lhs, start, context)?;
            check_comparison(lhs, end, context)
        }
        Expr::InList { lhs, rhs, .. } => {
            let operands =
                std::iter::once(lhs.as_ref()).chain(rhs.iter().map(|item| item.as_ref()));
            common_type_ordering(operands.map(|expr| (expr, context)))?;
            Ok(())
        }
        // Compiled as `base = when` for each WHEN.
        Expr::Case {
            base: Some(base),
            when_then_pairs,
            ..
        } => when_then_pairs
            .iter()
            .try_for_each(|(when, _)| check_comparison(base, when, context)),
        Expr::FunctionCall { name, args, .. } => {
            let func = context
                .catalog()
                .resolve_function(name.as_str(), args.len())?;
            if func.is_some_and(|func| func.needs_collation()) {
                common_type_ordering(args.iter().map(|arg| (arg.as_ref(), context)))?;
            }
            Ok(())
        }
        // These compare a value with the values of a subquery, whose types are
        // known only once it is planned. Planning it checks the comparison
        // through [in_subquery_collation].
        Expr::InSelect { .. } | Expr::InTable { .. } | Expr::SubqueryResult { .. } => Ok(()),
        // These compare nothing with a collation.
        Expr::Binary(..)
        | Expr::Case { base: None, .. }
        | Expr::Cast { .. }
        | Expr::Column { .. }
        | Expr::Default
        | Expr::DoublyQualified(..)
        | Expr::Exists(_)
        | Expr::FieldAccess { .. }
        | Expr::FunctionCallStar { .. }
        | Expr::Id(_)
        | Expr::IsNull(_)
        | Expr::Like { .. }
        | Expr::Literal(_)
        | Expr::MergedColumn(_)
        | Expr::Name(_)
        | Expr::NotNull(_)
        | Expr::Parenthesized(_)
        | Expr::Qualified(..)
        | Expr::Raise(..)
        | Expr::Register { .. }
        | Expr::RowId { .. }
        | Expr::Subquery(_)
        | Expr::Unary(..)
        | Expr::Variable(_) => Ok(()),
        Expr::Array { .. } | Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
    }
}

/// The check of [check_expr_collation] for `lhs` compared with `rhs`. Row
/// values, as in `(SELECT d, 1) = ('x' COLLATE nocase, 1)`, compare element by
/// element, each pair with its own collation, split as code generation splits
/// them.
fn check_comparison(lhs: &Expr, rhs: &Expr, context: &ExprContext<'_, '_>) -> Result<()> {
    let len = row_value_len(lhs)?.max(row_value_len(rhs)?);
    if len > 1 {
        for idx in 0..len {
            let lhs = row_value_component_expr(lhs, idx)?;
            let rhs = row_value_component_expr(rhs, idx)?;
            check_comparison(&lhs, &rhs, context)?;
        }
        return Ok(());
    }
    common_type_ordering([(lhs, context), (rhs, context)])?;
    Ok(())
}

/// Fails when values of `ty` take no COLLATE. As in PostgreSQL, only text
/// types take one: `date`, `uuid` and a type with an integer base don't,
/// `varchar` and a type with a text base do. No array takes one, because
/// arrays compare element by element without a collation, so the COLLATE
/// would be ignored. SQLite's own types all take one.
fn check_type_accepts_collate(ty: &ExprType, schema: &Schema) -> Result<()> {
    let accepts = match ty {
        ExprType::Custom { type_def, .. } => schema.type_accepts_collate(type_def)?,
        ExprType::Array { .. } => false,
        ExprType::Builtin(_) | ExprType::Untyped | ExprType::Null => true,
        // Nothing says the value is text, as in `(d * 2) COLLATE nocase` when no
        // function runs `*` for a date d.
        ExprType::Unknown => false,
        // Checked again once the type is known: `(SELECT d) COLLATE nocase` is
        // checked as a date when it is compiled.
        ExprType::NotKnownYet => true,
        // A COLLATE inside a row value, as in `(1, s COLLATE nocase) = (SELECT
        // 1, 'a')`, applies to one of its values, and that value is checked as
        // the operand of its own COLLATE.
        ExprType::RowValue => true,
    };
    if accepts {
        return Ok(());
    }
    Err(LimboError::ParseError(format!(
        "collations are not supported by type {ty}"
    )))
}

fn cannot_compare(a: &ExprType, b: &ExprType) -> LimboError {
    LimboError::ParseError(format!("cannot compare type {a} with type {b}"))
}

/// The order a value brings to a comparison with other values.
#[derive(Clone, Copy)]
enum OperandOrder {
    /// The value's type has no order.
    None,
    Type(TypeOrdering),
    /// An array whose element type nothing declares, such as `ARRAY[x]` over a
    /// column without a type. It compares with the order of the other values'
    /// array type, or as an array of numbers when there is none.
    ArrayOfUnknownElements,
}

/// The order a value of type `ty` brings to a comparison. `has_collate` says
/// whether a COLLATE applies to the compared values; it is an error when `ty`
/// takes none.
fn operand_order(ty: &ExprType, schema: &Schema, has_collate: bool) -> Result<OperandOrder> {
    if has_collate {
        check_type_accepts_collate(ty, schema)?;
    }
    if ty.is_array_of_unknown_elements() {
        return Ok(OperandOrder::ArrayOfUnknownElements);
    }
    Ok(match type_ordering(ty, schema) {
        // The values compare in the type's order, which no COLLATE changes, so
        // a COLLATE here would be ignored.
        Some(_) if has_collate => {
            return Err(LimboError::ParseError(format!(
                "collations are not supported by type {ty}"
            )))
        }
        Some(ordering) => OperandOrder::Type(ordering),
        None => OperandOrder::None,
    })
}

/// The collation of one expression, or None when it has none (SQLite's
/// `sqlite3ExprCollSeq`). A column has its declared collation, BINARY when it
/// declares none, and so do a register and a variable that stand for a column
/// (rewritten column references, a trigger's NEW.x and OLD.x). A value an index
/// method provides in place of an expression it computes has none. CAST and unary `+` pass on the collation of their operand. A
/// row value has the collation of its first element. Any other expression has
/// the collation of its leftmost COLLATE operator, or none when it contains no
/// COLLATE operator.
pub fn expr_collation_if_any(
    expr: &Expr,
    metadata: &impl ExprProperties,
    catalog: &CatalogContext<'_>,
) -> Result<Option<TextCollation>> {
    match expr {
        Expr::Column { table, column, .. } => metadata.column_collation(*table, *column),
        Expr::RowId { .. } => Ok(None),
        Expr::Register { register, .. } => Ok(metadata.register_collation(*register)),
        Expr::Variable(variable) => variable
            .col_collation
            .as_deref()
            .map(|name| catalog.resolve_collation(name))
            .transpose(),
        Expr::MergedColumn(columns) => {
            let first_column = columns
                .first()
                .expect("a merged column must have at least two source columns");
            expr_collation_if_any(first_column, metadata, catalog)
        }
        Expr::Cast { expr, .. } | Expr::Unary(UnaryOperator::Positive, expr) => {
            expr_collation_if_any(expr, metadata, catalog)
        }
        Expr::Parenthesized(exprs) => {
            let first = exprs.first().expect("parentheses must hold an expression");
            expr_collation_if_any(first, metadata, catalog)
        }
        Expr::Collate(_, name) => Ok(Some(catalog.resolve_collation(name.as_str())?)),
        _ => {
            let mut operand_with_collate = None;
            for_each_operand(expr, &mut |operand| {
                if operand_with_collate.is_none() && has_collate_operator(operand) {
                    operand_with_collate = Some(operand);
                }
            });
            match operand_with_collate {
                Some(operand) => expr_collation_if_any(operand, metadata, catalog),
                None => Ok(None),
            }
        }
    }
}

/// A column has its declared collation, BINARY when it declares none. A rowid
/// alias (INTEGER PRIMARY KEY) is the rowid and has none, as in SQLite.
pub(crate) fn column_collation(column: &crate::schema::Column) -> Option<TextCollation> {
    (!column.is_rowid_alias()).then(|| column.text_collation().unwrap_or(TextCollation::Binary))
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
        | Expr::Register { .. }
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
