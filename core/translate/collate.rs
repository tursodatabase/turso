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
        expr::expr_type::{query_column_type, type_of, type_ordering, ExprType},
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
    fn compare_strings(self, lhs: &str, rhs: &str) -> Ordering {
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
    let rhs_order = operand_order(
        &query_column_type(query, column, resolver),
        resolver.schema(),
    );
    let lhs_order = expr_operand_order(lhs, &lhs_context);
    let has_collate = has_collate_operator(lhs) || has_collate_operator(rhs);
    if let Some(ordering) = common_ordering([lhs_order, rhs_order], has_collate)? {
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
    let columns = arms.iter().map(|select| {
        (
            &select.result_columns[column].expr,
            ExprContext::new(Some(&select.table_references), resolver),
        )
    });
    first_collation(columns)
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
/// PostgreSQL has no operator that compares numeric with an array.
fn common_type_ordering<'a, 'c, 's: 'c>(
    exprs: impl IntoIterator<Item = (&'a Expr, impl Borrow<ExprContext<'c, 's>>)>,
) -> Result<Option<TypeOrdering>> {
    let mut has_collate = false;
    // Most comparisons have two operands; keep their orders off the heap.
    let orders: SmallVec<[OperandOrder; 2]> = exprs
        .into_iter()
        .map(|(expr, context)| {
            has_collate |= has_collate_operator(expr);
            expr_operand_order(expr, context.borrow())
        })
        .collect();
    common_ordering(orders, has_collate)
}

/// The one order among `orders`, as [common_type_ordering] decides it.
/// `has_collate` says whether any of the values has a COLLATE operator.
fn common_ordering(
    orders: impl IntoIterator<Item = OperandOrder>,
    has_collate: bool,
) -> Result<Option<TypeOrdering>> {
    let mut found: Option<TypeOrdering> = None;
    let mut has_array_of_unknown_elements = false;
    for order in orders {
        let ordering = match order {
            OperandOrder::None => continue,
            OperandOrder::ArrayOfUnknownElements => {
                has_array_of_unknown_elements = true;
                continue;
            }
            OperandOrder::Type(ordering) => ordering,
        };
        match found {
            Some(other) if other != ordering => return Err(cannot_compare(other, ordering)),
            _ => found = Some(ordering),
        }
    }
    let found = match (found, has_array_of_unknown_elements) {
        (None, true) => Some(TypeOrdering::Array),
        (Some(other), true) if !matches!(other, TypeOrdering::Array | TypeOrdering::TextArray) => {
            return Err(cannot_compare(other, TypeOrdering::Array));
        }
        (found, _) => found,
    };
    if let (Some(ordering), true) = (found, has_collate) {
        return Err(LimboError::ParseError(format!(
            "collations are not supported by type {}",
            ordering.name()
        )));
    }
    Ok(found)
}

fn cannot_compare(a: TypeOrdering, b: TypeOrdering) -> LimboError {
    LimboError::ParseError(format!(
        "cannot compare type {} with type {}",
        a.name(),
        b.name()
    ))
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

fn expr_operand_order(expr: &Expr, context: &ExprContext<'_, '_>) -> OperandOrder {
    let ty = type_of(expr, &context.type_env());
    operand_order(&ty, context.schema())
}

fn operand_order(ty: &ExprType, schema: &Schema) -> OperandOrder {
    if ty.is_array_of_unknown_elements() {
        return OperandOrder::ArrayOfUnknownElements;
    }
    match type_ordering(ty, schema) {
        Some(ordering) => OperandOrder::Type(ordering),
        None => OperandOrder::None,
    }
}

/// The collation of one expression, or None when it has none (SQLite's
/// `sqlite3ExprCollSeq`). A column has its declared collation, BINARY when it
/// declares none, and so do a register and a variable that stand for a column
/// (rewritten column references, a trigger's NEW.x and OLD.x). A value an index
/// method provides in place of an expression it computes has none. CAST and unary `+` pass on the collation of their operand. A
/// row value has the collation of its first element. Any other expression has
/// the collation of its leftmost COLLATE operator, or none when it contains no
/// COLLATE operator.
fn expr_collation_if_any(
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

#[cfg(test)]
mod tests {
    use crate::alloc::vec;
    use crate::namespace::NamespaceRef;
    use crate::{sync::Arc, MAIN_DB_ID};

    use turso_parser::ast::{Literal, Name, Operator, TableInternalId, UnaryOperator};

    use crate::{
        schema::{BTreeCharacteristics, BTreeTable, ColDef, ColDefFlags, Column, Table, Type},
        translate::plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan},
        translate::{emitter::Resolver, expr_context::TableMetadata, plan::TableReferences},
    };

    use super::*;

    #[test]
    fn test_locale_collation_names() {
        assert!(matches!(
            TextCollation::new("fr-FR").unwrap(),
            TextCollation::Locale(_)
        ));
        assert!(matches!(
            TextCollation::new("es-u-co-trad").unwrap(),
            TextCollation::Locale(_)
        ));
        assert!(matches!(
            TextCollation::new("en-u-kf-upper").unwrap(),
            TextCollation::Locale(_)
        ));
        assert!(matches!(
            TextCollation::new("en-US-u-kf-upper").unwrap(),
            TextCollation::Locale(_)
        ));
        assert!(TextCollation::new("compile_options").is_err());
    }

    #[test]
    fn collation_bits_round_trip_and_unknown_bits_are_an_error() {
        let locale = CollationSeq::new("fr-FR").unwrap();
        for collation in [
            CollationSeq::BINARY,
            CollationSeq::NOCASE,
            CollationSeq::RTRIM,
            locale,
        ]
        .into_iter()
        .chain(TypeOrdering::ALL.map(CollationSeq::Type))
        {
            assert_eq!(
                CollationSeq::from_bits(collation.to_bits()).unwrap(),
                collation
            );
        }
        assert!(CollationSeq::from_bits(CollationSeq::NO_COLLATION_BITS).is_err());
        assert!(CollationSeq::from_bits(u16::MAX).is_err());
    }

    #[test]
    fn test_locale_collation_compare() {
        let traditional_spanish = TextCollation::new("es-u-co-trad").unwrap();
        assert_eq!(
            traditional_spanish.compare_strings("pollo", "polvo"),
            Ordering::Greater
        );

        let upper_first = TextCollation::new("en-u-kf-upper").unwrap();
        assert_eq!(upper_first.compare_strings("A", "a"), Ordering::Less);

        let upper_first_us = TextCollation::new("en-US-u-kf-upper").unwrap();
        assert_eq!(upper_first_us.compare_strings("A", "a"), Ordering::Less);
    }

    #[test]
    fn expr_collation_single_table_single_column() {
        // A plain column has its declared collation, BINARY when it declares none.
        for collation in [
            None,
            Some(TextCollation::Binary),
            Some(TextCollation::NoCase),
            Some(TextCollation::Rtrim),
        ] {
            let table_references =
                get_table_references_single_table_single_column_with_collation(collation);
            let expr = Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            };
            let collseq = expr_collation_if_any(
                &expr,
                &TableMetadata::new(Some(&table_references)),
                &Resolver::empty_for_tests().catalog,
            )
            .unwrap();
            assert_eq!(collseq, Some(collation.unwrap_or(TextCollation::Binary)));
        }
    }

    #[test]
    fn expr_collation_single_table_single_column_with_collate() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(TextCollation::Binary),
        );
        // col COLLATE RTRIM, col COLLATE NOCASE, col COLLATE BINARY
        for collation in ["RTRIM", "NOCASE", "BINARY"] {
            let expected_collation = TextCollation::new(collation).unwrap();
            let expr = Expr::Collate(
                Box::new(Expr::Column {
                    database: None,
                    table: TableInternalId::from(1),
                    column: 0,
                    is_rowid_alias: false,
                }),
                Name::exact(collation.to_string()),
            );
            let collseq = expr_collation_if_any(
                &expr,
                &TableMetadata::new(Some(&table_references)),
                &Resolver::empty_for_tests().catalog,
            )
            .unwrap();
            assert_eq!(collseq, Some(expected_collation));
        }
    }

    #[test]
    fn expr_collation_multiple_collate_leftmost_wins() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(TextCollation::Binary),
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
        let collseq = expr_collation_if_any(
            &expr,
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests().catalog,
        )
        .unwrap();
        assert_eq!(collseq, Some(TextCollation::Rtrim));
    }

    #[test]
    fn expr_collation_unary_plus_and_cast_still_column() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(TextCollation::NoCase),
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
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests().catalog,
        )
        .unwrap();
        assert_eq!(collseq_plus, Some(TextCollation::NoCase));

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
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests().catalog,
        )
        .unwrap();
        assert_eq!(collseq_cast, Some(TextCollation::NoCase));
    }

    #[test]
    fn expr_collation_explicit_collate_anywhere_in_operand() {
        let table_references = get_table_references_two_tables_single_column_with_collations(
            Some(TextCollation::NoCase),
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
        let collseq = expr_collation_if_any(
            &expr,
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests().catalog,
        )
        .unwrap();
        assert_eq!(collseq, Some(TextCollation::Rtrim));
    }

    #[test]
    fn expr_collation_column_plus_column_has_none() {
        let table_references = get_table_references_two_tables_single_column_with_collations(
            Some(TextCollation::NoCase),
            Some(TextCollation::Rtrim),
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
        let collseq = expr_collation_if_any(
            &expr,
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests().catalog,
        )
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
        let collseq = expr_collation_if_any(
            &expr,
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests().catalog,
        )
        .unwrap();
        assert_eq!(collseq, Some(TextCollation::NoCase));
    }

    #[test]
    fn expr_collation_default_binary_when_no_collate_or_column() {
        let table_references = TableReferences::new_empty();
        let expr = Expr::Literal(Literal::String("abc".to_string()));
        let collseq = expr_collation_if_any(
            &expr,
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests().catalog,
        )
        .unwrap();
        assert_eq!(collseq, None);
    }

    #[test]
    fn expr_collation_rowid_has_none_even_with_declared_collation() {
        let table_references = get_table_references_single_table_rowid_alias_with_collation(Some(
            TextCollation::NoCase,
        ));
        let expr = Expr::RowId {
            database: None,
            table: TableInternalId::from(1),
        };
        // As in SQLite, the rowid has no collation even when its INTEGER PRIMARY KEY
        // alias declares one.
        let collseq = expr_collation_if_any(
            &expr,
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests().catalog,
        )
        .unwrap();
        assert_eq!(collseq, None);
    }

    #[test]
    fn comparison_collation_nocase_column_vs_binary_default() {
        // LHS has NOCASE column, RHS has no collation → NOCASE
        let table_refs = get_table_references_two_tables_single_column_with_collations(
            Some(TextCollation::NoCase),
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
            comparison_collation(
                &lhs,
                &rhs,
                &ExprContext::new(Some(&table_refs), &Resolver::empty_for_tests())
            )
            .unwrap(),
            CollationSeq::NOCASE
        );
        // Swapped: the LHS column is still a column, so its default BINARY
        // collation wins over the NOCASE column on the RHS.
        assert_eq!(
            comparison_collation(
                &rhs,
                &lhs,
                &ExprContext::new(Some(&table_refs), &Resolver::empty_for_tests())
            )
            .unwrap(),
            CollationSeq::BINARY
        );
    }

    #[test]
    fn comparison_collation_function_hides_column_collation() {
        // A column inside a function call is not a column operand, so the
        // NOCASE column on the other side supplies the collation.
        let table_refs = get_table_references_two_tables_single_column_with_collations(
            Some(TextCollation::NoCase),
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
                &ExprContext::new(Some(&table_refs), &Resolver::empty_for_tests())
            )
            .unwrap(),
            CollationSeq::NOCASE
        );
    }

    #[test]
    fn comparison_collation_uplus_and_parens_keep_column_collation() {
        // "+column" and "(column)" still count as columns, so the BINARY
        // column on the LHS wins over the NOCASE column on the RHS.
        let table_refs = get_table_references_two_tables_single_column_with_collations(
            Some(TextCollation::NoCase),
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
                &ExprContext::new(Some(&table_refs), &Resolver::empty_for_tests())
            )
            .unwrap(),
            CollationSeq::BINARY
        );
    }

    #[test]
    fn comparison_collation_explicit_beats_column() {
        // LHS column is NOCASE, but RHS has explicit RTRIM → RTRIM wins
        let table_refs = get_table_references_two_tables_single_column_with_collations(
            Some(TextCollation::NoCase),
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
            comparison_collation(
                &lhs,
                &rhs,
                &ExprContext::new(Some(&table_refs), &Resolver::empty_for_tests())
            )
            .unwrap(),
            CollationSeq::RTRIM
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
            comparison_collation(
                &lhs,
                &rhs,
                &ExprContext::new(Some(&table_refs), &Resolver::empty_for_tests())
            )
            .unwrap(),
            CollationSeq::BINARY
        );
    }

    // Helpers //

    fn get_table_references_single_table_single_column_with_collation(
        collation: Option<TextCollation>,
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
            namespace: NamespaceRef::root_of(MAIN_DB_ID),
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
        left: Option<TextCollation>,
        right: Option<TextCollation>,
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
            namespace: NamespaceRef::root_of(MAIN_DB_ID),
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
            namespace: NamespaceRef::root_of(MAIN_DB_ID),
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
        collation: Option<TextCollation>,
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
            namespace: NamespaceRef::root_of(MAIN_DB_ID),
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

/// Every query here compares, sorts or hashes only values of a type with its
/// own order. No instruction of its program may use a text collation: if one
/// does, some code path took the collation from the text rules instead of
/// from the value's type, and would compare '10.00' before '9.50'.
#[cfg(test)]
mod typed_value_order_tests {
    use super::CollationSeq;
    use crate::sync::Arc;
    use crate::vdbe::{builder::CursorType, insn::Insn};
    use crate::{
        ConnectOpts, Connection, Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect, IO,
    };

    /// Each query, with the number of its instructions that compare values the
    /// engine made itself (row counters, sequence numbers) with BINARY.
    const QUERIES: &[(&str, usize)] = &[
        // comparisons
        ("SELECT n FROM t WHERE n > 1.5", 0),
        ("SELECT n FROM t WHERE n BETWEEN 1.5 AND 2", 0),
        ("SELECT n FROM t WHERE a = '{1}'", 0),
        ("SELECT n FROM t WHERE b < '{x}'", 0),
        ("SELECT n FROM t WHERE n = (SELECT max(m) FROM u)", 0),
        ("SELECT n > 1.5, nullif(n, 2), n IS 1.5 FROM t", 0),
        // IN
        ("SELECT n FROM t WHERE n IN (1.5, 2)", 0),
        ("SELECT n FROM t WHERE n NOT IN (1.5, 2)", 0),
        ("SELECT n FROM t WHERE n IN (SELECT m FROM u)", 0),
        ("SELECT n FROM t WHERE n IN (SELECT m FROM u UNION SELECT n FROM t)", 0),
        ("SELECT n FROM t WHERE a IN (SELECT a FROM t)", 0),
        // CASE
        ("SELECT CASE n WHEN 1.5 THEN 1 WHEN 2 THEN 2 END FROM t", 0),
        ("SELECT n FROM t ORDER BY CASE WHEN a IS NULL THEN n ELSE n END", 0),
        // ORDER BY
        ("SELECT n FROM t ORDER BY n", 0),
        ("SELECT n FROM t ORDER BY a DESC, b", 0),
        ("SELECT n FROM t ORDER BY likely(n), coalesce(n, 1)", 0),
        ("SELECT n + 1 AS x FROM t ORDER BY x", 0),
        // GROUP BY
        ("SELECT n FROM t GROUP BY n", 0),
        ("SELECT a, b FROM t GROUP BY a, b", 0),
        ("SELECT n FROM t GROUP BY n HAVING n > 1", 0),
        // DISTINCT
        ("SELECT DISTINCT n FROM t", 0),
        ("SELECT DISTINCT a, b FROM t", 0),
        ("SELECT count(DISTINCT n) FROM t", 0),
        // compound
        ("SELECT n FROM t UNION SELECT m FROM u", 0),
        ("SELECT n FROM t INTERSECT SELECT m FROM u", 0),
        ("SELECT n FROM t EXCEPT SELECT m FROM u", 0),
        // The sorter's last key is the sequence number of the row.
        ("SELECT n FROM t UNION ALL SELECT m FROM u ORDER BY 1", 1),
        ("SELECT a FROM t UNION SELECT a FROM t", 0),
        // window
        ("SELECT min(n) OVER (PARTITION BY a ORDER BY b) FROM t", 0),
        ("SELECT max(n) OVER (ORDER BY n RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t", 0),
        ("SELECT n FROM t ORDER BY lag(n) OVER (ORDER BY a)", 0),
        // Two comparisons of the frame's row numbers.
        ("SELECT n FROM t ORDER BY first_value(b) OVER (ORDER BY n)", 2),
        // min/max
        ("SELECT min(n), max(a), min(b) FROM t", 0),
        ("SELECT max(n, 2), min(n, 1.5) FROM t", 0),
        // join
        ("SELECT t.n FROM t JOIN u ON t.n = u.m", 0),
        ("SELECT t.n FROM t LEFT JOIN u ON t.n = u.m", 0),
        ("SELECT t.n FROM t, u WHERE t.n > u.m", 0),
        ("SELECT t.n FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.m = t.n)", 0),
        // A value without a type compared with typed values.
        ("SELECT n FROM t WHERE 1.5 IN (n, 2)", 0),
        ("SELECT n FROM t WHERE '{1}' IN (a)", 0),
        // Registers that stand for columns.
        (
            "INSERT INTO w VALUES (1, '{1}', 1.5) ON CONFLICT(k) DO UPDATE \
             SET a = excluded.a WHERE excluded.a > w.a AND excluded.n > w.n",
            0,
        ),
        ("UPDATE w SET a = '{2}'", 0),
        ("INSERT INTO w VALUES (2, '{1}', 1.5) RETURNING a > '{0}', n > 1.5", 0),
        // Array function results.
        ("SELECT n FROM t WHERE b = ARRAY['9'] OR array_append(a, 1) > a", 0),
        ("SELECT n FROM t WHERE string_to_array('9', ',') = b", 0),
        // A compound subquery in FROM.
        ("SELECT x FROM (SELECT n AS x FROM t UNION ALL SELECT m FROM u) WHERE x > 1.5", 0),
        ("SELECT DISTINCT x FROM (SELECT 1 AS x UNION ALL SELECT n FROM t)", 0),
        // Automatic indexes.
        ("SELECT t.n FROM t, u WHERE t.n = u.m AND u.m > 1", 0),
    ];

    #[test]
    fn no_text_collation_on_typed_values() {
        let conn = connection_with_typed_tables();
        let mut failures = Vec::new();
        for &(sql, internal_comparisons) in QUERIES {
            let stmt = conn.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let program = &stmt.get_program().prepared;
            let instructions = program
                .insns
                .iter()
                .filter(|(insn, _)| compares_values(insn))
                .flat_map(|(insn, _)| {
                    insn.collations()
                        .into_iter()
                        .map(move |collation| (format!("{insn:?}"), collation))
                });
            let index_keys = program
                .cursor_ref
                .iter()
                .filter_map(|(_, cursor_type)| match cursor_type {
                    CursorType::BTreeIndex(index) if index.ephemeral => Some(index),
                    _ => None,
                })
                .flat_map(|index| {
                    index.columns.iter().map(move |column| {
                        (
                            format!("key {} of index {}", column.name, index.name),
                            column.collation,
                        )
                    })
                });
            let with_text_collation: Vec<String> = instructions
                .chain(index_keys)
                .filter(|(_, collation)| matches!(collation, CollationSeq::Text(_)))
                .map(|(place, _)| place)
                .collect();
            if with_text_collation.len() != internal_comparisons {
                failures.push(format!("{sql}\n    {}", with_text_collation.join("\n    ")));
            }
        }
        assert!(
            failures.is_empty(),
            "comparisons of typed values that use a text collation:\n{}",
            failures.join("\n")
        );
    }

    /// Functions and aggregates that never compare values carry an unused
    /// BINARY collation.
    fn compares_values(insn: &Insn) -> bool {
        match insn {
            Insn::Function { func, .. } => func.func.needs_collation(),
            Insn::AggStep { data } => data.func.compares_values(),
            _ => true,
        }
    }

    fn connection_with_typed_tables() -> Arc<Connection> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::Create,
            DatabaseOpts::new().with_custom_types(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(crate::SessionRole::Superuser))
            .unwrap();
        conn.execute("CREATE TABLE t(n numeric(10,2), a integer[], b text[]) STRICT")
            .unwrap();
        conn.execute("CREATE TABLE u(m numeric(10,2)) STRICT")
            .unwrap();
        conn.execute("CREATE TABLE w(k int PRIMARY KEY, a integer[], n numeric(10,2)) STRICT")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER w_grew AFTER UPDATE ON w WHEN NEW.a > OLD.a AND NEW.n > OLD.n \
             BEGIN SELECT 1; END",
        )
        .unwrap();
        conn
    }
}
