//! Orders of built-in types that SQLite's comparison of the stored value cannot
//! express: numeric, which is stored as a blob, arrays, and uuid, whose text
//! spellings must compare as the uuid they name. Every other type sorts and
//! compares by its stored value.
//!
//! B-trees, sorters and hash tables use these orders, so each one is total and
//! never fails: every value has a place, including NULL and values that are
//! not of the type (an ANY column, a corrupt blob). Values that compare equal
//! also hash alike, so hash joins and DISTINCT agree with `=`.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::numeric::decimal::{
    numeric_check_operand, numeric_compare, numeric_compare_operands, numeric_hash,
};
use crate::numeric::Numeric;
use crate::types::AsValueRef;
use crate::vdbe::array::{array_values_from_blob, parse_text_array, parse_text_array_of_texts};
use crate::{Value, ValueRef};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeOrdering {
    Numeric,
    /// Arrays whose elements are not texts, such as `integer[]`. An array in
    /// text form is read with numbers for its elements.
    Array,
    /// Arrays whose elements are texts, such as `text[]`. An array in text
    /// form is read with texts for its elements, so `{1}` and `{1.0}` differ.
    TextArray,
    /// 16-byte blobs and any text `uuid_blob` accepts (uppercase, braces, no
    /// hyphens), by the uuid's bytes, so stored uuids sort as their blobs do.
    Uuid,
}

impl TypeOrdering {
    pub const ALL: [Self; 4] = [Self::Numeric, Self::Array, Self::TextArray, Self::Uuid];

    /// The name EXPLAIN shows for the order.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Numeric => "numeric",
            Self::Array => "array",
            Self::TextArray => "text_array",
            Self::Uuid => "uuid",
        }
    }

    /// Whether values stored in the type's encoded form sort in this order
    /// under SQLite's comparison, so arrays of the type keep its order.
    pub const fn sorts_stored_values(self) -> bool {
        match self {
            Self::Numeric | Self::Array | Self::TextArray => false,
            Self::Uuid => true,
        }
    }

    pub fn compare(self, left: ValueRef, right: ValueRef) -> Ordering {
        match self {
            Self::Numeric => numeric_compare(left, right),
            Self::Array => ArraySortKey::of(left, parse_text_array)
                .cmp(&ArraySortKey::of(right, parse_text_array)),
            Self::TextArray => ArraySortKey::of(left, parse_text_array_of_texts)
                .cmp(&ArraySortKey::of(right, parse_text_array_of_texts)),
            Self::Uuid => UuidSortKey::of(left).cmp(&UuidSortKey::of(right)),
        }
    }

    /// Orders the two sides of a comparison operator, such as `n = 'abc'`.
    /// Unlike [Self::compare], it fails when a numeric is compared with a
    /// value that is not a number, or a uuid with a value that is not a uuid,
    /// as in PostgreSQL. The other orders never fail.
    pub fn compare_operands(self, left: ValueRef, right: ValueRef) -> crate::Result<Ordering> {
        match self {
            Self::Numeric => numeric_compare_operands(left, right),
            Self::Uuid => {
                let (left, right) = (UuidSortKey::of(left), UuidSortKey::of(right));
                left.check_is_uuid()?;
                right.check_is_uuid()?;
                Ok(left.cmp(&right))
            }
            _ => Ok(self.compare(left, right)),
        }
    }

    /// Fails where [Self::compare_operands] would fail for `value` as either
    /// side. A hash join checks its keys with it, because it compares them by
    /// hash instead of with [Self::compare_operands].
    pub fn check_operand(self, value: ValueRef) -> crate::Result<()> {
        match self {
            Self::Numeric => numeric_check_operand(value),
            Self::Uuid => UuidSortKey::of(value).check_is_uuid(),
            Self::Array | Self::TextArray => Ok(()),
        }
    }

    pub fn hash<H: Hasher>(self, value: ValueRef, state: &mut H) {
        match self {
            Self::Numeric => numeric_hash(value, state),
            Self::Array => ArraySortKey::of(value, parse_text_array).hash(state),
            Self::TextArray => ArraySortKey::of(value, parse_text_array_of_texts).hash(state),
            Self::Uuid => UuidSortKey::of(value).hash(state),
        }
    }
}

/// NULL first; then uuids, by their bytes; then any other value in SQLite's
/// order.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum UuidSortKey<'a> {
    Null,
    Uuid(uuid::Uuid),
    Other(ValueRef<'a>),
}

impl<'a> UuidSortKey<'a> {
    fn of(value: ValueRef<'a>) -> Self {
        let uuid = match value {
            ValueRef::Null => return Self::Null,
            ValueRef::Blob(blob) => uuid::Uuid::from_slice(blob).ok(),
            ValueRef::Text(text) => uuid::Uuid::parse_str(text.as_str()).ok(),
            ValueRef::Numeric(_) => None,
        };
        match uuid {
            Some(uuid) => Self::Uuid(uuid),
            None => Self::Other(value),
        }
    }

    fn check_is_uuid(&self) -> crate::Result<()> {
        match self {
            Self::Other(_) => Err(crate::LimboError::Constraint(
                "invalid UUID value".to_string(),
            )),
            Self::Null | Self::Uuid(_) => Ok(()),
        }
    }
}

impl Hash for UuidSortKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => 0u8.hash(state),
            Self::Uuid(uuid) => {
                1u8.hash(state);
                uuid.hash(state);
            }
            Self::Other(value) => {
                2u8.hash(state);
                hash_sqlite_value(*value, state);
            }
        }
    }
}

/// NULL first; then arrays, element by element and then shorter first; then
/// any other value in SQLite's order.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ArraySortKey<'a> {
    Null,
    Elements(ArrayElements),
    Other(ValueRef<'a>),
}

impl<'a> ArraySortKey<'a> {
    /// `parse_text` reads an array in text form.
    fn of(value: ValueRef<'a>, parse_text: fn(&str) -> Option<Vec<Value>>) -> Self {
        let elements = match value {
            ValueRef::Null => return Self::Null,
            ValueRef::Blob(blob) => array_values_from_blob(blob).ok(),
            ValueRef::Text(text) => parse_text(text.as_str()),
            ValueRef::Numeric(_) => None,
        };
        match elements {
            Some(elements) => Self::Elements(ArrayElements(elements)),
            None => Self::Other(value),
        }
    }
}

impl Hash for ArraySortKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => 0u8.hash(state),
            Self::Elements(elements) => {
                1u8.hash(state);
                elements.0.len().hash(state);
                for element in &elements.0 {
                    hash_sqlite_value(element.as_value_ref(), state);
                }
            }
            Self::Other(value) => {
                2u8.hash(state);
                hash_sqlite_value(*value, state);
            }
        }
    }
}

/// Array elements compared element by element, as PostgreSQL's `array_cmp`
/// does: a NULL element is greater than any other element and equal to
/// another NULL. Other elements compare in SQLite's order.
struct ArrayElements(Vec<Value>);

impl PartialEq for ArrayElements {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ArrayElements {}

impl PartialOrd for ArrayElements {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ArrayElements {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .iter()
            .map(null_last_key)
            .cmp(other.0.iter().map(null_last_key))
    }
}

/// Sort key of an array element that orders NULL after every other value.
fn null_last_key(value: &Value) -> (bool, ValueRef<'_>) {
    (matches!(value, Value::Null), value.as_value_ref())
}

/// Hashes a value so that values SQLite's comparison finds equal hash alike.
/// An integer and a real can be equal (1 and 1.0), so every number hashes as
/// the real nearest to it; numbers that are equal have the same nearest real.
fn hash_sqlite_value<H: Hasher>(value: ValueRef, state: &mut H) {
    match value {
        ValueRef::Null => 0u8.hash(state),
        ValueRef::Numeric(number) => {
            1u8.hash(state);
            let real = match number {
                Numeric::Integer(i) => i as f64,
                Numeric::Float(f) => f64::from(f),
            };
            // -0.0 equals 0.0 but has other bits.
            let real = if real == 0.0 { 0.0 } else { real };
            real.to_bits().hash(state);
        }
        ValueRef::Text(text) => {
            2u8.hash(state);
            text.as_str().hash(state);
        }
        ValueRef::Blob(blob) => {
            3u8.hash(state);
            blob.hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::TursoSliceExt;

    /// Checks that `ordering` sorts `groups` in the order given, that values in
    /// one group are equal, and that equal values hash alike.
    fn assert_order(ordering: TypeOrdering, groups: &[&[Value]]) {
        for (i, left_group) in groups.iter().enumerate() {
            for (j, right_group) in groups.iter().enumerate() {
                for l in left_group.iter() {
                    for r in right_group.iter() {
                        assert_eq!(
                            ordering.compare(l.as_value_ref(), r.as_value_ref()),
                            i.cmp(&j),
                            "{ordering:?}: {l:?} vs {r:?}"
                        );
                        if i == j {
                            assert_eq!(
                                hash_of(ordering, l),
                                hash_of(ordering, r),
                                "{l:?} vs {r:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    fn hash_of(ordering: TypeOrdering, value: &Value) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        ordering.hash(value.as_value_ref(), &mut hasher);
        hasher.finish()
    }

    fn int(i: i64) -> Value {
        Value::Numeric(Numeric::Integer(i))
    }

    #[test]
    fn array_order_compares_elements_then_length() {
        assert_order(
            TypeOrdering::Array,
            &[
                &[Value::Null],
                &[Value::from_text("{}")],
                &[Value::from_text("{1}"), Value::from_text("{1.0}")],
                &[Value::from_text("{1, 2}")],
                &[Value::from_text("{1, NULL}")],
                &[Value::from_text("{1, NULL, 1}")],
                &[Value::from_text("{2}")],
                &[Value::from_text("{a}")],
                &[Value::from_text("{NULL}")],
                &[int(5)],
                &[Value::from_text("not an array")],
                &[Value::from_blob(crate::alloc::vec![0xff, 0xff])],
            ],
        );
    }

    #[test]
    fn numeric_order_is_the_numeric_types_order() {
        assert_order(
            TypeOrdering::Numeric,
            &[
                &[Value::Null],
                &[int(-1)],
                &[
                    Value::from_text("1.5"),
                    Value::from_text("1.50"),
                    Value::from_f64(1.5),
                ],
                &[Value::from_text("abc")],
            ],
        );
    }

    #[test]
    fn uuid_order_reads_every_uuid_spelling() {
        let uuid = |hex: &str| {
            Value::from_blob(
                uuid::Uuid::parse_str(hex)
                    .unwrap()
                    .as_bytes()
                    .try_to_vec()
                    .unwrap(),
            )
        };
        assert_order(
            TypeOrdering::Uuid,
            &[
                &[Value::Null],
                &[uuid("00000000-0000-0000-0000-000000000000")],
                &[
                    uuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
                    Value::from_text("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"),
                    Value::from_text("{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"),
                    Value::from_text("a0eebc999c0b4ef8bb6d6bb9bd380a11"),
                ],
                &[int(5)],
                &[Value::from_text("not a uuid")],
                &[Value::from_blob(crate::alloc::vec![0xff, 0xff])],
            ],
        );
        assert!(TypeOrdering::Uuid
            .compare_operands(int(5).as_value_ref(), Value::Null.as_value_ref())
            .is_err());
    }
}
