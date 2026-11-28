//! Orders of built-in types that SQLite's comparison of the stored value cannot
//! express: numeric, which is stored as a blob, and arrays. Every other type
//! sorts and compares by its stored value.
//!
//! B-trees, sorters and hash tables use these orders, so each one is total and
//! never fails: every value has a place, including NULL and values that are
//! not of the type (an ANY column, a corrupt blob). Values that compare equal
//! also hash alike, so hash joins and DISTINCT agree with `=`.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::numeric::decimal::{numeric_compare, numeric_compare_operands, numeric_hash};
use crate::numeric::Numeric;
use crate::types::AsValueRef;
use crate::vdbe::array::{parse_text_array_of_texts_value, parse_text_array_value};
use crate::vdbe::array_value::ArrayValue;
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
}

impl TypeOrdering {
    pub const ALL: [Self; 3] = [Self::Numeric, Self::Array, Self::TextArray];

    /// The name EXPLAIN shows for the order.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Numeric => "numeric",
            Self::Array => "array",
            Self::TextArray => "text_array",
        }
    }

    pub fn compare(self, left: ValueRef, right: ValueRef) -> Ordering {
        match self {
            Self::Numeric => numeric_compare(left, right),
            Self::Array => ArraySortKey::of(left, parse_text_array_value)
                .cmp(&ArraySortKey::of(right, parse_text_array_value)),
            Self::TextArray => ArraySortKey::of(left, parse_text_array_of_texts_value)
                .cmp(&ArraySortKey::of(right, parse_text_array_of_texts_value)),
        }
    }

    /// Orders the two sides of a comparison operator, such as `n = 'abc'`.
    /// Unlike [Self::compare], it fails when a numeric is compared with a
    /// value that is not a number, as in PostgreSQL. The other orders never
    /// fail.
    pub fn compare_operands(self, left: ValueRef, right: ValueRef) -> crate::Result<Ordering> {
        match self {
            Self::Numeric => numeric_compare_operands(left, right),
            _ => Ok(self.compare(left, right)),
        }
    }

    pub fn hash<H: Hasher>(self, value: ValueRef, state: &mut H) {
        match self {
            Self::Numeric => numeric_hash(value, state),
            Self::Array => ArraySortKey::of(value, parse_text_array_value).hash(state),
            Self::TextArray => ArraySortKey::of(value, parse_text_array_of_texts_value).hash(state),
        }
    }
}

/// NULL first; then arrays, element by element, then shorter first, then by
/// lower bound; then any other value in SQLite's order.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ArraySortKey<'a> {
    Null,
    Elements(ArrayElements),
    Other(ValueRef<'a>),
}

impl<'a> ArraySortKey<'a> {
    /// `parse_text` reads an array in text form.
    fn of(value: ValueRef<'a>, parse_text: fn(&str) -> Option<ArrayValue>) -> Self {
        let array = match value {
            ValueRef::Null => return Self::Null,
            ValueRef::Blob(blob) => ArrayValue::decode(blob).ok(),
            ValueRef::Text(text) => parse_text(text.as_str()),
            ValueRef::Numeric(_) => None,
        };
        match array {
            Some(array) => Self::Elements(ArrayElements(array)),
            None => Self::Other(value),
        }
    }
}

impl Hash for ArraySortKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => 0u8.hash(state),
            Self::Elements(array) => {
                1u8.hash(state);
                array.0.elements.len().hash(state);
                array.0.lower_bound.hash(state);
                for element in &array.0.elements {
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

/// Arrays compared as PostgreSQL's `array_cmp` does: element by element, then
/// by length, then by lower bound. A NULL element is greater than any other
/// element and equal to another NULL. Other elements compare in SQLite's
/// order.
struct ArrayElements(ArrayValue);

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
            .elements
            .iter()
            .map(null_last_key)
            .cmp(other.0.elements.iter().map(null_last_key))
            .then(self.0.lower_bound.cmp(&other.0.lower_bound))
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

    fn array_with_bound(lower_bound: i32, elements: &[i64]) -> Value {
        ArrayValue {
            lower_bound,
            elements: elements.iter().map(|element| int(*element)).collect(),
        }
        .encode()
        .unwrap()
    }

    #[test]
    fn array_order_compares_the_lower_bound_after_the_elements() {
        assert_order(
            TypeOrdering::Array,
            &[
                &[array_with_bound(0, &[1, 2])],
                &[array_with_bound(1, &[1, 2]), Value::from_text("{1, 2}")],
                &[array_with_bound(5, &[1, 2])],
                &[array_with_bound(0, &[1, 2, 3])],
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
}
