//! Orders of custom types that SQLite's comparison of the stored value cannot
//! express. A type gets one from the function its `OPERATOR '<'` names.
//!
//! B-trees, sorters and hash tables use these orders, so each one is total and
//! never fails: every value has a place, including NULL and values that are
//! not of the type (an ANY column, a corrupt blob). Values that compare equal
//! also hash alike, so hash joins and DISTINCT agree with `=`.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::numeric::decimal::{numeric_compare, numeric_hash};
use crate::numeric::Numeric;
use crate::types::AsValueRef;
use crate::vdbe::array::{array_values_from_blob, parse_text_array};
use crate::{Value, ValueRef};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TypeOrdering {
    Numeric,
    Array,
    /// Test types: unsigned integers stored as text, and text that sorts by its
    /// reversed characters.
    TestUint,
    StringReverse,
}

impl TypeOrdering {
    /// The order named by `OPERATOR '<' name` in CREATE TYPE, or None when no
    /// Rust order implements that function.
    pub fn from_less_than_function(name: &str) -> Option<Self> {
        match name {
            "numeric_lt" => Some(Self::Numeric),
            "array_lt" => Some(Self::Array),
            "test_uint_lt" => Some(Self::TestUint),
            "string_reverse" => Some(Self::StringReverse),
            _ => None,
        }
    }

    pub fn compare(self, left: ValueRef, right: ValueRef) -> Ordering {
        match self {
            Self::Numeric => numeric_compare(left, right),
            Self::Array => ArraySortKey::of(left).cmp(&ArraySortKey::of(right)),
            Self::TestUint => UintSortKey::of(left).cmp(&UintSortKey::of(right)),
            Self::StringReverse => ReversedSortKey::of(left).compare(&ReversedSortKey::of(right)),
        }
    }

    pub fn hash<H: Hasher>(self, value: ValueRef, state: &mut H) {
        match self {
            Self::Numeric => numeric_hash(value, state),
            Self::Array => ArraySortKey::of(value).hash(state),
            Self::TestUint => UintSortKey::of(value).hash(state),
            // Two texts reverse to the same text only if they are the same.
            Self::StringReverse => hash_sqlite_value(value, state),
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
    fn of(value: ValueRef<'a>) -> Self {
        let elements = match value {
            ValueRef::Null => return Self::Null,
            ValueRef::Blob(blob) => array_values_from_blob(blob).ok(),
            ValueRef::Text(text) => parse_text_array(text.as_str()),
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

/// Array elements compared in SQLite's order, element by element.
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
            .map(AsValueRef::as_value_ref)
            .cmp(other.0.iter().map(AsValueRef::as_value_ref))
    }
}

/// Unsigned integers (non-negative integers, or text holding one) by value
/// first; then any other value in SQLite's order.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
enum UintSortKey<'a> {
    Null,
    Uint(u64),
    Other(SqliteOrdered<'a>),
}

impl<'a> UintSortKey<'a> {
    fn of(value: ValueRef<'a>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Numeric(Numeric::Integer(i)) if i >= 0 => Self::Uint(i as u64),
            ValueRef::Text(text) => match text.as_str().parse::<u64>() {
                Ok(n) => Self::Uint(n),
                Err(_) => Self::Other(SqliteOrdered(value)),
            },
            _ => Self::Other(SqliteOrdered(value)),
        }
    }
}

/// Text by its characters read backwards first; then any other value in
/// SQLite's order. NULL comes before everything.
enum ReversedSortKey<'a> {
    Null,
    Text(&'a str),
    Other(ValueRef<'a>),
}

impl<'a> ReversedSortKey<'a> {
    fn of(value: ValueRef<'a>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Text(text) => Self::Text(text.as_str()),
            _ => Self::Other(value),
        }
    }

    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Text(l), Self::Text(r)) => l.chars().rev().cmp(r.chars().rev()),
            (Self::Other(l), Self::Other(r)) => l.cmp(r),
            (l, r) => l.rank().cmp(&r.rank()),
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Text(_) => 1,
            Self::Other(_) => 2,
        }
    }
}

/// A value compared and hashed the way SQLite compares values with BINARY.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct SqliteOrdered<'a>(ValueRef<'a>);

impl Hash for SqliteOrdered<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_sqlite_value(self.0, state);
    }
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
                &[Value::from_text("{2}")],
                &[Value::from_text("{a}")],
                &[int(5)],
                &[Value::from_text("not an array")],
                &[Value::Blob(vec![0xff, 0xff])],
            ],
        );
    }

    #[test]
    fn uint_order_puts_numbers_before_other_values() {
        assert_order(
            TypeOrdering::TestUint,
            &[
                &[Value::Null],
                &[int(2), Value::from_text("2")],
                &[Value::from_text("10")],
                &[int(-1)],
                &[Value::from_text("x")],
            ],
        );
    }

    #[test]
    fn reversed_order_reads_text_backwards() {
        assert_order(
            TypeOrdering::StringReverse,
            &[
                &[Value::Null],
                &[Value::from_text("banana")],
                &[Value::from_text("apple")],
                &[Value::from_text("cherry")],
                &[int(1), Value::from_f64(1.0)],
                &[Value::Blob(vec![0])],
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
