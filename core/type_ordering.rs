//! Orders of custom types that SQLite's comparison of the stored value cannot
//! express. A type gets one from the function its `OPERATOR '<'` names.
//!
//! B-trees and sorters use these orders, so each one is total and never fails:
//! every value has a place, including NULL and values that are not of the type
//! (an ANY column, a corrupt blob).

use std::cmp::Ordering;

use crate::numeric::decimal::numeric_compare;
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
#[derive(PartialEq, Eq, PartialOrd, Ord)]
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

/// A value compared the way SQLite compares values with BINARY.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct SqliteOrdered<'a>(ValueRef<'a>);

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that `ordering` sorts `groups` in the order given and that values
    /// in one group are equal.
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
                    }
                }
            }
        }
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
                &[Value::from_blob(crate::alloc::vec![0xff, 0xff])],
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
                &[Value::from_blob(crate::alloc::vec![0])],
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
