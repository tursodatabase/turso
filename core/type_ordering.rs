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
use crate::vdbe::array::{array_values_from_blob, parse_text_array, parse_text_array_of_texts};
use crate::{Value, ValueRef};

/// One order of a type. Two handles are equal when they are the same order.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TypeOrdering(u16);

struct OrderingFunctions {
    name: &'static str,
    compare: fn(ValueRef, ValueRef) -> Ordering,
    compare_operands: fn(ValueRef, ValueRef) -> crate::Result<Ordering>,
    hash: fn(ValueRef, &mut dyn Hasher),
}

const BUILT_IN: [OrderingFunctions; 5] = [
    OrderingFunctions {
        name: "numeric",
        compare: numeric_compare,
        compare_operands: numeric_compare_operands,
        hash: numeric_hash_dyn,
    },
    OrderingFunctions {
        name: "array",
        compare: array_compare::<false>,
        compare_operands: |left, right| Ok(array_compare::<false>(left, right)),
        hash: array_hash::<false>,
    },
    OrderingFunctions {
        name: "text_array",
        compare: text_array_compare::<false>,
        compare_operands: |left, right| Ok(text_array_compare::<false>(left, right)),
        hash: text_array_hash::<false>,
    },
    OrderingFunctions {
        name: "array",
        compare: array_compare::<true>,
        compare_operands: |left, right| Ok(array_compare::<true>(left, right)),
        hash: array_hash::<true>,
    },
    OrderingFunctions {
        name: "text_array",
        compare: text_array_compare::<true>,
        compare_operands: |left, right| Ok(text_array_compare::<true>(left, right)),
        hash: text_array_hash::<true>,
    },
];

impl TypeOrdering {
    pub const NUMERIC: Self = Self(0);
    pub const ALL: [Self; 5] = [Self(0), Self(1), Self(2), Self(3), Self(4)];

    /// An array order with the database's rule for encoded NaN elements.
    pub const fn array(policy: crate::types::NanPolicy) -> Self {
        match policy {
            crate::types::NanPolicy::Null => Self(1),
            crate::types::NanPolicy::Preserve => Self(3),
        }
    }
    /// A text-array order with the database's rule for encoded NaN elements.
    pub const fn text_array(policy: crate::types::NanPolicy) -> Self {
        match policy {
            crate::types::NanPolicy::Null => Self(2),
            crate::types::NanPolicy::Preserve => Self(4),
        }
    }
    pub fn is_array(self) -> bool {
        matches!(self.0, 1..=4)
    }
    pub const fn name(self) -> &'static str {
        self.functions().name
    }
    pub fn compare(self, left: ValueRef, right: ValueRef) -> Ordering {
        (self.functions().compare)(left, right)
    }
    pub fn compare_operands(self, left: ValueRef, right: ValueRef) -> crate::Result<Ordering> {
        (self.functions().compare_operands)(left, right)
    }
    pub fn hash<H: Hasher>(self, value: ValueRef, state: &mut H) {
        (self.functions().hash)(value, state)
    }
    const fn functions(self) -> &'static OrderingFunctions {
        &BUILT_IN[self.0 as usize]
    }
}
impl std::fmt::Debug for TypeOrdering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

fn numeric_hash_dyn(value: ValueRef, mut state: &mut dyn Hasher) {
    numeric_hash(value, &mut state)
}

fn array_compare<const PRESERVE: bool>(left: ValueRef, right: ValueRef) -> Ordering {
    let policy = if PRESERVE {
        crate::types::NanPolicy::Preserve
    } else {
        crate::types::NanPolicy::Null
    };
    ArraySortKey::of(policy, left, parse_text_array).cmp(&ArraySortKey::of(
        policy,
        right,
        parse_text_array,
    ))
}
fn array_hash<const PRESERVE: bool>(value: ValueRef, mut state: &mut dyn Hasher) {
    let policy = if PRESERVE {
        crate::types::NanPolicy::Preserve
    } else {
        crate::types::NanPolicy::Null
    };
    ArraySortKey::of(policy, value, parse_text_array).hash(&mut state)
}
fn text_array_compare<const PRESERVE: bool>(left: ValueRef, right: ValueRef) -> Ordering {
    let policy = if PRESERVE {
        crate::types::NanPolicy::Preserve
    } else {
        crate::types::NanPolicy::Null
    };
    ArraySortKey::of(policy, left, parse_text_array_of_texts).cmp(&ArraySortKey::of(
        policy,
        right,
        parse_text_array_of_texts,
    ))
}
fn text_array_hash<const PRESERVE: bool>(value: ValueRef, mut state: &mut dyn Hasher) {
    let policy = if PRESERVE {
        crate::types::NanPolicy::Preserve
    } else {
        crate::types::NanPolicy::Null
    };
    ArraySortKey::of(policy, value, parse_text_array_of_texts).hash(&mut state)
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
    fn of(
        nan_policy: crate::types::NanPolicy,
        value: ValueRef<'a>,
        parse_text: fn(&str) -> Option<Vec<Value>>,
    ) -> Self {
        let elements = match value {
            ValueRef::Null => return Self::Null,
            ValueRef::Blob(blob) => array_values_from_blob(nan_policy, blob).ok(),
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
            crate::Double::new(real).canonical_bits().hash(state);
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
    fn nan_representations_compare_and_hash_alike_in_arrays() {
        let array = |value| {
            Value::Blob(
                crate::types::ImmutableRecord::from_values(&[value], 1)
                    .unwrap()
                    .into_payload(),
            )
        };
        let nans: Vec<_> = [
            0x7ff8_0000_0000_0000u64,
            0xfff8_0000_0000_0001,
            0x7ff0_0000_0000_0001,
            0x7fff_ffff_ffff_ffff,
        ]
        .into_iter()
        .map(|bits| array(Value::from_f64(f64::from_bits(bits))))
        .collect();
        for ordering in [
            TypeOrdering::array(crate::types::NanPolicy::Preserve),
            TypeOrdering::text_array(crate::types::NanPolicy::Preserve),
        ] {
            assert_order(
                ordering,
                &[
                    &[array(Value::from_f64(f64::INFINITY))],
                    &nans,
                    &[array(Value::Null)],
                ],
            );
        }
        for ordering in [
            TypeOrdering::array(crate::types::NanPolicy::Null),
            TypeOrdering::text_array(crate::types::NanPolicy::Null),
        ] {
            let null = array(Value::Null);
            for nan in &nans {
                assert_eq!(
                    ordering.compare(nan.as_value_ref(), null.as_value_ref()),
                    Ordering::Equal
                );
                assert_eq!(hash_of(ordering, nan), hash_of(ordering, &null));
            }
        }
    }

    #[test]
    fn array_order_compares_elements_then_length() {
        assert_order(
            TypeOrdering::array(crate::types::NanPolicy::Preserve),
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
            TypeOrdering::NUMERIC,
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
