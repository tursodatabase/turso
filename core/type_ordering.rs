//! Orders of built-in types that SQLite's comparison of the stored value cannot
//! express: numeric, which is stored as a blob, interval, whose equal values
//! can have different fields, and arrays. Every other type sorts and compares
//! by its stored value.
//!
//! B-trees, sorters and hash tables use these orders, so each one is total and
//! never fails: every value has a place, including NULL and values that are
//! not of the type (an ANY column, a corrupt blob). Values that compare equal
//! also hash alike, so hash joins and DISTINCT agree with `=`.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::functions::interval::Interval;
use crate::numeric::decimal::{numeric_compare, numeric_compare_operands, numeric_hash};
use crate::numeric::Numeric;
use crate::types::AsValueRef;
use crate::vdbe::array::{array_values_from_blob, parse_text_array, parse_text_array_of_texts};
use crate::{Value, ValueRef};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeOrdering {
    Numeric,
    /// Intervals by length, with a month as 30 days, as PostgreSQL's
    /// `interval_cmp`: `'1 day'` equals `'24 hours'`.
    Interval,
    /// Arrays whose elements are not texts, such as `integer[]`. An array in
    /// text form is read with numbers for its elements.
    Array,
    /// Arrays whose elements are texts, such as `text[]`. An array in text
    /// form is read with texts for its elements, so `{1}` and `{1.0}` differ.
    TextArray,
}

impl TypeOrdering {
    pub const ALL: [Self; 4] = [Self::Numeric, Self::Interval, Self::Array, Self::TextArray];

    /// The name EXPLAIN shows for the order.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Numeric => "numeric",
            Self::Interval => "interval",
            Self::Array => "array",
            Self::TextArray => "text_array",
        }
    }

    pub fn compare(self, left: ValueRef, right: ValueRef) -> Ordering {
        match self {
            Self::Numeric => numeric_compare(left, right),
            Self::Interval => IntervalSortKey::of(left).cmp(&IntervalSortKey::of(right)),
            Self::Array => ArraySortKey::of(left, parse_text_array)
                .cmp(&ArraySortKey::of(right, parse_text_array)),
            Self::TextArray => ArraySortKey::of(left, parse_text_array_of_texts)
                .cmp(&ArraySortKey::of(right, parse_text_array_of_texts)),
        }
    }

    /// Orders the two sides of a comparison operator, such as `n = 'abc'`.
    /// Unlike [Self::compare], it fails when a numeric or an interval is
    /// compared with a value that is not one, as in PostgreSQL. The array
    /// orders never fail.
    pub fn compare_operands(self, left: ValueRef, right: ValueRef) -> crate::Result<Ordering> {
        match self {
            Self::Numeric => numeric_compare_operands(left, right),
            Self::Interval => {
                let (left, right) = (IntervalSortKey::of(left), IntervalSortKey::of(right));
                left.check_is_interval()?;
                right.check_is_interval()?;
                Ok(left.cmp(&right))
            }
            Self::Array | Self::TextArray => Ok(self.compare(left, right)),
        }
    }

    pub fn hash<H: Hasher>(self, value: ValueRef, state: &mut H) {
        match self {
            Self::Numeric => numeric_hash(value, state),
            Self::Interval => IntervalSortKey::of(value).hash(state),
            Self::Array => ArraySortKey::of(value, parse_text_array).hash(state),
            Self::TextArray => ArraySortKey::of(value, parse_text_array_of_texts).hash(state),
        }
    }
}

/// NULL first; then intervals by [Interval::cmp_key]; then any other value in
/// SQLite's order. An interval is a stored one (16 bytes) or its text, which a
/// register holds after DECODE.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum IntervalSortKey<'a> {
    Null,
    Interval(i128),
    Other(ValueRef<'a>),
}

impl<'a> IntervalSortKey<'a> {
    fn of(value: ValueRef<'a>) -> Self {
        let interval = match value {
            ValueRef::Null => return Self::Null,
            ValueRef::Blob(blob) => Interval::from_blob(blob),
            // A text that is not an interval sorts after every interval;
            // `check_is_interval` reports its error for an operator.
            ValueRef::Text(text) => Interval::parse(text.as_str()).ok(),
            ValueRef::Numeric(_) => None,
        };
        match interval {
            Some(interval) => Self::Interval(interval.cmp_key()),
            None => Self::Other(value),
        }
    }

    /// The error reading the value as an interval gives, if it is not one.
    fn check_is_interval(&self) -> crate::Result<()> {
        match self {
            Self::Null | Self::Interval(_) => Ok(()),
            Self::Other(ValueRef::Text(text)) => {
                Interval::parse(text.as_str())?;
                unreachable!("a text that reads as an interval sorts as an interval")
            }
            Self::Other(value) => Err(crate::LimboError::InvalidDate(format!(
                "invalid input syntax for type interval: \"{value}\""
            ))),
        }
    }
}

impl Hash for IntervalSortKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => 0u8.hash(state),
            Self::Interval(key) => {
                1u8.hash(state);
                key.hash(state);
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

    fn stored_interval(text: &str) -> Value {
        Value::from_blob(Interval::parse(text).unwrap().to_blob().to_vec())
    }

    #[test]
    fn interval_order_is_by_length_with_30_day_months() {
        assert_order(
            TypeOrdering::Interval,
            &[
                &[Value::Null],
                &[stored_interval("-1 day"), Value::from_text("-24:00:00")],
                &[stored_interval("0"), Value::from_text("00:00:00")],
                &[
                    stored_interval("1 day"),
                    stored_interval("24 hours"),
                    Value::from_text("1 day"),
                ],
                &[
                    stored_interval("1 mon"),
                    stored_interval("30 days"),
                    Value::from_text("720:00:00"),
                ],
                &[stored_interval("31 days")],
                &[int(5)],
                &[Value::from_text("not an interval")],
                &[Value::from_blob(crate::alloc::vec![0xff, 0xff])],
            ],
        );
    }

    #[test]
    fn interval_operands_must_be_intervals() {
        let ordering = TypeOrdering::Interval;
        let day = stored_interval("1 day");
        let equal = ordering.compare_operands(
            day.as_value_ref(),
            Value::from_text("24 hours").as_value_ref(),
        );
        assert_eq!(equal.unwrap(), Ordering::Equal);
        let result =
            ordering.compare_operands(day.as_value_ref(), Value::from_text("abc").as_value_ref());
        assert!(
            matches!(result, Err(crate::LimboError::NotSupported(_))),
            "{result:?}"
        );
        for other in [int(5), Value::from_blob(crate::alloc::vec![0xff])] {
            let result = ordering.compare_operands(day.as_value_ref(), other.as_value_ref());
            assert!(
                matches!(result, Err(crate::LimboError::InvalidDate(_))),
                "{other:?}: {result:?}"
            );
        }
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
