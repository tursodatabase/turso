use std::collections::HashSet;

use limbo_core::Value;
use rand::Rng;

use crate::generation::{gen_random_text, pick, readable_name_custom, Arbitrary, ArbitraryFrom};
use crate::model::table::{Column, ColumnType, Name, SimValue, Table};

use super::ArbitraryFromMaybe;

impl Arbitrary for Name {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = readable_name_custom("_", rng);
        Name(name.replace("-", "_"))
    }
}

impl Arbitrary for Table {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = Name::arbitrary(rng).0;
        let columns = loop {
            let columns = (1..=rng.gen_range(1..10))
                .map(|_| Column::arbitrary(rng))
                .collect::<Vec<_>>();
            // TODO: see if there is a better way to detect duplicates here
            let mut set = HashSet::with_capacity(columns.len());
            set.extend(columns.iter());
            // Has repeated column name inside so generate again
            if set.len() != columns.len() {
                continue;
            }
            break columns;
        };

        Table {
            rows: Vec::new(),
            name,
            columns,
        }
    }
}

impl Arbitrary for Column {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = Name::arbitrary(rng).0;
        let column_type = ColumnType::arbitrary(rng);
        Self {
            name,
            column_type,
            primary: false,
            unique: false,
        }
    }
}

impl Arbitrary for ColumnType {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        pick(&[Self::Integer, Self::Float, Self::Text, Self::Blob], rng).to_owned()
    }
}

impl ArbitraryFrom<&Table> for Vec<SimValue> {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        let mut row = Vec::new();
        for column in table.columns.iter() {
            let value = SimValue::arbitrary_from(rng, &column.column_type);
            row.push(value);
        }
        row
    }
}

impl ArbitraryFrom<&Vec<&SimValue>> for SimValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, values: &Vec<&Self>) -> Self {
        if values.is_empty() {
            return Self(Value::Null);
        }

        pick(values, rng).to_owned().clone()
    }
}

impl ArbitraryFrom<&ColumnType> for SimValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, column_type: &ColumnType) -> Self {
        let value = match column_type {
            ColumnType::Integer => Value::Integer(rng.gen_range(i64::MIN..i64::MAX)),
            ColumnType::Float => Value::Float(rng.gen_range(-1e10..1e10)),
            ColumnType::Text => Value::build_text(gen_random_text(rng)),
            ColumnType::Blob => Value::Blob(gen_random_text(rng).as_bytes().to_vec()),
        };
        SimValue(value)
    }
}

pub(crate) struct LTValue(pub(crate) SimValue);

impl ArbitraryFrom<&Vec<&SimValue>> for LTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, values: &Vec<&SimValue>) -> Self {
        if values.is_empty() {
            return Self(SimValue(Value::Null));
        }

        // Get value less than all values
        let value = Value::exec_min(values.iter().map(|value| &value.0));
        Self::arbitrary_from(rng, &SimValue(value))
    }
}

impl ArbitraryFrom<&SimValue> for LTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, value: &SimValue) -> Self {
        let new_value = match &value.0 {
            Value::Integer(i) => Value::Integer(rng.gen_range(i64::MIN..*i - 1)),
            Value::Float(f) => Value::Float(f - rng.gen_range(0.0..1e10)),
            value @ Value::Text(..) => {
                // Either shorten the string, or make at least one character smaller and mutate the rest
                let mut t = value.to_string();
                if rng.gen_bool(0.01) {
                    t.pop();
                    Value::build_text(t)
                } else {
                    let mut t = t.chars().map(|c| c as u32).collect::<Vec<_>>();
                    let index = rng.gen_range(0..t.len());
                    t[index] -= 1;
                    // Mutate the rest of the string
                    for i in (index + 1)..t.len() {
                        t[i] = rng.gen_range('a' as u32..='z' as u32);
                    }
                    let t = t
                        .into_iter()
                        .map(|c| char::from_u32(c).unwrap_or('z'))
                        .collect::<String>();
                    Value::build_text(t)
                }
            }
            Value::Blob(b) => {
                // Either shorten the blob, or make at least one byte smaller and mutate the rest
                let mut b = b.clone();
                if rng.gen_bool(0.01) {
                    b.pop();
                    Value::Blob(b)
                } else {
                    let index = rng.gen_range(0..b.len());
                    b[index] -= 1;
                    // Mutate the rest of the blob
                    for i in (index + 1)..b.len() {
                        b[i] = rng.gen_range(0..=255);
                    }
                    Value::Blob(b)
                }
            }
            _ => unreachable!(),
        };
        Self(SimValue(new_value))
    }
}

pub(crate) struct GTValue(pub(crate) SimValue);

impl ArbitraryFrom<&Vec<&SimValue>> for GTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, values: &Vec<&SimValue>) -> Self {
        if values.is_empty() {
            return Self(SimValue(Value::Null));
        }
        // Get value greater than all values
        let value = Value::exec_max(values.iter().map(|value| &value.0));

        Self::arbitrary_from(rng, &SimValue(value))
    }
}

impl ArbitraryFrom<&SimValue> for GTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, value: &SimValue) -> Self {
        let new_value = match &value.0 {
            Value::Integer(i) => Value::Integer(rng.gen_range(*i..i64::MAX)),
            Value::Float(f) => Value::Float(rng.gen_range(*f..1e10)),
            value @ Value::Text(..) => {
                // Either lengthen the string, or make at least one character smaller and mutate the rest
                let mut t = value.to_string();
                if rng.gen_bool(0.01) {
                    t.push(rng.gen_range(0..=255) as u8 as char);
                    Value::build_text(t)
                } else {
                    let mut t = t.chars().map(|c| c as u32).collect::<Vec<_>>();
                    let index = rng.gen_range(0..t.len());
                    t[index] += 1;
                    // Mutate the rest of the string
                    for i in (index + 1)..t.len() {
                        t[i] = rng.gen_range('a' as u32..='z' as u32);
                    }
                    let t = t
                        .into_iter()
                        .map(|c| char::from_u32(c).unwrap_or('a'))
                        .collect::<String>();
                    Value::build_text(t)
                }
            }
            Value::Blob(b) => {
                // Either lengthen the blob, or make at least one byte smaller and mutate the rest
                let mut b = b.clone();
                if rng.gen_bool(0.01) {
                    b.push(rng.gen_range(0..=255));
                    Value::Blob(b)
                } else {
                    let index = rng.gen_range(0..b.len());
                    b[index] += 1;
                    // Mutate the rest of the blob
                    for i in (index + 1)..b.len() {
                        b[i] = rng.gen_range(0..=255);
                    }
                    Value::Blob(b)
                }
            }
            _ => unreachable!(),
        };
        Self(SimValue(new_value))
    }
}

pub(crate) struct LikeValue(pub(crate) SimValue);

impl ArbitraryFromMaybe<&SimValue> for LikeValue {
    fn arbitrary_from_maybe<R: Rng>(rng: &mut R, value: &SimValue) -> Option<Self> {
        match &value.0 {
            value @ Value::Text(..) => {
                let t = value.to_string();
                let mut t = t.chars().collect::<Vec<_>>();
                // Remove a number of characters, either insert `_` for each character removed, or
                // insert one `%` for the whole substring
                let mut i = 0;
                while i < t.len() {
                    if rng.gen_bool(0.1) {
                        t[i] = '_';
                    } else if rng.gen_bool(0.05) {
                        t[i] = '%';
                        // skip a list of characters
                        for _ in 0..rng.gen_range(0..=3.min(t.len() - i - 1)) {
                            t.remove(i + 1);
                        }
                    }
                    i += 1;
                }
                let index = rng.gen_range(0..t.len());
                t.insert(index, '%');
                Some(Self(SimValue(Value::build_text(
                    t.into_iter().collect::<String>(),
                ))))
            }
            _ => None,
        }
    }
}
