use indexmap::IndexSet;
use rand::Rng;
use turso_core::Value;

use crate::generation::{
    gen_random_text, pick, readable_name_custom, Arbitrary, ArbitraryFrom, GenerationContext,
};
use crate::model::table::{Column, ColumnType, Name, SimValue, Table};

use super::ArbitraryFromMaybe;

impl Arbitrary for Name {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, _c: &C) -> Self {
        let name = readable_name_custom("_", rng);
        Name(name.replace("-", "_"))
    }
}

impl Arbitrary for Table {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let opts = context.opts().table.clone();
        let name = Name::arbitrary(rng, context).0;
        let large_table =
            opts.large_table.enable && rng.random_bool(opts.large_table.large_table_prob);
        let column_size = if large_table {
            rng.random_range(opts.large_table.column_range)
        } else {
            rng.random_range(opts.column_range)
        } as usize;
        let mut column_set = IndexSet::with_capacity(column_size);
        for col in std::iter::repeat_with(|| Column::arbitrary(rng, context)) {
            column_set.insert(col);
            if column_set.len() == column_size {
                break;
            }
        }

        Table {
            rows: Vec::new(),
            name,
            columns: Vec::from_iter(column_set),
            indexes: vec![],
        }
    }
}

impl Arbitrary for Column {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let name = Name::arbitrary(rng, context).0;
        let column_type = ColumnType::arbitrary(rng, context);
        Self {
            name,
            column_type,
            primary: false,
            unique: false,
        }
    }
}

impl Arbitrary for ColumnType {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        pick(&[Self::Integer, Self::Float, Self::Text, Self::Blob], rng).to_owned()
    }
}

impl ArbitraryFrom<&Table> for Vec<SimValue> {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        table: &Table,
    ) -> Self {
        let mut row = Vec::new();
        for column in table.columns.iter() {
            let value = SimValue::arbitrary_from(rng, context, &column.column_type);
            row.push(value);
        }
        row
    }
}

impl ArbitraryFrom<&Vec<&SimValue>> for SimValue {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        values: &Vec<&Self>,
    ) -> Self {
        if values.is_empty() {
            return Self(Value::Null);
        }

        pick(values, rng).to_owned().clone()
    }
}

impl ArbitraryFrom<&ColumnType> for SimValue {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        column_type: &ColumnType,
    ) -> Self {
        let value = match column_type {
            ColumnType::Integer => Value::Integer(rng.random_range(i64::MIN..i64::MAX)),
            ColumnType::Float => Value::Float(rng.random_range(-1e10..1e10)),
            ColumnType::Text => Value::build_text(gen_random_text(rng)),
            ColumnType::Blob => Value::Blob(gen_random_text(rng).as_bytes().to_vec()),
        };
        SimValue(value)
    }
}

pub struct LTValue(pub SimValue);

impl ArbitraryFrom<&Vec<&SimValue>> for LTValue {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        values: &Vec<&SimValue>,
    ) -> Self {
        if values.is_empty() {
            return Self(SimValue(Value::Null));
        }

        // Get value less than all values
        let value = Value::exec_min(values.iter().map(|value| &value.0));
        Self::arbitrary_from(rng, context, &SimValue(value))
    }
}

impl ArbitraryFrom<&SimValue> for LTValue {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        value: &SimValue,
    ) -> Self {
        let new_value = match &value.0 {
            Value::Integer(i) => Value::Integer(rng.random_range(i64::MIN..*i - 1)),
            Value::Float(f) => Value::Float(f - rng.random_range(0.0..1e10)),
            value @ Value::Text(..) => {
                // Either shorten the string, or make at least one character smaller and mutate the rest
                let mut t = value.to_string();
                if rng.random_bool(0.01) {
                    t.pop();
                    Value::build_text(t)
                } else {
                    let mut t = t.chars().map(|c| c as u32).collect::<Vec<_>>();
                    let index = rng.random_range(0..t.len());
                    t[index] -= 1;
                    // Mutate the rest of the string
                    for val in t.iter_mut().skip(index + 1) {
                        *val = rng.random_range('a' as u32..='z' as u32);
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
                if rng.random_bool(0.01) {
                    b.pop();
                    Value::Blob(b)
                } else {
                    let index = rng.random_range(0..b.len());
                    b[index] -= 1;
                    // Mutate the rest of the blob
                    for val in b.iter_mut().skip(index + 1) {
                        *val = rng.random_range(0..=255);
                    }
                    Value::Blob(b)
                }
            }
            _ => unreachable!(),
        };
        Self(SimValue(new_value))
    }
}

pub struct GTValue(pub SimValue);

impl ArbitraryFrom<&Vec<&SimValue>> for GTValue {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        values: &Vec<&SimValue>,
    ) -> Self {
        if values.is_empty() {
            return Self(SimValue(Value::Null));
        }
        // Get value greater than all values
        let value = Value::exec_max(values.iter().map(|value| &value.0));

        Self::arbitrary_from(rng, context, &SimValue(value))
    }
}

impl ArbitraryFrom<&SimValue> for GTValue {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        value: &SimValue,
    ) -> Self {
        let new_value = match &value.0 {
            Value::Integer(i) => Value::Integer(rng.random_range(*i..i64::MAX)),
            Value::Float(f) => Value::Float(rng.random_range(*f..1e10)),
            value @ Value::Text(..) => {
                // Either lengthen the string, or make at least one character smaller and mutate the rest
                let mut t = value.to_string();
                if rng.random_bool(0.01) {
                    t.push(rng.random_range(0..=255) as u8 as char);
                    Value::build_text(t)
                } else {
                    let mut t = t.chars().map(|c| c as u32).collect::<Vec<_>>();
                    let index = rng.random_range(0..t.len());
                    t[index] += 1;
                    // Mutate the rest of the string
                    for val in t.iter_mut().skip(index + 1) {
                        *val = rng.random_range('a' as u32..='z' as u32);
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
                if rng.random_bool(0.01) {
                    b.push(rng.random_range(0..=255));
                    Value::Blob(b)
                } else {
                    let index = rng.random_range(0..b.len());
                    b[index] += 1;
                    // Mutate the rest of the blob
                    for val in b.iter_mut().skip(index + 1) {
                        *val = rng.random_range(0..=255);
                    }
                    Value::Blob(b)
                }
            }
            _ => unreachable!(),
        };
        Self(SimValue(new_value))
    }
}

pub struct LikeValue(pub SimValue);

impl ArbitraryFromMaybe<&SimValue> for LikeValue {
    fn arbitrary_from_maybe<R: Rng, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        value: &SimValue,
    ) -> Option<Self> {
        match &value.0 {
            value @ Value::Text(..) => {
                let t = value.to_string();
                let mut t = t.chars().collect::<Vec<_>>();
                // Remove a number of characters, either insert `_` for each character removed, or
                // insert one `%` for the whole substring
                let mut i = 0;
                while i < t.len() {
                    if rng.random_bool(0.1) {
                        t[i] = '_';
                    } else if rng.random_bool(0.05) {
                        t[i] = '%';
                        // skip a list of characters
                        for _ in 0..rng.random_range(0..=3.min(t.len() - i - 1)) {
                            t.remove(i + 1);
                        }
                    }
                    i += 1;
                }
                let index = rng.random_range(0..t.len());
                t.insert(index, '%');
                Some(Self(SimValue(Value::build_text(
                    t.into_iter().collect::<String>(),
                ))))
            }
            _ => None,
        }
    }
}
