use turso_core::Value;

use crate::{
    generation::{ArbitraryFrom, GenerationContext},
    model::table::SimValue,
};

pub struct LTValue(pub SimValue);

impl ArbitraryFrom<&Vec<&SimValue>> for LTValue {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
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
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
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
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
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
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
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
