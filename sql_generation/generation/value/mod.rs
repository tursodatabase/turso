use rand::Rng;
use turso_core::Value;

use crate::{
    generation::{gen_random_text, pick, ArbitraryFrom, GenerationContext},
    model::table::{ColumnType, SimValue, Table},
};

mod cmp;
mod pattern;

pub use cmp::{GTValue, LTValue};
pub use pattern::LikeValue;

impl ArbitraryFrom<&Table> for Vec<SimValue> {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
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
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
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
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        column_type: &ColumnType,
    ) -> Self {
        let value = match column_type {
            ColumnType::Integer => Value::from_i64(rng.random_range(i64::MIN..i64::MAX)),
            ColumnType::Float => Value::from_f64(rng.random_range(-1e10..1e10)),
            ColumnType::Text => {
                if rng.random_ratio(1, 100) {
                    Value::build_text(String::new())
                } else {
                    Value::build_text(gen_random_text(rng))
                }
            }
            ColumnType::Blob => Value::Blob(gen_random_text(rng).into_bytes()),
        };
        SimValue(value)
    }
}

impl ArbitraryFrom<ColumnType> for SimValue {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        column_type: ColumnType,
    ) -> Self {
        SimValue::arbitrary_from(rng, context, &column_type)
    }
}
