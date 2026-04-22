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
            // TODO: widen back to the full i64 range once tursodb's record-path
            // REAL↔INTEGER comparison uses SQLite's truncate-then-compare semantics
            // (see core/numeric/mod.rs sqlite_int_float_cmp). Currently, tursodb
            // promotes both sides to f64, so distinct i64s that round to the same f64
            // compare equal — which disagrees with the model when a virtual generated
            // column (REAL AS (<expr over int_col>)) feeds a predicate literal.
            // Staying within ±2^53 keeps every i64 exactly representable in f64.
            ColumnType::Integer => Value::from_i64(rng.random_range(-(1i64 << 53)..(1i64 << 53))),
            ColumnType::Float => Value::from_f64(rng.random_range(-1e10..1e10)),
            ColumnType::Text => Value::build_text(gen_random_text(rng)),
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
