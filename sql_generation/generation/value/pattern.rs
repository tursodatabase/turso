use turso_core::Value;

use crate::{
    generation::{ArbitraryFromMaybe, GenerationContext},
    model::table::SimValue,
};

pub struct LikeValue(pub SimValue);

impl ArbitraryFromMaybe<&SimValue> for LikeValue {
    fn arbitrary_from_maybe<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        value: &SimValue,
    ) -> Option<Self> {
        match &value.0 {
            value @ Value::Text(..) => {
                let t = value.to_string();
                let mut t = t.chars().collect::<Vec<_>>();
                if t.is_empty() {
                    return Some(Self(SimValue(Value::build_text("%".to_string()))));
                }
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
