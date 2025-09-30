use crate::model::Query;
use rand::Rng;
use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, frequency},
    model::query::{Create, Delete, Insert, Select, update::Update},
};

use super::property::Remaining;

impl ArbitraryFrom<&Remaining> for Query {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        remaining: &Remaining,
    ) -> Self {
        frequency(
            vec![
                (
                    remaining.create,
                    Box::new(|rng| Self::Create(Create::arbitrary(rng, context))),
                ),
                (
                    remaining.select,
                    Box::new(|rng| Self::Select(Select::arbitrary(rng, context))),
                ),
                (
                    remaining.insert,
                    Box::new(|rng| Self::Insert(Insert::arbitrary(rng, context))),
                ),
                (
                    remaining.update,
                    Box::new(|rng| Self::Update(Update::arbitrary(rng, context))),
                ),
                (
                    remaining.insert.min(remaining.delete),
                    Box::new(|rng| Self::Delete(Delete::arbitrary(rng, context))),
                ),
            ],
            rng,
        )
    }
}
