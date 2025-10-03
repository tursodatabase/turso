use crate::model::{Query, QueryDiscriminants};
use rand::{
    Rng,
    distr::{Distribution, weighted::WeightedIndex},
};
use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, query::SelectFree},
    model::{
        query::{Create, CreateIndex, Delete, Insert, Select, update::Update},
        table::Table,
    },
};

use super::property::Remaining;

fn random_create<R: rand::Rng>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    let mut create = Create::arbitrary(rng, conn_ctx);
    while conn_ctx
        .tables()
        .iter()
        .any(|t| t.name == create.table.name)
    {
        create = Create::arbitrary(rng, conn_ctx);
    }
    Query::Create(create)
}

fn random_select<R: rand::Rng>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    if rng.random_bool(0.7) {
        Query::Select(Select::arbitrary(rng, conn_ctx))
    } else {
        // Random expression
        Query::Select(SelectFree::arbitrary(rng, conn_ctx).0)
    }
}

fn random_insert<R: rand::Rng>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::Insert(Insert::arbitrary(rng, conn_ctx))
}

fn random_delete<R: rand::Rng>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::Delete(Delete::arbitrary(rng, conn_ctx))
}

fn random_update<R: rand::Rng>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::Update(Update::arbitrary(rng, conn_ctx))
}

fn random_drop<R: rand::Rng>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::Drop(sql_generation::model::query::Drop::arbitrary(rng, conn_ctx))
}

fn random_create_index<R: rand::Rng>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());

    let mut create_index = CreateIndex::arbitrary(rng, conn_ctx);
    while conn_ctx
        .tables()
        .iter()
        .find(|t| t.name == create_index.table_name)
        .expect("table should exist")
        .indexes
        .iter()
        .any(|i| i == &create_index.index_name)
    {
        create_index = CreateIndex::arbitrary(rng, conn_ctx);
    }

    Query::CreateIndex(create_index)
}

/// Possible queries that can be generated given the table state
///
/// Does not take into account transactional statements
pub fn possible_queries(tables: &[Table]) -> Vec<QueryDiscriminants> {
    let mut queries = vec![QueryDiscriminants::Select, QueryDiscriminants::Create];
    if !tables.is_empty() {
        queries.extend([
            QueryDiscriminants::Insert,
            QueryDiscriminants::Update,
            QueryDiscriminants::Delete,
            QueryDiscriminants::Drop,
            QueryDiscriminants::CreateIndex,
        ]);
    }
    queries
}

type QueryGenFunc<R, G> = fn(&mut R, &G) -> Query;

impl QueryDiscriminants {
    pub fn gen_function<R, G>(&self) -> QueryGenFunc<R, G>
    where
        R: rand::Rng,
        G: GenerationContext,
    {
        match self {
            QueryDiscriminants::Create => random_create,
            QueryDiscriminants::Select => random_select,
            QueryDiscriminants::Insert => random_insert,
            QueryDiscriminants::Delete => random_delete,
            QueryDiscriminants::Update => random_update,
            QueryDiscriminants::Drop => random_drop,
            QueryDiscriminants::CreateIndex => random_create_index,
            QueryDiscriminants::Begin
            | QueryDiscriminants::Commit
            | QueryDiscriminants::Rollback => {
                unreachable!("transactional queries should not be generated")
            }
        }
    }

    pub fn weight(&self, remaining: &Remaining) -> u32 {
        match self {
            QueryDiscriminants::Create => remaining.create,
            QueryDiscriminants::Select => remaining.select + remaining.select / 3, // remaining.select / 3 is for the random_expr generation
            QueryDiscriminants::Insert => remaining.insert,
            QueryDiscriminants::Delete => remaining.delete,
            QueryDiscriminants::Update => remaining.update,
            QueryDiscriminants::Drop => 0,
            QueryDiscriminants::CreateIndex => remaining.create_index,
            QueryDiscriminants::Begin
            | QueryDiscriminants::Commit
            | QueryDiscriminants::Rollback => {
                unreachable!("transactional queries should not be generated")
            }
        }
    }
}

impl ArbitraryFrom<&Remaining> for Query {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        remaining: &Remaining,
    ) -> Self {
        let queries = possible_queries(context.tables());
        let weights =
            WeightedIndex::new(queries.iter().map(|query| query.weight(remaining))).unwrap();

        let idx = weights.sample(rng);
        let query_fn = queries[idx].gen_function();
        let query = (query_fn)(rng, context);

        query
    }
}
