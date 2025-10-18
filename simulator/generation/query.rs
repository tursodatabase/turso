use crate::{
    generation::WeightedDistribution,
    model::{Query, QueryDiscriminants, metrics::Remaining},
};
use rand::{
    Rng,
    distr::{Distribution, weighted::WeightedIndex},
    seq::IndexedRandom,
};
use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, query::SelectFree},
    model::{
        query::{
            Create, CreateIndex, Delete, DropIndex, Insert, Select,
            alter_table::AlterTable,
            pragma::{Pragma, VacuumMode},
            update::Update,
        },
        table::Table,
    },
};

fn random_create<R: rand::Rng + ?Sized>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
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

fn random_select<R: rand::Rng + ?Sized>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    if !conn_ctx.tables().is_empty() && rng.random_bool(0.7) {
        Query::Select(Select::arbitrary(rng, conn_ctx))
    } else {
        // Random expression
        Query::Select(SelectFree::arbitrary(rng, conn_ctx).0)
    }
}

fn random_insert<R: rand::Rng + ?Sized>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::Insert(Insert::arbitrary(rng, conn_ctx))
}

fn random_delete<R: rand::Rng + ?Sized>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::Delete(Delete::arbitrary(rng, conn_ctx))
}

fn random_update<R: rand::Rng + ?Sized>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::Update(Update::arbitrary(rng, conn_ctx))
}

fn random_drop<R: rand::Rng + ?Sized>(rng: &mut R, conn_ctx: &impl GenerationContext) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::Drop(sql_generation::model::query::Drop::arbitrary(rng, conn_ctx))
}

fn random_create_index<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
) -> Query {
    assert!(!conn_ctx.tables().is_empty());

    let mut create_index = CreateIndex::arbitrary(rng, conn_ctx);
    while conn_ctx
        .tables()
        .iter()
        .find(|t| t.name == create_index.table_name)
        .expect("table should exist")
        .indexes
        .iter()
        .any(|i| i.index_name == create_index.index_name)
    {
        create_index = CreateIndex::arbitrary(rng, conn_ctx);
    }

    Query::CreateIndex(create_index)
}

fn random_pragma<R: rand::Rng + ?Sized>(rng: &mut R, _conn_ctx: &impl GenerationContext) -> Query {
    const ALL_MODES: [VacuumMode; 2] = [
        VacuumMode::None,
        // VacuumMode::Incremental, not implemented yet
        VacuumMode::Full,
    ];

    let mode = ALL_MODES.choose(rng).unwrap();

    Query::Pragma(Pragma::AutoVacuumMode(mode.clone()))
}

fn random_alter_table<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    Query::AlterTable(AlterTable::arbitrary(rng, conn_ctx))
}

fn random_drop_index<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
) -> Query {
    assert!(
        conn_ctx
            .tables()
            .iter()
            .any(|table| !table.indexes.is_empty())
    );
    Query::DropIndex(DropIndex::arbitrary(rng, conn_ctx))
}

/// Possible queries that can be generated given the table state
///
/// Does not take into account transactional statements
pub const fn possible_queries(tables: &[Table]) -> &'static [QueryDiscriminants] {
    if tables.is_empty() {
        &[QueryDiscriminants::Select, QueryDiscriminants::Create]
    } else {
        QueryDiscriminants::ALL_NO_TRANSACTION
    }
}

type QueryGenFunc<R, G> = fn(&mut R, &G) -> Query;

impl QueryDiscriminants {
    fn gen_function<R, G>(&self) -> QueryGenFunc<R, G>
    where
        R: rand::Rng + ?Sized,
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
            QueryDiscriminants::AlterTable => random_alter_table,
            QueryDiscriminants::DropIndex => random_drop_index,
            QueryDiscriminants::Begin
            | QueryDiscriminants::Commit
            | QueryDiscriminants::Rollback => {
                unreachable!("transactional queries should not be generated")
            }
            QueryDiscriminants::Placeholder => {
                unreachable!("Query Placeholders should not be generated")
            }
            QueryDiscriminants::Pragma => random_pragma,
        }
    }

    fn weight(&self, remaining: &Remaining) -> u32 {
        match self {
            QueryDiscriminants::Create => remaining.create,
            // remaining.select / 3 is for the random_expr generation
            // have a max of 1 so that we always generate at least a non zero weight for `QueryDistribution`
            QueryDiscriminants::Select => (remaining.select + remaining.select / 3).max(1),
            QueryDiscriminants::Insert => remaining.insert,
            QueryDiscriminants::Delete => remaining.delete,
            QueryDiscriminants::Update => remaining.update,
            QueryDiscriminants::Drop => remaining.drop,
            QueryDiscriminants::CreateIndex => remaining.create_index,
            QueryDiscriminants::AlterTable => remaining.alter_table,
            QueryDiscriminants::DropIndex => remaining.drop_index,
            QueryDiscriminants::Begin
            | QueryDiscriminants::Commit
            | QueryDiscriminants::Rollback => {
                unreachable!("transactional queries should not be generated")
            }
            QueryDiscriminants::Placeholder => {
                unreachable!("Query Placeholders should not be generated")
            }
            QueryDiscriminants::Pragma => remaining.pragma_count,
        }
    }
}

#[derive(Debug)]
pub(super) struct QueryDistribution {
    queries: &'static [QueryDiscriminants],
    weights: WeightedIndex<u32>,
}

impl QueryDistribution {
    pub fn new(queries: &'static [QueryDiscriminants], remaining: &Remaining) -> Self {
        let query_weights =
            WeightedIndex::new(queries.iter().map(|query| query.weight(remaining))).unwrap();
        Self {
            queries,
            weights: query_weights,
        }
    }
}

impl WeightedDistribution for QueryDistribution {
    type Item = QueryDiscriminants;
    type GenItem = Query;

    fn items(&self) -> &[Self::Item] {
        self.queries
    }

    fn weights(&self) -> &WeightedIndex<u32> {
        &self.weights
    }

    fn sample<R: rand::Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        ctx: &C,
    ) -> Self::GenItem {
        let weights = &self.weights;

        let idx = weights.sample(rng);
        let query_fn = self.queries[idx].gen_function();
        (query_fn)(rng, ctx)
    }
}

impl ArbitraryFrom<&QueryDistribution> for Query {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        query_distr: &QueryDistribution,
    ) -> Self {
        query_distr.sample(rng, context)
    }
}
