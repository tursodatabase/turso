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

fn random_create_materialized_view<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
) -> Query {
    assert!(!conn_ctx.tables().is_empty());
    let name = format!("matview_{}", rng.random_range(1000..9999u32));

    // 30% chance to generate a recursive CTE matview if we can find a suitable table
    if rng.random_bool(0.3) {
        if let Some(select) = try_generate_recursive_cte_select(rng, conn_ctx, &name) {
            return Query::CreateMaterializedView(CreateMaterializedView { name, select });
        }
    }

    // Default: simple SELECT * FROM table
    let table = conn_ctx.tables().choose(rng).unwrap();
    let select = Select::simple(
        table.name.clone(),
        sql_generation::model::query::predicate::Predicate::true_(),
    );
    Query::CreateMaterializedView(CreateMaterializedView { name, select })
}

fn random_drop_materialized_view<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
) -> Query {
    // Try to find a materialized view to drop from the context
    let matviews: Vec<_> = conn_ctx.views().iter().filter(|v| v.materialized).collect();
    if let Some(view) = matviews.choose(rng) {
        Query::DropMaterializedView(DropMaterializedView {
            name: view.name.clone(),
        })
    } else {
        // No matviews in context - generate a random name in the expected range
        // This handles the case where matviews were created earlier in the same plan
        // but shadow state hasn't been updated yet
        let name = format!("matview_{}", rng.random_range(1000..9999u32));
        Query::DropMaterializedView(DropMaterializedView { name })
    }
}

/// Try to generate a recursive CTE SELECT for a hierarchical matview.
/// Returns None if no suitable table is found (needs at least 2 INTEGER columns).
fn try_generate_recursive_cte_select<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
    cte_name: &str,
) -> Option<Select> {
    use sql_generation::model::table::ColumnType;

    // Find tables with at least 2 INTEGER columns (for id and parent_id pattern)
    let suitable_tables: Vec<_> = conn_ctx
        .tables()
        .iter()
        .filter(|t| {
            let int_cols: Vec<_> = t
                .columns
                .iter()
                .filter(|c| matches!(c.column_type, ColumnType::Integer))
                .collect();
            int_cols.len() >= 2
        })
        .collect();

    if suitable_tables.is_empty() {
        return None;
    }

    let table = suitable_tables.choose(rng).unwrap();
    let int_columns: Vec<_> = table
        .columns
        .iter()
        .filter(|c| matches!(c.column_type, ColumnType::Integer))
        .collect();

    // Pick first INTEGER column as id, second as parent_id
    let id_column = int_columns[0].name.clone();
    let parent_column = int_columns[1].name.clone();

    // Collect remaining columns as content columns
    let content_columns: Vec<String> = table
        .columns
        .iter()
        .filter(|c| c.name != id_column && c.name != parent_column)
        .map(|c| c.name.clone())
        .collect();

    Some(Select::recursive_hierarchy_cte(
        cte_name.to_string(),
        table.name.clone(),
        id_column,
        parent_column,
        content_columns,
        10, // max_depth (unused but required by API)
    ))
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
            QueryDiscriminants::CreateMaterializedView => random_create_materialized_view,
            QueryDiscriminants::DropMaterializedView => random_drop_materialized_view,
            QueryDiscriminants::CreateView | QueryDiscriminants::DropView => {
                unreachable!("regular view queries not yet implemented in generation")
            }
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
            QueryDiscriminants::Pragma => remaining.pragma_count + remaining.enable_cdc,
            QueryDiscriminants::CreateMaterializedView => remaining.create_matview,
            QueryDiscriminants::DropMaterializedView => remaining.drop_matview,
            QueryDiscriminants::CreateView | QueryDiscriminants::DropView => {
                0 // regular view queries not yet implemented in generation
            }
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
