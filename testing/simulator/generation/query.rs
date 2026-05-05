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
        table::{ColumnType, SimValue, Table},
    },
};
use turso_core::types;

#[derive(Debug, Clone, Copy)]
pub(super) struct PragmaGeneration {
    pub page_size_get_weight: u32,
    pub page_size_set_weight: u32,
    pub integrity_check_weight: u32,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct QueryGeneration {
    pub pragma: PragmaGeneration,
    pub storage_stress: bool,
}

pub(super) const STORAGE_STRESS_PAGE_SIZES: [u32; 3] = [512, 1024, 2048];

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

pub(super) fn random_storage_stress_insert<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
) -> Query {
    storage_stress_insert(rng, conn_ctx, StorageStressInsertRows::ProfileCapped)
}

pub(super) fn forced_storage_stress_fill_insert<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
) -> Query {
    storage_stress_insert(rng, conn_ctx, StorageStressInsertRows::ForcedFill)
}

enum StorageStressInsertRows {
    ProfileCapped,
    ForcedFill,
}

fn storage_stress_insert<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
    rows: StorageStressInsertRows,
) -> Query {
    const UNIQUE_BASE_OFFSET_RANGE: std::ops::Range<i64> = 3_000_000_000..4_000_000_000;
    const UNIQUE_COL_STRIDE: i64 = 10_000_000;
    const TARGET_ROWS: u32 = 80;
    const PAYLOAD_BYTES: usize = 3_000;

    let table = conn_ctx
        .tables()
        .iter()
        .filter(|table| !table.name.contains('.'))
        .max_by_key(|table| {
            table
                .columns
                .iter()
                .filter(|column| {
                    matches!(column.column_type, ColumnType::Text | ColumnType::Blob)
                        && !column.has_unique_or_pk()
                })
                .count()
        })
        .unwrap_or_else(|| conn_ctx.tables().choose(rng).unwrap());

    let insert_opts = &conn_ctx.opts().query.insert;
    let min_rows = insert_opts.min_rows.get();
    let num_rows = match rows {
        StorageStressInsertRows::ForcedFill => TARGET_ROWS.max(min_rows),
        StorageStressInsertRows::ProfileCapped => {
            let max_rows = insert_opts.max_rows.get().saturating_sub(1);
            if max_rows < min_rows {
                min_rows
            } else {
                TARGET_ROWS.clamp(min_rows, max_rows)
            }
        }
    };
    let base_offset = rng.random_range(UNIQUE_BASE_OFFSET_RANGE);
    let values = (0..num_rows)
        .map(|row_idx| {
            table
                .columns
                .iter()
                .enumerate()
                .map(|(col_idx, column)| {
                    if column.has_unique_or_pk() {
                        let offset =
                            base_offset + (col_idx as i64 * UNIQUE_COL_STRIDE) + row_idx as i64;
                        return SimValue::unique_for_type(&column.column_type, offset);
                    }

                    match column.column_type {
                        ColumnType::Text => {
                            let mut payload = String::with_capacity(PAYLOAD_BYTES + 32);
                            for _ in 0..PAYLOAD_BYTES {
                                payload.push('x');
                            }
                            payload.push_str(&format!("{base_offset}:{row_idx}"));
                            SimValue(types::Value::Text(payload.into()))
                        }
                        ColumnType::Blob => {
                            let mut payload = vec![b'x'; PAYLOAD_BYTES];
                            payload
                                .extend_from_slice(format!("{base_offset}:{row_idx}").as_bytes());
                            SimValue(types::Value::Blob(payload))
                        }
                        ColumnType::Integer | ColumnType::Float => {
                            SimValue::arbitrary_from(rng, conn_ctx, &column.column_type)
                        }
                    }
                })
                .collect()
        })
        .collect();

    Query::Insert(Insert::Values {
        table: table.name.clone(),
        values,
        on_conflict: None,
    })
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

fn random_pragma<R: rand::Rng + ?Sized>(
    rng: &mut R,
    conn_ctx: &impl GenerationContext,
    pragma_generation: PragmaGeneration,
) -> Query {
    #[derive(Clone, Copy)]
    enum PragmaKind {
        AutoVacuum,
        ForeignKeyList,
        IntegrityCheck,
        PageSizeGet,
        PageSizeSet,
    }

    let mut choices = Vec::new();
    choices.push((4, PragmaKind::AutoVacuum));
    if !conn_ctx.tables().is_empty() {
        choices.push((4, PragmaKind::ForeignKeyList));
    }
    if pragma_generation.integrity_check_weight > 0 {
        choices.push((
            pragma_generation.integrity_check_weight,
            PragmaKind::IntegrityCheck,
        ));
    }
    if pragma_generation.page_size_get_weight > 0 {
        choices.push((
            pragma_generation.page_size_get_weight,
            PragmaKind::PageSizeGet,
        ));
    }
    if pragma_generation.page_size_set_weight > 0 {
        choices.push((
            pragma_generation.page_size_set_weight,
            PragmaKind::PageSizeSet,
        ));
    }
    let idx = WeightedIndex::new(choices.iter().map(|(weight, _)| *weight))
        .unwrap()
        .sample(rng);

    let pragma = match choices[idx].1 {
        PragmaKind::AutoVacuum => {
            const ALL_MODES: [VacuumMode; 2] = [
                VacuumMode::None,
                // VacuumMode::Incremental, not implemented yet
                VacuumMode::Full,
            ];
            Pragma::AutoVacuumMode(ALL_MODES.choose(rng).unwrap().clone())
        }
        PragmaKind::ForeignKeyList => {
            let table = conn_ctx.tables().choose(rng).unwrap();
            Pragma::ForeignKeyList(table.name.clone())
        }
        PragmaKind::IntegrityCheck => Pragma::IntegrityCheck,
        PragmaKind::PageSizeGet => Pragma::PageSize,
        PragmaKind::PageSizeSet => {
            Pragma::PageSizeSet(*STORAGE_STRESS_PAGE_SIZES.choose(rng).unwrap())
        }
    };

    Query::Pragma(pragma)
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
const EMPTY_QUERIES_WITH_PRAGMA: &[QueryDiscriminants] = &[
    QueryDiscriminants::Select,
    QueryDiscriminants::Create,
    QueryDiscriminants::Pragma,
];

pub const fn possible_queries(tables: &[Table]) -> &'static [QueryDiscriminants] {
    if tables.is_empty() {
        EMPTY_QUERIES_WITH_PRAGMA
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
            | QueryDiscriminants::Rollback
            | QueryDiscriminants::Savepoint
            | QueryDiscriminants::RollbackToSavepoint
            | QueryDiscriminants::ReleaseSavepoint => {
                unreachable!("transactional queries should not be generated")
            }
            QueryDiscriminants::Placeholder => {
                unreachable!("Query Placeholders should not be generated")
            }
            QueryDiscriminants::Pragma => {
                unreachable!("pragma generation needs QueryDistribution options")
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
            | QueryDiscriminants::Rollback
            | QueryDiscriminants::Savepoint
            | QueryDiscriminants::RollbackToSavepoint
            | QueryDiscriminants::ReleaseSavepoint => {
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
    query_weights: Vec<u32>,
    weights: WeightedIndex<u32>,
    query_generation: QueryGeneration,
}

impl QueryDistribution {
    pub fn new(
        queries: &'static [QueryDiscriminants],
        remaining: &Remaining,
        query_generation: QueryGeneration,
    ) -> Self {
        let query_weights = queries
            .iter()
            .map(|query| {
                let weight = query.weight(remaining);
                if !query_generation.storage_stress || weight == 0 {
                    return weight;
                }
                match query {
                    QueryDiscriminants::Insert | QueryDiscriminants::Pragma => {
                        weight.saturating_mul(3)
                    }
                    _ => weight,
                }
            })
            .collect::<Vec<_>>();
        let weights = WeightedIndex::new(query_weights.iter().copied()).unwrap();
        Self {
            queries,
            query_weights,
            weights,
            query_generation,
        }
    }

    pub(super) fn storage_stress(&self) -> bool {
        self.query_generation.storage_stress
    }

    pub(super) fn positive_items(&self) -> impl Iterator<Item = QueryDiscriminants> + '_ {
        self.queries
            .iter()
            .zip(self.query_weights.iter())
            .filter(|(_, weight)| **weight > 0)
            .map(|(query, _)| *query)
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
        if matches!(self.queries[idx], QueryDiscriminants::Pragma) {
            return random_pragma(rng, ctx, self.query_generation.pragma);
        }
        if matches!(self.queries[idx], QueryDiscriminants::Insert)
            && self.query_generation.storage_stress
            && rng.random_bool(0.35)
        {
            return random_storage_stress_insert(rng, ctx);
        }
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
