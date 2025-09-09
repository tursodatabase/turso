use crate::generation::{
    gen_random_text, pick_n_unique, pick_unique, Arbitrary, ArbitraryFrom, ArbitrarySized,
    GenerationContext,
};
use crate::model::query::predicate::Predicate;
use crate::model::query::select::{
    CompoundOperator, CompoundSelect, Distinctness, FromClause, OrderBy, ResultColumn, SelectBody,
    SelectInner,
};
use crate::model::query::update::Update;
use crate::model::query::{Create, CreateIndex, Delete, Drop, Insert, Select};
use crate::model::table::{JoinTable, JoinType, JoinedTable, SimValue, Table, TableContext};
use indexmap::IndexSet;
use itertools::Itertools;
use rand::Rng;
use turso_parser::ast::{Expr, SortOrder};

use super::{backtrack, pick};

impl Arbitrary for Create {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        Create {
            table: Table::arbitrary(rng, context),
        }
    }
}

impl Arbitrary for FromClause {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let opts = &context.opts().query.from_clause;
        let weights = opts.as_weighted_index();
        let num_joins = opts.joins[rng.sample(weights)].num_joins;

        let mut tables = context.tables().clone();
        let mut table = pick(&tables, rng).clone();

        tables.retain(|t| t.name != table.name);

        let name = table.name.clone();

        let mut table_context = JoinTable {
            tables: Vec::new(),
            rows: Vec::new(),
        };

        let joins: Vec<_> = (0..num_joins)
            .filter_map(|_| {
                if tables.is_empty() {
                    return None;
                }
                let join_table = pick(&tables, rng).clone();
                let joined_table_name = join_table.name.clone();

                tables.retain(|t| t.name != join_table.name);
                table_context.rows = table_context
                    .rows
                    .iter()
                    .cartesian_product(join_table.rows.iter())
                    .map(|(t_row, j_row)| {
                        let mut row = t_row.clone();
                        row.extend(j_row.clone());
                        row
                    })
                    .collect();
                // TODO: inneficient. use a Deque to push_front?
                table_context.tables.insert(0, join_table);
                for row in &mut table.rows {
                    assert_eq!(
                        row.len(),
                        table.columns.len(),
                        "Row length does not match column length after join"
                    );
                }

                let predicate = Predicate::arbitrary_from(rng, context, &table);
                Some(JoinedTable {
                    table: joined_table_name,
                    join_type: JoinType::Inner,
                    on: predicate,
                })
            })
            .collect();
        FromClause { table: name, joins }
    }
}

impl Arbitrary for SelectInner {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let from = FromClause::arbitrary(rng, env);
        let tables = env.tables().clone();
        let join_table = from.into_join_table(&tables);
        let cuml_col_count = join_table.columns().count();

        let order_by = rng
            .random_bool(env.opts().query.select.order_by_prob)
            .then(|| {
                let order_by_table_candidates = from
                    .joins
                    .iter()
                    .map(|j| &j.table)
                    .chain(std::iter::once(&from.table))
                    .collect::<Vec<_>>();
                let order_by_col_count =
                    (rng.random::<f64>() * rng.random::<f64>() * (cuml_col_count as f64)) as usize; // skew towards 0
                if order_by_col_count == 0 {
                    return None;
                }
                let mut col_names = IndexSet::new();
                let mut order_by_cols = Vec::new();
                while order_by_cols.len() < order_by_col_count {
                    let table = pick(&order_by_table_candidates, rng);
                    let table = tables.iter().find(|t| t.name == table.as_str()).unwrap();
                    let col = pick(&table.columns, rng);
                    let col_name = format!("{}.{}", table.name, col.name);
                    if col_names.insert(col_name.clone()) {
                        order_by_cols.push((
                            col_name,
                            if rng.random_bool(0.5) {
                                SortOrder::Asc
                            } else {
                                SortOrder::Desc
                            },
                        ));
                    }
                }
                Some(OrderBy {
                    columns: order_by_cols,
                })
            })
            .flatten();

        SelectInner {
            distinctness: if env.opts().indexes {
                Distinctness::arbitrary(rng, env)
            } else {
                Distinctness::All
            },
            columns: vec![ResultColumn::Star],
            from: Some(from),
            where_clause: Predicate::arbitrary_from(rng, env, &join_table),
            order_by,
        }
    }
}

impl ArbitrarySized for SelectInner {
    fn arbitrary_sized<R: Rng, C: GenerationContext>(
        rng: &mut R,
        env: &C,
        num_result_columns: usize,
    ) -> Self {
        let mut select_inner = SelectInner::arbitrary(rng, env);
        let select_from = &select_inner.from.as_ref().unwrap();
        let table_names = select_from
            .joins
            .iter()
            .map(|j| &j.table)
            .chain(std::iter::once(&select_from.table));

        let flat_columns_names = table_names
            .flat_map(|t| {
                env.tables()
                    .iter()
                    .find(|table| table.name == *t)
                    .unwrap()
                    .columns
                    .iter()
                    .map(move |c| format!("{}.{}", t, c.name))
            })
            .collect::<Vec<_>>();
        let selected_columns = pick_unique(&flat_columns_names, num_result_columns, rng);
        let columns = selected_columns
            .map(|col_name| ResultColumn::Column(col_name.clone()))
            .collect();

        select_inner.columns = columns;
        select_inner
    }
}

impl Arbitrary for Distinctness {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        match rng.random_range(0..=5) {
            0..4 => Distinctness::All,
            _ => Distinctness::Distinct,
        }
    }
}

impl Arbitrary for CompoundOperator {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        match rng.random_range(0..=1) {
            0 => CompoundOperator::Union,
            1 => CompoundOperator::UnionAll,
            _ => unreachable!(),
        }
    }
}

/// SelectFree is a wrapper around Select that allows for arbitrary generation
/// of selects without requiring a specific environment, which is useful for generating
/// arbitrary expressions without referring to the tables.
pub struct SelectFree(pub Select);

impl Arbitrary for SelectFree {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let expr = Predicate(Expr::arbitrary_sized(rng, env, 8));
        let select = Select::expr(expr);
        Self(select)
    }
}

impl Arbitrary for Select {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        // Generate a number of selects based on the query size
        // If experimental indexes are enabled, we can have selects with compounds
        // Otherwise, we just have a single select with no compounds
        let opts = &env.opts().query.select;
        let num_compound_selects = if env.opts().indexes {
            opts.compound_selects[rng.sample(opts.compound_select_weighted_index())]
                .num_compound_selects
        } else {
            0
        };

        let min_column_count_across_tables =
            env.tables().iter().map(|t| t.columns.len()).min().unwrap();

        let num_result_columns = rng.random_range(1..=min_column_count_across_tables);

        let mut first = SelectInner::arbitrary_sized(rng, env, num_result_columns);

        let mut rest: Vec<SelectInner> = (0..num_compound_selects)
            .map(|_| SelectInner::arbitrary_sized(rng, env, num_result_columns))
            .collect();

        if !rest.is_empty() {
            // ORDER BY is not supported in compound selects yet
            first.order_by = None;
            for s in &mut rest {
                s.order_by = None;
            }
        }

        Self {
            body: SelectBody {
                select: Box::new(first),
                compounds: rest
                    .into_iter()
                    .map(|s| CompoundSelect {
                        operator: CompoundOperator::arbitrary(rng, env),
                        select: Box::new(s),
                    })
                    .collect(),
            },
            limit: None,
        }
    }
}

impl Arbitrary for Insert {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let opts = &env.opts().query.insert;
        let gen_values = |rng: &mut R| {
            let table = pick(env.tables(), rng);
            let num_rows = rng.random_range(opts.min_rows.get()..opts.max_rows.get());
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(rng, env, &c.column_type))
                        .collect()
                })
                .collect();
            Some(Insert::Values {
                table: table.name.clone(),
                values,
            })
        };

        let _gen_select = |rng: &mut R| {
            // Find a non-empty table
            let select_table = env.tables().iter().find(|t| !t.rows.is_empty())?;
            let row = pick(&select_table.rows, rng);
            let predicate = Predicate::arbitrary_from(rng, env, (select_table, row));
            // Pick another table to insert into
            let select = Select::simple(select_table.name.clone(), predicate);
            let table = pick(env.tables(), rng);
            Some(Insert::Select {
                table: table.name.clone(),
                select: Box::new(select),
            })
        };

        // TODO: Add back gen_select when https://github.com/tursodatabase/turso/issues/2129 is fixed.
        // Backtrack here cannot return None
        backtrack(vec![(1, Box::new(gen_values))], rng).unwrap()
    }
}

impl Arbitrary for Delete {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let table = pick(env.tables(), rng);
        Self {
            table: table.name.clone(),
            predicate: Predicate::arbitrary_from(rng, env, table),
        }
    }
}

impl Arbitrary for Drop {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let table = pick(env.tables(), rng);
        Self {
            table: table.name.clone(),
        }
    }
}

impl Arbitrary for CreateIndex {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        assert!(
            !env.tables().is_empty(),
            "Cannot create an index when no tables exist in the environment."
        );

        let table = pick(env.tables(), rng);

        if table.columns.is_empty() {
            panic!(
                "Cannot create an index on table '{}' as it has no columns.",
                table.name
            );
        }

        let num_columns_to_pick = rng.random_range(1..=table.columns.len());
        let picked_column_indices = pick_n_unique(0..table.columns.len(), num_columns_to_pick, rng);

        let columns = picked_column_indices
            .map(|i| {
                let column = &table.columns[i];
                (
                    column.name.clone(),
                    if rng.random_bool(0.5) {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    },
                )
            })
            .collect::<Vec<(String, SortOrder)>>();

        let index_name = format!(
            "idx_{}_{}",
            table.name,
            gen_random_text(rng).chars().take(8).collect::<String>()
        );

        CreateIndex {
            index_name,
            table_name: table.name.clone(),
            columns,
        }
    }
}

impl Arbitrary for Update {
    fn arbitrary<R: Rng, C: GenerationContext>(rng: &mut R, env: &C) -> Self {
        let table = pick(env.tables(), rng);
        let num_cols = rng.random_range(1..=table.columns.len());
        let columns = pick_unique(&table.columns, num_cols, rng);
        let set_values: Vec<(String, SimValue)> = columns
            .map(|column| {
                (
                    column.name.clone(),
                    SimValue::arbitrary_from(rng, env, &column.column_type),
                )
            })
            .collect();
        Update {
            table: table.name.clone(),
            set_values,
            predicate: Predicate::arbitrary_from(rng, env, table),
        }
    }
}
