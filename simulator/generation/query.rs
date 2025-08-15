use crate::generation::{Arbitrary, ArbitraryFrom, ArbitrarySizedFrom, Shadow};
use crate::model::query::predicate::Predicate;
use crate::model::query::select::{
    CompoundOperator, CompoundSelect, Distinctness, FromClause, OrderBy, ResultColumn, SelectBody,
    SelectInner,
};
use crate::model::query::update::Update;
use crate::model::query::{Create, Delete, Drop, Insert, Query, Select};
use crate::model::table::{JoinTable, JoinType, JoinedTable, SimValue, Table, TableContext};
use crate::SimulatorEnv;
use itertools::Itertools;
use rand::Rng;
use turso_sqlite3_parser::ast::{Expr, SortOrder};

use super::property::Remaining;
use super::{backtrack, frequency, pick};

impl Arbitrary for Create {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        Create {
            table: Table::arbitrary(rng),
        }
    }
}

impl ArbitraryFrom<&Vec<Table>> for FromClause {
    fn arbitrary_from<R: Rng>(rng: &mut R, tables: &Vec<Table>) -> Self {
        let num_joins = match rng.gen_range(0..=100) {
            0..=90 => 0,
            91..=97 => 1,
            98..=100 => 2,
            _ => unreachable!(),
        };

        let mut tables = tables.clone();
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

                let predicate = Predicate::arbitrary_from(rng, &table);
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

impl ArbitraryFrom<&SimulatorEnv> for SelectInner {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let from = FromClause::arbitrary_from(rng, &env.tables);
        let mut tables = env.tables.clone();
        // todo: this is a temporary hack because env is not separated from the tables
        let join_table = from
            .shadow(&mut tables)
            .expect("Failed to shadow FromClause");
        let cuml_col_count = join_table.columns().count();

        let order_by = 'order_by: {
            if rng.gen_bool(0.3) {
                let order_by_table_candidates = from
                    .joins
                    .iter()
                    .map(|j| j.table.clone())
                    .chain(std::iter::once(from.table.clone()))
                    .collect::<Vec<_>>();
                let order_by_col_count =
                    (rng.gen::<f64>() * rng.gen::<f64>() * (cuml_col_count as f64)) as usize; // skew towards 0
                if order_by_col_count == 0 {
                    break 'order_by None;
                }
                let mut col_names = std::collections::HashSet::new();
                let mut order_by_cols = Vec::new();
                while order_by_cols.len() < order_by_col_count {
                    let table = pick(&order_by_table_candidates, rng);
                    let table = tables.iter().find(|t| t.name == *table).unwrap();
                    let col = pick(&table.columns, rng);
                    let col_name = format!("{}.{}", table.name, col.name);
                    if col_names.insert(col_name.clone()) {
                        order_by_cols.push((
                            col_name,
                            if rng.gen_bool(0.5) {
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
            } else {
                None
            }
        };

        SelectInner {
            distinctness: if env.opts.experimental_indexes {
                Distinctness::arbitrary(rng)
            } else {
                Distinctness::All
            },
            columns: vec![ResultColumn::Star],
            from: Some(from),
            where_clause: Predicate::arbitrary_from(rng, &join_table),
            order_by,
        }
    }
}

impl ArbitrarySizedFrom<&SimulatorEnv> for SelectInner {
    fn arbitrary_sized_from<R: Rng>(
        rng: &mut R,
        env: &SimulatorEnv,
        num_result_columns: usize,
    ) -> Self {
        let mut select_inner = SelectInner::arbitrary_from(rng, env);
        let select_from = &select_inner.from.as_ref().unwrap();
        let table_names = select_from
            .joins
            .iter()
            .map(|j| j.table.clone())
            .chain(std::iter::once(select_from.table.clone()))
            .collect::<Vec<_>>();

        let flat_columns_names = table_names
            .iter()
            .flat_map(|t| {
                env.tables
                    .iter()
                    .find(|table| table.name == *t)
                    .unwrap()
                    .columns
                    .iter()
                    .map(|c| format!("{}.{}", t.clone(), c.name))
            })
            .collect::<Vec<_>>();
        let selected_columns = pick_unique(&flat_columns_names, num_result_columns, rng);
        let mut columns = Vec::new();
        for column_name in selected_columns {
            columns.push(ResultColumn::Column(column_name.clone()));
        }
        select_inner.columns = columns;
        select_inner
    }
}

impl Arbitrary for Distinctness {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        match rng.gen_range(0..=5) {
            0..4 => Distinctness::All,
            _ => Distinctness::Distinct,
        }
    }
}
impl Arbitrary for CompoundOperator {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        match rng.gen_range(0..=1) {
            0 => CompoundOperator::Union,
            1 => CompoundOperator::UnionAll,
            _ => unreachable!(),
        }
    }
}

/// SelectFree is a wrapper around Select that allows for arbitrary generation
/// of selects without requiring a specific environment, which is useful for generating
/// arbitrary expressions without referring to the tables.
pub(crate) struct SelectFree(pub(crate) Select);

impl ArbitraryFrom<&SimulatorEnv> for SelectFree {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let expr = Predicate(Expr::arbitrary_sized_from(rng, env, 8));
        let select = Select::expr(expr);
        Self(select)
    }
}

impl ArbitraryFrom<&SimulatorEnv> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        // Generate a number of selects based on the query size
        // If experimental indexes are enabled, we can have selects with compounds
        // Otherwise, we just have a single select with no compounds
        let num_compound_selects = if env.opts.experimental_indexes {
            match rng.gen_range(0..=100) {
                0..=95 => 0,
                96..=99 => 1,
                100 => 2,
                _ => unreachable!(),
            }
        } else {
            0
        };

        let min_column_count_across_tables =
            env.tables.iter().map(|t| t.columns.len()).min().unwrap();

        let num_result_columns = rng.gen_range(1..=min_column_count_across_tables);

        let first = SelectInner::arbitrary_sized_from(rng, env, num_result_columns);

        let rest: Vec<SelectInner> = (0..num_compound_selects)
            .map(|_| SelectInner::arbitrary_sized_from(rng, env, num_result_columns))
            .collect();

        Self {
            body: SelectBody {
                select: Box::new(first),
                compounds: rest
                    .into_iter()
                    .map(|s| CompoundSelect {
                        operator: CompoundOperator::arbitrary(rng),
                        select: Box::new(s),
                    })
                    .collect(),
            },
            limit: None,
        }
    }
}

impl ArbitraryFrom<&SimulatorEnv> for Insert {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let gen_values = |rng: &mut R| {
            let table = pick(&env.tables, rng);
            let num_rows = rng.gen_range(1..10);
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(rng, &c.column_type))
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
            let select_table = env.tables.iter().find(|t| !t.rows.is_empty())?;
            let row = pick(&select_table.rows, rng);
            let predicate = Predicate::arbitrary_from(rng, (select_table, row));
            // Pick another table to insert into
            let select = Select::simple(select_table.name.clone(), predicate);
            let table = pick(&env.tables, rng);
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

impl ArbitraryFrom<&SimulatorEnv> for Delete {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let table = pick(&env.tables, rng);
        Self {
            table: table.name.clone(),
            predicate: Predicate::arbitrary_from(rng, table),
        }
    }
}

impl ArbitraryFrom<&SimulatorEnv> for Drop {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let table = pick(&env.tables, rng);
        Self {
            table: table.name.clone(),
        }
    }
}

impl ArbitraryFrom<(&SimulatorEnv, &Remaining)> for Query {
    fn arbitrary_from<R: Rng>(rng: &mut R, (env, remaining): (&SimulatorEnv, &Remaining)) -> Self {
        frequency(
            vec![
                (
                    remaining.create,
                    Box::new(|rng| Self::Create(Create::arbitrary(rng))),
                ),
                (
                    remaining.read,
                    Box::new(|rng| Self::Select(Select::arbitrary_from(rng, env))),
                ),
                (
                    remaining.write,
                    Box::new(|rng| Self::Insert(Insert::arbitrary_from(rng, env))),
                ),
                (
                    remaining.update,
                    Box::new(|rng| Self::Update(Update::arbitrary_from(rng, env))),
                ),
                (
                    f64::min(remaining.write, remaining.delete),
                    Box::new(|rng| Self::Delete(Delete::arbitrary_from(rng, env))),
                ),
            ],
            rng,
        )
    }
}

fn pick_unique<T: ToOwned + PartialEq>(
    items: &[T],
    count: usize,
    rng: &mut impl rand::Rng,
) -> Vec<T::Owned>
where
    <T as ToOwned>::Owned: PartialEq,
{
    let mut picked: Vec<T::Owned> = Vec::new();
    while picked.len() < count {
        let item = pick(items, rng);
        if !picked.contains(&item.to_owned()) {
            picked.push(item.to_owned());
        }
    }
    picked
}

impl ArbitraryFrom<&SimulatorEnv> for Update {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let table = pick(&env.tables, rng);
        let num_cols = rng.gen_range(1..=table.columns.len());
        let columns = pick_unique(&table.columns, num_cols, rng);
        let set_values: Vec<(String, SimValue)> = columns
            .iter()
            .map(|column| {
                (
                    column.name.clone(),
                    SimValue::arbitrary_from(rng, &column.column_type),
                )
            })
            .collect();
        Update {
            table: table.name.clone(),
            set_values,
            predicate: Predicate::arbitrary_from(rng, table),
        }
    }
}
