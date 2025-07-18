use crate::generation::{Arbitrary, ArbitraryFrom, ArbitrarySizedFrom, Shadow};
use crate::model::query::predicate::Predicate;
use crate::model::query::select::{
    CompoundOperator, CompoundSelect, Distinctness, FromClause, JoinTable, JoinType, JoinedTable,
    ResultColumn, SelectBody, SelectInner,
};
use crate::model::query::update::Update;
use crate::model::query::{Create, Delete, Drop, Insert, Query, Select};
use crate::model::table::{SimValue, Table};
use crate::SimulatorEnv;
use itertools::Itertools;
use rand::Rng;
use turso_sqlite3_parser::ast::Expr;

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
        let query_size = (rng.gen_range(0.0..=15.0_f32).log2().ceil() as usize).saturating_sub(2);

        let mut tables = tables.clone();
        let mut table = pick(&tables, rng).clone();

        tables.retain(|t| t.name != table.name);

        let name = table.name.clone();

        let joins: Vec<_> = (0..query_size)
            .filter_map(|_| {
                if tables.is_empty() {
                    return None;
                }
                let join_table = pick(&tables, rng).clone();
                tables.retain(|t| t.name != join_table.name);
                table = JoinTable {
                    tables: vec![table.clone(), join_table.clone()],
                    rows: table
                        .rows
                        .iter()
                        .cartesian_product(join_table.rows.iter())
                        .map(|(t_row, j_row)| {
                            let mut row = t_row.clone();
                            row.extend(j_row.clone());
                            row
                        })
                        .collect(),
                }
                .into_table();
                for row in &mut table.rows {
                    assert_eq!(
                        row.len(),
                        table.columns.len(),
                        "Row length does not match column length after join"
                    );
                }

                let predicate = Predicate::arbitrary_from(rng, &table);
                Some(JoinedTable {
                    table: join_table.name.clone(),
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
            .expect("Failed to shadow FromClause")
            .into_table();

        SelectInner {
            distinctness: if env.opts.experimental_indexes {
                Distinctness::arbitrary(rng)
            } else {
                Distinctness::All
            },
            columns: vec![ResultColumn::Star],
            from: Some(from),
            where_clause: Predicate::arbitrary_from(rng, &join_table),
        }
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
        let query_size = (rng.gen_range(0.0..=15.0_f32).log2().ceil() as usize).saturating_sub(2);

        let table = pick(&env.tables, rng);

        // Generate a number of selects based on the query size
        // If experimental indexes are enabled, we can have selects with compounds
        // Otherwise, we just have a single select with no compounds
        let num_selects = if env.opts.experimental_indexes {
            rng.gen_range(0..=query_size)
        } else {
            0
        };

        let first = SelectInner::arbitrary_from(rng, env);

        let rest: Vec<SelectInner> = (0..num_selects)
            .map(|_| {
                let mut select = first.clone();
                select.where_clause = Predicate::arbitrary_from(rng, table);
                select
            })
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

impl ArbitraryFrom<&SimulatorEnv> for Update {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let table = pick(&env.tables, rng);
        let num_cols = rng.gen_range(1..=table.columns.len());
        let set_values: Vec<(String, SimValue)> = (0..num_cols)
            .map(|_| {
                let column = pick(&table.columns, rng);
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
