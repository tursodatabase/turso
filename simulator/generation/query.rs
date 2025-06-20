use std::collections::HashSet;

use crate::generation::{Arbitrary, ArbitraryFrom};
use crate::model::query::predicate::Predicate;
use crate::model::query::select::{Distinctness, ResultColumn};
use crate::model::query::update::Update;
use crate::model::query::{Create, Delete, Drop, Insert, Query, Select};
use crate::model::table::{SimValue, Table};
use crate::runner::env::SimulatorEnv;
use rand::Rng;

use super::property::Remaining;
use super::{backtrack, frequency, pick};

impl Arbitrary for Create {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        Create {
            table: Table::arbitrary(rng),
        }
    }
}

impl ArbitraryFrom<&SimulatorEnv> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let table = pick(&env.tables, rng);
        Self {
            table: table.name.clone(),
            result_columns: vec![ResultColumn::Star],
            predicate: Predicate::arbitrary_from(rng, table),
            limit: Some(rng.gen_range(0..=1000)),
            distinct: Distinctness::All,
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
            let select = Select {
                table: select_table.name.clone(),
                result_columns: vec![ResultColumn::Star],
                predicate,
                limit: None,
                distinct: Distinctness::All,
            };
            let table = pick(&env.tables, rng);
            Some(Insert::Select {
                table: table.name.clone(),
                select: Box::new(select),
            })
        };

        // Backtrack here cannot return None
        backtrack(
            vec![
                (1, Box::new(gen_values)),
                // todo: test and enable this once `INSERT INTO <table> SELECT * FROM <table>` is supported
                // (1, Box::new(|rng| gen_select(rng))),
            ],
            rng,
        )
        .unwrap()
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
        let mut seen = HashSet::new();
        let num_cols = rng.gen_range(1..=table.columns.len());
        let set_values: Vec<(String, SimValue)> = (0..num_cols)
            .map(|_| {
                let column = loop {
                    let column = pick(&table.columns, rng);
                    if seen.contains(&column.name) {
                        continue;
                    }
                    break column;
                };
                seen.insert(column.name.clone());
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
