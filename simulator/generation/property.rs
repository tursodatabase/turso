use serde::{Deserialize, Serialize};
use turso_core::LimboError;
use turso_sqlite3_parser::ast;

use crate::{
    model::{
        query::{
            predicate::Predicate,
            select::{Distinctness, ResultColumn},
            Create, Delete, Drop, Insert, Query, Select,
        },
        table::SimValue,
    },
    runner::env::LimboSimulatorEnv,
};

use super::{
    frequency, pick, pick_index,
    plan::{Assertion, Interaction, InteractionStats, ResultSet},
    ArbitraryFrom,
};

/// Properties are representations of executable specifications
/// about the database behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Property {
    /// Insert-Select is a property in which the inserted row
    /// must be in the resulting rows of a select query that has a
    /// where clause that matches the inserted row.
    /// The execution of the property is as follows
    ///     INSERT INTO <t> VALUES (...)
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate>
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - The inserted row will not be deleted.
    /// - The inserted row will not be updated.
    /// - The table `t` will not be renamed, dropped, or altered.
    InsertValuesSelect {
        /// The insert query
        insert: Insert,
        /// Selected row index
        row_index: usize,
        /// Additional interactions in the middle of the property
        queries: Vec<Query>,
        /// The select query
        select: Select,
    },
    /// Double Create Failure is a property in which creating
    /// the same table twice leads to an error.
    /// The execution of the property is as follows
    ///     CREATE TABLE <t> (...)
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     CREATE TABLE <t> (...) -> Error
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - Table `t` will not be renamed or dropped.
    DoubleCreateFailure {
        /// The create query
        create: Create,
        /// Additional interactions in the middle of the property
        queries: Vec<Query>,
    },
    /// Select Limit is a property in which the select query
    /// has a limit clause that is respected by the query.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t> WHERE <predicate> LIMIT <n>
    /// This property is a single-interaction property.
    /// The interaction has the following constraints;
    /// - The select query will respect the limit clause.
    SelectLimit {
        /// The select query
        select: Select,
    },
    /// Delete-Select is a property in which the deleted row
    /// must not be in the resulting rows of a select query that has a
    /// where clause that matches the deleted row. In practice, `p1` of
    /// the delete query will be used as the predicate for the select query,
    /// hence the select should return NO ROWS.
    /// The execution of the property is as follows
    ///     DELETE FROM <t> WHERE <predicate>
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate>
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - A row that holds for the predicate will not be inserted.
    /// - The table `t` will not be renamed, dropped, or altered.
    DeleteSelect {
        table: String,
        predicate: Predicate,
        queries: Vec<Query>,
    },
    /// Drop-Select is a property in which selecting from a dropped table
    /// should result in an error.
    /// The execution of the property is as follows
    ///     DROP TABLE <t>
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate> -> Error
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - The table `t` will not be created, no table will be renamed to `t`.
    DropSelect {
        table: String,
        queries: Vec<Query>,
        select: Select,
    },
    /// Select-Select-Optimizer is a property in which we test the optimizer by
    /// running two equivalent select queries, one with `SELECT <predicate> from <t>`
    /// and the other with `SELECT * from <t> WHERE <predicate>`. As highlighted by
    /// Rigger et al. in Non-Optimizing Reference Engine Construction(NoREC), SQLite
    /// tends to optimize `where` statements while keeping the result column expressions
    /// unoptimized. This property is used to test the optimizer. The property is successful
    /// if the two queries return the same number of rows.
    SelectSelectOptimizer {
        table: String,
        predicate: Predicate,
    },
    /// FsyncNoWait is a property which tests if we do not loose any data after not waiting for fsync.
    ///
    /// # Interactions
    /// - Executes the `query` without waiting for fsync
    /// - Drop all connections and Reopen the database
    /// - Execute the `query` again
    /// - Query tables to assert that the values were inserted
    ///
    FsyncNoWait {
        query: Query,
        tables: Vec<String>,
    },
    FaultyQuery {
        query: Query,
        tables: Vec<String>,
    },
}

impl Property {
    pub(crate) fn name(&self) -> &str {
        match self {
            Property::InsertValuesSelect { .. } => "Insert-Values-Select",
            Property::DoubleCreateFailure { .. } => "Double-Create-Failure",
            Property::SelectLimit { .. } => "Select-Limit",
            Property::DeleteSelect { .. } => "Delete-Select",
            Property::DropSelect { .. } => "Drop-Select",
            Property::SelectSelectOptimizer { .. } => "Select-Select-Optimizer",
            Property::FsyncNoWait { .. } => "FsyncNoWait",
            Property::FaultyQuery { .. } => "FaultyQuery",
        }
    }
    /// interactions construct a list of interactions, which is an executable representation of the property.
    /// the requirement of property -> vec<interaction> conversion emerges from the need to serialize the property,
    /// and `interaction` cannot be serialized directly.
    pub(crate) fn interactions(&self) -> Vec<Interaction> {
        match self {
            Property::InsertValuesSelect {
                insert,
                row_index,
                queries,
                select,
            } => {
                let (table, values) = if let Insert::Values { table, values } = insert {
                    (table, values)
                } else {
                    unreachable!(
                        "insert query should be Insert::Values for Insert-Values-Select property"
                    )
                };
                // Check that the insert query has at least 1 value
                assert!(
                    !values.is_empty(),
                    "insert query should have at least 1 value"
                );

                // Pick a random row within the insert values
                let row = values[*row_index].clone();

                // Assume that the table exists
                let assumption = Interaction::Assumption(Assertion {
                    message: format!("table {} exists", insert.table()),
                    func: Box::new({
                        let table_name = table.clone();
                        move |_: &Vec<ResultSet>, env: &LimboSimulatorEnv| {
                            Ok(env.tables.iter().any(|t| t.name == table_name))
                        }
                    }),
                });

                let assertion = Interaction::Assertion(Assertion {
                    message: format!(
                        "row [{:?}] not found in table {}",
                        row.iter().map(|v| v.to_string()).collect::<Vec<String>>(),
                        insert.table(),
                    ),
                    func: Box::new(move |stack: &Vec<ResultSet>, _: &LimboSimulatorEnv| {
                        let rows = stack.last().unwrap();
                        match rows {
                            Ok(rows) => Ok(rows.iter().any(|r| r == &row)),
                            Err(err) => Err(LimboError::InternalError(err.to_string())),
                        }
                    }),
                });

                let mut interactions = Vec::new();
                interactions.push(assumption);
                interactions.push(Interaction::Query(Query::Insert(insert.clone())));
                interactions.extend(queries.clone().into_iter().map(Interaction::Query));
                interactions.push(Interaction::Query(Query::Select(select.clone())));
                interactions.push(assertion);

                interactions
            }
            Property::DoubleCreateFailure { create, queries } => {
                let table_name = create.table.name.clone();

                let assumption = Interaction::Assumption(Assertion {
                    message: "Double-Create-Failure should not be called on an existing table"
                        .to_string(),
                    func: Box::new(move |_: &Vec<ResultSet>, env: &LimboSimulatorEnv| {
                        Ok(!env.tables.iter().any(|t| t.name == table_name))
                    }),
                });

                let cq1 = Interaction::Query(Query::Create(create.clone()));
                let cq2 = Interaction::Query(Query::Create(create.clone()));

                let table_name = create.table.name.clone();

                let assertion = Interaction::Assertion(Assertion {
                    message:
                        "creating two tables with the name should result in a failure for the second query"
                            .to_string(),
                    func: Box::new(move |stack: &Vec<ResultSet>, _: &LimboSimulatorEnv| {
                        let last = stack.last().unwrap();
                        match last {
                            Ok(_) => Ok(false),
                            Err(e) => Ok(e.to_string().to_lowercase().contains(&format!("table {table_name} already exists"))),
                        }
                    }),
                });

                let mut interactions = Vec::new();
                interactions.push(assumption);
                interactions.push(cq1);
                interactions.extend(queries.clone().into_iter().map(Interaction::Query));
                interactions.push(cq2);
                interactions.push(assertion);

                interactions
            }
            Property::SelectLimit { select } => {
                let table_name = select.table.clone();

                let assumption = Interaction::Assumption(Assertion {
                    message: format!("table {} exists", table_name),
                    func: Box::new({
                        let table_name = table_name.clone();
                        move |_: &Vec<ResultSet>, env: &LimboSimulatorEnv| {
                            Ok(env.tables.iter().any(|t| t.name == table_name))
                        }
                    }),
                });

                let limit = select
                    .limit
                    .expect("Property::SelectLimit without a LIMIT clause");

                let assertion = Interaction::Assertion(Assertion {
                    message: "select query should respect the limit clause".to_string(),
                    func: Box::new(move |stack: &Vec<ResultSet>, _: &LimboSimulatorEnv| {
                        let last = stack.last().unwrap();
                        match last {
                            Ok(rows) => Ok(limit >= rows.len()),
                            Err(_) => Ok(true),
                        }
                    }),
                });

                vec![
                    assumption,
                    Interaction::Query(Query::Select(select.clone())),
                    assertion,
                ]
            }
            Property::DeleteSelect {
                table,
                predicate,
                queries,
            } => {
                let assumption = Interaction::Assumption(Assertion {
                    message: format!("table {} exists", table),
                    func: Box::new({
                        let table = table.clone();
                        move |_: &Vec<ResultSet>, env: &LimboSimulatorEnv| {
                            Ok(env.tables.iter().any(|t| t.name == table))
                        }
                    }),
                });

                let delete = Interaction::Query(Query::Delete(Delete {
                    table: table.clone(),
                    predicate: predicate.clone(),
                }));

                let select = Interaction::Query(Query::Select(Select {
                    table: table.clone(),
                    result_columns: vec![ResultColumn::Star],
                    predicate: predicate.clone(),
                    limit: None,
                    distinct: Distinctness::All,
                }));

                let assertion = Interaction::Assertion(Assertion {
                    message: format!("`{}` should return no values for table `{}`", select, table,),
                    func: Box::new(move |stack: &Vec<ResultSet>, _: &LimboSimulatorEnv| {
                        let rows = stack.last().unwrap();
                        match rows {
                            Ok(rows) => Ok(rows.is_empty()),
                            Err(err) => Err(LimboError::InternalError(err.to_string())),
                        }
                    }),
                });

                let mut interactions = Vec::new();
                interactions.push(assumption);
                interactions.push(delete);
                interactions.extend(queries.clone().into_iter().map(Interaction::Query));
                interactions.push(select);
                interactions.push(assertion);

                interactions
            }
            Property::DropSelect {
                table,
                queries,
                select,
            } => {
                let assumption = Interaction::Assumption(Assertion {
                    message: format!("table {} exists", table),
                    func: Box::new({
                        let table = table.clone();
                        move |_: &Vec<ResultSet>, env: &LimboSimulatorEnv| {
                            Ok(env.tables.iter().any(|t| t.name == table))
                        }
                    }),
                });

                let table_name = table.clone();

                let assertion = Interaction::Assertion(Assertion {
                    message: format!(
                        "select query should result in an error for table '{}'",
                        table
                    ),
                    func: Box::new(move |stack: &Vec<ResultSet>, _: &LimboSimulatorEnv| {
                        let last = stack.last().unwrap();
                        match last {
                            Ok(_) => Ok(false),
                            Err(e) => Ok(e
                                .to_string()
                                .contains(&format!("Table {table_name} does not exist"))),
                        }
                    }),
                });

                let drop = Interaction::Query(Query::Drop(Drop {
                    table: table.clone(),
                }));

                let select = Interaction::Query(Query::Select(select.clone()));

                let mut interactions = Vec::new();

                interactions.push(assumption);
                interactions.push(drop);
                interactions.extend(queries.clone().into_iter().map(Interaction::Query));
                interactions.push(select);
                interactions.push(assertion);

                interactions
            }
            Property::SelectSelectOptimizer { table, predicate } => {
                let assumption = Interaction::Assumption(Assertion {
                    message: format!("table {} exists", table),
                    func: Box::new({
                        let table = table.clone();
                        move |_: &Vec<ResultSet>, env: &LimboSimulatorEnv| {
                            Ok(env.tables.iter().any(|t| t.name == table))
                        }
                    }),
                });
                let select1 = Interaction::Query(Query::Select(Select {
                    table: table.clone(),
                    result_columns: vec![ResultColumn::Expr(predicate.clone())],
                    predicate: Predicate::true_(),
                    limit: None,
                    distinct: Distinctness::All,
                }));

                let select2_query = Query::Select(Select {
                    table: table.clone(),
                    result_columns: vec![ResultColumn::Star],
                    predicate: predicate.clone(),
                    limit: None,
                    distinct: Distinctness::All,
                });
                let select2 = Interaction::Query(select2_query);

                let assertion = Interaction::Assertion(Assertion {
                    message: "select queries should return the same amount of results".to_string(),
                    func: Box::new(move |stack: &Vec<ResultSet>, _: &LimboSimulatorEnv| {
                        let select_star = stack.last().unwrap();
                        let select_predicate = stack.get(stack.len() - 2).unwrap();
                        match (select_predicate, select_star) {
                            (Ok(rows1), Ok(rows2)) => {
                                // If rows1 results have more than 1 column, there is a problem
                                if rows1.iter().any(|vs| vs.len() > 1) {
                                    return Err(LimboError::InternalError(
                                        "Select query without the star should return only one column".to_string(),
                                    ));
                                }
                                // Count the 1s in the select query without the star
                                let rows1_count = rows1
                                    .iter()
                                    .filter(|vs| {
                                        let v = vs.first().unwrap();
                                        v.as_bool()
                                    })
                                    .count();
                                Ok(rows1_count == rows2.len())
                            }
                            _ => Ok(false),
                        }
                    }),
                });

                vec![assumption, select1, select2, assertion]
            }
            Property::FsyncNoWait { query, tables } => {
                let checks = assert_all_table_values(tables);
                Vec::from_iter(
                    std::iter::once(Interaction::FsyncQuery(query.clone())).chain(checks),
                )
            }
            Property::FaultyQuery { query, tables } => {
                let checks = assert_all_table_values(tables);
                let first = std::iter::once(Interaction::FaultyQuery(query.clone()));
                Vec::from_iter(first.chain(checks))
            }
        }
    }
}

fn assert_all_table_values(tables: &[String]) -> impl Iterator<Item = Interaction> + use<'_> {
    let checks = tables.iter().flat_map(|table| {
        let select = Interaction::Query(Query::Select(Select {
            table: table.clone(),
            result_columns: vec![ResultColumn::Star],
            predicate: Predicate::true_(),
            limit: None,
            distinct: Distinctness::All,
        }));
        let assertion = Interaction::Assertion(Assertion {
            message: format!(
                "table {} should contain all of its values after the wal reopened",
                table
            ),
            func: Box::new({
                let table = table.clone();
                move |stack: &Vec<ResultSet>, env: &LimboSimulatorEnv| {
                    let table = env.tables.iter().find(|t| t.name == table).ok_or_else(|| {
                        LimboError::InternalError(format!("table {} should exist", table))
                    })?;
                    let last = stack.last().unwrap();
                    match last {
                        Ok(vals) => Ok(*vals == table.rows),
                        Err(err) => Err(LimboError::InternalError(format!("{}", err))),
                    }
                }
            }),
        });
        [select, assertion].into_iter()
    });
    checks
}

#[derive(Debug)]
pub(crate) struct Remaining {
    pub(crate) read: f64,
    pub(crate) write: f64,
    pub(crate) create: f64,
    pub(crate) create_index: f64,
    pub(crate) delete: f64,
    pub(crate) update: f64,
    pub(crate) drop: f64,
}

pub(crate) fn remaining(env: &LimboSimulatorEnv, stats: &InteractionStats) -> Remaining {
    let remaining_read = ((env.opts.max_interactions as f64 * env.opts.read_percent / 100.0)
        - (stats.read_count as f64))
        .max(0.0);
    let remaining_write = ((env.opts.max_interactions as f64 * env.opts.write_percent / 100.0)
        - (stats.write_count as f64))
        .max(0.0);
    let remaining_create = ((env.opts.max_interactions as f64 * env.opts.create_percent / 100.0)
        - (stats.create_count as f64))
        .max(0.0);

    let remaining_create_index =
        ((env.opts.max_interactions as f64 * env.opts.create_index_percent / 100.0)
            - (stats.create_index_count as f64))
            .max(0.0);

    let remaining_delete = ((env.opts.max_interactions as f64 * env.opts.delete_percent / 100.0)
        - (stats.delete_count as f64))
        .max(0.0);
    let remaining_update = ((env.opts.max_interactions as f64 * env.opts.update_percent / 100.0)
        - (stats.update_count as f64))
        .max(0.0);
    let remaining_drop = ((env.opts.max_interactions as f64 * env.opts.drop_percent / 100.0)
        - (stats.drop_count as f64))
        .max(0.0);

    Remaining {
        read: remaining_read,
        write: remaining_write,
        create: remaining_create,
        create_index: remaining_create_index,
        delete: remaining_delete,
        drop: remaining_drop,
        update: remaining_update,
    }
}

fn property_insert_values_select<R: rand::Rng>(
    rng: &mut R,
    env: &LimboSimulatorEnv,
    remaining: &Remaining,
) -> Property {
    // Get a random table
    let table = pick(&env.tables, rng);
    // Generate rows to insert
    let rows = (0..rng.gen_range(1..=5))
        .map(|_| Vec::<SimValue>::arbitrary_from(rng, table))
        .collect::<Vec<_>>();

    // Pick a random row to select
    let row_index = pick_index(rows.len(), rng);
    let row = rows[row_index].clone();

    // Insert the rows
    let insert_query = Insert::Values {
        table: table.name.clone(),
        values: rows,
    };

    // Create random queries respecting the constraints
    let mut queries = Vec::new();
    // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
    // - [x] The inserted row will not be deleted.
    // - [ ] The inserted row will not be updated. (todo: add this constraint once UPDATE is implemented)
    // - [ ] The table `t` will not be renamed, dropped, or altered. (todo: add this constraint once ALTER or DROP is implemented)
    for _ in 0..rng.gen_range(0..3) {
        let query = Query::arbitrary_from(rng, (env, remaining));
        match &query {
            Query::Delete(Delete {
                table: t,
                predicate,
            }) => {
                // The inserted row will not be deleted.
                if t == &table.name && predicate.test(&row, table) {
                    continue;
                }
            }
            Query::Create(Create { table: t }) => {
                // There will be no errors in the middle interactions.
                // - Creating the same table is an error
                if t.name == table.name {
                    continue;
                }
            }
            _ => (),
        }
        queries.push(query);
    }

    // Select the row
    let select_query = Select {
        table: table.name.clone(),
        result_columns: vec![ResultColumn::Star],
        predicate: Predicate::arbitrary_from(rng, (table, &row)),
        limit: None,
        distinct: Distinctness::All,
    };

    Property::InsertValuesSelect {
        insert: insert_query,
        row_index,
        queries,
        select: select_query,
    }
}

fn property_select_limit<R: rand::Rng>(rng: &mut R, env: &LimboSimulatorEnv) -> Property {
    // Get a random table
    let table = pick(&env.tables, rng);
    // Select the table
    let select = Select {
        table: table.name.clone(),
        result_columns: vec![ResultColumn::Star],
        predicate: Predicate::arbitrary_from(rng, table),
        limit: Some(rng.gen_range(1..=5)),
        distinct: Distinctness::All,
    };
    Property::SelectLimit { select }
}

fn property_double_create_failure<R: rand::Rng>(
    rng: &mut R,
    env: &LimboSimulatorEnv,
    remaining: &Remaining,
) -> Property {
    // Get a random table
    let table = pick(&env.tables, rng);
    // Create the table
    let create_query = Create {
        table: table.clone(),
    };

    // Create random queries respecting the constraints
    let mut queries = Vec::new();
    // The interactions in the middle has the following constraints;
    // - [x] There will be no errors in the middle interactions.(best effort)
    // - [ ] Table `t` will not be renamed or dropped.(todo: add this constraint once ALTER or DROP is implemented)
    for _ in 0..rng.gen_range(0..3) {
        let query = Query::arbitrary_from(rng, (env, remaining));
        if let Query::Create(Create { table: t }) = &query {
            // There will be no errors in the middle interactions.
            // - Creating the same table is an error
            if t.name == table.name {
                continue;
            }
        }
        queries.push(query);
    }

    Property::DoubleCreateFailure {
        create: create_query,
        queries,
    }
}

fn property_delete_select<R: rand::Rng>(
    rng: &mut R,
    env: &LimboSimulatorEnv,
    remaining: &Remaining,
) -> Property {
    // Get a random table
    let table = pick(&env.tables, rng);
    // Generate a random predicate
    let predicate = Predicate::arbitrary_from(rng, table);

    // Create random queries respecting the constraints
    let mut queries = Vec::new();
    // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
    // - [x] A row that holds for the predicate will not be inserted.
    // - [ ] The table `t` will not be renamed, dropped, or altered. (todo: add this constraint once ALTER or DROP is implemented)
    for _ in 0..rng.gen_range(0..3) {
        let query = Query::arbitrary_from(rng, (env, remaining));
        match &query {
            Query::Insert(Insert::Values { table: t, values }) => {
                // A row that holds for the predicate will not be inserted.
                if t == &table.name && values.iter().any(|v| predicate.test(v, table)) {
                    continue;
                }
            }
            Query::Create(Create { table: t }) => {
                // There will be no errors in the middle interactions.
                // - Creating the same table is an error
                if t.name == table.name {
                    continue;
                }
            }
            _ => (),
        }
        queries.push(query);
    }

    Property::DeleteSelect {
        table: table.name.clone(),
        predicate,
        queries,
    }
}

fn property_drop_select<R: rand::Rng>(
    rng: &mut R,
    env: &LimboSimulatorEnv,
    remaining: &Remaining,
) -> Property {
    // Get a random table
    let table = pick(&env.tables, rng);

    // Create random queries respecting the constraints
    let mut queries = Vec::new();
    // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
    // - [-] The table `t` will not be created, no table will be renamed to `t`. (todo: update this constraint once ALTER is implemented)
    for _ in 0..rng.gen_range(0..3) {
        let query = Query::arbitrary_from(rng, (env, remaining));
        if let Query::Create(Create { table: t }) = &query {
            // - The table `t` will not be created
            if t.name == table.name {
                continue;
            }
        }
        queries.push(query);
    }

    let select = Select {
        table: table.name.clone(),
        result_columns: vec![ResultColumn::Star],
        predicate: Predicate::arbitrary_from(rng, table),
        limit: None,
        distinct: Distinctness::All,
    };

    Property::DropSelect {
        table: table.name.clone(),
        queries,
        select,
    }
}

fn property_select_select_optimizer<R: rand::Rng>(rng: &mut R, env: &LimboSimulatorEnv) -> Property {
    // Get a random table
    let table = pick(&env.tables, rng);
    // Generate a random predicate
    let predicate = Predicate::arbitrary_from(rng, table);
    // Transform into a Binary predicate to force values to be casted to a bool
    let expr = ast::Expr::Binary(
        Box::new(predicate.0),
        ast::Operator::And,
        Box::new(Predicate::true_().0),
    );

    Property::SelectSelectOptimizer {
        table: table.name.clone(),
        predicate: Predicate(expr),
    }
}

fn property_fsync_no_wait<R: rand::Rng>(
    rng: &mut R,
    env: &LimboSimulatorEnv,
    remaining: &Remaining,
) -> Property {
    Property::FsyncNoWait {
        query: Query::arbitrary_from(rng, (env, remaining)),
        tables: env.tables.iter().map(|t| t.name.clone()).collect(),
    }
}

fn property_faulty_query<R: rand::Rng>(
    rng: &mut R,
    env: &LimboSimulatorEnv,
    remaining: &Remaining,
) -> Property {
    Property::FaultyQuery {
        query: Query::arbitrary_from(rng, (env, remaining)),
        tables: env.tables.iter().map(|t| t.name.clone()).collect(),
    }
}

impl ArbitraryFrom<(&LimboSimulatorEnv, &InteractionStats)> for Property {
    fn arbitrary_from<R: rand::Rng>(
        rng: &mut R,
        (env, stats): (&LimboSimulatorEnv, &InteractionStats),
    ) -> Self {
        let remaining_ = remaining(env, stats);
        frequency(
            vec![
                (
                    if !env.opts.disable_insert_values_select {
                        f64::min(remaining_.read, remaining_.write)
                    } else {
                        0.0
                    },
                    Box::new(|rng: &mut R| property_insert_values_select(rng, env, &remaining_)),
                ),
                (
                    if !env.opts.disable_double_create_failure {
                        remaining_.create / 2.0
                    } else {
                        0.0
                    },
                    Box::new(|rng: &mut R| property_double_create_failure(rng, env, &remaining_)),
                ),
                (
                    if !env.opts.disable_select_limit {
                        remaining_.read
                    } else {
                        0.0
                    },
                    Box::new(|rng: &mut R| property_select_limit(rng, env)),
                ),
                (
                    if !env.opts.disable_delete_select {
                        f64::min(remaining_.read, remaining_.write).min(remaining_.delete)
                    } else {
                        0.0
                    },
                    Box::new(|rng: &mut R| property_delete_select(rng, env, &remaining_)),
                ),
                (
                    if !env.opts.disable_drop_select {
                        // remaining_.drop
                        0.0
                    } else {
                        0.0
                    },
                    Box::new(|rng: &mut R| property_drop_select(rng, env, &remaining_)),
                ),
                (
                    if !env.opts.disable_select_optimizer {
                        remaining_.read / 2.0
                    } else {
                        0.0
                    },
                    Box::new(|rng: &mut R| property_select_select_optimizer(rng, env)),
                ),
                (
                    if !env.opts.disable_fsync_no_wait {
                        50.0 // Freestyle number
                    } else {
                        0.0
                    },
                    Box::new(|rng: &mut R| property_fsync_no_wait(rng, env, &remaining_)),
                ),
                (
                    if !env.opts.disable_faulty_query {
                        20.0
                    } else {
                        0.0
                    },
                    Box::new(|rng: &mut R| property_faulty_query(rng, env, &remaining_)),
                ),
            ],
            rng,
        )
    }
}
