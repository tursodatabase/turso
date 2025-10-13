//! FIXME: With the current API and generation logic in plan.rs,
//! for Properties that have intermediary queries we need to CLONE the current Context tables
//! to properly generate queries, as we need to shadow after each query generated to make sure we are generating
//! queries that are valid. This is specially valid with DROP and ALTER TABLE in the mix, because with outdated context
//! we can generate queries that reference tables that do not exist. This is not a correctness issue, but more of
//! an optimization issue that is good to point out for the future

use rand::distr::{Distribution, weighted::WeightedIndex};

use serde::{Deserialize, Serialize};
use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, pick, pick_index},
    model::{
        query::{
            Create, Delete, Drop, Insert, Select,
            predicate::Predicate,
            select::{
                CompoundOperator, CompoundSelect, FromClause, ResultColumn, SelectBody,
                SelectInner, SelectTable,
            },
            transaction::{Begin, Commit, Rollback},
            update::Update,
        },
        table::SimValue,
    },
};
use strum::IntoEnumIterator;
use turso_core::{LimboError, types};
use turso_parser::ast::{self, Distinctness};

use crate::{
    common::print_diff,
    generation::{
        Shadow as _, WeightedDistribution, plan::InteractionType, query::QueryDistribution,
    },
    model::{Query, QueryCapabilities, QueryDiscriminants},
    profiles::query::QueryProfile,
    runner::env::SimulatorEnv,
};

use super::plan::{Assertion, Interaction, InteractionStats, ResultSet};

/// Properties are representations of executable specifications
/// about the database behavior.
#[derive(Debug, Clone, Serialize, Deserialize, strum::EnumDiscriminants)]
#[strum_discriminants(derive(strum::EnumIter))]
pub enum Property {
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
        /// Interactive query information if any
        interactive: Option<InteractiveQueryInfo>,
    },
    /// ReadYourUpdatesBack is a property in which the updated rows
    /// must be in the resulting rows of a select query that has a
    /// where clause that matches the updated row.
    /// The execution of the property is as follows
    ///     UPDATE <t> SET <set_cols=set_vals> WHERE <predicate>
    ///     SELECT <set_cols> FROM <t> WHERE <predicate>
    /// These interactions are executed in immediate succession
    /// just to verify the property that our updates did what they
    /// were supposed to do.
    ReadYourUpdatesBack {
        update: Update,
        select: Select,
    },
    /// TableHasExpectedContent is a property in which the table
    /// must have the expected content, i.e. all the insertions and
    /// updates and deletions should have been persisted in the way
    /// we think they were.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t>
    ///     ASSERT <expected_content>
    TableHasExpectedContent {
        table: String,
    },
    /// AllTablesHaveExpectedContent is a property in which the table
    /// must have the expected content, i.e. all the insertions and
    /// updates and deletions should have been persisted in the way
    /// we think they were.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t>
    ///     ASSERT <expected_content>
    /// for each table in the simulator model
    AllTableHaveExpectedContent {
        tables: Vec<String>,
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
    /// Where-True-False-Null is a property that tests the boolean logic implementation
    /// in the database. It relies on the fact that `P == true || P == false || P == null` should return true,
    /// as SQLite uses a ternary logic system. This property is invented in "Finding Bugs in Database Systems via Query Partitioning"
    /// by Rigger et al. and it is canonically called Ternary Logic Partitioning (TLP).
    WhereTrueFalseNull {
        select: Select,
        predicate: Predicate,
    },
    /// UNION-ALL-Preserves-Cardinality is a property that tests the UNION ALL operator
    /// implementation in the database. It relies on the fact that `SELECT * FROM <t
    /// > WHERE <predicate> UNION ALL SELECT * FROM <t> WHERE <predicate>`
    /// should return the same number of rows as `SELECT <predicate> FROM <t> WHERE <predicate>`.
    /// > The property is succesfull when the UNION ALL of 2 select queries returns the same number of rows
    /// > as the sum of the two select queries.
    UNIONAllPreservesCardinality {
        select: Select,
        where_clause: Predicate,
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
    },
    FaultyQuery {
        query: Query,
    },
    /// InsertedSelectNested tests that when after a statement of the format `INSERT INTO <table> SELECT *
    /// FROM <table>`, the table contains exactly 2 rows for each row that it contained before the
    /// statement.
    ///
    /// The nesting_level controls the level of nesting of the SELECT clause. For example,
    /// nesting_level=0 generates the query above, nesting_level=1 generates this query:
    ///     INSERT INTO <table> SELECT * FROM (SELECT * FROM <table>)
    /// and nesting_level=1 generates this query:
    ///     INSERT INTO <table> SELECT * FROM (SELECT * FROM (SELECT * FROM <table>))
    InsertSelectNested {
        table: String,
        nesting_level: u8,
    },
    /// Property used to subsititute a property with its queries only
    Queries {
        queries: Vec<Query>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractiveQueryInfo {
    start_with_immediate: bool,
    end_with_commit: bool,
}

type PropertyQueryGenFunc<'a, R, G> =
    fn(&mut R, &G, &QueryDistribution, &Property) -> Option<Query>;

impl Property {
    pub(crate) fn name(&self) -> &str {
        match self {
            Property::InsertValuesSelect { .. } => "Insert-Values-Select",
            Property::ReadYourUpdatesBack { .. } => "Read-Your-Updates-Back",
            Property::TableHasExpectedContent { .. } => "Table-Has-Expected-Content",
            Property::AllTableHaveExpectedContent { .. } => "All-Tables-Have-Expected-Content",
            Property::DoubleCreateFailure { .. } => "Double-Create-Failure",
            Property::SelectLimit { .. } => "Select-Limit",
            Property::DeleteSelect { .. } => "Delete-Select",
            Property::DropSelect { .. } => "Drop-Select",
            Property::SelectSelectOptimizer { .. } => "Select-Select-Optimizer",
            Property::WhereTrueFalseNull { .. } => "Where-True-False-Null",
            Property::FsyncNoWait { .. } => "FsyncNoWait",
            Property::FaultyQuery { .. } => "FaultyQuery",
            Property::UNIONAllPreservesCardinality { .. } => "UNION-All-Preserves-Cardinality",
            Property::Queries { .. } => "Queries",
            Property::InsertSelectNested { .. } => "InsertSelectNested",
        }
    }

    /// Property Does some sort of fault injection
    pub fn check_tables(&self) -> bool {
        matches!(
            self,
            Property::FsyncNoWait { .. } | Property::FaultyQuery { .. }
        )
    }

    pub fn get_extensional_queries(&mut self) -> Option<&mut Vec<Query>> {
        match self {
            Property::InsertValuesSelect { queries, .. }
            | Property::DoubleCreateFailure { queries, .. }
            | Property::DeleteSelect { queries, .. }
            | Property::DropSelect { queries, .. }
            | Property::Queries { queries } => Some(queries),
            Property::FsyncNoWait { .. } | Property::FaultyQuery { .. } => None,
            Property::SelectLimit { .. }
            | Property::SelectSelectOptimizer { .. }
            | Property::WhereTrueFalseNull { .. }
            | Property::UNIONAllPreservesCardinality { .. }
            | Property::ReadYourUpdatesBack { .. }
            | Property::InsertSelectNested { .. }
            | Property::TableHasExpectedContent { .. }
            | Property::AllTableHaveExpectedContent { .. } => None,
        }
    }

    pub(super) fn get_extensional_query_gen_function<R, G>(&self) -> PropertyQueryGenFunc<R, G>
    where
        R: rand::Rng + ?Sized,
        G: GenerationContext,
    {
        match self {
            Property::InsertValuesSelect { .. } => {
                // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
                // - [x] The inserted row will not be deleted.
                // - [x] The inserted row will not be updated.
                // - [ ] The table `t` will not be renamed, dropped, or altered. (todo: add this constraint once ALTER or DROP is implemented)
                |rng: &mut R, ctx: &G, query_distr: &QueryDistribution, property: &Property| {
                    let Property::InsertValuesSelect {
                        insert, row_index, ..
                    } = property
                    else {
                        unreachable!();
                    };
                    let query = Query::arbitrary_from(rng, ctx, query_distr);
                    let table_name = insert.table();
                    let table = ctx
                        .tables()
                        .iter()
                        .find(|table| table.name == table_name)
                        .unwrap();

                    let rows = insert.rows();
                    let row = &rows[*row_index];

                    match &query {
                        Query::Delete(Delete {
                            table: t,
                            predicate,
                        }) if t == &table.name && predicate.test(row, table) => {
                            // The inserted row will not be deleted.
                            None
                        }
                        Query::Create(Create { table: t }) if t.name == table.name => {
                            // There will be no errors in the middle interactions.
                            // - Creating the same table is an error
                            None
                        }
                        Query::Update(Update {
                            table: t,
                            set_values: _,
                            predicate,
                        }) if t == &table.name && predicate.test(row, table) => {
                            // The inserted row will not be updated.
                            None
                        }
                        Query::Drop(Drop { table: t }) if *t == table.name => {
                            // Cannot drop the table we are inserting
                            None
                        }
                        _ => Some(query),
                    }
                }
            }
            Property::DoubleCreateFailure { .. } => {
                // The interactions in the middle has the following constraints;
                // - [x] There will be no errors in the middle interactions.(best effort)
                // - [ ] Table `t` will not be renamed or dropped.(todo: add this constraint once ALTER or DROP is implemented)
                |rng: &mut R, ctx: &G, query_distr: &QueryDistribution, property: &Property| {
                    let Property::DoubleCreateFailure { create, .. } = property else {
                        unreachable!()
                    };

                    let table_name = create.table.name.clone();
                    let table = ctx
                        .tables()
                        .iter()
                        .find(|table| table.name == table_name)
                        .unwrap();

                    let query = Query::arbitrary_from(rng, ctx, query_distr);
                    match &query {
                        Query::Create(Create { table: t }) if t.name == table.name => {
                            // There will be no errors in the middle interactions.
                            // - Creating the same table is an error
                            None
                        }
                        Query::Drop(Drop { table: t }) if *t == table.name => {
                            // Cannot Drop the created table
                            None
                        }
                        _ => Some(query),
                    }
                }
            }
            Property::DeleteSelect { .. } => {
                // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
                // - [x] A row that holds for the predicate will not be inserted.
                // - [ ] The table `t` will not be renamed, dropped, or altered. (todo: add this constraint once ALTER or DROP is implemented)

                |rng, ctx, query_distr, property| {
                    let Property::DeleteSelect {
                        table: table_name,
                        predicate,
                        ..
                    } = property
                    else {
                        unreachable!()
                    };

                    let table_name = table_name.clone();
                    let table = ctx
                        .tables()
                        .iter()
                        .find(|table| table.name == table_name)
                        .unwrap();
                    let query = Query::arbitrary_from(rng, ctx, query_distr);
                    match &query {
                        Query::Insert(Insert::Values { table: t, values })
                            if *t == table_name
                                && values.iter().any(|v| predicate.test(v, table)) =>
                        {
                            // A row that holds for the predicate will not be inserted.
                            None
                        }
                        Query::Insert(Insert::Select {
                            table: t,
                            select: _,
                        }) if t == &table.name => {
                            // A row that holds for the predicate will not be inserted.
                            None
                        }
                        Query::Update(Update { table: t, .. }) if t == &table.name => {
                            // A row that holds for the predicate will not be updated.
                            None
                        }
                        Query::Create(Create { table: t }) if t.name == table.name => {
                            // There will be no errors in the middle interactions.
                            // - Creating the same table is an error
                            None
                        }
                        Query::Drop(Drop { table: t }) if *t == table.name => {
                            // Cannot Drop the same table
                            None
                        }
                        _ => Some(query),
                    }
                }
            }
            Property::DropSelect { .. } => {
                // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
                // - [-] The table `t` will not be created, no table will be renamed to `t`. (todo: update this constraint once ALTER is implemented)
                |rng, ctx, query_distr, property: &Property| {
                    let Property::DropSelect {
                        table: table_name, ..
                    } = property
                    else {
                        unreachable!()
                    };

                    let query = Query::arbitrary_from(rng, ctx, query_distr);
                    if let Query::Create(Create { table: t }) = &query
                        && t.name == *table_name
                    {
                        // - The table `t` will not be created
                        None
                    } else {
                        Some(query)
                    }
                }
            }
            Property::Queries { .. } => {
                unreachable!("No extensional querie generation for `Property::Queries`")
            }
            Property::FsyncNoWait { .. } | Property::FaultyQuery { .. } => {
                unreachable!("No extensional queries")
            }
            Property::SelectLimit { .. }
            | Property::SelectSelectOptimizer { .. }
            | Property::WhereTrueFalseNull { .. }
            | Property::UNIONAllPreservesCardinality { .. }
            | Property::ReadYourUpdatesBack { .. }
            | Property::TableHasExpectedContent { .. }
            | Property::InsertSelectNested { .. }
            | Property::AllTableHaveExpectedContent { .. } => {
                unreachable!("No extensional queries")
            }
        }
    }

    /// interactions construct a list of interactions, which is an executable representation of the property.
    /// the requirement of property -> vec<interaction> conversion emerges from the need to serialize the property,
    /// and `interaction` cannot be serialized directly.
    pub(crate) fn interactions(&self, connection_index: usize) -> Vec<Interaction> {
        match self {
            Property::AllTableHaveExpectedContent { tables } => {
                assert_all_table_values(tables, connection_index).collect()
            }
            Property::TableHasExpectedContent { table } => {
                let table = table.to_string();
                let table_name = table.clone();
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {} exists", table.clone()),
                    move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                        let conn_tables = env.get_conn_tables(connection_index);
                        if conn_tables.iter().any(|t| t.name == table_name) {
                            Ok(Ok(()))
                        } else {
                            Ok(Err(format!("table {table_name} does not exist")))
                        }
                    },
                ));

                let select_interaction = InteractionType::Query(Query::Select(Select::simple(
                    table.clone(),
                    Predicate::true_(),
                )));

                let assertion = InteractionType::Assertion(Assertion::new(
                    format!("table {} should have the expected content", table.clone()),
                    move |stack: &Vec<ResultSet>, env| {
                        let rows = stack.last().unwrap();
                        let Ok(rows) = rows else {
                            return Ok(Err(format!("expected rows but got error: {rows:?}")));
                        };
                        let conn_tables = env.get_conn_tables(connection_index);
                        let sim_table = conn_tables
                            .iter()
                            .find(|t| t.name == table)
                            .expect("table should be in enviroment");
                        if rows.len() != sim_table.rows.len() {
                            print_diff(&sim_table.rows, rows, "simulator", "database");
                            return Ok(Err(format!(
                                "expected {} rows but got {} for table {}",
                                sim_table.rows.len(),
                                rows.len(),
                                table.clone()
                            )));
                        }
                        for expected_row in sim_table.rows.iter() {
                            if !rows.contains(expected_row) {
                                print_diff(&sim_table.rows, rows, "simulator", "database");
                                return Ok(Err(format!(
                                    "expected row {:?} not found in table {}",
                                    expected_row,
                                    table.clone()
                                )));
                            }
                        }
                        Ok(Ok(()))
                    },
                ));

                vec![
                    Interaction::new(connection_index, assumption),
                    Interaction::new(connection_index, select_interaction),
                    Interaction::new(connection_index, assertion),
                ]
            }
            Property::ReadYourUpdatesBack { update, select } => {
                let table = update.table().to_string();
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {} exists", table.clone()),
                    move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                        let conn_tables = env.get_conn_tables(connection_index);
                        if conn_tables.iter().any(|t| t.name == table.clone()) {
                            Ok(Ok(()))
                        } else {
                            Ok(Err(format!("table {} does not exist", table.clone())))
                        }
                    },
                ));

                let update_interaction = InteractionType::Query(Query::Update(update.clone()));
                let select_interaction = InteractionType::Query(Query::Select(select.clone()));

                let update = update.clone();

                let table = update.table().to_string();

                let assertion = InteractionType::Assertion(Assertion::new(
                    format!(
                        "updated rows should be found and have the updated values for table {}",
                        table.clone()
                    ),
                    move |stack: &Vec<ResultSet>, _| {
                        let rows = stack.last().unwrap();
                        match rows {
                            Ok(rows) => {
                                for row in rows {
                                    for (i, (col, val)) in update.set_values.iter().enumerate() {
                                        if &row[i] != val {
                                            let update_rows = update
                                                .set_values
                                                .iter()
                                                .map(|(_, val)| val.clone())
                                                .collect::<Vec<_>>();
                                            print_diff(
                                                &[row.to_vec()],
                                                &[update_rows],
                                                "database",
                                                "update-clause",
                                            );
                                            return Ok(Err(format!(
                                                "updated row {} has incorrect value for column {col}: expected {val}, got {}",
                                                i, row[i]
                                            )));
                                        }
                                    }
                                }
                                Ok(Ok(()))
                            }
                            Err(err) => Err(LimboError::InternalError(err.to_string())),
                        }
                    },
                ));

                vec![
                    Interaction::new(connection_index, assumption),
                    Interaction::new(connection_index, update_interaction),
                    Interaction::new(connection_index, select_interaction),
                    Interaction::new(connection_index, assertion),
                ]
            }
            Property::InsertValuesSelect {
                insert,
                row_index,
                queries,
                select,
                interactive,
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
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {} exists", insert.table()),
                    {
                        let table_name = table.clone();
                        move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                            let conn_tables = env.get_conn_tables(connection_index);
                            if conn_tables.iter().any(|t| t.name == table_name) {
                                Ok(Ok(()))
                            } else {
                                Ok(Err(format!("table {table_name} does not exist")))
                            }
                        }
                    },
                ));

                let assertion = InteractionType::Assertion(Assertion::new(
                    format!(
                        "row [{:?}] should be found in table {}, interactive={} commit={}, rollback={}",
                        row.iter().map(|v| v.to_string()).collect::<Vec<String>>(),
                        insert.table(),
                        interactive.is_some(),
                        interactive
                            .as_ref()
                            .map(|i| i.end_with_commit)
                            .unwrap_or(false),
                        interactive
                            .as_ref()
                            .map(|i| !i.end_with_commit)
                            .unwrap_or(false),
                    ),
                    move |stack: &Vec<ResultSet>, _| {
                        let rows = stack.last().unwrap();
                        match rows {
                            Ok(rows) => {
                                let found = rows.iter().any(|r| r == &row);
                                if found {
                                    Ok(Ok(()))
                                } else {
                                    Ok(Err(format!(
                                        "row [{:?}] not found in table",
                                        row.iter().map(|v| v.to_string()).collect::<Vec<String>>()
                                    )))
                                }
                            }
                            Err(err) => Err(LimboError::InternalError(err.to_string())),
                        }
                    },
                ));

                let mut interactions = Vec::new();
                interactions.push(Interaction::new(connection_index, assumption));
                interactions.push(Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Insert(insert.clone())),
                ));
                interactions.extend(
                    queries
                        .clone()
                        .into_iter()
                        .map(|q| Interaction::new(connection_index, InteractionType::Query(q))),
                );
                interactions.push(Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Select(select.clone())),
                ));
                interactions.push(Interaction::new(connection_index, assertion));

                interactions
            }
            Property::DoubleCreateFailure { create, queries } => {
                let table_name = create.table.name.clone();

                let assumption = InteractionType::Assumption(Assertion::new(
                    "Double-Create-Failure should not be called on an existing table".to_string(),
                    move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                        let conn_tables = env.get_conn_tables(connection_index);
                        if !conn_tables.iter().any(|t| t.name == table_name) {
                            Ok(Ok(()))
                        } else {
                            Ok(Err(format!("table {table_name} already exists")))
                        }
                    },
                ));

                let cq1 = InteractionType::Query(Query::Create(create.clone()));
                let cq2 = InteractionType::Query(Query::Create(create.clone()));

                let table_name = create.table.name.clone();

                let assertion = InteractionType::Assertion(Assertion::new("creating two tables with the name should result in a failure for the second query"
                                    .to_string(), move |stack: &Vec<ResultSet>, env| {
                                let last = stack.last().unwrap();
                                match last {
                                    Ok(success) => Ok(Err(format!("expected table creation to fail but it succeeded: {success:?}"))),
                                    Err(e) => {
                                        if e.to_string().to_lowercase().contains(&format!("table {table_name} already exists")) {
                                             // On error we rollback the transaction if there is any active here
                                            env.rollback_conn(connection_index);
                                            Ok(Ok(()))
                                        } else {
                                            Ok(Err(format!("expected table already exists error, got: {e}")))
                                        }
                                    }
                                }
                            }) );

                let mut interactions = Vec::new();
                interactions.push(Interaction::new(connection_index, assumption));
                interactions.push(Interaction::new(connection_index, cq1));
                interactions.extend(
                    queries
                        .clone()
                        .into_iter()
                        .map(|q| Interaction::new(connection_index, InteractionType::Query(q))),
                );
                interactions.push(Interaction::new_ignore_error(connection_index, cq2));
                interactions.push(Interaction::new(connection_index, assertion));

                interactions
            }
            Property::SelectLimit { select } => {
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!(
                        "table ({}) exists",
                        select
                            .dependencies()
                            .into_iter()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    {
                        let table_name = select.dependencies();
                        move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                            let conn_tables = env.get_conn_tables(connection_index);
                            if table_name
                                .iter()
                                .all(|table| conn_tables.iter().any(|t| t.name == *table))
                            {
                                Ok(Ok(()))
                            } else {
                                let missing_tables = table_name
                                    .iter()
                                    .filter(|t| !conn_tables.iter().any(|t2| t2.name == **t))
                                    .collect::<Vec<&String>>();
                                Ok(Err(format!("missing tables: {missing_tables:?}")))
                            }
                        }
                    },
                ));

                let limit = select
                    .limit
                    .expect("Property::SelectLimit without a LIMIT clause");

                let assertion = InteractionType::Assertion(Assertion::new(
                    "select query should respect the limit clause".to_string(),
                    move |stack: &Vec<ResultSet>, _| {
                        let last = stack.last().unwrap();
                        match last {
                            Ok(rows) => {
                                if limit >= rows.len() {
                                    Ok(Ok(()))
                                } else {
                                    Ok(Err(format!(
                                        "limit {} violated: got {} rows",
                                        limit,
                                        rows.len()
                                    )))
                                }
                            }
                            Err(_) => Ok(Ok(())),
                        }
                    },
                ));

                vec![
                    Interaction::new(connection_index, assumption),
                    Interaction::new(
                        connection_index,
                        InteractionType::Query(Query::Select(select.clone())),
                    ),
                    Interaction::new(connection_index, assertion),
                ]
            }
            Property::DeleteSelect {
                table,
                predicate,
                queries,
            } => {
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {table} exists"),
                    {
                        let table = table.clone();
                        move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                            let conn_tables = env.get_conn_tables(connection_index);
                            if conn_tables.iter().any(|t| t.name == table) {
                                Ok(Ok(()))
                            } else {
                                {
                                    let available_tables: Vec<String> =
                                        conn_tables.iter().map(|t| t.name.clone()).collect();
                                    Ok(Err(format!(
                                        "table \'{table}\' not found. Available tables: {available_tables:?}"
                                    )))
                                }
                            }
                        }
                    },
                ));

                let delete = InteractionType::Query(Query::Delete(Delete {
                    table: table.clone(),
                    predicate: predicate.clone(),
                }));

                let select = InteractionType::Query(Query::Select(Select::simple(
                    table.clone(),
                    predicate.clone(),
                )));

                let assertion = InteractionType::Assertion(Assertion::new(
                    format!("`{select}` should return no values for table `{table}`",),
                    move |stack: &Vec<ResultSet>, _| {
                        let rows = stack.last().unwrap();
                        match rows {
                            Ok(rows) => {
                                if rows.is_empty() {
                                    Ok(Ok(()))
                                } else {
                                    Ok(Err(format!(
                                        "expected no rows but got {} rows: {:?}",
                                        rows.len(),
                                        rows.iter()
                                            .map(|r| print_row(r))
                                            .collect::<Vec<String>>()
                                            .join(", ")
                                    )))
                                }
                            }
                            Err(err) => Err(LimboError::InternalError(err.to_string())),
                        }
                    },
                ));

                let mut interactions = Vec::new();
                interactions.push(Interaction::new(connection_index, assumption));
                interactions.push(Interaction::new(connection_index, delete));
                interactions.extend(
                    queries
                        .clone()
                        .into_iter()
                        .map(|q| Interaction::new(connection_index, InteractionType::Query(q))),
                );
                interactions.push(Interaction::new(connection_index, select));
                interactions.push(Interaction::new(connection_index, assertion));

                interactions
            }
            Property::DropSelect {
                table,
                queries,
                select,
            } => {
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {table} exists"),
                    {
                        let table = table.clone();
                        move |_, env: &mut SimulatorEnv| {
                            let conn_tables = env.get_conn_tables(connection_index);
                            if conn_tables.iter().any(|t| t.name == table) {
                                Ok(Ok(()))
                            } else {
                                {
                                    let available_tables: Vec<String> =
                                        conn_tables.iter().map(|t| t.name.clone()).collect();
                                    Ok(Err(format!(
                                        "table \'{table}\' not found. Available tables: {available_tables:?}"
                                    )))
                                }
                            }
                        }
                    },
                ));

                let table_name = table.clone();

                let assertion = InteractionType::Assertion(Assertion::new(
                    format!("select query should result in an error for table '{table}'"),
                    move |stack: &Vec<ResultSet>, _| {
                        let last = stack.last().unwrap();
                        match last {
                            Ok(success) => Ok(Err(format!(
                                "expected table creation to fail but it succeeded: {success:?}"
                            ))),
                            Err(e) => match e {
                                e if e
                                    .to_string()
                                    .contains(&format!("no such table: {table_name}")) =>
                                {
                                    Ok(Ok(()))
                                }
                                _ => Ok(Err(format!(
                                    "expected table does not exist error, got: {e}"
                                ))),
                            },
                        }
                    },
                ));

                let drop = InteractionType::Query(Query::Drop(Drop {
                    table: table.clone(),
                }));

                let select = InteractionType::Query(Query::Select(select.clone()));

                let mut interactions = Vec::new();

                interactions.push(Interaction::new(connection_index, assumption));
                interactions.push(Interaction::new(connection_index, drop));
                interactions.extend(
                    queries
                        .clone()
                        .into_iter()
                        .map(|q| Interaction::new(connection_index, InteractionType::Query(q))),
                );
                interactions.push(Interaction::new_ignore_error(connection_index, select));
                interactions.push(Interaction::new(connection_index, assertion));

                interactions
            }
            Property::SelectSelectOptimizer { table, predicate } => {
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {table} exists"),
                    {
                        let table = table.clone();
                        move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                            let conn_tables = env.get_conn_tables(connection_index);
                            if conn_tables.iter().any(|t| t.name == table) {
                                Ok(Ok(()))
                            } else {
                                {
                                    let available_tables: Vec<String> =
                                        conn_tables.iter().map(|t| t.name.clone()).collect();
                                    Ok(Err(format!(
                                        "table \'{table}\' not found. Available tables: {available_tables:?}"
                                    )))
                                }
                            }
                        }
                    },
                ));

                let select1 = InteractionType::Query(Query::Select(Select::single(
                    table.clone(),
                    vec![ResultColumn::Expr(predicate.clone())],
                    Predicate::true_(),
                    None,
                    Distinctness::All,
                )));

                let select2_query = Query::Select(Select::simple(table.clone(), predicate.clone()));

                let select2 = InteractionType::Query(select2_query);

                let assertion = InteractionType::Assertion(Assertion::new(
                    "select queries should return the same amount of results".to_string(),
                    move |stack: &Vec<ResultSet>, _| {
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
                                tracing::debug!(
                                    "select1 returned {} rows, select2 returned {} rows",
                                    rows1_count,
                                    rows2.len()
                                );
                                if rows1_count == rows2.len() {
                                    Ok(Ok(()))
                                } else {
                                    Ok(Err(format!(
                                        "row counts don't match: {} vs {}",
                                        rows1_count,
                                        rows2.len()
                                    )))
                                }
                            }
                            (Err(e1), Err(e2)) => {
                                tracing::debug!("Error in select1 AND select2: {}, {}", e1, e2);
                                Ok(Ok(()))
                            }
                            (Err(e), _) | (_, Err(e)) => {
                                tracing::error!("Error in select1 OR select2: {}", e);
                                Err(LimboError::InternalError(e.to_string()))
                            }
                        }
                    },
                ));

                vec![
                    Interaction::new(connection_index, assumption),
                    Interaction::new(connection_index, select1),
                    Interaction::new(connection_index, select2),
                    Interaction::new(connection_index, assertion),
                ]
            }
            Property::FsyncNoWait { query } => {
                vec![Interaction::new(
                    connection_index,
                    InteractionType::FsyncQuery(query.clone()),
                )]
            }
            Property::FaultyQuery { query } => {
                let query_clone = query.clone();
                // A fault may not occur as we first signal we want a fault injected,
                // then when IO is called the fault triggers. It may happen that a fault is injected
                // but no IO happens right after it
                let assert = Assertion::new(
                    "fault occured".to_string(),
                    move |stack, env: &mut SimulatorEnv| {
                        let last = stack.last().unwrap();
                        match last {
                            Ok(_) => {
                                let _ = query_clone
                                    .shadow(&mut env.get_conn_tables_mut(connection_index));
                                Ok(Ok(()))
                            }
                            Err(err) => {
                                // We cannot make any assumptions about the error content; all we are about is, if the statement errored,
                                // we don't shadow the results into the simulator env, i.e. we assume whatever the statement did was rolled back.
                                tracing::error!("Fault injection produced error: {err}");

                                // On error we rollback the transaction if there is any active here
                                env.rollback_conn(connection_index);
                                Ok(Ok(()))
                            }
                        }
                    },
                );
                [
                    InteractionType::FaultyQuery(query.clone()),
                    InteractionType::Assertion(assert),
                ]
                .into_iter()
                .map(|i| Interaction::new(connection_index, i))
                .collect()
            }
            Property::WhereTrueFalseNull { select, predicate } => {
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!(
                        "tables ({}) exists",
                        select
                            .dependencies()
                            .into_iter()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    {
                        let tables = select.dependencies();
                        move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                            let conn_tables = env.get_conn_tables(connection_index);
                            if tables
                                .iter()
                                .all(|table| conn_tables.iter().any(|t| t.name == *table))
                            {
                                Ok(Ok(()))
                            } else {
                                let missing_tables = tables
                                    .iter()
                                    .filter(|t| !conn_tables.iter().any(|t2| t2.name == **t))
                                    .collect::<Vec<&String>>();
                                Ok(Err(format!("missing tables: {missing_tables:?}")))
                            }
                        }
                    },
                ));

                let old_predicate = select.body.select.where_clause.clone();

                let p_true = Predicate::and(vec![old_predicate.clone(), predicate.clone()]);
                let p_false = Predicate::and(vec![
                    old_predicate.clone(),
                    Predicate::not(predicate.clone()),
                ]);
                let p_null = Predicate::and(vec![
                    old_predicate.clone(),
                    Predicate::is(predicate.clone(), Predicate::null()),
                ]);

                let select_tlp = Select {
                    body: SelectBody {
                        select: Box::new(SelectInner {
                            distinctness: select.body.select.distinctness,
                            columns: select.body.select.columns.clone(),
                            from: select.body.select.from.clone(),
                            where_clause: p_true,
                            order_by: None,
                        }),
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::UnionAll,
                                select: Box::new(SelectInner {
                                    distinctness: select.body.select.distinctness,
                                    columns: select.body.select.columns.clone(),
                                    from: select.body.select.from.clone(),
                                    where_clause: p_false,
                                    order_by: None,
                                }),
                            },
                            CompoundSelect {
                                operator: CompoundOperator::UnionAll,
                                select: Box::new(SelectInner {
                                    distinctness: select.body.select.distinctness,
                                    columns: select.body.select.columns.clone(),
                                    from: select.body.select.from.clone(),
                                    where_clause: p_null,
                                    order_by: None,
                                }),
                            },
                        ],
                    },
                    limit: None,
                };

                let select = InteractionType::Query(Query::Select(select.clone()));
                let select_tlp = InteractionType::Query(Query::Select(select_tlp));

                // select and select_tlp should return the same rows
                let assertion = InteractionType::Assertion(Assertion::new(
                    "select and select_tlp should return the same rows".to_string(),
                    move |stack: &Vec<ResultSet>, _: &mut SimulatorEnv| {
                        if stack.len() < 2 {
                            return Err(LimboError::InternalError(
                                "Not enough result sets on the stack".to_string(),
                            ));
                        }

                        let select_result_set = stack.get(stack.len() - 2).unwrap();
                        let select_tlp_result_set = stack.last().unwrap();

                        match (select_result_set, select_tlp_result_set) {
                            (Ok(select_rows), Ok(select_tlp_rows)) => {
                                if select_rows.len() != select_tlp_rows.len() {
                                    return Ok(Err(format!(
                                        "row count mismatch: select returned {} rows, select_tlp returned {} rows",
                                        select_rows.len(),
                                        select_tlp_rows.len()
                                    )));
                                }
                                // Check if any row in select_rows is not in select_tlp_rows
                                for row in select_rows.iter() {
                                    if !select_tlp_rows.iter().any(|r| r == row) {
                                        tracing::debug!(
                                            "select and select_tlp returned different rows, ({}) is in select but not in select_tlp",
                                            row.iter()
                                                .map(|v| v.to_string())
                                                .collect::<Vec<String>>()
                                                .join(", ")
                                        );
                                        return Ok(Err(format!(
                                            "row mismatch: row [{}] exists in select results but not in select_tlp results",
                                            print_row(row)
                                        )));
                                    }
                                }
                                // Check if any row in select_tlp_rows is not in select_rows
                                for row in select_tlp_rows.iter() {
                                    if !select_rows.iter().any(|r| r == row) {
                                        tracing::debug!(
                                            "select and select_tlp returned different rows, ({}) is in select_tlp but not in select",
                                            row.iter()
                                                .map(|v| v.to_string())
                                                .collect::<Vec<String>>()
                                                .join(", ")
                                        );

                                        return Ok(Err(format!(
                                            "row mismatch: row [{}] exists in select_tlp but not in select",
                                            print_row(row)
                                        )));
                                    }
                                }
                                // If we reach here, the rows are the same
                                tracing::trace!(
                                    "select and select_tlp returned the same rows: {:?}",
                                    select_rows
                                );

                                Ok(Ok(()))
                            }
                            (Err(e), _) | (_, Err(e)) => {
                                tracing::error!("Error in select or select_tlp: {}", e);
                                Err(LimboError::InternalError(e.to_string()))
                            }
                        }
                    },
                ));

                vec![
                    Interaction::new(connection_index, assumption),
                    Interaction::new(connection_index, select),
                    Interaction::new(connection_index, select_tlp),
                    Interaction::new(connection_index, assertion),
                ]
            }
            Property::UNIONAllPreservesCardinality {
                select,
                where_clause,
            } => {
                let s1 = select.clone();
                let mut s2 = select.clone();
                s2.body.select.where_clause = where_clause.clone();
                let s3 = Select::compound(s1.clone(), s2.clone(), CompoundOperator::UnionAll);

                vec![
                    InteractionType::Query(Query::Select(s1.clone())),
                    InteractionType::Query(Query::Select(s2.clone())),
                    InteractionType::Query(Query::Select(s3.clone())),
                    InteractionType::Assertion(Assertion::new(
                        "UNION ALL should preserve cardinality".to_string(),
                        move |stack: &Vec<ResultSet>, _: &mut SimulatorEnv| {
                            if stack.len() < 3 {
                                return Err(LimboError::InternalError(
                                    "Not enough result sets on the stack".to_string(),
                                ));
                            }

                            let select1 = stack.get(stack.len() - 3).unwrap();
                            let select2 = stack.get(stack.len() - 2).unwrap();
                            let union_all = stack.last().unwrap();

                            match (select1, select2, union_all) {
                                (Ok(rows1), Ok(rows2), Ok(union_rows)) => {
                                    let count1 = rows1.len();
                                    let count2 = rows2.len();
                                    let union_count = union_rows.len();
                                    if union_count == count1 + count2 {
                                        Ok(Ok(()))
                                    } else {
                                        Ok(Err(format!(
                                            "UNION ALL should preserve cardinality but it didn't: {count1} + {count2} != {union_count}"
                                        )))
                                    }
                                }
                                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                                    tracing::error!("Error in select queries: {}", e);
                                    Err(LimboError::InternalError(e.to_string()))
                                }
                            }
                        },
                    )),
                ].into_iter().map(|i| Interaction::new(connection_index, i)).collect()
            }
            Property::Queries { queries } => queries
                .clone()
                .into_iter()
                .map(|query| Interaction::new(connection_index, InteractionType::Query(query)))
                .collect(),
            Property::InsertSelectNested {
                table,
                nesting_level,
            } => {
                let table = table.to_string();
                let table_name = table.clone();
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {} exists", table.clone()),
                    move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                        let conn_tables = env.get_conn_tables(connection_index);
                        if conn_tables.iter().any(|t| t.name == table_name) {
                            Ok(Ok(()))
                        } else {
                            Ok(Err(format!("table {table_name} does not exist")))
                        }
                    },
                ));

                let mut select_query = Select::simple(table.clone(), Predicate::true_());
                let select_interaction =
                    InteractionType::Query(Query::Select(select_query.clone()));

                for _ in 0..*nesting_level {
                    select_query = Select {
                        body: SelectBody {
                            select: Box::new(SelectInner {
                                distinctness: Distinctness::All,
                                columns: vec![ResultColumn::Star],
                                from: Some(FromClause {
                                    table: SelectTable::Select(select_query),
                                    joins: vec![],
                                }),
                                where_clause: Predicate::true_(),
                                order_by: None,
                            }),
                            compounds: Vec::new(),
                        },
                        limit: None,
                    }
                }

                let insert_interaction = InteractionType::Query(Query::Insert(Insert::Select {
                    table: table.clone(),
                    select: Box::new(select_query),
                }));

                let assertion = InteractionType::Assertion(Assertion::new(
                    format!("table {} should have the expected content", table.clone()),
                    move |stack: &Vec<ResultSet>, env| {
                        let before_rows = &stack[stack.len() - 2];
                        let Ok(before_rows) = before_rows else {
                            return Ok(Err(format!(
                                "expected rows but got error: {before_rows:?}"
                            )));
                        };
                        let mut expected_rows = {
                            let mut rows = Vec::new();
                            rows.extend(before_rows.clone());
                            rows.extend(before_rows.clone());
                            rows
                        };
                        let conn_tables = env.get_conn_tables(connection_index);
                        let sim_table = conn_tables
                            .iter()
                            .find(|t| t.name == table)
                            .expect("table should be in enviroment");

                        if expected_rows.len() != sim_table.rows.len() {
                            return Ok(Err(format!(
                                "expected {} rows after self-inserting table but got {} for table {}",
                                expected_rows.len(),
                                sim_table.rows.len(),
                                table.clone()
                            )));
                        }

                        let after_rows = &sim_table.rows.clone();
                        for after_row in after_rows.iter() {
                            if !expected_rows.contains(after_row) {
                                return Ok(Err(format!(
                                    "expected row {:?} not found in table {}",
                                    after_row,
                                    table.clone()
                                )));
                            } else {
                                let idx = expected_rows
                                    .iter()
                                    .position(|row| row == after_row)
                                    .unwrap();
                                expected_rows.remove(idx);
                            }
                        }

                        Ok(Ok(()))
                    },
                ));

                vec![
                    Interaction::new(connection_index, assumption),
                    Interaction::new(connection_index, select_interaction),
                    Interaction::new(connection_index, insert_interaction),
                    Interaction::new(connection_index, assertion),
                ]
            }
        }
    }
}

fn assert_all_table_values(
    tables: &[String],
    connection_index: usize,
) -> impl Iterator<Item = Interaction> + use<'_> {
    tables.iter().flat_map(move |table| {
        let select = InteractionType::Query(Query::Select(Select::simple(
            table.clone(),
            Predicate::true_(),
        )));

        let assertion = InteractionType::Assertion(Assertion::new(format!("table {table} should contain all of its expected values"), {
                let table = table.clone();
                move |stack: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                    let conn_ctx = env.get_conn_tables(connection_index);
                    let table = conn_ctx.iter().find(|t| t.name == table).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "table {table} should exist in simulator env"
                        ))
                    })?;
                    let last = stack.last().unwrap();
                    match last {
                        Ok(vals) => {
                            // Check if all values in the table are present in the result set
                            // Find a value in the table that is not in the result set
                            let model_contains_db = table.rows.iter().find(|v| {
                                !vals.contains(v)
                            });
                            let db_contains_model = vals.iter().find(|v| {
                                !table.rows.contains(v)
                            });


                            if let Some(model_contains_db) = model_contains_db {
                                tracing::debug!(
                                    "table {} does not contain the expected values, the simulator model has more rows than the database: {:?}",
                                    table.name,
                                    print_row(model_contains_db)
                                );
                                print_diff(&table.rows, vals, "simulator", "database");

                                Ok(Err(format!("table {} does not contain the expected values, the simulator model has more rows than the database: {:?}", table.name, print_row(model_contains_db))))
                            } else if let Some(db_contains_model) = db_contains_model {
                                tracing::debug!(
                                    "table {} does not contain the expected values, the database has more rows than the simulator model: {:?}",
                                    table.name,
                                    print_row(db_contains_model)
                                );
                                print_diff(&table.rows, vals, "simulator", "database");

                                Ok(Err(format!("table {} does not contain the expected values, the database has more rows than the simulator model: {:?}", table.name, print_row(db_contains_model))))
                            } else {
                                Ok(Ok(()))
                            }
                        }
                        Err(err) => Err(LimboError::InternalError(format!("{err}"))),
                    }
                }
            }));
        [select, assertion].into_iter().map(move |i| Interaction::new(connection_index, i))
    })
}

#[derive(Debug)]
pub(crate) struct Remaining {
    pub(crate) select: u32,
    pub(crate) insert: u32,
    pub(crate) create: u32,
    pub(crate) create_index: u32,
    pub(crate) delete: u32,
    pub(crate) update: u32,
    pub(crate) drop: u32,
}

pub(crate) fn remaining(
    max_interactions: u32,
    opts: &QueryProfile,
    stats: &InteractionStats,
    mvcc: bool,
) -> Remaining {
    let total_weight = opts.select_weight
        + opts.create_table_weight
        + opts.create_index_weight
        + opts.insert_weight
        + opts.update_weight
        + opts.delete_weight
        + opts.drop_table_weight;

    let total_select = (max_interactions * opts.select_weight) / total_weight;
    let total_insert = (max_interactions * opts.insert_weight) / total_weight;
    let total_create = (max_interactions * opts.create_table_weight) / total_weight;
    let total_create_index = (max_interactions * opts.create_index_weight) / total_weight;
    let total_delete = (max_interactions * opts.delete_weight) / total_weight;
    let total_update = (max_interactions * opts.update_weight) / total_weight;
    let total_drop = (max_interactions * opts.drop_table_weight) / total_weight;

    let remaining_select = total_select
        .checked_sub(stats.select_count)
        .unwrap_or_default();
    let remaining_insert = total_insert
        .checked_sub(stats.insert_count)
        .unwrap_or_default();
    let remaining_create = total_create
        .checked_sub(stats.create_count)
        .unwrap_or_default();
    let mut remaining_create_index = total_create_index
        .checked_sub(stats.create_index_count)
        .unwrap_or_default();
    let remaining_delete = total_delete
        .checked_sub(stats.delete_count)
        .unwrap_or_default();
    let remaining_update = total_update
        .checked_sub(stats.update_count)
        .unwrap_or_default();
    let remaining_drop = total_drop.checked_sub(stats.drop_count).unwrap_or_default();

    if mvcc {
        // TODO: index not supported yet for mvcc
        remaining_create_index = 0;
    }

    Remaining {
        select: remaining_select,
        insert: remaining_insert,
        create: remaining_create,
        create_index: remaining_create_index,
        delete: remaining_delete,
        drop: remaining_drop,
        update: remaining_update,
    }
}

fn property_insert_values_select<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    // Get a random table
    let table = pick(ctx.tables(), rng);
    // Generate rows to insert
    let rows = (0..rng.random_range(1..=5))
        .map(|_| Vec::<SimValue>::arbitrary_from(rng, ctx, table))
        .collect::<Vec<_>>();

    // Pick a random row to select
    let row_index = pick_index(rows.len(), rng);
    let row = rows[row_index].clone();

    // Insert the rows
    let insert_query = Query::Insert(Insert::Values {
        table: table.name.clone(),
        values: rows,
    });

    // Choose if we want queries to be executed in an interactive transaction
    let interactive = if !mvcc && rng.random_bool(0.5) {
        Some(InteractiveQueryInfo {
            start_with_immediate: rng.random_bool(0.5),
            end_with_commit: rng.random_bool(0.5),
        })
    } else {
        None
    };

    let amount = rng.random_range(0..3);

    let mut queries = Vec::with_capacity(amount + 2);

    if let Some(ref interactive) = interactive {
        queries.push(Query::Begin(if interactive.start_with_immediate {
            Begin::Immediate
        } else {
            Begin::Deferred
        }));
    }

    queries.extend(std::iter::repeat_n(Query::Placeholder, amount));

    if let Some(ref interactive) = interactive {
        queries.push(if interactive.end_with_commit {
            Query::Commit(Commit)
        } else {
            Query::Rollback(Rollback)
        });
    }

    // Select the row
    let select_query = Select::simple(
        table.name.clone(),
        Predicate::arbitrary_from(rng, ctx, (table, &row)),
    );

    Property::InsertValuesSelect {
        insert: insert_query.unwrap_insert(),
        row_index,
        queries,
        select: select_query,
        interactive,
    }
}

fn property_read_your_updates_back<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    // e.g. UPDATE t SET a=1, b=2 WHERE c=1;
    let update = Update::arbitrary(rng, ctx);
    // e.g. SELECT a, b FROM t WHERE c=1;
    let select = Select::single(
        update.table().to_string(),
        update
            .set_values
            .iter()
            .map(|(col, _)| ResultColumn::Column(col.clone()))
            .collect(),
        update.predicate.clone(),
        None,
        Distinctness::All,
    );

    Property::ReadYourUpdatesBack { update, select }
}

fn property_table_has_expected_content<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    // Get a random table
    let table = pick(ctx.tables(), rng);
    Property::TableHasExpectedContent {
        table: table.name.clone(),
    }
}

fn property_all_tables_have_expected_content<R: rand::Rng + ?Sized>(
    _rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    Property::AllTableHaveExpectedContent {
        tables: ctx.tables().iter().map(|t| t.name.clone()).collect(),
    }
}

fn property_select_limit<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    // Get a random table
    let table = pick(ctx.tables(), rng);
    // Select the table
    let select = Select::single(
        table.name.clone(),
        vec![ResultColumn::Star],
        Predicate::arbitrary_from(rng, ctx, table),
        Some(rng.random_range(1..=5)),
        Distinctness::All,
    );
    Property::SelectLimit { select }
}

fn property_double_create_failure<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    // Create the table
    let create_query = Create::arbitrary(rng, ctx);

    let amount = rng.random_range(0..3);

    let queries = vec![Query::Placeholder; amount];

    Property::DoubleCreateFailure {
        create: create_query,
        queries,
    }
}

fn property_delete_select<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    // Get a random table
    let table = pick(ctx.tables(), rng);
    // Generate a random predicate
    let predicate = Predicate::arbitrary_from(rng, ctx, table);

    let amount = rng.random_range(0..3);

    let queries = vec![Query::Placeholder; amount];

    Property::DeleteSelect {
        table: table.name.clone(),
        predicate,
        queries,
    }
}

fn property_drop_select<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    // Get a random table
    let table = pick(ctx.tables(), rng);

    let amount = rng.random_range(0..3);

    let queries = vec![Query::Placeholder; amount];

    let select = Select::simple(
        table.name.clone(),
        Predicate::arbitrary_from(rng, ctx, table),
    );

    Property::DropSelect {
        table: table.name.clone(),
        queries,
        select,
    }
}

fn property_select_select_optimizer<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    // Get a random table
    let table = pick(ctx.tables(), rng);
    // Generate a random predicate
    let predicate = Predicate::arbitrary_from(rng, ctx, table);
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

fn property_where_true_false_null<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    // Get a random table
    let table = pick(ctx.tables(), rng);
    // Generate a random predicate
    let p1 = Predicate::arbitrary_from(rng, ctx, table);
    let p2 = Predicate::arbitrary_from(rng, ctx, table);

    // Create the select query
    let select = Select::simple(table.name.clone(), p1);

    Property::WhereTrueFalseNull {
        select,
        predicate: p2,
    }
}

fn property_union_all_preserves_cardinality<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    // Get a random table
    let table = pick(ctx.tables(), rng);
    // Generate a random predicate
    let p1 = Predicate::arbitrary_from(rng, ctx, table);
    let p2 = Predicate::arbitrary_from(rng, ctx, table);

    // Create the select query
    let select = Select::single(
        table.name.clone(),
        vec![ResultColumn::Star],
        p1,
        None,
        Distinctness::All,
    );

    Property::UNIONAllPreservesCardinality {
        select,
        where_clause: p2,
    }
}

fn property_fsync_no_wait<R: rand::Rng + ?Sized>(
    rng: &mut R,
    query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    Property::FsyncNoWait {
        query: Query::arbitrary_from(rng, ctx, query_distr),
    }
}

fn property_faulty_query<R: rand::Rng + ?Sized>(
    rng: &mut R,
    query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    Property::FaultyQuery {
        query: Query::arbitrary_from(rng, ctx, query_distr),
    }
}

fn property_insert_select_nested<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    let table = pick(ctx.tables(), rng).name.clone();
    let nesting_level = rng.random_range(0..=10);
    Property::InsertSelectNested {
        table,
        nesting_level,
    }
}

type PropertyGenFunc<R, G> = fn(&mut R, &QueryDistribution, &G, bool) -> Property;

impl PropertyDiscriminants {
    pub(super) fn gen_function<R, G>(&self) -> PropertyGenFunc<R, G>
    where
        R: rand::Rng + ?Sized,
        G: GenerationContext,
    {
        match self {
            PropertyDiscriminants::InsertValuesSelect => property_insert_values_select,
            PropertyDiscriminants::ReadYourUpdatesBack => property_read_your_updates_back,
            PropertyDiscriminants::TableHasExpectedContent => property_table_has_expected_content,
            PropertyDiscriminants::AllTableHaveExpectedContent => {
                property_all_tables_have_expected_content
            }
            PropertyDiscriminants::DoubleCreateFailure => property_double_create_failure,
            PropertyDiscriminants::SelectLimit => property_select_limit,
            PropertyDiscriminants::DeleteSelect => property_delete_select,
            PropertyDiscriminants::DropSelect => property_drop_select,
            PropertyDiscriminants::SelectSelectOptimizer => property_select_select_optimizer,
            PropertyDiscriminants::WhereTrueFalseNull => property_where_true_false_null,
            PropertyDiscriminants::UNIONAllPreservesCardinality => {
                property_union_all_preserves_cardinality
            }
            PropertyDiscriminants::FsyncNoWait => property_fsync_no_wait,
            PropertyDiscriminants::FaultyQuery => property_faulty_query,
            PropertyDiscriminants::InsertSelectNested => property_insert_select_nested,
            PropertyDiscriminants::Queries => {
                unreachable!("should not try to generate queries property")
            }
        }
    }

    pub fn weight(
        &self,
        env: &SimulatorEnv,
        remaining: &Remaining,
        ctx: &impl GenerationContext,
    ) -> u32 {
        let opts = ctx.opts();
        match self {
            PropertyDiscriminants::InsertValuesSelect => {
                if !env.opts.disable_insert_values_select && !ctx.tables().is_empty() {
                    u32::min(remaining.select, remaining.insert).max(1)
                } else {
                    0
                }
            }
            PropertyDiscriminants::ReadYourUpdatesBack => {
                u32::min(remaining.select, remaining.insert).max(1)
            }
            PropertyDiscriminants::TableHasExpectedContent => {
                if !ctx.tables().is_empty() {
                    remaining.select.max(1)
                } else {
                    0
                }
            }
            // AllTableHaveExpectedContent should only be generated by Properties that inject faults
            PropertyDiscriminants::AllTableHaveExpectedContent => 0,
            PropertyDiscriminants::DoubleCreateFailure => {
                if !env.opts.disable_double_create_failure {
                    remaining.create / 2
                } else {
                    0
                }
            }
            PropertyDiscriminants::SelectLimit => {
                if !env.opts.disable_select_limit && !ctx.tables().is_empty() {
                    remaining.select
                } else {
                    0
                }
            }
            PropertyDiscriminants::DeleteSelect => {
                if !env.opts.disable_delete_select && !ctx.tables().is_empty() {
                    u32::min(remaining.select, remaining.insert).min(remaining.delete)
                } else {
                    0
                }
            }
            PropertyDiscriminants::DropSelect => {
                if !env.opts.disable_drop_select && !ctx.tables().is_empty() {
                    remaining.drop
                } else {
                    0
                }
            }
            PropertyDiscriminants::SelectSelectOptimizer => {
                if !env.opts.disable_select_optimizer && !ctx.tables().is_empty() {
                    remaining.select / 2
                } else {
                    0
                }
            }
            PropertyDiscriminants::WhereTrueFalseNull => {
                if opts.indexes
                    && !env.opts.disable_where_true_false_null
                    && !ctx.tables().is_empty()
                {
                    remaining.select / 2
                } else {
                    0
                }
            }
            PropertyDiscriminants::UNIONAllPreservesCardinality => {
                if opts.indexes
                    && !env.opts.disable_union_all_preserves_cardinality
                    && !ctx.tables().is_empty()
                {
                    remaining.select / 3
                } else {
                    0
                }
            }
            PropertyDiscriminants::FsyncNoWait => {
                if env.profile.io.enable && !env.opts.disable_fsync_no_wait {
                    50 // Freestyle number
                } else {
                    0
                }
            }
            PropertyDiscriminants::FaultyQuery => {
                if env.profile.io.enable
                    && env.profile.io.fault.enable
                    && !env.opts.disable_faulty_query
                {
                    20
                } else {
                    0
                }
            }
            PropertyDiscriminants::InsertSelectNested => {
                if !env.opts.disable_insert_nested_select {
                    u32::min(remaining.select, remaining.insert).max(1)
                } else {
                    0
                }
            }
            PropertyDiscriminants::Queries => {
                unreachable!("queries property should not be generated")
            }
        }
    }

    fn can_generate(queries: &[QueryDiscriminants]) -> Vec<PropertyDiscriminants> {
        let queries_capabilities = QueryCapabilities::from_list_queries(queries);

        PropertyDiscriminants::iter()
            .filter(|property| {
                !matches!(property, PropertyDiscriminants::Queries)
                    && queries_capabilities.contains(property.requirements())
            })
            .collect()
    }

    pub const fn requirements(&self) -> QueryCapabilities {
        match self {
            PropertyDiscriminants::InsertValuesSelect => {
                QueryCapabilities::SELECT.union(QueryCapabilities::INSERT)
            }
            PropertyDiscriminants::ReadYourUpdatesBack => {
                QueryCapabilities::SELECT.union(QueryCapabilities::UPDATE)
            }
            PropertyDiscriminants::TableHasExpectedContent => QueryCapabilities::SELECT,
            PropertyDiscriminants::AllTableHaveExpectedContent => QueryCapabilities::SELECT,
            PropertyDiscriminants::DoubleCreateFailure => QueryCapabilities::CREATE,
            PropertyDiscriminants::SelectLimit => QueryCapabilities::SELECT,
            PropertyDiscriminants::DeleteSelect => {
                QueryCapabilities::SELECT.union(QueryCapabilities::DELETE)
            }
            PropertyDiscriminants::DropSelect => {
                QueryCapabilities::SELECT.union(QueryCapabilities::DROP)
            }
            PropertyDiscriminants::SelectSelectOptimizer => QueryCapabilities::SELECT,
            PropertyDiscriminants::WhereTrueFalseNull => QueryCapabilities::SELECT,
            PropertyDiscriminants::UNIONAllPreservesCardinality => QueryCapabilities::SELECT,
            PropertyDiscriminants::FsyncNoWait => QueryCapabilities::all(),
            PropertyDiscriminants::FaultyQuery => QueryCapabilities::all(),
            PropertyDiscriminants::InsertSelectNested => {
                QueryCapabilities::SELECT.union(QueryCapabilities::INSERT)
            }
            PropertyDiscriminants::Queries => panic!("queries property should not be generated"),
        }
    }
}

pub(super) struct PropertyDistribution<'a> {
    properties: Vec<PropertyDiscriminants>,
    weights: WeightedIndex<u32>,
    query_distr: &'a QueryDistribution,
    mvcc: bool,
}

impl<'a> PropertyDistribution<'a> {
    pub fn new(
        env: &SimulatorEnv,
        remaining: &Remaining,
        query_distr: &'a QueryDistribution,
        ctx: &impl GenerationContext,
    ) -> Result<Self, rand::distr::weighted::Error> {
        let properties = PropertyDiscriminants::can_generate(query_distr.items());
        let weights = WeightedIndex::new(
            properties
                .iter()
                .map(|property| property.weight(env, remaining, ctx)),
        )?;

        Ok(Self {
            properties,
            weights,
            query_distr,
            mvcc: env.profile.experimental_mvcc,
        })
    }
}

impl<'a> WeightedDistribution for PropertyDistribution<'a> {
    type Item = PropertyDiscriminants;

    type GenItem = Property;

    fn items(&self) -> &[Self::Item] {
        &self.properties
    }

    fn weights(&self) -> &WeightedIndex<u32> {
        &self.weights
    }

    fn sample<R: rand::Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        conn_ctx: &C,
    ) -> Self::GenItem {
        let properties = &self.properties;
        let idx = self.weights.sample(rng);
        let property_fn = properties[idx].gen_function();
        (property_fn)(rng, self.query_distr, conn_ctx, self.mvcc)
    }
}

impl<'a> ArbitraryFrom<&PropertyDistribution<'a>> for Property {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        conn_ctx: &C,
        property_distr: &PropertyDistribution<'a>,
    ) -> Self {
        property_distr.sample(rng, conn_ctx)
    }
}

fn print_row(row: &[SimValue]) -> String {
    row.iter()
        .map(|v| match &v.0 {
            types::Value::Null => "NULL".to_string(),
            types::Value::Integer(i) => i.to_string(),
            types::Value::Float(f) => f.to_string(),
            types::Value::Text(t) => t.to_string(),
            types::Value::Blob(b) => format!(
                "X'{}'",
                b.iter()
                    .fold(String::new(), |acc, b| acc + &format!("{b:02X}"))
            ),
        })
        .collect::<Vec<String>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interactions_insertselectednested() {
        assert_insertselectnested_with_nesting_level(
            0,
            "INSERT INTO table1 SELECT * FROM table1 WHERE TRUE",
        );
        assert_insertselectnested_with_nesting_level(
            1,
            "INSERT INTO table1 SELECT * FROM (SELECT * FROM table1 WHERE TRUE) WHERE TRUE",
        );
    }

    fn assert_insertselectnested_with_nesting_level(nesting_level: u8, expected_insert: &str) {
        let property = Property::InsertSelectNested {
            table: "table1".to_owned(),
            nesting_level,
        };
        let result = property.interactions(123);
        let _expected: Vec<Interaction> = vec![];
        assert_eq!(4, result.len());

        if !matches!(result[0].interaction, InteractionType::Assumption(_)) {
            panic!(
                "unexpected interaction type for assumption: {}",
                result[0].interaction
            );
        }

        assert_eq!(
            "SELECT * FROM table1 WHERE TRUE",
            format!("{}", &result[1].interaction)
        );

        assert!(matches!(result[2].interaction, InteractionType::Query(_)));

        assert_eq!(expected_insert, format!("{}", &result[2].interaction));

        assert!(matches!(
            result[3].interaction,
            InteractionType::Assertion(_)
        ));
    }
}
