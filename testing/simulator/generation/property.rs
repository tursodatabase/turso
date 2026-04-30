//! FIXME: With the current API and generation logic in plan.rs,
//! for Properties that have intermediary queries we need to CLONE the current Context tables
//! to properly generate queries, as we need to shadow after each query generated to make sure we are generating
//! queries that are valid. This is specially valid with DROP and ALTER TABLE in the mix, because with outdated context
//! we can generate queries that reference tables that do not exist. This is not a correctness issue, but more of
//! an optimization issue that is good to point out for the future

use std::num::NonZeroUsize;

use rand::distr::{Distribution, weighted::WeightedIndex};
use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, pick, pick_index},
    model::{
        query::{
            Create, Delete, Drop, Insert, Select,
            alter_table::{AlterTable, AlterTableType},
            predicate::Predicate,
            select::{CompoundOperator, CompoundSelect, ResultColumn, SelectBody, SelectInner},
            transaction::{Begin, Commit, Rollback},
            update::{SetValue, Update},
        },
        table::{ColumnType, SimValue, Table},
    },
};
use strum::IntoEnumIterator;
use turso_core::{LimboError, Numeric, types};
use turso_parser::ast::{self, Distinctness};

use crate::{
    common::print_diff,
    generation::{Shadow, WeightedDistribution, query::QueryDistribution},
    model::{
        Query, QueryCapabilities, QueryDiscriminants, ReleaseSavepoint, ResultSet,
        RollbackToSavepoint, Savepoint,
        interactions::{
            Assertion, Interaction, InteractionBuilder, InteractionType, PropertyMetadata,
        },
        metrics::Remaining,
        property::{InteractiveQueryInfo, Property, PropertyDiscriminants},
    },
    runner::env::SimulatorEnv,
};

type PropertyQueryGenFunc<'a, R, G> =
    fn(&mut R, &G, &QueryDistribution, &Property) -> Option<Query>;

impl Property {
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
                // - [x] The table `t` will not be renamed, dropped, or altered.
                |rng: &mut R, ctx: &G, query_distr: &QueryDistribution, property: &Property| {
                    let Property::InsertValuesSelect {
                        insert, row_index, ..
                    } = property
                    else {
                        unreachable!();
                    };
                    let query = Query::arbitrary_from(rng, ctx, query_distr);
                    let table_name = insert.table();
                    let Some(table) = Self::table_with_name(ctx, table_name) else {
                        tracing::info!(
                            property = "InsertValuesSelect",
                            "extensional query generation exhausted max attempts; falling back to SELECT TRUE"
                        );
                        return None;
                    };

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
                        Query::AlterTable(AlterTable { table_name: t, .. }) if *t == table.name => {
                            // Cannot alter the table we are inserting
                            None
                        }
                        _ => Some(query),
                    }
                }
            }
            Property::DoubleCreateFailure { .. } => {
                // The interactions in the middle has the following constraints;
                // - [x] There will be no errors in the middle interactions.(best effort)
                // - [x] Table `t` will not be renamed or dropped.
                |rng: &mut R, ctx: &G, query_distr: &QueryDistribution, property: &Property| {
                    let Property::DoubleCreateFailure { create, .. } = property else {
                        unreachable!()
                    };

                    let Some(table) = Self::table_with_name(ctx, &create.table.name)
                    else {
                        tracing::info!(
                            property = "DoubleCreateFailure",
                            "extensional query generation exhausted max attempts; falling back to SELECT TRUE"
                        );
                        return None;
                    };

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
                        Query::AlterTable(AlterTable { table_name: t, .. }) if *t == table.name => {
                            // Cannot alter the table we created
                            None
                        }
                        _ => Some(query),
                    }
                }
            }
            Property::DeleteSelect { .. } => {
                // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
                // - [x] A row that holds for the predicate will not be inserted.
                // - [x] The table `t` will not be renamed, dropped, or altered.
                // - [x] Under MVCC, there will be no DDL (this is a best effort, because concurrent
                //       transactions could still drop the table, in which case this property will
                //       be skipped).

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

                    let Some(table) = Self::table_with_name(ctx, &table_name) else {
                        tracing::info!(
                            property = "DeleteSelect",
                            "extensional query generation exhausted max attempts; falling back to SELECT TRUE"
                        );
                        return None;
                    };
                    let query = Query::arbitrary_from(rng, ctx, query_distr);
                    match &query {
                        Query::Insert(Insert::Values {
                            table: t, values, ..
                        }) if *t == table_name
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
                        Query::AlterTable(AlterTable { table_name: t, .. }) if *t == table.name => {
                            // Cannot alter the same table
                            None
                        }
                        _ if ctx.opts().mvcc && query.is_ddl() => {
                            // No DDL in MVCC
                            None
                        }
                        _ => Some(query),
                    }
                }
            }
            Property::DropSelect { .. } => {
                // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
                // - [x] The table `t` will not be created, no table will be renamed to `t`.
                |rng, ctx, query_distr, property: &Property| {
                    let Property::DropSelect {
                        table: table_name, ..
                    } = property
                    else {
                        unreachable!()
                    };

                    let query = Query::arbitrary_from(rng, ctx, query_distr);
                    match &query {
                        Query::Create(Create { table: t }) if t.name == *table_name => {
                            // - The table `t` will not be created
                            None
                        }
                        Query::AlterTable(AlterTable {
                            table_name: t,
                            alter_table_type: AlterTableType::RenameTo { new_name },
                        }) if t == table_name || new_name == table_name => {
                            // no table will be renamed to `t`
                            None
                        }
                        _ => Some(query),
                    }
                }
            }
            Property::SavepointRollback { .. } => {
                |rng: &mut R, ctx: &G, _query_distr: &QueryDistribution, property: &Property| {
                    let Property::SavepointRollback { write_kinds, .. } = property else {
                        unreachable!()
                    };
                    random_main_table_write(rng, ctx, write_kinds)
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
            | Property::UnionAllPreservesCardinality { .. }
            | Property::ReadYourUpdatesBack { .. }
            | Property::TableHasExpectedContent { .. }
            | Property::EachColumnMatchesModel { .. }
            | Property::AllTableHaveExpectedContent { .. } => {
                unreachable!("No extensional queries")
            }
        }
    }

    fn table_with_name<'a, G>(
        ctx: &'a G,
        table_name: &str,
    ) -> Option<&'a Table>
    where
        G: GenerationContext,
    {
        let table = ctx.tables().iter().find(|table| table.name == table_name);
        if ctx.opts().mvcc {
            // Under MVCC, due to difficulties in tracking concurrent state, the table
            // could be absent.
            table
        } else {
            Some(table.unwrap())
        }
    }

    /// interactions construct a list of interactions, which is an executable representation of the property.
    /// the requirement of property -> vec<interaction> conversion emerges from the need to serialize the property,
    /// and `interaction` cannot be serialized directly.
    pub(crate) fn interactions(
        &self,
        connection_index: usize,
        id: NonZeroUsize,
    ) -> Vec<Interaction> {
        let interactions: Vec<InteractionBuilder> = match self {
            Property::AllTableHaveExpectedContent { tables } => {
                assert_all_table_values(tables, connection_index).collect()
            }
            Property::TableHasExpectedContent { table } => {
                let table = table.to_string();
                let table_dependency = table.clone();
                let table_name = table.clone();
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {table} exists"),
                    move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                        let conn_tables = env.get_conn_tables(connection_index);
                        if conn_tables.iter().any(|t| t.name == table_name) {
                            Ok(Ok(()))
                        } else {
                            Ok(Err(format!("table {table_name} does not exist")))
                        }
                    },
                    vec![table_dependency.clone()],
                ));

                let select_interaction = InteractionType::Query(Query::Select(Select::simple(
                    table.clone(),
                    Predicate::true_(),
                )));

                let assertion = InteractionType::Assertion(Assertion::new(
                    format!("table {table} should have the expected content"),
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
                    vec![table_dependency],
                ));

                vec![
                    InteractionBuilder::with_interaction(assumption),
                    InteractionBuilder::with_interaction(select_interaction),
                    InteractionBuilder::with_interaction(assertion),
                ]
            }
            Property::EachColumnMatchesModel { table, columns } => {
                let table_name = table.clone();

                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {table_name} exists"),
                    {
                        let table_name = table_name.clone();
                        move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                            let conn_tables = env.get_conn_tables(connection_index);
                            if conn_tables.iter().any(|t| t.name == table_name) {
                                Ok(Ok(()))
                            } else {
                                Ok(Err(format!("table {table_name} does not exist")))
                            }
                        }
                    },
                    vec![table_name.clone()],
                ));

                // Columns were captured at generation time. If ALTER TABLE shifts
                // them before this property fires, the runtime model lookup below
                // will surface that mismatch rather than silently passing.
                let columns: Vec<String> = columns.clone();

                let mut builders = Vec::with_capacity(columns.len() + 2);
                builders.push(InteractionBuilder::with_interaction(assumption));
                for col in &columns {
                    let select = Select::single(
                        table_name.clone(),
                        vec![ResultColumn::Column(col.clone())],
                        Predicate::true_(),
                        None,
                        Distinctness::All,
                    );
                    builders.push(InteractionBuilder::with_interaction(
                        InteractionType::Query(Query::Select(select)),
                    ));
                }

                let columns_for_assertion = columns.clone();
                let table_for_assertion = table_name.clone();
                let assertion = InteractionType::Assertion(Assertion::new(
                    format!("each column of {table_name} matches the simulator model"),
                    move |stack: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                        let n = columns_for_assertion.len();
                        if stack.len() < n {
                            return Err(LimboError::InternalError(format!(
                                "EachColumnMatchesModel: expected {n} results on stack, got {}",
                                stack.len()
                            )));
                        }
                        let conn_tables = env.get_conn_tables(connection_index);
                        let Some(model_table) =
                            conn_tables.iter().find(|t| t.name == table_for_assertion)
                        else {
                            return Ok(Err(format!(
                                "table {table_for_assertion} disappeared from model"
                            )));
                        };
                        let model_cols: Vec<&str> = model_table
                            .columns
                            .iter()
                            .map(|c| c.name.as_str())
                            .collect();

                        let start = stack.len() - n;
                        for (i, col_name) in columns_for_assertion.iter().enumerate() {
                            let result = &stack[start + i];
                            let Ok(rows) = result else {
                                return Ok(Err(format!(
                                    "SELECT {col_name} FROM {table_for_assertion} returned an error: {result:?}"
                                )));
                            };

                            let Some(col_pos) = model_cols.iter().position(|c| *c == col_name)
                            else {
                                return Ok(Err(format!(
                                    "column {col_name} no longer in model for {table_for_assertion}"
                                )));
                            };

                            // Multiset comparison: counts of each value must match.
                            use std::collections::BTreeMap;
                            let mut db_counts: BTreeMap<&SimValue, usize> = BTreeMap::new();
                            for row in rows {
                                if let Some(v) = row.first() {
                                    *db_counts.entry(v).or_insert(0) += 1;
                                }
                            }
                            let mut model_counts: BTreeMap<&SimValue, usize> = BTreeMap::new();
                            for row in &model_table.rows {
                                if let Some(v) = row.get(col_pos) {
                                    *model_counts.entry(v).or_insert(0) += 1;
                                }
                            }

                            if db_counts != model_counts {
                                return Ok(Err(format!(
                                    "column {col_name} of {table_for_assertion} disagrees with model: db={:?} model={:?}",
                                    db_counts, model_counts
                                )));
                            }
                        }
                        Ok(Ok(()))
                    },
                    vec![table_name.clone()],
                ));
                builders.push(InteractionBuilder::with_interaction(assertion));
                builders
            }
            Property::ReadYourUpdatesBack {
                update,
                select_before,
                select_after,
            } => {
                let table = update.table().to_string();
                let table_dependency = table.clone();
                let assumption = InteractionType::Assumption(Assertion::new(
                    format!("table {table} exists"),
                    move |_: &Vec<ResultSet>, env: &mut SimulatorEnv| {
                        let conn_tables = env.get_conn_tables(connection_index);
                        if conn_tables.iter().any(|t| t.name == table) {
                            Ok(Ok(()))
                        } else {
                            Ok(Err(format!("table {table} does not exist")))
                        }
                    },
                    vec![table_dependency.clone()],
                ));

                let before_interaction =
                    InteractionType::Query(Query::Select(select_before.clone()));
                let begin_tx = InteractionType::Query(Query::Begin(
                    sql_generation::model::query::transaction::Begin::Immediate,
                ));
                let update_interaction = InteractionType::Query(Query::Update(update.clone()));
                let commit_tx = InteractionType::Query(Query::Commit(
                    sql_generation::model::query::transaction::Commit,
                ));
                let after_interaction = InteractionType::Query(Query::Select(select_after.clone()));

                let update = update.clone();
                let table = update.table().to_string();

                let assertion = InteractionType::Assertion(Assertion::new(
                    format!(
                        "verify UPDATE result for table {table}: success=values updated, failure=unchanged"
                    ),
                    move |stack: &Vec<ResultSet>, _| {
                        // Stack: [before, BEGIN, UPDATE, COMMIT, after]
                        if stack.len() < 5 {
                            return Err(LimboError::InternalError(
                                "ReadYourUpdatesBack: expected 5 results on stack".into(),
                            ));
                        }
                        let before = &stack[stack.len() - 5];
                        let update_result = &stack[stack.len() - 3];
                        let after = &stack[stack.len() - 1];

                        let update_succeeded = update_result.is_ok();

                        match (before, after) {
                            (Ok(before_rows), Ok(after_rows)) => {
                                if update_succeeded {
                                    // Verify rows have updated values
                                    for row in after_rows {
                                        for (i, (col, set_val)) in
                                            update.set_values.iter().enumerate()
                                        {
                                            match set_val {
                                                SetValue::Simple(expected) => {
                                                    if &row[i] != expected {
                                                        print_diff(
                                                            &[row.to_vec()],
                                                            &[vec![expected.clone()]],
                                                            "database",
                                                            "expected",
                                                        );
                                                        return Ok(Err(format!(
                                                            "row has incorrect value for column {col}: expected {expected}, got {}",
                                                            row[i]
                                                        )));
                                                    }
                                                }
                                                SetValue::CaseWhen { then_value, .. } => {
                                                    let is_then = &row[i] == then_value;
                                                    let existed_before = before_rows
                                                        .iter()
                                                        .any(|br| br[i] == row[i]);
                                                    if !is_then && !existed_before {
                                                        return Ok(Err(format!(
                                                            "CaseWhen: row col {col} has unexpected value {} (expected {} or a value from before)",
                                                            row[i], then_value
                                                        )));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Ok(Ok(()))
                                } else {
                                    // UPDATE failed - verify rollback (rows unchanged)
                                    let rows_equal_as_multiset =
                                        |a: &[Vec<SimValue>], b: &[Vec<SimValue>]| {
                                            if a.len() != b.len() {
                                                return false;
                                            }
                                            // for each row in a, check it appears the same number of times in b
                                            let count_in =
                                                |row: &Vec<SimValue>, set: &[Vec<SimValue>]| {
                                                    set.iter().filter(|r| *r == row).count()
                                                };
                                            a.iter().all(|row| count_in(row, a) == count_in(row, b))
                                        };
                                    if !rows_equal_as_multiset(before_rows, after_rows) {
                                        print_diff(before_rows, after_rows, "before", "after");
                                        return Ok(Err(format!(
                                            "UPDATE failed but rows changed - rollback failed: {} rows before, {} after",
                                            before_rows.len(),
                                            after_rows.len()
                                        )));
                                    }
                                    Ok(Ok(()))
                                }
                            }
                            (Err(e), _) | (_, Err(e)) => {
                                Err(LimboError::InternalError(format!("SELECT failed: {e}")))
                            }
                        }
                    },
                    vec![table_dependency],
                ));

                let mut update_builder = InteractionBuilder::with_interaction(update_interaction);
                update_builder.ignore_error(true);

                vec![
                    InteractionBuilder::with_interaction(assumption),
                    InteractionBuilder::with_interaction(before_interaction),
                    InteractionBuilder::with_interaction(begin_tx),
                    update_builder,
                    InteractionBuilder::with_interaction(commit_tx),
                    InteractionBuilder::with_interaction(after_interaction),
                    InteractionBuilder::with_interaction(assertion),
                ]
            }
            Property::InsertValuesSelect {
                insert,
                row_index,
                queries,
                select,
                interactive,
            } => {
                let (table, values) = if let Insert::Values {
                    table,
                    values,
                    on_conflict: None,
                } = insert
                {
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
                    vec![insert.table().to_string()],
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
                    vec![insert.table().to_string()],
                ));

                let mut interactions = Vec::new();
                interactions.push(InteractionBuilder::with_interaction(assumption));
                interactions.push(InteractionBuilder::with_interaction(
                    InteractionType::Query(Query::Insert(insert.clone())),
                ));
                interactions.extend(queries.clone().into_iter().map(|q| {
                    let mut builder =
                        InteractionBuilder::with_interaction(InteractionType::Query(q));
                    builder.property_meta(PropertyMetadata::new(self, true));
                    builder
                }));
                interactions.push(InteractionBuilder::with_interaction(
                    InteractionType::Query(Query::Select(select.clone())),
                ));
                interactions.push(InteractionBuilder::with_interaction(assertion));

                interactions
            }
            Property::DoubleCreateFailure { create, queries } => {
                let table_name = create.table.name.clone();
                let table_dependency = table_name.clone();

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
                    vec![table_dependency.clone()],
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
                            }, vec![table_dependency],) );

                let mut interactions = Vec::new();
                interactions.push(InteractionBuilder::with_interaction(assumption));
                interactions.push(InteractionBuilder::with_interaction(cq1));
                interactions.extend(queries.clone().into_iter().map(|q| {
                    let mut builder =
                        InteractionBuilder::with_interaction(InteractionType::Query(q));
                    builder.property_meta(PropertyMetadata::new(self, true));
                    builder
                }));
                interactions.push({
                    let mut builder = InteractionBuilder::with_interaction(cq2);
                    builder.ignore_error(true);
                    builder
                });
                interactions.push(InteractionBuilder::with_interaction(assertion));

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
                    select.dependencies().into_iter().collect(),
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
                    select.dependencies().into_iter().collect(),
                ));

                vec![
                    InteractionBuilder::with_interaction(assumption),
                    InteractionBuilder::with_interaction(InteractionType::Query(Query::Select(
                        select.clone(),
                    ))),
                    InteractionBuilder::with_interaction(assertion),
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
                    vec![table.clone()],
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
                    vec![table.clone()],
                ));

                let mut interactions = Vec::new();
                interactions.push(InteractionBuilder::with_interaction(assumption));
                interactions.push(InteractionBuilder::with_interaction(delete));
                interactions.extend(queries.clone().into_iter().map(|q| {
                    let mut builder =
                        InteractionBuilder::with_interaction(InteractionType::Query(q));
                    builder.property_meta(PropertyMetadata::new(self, true));
                    builder
                }));
                interactions.push(InteractionBuilder::with_interaction(select));
                interactions.push(InteractionBuilder::with_interaction(assertion));

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
                    vec![table.clone()],
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
                            Err(e) => {
                                let err_str = e.to_string();
                                // Match both qualified ("no such table: aux1.foo")
                                // and bare ("no such table: foo") error messages
                                let bare_name = table_name
                                    .split_once('.')
                                    .map_or(table_name.as_str(), |(_, n)| n);
                                if err_str.contains(&format!("no such table: {table_name}"))
                                    || err_str.contains(&format!("no such table: {bare_name}"))
                                {
                                    Ok(Ok(()))
                                } else {
                                    Ok(Err(format!(
                                        "expected table does not exist error, got: {e}"
                                    )))
                                }
                            }
                        }
                    },
                    vec![table.clone()],
                ));

                let drop = InteractionType::Query(Query::Drop(Drop {
                    table: table.clone(),
                }));

                let select = InteractionType::Query(Query::Select(select.clone()));

                let mut interactions = Vec::new();

                interactions.push(InteractionBuilder::with_interaction(assumption));
                interactions.push(InteractionBuilder::with_interaction(drop));
                interactions.extend(queries.clone().into_iter().map(|q| {
                    let mut builder =
                        InteractionBuilder::with_interaction(InteractionType::Query(q));
                    builder.property_meta(PropertyMetadata::new(self, true));
                    builder
                }));
                interactions.push({
                    let mut builder = InteractionBuilder::with_interaction(select);
                    builder.ignore_error(true);
                    builder
                });
                interactions.push(InteractionBuilder::with_interaction(assertion));

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
                    vec![table.clone()],
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
                    vec![table.clone()],
                ));

                vec![
                    InteractionBuilder::with_interaction(assumption),
                    InteractionBuilder::with_interaction(select1),
                    InteractionBuilder::with_interaction(select2),
                    InteractionBuilder::with_interaction(assertion),
                ]
            }
            Property::FsyncNoWait { query } => {
                vec![InteractionBuilder::with_interaction(
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

                                if let LimboError::CheckpointFailed(msg) = err {
                                    // Checkpoint failure means the transaction is committed because the WAL commit succeeded, so we DON'T rollback,
                                    // and the results of the query are shadowed into the simulator environment
                                    query_clone
                                        .shadow(&mut env.get_conn_tables_mut(connection_index))
                                        .expect("Failed to shadow tables");
                                    tracing::error!(
                                        "Fault injection produced CheckpointFailed error: {msg}"
                                    );
                                    return Ok(Ok(()));
                                }

                                // On error we rollback the transaction if there is any active here
                                env.rollback_conn(connection_index);
                                Ok(Ok(()))
                            }
                        }
                    },
                    query.dependencies().into_iter().collect(),
                );
                [
                    InteractionType::FaultyQuery(query.clone()),
                    InteractionType::Assertion(assert),
                ]
                .into_iter()
                .map(InteractionBuilder::with_interaction)
                .collect()
            }
            Property::WhereTrueFalseNull { select, predicate } => {
                let tables_dependencies = select.dependencies().into_iter().collect::<Vec<_>>();
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
                    tables_dependencies.clone(),
                ));

                let old_predicate = select.body.select.where_clause.clone();

                let p_true = Predicate::and(vec![old_predicate.clone(), predicate.clone()]);
                let p_false = Predicate::and(vec![
                    old_predicate.clone(),
                    Predicate::not(predicate.clone()),
                ]);
                let p_null = Predicate::and(vec![
                    old_predicate,
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
                    tables_dependencies,
                ));

                vec![
                    InteractionBuilder::with_interaction(assumption),
                    InteractionBuilder::with_interaction(select),
                    InteractionBuilder::with_interaction(select_tlp),
                    InteractionBuilder::with_interaction(assertion),
                ]
            }
            Property::UnionAllPreservesCardinality {
                select,
                where_clause,
            } => {
                let s1 = select.clone();
                let mut s2 = select.clone();
                s2.body.select.where_clause = where_clause.clone();
                let s3 = Select::compound(s1.clone(), s2.clone(), CompoundOperator::UnionAll);

                vec![
                    InteractionType::Query(Query::Select(s1)),
                    InteractionType::Query(Query::Select(s2)),
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
                        s3.dependencies().into_iter().collect()
                    )
                ),
                ].into_iter().map(InteractionBuilder::with_interaction).collect()
            }
            Property::Queries { queries } => queries
                .clone()
                .into_iter()
                .map(|query| InteractionBuilder::with_interaction(InteractionType::Query(query)))
                .collect(),
            Property::SavepointRollback {
                queries, tables, ..
            } => {
                let savepoint_name = format!("sim_sp_{}", id.get());
                let mut interactions = Vec::with_capacity(queries.len() + 4 + tables.len() * 2);
                interactions.push(InteractionBuilder::with_interaction(
                    InteractionType::Query(Query::Savepoint(Savepoint {
                        name: savepoint_name.clone(),
                    })),
                ));
                interactions.extend(queries.clone().into_iter().map(|query| {
                    InteractionBuilder::with_interaction(InteractionType::Query(query))
                }));
                interactions.push(InteractionBuilder::with_interaction(
                    InteractionType::Query(Query::RollbackToSavepoint(RollbackToSavepoint {
                        name: savepoint_name.clone(),
                    })),
                ));
                interactions.push(InteractionBuilder::with_interaction(
                    InteractionType::Query(Query::ReleaseSavepoint(ReleaseSavepoint {
                        name: savepoint_name,
                    })),
                ));
                interactions.extend(assert_all_table_values(tables, connection_index));
                interactions.push(assert_integrity_check(tables, connection_index));
                interactions
            }
        };

        assert!(!interactions.is_empty());

        interactions
            .into_iter()
            .map(|mut builder| {
                if !builder.has_property_meta() {
                    builder.property_meta(PropertyMetadata::new(self, false));
                }
                builder.connection_index(connection_index).id(id);
                builder.build().unwrap()
            })
            .collect()
    }
}

fn random_main_table_write<R: rand::Rng + ?Sized>(
    rng: &mut R,
    ctx: &impl GenerationContext,
    write_kinds: &[QueryDiscriminants],
) -> Option<Query> {
    let tables = ctx
        .tables()
        .iter()
        .filter(|table| !table.name.contains('.'))
        .collect::<Vec<_>>();

    if tables.is_empty() {
        return None;
    }

    let table = *pick(&tables, rng);
    let write_kind = pick(write_kinds, rng);

    match write_kind {
        QueryDiscriminants::Insert => Some(random_main_table_insert(rng, ctx, table)),
        QueryDiscriminants::Update => Some(random_main_table_update(rng, ctx, table)),
        QueryDiscriminants::Delete => Some(random_main_table_delete(rng, table)),
        _ => None,
    }
}

fn random_main_table_insert<R: rand::Rng + ?Sized>(
    rng: &mut R,
    ctx: &impl GenerationContext,
    table: &Table,
) -> Query {
    const UNIQUE_BASE_OFFSET_RANGE: std::ops::Range<i64> = 3_000_000_000..4_000_000_000;
    const UNIQUE_COL_STRIDE: i64 = 10_000_000;

    let num_rows = rng.random_range(1..=5);
    let base_offset: i64 = rng.random_range(UNIQUE_BASE_OFFSET_RANGE);
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
                        SimValue::unique_for_type(&column.column_type, offset)
                    } else {
                        SimValue::arbitrary_from(rng, ctx, &column.column_type)
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

fn random_main_table_update<R: rand::Rng + ?Sized>(
    rng: &mut R,
    ctx: &impl GenerationContext,
    table: &Table,
) -> Query {
    let update_opts = &ctx.opts().query.update;
    let unique_columns = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            column.has_unique_or_pk() && !matches!(column.column_type, ColumnType::Blob)
        })
        .collect::<Vec<_>>();
    let non_unique_columns = table
        .columns
        .iter()
        .filter(|column| !column.has_unique_or_pk())
        .collect::<Vec<_>>();

    let last_row_idx = table.rows.len().saturating_sub(1);
    let conflict_capable_columns = if table.rows.len() >= 2 {
        unique_columns
            .iter()
            .filter(|(col_idx, _)| table.rows[0][*col_idx] != table.rows[last_row_idx][*col_idx])
            .copied()
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    if update_opts.force_late_failure
        && !conflict_capable_columns.is_empty()
        && !non_unique_columns.is_empty()
    {
        let (col_idx, unique_col) = *pick(&conflict_capable_columns, rng);
        let marker_col = *pick(&non_unique_columns, rng);
        let first_val = table.rows[0][col_idx].clone();
        let last_val = table.rows[last_row_idx][col_idx].clone();
        let marker_value = match update_opts.padding_size {
            Some(size) if matches!(marker_col.column_type, ColumnType::Blob) => {
                SimValue(turso_core::Value::Blob(vec![b'X'; size]))
            }
            Some(size) => SimValue(turso_core::Value::Text("X".repeat(size).into())),
            None => SimValue::arbitrary_from(rng, ctx, &marker_col.column_type),
        };

        return Query::Update(Update {
            table: table.name.clone(),
            set_values: vec![
                (marker_col.name.clone(), SetValue::Simple(marker_value)),
                (
                    unique_col.name.clone(),
                    SetValue::CaseWhen {
                        condition: Box::new(Predicate::eq(
                            Predicate::column(unique_col.name.clone()),
                            Predicate::value(last_val),
                        )),
                        then_value: first_val,
                        else_column: unique_col.name.clone(),
                    },
                ),
            ],
            predicate: Predicate::true_(),
        });
    }

    let column = if non_unique_columns.is_empty() {
        pick(&table.columns, rng)
    } else {
        *pick(&non_unique_columns, rng)
    };
    let value = match (update_opts.padding_size, &column.column_type) {
        (Some(size), ColumnType::Blob) => SimValue(turso_core::Value::Blob(vec![b'X'; size])),
        (Some(size), ColumnType::Text) => {
            SimValue(turso_core::Value::Text("X".repeat(size).into()))
        }
        _ => SimValue::arbitrary_from(rng, ctx, &column.column_type),
    };

    Query::Update(Update {
        table: table.name.clone(),
        set_values: vec![(column.name.clone(), SetValue::Simple(value))],
        predicate: if rng.random_bool(0.8) {
            Predicate::true_()
        } else {
            Predicate::false_()
        },
    })
}

fn random_main_table_delete<R: rand::Rng + ?Sized>(rng: &mut R, table: &Table) -> Query {
    Query::Delete(Delete {
        table: table.name.clone(),
        predicate: if rng.random_bool(0.5) {
            Predicate::true_()
        } else {
            Predicate::false_()
        },
    })
}

fn assert_integrity_check(tables: &[String], connection_index: usize) -> InteractionBuilder {
    let tables = tables.to_vec();
    InteractionBuilder::with_interaction(InteractionType::Assertion(Assertion::new(
        "PRAGMA integrity_check should be ok after savepoint rollback".to_string(),
        move |_stack: &Vec<ResultSet>, env: &mut SimulatorEnv| {
            let result = run_integrity_check(env, connection_index)?;
            if result == "ok" {
                Ok(Ok(()))
            } else {
                Ok(Err(format!("integrity_check returned {result:?}")))
            }
        },
        tables,
    )))
}

fn run_integrity_check(
    env: &mut SimulatorEnv,
    connection_index: usize,
) -> turso_core::Result<String> {
    match &mut env.connections[connection_index] {
        crate::runner::env::SimConnection::LimboConnection(conn) => {
            let mut rows = conn.query("PRAGMA integrity_check")?.ok_or_else(|| {
                LimboError::InternalError("integrity_check returned no rows".into())
            })?;
            let mut result = Vec::new();
            rows.run_with_row_callback(|row| {
                let value = row
                    .get_value(0)
                    .to_text()
                    .ok_or_else(|| {
                        LimboError::InternalError(
                            "integrity_check returned a non-text value".to_string(),
                        )
                    })?
                    .to_string();
                result.push(value);
                Ok(())
            })?;
            Ok(result.join("\n"))
        }
        crate::runner::env::SimConnection::SQLiteConnection(conn) => {
            let mut stmt = conn
                .prepare("PRAGMA integrity_check")
                .map_err(|e| LimboError::InternalError(e.to_string()))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| LimboError::InternalError(e.to_string()))?;
            let mut result = Vec::new();
            for row in rows {
                result.push(row.map_err(|e| LimboError::InternalError(e.to_string()))?);
            }
            Ok(result.join("\n"))
        }
        crate::runner::env::SimConnection::Disconnected => Err(LimboError::InternalError(
            "connection is disconnected during integrity_check assertion".into(),
        )),
    }
}

fn assert_all_table_values(
    tables: &[String],
    connection_index: usize,
) -> impl Iterator<Item = InteractionBuilder> + use<'_> {
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
            }, vec![table.clone()]));
        [select, assertion].into_iter().map(InteractionBuilder::with_interaction)
    })
}

fn property_insert_values_select<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());

    let non_unique_tables: Vec<_> = ctx
        .tables()
        .iter()
        .filter(|t| !t.has_any_unique_column())
        .collect();

    let table = if non_unique_tables.is_empty() {
        pick(ctx.tables(), rng)
    } else {
        *pick(&non_unique_tables, rng)
    };

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
        on_conflict: None,
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

    Property::ReadYourUpdatesBack {
        update,
        select_before: select.clone(),
        select_after: select,
    }
}

fn property_savepoint_rollback<R: rand::Rng + ?Sized>(
    rng: &mut R,
    query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    let tables = ctx
        .tables()
        .iter()
        .filter(|table| !table.name.contains('.'))
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    assert!(!tables.is_empty());
    let write_kinds = query_distr
        .positive_items()
        .filter(|query| {
            matches!(
                query,
                QueryDiscriminants::Insert
                    | QueryDiscriminants::Update
                    | QueryDiscriminants::Delete
            )
        })
        .collect::<Vec<_>>();
    assert!(!write_kinds.is_empty());
    let amount = rng.random_range(1..=5);
    Property::SavepointRollback {
        queries: std::iter::repeat_n(Query::Placeholder, amount).collect(),
        tables,
        write_kinds,
    }
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

fn property_each_column_matches_model<R: rand::Rng + ?Sized>(
    rng: &mut R,
    _query_distr: &QueryDistribution,
    ctx: &impl GenerationContext,
    _mvcc: bool,
) -> Property {
    assert!(!ctx.tables().is_empty());
    let table = pick(ctx.tables(), rng);
    Property::EachColumnMatchesModel {
        table: table.name.clone(),
        columns: table.columns.iter().map(|c| c.name.clone()).collect(),
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

    Property::UnionAllPreservesCardinality {
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

type PropertyGenFunc<R, G> = fn(&mut R, &QueryDistribution, &G, bool) -> Property;

impl PropertyDiscriminants {
    fn gen_function<R, G>(&self) -> PropertyGenFunc<R, G>
    where
        R: rand::Rng + ?Sized,
        G: GenerationContext,
    {
        match self {
            PropertyDiscriminants::InsertValuesSelect => property_insert_values_select,
            PropertyDiscriminants::ReadYourUpdatesBack => property_read_your_updates_back,
            PropertyDiscriminants::SavepointRollback => property_savepoint_rollback,
            PropertyDiscriminants::TableHasExpectedContent => property_table_has_expected_content,
            PropertyDiscriminants::EachColumnMatchesModel => property_each_column_matches_model,
            PropertyDiscriminants::AllTableHaveExpectedContent => {
                property_all_tables_have_expected_content
            }
            PropertyDiscriminants::DoubleCreateFailure => property_double_create_failure,
            PropertyDiscriminants::SelectLimit => property_select_limit,
            PropertyDiscriminants::DeleteSelect => property_delete_select,
            PropertyDiscriminants::DropSelect => property_drop_select,
            PropertyDiscriminants::SelectSelectOptimizer => property_select_select_optimizer,
            PropertyDiscriminants::WhereTrueFalseNull => property_where_true_false_null,
            PropertyDiscriminants::UnionAllPreservesCardinality => {
                property_union_all_preserves_cardinality
            }
            PropertyDiscriminants::FsyncNoWait => property_fsync_no_wait,
            PropertyDiscriminants::FaultyQuery => property_faulty_query,
            PropertyDiscriminants::Queries => {
                unreachable!("should not try to generate queries property")
            }
        }
    }

    fn weight(
        &self,
        env: &SimulatorEnv,
        remaining: &Remaining,
        ctx: &impl GenerationContext,
    ) -> u32 {
        match self {
            PropertyDiscriminants::InsertValuesSelect => {
                if !env.opts.disable_insert_values_select && !ctx.tables().is_empty() {
                    let has_non_unique_table =
                        ctx.tables().iter().any(|t| !t.has_any_unique_column());
                    if has_non_unique_table && remaining.select > 0 && remaining.insert > 0 {
                        u32::min(remaining.select, remaining.insert).max(1)
                    } else {
                        0 // all tables have UNIQUE columns, skip this property
                    }
                } else {
                    0
                }
            }

            PropertyDiscriminants::ReadYourUpdatesBack => {
                if remaining.select > 0 && remaining.update > 0 {
                    u32::min(remaining.select, remaining.update).max(1)
                } else {
                    0
                }
            }
            PropertyDiscriminants::SavepointRollback => {
                if !env.opts.disable_savepoint_rollback
                    && !env.profile.mvcc
                    && ctx.tables().iter().any(|table| !table.name.contains('.'))
                {
                    remaining.insert + remaining.update + remaining.delete
                } else {
                    0
                }
            }
            PropertyDiscriminants::TableHasExpectedContent => {
                if !ctx.tables().is_empty() {
                    remaining.select.max(1)
                } else {
                    0
                }
            }
            PropertyDiscriminants::EachColumnMatchesModel => {
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
                if !env.opts.disable_where_true_false_null && !ctx.tables().is_empty() {
                    remaining.select / 2
                } else {
                    0
                }
            }
            PropertyDiscriminants::UnionAllPreservesCardinality => {
                if !env.opts.disable_union_all_preserves_cardinality && !ctx.tables().is_empty() {
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
            PropertyDiscriminants::SavepointRollback => QueryCapabilities::INSERT,
            PropertyDiscriminants::TableHasExpectedContent => QueryCapabilities::SELECT,
            PropertyDiscriminants::EachColumnMatchesModel => QueryCapabilities::SELECT,
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
            PropertyDiscriminants::UnionAllPreservesCardinality => QueryCapabilities::SELECT,
            PropertyDiscriminants::FsyncNoWait => QueryCapabilities::all(),
            PropertyDiscriminants::FaultyQuery => QueryCapabilities::all(),
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
            mvcc: env.profile.mvcc,
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
            types::Value::Numeric(Numeric::Integer(i)) => i.to_string(),
            types::Value::Numeric(Numeric::Float(f)) => f.to_string(),
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
