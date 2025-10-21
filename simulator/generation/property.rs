use serde::{Deserialize, Serialize};
use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, frequency, pick, pick_index},
    model::{
        query::{
            Create, Delete, Drop, Insert, Select,
            predicate::Predicate,
            select::{CompoundOperator, CompoundSelect, ResultColumn, SelectBody, SelectInner},
            transaction::{Begin, Commit, Rollback},
            update::Update,
        },
        table::SimValue,
    },
};
use turso_core::{LimboError, types};
use turso_parser::ast::{self, Distinctness, Expr, Literal, Name};

use crate::{
    common::print_diff,
    generation::{
        Shadow as _,
        assertion::{Assertion, CmpOp, CountExpr, RelExpr},
        plan::{Control, InteractionType},
        value_to_literal,
    },
    model::Query,
    profiles::query::QueryProfile,
    runner::env::SimulatorEnv,
};

use super::plan::{Interaction, InteractionStats, ResultSet};

/// Properties are representations of executable specifications
/// about the database behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        tables: Vec<String>,
    },
    FaultyQuery {
        query: Query,
        tables: Vec<String>,
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

impl Property {
    pub(crate) fn name(&self) -> &str {
        match self {
            Property::InsertValuesSelect { .. } => "Insert-Values-Select",
            Property::ReadYourUpdatesBack { .. } => "Read-Your-Updates-Back",
            Property::TableHasExpectedContent { .. } => "Table-Has-Expected-Content",
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
        }
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
            | Property::TableHasExpectedContent { .. } => None,
        }
    }

    /// interactions construct a list of interactions, which is an executable representation of the property.
    /// the requirement of property -> vec<interaction> conversion emerges from the need to serialize the property,
    /// and `interaction` cannot be serialized directly.
    pub(crate) fn interactions(&self, connection_index: usize) -> Vec<Interaction> {
        match self {
            Property::TableHasExpectedContent { table } => {
                let assumption = InteractionType::Assumption(Assertion::table_exists(&table));
                let select_interaction = Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Select(Select::simple(
                        table.clone(),
                        Predicate::true_(),
                    ))),
                )
                .bind("q1".to_string());

                let assertion = InteractionType::Assertion(Assertion::shadow_equivalence(
                    "q1".to_string(),
                    table.clone(),
                    connection_index,
                ));

                vec![
                    Interaction::new(connection_index, assumption),
                    select_interaction,
                    Interaction::new(connection_index, assertion),
                ]
            }
            Property::ReadYourUpdatesBack { update, select } => {
                // Assumption: the target table exists on this connection
                let table = update.table().to_string();
                let assumption = Interaction::new(
                    connection_index,
                    InteractionType::Assumption(Assertion::SchemaHasTable {
                        conn_index: connection_index,
                        table: table.clone(),
                    }),
                );

                // 1) Execute the UPDATE
                let upd = Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Update(update.clone())),
                );

                // 2) Original SELECT (as provided)
                let q_sel_name = "q_sel".to_string();
                let sel_orig = Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Select(select.clone())),
                )
                .bind(q_sel_name.clone());

                // 3) Build a WHERE that adds col = value for each updated column
                //    (keep FROM, columns, distinctness, etc. identical to `select`)
                let mut eq_conjuncts: Vec<Predicate> = Vec::new();
                for (col, val) in &update.set_values {
                    // Expr/Predicate construction: adapt to your actual constructors
                    let lhs = Expr::Name(Name::exact(col.clone()));
                    let rhs = Expr::Literal(value_to_literal(&val.0));
                    eq_conjuncts.push(Predicate::eq(Predicate(lhs), Predicate(rhs)));
                }

                let mut where_chk_parts = vec![select.body.select.where_clause.clone()];
                where_chk_parts.extend(eq_conjuncts);
                let where_chk = Predicate::and(where_chk_parts);

                let mut select_chk = select.clone();
                select_chk.body.select.where_clause = where_chk;

                let q_chk_name = "q_chk".to_string();
                let sel_chk = Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Select(select_chk)),
                )
                .bind(q_chk_name.clone());

                // 4) Assertion: count(q_sel) == count(q_chk)
                let assertion = Interaction::new(
                    connection_index,
                    InteractionType::Assertion(Assertion::CountCmp {
                        left: CountExpr::Count(RelExpr::Var(q_sel_name.clone())),
                        op: CmpOp::Eq,
                        right: CountExpr::Count(RelExpr::Var(q_chk_name.clone())),
                    }),
                );

                vec![assumption, upd, sel_orig, sel_chk, assertion]
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
                let assumption = InteractionType::Assumption(Assertion::table_exists(&table));

                let assertion = InteractionType::Assertion(Assertion::expected_in_result_set(
                    "q1".to_string(),
                    vec![row.clone()],
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
                interactions.push(
                    Interaction::new(
                        connection_index,
                        InteractionType::Query(Query::Select(select.clone())),
                    )
                    .bind("q1".to_string()),
                );

                interactions.push(Interaction::new(connection_index, assertion));

                interactions
            }
            Property::DoubleCreateFailure { create, queries } => {
                let assumption = InteractionType::Assumption(Assertion::table_should_not_exist(
                    &create.table.name,
                    connection_index,
                ));

                let cq1 = InteractionType::Query(Query::Create(create.clone()));
                let cq2 = InteractionType::Query(Query::Create(create.clone()));

                let assertion = InteractionType::Assertion(Assertion::expect_error(
                    "q2".to_string(),
                    format!("table {} already exists", &create.table.name),
                    "creating two tables with the same name should result in a failure for the second query".to_string(),
                ));

                let mut interactions = Vec::new();
                interactions.push(Interaction::new(connection_index, assumption));
                interactions.push(Interaction::new(connection_index, cq1));
                interactions.extend(
                    queries
                        .clone()
                        .into_iter()
                        .map(|q| Interaction::new(connection_index, InteractionType::Query(q))),
                );
                interactions.push(
                    Interaction::new_ignore_error(connection_index, cq2).bind("q2".to_string()),
                );
                interactions.push(Interaction::new(connection_index, assertion));

                interactions
            }
            Property::SelectLimit { select } => {
                let assumptions = select
                    .dependencies()
                    .iter()
                    .map(|table| Assertion::table_exists(table))
                    .collect::<Vec<Assertion>>();

                let limit = select
                    .limit
                    .expect("Property::SelectLimit without a LIMIT clause");

                let assertion = InteractionType::Assertion(Assertion::compare_result_length(
                    "q1".to_string(),
                    limit,
                    "<=",
                    "select query should respect the limit clause",
                ));

                let mut interactions = Vec::new();
                interactions.extend(
                    assumptions.into_iter().map(|a| {
                        Interaction::new(connection_index, InteractionType::Assumption(a))
                    }),
                );
                interactions.push(Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Select(select.clone())),
                ));
                interactions.push(Interaction::new(connection_index, assertion));
                interactions
            }
            Property::DeleteSelect {
                table,
                predicate,
                queries,
            } => {
                let assumption = InteractionType::Assumption(Assertion::table_exists(table));

                let delete = InteractionType::Query(Query::Delete(Delete {
                    table: table.clone(),
                    predicate: predicate.clone(),
                }));

                let select = Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Select(Select::simple(
                        table.clone(),
                        predicate.clone(),
                    ))),
                )
                .bind("q1".to_string());

                let assertion = InteractionType::Assertion(Assertion::expected_result(
                    "q1".to_string(),
                    vec![],
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
                interactions.push(select);
                interactions.push(Interaction::new(connection_index, assertion));

                interactions
            }
            Property::DropSelect {
                table,
                queries,
                select,
            } => {
                let assumption = InteractionType::Assumption(Assertion::table_exists(&table));

                let drop = InteractionType::Query(Query::Drop(Drop {
                    table: table.clone(),
                }));

                let select = Interaction::new(
                    connection_index,
                    InteractionType::Query(Query::Select(select.clone())),
                )
                .bind("q1".to_string());

                let assertion = InteractionType::Assertion(Assertion::expect_error(
                    "q1".to_string(),
                    format!("Table {} does not exist", table),
                    "selecting from a dropped table should result in an error".to_string(),
                ));

                let mut interactions = Vec::new();

                interactions.push(Interaction::new(connection_index, assumption));
                interactions.push(Interaction::new(connection_index, drop));
                interactions.extend(
                    queries
                        .clone()
                        .into_iter()
                        .map(|q| Interaction::new(connection_index, InteractionType::Query(q))),
                );
                interactions.push(select);
                interactions.push(Interaction::new(connection_index, assertion));

                interactions
            }
            Property::SelectSelectOptimizer { table, predicate } => {
                // Assumption: table exists
                let assumption = Interaction::new(
                    connection_index,
                    InteractionType::Assumption(Assertion::SchemaHasTable {
                        conn_index: connection_index,
                        table: table.clone(),
                    }),
                );

                // Expr-path count: COALESCE(SUM(CASE WHEN <p> THEN 1 ELSE 0 END), 0)
                let q_count_expr = Query::Select(Select::single(
                    table.clone(),
                    vec![ResultColumn::Expr(Predicate(Expr::fun(
                        Name::exact("COALESCE".to_string()),
                        vec![
                            Expr::fun(
                                Name::exact("SUM".to_string()),
                                vec![Expr::case_when(
                                    None,
                                    vec![(predicate.0.clone(), Expr::lit_integer(1))],
                                    Some(Expr::lit_integer(0)),
                                )],
                            ),
                            Expr::lit_integer(0),
                        ],
                    )))],
                    Predicate::true_(),
                    None,
                    Distinctness::All,
                ));

                // WHERE-path count: COUNT(*)
                let q_count_where = Query::Select(Select::single(
                    table.clone(),
                    vec![ResultColumn::Expr(Predicate(Expr::fun(
                        Name::exact("COUNT".to_string()),
                        vec![Expr::lit_string("*".to_string())],
                    )))],
                    predicate.clone(), // WHERE <predicate>
                    None,
                    Distinctness::All,
                ));

                // Bind names
                let c_expr = "c_expr".to_string();
                let c_where = "c_where".to_string();

                // Assert the two 1×1 relations are equal (bag semantics == exact equality here)
                let assertion = Assertion::EqBag {
                    left: RelExpr::Var(c_expr.clone()),
                    right: RelExpr::Var(c_where.clone()),
                };

                vec![
                    assumption,
                    Interaction::new(connection_index, InteractionType::Query(q_count_expr))
                        .bind(c_expr),
                    Interaction::new(connection_index, InteractionType::Query(q_count_where))
                        .bind(c_where),
                    Interaction::new(connection_index, InteractionType::Assertion(assertion)),
                ]
            }

            Property::FsyncNoWait { query, tables } => {
                let checks = assert_all_table_values(tables, connection_index);
                Vec::from_iter(
                    std::iter::once(Interaction::new(
                        connection_index,
                        InteractionType::FsyncQuery(query.clone()),
                    ))
                    .chain(checks),
                )
            }
            Property::FaultyQuery { query, tables } => {
                // Optionally: table-existence assumptions to abort early if needed
                let assumptions = tables.iter().map(|t| {
                    Interaction::new(
                        connection_index,
                        InteractionType::Assumption(Assertion::SchemaHasTable {
                            conn_index: connection_index,
                            table: t.clone(),
                        }),
                    )
                });

                // Execute the faulty query and bind its result (Ok or Err) under a name
                let fq = "faulty_q".to_string();

                // Control flow:
                // - If Ok  -> shadow the effects into simulator tables
                // - If Err -> rollback any active txn on this connection
                let shadow_if_ok = Interaction::new(
                    connection_index,
                    InteractionType::Control(Control::ShadowIfOk {
                        from: fq.clone(),
                        apply: query.clone(),
                        conn_index: connection_index,
                    }),
                );
                let rollback_if_err = Interaction::new(
                    connection_index,
                    InteractionType::Control(Control::RollbackIfErr {
                        from: fq.clone(),
                        conn_index: connection_index,
                    }),
                );

                // After normalizing the simulator state via the control step above,
                // run your existing content checks (compare actual DB vs simulator snapshots).
                // `assert_all_table_values` should return a sequence of Interaction::Assertion(...)
                let checks = assert_all_table_values(tables, connection_index);

                let first = [
                    Interaction::new(
                        connection_index,
                        InteractionType::FaultyQuery(query.clone()),
                    )
                    .bind(fq),
                    shadow_if_ok,
                    rollback_if_err,
                ];

                first
                    .into_iter()
                    .chain(assumptions)
                    .chain(checks)
                    .collect::<Vec<_>>()
            }
            Property::WhereTrueFalseNull { select, predicate } => {
                // Assumptions: all dependent tables exist on this connection
                let assumptions: Vec<Interaction> = select
                    .dependencies()
                    .into_iter()
                    .map(|table| {
                        Interaction::new(
                            connection_index,
                            InteractionType::Assumption(Assertion::SchemaHasTable {
                                conn_index: connection_index,
                                table,
                            }),
                        )
                    })
                    .collect();

                // Keep the original WHERE (if any)
                let old_pred = select.body.select.where_clause.clone();

                // Build the three TLP partitions: TRUE / FALSE / NULL
                let p_true = Predicate::and(vec![old_pred.clone(), predicate.clone()]);
                let p_false =
                    Predicate::and(vec![old_pred.clone(), Predicate::not(predicate.clone())]);
                let p_null = Predicate::and(vec![
                    old_pred.clone(),
                    Predicate::is(predicate.clone(), Predicate::null()),
                ]);

                // Original SELECT (baseline)
                let sel_orig = Query::Select(select.clone());

                // TLP UNION ALL query: (old AND p) UNION ALL (old AND NOT p) UNION ALL (old AND p IS NULL)
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
                let sel_tlp = Query::Select(select_tlp);

                // Bind names (use your fresh-name generator if available)
                let q_orig = "q_orig".to_string();
                let q_tlp = "q_tlp".to_string();

                // Assertion: bag equality of rows (order-insensitive, multiplicity-aware)
                //   distinctness of the base query is preserved in all three branches above,
                //   so EqBag is the right semantics.
                let assertion = Assertion::EqBag {
                    left: RelExpr::Var(q_orig.clone()),
                    right: RelExpr::Var(q_tlp.clone()),
                };

                let mut interactions: Vec<Interaction> = Vec::new();
                interactions.extend(assumptions);
                interactions.push(
                    Interaction::new(connection_index, InteractionType::Query(sel_orig))
                        .bind(q_orig),
                );
                interactions.push(
                    Interaction::new(connection_index, InteractionType::Query(sel_tlp)).bind(q_tlp),
                );
                interactions.push(Interaction::new(
                    connection_index,
                    InteractionType::Assertion(assertion),
                ));

                interactions
            }
            Property::UNIONAllPreservesCardinality {
                select,
                where_clause,
            } => {
                // Build the three queries
                let s1 = select.clone(); // Q1: original
                let mut s2 = select.clone(); // Q2: with WHERE
                s2.body.select.where_clause = where_clause.clone();
                let s3 = Select::compound(s1.clone(), s2.clone(), CompoundOperator::UnionAll); // U = Q1 UNION ALL Q2

                // Unique names for bound results (use your fresh-name generator if you have one)
                let q1 = "q1".to_string();
                let q2 = "q2".to_string();
                let u = "u".to_string();

                // AST: count(u) == count(q1) + count(q2)
                let assertion = Assertion::CountCmp {
                    left: CountExpr::Count(RelExpr::Var(u.clone())),
                    op: CmpOp::Eq,
                    right: CountExpr::Add(
                        Box::new(CountExpr::Count(RelExpr::Var(q1.clone()))),
                        Box::new(CountExpr::Count(RelExpr::Var(q2.clone()))),
                    ),
                };

                vec![
                    Interaction::new(
                        connection_index,
                        InteractionType::Query(Query::Select(s1.clone())),
                    )
                    .bind(q1),
                    Interaction::new(
                        connection_index,
                        InteractionType::Query(Query::Select(s2.clone())),
                    )
                    .bind(q2),
                    Interaction::new(
                        connection_index,
                        InteractionType::Query(Query::Select(s3.clone())),
                    )
                    .bind(u),
                    Interaction::new(connection_index, InteractionType::Assertion(assertion)),
                ]
            }
            Property::Queries { queries } => queries
                .clone()
                .into_iter()
                .map(|query| Interaction::new(connection_index, InteractionType::Query(query)))
                .collect(),
        }
    }
}

fn assert_all_table_values(
    tables: &[String],
    connection_index: usize,
) -> impl Iterator<Item = Interaction> + use<'_> {
    tables.iter().flat_map(move |table| {
        let select = Interaction::new(
            connection_index,
            InteractionType::Query(Query::Select(Select::simple(
                table.clone(),
                Predicate::true_(),
            ))),
        )
        .bind("q".to_string());

        let assertion = Interaction::new(
            connection_index,
            InteractionType::Assertion(Assertion::shadow_equivalence(
                "q".to_string(),
                table.clone(),
                connection_index,
            )),
        );

        [select, assertion].into_iter()
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

fn property_insert_values_select<R: rand::Rng>(
    rng: &mut R,
    remaining: &Remaining,
    ctx: &impl GenerationContext,
    mvcc: bool,
) -> Property {
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
    let insert_query = Insert::Values {
        table: table.name.clone(),
        values: rows,
    };

    // Choose if we want queries to be executed in an interactive transaction
    let interactive = if !mvcc && rng.random_bool(0.5) {
        Some(InteractiveQueryInfo {
            start_with_immediate: rng.random_bool(0.5),
            end_with_commit: rng.random_bool(0.5),
        })
    } else {
        None
    };
    // Create random queries respecting the constraints
    let mut queries = Vec::new();
    // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
    // - [x] The inserted row will not be deleted.
    // - [x] The inserted row will not be updated.
    // - [ ] The table `t` will not be renamed, dropped, or altered. (todo: add this constraint once ALTER or DROP is implemented)
    if let Some(ref interactive) = interactive {
        queries.push(Query::Begin(if interactive.start_with_immediate {
            Begin::Immediate
        } else {
            Begin::Deferred
        }));
    }
    for _ in 0..rng.random_range(0..3) {
        let query = Query::arbitrary_from(rng, ctx, remaining);
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
            Query::Update(Update {
                table: t,
                set_values: _,
                predicate,
            }) => {
                // The inserted row will not be updated.
                if t == &table.name && predicate.test(&row, table) {
                    continue;
                }
            }
            _ => (),
        }
        queries.push(query);
    }
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
        insert: insert_query,
        row_index,
        queries,
        select: select_query,
        interactive,
    }
}

fn property_read_your_updates_back<R: rand::Rng>(
    rng: &mut R,
    ctx: &impl GenerationContext,
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

fn property_table_has_expected_content<R: rand::Rng>(
    rng: &mut R,
    ctx: &impl GenerationContext,
) -> Property {
    // Get a random table
    let table = pick(ctx.tables(), rng);
    Property::TableHasExpectedContent {
        table: table.name.clone(),
    }
}

fn property_select_limit<R: rand::Rng>(rng: &mut R, ctx: &impl GenerationContext) -> Property {
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

fn property_double_create_failure<R: rand::Rng>(
    rng: &mut R,
    remaining: &Remaining,
    ctx: &impl GenerationContext,
) -> Property {
    // Create the table
    let create_query = Create::arbitrary(rng, ctx);
    let table = &create_query.table;

    // Create random queries respecting the constraints
    let mut queries = Vec::new();
    // The interactions in the middle has the following constraints;
    // - [x] There will be no errors in the middle interactions.(best effort)
    // - [ ] Table `t` will not be renamed or dropped.(todo: add this constraint once ALTER or DROP is implemented)
    for _ in 0..rng.random_range(0..3) {
        let query = Query::arbitrary_from(rng, ctx, remaining);
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
    remaining: &Remaining,
    ctx: &impl GenerationContext,
) -> Property {
    // Get a random table
    let table = pick(ctx.tables(), rng);
    // Generate a random predicate
    let predicate = Predicate::arbitrary_from(rng, ctx, table);

    // Create random queries respecting the constraints
    let mut queries = Vec::new();
    // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
    // - [x] A row that holds for the predicate will not be inserted.
    // - [ ] The table `t` will not be renamed, dropped, or altered. (todo: add this constraint once ALTER or DROP is implemented)
    for _ in 0..rng.random_range(0..3) {
        let query = Query::arbitrary_from(rng, ctx, remaining);
        match &query {
            Query::Insert(Insert::Values { table: t, values }) => {
                // A row that holds for the predicate will not be inserted.
                if t == &table.name && values.iter().any(|v| predicate.test(v, table)) {
                    continue;
                }
            }
            Query::Insert(Insert::Select {
                table: t,
                select: _,
            }) => {
                // A row that holds for the predicate will not be inserted.
                if t == &table.name {
                    continue;
                }
            }
            Query::Update(Update { table: t, .. }) => {
                // A row that holds for the predicate will not be updated.
                if t == &table.name {
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
    remaining: &Remaining,
    ctx: &impl GenerationContext,
) -> Property {
    // Get a random table
    let table = pick(ctx.tables(), rng);

    // Create random queries respecting the constraints
    let mut queries = Vec::new();
    // - [x] There will be no errors in the middle interactions. (this constraint is impossible to check, so this is just best effort)
    // - [-] The table `t` will not be created, no table will be renamed to `t`. (todo: update this constraint once ALTER is implemented)
    for _ in 0..rng.random_range(0..3) {
        let query = Query::arbitrary_from(rng, ctx, remaining);
        if let Query::Create(Create { table: t }) = &query {
            // - The table `t` will not be created
            if t.name == table.name {
                continue;
            }
        }
        queries.push(query);
    }

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

fn property_select_select_optimizer<R: rand::Rng>(
    rng: &mut R,
    ctx: &impl GenerationContext,
) -> Property {
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

fn property_where_true_false_null<R: rand::Rng>(
    rng: &mut R,
    ctx: &impl GenerationContext,
) -> Property {
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

fn property_union_all_preserves_cardinality<R: rand::Rng>(
    rng: &mut R,
    ctx: &impl GenerationContext,
) -> Property {
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

fn property_fsync_no_wait<R: rand::Rng>(
    rng: &mut R,
    remaining: &Remaining,
    ctx: &impl GenerationContext,
) -> Property {
    Property::FsyncNoWait {
        query: Query::arbitrary_from(rng, ctx, remaining),
        tables: ctx.tables().iter().map(|t| t.name.clone()).collect(),
    }
}

fn property_faulty_query<R: rand::Rng>(
    rng: &mut R,
    remaining: &Remaining,
    ctx: &impl GenerationContext,
) -> Property {
    Property::FaultyQuery {
        query: Query::arbitrary_from(rng, ctx, remaining),
        tables: ctx.tables().iter().map(|t| t.name.clone()).collect(),
    }
}

impl ArbitraryFrom<(&SimulatorEnv, &InteractionStats)> for Property {
    fn arbitrary_from<R: rand::Rng, C: GenerationContext>(
        rng: &mut R,
        conn_ctx: &C,
        (env, stats): (&SimulatorEnv, &InteractionStats),
    ) -> Self {
        let opts = conn_ctx.opts();
        let remaining_ = remaining(
            env.opts.max_interactions,
            &env.profile.query,
            stats,
            env.profile.experimental_mvcc,
        );

        #[allow(clippy::type_complexity)]
        let choices: Vec<(_, Box<dyn Fn(&mut R) -> Property>)> = vec![
            (
                if !env.opts.disable_insert_values_select {
                    u32::min(remaining_.select, remaining_.insert).max(1)
                } else {
                    0
                },
                Box::new(|rng: &mut R| {
                    property_insert_values_select(
                        rng,
                        &remaining_,
                        conn_ctx,
                        env.profile.experimental_mvcc,
                    )
                }),
            ),
            (
                remaining_.select.max(1),
                Box::new(|rng: &mut R| property_table_has_expected_content(rng, conn_ctx)),
            ),
            (
                u32::min(remaining_.select, remaining_.insert).max(1),
                Box::new(|rng: &mut R| property_read_your_updates_back(rng, conn_ctx)),
            ),
            (
                if !env.opts.disable_double_create_failure {
                    remaining_.create / 2
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_double_create_failure(rng, &remaining_, conn_ctx)),
            ),
            (
                if !env.opts.disable_select_limit {
                    remaining_.select
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_select_limit(rng, conn_ctx)),
            ),
            (
                if !env.opts.disable_delete_select {
                    u32::min(remaining_.select, remaining_.insert).min(remaining_.delete)
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_delete_select(rng, &remaining_, conn_ctx)),
            ),
            (
                if !env.opts.disable_drop_select {
                    // remaining_.drop
                    0
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_drop_select(rng, &remaining_, conn_ctx)),
            ),
            (
                if !env.opts.disable_select_optimizer {
                    remaining_.select / 2
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_select_select_optimizer(rng, conn_ctx)),
            ),
            (
                if opts.indexes && !env.opts.disable_where_true_false_null {
                    remaining_.select / 2
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_where_true_false_null(rng, conn_ctx)),
            ),
            (
                if opts.indexes && !env.opts.disable_union_all_preserves_cardinality {
                    remaining_.select / 3
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_union_all_preserves_cardinality(rng, conn_ctx)),
            ),
            (
                if env.profile.io.enable && !env.opts.disable_fsync_no_wait {
                    50 // Freestyle number
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_fsync_no_wait(rng, &remaining_, conn_ctx)),
            ),
            (
                if env.profile.io.enable
                    && env.profile.io.fault.enable
                    && !env.opts.disable_faulty_query
                {
                    20
                } else {
                    0
                },
                Box::new(|rng: &mut R| property_faulty_query(rng, &remaining_, conn_ctx)),
            ),
        ];

        frequency(choices, rng)
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
