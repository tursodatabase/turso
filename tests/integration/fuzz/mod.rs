pub mod grammar_generator;

#[cfg(test)]
mod tests {
    use rand::seq::{IndexedRandom, SliceRandom};
    use std::collections::HashSet;

    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use rusqlite::{params, types::Value};

    use crate::{
        common::{
            do_flush, limbo_exec_rows, limbo_exec_rows_fallible, rng_from_time, sqlite_exec_rows,
            TempDatabase,
        },
        fuzz::grammar_generator::{const_str, rand_int, rand_str, GrammarGenerator},
    };

    use super::grammar_generator::SymbolHandle;

    /// [See this issue for more info](https://github.com/tursodatabase/turso/issues/1763)
    #[test]
    pub fn fuzz_failure_issue_1763() {
        let db = TempDatabase::new_empty(false);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
        let offending_query = "SELECT ((ceil(pow((((2.0))), (-2.0 - -1.0) / log(0.5)))) - -2.0)";
        let limbo_result = limbo_exec_rows(&db, &limbo_conn, offending_query);
        let sqlite_result = sqlite_exec_rows(&sqlite_conn, offending_query);
        assert_eq!(
            limbo_result, sqlite_result,
            "query: {offending_query}, limbo: {limbo_result:?}, sqlite: {sqlite_result:?}"
        );
    }

    #[test]
    pub fn arithmetic_expression_fuzz_ex1() {
        let db = TempDatabase::new_empty(false);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        for query in [
            "SELECT ~1 >> 1536",
            "SELECT ~ + 3 << - ~ (~ (8)) - + -1 - 3 >> 3 + -6 * (-7 * 9 >> - 2)",
        ] {
            let limbo = limbo_exec_rows(&db, &limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}"
            );
        }
    }

    #[test]
    pub fn rowid_seek_fuzz() {
        let db = TempDatabase::new_with_rusqlite(
            "CREATE TABLE t (x INTEGER PRIMARY KEY autoincrement)",
            false,
        ); // INTEGER PRIMARY KEY is a rowid alias, so an index is not created
        let sqlite_conn = rusqlite::Connection::open(db.path.clone()).unwrap();

        let (mut rng, _seed) = rng_from_time_or_env();

        let mut values: Vec<i32> = Vec::with_capacity(3000);
        while values.len() < 3000 {
            let val = rng.random_range(-100000..100000);
            if !values.contains(&val) {
                values.push(val);
            }
        }
        let insert = format!(
            "INSERT INTO t VALUES {}",
            values
                .iter()
                .map(|x| format!("({x})"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        sqlite_conn.execute(&insert, params![]).unwrap();
        sqlite_conn.close().unwrap();
        let sqlite_conn = rusqlite::Connection::open(db.path.clone()).unwrap();
        let limbo_conn = db.connect_limbo();

        const COMPARISONS: [&str; 4] = ["<", "<=", ">", ">="];
        const ORDER_BY: [Option<&str>; 4] = [
            None,
            Some("ORDER BY x"),
            Some("ORDER BY x DESC"),
            Some("ORDER BY x ASC"),
        ];

        let (mut rng, seed) = rng_from_time_or_env();
        tracing::info!("rowid_seek_fuzz seed: {}", seed);

        for iteration in 0..2 {
            tracing::trace!("rowid_seek_fuzz iteration: {}", iteration);

            for comp in COMPARISONS.iter() {
                for order_by in ORDER_BY.iter() {
                    let test_values = generate_random_comparison_values(&mut rng);

                    for test_value in test_values.iter() {
                        let query = format!(
                            "SELECT * FROM t WHERE x {} {} {}",
                            comp,
                            test_value,
                            order_by.unwrap_or("")
                        );

                        log::trace!("query: {query}");
                        let limbo_result = limbo_exec_rows(&db, &limbo_conn, &query);
                        let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);
                        assert_eq!(
                            limbo_result, sqlite_result,
                            "query: {query}, limbo: {limbo_result:?}, sqlite: {sqlite_result:?}, seed: {seed}"
                        );
                    }
                }
            }
        }
    }

    fn generate_random_comparison_values(rng: &mut ChaCha8Rng) -> Vec<String> {
        let mut values = Vec::new();

        for _ in 0..1000 {
            let val = rng.random_range(-10000..10000);
            values.push(val.to_string());
        }

        values.push(i64::MAX.to_string());
        values.push(i64::MIN.to_string());
        values.push("0".to_string());

        for _ in 0..5 {
            let val: f64 = rng.random_range(-10000.0..10000.0);
            values.push(val.to_string());
        }

        values.push("NULL".to_string()); // Man's greatest mistake
        values.push("'NULL'".to_string()); // SQLite dared to one up on that mistake
        values.push("0.0".to_string());
        values.push("-0.0".to_string());
        values.push("1.5".to_string());
        values.push("-1.5".to_string());
        values.push("999.999".to_string());

        values.push("'text'".to_string());
        values.push("'123'".to_string());
        values.push("''".to_string());
        values.push("'0'".to_string());
        values.push("'hello'".to_string());

        values.push("'0x10'".to_string());
        values.push("'+123'".to_string());
        values.push("' 123 '".to_string());
        values.push("'1.5e2'".to_string());
        values.push("'inf'".to_string());
        values.push("'-inf'".to_string());
        values.push("'nan'".to_string());

        values.push("X'41'".to_string());
        values.push("X''".to_string());

        values.push("(1 + 1)".to_string());
        // values.push("(SELECT 1)".to_string()); subqueries ain't implemented yet homes.

        values
    }

    fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
        let seed = std::env::var("SEED").map_or(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            |v| {
                v.parse()
                    .expect("Failed to parse SEED environment variable as u64")
            },
        );
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    }

    #[test]
    pub fn index_scan_fuzz() {
        let db = TempDatabase::new_with_rusqlite("CREATE TABLE t (x PRIMARY KEY)", true);
        let sqlite_conn = rusqlite::Connection::open(db.path.clone()).unwrap();

        let insert = format!(
            "INSERT INTO t VALUES {}",
            (0..10000)
                .map(|x| format!("({x})"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        sqlite_conn.execute(&insert, params![]).unwrap();
        sqlite_conn.close().unwrap();
        let sqlite_conn = rusqlite::Connection::open(db.path.clone()).unwrap();
        let limbo_conn = db.connect_limbo();

        const COMPARISONS: [&str; 5] = ["=", "<", "<=", ">", ">="];

        const ORDER_BY: [Option<&str>; 4] = [
            None,
            Some("ORDER BY x"),
            Some("ORDER BY x DESC"),
            Some("ORDER BY x ASC"),
        ];

        for comp in COMPARISONS.iter() {
            for order_by in ORDER_BY.iter() {
                for max in 0..=10000 {
                    let query = format!(
                        "SELECT * FROM t WHERE x {} {} {} LIMIT 3",
                        comp,
                        max,
                        order_by.unwrap_or(""),
                    );
                    let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
                    let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
                    assert_eq!(
                        limbo, sqlite,
                        "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}",
                    );
                }
            }
        }
    }

    #[test]
    /// A test for verifying that index seek+scan works correctly for compound keys
    /// on indexes with various column orderings.
    pub fn index_scan_compound_key_fuzz() {
        let (mut rng, seed) = if std::env::var("SEED").is_ok() {
            let seed = std::env::var("SEED").unwrap().parse::<u64>().unwrap();
            (ChaCha8Rng::seed_from_u64(seed), seed)
        } else {
            rng_from_time()
        };
        let table_defs: [&str; 8] = [
            "CREATE TABLE t (x, y, z, nonindexed_col, PRIMARY KEY (x, y, z))",
            "CREATE TABLE t (x, y, z, nonindexed_col, PRIMARY KEY (x desc, y, z))",
            "CREATE TABLE t (x, y, z, nonindexed_col, PRIMARY KEY (x, y desc, z))",
            "CREATE TABLE t (x, y, z, nonindexed_col, PRIMARY KEY (x, y, z desc))",
            "CREATE TABLE t (x, y, z, nonindexed_col, PRIMARY KEY (x desc, y desc, z))",
            "CREATE TABLE t (x, y, z, nonindexed_col, PRIMARY KEY (x desc, y, z desc))",
            "CREATE TABLE t (x, y, z, nonindexed_col, PRIMARY KEY (x, y desc, z desc))",
            "CREATE TABLE t (x, y, z, nonindexed_col, PRIMARY KEY (x desc, y desc, z desc))",
        ];
        // Create all different 3-column primary key permutations
        let dbs = [
            TempDatabase::new_with_rusqlite(table_defs[0], true),
            TempDatabase::new_with_rusqlite(table_defs[1], true),
            TempDatabase::new_with_rusqlite(table_defs[2], true),
            TempDatabase::new_with_rusqlite(table_defs[3], true),
            TempDatabase::new_with_rusqlite(table_defs[4], true),
            TempDatabase::new_with_rusqlite(table_defs[5], true),
            TempDatabase::new_with_rusqlite(table_defs[6], true),
            TempDatabase::new_with_rusqlite(table_defs[7], true),
        ];
        let mut pk_tuples = HashSet::new();
        while pk_tuples.len() < 100000 {
            pk_tuples.insert((
                rng.random_range(0..3000),
                rng.random_range(0..3000),
                rng.random_range(0..3000),
            ));
        }
        let mut tuples = Vec::new();
        for pk_tuple in pk_tuples {
            tuples.push(format!(
                "({}, {}, {}, {})",
                pk_tuple.0,
                pk_tuple.1,
                pk_tuple.2,
                rng.random_range(0..3000)
            ));
        }
        let insert = format!("INSERT INTO t VALUES {}", tuples.join(", "));

        // Insert all tuples into all databases
        let sqlite_conns = dbs
            .iter()
            .map(|db| rusqlite::Connection::open(db.path.clone()).unwrap())
            .collect::<Vec<_>>();
        for sqlite_conn in sqlite_conns.into_iter() {
            sqlite_conn.execute(&insert, params![]).unwrap();
            sqlite_conn.close().unwrap();
        }
        let sqlite_conns = dbs
            .iter()
            .map(|db| rusqlite::Connection::open(db.path.clone()).unwrap())
            .collect::<Vec<_>>();
        let limbo_conns = dbs.iter().map(|db| db.connect_limbo()).collect::<Vec<_>>();

        const COMPARISONS: [&str; 5] = ["=", "<", "<=", ">", ">="];

        // For verifying index scans, we only care about cases where all but potentially the last column are constrained by an equality (=),
        // because this is the only way to utilize an index efficiently for seeking. This is called the "left-prefix rule" of indexes.
        // Hence we generate constraint combinations in this manner; as soon as a comparison is not an equality, we stop generating more constraints for the where clause.
        // Examples:
        // x = 1 AND y = 2 AND z > 3
        // x = 1 AND y > 2
        // x > 1
        let col_comp_first = COMPARISONS
            .iter()
            .cloned()
            .map(|x| (Some(x), None, None))
            .collect::<Vec<_>>();
        let col_comp_second = COMPARISONS
            .iter()
            .cloned()
            .map(|x| (Some("="), Some(x), None))
            .collect::<Vec<_>>();
        let col_comp_third = COMPARISONS
            .iter()
            .cloned()
            .map(|x| (Some("="), Some("="), Some(x)))
            .collect::<Vec<_>>();

        let all_comps = [col_comp_first, col_comp_second, col_comp_third].concat();

        const ORDER_BY: [Option<&str>; 3] = [None, Some("DESC"), Some("ASC")];

        const ITERATIONS: usize = 10000;
        for i in 0..ITERATIONS {
            if i % (ITERATIONS / 100) == 0 {
                println!(
                    "index_scan_compound_key_fuzz: iteration {}/{}",
                    i + 1,
                    ITERATIONS
                );
            }
            // let's choose random columns from the table
            let col_choices = ["x", "y", "z", "nonindexed_col"];
            let col_choices_weights = [10.0, 10.0, 10.0, 3.0];
            let num_cols_in_select = rng.random_range(1..=4);
            let mut select_cols = col_choices
                .choose_multiple_weighted(&mut rng, num_cols_in_select, |s| {
                    let idx = col_choices.iter().position(|c| c == s).unwrap();
                    col_choices_weights[idx]
                })
                .unwrap()
                .collect::<Vec<_>>()
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>();

            // sort select cols by index of col_choices
            select_cols.sort_by_cached_key(|x| col_choices.iter().position(|c| c == x).unwrap());

            let (comp1, comp2, comp3) = all_comps[rng.random_range(0..all_comps.len())];
            // Similarly as for the constraints, generate order by permutations so that the only columns involved in the index seek are potentially part of the ORDER BY.
            let (order_by1, order_by2, order_by3) = {
                if comp1.is_some() && comp2.is_some() && comp3.is_some() {
                    (
                        ORDER_BY[rng.random_range(0..ORDER_BY.len())],
                        ORDER_BY[rng.random_range(0..ORDER_BY.len())],
                        ORDER_BY[rng.random_range(0..ORDER_BY.len())],
                    )
                } else if comp1.is_some() && comp2.is_some() {
                    (
                        ORDER_BY[rng.random_range(0..ORDER_BY.len())],
                        ORDER_BY[rng.random_range(0..ORDER_BY.len())],
                        None,
                    )
                } else {
                    (ORDER_BY[rng.random_range(0..ORDER_BY.len())], None, None)
                }
            };

            // Generate random values for the WHERE clause constraints. Only involve primary key columns.
            let (col_val_first, col_val_second, col_val_third) = {
                if comp1.is_some() && comp2.is_some() && comp3.is_some() {
                    (
                        Some(rng.random_range(0..=3000)),
                        Some(rng.random_range(0..=3000)),
                        Some(rng.random_range(0..=3000)),
                    )
                } else if comp1.is_some() && comp2.is_some() {
                    (
                        Some(rng.random_range(0..=3000)),
                        Some(rng.random_range(0..=3000)),
                        None,
                    )
                } else {
                    (Some(rng.random_range(0..=3000)), None, None)
                }
            };

            // Use a small limit to make the test complete faster
            let limit = 5;

            // Generate WHERE clause string
            let where_clause_components = vec![
                comp1.map(|x| format!("x {} {}", x, col_val_first.unwrap())),
                comp2.map(|x| format!("y {} {}", x, col_val_second.unwrap())),
                comp3.map(|x| format!("z {} {}", x, col_val_third.unwrap())),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            let where_clause = if where_clause_components.is_empty() {
                "".to_string()
            } else {
                format!("WHERE {}", where_clause_components.join(" AND "))
            };

            // Generate ORDER BY string
            let order_by_components = vec![
                order_by1.map(|x| format!("x {x}")),
                order_by2.map(|x| format!("y {x}")),
                order_by3.map(|x| format!("z {x}")),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            let order_by = if order_by_components.is_empty() {
                "".to_string()
            } else {
                format!("ORDER BY {}", order_by_components.join(", "))
            };

            // Generate final query string
            let query = format!(
                "SELECT {} FROM t {} {} LIMIT {}",
                select_cols.join(", "),
                where_clause,
                order_by,
                limit
            );
            log::debug!("query: {query}");

            // Execute the query on all databases and compare the results
            for (i, sqlite_conn) in sqlite_conns.iter().enumerate() {
                let limbo = limbo_exec_rows(&dbs[i], &limbo_conns[i], &query);
                let sqlite = sqlite_exec_rows(sqlite_conn, &query);
                if limbo != sqlite {
                    // if the order by contains exclusively components that are constrained by an equality (=),
                    // sqlite sometimes doesn't bother with ASC/DESC because it doesn't semantically matter
                    // so we need to check that limbo and sqlite return the same results when the ordering is reversed.
                    // because we are generally using LIMIT (to make the test complete faster), we need to rerun the query
                    // without limit and then check that the results are the same if reversed.
                    let order_by_only_equalities = !order_by_components.is_empty()
                        && order_by_components.iter().all(|o: &String| {
                            if o.starts_with("x ") {
                                comp1 == Some("=")
                            } else if o.starts_with("y ") {
                                comp2 == Some("=")
                            } else {
                                comp3 == Some("=")
                            }
                        });

                    let query_no_limit =
                        format!("SELECT * FROM t {} {} {}", where_clause, order_by, "");
                    let limbo_no_limit = limbo_exec_rows(&dbs[i], &limbo_conns[i], &query_no_limit);
                    let sqlite_no_limit = sqlite_exec_rows(sqlite_conn, &query_no_limit);
                    let limbo_rev = limbo_no_limit.iter().cloned().rev().collect::<Vec<_>>();
                    if limbo_rev == sqlite_no_limit && order_by_only_equalities {
                        continue;
                    }

                    // finally, if the order by columns specified contain duplicates, sqlite might've returned the rows in an arbitrary different order.
                    // e.g. SELECT x,y,z FROM t ORDER BY x,y -- if there are duplicates on (x,y), the ordering returned might be different for limbo and sqlite.
                    // let's check this case and forgive ourselves if the ordering is different for this reason (but no other reason!)
                    let order_by_cols = select_cols
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| {
                            order_by_components
                                .iter()
                                .any(|o| o.starts_with(col_choices[*i]))
                        })
                        .map(|(i, _)| i)
                        .collect::<Vec<_>>();
                    let duplicate_on_order_by_exists = {
                        let mut exists = false;
                        'outer: for (i, row) in limbo_no_limit.iter().enumerate() {
                            for (j, other_row) in limbo_no_limit.iter().enumerate() {
                                if i != j
                                    && order_by_cols.iter().all(|&col| row[col] == other_row[col])
                                {
                                    exists = true;
                                    break 'outer;
                                }
                            }
                        }
                        exists
                    };
                    if duplicate_on_order_by_exists {
                        let len_equal = limbo_no_limit.len() == sqlite_no_limit.len();
                        let all_contained =
                            len_equal && limbo_no_limit.iter().all(|x| sqlite_no_limit.contains(x));
                        if all_contained {
                            continue;
                        }
                    }

                    panic!(
                        "DIFFERENT RESULTS! limbo: {:?}, sqlite: {:?}, seed: {}, query: {}, table def: {}",
                        limbo, sqlite, seed, query, table_defs[i]
                    );
                }
            }
        }
    }

    #[test]
    /// Create a table with a random number of columns and indexes, and then randomly update or delete rows from the table.
    /// Verify that the results are the same for SQLite and Turso.
    pub fn table_index_mutation_fuzz() {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time();
        println!("index_scan_single_key_mutation_fuzz seed: {seed}");

        const OUTER_ITERATIONS: usize = 100;
        for i in 0..OUTER_ITERATIONS {
            println!(
                "table_index_mutation_fuzz iteration {}/{}",
                i + 1,
                OUTER_ITERATIONS
            );
            let limbo_db = TempDatabase::new_empty(true);
            let sqlite_db = TempDatabase::new_empty(true);
            let num_cols = rng.random_range(1..=10);
            let mut table_cols = vec!["id INTEGER PRIMARY KEY AUTOINCREMENT".to_string()];
            table_cols.extend(
                (0..num_cols)
                    .map(|i| format!("c{i} INTEGER"))
                    .collect::<Vec<_>>(),
            );
            let table_def = table_cols.join(", ");
            let table_def = format!("CREATE TABLE t ({table_def})");

            let num_indexes = rng.random_range(0..=num_cols);
            let indexes = (0..num_indexes)
                .map(|i| format!("CREATE INDEX idx_{i} ON t(c{i})"))
                .collect::<Vec<_>>();

            // Create tables and indexes in both databases
            let limbo_conn = limbo_db.connect_limbo();
            limbo_exec_rows(&limbo_db, &limbo_conn, &table_def);
            for t in indexes.iter() {
                limbo_exec_rows(&limbo_db, &limbo_conn, t);
            }

            let sqlite_conn = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();
            sqlite_conn.execute(&table_def, params![]).unwrap();
            for t in indexes.iter() {
                sqlite_conn.execute(t, params![]).unwrap();
            }

            // Generate initial data
            let num_inserts = rng.random_range(10..=1000);
            let mut tuples = HashSet::new();
            while tuples.len() < num_inserts {
                tuples.insert(
                    (0..num_cols)
                        .map(|_| rng.random_range(0..1000))
                        .collect::<Vec<_>>(),
                );
            }
            let mut insert_values = Vec::new();
            for tuple in tuples {
                insert_values.push(format!(
                    "({})",
                    tuple
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            // Track executed statements in case we fail
            let mut dml_statements = Vec::new();
            let col_names = (0..num_cols)
                .map(|i| format!("c{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let insert = format!(
                "INSERT INTO t ({}) VALUES {}",
                col_names,
                insert_values.join(", ")
            );
            dml_statements.push(insert.clone());

            // Insert initial data into both databases
            sqlite_conn.execute(&insert, params![]).unwrap();
            limbo_exec_rows(&limbo_db, &limbo_conn, &insert);

            const COMPARISONS: [&str; 3] = ["=", "<", ">"];
            const INNER_ITERATIONS: usize = 20;

            for _ in 0..INNER_ITERATIONS {
                let do_update = rng.random_range(0..2) == 0;

                let comparison = COMPARISONS[rng.random_range(0..COMPARISONS.len())];
                let affected_col = rng.random_range(0..num_cols);
                let predicate_col = rng.random_range(0..num_cols);
                let predicate_value = rng.random_range(0..1000);

                enum WhereClause {
                    Normal,
                    Gaps,
                    Omit,
                }

                let where_kind = match rng.random_range(0..10) {
                    0..8 => WhereClause::Normal,
                    8 => WhereClause::Gaps,
                    9 => WhereClause::Omit,
                    _ => unreachable!(),
                };

                let where_clause = match where_kind {
                    WhereClause::Normal => format!("WHERE c{predicate_col} {comparison} {predicate_value}"),
                    WhereClause::Gaps => format!("WHERE c{predicate_col} {comparison} {predicate_value} AND c{predicate_col} % 2 = 0"),
                    WhereClause::Omit => "".to_string(),
                };

                let query = if do_update {
                    let new_y = rng.random_range(0..1000);
                    format!("UPDATE t SET c{affected_col} = {new_y} {where_clause}")
                } else {
                    format!("DELETE FROM t {where_clause}")
                };

                dml_statements.push(query.clone());

                // Execute on both databases
                sqlite_conn.execute(&query, params![]).unwrap();
                let limbo_res = limbo_exec_rows_fallible(&limbo_db, &limbo_conn, &query);
                if let Err(e) = &limbo_res {
                    // print all the DDL and DML statements
                    println!("{table_def};");
                    for t in indexes.iter() {
                        println!("{t};");
                    }
                    for t in dml_statements.iter() {
                        println!("{t};");
                    }
                    panic!("Error executing query: {e}");
                }

                // Verify results match exactly
                let verify_query = format!(
                    "SELECT * FROM t ORDER BY {}",
                    (0..num_cols)
                        .map(|i| format!("c{i}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &verify_query);
                let limbo_rows = limbo_exec_rows(&limbo_db, &limbo_conn, &verify_query);

                assert_eq!(
                    sqlite_rows, limbo_rows,
                    "Different results after mutation! limbo: {limbo_rows:?}, sqlite: {sqlite_rows:?}, seed: {seed}, query: {query}",
                );

                if sqlite_rows.is_empty() {
                    break;
                }
            }
        }
    }

    #[test]
    pub fn partial_index_mutation_and_upsert_fuzz() {
        let _ = env_logger::try_init();
        const OUTER_ITERS: usize = 5;
        const INNER_ITERS: usize = 500;

        let (mut rng, seed) = if std::env::var("SEED").is_ok() {
            let seed = std::env::var("SEED").unwrap().parse::<u64>().unwrap();
            (ChaCha8Rng::seed_from_u64(seed), seed)
        } else {
            rng_from_time()
        };
        println!("partial_index_mutation_and_upsert_fuzz seed: {seed}");
        // we want to hit unique constraints fairly often so limit the insert values
        const K_POOL: [&str; 35] = [
            "a", "aa", "abc", "A", "B", "zzz", "foo", "bar", "baz", "fizz", "buzz", "bb", "cc",
            "dd", "ee", "ff", "gg", "hh", "jj", "kk", "ll", "mm", "nn", "oo", "pp", "qq", "rr",
            "ss", "tt", "uu", "vv", "ww", "xx", "yy", "zz",
        ];
        for outer in 0..OUTER_ITERS {
            println!(" ");
            println!(
                "partial_index_mutation_and_upsert_fuzz iteration {}/{}",
                outer + 1,
                OUTER_ITERS
            );

            // Columns: id (rowid PK), plus a few data columns we can reference in predicates/keys.
            let limbo_db = TempDatabase::new_empty(true);
            let sqlite_db = TempDatabase::new_empty(true);
            let limbo_conn = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let num_cols = rng.random_range(2..=4);
            // We'll always include a TEXT "k" and a couple INT columns to give predicates variety.
            // Build: id INTEGER PRIMARY KEY, k TEXT, c0 INT, c1 INT, ...
            let mut cols: Vec<String> = vec![
                "id INTEGER PRIMARY KEY AUTOINCREMENT".into(),
                "k TEXT".into(),
            ];
            for i in 0..(num_cols - 1) {
                cols.push(format!("c{i} INT"));
            }
            let create = format!("CREATE TABLE t ({})", cols.join(", "));
            println!("{create};");
            limbo_exec_rows(&limbo_db, &limbo_conn, &create);
            sqlite.execute(&create, rusqlite::params![]).unwrap();

            // Helper to list usable columns for keys/predicates
            let int_cols: Vec<String> = (0..(num_cols - 1)).map(|i| format!("c{i}")).collect();
            let functions = ["lower", "upper", "length"];

            let num_pidx = rng.random_range(0..=3);
            let mut idx_ddls: Vec<String> = Vec::new();
            for i in 0..num_pidx {
                // Pick 1 or 2 key columns; always include "k" sometimes to get frequent conflicts.
                let mut key_cols = Vec::new();
                if rng.random_bool(0.7) {
                    key_cols.push("k".to_string());
                }
                if key_cols.is_empty() || rng.random_bool(0.5) {
                    // Add one INT col to make compound keys common
                    if !int_cols.is_empty() {
                        let c = int_cols[rng.random_range(0..int_cols.len())].clone();
                        if !key_cols.contains(&c) {
                            key_cols.push(c);
                        }
                    }
                }
                // Ensure at least one key column
                if key_cols.is_empty() {
                    key_cols.push("k".to_string());
                }
                // Build a simple deterministic partial predicate:
                // Examples:
                //   c0 > 10 AND c1 < 50
                //   c0 IS NOT NULL
                //   id > 5 AND c0 >= 0
                //   lower(k) = k
                let pred = {
                    // parts we can AND/OR (we’ll only AND for stability)
                    let mut parts: Vec<String> = Vec::new();

                    // Maybe include rowid (id) bound
                    if rng.random_bool(0.4) {
                        let n = rng.random_range(0..20);
                        let op = *["<", "<=", ">", ">="].choose(&mut rng).unwrap();
                        parts.push(format!("id {op} {n}"));
                    }

                    // Maybe include int column comparison
                    if !int_cols.is_empty() && rng.random_bool(0.8) {
                        let c = &int_cols[rng.random_range(0..int_cols.len())];
                        match rng.random_range(0..3) {
                            0 => parts.push(format!("{c} IS NOT NULL")),
                            1 => {
                                let n = rng.random_range(-10..=20);
                                let op = *["<", "<=", "=", ">=", ">"].choose(&mut rng).unwrap();
                                parts.push(format!("{c} {op} {n}"));
                            }
                            _ => {
                                let n = rng.random_range(0..=1);
                                parts.push(format!(
                                    "{c} IS {}",
                                    if n == 0 { "NULL" } else { "NOT NULL" }
                                ));
                            }
                        }
                    }

                    if rng.random_bool(0.2) {
                        parts.push(format!("{}(k) = k", functions.choose(&mut rng).unwrap()));
                    }
                    // Guarantee at least one part
                    if parts.is_empty() {
                        parts.push("1".to_string());
                    }
                    parts.join(" AND ")
                };

                let ddl = format!(
                    "CREATE UNIQUE INDEX idx_p{}_{} ON t({}) WHERE {}",
                    outer,
                    i,
                    key_cols.join(","),
                    pred
                );
                idx_ddls.push(ddl.clone());
                // Create in both engines
                println!("{ddl};");
                limbo_exec_rows(&limbo_db, &limbo_conn, &ddl);
                sqlite.execute(&ddl, rusqlite::params![]).unwrap();
            }

            let seed_rows = rng.random_range(10..=80);
            for _ in 0..seed_rows {
                let k = *K_POOL.choose(&mut rng).unwrap();
                let mut vals: Vec<String> = vec!["NULL".into(), format!("'{k}'")]; // id NULL -> auto
                for _ in 0..(num_cols - 1) {
                    // bias a bit toward small ints & NULL to make predicate flipping common
                    let v = match rng.random_range(0..6) {
                        0 => "NULL".into(),
                        _ => rng.random_range(-5..=15).to_string(),
                    };
                    vals.push(v);
                }
                let ins = format!("INSERT INTO t VALUES ({})", vals.join(", "));
                // Execute on both; ignore errors due to partial unique conflicts (keep seeding going)
                let _ = sqlite.execute(&ins, rusqlite::params![]);
                let _ = limbo_exec_rows_fallible(&limbo_db, &limbo_conn, &ins);
            }

            for _ in 0..INNER_ITERS {
                let action = rng.random_range(0..4); // 0: INSERT, 1: UPDATE, 2: DELETE, 3: UPSERT (catch-all)
                let stmt = match action {
                    // INSERT
                    0 => {
                        let k = *K_POOL.choose(&mut rng).unwrap();
                        let mut cols_list = vec!["k".to_string()];
                        let mut vals_list = vec![format!("'{k}'")];
                        for i in 0..(num_cols - 1) {
                            if rng.random_bool(0.8) {
                                cols_list.push(format!("c{i}"));
                                vals_list.push(if rng.random_bool(0.15) {
                                    "NULL".into()
                                } else {
                                    rng.random_range(-5..=15).to_string()
                                });
                            }
                        }
                        format!(
                            "INSERT INTO t({}) VALUES({})",
                            cols_list.join(","),
                            vals_list.join(",")
                        )
                    }

                    // UPDATE (randomly touch either key or predicate column)
                    1 => {
                        // choose a column
                        let col_pick = if rng.random_bool(0.5) {
                            "k".to_string()
                        } else {
                            format!("c{}", rng.random_range(0..(num_cols - 1)))
                        };
                        let new_val = if col_pick == "k" {
                            format!("'{}'", K_POOL.choose(&mut rng).unwrap())
                        } else if rng.random_bool(0.2) {
                            "NULL".into()
                        } else {
                            rng.random_range(-5..=15).to_string()
                        };
                        // predicate to affect some rows
                        let wc = if rng.random_bool(0.6) {
                            let pred_col = format!("c{}", rng.random_range(0..(num_cols - 1)));
                            let op = *["<", "<=", "=", ">=", ">"].choose(&mut rng).unwrap();
                            let n = rng.random_range(-5..=15);
                            format!("WHERE {pred_col} {op} {n}")
                        } else {
                            // toggle rows by id parity
                            "WHERE (id % 2) = 0".into()
                        };
                        format!("UPDATE t SET {col_pick} = {new_val} {wc}")
                    }

                    // DELETE
                    2 => {
                        let wc = if rng.random_bool(0.5) {
                            // delete rows inside partial predicate zones
                            match int_cols.len() {
                                0 => "WHERE lower(k) = k".to_string(),
                                _ => {
                                    let c = &int_cols[rng.random_range(0..int_cols.len())];
                                    let n = rng.random_range(-5..=15);
                                    let op = *["<", "<=", "=", ">=", ">"].choose(&mut rng).unwrap();
                                    format!("WHERE {c} {op} {n}")
                                }
                            }
                        } else {
                            "WHERE id % 3 = 1".to_string()
                        };
                        format!("DELETE FROM t {wc}")
                    }

                    // UPSERT catch-all is allowed even if only partial unique constraints exist
                    3 => {
                        let k = *K_POOL.choose(&mut rng).unwrap();
                        let mut cols_list = vec!["k".to_string()];
                        let mut vals_list = vec![format!("'{k}'")];
                        for i in 0..(num_cols - 1) {
                            if rng.random_bool(0.8) {
                                cols_list.push(format!("c{i}"));
                                vals_list.push(if rng.random_bool(0.2) {
                                    "NULL".into()
                                } else {
                                    rng.random_range(-5..=15).to_string()
                                });
                            }
                        }
                        if rng.random_bool(0.3) {
                            // 30% chance ON CONFLICT DO UPDATE SET ...
                            let mut set_list = Vec::new();
                            let num_set = rng.random_range(1..=cols_list.len());
                            let set_cols = cols_list
                                .choose_multiple(&mut rng, num_set)
                                .cloned()
                                .collect::<Vec<_>>();
                            for c in set_cols.iter() {
                                let v = if c == "k" {
                                    format!("'{}'", K_POOL.choose(&mut rng).unwrap())
                                } else if rng.random_bool(0.2) {
                                    "NULL".into()
                                } else {
                                    rng.random_range(-5..=15).to_string()
                                };
                                set_list.push(format!("{c} = {v}"));
                            }
                            format!(
                                "INSERT INTO t({}) VALUES({}) ON CONFLICT DO UPDATE SET {}",
                                cols_list.join(","),
                                vals_list.join(","),
                                set_list.join(", ")
                            )
                        } else {
                            format!(
                                "INSERT INTO t({}) VALUES({}) ON CONFLICT DO NOTHING",
                                cols_list.join(","),
                                vals_list.join(",")
                            )
                        }
                    }
                    _ => unreachable!(),
                };

                // Execute on SQLite first; capture success/error, then run on turso and demand same outcome.
                let sqlite_res = sqlite.execute(&stmt, rusqlite::params![]);
                let limbo_res = limbo_exec_rows_fallible(&limbo_db, &limbo_conn, &stmt);

                match (sqlite_res, limbo_res) {
                    (Ok(_), Ok(_)) => {
                        println!("{stmt};");
                        // Compare canonical table state
                        let verify = format!(
                            "SELECT id, k{} FROM t ORDER BY id, k{}",
                            (0..(num_cols - 1))
                                .map(|i| format!(", c{i}"))
                                .collect::<String>(),
                            (0..(num_cols - 1))
                                .map(|i| format!(", c{i}"))
                                .collect::<String>(),
                        );
                        let s = sqlite_exec_rows(&sqlite, &verify);
                        let l = limbo_exec_rows(&limbo_db, &limbo_conn, &verify);
                        assert_eq!(
                            l, s,
                            "stmt: {stmt}, seed: {seed}, create: {create}, idx: {idx_ddls:?}"
                        );
                    }
                    (Err(_), Err(_)) => {
                        // Both errored
                        continue;
                    }
                    // Mismatch: dump context
                    (ok_sqlite, ok_turso) => {
                        println!("{stmt};");
                        eprintln!("Schema: {create};");
                        for d in idx_ddls.iter() {
                            eprintln!("{d};");
                        }
                        panic!(
                            "DML outcome mismatch (sqlite: {ok_sqlite:?}, turso ok: {ok_turso:?}) \n
                         stmt: {stmt}, seed: {seed}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    pub fn compound_select_fuzz() {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time();
        log::info!("compound_select_fuzz seed: {seed}");

        // Constants for fuzzing parameters
        const MAX_TABLES: usize = 7;
        const MIN_TABLES: usize = 1;
        const MAX_ROWS_PER_TABLE: usize = 40;
        const MIN_ROWS_PER_TABLE: usize = 5;
        const NUM_FUZZ_ITERATIONS: usize = 2000;
        // How many more SELECTs than tables can be in a UNION (e.g., if 2 tables, max 2+2=4 SELECTs)
        const MAX_SELECTS_IN_UNION_EXTRA: usize = 2;
        const MAX_LIMIT_VALUE: usize = 50;

        let db = TempDatabase::new_empty(true);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let mut table_names = Vec::new();
        let num_tables = rng.random_range(MIN_TABLES..=MAX_TABLES);

        const COLS: [&str; 3] = ["c1", "c2", "c3"];
        for i in 0..num_tables {
            let table_name = format!("t{i}");
            let create_table_sql = format!(
                "CREATE TABLE {} ({})",
                table_name,
                COLS.iter()
                    .map(|c| format!("{c} INTEGER"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            limbo_exec_rows(&db, &limbo_conn, &create_table_sql);
            sqlite_exec_rows(&sqlite_conn, &create_table_sql);

            let num_rows_to_insert = rng.random_range(MIN_ROWS_PER_TABLE..=MAX_ROWS_PER_TABLE);
            for _ in 0..num_rows_to_insert {
                let c1_val: i64 = rng.random_range(-3..3);
                let c2_val: i64 = rng.random_range(-3..3);
                let c3_val: i64 = rng.random_range(-3..3);

                let insert_sql =
                    format!("INSERT INTO {table_name} VALUES ({c1_val}, {c2_val}, {c3_val})",);
                limbo_exec_rows(&db, &limbo_conn, &insert_sql);
                sqlite_exec_rows(&sqlite_conn, &insert_sql);
            }
            table_names.push(table_name);
        }

        for iter_num in 0..NUM_FUZZ_ITERATIONS {
            // Number of SELECT clauses
            let num_selects_in_union =
                rng.random_range(1..=(table_names.len() + MAX_SELECTS_IN_UNION_EXTRA));
            let mut select_statements = Vec::new();

            // Randomly pick a subset of columns to select from
            let num_cols_to_select = rng.random_range(1..=COLS.len());
            let cols_to_select = COLS
                .choose_multiple(&mut rng, num_cols_to_select)
                .map(|c| c.to_string())
                .collect::<Vec<_>>();

            let mut has_right_most_values = false;
            for i in 0..num_selects_in_union {
                let p = 1.0 / table_names.len() as f64;
                // Randomly decide whether to use a VALUES clause or a SELECT clause
                if rng.random_bool(p) {
                    let values = (0..cols_to_select.len())
                        .map(|_| rng.random_range(-3..3))
                        .map(|val| val.to_string())
                        .collect::<Vec<_>>();
                    select_statements.push(format!("VALUES({})", values.join(", ")));
                    if i == (num_selects_in_union - 1) {
                        has_right_most_values = true;
                    }
                } else {
                    // Randomly pick a table
                    let table_to_select_from = &table_names[rng.random_range(0..table_names.len())];
                    select_statements.push(format!(
                        "SELECT {} FROM {}",
                        cols_to_select.join(", "),
                        table_to_select_from
                    ));
                }
            }

            const COMPOUND_OPERATORS: [&str; 4] =
                [" UNION ALL ", " UNION ", " INTERSECT ", " EXCEPT "];

            let mut query = String::new();
            for (i, select_statement) in select_statements.iter().enumerate() {
                if i > 0 {
                    query.push_str(COMPOUND_OPERATORS.choose(&mut rng).unwrap());
                }
                query.push_str(select_statement);
            }

            // if the right most SELECT is a VALUES clause, no limit is not allowed
            if rng.random_bool(0.8) && !has_right_most_values {
                let limit_val = rng.random_range(0..=MAX_LIMIT_VALUE); // LIMIT 0 is valid

                if rng.random_bool(0.8) {
                    query = format!("{query} LIMIT {limit_val}");
                } else {
                    let offset_val = rng.random_range(0..=MAX_LIMIT_VALUE);
                    query = format!("{query} LIMIT {limit_val} OFFSET {offset_val}");
                }
            }

            log::debug!(
                "Iteration {}/{}: Query: {}",
                iter_num + 1,
                NUM_FUZZ_ITERATIONS,
                query
            );

            let limbo_results = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite_results = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_results,
                sqlite_results,
                "query: {}, limbo.len(): {}, sqlite.len(): {}, limbo: {:?}, sqlite: {:?}, seed: {}",
                query,
                limbo_results.len(),
                sqlite_results.len(),
                limbo_results,
                sqlite_results,
                seed
            );
        }
    }

    #[test]
    pub fn ddl_compatibility_fuzz() {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time();
        const ITERATIONS: usize = 1000;
        for i in 0..ITERATIONS {
            let db = TempDatabase::new_empty(true);
            let conn = db.connect_limbo();
            let num_cols = rng.random_range(1..=5);
            let col_names: Vec<String> = (0..num_cols).map(|c| format!("c{c}")).collect();

            // Decide whether to use a table-level PRIMARY KEY (possibly compound)
            let use_table_pk = num_cols >= 1 && rng.random_bool(0.6);
            let pk_len = if use_table_pk {
                if num_cols == 1 {
                    1
                } else {
                    rng.random_range(1..=num_cols.min(3))
                }
            } else {
                0
            };
            let pk_cols: Vec<String> = if use_table_pk {
                let mut col_names_shuffled = col_names.clone();
                col_names_shuffled.shuffle(&mut rng);
                col_names_shuffled.iter().take(pk_len).cloned().collect()
            } else {
                Vec::new()
            };

            let mut has_primary_key = false;

            // Column definitions with optional types and column-level constraints
            let mut column_defs: Vec<String> = Vec::new();
            for name in col_names.iter() {
                let mut parts = vec![name.clone()];
                if rng.random_bool(0.7) {
                    let types = ["INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC"];
                    let t = types[rng.random_range(0..types.len())];
                    parts.push(t.to_string());
                }
                if !use_table_pk && !has_primary_key && rng.random_bool(0.3) {
                    has_primary_key = true;
                    parts.push("PRIMARY KEY".to_string());
                } else if rng.random_bool(0.2) {
                    parts.push("UNIQUE".to_string());
                }
                column_defs.push(parts.join(" "));
            }

            // Table-level constraints: PRIMARY KEY and some UNIQUE constraints (including compound)
            let mut table_constraints: Vec<String> = Vec::new();
            if use_table_pk {
                let mut spec_parts: Vec<String> = Vec::new();
                for col in pk_cols.iter() {
                    if rng.random_bool(0.5) {
                        let dir = if rng.random_bool(0.5) { "DESC" } else { "ASC" };
                        spec_parts.push(format!("{col} {dir}"));
                    } else {
                        spec_parts.push(col.clone());
                    }
                }
                table_constraints.push(format!("PRIMARY KEY ({})", spec_parts.join(", ")));
            }

            let num_uniques = if num_cols >= 2 {
                rng.random_range(0..=2)
            } else {
                rng.random_range(0..=1)
            };
            for _ in 0..num_uniques {
                let len = if num_cols == 1 {
                    1
                } else {
                    rng.random_range(1..=num_cols.min(3))
                };
                let start = rng.random_range(0..num_cols);
                let mut uniq_cols: Vec<String> = Vec::new();
                for k in 0..len {
                    let idx = (start + k) % num_cols;
                    uniq_cols.push(col_names[idx].clone());
                }
                table_constraints.push(format!("UNIQUE ({})", uniq_cols.join(", ")));
            }

            let mut elements = column_defs;
            elements.extend(table_constraints);
            let table_name = format!("t{i}");
            let create_sql = format!("CREATE TABLE {table_name} ({})", elements.join(", "));

            println!("{create_sql}");

            limbo_exec_rows(&db, &conn, &create_sql);
            do_flush(&conn, &db).unwrap();

            // Open with rusqlite and verify integrity_check returns OK
            let sqlite_conn = rusqlite::Connection::open(db.path.clone()).unwrap();
            let rows = sqlite_exec_rows(&sqlite_conn, "PRAGMA integrity_check");
            assert!(
                !rows.is_empty(),
                "integrity_check returned no rows (seed: {seed})"
            );
            match &rows[0][0] {
                Value::Text(s) => assert!(
                    s.eq_ignore_ascii_case("ok"),
                    "integrity_check failed (seed: {seed}): {rows:?}",
                ),
                other => panic!("unexpected integrity_check result (seed: {seed}): {other:?}",),
            }

            // Verify the stored SQL matches the create table statement
            let conn = db.connect_limbo();
            let verify_sql = format!(
                "SELECT sql FROM sqlite_schema WHERE name = '{table_name}' and type = 'table'"
            );
            let res = limbo_exec_rows(&db, &conn, &verify_sql);
            assert!(res.len() == 1, "Expected 1 row, got {res:?}");
            let Value::Text(s) = &res[0][0] else {
                panic!("sql should be TEXT");
            };
            assert_eq!(s.as_str(), create_sql);
        }
    }

    #[test]
    pub fn arithmetic_expression_fuzz() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (unary_op, unary_op_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        unary_op_builder
            .concat(" ")
            .push(g.create().choice().options_str(["~", "+", "-"]).build())
            .push(expr)
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(
                g.create()
                    .choice()
                    .options_str(["+", "-", "*", "/", "%", "&", "|", "<<", ">>"])
                    .build(),
            )
            .push(expr)
            .build();

        expr_builder
            .choice()
            .option_w(unary_op, 1.0)
            .option_w(bin_op, 1.0)
            .option_w(paren, 1.0)
            .option_symbol_w(rand_int(-10..10), 1.0)
            .build();

        let sql = g.create().concat(" ").push_str("SELECT").push(expr).build();

        let db = TempDatabase::new_empty(false);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?} seed: {seed}"
            );
        }
    }

    #[test]
    pub fn fuzz_ex() {
        let _ = env_logger::try_init();
        let db = TempDatabase::new_empty(false);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        for query in [
            "SELECT FALSE",
            "SELECT NOT FALSE",
            "SELECT ((NULL) IS NOT TRUE <= ((NOT (FALSE))))",
            "SELECT ifnull(0, NOT 0)",
            "SELECT like('a%', 'a') = 1",
            "SELECT CASE ( NULL < NULL ) WHEN ( 0 ) THEN ( NULL ) ELSE ( 2.0 ) END;",
            "SELECT (COALESCE(0, COALESCE(0, 0)));",
            "SELECT CAST((1 > 0) AS INTEGER);",
            "SELECT substr('ABC', -1)",
        ] {
            let limbo = limbo_exec_rows(&db, &limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}"
            );
        }
    }

    #[test]
    pub fn math_expression_fuzz_run() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (scalar, scalar_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(
                g.create()
                    .choice()
                    .options_str(["+", "-", "/", "*"])
                    .build(),
            )
            .push(expr)
            .build();

        scalar_builder
            .choice()
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str([
                                "acos", "acosh", "asin", "asinh", "atan", "atanh", "ceil",
                                "ceiling", "cos", "cosh", "degrees", "exp", "floor", "ln", "log",
                                "log10", "log2", "radians", "sin", "sinh", "sqrt", "tan", "tanh",
                                "trunc",
                            ])
                            .build(),
                    )
                    .push_str("(")
                    .push(expr)
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str(["atan2", "log", "mod", "pow", "power"])
                            .build(),
                    )
                    .push_str("(")
                    .push(g.create().concat("").push(expr).repeat(2..3, ", ").build())
                    .push_str(")")
                    .build(),
            )
            .build();

        expr_builder
            .choice()
            .options_str(["-2.0", "-1.0", "0.0", "0.5", "1.0", "2.0"])
            .option_w(bin_op, 10.0)
            .option_w(paren, 10.0)
            .option_w(scalar, 10.0)
            .build();

        let sql = g.create().concat(" ").push_str("SELECT").push(expr).build();

        let db = TempDatabase::new_empty(false);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {query}");
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            match (&limbo[0][0], &sqlite[0][0]) {
                // compare only finite results because some evaluations are not so stable around infinity
                (rusqlite::types::Value::Real(limbo), rusqlite::types::Value::Real(sqlite))
                    if limbo.is_finite() && sqlite.is_finite() =>
                {
                    assert!(
                        (limbo - sqlite).abs() < 1e-9
                            || (limbo - sqlite) / (limbo.abs().max(sqlite.abs())) < 1e-9,
                        "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?} seed: {seed}"
                    )
                }
                _ => {}
            }
        }
    }

    #[test]
    pub fn string_expression_fuzz_run() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (scalar, scalar_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();
        let (number, number_builder) = g.create_handle();

        number_builder
            .choice()
            .option_symbol(rand_int(-5..10))
            .option(
                g.create()
                    .concat(" ")
                    .push(number)
                    .push(g.create().choice().options_str(["+", "-", "*"]).build())
                    .push(number)
                    .build(),
            )
            .build();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(g.create().choice().options_str(["||"]).build())
            .push(expr)
            .build();

        scalar_builder
            .choice()
            .option(
                g.create()
                    .concat("")
                    .push_str("char(")
                    .push(
                        g.create()
                            .concat("")
                            .push_symbol(rand_int(65..91))
                            .repeat(1..8, ", ")
                            .build(),
                    )
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str(["ltrim", "rtrim", "trim"])
                            .build(),
                    )
                    .push_str("(")
                    .push(g.create().concat("").push(expr).repeat(2..3, ", ").build())
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str([
                                "ltrim", "rtrim", "lower", "upper", "quote", "hex", "trim",
                            ])
                            .build(),
                    )
                    .push_str("(")
                    .push(expr)
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(g.create().choice().options_str(["replace"]).build())
                    .push_str("(")
                    .push(g.create().concat("").push(expr).repeat(3..4, ", ").build())
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str(["substr", "substring"])
                            .build(),
                    )
                    .push_str("(")
                    .push(expr)
                    .push_str(", ")
                    .push(
                        g.create()
                            .concat("")
                            .push(number)
                            .repeat(1..3, ", ")
                            .build(),
                    )
                    .push_str(")")
                    .build(),
            )
            .build();

        expr_builder
            .choice()
            .option_w(bin_op, 1.0)
            .option_w(paren, 1.0)
            .option_w(scalar, 1.0)
            .option(
                g.create()
                    .concat("")
                    .push_str("'")
                    .push_symbol(rand_str("", 2))
                    .push_str("'")
                    .build(),
            )
            .build();

        let sql = g.create().concat(" ").push_str("SELECT").push(expr).build();

        let db = TempDatabase::new_empty(false);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {query}");
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?} seed: {seed}"
            );
        }
    }

    struct TestTable {
        pub name: &'static str,
        pub columns: Vec<&'static str>,
    }

    /// Expressions that can be used in both SELECT and WHERE positions.
    struct CommonBuilders {
        pub bin_op: SymbolHandle,
        pub unary_infix_op: SymbolHandle,
        pub scalar: SymbolHandle,
        pub paren: SymbolHandle,
        pub coalesce_expr: SymbolHandle,
        pub cast_expr: SymbolHandle,
        pub case_expr: SymbolHandle,
        pub cmp_op: SymbolHandle,
        pub number: SymbolHandle,
    }

    /// Expressions that can be used only in WHERE position due to Limbo limitations.
    struct PredicateBuilders {
        pub in_op: SymbolHandle,
    }

    fn common_builders(g: &GrammarGenerator, tables: Option<&[TestTable]>) -> CommonBuilders {
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (unary_infix_op, unary_infix_op_builder) = g.create_handle();
        let (scalar, scalar_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();
        let (like_pattern, like_pattern_builder) = g.create_handle();
        let (glob_pattern, glob_pattern_builder) = g.create_handle();
        let (coalesce_expr, coalesce_expr_builder) = g.create_handle();
        let (cast_expr, cast_expr_builder) = g.create_handle();
        let (case_expr, case_expr_builder) = g.create_handle();
        let (cmp_op, cmp_op_builder) = g.create_handle();
        let (column, column_builder) = g.create_handle();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        unary_infix_op_builder
            .concat(" ")
            .push(g.create().choice().options_str(["NOT"]).build())
            .push(expr)
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(
                g.create()
                    .choice()
                    .options_str(["AND", "OR", "IS", "IS NOT", "=", "<>", ">", "<", ">=", "<="])
                    .build(),
            )
            .push(expr)
            .build();

        like_pattern_builder
            .choice()
            .option_str("%")
            .option_str("_")
            .option_symbol(rand_str("", 1))
            .repeat(1..10, "")
            .build();

        glob_pattern_builder
            .choice()
            .option_str("*")
            .option_str("**")
            .option_str("A")
            .option_str("B")
            .repeat(1..10, "")
            .build();

        coalesce_expr_builder
            .concat("")
            .push_str("COALESCE(")
            .push(g.create().concat("").push(expr).repeat(2..5, ",").build())
            .push_str(")")
            .build();

        cast_expr_builder
            .concat(" ")
            .push_str("CAST ( (")
            .push(expr)
            .push_str(") AS ")
            // cast to INTEGER/REAL/TEXT types can be added when Limbo will use proper equality semantic between values (e.g. 1 = 1.0)
            .push(g.create().choice().options_str(["NUMERIC"]).build())
            .push_str(")")
            .build();

        case_expr_builder
            .concat(" ")
            .push_str("CASE (")
            .push(expr)
            .push_str(")")
            .push(
                g.create()
                    .concat(" ")
                    .push_str("WHEN (")
                    .push(expr)
                    .push_str(") THEN (")
                    .push(expr)
                    .push_str(")")
                    .repeat(1..5, " ")
                    .build(),
            )
            .push_str("ELSE (")
            .push(expr)
            .push_str(") END")
            .build();

        scalar_builder
            .choice()
            .option(coalesce_expr)
            .option(
                g.create()
                    .concat("")
                    .push_str("like('")
                    .push(like_pattern)
                    .push_str("', '")
                    .push(like_pattern)
                    .push_str("')")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push_str("glob('")
                    .push(glob_pattern)
                    .push_str("', '")
                    .push(glob_pattern)
                    .push_str("')")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push_str("ifnull(")
                    .push(expr)
                    .push_str(",")
                    .push(expr)
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push_str("iif(")
                    .push(expr)
                    .push_str(",")
                    .push(expr)
                    .push_str(",")
                    .push(expr)
                    .push_str(")")
                    .build(),
            )
            .build();

        let number = g
            .create()
            .choice()
            .option_symbol(rand_int(-0xff..0x100))
            .option_symbol(rand_int(-0xffff..0x10000))
            .option_symbol(rand_int(-0xffffff..0x1000000))
            .option_symbol(rand_int(-0xffffffff..0x100000000))
            .option_symbol(rand_int(-0xffffffffffff..0x1000000000000))
            .build();

        let mut column_builder = column_builder
            .choice()
            .option(
                g.create()
                    .concat(" ")
                    .push_str("(")
                    .push(column)
                    .push_str(")")
                    .build(),
            )
            .option(number)
            .option(
                g.create()
                    .concat(" ")
                    .push_str("(")
                    .push(column)
                    .push(
                        g.create()
                            .choice()
                            .options_str([
                                "+", "-", "*", "/", "||", "=", "<>", ">", "<", ">=", "<=", "IS",
                                "IS NOT",
                            ])
                            .build(),
                    )
                    .push(column)
                    .push_str(")")
                    .build(),
            );

        if let Some(tables) = tables {
            for table in tables.iter() {
                for column in table.columns.iter() {
                    column_builder = column_builder
                        .option_symbol_w(const_str(&format!("{}.{}", table.name, column)), 1.0);
                }
            }
        }

        column_builder.build();

        cmp_op_builder
            .concat(" ")
            .push(column)
            .push(
                g.create()
                    .choice()
                    .options_str(["=", "<>", ">", "<", ">=", "<=", "IS", "IS NOT"])
                    .build(),
            )
            .push(column)
            .build();

        expr_builder
            .choice()
            .option_w(bin_op, 3.0)
            .option_w(unary_infix_op, 2.0)
            .option_w(paren, 2.0)
            .option_w(scalar, 4.0)
            .option_w(coalesce_expr, 1.0)
            .option_w(cast_expr, 1.0)
            .option_w(case_expr, 1.0)
            .option_w(cmp_op, 1.0)
            .options_str(["1", "0", "NULL", "2.0", "1.5", "-0.5", "-2.0", "(1 / 0)"])
            .build();

        CommonBuilders {
            bin_op,
            unary_infix_op,
            scalar,
            paren,
            coalesce_expr,
            cast_expr,
            case_expr,
            cmp_op,
            number,
        }
    }

    fn predicate_builders(g: &GrammarGenerator, tables: Option<&[TestTable]>) -> PredicateBuilders {
        let (in_op, in_op_builder) = g.create_handle();
        let (column, column_builder) = g.create_handle();
        let mut column_builder = column_builder
            .choice()
            .option(
                g.create()
                    .concat(" ")
                    .push_str("(")
                    .push(column)
                    .push_str(")")
                    .build(),
            )
            .option_symbol(rand_int(-0xffffffff..0x100000000))
            .option(
                g.create()
                    .concat(" ")
                    .push_str("(")
                    .push(column)
                    .push(g.create().choice().options_str(["+", "-"]).build())
                    .push(column)
                    .push_str(")")
                    .build(),
            );

        if let Some(tables) = tables {
            for table in tables.iter() {
                for column in table.columns.iter() {
                    column_builder = column_builder
                        .option_symbol_w(const_str(&format!("{}.{}", table.name, column)), 1.0);
                }
            }
        }

        column_builder.build();

        in_op_builder
            .concat(" ")
            .push(column)
            .push(g.create().choice().options_str(["IN", "NOT IN"]).build())
            .push_str("(")
            .push(
                g.create()
                    .concat("")
                    .push(column)
                    .repeat(1..5, ", ")
                    .build(),
            )
            .push_str(")")
            .build();

        PredicateBuilders { in_op }
    }

    fn build_logical_expr(
        g: &GrammarGenerator,
        common: &CommonBuilders,
        predicate: Option<&PredicateBuilders>,
    ) -> SymbolHandle {
        let (handle, builder) = g.create_handle();
        let mut builder = builder
            .choice()
            .option_w(common.cast_expr, 1.0)
            .option_w(common.case_expr, 1.0)
            .option_w(common.cmp_op, 1.0)
            .option_w(common.coalesce_expr, 1.0)
            .option_w(common.unary_infix_op, 2.0)
            .option_w(common.bin_op, 3.0)
            .option_w(common.paren, 2.0)
            .option_w(common.scalar, 4.0)
            // unfortunately, sqlite behaves weirdly when IS operator is used with TRUE/FALSE constants
            // e.g. 8 IS TRUE == 1 (although 8 = TRUE == 0)
            // so, we do not use TRUE/FALSE constants as they will produce diff with sqlite results
            .options_str(["1", "0", "NULL", "2.0", "1.5", "-0.5", "-2.0", "(1 / 0)"]);

        if let Some(predicate) = predicate {
            builder = builder.option_w(predicate.in_op, 1.0);
        }

        builder.build();

        handle
    }

    #[test]
    pub fn logical_expression_fuzz_run() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let builders = common_builders(&g, None);
        let expr = build_logical_expr(&g, &builders, None);

        let sql = g
            .create()
            .concat(" ")
            .push_str("SELECT ")
            .push(expr)
            .build();

        let db = TempDatabase::new_empty(false);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {query}");
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?} seed: {seed}"
            );
        }
    }

    #[test]
    pub fn table_logical_expression_fuzz_ex1() {
        let _ = env_logger::try_init();

        for queries in [
            [
                "CREATE TABLE t (x)",
                "INSERT INTO t VALUES (10)",
                "SELECT * FROM t WHERE  x = 1 AND 1 OR 0",
            ],
            [
                "CREATE TABLE t (x)",
                "INSERT INTO t VALUES (-3258184727)",
                "SELECT * FROM t",
            ],
        ] {
            let db = TempDatabase::new_empty(false);
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            for query in queries.iter() {
                let limbo = limbo_exec_rows(&db, &limbo_conn, query);
                let sqlite = sqlite_exec_rows(&sqlite_conn, query);
                assert_eq!(
                    limbo, sqlite,
                    "queries: {queries:?}, query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}"
                );
            }
        }
    }

    #[test]
    pub fn min_max_agg_fuzz() {
        let _ = env_logger::try_init();

        let datatypes = ["INTEGER", "TEXT", "REAL", "BLOB"];
        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");

        for _ in 0..1000 {
            // Create table with random datatype
            let datatype = datatypes[rng.random_range(0..datatypes.len())];
            let create_table = format!("CREATE TABLE t (x {datatype})");

            let db = TempDatabase::new_empty(false);
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            limbo_exec_rows(&db, &limbo_conn, &create_table);
            sqlite_exec_rows(&sqlite_conn, &create_table);

            // Insert 5 random values of random types
            let mut values = Vec::new();
            for _ in 0..5 {
                let value = match rng.random_range(0..4) {
                    0 => rng.random_range(-1000..1000).to_string(), // Integer
                    1 => format!(
                        "'{}'",
                        (0..10)
                            .map(|_| rng.random_range(b'a'..=b'z') as char)
                            .collect::<String>()
                    ), // Text
                    2 => format!("{:.2}", rng.random_range(-100..100) as f64 / 10.0), // Real
                    3 => "NULL".to_string(),                        // NULL
                    _ => unreachable!(),
                };
                values.push(format!("({value})"));
            }

            let insert = format!("INSERT INTO t VALUES {}", values.join(","));
            limbo_exec_rows(&db, &limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            // Test min and max
            for agg in ["min(x)", "max(x)"] {
                let query = format!("SELECT {agg} FROM t");
                let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
                let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

                assert_eq!(
                    limbo, sqlite,
                    "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}, seed: {seed}, values: {values:?}, schema: {create_table}"
                );
            }
        }
    }

    #[test]
    pub fn affinity_fuzz() {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time();
        log::info!("affinity_fuzz seed: {seed}");

        for iteration in 0..500 {
            let db = TempDatabase::new_empty(false);
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            // Test different column affinities - cover all SQLite affinity types
            let affinities = [
                "INTEGER",
                "TEXT",
                "REAL",
                "NUMERIC",
                "BLOB",
                "INT",
                "TINYINT",
                "SMALLINT",
                "MEDIUMINT",
                "BIGINT",
                "UNSIGNED BIG INT",
                "INT2",
                "INT8",
                "CHARACTER(20)",
                "VARCHAR(255)",
                "VARYING CHARACTER(255)",
                "NCHAR(55)",
                "NATIVE CHARACTER(70)",
                "NVARCHAR(100)",
                "CLOB",
                "DOUBLE",
                "DOUBLE PRECISION",
                "FLOAT",
                "DECIMAL(10,5)",
                "BOOLEAN",
                "DATE",
                "DATETIME",
            ];
            let affinity = affinities[rng.random_range(0..affinities.len())];

            let create_table = format!("CREATE TABLE t (x {affinity})");
            limbo_exec_rows(&db, &limbo_conn, &create_table);
            sqlite_exec_rows(&sqlite_conn, &create_table);

            // Insert various values that test affinity conversion rules
            let mut values = Vec::new();
            for _ in 0..20 {
                let value = match rng.random_range(0..9) {
                    0 => format!("'{}'", rng.random_range(-10000..10000)), // Pure integer as text
                    1 => format!(
                        "'{}.{}'",
                        rng.random_range(-1000..1000),
                        rng.random_range(1..999) // Ensure non-zero decimal part
                    ), // Float as text with decimal
                    2 => format!("'a{}'", rng.random_range(0..1000)), // Text with integer suffix
                    3 => format!("'  {}  '", rng.random_range(-100..100)), // Integer with whitespace
                    4 => format!("'-{}'", rng.random_range(1..1000)), // Negative integer as text
                    5 => format!("{}", rng.random_range(-10000..10000)), // Direct integer
                    6 => format!(
                        "{}.{}",
                        rng.random_range(-100..100),
                        rng.random_range(1..999) // Ensure non-zero decimal part
                    ), // Direct float
                    7 => "'text_value'".to_string(), // Pure text that won't convert
                    8 => "NULL".to_string(),         // NULL value
                    _ => unreachable!(),
                };
                values.push(format!("({value})"));
            }

            let insert = format!("INSERT INTO t VALUES {}", values.join(","));
            limbo_exec_rows(&db, &limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            // Query values and their types to verify affinity rules are applied correctly
            let query = "SELECT x, typeof(x) FROM t";
            let limbo_result = limbo_exec_rows(&db, &limbo_conn, query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, query);

            assert_eq!(
                limbo_result, sqlite_result,
                "iteration: {iteration}, seed: {seed}, affinity: {affinity}, values: {values:?}"
            );

            // Also test with ORDER BY to ensure affinity affects sorting
            let query_ordered = "SELECT x FROM t ORDER BY x";
            let limbo_ordered = limbo_exec_rows(&db, &limbo_conn, query_ordered);
            let sqlite_ordered = sqlite_exec_rows(&sqlite_conn, query_ordered);

            assert_eq!(
                limbo_ordered, sqlite_ordered,
                "ORDER BY failed - iteration: {iteration}, seed: {seed}, affinity: {affinity}"
            );
        }
    }

    #[test]
    // Simple fuzz test for SUM with floats
    pub fn sum_agg_fuzz_floats() {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");

        for _ in 0..100 {
            let db = TempDatabase::new_empty(false);
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            limbo_exec_rows(&db, &limbo_conn, "CREATE TABLE t(x)");
            sqlite_exec_rows(&sqlite_conn, "CREATE TABLE t(x)");

            // Insert 50-100 mixed values: floats, text, NULL
            let mut values = Vec::new();
            for _ in 0..rng.random_range(50..=100) {
                let value = rng.random_range(-100.0..100.0).to_string();
                values.push(format!("({value})"));
            }

            let insert = format!("INSERT INTO t VALUES {}", values.join(","));
            limbo_exec_rows(&db, &limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            let query = "SELECT sum(x) FROM t ORDER BY x";
            let limbo_result = limbo_exec_rows(&db, &limbo_conn, query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, query);

            let limbo_val = match limbo_result.first().and_then(|row| row.first()) {
                Some(Value::Real(f)) => *f,
                Some(Value::Null) | None => 0.0,
                _ => panic!("Unexpected type in limbo result: {limbo_result:?}"),
            };

            let sqlite_val = match sqlite_result.first().and_then(|row| row.first()) {
                Some(Value::Real(f)) => *f,
                Some(Value::Null) | None => 0.0,
                _ => panic!("Unexpected type in limbo result: {limbo_result:?}"),
            };
            assert_eq!(limbo_val, sqlite_val, "seed: {seed}, values: {values:?}");
        }
    }

    #[test]
    // Simple fuzz test for SUM with mixed numeric/non-numeric values (issue #2133)
    pub fn sum_agg_fuzz() {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");

        for _ in 0..100 {
            let db = TempDatabase::new_empty(false);
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            limbo_exec_rows(&db, &limbo_conn, "CREATE TABLE t(x)");
            sqlite_exec_rows(&sqlite_conn, "CREATE TABLE t(x)");

            // Insert 3-4 mixed values: integers, text, NULL
            let mut values = Vec::new();
            for _ in 0..rng.random_range(3..=4) {
                let value = match rng.random_range(0..3) {
                    0 => rng.random_range(-100..100).to_string(), // Integer
                    1 => format!(
                        "'{}'",
                        (0..3)
                            .map(|_| rng.random_range(b'a'..=b'z') as char)
                            .collect::<String>()
                    ), // Text
                    2 => "NULL".to_string(),                      // NULL
                    _ => unreachable!(),
                };
                values.push(format!("({value})"));
            }

            let insert = format!("INSERT INTO t VALUES {}", values.join(","));
            limbo_exec_rows(&db, &limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            let query = "SELECT sum(x) FROM t";
            let limbo = limbo_exec_rows(&db, &limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);

            assert_eq!(limbo, sqlite, "seed: {seed}, values: {values:?}");
        }
    }

    #[test]
    fn concat_ws_fuzz() {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");

        for _ in 0..100 {
            let db = TempDatabase::new_empty(false);
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            let num_args = rng.random_range(7..=17);
            let mut args = Vec::new();
            for _ in 0..num_args {
                let arg = match rng.random_range(0..3) {
                    0 => rng.random_range(-100..100).to_string(),
                    1 => format!(
                        "'{}'",
                        (0..rng.random_range(1..=5))
                            .map(|_| rng.random_range(b'a'..=b'z') as char)
                            .collect::<String>()
                    ),
                    2 => "NULL".to_string(),
                    _ => unreachable!(),
                };
                args.push(arg);
            }

            let sep = match rng.random_range(0..=2) {
                0 => "','",
                1 => "'-'",
                2 => "NULL",
                _ => unreachable!(),
            };

            let query = format!("SELECT concat_ws({}, {})", sep, args.join(", "));

            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(limbo, sqlite, "seed: {seed}, sep: {sep}, args: {args:?}");
        }
    }

    #[test]
    // Simple fuzz test for TOTAL with mixed numeric/non-numeric values
    pub fn total_agg_fuzz() {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");

        for _ in 0..100 {
            let db = TempDatabase::new_empty(false);
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            limbo_exec_rows(&db, &limbo_conn, "CREATE TABLE t(x)");
            sqlite_exec_rows(&sqlite_conn, "CREATE TABLE t(x)");

            // Insert 3-4 mixed values: integers, text, NULL
            let mut values = Vec::new();
            for _ in 0..rng.random_range(3..=4) {
                let value = match rng.random_range(0..3) {
                    0 => rng.random_range(-100..100).to_string(), // Integer
                    1 => format!(
                        "'{}'",
                        (0..3)
                            .map(|_| rng.random_range(b'a'..=b'z') as char)
                            .collect::<String>()
                    ), // Text
                    2 => "NULL".to_string(),                      // NULL
                    _ => unreachable!(),
                };
                values.push(format!("({value})"));
            }

            let insert = format!("INSERT INTO t VALUES {}", values.join(","));
            limbo_exec_rows(&db, &limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            let query = "SELECT total(x) FROM t";
            let limbo = limbo_exec_rows(&db, &limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);

            assert_eq!(limbo, sqlite, "seed: {seed}, values: {values:?}");
        }
    }

    #[test]
    pub fn table_logical_expression_fuzz_run() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let tables = vec![TestTable {
            name: "t",
            columns: vec!["x", "y", "z"],
        }];
        let builders = common_builders(&g, Some(&tables));
        let predicate = predicate_builders(&g, Some(&tables));
        let expr = build_logical_expr(&g, &builders, Some(&predicate));

        let db = TempDatabase::new_empty(true);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
        for table in tables.iter() {
            let columns_with_first_column_as_pk = {
                let mut columns = vec![];
                columns.push(format!("{} PRIMARY KEY", table.columns[0]));
                columns.extend(table.columns[1..].iter().map(|c| c.to_string()));
                columns.join(", ")
            };
            let query = format!(
                "CREATE TABLE {} ({})",
                table.name, columns_with_first_column_as_pk
            );
            dbg!(&query);
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}",
            );
        }

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {seed}");

        let mut i = 0;
        let mut primary_key_set = HashSet::with_capacity(100);
        while i < 100 {
            let x = g.generate(&mut rng, builders.number, 1);
            if primary_key_set.contains(&x) {
                continue;
            }
            primary_key_set.insert(x.clone());
            let (y, z) = (
                g.generate(&mut rng, builders.number, 1),
                g.generate(&mut rng, builders.number, 1),
            );
            let query = format!("INSERT INTO t VALUES ({x}, {y}, {z})");
            log::info!("insert: {query}");
            dbg!(&query);
            assert_eq!(
                limbo_exec_rows(&db, &limbo_conn, &query),
                sqlite_exec_rows(&sqlite_conn, &query),
                "seed: {seed}",
            );
            i += 1;
        }
        // verify the same number of rows in both tables
        let query = "SELECT COUNT(*) FROM t".to_string();
        let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
        let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
        assert_eq!(limbo, sqlite, "seed: {seed}");

        let sql = g
            .create()
            .concat(" ")
            .push_str("SELECT * FROM t WHERE ")
            .push(expr)
            .build();

        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {query}");
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

            if limbo.len() != sqlite.len() {
                panic!("MISMATCHING ROW COUNT (limbo: {}, sqlite: {}) for query: {}\n\n limbo: {:?}\n\n sqlite: {:?}", limbo.len(), sqlite.len(), query, limbo, sqlite);
            }
            // find first row where limbo and sqlite differ
            let diff_rows = limbo
                .iter()
                .zip(sqlite.iter())
                .filter(|(l, s)| l != s)
                .collect::<Vec<_>>();
            if !diff_rows.is_empty() {
                // due to different choices in index usage (usually in these cases sqlite is smart enough to use an index and we aren't),
                // sqlite might return rows in a different order
                // check if all limbo rows are present in sqlite
                let all_present = limbo.iter().all(|l| sqlite.iter().any(|s| l == s));
                if !all_present {
                    panic!("MISMATCHING ROWS (limbo: {}, sqlite: {}) for query: {}\n\n limbo: {:?}\n\n sqlite: {:?}\n\n differences: {:?}", limbo.len(), sqlite.len(), query, limbo, sqlite, diff_rows);
                }
            }
        }
    }

    #[test]
    pub fn fuzz_distinct() {
        let db = TempDatabase::new_empty(true);
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time_or_env();
        tracing::info!("fuzz_distinct seed: {}", seed);

        let columns = ["a", "b", "c", "d", "e"];

        // Create table with 3 integer columns
        let create_table = format!("CREATE TABLE t ({})", columns.join(", "));
        limbo_exec_rows(&db, &limbo_conn, &create_table);
        sqlite_exec_rows(&sqlite_conn, &create_table);

        // Insert some random data
        for _ in 0..1000 {
            let values = (0..columns.len())
                .map(|_| rng.random_range(1..3)) // intentionally narrow range
                .collect::<Vec<_>>();
            let query = format!(
                "INSERT INTO t VALUES ({})",
                values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            limbo_exec_rows(&db, &limbo_conn, &query);
            sqlite_exec_rows(&sqlite_conn, &query);
        }

        // Test different DISTINCT + ORDER BY combinations
        for _ in 0..300 {
            // Randomly select columns for DISTINCT
            let num_distinct_cols = rng.random_range(1..=columns.len());
            let mut available_cols = columns.to_vec();
            let mut distinct_cols = Vec::with_capacity(num_distinct_cols);

            for _ in 0..num_distinct_cols {
                let idx = rng.random_range(0..available_cols.len());
                distinct_cols.push(available_cols.remove(idx));
            }
            let distinct_cols = distinct_cols.join(", ");

            // Randomly select columns for ORDER BY
            let num_order_cols = rng.random_range(1..=columns.len());
            let mut available_cols = columns.to_vec();
            let mut order_cols = Vec::with_capacity(num_order_cols);

            for _ in 0..num_order_cols {
                let idx = rng.random_range(0..available_cols.len());
                order_cols.push(available_cols.remove(idx));
            }
            let order_cols = order_cols.join(", ");

            let query = format!("SELECT DISTINCT {distinct_cols} FROM t ORDER BY {order_cols}");

            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(limbo, sqlite, "seed: {seed}, query: {query}");
        }
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_create_table_drop_table_alter_table() {
        let db = TempDatabase::new_empty(true);
        let limbo_conn = db.connect_limbo();

        let (mut rng, seed) = rng_from_time_or_env();
        tracing::info!("create_table_drop_table_fuzz seed: {}", seed);

        // Keep track of current tables and their columns in memory
        let mut current_tables: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut table_counter = 0;

        // Column types for random generation
        const COLUMN_TYPES: [&str; 6] = ["INTEGER", "TEXT", "REAL", "BLOB", "BOOLEAN", "NUMERIC"];
        const COLUMN_NAMES: [&str; 8] = [
            "id", "name", "value", "data", "info", "field", "col", "attr",
        ];

        let mut undroppable_cols = HashSet::new();

        for iteration in 0..50000 {
            println!("iteration: {iteration} (seed: {seed})");
            let operation = rng.random_range(0..100); // 0: create, 1: drop, 2: alter, 3: alter rename

            match operation {
                0..20 => {
                    // Create table
                    if current_tables.len() < 10 {
                        // Limit number of tables
                        let table_name = format!("table_{table_counter}");
                        table_counter += 1;

                        let num_columns = rng.random_range(1..6);
                        let mut columns = Vec::new();

                        for i in 0..num_columns {
                            let col_name = if i == 0 && rng.random_bool(0.3) {
                                "id".to_string()
                            } else {
                                format!(
                                    "{}_{}",
                                    COLUMN_NAMES[rng.random_range(0..COLUMN_NAMES.len())],
                                    rng.random_range(0..u64::MAX)
                                )
                            };

                            let col_type = COLUMN_TYPES[rng.random_range(0..COLUMN_TYPES.len())];
                            let constraint = if i == 0 && rng.random_bool(0.2) {
                                " PRIMARY KEY"
                            } else if rng.random_bool(0.1) {
                                " UNIQUE"
                            } else {
                                ""
                            };

                            if constraint.contains("UNIQUE") || constraint.contains("PRIMARY KEY") {
                                undroppable_cols.insert((table_name.clone(), col_name.clone()));
                            }

                            columns.push(format!("{col_name} {col_type}{constraint}"));
                        }

                        let create_sql =
                            format!("CREATE TABLE {} ({})", table_name, columns.join(", "));

                        // Execute the create table statement
                        limbo_exec_rows(&db, &limbo_conn, &create_sql);

                        // Successfully created table, update our tracking
                        current_tables.insert(
                            table_name.clone(),
                            columns
                                .iter()
                                .map(|c| c.split_whitespace().next().unwrap().to_string())
                                .collect(),
                        );
                    }
                }

                20..30 => {
                    // Drop table
                    if !current_tables.is_empty() {
                        let table_names: Vec<String> = current_tables.keys().cloned().collect();
                        let table_to_drop = &table_names[rng.random_range(0..table_names.len())];

                        let drop_sql = format!("DROP TABLE {table_to_drop}");
                        limbo_exec_rows(&db, &limbo_conn, &drop_sql);

                        // Successfully dropped table, update our tracking
                        current_tables.remove(table_to_drop);
                    }
                }
                30..60 => {
                    // Alter table - add column
                    if !current_tables.is_empty() {
                        let table_names: Vec<String> = current_tables.keys().cloned().collect();
                        let table_to_alter = &table_names[rng.random_range(0..table_names.len())];

                        let new_col_name = format!("new_col_{}", rng.random_range(0..u64::MAX));
                        let col_type = COLUMN_TYPES[rng.random_range(0..COLUMN_TYPES.len())];

                        let alter_sql = format!(
                            "ALTER TABLE {} ADD COLUMN {} {}",
                            table_to_alter, &new_col_name, col_type
                        );

                        limbo_exec_rows(&db, &limbo_conn, &alter_sql);

                        // Successfully added column, update our tracking
                        let table_name = table_to_alter.clone();
                        if let Some(columns) = current_tables.get_mut(&table_name) {
                            columns.push(new_col_name);
                        }
                    }
                }
                60..100 => {
                    // Alter table - drop column
                    if !current_tables.is_empty() {
                        let table_names: Vec<String> = current_tables.keys().cloned().collect();
                        let table_to_alter = &table_names[rng.random_range(0..table_names.len())];

                        let table_name = table_to_alter.clone();
                        if let Some(columns) = current_tables.get(&table_name) {
                            let droppable_cols = columns
                                .iter()
                                .filter(|c| {
                                    !undroppable_cols.contains(&(table_name.clone(), c.to_string()))
                                })
                                .collect::<Vec<_>>();
                            if columns.len() > 1 && !droppable_cols.is_empty() {
                                // Don't drop the last column
                                let col_index = rng.random_range(0..droppable_cols.len());
                                let col_to_drop = droppable_cols[col_index].clone();

                                let alter_sql = format!(
                                    "ALTER TABLE {table_to_alter} DROP COLUMN {col_to_drop}"
                                );
                                limbo_exec_rows(&db, &limbo_conn, &alter_sql);

                                // Successfully dropped column, update our tracking
                                let columns = current_tables.get_mut(&table_name).unwrap();
                                columns.retain(|c| c != &col_to_drop);
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        // Final verification - the test passes if we didn't crash
        println!(
            "create_table_drop_table_fuzz completed successfully with {} tables remaining",
            current_tables.len()
        );
    }
}
