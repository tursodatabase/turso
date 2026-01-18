pub mod cte;
pub mod grammar_generator;
pub mod journal_mode;
pub mod orderby_collation;
pub mod rowid_alias;
pub mod test_join_optimizer;

#[cfg(test)]
mod fuzz_tests {
    use rand::seq::{IndexedRandom, IteratorRandom, SliceRandom};
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::{params, types::Value};
    use std::{collections::HashSet, io::Write};
    use tempfile::{NamedTempFile, TempDir};

    use core_tester::common::{
        do_flush, limbo_exec_rows, limbo_exec_rows_fallible, limbo_stmt_get_column_names,
        maybe_setup_tracing, rng_from_time_or_env, rusqlite_integrity_check, sqlite_exec_rows,
        TempDatabase,
    };

    use super::grammar_generator::{const_str, rand_int, rand_str, GrammarGenerator};

    use super::grammar_generator::SymbolHandle;

    /// [See this issue for more info](https://github.com/tursodatabase/turso/issues/1763)
    #[turso_macros::test(mvcc)]
    pub fn fuzz_failure_issue_1763(db: TempDatabase) {
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
        let offending_query = "SELECT ((ceil(pow((((2.0))), (-2.0 - -1.0) / log(0.5)))) - -2.0)";
        let limbo_result = limbo_exec_rows(&limbo_conn, offending_query);
        let sqlite_result = sqlite_exec_rows(&sqlite_conn, offending_query);
        assert_eq!(
            limbo_result, sqlite_result,
            "query: {offending_query}, limbo: {limbo_result:?}, sqlite: {sqlite_result:?}"
        );
    }

    #[turso_macros::test(mvcc)]
    pub fn arithmetic_expression_fuzz_ex1(db: TempDatabase) {
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        for query in [
            "SELECT ~1 >> 1536",
            "SELECT ~ + 3 << - ~ (~ (8)) - + -1 - 3 >> 3 + -6 * (-7 * 9 >> - 2)",
        ] {
            let limbo = limbo_exec_rows(&limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}"
            );
        }
    }

    // INTEGER PRIMARY KEY is a rowid alias, so an index is not created
    #[turso_macros::test(
        mvcc,
        init_sql = "CREATE TABLE t (x INTEGER PRIMARY KEY autoincrement)"
    )]
    pub fn rowid_seek_fuzz(db: TempDatabase) {
        let _ = tracing_subscriber::fmt::try_init();
        let sqlite_path = db.path.parent().unwrap().join("sqlite.db");
        let sqlite_conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        sqlite_conn
            .execute(db.init_sql.as_ref().unwrap(), [])
            .unwrap();

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
        let sqlite_conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        let limbo_conn = db.connect_limbo();
        limbo_exec_rows(&limbo_conn, &insert);

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
            tracing::info!("rowid_seek_fuzz iteration: {}", iteration);

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

                        tracing::info!("query: {query}");
                        let limbo_result = limbo_exec_rows(&limbo_conn, &query);
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

    #[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (x PRIMARY KEY)")]
    pub fn index_scan_fuzz(db: TempDatabase) {
        maybe_setup_tracing();
        let sqlite_path = db.path.parent().unwrap().join("sqlite.db");
        let sqlite_conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        sqlite_conn
            .execute(db.init_sql.as_ref().unwrap(), [])
            .unwrap();

        let insert = format!(
            "INSERT INTO t VALUES {}",
            (0..10000)
                .map(|x| format!("({x})"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        sqlite_conn.execute(&insert, params![]).unwrap();
        sqlite_conn.close().unwrap();
        let sqlite_conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        let limbo_conn = db.connect_limbo();
        limbo_exec_rows(&limbo_conn, &insert);

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
                    let limbo = limbo_exec_rows(&limbo_conn, &query);
                    let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
                    assert_eq!(
                        limbo, sqlite,
                        "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}",
                    );
                }
            }
        }
    }

    #[turso_macros::test(mvcc)]
    /// A test for verifying that index seek+scan works correctly for compound keys
    /// on indexes with various column orderings.
    pub fn index_scan_compound_key_fuzz(db: TempDatabase) {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();

        let is_mvcc = db.enable_mvcc;
        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);
        let table_defs = [
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
        let dbs = table_defs
            .iter()
            .map(|init_sql| builder.clone().with_init_sql(init_sql).build())
            .collect::<Vec<_>>();
        let mut pk_tuples = HashSet::new();
        let num_tuples = if is_mvcc { 10000 } else { 100000 };
        while pk_tuples.len() < num_tuples {
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

        let tmp_dir = TempDir::new().unwrap();
        let sqlite_paths: Vec<_> = dbs
            .iter()
            .enumerate()
            .map(|(i, db)| {
                if is_mvcc {
                    tmp_dir
                        .path()
                        .join(std::path::PathBuf::from(format!("sqlite_{i}.db")))
                } else {
                    db.path.clone()
                }
            })
            .collect();
        // Insert all tuples into all databases
        // In mvcc we need to create separate databases as SQLite will not read an MVCC database
        let sqlite_conns = sqlite_paths
            .iter()
            .map(|db| rusqlite::Connection::open(db).unwrap())
            .collect::<Vec<_>>();
        for (i, sqlite_conn) in sqlite_conns.into_iter().enumerate() {
            if is_mvcc {
                sqlite_conn.execute(table_defs[i], []).unwrap();
            }
            sqlite_conn.execute(&insert, params![]).unwrap();
            sqlite_conn.close().unwrap();
        }
        let sqlite_conns = sqlite_paths
            .iter()
            .map(|db| rusqlite::Connection::open(db).unwrap())
            .collect::<Vec<_>>();
        let limbo_conns = dbs.iter().map(|db| db.connect_limbo()).collect::<Vec<_>>();
        if is_mvcc {
            for limbo_conn in limbo_conns.iter() {
                limbo_conn.execute(&insert).unwrap();
            }
        }

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

            /// Generate a comparison string (e.g. x > 10 AND x < 20) or just x > 10.
            fn generate_comparison(
                operator: &str,
                col_name: &str,
                col_val: i32,
                rng: &mut ChaCha8Rng,
            ) -> String {
                // 5% chance of using NULL as the comparison value
                let val_str = if rng.random_range(0..20) == 0 {
                    "NULL".to_string()
                } else {
                    col_val.to_string()
                };
                if operator != "=" && rng.random_range(0..3) == 1 {
                    let val2 = if rng.random_range(0..20) == 0 {
                        "NULL".to_string()
                    } else {
                        rng.random_range(0..=3000).to_string()
                    };
                    let op2 = COMPARISONS[rng.random_range(0..COMPARISONS.len())];
                    format!("{col_name} {operator} {val_str} AND {col_name} {op2} {val2}")
                } else {
                    format!("{col_name} {operator} {val_str}")
                }
            }

            // Generate WHERE clause string.
            // Sometimes add another inequality to the WHERE clause (e.g. x > 10 AND x < 20) to exercise range queries.
            let where_clause_components = vec![
                comp1.map(|x| generate_comparison(x, "x", col_val_first.unwrap(), &mut rng)),
                comp2.map(|x| generate_comparison(x, "y", col_val_second.unwrap(), &mut rng)),
                comp3.map(|x| generate_comparison(x, "z", col_val_third.unwrap(), &mut rng)),
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
                let limbo = limbo_exec_rows(&limbo_conns[i], &query);
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
                    let limbo_no_limit = limbo_exec_rows(&limbo_conns[i], &query_no_limit);
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

    fn join_fuzz_inner(db: TempDatabase, add_indexes: bool, iterations: usize, rows: i64) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("join_fuzz_inner (add_indexes={add_indexes}) seed: {seed}",);

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);
        let limbo_db = builder.clone().build();
        let sqlite_db = builder.clone().build();
        let limbo_conn = limbo_db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

        let schema = r#"
        CREATE TABLE t1(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t2(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t3(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t4(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);"#;

        sqlite_conn.execute_batch(schema).unwrap();
        limbo_conn.prepare_execute_batch(schema).unwrap();

        if add_indexes {
            let index_ddl = r#"
            CREATE INDEX t1_a_idx ON t1(a);
            CREATE INDEX t1_b_idx ON t1(b);
            CREATE INDEX t1_c_idx ON t1(c);
            CREATE INDEX t1_d_idx ON t1(d);

            CREATE INDEX t2_a_idx ON t2(a);
            CREATE INDEX t2_b_idx ON t2(b);
            CREATE INDEX t2_c_idx ON t2(c);
            CREATE INDEX t2_d_idx ON t2(d);

            CREATE INDEX t3_a_idx ON t3(a);
            CREATE INDEX t3_b_idx ON t3(b);
            CREATE INDEX t3_c_idx ON t3(c);
            CREATE INDEX t3_d_idx ON t3(d);

            CREATE INDEX t4_a_idx ON t4(a);
            CREATE INDEX t4_b_idx ON t4(b);
            CREATE INDEX t4_c_idx ON t4(c);
            CREATE INDEX t4_d_idx ON t4(d);
        "#;
            sqlite_conn.execute_batch(index_ddl).unwrap();
            limbo_conn.prepare_execute_batch(index_ddl).unwrap();
        }

        let tables = ["t1", "t2", "t3", "t4"];
        for (t_idx, tname) in tables.iter().enumerate() {
            for i in 0..rows {
                let id = i + 1 + (t_idx as i64) * 10_000;

                // 25% chance of NULL per column.
                let gen_val = |rng: &mut ChaCha8Rng| {
                    if rng.random_range(0..4) == 0 {
                        None
                    } else {
                        Some(rng.random_range(-10..=20))
                    }
                };
                let a = gen_val(&mut rng);
                let b = gen_val(&mut rng);
                let c = gen_val(&mut rng);
                let d = gen_val(&mut rng);

                let fmt_val = |v: Option<i32>| match v {
                    Some(x) => x.to_string(),
                    None => "NULL".to_string(),
                };

                let stmt = format!(
                    "INSERT INTO {tname}(id,a,b,c,d) VALUES ({id}, {a}, {b}, {c}, {d})",
                    a = fmt_val(a),
                    b = fmt_val(b),
                    c = fmt_val(c),
                    d = fmt_val(d),
                );

                sqlite_conn.execute(&stmt, params![]).unwrap();
                limbo_conn.execute(&stmt).unwrap();
            }
        }

        let non_pk_cols = ["a", "b", "c", "d"];

        for iter in 0..iterations {
            if iter % (iterations / 100).max(1) == 0 {
                println!(
                    "join_fuzz_inner(add_indexes={}) iter {}/{}",
                    add_indexes,
                    iter + 1,
                    iterations
                );
            }

            let num_tables = rng.random_range(2..=4);
            let used_tables = &tables[..num_tables];

            let mut select_cols: Vec<String> = Vec::new();
            for t in used_tables.iter() {
                select_cols.push(format!("{t}.id"));
            }
            let select_clause = select_cols.join(", ");
            let mut from_clause = format!("FROM {}", used_tables[0]);
            for i in 1..num_tables {
                let left = used_tables[i - 1];
                let right = used_tables[i];

                let join_type = if rng.random_bool(0.5) {
                    "JOIN"
                } else {
                    "LEFT JOIN"
                };

                let num_preds = rng.random_range(1..=3);
                let mut preds = Vec::new();
                for _ in 0..num_preds {
                    let col = non_pk_cols[rng.random_range(0..non_pk_cols.len())];
                    preds.push(format!("{left}.{col} = {right}.{col}"));
                }
                preds.sort();
                preds.dedup();

                let on_clause = preds.join(" AND ");
                from_clause = format!("{from_clause} {join_type} {right} ON {on_clause}");
            }

            // WHERE clause: 0..2 predicates on non-pk cols
            let mut where_parts = Vec::new();
            let num_where = rng.random_range(0..=2);
            for _ in 0..num_where {
                let t = used_tables[rng.random_range(0..num_tables)];
                let col = non_pk_cols[rng.random_range(0..non_pk_cols.len())];
                let kind = rng.random_range(0..4);
                let cond = match kind {
                    0 => {
                        let val = rng.random_range(-10..=20);
                        format!("{t}.{col} = {val}")
                    }
                    1 => {
                        let val = rng.random_range(-10..=20);
                        format!("{t}.{col} <> {val}")
                    }
                    2 => format!("{t}.{col} IS NULL"),
                    3 => format!("{t}.{col} IS NOT NULL"),
                    _ => unreachable!(),
                };
                where_parts.push(cond);
            }
            let where_clause = if where_parts.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", where_parts.join(" AND "))
            };
            let order_clause = format!("ORDER BY {}", select_cols.join(", "));
            let limit = 50;
            let query = format!(
                "SELECT {select_clause} {from_clause} {where_clause} {order_clause} LIMIT {limit}",
            );
            let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
            let limbo_rows = limbo_exec_rows(&limbo_conn, &query);
            if sqlite_rows != limbo_rows {
                panic!(
                "JOIN FUZZ MISMATCH (add_indexes={})\nseed: {}\niteration: {}\nquery: {}\n\
                 sqlite ({} rows): {:?}\nlimbo ({} rows): {:?}\nsqlite path: {:?}\nlimbo path: {:?}",
                add_indexes,
                seed,
                iter,
                query,
                sqlite_rows.len(),
                sqlite_rows,
                limbo_rows.len(),
                limbo_rows,
                sqlite_db.path,
                limbo_db.path,
            );
            }
        }
    }

    #[turso_macros::test()]
    pub fn join_fuzz_unindexed_keys(db: TempDatabase) {
        join_fuzz_inner(db, false, 2000, 200);
    }

    #[turso_macros::test()]
    pub fn join_fuzz_indexed_keys(db: TempDatabase) {
        join_fuzz_inner(db, true, 2000, 200);
    }

    // TODO: Mvcc indexes
    #[turso_macros::test()]
    pub fn collation_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("collation_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        // Build six table variants that assign BINARY/NOCASE/RTRIM across (a,b,c)
        // and include UNIQUE constraints so that auto-created indexes must honor column collations.
        let variants: [(&str, &str, &str); 6] = [
            ("BINARY", "NOCASE", "RTRIM"),
            ("BINARY", "RTRIM", "NOCASE"),
            ("NOCASE", "BINARY", "RTRIM"),
            ("NOCASE", "RTRIM", "BINARY"),
            ("RTRIM", "BINARY", "NOCASE"),
            ("RTRIM", "NOCASE", "BINARY"),
        ];

        let table_defs: Vec<String> = variants
            .iter()
            .flat_map(|(ca, cb, cc)| {
                // Create unique indexes so that index seek/scan behavior with unique constraints is exercised too.
                vec![
                    // No unique constraints
                    format!(
                        "CREATE TABLE t (a TEXT COLLATE {ca}, b TEXT COLLATE {cb}, c TEXT COLLATE {cc})"
                    ),
                    // Single column unique constraints
                    format!(
                        "CREATE TABLE t (a TEXT COLLATE {ca}, b TEXT COLLATE {cb}, c TEXT COLLATE {cc}, UNIQUE(a))"
                    ),
                    format!(
                        "CREATE TABLE t (a TEXT COLLATE {ca}, b TEXT COLLATE {cb}, c TEXT COLLATE {cc}, UNIQUE(b))"
                    ),
                    format!(
                        "CREATE TABLE t (a TEXT COLLATE {ca}, b TEXT COLLATE {cb}, c TEXT COLLATE {cc}, UNIQUE(c))"
                    ),
                    // Two column unique constraints
                    format!(
                        "CREATE TABLE t (a TEXT COLLATE {ca}, b TEXT COLLATE {cb}, c TEXT COLLATE {cc}, UNIQUE(a,b))"
                    ),
                    format!(
                        "CREATE TABLE t (a TEXT COLLATE {ca}, b TEXT COLLATE {cb}, c TEXT COLLATE {cc}, UNIQUE(a,c))"
                    ),
                    format!(
                        "CREATE TABLE t (a TEXT COLLATE {ca}, b TEXT COLLATE {cb}, c TEXT COLLATE {cc}, UNIQUE(b,c))"
                    ),
                    // Three column unique constraint
                    format!(
                        "CREATE TABLE t (a TEXT COLLATE {ca}, b TEXT COLLATE {cb}, c TEXT COLLATE {cc}, UNIQUE(a,b,c))"
                    ),
                ]
            })
            .collect();

        // Create databases for each variant using rusqlite, then open limbo on the same file.
        let dbs: Vec<TempDatabase> = table_defs
            .iter()
            .map(|ddl| builder.clone().with_init_sql(ddl).build())
            .collect();

        // Seed data focuses on case and trailing spaces to exercise NOCASE and RTRIM semantics.
        const STR_POOL: [&str; 36] = [
            "", " ", "  ", "a", "A", "a ", "A  ", "aa", "Aa", "AA", "aa ", "AA   ", "abc", "ABC",
            "abc ", "ABC   ", "b", "B", "b ", "B  ", "ba", "BA", "ba ", "BA  ", "c", "C", "c  ",
            " C", "c C", "C c", "foo", "Foo", "FOO", "bar", "Bar", "BAR",
        ];

        // Insert rows into the SQLite side (shared file) and ignore uniqueness errors to keep seeding going.
        let row_target = 800usize;
        for db in dbs.iter() {
            let sqlite_conn = rusqlite::Connection::open(db.path.clone()).unwrap();
            for _ in 0..row_target {
                let a = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let b = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let c = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let insert = format!(
                    "INSERT INTO t(a,b,c) VALUES ('{}','{}','{}')",
                    a.replace("'", "''"),
                    b.replace("'", "''"),
                    c.replace("'", "''"),
                );
                let _ = sqlite_conn.execute(&insert, params![]);
            }
            sqlite_conn.close().unwrap();
        }

        // Open connections for query phase
        let sqlite_conns: Vec<rusqlite::Connection> = dbs
            .iter()
            .map(|db| rusqlite::Connection::open(db.path.clone()).unwrap())
            .collect();
        let limbo_conns: Vec<_> = dbs.iter().map(|db| db.connect_limbo()).collect();

        // Fuzz WHERE clauses with and without explicit COLLATE on a/b/c
        let columns = ["a", "b", "c"];
        let collates = [None, Some("BINARY"), Some("NOCASE"), Some("RTRIM")];
        let (mut rng, seed) = rng_from_time_or_env();
        println!("collation_fuzz seed: {seed}");

        const ITERS: usize = 1000;
        for iter in 0..ITERS {
            if iter % (ITERS / 100).max(1) == 0 {
                println!("collation_fuzz: iteration {}/{}", iter + 1, ITERS);
            }

            // Choose predicate spec
            let col = columns[rng.random_range(0..columns.len())];
            let coll = collates[rng.random_range(0..collates.len())];
            let val = STR_POOL[rng.random_range(0..STR_POOL.len())];
            let collate_clause = coll.map(|c| format!(" COLLATE {c}")).unwrap_or_default();
            let where_clause =
                format!("WHERE {col}{collate_clause} = '{}'", val.replace("'", "''"));

            let mut cols_clone = columns.to_vec();
            cols_clone.shuffle(&mut rng);
            let order_by = {
                let mut order_by = String::new();
                for col in cols_clone.iter() {
                    let collate = collates
                        .choose(&mut rng)
                        .unwrap()
                        .map(|c| format!(" COLLATE {c}"))
                        .unwrap_or_default();
                    let sort_order = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
                    order_by.push_str(&format!("{col}{collate} {sort_order}, "));
                }
                order_by.push_str("rowid ASC"); // sqlite and turso might return within-group rows in different orders which is semantically ok, so let's add rowid as a tiebreaker
                order_by
            };

            let query = format!("SELECT a, b, c FROM t {where_clause} ORDER BY {order_by}");
            for i in 0..sqlite_conns.len() {
                let sqlite_rows = sqlite_exec_rows(&sqlite_conns[i], &query);
                let limbo_rows = limbo_exec_rows(&limbo_conns[i], &query);
                assert_eq!(
                    sqlite_rows, limbo_rows,
                    "Different results! limbo: {:?}, sqlite: {:?}, seed: {}, query: {}, table def: {}",
                    limbo_rows, sqlite_rows, seed, query, table_defs[i]
                );
            }
        }
    }

    // TODO: mvcc indexes
    #[turso_macros::test()]
    #[allow(unused_assignments)]
    pub fn fk_deferred_constraints_and_triggers_fuzz(db: TempDatabase) {
        let _ = tracing_subscriber::fmt::try_init();
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("fk_deferred_constraints_and_triggers_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const OUTER_ITERS: usize = 10;
        const INNER_ITERS: usize = 100;

        for outer in 0..OUTER_ITERS {
            println!(
                "fk_deferred_constraints_and_triggers_fuzz {}/{}",
                outer + 1,
                OUTER_ITERS
            );

            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let mut stmts: Vec<String> = Vec::new();
            let mut log_and_exec = |sql: &str| {
                stmts.push(sql.to_string());
                sql.to_string()
            };
            // Enable FKs
            let s = log_and_exec("PRAGMA foreign_keys=ON");
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            let get_constraint_type = |rng: &mut ChaCha8Rng| match rng.random_range(0..3) {
                0 => "INTEGER PRIMARY KEY",
                1 => "UNIQUE",
                2 => "PRIMARY KEY",
                _ => unreachable!(),
            };

            // Mix of immediate and deferred FK constraints
            let s = log_and_exec(&format!(
                "CREATE TABLE parent(id {}, a INT, b INT)",
                get_constraint_type(&mut rng)
            ));
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Child with DEFERRABLE INITIALLY DEFERRED FK
            let s = log_and_exec(&format!(
                "CREATE TABLE child_deferred(id {}, pid INT, x INT, \
             FOREIGN KEY(pid) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
                get_constraint_type(&mut rng)
            ));
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Child with immediate FK (default)
            let s = log_and_exec(&format!(
                "CREATE TABLE child_immediate(id {}, pid INT, y INT, \
             FOREIGN KEY(pid) REFERENCES parent(id))",
                get_constraint_type(&mut rng)
            ));
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            let composite_constraint = match rng.random_range(0..2) {
                0 => "PRIMARY KEY",
                1 => "UNIQUE",
                _ => unreachable!(),
            };
            // Composite key parent for deferred testing
            let s = log_and_exec(
                &format!("CREATE TABLE parent_comp(a INT NOT NULL, b INT NOT NULL, c INT, {composite_constraint}(a,b))"),
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Child with composite deferred FK
            let s = log_and_exec(
                "CREATE TABLE child_comp_deferred(id INTEGER PRIMARY KEY, ca INT, cb INT, z INT, \
             FOREIGN KEY(ca,cb) REFERENCES parent_comp(a,b) DEFERRABLE INITIALLY DEFERRED)",
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Seed initial data
            let mut parent_ids = std::collections::HashSet::new();
            for _ in 0..rng.random_range(10..=25) {
                let id = rng.random_range(1..=50) as i64;
                if parent_ids.insert(id) {
                    let a = rng.random_range(-5..=25);
                    let b = rng.random_range(-5..=25);
                    let stmt = log_and_exec(&format!("INSERT INTO parent VALUES ({id}, {a}, {b})"));
                    limbo_exec_rows(&limbo, &stmt);
                    sqlite.execute(&stmt, params![]).unwrap();
                }
            }

            // Seed composite parent
            let mut comp_pairs = std::collections::HashSet::new();
            for _ in 0..rng.random_range(3..=10) {
                let a = rng.random_range(-3..=6) as i64;
                let b = rng.random_range(-3..=6) as i64;
                if comp_pairs.insert((a, b)) {
                    let c = rng.random_range(0..=20);
                    let stmt =
                        log_and_exec(&format!("INSERT INTO parent_comp VALUES ({a}, {b}, {c})"));
                    limbo_exec_rows(&limbo, &stmt);
                    sqlite.execute(&stmt, params![]).unwrap();
                }
            }

            // Add triggers on every outer iteration (max 2 triggers)
            // Create a log table for trigger operations
            let s = log_and_exec(
                "CREATE TABLE trigger_log(action TEXT, table_name TEXT, id_val INT, extra_val INT)",
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Create a stats table for tracking operations
            let s = log_and_exec("CREATE TABLE trigger_stats(op_type TEXT PRIMARY KEY, count INT)");
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Define all available trigger types
            let trigger_definitions: Vec<&str> = vec![
                // BEFORE INSERT trigger on parent - logs and potentially creates a child
                "CREATE TRIGGER trig_parent_before_insert BEFORE INSERT ON parent BEGIN
                 INSERT INTO trigger_log VALUES ('BEFORE_INSERT', 'parent', NEW.id, NEW.a);
                 INSERT INTO trigger_stats VALUES ('parent_insert', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Sometimes create a deferred child referencing this parent
                 INSERT INTO child_deferred VALUES (NEW.id + 10000, NEW.id, NEW.a);
                END",
                // AFTER INSERT trigger on child_deferred - logs and updates parent
                "CREATE TRIGGER trig_child_deferred_after_insert AFTER INSERT ON child_deferred BEGIN
                 INSERT INTO trigger_log VALUES ('AFTER_INSERT', 'child_deferred', NEW.id, NEW.pid);
                 INSERT INTO trigger_stats VALUES ('child_deferred_insert', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Update parent's 'a' column if parent exists
                 UPDATE parent SET a = a + 1 WHERE id = NEW.pid;
                END",
                // BEFORE UPDATE OF 'a' on parent - logs and modifies the update
                "CREATE TRIGGER trig_parent_before_update_a BEFORE UPDATE OF a ON parent BEGIN
                 INSERT INTO trigger_log VALUES ('BEFORE_UPDATE_A', 'parent', OLD.id, OLD.a);
                 INSERT INTO trigger_stats VALUES ('parent_update_a', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Also update 'b' column when 'a' is updated
                 UPDATE parent SET b = NEW.a * 2 WHERE id = NEW.id;
                END",
                // AFTER UPDATE OF 'pid' on child_deferred - logs and creates/updates related records
                "CREATE TRIGGER trig_child_deferred_after_update_pid AFTER UPDATE OF pid ON child_deferred BEGIN
                 INSERT INTO trigger_log VALUES ('AFTER_UPDATE_PID', 'child_deferred', NEW.id, NEW.pid);
                 INSERT INTO trigger_stats VALUES ('child_deferred_update_pid', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Create a child_immediate referencing the new parent
                 INSERT INTO child_immediate VALUES (NEW.id + 20000, NEW.pid, NEW.x);
                 -- Update parent's 'b' column
                 UPDATE parent SET b = b + 1 WHERE id = NEW.pid;
                END",
                // BEFORE DELETE on parent - logs and cascades to children
                "CREATE TRIGGER trig_parent_before_delete BEFORE DELETE ON parent BEGIN
                 INSERT INTO trigger_log VALUES ('BEFORE_DELETE', 'parent', OLD.id, OLD.a);
                 INSERT INTO trigger_stats VALUES ('parent_delete', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Delete all children that reference the deleted parent
                 DELETE FROM child_deferred WHERE pid = OLD.id;
                END",
                // AFTER DELETE on child_deferred - logs and updates parent stats
                "CREATE TRIGGER trig_child_deferred_after_delete AFTER DELETE ON child_deferred BEGIN
                 INSERT INTO trigger_log VALUES ('AFTER_DELETE', 'child_deferred', OLD.id, OLD.pid);
                 INSERT INTO trigger_stats VALUES ('child_deferred_delete', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Update parent's 'a' column
                 UPDATE parent SET a = a - 1 WHERE id = OLD.pid;
                END",
                // BEFORE INSERT on child_immediate - logs, creates parent if needed, updates stats
                "CREATE TRIGGER trig_child_immediate_before_insert BEFORE INSERT ON child_immediate BEGIN
                 INSERT INTO trigger_log VALUES ('BEFORE_INSERT', 'child_immediate', NEW.id, NEW.pid);
                 INSERT INTO trigger_stats VALUES ('child_immediate_insert', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Create parent if it doesn't exist (with a default value)
                 INSERT OR IGNORE INTO parent VALUES (NEW.pid, NEW.y, NEW.y * 2);
                 -- Update parent's 'a' column
                 UPDATE parent SET a = a + NEW.y WHERE id = NEW.pid;
                END",
                // AFTER UPDATE OF 'y' on child_immediate - logs and cascades updates
                "CREATE TRIGGER trig_child_immediate_after_update_y AFTER UPDATE OF y ON child_immediate BEGIN
                 INSERT INTO trigger_log VALUES ('AFTER_UPDATE_Y', 'child_immediate', NEW.id, NEW.y);
                 INSERT INTO trigger_stats VALUES ('child_immediate_update_y', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Update parent's 'a' based on the change
                 UPDATE parent SET a = a + (NEW.y - OLD.y) WHERE id = NEW.pid;
                 -- Also create a deferred child referencing the same parent
                 INSERT INTO child_deferred VALUES (NEW.id + 30000, NEW.pid, NEW.y);
                END",
                // BEFORE UPDATE on child_deferred using UPDATE OR IGNORE - tests OR IGNORE propagation from UPDATE
                "CREATE TRIGGER trig_child_deferred_before_update_or_ignore BEFORE UPDATE ON child_deferred BEGIN
                 INSERT INTO trigger_log VALUES ('BEFORE_UPDATE', 'child_deferred', NEW.id, NEW.pid);
                 INSERT INTO trigger_stats VALUES ('child_deferred_update', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Try to insert a parent that might already exist (OR IGNORE should propagate)
                 INSERT INTO parent VALUES (NEW.pid, NEW.x, NEW.x * 2);
                 -- Update another child_deferred row (might conflict on unique constraint)
                 UPDATE OR IGNORE child_deferred SET x = NEW.x WHERE id = NEW.id + 1;
                END",
                // AFTER INSERT on parent using UPDATE OR REPLACE in trigger - tests OR REPLACE propagation
                "CREATE TRIGGER trig_parent_after_insert_or_replace AFTER INSERT ON parent BEGIN
                 INSERT INTO trigger_log VALUES ('AFTER_INSERT', 'parent', NEW.id, NEW.a);
                 INSERT INTO trigger_stats VALUES ('parent_after_insert', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Use UPDATE OR REPLACE which might delete conflicting rows
                 UPDATE OR REPLACE parent SET a = NEW.a + 1 WHERE id = NEW.id + 1;
                END",
                // BEFORE DELETE on child_immediate using UPDATE OR IGNORE - tests cascading with OR IGNORE
                "CREATE TRIGGER trig_child_immediate_before_delete BEFORE DELETE ON child_immediate BEGIN
                 INSERT INTO trigger_log VALUES ('BEFORE_DELETE', 'child_immediate', OLD.id, OLD.pid);
                 INSERT INTO trigger_stats VALUES ('child_immediate_delete', 1) ON CONFLICT(op_type) DO UPDATE SET count=count+1;
                 -- Try to update parent, ignoring if it would cause constraint violation
                 UPDATE OR IGNORE parent SET id = OLD.pid + 50000 WHERE id = OLD.pid;
                END",
            ];

            // Randomly select up to 2 triggers from the list
            let num_triggers = rng.random_range(1..=2);
            let mut selected_indices = std::collections::HashSet::new();
            while selected_indices.len() < num_triggers {
                selected_indices.insert(rng.random_range(0..trigger_definitions.len()));
            }

            // Create the selected triggers
            for &idx in selected_indices.iter() {
                let s = log_and_exec(trigger_definitions[idx]);
                limbo_exec_rows(&limbo, &s);
                sqlite.execute(&s, params![]).unwrap();
            }

            // Transaction-based mutations with mix of deferred and immediate operations
            let mut in_tx = false;
            for tx_num in 0..INNER_ITERS {
                // Decide if we're in a transaction
                let start_a_transaction = rng.random_bool(0.7);

                if start_a_transaction && !in_tx {
                    in_tx = true;
                    let s = log_and_exec("BEGIN");
                    let sres = sqlite.execute(&s, params![]);
                    let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &s);
                    match (&sres, &lres) {
                        (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                        _ => {
                            eprintln!("BEGIN mismatch");
                            eprintln!("sqlite result: {sres:?}");
                            eprintln!("limbo result: {lres:?}");
                            let file = std::fs::File::create("fk_deferred.sql").unwrap();
                            for stmt in stmts.iter() {
                                writeln!(&file, "{stmt};").unwrap();
                            }
                            eprintln!("Wrote `tests/fk_deferred.sql` for debugging");
                            eprintln!("turso path: {}", limbo_db.path.display());
                            eprintln!("sqlite path: {}", sqlite_db.path.display());
                            panic!("BEGIN mismatch");
                        }
                    }
                }

                let op = rng.random_range(0..12);
                let stmt = match op {
                    // Insert into child_deferred (can violate temporarily in transaction)
                    0 => {
                        let id = rng.random_range(1000..=2000);
                        let pid = if rng.random_bool(0.6) {
                            *parent_ids.iter().choose(&mut rng).unwrap_or(&1)
                        } else {
                            // Non-existent parent - OK if deferred and fixed before commit
                            rng.random_range(200..=300) as i64
                        };
                        let x = rng.random_range(-10..=10);
                        format!("INSERT INTO child_deferred VALUES ({id}, {pid}, {x})")
                    }
                    // Insert into child_immediate (must satisfy FK immediately)
                    1 => {
                        let id = rng.random_range(3000..=4000);
                        let pid = if rng.random_bool(0.8) {
                            *parent_ids.iter().choose(&mut rng).unwrap_or(&1)
                        } else {
                            rng.random_range(200..=300) as i64
                        };
                        let y = rng.random_range(-10..=10);
                        format!("INSERT INTO child_immediate VALUES ({id}, {pid}, {y})")
                    }
                    // Insert parent (may fix deferred violations)
                    2 => {
                        let id = rng.random_range(1..=300);
                        let a = rng.random_range(-5..=25);
                        let b = rng.random_range(-5..=25);
                        parent_ids.insert(id as i64);
                        format!("INSERT INTO parent VALUES ({id}, {a}, {b})")
                    }
                    // Delete parent (may cause violations)
                    3 => {
                        let id = if rng.random_bool(0.5) {
                            *parent_ids.iter().choose(&mut rng).unwrap_or(&1)
                        } else {
                            rng.random_range(1..=300) as i64
                        };
                        format!("DELETE FROM parent WHERE id={id}")
                    }
                    // Update parent PK
                    4 => {
                        let old = rng.random_range(1..=300);
                        let new = rng.random_range(1..=350);
                        format!("UPDATE parent SET id={new} WHERE id={old}")
                    }
                    // Update child_deferred FK
                    5 => {
                        let id = rng.random_range(1000..=2000);
                        let pid = if rng.random_bool(0.5) {
                            *parent_ids.iter().choose(&mut rng).unwrap_or(&1)
                        } else {
                            rng.random_range(200..=400) as i64
                        };
                        format!("UPDATE child_deferred SET pid={pid} WHERE id={id}")
                    }
                    // Insert into composite deferred child
                    6 => {
                        let id = rng.random_range(5000..=6000);
                        let (ca, cb) = if rng.random_bool(0.6) {
                            *comp_pairs.iter().choose(&mut rng).unwrap_or(&(1, 1))
                        } else {
                            // Non-existent composite parent
                            (
                                rng.random_range(-5..=8) as i64,
                                rng.random_range(-5..=8) as i64,
                            )
                        };
                        let z = rng.random_range(0..=10);
                        format!(
                                "INSERT INTO child_comp_deferred VALUES ({id}, {ca}, {cb}, {z}) ON CONFLICT DO NOTHING"
                            )
                    }
                    // Insert composite parent
                    7 => {
                        let a = rng.random_range(-5..=8) as i64;
                        let b = rng.random_range(-5..=8) as i64;
                        let c = rng.random_range(0..=20);
                        comp_pairs.insert((a, b));
                        format!("INSERT INTO parent_comp VALUES ({a}, {b}, {c})")
                    }
                    // UPSERT with deferred child
                    8 => {
                        let id = rng.random_range(1000..=2000);
                        let pid = if rng.random_bool(0.5) {
                            *parent_ids.iter().choose(&mut rng).unwrap_or(&1)
                        } else {
                            rng.random_range(200..=400) as i64
                        };
                        let x = rng.random_range(-10..=10);
                        format!(
                            "INSERT INTO child_deferred VALUES ({id}, {pid}, {x})
                             ON CONFLICT(id) DO UPDATE SET pid=excluded.pid, x=excluded.x"
                        )
                    }
                    // Delete from child_deferred
                    9 => {
                        let id = rng.random_range(1000..=2000);
                        format!("DELETE FROM child_deferred WHERE id={id}")
                    }
                    // Self-referential deferred insert (create temp violation then fix)
                    10 if start_a_transaction => {
                        let id = rng.random_range(400..=500);
                        let pid = id + 1; // References non-existent yet
                        format!("INSERT INTO child_deferred VALUES ({id}, {pid}, 0)")
                    }
                    _ => {
                        // Default: simple parent insert
                        let id = rng.random_range(1..=300);
                        format!("INSERT INTO parent VALUES ({id}, 0, 0)")
                    }
                };

                let stmt = log_and_exec(&stmt);
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);

                if !start_a_transaction && !in_tx {
                    match (sres, lres) {
                        (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                        (s, l) => {
                            eprintln!("Non-tx mismatch: sqlite={s:?}, limbo={l:?}");
                            eprintln!("Statement: {stmt}");
                            eprintln!("Seed: {seed}, outer: {outer}, tx: {tx_num}, in_tx={in_tx}");
                            let mut file = std::fs::File::create("fk_deferred.sql").unwrap();
                            for stmt in stmts.iter() {
                                writeln!(file, "{stmt};").expect("write to file");
                            }
                            eprintln!("turso path: {}", limbo_db.path.display());
                            eprintln!("sqlite path: {}", sqlite_db.path.display());
                            panic!("Non-transactional operation mismatch, file written to 'tests/fk_deferred.sql'");
                        }
                    }
                }

                // Randomly COMMIT or ROLLBACK some of the time
                if in_tx && rng.random_bool(0.4) {
                    let commit = rng.random_bool(0.7);
                    let s = log_and_exec("COMMIT");

                    let sres = sqlite.execute(&s, params![]);
                    let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &s);

                    match (sres, lres) {
                        (Ok(_), Ok(_)) => {}
                        (Err(_), Err(_)) => {
                            // Both failed - OK, deferred constraint violation at commit
                            if commit && in_tx {
                                in_tx = false;
                                let s = if commit {
                                    log_and_exec("ROLLBACK")
                                } else {
                                    log_and_exec("SELECT 1") // noop if we already rolled back
                                };

                                let sres = sqlite.execute(&s, params![]);
                                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &s);
                                match (sres, lres) {
                                    (Ok(_), Ok(_)) => {}
                                    (s, l) => {
                                        eprintln!("Post-failed-commit cleanup mismatch: sqlite={s:?}, limbo={l:?}");
                                        let mut file =
                                            std::fs::File::create("fk_deferred.sql").unwrap();
                                        for stmt in stmts.iter() {
                                            writeln!(file, "{stmt};").expect("write to file");
                                        }
                                        eprintln!("turso path: {}", limbo_db.path.display());
                                        eprintln!("sqlite path: {}", sqlite_db.path.display());
                                        panic!("Post-failed-commit cleanup mismatch, file written to 'tests/fk_deferred.sql'");
                                    }
                                }
                            }
                        }
                        (s, l) => {
                            eprintln!("\n=== COMMIT/ROLLBACK mismatch ===");
                            eprintln!("Operation: {s:?}");
                            eprintln!("sqlite={s:?}, limbo={l:?}");
                            eprintln!("Seed: {seed}, outer: {outer}, tx: {tx_num}, in_tx={in_tx}");
                            eprintln!("--- Replay statements ({}) ---", stmts.len());
                            let mut file = std::fs::File::create("fk_deferred.sql").unwrap();
                            for stmt in stmts.iter() {
                                writeln!(file, "{stmt};").expect("write to file");
                            }
                            eprintln!("Turso path: {}", limbo_db.path.display());
                            eprintln!("Sqlite path: {}", sqlite_db.path.display());
                            panic!(
                                "outcome mismatch, .sql file written to `tests/fk_deferred.sql`"
                            );
                        }
                    }
                    in_tx = false;
                }
            }
            // Print all statements
            if std::env::var("VERBOSE").is_ok() {
                println!("{}", stmts.join("\n"));
                println!("--------- ITERATION COMPLETED ---------");
            }
        }
    }

    // TODO: mvcc state mismatch
    #[turso_macros::test()]
    pub fn fk_single_pk_mutation_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("fk_single_pk_mutation_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const OUTER_ITERS: usize = 20;
        const INNER_ITERS: usize = 100;

        for outer in 0..OUTER_ITERS {
            println!("fk_single_pk_mutation_fuzz {}/{}", outer + 1, OUTER_ITERS);

            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            // Statement log for this iteration
            let mut stmts: Vec<String> = Vec::new();
            let mut log_and_exec = |sql: &str| {
                stmts.push(sql.to_string());
                sql.to_string()
            };

            // Enable FKs in both engines
            let s = log_and_exec("PRAGMA foreign_keys=ON");
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            let s = log_and_exec("CREATE TABLE p(id INTEGER PRIMARY KEY, a INT, b INT)");
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            let s = log_and_exec(
            "CREATE TABLE c(id INTEGER PRIMARY KEY, x INT, y INT, FOREIGN KEY(x) REFERENCES p(id))",
        );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Seed parent
            let n_par = rng.random_range(5..=40);
            let mut used_ids = std::collections::HashSet::new();
            for _ in 0..n_par {
                let mut id;
                loop {
                    id = rng.random_range(1..=200) as i64;
                    if used_ids.insert(id) {
                        break;
                    }
                }
                let a = rng.random_range(-5..=25);
                let b = rng.random_range(-5..=25);
                let stmt = log_and_exec(&format!("INSERT INTO p VALUES ({id}, {a}, {b})"));
                let l_res = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                let s_res = sqlite.execute(&stmt, params![]);
                match (l_res, s_res) {
                    (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                    _ => {
                        panic!("Seeding parent insert mismatch");
                    }
                }
            }

            // Seed child
            let n_child = rng.random_range(5..=80);
            for i in 0..n_child {
                let id = 1000 + i as i64;
                let x = if rng.random_bool(0.8) {
                    *used_ids.iter().choose(&mut rng).unwrap()
                } else {
                    rng.random_range(1..=220) as i64
                };
                let y = rng.random_range(-10..=10);
                let stmt = log_and_exec(&format!("INSERT INTO c VALUES ({id}, {x}, {y})"));
                match (
                    sqlite.execute(&stmt, params![]),
                    limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt),
                ) {
                    (Ok(_), Ok(_)) => {}
                    (Err(_), Err(_)) => {}
                    (x, y) => {
                        eprintln!("\n=== FK fuzz failure (seeding mismatch) ===");
                        eprintln!("seed: {seed}, outer: {}", outer + 1);
                        eprintln!("sqlite: {x:?}, limbo: {y:?}");
                        eprintln!("last stmt: {stmt}");
                        eprintln!("--- replay statements ({}) ---", stmts.len());
                        for (i, s) in stmts.iter().enumerate() {
                            eprintln!("{:04}: {};", i + 1, s);
                        }
                        panic!("Seeding child insert mismatch");
                    }
                }
            }

            // Mutations
            for _ in 0..INNER_ITERS {
                let action = rng.random_range(0..8);
                let stmt = match action {
                    // Parent INSERT
                    0 => {
                        let mut id;
                        let mut tries = 0;
                        loop {
                            id = rng.random_range(1..=250) as i64;
                            if !used_ids.contains(&id) || tries > 10 {
                                break;
                            }
                            tries += 1;
                        }
                        let a = rng.random_range(-5..=25);
                        let b = rng.random_range(-5..=25);
                        format!(
                            "INSERT {} INTO p VALUES({id}, {a}, {b})",
                            if rng.random_bool(0.5) {
                                "OR REPLACE "
                            } else {
                                "OR ROLLBACK"
                            }
                        )
                    }
                    // Parent UPDATE
                    1 => {
                        if rng.random_bool(0.5) {
                            let old = rng.random_range(1..=250);
                            let new_id = rng.random_range(1..=260);
                            format!("UPDATE p SET id={new_id} WHERE id={old}")
                        } else {
                            let a = rng.random_range(-5..=25);
                            let b = rng.random_range(-5..=25);
                            let tgt = rng.random_range(1..=260);
                            format!("UPDATE p SET a={a}, b={b} WHERE id={tgt}")
                        }
                    }
                    // Parent DELETE
                    2 => {
                        let del_id = rng.random_range(1..=260);
                        format!("DELETE FROM p WHERE id={del_id}")
                    }
                    // Child INSERT
                    3 => {
                        let id = rng.random_range(1000..=2000);
                        let x = if rng.random_bool(0.7) {
                            if let Some(p) = used_ids.iter().choose(&mut rng) {
                                *p
                            } else {
                                rng.random_range(1..=260) as i64
                            }
                        } else {
                            rng.random_range(1..=260) as i64
                        };
                        let y = rng.random_range(-10..=10);
                        format!(
                            "INSERT {} INTO c VALUES({id}, {x}, {y})",
                            if rng.random_bool(0.4) {
                                "OR REPLACE "
                            } else {
                                "OR FAIL"
                            }
                        )
                    }
                    // Child UPDATE
                    4 => {
                        let pick = rng.random_range(1000..=2000);
                        if rng.random_bool(0.6) {
                            let new_x = if rng.random_bool(0.7) {
                                if let Some(p) = used_ids.iter().choose(&mut rng) {
                                    *p
                                } else {
                                    rng.random_range(1..=260) as i64
                                }
                            } else {
                                rng.random_range(1..=260) as i64
                            };
                            format!("UPDATE c SET x={new_x} WHERE id={pick}")
                        } else {
                            let new_y = rng.random_range(-10..=10);
                            format!("UPDATE c SET y={new_y} WHERE id={pick}")
                        }
                    }
                    5 => {
                        // UPSERT parent
                        let pick = rng.random_range(1..=250);
                        if rng.random_bool(0.5) {
                            let a = rng.random_range(-5..=25);
                            let b = rng.random_range(-5..=25);
                            format!(
                                "INSERT INTO p VALUES({pick}, {a}, {b}) ON CONFLICT(id) DO UPDATE SET a=excluded.a, b=excluded.b"
                            )
                        } else {
                            let a = rng.random_range(-5..=25);
                            let b = rng.random_range(-5..=25);
                            format!(
                                "INSERT INTO p VALUES({pick}, {a}, {b}) \
                             ON CONFLICT(id) DO NOTHING"
                            )
                        }
                    }
                    6 => {
                        // UPSERT child
                        let pick = rng.random_range(1000..=2000);
                        if rng.random_bool(0.5) {
                            let x = if rng.random_bool(0.7) {
                                if let Some(p) = used_ids.iter().choose(&mut rng) {
                                    *p
                                } else {
                                    rng.random_range(1..=260) as i64
                                }
                            } else {
                                rng.random_range(1..=260) as i64
                            };
                            format!(
                                "INSERT INTO c VALUES({pick}, {x}, 0) ON CONFLICT(id) DO UPDATE SET x=excluded.x"
                            )
                        } else {
                            let x = if rng.random_bool(0.7) {
                                if let Some(p) = used_ids.iter().choose(&mut rng) {
                                    *p
                                } else {
                                    rng.random_range(1..=260) as i64
                                }
                            } else {
                                rng.random_range(1..=260) as i64
                            };
                            format!(
                                "INSERT INTO c VALUES({pick}, {x}, 0) ON CONFLICT(id) DO NOTHING"
                            )
                        }
                    }
                    // Child DELETE
                    _ => {
                        let pick = rng.random_range(1000..=2000);
                        format!("DELETE FROM c WHERE id={pick}")
                    }
                };

                let stmt = log_and_exec(&stmt);

                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);

                match (sres, lres) {
                    (Ok(_), Ok(_)) => {
                        if stmt.starts_with("INSERT INTO p VALUES(") {
                            if let Some(tok) = stmt.split_whitespace().nth(4) {
                                if let Some(idtok) = tok.split(['(', ',']).nth(1) {
                                    if let Ok(idnum) = idtok.parse::<i64>() {
                                        used_ids.insert(idnum);
                                    }
                                }
                            }
                        }
                        let sp = sqlite_exec_rows(&sqlite, "SELECT id,a,b FROM p ORDER BY id");
                        let sc = sqlite_exec_rows(&sqlite, "SELECT id,x,y FROM c ORDER BY id");
                        let lp = limbo_exec_rows(&limbo, "SELECT id,a,b FROM p ORDER BY id");
                        let lc = limbo_exec_rows(&limbo, "SELECT id,x,y FROM c ORDER BY id");

                        if sp != lp || sc != lc {
                            eprintln!("\n=== FK fuzz failure (state mismatch) ===");
                            eprintln!("seed: {seed}, outer: {}", outer + 1);
                            eprintln!("last stmt: {stmt}");
                            eprintln!("sqlite p: {sp:?}\nsqlite c: {sc:?}");
                            eprintln!("limbo  p: {lp:?}\nlimbo  c: {lc:?}");
                            eprintln!("--- replay statements ({}) ---", stmts.len());
                            for (i, s) in stmts.iter().enumerate() {
                                eprintln!("{:04}: {};", i + 1, s);
                            }
                            panic!("State mismatch");
                        }
                    }
                    (Err(_), Err(_)) => { /* parity OK */ }
                    (ok_sqlite, ok_limbo) => {
                        eprintln!("\n=== FK fuzz failure (outcome mismatch) ===");
                        eprintln!("seed: {seed}, outer: {}", outer + 1);
                        eprintln!("sqlite: {ok_sqlite:?}, limbo: {ok_limbo:?}");
                        eprintln!("last stmt: {stmt}");
                        // dump final states to help decide who is right
                        let sp = sqlite_exec_rows(&sqlite, "SELECT id,a,b FROM p ORDER BY id");
                        let sc = sqlite_exec_rows(&sqlite, "SELECT id,x,y FROM c ORDER BY id");
                        let lp = limbo_exec_rows(&limbo, "SELECT id,a,b FROM p ORDER BY id");
                        let lc = limbo_exec_rows(&limbo, "SELECT id,x,y FROM c ORDER BY id");
                        eprintln!("sqlite p: {sp:?}\nsqlite c: {sc:?}");
                        eprintln!("turso p: {lp:?}\nturso c: {lc:?}");
                        eprintln!(
                            "--- writing ({}) statements to fk_fuzz_statements.sql ---",
                            stmts.len()
                        );
                        let mut file = std::fs::File::create("fk_fuzz_statements.sql").unwrap();
                        for s in stmts.iter() {
                            let _ = file.write_fmt(format_args!("{s};\n"));
                        }
                        file.flush().unwrap();
                        panic!("DML outcome mismatch, statements written to tests/fk_fuzz_statements.sql");
                    }
                }
            }
        }
    }

    // TODO: mvcc does not work
    #[turso_macros::test()]
    pub fn fk_edgecases_fuzzing(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("fk_edgecases_minifuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const OUTER_ITERS: usize = 20;
        const INNER_ITERS: usize = 100;

        fn assert_parity(
            seed: u64,
            stmts: &[String],
            sqlite_res: rusqlite::Result<usize>,
            limbo_res: Result<Vec<Vec<rusqlite::types::Value>>, turso_core::LimboError>,
            last_stmt: &str,
            tag: &str,
        ) {
            match (sqlite_res.is_ok(), limbo_res.is_ok()) {
                (true, true) | (false, false) => (),
                _ => {
                    eprintln!("\n=== {tag} mismatch ===");
                    eprintln!("seed: {seed}");
                    eprintln!("sqlite: {sqlite_res:?}, limbo: {limbo_res:?}");
                    eprintln!("stmt: {last_stmt}");
                    eprintln!("--- replay statements ({}) ---", stmts.len());
                    for (i, s) in stmts.iter().enumerate() {
                        eprintln!("{:04}: {};", i + 1, s);
                    }
                    panic!("{tag}: engines disagree");
                }
            }
        }

        // parent rowid, child textified integers -> MustBeInt coercion path
        for outer in 0..OUTER_ITERS {
            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let mut stmts: Vec<String> = Vec::new();
            let log = |s: &str, stmts: &mut Vec<String>| {
                stmts.push(s.to_string());
                s.to_string()
            };

            for s in [
                "PRAGMA foreign_keys=ON",
                "CREATE TABLE p(id INTEGER PRIMARY KEY, a INT)",
                "CREATE TABLE c(id INTEGER PRIMARY KEY, x INT, FOREIGN KEY(x) REFERENCES p(id))",
            ] {
                let s = log(s, &mut stmts);
                let _ = limbo_exec_rows_fallible(&limbo_db, &limbo, &s);
                let _ = sqlite.execute(&s, params![]);
            }

            // Seed a few parents
            for _ in 0..rng.random_range(2..=5) {
                let id = rng.random_range(1..=15);
                let a = rng.random_range(-5..=5);
                let s = log(&format!("INSERT INTO p VALUES({id},{a})"), &mut stmts);
                let _ = limbo_exec_rows_fallible(&limbo_db, &limbo, &s);
                let _ = sqlite.execute(&s, params![]);
            }

            // try random child inserts with weird text-ints
            for i in 0..INNER_ITERS {
                let id = 1000 + i as i64;
                let raw = if rng.random_bool(0.7) {
                    1 + rng.random_range(0..=15)
                } else {
                    rng.random_range(100..=200) as i64
                };

                // Randomly decorate the integer as text with spacing/zeros/plus
                let pad_left_zeros = rng.random_range(0..=2);
                let spaces_left = rng.random_range(0..=2);
                let spaces_right = rng.random_range(0..=2);
                let plus = if rng.random_bool(0.3) { "+" } else { "" };
                let txt_num = format!(
                    "{plus}{:0width$}",
                    raw,
                    width = (1 + pad_left_zeros) as usize
                );
                let txt = format!(
                    "'{}{}{}'",
                    " ".repeat(spaces_left),
                    txt_num,
                    " ".repeat(spaces_right)
                );

                let stmt = log(&format!("INSERT INTO c VALUES({id}, {txt})"), &mut stmts);
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                assert_parity(seed, &stmts, sres, lres, &stmt, "A: rowid-coercion");
            }
            println!("A {}/{} ok", outer + 1, OUTER_ITERS);
        }

        // slf-referential rowid FK
        for outer in 0..OUTER_ITERS {
            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let mut stmts: Vec<String> = Vec::new();
            let log = |s: &str, stmts: &mut Vec<String>| {
                stmts.push(s.to_string());
                s.to_string()
            };

            for s in [
                "PRAGMA foreign_keys=ON",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, rid REFERENCES t(id))",
            ] {
                let s = log(s, &mut stmts);
                limbo_exec_rows(&limbo, &s);
                sqlite.execute(&s, params![]).unwrap();
            }

            // Self-match should succeed for many ids
            for _ in 0..INNER_ITERS {
                let id = rng.random_range(1..=500);
                let stmt = log(
                    &format!("INSERT INTO t(id,rid) VALUES({id},{id})"),
                    &mut stmts,
                );
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                assert_parity(seed, &stmts, sres, lres, &stmt, "B1: self-row ok");
            }

            // Mismatch (rid != id) should fail (unless the referenced id already exists).
            for _ in 0..rng.random_range(1..=10) {
                let id = rng.random_range(1..=20);
                let s = log(
                    &format!("INSERT INTO t(id,rid) VALUES({id},{id})"),
                    &mut stmts,
                );
                let s_res = sqlite.execute(&s, params![]);
                let turso_rs = limbo_exec_rows_fallible(&limbo_db, &limbo, &s);
                match (s_res.is_ok(), turso_rs.is_ok()) {
                    (true, true) | (false, false) => {}
                    _ => panic!("Seeding self-ref failed differently"),
                }
            }

            for _ in 0..INNER_ITERS {
                let id = rng.random_range(600..=900);
                let ref_ = rng.random_range(1..=25);
                let stmt = log(
                    &format!("INSERT INTO t(id,rid) VALUES({id},{ref_})"),
                    &mut stmts,
                );
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                assert_parity(seed, &stmts, sres, lres, &stmt, "B2: self-row mismatch");
            }
            println!("B {}/{} ok", outer + 1, OUTER_ITERS);
        }

        // self-referential UNIQUE(u,v) parent (fast-path for composite)
        for outer in 0..OUTER_ITERS {
            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let mut stmts: Vec<String> = Vec::new();
            let log = |s: &str, stmts: &mut Vec<String>| {
                stmts.push(s.to_string());
                s.to_string()
            };

            let s = log("PRAGMA foreign_keys=ON", &mut stmts);
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Variant the schema a bit: TEXT/TEXT, NUMERIC/TEXT, etc.
            let decls = [
                ("TEXT", "TEXT"),
                ("TEXT", "NUMERIC"),
                ("NUMERIC", "TEXT"),
                ("TEXT", "BLOB"),
            ];
            let (tu, tv) = decls[rng.random_range(0..decls.len())];

            let s = log(
                &format!(
                    "CREATE TABLE sr(u {tu}, v {tv}, cu {tu}, cv {tv}, UNIQUE(u,v), \
             FOREIGN KEY(cu,cv) REFERENCES sr(u,v))"
                ),
                &mut stmts,
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Self-matching composite rows should succeed
            for _ in 0..INNER_ITERS {
                // Random small tokens, possibly padded
                let u = format!("U{}", rng.random_range(0..50));
                let v = format!("V{}", rng.random_range(0..50));
                let mut cu = u.clone();
                let mut cv = v.clone();

                // occasionally wrap child refs as blobs/text to stress coercion on parent index
                if rng.random_bool(0.2) {
                    // child cv as hex blob of ascii v
                    let hex: String = v.bytes().map(|b| format!("{b:02X}")).collect();
                    cv = format!("x'{hex}'");
                } else {
                    cu = format!("'{cu}'");
                    cv = format!("'{cv}'");
                }

                let stmt = log(
                    &format!("INSERT INTO sr(u,v,cu,cv) VALUES('{u}','{v}',{cu},{cv})"),
                    &mut stmts,
                );
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                assert_parity(seed, &stmts, sres, lres, &stmt, "C1: self-UNIQUE ok");
            }

            // Non-self-match likely fails unless earlier rows happen to satisfy (u,v)
            for _ in 0..INNER_ITERS {
                let u = format!("U{}", rng.random_range(60..100));
                let v = format!("V{}", rng.random_range(60..100));
                let cu = format!("'U{}'", rng.random_range(0..40));
                let cv = format!("'{}{}'", "V", rng.random_range(0..40));
                let stmt = log(
                    &format!("INSERT INTO sr(u,v,cu,cv) VALUES('{u}','{v}',{cu},{cv})"),
                    &mut stmts,
                );
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                assert_parity(seed, &stmts, sres, lres, &stmt, "C2: self-UNIQUE mismatch");
            }
            println!("C {}/{} ok", outer + 1, OUTER_ITERS);
        }

        // parent TEXT UNIQUE(u,v), child types differ; rely on parent-index affinities
        for outer in 0..OUTER_ITERS {
            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let mut stmts: Vec<String> = Vec::new();
            let log = |s: &str, stmts: &mut Vec<String>| {
                stmts.push(s.to_string());
                s.to_string()
            };

            for s in [
                "PRAGMA foreign_keys=ON",
                "CREATE TABLE parent(u TEXT, v TEXT, UNIQUE(u,v))",
                "CREATE TABLE child(id INTEGER PRIMARY KEY, cu INT, cv BLOB, \
                                FOREIGN KEY(cu,cv) REFERENCES parent(u,v))",
            ] {
                let s = log(s, &mut stmts);
                limbo_exec_rows(&limbo, &s);
                sqlite.execute(&s, params![]).unwrap();
            }

            for _ in 0..rng.random_range(3..=8) {
                let u_raw = rng.random_range(0..=9);
                let v_raw = rng.random_range(0..=9);
                let u = if rng.random_bool(0.4) {
                    format!("+{u_raw}")
                } else {
                    format!("{u_raw}")
                };
                let v = if rng.random_bool(0.5) {
                    format!("{v_raw:02}",)
                } else {
                    format!("{v_raw}")
                };
                let s = log(
                    &format!("INSERT INTO parent VALUES('{u}','{v}')"),
                    &mut stmts,
                );
                let l_res = limbo_exec_rows_fallible(&limbo_db, &limbo, &s);
                let s_res = sqlite.execute(&s, params![]);
                match (s_res, l_res) {
                    (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                    (x, y) => {
                        panic!("Parent seeding mismatch: sqlite {x:?}, limbo {y:?}");
                    }
                }
            }

            for i in 0..INNER_ITERS {
                let id = i as i64 + 1;
                let u_txt = if rng.random_bool(0.7) {
                    format!("+{}", rng.random_range(0..=9))
                } else {
                    format!("{}", rng.random_range(0..=9))
                };
                let v_txt = if rng.random_bool(0.5) {
                    format!("{:02}", rng.random_range(0..=9))
                } else {
                    format!("{}", rng.random_range(0..=9))
                };

                // produce child literals that *look different* but should match under TEXT affinity
                // cu uses integer-ish form of u; cv uses blob of ASCII v or quoted v randomly.
                let cu = if let Ok(u_int) = u_txt.trim().trim_start_matches('+').parse::<i64>() {
                    if rng.random_bool(0.5) {
                        format!("{u_int}",)
                    } else {
                        format!("'{u_txt}'")
                    }
                } else {
                    format!("'{u_txt}'")
                };
                let cv = if rng.random_bool(0.6) {
                    let hex: String = v_txt
                        .as_bytes()
                        .iter()
                        .map(|b| format!("{b:02X}"))
                        .collect();
                    format!("x'{hex}'")
                } else {
                    format!("'{v_txt}'")
                };

                let stmt = log(
                    &format!("INSERT INTO child VALUES({id}, {cu}, {cv})"),
                    &mut stmts,
                );
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                assert_parity(seed, &stmts, sres, lres, &stmt, "D1: parent-index affinity");
            }

            for i in 0..(INNER_ITERS / 3) {
                let id = 10_000 + i as i64;
                let cu = rng.random_range(0..=9);
                let miss = rng.random_range(10..=19);
                let stmt = log(
                    &format!("INSERT INTO child VALUES({id}, {cu}, x'{miss:02X}')"),
                    &mut stmts,
                );
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                assert_parity(seed, &stmts, sres, lres, &stmt, "D2: parent-index negative");
            }
            println!("D {}/{} ok", outer + 1, OUTER_ITERS);
        }

        println!("fk_edgecases_minifuzz complete (seed {seed})");
    }

    // Fuzz test for ON DELETE/UPDATE CASCADE, SET NULL, SET DEFAULT actions
    #[turso_macros::test()]
    pub fn fk_cascade_actions_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("fk_cascade_actions_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const OUTER_ITERS: usize = 50;
        const INNER_ITERS: usize = 200;

        for outer in 0..OUTER_ITERS {
            println!("fk_cascade_actions_fuzz {}/{}", outer + 1, OUTER_ITERS);

            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let mut stmts: Vec<String> = Vec::new();
            let mut log_and_exec = |sql: &str| {
                stmts.push(sql.to_string());
                sql.to_string()
            };

            // Enable FKs
            let s = log_and_exec("PRAGMA foreign_keys=ON");
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Randomly pick action types for this iteration
            let on_delete_actions = ["CASCADE", "SET NULL", "NO ACTION", "RESTRICT"];
            let on_update_actions = ["CASCADE", "SET NULL", "NO ACTION", "RESTRICT"];

            let del_action = on_delete_actions[rng.random_range(0..on_delete_actions.len())];
            let upd_action = on_update_actions[rng.random_range(0..on_update_actions.len())];

            // Parent table with INTEGER PRIMARY KEY
            let s = log_and_exec("CREATE TABLE parent(id INTEGER PRIMARY KEY, a INT, b INT)");
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Child with CASCADE/SET NULL FK
            let s = log_and_exec(&format!(
                "CREATE TABLE child_cascade(id INTEGER PRIMARY KEY, pid INT, x INT, \
                 FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE {del_action} ON UPDATE {upd_action})"
            ));
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Second child with different action combo
            let del_action2 = on_delete_actions[rng.random_range(0..on_delete_actions.len())];
            let upd_action2 = on_update_actions[rng.random_range(0..on_update_actions.len())];
            let s = log_and_exec(&format!(
                "CREATE TABLE child_mixed(id INTEGER PRIMARY KEY, pid INT, y INT, \
                 FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE {del_action2} ON UPDATE {upd_action2})"
            ));
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Composite key parent for testing composite CASCADE
            let s = log_and_exec(
                "CREATE TABLE parent_comp(a INT NOT NULL, b INT NOT NULL, c INT, PRIMARY KEY(a,b))",
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Child with composite FK and CASCADE
            let s = log_and_exec(
                "CREATE TABLE child_comp(id INTEGER PRIMARY KEY, ca INT, cb INT, z INT, \
                 FOREIGN KEY(ca,cb) REFERENCES parent_comp(a,b) ON DELETE CASCADE ON UPDATE CASCADE)"
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Seed initial parent data
            let mut parent_ids = std::collections::HashSet::new();
            for _ in 0..rng.random_range(10..=30) {
                let id = rng.random_range(1..=100) as i64;
                if parent_ids.insert(id) {
                    let a = rng.random_range(-5..=25);
                    let b = rng.random_range(-5..=25);
                    let stmt = log_and_exec(&format!("INSERT INTO parent VALUES ({id}, {a}, {b})"));
                    limbo_exec_rows(&limbo, &stmt);
                    sqlite.execute(&stmt, params![]).unwrap();
                }
            }

            // Seed composite parent data
            let mut comp_pairs = std::collections::HashSet::new();
            for _ in 0..rng.random_range(5..=15) {
                let a = rng.random_range(-3..=10) as i64;
                let b = rng.random_range(-3..=10) as i64;
                if comp_pairs.insert((a, b)) {
                    let c = rng.random_range(0..=20);
                    let stmt =
                        log_and_exec(&format!("INSERT INTO parent_comp VALUES ({a}, {b}, {c})"));
                    limbo_exec_rows(&limbo, &stmt);
                    sqlite.execute(&stmt, params![]).unwrap();
                }
            }

            // Seed child data
            for _ in 0..rng.random_range(15..=40) {
                let id = rng.random_range(1000..=2000);
                let pid = if let Some(p) = parent_ids.iter().choose(&mut rng) {
                    *p
                } else {
                    continue;
                };
                let x = rng.random_range(-10..=10);
                let stmt = log_and_exec(&format!(
                    "INSERT OR IGNORE INTO child_cascade VALUES ({id}, {pid}, {x})"
                ));
                let _ = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                let _ = sqlite.execute(&stmt, params![]);
            }

            for _ in 0..rng.random_range(10..=30) {
                let id = rng.random_range(3000..=4000);
                let pid = if let Some(p) = parent_ids.iter().choose(&mut rng) {
                    *p
                } else {
                    continue;
                };
                let y = rng.random_range(-10..=10);
                let stmt = log_and_exec(&format!(
                    "INSERT OR IGNORE INTO child_mixed VALUES ({id}, {pid}, {y})"
                ));
                let _ = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                let _ = sqlite.execute(&stmt, params![]);
            }

            // Seed composite child data
            for _ in 0..rng.random_range(8..=20) {
                let id = rng.random_range(5000..=6000);
                if let Some((a, b)) = comp_pairs.iter().choose(&mut rng) {
                    let z = rng.random_range(0..=10);
                    let stmt = log_and_exec(&format!(
                        "INSERT OR IGNORE INTO child_comp VALUES ({id}, {a}, {b}, {z})"
                    ));
                    let _ = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                    let _ = sqlite.execute(&stmt, params![]);
                }
            }

            // Now fuzz mutations that trigger CASCADE/SET NULL behavior
            for _ in 0..INNER_ITERS {
                let op = rng.random_range(0..14);
                let stmt = match op {
                    // DELETE parent (triggers ON DELETE action)
                    0 | 1 => {
                        let id = if rng.random_bool(0.7) {
                            if let Some(p) = parent_ids.iter().choose(&mut rng) {
                                *p
                            } else {
                                rng.random_range(1..=100) as i64
                            }
                        } else {
                            rng.random_range(1..=150) as i64
                        };
                        format!("DELETE FROM parent WHERE id={id}")
                    }
                    // UPDATE parent PK (triggers ON UPDATE action)
                    2 | 3 => {
                        let old_id = if rng.random_bool(0.7) {
                            if let Some(p) = parent_ids.iter().choose(&mut rng) {
                                *p
                            } else {
                                rng.random_range(1..=100) as i64
                            }
                        } else {
                            rng.random_range(1..=150) as i64
                        };
                        let new_id = rng.random_range(1..=200);
                        parent_ids.remove(&old_id);
                        parent_ids.insert(new_id as i64);
                        format!("UPDATE parent SET id={new_id} WHERE id={old_id}")
                    }
                    // DELETE composite parent (triggers composite CASCADE)
                    4 => {
                        if let Some((a, b)) = comp_pairs.iter().choose(&mut rng).cloned() {
                            format!("DELETE FROM parent_comp WHERE a={a} AND b={b}")
                        } else {
                            let a = rng.random_range(-3..=10);
                            let b = rng.random_range(-3..=10);
                            format!("DELETE FROM parent_comp WHERE a={a} AND b={b}")
                        }
                    }
                    // UPDATE composite parent key (triggers composite CASCADE UPDATE)
                    5 => {
                        if let Some((old_a, old_b)) = comp_pairs.iter().choose(&mut rng).cloned() {
                            let new_a = rng.random_range(-5..=15);
                            let new_b = rng.random_range(-5..=15);
                            comp_pairs.remove(&(old_a, old_b));
                            comp_pairs.insert((new_a as i64, new_b as i64));
                            format!(
                                "UPDATE parent_comp SET a={new_a}, b={new_b} WHERE a={old_a} AND b={old_b}"
                            )
                        } else {
                            continue;
                        }
                    }
                    // INSERT new parent
                    6 => {
                        let id = rng.random_range(1..=150);
                        let a = rng.random_range(-5..=25);
                        let b = rng.random_range(-5..=25);
                        parent_ids.insert(id as i64);
                        format!("INSERT OR IGNORE INTO parent VALUES({id}, {a}, {b})")
                    }
                    // INSERT child_cascade
                    7 => {
                        let id = rng.random_range(1000..=2500);
                        let pid = if rng.random_bool(0.8) {
                            if let Some(p) = parent_ids.iter().choose(&mut rng) {
                                *p
                            } else {
                                rng.random_range(1..=150) as i64
                            }
                        } else {
                            rng.random_range(1..=150) as i64
                        };
                        let x = rng.random_range(-10..=10);
                        format!("INSERT OR IGNORE INTO child_cascade VALUES({id}, {pid}, {x})")
                    }
                    // INSERT composite child
                    8 => {
                        let id = rng.random_range(5000..=6500);
                        if let Some((a, b)) = comp_pairs.iter().choose(&mut rng) {
                            let z = rng.random_range(0..=10);
                            format!("INSERT OR IGNORE INTO child_comp VALUES({id}, {a}, {b}, {z})")
                        } else {
                            continue;
                        }
                    }
                    // DELETE child directly
                    9 => {
                        let id = rng.random_range(1000..=2500);
                        format!("DELETE FROM child_cascade WHERE id={id}")
                    }
                    // INSERT OR REPLACE on parent (triggers ON DELETE action)
                    10 => {
                        let id = if let Some(p) = parent_ids.iter().choose(&mut rng) {
                            *p
                        } else {
                            rng.random_range(1..=100) as i64
                        };
                        let a = rng.random_range(-5..=25);
                        let b = rng.random_range(-5..=25);
                        parent_ids.insert(id);
                        format!("INSERT OR REPLACE INTO parent VALUES({id}, {a}, {b})")
                    }
                    // INSERT OR REPLACE on composite parent (triggers ON DELETE action)
                    11 => {
                        if let Some((a, b)) = comp_pairs.iter().choose(&mut rng).cloned() {
                            let c = rng.random_range(0..=20);
                            format!("INSERT OR REPLACE INTO parent_comp VALUES({a}, {b}, {c})")
                        } else {
                            let a = rng.random_range(-3..=10);
                            let b = rng.random_range(-3..=10);
                            let c = rng.random_range(0..=20);
                            comp_pairs.insert((a as i64, b as i64));
                            format!("INSERT OR REPLACE INTO parent_comp VALUES({a}, {b}, {c})")
                        }
                    }
                    // UPSERT on parent that updates columns (triggers ON UPDATE action)
                    12 => {
                        let id = if rng.random_bool(0.8) {
                            if let Some(p) = parent_ids.iter().choose(&mut rng) {
                                *p
                            } else {
                                rng.random_range(1..=100) as i64
                            }
                        } else {
                            rng.random_range(1..=150) as i64
                        };
                        let new_a = rng.random_range(-5..=25);
                        let new_b = rng.random_range(-5..=25);
                        parent_ids.insert(id);
                        format!("INSERT INTO parent VALUES({id}, {new_a}, {new_b}) ON CONFLICT(id) DO UPDATE SET a={new_a}, b={new_b}")
                    }
                    // UPSERT on composite parent that updates columns (triggers ON UPDATE action)
                    _ => {
                        if let Some((a, b)) = comp_pairs.iter().choose(&mut rng).cloned() {
                            let new_c = rng.random_range(0..=20);
                            format!("INSERT INTO parent_comp VALUES({a}, {b}, {new_c}) ON CONFLICT(a,b) DO UPDATE SET c={new_c}")
                        } else {
                            let a = rng.random_range(-3..=10);
                            let b = rng.random_range(-3..=10);
                            let c = rng.random_range(0..=20);
                            comp_pairs.insert((a as i64, b as i64));
                            format!("INSERT INTO parent_comp VALUES({a}, {b}, {c}) ON CONFLICT(a,b) DO UPDATE SET c={c}")
                        }
                    }
                };

                let stmt = log_and_exec(&stmt);
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);

                match (sres, lres) {
                    (Ok(_), Ok(_)) => {
                        // Verify state parity after successful operations
                        let sp = sqlite_exec_rows(&sqlite, "SELECT id,a,b FROM parent ORDER BY id");
                        let lp = limbo_exec_rows(&limbo, "SELECT id,a,b FROM parent ORDER BY id");
                        let sc_cascade = sqlite_exec_rows(
                            &sqlite,
                            "SELECT id,pid,x FROM child_cascade ORDER BY id",
                        );
                        let lc_cascade = limbo_exec_rows(
                            &limbo,
                            "SELECT id,pid,x FROM child_cascade ORDER BY id",
                        );
                        let sc_mixed = sqlite_exec_rows(
                            &sqlite,
                            "SELECT id,pid,y FROM child_mixed ORDER BY id",
                        );
                        let lc_mixed =
                            limbo_exec_rows(&limbo, "SELECT id,pid,y FROM child_mixed ORDER BY id");
                        let sp_comp =
                            sqlite_exec_rows(&sqlite, "SELECT a,b,c FROM parent_comp ORDER BY a,b");
                        let lp_comp =
                            limbo_exec_rows(&limbo, "SELECT a,b,c FROM parent_comp ORDER BY a,b");
                        let sc_comp = sqlite_exec_rows(
                            &sqlite,
                            "SELECT id,ca,cb,z FROM child_comp ORDER BY id",
                        );
                        let lc_comp = limbo_exec_rows(
                            &limbo,
                            "SELECT id,ca,cb,z FROM child_comp ORDER BY id",
                        );

                        if sp != lp
                            || sc_cascade != lc_cascade
                            || sc_mixed != lc_mixed
                            || sp_comp != lp_comp
                            || sc_comp != lc_comp
                        {
                            eprintln!("\n=== CASCADE fuzz failure (state mismatch) ===");
                            eprintln!("seed: {seed}, outer: {}", outer + 1);
                            eprintln!("del_action: {del_action}, upd_action: {upd_action}");
                            eprintln!("del_action2: {del_action2}, upd_action2: {upd_action2}");
                            eprintln!("last stmt: {stmt}");
                            eprintln!("sqlite parent: {sp:?}");
                            eprintln!("limbo  parent: {lp:?}");
                            eprintln!("sqlite child_cascade: {sc_cascade:?}");
                            eprintln!("limbo  child_cascade: {lc_cascade:?}");
                            eprintln!("sqlite child_mixed: {sc_mixed:?}");
                            eprintln!("limbo  child_mixed: {lc_mixed:?}");
                            eprintln!("sqlite parent_comp: {sp_comp:?}");
                            eprintln!("limbo  parent_comp: {lp_comp:?}");
                            eprintln!("sqlite child_comp: {sc_comp:?}");
                            eprintln!("limbo  child_comp: {lc_comp:?}");
                            eprintln!("--- replay statements ({}) ---", stmts.len());
                            let mut file = std::fs::File::create("fk_cascade_fuzz.sql").unwrap();
                            for s in stmts.iter() {
                                let _ = file.write_fmt(format_args!("{s};\n"));
                            }
                            file.flush().unwrap();
                            panic!(
                                "CASCADE state mismatch, statements written to fk_cascade_fuzz.sql"
                            );
                        }
                    }
                    (Err(_), Err(_)) => { /* both failed - parity OK */ }
                    (ok_sqlite, ok_limbo) => {
                        eprintln!("\n=== CASCADE fuzz failure (outcome mismatch) ===");
                        eprintln!("seed: {seed}, outer: {}", outer + 1);
                        eprintln!("del_action: {del_action}, upd_action: {upd_action}");
                        eprintln!("sqlite: {ok_sqlite:?}, limbo: {ok_limbo:?}");
                        eprintln!("last stmt: {stmt}");
                        let mut file = std::fs::File::create("fk_cascade_fuzz.sql").unwrap();
                        for s in stmts.iter() {
                            let _ = file.write_fmt(format_args!("{s};\n"));
                        }
                        file.flush().unwrap();
                        panic!(
                            "CASCADE outcome mismatch, statements written to fk_cascade_fuzz.sql"
                        );
                    }
                }
            }
        }
        println!("fk_cascade_actions_fuzz complete (seed {seed})");
    }

    // Fuzz test for recursive CASCADE (A->B->C chains)
    #[turso_macros::test()]
    pub fn fk_recursive_cascade_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("fk_recursive_cascade_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const OUTER_ITERS: usize = 25;
        const INNER_ITERS: usize = 200;

        for outer in 0..OUTER_ITERS {
            println!("fk_recursive_cascade_fuzz {}/{}", outer + 1, OUTER_ITERS);

            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let mut stmts: Vec<String> = Vec::new();
            let mut log_and_exec = |sql: &str| {
                stmts.push(sql.to_string());
                sql.to_string()
            };

            // Enable FKs
            let s = log_and_exec("PRAGMA foreign_keys=ON");
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Create a 3-level hierarchy: grandparent -> parent -> child
            // All with CASCADE to test recursive deletion/update
            let s = log_and_exec("CREATE TABLE gp(id INTEGER PRIMARY KEY, v INT)");
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            let s = log_and_exec(
                "CREATE TABLE p(id INTEGER PRIMARY KEY, gp_id INT, v INT, \
                 FOREIGN KEY(gp_id) REFERENCES gp(id) ON DELETE CASCADE ON UPDATE CASCADE)",
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            let s = log_and_exec(
                "CREATE TABLE c(id INTEGER PRIMARY KEY, p_id INT, v INT, \
                 FOREIGN KEY(p_id) REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Seed grandparents
            let mut gp_ids = std::collections::HashSet::new();
            for _ in 0..rng.random_range(5..=15) {
                let id = rng.random_range(1..=50) as i64;
                if gp_ids.insert(id) {
                    let v = rng.random_range(0..=100);
                    let stmt = log_and_exec(&format!("INSERT INTO gp VALUES ({id}, {v})"));
                    limbo_exec_rows(&limbo, &stmt);
                    sqlite.execute(&stmt, params![]).unwrap();
                }
            }

            // Seed parents
            let mut p_ids = std::collections::HashSet::new();
            for _ in 0..rng.random_range(10..=30) {
                let id = rng.random_range(100..=200) as i64;
                if let Some(gp_id) = gp_ids.iter().choose(&mut rng) {
                    if p_ids.insert(id) {
                        let v = rng.random_range(0..=100);
                        let stmt =
                            log_and_exec(&format!("INSERT INTO p VALUES ({id}, {gp_id}, {v})"));
                        limbo_exec_rows(&limbo, &stmt);
                        sqlite.execute(&stmt, params![]).unwrap();
                    }
                }
            }

            // Seed children
            for _ in 0..rng.random_range(20..=50) {
                let id = rng.random_range(1000..=2000) as i64;
                if let Some(p_id) = p_ids.iter().choose(&mut rng) {
                    let v = rng.random_range(0..=100);
                    let stmt = log_and_exec(&format!(
                        "INSERT OR IGNORE INTO c VALUES ({id}, {p_id}, {v})"
                    ));
                    let _ = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
                    let _ = sqlite.execute(&stmt, params![]);
                }
            }

            // Fuzz mutations on the hierarchy
            for _ in 0..INNER_ITERS {
                let op = rng.random_range(0..12);
                let stmt = match op {
                    // DELETE grandparent (should cascade to parent and child)
                    0 | 1 => {
                        if let Some(id) = gp_ids.iter().choose(&mut rng).cloned() {
                            format!("DELETE FROM gp WHERE id={id}")
                        } else {
                            continue;
                        }
                    }
                    // UPDATE grandparent PK (should cascade update to parent's gp_id)
                    2 => {
                        if let Some(old_id) = gp_ids.iter().choose(&mut rng).cloned() {
                            let new_id = rng.random_range(1..=100);
                            gp_ids.remove(&old_id);
                            gp_ids.insert(new_id as i64);
                            format!("UPDATE gp SET id={new_id} WHERE id={old_id}")
                        } else {
                            continue;
                        }
                    }
                    // DELETE parent (should cascade to child only)
                    3 => {
                        if let Some(id) = p_ids.iter().choose(&mut rng).cloned() {
                            format!("DELETE FROM p WHERE id={id}")
                        } else {
                            continue;
                        }
                    }
                    // UPDATE parent PK (should cascade to child's p_id)
                    4 => {
                        if let Some(old_id) = p_ids.iter().choose(&mut rng).cloned() {
                            let new_id = rng.random_range(100..=300);
                            p_ids.remove(&old_id);
                            p_ids.insert(new_id as i64);
                            format!("UPDATE p SET id={new_id} WHERE id={old_id}")
                        } else {
                            continue;
                        }
                    }
                    // INSERT new grandparent
                    5 => {
                        let id = rng.random_range(1..=100);
                        let v = rng.random_range(0..=100);
                        gp_ids.insert(id as i64);
                        format!("INSERT OR IGNORE INTO gp VALUES({id}, {v})")
                    }
                    // INSERT new parent
                    6 => {
                        let id = rng.random_range(100..=300);
                        if let Some(gp_id) = gp_ids.iter().choose(&mut rng) {
                            let v = rng.random_range(0..=100);
                            p_ids.insert(id as i64);
                            format!("INSERT OR IGNORE INTO p VALUES({id}, {gp_id}, {v})")
                        } else {
                            continue;
                        }
                    }
                    // INSERT new child
                    7 => {
                        let id = rng.random_range(1000..=3000);
                        if let Some(p_id) = p_ids.iter().choose(&mut rng) {
                            let v = rng.random_range(0..=100);
                            format!("INSERT OR IGNORE INTO c VALUES({id}, {p_id}, {v})")
                        } else {
                            continue;
                        }
                    }
                    // INSERT OR REPLACE on grandparent (triggers recursive cascade)
                    8 => {
                        if let Some(id) = gp_ids.iter().choose(&mut rng).cloned() {
                            let v = rng.random_range(0..=100);
                            format!("INSERT OR REPLACE INTO gp VALUES({id}, {v})")
                        } else {
                            let id = rng.random_range(1..=50);
                            let v = rng.random_range(0..=100);
                            gp_ids.insert(id as i64);
                            format!("INSERT OR REPLACE INTO gp VALUES({id}, {v})")
                        }
                    }
                    // INSERT OR REPLACE on parent (triggers cascade to children)
                    9 => {
                        if let Some(id) = p_ids.iter().choose(&mut rng).cloned() {
                            if let Some(gp_id) = gp_ids.iter().choose(&mut rng) {
                                let v = rng.random_range(0..=100);
                                format!("INSERT OR REPLACE INTO p VALUES({id}, {gp_id}, {v})")
                            } else {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }
                    // UPSERT on grandparent that updates value (doesn't change FK key, so no cascade)
                    10 => {
                        if let Some(id) = gp_ids.iter().choose(&mut rng).cloned() {
                            let new_v = rng.random_range(0..=100);
                            format!("INSERT INTO gp VALUES({id}, {new_v}) ON CONFLICT(id) DO UPDATE SET v={new_v}")
                        } else {
                            let id = rng.random_range(1..=50);
                            let v = rng.random_range(0..=100);
                            gp_ids.insert(id as i64);
                            format!("INSERT INTO gp VALUES({id}, {v}) ON CONFLICT(id) DO UPDATE SET v={v}")
                        }
                    }
                    // UPSERT on parent that updates value (doesn't change FK key)
                    _ => {
                        if let Some(id) = p_ids.iter().choose(&mut rng).cloned() {
                            if let Some(gp_id) = gp_ids.iter().choose(&mut rng) {
                                let new_v = rng.random_range(0..=100);
                                format!("INSERT INTO p VALUES({id}, {gp_id}, {new_v}) ON CONFLICT(id) DO UPDATE SET v={new_v}")
                            } else {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }
                };

                let stmt = log_and_exec(&stmt);
                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);

                match (sres, lres) {
                    (Ok(_), Ok(_)) => {
                        // Verify state parity
                        let s_gp = sqlite_exec_rows(&sqlite, "SELECT id,v FROM gp ORDER BY id");
                        let l_gp = limbo_exec_rows(&limbo, "SELECT id,v FROM gp ORDER BY id");
                        let s_p = sqlite_exec_rows(&sqlite, "SELECT id,gp_id,v FROM p ORDER BY id");
                        let l_p = limbo_exec_rows(&limbo, "SELECT id,gp_id,v FROM p ORDER BY id");
                        let s_c = sqlite_exec_rows(&sqlite, "SELECT id,p_id,v FROM c ORDER BY id");
                        let l_c = limbo_exec_rows(&limbo, "SELECT id,p_id,v FROM c ORDER BY id");

                        if s_gp != l_gp || s_p != l_p || s_c != l_c {
                            eprintln!("\n=== Recursive CASCADE fuzz failure ===");
                            eprintln!("seed: {seed}, outer: {}", outer + 1);
                            eprintln!("last stmt: {stmt}");
                            eprintln!("sqlite gp: {s_gp:?}");
                            eprintln!("limbo  gp: {l_gp:?}");
                            eprintln!("sqlite p: {s_p:?}");
                            eprintln!("limbo  p: {l_p:?}");
                            eprintln!("sqlite c: {s_c:?}");
                            eprintln!("limbo  c: {l_c:?}");
                            let mut file =
                                std::fs::File::create("fk_recursive_cascade_fuzz.sql").unwrap();
                            for s in stmts.iter() {
                                let _ = file.write_fmt(format_args!("{s};\n"));
                            }
                            file.flush().unwrap();
                            panic!("Recursive CASCADE mismatch");
                        }
                    }
                    (Err(_), Err(_)) => {}
                    (ok_sqlite, ok_limbo) => {
                        eprintln!("\n=== Recursive CASCADE outcome mismatch ===");
                        eprintln!("seed: {seed}");
                        eprintln!("sqlite: {ok_sqlite:?}, limbo: {ok_limbo:?}");
                        eprintln!("stmt: {stmt}");
                        let mut file =
                            std::fs::File::create("fk_recursive_cascade_fuzz.sql").unwrap();
                        for s in stmts.iter() {
                            let _ = file.write_fmt(format_args!("{s};\n"));
                        }
                        file.flush().unwrap();
                        panic!("Recursive CASCADE outcome mismatch");
                    }
                }
            }
        }
        println!("fk_recursive_cascade_fuzz complete (seed {seed})");
    }

    // TODO: mvcc indexes
    #[turso_macros::test()]
    pub fn fk_composite_pk_mutation_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("fk_composite_pk_mutation_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const OUTER_ITERS: usize = 10;
        const INNER_ITERS: usize = 100;

        for outer in 0..OUTER_ITERS {
            println!(
                "fk_composite_pk_mutation_fuzz {}/{}",
                outer + 1,
                OUTER_ITERS
            );

            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            let mut stmts: Vec<String> = Vec::new();
            let mut log_and_exec = |sql: &str| {
                stmts.push(sql.to_string());
                sql.to_string()
            };

            // Enable FKs in both engines
            let _ = log_and_exec("PRAGMA foreign_keys=ON");
            limbo_exec_rows(&limbo, "PRAGMA foreign_keys=ON");
            sqlite.execute("PRAGMA foreign_keys=ON", params![]).unwrap();

            // Parent PK is composite (a,b). Child references (x,y) -> (a,b).
            let s = log_and_exec(
                "CREATE TABLE p(a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY(a,b))",
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            let s = log_and_exec(
                "CREATE TABLE c(id INTEGER PRIMARY KEY, x INT, y INT, w INT, \
             FOREIGN KEY(x,y) REFERENCES p(a,b))",
            );
            limbo_exec_rows(&limbo, &s);
            sqlite.execute(&s, params![]).unwrap();

            // Seed parent: small grid of (a,b)
            let mut pairs: Vec<(i64, i64)> = Vec::new();
            for _ in 0..rng.random_range(5..=25) {
                let a = rng.random_range(-3..=6);
                let b = rng.random_range(-3..=6);
                if !pairs.contains(&(a, b)) {
                    pairs.push((a, b));
                    let v = rng.random_range(0..=20);
                    let stmt = log_and_exec(&format!("INSERT INTO p VALUES({a},{b},{v})"));
                    limbo_exec_rows(&limbo, &stmt);
                    sqlite.execute(&stmt, params![]).unwrap();
                }
            }

            // Seed child rows, 70% chance to reference existing (a,b)
            for i in 0..rng.random_range(5..=60) {
                let id = 5000 + i as i64;
                let (x, y) = if rng.random_bool(0.7) {
                    *pairs.choose(&mut rng).unwrap_or(&(0, 0))
                } else {
                    (rng.random_range(-4..=7), rng.random_range(-4..=7))
                };
                let w = rng.random_range(-10..=10);
                let stmt = log_and_exec(&format!("INSERT INTO c VALUES({id}, {x}, {y}, {w})"));
                let _ = sqlite.execute(&stmt, params![]);
                let _ = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);
            }

            for _ in 0..INNER_ITERS {
                let op = rng.random_range(0..7);
                let stmt = log_and_exec(&match op {
                    // INSERT parent
                    0 => {
                        let a = rng.random_range(-4..=8);
                        let b = rng.random_range(-4..=8);
                        let v = rng.random_range(0..=20);
                        format!("INSERT INTO p VALUES({a},{b},{v})")
                    }
                    // UPDATE parent composite key (a,b)
                    1 => {
                        let a_old = rng.random_range(-4..=8);
                        let b_old = rng.random_range(-4..=8);
                        let a_new = rng.random_range(-4..=8);
                        let b_new = rng.random_range(-4..=8);
                        format!("UPDATE p SET a={a_new}, b={b_new} WHERE a={a_old} AND b={b_old}")
                    }
                    // DELETE parent
                    2 => {
                        let a = rng.random_range(-4..=8);
                        let b = rng.random_range(-4..=8);
                        format!("DELETE FROM p WHERE a={a} AND b={b}")
                    }
                    // INSERT child
                    3 => {
                        let id = rng.random_range(5000..=7000);
                        let (x, y) = if rng.random_bool(0.7) {
                            *pairs.choose(&mut rng).unwrap_or(&(0, 0))
                        } else {
                            (rng.random_range(-4..=8), rng.random_range(-4..=8))
                        };
                        let w = rng.random_range(-10..=10);
                        format!("INSERT INTO c VALUES({id},{x},{y},{w})")
                    }
                    // UPDATE child FK columns (x,y)
                    4 => {
                        let id = rng.random_range(5000..=7000);
                        let (x, y) = if rng.random_bool(0.7) {
                            *pairs.choose(&mut rng).unwrap_or(&(0, 0))
                        } else {
                            (rng.random_range(-4..=8), rng.random_range(-4..=8))
                        };
                        format!("UPDATE c SET x={x}, y={y} WHERE id={id}")
                    }
                    5 => {
                        // UPSERT parent
                        if rng.random_bool(0.5) {
                            let a = rng.random_range(-4..=8);
                            let b = rng.random_range(-4..=8);
                            let v = rng.random_range(0..=20);
                            format!(
                                "INSERT INTO p VALUES({a},{b},{v}) ON CONFLICT(a,b) DO UPDATE SET v=excluded.v"
                            )
                        } else {
                            let a = rng.random_range(-4..=8);
                            let b = rng.random_range(-4..=8);
                            format!(
                                "INSERT INTO p VALUES({a},{b},{}) ON CONFLICT(a,b) DO NOTHING",
                                rng.random_range(0..=20)
                            )
                        }
                    }
                    6 => {
                        // UPSERT child
                        let id = rng.random_range(5000..=7000);
                        let (x, y) = if rng.random_bool(0.7) {
                            *pairs.choose(&mut rng).unwrap_or(&(0, 0))
                        } else {
                            (rng.random_range(-4..=8), rng.random_range(-4..=8))
                        };
                        format!(
                            "INSERT INTO c VALUES({id},{x},{y},{}) ON CONFLICT(id) DO UPDATE SET x=excluded.x, y=excluded.y",
                            rng.random_range(-10..=10)
                        )
                    }
                    // DELETE child
                    _ => {
                        let id = rng.random_range(5000..=7000);
                        format!("DELETE FROM c WHERE id={id}")
                    }
                });

                let sres = sqlite.execute(&stmt, params![]);
                let lres = limbo_exec_rows_fallible(&limbo_db, &limbo, &stmt);

                match (sres, lres) {
                    (Ok(_), Ok(_)) => {
                        // Compare canonical states
                        let sp = sqlite_exec_rows(&sqlite, "SELECT a,b,v FROM p ORDER BY a,b,v");
                        let sc = sqlite_exec_rows(&sqlite, "SELECT id,x,y,w FROM c ORDER BY id");
                        let lp = limbo_exec_rows(&limbo, "SELECT a,b,v FROM p ORDER BY a,b,v");
                        let lc = limbo_exec_rows(&limbo, "SELECT id,x,y,w FROM c ORDER BY id");
                        assert_eq!(sp, lp, "seed {seed}, stmt {stmt}");
                        assert_eq!(sc, lc, "seed {seed}, stmt {stmt}");
                    }
                    (Err(_), Err(_)) => { /* both errored -> parity OK */ }
                    (ok_s, ok_l) => {
                        eprintln!(
                            "Mismatch sqlite={ok_s:?}, limbo={ok_l:?}, stmt={stmt}, seed={seed}"
                        );
                        let sp = sqlite_exec_rows(&sqlite, "SELECT a,b,v FROM p ORDER BY a,b,v");
                        let sc = sqlite_exec_rows(&sqlite, "SELECT id,x,y,w FROM c ORDER BY id");
                        let lp = limbo_exec_rows(&limbo, "SELECT a,b,v FROM p ORDER BY a,b,v");
                        let lc = limbo_exec_rows(&limbo, "SELECT id,x,y,w FROM c ORDER BY id");
                        eprintln!(
                            "sqlite p={sp:?}\nsqlite c={sc:?}\nlimbo p={lp:?}\nlimbo c={lc:?}"
                        );
                        let mut file =
                            std::fs::File::create("fk_composite_fuzz_statements.sql").unwrap();
                        for s in stmts.iter() {
                            let _ = writeln!(&file, "{s};");
                        }
                        file.flush().unwrap();
                        panic!("DML outcome mismatch, sql file written to tests/fk_composite_fuzz_statements.sql");
                    }
                }
            }
        }
    }
    #[turso_macros::test(mvcc)]
    /// Create a table with a random number of columns and indexes, and then randomly update or delete rows from the table.
    /// Verify that the results are the same for SQLite and Turso.
    pub fn table_index_mutation_fuzz(db: TempDatabase) {
        let is_mvcc = db.enable_mvcc;
        /// Format a nice diff between two result sets for better error messages
        #[allow(clippy::too_many_arguments)]
        fn format_rows_diff(
            sqlite_rows: &[Vec<Value>],
            limbo_rows: &[Vec<Value>],
            seed: u64,
            query: &str,
            table_def: &str,
            indexes: &[String],
            trigger: Option<&String>,
            dml_statements: &[String],
        ) -> String {
            let mut diff = String::new();
            let sqlite_rows_len = sqlite_rows.len();
            let limbo_rows_len = limbo_rows.len();
            diff.push_str(&format!(
            "\n\n=== Row Count Difference ===\nSQLite: {sqlite_rows_len} rows, Limbo: {limbo_rows_len} rows\n",
        ));

            // Find rows that differ at the same index
            let max_len = sqlite_rows.len().max(limbo_rows.len());
            let mut diff_indices = Vec::new();
            for i in 0..max_len {
                let sqlite_row = sqlite_rows.get(i);
                let limbo_row = limbo_rows.get(i);
                if sqlite_row != limbo_row {
                    diff_indices.push(i);
                }
            }

            if !diff_indices.is_empty() {
                diff.push_str("\n=== Rows Differing at Same Index (showing first 10) ===\n");
                for &idx in diff_indices.iter().take(10) {
                    diff.push_str(&format!("\nIndex {idx}:\n"));
                    if let Some(sqlite_row) = sqlite_rows.get(idx) {
                        diff.push_str(&format!("  SQLite: {sqlite_row:?}\n"));
                    } else {
                        diff.push_str("  SQLite: <missing>\n");
                    }
                    if let Some(limbo_row) = limbo_rows.get(idx) {
                        diff.push_str(&format!("  Limbo:  {limbo_row:?}\n"));
                    } else {
                        diff.push_str("  Limbo:  <missing>\n");
                    }
                }
                if diff_indices.len() > 10 {
                    diff.push_str(&format!(
                        "\n... and {} more differences\n",
                        diff_indices.len() - 10
                    ));
                }
            }

            // Find rows that are in one but not the other (using linear search since Value doesn't implement Hash)
            let mut only_in_sqlite = Vec::new();
            for sqlite_row in sqlite_rows.iter() {
                if !limbo_rows.iter().any(|limbo_row| limbo_row == sqlite_row) {
                    only_in_sqlite.push(sqlite_row);
                }
            }

            let mut only_in_limbo = Vec::new();
            for limbo_row in limbo_rows.iter() {
                if !sqlite_rows.iter().any(|sqlite_row| sqlite_row == limbo_row) {
                    only_in_limbo.push(limbo_row);
                }
            }

            if !only_in_sqlite.is_empty() {
                diff.push_str("\n=== Rows Only in SQLite (showing first 10) ===\n");
                for row in only_in_sqlite.iter().take(10) {
                    diff.push_str(&format!("  {row:?}\n"));
                }
                if only_in_sqlite.len() > 10 {
                    diff.push_str(&format!(
                        "\n... and {} more rows\n",
                        only_in_sqlite.len() - 10
                    ));
                }
            }

            if !only_in_limbo.is_empty() {
                diff.push_str("\n=== Rows Only in Limbo (showing first 10) ===\n");
                for row in only_in_limbo.iter().take(10) {
                    diff.push_str(&format!("  {row:?}\n"));
                }
                if only_in_limbo.len() > 10 {
                    diff.push_str(&format!(
                        "\n... and {} more rows\n",
                        only_in_limbo.len() - 10
                    ));
                }
            }

            diff.push_str(&format!(
                "\n=== Context ===\nSeed: {seed}\nQuery: {query}\n",
            ));

            diff.push_str("\n=== DDL/DML to Reproduce ===\n");
            diff.push_str(&format!("{table_def};\n"));
            for idx in indexes.iter() {
                diff.push_str(&format!("{idx};\n"));
            }
            if let Some(trigger) = trigger {
                diff.push_str(&format!("{trigger};\n"));
            }
            for dml in dml_statements.iter() {
                diff.push_str(&format!("{dml};\n"));
            }

            diff
        }

        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("table_index_mutation_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const OUTER_ITERATIONS: usize = 30;
        for i in 0..OUTER_ITERATIONS {
            println!(
                "table_index_mutation_fuzz iteration {}/{}",
                i + 1,
                OUTER_ITERATIONS
            );
            let limbo_db = builder.clone().build();
            // For the sqlite comparison database, use a separate builder without MVCC init_sql
            let sqlite_db = TempDatabase::builder()
                .with_flags(flags)
                .with_opts(opts)
                .build();
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
            let mut indexes = Vec::new();
            for i in 0..num_indexes {
                // Decide if this should be a single-column or multi-column index
                let is_multi_column = rng.random_bool(0.5) && num_cols > 1;
                // Expression indexes are not supported in MVCC (at least yet)
                let is_expr = !is_mvcc && rng.random_bool(0.3);

                if is_multi_column {
                    // Create a multi-column index with 2-3 columns
                    let num_index_cols = rng.random_range(2..=3.min(num_cols));
                    let mut index_cols = Vec::new();
                    let mut available_cols: Vec<usize> = (0..num_cols).collect();

                    for _ in 0..num_index_cols {
                        let idx = rng.random_range(0..available_cols.len());
                        let col = available_cols.remove(idx);
                        index_cols.push(format!("c{col}"));
                    }

                    indexes.push(format!(
                        "CREATE INDEX idx_{i} ON t({})",
                        index_cols.join(", ")
                    ));
                } else {
                    // Single-column index
                    let col = rng.random_range(0..num_cols);
                    indexes.push(format!(
                        "CREATE INDEX idx_{i} ON {}",
                        if is_expr {
                            format!("t(LOWER(c{col}))")
                        } else {
                            format!("t(c{col})")
                        }
                    ));
                }
            }

            // Create tables and indexes in both databases
            let limbo_conn = limbo_db.connect_limbo();
            limbo_exec_rows(&limbo_conn, &table_def);
            for t in indexes.iter() {
                limbo_exec_rows(&limbo_conn, t);
            }

            let sqlite_conn = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();
            sqlite_conn.execute(&table_def, params![]).unwrap();
            for t in indexes.iter() {
                sqlite_conn.execute(t, params![]).unwrap();
            }

            // Triggers are not supported in MVCC (at least yet)
            let use_trigger = !is_mvcc;

            // Generate initial data
            // Triggers can cause quadratic complexity to the tested operations so limit total row count
            // whenever we have one to make the test runtime reasonable.
            let num_inserts = if use_trigger {
                rng.random_range(10..=100)
            } else {
                rng.random_range(10..=1000)
            };
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
            let insert_type = match rng.random_range(0..3) {
                0 => "",
                1 => "OR REPLACE",
                2 => "OR IGNORE",
                _ => unreachable!(),
            };
            let insert = format!(
                "INSERT {} INTO t ({}) VALUES {}",
                insert_type,
                col_names,
                insert_values.join(", ")
            );
            dml_statements.push(insert.clone());

            // Insert initial data into both databases
            sqlite_conn.execute(&insert, params![]).unwrap();
            limbo_exec_rows(&limbo_conn, &insert);

            // Self-affecting triggers (e.g CREATE TRIGGER t BEFORE DELETE ON t BEGIN UPDATE t ... END) are
            // an easy source of bugs, so create one some of the time.
            let trigger = if use_trigger {
                // Create a random trigger
                let trigger_time = if rng.random_bool(0.5) {
                    "BEFORE"
                } else {
                    "AFTER"
                };
                let trigger_event = match rng.random_range(0..3) {
                    0 => "INSERT".to_string(),
                    1 => {
                        // Optionally specify columns for UPDATE trigger
                        if rng.random_bool(0.5) {
                            let update_col = rng.random_range(0..num_cols);
                            format!("UPDATE OF c{update_col}")
                        } else {
                            "UPDATE".to_string()
                        }
                    }
                    2 => "DELETE".to_string(),
                    _ => unreachable!(),
                };

                // Determine if OLD/NEW references are available based on trigger event
                let has_old =
                    trigger_event.starts_with("UPDATE") || trigger_event.starts_with("DELETE");
                let has_new =
                    trigger_event.starts_with("UPDATE") || trigger_event.starts_with("INSERT");

                // Generate trigger action (INSERT, UPDATE, or DELETE)
                let trigger_action = match rng.random_range(0..3) {
                    0 => {
                        // INSERT action
                        let values = (0..num_cols)
                            .map(|i| {
                                // Randomly use OLD/NEW values if available
                                if has_old && rng.random_bool(0.3) {
                                    format!("OLD.c{i}")
                                } else if has_new && rng.random_bool(0.3) {
                                    format!("NEW.c{i}")
                                } else {
                                    rng.random_range(0..1000).to_string()
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        let insert_conflict_action = match rng.random_range(0..3) {
                            0 => "",
                            1 => " OR REPLACE",
                            2 => " OR IGNORE",
                            _ => unreachable!(),
                        };
                        format!(
                            "INSERT{insert_conflict_action} INTO t ({col_names}) VALUES ({values})"
                        )
                    }
                    1 => {
                        // UPDATE action
                        let update_col = rng.random_range(0..num_cols);
                        let new_value = if has_old && rng.random_bool(0.3) {
                            let ref_col = rng.random_range(0..num_cols);
                            // Sometimes make it a function of the OLD column
                            if rng.random_bool(0.5) {
                                let operator = *["+", "-", "*"].choose(&mut rng).unwrap();
                                let amount = rng.random_range(1..100);
                                format!("OLD.c{ref_col} {operator} {amount}")
                            } else {
                                format!("OLD.c{ref_col}")
                            }
                        } else if has_new && rng.random_bool(0.3) {
                            let ref_col = rng.random_range(0..num_cols);
                            // Sometimes make it a function of the NEW column
                            if rng.random_bool(0.5) {
                                let operator = *["+", "-", "*"].choose(&mut rng).unwrap();
                                let amount = rng.random_range(1..100);
                                format!("NEW.c{ref_col} {operator} {amount}")
                            } else {
                                format!("NEW.c{ref_col}")
                            }
                        } else {
                            rng.random_range(0..1000).to_string()
                        };
                        let op = match rng.random_range(0..=3) {
                            0 => "<",
                            1 => "<=",
                            2 => ">",
                            3 => ">=",
                            _ => unreachable!(),
                        };
                        let threshold = if has_old && rng.random_bool(0.3) {
                            let ref_col = rng.random_range(0..num_cols);
                            format!("OLD.c{ref_col}")
                        } else if has_new && rng.random_bool(0.3) {
                            let ref_col = rng.random_range(0..num_cols);
                            format!("NEW.c{ref_col}")
                        } else {
                            rng.random_range(0..1000).to_string()
                        };
                        format!("UPDATE t SET c{update_col} = {new_value} WHERE c{update_col} {op} {threshold}")
                    }
                    2 => {
                        // DELETE action
                        let delete_col = rng.random_range(0..num_cols);
                        let op = match rng.random_range(0..=3) {
                            0 => "<",
                            1 => "<=",
                            2 => ">",
                            3 => ">=",
                            _ => unreachable!(),
                        };
                        let threshold = if has_old && rng.random_bool(0.3) {
                            let ref_col = rng.random_range(0..num_cols);
                            format!("OLD.c{ref_col}")
                        } else if has_new && rng.random_bool(0.3) {
                            let ref_col = rng.random_range(0..num_cols);
                            format!("NEW.c{ref_col}")
                        } else {
                            rng.random_range(0..1000).to_string()
                        };
                        format!("DELETE FROM t WHERE c{delete_col} {op} {threshold}")
                    }
                    _ => unreachable!(),
                };

                let create_trigger = format!(
                    "CREATE TRIGGER test_trigger {trigger_time} {trigger_event} ON t BEGIN {trigger_action}; END;",
                );

                sqlite_conn.execute(&create_trigger, params![]).unwrap();
                limbo_exec_rows(&limbo_conn, &create_trigger);
                Some(create_trigger)
            } else {
                None
            };
            if let Some(ref trigger) = trigger {
                println!("{trigger};");
            }

            const COMPARISONS: [&str; 3] = ["=", "<", ">"];
            const INNER_ITERATIONS: usize = 20;

            for _ in 0..INNER_ITERATIONS {
                let do_update = rng.random_range(0..2) == 0;

                let comparison = COMPARISONS[rng.random_range(0..COMPARISONS.len())];
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
                    let affected_col = rng.random_range(0..num_cols);
                    let num_updates = rng.random_range(1..=num_cols);
                    let mut values = Vec::new();
                    for _ in 0..num_updates {
                        let new_y = if rng.random_bool(0.5) {
                            // Update to a constant value
                            rng.random_range(0..1000).to_string()
                        } else {
                            let source_col = rng.random_range(0..num_cols);
                            // Update to a value that is a function of the another column
                            let operator = *["+", "-"].choose(&mut rng).unwrap();
                            let amount = rng.random_range(0..1000);
                            format!("c{source_col} {operator} {amount}")
                        };
                        values.push(format!("c{affected_col} = {new_y}"));
                    }
                    format!("UPDATE t SET {} {where_clause}", values.join(", "))
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
                let limbo_rows = limbo_exec_rows(&limbo_conn, &verify_query);

                if sqlite_rows != limbo_rows {
                    let diff_msg = format_rows_diff(
                        &sqlite_rows,
                        &limbo_rows,
                        seed,
                        &query,
                        &table_def,
                        &indexes,
                        trigger.as_ref(),
                        &dml_statements,
                    );
                    panic!("Different results after mutation!{diff_msg}");
                }

                // Run integrity check on limbo db using rusqlite
                // Skip for MVCC databases since rusqlite can't read MVCC version (255)
                if !is_mvcc {
                    if let Err(e) = rusqlite_integrity_check(&limbo_db.path) {
                        println!("{table_def};");
                        for t in indexes.iter() {
                            println!("{t};");
                        }
                        if let Some(trigger) = trigger {
                            println!("{trigger};");
                        }
                        for t in dml_statements.iter() {
                            println!("{t};");
                        }
                        println!("{query};");
                        panic!("seed: {seed}, error: {e}");
                    }
                }

                if sqlite_rows.is_empty() {
                    break;
                }
            }
        }
    }

    // TODO: mvcc index
    #[turso_macros::test()]
    pub fn partial_index_mutation_and_upsert_fuzz(db: TempDatabase) {
        index_mutation_upsert_fuzz(db, 1.0, 4);
    }

    // TODO: mvcc fails
    #[turso_macros::test()]
    pub fn simple_index_mutation_and_upsert_fuzz(db: TempDatabase) {
        index_mutation_upsert_fuzz(db, 0.0, 4);
    }

    fn index_mutation_upsert_fuzz(
        db: TempDatabase,
        partial_index_prob: f64,
        conflict_chain_max_len: u32,
    ) {
        let _ = env_logger::try_init();
        const OUTER_ITERS: usize = 5;
        const INNER_ITERS: usize = 500;

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        let (mut rng, seed) = rng_from_time_or_env();
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
            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
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
            limbo_exec_rows(&limbo_conn, &create);
            sqlite.execute(&create, rusqlite::params![]).unwrap();

            // Helper to list usable columns for keys/predicates
            let int_cols: Vec<String> = (0..(num_cols - 1)).map(|i| format!("c{i}")).collect();
            let functions = ["lower", "upper", "length"];

            let num_pidx = rng.random_range(0..=3);
            let mut conflict_match_targets = vec!["".to_string(), "(id)".to_string()];
            let mut idx_ddls: Vec<String> = Vec::new();
            for i in 0..num_pidx {
                let is_expr = rng.random_bool(0.3);
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
                        match rng.random_range(0..4) {
                            0 => parts.push(format!("{c} IS NOT NULL")),
                            1 => {
                                let n = rng.random_range(-10..=20);
                                let op = *["<", "<=", "=", ">=", ">"].choose(&mut rng).unwrap();
                                parts.push(format!("{c} {op} {n}"));
                            }
                            2 => {
                                let n = rng.random_range(0..=1);
                                parts.push(format!(
                                    "{c} IS {}",
                                    if n == 0 { "NULL" } else { "NOT NULL" }
                                ));
                            }
                            _ => {
                                // BETWEEN expression
                                let lo = rng.random_range(-10..=10);
                                let hi = rng.random_range(lo..=20);
                                if rng.random_bool(0.2) {
                                    parts.push(format!("{c} NOT BETWEEN {lo} AND {hi}"));
                                } else {
                                    parts.push(format!("{c} BETWEEN {lo} AND {hi}"));
                                }
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

                let ddl = if rng.random_bool(partial_index_prob) {
                    format!(
                        "CREATE UNIQUE INDEX idx_p{}_{} ON {} WHERE {}",
                        outer,
                        i,
                        if is_expr {
                            format!(
                                "t({})",
                                key_cols
                                    .iter()
                                    .map(|c| format!(
                                        "{}( {})",
                                        functions.choose(&mut rng).unwrap(),
                                        c
                                    ))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        } else {
                            format!("t({})", key_cols.join(","))
                        },
                        pred
                    )
                } else {
                    // ON CONFLICT (...) can use only column set with non-partial UNIQUE constraint
                    conflict_match_targets.push(format!("({})", key_cols.join(",")));
                    format!(
                        "CREATE UNIQUE INDEX idx_p{}_{} ON t({})",
                        outer,
                        i,
                        key_cols.join(","),
                    )
                };
                idx_ddls.push(ddl.clone());
                // Create in both engines
                println!("{ddl};");
                limbo_exec_rows(&limbo_conn, &ddl);
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
                println!("{ins}; -- seed");
                // Execute on both; ignore errors due to partial unique conflicts (keep seeding going)
                let sqlite_res = sqlite.execute(&ins, rusqlite::params![]);
                let limbo_res = limbo_exec_rows_fallible(&limbo_db, &limbo_conn, &ins);
                assert!(sqlite_res.is_ok() == limbo_res.is_ok());
            }

            for _ in 0..INNER_ITERS {
                // Randomly inject transaction statements -- we don't care if they are legal,
                // we just care that tursodb/sqlite behave the same way.
                if rng.random_bool(0.15) {
                    let tx_stmt = match rng.random_range(0..4) {
                        0 => "BEGIN",
                        1 => "BEGIN IMMEDIATE",
                        2 => "COMMIT",
                        3 => "ROLLBACK",
                        _ => unreachable!(),
                    };
                    println!("{tx_stmt};");
                    let sqlite_res = sqlite.execute(tx_stmt, rusqlite::params![]);
                    let limbo_res = limbo_exec_rows_fallible(&limbo_db, &limbo_conn, tx_stmt);
                    // Both should succeed or both should fail
                    assert!(sqlite_res.is_ok() == limbo_res.is_ok());
                }
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
                            "INSERT {} INTO t({}) VALUES({})",
                            if rng.random_bool(0.3) {
                                "OR REPLACE"
                            } else if rng.random_bool(0.3) {
                                "OR IGNORE"
                            } else {
                                ""
                            },
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
                        let chain_length = rng.random_range(0..=conflict_chain_max_len);
                        let mut on_conflict = String::new();
                        for _ in 0..chain_length {
                            let idx = rng.random_range(0..conflict_match_targets.len());
                            let target = &conflict_match_targets[idx];
                            if rng.random_bool(0.8) {
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
                                on_conflict.push_str(&format!(
                                    " ON CONFLICT{} DO UPDATE SET {}",
                                    target,
                                    set_list.join(", ")
                                ));
                            } else {
                                on_conflict.push_str(&format!(" ON CONFLICT{target} DO NOTHING"));
                            }
                        }
                        format!(
                            "INSERT INTO t({}) VALUES({}) {}",
                            cols_list.join(","),
                            vals_list.join(","),
                            on_conflict
                        )
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
                        let l = limbo_exec_rows(&limbo_conn, &verify);
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

    #[turso_macros::test(mvcc)]
    pub fn compound_select_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
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

            limbo_exec_rows(&limbo_conn, &create_table_sql);
            sqlite_exec_rows(&sqlite_conn, &create_table_sql);

            let num_rows_to_insert = rng.random_range(MIN_ROWS_PER_TABLE..=MAX_ROWS_PER_TABLE);
            for _ in 0..num_rows_to_insert {
                let c1_val: i64 = rng.random_range(-3..3);
                let c2_val: i64 = rng.random_range(-3..3);
                let c3_val: i64 = rng.random_range(-3..3);

                let insert_sql =
                    format!("INSERT INTO {table_name} VALUES ({c1_val}, {c2_val}, {c3_val})",);
                limbo_exec_rows(&limbo_conn, &insert_sql);
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

            let limbo_results = limbo_exec_rows(&limbo_conn, &query);
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

    // TODO: indexes
    #[turso_macros::test()]
    pub fn ddl_compatibility_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        const ITERATIONS: usize = 1000;

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        for i in 0..ITERATIONS {
            let db = builder.clone().build();
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

            limbo_exec_rows(&conn, &create_sql);
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
            let res = limbo_exec_rows(&conn, &verify_sql);
            assert!(res.len() == 1, "Expected 1 row, got {res:?}");
            let Value::Text(s) = &res[0][0] else {
                panic!("sql should be TEXT");
            };
            assert_eq!(s.as_str(), create_sql);
        }
    }

    #[turso_macros::test(mvcc)]
    pub fn arithmetic_expression_fuzz(db: TempDatabase) {
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

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            let limbo = limbo_exec_rows(&limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?} seed: {seed}"
            );
        }
    }

    #[turso_macros::test(mvcc)]
    pub fn fuzz_ex(db: TempDatabase) {
        let _ = env_logger::try_init();
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
            let limbo = limbo_exec_rows(&limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}"
            );
        }
    }

    #[turso_macros::test(mvcc)]
    pub fn math_expression_fuzz_run(db: TempDatabase) {
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

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {query}");
            let limbo = limbo_exec_rows(&limbo_conn, &query);
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

    #[turso_macros::test(mvcc)]
    pub fn string_expression_fuzz_run(db: TempDatabase) {
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

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {query}");
            let limbo = limbo_exec_rows(&limbo_conn, &query);
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
            .option_symbol(rand_int(-1..2))
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

    fn predicate_builders(
        g: &GrammarGenerator,
        common: &CommonBuilders,
        tables: Option<&[TestTable]>,
    ) -> PredicateBuilders {
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
            .option(common.number)
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
                    .repeat(1..3, ", ")
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

    #[turso_macros::test(mvcc)]
    pub fn logical_expression_fuzz_run(db: TempDatabase) {
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

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {query}");
            let limbo = limbo_exec_rows(&limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?} seed: {seed}"
            );
        }
    }

    #[turso_macros::test(mvcc)]
    pub fn table_logical_expression_fuzz_ex1(db: TempDatabase) {
        let _ = env_logger::try_init();

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

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
            let db = builder.clone().build();
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            for query in queries.iter() {
                let limbo = limbo_exec_rows(&limbo_conn, query);
                let sqlite = sqlite_exec_rows(&sqlite_conn, query);
                assert_eq!(
                    limbo, sqlite,
                    "queries: {queries:?}, query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}"
                );
            }
        }
    }

    #[turso_macros::test(mvcc)]
    pub fn min_max_agg_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();

        let datatypes = ["INTEGER", "TEXT", "REAL", "BLOB"];
        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        for _ in 0..1000 {
            // Create table with random datatype
            let datatype = datatypes[rng.random_range(0..datatypes.len())];
            let create_table = format!("CREATE TABLE t (x {datatype})");

            let db = builder.clone().build();
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            limbo_exec_rows(&limbo_conn, &create_table);
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
            limbo_exec_rows(&limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            // Test min and max
            for agg in ["min(x)", "max(x)"] {
                let query = format!("SELECT {agg} FROM t");
                let limbo = limbo_exec_rows(&limbo_conn, &query);
                let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

                assert_eq!(
                    limbo, sqlite,
                    "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}, seed: {seed}, values: {values:?}, schema: {create_table}"
                );
            }
        }
    }

    #[turso_macros::test(mvcc)]
    pub fn affinity_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("affinity_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        for iteration in 0..500 {
            let db = builder.clone().build();
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
            limbo_exec_rows(&limbo_conn, &create_table);
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
            limbo_exec_rows(&limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            // Query values and their types to verify affinity rules are applied correctly
            let query = "SELECT x, typeof(x) FROM t";
            let limbo_result = limbo_exec_rows(&limbo_conn, query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, query);

            assert_eq!(
                limbo_result, sqlite_result,
                "iteration: {iteration}, seed: {seed}, affinity: {affinity}, values: {values:?}"
            );

            // Also test with ORDER BY to ensure affinity affects sorting
            let query_ordered = "SELECT x FROM t ORDER BY x";
            let limbo_ordered = limbo_exec_rows(&limbo_conn, query_ordered);
            let sqlite_ordered = sqlite_exec_rows(&sqlite_conn, query_ordered);

            assert_eq!(
                limbo_ordered, sqlite_ordered,
                "ORDER BY failed - iteration: {iteration}, seed: {seed}, affinity: {affinity}"
            );
        }
    }

    #[turso_macros::test(mvcc)]
    // Simple fuzz test for SUM with floats
    pub fn sum_agg_fuzz_floats(db: TempDatabase) {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        for _ in 0..100 {
            let db = builder.clone().build();
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            limbo_exec_rows(&limbo_conn, "CREATE TABLE t(x)");
            sqlite_exec_rows(&sqlite_conn, "CREATE TABLE t(x)");

            // Insert 50-100 mixed values: floats, text, NULL
            let mut values = Vec::new();
            for _ in 0..rng.random_range(50..=100) {
                let value = rng.random_range(-100.0..100.0).to_string();
                values.push(format!("({value})"));
            }

            let insert = format!("INSERT INTO t VALUES {}", values.join(","));
            limbo_exec_rows(&limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            let query = "SELECT sum(x) FROM t ORDER BY x";
            let limbo_result = limbo_exec_rows(&limbo_conn, query);
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

    #[turso_macros::test(mvcc)]
    // Simple fuzz test for SUM with mixed numeric/non-numeric values (issue #2133)
    pub fn sum_agg_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        for _ in 0..100 {
            let db = builder.clone().build();
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            limbo_exec_rows(&limbo_conn, "CREATE TABLE t(x)");
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
            limbo_exec_rows(&limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            let query = "SELECT sum(x) FROM t";
            let limbo = limbo_exec_rows(&limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);

            assert_eq!(limbo, sqlite, "seed: {seed}, values: {values:?}");
        }
    }

    #[turso_macros::test(mvcc)]
    fn concat_ws_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        for _ in 0..100 {
            let db = builder.clone().build();
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

            let limbo = limbo_exec_rows(&limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(limbo, sqlite, "seed: {seed}, sep: {sep}, args: {args:?}");
        }
    }

    #[turso_macros::test(mvcc)]
    // Simple fuzz test for TOTAL with mixed numeric/non-numeric values
    pub fn total_agg_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        for _ in 0..100 {
            let db = builder.clone().build();
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            limbo_exec_rows(&limbo_conn, "CREATE TABLE t(x)");
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
            limbo_exec_rows(&limbo_conn, &insert);
            sqlite_exec_rows(&sqlite_conn, &insert);

            let query = "SELECT total(x) FROM t";
            let limbo = limbo_exec_rows(&limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);

            assert_eq!(limbo, sqlite, "seed: {seed}, values: {values:?}");
        }
    }

    // TODO: mvcc indexes
    #[turso_macros::test()]
    pub fn table_logical_expression_fuzz_run(db: TempDatabase) {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let tables = vec![TestTable {
            name: "t",
            columns: vec!["x", "y", "z"],
        }];
        let builders = common_builders(&g, Some(&tables));
        let predicate = predicate_builders(&g, &builders, Some(&tables));
        let expr = build_logical_expr(&g, &builders, Some(&predicate));

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
            log::info!("schema: {query}");
            let limbo = limbo_exec_rows(&limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo, sqlite,
                "query: {query}, limbo: {limbo:?}, sqlite: {sqlite:?}",
            );
        }

        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("seed: {seed}");

        let mut i = 0;
        let mut primary_key_set = HashSet::with_capacity(100);
        while i < 1000 {
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
                limbo_exec_rows(&limbo_conn, &query),
                sqlite_exec_rows(&sqlite_conn, &query),
                "seed: {seed}",
            );
            i += 1;
        }
        // verify the same number of rows in both tables
        let query = "SELECT COUNT(*) FROM t".to_string();
        let limbo = limbo_exec_rows(&limbo_conn, &query);
        let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
        assert_eq!(limbo, sqlite, "seed: {seed}");

        let sql = g
            .create()
            .concat(" ")
            .push_str("SELECT ")
            .push(
                g.create()
                    .choice()
                    .option_str("*")
                    .option_str("COUNT(*)")
                    .build(),
            )
            .push_str(" FROM t WHERE ")
            .push(expr)
            .build();

        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {query}");
            let limbo = limbo_exec_rows(&limbo_conn, &query);
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

    #[turso_macros::test(mvcc)]
    pub fn fuzz_distinct(db: TempDatabase) {
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time_or_env();
        tracing::info!("fuzz_distinct seed: {}", seed);

        let columns = ["a", "b", "c", "d", "e"];

        // Create table with 3 integer columns
        let create_table = format!("CREATE TABLE t ({})", columns.join(", "));
        limbo_exec_rows(&limbo_conn, &create_table);
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
            limbo_exec_rows(&limbo_conn, &query);
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

            let limbo = limbo_exec_rows(&limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(limbo, sqlite, "seed: {seed}, query: {query}");
        }
    }

    #[turso_macros::test(mvcc)]
    fn fuzz_long_create_table_drop_table_alter_table(db: TempDatabase) {
        let limbo_conn = db.connect_limbo();

        let (mut rng, seed) = rng_from_time_or_env();
        let mvcc = db.enable_mvcc;
        tracing::info!("create_table_drop_table_fuzz seed: {seed}, mvcc: {mvcc}");

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

        let mut stmts = vec![];

        for iteration in 0..2000 {
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
                                if !mvcc || col_type == "INTEGER" {
                                    " PRIMARY KEY"
                                } else {
                                    ""
                                }
                            } else if rng.random_bool(0.1) {
                                if !mvcc {
                                    " UNIQUE"
                                } else {
                                    ""
                                }
                            } else {
                                ""
                            };

                            if constraint.contains("UNIQUE") || constraint.contains("PRIMARY KEY") {
                                undroppable_cols.insert((table_name.clone(), col_name.clone()));
                            }

                            columns.push(format!("{col_name} {col_type}{constraint}"));
                        }

                        let create_sql =
                            format!("CREATE TABLE {table_name} ({})", columns.join(", "));

                        // Execute the create table statement
                        stmts.push(create_sql.clone());
                        limbo_exec_rows(&limbo_conn, &create_sql);
                        let column_names = columns
                            .iter()
                            .map(|c| c.split_whitespace().next().unwrap().to_string())
                            .collect::<Vec<_>>();

                        // Insert a single row into the table
                        let insert_sql = format!(
                            "INSERT INTO {table_name} ({}) VALUES ({})",
                            column_names.join(", "),
                            (0..columns.len())
                                .map(|i| i.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                        stmts.push(insert_sql.clone());
                        limbo_exec_rows(&limbo_conn, &insert_sql);

                        // Successfully created table, update our tracking
                        current_tables.insert(table_name.clone(), column_names);
                    }
                }

                20..30 => {
                    // Drop table
                    if !current_tables.is_empty() {
                        let table_names: Vec<String> = current_tables.keys().cloned().collect();
                        let table_to_drop = &table_names[rng.random_range(0..table_names.len())];

                        let drop_sql = format!("DROP TABLE {table_to_drop}");
                        stmts.push(drop_sql.clone());
                        limbo_exec_rows(&limbo_conn, &drop_sql);

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

                        stmts.push(alter_sql.clone());
                        limbo_exec_rows(&limbo_conn, &alter_sql);

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
                                stmts.push(alter_sql.clone());
                                limbo_exec_rows(&limbo_conn, &alter_sql);

                                // Successfully dropped column, update our tracking
                                let columns = current_tables.get_mut(&table_name).unwrap();
                                columns.retain(|c| c != &col_to_drop);
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }

            // Do SELECT * FROM <table> for all current tables and just verify there is 1 row and the column count and names match the expected columns in the table
            for (table_name, columns) in current_tables.iter() {
                let select_sql = format!("SELECT * FROM {table_name}");
                let col_names_actual = limbo_stmt_get_column_names(&db, &limbo_conn, &select_sql);
                let col_names_expected = columns
                    .iter()
                    .map(|c| c.split_whitespace().next().unwrap().to_string())
                    .collect::<Vec<_>>();
                assert_eq!(
                    col_names_actual, col_names_expected,
                    "seed: {seed}, mvcc: {mvcc}, table: {table_name}"
                );
                let limbo = limbo_exec_rows(&limbo_conn, &select_sql);
                assert_eq!(
                    limbo.len(),
                    1,
                    "seed: {seed}, mvcc: {mvcc}, table: {table_name}"
                );
                assert_eq!(
                    limbo[0].len(),
                    columns.len(),
                    "seed: {seed}, mvcc: {mvcc}, table: {table_name}"
                );
            }
            if !mvcc {
                if let Err(e) = rusqlite_integrity_check(&db.path) {
                    for stmt in stmts.iter() {
                        println!("{stmt};");
                    }
                    panic!("seed: {seed}, mvcc: {mvcc}, error: {e}");
                }
            }
        }

        // Final verification - the test passes if we didn't crash
        println!(
            "create_table_drop_table_fuzz completed successfully with {} tables remaining. (mvcc: {mvcc}, seed: {seed})",
            current_tables.len()
        );
    }

    #[turso_macros::test()]
    #[cfg(feature = "test_helper")]
    pub fn fuzz_pending_byte_database(db: TempDatabase) -> anyhow::Result<()> {
        use core_tester::common::rusqlite_integrity_check;

        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        tracing::debug!(seed);

        // TODO: currently assume that page size is 4096 bytes (4 Kib)
        const PAGE_SIZE: u32 = 4 * 2u32.pow(10);

        /// 100 Mib
        const MAX_DB_SIZE_BYTES: u32 = 100 * 2u32.pow(20);

        const MAX_PAGENO: u32 = MAX_DB_SIZE_BYTES / PAGE_SIZE;

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        for _ in 0..10 {
            // generate a random pending page that is smaller than the 100 MB mark

            let pending_byte_pgno = rng.random_range(2..MAX_PAGENO);
            let pending_byte = pending_byte_pgno * PAGE_SIZE;

            tracing::debug!(pending_byte_pgno, pending_byte);

            let db_path = tempfile::NamedTempFile::new()?;

            {
                let db = builder.clone().with_db_path(db_path.path()).build();

                let prev_pending_byte = TempDatabase::get_pending_byte();
                tracing::debug!(prev_pending_byte);

                TempDatabase::set_pending_byte(pending_byte);

                let new_pending_byte = TempDatabase::get_pending_byte();
                tracing::debug!(new_pending_byte);

                // Insert more than enough to pass the PENDING_BYTE
                let query = format!("insert into t select replace(zeroblob({PAGE_SIZE}), x'00', 'A') from generate_series(1, {});", MAX_PAGENO * 2);

                let conn = db.connect_limbo();

                conn.execute("create table t(x);")?;

                conn.execute(&query)?;

                conn.close()?;
            }

            rusqlite_integrity_check(db_path.path())?;

            TempDatabase::reset_pending_byte();
        }

        Ok(())
    }

    // TODO: mvcc indexes
    #[turso_macros::test()]
    /// Tests for correlated and uncorrelated subqueries occurring in the WHERE clause of a SELECT statement.
    pub fn table_subquery_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("table_subquery_fuzz seed: {seed}");

        // Constants for fuzzing parameters
        const NUM_FUZZ_ITERATIONS: usize = 2000;
        const MAX_ROWS_PER_TABLE: usize = 100;
        const MIN_ROWS_PER_TABLE: usize = 5;
        const MAX_SUBQUERY_DEPTH: usize = 4;

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let mut debug_ddl_dml_string = String::new();

        // Create 3 simple tables
        let table_schemas = [
            "CREATE TABLE t1 (id INT PRIMARY KEY, value1 INTEGER, value2 INTEGER);",
            "CREATE TABLE t2 (id INT PRIMARY KEY, ref_id INTEGER, data INTEGER);",
            "CREATE TABLE t3 (id INT PRIMARY KEY, category INTEGER, amount INTEGER);",
        ];

        for schema in &table_schemas {
            debug_ddl_dml_string.push_str(schema);
            limbo_exec_rows(&limbo_conn, schema);
            sqlite_exec_rows(&sqlite_conn, schema);
        }

        // Populate tables with random data
        for table_num in 1..=3 {
            let num_rows = rng.random_range(MIN_ROWS_PER_TABLE..=MAX_ROWS_PER_TABLE);
            for i in 1..=num_rows {
                let insert_sql = match table_num {
                    1 => format!(
                        "INSERT INTO t1 VALUES ({}, {}, {});",
                        i,
                        rng.random_range(-10..20),
                        rng.random_range(-5..15)
                    ),
                    2 => format!(
                        "INSERT INTO t2 VALUES ({}, {}, {});",
                        i,
                        rng.random_range(1..=num_rows), // ref_id references t1 approximately
                        rng.random_range(-5..10)
                    ),
                    3 => format!(
                        "INSERT INTO t3 VALUES ({}, {}, {});",
                        i,
                        rng.random_range(1..5), // category 1-4
                        rng.random_range(0..100)
                    ),
                    _ => unreachable!(),
                };
                log::debug!("{insert_sql}");
                debug_ddl_dml_string.push_str(&insert_sql);
                limbo_exec_rows(&limbo_conn, &insert_sql);
                sqlite_exec_rows(&sqlite_conn, &insert_sql);
            }
        }

        log::info!("DDL/DML to reproduce manually:\n{debug_ddl_dml_string}");

        // Helper function to generate random simple WHERE condition
        let gen_simple_where = |rng: &mut ChaCha8Rng, table: &str| -> String {
            let conditions = match table {
                "t1" => vec![
                    format!("value1 > {}", rng.random_range(-5..15)),
                    format!("value2 < {}", rng.random_range(-5..15)),
                    format!("id <= {}", rng.random_range(1..20)),
                    "value1 IS NOT NULL".to_string(),
                ],
                "t2" => vec![
                    format!("data > {}", rng.random_range(-3..8)),
                    format!("ref_id = {}", rng.random_range(1..15)),
                    format!("id < {}", rng.random_range(5..25)),
                    "data IS NOT NULL".to_string(),
                ],
                "t3" => vec![
                    format!("category = {}", rng.random_range(1..5)),
                    format!("amount > {}", rng.random_range(0..50)),
                    format!("id <= {}", rng.random_range(1..20)),
                    "amount IS NOT NULL".to_string(),
                ],
                _ => vec!["1=1".to_string()],
            };
            conditions[rng.random_range(0..conditions.len())].clone()
        };

        // Helper function to generate simple subquery
        fn gen_subquery(
            rng: &mut ChaCha8Rng,
            depth: usize,
            outer_table: Option<&str>,
            allowed_outer_cols: Option<&[&str]>,
        ) -> String {
            if depth > MAX_SUBQUERY_DEPTH {
                return "SELECT 1".to_string();
            }

            let gen_simple_where_inner = |rng: &mut ChaCha8Rng, table: &str| -> String {
                let conditions = match table {
                    "t1" => vec![
                        format!("value1 > {}", rng.random_range(-5..15)),
                        format!("value2 < {}", rng.random_range(-5..15)),
                        format!("id <= {}", rng.random_range(1..20)),
                        "value1 IS NOT NULL".to_string(),
                    ],
                    "t2" => vec![
                        format!("data > {}", rng.random_range(-3..8)),
                        format!("ref_id = {}", rng.random_range(1..15)),
                        format!("id < {}", rng.random_range(5..25)),
                        "data IS NOT NULL".to_string(),
                    ],
                    "t3" => vec![
                        format!("category = {}", rng.random_range(1..5)),
                        format!("amount > {}", rng.random_range(0..50)),
                        format!("id <= {}", rng.random_range(1..20)),
                        "amount IS NOT NULL".to_string(),
                    ],
                    _ => vec!["1=1".to_string()],
                };
                conditions[rng.random_range(0..conditions.len())].clone()
            };

            // Helper function to generate correlated WHERE conditions
            let gen_correlated_where =
                |rng: &mut ChaCha8Rng, inner_table: &str, outer_table: &str| -> String {
                    let pick =
                        |rng: &mut ChaCha8Rng, mut candidates: Vec<(String, &'static str)>| {
                            let filtered: Vec<String> = if let Some(allowed) = allowed_outer_cols {
                                candidates
                                    .drain(..)
                                    .filter(|(_, col)| allowed.contains(col))
                                    .map(|(cond, _)| cond)
                                    .collect()
                            } else {
                                candidates.drain(..).map(|(cond, _)| cond).collect()
                            };
                            if filtered.is_empty() {
                                "1=1".to_string()
                            } else {
                                filtered[rng.random_range(0..filtered.len())].clone()
                            }
                        };
                    match (outer_table, inner_table) {
                        ("t1", "t2") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.ref_id = {outer_table}.id"), "id"),
                                (format!("{inner_table}.id < {outer_table}.value1"), "value1"),
                                (
                                    format!("{inner_table}.data > {outer_table}.value2"),
                                    "value2",
                                ),
                            ],
                        ),
                        ("t1", "t3") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.id"), "id"),
                                (
                                    format!("{inner_table}.category < {outer_table}.value1"),
                                    "value1",
                                ),
                                (
                                    format!("{inner_table}.amount > {outer_table}.value2"),
                                    "value2",
                                ),
                            ],
                        ),
                        ("t2", "t1") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.ref_id"), "ref_id"),
                                (format!("{inner_table}.value1 > {outer_table}.data"), "data"),
                                (format!("{inner_table}.value2 < {outer_table}.id"), "id"),
                            ],
                        ),
                        ("t2", "t3") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.id"), "id"),
                                (
                                    format!("{inner_table}.category = {outer_table}.ref_id"),
                                    "ref_id",
                                ),
                                (format!("{inner_table}.amount > {outer_table}.data"), "data"),
                            ],
                        ),
                        ("t3", "t1") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.id"), "id"),
                                (
                                    format!("{inner_table}.value1 > {outer_table}.category"),
                                    "category",
                                ),
                                (
                                    format!("{inner_table}.value2 < {outer_table}.amount"),
                                    "amount",
                                ),
                            ],
                        ),
                        ("t3", "t2") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.id"), "id"),
                                (
                                    format!("{inner_table}.ref_id = {outer_table}.category"),
                                    "category",
                                ),
                                (
                                    format!("{inner_table}.data < {outer_table}.amount"),
                                    "amount",
                                ),
                            ],
                        ),
                        _ => "1=1".to_string(),
                    }
                };

            let subquery_types = [
                // Simple scalar subqueries - single column only for safe nesting
                "SELECT MAX(amount) FROM t3".to_string(),
                "SELECT MIN(value1) FROM t1".to_string(),
                "SELECT COUNT(*) FROM t2".to_string(),
                "SELECT AVG(amount) FROM t3".to_string(),
                "SELECT id FROM t1".to_string(),
                "SELECT ref_id FROM t2".to_string(),
                "SELECT category FROM t3".to_string(),
                // Subqueries with WHERE - single column only
                format!(
                    "SELECT MAX(amount) FROM t3 WHERE {}",
                    gen_simple_where_inner(rng, "t3")
                ),
                format!(
                    "SELECT value1 FROM t1 WHERE {}",
                    gen_simple_where_inner(rng, "t1")
                ),
                format!(
                    "SELECT ref_id FROM t2 WHERE {}",
                    gen_simple_where_inner(rng, "t2")
                ),
            ];

            let base_query = &subquery_types[rng.random_range(0..subquery_types.len())];

            // Add correlated conditions if outer_table is provided and sometimes
            let final_query = if let Some(outer_table) = outer_table {
                let can_correlate = match allowed_outer_cols {
                    Some(cols) => !cols.is_empty(),
                    None => true,
                };
                if can_correlate && rng.random_bool(0.4) {
                    // 40% chance for correlation
                    // Extract the inner table from the base query
                    let inner_table = if base_query.contains("FROM t1") {
                        "t1"
                    } else if base_query.contains("FROM t2") {
                        "t2"
                    } else if base_query.contains("FROM t3") {
                        "t3"
                    } else {
                        return base_query.clone(); // fallback
                    };

                    let correlated_condition = gen_correlated_where(rng, inner_table, outer_table);

                    if base_query.contains("WHERE") {
                        format!("{base_query} AND {correlated_condition}")
                    } else {
                        format!("{base_query} WHERE {correlated_condition}")
                    }
                } else {
                    base_query.clone()
                }
            } else {
                base_query.clone()
            };

            // Sometimes add nesting - but use scalar subquery for nesting to avoid column count issues
            if depth < 1 && rng.random_bool(0.2) {
                // Reduced probability and depth
                let nested = gen_scalar_subquery(rng, 0, outer_table, allowed_outer_cols);
                if final_query.contains("WHERE") {
                    format!("{final_query} AND id IN ({nested})")
                } else {
                    format!("{final_query} WHERE id IN ({nested})")
                }
            } else {
                final_query
            }
        }

        // Helper function to generate scalar subquery (single column only)
        fn gen_scalar_subquery(
            rng: &mut ChaCha8Rng,
            depth: usize,
            outer_table: Option<&str>,
            allowed_outer_cols: Option<&[&str]>,
        ) -> String {
            if depth > MAX_SUBQUERY_DEPTH {
                // Reduced nesting depth
                return "SELECT 1".to_string();
            }

            let gen_simple_where_inner = |rng: &mut ChaCha8Rng, table: &str| -> String {
                let conditions = match table {
                    "t1" => vec![
                        format!("value1 > {}", rng.random_range(-5..15)),
                        format!("value2 < {}", rng.random_range(-5..15)),
                        format!("id <= {}", rng.random_range(1..20)),
                        "value1 IS NOT NULL".to_string(),
                    ],
                    "t2" => vec![
                        format!("data > {}", rng.random_range(-3..8)),
                        format!("ref_id = {}", rng.random_range(1..15)),
                        format!("id < {}", rng.random_range(5..25)),
                        "data IS NOT NULL".to_string(),
                    ],
                    "t3" => vec![
                        format!("category = {}", rng.random_range(1..5)),
                        format!("amount > {}", rng.random_range(0..50)),
                        format!("id <= {}", rng.random_range(1..20)),
                        "amount IS NOT NULL".to_string(),
                    ],
                    _ => vec!["1=1".to_string()],
                };
                conditions[rng.random_range(0..conditions.len())].clone()
            };

            // Helper function to generate correlated WHERE conditions
            let gen_correlated_where =
                |rng: &mut ChaCha8Rng, inner_table: &str, outer_table: &str| -> String {
                    let pick =
                        |rng: &mut ChaCha8Rng, mut candidates: Vec<(String, &'static str)>| {
                            let filtered: Vec<String> = if let Some(allowed) = allowed_outer_cols {
                                candidates
                                    .drain(..)
                                    .filter(|(_, col)| allowed.contains(col))
                                    .map(|(cond, _)| cond)
                                    .collect()
                            } else {
                                candidates.drain(..).map(|(cond, _)| cond).collect()
                            };
                            if filtered.is_empty() {
                                "1=1".to_string()
                            } else {
                                filtered[rng.random_range(0..filtered.len())].clone()
                            }
                        };
                    match (outer_table, inner_table) {
                        ("t1", "t2") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.ref_id = {outer_table}.id"), "id"),
                                (format!("{inner_table}.id < {outer_table}.value1"), "value1"),
                                (
                                    format!("{inner_table}.data > {outer_table}.value2"),
                                    "value2",
                                ),
                            ],
                        ),
                        ("t1", "t3") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.id"), "id"),
                                (
                                    format!("{inner_table}.category < {outer_table}.value1"),
                                    "value1",
                                ),
                                (
                                    format!("{inner_table}.amount > {outer_table}.value2"),
                                    "value2",
                                ),
                            ],
                        ),
                        ("t2", "t1") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.ref_id"), "ref_id"),
                                (format!("{inner_table}.value1 > {outer_table}.data"), "data"),
                                (format!("{inner_table}.value2 < {outer_table}.id"), "id"),
                            ],
                        ),
                        ("t2", "t3") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.id"), "id"),
                                (
                                    format!("{inner_table}.category = {outer_table}.ref_id"),
                                    "ref_id",
                                ),
                                (format!("{inner_table}.amount > {outer_table}.data"), "data"),
                            ],
                        ),
                        ("t3", "t1") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.id"), "id"),
                                (
                                    format!("{inner_table}.value1 > {outer_table}.category"),
                                    "category",
                                ),
                                (
                                    format!("{inner_table}.value2 < {outer_table}.amount"),
                                    "amount",
                                ),
                            ],
                        ),
                        ("t3", "t2") => pick(
                            rng,
                            vec![
                                (format!("{inner_table}.id = {outer_table}.id"), "id"),
                                (
                                    format!("{inner_table}.ref_id = {outer_table}.category"),
                                    "category",
                                ),
                                (
                                    format!("{inner_table}.data < {outer_table}.amount"),
                                    "amount",
                                ),
                            ],
                        ),
                        _ => "1=1".to_string(),
                    }
                };

            let scalar_subquery_types = [
                // Only scalar subqueries - single column only
                "SELECT MAX(amount) FROM t3".to_string(),
                "SELECT MIN(value1) FROM t1".to_string(),
                "SELECT COUNT(*) FROM t2".to_string(),
                "SELECT AVG(amount) FROM t3".to_string(),
                "SELECT id FROM t1".to_string(),
                "SELECT ref_id FROM t2".to_string(),
                "SELECT category FROM t3".to_string(),
                // Scalar subqueries with WHERE
                format!(
                    "SELECT MAX(amount) FROM t3 WHERE {}",
                    gen_simple_where_inner(rng, "t3")
                ),
                format!(
                    "SELECT value1 FROM t1 WHERE {}",
                    gen_simple_where_inner(rng, "t1")
                ),
                format!(
                    "SELECT ref_id FROM t2 WHERE {}",
                    gen_simple_where_inner(rng, "t2")
                ),
            ];

            let base_query =
                &scalar_subquery_types[rng.random_range(0..scalar_subquery_types.len())];

            // Add correlated conditions if outer_table is provided and sometimes
            let final_query = if let Some(outer_table) = outer_table {
                let can_correlate = match allowed_outer_cols {
                    Some(cols) => !cols.is_empty(),
                    None => true,
                };
                if can_correlate && rng.random_bool(0.4) {
                    // 40% chance for correlation
                    // Extract the inner table from the base query
                    let inner_table = if base_query.contains("FROM t1") {
                        "t1"
                    } else if base_query.contains("FROM t2") {
                        "t2"
                    } else if base_query.contains("FROM t3") {
                        "t3"
                    } else {
                        return base_query.clone(); // fallback
                    };

                    let correlated_condition = gen_correlated_where(rng, inner_table, outer_table);

                    if base_query.contains("WHERE") {
                        format!("{base_query} AND {correlated_condition}")
                    } else {
                        format!("{base_query} WHERE {correlated_condition}")
                    }
                } else {
                    base_query.clone()
                }
            } else {
                base_query.clone()
            };

            // Sometimes add nesting
            if depth < 1 && rng.random_bool(0.2) {
                // Reduced probability and depth
                let nested = gen_scalar_subquery(rng, depth + 1, outer_table, allowed_outer_cols);
                if final_query.contains("WHERE") {
                    format!("{final_query} AND id IN ({nested})")
                } else {
                    format!("{final_query} WHERE id IN ({nested})")
                }
            } else {
                final_query
            }
        }

        // Helper to generate a SELECT-list expression as a scalar subquery, optionally correlated
        fn gen_selectlist_scalar_expr(rng: &mut ChaCha8Rng, outer_table: &str) -> String {
            // Reuse scalar subquery generator; return the inner SELECT (without wrapping)
            gen_scalar_subquery(rng, 0, Some(outer_table), None)
        }

        // Helper to generate a GROUP BY expression which may include a correlated scalar subquery
        fn gen_group_by_expr(rng: &mut ChaCha8Rng, main_table: &str) -> String {
            // Either a plain column or a correlated scalar subquery
            if rng.random_bool(0.4) {
                // Prefer plain columns most of the time to keep GROUP BY semantics simple
                match main_table {
                    "t1" => ["id", "value1", "value2"][rng.random_range(0..3)].to_string(),
                    "t2" => ["id", "ref_id", "data"][rng.random_range(0..3)].to_string(),
                    "t3" => ["id", "category", "amount"][rng.random_range(0..3)].to_string(),
                    _ => "id".to_string(),
                }
            } else {
                // If GROUP BY is present, a subquery that references outer columns would be invalid
                // unless it only references GROUP BY columns; since this subquery becomes the
                // grouping expression itself, disallow correlation entirely here.
                format!(
                    "({})",
                    gen_scalar_subquery(rng, 0, Some(main_table), Some(&[]))
                )
            }
        }

        // Helper to generate a HAVING condition comparing an aggregate to a scalar subquery
        fn gen_having_condition(rng: &mut ChaCha8Rng, main_table: &str) -> String {
            let (agg_func, agg_col) = match main_table {
                "t1" => [
                    ("SUM", "value1"),
                    ("SUM", "value2"),
                    ("MAX", "value1"),
                    ("MAX", "value2"),
                    ("MIN", "value1"),
                    ("MIN", "value2"),
                    ("COUNT", "*"),
                ][rng.random_range(0..7)],
                "t2" => [
                    ("SUM", "data"),
                    ("MAX", "data"),
                    ("MIN", "data"),
                    ("COUNT", "*"),
                ][rng.random_range(0..4)],
                "t3" => [
                    ("SUM", "amount"),
                    ("MAX", "amount"),
                    ("MIN", "amount"),
                    ("COUNT", "*"),
                ][rng.random_range(0..4)],
                _ => ("COUNT", "*"),
            };
            let op = [">", "<", ">=", "<=", "=", "<>"][rng.random_range(0..6)];
            let rhs = gen_scalar_subquery(rng, 0, Some(main_table), Some(&[]));
            if agg_col == "*" {
                format!("COUNT(*) {op} ({rhs})")
            } else {
                format!("{agg_func}({agg_col}) {op} ({rhs})")
            }
        }

        // Helper to generate LIMIT/OFFSET clause (optionally empty). Expressions may be subqueries.
        fn gen_limit_offset_clause(rng: &mut ChaCha8Rng) -> String {
            // 50% of the time, no LIMIT/OFFSET
            if rng.random_bool(0.5) {
                return String::new();
            }

            fn gen_limit_like_expr(rng: &mut ChaCha8Rng) -> String {
                // Small literal or a scalar subquery from a random table
                if rng.random_bool(0.6) {
                    // Keep literal sizes modest
                    format!("{}", rng.random_range(0..20))
                } else {
                    let which = rng.random_range(0..3);
                    match which {
                        0 => "(SELECT COUNT(*) FROM t1)".to_string(),
                        1 => "(SELECT COUNT(*) FROM t2)".to_string(),
                        _ => "(SELECT COUNT(*) FROM t3)".to_string(),
                    }
                }
            }

            let mut clause = String::new();
            let limit_expr = gen_limit_like_expr(rng);
            clause.push_str(&format!(" LIMIT {limit_expr}",));
            if rng.random_bool(0.5) {
                let offset_expr = gen_limit_like_expr(rng);
                clause.push_str(&format!(" OFFSET {offset_expr}",));
            }
            clause
        }

        for iter_num in 0..NUM_FUZZ_ITERATIONS {
            let main_table = ["t1", "t2", "t3"][rng.random_range(0..3)];

            let query_type = rng.random_range(0..8); // Add GROUP BY/HAVING variants
            let mut query = match query_type {
                0 => {
                    // Comparison subquery: WHERE column <op> (SELECT ...)
                    let column = match main_table {
                        "t1" => ["value1", "value2", "id"][rng.random_range(0..3)],
                        "t2" => ["data", "ref_id", "id"][rng.random_range(0..3)],
                        "t3" => ["amount", "category", "id"][rng.random_range(0..3)],
                        _ => "id",
                    };
                    let op = [">", "<", ">=", "<=", "=", "<>"][rng.random_range(0..6)];
                    let subquery = gen_scalar_subquery(&mut rng, 0, Some(main_table), None);
                    format!("SELECT * FROM {main_table} WHERE {column} {op} ({subquery})",)
                }
                1 => {
                    // EXISTS subquery: WHERE [NOT] EXISTS (SELECT ...)
                    let not_exists = if rng.random_bool(0.3) { "NOT " } else { "" };
                    let subquery = gen_subquery(&mut rng, 0, Some(main_table), None);
                    format!("SELECT * FROM {main_table} WHERE {not_exists}EXISTS ({subquery})",)
                }
                2 => {
                    // IN subquery with single column: WHERE column [NOT] IN (SELECT ...)
                    let not_in = if rng.random_bool(0.3) { "NOT " } else { "" };
                    let column = match main_table {
                        "t1" => ["value1", "value2", "id"][rng.random_range(0..3)],
                        "t2" => ["data", "ref_id", "id"][rng.random_range(0..3)],
                        "t3" => ["amount", "category", "id"][rng.random_range(0..3)],
                        _ => "id",
                    };
                    let subquery = gen_scalar_subquery(&mut rng, 0, Some(main_table), None);
                    format!("SELECT * FROM {main_table} WHERE {column} {not_in}IN ({subquery})",)
                }
                3 => {
                    // IN subquery with tuple: WHERE (col1, col2) [NOT] IN (SELECT col1, col2 ...)
                    let not_in = if rng.random_bool(0.3) { "NOT " } else { "" };
                    let (columns, sub_columns) = match main_table {
                        "t1" => {
                            if rng.random_bool(0.5) {
                                ("(id, value1)", "SELECT id, value1 FROM t1")
                            } else {
                                ("id", "SELECT id FROM t1")
                            }
                        }
                        "t2" => {
                            if rng.random_bool(0.5) {
                                ("(ref_id, data)", "SELECT ref_id, data FROM t2")
                            } else {
                                ("ref_id", "SELECT ref_id FROM t2")
                            }
                        }
                        "t3" => {
                            if rng.random_bool(0.5) {
                                ("(id, category)", "SELECT id, category FROM t3")
                            } else {
                                ("id", "SELECT id FROM t3")
                            }
                        }
                        _ => ("id", "SELECT id FROM t1"),
                    };
                    let subquery = if rng.random_bool(0.5) {
                        sub_columns.to_string()
                    } else {
                        let base = sub_columns;
                        let table_for_where = base.split("FROM ").nth(1).unwrap_or("t1");
                        format!(
                            "{} WHERE {}",
                            base,
                            gen_simple_where(&mut rng, table_for_where)
                        )
                    };
                    format!("SELECT * FROM {main_table} WHERE {columns} {not_in}IN ({subquery})",)
                }
                4 => {
                    // Correlated EXISTS subquery: WHERE [NOT] EXISTS (SELECT ... WHERE correlation)
                    let not_exists = if rng.random_bool(0.3) { "NOT " } else { "" };

                    // Choose a different table for the subquery to ensure correlation is meaningful
                    let inner_tables = match main_table {
                        "t1" => ["t2", "t3"],
                        "t2" => ["t1", "t3"],
                        "t3" => ["t1", "t2"],
                        _ => ["t1", "t2"],
                    };
                    let inner_table = inner_tables[rng.random_range(0..inner_tables.len())];

                    // Generate correlated condition
                    let correlated_condition = match (main_table, inner_table) {
                        ("t1", "t2") => {
                            let conditions = [
                                format!("{inner_table}.ref_id = {main_table}.id"),
                                format!("{inner_table}.id < {main_table}.value1"),
                                format!("{inner_table}.data > {main_table}.value2"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t1", "t3") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.id"),
                                format!("{inner_table}.category < {main_table}.value1"),
                                format!("{inner_table}.amount > {main_table}.value2"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t2", "t1") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.ref_id"),
                                format!("{inner_table}.value1 > {main_table}.data"),
                                format!("{inner_table}.value2 < {main_table}.id"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t2", "t3") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.id"),
                                format!("{inner_table}.category = {main_table}.ref_id"),
                                format!("{inner_table}.amount > {main_table}.data"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t3", "t1") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.id"),
                                format!("{inner_table}.value1 > {main_table}.category"),
                                format!("{inner_table}.value2 < {main_table}.amount"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t3", "t2") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.id"),
                                format!("{inner_table}.ref_id = {main_table}.category"),
                                format!("{inner_table}.data < {main_table}.amount"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        _ => "1=1".to_string(),
                    };

                    format!(
                        "SELECT * FROM {main_table} WHERE {not_exists}EXISTS (SELECT 1 FROM {inner_table} WHERE {correlated_condition})",
                    )
                }
                5 => {
                    // Correlated comparison subquery: WHERE column <op> (SELECT ... WHERE correlation)
                    let column = match main_table {
                        "t1" => ["value1", "value2", "id"][rng.random_range(0..3)],
                        "t2" => ["data", "ref_id", "id"][rng.random_range(0..3)],
                        "t3" => ["amount", "category", "id"][rng.random_range(0..3)],
                        _ => "id",
                    };
                    let op = [">", "<", ">=", "<=", "=", "<>"][rng.random_range(0..6)];

                    // Choose a different table for the subquery
                    let inner_tables = match main_table {
                        "t1" => ["t2", "t3"],
                        "t2" => ["t1", "t3"],
                        "t3" => ["t1", "t2"],
                        _ => ["t1", "t2"],
                    };
                    let inner_table = inner_tables[rng.random_range(0..inner_tables.len())];

                    // Choose what to select from inner table
                    let select_column = match inner_table {
                        "t1" => ["value1", "value2", "id"][rng.random_range(0..3)],
                        "t2" => ["data", "ref_id", "id"][rng.random_range(0..3)],
                        "t3" => ["amount", "category", "id"][rng.random_range(0..3)],
                        _ => "id",
                    };

                    // Generate correlated condition
                    let correlated_condition = match (main_table, inner_table) {
                        ("t1", "t2") => {
                            let conditions = [
                                format!("{inner_table}.ref_id = {main_table}.id"),
                                format!("{inner_table}.id < {main_table}.value1"),
                                format!("{inner_table}.data > {main_table}.value2"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t1", "t3") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.id"),
                                format!("{inner_table}.category < {main_table}.value1"),
                                format!("{inner_table}.amount > {main_table}.value2"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t2", "t1") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.ref_id"),
                                format!("{inner_table}.value1 > {main_table}.data"),
                                format!("{inner_table}.value2 < {main_table}.id"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t2", "t3") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.id"),
                                format!("{inner_table}.category = {main_table}.ref_id"),
                                format!("{inner_table}.amount > {main_table}.data"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t3", "t1") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.id"),
                                format!("{inner_table}.value1 > {main_table}.category"),
                                format!("{inner_table}.value2 < {main_table}.amount"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        ("t3", "t2") => {
                            let conditions = [
                                format!("{inner_table}.id = {main_table}.id"),
                                format!("{inner_table}.ref_id = {main_table}.category"),
                                format!("{inner_table}.data < {main_table}.amount"),
                            ];
                            conditions[rng.random_range(0..conditions.len())].clone()
                        }
                        _ => "1=1".to_string(),
                    };

                    format!(
                        "SELECT * FROM {main_table} WHERE {column} {op} (SELECT {select_column} FROM {inner_table} WHERE {correlated_condition})",
                    )
                }
                6 => {
                    // Aggregated query with GROUP BY and optional HAVING; allow subqueries in GROUP BY/HAVING
                    let group_expr = gen_group_by_expr(&mut rng, main_table);
                    let (agg_func, agg_col) = match main_table {
                        "t1" => [
                            ("SUM", "value1"),
                            ("SUM", "value2"),
                            ("MAX", "value1"),
                            ("MAX", "value2"),
                            ("COUNT", "*"),
                        ][rng.random_range(0..5)],
                        "t2" => [("SUM", "data"), ("MAX", "data"), ("COUNT", "*")]
                            [rng.random_range(0..3)],
                        "t3" => [("SUM", "amount"), ("MAX", "amount"), ("COUNT", "*")]
                            [rng.random_range(0..3)],
                        _ => ("COUNT", "*"),
                    };
                    let mut q;
                    if agg_col == "*" {
                        q = format!("SELECT {group_expr} AS g, COUNT(*) AS c FROM {main_table}");
                    } else {
                        q = format!("SELECT {group_expr} AS g, {agg_func}({agg_col}) AS a FROM {main_table}");
                    }
                    if rng.random_bool(0.5) {
                        q.push_str(&format!(
                            " WHERE {}",
                            gen_simple_where(&mut rng, main_table)
                        ));
                    }
                    q.push_str(&format!(" GROUP BY {group_expr}"));
                    if rng.random_bool(0.4) {
                        q.push_str(&format!(
                            " HAVING {}",
                            gen_having_condition(&mut rng, main_table)
                        ));
                    }
                    q
                }
                7 => {
                    // Simple GROUP BY without HAVING (baseline support); may use subquery in GROUP BY
                    let group_expr = gen_group_by_expr(&mut rng, main_table);
                    let select_expr = if rng.random_bool(0.5) {
                        // Use aggregate
                        match main_table {
                            "t1" => "SUM(value1) AS s".to_string(),
                            "t2" => "SUM(data) AS s".to_string(),
                            _ => "SUM(amount) AS s".to_string(),
                        }
                    } else {
                        "COUNT(*) AS c".to_string()
                    };
                    let mut q =
                        format!("SELECT {group_expr} AS g, {select_expr} FROM {main_table}");
                    if rng.random_bool(0.5) {
                        q.push_str(&format!(
                            " WHERE {}",
                            gen_simple_where(&mut rng, main_table)
                        ));
                    }
                    q.push_str(&format!(" GROUP BY {group_expr}"));
                    q
                }
                _ => unreachable!(),
            };
            // Optionally inject a SELECT-list scalar subquery into non-aggregated SELECT * queries
            if query.starts_with("SELECT * FROM ") && rng.random_bool(0.4) {
                let sel_expr = gen_selectlist_scalar_expr(&mut rng, main_table);
                let replacement = "SELECT *, (".to_string() + &sel_expr + ") AS s_sub FROM ";
                query = query.replacen("SELECT * FROM ", &replacement, 1);
            }

            // Optionally append LIMIT/OFFSET (with or without subqueries)
            let limit_clause = gen_limit_offset_clause(&mut rng);
            if !limit_clause.is_empty() {
                query.push_str(&limit_clause);
            }

            log::info!(
                "Iteration {}/{NUM_FUZZ_ITERATIONS}: Query: {query}",
                iter_num + 1,
            );

            let limbo_results = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_results = sqlite_exec_rows(&sqlite_conn, &query);

            // Check if results match
            if limbo_results.len() != sqlite_results.len() {
                panic!(
                    "Row count mismatch for query: {}\nLimbo: {} rows, SQLite: {} rows\nLimbo: {:?}\nSQLite: {:?}\nSeed: {}\n\n DDL/DML to reproduce manually:\n{}",
                    query, limbo_results.len(), sqlite_results.len(), limbo_results, sqlite_results, seed, debug_ddl_dml_string
                );
            }

            // Check if all rows match (order might be different)
            // Since Value doesn't implement Ord, we'll check containment both ways
            let all_limbo_in_sqlite = limbo_results.iter().all(|limbo_row| {
                sqlite_results
                    .iter()
                    .any(|sqlite_row| limbo_row == sqlite_row)
            });
            let all_sqlite_in_limbo = sqlite_results.iter().all(|sqlite_row| {
                limbo_results
                    .iter()
                    .any(|limbo_row| sqlite_row == limbo_row)
            });

            if !all_limbo_in_sqlite || !all_sqlite_in_limbo {
                panic!(
                    "Results mismatch for query: {query}\nLimbo: {limbo_results:?}\nSQLite: {sqlite_results:?}\nSeed: {seed}",
                );
            }
        }
    }

    /// Fuzz test for DELETE/UPDATE statements with IN/NOT IN subqueries.
    /// This test generates random DELETE and UPDATE statements using IN/NOT IN subqueries
    /// and compares results between Limbo and SQLite to ensure correctness.
    #[turso_macros::test(mvcc)]
    pub fn dml_subquery_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("dml_subquery_fuzz seed: {seed}");

        const NUM_FUZZ_ITERATIONS: usize = 500;
        const MAX_ROWS_PER_TABLE: usize = 500;
        const MIN_ROWS_PER_TABLE: usize = 50;

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let mut debug_ddl_dml_string = String::new();

        // Create two related tables for subquery testing
        let table_schemas = [
            "CREATE TABLE main_data (id INTEGER PRIMARY KEY, value INTEGER, category INTEGER);",
            "CREATE TABLE ref_data (ref_id INTEGER);",
        ];

        for schema in &table_schemas {
            debug_ddl_dml_string.push_str(schema);
            debug_ddl_dml_string.push('\n');
            limbo_exec_rows(&limbo_conn, schema);
            sqlite_exec_rows(&sqlite_conn, schema);
        }

        fn populate_tables(
            limbo_conn: &std::sync::Arc<turso_core::Connection>,
            sqlite_conn: &rusqlite::Connection,
            rng: &mut ChaCha8Rng,
            debug_string: &mut String,
            min_rows: usize,
            max_rows: usize,
        ) {
            // Populate main_data
            let num_main_rows = rng.random_range(min_rows..=max_rows);
            for i in 1..=num_main_rows {
                let insert_sql = format!(
                    "INSERT INTO main_data VALUES ({}, {}, {});",
                    i,
                    rng.random_range(-100..100),
                    rng.random_range(1..10)
                );
                debug_string.push_str(&insert_sql);
                debug_string.push('\n');
                limbo_exec_rows(limbo_conn, &insert_sql);
                sqlite_exec_rows(sqlite_conn, &insert_sql);
            }

            // Populate ref_data with subset of main_data ids and some NULLs
            let num_ref_rows = rng.random_range(min_rows / 2..=max_rows / 2);
            for _ in 0..num_ref_rows {
                let ref_val = if rng.random_bool(0.1) {
                    "NULL".to_string()
                } else {
                    rng.random_range(1..=num_main_rows as i64).to_string()
                };
                let insert_sql = format!("INSERT INTO ref_data VALUES ({ref_val});");
                debug_string.push_str(&insert_sql);
                debug_string.push('\n');
                limbo_exec_rows(limbo_conn, &insert_sql);
                sqlite_exec_rows(sqlite_conn, &insert_sql);
            }
        }

        fn verify_tables_match(
            limbo_conn: &std::sync::Arc<turso_core::Connection>,
            sqlite_conn: &rusqlite::Connection,
            query: &str,
            seed: u64,
            debug_string: &str,
        ) {
            let verify_query = "SELECT * FROM main_data ORDER BY id";
            let limbo_rows = limbo_exec_rows(limbo_conn, verify_query);
            let sqlite_rows = sqlite_exec_rows(sqlite_conn, verify_query);

            if limbo_rows != sqlite_rows {
                panic!(
                    "Results mismatch after query: {query}\n\
                     Limbo main_data: {limbo_rows:?}\n\
                     SQLite main_data: {sqlite_rows:?}\n\
                     Seed: {seed}\n\n\
                     DDL/DML to reproduce:\n{debug_string}"
                );
            }
        }

        populate_tables(
            &limbo_conn,
            &sqlite_conn,
            &mut rng,
            &mut debug_ddl_dml_string,
            MIN_ROWS_PER_TABLE,
            MAX_ROWS_PER_TABLE,
        );

        log::info!("DDL/DML to reproduce manually:\n{debug_ddl_dml_string}");

        for iter_num in 0..NUM_FUZZ_ITERATIONS {
            let query_type = rng.random_range(0..14);
            let query = match query_type {
                0 => {
                    // DELETE with IN subquery
                    let subquery_filter = if rng.random_bool(0.5) {
                        " WHERE ref_id IS NOT NULL"
                    } else {
                        ""
                    };
                    format!(
                        "DELETE FROM main_data WHERE id IN (SELECT ref_id FROM ref_data{subquery_filter});",
                    )
                }
                1 => {
                    // DELETE with NOT IN subquery
                    let subquery_filter = if rng.random_bool(0.5) {
                        " WHERE ref_id IS NOT NULL"
                    } else {
                        ""
                    };
                    format!(
                        "DELETE FROM main_data WHERE id NOT IN (SELECT ref_id FROM ref_data{subquery_filter});",
                    )
                }
                2 => {
                    // UPDATE with IN subquery
                    let new_value = rng.random_range(-1000..1000);
                    let subquery_filter = if rng.random_bool(0.5) {
                        " WHERE ref_id IS NOT NULL"
                    } else {
                        ""
                    };
                    format!(
                        "UPDATE main_data SET value = {new_value} WHERE id IN (SELECT ref_id FROM ref_data{subquery_filter});",
                    )
                }
                3 => {
                    // UPDATE with NOT IN subquery
                    let new_value = rng.random_range(-1000..1000);
                    let subquery_filter = if rng.random_bool(0.5) {
                        " WHERE ref_id IS NOT NULL"
                    } else {
                        ""
                    };
                    format!(
                        "UPDATE main_data SET value = {new_value} WHERE id NOT IN (SELECT ref_id FROM ref_data{subquery_filter});",
                    )
                }
                4 => {
                    // DELETE with IN subquery + additional condition
                    let category = rng.random_range(1..10);
                    format!(
                        "DELETE FROM main_data WHERE category = {category} AND id IN (SELECT ref_id FROM ref_data WHERE ref_id IS NOT NULL);",
                    )
                }
                5 => {
                    // UPDATE with NOT IN subquery + additional condition
                    let new_value = rng.random_range(-1000..1000);
                    let category = rng.random_range(1..10);
                    format!(
                        "UPDATE main_data SET value = {new_value} WHERE category = {category} AND id NOT IN (SELECT ref_id FROM ref_data WHERE ref_id IS NOT NULL);",
                    )
                }
                6 => {
                    // DELETE with EXISTS subquery (correlated)
                    "DELETE FROM main_data WHERE EXISTS (SELECT 1 FROM ref_data WHERE ref_data.ref_id = main_data.id);".into()
                }
                7 => {
                    // DELETE with NOT EXISTS subquery (correlated)
                    "DELETE FROM main_data WHERE NOT EXISTS (SELECT 1 FROM ref_data WHERE ref_data.ref_id = main_data.id);".into()
                }
                8 => {
                    // UPDATE with EXISTS subquery (correlated)
                    let new_value = rng.random_range(-1000..1000);
                    format!(
                        "UPDATE main_data SET value = {new_value} WHERE EXISTS (SELECT 1 FROM ref_data WHERE ref_data.ref_id = main_data.id);",
                    )
                }
                9 => {
                    // UPDATE with NOT EXISTS subquery (correlated)
                    let new_value = rng.random_range(-1000..1000);
                    format!(
                        "UPDATE main_data SET value = {new_value} WHERE NOT EXISTS (SELECT 1 FROM ref_data WHERE ref_data.ref_id = main_data.id);",
                    )
                }
                10 => {
                    // DELETE with scalar comparison subquery (=)
                    let category = rng.random_range(1..10);
                    format!(
                        "DELETE FROM main_data WHERE category = (SELECT {category} FROM (SELECT {category} AS c));",
                    )
                }
                11 => {
                    // DELETE with scalar comparison subquery (>) using aggregate
                    "DELETE FROM main_data WHERE value > (SELECT AVG(value) FROM main_data);".into()
                }
                12 => {
                    // UPDATE with scalar comparison subquery (=)
                    let new_value = rng.random_range(-1000..1000);
                    let category = rng.random_range(1..10);
                    format!(
                        "UPDATE main_data SET value = {new_value} WHERE category = (SELECT {category} FROM (SELECT {category} AS c));",
                    )
                }
                13 => {
                    // UPDATE with scalar comparison subquery (<) using aggregate
                    let new_value = rng.random_range(-1000..1000);
                    format!(
                        "UPDATE main_data SET value = {new_value} WHERE value < (SELECT AVG(value) FROM main_data);",
                    )
                }
                _ => unreachable!(),
            };

            log::info!(
                "Iteration {}/{NUM_FUZZ_ITERATIONS}: Query: {query}",
                iter_num + 1,
            );

            debug_ddl_dml_string.push_str(&query);
            debug_ddl_dml_string.push('\n');

            // Execute on both databases
            limbo_exec_rows(&limbo_conn, &query);
            sqlite_conn.execute(&query, params![]).unwrap();

            // Verify tables match
            verify_tables_match(
                &limbo_conn,
                &sqlite_conn,
                &query,
                seed,
                &debug_ddl_dml_string,
            );

            // Periodically repopulate tables to ensure we have data to work with
            if iter_num % 50 == 49 {
                // Clear and repopulate
                limbo_exec_rows(&limbo_conn, "DELETE FROM main_data;");
                sqlite_conn
                    .execute("DELETE FROM main_data;", params![])
                    .unwrap();
                limbo_exec_rows(&limbo_conn, "DELETE FROM ref_data;");
                sqlite_conn
                    .execute("DELETE FROM ref_data;", params![])
                    .unwrap();

                debug_ddl_dml_string.push_str("DELETE FROM main_data;\n");
                debug_ddl_dml_string.push_str("DELETE FROM ref_data;\n");

                populate_tables(
                    &limbo_conn,
                    &sqlite_conn,
                    &mut rng,
                    &mut debug_ddl_dml_string,
                    MIN_ROWS_PER_TABLE,
                    MAX_ROWS_PER_TABLE,
                );
            }
        }
    }

    /// Fuzz test for UPDATE OR REPLACE/IGNORE statements.
    /// This test generates random UPDATE statements with conflict resolution
    /// clauses and compares results between Limbo and SQLite to ensure correctness.
    #[turso_macros::test(mvcc)]
    pub fn update_or_conflict_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        log::info!("update_or_conflict_fuzz seed: {seed}");

        const NUM_FUZZ_ITERATIONS: usize = 200;
        const ROWS_PER_TABLE: usize = 500;

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let mut debug_ddl_dml_string = String::new();

        // Create table with UNIQUE constraints to trigger conflict resolution
        let schema = "CREATE TABLE t1 (x INTEGER PRIMARY KEY, y INTEGER UNIQUE, z INTEGER NOT NULL DEFAULT 99);";
        debug_ddl_dml_string.push_str(schema);
        debug_ddl_dml_string.push('\n');
        limbo_exec_rows(&limbo_conn, schema);
        sqlite_exec_rows(&sqlite_conn, schema);

        fn populate_table(
            limbo_conn: &std::sync::Arc<turso_core::Connection>,
            sqlite_conn: &rusqlite::Connection,
            rng: &mut ChaCha8Rng,
            debug_string: &mut String,
        ) {
            // Insert rows with unique y values
            for i in 1..=ROWS_PER_TABLE {
                let insert_sql = format!(
                    "INSERT INTO t1 VALUES ({}, {}, {});",
                    i,
                    i * 10, // y values: 10, 20, 30, ...
                    rng.random_range(1..ROWS_PER_TABLE as i64)
                );
                debug_string.push_str(&insert_sql);
                debug_string.push('\n');
                limbo_exec_rows(limbo_conn, &insert_sql);
                sqlite_exec_rows(sqlite_conn, &insert_sql);
            }
        }

        fn verify_tables_match(
            limbo_conn: &std::sync::Arc<turso_core::Connection>,
            sqlite_conn: &rusqlite::Connection,
            query: &str,
            seed: u64,
            debug_string: &str,
        ) {
            let verify_query = "SELECT * FROM t1 ORDER BY x";
            let limbo_rows = limbo_exec_rows(limbo_conn, verify_query);
            let sqlite_rows = sqlite_exec_rows(sqlite_conn, verify_query);

            if limbo_rows != sqlite_rows {
                panic!(
                    "Results mismatch after query: {query}\n\
                     Limbo t1: {limbo_rows:?}\n\
                     SQLite t1: {sqlite_rows:?}\n\
                     Seed: {seed}\n\n\
                     DDL/DML to reproduce:\n{debug_string}"
                );
            }
        }

        populate_table(
            &limbo_conn,
            &sqlite_conn,
            &mut rng,
            &mut debug_ddl_dml_string,
        );

        log::info!("DDL/DML to reproduce manually:\n{debug_ddl_dml_string}");

        for iter_num in 0..NUM_FUZZ_ITERATIONS {
            let query_type = rng.random_range(0..10);
            let query = match query_type {
                0 => {
                    // UPDATE OR IGNORE - set y to existing value (should skip)
                    let existing_y = rng.random_range(1..=20) * 10;
                    let target_x = rng.random_range(1..=20);
                    format!("UPDATE OR IGNORE t1 SET y = {existing_y} WHERE x = {target_x};")
                }
                1 => {
                    // UPDATE OR REPLACE - set y to existing value (should delete conflicting row)
                    let existing_y = rng.random_range(1..=20) * 10;
                    let target_x = rng.random_range(1..=20);
                    format!("UPDATE OR REPLACE t1 SET y = {existing_y} WHERE x = {target_x};")
                }
                2 => {
                    // UPDATE OR IGNORE with expression (y - 10 may cause conflict)
                    let target_x = rng.random_range(2..=20); // avoid x=1 since y-10=0
                    format!("UPDATE OR IGNORE t1 SET y = y - 10 WHERE x = {target_x};")
                }
                3 => {
                    // UPDATE OR REPLACE with expression (y - 10 may cause conflict)
                    let target_x = rng.random_range(2..=20);
                    format!("UPDATE OR REPLACE t1 SET y = y - 10 WHERE x = {target_x};")
                }
                4 => {
                    // UPDATE OR IGNORE - multiple rows (WHERE x > ...)
                    let min_x = rng.random_range(1..=15);
                    let new_y = rng.random_range(-100..100);
                    format!("UPDATE OR IGNORE t1 SET y = {new_y} WHERE x > {min_x};")
                }
                5 => {
                    // UPDATE OR REPLACE with no conflict (new unique value)
                    let target_x = rng.random_range(1..=20);
                    let new_y = rng.random_range(1000..2000); // unlikely to conflict
                    format!("UPDATE OR REPLACE t1 SET y = {new_y} WHERE x = {target_x};")
                }
                6 => {
                    // UPDATE OR IGNORE with NOT NULL violation
                    let target_x = rng.random_range(1..=20);
                    format!("UPDATE OR IGNORE t1 SET z = NULL WHERE x = {target_x};")
                }
                7 => {
                    // UPDATE OR REPLACE with NOT NULL violation (should use default)
                    let target_x = rng.random_range(1..=20);
                    format!("UPDATE OR REPLACE t1 SET z = NULL WHERE x = {target_x};")
                }
                8 => {
                    // Regular UPDATE (no conflict clause) with safe value
                    let target_x = rng.random_range(1..=20);
                    let new_z = rng.random_range(1..500);
                    format!("UPDATE t1 SET z = {new_z} WHERE x = {target_x};")
                }
                9 => {
                    // UPDATE OR REPLACE with multiple rows, expression may cause cascading conflicts
                    let min_x = rng.random_range(5..=15);
                    format!("UPDATE OR REPLACE t1 SET y = y - 10 WHERE x > {min_x};")
                }
                10 => {
                    // UPDATE OR FAIL with existing value
                    let min_x = rng.random_range(1..=15);
                    format!("UPDATE OR FAIL t1 SET y = 10 WHERE x > {min_x};")
                }
                11 => {
                    // UPDATE OR ROLLBACK with existing value (should error)
                    let min_x = rng.random_range(1..=15);
                    format!("UPDATE OR ROLLBACK t1 SET y = 10 WHERE x > {min_x};")
                }
                12 => {
                    // UPDATE OR FAIL with safe value
                    let value = rng.random_range(1000..2000);
                    format!("UPDATE OR FAIL t1 SET y = y + 1 WHERE x <= {value};")
                }
                13 => {
                    // UPDATE OR ROLLBACK with safe value
                    let value = rng.random_range(1000..2000);
                    format!("UPDATE OR ROLLBACK t1 SET y = y + 1 WHERE x <= {value};")
                }
                _ => unreachable!(),
            };

            log::info!(
                "Iteration {}/{NUM_FUZZ_ITERATIONS}: Query: {query}",
                iter_num + 1,
            );

            debug_ddl_dml_string.push_str(&query);
            debug_ddl_dml_string.push('\n');

            // Execute on both databases (ignore errors from constraint violations for ABORT mode)
            let _ = limbo_exec_rows_fallible(&db, &limbo_conn, &query);
            let _ = sqlite_conn.execute(&query, params![]);

            // Verify tables match
            verify_tables_match(
                &limbo_conn,
                &sqlite_conn,
                &query,
                seed,
                &debug_ddl_dml_string,
            );

            // Periodically repopulate table to ensure we have data to work with
            if iter_num % 30 == 29 {
                // Clear and repopulate
                limbo_exec_rows(&limbo_conn, "DELETE FROM t1;");
                sqlite_conn.execute("DELETE FROM t1;", params![]).unwrap();
                debug_ddl_dml_string.push_str("DELETE FROM t1;\n");

                populate_table(
                    &limbo_conn,
                    &sqlite_conn,
                    &mut rng,
                    &mut debug_ddl_dml_string,
                );
            }
        }
    }

    #[derive(Clone, Debug)]
    struct FuzzTestColumn {
        name: String,
        ty: String,
        collation: Option<&'static str>, // BINARY/NOCASE/RTRIM
        inline_unique: bool,
        is_pk_autoinc: bool, // INTEGER PRIMARY KEY AUTOINCREMENT
    }

    #[derive(Clone, Debug)]
    struct FuzzTestTable {
        name: String,
        columns: Vec<FuzzTestColumn>,
        unique_table_constraints: Vec<Vec<usize>>, // indices into columns for table-level UNIQUE(...)
    }

    #[derive(Clone, Debug)]
    struct FuzzTestIndexCol {
        col: usize, // index into table.columns
        collation: Option<&'static str>,
        sort_order: &'static str, // "ASC" or "DESC"
    }

    #[derive(Clone, Debug)]
    struct FuzzTestIndex {
        name: String,
        table: String,
        cols: Vec<FuzzTestIndexCol>,
        unique: bool,
    }

    #[derive(Clone, Debug, Default)]
    struct FuzzTestDbState {
        tables: Vec<FuzzTestTable>,
        indices: Vec<FuzzTestIndex>,
        table_counter: usize,
        index_counter: usize,
    }

    impl FuzzTestDbState {
        fn next_table_name(&mut self) -> String {
            self.table_counter += 1;
            format!("t{}", self.table_counter)
        }
        fn next_index_name(&mut self) -> String {
            self.index_counter += 1;
            format!("i{}", self.index_counter)
        }
    }

    const COLLATIONS: [&str; 3] = ["BINARY", "NOCASE", "RTRIM"];
    const TYPES: [&str; 5] = ["INT", "TEXT", "REAL", "BLOB", "NUMERIC"];

    fn random_collation<R: Rng>(rng: &mut R) -> Option<&'static str> {
        if rng.random_bool(0.65) {
            Some(COLLATIONS[rng.random_range(0..COLLATIONS.len())])
        } else {
            None
        }
    }

    fn random_type<R: Rng>(rng: &mut R) -> &'static str {
        TYPES[rng.random_range(0..TYPES.len())]
    }

    fn quote_ident(s: &str) -> String {
        // use simple quoting with double quotes
        format!("\"{}\"", s.replace('"', "\"\""))
    }

    fn sql_value_string<R: Rng>(rng: &mut R) -> String {
        match rng.random_range(0..9) {
            0 => "NULL".to_string(),
            1 => rng.random_range(-1000..=1000).to_string(),
            2 => {
                let f: f64 = rng.random_range(-10000.0..10000.0);
                // avoid NaN comparisons
                if rng.random_bool(0.1) {
                    "0.0".to_string()
                } else {
                    f.to_string()
                }
            }
            3 => format!("'{}'", " ".repeat(rng.random_range(0..=3))), // spaces test RTRIM
            4 => format!("'{}'", "A".repeat(rng.random_range(0..=3))), // test NOCASE
            5 => format!("'{}'", "a".repeat(rng.random_range(0..=3))),
            6 => format!(
                "X'{:02X}{:02X}{:02X}'",
                rng.random_range(0..=255),
                rng.random_range(0..=255),
                rng.random_range(0..=255)
            ),
            7 => format!("'{}'", rng.random_range(0..=1_000_000_000)),
            _ => format!(
                "'{}'",
                ["foo", "Foo", "FOO", "bar ", " Bar", "baz  "][rng.random_range(0..6)]
                    .replace('\'', "''")
            ),
        }
    }

    fn build_create_table_sql<R: Rng>(rng: &mut R, tname: &str) -> FuzzTestTable {
        // number of columns
        let mut num_cols = rng.random_range(1..=6);
        let mut columns = Vec::<FuzzTestColumn>::new();

        // Sometimes include AUTOINCREMENT pk which also creates sqlite_sequence on first insert
        let include_autoinc = rng.random_bool(0.25);
        if include_autoinc {
            let cname = "id".to_string();
            columns.push(FuzzTestColumn {
                name: cname,
                ty: "INTEGER".to_string(),
                collation: None,
                inline_unique: false,
                is_pk_autoinc: true,
            });
            // ensure at least one more column so selects have options
            num_cols = num_cols.max(2);
        }

        for i in (columns.len())..num_cols {
            let cname = format!("c{}", i + 1);
            let ty = random_type(rng).to_string();
            let coll = if rng.random_bool(0.7) {
                random_collation(rng)
            } else {
                None
            };
            columns.push(FuzzTestColumn {
                name: cname,
                ty,
                collation: coll,
                inline_unique: false,
                is_pk_autoinc: false,
            });
        }

        // Choose number of unique constraints to place (possibly 0)
        // We mix inline and table-level placements. Unique indexes will be created in separate "create index" actions.
        let mut unique_table_constraints: Vec<Vec<usize>> = Vec::new();
        let mut available_cols: Vec<usize> = (0..columns.len()).collect();
        // do not use the autoincrement pk for unique constraints (already unique)
        available_cols.retain(|&i| !columns[i].is_pk_autoinc);

        let uniq_groups = if available_cols.is_empty() {
            0
        } else {
            rng.random_range(0..=3)
        };
        for _ in 0..uniq_groups {
            if available_cols.is_empty() {
                break;
            }
            let width = rng.random_range(1..=available_cols.len().min(3));
            let mut cols_pick = available_cols.clone();
            cols_pick.shuffle(rng);
            cols_pick.truncate(width);

            // randomize ordering of unique constraint columns
            cols_pick.shuffle(rng);

            // Place either inline UNIQUE (only allowed when width == 1) or a table-level UNIQUE group
            if width == 1 && rng.random_bool(0.5) {
                columns[cols_pick[0]].inline_unique = true;
            } else {
                unique_table_constraints.push(cols_pick);
            }

            // Don't remove cols from availability to allow overlapping constraints occasionally
            // That helps fuzz multi-constraint interactions.
        }

        FuzzTestTable {
            name: tname.to_string(),
            columns,
            unique_table_constraints,
        }
    }

    fn create_table_stmt(tbl: &FuzzTestTable) -> String {
        let mut defs: Vec<String> = Vec::new();
        for c in &tbl.columns {
            if c.is_pk_autoinc {
                defs.push(format!(
                    "{} INTEGER PRIMARY KEY AUTOINCREMENT",
                    quote_ident(&c.name)
                ));
                continue;
            }
            let mut part = format!("{} {}", quote_ident(&c.name), c.ty);
            if let Some(coll) = c.collation {
                part.push_str(&format!(" COLLATE {coll}"));
            }
            if c.inline_unique {
                part.push_str(" UNIQUE");
            }
            defs.push(part);
        }
        for grp in &tbl.unique_table_constraints {
            let cols = grp
                .iter()
                .map(|&i| quote_ident(&tbl.columns[i].name))
                .collect::<Vec<_>>()
                .join(", ");
            defs.push(format!("UNIQUE ({cols})"));
        }
        format!(
            "CREATE TABLE {} ({})",
            quote_ident(&tbl.name),
            defs.join(", ")
        )
    }

    fn random_index_for_table<R: Rng>(
        rng: &mut R,
        state: &mut FuzzTestDbState,
        tbl: &FuzzTestTable,
    ) -> FuzzTestIndex {
        // choose number of index columns
        let cols_count = rng.random_range(1..=tbl.columns.len().min(4));
        let mut idx_cols_indices: Vec<usize> = (0..tbl.columns.len()).collect();
        // Avoid including autoincrement PK in composite index too often to generate more variety
        if tbl.columns.iter().any(|c| c.is_pk_autoinc) && rng.random_bool(0.7) {
            idx_cols_indices.retain(|&i| !tbl.columns[i].is_pk_autoinc);
            if idx_cols_indices.is_empty() {
                idx_cols_indices = (0..tbl.columns.len()).collect();
            }
        }
        idx_cols_indices.shuffle(rng);
        idx_cols_indices.truncate(cols_count);
        // randomize order again
        idx_cols_indices.shuffle(rng);

        let mut cols: Vec<FuzzTestIndexCol> = Vec::new();
        for &ci in &idx_cols_indices {
            let coll_over = if rng.random_bool(0.5) {
                random_collation(rng)
            } else {
                None
            };
            let sort = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
            cols.push(FuzzTestIndexCol {
                col: ci,
                collation: coll_over,
                sort_order: sort,
            });
        }

        let idx_name = state.next_index_name();
        FuzzTestIndex {
            name: idx_name,
            table: tbl.name.clone(),
            cols,
            unique: rng.random_bool(0.5),
        }
    }

    fn create_index_stmt(tbl: &FuzzTestTable, idx: &FuzzTestIndex) -> String {
        let mut parts: Vec<String> = Vec::new();
        for ic in &idx.cols {
            let mut piece = quote_ident(&tbl.columns[ic.col].name);
            if let Some(coll) = ic.collation {
                piece.push_str(&format!(" COLLATE {coll}"));
            }
            piece.push_str(&format!(" {}", ic.sort_order));
            parts.push(piece);
        }
        format!(
            "CREATE {} INDEX {} ON {} ({})",
            if idx.unique { "UNIQUE" } else { "" },
            quote_ident(&idx.name),
            quote_ident(&idx.table),
            parts.join(", ")
        )
        .replace("  ", " ")
    }

    fn insert_random_rows_stmt<R: Rng>(
        rng: &mut R,
        tbl: &FuzzTestTable,
        rows: usize,
    ) -> Vec<String> {
        // Insert rows individually so we can ignore uniqueness violations cleanly.
        let mut stmts = Vec::new();
        for _ in 0..rows {
            let mut vals = Vec::new();
            for c in &tbl.columns {
                if c.is_pk_autoinc {
                    // Let autoincrement assign
                    vals.push("NULL".to_string());
                } else {
                    vals.push(sql_value_string(rng));
                }
            }
            let stmt = format!(
                "INSERT INTO {} VALUES ({})",
                quote_ident(&tbl.name),
                vals.join(", ")
            );
            stmts.push(stmt);
        }
        stmts
    }

    fn random_select_stmt<R: Rng>(rng: &mut R, tbl: &FuzzTestTable) -> String {
        // select random subset of columns
        let mut col_indices: Vec<usize> = (0..tbl.columns.len()).collect();
        let select_count = rng.random_range(1..=tbl.columns.len());
        col_indices.shuffle(rng);
        col_indices.truncate(select_count);
        // keep a deterministic order in select list for easier comparison
        col_indices.sort_unstable();

        let select_cols = col_indices
            .iter()
            .map(|&i| quote_ident(&tbl.columns[i].name))
            .collect::<Vec<_>>()
            .join(", ");

        // Small chance of adding simple WHERE
        let where_clause = if rng.random_bool(0.4) {
            // equality or IS NULL on one or two predicates
            let preds = rng.random_range(1..=2);
            let mut chosen = (0..tbl.columns.len()).collect::<Vec<_>>();
            chosen.shuffle(rng);
            let mut parts = Vec::new();
            for &ci in chosen.iter().take(preds) {
                let cn = &tbl.columns[ci].name;
                let kind = rng.random_range(0..4);
                let part = match kind {
                    0 => format!("{} IS NULL", quote_ident(cn)),
                    1 => format!("{} = {}", quote_ident(cn), sql_value_string(rng)),
                    2 => format!("{} <> {}", quote_ident(cn), sql_value_string(rng)),
                    _ => format!("{} IS NOT NULL", quote_ident(cn)),
                };
                parts.push(part);
            }
            format!(" WHERE {}", parts.join(" AND "))
        } else {
            String::new()
        };

        // ORDER BY some columns with optional explicit collate/sort to exercise index usage
        let mut order_cols = col_indices.clone();
        order_cols.shuffle(rng);
        order_cols.truncate(rng.random_range(1..=order_cols.len()));
        let mut order_parts = Vec::new();
        for ci in order_cols {
            let mut piece = quote_ident(&tbl.columns[ci].name);
            if rng.random_bool(0.5) {
                if let Some(coll) = random_collation(rng) {
                    piece.push_str(&format!(" COLLATE {coll}"));
                }
            }
            piece.push_str(if rng.random_bool(0.5) {
                " ASC"
            } else {
                " DESC"
            });
            order_parts.push(piece);
        }
        // Append rowid tiebreaker for determinism, most tables will have rowid
        order_parts.push("rowid ASC".to_string());
        let order_clause = format!(" ORDER BY {}", order_parts.join(", "));

        // Optional LIMIT
        let limit_clause = if rng.random_bool(0.6) {
            format!(" LIMIT {}", rng.random_range(1..=50))
        } else {
            String::new()
        };

        format!(
            "SELECT {} FROM {}{}{}{}",
            select_cols,
            quote_ident(&tbl.name),
            where_clause,
            order_clause,
            limit_clause
        )
    }

    #[allow(dead_code)]
    enum Action {
        CreateTable,
        CreateIndex,
        InsertData,
        Select,
    }

    fn pick_action<R: Rng>(rng: &mut R, db_state: &FuzzTestDbState) -> Action {
        if db_state.tables.is_empty() {
            return Action::CreateTable;
        }
        match rng.random_range(0..100) {
            0..=14 => Action::CreateTable,  // ~15%
            15..=34 => Action::CreateIndex, // ~20%
            // temporary disable this action - because right now we still have bug with affinity for insertion to the indices
            // 35..=55 => Action::InsertData,  // ~20%
            _ => Action::Select, // leftover
        }
    }

    #[test]
    pub fn test_data_layout_compatibility() {
        maybe_setup_tracing();
        const OUTER: usize = 100;
        const INNER: usize = 10;

        let (mut rng, seed) = rng_from_time_or_env();
        let left = NamedTempFile::new().unwrap();
        let right = NamedTempFile::new().unwrap();

        let (_left, left) = left.keep().unwrap();
        let (_right, right) = right.keep().unwrap();
        // let left = left.path();
        // let right = right.path();

        tracing::info!(
            "test_data_layout_compatibility seed: {}, left_path={:?}, right_path={:?}",
            seed,
            left,
            right
        );
        let mut state = FuzzTestDbState::default();

        for i in 0..OUTER {
            tracing::info!(
                "test_data_layout_compatibility: outer iter {}/{}",
                i + 1,
                OUTER
            );
            let (turso_path, sqlite_path) = if i % 2 == 0 {
                (&left, &right)
            } else {
                (&right, &left)
            };
            let turso_db = TempDatabase::builder().with_db_path(turso_path).build();
            let turso_conn = turso_db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open(sqlite_path).unwrap();
            for _ in 0..INNER {
                let action = pick_action(&mut rng, &state);
                match action {
                    Action::CreateTable => {
                        // Create a new randomized table
                        let tname = state.next_table_name();
                        let table = build_create_table_sql(&mut rng, &tname);
                        let query = create_table_stmt(&table);

                        tracing::info!("table: {}", query);
                        let turso_result = turso_conn.execute(&query);
                        let sqlite_result = sqlite_conn.execute(&query, ());
                        assert_eq!(turso_result.is_ok(), sqlite_result.is_ok());

                        if turso_result.is_ok() {
                            let ins_cnt = rng.random_range(0..=30);
                            for ins_stmt in insert_random_rows_stmt(&mut rng, &table, ins_cnt) {
                                tracing::info!("insert: {}", ins_stmt);
                                let turso_result = turso_conn.execute(&ins_stmt);
                                let sqlite_result = sqlite_conn.execute(&ins_stmt, ());
                                assert_eq!(turso_result.is_ok(), sqlite_result.is_ok());
                            }
                        }
                        state.tables.push(table);
                    }
                    Action::CreateIndex => {
                        if state.tables.is_empty() {
                            continue;
                        }
                        let t_idx = rng.random_range(0..state.tables.len());
                        let tbl = state.tables[t_idx].clone();
                        let idx = random_index_for_table(&mut rng, &mut state, &tbl);
                        let query = create_index_stmt(&tbl, &idx);

                        tracing::info!("index: {}", query);
                        let turso_result = turso_conn.execute(&query);
                        let sqlite_result = sqlite_conn.execute(&query, ());
                        assert_eq!(turso_result.is_ok(), sqlite_result.is_ok());
                        state.indices.push(idx);
                    }
                    Action::InsertData => {
                        let t_idx = rng.random_range(0..state.tables.len());
                        let table = state.tables[t_idx].clone();
                        let ins_cnt = rng.random_range(0..=30);
                        for ins_stmt in insert_random_rows_stmt(&mut rng, &table, ins_cnt) {
                            tracing::info!("insert: {}", ins_stmt);
                            let turso_result = turso_conn.execute(&ins_stmt);
                            let sqlite_result = sqlite_conn.execute(&ins_stmt, ());
                            assert_eq!(turso_result.is_ok(), sqlite_result.is_ok());
                        }
                    }
                    Action::Select => {
                        if state.tables.is_empty() {
                            continue;
                        }
                        // pick a random table to select from
                        let t_idx = rng.random_range(0..state.tables.len());
                        let tbl = &state.tables[t_idx];

                        let query = random_select_stmt(&mut rng, tbl);

                        tracing::info!("query: {}", query);
                        let limbo_rows = limbo_exec_rows(&turso_conn, &query);
                        let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);

                        assert_eq!(
                            limbo_rows, sqlite_rows,
                            "Mismatch on query: {query}\nseed: {seed}\nlimbo: {limbo_rows:?}\nsqlite: {sqlite_rows:?}"
                        );
                    }
                }
            }
        }
    }
}
