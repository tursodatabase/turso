#[cfg(test)]
mod raise_tests {
    use std::panic::AssertUnwindSafe;

    use rand::seq::IndexedRandom;
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    use crate::helpers;
    use core_tester::common::{
        limbo_exec_rows, limbo_exec_rows_fallible, sqlite_exec_rows, TempDatabase,
    };

    /// The four RAISE() conflict types.
    #[derive(Debug, Clone, Copy)]
    enum RaiseType {
        Ignore,
        Abort,
        Fail,
        Rollback,
    }

    impl RaiseType {
        fn sql(&self) -> &'static str {
            match self {
                RaiseType::Ignore => "IGNORE",
                RaiseType::Abort => "ABORT",
                RaiseType::Fail => "FAIL",
                RaiseType::Rollback => "ROLLBACK",
            }
        }
    }

    const ALL_RAISE_TYPES: [RaiseType; 4] = [
        RaiseType::Ignore,
        RaiseType::Abort,
        RaiseType::Fail,
        RaiseType::Rollback,
    ];

    #[derive(Debug, Clone, Copy)]
    enum TriggerTiming {
        Before,
        After,
    }

    impl TriggerTiming {
        fn sql(&self) -> &'static str {
            match self {
                TriggerTiming::Before => "BEFORE",
                TriggerTiming::After => "AFTER",
            }
        }
    }

    const ALL_TIMINGS: [TriggerTiming; 2] = [TriggerTiming::Before, TriggerTiming::After];

    #[derive(Debug, Clone, Copy)]
    enum TriggerEvent {
        Insert,
        Update,
        Delete,
    }

    impl TriggerEvent {
        fn sql(&self) -> &'static str {
            match self {
                TriggerEvent::Insert => "INSERT",
                TriggerEvent::Update => "UPDATE",
                TriggerEvent::Delete => "DELETE",
            }
        }
    }

    const ALL_EVENTS: [TriggerEvent; 3] = [
        TriggerEvent::Insert,
        TriggerEvent::Update,
        TriggerEvent::Delete,
    ];

    fn random_raise_type(rng: &mut ChaCha8Rng) -> RaiseType {
        *ALL_RAISE_TYPES.choose(rng).unwrap()
    }

    fn random_timing(rng: &mut ChaCha8Rng) -> TriggerTiming {
        *ALL_TIMINGS.choose(rng).unwrap()
    }

    fn random_event(rng: &mut ChaCha8Rng) -> TriggerEvent {
        *ALL_EVENTS.choose(rng).unwrap()
    }

    /// Build a WHEN clause that fires on a specific value.
    fn when_clause(event: TriggerEvent, threshold: i64) -> String {
        match event {
            TriggerEvent::Insert => format!("WHEN NEW.x = {threshold}"),
            TriggerEvent::Update => format!("WHEN NEW.x = {threshold}"),
            TriggerEvent::Delete => format!("WHEN OLD.x = {threshold}"),
        }
    }

    /// Create a trigger that does some side-effect work before RAISEing.
    fn create_trigger_sql(
        trigger_name: &str,
        timing: TriggerTiming,
        event: TriggerEvent,
        table: &str,
        raise_type: RaiseType,
        when_threshold: Option<i64>,
        with_side_effects: bool,
    ) -> String {
        let raise_call = match raise_type {
            RaiseType::Ignore => "SELECT RAISE(IGNORE)".to_string(),
            rt => format!(
                "SELECT RAISE({}, 'raise_{}')",
                rt.sql(),
                rt.sql().to_lowercase()
            ),
        };

        let when = when_threshold
            .map(|t| format!(" {}", when_clause(event, t)))
            .unwrap_or_default();

        let side_effect = if with_side_effects {
            format!("INSERT INTO log VALUES ('{trigger_name} fired'); ")
        } else {
            String::new()
        };

        format!(
            "CREATE TRIGGER {trigger_name} {} {} ON {table}{when} BEGIN {side_effect}{raise_call}; END",
            timing.sql(),
            event.sql(),
        )
    }

    /// Main differential fuzz test for RAISE() in triggers.
    ///
    /// Strategy:
    /// 1. Create tables and seed data on both Turso and SQLite.
    /// 2. Randomly create triggers with various RAISE types, timings, events, and WHEN clauses.
    /// 3. Execute random DML statements that may fire those triggers.
    /// 4. After each DML, compare table state between engines.
    /// 5. Periodically drop and recreate triggers to vary coverage.
    #[turso_macros::test(mvcc)]
    pub fn raise_differential_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("raise_differential_fuzz");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        // Schema: main table + log table for trigger side-effects
        for schema in [
            "CREATE TABLE t (x INTEGER, y INTEGER)",
            "CREATE TABLE log (msg TEXT)",
        ] {
            helpers::execute_on_both(&limbo_conn, &sqlite_conn, schema, "");
        }

        // Seed initial data
        for x in 1..=5 {
            let stmt = format!("INSERT INTO t VALUES ({x}, {})", x * 10);
            helpers::execute_on_both(&limbo_conn, &sqlite_conn, &stmt, "");
        }
        let verify_queries = [
            ("t", "SELECT x, y FROM t ORDER BY x, y"),
            ("log", "SELECT msg FROM log ORDER BY msg"),
        ];
        const STEPS: usize = 500;
        let mut history = Vec::with_capacity(STEPS + 64);
        let mut trigger_counter = 0u32;
        let mut active_triggers: Vec<String> = Vec::new();
        for step in 0..STEPS {
            helpers::log_progress("raise_differential_fuzz", step, STEPS, 8);
            let action = rng.random_range(0..100);
            let stmt = if action < 10 && !active_triggers.is_empty() {
                // Drop a random trigger (10% of the time if triggers exist)
                let idx = rng.random_range(0..active_triggers.len());
                let name = active_triggers.remove(idx);
                format!("DROP TRIGGER {name}")
            } else if action < 35 {
                // Create a new trigger (25% of the time)
                let timing = random_timing(&mut rng);
                let event = random_event(&mut rng);
                let raise_type = random_raise_type(&mut rng);
                let with_when = rng.random_bool(0.6);
                let when_threshold = if with_when {
                    Some(rng.random_range(1..=10))
                } else {
                    None
                };
                let with_side_effects = rng.random_bool(0.5);

                trigger_counter += 1;
                let name = format!("tr_{trigger_counter}");
                let sql = create_trigger_sql(
                    &name,
                    timing,
                    event,
                    "t",
                    raise_type,
                    when_threshold,
                    with_side_effects,
                );
                active_triggers.push(name);
                sql
            } else if action < 40 {
                // Clear log table (5% of the time)
                "DELETE FROM log".to_string()
            } else {
                // DML on the main table (60% of the time)
                helpers::random_dml(&mut rng, "t", "x", "y")
            };

            history.push(stmt.clone());

            let sqlite_res = sqlite_conn.execute(&stmt, params![]);
            let limbo_res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                limbo_exec_rows_fallible(&db, &limbo_conn, &stmt)
            }))
            .unwrap();
            helpers::assert_outcome_parity(
                &sqlite_res,
                &limbo_res,
                &stmt,
                &format!("SEED=({seed})"),
            );
            // After each step, verify both engines have identical table state
            for (label, verify_query) in verify_queries {
                helpers::assert_differential(&limbo_conn, &sqlite_conn, verify_query, label);
            }
        }
    }

    /// Targeted test: RAISE(FAIL) preserves prior rows in multi-row operations.
    /// Runs on both engines and compares state.
    #[turso_macros::test(mvcc)]
    pub fn raise_fail_prior_rows_parity(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("raise_fail_prior_rows_parity");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ROUNDS: usize = 100;
        for round in 0..ROUNDS {
            helpers::log_progress("raise_fail_prior_rows_parity", round, ROUNDS, 5);

            // Fresh tables each round
            for stmt in [
                "DROP TABLE IF EXISTS t",
                "DROP TABLE IF EXISTS log",
                "DROP TRIGGER IF EXISTS fail_tr",
                "CREATE TABLE t (x INTEGER, y INTEGER)",
                "CREATE TABLE log (msg TEXT)",
            ] {
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, stmt, "");
            }

            // Seed some rows
            let seed_count = rng.random_range(3..=8);
            for i in 1..=seed_count {
                let stmt = format!("INSERT INTO t VALUES ({i}, {})", i * 10);
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, &stmt, "");
            }
            // Pick a random row to trigger FAIL on
            let fail_target = rng.random_range(1..=seed_count);
            let timing = random_timing(&mut rng);
            let event = random_event(&mut rng);
            let with_side_effects = rng.random_bool(0.5);
            let trigger_sql = create_trigger_sql(
                "fail_tr",
                timing,
                event,
                "t",
                RaiseType::Fail,
                Some(fail_target),
                with_side_effects,
            );
            helpers::execute_on_both(&limbo_conn, &sqlite_conn, &trigger_sql, "");
            // Wrap in explicit transaction so FAIL doesn't autocommit partial changes
            // (both engines should behave identically either way)
            let use_txn = rng.random_bool(0.5);
            if use_txn {
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, "BEGIN", "");
            }

            // Generate a DML that touches multiple rows to exercise FAIL
            let dml = match event {
                TriggerEvent::Insert => {
                    let values: Vec<String> = (1..=seed_count)
                        .map(|i| format!("({i}, {})", rng.random_range(1..=100)))
                        .collect();
                    format!("INSERT INTO t VALUES {}", values.join(", "))
                }
                TriggerEvent::Update => {
                    format!("UPDATE t SET y = {}", rng.random_range(1..=100))
                }
                TriggerEvent::Delete => "DELETE FROM t".to_string(),
            };

            let sqlite_res = sqlite_conn.execute(&dml, params![]);
            let limbo_res = limbo_exec_rows_fallible(&db, &limbo_conn, &dml);
            helpers::assert_outcome_parity(
                &sqlite_res,
                &limbo_res,
                &dml,
                &format!("RAISE(FAIL) parity in round {round} (SEED=({seed}))"),
            );
            // State parity
            for (label, query) in [
                ("t", "SELECT x, y FROM t ORDER BY x, y"),
                ("log", "SELECT msg FROM log ORDER BY msg"),
            ] {
                helpers::assert_differential(&limbo_conn, &sqlite_conn, query, label);
            }
            if use_txn {
                // Rollback to clean up for next round
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, "ROLLBACK", "");
            }
        }
    }

    /// Targeted test: trigger body side-effects are rolled back on any RAISE.
    #[turso_macros::test(mvcc)]
    pub fn raise_trigger_body_rollback_parity(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("raise_trigger_body_rollback_parity");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ROUNDS: usize = 100;
        for round in 0..ROUNDS {
            helpers::log_progress("raise_trigger_body_rollback_parity", round, ROUNDS, 5);

            // Fresh tables each round
            for stmt in [
                "DROP TABLE IF EXISTS t",
                "DROP TABLE IF EXISTS log",
                "DROP TRIGGER IF EXISTS body_tr",
                "CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)",
                "CREATE TABLE log (msg TEXT)",
            ] {
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, stmt, "");
            }

            // Seed
            for i in 1..=3 {
                let stmt = format!("INSERT INTO t VALUES ({i}, {})", i * 10);
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, &stmt, "");
            }
            let raise_type = random_raise_type(&mut rng);
            let timing = random_timing(&mut rng);
            // Trigger that inserts into log, then RAISEs — log should be rolled back
            let trigger_sql = create_trigger_sql(
                "body_tr",
                timing,
                TriggerEvent::Insert,
                "t",
                raise_type,
                None, // always fires
                true, // with side effects
            );
            limbo_conn.execute(&trigger_sql).unwrap();
            sqlite_conn.execute(&trigger_sql, params![]).unwrap();

            // Use explicit transaction for ROLLBACK type to avoid full rollback
            let use_txn = matches!(raise_type, RaiseType::Rollback) || rng.random_bool(0.5);
            if use_txn {
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, "BEGIN", "");
            }

            let dml = format!(
                "INSERT INTO t VALUES ({}, {})",
                rng.random_range(10..=20),
                rng.random_range(1..=100)
            );

            let sqlite_res = sqlite_conn.execute(&dml, params![]);
            let limbo_res = limbo_exec_rows_fallible(&db, &limbo_conn, &dml);
            helpers::assert_outcome_parity(
                &sqlite_res,
                &limbo_res,
                &dml,
                &format!(
                    "RAISE({}) trigger body rollback parity in round {round} (SEED=({seed}))",
                    raise_type.sql()
                ),
            );

            // Verify log table: trigger body side-effects should be rolled back
            let sqlite_log = sqlite_exec_rows(&sqlite_conn, "SELECT count(*) FROM log");
            let limbo_log = limbo_exec_rows(&limbo_conn, "SELECT count(*) FROM log");
            assert_eq!(
                limbo_log, sqlite_log,
                "RAISE({}) trigger body rollback mismatch in round {round}\nseed: {seed}\ndml: {dml}\ntrigger: {trigger_sql}",
                raise_type.sql()
            );

            // Verify main table state
            let sqlite_t = sqlite_exec_rows(&sqlite_conn, "SELECT x, y FROM t ORDER BY x, y");
            let limbo_t = limbo_exec_rows(&limbo_conn, "SELECT x, y FROM t ORDER BY x, y");
            assert_eq!(
                limbo_t, sqlite_t,
                "RAISE({}) main table mismatch in round {round}\nseed: {seed}\ndml: {dml}\ntrigger: {trigger_sql}",
                raise_type.sql()
            );
            if use_txn {
                let _ = sqlite_conn.execute("ROLLBACK", params![]);
                let _ = limbo_conn.execute("ROLLBACK");
            }
        }
    }
}
