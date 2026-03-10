#[cfg(test)]
mod subjournal_tests {
    use std::panic::AssertUnwindSafe;

    use rand::seq::IndexedRandom;
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    use crate::helpers::{self, history_tail};
    use core_tester::common::{
        limbo_exec_rows, limbo_exec_rows_fallible, sqlite_exec_rows, TempDatabase,
    };

    const SAVEPOINT_NAMES: [&str; 6] = ["sp0", "sp1", "outer", "inner", "alpha", "beta"];
    const TAG_POOL: [&str; 6] = ["a", "b", "c", "d", "foo", "bar"];

    fn random_savepoint_name(rng: &mut ChaCha8Rng) -> &'static str {
        SAVEPOINT_NAMES.choose(rng).unwrap()
    }

    fn random_tag(rng: &mut ChaCha8Rng) -> &'static str {
        TAG_POOL.choose(rng).unwrap()
    }

    fn random_nullable_int(rng: &mut ChaCha8Rng, range: std::ops::RangeInclusive<i64>) -> String {
        helpers::random_nullable_int(rng, range, 0.15)
    }

    // ── DML generators ──────────────────────────────────────────────────
    //
    // Each generator targets specific stmt_journal.rs toggle paths:
    //
    //   Table "t":       NOT NULL, UNIQUE(val), CHECK ─ has constraints + FK parent
    //   Table "t_auto":  AUTOINCREMENT ─ multi_write via sqlite_sequence
    //   Table "t_bare":  NO constraints, NO FKs ─ exercises may_abort=false, multi_write=false
    //   Table "t_trig":  has BEFORE INSERT/UPDATE/DELETE triggers with RAISE(ABORT)
    //   Table "t_pidx":  has partial unique index ─ UPDATE multi_write toggle
    //   Table "child":   deferred FK referencing t ─ FK-based may_abort

    /// INSERT into main constrained table "t".
    /// Covers: multi-row INSERT, REPLACE, IGNORE, ABORT, UPSERT (ON CONFLICT DO UPDATE).
    fn random_insert_t(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..9) {
            // Single-row INSERT (multi_write=false if no conflict/autoincrement)
            0 => {
                let id = rng.random_range(1..=100);
                let val = random_nullable_int(rng, 1..=50);
                let tag = random_tag(rng);
                format!("INSERT INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}')")
            }
            // Multi-row INSERT (multi_write=true)
            1 => {
                let count = rng.random_range(2..=6);
                let rows: Vec<String> = (0..count)
                    .map(|_| {
                        let id = rng.random_range(1..=100);
                        let val = random_nullable_int(rng, 1..=50);
                        let tag = random_tag(rng);
                        format!("({id}, {val}, '{tag}')")
                    })
                    .collect();
                format!("INSERT INTO t(id, val, tag) VALUES {}", rows.join(", "))
            }
            // INSERT OR REPLACE (multi_write=true via is_replace; may_abort=true via notnull+check)
            2 => {
                let id = rng.random_range(1..=100);
                let val = random_nullable_int(rng, 1..=50);
                let tag = random_tag(rng);
                format!("INSERT OR REPLACE INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}')")
            }
            // INSERT OR IGNORE (may_abort=false since ignore resolution, unless FKs)
            3 => {
                let id = rng.random_range(1..=100);
                let val = random_nullable_int(rng, 1..=50);
                let tag = random_tag(rng);
                format!("INSERT OR IGNORE INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}')")
            }
            // INSERT OR ABORT (has_abort_resolution=true + constraints → may_abort=true)
            4 => {
                let id = rng.random_range(1..=100);
                let val = random_nullable_int(rng, 1..=50);
                let tag = random_tag(rng);
                format!("INSERT OR ABORT INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}')")
            }
            // Multi-row INSERT with conflict clause
            5 => {
                let count = rng.random_range(2..=4);
                let rows: Vec<String> = (0..count)
                    .map(|_| {
                        let id = rng.random_range(1..=100);
                        let val = random_nullable_int(rng, 1..=50);
                        let tag = random_tag(rng);
                        format!("({id}, {val}, '{tag}')")
                    })
                    .collect();
                let conflict = match rng.random_range(0..3) {
                    0 => "OR REPLACE",
                    1 => "OR IGNORE",
                    2 => "OR ABORT",
                    _ => unreachable!(),
                };
                format!(
                    "INSERT {conflict} INTO t(id, val, tag) VALUES {}",
                    rows.join(", ")
                )
            }
            // UPSERT: ON CONFLICT DO UPDATE (has_upsert=true → multi_write=true)
            6 => {
                let id = rng.random_range(1..=100);
                let val = rng.random_range(1..=50);
                let tag = random_tag(rng);
                format!(
                    "INSERT INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}') \
                     ON CONFLICT(val) DO UPDATE SET tag = excluded.tag"
                )
            }
            // UPSERT with DO NOTHING
            7 => {
                let id = rng.random_range(1..=100);
                let val = rng.random_range(1..=50);
                let tag = random_tag(rng);
                format!(
                    "INSERT INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}') \
                     ON CONFLICT(id) DO NOTHING"
                )
            }
            // UPSERT targeting UNIQUE(val) with multi-column update
            8 => {
                let id = rng.random_range(1..=100);
                let val = rng.random_range(1..=50);
                let tag = random_tag(rng);
                format!(
                    "INSERT INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}') \
                     ON CONFLICT(id) DO UPDATE SET val = excluded.val, tag = excluded.tag"
                )
            }
            _ => unreachable!(),
        }
    }

    /// INSERT into auto-increment table.
    /// Covers: multi_write=true because sqlite_sequence is updated before constraint checks.
    fn random_insert_auto(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..3) {
            // Auto-generated rowid (multi_write=true via autoincrement)
            0 => {
                let val = random_nullable_int(rng, 1..=50);
                let tag = random_tag(rng);
                format!("INSERT INTO t_auto(val, tag) VALUES ({val}, '{tag}')")
            }
            // Explicit id with REPLACE (multi_write=true via autoincrement AND replace)
            1 => {
                let id = rng.random_range(1..=100);
                let val = random_nullable_int(rng, 1..=50);
                let tag = random_tag(rng);
                format!("INSERT OR REPLACE INTO t_auto(id, val, tag) VALUES ({id}, {val}, '{tag}')")
            }
            // Multi-row auto-increment
            2 => {
                let count = rng.random_range(2..=4);
                let rows: Vec<String> = (0..count)
                    .map(|_| {
                        let val = random_nullable_int(rng, 1..=50);
                        let tag = random_tag(rng);
                        format!("({val}, '{tag}')")
                    })
                    .collect();
                format!("INSERT INTO t_auto(val, tag) VALUES {}", rows.join(", "))
            }
            _ => unreachable!(),
        }
    }

    /// DML on bare table (no constraints, no FKs).
    /// Covers: multi_write=false for single-row ops, may_abort=false for all ops.
    /// This is the path where subjournaling is DISABLED — critical to verify
    /// that disabling it doesn't cause corruption.
    fn random_dml_bare(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..8) {
            // Single-row INSERT (multi_write=false, may_abort=false → NO subjournal)
            0 => {
                let id = rng.random_range(1..=200);
                let x = rng.random_range(1..=100);
                let y = random_tag(rng);
                format!("INSERT INTO t_bare(id, x, y) VALUES ({id}, {x}, '{y}')")
            }
            // Multi-row INSERT (multi_write=true, may_abort=false → NO subjournal)
            1 => {
                let count = rng.random_range(2..=5);
                let rows: Vec<String> = (0..count)
                    .map(|_| {
                        let id = rng.random_range(1..=200);
                        let x = rng.random_range(1..=100);
                        let y = random_tag(rng);
                        format!("({id}, {x}, '{y}')")
                    })
                    .collect();
                format!("INSERT OR IGNORE INTO t_bare(id, x, y) VALUES {}", rows.join(", "))
            }
            // INSERT OR REPLACE on bare table (multi_write=true via replace, may_abort=false
            // since no NOT NULL/CHECK — replace_can_abort is false)
            2 => {
                let id = rng.random_range(1..=200);
                let x = rng.random_range(1..=100);
                let y = random_tag(rng);
                format!("INSERT OR REPLACE INTO t_bare(id, x, y) VALUES ({id}, {x}, '{y}')")
            }
            // Single-row UPDATE by PK (multi_write=false, may_abort=false → NO subjournal)
            3 => {
                let id = rng.random_range(1..=200);
                let x = rng.random_range(1..=100);
                format!("UPDATE t_bare SET x = {x} WHERE id = {id}")
            }
            // Multi-row UPDATE (multi_write=true, may_abort=false → NO subjournal)
            4 => {
                let x = rng.random_range(1..=100);
                format!("UPDATE t_bare SET x = {x} WHERE id > {}", rng.random_range(1..=100))
            }
            // Single-row DELETE by PK (multi_write=false, may_abort=false → NO subjournal)
            5 => {
                format!("DELETE FROM t_bare WHERE id = {}", rng.random_range(1..=200))
            }
            // Multi-row DELETE (multi_write=true, may_abort=false → NO subjournal)
            6 => {
                format!("DELETE FROM t_bare WHERE x < {}", rng.random_range(1..=100))
            }
            // DELETE all
            7 => "DELETE FROM t_bare".to_string(),
            _ => unreachable!(),
        }
    }

    /// DML on trigger table.
    /// Covers: has_triggers=true → multi_write=true AND may_abort=true for all DML.
    /// The trigger fires RAISE(ABORT) probabilistically to exercise partial rollback.
    fn random_dml_trig(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..6) {
            // Single-row INSERT (multi_write=true due to trigger, may_abort=true)
            0 => {
                let id = rng.random_range(1..=100);
                let val = rng.random_range(1..=50);
                let tag = random_tag(rng);
                format!("INSERT INTO t_trig(id, val, tag) VALUES ({id}, {val}, '{tag}')")
            }
            // INSERT OR IGNORE (trigger still forces multi_write+may_abort)
            1 => {
                let id = rng.random_range(1..=100);
                let val = rng.random_range(1..=50);
                let tag = random_tag(rng);
                format!("INSERT OR IGNORE INTO t_trig(id, val, tag) VALUES ({id}, {val}, '{tag}')")
            }
            // Multi-row INSERT
            2 => {
                let count = rng.random_range(2..=4);
                let rows: Vec<String> = (0..count)
                    .map(|_| {
                        let id = rng.random_range(1..=100);
                        let val = rng.random_range(1..=50);
                        let tag = random_tag(rng);
                        format!("({id}, {val}, '{tag}')")
                    })
                    .collect();
                format!("INSERT OR IGNORE INTO t_trig(id, val, tag) VALUES {}", rows.join(", "))
            }
            // UPDATE (multi_write=true due to trigger, may_abort=true)
            3 => {
                let new_val = rng.random_range(1..=50);
                let target = rng.random_range(1..=100);
                format!("UPDATE t_trig SET val = {new_val} WHERE id = {target}")
            }
            // UPDATE multiple rows
            4 => {
                let new_val = rng.random_range(1..=50);
                format!("UPDATE t_trig SET val = {new_val} WHERE id > {}", rng.random_range(1..=50))
            }
            // DELETE (multi_write=true due to trigger, may_abort=true)
            5 => {
                if rng.random_bool(0.2) {
                    "DELETE FROM t_trig".to_string()
                } else {
                    format!("DELETE FROM t_trig WHERE id = {}", rng.random_range(1..=100))
                }
            }
            _ => unreachable!(),
        }
    }

    /// DML on partial-unique-index table.
    /// Covers: UPDATE has_partial_unique=true → multi_write stays true even for single-row.
    fn random_dml_pidx(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..5) {
            // INSERT
            0 => {
                let id = rng.random_range(1..=150);
                let x = rng.random_range(1..=50);
                let y = random_tag(rng);
                format!("INSERT OR IGNORE INTO t_pidx(id, x, y) VALUES ({id}, {x}, '{y}')")
            }
            // INSERT OR REPLACE
            1 => {
                let id = rng.random_range(1..=150);
                let x = rng.random_range(1..=50);
                let y = random_tag(rng);
                format!("INSERT OR REPLACE INTO t_pidx(id, x, y) VALUES ({id}, {x}, '{y}')")
            }
            // Single-row UPDATE by PK (multi_write=true due to partial unique index)
            2 => {
                let id = rng.random_range(1..=150);
                let x = rng.random_range(1..=50);
                format!("UPDATE t_pidx SET x = {x} WHERE id = {id}")
            }
            // UPDATE OR REPLACE (multi_write=true via replace + partial unique)
            3 => {
                let id = rng.random_range(1..=150);
                let x = rng.random_range(1..=50);
                format!("UPDATE OR REPLACE t_pidx SET x = {x} WHERE id = {id}")
            }
            // DELETE
            4 => {
                format!("DELETE FROM t_pidx WHERE id = {}", rng.random_range(1..=150))
            }
            _ => unreachable!(),
        }
    }

    /// UPDATE on main table "t".
    fn random_update_t(rng: &mut ChaCha8Rng) -> String {
        let where_clause = match rng.random_range(0..5) {
            0 => format!("WHERE id = {}", rng.random_range(1..=100)),
            1 => format!("WHERE val = {}", rng.random_range(1..=50)),
            2 => format!("WHERE tag = '{}'", random_tag(rng)),
            3 => "WHERE (id % 3) = 0".to_string(),
            4 => format!(
                "WHERE id BETWEEN {} AND {}",
                rng.random_range(1..=50),
                rng.random_range(50..=100)
            ),
            _ => unreachable!(),
        };

        match rng.random_range(0..6) {
            0 => {
                let new_val = random_nullable_int(rng, 1..=50);
                format!("UPDATE t SET val = {new_val} {where_clause}")
            }
            1 => {
                let new_val = random_nullable_int(rng, 1..=50);
                let new_tag = random_tag(rng);
                format!("UPDATE t SET val = {new_val}, tag = '{new_tag}' {where_clause}")
            }
            // UPDATE OR REPLACE (multi_write=true via is_replace)
            2 => {
                let new_val = random_nullable_int(rng, 1..=50);
                format!("UPDATE OR REPLACE t SET val = {new_val} {where_clause}")
            }
            3 => {
                let new_val = random_nullable_int(rng, 1..=50);
                format!("UPDATE OR IGNORE t SET val = {new_val} {where_clause}")
            }
            4 => {
                let new_val = random_nullable_int(rng, 1..=50);
                format!("UPDATE OR ABORT t SET val = {new_val} {where_clause}")
            }
            // UPDATE all rows (multi_write=true)
            5 => {
                let new_val = random_nullable_int(rng, 1..=50);
                format!("UPDATE t SET val = {new_val}")
            }
            _ => unreachable!(),
        }
    }

    /// DELETE from main table "t".
    fn random_delete_t(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..5) {
            0 => format!("DELETE FROM t WHERE id = {}", rng.random_range(1..=100)),
            1 => format!("DELETE FROM t WHERE val = {}", rng.random_range(1..=50)),
            2 => format!("DELETE FROM t WHERE tag = '{}'", random_tag(rng)),
            3 => {
                let lo = rng.random_range(1..=50);
                let hi = rng.random_range(50..=100);
                format!("DELETE FROM t WHERE id BETWEEN {lo} AND {hi}")
            }
            4 => "DELETE FROM t".to_string(),
            _ => unreachable!(),
        }
    }

    /// DML targeting the FK child table.
    fn random_fk_dml(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..6) {
            0 => {
                let id = rng.random_range(1..=200);
                let pid = match rng.random_range(0..10) {
                    0..=1 => "NULL".to_string(),
                    2..=7 => rng.random_range(1..=100).to_string(),
                    8..=9 => rng.random_range(101..=200).to_string(),
                    _ => unreachable!(),
                };
                let note = random_tag(rng);
                format!("INSERT INTO child(id, pid, note) VALUES ({id}, {pid}, '{note}')")
            }
            1 => {
                let id = rng.random_range(1..=200);
                let pid = rng.random_range(1..=100).to_string();
                let note = random_tag(rng);
                format!(
                    "INSERT OR REPLACE INTO child(id, pid, note) VALUES ({id}, {pid}, '{note}')"
                )
            }
            2 => {
                let id = rng.random_range(1..=200);
                let pid = rng.random_range(1..=150).to_string();
                format!("UPDATE child SET pid = {pid} WHERE id = {id}")
            }
            3 => {
                format!("DELETE FROM child WHERE id = {}", rng.random_range(1..=200))
            }
            4 => {
                format!("DELETE FROM t WHERE id = {}", rng.random_range(1..=100))
            }
            5 => {
                let old_id = rng.random_range(1..=100);
                let new_id = rng.random_range(1..=100);
                format!("UPDATE t SET id = {new_id} WHERE id = {old_id}")
            }
            _ => unreachable!(),
        }
    }

    /// Differential fuzz test for subjournal correctness.
    ///
    /// Exercises all stmt_journal.rs toggle paths across 6 tables that each
    /// target different combinations of multi_write/may_abort:
    ///
    /// | Table    | multi_write toggles                | may_abort toggles               |
    /// |----------|------------------------------------|---------------------------------|
    /// | t        | multi-row, REPLACE, UPSERT, FKs   | NOT NULL, UNIQUE, CHECK, FKs    |
    /// | t_auto   | AUTOINCREMENT                      | NOT NULL (from val col)          |
    /// | t_bare   | none (exercises =false path)       | none (exercises =false path)     |
    /// | t_trig   | triggers                           | RAISE(ABORT) in triggers         |
    /// | t_pidx   | partial unique index               | UNIQUE (from partial index)      |
    /// | child    | FKs                                | deferred FK violations           |
    ///
    /// `PRAGMA cache_size=50` forces page spilling to exercise subjournal I/O.
    #[turso_macros::test]
    pub fn subjournal_differential_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("subjournal_differential_fuzz");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open(db.path.with_extension("sqlite")).unwrap();

        limbo_conn.execute("PRAGMA cache_size = 50").unwrap();
        sqlite_conn
            .execute("PRAGMA cache_size = 50", params![])
            .unwrap();

        limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite_conn
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();

        let schemas = [
            // Main constrained table: NOT NULL + UNIQUE + CHECK + is FK parent
            "CREATE TABLE t (
                id INTEGER PRIMARY KEY,
                val INTEGER NOT NULL,
                tag TEXT,
                UNIQUE(val),
                CHECK(val > 0 OR val IS NULL)
            )",
            // AUTOINCREMENT: multi_write=true via sqlite_sequence
            "CREATE TABLE t_auto (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                val INTEGER NOT NULL,
                tag TEXT
            )",
            // Bare table: NO constraints, NO FKs → multi_write=false, may_abort=false
            "CREATE TABLE t_bare (
                id INTEGER PRIMARY KEY,
                x INTEGER,
                y TEXT
            )",
            // Trigger table: triggers force multi_write=true, may_abort=true
            "CREATE TABLE t_trig (
                id INTEGER PRIMARY KEY,
                val INTEGER,
                tag TEXT
            )",
            // Partial unique index table
            "CREATE TABLE t_pidx (
                id INTEGER PRIMARY KEY,
                x INTEGER,
                y TEXT
            )",
            // FK child: deferred FK → may_abort=true via has_fks
            "CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                pid INTEGER,
                note TEXT,
                FOREIGN KEY(pid) REFERENCES t(id) DEFERRABLE INITIALLY DEFERRED
            )",
        ];

        for schema in schemas {
            limbo_conn.execute(schema).unwrap();
            sqlite_conn.execute(schema, params![]).unwrap();
        }

        // Triggers that conditionally RAISE(ABORT) — exercises has_triggers path.
        // The val % 7 == 0 condition means ~14% of trigger-table DML will abort.
        let triggers = [
            "CREATE TRIGGER tr_ins BEFORE INSERT ON t_trig
             BEGIN
               SELECT RAISE(ABORT, 'trigger abort insert') WHERE NEW.val % 7 = 0;
             END",
            "CREATE TRIGGER tr_upd BEFORE UPDATE ON t_trig
             BEGIN
               SELECT RAISE(ABORT, 'trigger abort update') WHERE NEW.val % 7 = 0;
             END",
            "CREATE TRIGGER tr_del BEFORE DELETE ON t_trig
             BEGIN
               SELECT RAISE(ABORT, 'trigger abort delete') WHERE OLD.val % 11 = 0;
             END",
        ];

        for trigger in triggers {
            limbo_conn.execute(trigger).unwrap();
            sqlite_conn.execute(trigger, params![]).unwrap();
        }

        // Partial unique index: WHERE x > 10 means only rows with x>10 are subject
        // to uniqueness, so UPDATE has has_partial_unique=true → multi_write stays true.
        let pidx = "CREATE UNIQUE INDEX idx_pidx_partial ON t_pidx(x) WHERE x > 10";
        limbo_conn.execute(pidx).unwrap();
        sqlite_conn.execute(pidx, params![]).unwrap();

        // ── Seed tables ─────────────────────────────────────────────────

        for id in 1..=200 {
            let seed_stmt = format!(
                "INSERT INTO t(id, val, tag) VALUES ({id}, {}, '{}')",
                id * 10,
                TAG_POOL[id as usize % TAG_POOL.len()]
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        for id in 1..=500 {
            let seed_stmt = format!(
                "INSERT INTO t_auto(val, tag) VALUES ({}, '{}')",
                id * 10,
                TAG_POOL[id as usize % TAG_POOL.len()]
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        for id in 1..=200 {
            let seed_stmt = format!(
                "INSERT INTO t_bare(id, x, y) VALUES ({id}, {}, '{}')",
                id * 5,
                TAG_POOL[id as usize % TAG_POOL.len()]
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        // Seed trigger table — use values that won't trigger RAISE(ABORT).
        // Trigger fires when val % 7 == 0, so ensure no seed val is divisible by 7.
        for id in 1..=100 {
            let mut val = id * 10 + 1;
            if val % 7 == 0 {
                val += 1;
            }
            let seed_stmt = format!(
                "INSERT INTO t_trig(id, val, tag) VALUES ({id}, {val}, '{}')",
                TAG_POOL[id as usize % TAG_POOL.len()]
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        for id in 1..=150 {
            let seed_stmt = format!(
                "INSERT INTO t_pidx(id, x, y) VALUES ({id}, {}, '{}')",
                id * 3,
                TAG_POOL[id as usize % TAG_POOL.len()]
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        for id in 1..=800 {
            let pid = if id % 5 == 0 {
                "NULL".to_string()
            } else {
                ((id % 200) + 1).to_string()
            };
            let seed_stmt = format!(
                "INSERT INTO child(id, pid, note) VALUES ({id}, {pid}, '{}')",
                TAG_POOL[id as usize % TAG_POOL.len()]
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        // ── Fuzz loop ───────────────────────────────────────────────────

        let iterations = helpers::fuzz_iterations(30000);
        let mut history = Vec::with_capacity(iterations + 16);
        let verify_queries = [
            ("t", "SELECT id, val, tag FROM t ORDER BY id"),
            ("t_auto", "SELECT id, val, tag FROM t_auto ORDER BY id"),
            ("t_bare", "SELECT id, x, y FROM t_bare ORDER BY id"),
            ("t_trig", "SELECT id, val, tag FROM t_trig ORDER BY id"),
            ("t_pidx", "SELECT id, x, y FROM t_pidx ORDER BY id"),
            ("child", "SELECT id, pid, note FROM child ORDER BY id"),
        ];

        for step in 0..iterations {
            helpers::log_progress("subjournal_differential_fuzz", step, iterations, 10);

            let stmt = match rng.random_range(0..100) {
                // INSERT/UPDATE/DELETE on constrained table "t" (20%)
                0..=7 => random_insert_t(&mut rng),
                8..=13 => random_update_t(&mut rng),
                14..=19 => random_delete_t(&mut rng),
                // Auto-increment table (5%)
                20..=24 => random_insert_auto(&mut rng),
                // Bare table — exercises subjournal DISABLED path (10%)
                25..=34 => random_dml_bare(&mut rng),
                // Trigger table — exercises trigger-forced subjournal (10%)
                35..=44 => random_dml_trig(&mut rng),
                // Partial unique index table (5%)
                45..=49 => random_dml_pidx(&mut rng),
                // FK child table DML (5%)
                50..=54 => random_fk_dml(&mut rng),
                // SAVEPOINT begin (15%)
                55..=69 => format!("SAVEPOINT {}", random_savepoint_name(&mut rng)),
                // RELEASE savepoint (15%)
                70..=84 => {
                    let name = random_savepoint_name(&mut rng);
                    if rng.random_bool(0.5) {
                        format!("RELEASE {name}")
                    } else {
                        format!("RELEASE SAVEPOINT {name}")
                    }
                }
                // ROLLBACK TO savepoint (10%)
                85..=94 => {
                    let name = random_savepoint_name(&mut rng);
                    if rng.random_bool(0.5) {
                        format!("ROLLBACK TO {name}")
                    } else {
                        format!("ROLLBACK TO SAVEPOINT {name}")
                    }
                }
                // DELETE/UPDATE on t_auto (5%)
                95..=97 => {
                    if rng.random_bool(0.3) {
                        "DELETE FROM t_auto".to_string()
                    } else {
                        format!(
                            "DELETE FROM t_auto WHERE id = {}",
                            rng.random_range(1..=100)
                        )
                    }
                }
                98..=99 => {
                    let new_val = random_nullable_int(&mut rng, 1..=50);
                    format!(
                        "UPDATE t_auto SET val = {new_val} WHERE id = {}",
                        rng.random_range(1..=100)
                    )
                }
                _ => unreachable!(),
            };

            history.push(stmt.clone());

            let sqlite_res = sqlite_conn.execute(&stmt, params![]);
            let limbo_res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                limbo_exec_rows_fallible(&db, &limbo_conn, &stmt)
            }));
            let limbo_res = match limbo_res {
                Ok(res) => res,
                Err(_) => {
                    panic!(
                        "turso panicked\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            };

            match (sqlite_res, limbo_res) {
                (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                (sqlite_outcome, limbo_outcome) => {
                    panic!(
                        "subjournal outcome mismatch\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nsqlite: {sqlite_outcome:?}\nturso: {limbo_outcome:?}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            }

            // Verify full table state every 10 steps
            if step % 10 == 0 {
                for (label, verify_query) in verify_queries {
                    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, verify_query);
                    let limbo_rows = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        limbo_exec_rows(&limbo_conn, verify_query)
                    }));
                    let limbo_rows = match limbo_rows {
                        Ok(rows) => rows,
                        Err(_) => {
                            panic!(
                                "turso panicked during verify ({label})\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent:\n{}",
                                history_tail(&history, 60)
                            );
                        }
                    };
                    assert_eq!(
                        limbo_rows,
                        sqlite_rows,
                        "subjournal state mismatch ({label})\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            }
        }

        // Final full verification
        for (label, verify_query) in verify_queries {
            let sqlite_rows = sqlite_exec_rows(&sqlite_conn, verify_query);
            let limbo_rows = limbo_exec_rows(&limbo_conn, verify_query);
            assert_eq!(
                limbo_rows,
                sqlite_rows,
                "final state mismatch ({label})\nseed: {seed}\nrecent:\n{}",
                history_tail(&history, 60)
            );
        }
    }

    /// Targeted test: INSERT OR REPLACE inside a transaction with constraints.
    /// REPLACE is multi_write (deletes conflicting row + inserts new one) and
    /// may_abort (NOT NULL without default falls back to ABORT).
    #[turso_macros::test]
    pub fn subjournal_replace_abort_fallback(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("subjournal_replace_abort_fallback");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open(db.path.with_extension("sqlite")).unwrap();

        limbo_conn.execute("PRAGMA cache_size = 50").unwrap();
        sqlite_conn
            .execute("PRAGMA cache_size = 50", params![])
            .unwrap();

        let schema = "CREATE TABLE t (
            id INTEGER PRIMARY KEY,
            val INTEGER NOT NULL,
            tag TEXT,
            UNIQUE(val),
            CHECK(val > 0)
        )";
        limbo_conn.execute(schema).unwrap();
        sqlite_conn.execute(schema, params![]).unwrap();

        for id in 1..=100 {
            let stmt = format!(
                "INSERT INTO t(id, val, tag) VALUES ({id}, {}, 'seed')",
                id * 10
            );
            limbo_conn.execute(&stmt).unwrap();
            sqlite_conn.execute(&stmt, params![]).unwrap();
        }

        let iterations = helpers::fuzz_iterations(10000);
        let mut history = Vec::with_capacity(iterations + 16);

        for step in 0..iterations {
            helpers::log_progress("subjournal_replace_abort_fallback", step, iterations, 8);

            let stmt = match rng.random_range(0..100) {
                // INSERT OR REPLACE that may conflict on UNIQUE(val) and abort on NOT NULL/CHECK
                0..=39 => {
                    let id = rng.random_range(1..=120);
                    let val = random_nullable_int(&mut rng, -5..=60);
                    let tag = random_tag(&mut rng);
                    format!("INSERT OR REPLACE INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}')")
                }
                // Multi-row INSERT OR REPLACE
                40..=54 => {
                    let count = rng.random_range(2..=5);
                    let rows: Vec<String> = (0..count)
                        .map(|_| {
                            let id = rng.random_range(1..=120);
                            let val = random_nullable_int(&mut rng, -5..=60);
                            let tag = random_tag(&mut rng);
                            format!("({id}, {val}, '{tag}')")
                        })
                        .collect();
                    format!(
                        "INSERT OR REPLACE INTO t(id, val, tag) VALUES {}",
                        rows.join(", ")
                    )
                }
                // SAVEPOINT
                55..=69 => format!("SAVEPOINT {}", random_savepoint_name(&mut rng)),
                // RELEASE
                70..=84 => format!("RELEASE {}", random_savepoint_name(&mut rng)),
                // ROLLBACK TO
                85..=99 => format!("ROLLBACK TO {}", random_savepoint_name(&mut rng)),
                _ => unreachable!(),
            };

            history.push(stmt.clone());

            let sqlite_res = sqlite_conn.execute(&stmt, params![]);
            let limbo_res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                limbo_exec_rows_fallible(&db, &limbo_conn, &stmt)
            }));
            let limbo_res = match limbo_res {
                Ok(res) => res,
                Err(_) => {
                    panic!(
                        "turso panicked\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            };

            match (sqlite_res, limbo_res) {
                (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                (s, l) => {
                    panic!(
                        "replace fallback outcome mismatch\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nsqlite: {s:?}\nturso: {l:?}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            }

            if step % 5 == 0 {
                let verify = "SELECT id, val, tag FROM t ORDER BY id";
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, verify);
                let limbo_rows = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    limbo_exec_rows(&limbo_conn, verify)
                }));
                let limbo_rows = match limbo_rows {
                    Ok(rows) => rows,
                    Err(_) => {
                        panic!(
                            "turso panicked during verify\nseed: {seed}\nstep: {step}\nrecent:\n{}",
                            history_tail(&history, 60)
                        );
                    }
                };
                assert_eq!(
                    limbo_rows,
                    sqlite_rows,
                    "replace fallback state mismatch\nseed: {seed}\nstep: {step}\nrecent:\n{}",
                    history_tail(&history, 60)
                );
            }
        }
    }

    /// Targeted test: UPDATE OR REPLACE with UNIQUE constraint + nested savepoints.
    /// Tests the update path where REPLACE causes delete of conflicting row.
    #[turso_macros::test]
    pub fn subjournal_update_replace_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("subjournal_update_replace_fuzz");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open(db.path.with_extension("sqlite")).unwrap();

        limbo_conn.execute("PRAGMA cache_size = 50").unwrap();
        sqlite_conn
            .execute("PRAGMA cache_size = 50", params![])
            .unwrap();

        let schema = "CREATE TABLE t (
            id INTEGER PRIMARY KEY,
            val INTEGER NOT NULL UNIQUE,
            tag TEXT,
            CHECK(val > 0)
        )";
        limbo_conn.execute(schema).unwrap();
        sqlite_conn.execute(schema, params![]).unwrap();

        for id in 1..=150 {
            let stmt = format!(
                "INSERT INTO t(id, val, tag) VALUES ({id}, {}, 'seed')",
                id * 10
            );
            limbo_conn.execute(&stmt).unwrap();
            sqlite_conn.execute(&stmt, params![]).unwrap();
        }

        let iterations = helpers::fuzz_iterations(15000);
        let mut history = Vec::with_capacity(iterations + 16);

        for step in 0..iterations {
            helpers::log_progress("subjournal_update_replace_fuzz", step, iterations, 8);

            let stmt = match rng.random_range(0..100) {
                // UPDATE OR REPLACE (key path: may delete conflicting row + update target)
                0..=29 => {
                    let new_val = random_nullable_int(&mut rng, -5..=80);
                    let target = rng.random_range(1..=150);
                    format!("UPDATE OR REPLACE t SET val = {new_val} WHERE id = {target}")
                }
                // UPDATE OR REPLACE multiple rows
                30..=44 => {
                    let new_val = random_nullable_int(&mut rng, -5..=80);
                    format!(
                        "UPDATE OR REPLACE t SET val = {new_val} WHERE (id % {}) = 0",
                        rng.random_range(2..=7)
                    )
                }
                // Plain UPDATE (may hit NOT NULL / CHECK)
                45..=54 => {
                    let new_val = random_nullable_int(&mut rng, -5..=80);
                    let target = rng.random_range(1..=150);
                    format!("UPDATE t SET val = {new_val} WHERE id = {target}")
                }
                // INSERT OR REPLACE to keep table populated
                55..=64 => {
                    let id = rng.random_range(1..=200);
                    let val = rng.random_range(1..=80);
                    let tag = random_tag(&mut rng);
                    format!("INSERT OR REPLACE INTO t(id, val, tag) VALUES ({id}, {val}, '{tag}')")
                }
                // SAVEPOINT
                65..=79 => format!("SAVEPOINT {}", random_savepoint_name(&mut rng)),
                // RELEASE
                80..=89 => format!("RELEASE {}", random_savepoint_name(&mut rng)),
                // ROLLBACK TO
                90..=99 => format!("ROLLBACK TO {}", random_savepoint_name(&mut rng)),
                _ => unreachable!(),
            };

            history.push(stmt.clone());

            let sqlite_res = sqlite_conn.execute(&stmt, params![]);
            let limbo_res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                limbo_exec_rows_fallible(&db, &limbo_conn, &stmt)
            }));
            let limbo_res = match limbo_res {
                Ok(res) => res,
                Err(_) => {
                    panic!(
                        "turso panicked\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            };

            match (sqlite_res, limbo_res) {
                (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                (s, l) => {
                    panic!(
                        "update replace outcome mismatch\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nsqlite: {s:?}\nturso: {l:?}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            }

            if step % 5 == 0 {
                let verify = "SELECT id, val, tag FROM t ORDER BY id";
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, verify);
                let limbo_rows = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    limbo_exec_rows(&limbo_conn, verify)
                }));
                let limbo_rows = match limbo_rows {
                    Ok(rows) => rows,
                    Err(_) => {
                        panic!(
                            "turso panicked during verify\nseed: {seed}\nstep: {step}\nrecent:\n{}",
                            history_tail(&history, 60)
                        );
                    }
                };
                assert_eq!(
                    limbo_rows,
                    sqlite_rows,
                    "update replace state mismatch\nseed: {seed}\nstep: {step}\nrecent:\n{}",
                    history_tail(&history, 60)
                );
            }
        }
    }

    /// Targeted test: DELETE with FK constraints inside nested savepoints.
    /// Exercises the delete path where may_abort depends on FK/trigger presence.
    #[turso_macros::test]
    pub fn subjournal_delete_fk_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("subjournal_delete_fk_fuzz");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open(db.path.with_extension("sqlite")).unwrap();

        limbo_conn.execute("PRAGMA cache_size = 50").unwrap();
        sqlite_conn
            .execute("PRAGMA cache_size = 50", params![])
            .unwrap();

        limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite_conn
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();

        for schema in [
            "CREATE TABLE parent (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
            "CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                pid INTEGER NOT NULL,
                note TEXT,
                FOREIGN KEY(pid) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
            )",
        ] {
            limbo_conn.execute(schema).unwrap();
            sqlite_conn.execute(schema, params![]).unwrap();
        }

        for id in 1..=100 {
            let stmt = format!("INSERT INTO parent(id, name) VALUES ({id}, 'p{id}')");
            limbo_conn.execute(&stmt).unwrap();
            sqlite_conn.execute(&stmt, params![]).unwrap();
        }

        for id in 1..=200 {
            let pid = (id % 100) + 1;
            let stmt = format!("INSERT INTO child(id, pid, note) VALUES ({id}, {pid}, 'c{id}')");
            limbo_conn.execute(&stmt).unwrap();
            sqlite_conn.execute(&stmt, params![]).unwrap();
        }

        let iterations = helpers::fuzz_iterations(20000);
        let mut history = Vec::with_capacity(iterations + 16);
        let verify_queries = [
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, note FROM child ORDER BY id"),
        ];

        for step in 0..iterations {
            helpers::log_progress("subjournal_delete_fk_fuzz", step, iterations, 8);

            let stmt = match rng.random_range(0..100) {
                0..=14 => {
                    if rng.random_bool(0.1) {
                        "DELETE FROM parent".to_string()
                    } else {
                        format!(
                            "DELETE FROM parent WHERE id = {}",
                            rng.random_range(1..=100)
                        )
                    }
                }
                15..=24 => {
                    if rng.random_bool(0.1) {
                        "DELETE FROM child".to_string()
                    } else {
                        format!("DELETE FROM child WHERE id = {}", rng.random_range(1..=200))
                    }
                }
                25..=34 => {
                    let id = rng.random_range(1..=120);
                    format!("INSERT OR IGNORE INTO parent(id, name) VALUES ({id}, 'p{id}')")
                }
                35..=49 => {
                    let id = rng.random_range(1..=300);
                    let pid = rng.random_range(1..=150);
                    let note = random_tag(&mut rng);
                    match rng.random_range(0..3) {
                        0 => format!(
                            "INSERT INTO child(id, pid, note) VALUES ({id}, {pid}, '{note}')"
                        ),
                        1 => format!(
                            "INSERT OR IGNORE INTO child(id, pid, note) VALUES ({id}, {pid}, '{note}')"
                        ),
                        2 => format!(
                            "INSERT OR REPLACE INTO child(id, pid, note) VALUES ({id}, {pid}, '{note}')"
                        ),
                        _ => unreachable!(),
                    }
                }
                50..=54 => {
                    let old = rng.random_range(1..=100);
                    let new = rng.random_range(1..=120);
                    format!("UPDATE parent SET id = {new} WHERE id = {old}")
                }
                55..=69 => format!("SAVEPOINT {}", random_savepoint_name(&mut rng)),
                70..=84 => format!("RELEASE {}", random_savepoint_name(&mut rng)),
                85..=99 => format!("ROLLBACK TO {}", random_savepoint_name(&mut rng)),
                _ => unreachable!(),
            };

            history.push(stmt.clone());

            let sqlite_res = sqlite_conn.execute(&stmt, params![]);
            let limbo_res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                limbo_exec_rows_fallible(&db, &limbo_conn, &stmt)
            }));
            let limbo_res = match limbo_res {
                Ok(res) => res,
                Err(_) => {
                    panic!(
                        "turso panicked\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            };

            match (sqlite_res, limbo_res) {
                (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                (s, l) => {
                    panic!(
                        "delete FK outcome mismatch\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nsqlite: {s:?}\nturso: {l:?}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            }

            if step % 10 == 0 {
                for (label, verify_query) in verify_queries {
                    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, verify_query);
                    let limbo_rows = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        limbo_exec_rows(&limbo_conn, verify_query)
                    }));
                    let limbo_rows = match limbo_rows {
                        Ok(rows) => rows,
                        Err(_) => {
                            panic!(
                                "turso panicked during verify ({label})\nseed: {seed}\nstep: {step}\nrecent:\n{}",
                                history_tail(&history, 60)
                            );
                        }
                    };
                    assert_eq!(
                        limbo_rows, sqlite_rows,
                        "delete FK state mismatch ({label})\nseed: {seed}\nstep: {step}\nrecent:\n{}",
                        history_tail(&history, 60)
                    );
                }
            }
        }
    }
}
