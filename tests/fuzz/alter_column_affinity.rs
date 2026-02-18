//! Fuzz regression for ALTER COLUMN type rewrite.
//!
//! This exercises Turso's non-standard `ALTER COLUMN ... TO ...` path, which rewrites
//! all table rows and must preserve both row contents and index consistency.

#[cfg(test)]
mod tests {
    use core_tester::common::{
        limbo_exec_rows, rng_from_time_or_env, rusqlite_integrity_check, TempDatabase,
    };
    use rand::Rng;

    #[turso_macros::test]
    fn alter_column_affinity_transition_matrix_fuzz(db: TempDatabase) {
        let (mut rng, seed) = rng_from_time_or_env();
        println!("alter_column_affinity_transition_matrix_fuzz seed: {seed}");

        let rows = std::env::var("ALTER_COLUMN_FUZZ_ROWS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(120);

        let conn = db.connect_limbo();

        const AFFINITIES: [(&str, &str); 5] = [
            ("integer", "INTEGER"),
            ("real", "REAL"),
            ("numeric", "NUMERIC"),
            ("text", "TEXT"),
            ("blob", "BLOB"),
        ];

        let mut transition_idx = 0usize;
        for (from_aff_name, from_decl_type) in AFFINITIES {
            for (to_aff_name, to_decl_type) in AFFINITIES {
                if from_aff_name == to_aff_name {
                    continue;
                }

                let table_name = format!("t_{transition_idx}_{from_aff_name}_to_{to_aff_name}");
                let create = format!(
                    "CREATE TABLE {table_name}(id INTEGER PRIMARY KEY, v {from_decl_type}, payload TEXT)"
                );
                conn.execute(&create).unwrap();
                conn.execute(&format!(
                    "CREATE INDEX idx_{table_name}_v ON {table_name}(v)"
                ))
                .unwrap();
                conn.execute(&format!(
                    "CREATE INDEX idx_{table_name}_v_partial ON {table_name}(v) WHERE (id % 2) = 0"
                ))
                .unwrap();

                for id in 1..=rows {
                    let value_sql = match rng.random_range(0..8) {
                        0 => "NULL".to_string(),
                        1 => rng.random_range(-100_000..100_000).to_string(),
                        2 => format!("{:.6}", rng.random_range(-1000.0..1000.0)),
                        3 => format!("'{}'", rng.random_range(-50_000..50_000)),
                        4 => format!("'t{}'", rng.random_range(0..10_000)),
                        5 => "X'616263'".to_string(), // "abc"
                        6 => "X'00FF10'".to_string(),
                        _ => format!("'{}'", rng.random_range(0..10_000)),
                    };
                    let payload = format!("p{}", rng.random_range(0..1_000_000));
                    let stmt = format!(
                        "INSERT INTO {table_name}(id, v, payload) VALUES ({id}, {value_sql}, '{}')",
                        payload
                    );
                    conn.execute(&stmt).unwrap();
                }

                let ids_before =
                    limbo_exec_rows(&conn, &format!("SELECT id FROM {table_name} ORDER BY id"));
                let count_before =
                    limbo_exec_rows(&conn, &format!("SELECT COUNT(*) FROM {table_name}"));

                conn.execute(&format!(
                    "ALTER TABLE {table_name} ALTER COLUMN v TO v2 {to_decl_type}"
                ))
                .unwrap();

                let ids_after =
                    limbo_exec_rows(&conn, &format!("SELECT id FROM {table_name} ORDER BY id"));
                let count_after =
                    limbo_exec_rows(&conn, &format!("SELECT COUNT(*) FROM {table_name}"));
                similar_asserts::assert_eq!(
                    Before: ids_before,
                    After: ids_after,
                    "row id set changed after ALTER COLUMN rewrite {from_aff_name}->{to_aff_name} (seed: {seed})",
                );
                similar_asserts::assert_eq!(
                    Before: count_before,
                    After: count_after,
                    "row count changed after ALTER COLUMN rewrite {from_aff_name}->{to_aff_name} (seed: {seed})",
                );

                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
                rusqlite_integrity_check(db.path.as_path()).unwrap_or_else(|e| {
                    panic!(
                        "SQLite integrity_check failed after transition {from_aff_name}->{to_aff_name}: {e}"
                    )
                });

                transition_idx += 1;
            }
        }
    }
}
