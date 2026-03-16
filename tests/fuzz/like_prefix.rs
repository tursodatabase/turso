//! Differential fuzzer for LIKE prefix optimization.
//!
//! Tests that the LIKE prefix → range rewrite (including residual consumption)
//! produces identical results to SQLite across varied collations, prefix
//! patterns, and data distributions.

#[cfg(test)]
mod tests {
    use crate::helpers;
    use core_tester::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
    use rand::Rng;

    const COLLATIONS: [&str; 3] = ["BINARY", "NOCASE", "RTRIM"];

    /// ASCII byte pool covering the interesting ranges for LIKE/collation:
    /// letters (both cases), digits, boundary/special characters, DEL (0x7F),
    /// and space (relevant for RTRIM).
    const BYTE_POOL: &[u8] =
        b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@#[{\\]^_`~ \x7F";

    /// Unicode strings mixed in occasionally to ensure the optimization
    /// correctly bails out for non-ASCII without panicking or corrupting.
    const UNICODE_POOL: [&str; 8] = [
        "café",
        "über",
        "日本語",
        "中文",
        "Ñoño",
        "résumé",
        "naïve",
        "🦀",
    ];

    /// Generate a random string of length 0..=max_len. Mostly ASCII from
    /// BYTE_POOL, but ~10% of the time picks a unicode string instead.
    fn random_string(rng: &mut impl Rng, max_len: usize) -> String {
        if rng.random_bool(0.1) {
            return UNICODE_POOL[rng.random_range(0..UNICODE_POOL.len())].to_string();
        }
        let len = rng.random_range(0..=max_len);
        (0..len)
            .map(|_| BYTE_POOL[rng.random_range(0..BYTE_POOL.len())] as char)
            .collect()
    }

    /// Generate a random LIKE pattern from a random prefix string plus a
    /// random wildcard suffix.
    fn random_like_pattern(rng: &mut impl Rng) -> String {
        let prefix = random_string(rng, 4);
        let suffix = match rng.random_range(0..5) {
            0 => "%".to_string(),
            1 => "_%".to_string(),
            2 => format!("{}%", random_string(rng, 2)),
            3 => String::new(), // exact match, no wildcard
            _ => format!("%{}", random_string(rng, 2)),
        };
        format!("{prefix}{suffix}")
    }

    #[turso_macros::test(mvcc)]
    pub fn like_prefix_differential_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test_tracing("like_prefix_differential_fuzz");
        let builder = helpers::builder_from_db(&db);

        let iters = helpers::fuzz_iterations(200);

        for iter in 0..iters {
            helpers::log_progress("like_prefix_differential_fuzz", iter, iters, 10);

            let collation = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let has_index = rng.random_bool(0.8);

            let table_ddl =
                format!("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT COLLATE {collation})");
            let index_ddl = "CREATE INDEX idx_val ON t(val)";

            let limbo_db = builder.clone().with_init_sql(&table_ddl).build();
            let limbo_conn = limbo_db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute(&table_ddl, ()).unwrap();

            if has_index {
                limbo_conn.execute(index_ddl).unwrap();
                sqlite_conn.execute(index_ddl, ()).unwrap();
            }

            // Insert random data including NULLs
            let num_rows = rng.random_range(20..=80);
            for i in 0..num_rows {
                let insert = if rng.random_bool(0.05) {
                    format!("INSERT INTO t VALUES ({i}, NULL)")
                } else {
                    let val = random_string(&mut rng, 8);
                    format!("INSERT INTO t VALUES ({i}, '{}')", val.replace('\'', "''"))
                };
                sqlite_conn.execute(&insert, ()).unwrap();
                limbo_conn.execute(&insert).unwrap();
            }

            // Test random LIKE patterns
            let num_queries = rng.random_range(3..=8);
            for _ in 0..num_queries {
                let pattern = random_like_pattern(&mut rng);
                let query = format!(
                    "SELECT id, val FROM t WHERE val LIKE '{}' ORDER BY id",
                    pattern.replace('\'', "''")
                );

                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
                let limbo_rows = limbo_exec_rows(&limbo_conn, &query);

                similar_asserts::assert_eq!(
                    Turso: limbo_rows,
                    Sqlite: sqlite_rows,
                    "MISMATCH!\nQuery: {query}\nCollation: {collation}\nHas index: {has_index}\nSeed: {seed}\nIteration: {iter}",
                );
            }
        }
    }

    /// Targeted test for the boundary between consumed and non-consumed LIKE.
    /// Generates random data and patterns that specifically exercise the
    /// BINARY collation range where false positives could leak through
    /// between uppercase and lowercase ASCII ranges.
    #[turso_macros::test(mvcc)]
    pub fn like_prefix_binary_boundary_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test_tracing("like_prefix_binary_boundary_fuzz");
        let builder = helpers::builder_from_db(&db);

        /// Byte pool biased toward boundary characters between uppercase and
        /// lowercase ASCII ranges: 'Z'=90, '['=91 .. '`'=96, 'a'=97.
        const BOUNDARY_BYTES: &[u8] = b"abcABCxyzXYZ@Z[\\]^_`{z";

        let iters = helpers::fuzz_iterations(100);

        for iter in 0..iters {
            helpers::log_progress("like_prefix_binary_boundary_fuzz", iter, iters, 10);

            // Always BINARY collation — this is where false positives can occur
            let table_ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)";
            let index_ddl = "CREATE INDEX idx_val ON t(val)";

            let limbo_db = builder.clone().with_init_sql(table_ddl).build();
            let limbo_conn = limbo_db.connect_limbo();
            limbo_conn.execute(index_ddl).unwrap();

            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute(table_ddl, ()).unwrap();
            sqlite_conn.execute(index_ddl, ()).unwrap();

            // Insert random boundary-biased data
            let num_rows = rng.random_range(30..=60);
            for i in 0..num_rows {
                let len = rng.random_range(1..=6);
                let val: String = (0..len)
                    .map(|_| BOUNDARY_BYTES[rng.random_range(0..BOUNDARY_BYTES.len())] as char)
                    .collect();
                let insert = format!("INSERT INTO t VALUES ({i}, '{}')", val.replace('\'', "''"));
                sqlite_conn.execute(&insert, ()).unwrap();
                limbo_conn.execute(&insert).unwrap();
            }

            // Generate random prefix patterns from boundary bytes
            let num_queries = rng.random_range(4..=10);
            for _ in 0..num_queries {
                let prefix_len = rng.random_range(1..=3);
                let prefix: String = (0..prefix_len)
                    .map(|_| BOUNDARY_BYTES[rng.random_range(0..BOUNDARY_BYTES.len())] as char)
                    .collect();
                let pattern = format!("{prefix}%");
                let query = format!(
                    "SELECT id, val FROM t WHERE val LIKE '{}' ORDER BY id",
                    pattern.replace('\'', "''")
                );

                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
                let limbo_rows = limbo_exec_rows(&limbo_conn, &query);

                similar_asserts::assert_eq!(
                    Turso: limbo_rows,
                    Sqlite: sqlite_rows,
                    "MISMATCH!\nQuery: {query}\nSeed: {seed}\nIteration: {iter}",
                );
            }
        }
    }
}
