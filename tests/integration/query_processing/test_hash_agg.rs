//! Integration tests for hash aggregation, particularly spill-to-disk scenarios.
//!
//! These tests create enough distinct groups to exceed the 32KB debug memory budget,
//! forcing the hash table to spill to disk and verifying correctness after merge.
//!
//! IMPORTANT: We use `ORDER BY <aggregate>` instead of `ORDER BY <group_key>` because
//! ORDER BY on the group key causes the optimizer to use sort-based aggregation.
//! ORDER BY on an aggregate forces hash aggregation while still providing deterministic results.

use crate::common::{ExecRows, TempDatabase};

/// Comprehensive test for hash aggregation with spill-to-disk.
/// Tests multiple data types, aggregate functions, and edge cases in a single test
/// to verify correctness after spilling and merging partitions.
#[test]
fn test_hash_agg_spill() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    // Create table with multiple column types
    conn.execute(
        "CREATE TABLE spill_test (
            grp_int INT,
            grp_text TEXT,
            val_int INT,
            val_real REAL
        )",
    )
    .unwrap();

    // Insert 500 groups with 10 rows each = 5000 rows
    // This exceeds the 32KB debug memory budget, forcing spills
    conn.execute(
        "INSERT INTO spill_test
         SELECT
           g.value,
           'group_' || g.value,
           CASE WHEN v.value % 2 = 0 THEN g.value * 10 + v.value ELSE -(g.value * 10 + v.value) END,
           g.value * 0.1 + v.value * 0.01
         FROM generate_series(0, 499) AS g, generate_series(0, 9) AS v",
    )
    .unwrap();

    // === Test 1: Basic aggregates with INT group key ===
    // sum(val_int) for group i: positive at v=0,2,4,6,8 and negative at v=1,3,5,7,9
    // = (i*10+0)+(i*10+2)+(i*10+4)+(i*10+6)+(i*10+8) - (i*10+1)-(i*10+3)-(i*10+5)-(i*10+7)-(i*10+9)
    // = 5*i*10 + 20 - 5*i*10 - 25 = -5
    let results: Vec<(i64, i64, i64, i64, i64)> = conn.exec_rows(
        "SELECT grp_int, sum(val_int), count(val_int), min(val_int), max(val_int)
         FROM spill_test GROUP BY grp_int ORDER BY max(val_int) LIMIT 3",
    );
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (0, -5, 10, -9, 8));
    assert_eq!(results[1], (1, -5, 10, -19, 18));
    assert_eq!(results[2], (2, -5, 10, -29, 28));

    // === Test 2: TEXT group key ===
    let results: Vec<(String, i64, i64)> = conn.exec_rows(
        "SELECT grp_text, sum(val_int), count(val_int)
         FROM spill_test GROUP BY grp_text ORDER BY max(val_int) LIMIT 3",
    );
    assert_eq!(results[0], ("group_0".to_string(), -5, 10));
    assert_eq!(results[1], ("group_1".to_string(), -5, 10));
    assert_eq!(results[2], ("group_2".to_string(), -5, 10));

    // === Test 3: REAL values with floating point aggregation ===
    // val_real for group i = i*0.1 + v*0.01 for v in 0..9
    // sum = 10*(i*0.1) + (0.00+0.01+...+0.09) = i + 0.45
    let results: Vec<(i64, f64, f64, f64)> = conn.exec_rows(
        "SELECT grp_int, sum(val_real), min(val_real), max(val_real)
         FROM spill_test GROUP BY grp_int ORDER BY sum(val_real) LIMIT 3",
    );
    assert_eq!(results[0].0, 0);
    assert!((results[0].1 - 0.45).abs() < 0.0001);
    assert!((results[0].2 - 0.0).abs() < 0.0001);
    assert!((results[0].3 - 0.09).abs() < 0.0001);
    assert_eq!(results[1].0, 1);
    assert!((results[1].1 - 1.45).abs() < 0.0001);

    // === Test 4: Multiple aggregates (increases memory pressure) ===
    let results: Vec<(i64, i64, f64, i64, i64, i64, f64)> = conn.exec_rows(
        "SELECT grp_int, sum(val_int), total(val_int), min(val_int), max(val_int), count(val_int), avg(val_real)
         FROM spill_test GROUP BY grp_int ORDER BY max(val_int) LIMIT 2",
    );
    assert_eq!(results[0].0, 0); // grp_int
    assert_eq!(results[0].1, -5); // sum
    assert!((results[0].2 - (-5.0)).abs() < 0.0001); // total
    assert_eq!(results[0].3, -9); // min
    assert_eq!(results[0].4, 8); // max
    assert_eq!(results[0].5, 10); // count
    assert!((results[0].6 - 0.045).abs() < 0.0001); // avg(val_real) = 0.45/10

    // === Test 5: Verify last groups (ensures spilled partitions merge correctly) ===
    let results: Vec<(i64, i64, i64)> = conn.exec_rows(
        "SELECT grp_int, min(val_int), max(val_int)
         FROM spill_test GROUP BY grp_int ORDER BY max(val_int) DESC LIMIT 3",
    );
    assert_eq!(results[0], (499, -4999, 4998));
    assert_eq!(results[1], (498, -4989, 4988));
    assert_eq!(results[2], (497, -4979, 4978));

    // === Test 6: Composite group key (INT, TEXT) ===
    conn.execute("DROP TABLE spill_test").unwrap();
    conn.execute("CREATE TABLE spill_test (grp_int INT, grp_text TEXT, val INT)")
        .unwrap();
    conn.execute(
        "INSERT INTO spill_test
         SELECT g.value / 10, 'cat_' || (g.value % 10), g.value + v.value
         FROM generate_series(0, 499) AS g, generate_series(0, 9) AS v",
    )
    .unwrap();

    let results: Vec<(i64, String, i64, i64)> = conn.exec_rows(
        "SELECT grp_int, grp_text, sum(val), count(val)
         FROM spill_test GROUP BY grp_int, grp_text ORDER BY sum(val) LIMIT 2",
    );
    // (grp_int=0, grp_text='cat_0') -> g.value=0, sum = 0+1+...+9 = 45
    assert_eq!(results[0], (0, "cat_0".to_string(), 45, 10));
    // (grp_int=0, grp_text='cat_1') -> g.value=1, sum = 1+2+...+10 = 55
    assert_eq!(results[1], (0, "cat_1".to_string(), 55, 10));

    // === Test 7: NULL handling ===
    conn.execute("DROP TABLE spill_test").unwrap();
    conn.execute("CREATE TABLE spill_test (grp INT, val INT)")
        .unwrap();
    conn.execute(
        "INSERT INTO spill_test
         SELECT g.value, CASE WHEN v.value % 3 = 0 THEN NULL ELSE g.value * 10 + v.value END
         FROM generate_series(0, 499) AS g, generate_series(0, 9) AS v",
    )
    .unwrap();

    // Each group has 4 NULLs (v=0,3,6,9) and 6 non-NULL values (v=1,2,4,5,7,8)
    // sum for group i = (i*10+1)+(i*10+2)+(i*10+4)+(i*10+5)+(i*10+7)+(i*10+8) = 60*i + 27
    let results: Vec<(i64, i64, i64, i64)> = conn.exec_rows(
        "SELECT grp, sum(val), count(val), count(*)
         FROM spill_test GROUP BY grp ORDER BY sum(val) LIMIT 3",
    );
    assert_eq!(results[0], (0, 27, 6, 10));
    assert_eq!(results[1], (1, 87, 6, 10));
    assert_eq!(results[2], (2, 147, 6, 10));

    // === Test 8: DISTINCT aggregates ===
    conn.execute("DROP TABLE spill_test").unwrap();
    conn.execute("CREATE TABLE spill_test (grp INT, val INT)")
        .unwrap();
    conn.execute(
        "INSERT INTO spill_test
         SELECT g.value, g.value % 10
         FROM generate_series(0, 499) AS g, generate_series(0, 9) AS v",
    )
    .unwrap();

    // Each group i has val = i % 10, repeated 10 times
    let results: Vec<(i64, i64, i64)> = conn.exec_rows(
        "SELECT grp, sum(distinct val), count(distinct val)
         FROM spill_test GROUP BY grp ORDER BY sum(distinct val), grp",
    );
    assert_eq!(results.len(), 500);
    let group_0 = results.iter().find(|r| r.0 == 0).unwrap();
    let group_123 = results.iter().find(|r| r.0 == 123).unwrap();
    let group_499 = results.iter().find(|r| r.0 == 499).unwrap();
    assert_eq!(*group_0, (0, 0, 1)); // val=0
    assert_eq!(*group_123, (123, 3, 1)); // val=123%10=3
    assert_eq!(*group_499, (499, 9, 1)); // val=499%10=9

    // === Test 9: Verify total group count ===
    let results: Vec<(i64,)> =
        conn.exec_rows("SELECT count(*) FROM (SELECT grp FROM spill_test GROUP BY grp)");
    assert_eq!(results[0].0, 500);
}
