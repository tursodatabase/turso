//! Example demonstrating time-based table partitioning with mathematical verification.
//!
//! This example:
//! - Creates a partitioned table with sin/cos values
//! - Inserts data every minute for 20 virtual days (28,800 rows)
//! - Queries all data across multiple partitions
//! - Verifies mathematical identity: sin²(x) + cos²(x) = 1
//!
//! Run with: cargo run --example partitioning

use std::sync::Arc;
use turso_core::partition::{DefaultPathResolver, PartitionConfig};
use turso_core::{Database, PlatformIO, Result, Value};

/// Microseconds per minute
const MICROS_PER_MINUTE: i64 = 60 * 1_000_000;
/// Number of days to simulate
const SIMULATION_DAYS: i64 = 20;
/// Total number of data points (one per minute for 20 days)
const TOTAL_POINTS: i64 = SIMULATION_DAYS * 24 * 60; // 28,800 points
/// Tolerance for floating point comparison
const EPSILON: f64 = 1e-10;

fn main() -> Result<()> {
    println!("=== Turso Partitioning: Sin/Cos Verification Example ===\n");

    // Create a temporary directory for our example
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("sincos.db");
    let partitions_dir = temp_dir.path().join("partitions");
    std::fs::create_dir_all(&partitions_dir).expect("Failed to create partitions directory");

    println!("Database path: {}", db_path.display());
    println!("Partitions directory: {}", partitions_dir.display());
    println!(
        "Simulation: {} days, {} data points\n",
        SIMULATION_DAYS, TOTAL_POINTS
    );

    // =========================================================================
    // PART 1: Create Database and Partitioned Table
    // =========================================================================
    println!("--- Part 1: Creating Database and Partitioned Table ---\n");

    let io = Arc::new(PlatformIO::new()?);
    let db = Database::open_file(io.clone(), db_path.to_str().unwrap())?;
    let conn = db.connect()?;

    // Create a partitioned table for trigonometric data
    let table_schema = "CREATE TABLE trig_data (
        id INTEGER PRIMARY KEY,
        timestamp INTEGER NOT NULL,
        angle_rad REAL NOT NULL,
        sin_value REAL NOT NULL,
        cos_value REAL NOT NULL
    )";

    conn.execute(&format!("{} PARTITION BY (timestamp)", table_schema))?;
    println!("Created partitioned table: trig_data");
    println!("  Columns: id, timestamp, angle_rad, sin_value, cos_value");

    // =========================================================================
    // PART 2: Configure Partition Manager
    // =========================================================================
    println!("\n--- Part 2: Configuring Partition Manager ---\n");

    let resolver = Box::new(DefaultPathResolver::daily(partitions_dir.clone()));
    let config = PartitionConfig::new(resolver, table_schema.to_string(), "timestamp".to_string());

    conn.register_partitioned_table("trig_data", config)?;
    println!("Registered 'trig_data' with daily partitions");

    // =========================================================================
    // PART 3: Insert Data (every minute for 20 days)
    // =========================================================================
    println!("\n--- Part 3: Inserting Data ---\n");

    // Start timestamp: 2025-01-01 00:00:00 UTC
    let start_timestamp: i64 = 1735689600_000_000;

    println!(
        "Inserting {} data points (1 per minute for {} days)...",
        TOTAL_POINTS, SIMULATION_DAYS
    );
    let insert_start = std::time::Instant::now();

    let mut inserted_count = 0;
    let mut current_day = -1i64;

    for i in 0..TOTAL_POINTS {
        let timestamp = start_timestamp + i * MICROS_PER_MINUTE;
        let day = i / (24 * 60);

        // Progress indicator per day
        if day != current_day {
            current_day = day;
            if day > 0 && day % 5 == 0 {
                println!("  Day {}/{} completed...", day, SIMULATION_DAYS);
            }
        }

        // Calculate angle in radians (one full cycle per day)
        let minutes_in_day = i % (24 * 60);
        let angle_rad = (minutes_in_day as f64) * 2.0 * std::f64::consts::PI / (24.0 * 60.0);

        let sin_value = angle_rad.sin();
        let cos_value = angle_rad.cos();

        let insert_sql = format!(
            "INSERT INTO trig_data (id, timestamp, angle_rad, sin_value, cos_value) \
             VALUES ({}, {}, {}, {}, {})",
            i + 1,
            timestamp,
            angle_rad,
            sin_value,
            cos_value
        );

        conn.execute(&insert_sql)?;
        inserted_count += 1;
    }

    let insert_duration = insert_start.elapsed();
    println!(
        "\nInserted {} rows in {:.2?}",
        inserted_count, insert_duration
    );
    println!(
        "  Rate: {:.0} inserts/sec",
        inserted_count as f64 / insert_duration.as_secs_f64()
    );

    // =========================================================================
    // PART 4: List Created Partitions
    // =========================================================================
    println!("\n--- Part 4: Partition Summary ---\n");

    let partitions = conn.list_partitions("trig_data")?;
    println!("Created {} partitions:", partitions.len());

    let mut total_size: u64 = 0;
    for p in &partitions {
        println!(
            "  {} ({} bytes, attached: {})",
            p.db_alias, p.size_bytes, p.attached
        );
        total_size += p.size_bytes;
    }
    println!("\nTotal partition size: {} KB", total_size / 1024);

    // =========================================================================
    // PART 5: Query All Data and Verify
    // =========================================================================
    println!("\n--- Part 5: Querying and Verifying Data ---\n");

    // For multiple partitions, we need to query each one and aggregate results
    let query_start = std::time::Instant::now();

    let mut all_rows: Vec<(i64, i64, f64, f64, f64)> = Vec::new();
    let mut query_errors = 0;

    for partition in &partitions {
        // Ensure partition is attached (it should already be from INSERT)
        if !partition.attached {
            let path = std::path::PathBuf::from(&partition.file_path);
            if let Err(e) = conn.attach_partition("trig_data", &path) {
                println!("  Warning: Could not attach {}: {}", partition.db_alias, e);
                continue;
            }
        }

        // Query this partition using qualified table name
        let query_sql = format!(
            "SELECT id, timestamp, angle_rad, sin_value, cos_value FROM {}.trig_data",
            partition.db_alias
        );

        match conn.prepare(&query_sql) {
            Ok(mut stmt) => match stmt.run_collect_rows() {
                Ok(rows) => {
                    for row in rows {
                        if row.len() >= 5 {
                            let id = extract_integer(&row[0]);
                            let ts = extract_integer(&row[1]);
                            let angle = extract_float(&row[2]);
                            let sin_val = extract_float(&row[3]);
                            let cos_val = extract_float(&row[4]);
                            all_rows.push((id, ts, angle, sin_val, cos_val));
                        }
                    }
                }
                Err(e) => {
                    println!("  Error querying {}: {}", partition.db_alias, e);
                    query_errors += 1;
                }
            },
            Err(e) => {
                println!("  Error preparing query for {}: {}", partition.db_alias, e);
                query_errors += 1;
            }
        }
    }

    let query_duration = query_start.elapsed();
    println!(
        "Queried {} rows from {} partitions in {:.2?}",
        all_rows.len(),
        partitions.len(),
        query_duration
    );

    if query_errors > 0 {
        println!("  ({} partition query errors)", query_errors);
    }

    // =========================================================================
    // PART 6: Mathematical Verification
    // =========================================================================
    println!("\n--- Part 6: Mathematical Verification ---\n");

    println!(
        "Verifying sin²(x) + cos²(x) = 1 for all {} data points...",
        all_rows.len()
    );

    let verify_start = std::time::Instant::now();
    let mut verification_errors = 0;
    let mut max_deviation: f64 = 0.0;
    let mut sum_deviation: f64 = 0.0;

    for (id, _ts, angle, sin_val, cos_val) in &all_rows {
        // Verify: sin²(x) + cos²(x) should equal 1
        let identity = sin_val * sin_val + cos_val * cos_val;
        let deviation = (identity - 1.0).abs();

        sum_deviation += deviation;
        if deviation > max_deviation {
            max_deviation = deviation;
        }

        if deviation > EPSILON {
            if verification_errors < 5 {
                println!(
                    "  ERROR at id={}: angle={:.6}, sin={:.10}, cos={:.10}, sin²+cos²={:.15}",
                    id, angle, sin_val, cos_val, identity
                );
            }
            verification_errors += 1;
        }

        // Also verify that sin and cos values are correct
        let expected_sin = angle.sin();
        let expected_cos = angle.cos();

        let sin_diff = (sin_val - expected_sin).abs();
        let cos_diff = (cos_val - expected_cos).abs();

        if sin_diff > EPSILON || cos_diff > EPSILON {
            if verification_errors < 5 {
                println!("  ERROR at id={}: sin mismatch ({:.10} vs {:.10}) or cos mismatch ({:.10} vs {:.10})",
                    id, sin_val, expected_sin, cos_val, expected_cos);
            }
            verification_errors += 1;
        }
    }

    let verify_duration = verify_start.elapsed();
    let avg_deviation = if !all_rows.is_empty() {
        sum_deviation / all_rows.len() as f64
    } else {
        0.0
    };

    println!("\nVerification completed in {:.2?}", verify_duration);
    println!("  Total points verified: {}", all_rows.len());
    println!("  Expected points: {}", TOTAL_POINTS);
    println!("  Verification errors: {}", verification_errors);
    println!("  Max deviation from identity: {:.2e}", max_deviation);
    println!("  Avg deviation from identity: {:.2e}", avg_deviation);

    // =========================================================================
    // PART 7: Final Summary
    // =========================================================================
    println!("\n--- Summary ---\n");

    let success = verification_errors == 0 && all_rows.len() as i64 == TOTAL_POINTS;

    if success {
        println!(
            "SUCCESS: All {} data points verified correctly!",
            TOTAL_POINTS
        );
        println!("  - {} daily partitions created", partitions.len());
        println!("  - Total storage: {} KB", total_size / 1024);
        println!("  - sin²(x) + cos²(x) = 1 verified for all points");
    } else {
        println!("FAILURE:");
        if all_rows.len() as i64 != TOTAL_POINTS {
            println!(
                "  - Missing data: got {} rows, expected {}",
                all_rows.len(),
                TOTAL_POINTS
            );
        }
        if verification_errors > 0 {
            println!("  - {} verification errors", verification_errors);
        }
    }

    // =========================================================================
    // PART 8: Cleanup
    // =========================================================================
    println!("\n--- Cleanup ---\n");

    conn.unregister_partitioned_table("trig_data")?;
    println!("Unregistered 'trig_data' from partition manager");

    println!("\n=== Example Complete ===");

    Ok(())
}

/// Extract integer from Value
fn extract_integer(value: &Value) -> i64 {
    match value {
        Value::Integer(i) => *i,
        _ => 0,
    }
}

/// Extract float from Value
fn extract_float(value: &Value) -> f64 {
    match value {
        Value::Float(f) => *f,
        Value::Integer(i) => *i as f64,
        _ => 0.0,
    }
}
