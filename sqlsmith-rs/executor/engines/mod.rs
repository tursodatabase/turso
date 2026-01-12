use serde::{Deserialize, Serialize};
use sqlsmith_rs_common::profile::Profile;
use sqlsmith_rs_common::rand_by_seed::LcgRng;
use sqlsmith_rs_drivers::{new_conn, DatabaseDriver, DRIVER_KIND};
use std::collections::HashMap;
use std::time::Duration;

mod sqlite_engine;
pub use sqlite_engine::SqliteEngine;

mod limbo_engine;
pub use limbo_engine::LimboEngine;

use crate::generators::common::SqlKind;

// Define Engine trait
pub trait Engine {
    fn run(&mut self);
    fn generate_sql(&mut self) -> String;
    fn get_driver_kind(&self) -> DRIVER_KIND;
    fn get_sqlite_driver_box(&mut self) -> Option<&mut dyn DatabaseDriver>;
    fn get_limbo_driver_box(&mut self) -> Option<&mut dyn DatabaseDriver>;
}

pub fn with_driver_kind(
    seed: u64,
    kind: DRIVER_KIND,
    run_count: usize,
    profile: &Profile,
) -> anyhow::Result<Box<dyn Engine>> {
    let thread_per_exec = profile.thread_per_exec.unwrap_or(5);
    match kind {
        DRIVER_KIND::SQLITE_IN_MEM => {
            let rt = tokio::runtime::Runtime::new()?;
            let driver = rt.block_on(new_conn(DRIVER_KIND::SQLITE_IN_MEM))?;
            Ok(Box::new(SqliteEngine {
                rng: LcgRng::new(seed),
                sqlite_driver_box: Box::new(driver),
                run_count,
                thread_per_exec,
                stmt_prob: profile.stmt_prob.clone(),
                debug: profile.debug.clone(),
            }))
        }
        DRIVER_KIND::LIMBO_IN_MEM => {
            let rt = tokio::runtime::Runtime::new()?;
            let driver = rt.block_on(sqlsmith_rs_drivers::limbo_in_mem::LimboDriver::new())?;
            Ok(Box::new(LimboEngine {
                rng: LcgRng::new(seed),
                limbo_driver_box: Box::new(driver),
                run_count,
                thread_per_exec,
                stmt_prob: profile.stmt_prob.clone(),
                debug: profile.debug.clone(),
            }))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExecutionStats {
    pub elapsed_ms: u64,
    pub success_count: usize,
    pub failed_expected_count: usize,
    pub failed_new_count: usize,
    pub total_queries: usize,
    pub thread_count: usize,
    pub queries_per_second: f64,
    pub error_rate: f64,
    pub stmt_type_counts: HashMap<String, usize>,
    pub executor_id: String,
    pub timestamp: String,
}

impl ExecutionStats {
    pub fn new(
        elapsed: Duration,
        success_count: usize,
        failed_expected_count: usize,
        failed_new_count: usize,
        thread_count: usize,
        stmt_type_counts: HashMap<String, usize>,
        executor_id: String,
    ) -> Self {
        let total_queries = success_count + failed_expected_count + failed_new_count;
        let elapsed_ms = elapsed.as_millis() as u64;
        let queries_per_second = if elapsed_ms > 0 {
            (total_queries as f64) / (elapsed_ms as f64 / 1000.0)
        } else {
            0.0
        };
        let error_rate = if total_queries > 0 {
            (failed_new_count as f64 / total_queries as f64) * 100.0
        } else {
            0.0
        };

        Self {
            elapsed_ms,
            success_count,
            failed_expected_count,
            failed_new_count,
            total_queries,
            thread_count,
            queries_per_second,
            error_rate,
            stmt_type_counts,
            executor_id,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }
}

pub fn generate_sql_by_prob<F>(
    prob: &sqlsmith_rs_common::profile::StmtProb,
    rng: &mut LcgRng,
    mut get_stmt: F,
) -> String
where
    F: FnMut(SqlKind, &mut LcgRng) -> Option<String>,
{
    let thresholds = [
        (prob.SELECT, SqlKind::Select),
        (prob.INSERT, SqlKind::Insert),
        (prob.UPDATE, SqlKind::Update),
        (prob.DELETE, SqlKind::Delete),
        (prob.VACUUM, SqlKind::Vacuum),
        (prob.PRAGMA, SqlKind::Pragma),
        (prob.CREATE_TRIGGER, SqlKind::CreateTrigger),
        (prob.DROP_TRIGGER, SqlKind::DropTrigger),
        (prob.DATE_FUNC, SqlKind::DateFunc), // Added support for DATE_FUNC
        (prob.ALTER_TABLE, SqlKind::AlterTable), // Added support for ALTER_TABLE
        (prob.CREATE_TABLE, SqlKind::CreateTable),
        (prob.TRANSACTION, SqlKind::Transaction),
    ];

    let total: u64 = thresholds.iter().map(|(p, _)| p).sum();
    if total == 0 {
        return "SELECT 3;".to_string();
    }

    let r = (rng.rand().abs() as u64) % total;
    let mut accum = 0;

    for (prob, kind) in thresholds {
        accum += prob;
        if r < accum {
            return get_stmt(kind, rng).unwrap_or_else(|| "SELECT 5;".to_string());
        }
    }

    "SELECT 4;".to_string()
}

pub fn submit_stats_blocking(stats: ExecutionStats) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::blocking::Client::new();

    match client
        .post("http://127.0.0.1:8080/internal/stat/submit")
        .json(&stats)
        .send()
    {
        Ok(response) => {
            if response.status().is_success() {
                log::info!(
                    "Statistics submitted successfully for executor: {}",
                    stats.executor_id
                );
                Ok(())
            } else {
                let error_msg = format!("Failed to submit statistics: HTTP {}", response.status());
                log::warn!("{}", error_msg);
                Err(error_msg.into())
            }
        }
        Err(e) => {
            log::warn!("Failed to submit statistics to server: {}", e);
            Err(e.into())
        }
    }
}
