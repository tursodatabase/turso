use crate::{
    config::BenchConfig,
    database::{DatabaseManager, OperationMode},
    metrics::{BenchmarkMetrics, OperationMetrics},
    Result,
};
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{info, warn};

/// Main benchmark runner
pub struct BenchmarkRunner {
    config: BenchConfig,
}

impl BenchmarkRunner {
    /// Create a new benchmark runner
    pub fn new(config: BenchConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Run the benchmark
    pub async fn run(&self) -> Result<()> {
        info!("Starting TursoDB benchmark");
        self.print_config();

        let mut benchmark_metrics = BenchmarkMetrics::new();
        benchmark_metrics.start_timing();

        // Create and run benchmark threads
        let mut join_set = JoinSet::new();
        
        for thread_id in 0..self.config.num_threads {
            let config = self.config.clone();
            
            join_set.spawn(async move {
                Self::run_thread(thread_id, config).await
            });
        }

        // Wait for all threads to complete and collect metrics
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(metrics)) => {
                    benchmark_metrics.add_thread_metrics(metrics);
                }
                Ok(Err(e)) => {
                    warn!("Thread failed: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    warn!("Thread panicked: {}", e);
                    return Err(anyhow::anyhow!("Thread panicked: {}", e));
                }
            }
        }

        // Calculate and display results
        let stats = benchmark_metrics.calculate_combined_stats();
        let operation_name = match OperationMode::try_from(self.config.access_mode)? {
            OperationMode::Insert => "Inserts",
            OperationMode::Update => "Updates", 
            OperationMode::Delete => "Deletes",
        };

        stats.print_results(operation_name, self.config.num_threads);

        info!("Benchmark completed successfully");
        Ok(())
    }

    /// Run benchmark for a single thread
    async fn run_thread(thread_id: u32, config: BenchConfig) -> Result<OperationMetrics> {
        info!("Starting thread {}", thread_id);

        // Create metrics collector
        let latency_file = config.latency_file.as_ref().map(|f| {
            format!("{}_thread_{}", f, thread_id)
        });
        let iops_file = config.iops_file.as_ref().map(|f| {
            format!("{}_thread_{}", f, thread_id)
        });

        let mut metrics = OperationMetrics::new(
            latency_file.as_deref(),
            iops_file.as_deref(),
        )?;

        // Create database manager
        let mut db_manager = DatabaseManager::new(config.clone(), thread_id).await?;

        // Start timing
        metrics.start_timing();

        // Execute operations
        db_manager.execute_operations(&mut metrics).await?;

        // Clean up
        db_manager.close()?;

        info!("Thread {} completed", thread_id);
        Ok(metrics)
    }

    /// Print benchmark configuration
    fn print_config(&self) {
        println!("-----------------------------------------");
        println!("[TursoDB Benchmark Configuration]");
        println!("-----------------------------------------");
        
        let operation_name = match OperationMode::try_from(self.config.access_mode) {
            Ok(OperationMode::Insert) => "Insert",
            Ok(OperationMode::Update) => "Update",
            Ok(OperationMode::Delete) => "Delete",
            Err(_) => "Unknown",
        };

        println!("Operation Mode: {} ({})", operation_name, self.config.access_mode);
        println!("Database Path: {}", self.config.path);
        println!("Number of Threads: {}", self.config.num_threads);
        println!("Transactions per Thread: {}", self.config.transactions);
        println!("Number of Tables: {}", self.config.num_tables);
        println!("Number of Databases: {}", self.config.num_databases);
        
        if self.config.interval_ms > 0 {
            println!("Transaction Interval: {} ms", self.config.interval_ms);
        }
        
        if self.config.overlap_ratio > 0 {
            println!("Overlap Ratio: {}%", self.config.overlap_ratio);
        }
        
        if self.config.random_insert {
            println!("Random Insert Order: Enabled");
        }
        
        println!("MVCC: {}", if self.config.enable_mvcc { "Enabled" } else { "Disabled" });
        println!("Indexes: {}", if self.config.enable_indexes { "Enabled" } else { "Disabled" });
        println!("Views: {}", if self.config.enable_views { "Enabled" } else { "Disabled" });
        
        if let Some(ref latency_file) = self.config.latency_file {
            println!("Latency Output: {}", latency_file);
        }
        
        if let Some(ref iops_file) = self.config.iops_file {
            println!("IOPS Output: {}", iops_file);
        }
        
        println!();
    }
}