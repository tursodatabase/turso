use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use turso_bench::{BenchConfig, BenchmarkRunner};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "turso_bench=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse command line arguments
    let config = BenchConfig::parse();

    // Show help and version information
    println!("TursoDB Benchmark Tool v{}", env!("CARGO_PKG_VERSION"));
    println!("A benchmarking tool for TursoDB compatible with mobibench functionality\n");

    // Create and run benchmark
    let runner = BenchmarkRunner::new(config)?;
    runner.run().await?;

    Ok(())
}