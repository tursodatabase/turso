use clap::{Parser, Subcommand};
use miette::{NamedSource, Report};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use turso_test_runner::{
    backends::cli::CliBackend, create_output, summarize, Format, OutputFormat, ParseError,
    RunnerConfig, TestRunner,
};

#[derive(Parser)]
#[command(name = "turso-test-runner")]
#[command(about = "SQL test runner for Turso/Limbo")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run tests
    Run {
        /// Test files or directories
        #[arg(required = true)]
        paths: Vec<PathBuf>,

        /// Path to tursodb binary
        #[arg(long, default_value = "tursodb")]
        binary: PathBuf,

        /// Filter tests by name pattern
        #[arg(short, long)]
        filter: Option<String>,

        /// Number of parallel jobs
        #[arg(short, long, default_value_t = num_cpus::get())]
        jobs: usize,

        /// Output format (pretty, json)
        #[arg(short, long, default_value = "pretty")]
        output: String,

        /// Timeout per query in seconds
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },

    /// Validate test file syntax
    Check {
        /// Test files or directories
        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            paths,
            binary,
            filter,
            jobs,
            output,
            timeout,
        } => run_tests(paths, binary, filter, jobs, output, timeout).await,
        Commands::Check { paths } => check_files(paths),
    }
}

async fn run_tests(
    paths: Vec<PathBuf>,
    binary: PathBuf,
    filter: Option<String>,
    jobs: usize,
    output_format: String,
    timeout: u64,
) -> ExitCode {
    // Parse output format
    let format: Format = match output_format.parse() {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ExitCode::from(2);
        }
    };

    // Create backend
    let backend = CliBackend::new(&binary).with_timeout(Duration::from_secs(timeout));

    // Create runner config
    let mut config = RunnerConfig::default().with_max_jobs(jobs);
    if let Some(f) = filter {
        config = config.with_filter(f);
    }

    // Create runner
    let runner = TestRunner::new(backend).with_config(config);

    // Run tests
    let results = match runner.run_paths(&paths).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error running tests: {}", e);
            return ExitCode::from(1);
        }
    };

    // Create output formatter
    let mut output: Box<dyn OutputFormat> = create_output(format);

    // Write results
    for file_result in &results {
        output.write_file(file_result);
    }

    // Write summary
    let summary = summarize(&results);
    output.write_summary(&summary);
    output.flush();

    // Return appropriate exit code
    if summary.is_success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn check_files(paths: Vec<PathBuf>) -> ExitCode {
    let mut has_errors = false;

    for path in &paths {
        if path.is_dir() {
            // Glob for .sqltest files
            let pattern = path.join("**/*.sqltest");
            let pattern_str = pattern.to_string_lossy();

            match glob::glob(&pattern_str) {
                Ok(entries) => {
                    for entry in entries {
                        match entry {
                            Ok(file_path) => {
                                if !check_single_file(&file_path) {
                                    has_errors = true;
                                }
                            }
                            Err(e) => {
                                eprintln!("Error: {}", e);
                                has_errors = true;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error: invalid glob pattern: {}", e);
                    has_errors = true;
                }
            }
        } else if path.is_file() {
            if !check_single_file(path) {
                has_errors = true;
            }
        } else {
            eprintln!("Error: {} does not exist", path.display());
            has_errors = true;
        }
    }

    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn check_single_file(path: &PathBuf) -> bool {
    match std::fs::read_to_string(path) {
        Ok(content) => match turso_test_runner::parse(&content) {
            Ok(file) => {
                println!(
                    "{} - OK ({} databases, {} setups, {} tests)",
                    path.display(),
                    file.databases.len(),
                    file.setups.len(),
                    file.tests.len()
                );
                true
            }
            Err(e) => {
                print_parse_error(path, &content, e);
                false
            }
        },
        Err(e) => {
            eprintln!("{} - ERROR: {}", path.display(), e);
            false
        }
    }
}

fn print_parse_error(path: &PathBuf, content: &str, error: ParseError) {
    // Use miette for nice error display with source context
    let report = Report::from(error)
        .with_source_code(NamedSource::new(path.display().to_string(), content.to_string()));
    eprintln!("{:?}", report);
}
