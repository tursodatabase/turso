use clap::{Parser, Subcommand};
use miette::{NamedSource, Report};
use owo_colors::OwoColorize;
use std::process::ExitCode;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use std::{path::PathBuf, time::Instant};
use turso_test_runner::{
    DefaultDatabases, Format, OutputFormat, ParseError, RunnerConfig, TestRunner,
    backends::cli::CliBackend, backends::rust::RustBackend, create_output, load_test_files,
    summarize, tcl_converter,
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

        /// Backend to use: "rust" (native) or "cli" (subprocess)
        #[arg(long, default_value = "rust")]
        backend: String,

        /// Path to tursodb binary (only used with --backend cli)
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

        /// Timeout per query in seconds (only used with --backend cli)
        #[arg(long, default_value_t = 90)]
        timeout: u64,

        /// Enable MVCC mode (experimental journal mode)
        #[arg(long)]
        mvcc: bool,
    },

    /// Validate test file syntax
    Check {
        /// Test files or directories
        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },

    /// Convert Tcl .test files to .sqltest format
    Convert {
        /// Tcl test files to convert
        #[arg(required = true)]
        paths: Vec<PathBuf>,

        /// Output directory (default: write next to source files)
        #[arg(short, long)]
        output_dir: Option<PathBuf>,

        /// Print to stdout instead of writing files
        #[arg(long)]
        stdout: bool,

        /// Show verbose warnings
        #[arg(short, long)]
        verbose: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            paths,
            backend,
            binary,
            filter,
            jobs,
            output,
            timeout,
            mvcc,
        } => run_tests(paths, backend, binary, filter, jobs, output, timeout, mvcc).await,
        Commands::Check { paths } => check_files(paths),
        Commands::Convert {
            paths,
            output_dir,
            stdout,
            verbose,
        } => convert_files(paths, output_dir, stdout, verbose),
    }
}

/// Default seed for reproducible database generation
const DEFAULT_SEED: u64 = 42;
/// Default number of users to generate
const DEFAULT_USER_COUNT: usize = 10000;

async fn run_tests(
    paths: Vec<PathBuf>,
    backend_type: String,
    binary: PathBuf,
    filter: Option<String>,
    jobs: usize,
    output_format: String,
    timeout: u64,
    mvcc: bool,
) -> ExitCode {
    // Resolve paths, trying to add .sqltest extension if missing
    let mut resolved_paths = Vec::new();
    let mut missing = Vec::new();

    for path in paths {
        if path.exists() {
            resolved_paths.push(path);
        } else {
            // Try adding .sqltest extension
            let with_ext = path.with_extension("sqltest");
            if with_ext.exists() {
                resolved_paths.push(with_ext);
            } else {
                missing.push(path);
            }
        }
    }

    if !missing.is_empty() {
        eprintln!("Error: the following paths do not exist:");
        for path in missing {
            eprintln!("  - {}", path.display());
        }
        return ExitCode::from(1);
    }

    let paths = resolved_paths;

    // Parse output format
    let format: Format = match output_format.parse() {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ExitCode::from(2);
        }
    };

    // Load and parse test files
    let loaded = match load_test_files(&paths).await {
        Ok(loaded) => loaded,
        Err(e) => {
            eprintln!("Error loading test files: {}", e);
            return ExitCode::from(1);
        }
    };

    // Check if we need to generate default databases
    let needs = DefaultDatabases::scan_needs(loaded.test_files());

    // Generate default databases if needed
    let default_dbs = if needs.any() {
        eprintln!("Generating default databases...");
        match DefaultDatabases::generate(needs, DEFAULT_SEED, DEFAULT_USER_COUNT, mvcc).await {
            Ok(dbs) => dbs,
            Err(e) => {
                eprintln!("Error generating default databases: {}", e);
                return ExitCode::from(1);
            }
        }
    } else {
        None
    };

    // Create backend with resolver if we have generated databases
    let resolver = if let Some(ref dbs) = default_dbs {
        Some(Arc::new(DefaultDatabasesResolver(
            dbs.default_path.clone(),
            dbs.no_rowid_alias_path.clone(),
        )))
    } else {
        None
    };

    // Create runner config
    let mut config = RunnerConfig::default().with_max_jobs(jobs).with_mvcc(mvcc);
    if let Some(f) = filter {
        config = config.with_filter(f);
    }

    // Create output formatter
    let mut output: Box<dyn OutputFormat> = create_output(format);
    let start = Instant::now();

    // Run tests based on backend selection
    let results = match backend_type.as_str() {
        "cli" => {
            let mut backend = if let Some(ref dbs) = default_dbs {
                CliBackend::new(&binary)
                    .with_timeout(Duration::from_secs(timeout))
                    .with_mvcc(mvcc)
                    .with_default_db_resolver(Arc::new(DefaultDatabasesResolver(
                        dbs.default_path.clone(),
                        dbs.no_rowid_alias_path.clone(),
                    )))
            } else {
                CliBackend::new(&binary)
                    .with_timeout(Duration::from_secs(timeout))
                    .with_mvcc(mvcc)
            };
            if let Some(resolver) = resolver {
                backend = backend.with_default_db_resolver(resolver);
            }
            let runner = TestRunner::new(backend).with_config(config);
            runner
                .run_loaded_tests(loaded, |result| {
                    output.write_test(result);
                    output.flush();
                })
                .await
        }
        "rust" => {
            let mut backend = RustBackend::new();
            if let Some(resolver) = resolver {
                backend = backend.with_default_db_resolver(resolver);
            }
            let runner = TestRunner::new(backend).with_config(config);
            runner
                .run_loaded_tests(loaded, |result| {
                    output.write_test(result);
                    output.flush();
                })
                .await
        }
        other => {
            eprintln!("Error: unknown backend '{}'. Use 'rust' or 'cli'", other);
            return ExitCode::from(2);
        }
    };

    let results = match results {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error running tests: {}", e);
            return ExitCode::from(1);
        }
    };

    // Write summary
    let summary = summarize(start, &results);
    output.write_summary(&summary);
    output.flush();

    // Return appropriate exit code
    if summary.is_success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

/// Simple resolver that holds the paths directly
struct DefaultDatabasesResolver(Option<PathBuf>, Option<PathBuf>);

impl turso_test_runner::DefaultDatabaseResolver for DefaultDatabasesResolver {
    fn resolve(&self, location: &turso_test_runner::DatabaseLocation) -> Option<PathBuf> {
        match location {
            turso_test_runner::DatabaseLocation::Default => self.0.clone(),
            turso_test_runner::DatabaseLocation::DefaultNoRowidAlias => self.1.clone(),
            _ => None,
        }
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
    let report = Report::from(error).with_source_code(NamedSource::new(
        path.display().to_string(),
        content.to_string(),
    ));
    eprintln!("{:?}", report);
}

fn convert_files(
    paths: Vec<PathBuf>,
    output_dir: Option<PathBuf>,
    to_stdout: bool,
    verbose: bool,
) -> ExitCode {
    let mut total_tests = 0;
    let mut total_warnings = 0;
    let mut files_processed = 0;

    for path in &paths {
        if path.is_dir() {
            // Glob for .test files
            let pattern = path.join("*.test");
            let pattern_str = pattern.to_string_lossy();

            match glob::glob(&pattern_str) {
                Ok(entries) => {
                    for entry in entries {
                        match entry {
                            Ok(file_path) => {
                                let (tests, warnings) = convert_single_file(
                                    &file_path,
                                    &output_dir,
                                    to_stdout,
                                    verbose,
                                );
                                total_tests += tests;
                                total_warnings += warnings;
                                files_processed += 1;
                            }
                            Err(e) => {
                                eprintln!("{}: {}", "Error".red().bold(), e);
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{}: invalid glob pattern: {}", "Error".red().bold(), e);
                }
            }
        } else if path.is_file() {
            let (tests, warnings) = convert_single_file(path, &output_dir, to_stdout, verbose);
            total_tests += tests;
            total_warnings += warnings;
            files_processed += 1;
        } else {
            eprintln!(
                "{}: {} does not exist",
                "Error".red().bold(),
                path.display()
            );
        }
    }

    // Print summary
    println!();
    println!(
        "{}: {} files processed, {} tests converted, {} warnings",
        "Summary".cyan().bold(),
        files_processed,
        total_tests,
        total_warnings
    );

    if total_warnings > 0 {
        println!(
            "{}",
            "Some tests require manual conversion. See warnings above.".yellow()
        );
    }

    ExitCode::SUCCESS
}

fn convert_single_file(
    path: &PathBuf,
    output_dir: &Option<PathBuf>,
    to_stdout: bool,
    _verbose: bool,
) -> (usize, usize) {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("{} - {}: {}", path.display(), "ERROR".red().bold(), e);
            return (0, 0);
        }
    };

    let path_display = Rc::new(path.display().to_string());

    let source_name = path.file_name().unwrap_or_default().to_string_lossy();
    let result = tcl_converter::convert(&content, &source_name);

    let test_count = result.tests.len();
    let warning_count = result.warnings.len();

    // Print file header
    println!(
        "{} - {} tests, {} warnings",
        path_display.cyan(),
        test_count,
        warning_count
    );

    // Print warnings using miette
    for warning in &result.warnings {
        let report = warning.to_report(&path_display, content.clone());
        // Use debug format {:?} to get full miette diagnostic with source snippets
        eprintln!("{:?}", report);
    }

    if test_count == 0 {
        println!("  {} No tests found to convert", "INFO".blue());
        return (0, warning_count);
    }

    // Generate output - may produce multiple files per database type
    let generated = tcl_converter::generate_sqltest(&result);

    if to_stdout {
        for (key, file) in &generated.files {
            println!(
                "\n{} ({} tests)",
                format!("--- {} tests ---", key).green().bold(),
                file.test_count
            );
            println!("{}", file.content);
        }
        println!("{}", "--- End ---".green().bold());
    } else {
        let file_stem = path.file_stem().unwrap_or_default().to_string_lossy();
        let num_files = generated.files.len();

        // Determine base output path
        let base_dir = output_dir.clone().unwrap_or_else(|| {
            path.parent()
                .unwrap_or(std::path::Path::new("."))
                .to_path_buf()
        });

        // If multiple files, create a subdirectory
        let (output_base, use_subdir) = if num_files > 1 {
            let subdir = base_dir.join(file_stem.as_ref());
            if !subdir.exists() {
                if let Err(e) = std::fs::create_dir_all(&subdir) {
                    eprintln!(
                        "  {} Failed to create directory {}: {}",
                        "ERROR".red().bold(),
                        subdir.display(),
                        e
                    );
                    return (0, warning_count);
                }
            }
            (subdir, true)
        } else {
            // Ensure base dir exists
            if !base_dir.exists() {
                if let Err(e) = std::fs::create_dir_all(&base_dir) {
                    eprintln!(
                        "  {} Failed to create directory {}: {}",
                        "ERROR".red().bold(),
                        base_dir.display(),
                        e
                    );
                    return (0, warning_count);
                }
            }
            (base_dir, false)
        };

        // Write each file
        for (key, file) in &generated.files {
            let output_path = if use_subdir {
                // In subdirectory: use key as filename
                output_base.join(format!("{}.sqltest", key))
            } else {
                // Single file: use original name
                output_base.join(format!("{}.sqltest", file_stem))
            };

            match std::fs::write(&output_path, &file.content) {
                Ok(_) => {
                    println!(
                        "  {} Written to {}",
                        "OK".green().bold(),
                        output_path.display()
                    );
                }
                Err(e) => {
                    eprintln!(
                        "  {} Failed to write {}: {}",
                        "ERROR".red().bold(),
                        output_path.display(),
                        e
                    );
                }
            }
        }
    }

    (test_count, warning_count)
}
