use clap::{Parser, Subcommand};
use miette::{NamedSource, Report};
use owo_colors::OwoColorize;
use std::process::ExitCode;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use std::{
    path::{Path, PathBuf},
    time::Instant,
};
use turso_test_runner::{
    DefaultDatabases, Format, GeneratorConfig, OutputFormat, ParseError, RunnerConfig, TestRunner,
    backends::cli::CliBackend, backends::js::JsBackend, backends::rust::RustBackend, create_output,
    generate_database, load_test_files, summarize, tcl_converter,
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

        /// Backend to use: "rust" (native), "cli" (subprocess), or "js" (JavaScript bindings)
        #[arg(long, default_value = "rust")]
        backend: String,

        /// Path to tursodb binary (only used with --backend cli)
        #[arg(long, default_value = "tursodb")]
        binary: PathBuf,

        /// Path to node binary (only used with --backend js)
        #[arg(long, default_value = "node")]
        node: PathBuf,

        /// Path to JavaScript runner script (only used with --backend js)
        #[arg(long)]
        js_script: Option<PathBuf>,

        /// Filter tests by name pattern
        #[arg(short, long)]
        filter: Option<String>,

        /// Number of parallel jobs
        #[arg(short, long, default_value_t = num_cpus::get())]
        jobs: usize,

        /// Output format (pretty, json)
        #[arg(short, long, default_value = "pretty")]
        output: String,

        /// Timeout per query in seconds (only used with --backend cli or js)
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

    /// [DEBUG ONLY] Generate test databases and save them to a directory.
    /// This is intended for debugging purposes only - to inspect the database
    /// files that the test runner generates for tests using :default: databases.
    GenerateDb {
        /// Output directory to save the generated databases
        #[arg(required = true)]
        output_dir: PathBuf,

        /// Number of users to generate (default: 10000)
        #[arg(long, default_value_t = 10000)]
        user_count: usize,

        /// Seed for reproducible random generation (default: 42)
        #[arg(long, default_value_t = 42)]
        seed: u64,

        /// Enable MVCC mode (experimental journal mode)
        #[arg(long)]
        mvcc: bool,
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
            node,
            js_script,
            filter,
            jobs,
            output,
            timeout,
            mvcc,
        } => {
            run_tests(
                paths, backend, binary, node, js_script, filter, jobs, output, timeout, mvcc,
            )
            .await
        }
        Commands::Check { paths } => check_files(paths),
        Commands::Convert {
            paths,
            output_dir,
            stdout,
            verbose,
        } => convert_files(paths, output_dir, stdout, verbose),
        Commands::GenerateDb {
            output_dir,
            user_count,
            seed,
            mvcc,
        } => generate_debug_databases(output_dir, user_count, seed, mvcc).await,
    }
}

/// Default seed for reproducible database generation
const DEFAULT_SEED: u64 = 42;
/// Default number of users to generate
const DEFAULT_USER_COUNT: usize = 10000;

#[allow(clippy::too_many_arguments)]
async fn run_tests(
    paths: Vec<PathBuf>,
    backend_type: String,
    binary: PathBuf,
    node: PathBuf,
    js_script: Option<PathBuf>,
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
            eprintln!("Error: {e}");
            return ExitCode::from(2);
        }
    };

    // Load and parse test files
    let loaded = match load_test_files(&paths).await {
        Ok(loaded) => loaded,
        Err(e) => {
            eprintln!("Error loading test files: {e}");
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
                eprintln!("Error generating default databases: {e}");
                return ExitCode::from(1);
            }
        }
    } else {
        None
    };

    // Create backend with resolver if we have generated databases
    let resolver = default_dbs.as_ref().map(|dbs| {
        Arc::new(DefaultDatabasesResolver(
            dbs.default_path.clone(),
            dbs.no_rowid_alias_path.clone(),
        ))
    });

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
            let mut backend = CliBackend::new(&binary)
                .with_timeout(Duration::from_secs(timeout))
                .with_mvcc(mvcc);
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
            let mut backend = RustBackend::new().with_mvcc(mvcc);
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
        "js" => {
            // Default: script is in the bindings/javascript directory
            let js_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../bindings/javascript");

            // Determine script path - use provided or default to bundled script
            let script_path = js_script.unwrap_or_else(|| js_dir.join("turso-sql-runner.mjs"));

            if !script_path.exists() {
                eprintln!(
                    "Error: JavaScript runner script not found at {}",
                    script_path.display()
                );
                eprintln!("Use --js-script to specify a custom script path");
                return ExitCode::from(2);
            }

            // Check that native bindings are built
            let native_dir = js_dir.join("packages/native");
            let has_native_bindings = native_dir
                .read_dir()
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .any(|e| e.path().extension().is_some_and(|ext| ext == "node"))
                })
                .unwrap_or(false);

            if !has_native_bindings {
                eprintln!("Error: JavaScript native bindings not found");
                eprintln!("Expected .node file in {}", native_dir.display());
                eprintln!();
                eprintln!("Build the bindings first:");
                eprintln!("  make -C turso-test-runner build-js-bindings");
                eprintln!();
                eprintln!("Or manually:");
                eprintln!("  cd bindings/javascript");
                eprintln!("  yarn install");
                eprintln!("  yarn workspace @tursodatabase/database-common build");
                eprintln!("  yarn workspace @tursodatabase/database build");
                return ExitCode::from(2);
            }

            let mut backend = JsBackend::new(&node, &script_path)
                .with_timeout(Duration::from_secs(timeout))
                .with_mvcc(mvcc);
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
            eprintln!("Error: unknown backend '{other}'. Use 'rust', 'cli', or 'js'");
            return ExitCode::from(2);
        }
    };

    let results = match results {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error running tests: {e}");
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
                                eprintln!("Error: {e}");
                                has_errors = true;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error: invalid glob pattern: {e}");
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

fn print_parse_error(path: &Path, content: &str, error: ParseError) {
    // Use miette for nice error display with source context
    let report = Report::from(error).with_source_code(NamedSource::new(
        path.display().to_string(),
        content.to_string(),
    ));
    eprintln!("{report:?}");
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
        eprintln!("{report:?}");
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
                format!("--- {key} tests ---").green().bold(),
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
                output_base.join(format!("{key}.sqltest"))
            } else {
                // Single file: use original name
                output_base.join(format!("{file_stem}.sqltest"))
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

/// Generate test databases for debugging purposes.
///
/// This command creates the same databases that the test runner generates
/// for tests using `:default:` and `:default-no-rowidalias:` database locations,
/// but saves them to a specified directory instead of a temporary location.
///
/// **DEBUG ONLY**: This is intended for inspecting generated databases during
/// debugging, not for normal test execution.
async fn generate_debug_databases(
    output_dir: PathBuf,
    user_count: usize,
    seed: u64,
    mvcc: bool,
) -> ExitCode {
    eprintln!(
        "{}",
        "WARNING: This command is for debugging only!"
            .yellow()
            .bold()
    );
    eprintln!();

    // Create output directory if it doesn't exist
    if !output_dir.exists() {
        if let Err(e) = std::fs::create_dir_all(&output_dir) {
            eprintln!(
                "{}: Failed to create output directory {}: {}",
                "Error".red().bold(),
                output_dir.display(),
                e
            );
            return ExitCode::from(1);
        }
    }

    // Generate default database (INTEGER PRIMARY KEY - has rowid alias)
    let default_db_path = output_dir.join("database.db");
    eprintln!(
        "Generating default database (INTEGER PRIMARY KEY) at {}...",
        default_db_path.display()
    );

    let config = GeneratorConfig {
        db_path: default_db_path.to_string_lossy().to_string(),
        user_count,
        seed,
        no_rowid_alias: false,
        mvcc,
    };

    if let Err(e) = generate_database(&config).await {
        eprintln!(
            "{}: Failed to generate default database: {}",
            "Error".red().bold(),
            e
        );
        return ExitCode::from(1);
    }

    eprintln!("  {} {}", "OK".green().bold(), default_db_path.display());

    // Generate no-rowid-alias database (INT PRIMARY KEY - no rowid alias)
    let no_rowid_db_path = output_dir.join("database-no-rowidalias.db");
    eprintln!(
        "Generating no-rowid-alias database (INT PRIMARY KEY) at {}...",
        no_rowid_db_path.display()
    );

    let config = GeneratorConfig {
        db_path: no_rowid_db_path.to_string_lossy().to_string(),
        user_count,
        seed,
        no_rowid_alias: true,
        mvcc,
    };

    if let Err(e) = generate_database(&config).await {
        eprintln!(
            "{}: Failed to generate no-rowid-alias database: {}",
            "Error".red().bold(),
            e
        );
        return ExitCode::from(1);
    }

    eprintln!("  {} {}", "OK".green().bold(), no_rowid_db_path.display());

    // Print summary
    eprintln!();
    eprintln!(
        "{}: Generated {} databases in {}",
        "Done".green().bold(),
        2,
        output_dir.display()
    );
    eprintln!("  - database.db (INTEGER PRIMARY KEY, rowid alias enabled)");
    eprintln!("  - database-no-rowidalias.db (INT PRIMARY KEY, no rowid alias)");
    eprintln!();
    eprintln!("Configuration: seed={seed}, user_count={user_count}, mvcc={mvcc}");

    ExitCode::SUCCESS
}
