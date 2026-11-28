use async_trait::async_trait;
use owo_colors::OwoColorize;
use sqltest::cli::{BackendRun, Flavor};
use sqltest::{BackendError, DefaultDatabaseResolver, FileResult, LoadedTests};
use sqltest_sqlite::generator::{
    DefaultDatabases, GeneratorConfig, INTEGRITY_FIXTURE_RELATIVE_PATHS, generate_database,
    generate_integrity_fixture,
};
use sqltest_sqlite::rust_backend::RustBackend;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

const DEFAULT_SEED: u64 = 42;
const DEFAULT_USER_COUNT: usize = 10000;

#[tokio::main]
async fn main() -> ExitCode {
    sqltest::cli::run(&SqliteFlavor).await
}

struct SqliteFlavor;

#[async_trait(?Send)]
impl Flavor for SqliteFlavor {
    fn bin_name(&self) -> &'static str {
        "sqltest"
    }

    fn extra_backends(&self) -> &'static [&'static str] {
        &["rust"]
    }

    fn default_backend(&self) -> &'static str {
        "rust"
    }

    fn generates_databases(&self) -> bool {
        true
    }

    async fn prepare_databases(
        &self,
        loaded: &LoadedTests,
        mvcc: bool,
    ) -> Result<Option<Arc<dyn DefaultDatabaseResolver>>, ExitCode> {
        let integrity_fixtures = collect_integrity_fixtures(loaded.test_files());
        if !integrity_fixtures.is_empty() {
            eprintln!("Generating integrity-check fixtures...");
            for rel in integrity_fixtures {
                let fixture_path = PathBuf::from(&rel);
                if let Err(e) = generate_integrity_fixture(&fixture_path, &rel).await {
                    eprintln!(
                        "Error generating integrity fixture '{}': {e}",
                        fixture_path.display()
                    );
                    return Err(ExitCode::from(1));
                }
            }
        }

        let needs = DefaultDatabases::scan_needs(loaded.test_files());
        if !needs.any() {
            return Ok(None);
        }
        eprintln!("Generating default databases...");
        match DefaultDatabases::generate(needs, DEFAULT_SEED, DEFAULT_USER_COUNT, mvcc).await {
            Ok(Some(dbs)) => Ok(Some(Arc::new(GeneratedDatabases(dbs)))),
            Ok(None) => Ok(None),
            Err(e) => {
                eprintln!("Error generating default databases: {e}");
                Err(ExitCode::from(1))
            }
        }
    }

    async fn run_backend(
        &self,
        name: &str,
        run: BackendRun<'_>,
    ) -> Option<Result<Vec<FileResult>, BackendError>> {
        if name != "rust" {
            return None;
        }
        let mut backend = RustBackend::new()
            .with_mvcc(run.mvcc)
            .with_spec_model(run.spec_model);
        if let Some(resolver) = run.resolver.clone() {
            backend = backend.with_default_db_resolver(resolver);
        }
        Some(run.run(backend).await)
    }

    async fn generate_debug_databases(
        &self,
        output_dir: PathBuf,
        user_count: usize,
        seed: u64,
        mvcc: bool,
    ) -> ExitCode {
        generate_debug_databases(output_dir, user_count, seed, mvcc).await
    }
}

/// Keeps the generated databases alive while the tests run.
struct GeneratedDatabases(DefaultDatabases);

impl DefaultDatabaseResolver for GeneratedDatabases {
    fn resolve(&self, location: &sqltest::DatabaseLocation) -> Option<PathBuf> {
        match location {
            sqltest::DatabaseLocation::Default => self.0.default_path.clone(),
            sqltest::DatabaseLocation::DefaultNoRowidAlias => self.0.no_rowid_alias_path.clone(),
            _ => None,
        }
    }
}

fn collect_integrity_fixtures<'a>(
    test_files: impl Iterator<Item = &'a sqltest::TestFile>,
) -> Vec<String> {
    let mut fixtures = std::collections::BTreeSet::new();
    for file in test_files {
        for db in &file.databases {
            if let sqltest::DatabaseLocation::Path(path) = &db.location {
                for rel in INTEGRITY_FIXTURE_RELATIVE_PATHS {
                    if path == Path::new(rel) {
                        fixtures.insert((*rel).to_string());
                    }
                }
            }
        }
    }
    fixtures.into_iter().collect()
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
