use anyhow::Result;
use clap::{ArgGroup, Parser};
use std::{
    io::IsTerminal,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use turso_core::{DatabaseOpts, OpenFlags};

use crate::database::{open_connection, DatabaseOpenOptions, DatabaseProvider};

pub struct ServerApp {
    pub opts: ServerOpts,
    pub database_provider: Arc<DatabaseProvider>,
    pub interrupt_count: Arc<AtomicUsize>,
    _tracing_guard: WorkerGuard,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "turso-sync-server")]
#[command(version, about = "Standalone Turso sync server")]
#[command(group(
    ArgGroup::new("database_source")
        .required(true)
        .args(["db_file", "db_dir"])
))]
pub struct ServerOpts {
    #[clap(
        long = "sync-server",
        visible_alias = "listen",
        help = "Listen address for the sync server (for example 0.0.0.0:8080)"
    )]
    pub sync_server_address: String,
    #[clap(long, help = "SQLite database file for single-db mode")]
    pub db_file: Option<PathBuf>,
    #[clap(
        long,
        help = "Directory containing database files for /db/{name} routing"
    )]
    pub db_dir: Option<PathBuf>,
    #[clap(
        short = 'v',
        long,
        help = "Select VFS. options are io_uring (if feature enabled), experimental_win_iocp (if feature enabled on windows), memory, and syscall"
    )]
    pub vfs: Option<String>,
    #[clap(long, help = "Open the database in read-only mode")]
    pub readonly: bool,
    #[clap(short = 't', long, help = "Specify output file for log traces")]
    pub tracing_output: Option<String>,
    #[clap(long, help = "Enable experimental views feature")]
    pub experimental_views: bool,
    #[clap(
        long,
        help = "Enable experimental custom types (CREATE TYPE / DROP TYPE)"
    )]
    pub experimental_custom_types: bool,
    #[clap(long, help = "Enable experimental encryption feature")]
    pub experimental_encryption: bool,
    #[clap(long, help = "Enable experimental index method feature")]
    pub experimental_index_method: bool,
    #[clap(long, help = "Enable experimental autovacuum feature")]
    pub experimental_autovacuum: bool,
    #[clap(long, help = "Enable experimental vacuum feature")]
    pub experimental_vacuum: bool,
    #[clap(long, help = "Enable experimental attach feature")]
    pub experimental_attach: bool,
    #[clap(long, help = "Enable experimental generated columns feature")]
    pub experimental_generated_columns: bool,
    #[clap(long, help = "Enable experimental WITHOUT ROWID tables feature")]
    pub experimental_without_rowid: bool,
    #[clap(
        long,
        help = "Enable experimental multiprocess WAL coordination (on Windows, use --vfs experimental_win_iocp)"
    )]
    pub experimental_multiprocess_wal: bool,
    #[clap(
        long,
        help = "Enable unsafe testing features (e.g. sqlite_dbpage writes)"
    )]
    pub unsafe_testing: bool,
}

impl ServerApp {
    pub fn new() -> Result<Self> {
        let opts = ServerOpts::parse();
        let tracing_guard = init_tracing(&opts)?;
        let database_provider = Arc::new(create_database_provider(&opts)?);
        let interrupt_count = Arc::new(AtomicUsize::new(0));

        {
            let interrupt_count = Arc::clone(&interrupt_count);
            ctrlc::set_handler(move || {
                interrupt_count.fetch_add(1, Ordering::Release);
            })?;
        }

        Ok(Self {
            opts,
            database_provider,
            interrupt_count,
            _tracing_guard: tracing_guard,
        })
    }
}

fn init_tracing(opts: &ServerOpts) -> Result<WorkerGuard, std::io::Error> {
    let ((non_blocking, guard), should_emit_ansi) = if let Some(file) = &opts.tracing_output {
        (
            tracing_appender::non_blocking(
                std::fs::File::options()
                    .append(true)
                    .create(true)
                    .open(file)?,
            ),
            false,
        )
    } else {
        (
            tracing_appender::non_blocking(std::io::stderr()),
            std::io::stderr().is_terminal(),
        )
    };

    let default_env_filter = EnvFilter::builder()
        .with_default_directive(tracing::level_filters::LevelFilter::WARN.into())
        .from_env_lossy();

    if let Err(error) = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_ansi(should_emit_ansi),
        )
        .with(default_env_filter)
        .try_init()
    {
        eprintln!("Unable to setup tracing appender: {error:?}");
    }

    Ok(guard)
}

fn create_database_provider(opts: &ServerOpts) -> Result<DatabaseProvider> {
    let open_options = create_open_options(opts);

    if let Some(db_dir) = &opts.db_dir {
        if !db_dir.is_dir() {
            return Err(anyhow::anyhow!(
                "database directory does not exist: {}",
                db_dir.display()
            ));
        }
        return Ok(DatabaseProvider::directory(db_dir.clone(), open_options));
    }

    let db_file = opts
        .db_file
        .clone()
        .expect("db_file must be set when db_dir is not set");
    let connection = open_connection(&db_file, &open_options)?;

    Ok(DatabaseProvider::single(connection))
}

fn create_open_options(opts: &ServerOpts) -> DatabaseOpenOptions {
    let db_opts = DatabaseOpts::new()
        .with_views(opts.experimental_views)
        .with_custom_types(opts.experimental_custom_types)
        .with_encryption(opts.experimental_encryption)
        .with_index_method(opts.experimental_index_method)
        .with_autovacuum(opts.experimental_autovacuum)
        .with_vacuum(opts.experimental_vacuum)
        .with_attach(opts.experimental_attach)
        .with_generated_columns(opts.experimental_generated_columns)
        .with_without_rowid(opts.experimental_without_rowid)
        .with_multiprocess_wal(opts.experimental_multiprocess_wal)
        .with_unsafe_testing(opts.unsafe_testing);

    let flags = if opts.readonly {
        OpenFlags::default().union(OpenFlags::ReadOnly)
    } else {
        OpenFlags::default()
    };

    DatabaseOpenOptions {
        vfs: opts.vfs.clone(),
        flags,
        db_opts,
    }
}

#[cfg(test)]
mod tests {
    use super::ServerOpts;
    use clap::Parser;

    #[test]
    fn db_file_and_db_dir_conflict() {
        let error = ServerOpts::try_parse_from([
            "turso-sync-server",
            "--sync-server",
            "127.0.0.1:8080",
            "--db-file",
            "server.db",
            "--db-dir",
            "dbs",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn database_source_is_required() {
        let error =
            ServerOpts::try_parse_from(["turso-sync-server", "--sync-server", "127.0.0.1:8080"])
                .unwrap_err();

        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }
}
