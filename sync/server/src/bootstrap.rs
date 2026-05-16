use anyhow::Result;
use clap::Parser;
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
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags};

pub struct ServerApp {
    pub opts: ServerOpts,
    pub connection: Arc<Connection>,
    pub interrupt_count: Arc<AtomicUsize>,
    _tracing_guard: WorkerGuard,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "turso-sync-server")]
#[command(version, about = "Standalone Turso sync server")]
pub struct ServerOpts {
    #[clap(index = 1, help = "SQLite database file", default_value = ":memory:")]
    pub database: PathBuf,
    #[clap(
        long = "sync-server",
        visible_alias = "listen",
        help = "Listen address for the sync server (for example 0.0.0.0:8080)"
    )]
    pub sync_server_address: String,
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
        let connection = open_connection(&opts)?;
        let interrupt_count = Arc::new(AtomicUsize::new(0));

        {
            let interrupt_count = Arc::clone(&interrupt_count);
            ctrlc::set_handler(move || {
                interrupt_count.fetch_add(1, Ordering::Release);
            })?;
        }

        Ok(Self {
            opts,
            connection,
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

fn open_connection(opts: &ServerOpts) -> Result<Arc<Connection>> {
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

    let db_file = normalize_db_path(opts.database.to_string_lossy().to_string());

    if db_file.starts_with("file:") {
        let (_, connection) = Connection::from_uri(&db_file, db_opts)?;
        return Ok(connection);
    }

    let flags = if opts.readonly {
        OpenFlags::default().union(OpenFlags::ReadOnly)
    } else {
        OpenFlags::default()
    };

    let (_, database) = Database::open_new(
        &db_file,
        opts.vfs.as_deref(),
        flags,
        db_opts.turso_cli(),
        None,
    )?;

    database.connect().map_err(Into::into)
}

/// Normalize `path?key=val` to `file:path?key=val` so query parameters
/// are parsed as URI options (e.g. `?locking=shared_reads`) instead of
/// being treated as part of the filename.
fn normalize_db_path(db_file: String) -> String {
    if db_file.starts_with("file:") {
        return db_file;
    }

    if let Some(pos) = db_file.rfind('?') {
        let query = &db_file[pos + 1..];
        if query.contains('=') {
            let path = &db_file[..pos];
            let encoded_path = path.replace('?', "%3F");
            return format!("file:{encoded_path}?{query}");
        }
    }

    db_file
}

#[cfg(test)]
mod tests {
    use super::normalize_db_path;

    #[test]
    fn normalize_db_path_adds_file_prefix_for_query_params() {
        assert_eq!(
            normalize_db_path("test.db?locking=shared_reads".into()),
            "file:test.db?locking=shared_reads"
        );
    }

    #[test]
    fn normalize_db_path_preserves_existing_file_prefix() {
        assert_eq!(
            normalize_db_path("file:test.db?locking=shared_reads".into()),
            "file:test.db?locking=shared_reads"
        );
    }

    #[test]
    fn normalize_db_path_keeps_plain_paths_unchanged() {
        assert_eq!(normalize_db_path("test.db".into()), "test.db");
    }
}
