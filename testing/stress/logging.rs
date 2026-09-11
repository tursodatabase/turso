use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{reload, EnvFilter};

const LOG_LEVEL_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const LOG_LEVEL_FILE: &str = "RUST_LOG";

pub type LogLevelReloadHandle = reload::Handle<EnvFilter, tracing_subscriber::Registry>;

pub struct Tracer {
    _guard: WorkerGuard,
}

impl Tracer {
    pub(crate) fn new(log_path: &str) -> Result<Self, std::io::Error> {
        let log_file = std::fs::File::create(log_path)?;
        let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let (filter_layer, reload_handle) = reload::Layer::new(filter);

        if let Err(e) = tracing_subscriber::registry()
            .with(filter_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false)
                    .with_line_number(true)
                    .with_thread_ids(true)
                    .json(),
            )
            .try_init()
        {
            println!("Unable to setup tracing appender: {e:?}");
        }

        Self::spawn_log_level_watcher(reload_handle);

        Ok(Self { _guard: guard })
    }

    /// Spawns a background thread that watches for a RUST_LOG file and dynamically
    /// updates the log level when the file contents change.
    fn spawn_log_level_watcher(reload_handle: LogLevelReloadHandle) {
        std::thread::spawn(move || {
            let mut last_content: Option<String> = None;

            loop {
                std::thread::sleep(LOG_LEVEL_POLL_INTERVAL);

                let content = match std::fs::read_to_string(LOG_LEVEL_FILE) {
                    Ok(content) => content.trim().to_string(),
                    Err(_) => {
                        continue;
                    }
                };

                if last_content.as_ref() == Some(&content) {
                    continue;
                }

                match content.parse::<EnvFilter>() {
                    Ok(new_filter) => {
                        if let Err(e) = reload_handle.reload(new_filter) {
                            eprintln!("Failed to reload log filter: {e}");
                        } else {
                            last_content = Some(content);
                        }
                    }
                    Err(e) => {
                        eprintln!("Invalid log filter in {LOG_LEVEL_FILE}: {e}");
                        last_content = Some(content);
                    }
                }
            }
        });
    }
}
