use std::fs::OpenOptions;
use std::path::Path;
use std::time::SystemTime;

use crate::profile::DebugOptions;

pub fn init(debug: Option<&DebugOptions>) {
    let mut dispatch = fern::Dispatch::new()
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}][{}] {}",
                humantime::format_rfc3339_seconds(SystemTime::now()),
                record.level(),
                std::env::args()
                    .next()
                    .map(|p| Path::new(&p)
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned())
                    .unwrap_or_else(|| "unknown".to_string()),
                record
                    .target()
                    .rsplit("::")
                    .next()
                    .unwrap_or(record.target()),
                message
            ))
        });

    if debug.map(|d| d.write_log_file).unwrap_or(false) {
        dispatch = dispatch.chain(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open("sqlsmith-rs.log")
                .expect("Failed to open log file"),
        );
    }

    dispatch
        .apply()
        .expect("Failed to configure logging with fern");
    log::info!("Logging configured with fern.");
}
