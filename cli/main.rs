#![allow(clippy::arc_with_non_send_sync)]
mod app;
mod commands;
mod config;
mod helper;
mod input;
mod opcodes_dictionary;

use clap::Parser;
use config::CONFIG_DIR;
use rustyline::{error::ReadlineError, Config, Editor};
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, LazyLock},
};
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::app::Opts;

fn rustyline_config() -> Config {
    Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .auto_add_history(true)
        .build()
}

pub static HOME_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| dirs::home_dir().expect("Could not determine home directory"));

pub static HISTORY_FILE: LazyLock<PathBuf> = LazyLock::new(|| HOME_DIR.join(".limbo_history"));

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    let _guard = init_tracing(opts.tracing_output.as_ref())?;
    let mut app = app::Limbo::new(opts)?;

    if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        let mut rl = Editor::with_config(rustyline_config())?;
        if HISTORY_FILE.exists() {
            rl.load_history(HISTORY_FILE.as_path())?;
        }
        let config_file = CONFIG_DIR.join("limbo.toml");

        let config = config::Config::from_config_file(config_file);
        tracing::info!("Configuration: {:?}", config);
        app = app.with_config(config);

        app = app.with_readline(rl);
    } else {
        tracing::debug!("not in tty");
    }

    loop {
        let readline = app.readline();
        match readline {
            Ok(line) => match app.handle_input_line(line.trim()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{e}");
                }
            },
            Err(ReadlineError::Interrupted) => {
                // At prompt, increment interrupt count
                if app.interrupt_count.fetch_add(1, Ordering::SeqCst) >= 1 {
                    eprintln!("Interrupted. Exiting...");
                    let _ = app.close_conn();
                    break;
                }
                println!("Use .quit to exit or press Ctrl-C again to force quit.");
                app.reset_input();
                continue;
            }
            Err(ReadlineError::Eof) => {
                app.handle_remaining_input();
                let _ = app.close_conn();
                break;
            }
            Err(err) => {
                let _ = app.close_conn();
                anyhow::bail!(err)
            }
        }
    }
    Ok(())
}

fn init_tracing(tracing_output: Option<&String>) -> Result<WorkerGuard, std::io::Error> {
    let ((non_blocking, guard), should_emit_ansi) = if let Some(file) = tracing_output {
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
        (tracing_appender::non_blocking(std::io::stderr()), true)
    };
    // Disable rustyline traces
    if let Err(e) = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_ansi(should_emit_ansi),
        )
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::OFF.into())
                .from_env_lossy()
                .add_directive("rustyline=off".parse().unwrap()),
        )
        .try_init()
    {
        println!("Unable to setup tracing appender: {:?}", e);
    }
    Ok(guard)
}
