#![allow(clippy::arc_with_non_send_sync)]
mod app;
mod commands;
mod config;
mod helper;
mod input;
mod mcp_server;
mod opcodes_dictionary;
mod server;

use config::CONFIG_DIR;
use mcp_server::TursoMcpServer;
use rustyline::{error::ReadlineError, Config, Editor};
use server::SqlServer;
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, LazyLock},
};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn rustyline_config() -> Config {
    Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .auto_add_history(true)
        .build()
}

pub static HOME_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| dirs::home_dir().expect("Could not determine home directory"));

pub static HISTORY_FILE: LazyLock<PathBuf> = LazyLock::new(|| HOME_DIR.join(".limbo_history"));

fn run_mcp_server(app: app::Limbo) -> anyhow::Result<()> {
    let conn = app.get_connection();
    let interrupt_count = app.get_interrupt_count();
    let mcp_server = TursoMcpServer::new(conn, interrupt_count);

    mcp_server.run()
}

fn run_sql_server(app: app::Limbo) -> anyhow::Result<()> {
    let address = app.get_server_address();
    let connection = app.get_connection();
    let interrupt_count = app.get_interrupt_count();
    let db_file = app.get_database_file_display();

    let sql_server = SqlServer::new(connection, interrupt_count);
    sql_server.run(address, &db_file)
}

fn main() -> anyhow::Result<()> {
    let (mut app, _guard) = app::Limbo::new()?;

    if app.is_mcp_mode() {
        return run_mcp_server(app);
    }

    if app.is_server_mode() {
        return run_sql_server(app);
    }

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
