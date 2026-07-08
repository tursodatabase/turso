// pgmicro — PostgreSQL-compatible micro database CLI
#![allow(clippy::arc_with_non_send_sync)]

#[path = "../../cli/config/mod.rs"]
mod config;
#[path = "../../cli/helper.rs"]
mod helper;
mod read_state_machine;

// Stubs for shared modules that reference crate-level types from the tursodb binary.
// pgmicro doesn't use these codepaths, but the types must exist so shared code compiles.
mod commands {
    #[derive(clap::Parser, Debug)]
    #[command(name = "pgmicro-stub", disable_help_flag(true))]
    pub struct CommandParser {}
}
mod input {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    #[allow(dead_code)]
    pub enum OutputMode {
        List,
        Pretty,
        Line,
    }
}

use clap::Parser;
use comfy_table::{Attribute, Cell, CellAlignment, ContentArrangement, Row, Table};
use config::{TableConfig, CONFIG_DIR};
use helper::LimboHelper;
use read_state_machine::ReadState;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use turso_core::{DatabaseOpts, LimboError, OpenFlags, Statement, Value};
use turso_pg::Connection;
use turso_pg_server::TursoPgServer;

// ---------------------------------------------------------------------------
// Statics
// ---------------------------------------------------------------------------

pub static HOME_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| dirs::home_dir().expect("Could not determine home directory"));

pub static HISTORY_FILE: LazyLock<PathBuf> = LazyLock::new(|| HOME_DIR.join(".pgmicro_history"));

const PROMPT: &str = "pgmicro> ";
const PROMPT_CONT: &str = "pgmicro-> ";

// ---------------------------------------------------------------------------
// CLI options
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "pgmicro")]
#[command(author, version, about = "PostgreSQL-compatible micro database")]
struct Opts {
    #[clap(index = 1, help = "Database file", default_value = ":memory:")]
    database: Option<PathBuf>,

    #[clap(index = 2, help = "Optional SQL command to execute")]
    sql: Option<String>,

    #[clap(short, long, help = "Don't display program information on start")]
    quiet: bool,

    #[clap(short = 'v', long, help = "Select VFS")]
    vfs: Option<String>,

    #[clap(long, help = "Open the database in read-only mode")]
    readonly: bool,

    #[clap(short = 't', long, help = "Specify output file for log traces")]
    tracing_output: Option<String>,

    #[clap(
        long,
        help = "Start PostgreSQL wire protocol server at given address (e.g. 0.0.0.0:5432)"
    )]
    server: Option<String>,
}

// ---------------------------------------------------------------------------
// Database setup
// ---------------------------------------------------------------------------

fn open_database(
    db_path: &str,
    vfs: Option<&String>,
    readonly: bool,
) -> anyhow::Result<(Arc<dyn turso_core::IO>, Connection)> {
    let db_opts = DatabaseOpts::new()
        .with_views(true)
        .with_custom_types(true)
        .with_encryption(true)
        .with_index_method(true)
        .with_autovacuum(true)
        .with_attach(true)
        .with_generated_columns(true);

    let flags = if readonly {
        OpenFlags::default().union(OpenFlags::ReadOnly)
    } else {
        OpenFlags::default()
    };

    let (io, db) =
        turso_pg::open_database(db_path, vfs.map(|v| v.as_str()), flags, db_opts.turso_cli())?;
    let conn = Connection::new(db.connect()?);
    Ok((io, conn))
}

/// Discover and attach existing PG schema database files in the same directory.
fn auto_attach_pg_schemas(conn: &Connection, db_file: &str) {
    if db_file == ":memory:" {
        return;
    }
    let dir = Path::new(db_file)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(schema) = name
            .strip_prefix("turso-postgres-schema-")
            .and_then(|s| s.strip_suffix(".db"))
        else {
            continue;
        };
        let path = entry.path().to_string_lossy().to_string();
        let sql = format!("ATTACH '{path}' AS \"{schema}\"");
        tracing::info!("Auto-attaching PG schema '{}' from {}", schema, path);
        if let Err(e) = conn.inner().execute(&sql) {
            tracing::warn!("Failed to attach schema '{}': {}", schema, e);
        }
    }
}

// ---------------------------------------------------------------------------
// Tracing setup
// ---------------------------------------------------------------------------

fn init_tracing(opts: &Opts) -> Result<WorkerGuard, std::io::Error> {
    let (non_blocking, guard) = if let Some(ref path) = opts.tracing_output {
        let file = std::fs::File::create(path)?;
        tracing_appender::non_blocking(file)
    } else {
        tracing_appender::non_blocking(std::io::sink())
    };
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .init();
    Ok(guard)
}

// ---------------------------------------------------------------------------
// Meta-commands
// ---------------------------------------------------------------------------

fn cmd_help(w: &mut dyn Write) {
    let _ = writeln!(w, "Meta-commands:");
    let _ = writeln!(w, "  \\dt            List tables");
    let _ = writeln!(w, "  \\dt+           List tables (extended)");
    let _ = writeln!(w, "  \\d <table>     Describe table columns");
    let _ = writeln!(w, "  \\d+ <table>    Describe table (extended)");
    let _ = writeln!(w, "  \\di            List indexes");
    let _ = writeln!(w, "  \\dv            List views");
    let _ = writeln!(w, "  \\dn            List schemas");
    let _ = writeln!(w, "  \\dT            List types");
    let _ = writeln!(w, "  \\du            List roles");
    let _ = writeln!(w, "  \\df            List functions");
    let _ = writeln!(w, "  \\l             List databases");
    let _ = writeln!(w, "  \\x             Toggle expanded display");
    let _ = writeln!(w, "  \\timing        Toggle query timing");
    let _ = writeln!(w, "  \\echo <text>   Print text");
    let _ = writeln!(w, "  \\conninfo      Show connection info");
    let _ = writeln!(w, "  \\?             Show this help");
    let _ = writeln!(w, "  \\q             Quit");
}

fn cmd_list_tables(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![Cell::new("Table").add_attribute(Attribute::Bold)]);
            let _ = rows.run_with_row_callback(|row| {
                let name = row.get_value(0).to_string();
                table.add_row(vec![Cell::new(&name)]);
                Ok(())
            });
            if !table.is_empty() {
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "No tables found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "No tables found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_describe_table(conn: &Connection, table_name: &str, w: &mut dyn Write) {
    let safe_name = table_name.replace('\'', "''");
    let sql = format!(
        "SELECT a.attname, t.typname, a.attnotnull, COALESCE(d.adbin, ''), a.attnum \
         FROM pg_attribute a \
         JOIN pg_class c ON a.attrelid = c.oid \
         JOIN pg_type t ON a.atttypid = t.oid \
         LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
         WHERE c.relname = '{safe_name}' AND a.attnum > 0 AND a.attisdropped = 0 \
         ORDER BY a.attnum"
    );
    match conn.query(&sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("Column").add_attribute(Attribute::Bold),
                Cell::new("Type").add_attribute(Attribute::Bold),
                Cell::new("Nullable").add_attribute(Attribute::Bold),
                Cell::new("Default").add_attribute(Attribute::Bold),
            ]);
            let mut found = false;
            let _ = rows.run_with_row_callback(|row| {
                found = true;
                let col_name = row.get_value(0).to_string();
                let col_type = row.get_value(1).to_string();
                let notnull = row.get_value(2).to_string();
                let nullable = if notnull == "1" { "NOT NULL" } else { "NULL" };
                let default_str = row.get_value(3).to_string();
                table.add_row(vec![
                    Cell::new(&col_name),
                    Cell::new(&col_type),
                    Cell::new(nullable),
                    Cell::new(default_str),
                ]);
                Ok(())
            });
            if found {
                let _ = writeln!(w, "Table: {table_name}");
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "Table '{table_name}' not found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "Table '{table_name}' not found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_list_databases(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT datname FROM pg_database ORDER BY datname";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![Cell::new("Name").add_attribute(Attribute::Bold)]);
            let _ = rows.run_with_row_callback(|row| {
                let name = row.get_value(0).to_string();
                table.add_row(vec![Cell::new(&name)]);
                Ok(())
            });
            let _ = writeln!(w, "{table}");
        }
        Ok(None) => {}
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_conninfo(db_file: &str, w: &mut dyn Write) {
    let _ = writeln!(w, "Database: {db_file}");
    let _ = writeln!(w, "Dialect:  PostgreSQL");
}

fn cmd_list_indexes(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT c2.relname, c.relname \
               FROM pg_index i \
               JOIN pg_class c ON i.indexrelid = c.oid \
               JOIN pg_class c2 ON i.indrelid = c2.oid \
               ORDER BY 1, 2";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("Table").add_attribute(Attribute::Bold),
                Cell::new("Index").add_attribute(Attribute::Bold),
            ]);
            let _ = rows.run_with_row_callback(|row| {
                let tbl = row.get_value(0).to_string();
                let idx = row.get_value(1).to_string();
                table.add_row(vec![Cell::new(&tbl), Cell::new(&idx)]);
                Ok(())
            });
            if !table.is_empty() {
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "No indexes found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "No indexes found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_list_views(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT relname FROM pg_class WHERE relkind = 'v' ORDER BY relname";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![Cell::new("View").add_attribute(Attribute::Bold)]);
            let _ = rows.run_with_row_callback(|row| {
                let name = row.get_value(0).to_string();
                table.add_row(vec![Cell::new(&name)]);
                Ok(())
            });
            if !table.is_empty() {
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "No views found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "No views found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_list_schemas(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT nspname, rolname \
               FROM pg_namespace n \
               JOIN pg_roles r ON n.nspowner = r.oid \
               ORDER BY nspname";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("Schema").add_attribute(Attribute::Bold),
                Cell::new("Owner").add_attribute(Attribute::Bold),
            ]);
            let _ = rows.run_with_row_callback(|row| {
                let name = row.get_value(0).to_string();
                let owner = row.get_value(1).to_string();
                table.add_row(vec![Cell::new(&name), Cell::new(&owner)]);
                Ok(())
            });
            if !table.is_empty() {
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "No schemas found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "No schemas found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_list_types(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT typname, typcategory FROM pg_type WHERE typtype = 'e' ORDER BY typname";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("Type").add_attribute(Attribute::Bold),
                Cell::new("Category").add_attribute(Attribute::Bold),
            ]);
            let _ = rows.run_with_row_callback(|row| {
                let name = row.get_value(0).to_string();
                let cat = row.get_value(1).to_string();
                table.add_row(vec![Cell::new(&name), Cell::new(&cat)]);
                Ok(())
            });
            if !table.is_empty() {
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "No types found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "No types found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_list_roles(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT rolname, CASE WHEN rolsuper = 1 THEN 'Superuser' ELSE '' END \
               FROM pg_roles ORDER BY rolname";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("Role").add_attribute(Attribute::Bold),
                Cell::new("Attributes").add_attribute(Attribute::Bold),
            ]);
            let _ = rows.run_with_row_callback(|row| {
                let name = row.get_value(0).to_string();
                let attrs = row.get_value(1).to_string();
                table.add_row(vec![Cell::new(&name), Cell::new(&attrs)]);
                Ok(())
            });
            if !table.is_empty() {
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "No roles found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "No roles found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_list_functions(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT p.proname, p.pronargs, COALESCE(t.typname, '') \
               FROM pg_proc p \
               LEFT JOIN pg_type t ON p.prorettype = t.oid \
               ORDER BY p.proname";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("Function").add_attribute(Attribute::Bold),
                Cell::new("Args").add_attribute(Attribute::Bold),
                Cell::new("Return Type").add_attribute(Attribute::Bold),
            ]);
            let _ = rows.run_with_row_callback(|row| {
                let name = row.get_value(0).to_string();
                let nargs = row.get_value(1).to_string();
                let ret = row.get_value(2).to_string();
                table.add_row(vec![Cell::new(&name), Cell::new(&nargs), Cell::new(&ret)]);
                Ok(())
            });
            if !table.is_empty() {
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "No functions found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "No functions found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

fn cmd_describe_table_extended(conn: &Connection, table_name: &str, w: &mut dyn Write) {
    // First show the regular column description
    cmd_describe_table(conn, table_name, w);

    let safe_name = table_name.replace('\'', "''");

    // Show indexes
    let idx_sql = format!(
        "SELECT c.relname \
         FROM pg_index i \
         JOIN pg_class c ON i.indexrelid = c.oid \
         JOIN pg_class c2 ON i.indrelid = c2.oid \
         WHERE c2.relname = '{safe_name}' \
         ORDER BY c.relname"
    );
    if let Ok(Some(mut rows)) = conn.query(&idx_sql) {
        let mut indexes = Vec::new();
        let _ = rows.run_with_row_callback(|row| {
            indexes.push(row.get_value(0).to_string());
            Ok(())
        });
        if !indexes.is_empty() {
            let _ = writeln!(w, "Indexes:");
            for idx in &indexes {
                let _ = writeln!(w, "  {idx}");
            }
        }
    }

    // Show constraints
    let con_sql = format!(
        "SELECT conname, contype \
         FROM pg_constraint con \
         JOIN pg_class c ON con.conrelid = c.oid \
         WHERE c.relname = '{safe_name}' \
         ORDER BY conname"
    );
    if let Ok(Some(mut rows)) = conn.query(&con_sql) {
        let mut constraints = Vec::new();
        let _ = rows.run_with_row_callback(|row| {
            let name = row.get_value(0).to_string();
            let ctype = row.get_value(1).to_string();
            let kind = match ctype.as_str() {
                "p" => "PRIMARY KEY",
                "u" => "UNIQUE",
                "c" => "CHECK",
                "f" => "FOREIGN KEY",
                _ => &ctype,
            };
            constraints.push(format!("{name} ({kind})"));
            Ok(())
        });
        if !constraints.is_empty() {
            let _ = writeln!(w, "Constraints:");
            for c in &constraints {
                let _ = writeln!(w, "  {c}");
            }
        }
    }
}

fn cmd_list_tables_extended(conn: &Connection, w: &mut dyn Write) {
    let sql = "SELECT c.relname, c.reltuples \
               FROM pg_class c \
               JOIN pg_namespace n ON c.relnamespace = n.oid \
               WHERE c.relkind = 'r' AND n.nspname = 'public' \
               ORDER BY c.relname";
    match conn.query(sql) {
        Ok(Some(mut rows)) => {
            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("Table").add_attribute(Attribute::Bold),
                Cell::new("Rows").add_attribute(Attribute::Bold),
            ]);
            let _ = rows.run_with_row_callback(|row| {
                let name = row.get_value(0).to_string();
                let nrows = row.get_value(1).to_string();
                table.add_row(vec![Cell::new(&name), Cell::new(&nrows)]);
                Ok(())
            });
            if !table.is_empty() {
                let _ = writeln!(w, "{table}");
            } else {
                let _ = writeln!(w, "No tables found.");
            }
        }
        Ok(None) => {
            let _ = writeln!(w, "No tables found.");
        }
        Err(e) => {
            let _ = writeln!(w, "Error: {e}");
        }
    }
}

/// Dispatch a backslash meta-command. Returns true if the REPL should quit.
fn handle_meta_command(
    line: &str,
    conn: &Connection,
    db_file: &str,
    expanded_display: &mut bool,
    timing: &mut bool,
    w: &mut dyn Write,
) -> bool {
    let trimmed = line.trim();
    let (cmd, arg) = match trimmed.find(char::is_whitespace) {
        Some(pos) => (&trimmed[..pos], trimmed[pos..].trim()),
        None => (trimmed, ""),
    };
    match cmd {
        "\\q" => return true,
        "\\?" => cmd_help(w),
        "\\dt+" => cmd_list_tables_extended(conn, w),
        "\\dt" => cmd_list_tables(conn, w),
        "\\di" => cmd_list_indexes(conn, w),
        "\\dv" => cmd_list_views(conn, w),
        "\\dn" => cmd_list_schemas(conn, w),
        "\\dT" => cmd_list_types(conn, w),
        "\\du" | "\\dg" => cmd_list_roles(conn, w),
        "\\df" => cmd_list_functions(conn, w),
        "\\d+" => {
            if arg.is_empty() {
                cmd_list_tables_extended(conn, w);
            } else {
                cmd_describe_table_extended(conn, arg, w);
            }
        }
        "\\d" => {
            if arg.is_empty() {
                cmd_list_tables(conn, w);
            } else {
                cmd_describe_table(conn, arg, w);
            }
        }
        "\\l" => cmd_list_databases(conn, w),
        "\\x" => {
            *expanded_display = !*expanded_display;
            if *expanded_display {
                let _ = writeln!(w, "Expanded display is on.");
            } else {
                let _ = writeln!(w, "Expanded display is off.");
            }
        }
        "\\timing" => {
            *timing = !*timing;
            if *timing {
                let _ = writeln!(w, "Timing is on.");
            } else {
                let _ = writeln!(w, "Timing is off.");
            }
        }
        "\\echo" => {
            let _ = writeln!(w, "{arg}");
        }
        "\\conninfo" => cmd_conninfo(db_file, w),
        _ => {
            let _ = writeln!(w, "Unknown command: {cmd}. Type \\? for help.");
        }
    }
    false
}

// ---------------------------------------------------------------------------
// SQL execution
// ---------------------------------------------------------------------------

fn execute_sql(
    conn: &Connection,
    sql: &str,
    table_config: &TableConfig,
    expanded: bool,
    w: &mut dyn Write,
) -> bool {
    let runner = conn.query_runner(sql.as_bytes());
    let mut had_error = false;
    for mut output in runner {
        match output {
            Ok(Some(ref mut stmt)) => {
                let result = if expanded {
                    print_result_set_expanded(stmt, w)
                } else {
                    print_result_set(stmt, table_config, w)
                };
                if let Err(e) = result {
                    let _ = writeln!(w, "Error: {e}");
                    had_error = true;
                    break;
                }
            }
            Ok(None) => {}
            Err(ref e) => {
                let _ = writeln!(w, "Error: {e}");
                had_error = true;
                break;
            }
        }
    }
    had_error
}

fn print_result_set(
    stmt: &mut Statement,
    table_config: &TableConfig,
    w: &mut dyn Write,
) -> Result<(), LimboError> {
    let num_columns = stmt.num_columns();
    if num_columns == 0 {
        stmt.run_with_row_callback(|_| Ok(()))?;
        return Ok(());
    }

    let header_color = table_config.header_color.as_comfy_table_color();
    let column_colors: Vec<_> = table_config
        .column_colors
        .iter()
        .map(|c| c.as_comfy_table_color())
        .collect();

    let column_names: Vec<String> = (0..num_columns)
        .map(|i| stmt.get_column_name(i).to_string())
        .collect();

    let mut table = Table::new();
    table
        .set_content_arrangement(ContentArrangement::Dynamic)
        .apply_modifier("││──├─┼┤│─┼├┤┬┴┌┐└┘");

    let header: Vec<Cell> = column_names
        .iter()
        .map(|name| {
            Cell::new(name)
                .add_attribute(Attribute::Bold)
                .fg(header_color)
        })
        .collect();
    table.set_header(header);

    stmt.run_with_row_callback(|row| {
        let mut table_row = Row::new();
        table_row.max_height(1);
        for (idx, value) in row.get_values().enumerate() {
            let (content, alignment) = match value {
                Value::Null => ("NULL".to_string(), CellAlignment::Left),
                Value::Numeric(_) => (format!("{value}"), CellAlignment::Right),
                Value::Text(_) => (format!("{value}"), CellAlignment::Left),
                Value::Blob(_) => (format!("{value}"), CellAlignment::Left),
            };
            table_row.add_cell(
                Cell::new(content)
                    .set_alignment(alignment)
                    .fg(column_colors[idx % column_colors.len()]),
            );
        }
        table.add_row(table_row);
        Ok(())
    })?;

    if !table.is_empty() {
        writeln!(w, "{table}").map_err(|e| turso_core::io_error(e, "write"))?;
    }
    Ok(())
}

fn print_result_set_expanded(stmt: &mut Statement, w: &mut dyn Write) -> Result<(), LimboError> {
    let num_columns = stmt.num_columns();
    if num_columns == 0 {
        stmt.run_with_row_callback(|_| Ok(()))?;
        return Ok(());
    }

    let column_names: Vec<String> = (0..num_columns)
        .map(|i| stmt.get_column_name(i).to_string())
        .collect();

    let max_col_width = column_names.iter().map(|n| n.len()).max().unwrap_or(0);
    let mut record_num = 0u64;

    stmt.run_with_row_callback(|row| {
        record_num += 1;
        let separator = "-".repeat(max_col_width + 3);
        writeln!(w, "-[ RECORD {record_num} ]{separator}")
            .map_err(|e| turso_core::io_error(e, "write"))?;
        for (idx, value) in row.get_values().enumerate() {
            let col = &column_names[idx];
            let val_str = match value {
                Value::Null => "NULL".to_string(),
                _ => format!("{value}"),
            };
            writeln!(w, "{col:<max_col_width$} | {val_str}")
                .map_err(|e| turso_core::io_error(e, "write"))?;
        }
        Ok(())
    })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// REPL
// ---------------------------------------------------------------------------

fn rustyline_config() -> rustyline::Config {
    rustyline::Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .auto_add_history(true)
        .build()
}

struct Repl {
    conn: Connection,
    #[allow(dead_code)]
    io: Arc<dyn turso_core::IO>,
    db_file: String,
    table_config: TableConfig,
    input_buf: String,
    read_state: ReadState,
    interrupt_count: Arc<AtomicUsize>,
    had_error: bool,
    expanded_display: bool,
    timing: bool,
}

impl Repl {
    fn new(
        conn: Connection,
        io: Arc<dyn turso_core::IO>,
        db_file: String,
        table_config: TableConfig,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            conn,
            io,
            db_file,
            table_config,
            input_buf: String::new(),
            read_state: ReadState::default(),
            interrupt_count,
            had_error: false,
            expanded_display: false,
            timing: false,
        }
    }

    fn prompt(&self) -> &str {
        if self.input_buf.is_empty() {
            PROMPT
        } else {
            PROMPT_CONT
        }
    }

    fn reset_input(&mut self) {
        self.input_buf.clear();
        self.read_state = ReadState::default();
    }

    fn consume(&mut self, flush: bool) {
        if self.input_buf.trim().is_empty() {
            return;
        }

        let trimmed = self.input_buf.trim();

        // Backslash meta-commands
        if trimmed.starts_with('\\') {
            let input = self.input_buf.clone();
            let quit = handle_meta_command(
                input.trim(),
                &self.conn,
                &self.db_file,
                &mut self.expanded_display,
                &mut self.timing,
                &mut std::io::stdout(),
            );
            self.reset_input();
            if quit {
                std::process::exit(0);
            }
            return;
        }

        let is_complete = self.read_state.is_complete();
        if is_complete || flush {
            let sql = self.input_buf.clone();
            let start = if self.timing {
                Some(Instant::now())
            } else {
                None
            };
            let had_err = execute_sql(
                &self.conn,
                sql.trim(),
                &self.table_config,
                self.expanded_display,
                &mut std::io::stdout(),
            );
            if let Some(start) = start {
                let elapsed = start.elapsed();
                let _ = writeln!(
                    std::io::stdout(),
                    "Time: {:.3}ms",
                    elapsed.as_secs_f64() * 1000.0
                );
            }
            if had_err {
                self.had_error = true;
            }
            self.reset_input();
        }
    }

    fn run_interactive(&mut self) {
        let mut rl = match Editor::<LimboHelper, DefaultHistory>::with_config(rustyline_config()) {
            Ok(rl) => rl,
            Err(e) => {
                eprintln!("Failed to initialize readline: {e}");
                return;
            }
        };

        if HISTORY_FILE.exists() {
            let _ = rl.load_history(HISTORY_FILE.as_path());
        }

        let config_file = CONFIG_DIR.join("limbo.toml");
        let config = config::Config::from_config_file(config_file);
        let h = LimboHelper::new(self.conn.inner().clone(), Some(config.highlight));
        rl.set_helper(Some(h));

        println!("pgmicro v{}", env!("CARGO_PKG_VERSION"));
        println!("Type \\? for help, \\q to quit.");
        if self.db_file == ":memory:" {
            println!("Connected to a transient in-memory database.");
        }

        loop {
            let prompt = self.prompt().to_string();
            match rl.readline(&prompt) {
                Ok(line) => {
                    self.interrupt_count.store(0, Ordering::Release);
                    self.read_state.process(&line);
                    self.input_buf.push_str(&line);
                    if !self.input_buf.ends_with(char::is_whitespace) {
                        self.input_buf.push('\n');
                    }
                    self.consume(false);
                }
                Err(ReadlineError::Interrupted) => {
                    if self.interrupt_count.fetch_add(1, Ordering::SeqCst) >= 1 {
                        eprintln!("Interrupted. Exiting...");
                        break;
                    }
                    println!("Use \\q to exit or press Ctrl-C again to force quit.");
                    self.reset_input();
                }
                Err(ReadlineError::Eof) => {
                    self.consume(true);
                    break;
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    break;
                }
            }
        }

        let _ = rl.save_history(HISTORY_FILE.as_path());
    }

    fn run_stdin(&mut self) {
        let stdin = std::io::stdin();
        loop {
            let prev_len = self.input_buf.len();
            if std::io::BufRead::read_line(&mut stdin.lock(), &mut self.input_buf).unwrap_or(0) == 0
            {
                self.consume(true);
                break;
            }
            self.read_state.process(&self.input_buf[prev_len..]);
            self.consume(false);
        }
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    let _guard = init_tracing(&opts)?;

    let db_file = opts
        .database
        .as_ref()
        .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string());

    let (io, conn) = open_database(&db_file, opts.vfs.as_ref(), opts.readonly)?;

    let interrupt_count = Arc::new(AtomicUsize::new(0));
    {
        let ic = Arc::clone(&interrupt_count);
        ctrlc::set_handler(move || {
            ic.fetch_add(1, Ordering::Release);
        })
        .expect("Error setting Ctrl-C handler");
    }

    // Server mode: start PG wire protocol server and exit
    if let Some(ref address) = opts.server {
        auto_attach_pg_schemas(&conn, &db_file);
        let server = TursoPgServer::new(address.clone(), db_file, conn, interrupt_count);
        return server.run();
    }

    let table_config = TableConfig::adaptive_colors();

    // Execute a single SQL command and exit
    if let Some(ref sql) = opts.sql {
        let had_error = execute_sql(&conn, sql, &table_config, false, &mut std::io::stdout());
        conn.close()?;
        if had_error {
            std::process::exit(1);
        }
        return Ok(());
    }

    let mut repl = Repl::new(conn.clone(), io, db_file, table_config, interrupt_count);

    let interactive = IsTerminal::is_terminal(&std::io::stdin());
    if interactive {
        repl.run_interactive();
    } else {
        repl.run_stdin();
    }

    let _ = conn.close();
    if !interactive && repl.had_error {
        std::process::exit(1);
    }
    Ok(())
}
