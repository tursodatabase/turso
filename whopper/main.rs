/// Whopper is a deterministic simulator for testing the Turso database.
use clap::Parser;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::{
    generation::{Arbitrary, GenerationContext, Opts},
    model::{
        query::{
            create::Create, create_index::CreateIndex, delete::Delete, drop_index::DropIndex,
            insert::Insert, select::Select, update::Update,
        },
        table::{Column, ColumnType, Index, Table},
    },
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::trace;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use turso_core::{
    CipherMode, Connection, Database, DatabaseOpts, EncryptionOpts, IO, OpenFlags, Statement,
};
use turso_parser::ast::{ColumnConstraint, SortOrder};

mod io;
use crate::io::FILE_SIZE_SOFT_LIMIT;
use io::{IOFaultConfig, SimulatorIO};

#[derive(Parser)]
#[command(name = "turso_whopper")]
#[command(about = "The Turso Whopper Simulator")]
struct Args {
    /// Simulation mode (fast, chaos, ragnarök/ragnarok)
    #[arg(long, default_value = "fast")]
    mode: String,
    /// Keep mmap I/O files on disk after run
    #[arg(long)]
    keep: bool,
    /// Disable creation and manipulation of indexes
    #[arg(long)]
    disable_indexes: bool,
    /// Enable MVCC (Multi-Version Concurrency Control)
    #[arg(long)]
    enable_mvcc: bool,
    /// Enable database encryption
    #[arg(long)]
    enable_encryption: bool,
}

struct SimulatorConfig {
    max_connections: usize,
    max_steps: usize,
    cosmic_ray_probability: f64,
}

#[derive(Debug)]
enum FiberState {
    Idle,
    InTx,
}

struct SimulatorFiber {
    connection: Arc<Connection>,
    state: FiberState,
    statement: RefCell<Option<Statement>>,
}

struct SimulatorContext {
    fibers: Vec<SimulatorFiber>,
    tables: Vec<Table>,
    indexes: Vec<(String, String)>,
    opts: Opts,
    stats: Stats,
    disable_indexes: bool,
    enable_mvcc: bool,
}

#[derive(Default)]
struct Stats {
    inserts: usize,
    updates: usize,
    deletes: usize,
    integrity_checks: usize,
}

fn may_be_set_encryption(
    conn: Arc<Connection>,
    opts: &Option<EncryptionOpts>,
) -> anyhow::Result<Arc<Connection>> {
    if let Some(opts) = opts {
        conn.pragma_update("cipher", format!("'{}'", opts.cipher.clone()))?;
        conn.pragma_update("hexkey", format!("'{}'", opts.hexkey.clone()))?;
    }
    Ok(conn)
}

fn main() -> anyhow::Result<()> {
    init_logger();

    let args = Args::parse();

    let seed = std::env::var("SEED")
        .ok()
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap_or_else(|| {
            let mut rng = rand::rng();
            rng.next_u64()
        });

    println!("mode = {}", args.mode);
    println!("seed = {seed}");

    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // Create a separate RNG for IO operations with a derived seed
    let io_rng = ChaCha8Rng::seed_from_u64(seed.wrapping_add(1));

    let config = gen_config(&mut rng, &args.mode)?;

    let fault_config = IOFaultConfig {
        cosmic_ray_probability: config.cosmic_ray_probability,
    };

    if config.cosmic_ray_probability > 0.0 {
        println!("cosmic ray probability = {}", config.cosmic_ray_probability);
    }

    let simulator_io = Arc::new(SimulatorIO::new(args.keep, io_rng, fault_config));
    let file_sizes = simulator_io.file_sizes();
    let io = simulator_io as Arc<dyn IO>;

    let db_path = format!("whopper-{}-{}.db", seed, std::process::id());
    let wal_path = format!("{db_path}-wal");

    let encryption_opts = if args.enable_encryption {
        let opts = random_encryption_config(&mut rng);
        println!("cipher = {}, key = {}", opts.cipher, opts.hexkey);
        Some(opts)
    } else {
        None
    };

    let db = {
        let opts = DatabaseOpts::new().with_encryption(encryption_opts.is_some());

        match Database::open_file_with_flags(
            io.clone(),
            &db_path,
            OpenFlags::default(),
            opts,
            encryption_opts.clone(),
        ) {
            Ok(db) => db,
            Err(e) => {
                return Err(anyhow::anyhow!("Database open failed: {}", e));
            }
        }
    };

    let boostrap_conn = match db.connect() {
        Ok(conn) => may_be_set_encryption(conn, &encryption_opts)?,
        Err(e) => {
            return Err(anyhow::anyhow!("Connection failed: {}", e));
        }
    };

    // Enable MVCC if requested
    if args.enable_mvcc {
        boostrap_conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")?;
    }

    let schema = create_initial_schema(&mut rng);
    let tables = schema.iter().map(|t| t.table.clone()).collect::<Vec<_>>();
    for create_table in &schema {
        let sql = create_table.to_string();
        trace!("{}", sql);
        boostrap_conn.execute(&sql)?;
    }

    let indexes = if args.disable_indexes {
        Vec::new()
    } else {
        create_initial_indexes(&mut rng, &tables)
    };
    for create_index in &indexes {
        let sql = create_index.to_string();
        trace!("{}", sql);
        boostrap_conn.execute(&sql)?;
    }

    boostrap_conn.close()?;

    let mut fibers = Vec::new();
    for i in 0..config.max_connections {
        let conn = match db.connect() {
            Ok(conn) => may_be_set_encryption(conn, &encryption_opts)?,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to create fiber connection {}: {}",
                    i,
                    e
                ));
            }
        };
        fibers.push(SimulatorFiber {
            connection: conn,
            state: FiberState::Idle,
            statement: RefCell::new(None),
        });
    }

    let mut context = SimulatorContext {
        fibers,
        tables,
        indexes: indexes
            .iter()
            .map(|idx| (idx.table_name.clone(), idx.index_name.clone()))
            .collect(),
        opts: Opts::default(),
        stats: Stats::default(),
        disable_indexes: args.disable_indexes,
        enable_mvcc: args.enable_mvcc,
    };

    let progress_interval = config.max_steps / 10;
    let progress_stages = [
        "       .             I/U/D/C",
        "       .             ",
        "       .             ",
        "       |             ",
        "       |             ",
        "      ╱|╲            ",
        "     ╱╲|╱╲           ",
        "    ╱╲╱|╲╱╲          ",
        "   ╱╲╱╲|╱╲╱╲         ",
        "  ╱╲╱╲╱|╲╱╲╱╲        ",
        " ╱╲╱╲╱╲|╱╲╱╲╱╲       ",
    ];
    let mut progress_index = 0;
    println!("{}", progress_stages[progress_index]);
    progress_index += 1;

    for i in 0..config.max_steps {
        let fiber_idx = i % context.fibers.len();
        perform_work(fiber_idx, &mut rng, &mut context)?;
        io.step()?;
        if progress_interval > 0 && (i + 1) % progress_interval == 0 {
            println!(
                "{}{}/{}/{}/{}",
                progress_stages[progress_index],
                context.stats.inserts,
                context.stats.updates,
                context.stats.deletes,
                context.stats.integrity_checks
            );
            progress_index += 1;
        }
        if file_size_soft_limit_exceeded(&wal_path, file_sizes.clone()) {
            break;
        }
    }
    Ok(())
}

fn gen_config(rng: &mut ChaCha8Rng, mode: &str) -> anyhow::Result<SimulatorConfig> {
    match mode {
        "fast" => Ok(SimulatorConfig {
            max_connections: rng.random_range(1..=8) as usize,
            max_steps: 100000,
            cosmic_ray_probability: 0.0,
        }),
        "chaos" => Ok(SimulatorConfig {
            max_connections: rng.random_range(1..=8) as usize,
            max_steps: 10000000,
            cosmic_ray_probability: 0.0,
        }),
        "ragnarök" | "ragnarok" => Ok(SimulatorConfig {
            max_connections: rng.random_range(1..=8) as usize,
            max_steps: 1000000,
            cosmic_ray_probability: 0.0001,
        }),
        _ => Err(anyhow::anyhow!("Unknown mode: {}", mode)),
    }
}

fn create_initial_indexes(rng: &mut ChaCha8Rng, tables: &[Table]) -> Vec<CreateIndex> {
    let mut indexes = Vec::new();

    // Create 0-3 indexes per table
    for table in tables {
        let num_indexes = rng.random_range(0..=3);
        for i in 0..num_indexes {
            if !table.columns.is_empty() {
                // Pick 1-3 columns for the index
                let num_columns = rng.random_range(1..=std::cmp::min(3, table.columns.len()));
                let mut selected_columns = Vec::new();
                let mut available_columns = table.columns.clone();

                for _ in 0..num_columns {
                    if available_columns.is_empty() {
                        break;
                    }
                    let col_idx = rng.random_range(0..available_columns.len());
                    let column = available_columns.remove(col_idx);
                    let sort_order = if rng.random_bool(0.5) {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    };
                    selected_columns.push((column.name, sort_order));
                }

                if !selected_columns.is_empty() {
                    let index_name = format!("idx_{}_{}", table.name, i);
                    let create_index = CreateIndex {
                        index: Index {
                            index_name,
                            table_name: table.name.clone(),
                            columns: selected_columns,
                        },
                    };
                    indexes.push(create_index);
                }
            }
        }
    }

    indexes
}

fn create_initial_schema(rng: &mut ChaCha8Rng) -> Vec<Create> {
    let mut schema = Vec::new();

    // Generate random number of tables (1-5)
    let num_tables = rng.random_range(1..=5);

    for i in 0..num_tables {
        let table_name = format!("table_{i}");

        // Generate random number of columns (2-8)
        let num_columns = rng.random_range(2..=8);
        let mut columns = Vec::new();

        // TODO: there is no proper unique generation yet in whopper, so disable primary keys for now

        // let primary = ColumnConstraint::PrimaryKey {
        //     order: None,
        //     conflict_clause: None,
        //     auto_increment: false,
        // };
        // Always add an id column as primary key
        columns.push(Column {
            name: "id".to_string(),
            column_type: ColumnType::Integer,
            constraints: vec![],
        });

        // Add random columns
        for j in 1..num_columns {
            let col_type = match rng.random_range(0..3) {
                0 => ColumnType::Integer,
                1 => ColumnType::Text,
                _ => ColumnType::Float,
            };

            // FIXME: before sql_generation did not incorporate ColumnConstraint into the sql string
            // now it does and it the simulation here fails `whopper` with UNIQUE CONSTRAINT ERROR
            // 20% chance of unique
            let constraints = if rng.random_bool(0.0) {
                vec![ColumnConstraint::Unique(None)]
            } else {
                Vec::new()
            };

            columns.push(Column {
                name: format!("col_{j}"),
                column_type: col_type,
                constraints,
            });
        }

        let table = Table {
            name: table_name,
            columns,
            rows: vec![],
            indexes: vec![],
        };

        schema.push(Create { table });
    }
    schema
}

fn random_encryption_config(rng: &mut ChaCha8Rng) -> EncryptionOpts {
    let cipher_modes = [
        CipherMode::Aes128Gcm,
        CipherMode::Aes256Gcm,
        CipherMode::Aegis256,
        CipherMode::Aegis128L,
        CipherMode::Aegis128X2,
        CipherMode::Aegis128X4,
        CipherMode::Aegis256X2,
        CipherMode::Aegis256X4,
    ];

    let cipher_mode = cipher_modes[rng.random_range(0..cipher_modes.len())];

    let key_size = cipher_mode.required_key_size();
    let mut key = vec![0u8; key_size];
    rng.fill_bytes(&mut key);

    EncryptionOpts {
        cipher: cipher_mode.to_string(),
        hexkey: hex::encode(&key),
    }
}

fn perform_work(
    fiber_idx: usize,
    rng: &mut ChaCha8Rng,
    context: &mut SimulatorContext,
) -> anyhow::Result<()> {
    // If we have a statement, step it.
    let done = {
        let mut stmt_borrow = context.fibers[fiber_idx].statement.borrow_mut();
        if let Some(stmt) = stmt_borrow.as_mut() {
            let step_result = {
                let span = tracing::debug_span!(
                    "fiber",
                    fiber_idx = fiber_idx,
                    state = format!("{:?}", context.fibers[fiber_idx].state)
                );
                let _enter = span.enter();
                stmt.step()
            };
            match step_result {
                Ok(result) => {
                    tracing::debug!("fiber: {} {:?}", fiber_idx, result);
                    matches!(result, turso_core::StepResult::Done)
                }
                Err(e) => {
                    match e {
                        turso_core::LimboError::SchemaUpdated => {
                            trace!("{} Schema changed, rolling back transaction", fiber_idx);
                            drop(stmt_borrow);
                            context.fibers[fiber_idx].statement.replace(None);
                            // Rollback the transaction if we're in one
                            if matches!(context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) =
                                    context.fibers[fiber_idx].connection.prepare("ROLLBACK")
                            {
                                context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        turso_core::LimboError::Busy => {
                            trace!("{} Database busy, rolling back transaction", fiber_idx);
                            drop(stmt_borrow);
                            context.fibers[fiber_idx].statement.replace(None);
                            // Rollback the transaction if we're in one
                            if matches!(context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) =
                                    context.fibers[fiber_idx].connection.prepare("ROLLBACK")
                            {
                                context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        turso_core::LimboError::WriteWriteConflict => {
                            trace!(
                                "{} Write-write conflict, transaction automatically rolled back",
                                fiber_idx
                            );
                            drop(stmt_borrow);
                            context.fibers[fiber_idx].statement.replace(None);
                            context.fibers[fiber_idx].state = FiberState::Idle;
                            return Ok(());
                        }
                        turso_core::LimboError::BusySnapshot => {
                            trace!("{} Stale snapshot, rolling back transaction", fiber_idx);
                            drop(stmt_borrow);
                            context.fibers[fiber_idx].statement.replace(None);
                            if matches!(context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) =
                                    context.fibers[fiber_idx].connection.prepare("ROLLBACK")
                            {
                                context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        _ => {
                            return Err(e.into());
                        }
                    }
                }
            }
        } else {
            true
        }
    };
    // If the statement has more work, we're done for this simulation step
    if !done {
        return Ok(());
    }
    context.fibers[fiber_idx].statement.replace(None);
    match context.fibers[fiber_idx].state {
        FiberState::Idle => {
            let action = rng.random_range(0..100);
            if action <= 29 {
                let begin_cmd = if context.enable_mvcc {
                    match rng.random_range(0..3) {
                        0 => "BEGIN DEFERRED",
                        1 => "BEGIN IMMEDIATE",
                        _ => "BEGIN CONCURRENT",
                    }
                } else {
                    "BEGIN"
                };
                if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare(begin_cmd) {
                    context.fibers[fiber_idx].statement.replace(Some(stmt));
                    context.fibers[fiber_idx].state = FiberState::InTx;
                    trace!("fiber: {} {}", fiber_idx, begin_cmd);
                }
            } else if action == 30 {
                // Integrity check
                if let Ok(stmt) = context.fibers[fiber_idx]
                    .connection
                    .prepare("PRAGMA integrity_check")
                {
                    context.fibers[fiber_idx].statement.replace(Some(stmt));
                    context.stats.integrity_checks += 1;
                    trace!("fiber: {} PRAGMA integrity_check", fiber_idx);
                }
            }
        }
        FiberState::InTx => {
            let action = rng.random_range(0..100);
            match action {
                0..=9 => {
                    // SELECT (10%)
                    let select = Select::arbitrary(rng, context);
                    if let Ok(stmt) = context.fibers[fiber_idx]
                        .connection
                        .prepare(select.to_string())
                    {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                    }
                    trace!("fiber: {} SELECT: {}", fiber_idx, select.to_string());
                }
                10..=39 => {
                    // INSERT (30%)
                    let insert = Insert::arbitrary(rng, context);
                    if let Ok(stmt) = context.fibers[fiber_idx]
                        .connection
                        .prepare(insert.to_string())
                    {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.stats.inserts += 1;
                    }
                    trace!("fiber: {} INSERT: {}", fiber_idx, insert.to_string());
                }
                40..=59 => {
                    // UPDATE (20%)
                    let update = Update::arbitrary(rng, context);
                    if let Ok(stmt) = context.fibers[fiber_idx]
                        .connection
                        .prepare(update.to_string())
                    {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.stats.updates += 1;
                    }
                    trace!("fiber: {} UPDATE: {}", fiber_idx, update.to_string());
                }
                60..=69 => {
                    // DELETE (10%)
                    let delete = Delete::arbitrary(rng, context);
                    if let Ok(stmt) = context.fibers[fiber_idx]
                        .connection
                        .prepare(delete.to_string())
                    {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.stats.deletes += 1;
                    }
                    trace!("fiber: {} DELETE: {}", fiber_idx, delete.to_string());
                }
                70..=71 => {
                    // CREATE INDEX (2%)
                    if !context.disable_indexes {
                        let create_index = CreateIndex::arbitrary(rng, context);
                        let sql = create_index.to_string();
                        if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare(&sql) {
                            context.fibers[fiber_idx].statement.replace(Some(stmt));
                            context.indexes.push((
                                create_index.index.table_name.clone(),
                                create_index.index_name.clone(),
                            ));
                        }
                        trace!("fiber: {} CREATE INDEX: {}", fiber_idx, sql);
                    }
                }
                72..=73 => {
                    // DROP INDEX (2%)
                    if !context.disable_indexes && !context.indexes.is_empty() {
                        let index_idx = rng.random_range(0..context.indexes.len());
                        let (table_name, index_name) = context.indexes.remove(index_idx);
                        let drop_index = DropIndex {
                            table_name,
                            index_name,
                        };
                        let sql = drop_index.to_string();
                        if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare(&sql) {
                            context.fibers[fiber_idx].statement.replace(Some(stmt));
                        }
                        trace!("fiber: {} DROP INDEX: {}", fiber_idx, sql);
                    }
                }
                74..=86 => {
                    // COMMIT (13%)
                    if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare("COMMIT") {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.fibers[fiber_idx].state = FiberState::Idle;
                    }
                    trace!("fiber: {} COMMIT", fiber_idx);
                }
                87..=99 => {
                    // ROLLBACK (13%)
                    if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare("ROLLBACK") {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.fibers[fiber_idx].state = FiberState::Idle;
                    }
                    trace!("fiber: {} ROLLBACK", fiber_idx);
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn file_size_soft_limit_exceeded(
    wal_path: &str,
    file_sizes: Arc<std::sync::Mutex<HashMap<String, u64>>>,
) -> bool {
    let wal_size = {
        let sizes = file_sizes.lock().unwrap();
        sizes.get(wal_path).cloned().unwrap_or(0)
    };
    wal_size > FILE_SIZE_SOFT_LIMIT
}

impl GenerationContext for SimulatorContext {
    fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    fn opts(&self) -> &Opts {
        &self.opts
    }
}

fn init_logger() {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_line_number(true)
                .without_time()
                .with_thread_ids(false),
        )
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .try_init();
}
