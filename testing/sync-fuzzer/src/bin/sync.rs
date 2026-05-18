use std::{
    env,
    ffi::OsString,
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use prost::Message;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tempfile::TempDir;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, timeout, Instant},
};
use tracing_subscriber::EnvFilter;
use turso::{
    sync::{Builder, Database, RemoteEncryptionCipher},
    Connection, Error as TursoError, Value,
};

const SYNC_INTERVAL: Duration = Duration::from_millis(350);
const READER_INTERVAL: Duration = Duration::from_millis(175);
const LOCAL_WRITER_INTERVAL: Duration = Duration::from_millis(275);
const LOCAL_WRITER_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const LOCAL_WRITER_RETRY_TIMEOUT: Duration = Duration::from_secs(3);
const REMOTE_WRITER_INTERVAL: Duration = Duration::from_millis(425);
const BACKGROUND_PRESSURE_DURATION: Duration = Duration::from_secs(12);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const ITEM_COLUMNS: &[&str] = &[
    "id", "owner", "payload", "rev", "note", "bucket", "tag", "status",
];
const SEEDED_DEFAULT_STEPS: usize = 60;
const SEEDED_DEFAULT_REPLICAS: usize = 3;
const SEEDED_DEFAULT_CHECK_EVERY: usize = 10;
const SEEDED_RECENT_EVENT_LIMIT: usize = 24;
const GENERATION_ROLLOVER_TIMEOUT: Duration = Duration::from_secs(60);
const GENERATION_ROLLOVER_POLL_INTERVAL: Duration = Duration::from_millis(200);
const GENERATION_PRESSURE_BATCHES: usize = 96;
const GENERATION_PRESSURE_ATTEMPTS: usize = 10;
const GENERATION_PRESSURE_POLLS_PER_ATTEMPT: usize = 10;
const ORPHAN_INDEX_PROBE_ROUNDS: usize = 4;
const SEEDED_TARGETED_LABEL: &str = "seeded targeted coverage";
const REMOTE_SQL_TIMEOUT: Duration = Duration::from_secs(15);
const PULL_UPDATES_PROBE_TIMEOUT: Duration = Duration::from_secs(60);
const REMOTE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const MVCC_LOG_HDR_SIZE: u64 = 56;
const MVCC_GENERATION_ID_MAX_EXCLUSIVE: u64 = 1_000_000_000_000_000_000;
const REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER_ENV: &str =
    "TURSO_SYNC_DEBUG_INJECT_REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER";

#[derive(Clone)]
struct Config {
    remote_url: String,
    auth_token: Option<String>,
    remote_encryption_key: Option<String>,
    remote_encryption_cipher: Option<RemoteEncryptionCipher>,
}

#[derive(Clone)]
struct LocalReplica {
    name: String,
    path: PathBuf,
    db: Database,
    protocol: SyncProtocol,
    last_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct TableSnapshot {
    schema_sql: String,
    indexes: Vec<IndexSnapshot>,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IndexSnapshot {
    name: String,
    sql: String,
}

#[derive(Clone, Copy)]
enum ConvergenceSyncMode {
    PushAndPull,
    PullOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunMode {
    Scripted,
    Seeded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncProtocol {
    Mvcc,
    Legacy,
}

#[derive(Debug, Clone, Copy)]
struct RunArgs {
    mode: RunMode,
    protocol: SyncProtocol,
    seed: u64,
    steps: usize,
    replicas: usize,
    check_every: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct MvccRevision {
    generation: u64,
    #[serde(default)]
    log_offset: u64,
    #[serde(default)]
    wal_fragment_no: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct RichMvccRevision {
    generation: u64,
    log_offset: u64,
    #[serde(default)]
    wal_fragment_no: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DbInfo {
    current_generation: u64,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PullUpdatesStreamKind {
    Pages,
    MvccLogicalLog,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PullUpdatesApplyMode {
    Incremental,
    ReplaceBase,
}

#[derive(Debug)]
struct PullUpdatesProbe {
    server_revision: String,
    db_size: u64,
    stream_kind: Option<PullUpdatesStreamKind>,
    apply_mode: Option<PullUpdatesApplyMode>,
    trailing_messages: usize,
    raw_logical_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum ProbePageUpdatesEncodingReq {
    Raw = 0,
    Zstd = 1,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProbePullUpdatesReqProtoBody {
    #[prost(enumeration = "ProbePageUpdatesEncodingReq", tag = "1")]
    encoding: i32,
    #[prost(string, tag = "2")]
    server_revision: String,
    #[prost(string, tag = "3")]
    client_revision: String,
    #[prost(uint32, tag = "4")]
    long_poll_timeout_ms: u32,
    #[prost(bytes, tag = "5")]
    server_pages_selector: Vec<u8>,
    #[prost(bytes, tag = "6")]
    client_pages: Vec<u8>,
    #[prost(string, tag = "7")]
    server_query_selector: String,
    #[prost(bool, tag = "8")]
    logical_updates: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProbePageSetRawEncodingProto {}

#[derive(Clone, PartialEq, prost::Message)]
struct ProbePageSetZstdEncodingProto {
    #[prost(int32, tag = "1")]
    level: i32,
    #[prost(uint32, repeated, tag = "2")]
    pages_dict: Vec<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProbePullUpdatesRespProtoBody {
    #[prost(string, tag = "1")]
    server_revision: String,
    #[prost(uint64, tag = "2")]
    db_size: u64,
    #[prost(optional, message, tag = "3")]
    raw_encoding: Option<ProbePageSetRawEncodingProto>,
    #[prost(optional, message, tag = "4")]
    zstd_encoding: Option<ProbePageSetZstdEncodingProto>,
    #[prost(enumeration = "ProbePullUpdatesStreamKind", tag = "5")]
    stream_kind: i32,
    #[prost(enumeration = "ProbePullUpdatesApplyMode", tag = "6")]
    apply_mode: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum ProbePullUpdatesStreamKind {
    Pages = 0,
    MvccLogicalLog = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum ProbePullUpdatesApplyMode {
    Incremental = 0,
    ReplaceBase = 1,
}

#[derive(Clone, Copy, Debug)]
enum ProbeLogicalSchemaAction {
    Create,
    Drop,
}

#[derive(Clone, Copy, Debug)]
enum ProbeLogicalSchemaKind {
    Table,
    Index,
}

#[derive(Clone, Copy)]
struct ExtraColumnSpec {
    name: &'static str,
    ddl: &'static str,
}

const SEEDED_EXTRA_COLUMNS: &[ExtraColumnSpec] = &[
    ExtraColumnSpec {
        name: "note",
        ddl: "TEXT",
    },
    ExtraColumnSpec {
        name: "bucket",
        ddl: "INTEGER NOT NULL DEFAULT 0",
    },
    ExtraColumnSpec {
        name: "tag",
        ddl: "TEXT",
    },
    ExtraColumnSpec {
        name: "status",
        ddl: "INTEGER NOT NULL DEFAULT 0",
    },
];

struct ScenarioRng {
    state: u64,
}

struct SeededScenarioState {
    seed: u64,
    table: String,
    available_columns: Vec<&'static str>,
    indexed_columns: Vec<&'static str>,
    next_column_idx: usize,
    next_local_ids: Vec<i64>,
    local_live_ids: Vec<Vec<i64>>,
    local_cdc_high_water: Vec<Option<i64>>,
    next_remote_id: i64,
    remote_live_ids: Vec<i64>,
    bootstrap_checks: usize,
    recent_events: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
enum SchemaActor {
    Local(usize),
    Remote,
}

impl SchemaActor {
    fn label(self) -> &'static str {
        match self {
            SchemaActor::Local(_) => "local",
            SchemaActor::Remote => "remote",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = parse_args()?;
    let config = load_config()?;
    match args.mode {
        RunMode::Scripted => run_scripted(&config, args.protocol).await,
        RunMode::Seeded => run_seeded(&config, &args).await,
    }
}

fn init_logging() {
    let filter = env::var("SYNC_EXAMPLE_LOG")
        .ok()
        .or_else(|| env::var("RUST_LOG").ok())
        .unwrap_or_else(|| "warn".to_string());
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_ansi(true)
        .without_time()
        .try_init();
}

async fn run_scripted(config: &Config, protocol: SyncProtocol) -> Result<()> {
    let dir = TempDir::new().context("failed to create tempdir for local sync replica")?;
    let table = match protocol {
        SyncProtocol::Mvcc => unique_name("mvcc_sync_items"),
        SyncProtocol::Legacy => unique_name("legacy_sync_scripted"),
    };

    println!("remote url: {}", config.remote_url);
    println!("auth enabled: {}", config.auth_token.is_some());
    println!(
        "remote encryption enabled: {}",
        config.remote_encryption_key.is_some()
    );
    if let Some(cipher) = config.remote_encryption_cipher {
        println!("remote encryption cipher: {cipher:?}");
    }
    println!("mode: scripted");
    println!("protocol: {protocol:?}");
    println!("local dir: {}", dir.path().display());
    println!("table: {table}");

    maybe_cleanup_remote_test_tables(&config).await?;

    let remote_mode = query_remote_journal_mode(&config).await?;
    println!("remote journal_mode: {remote_mode}");
    match protocol {
        SyncProtocol::Mvcc if !remote_mode.eq_ignore_ascii_case("mvcc") => {
            bail!(
                "scripted MVCC mode expects the remote database to already be in MVCC mode; got journal_mode={remote_mode}"
            );
        }
        SyncProtocol::Legacy if remote_mode.eq_ignore_ascii_case("mvcc") => {
            bail!(
                "scripted legacy mode expects a non-MVCC remote database; got journal_mode={remote_mode}"
            );
        }
        _ => {}
    }

    run_scripted_bug_files_text_pk_secondary_index(config, dir.path(), protocol).await?;
    if protocol == SyncProtocol::Legacy {
        println!("scripted legacy sync checks completed successfully");
        return Ok(());
    }

    let local = build_local_replica("a", dir.path().join("a.db"), config, false, protocol).await?;
    let conn = local
        .db
        .connect()
        .await
        .context("failed to connect local replica")?;
    ensure_mvcc_mode(&conn).await?;
    initialize_table(&conn, &table).await?;
    local.db.push().await.context("initial local push failed")?;

    let bootstrap = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "bootstrap",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;
    print_snapshot("bootstrap snapshot", &bootstrap);
    assert_scripted_mvcc_incremental_page_pull_is_rejected(&config, &local).await?;
    assert_scripted_current_generation_logical_pull_after_remote_drop(&config).await?;
    run_orphan_index_checkpoint_probe(&config, dir.path(), "scripted").await?;

    phase_one_local(&local.db, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 1 local writes",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;
    print_snapshot("phase 1 snapshot", &snapshot);

    phase_two_remote(&config, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 2 remote schema evolution",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;
    print_snapshot("phase 2 snapshot", &snapshot);

    phase_three_local(&local.db, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 3 local schema evolution",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;
    print_snapshot("phase 3 snapshot", &snapshot);

    phase_four_remote(&config, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 4 remote writes",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;
    print_snapshot("phase 4 snapshot", &snapshot);

    // Keep the background phase running long enough to exercise repeated
    // push/pull/checkpoint activity while both sides continue mutating data.
    let (stop_tx, stop_rx) = watch::channel(false);
    let sync_worker = spawn_sync_worker(local.clone(), stop_rx.clone(), table.clone());
    let reader_worker = spawn_reader_worker(local.clone(), stop_rx.clone(), table.clone());
    let local_writer = spawn_local_writer_worker(local.clone(), stop_rx.clone(), table.clone());
    let remote_writer = spawn_remote_writer_worker(config.clone(), stop_rx.clone(), table.clone());

    sleep(BACKGROUND_PRESSURE_DURATION).await;

    stop_tx
        .send(true)
        .map_err(|_| anyhow!("failed to stop background workers"))?;
    sync_worker.await.context("sync worker join failed")??;
    reader_worker.await.context("reader worker join failed")??;
    local_writer
        .await
        .context("local writer worker join failed")??;
    remote_writer
        .await
        .context("remote writer worker join failed")??;

    let background_snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "background pressure",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;
    print_snapshot("background pressure snapshot", &background_snapshot);

    checkpoint_drill(&local, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "checkpoint drill",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;
    print_snapshot("checkpoint snapshot", &snapshot);

    drain_local(&local).await?;
    let stats = local
        .db
        .stats()
        .await
        .context("failed to read local sync stats")?;
    println!(
        "final stats: wal={} sent={} recv={}",
        stats.main_wal_size, stats.network_sent_bytes, stats.network_received_bytes
    );

    let final_snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "final drain",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;
    print_snapshot("final snapshot", &final_snapshot);

    println!("comprehensive MVCC sync demo completed successfully");
    Ok(())
}

async fn run_seeded(config: &Config, args: &RunArgs) -> Result<()> {
    let dir = TempDir::new().context("failed to create tempdir for seeded sync replicas")?;
    let table = match args.protocol {
        SyncProtocol::Mvcc => unique_name("mvcc_sync_fuzz"),
        SyncProtocol::Legacy => unique_name("legacy_sync_fuzz"),
    };

    println!("remote url: {}", config.remote_url);
    println!("auth enabled: {}", config.auth_token.is_some());
    println!(
        "remote encryption enabled: {}",
        config.remote_encryption_key.is_some()
    );
    println!("mode: seeded");
    println!("protocol: {:?}", args.protocol);
    println!(
        "seeded config: seed={} steps={} replicas={} check_every={}",
        args.seed, args.steps, args.replicas, args.check_every
    );
    println!("local dir: {}", dir.path().display());
    println!("table: {table}");

    maybe_cleanup_remote_test_tables(config).await?;

    let remote_mode = query_remote_journal_mode(config).await?;
    println!("remote journal_mode: {remote_mode}");
    match args.protocol {
        SyncProtocol::Mvcc if !remote_mode.eq_ignore_ascii_case("mvcc") => {
            bail!(
                "MVCC seeded scenario expects the remote database to already be in MVCC mode; got journal_mode={remote_mode}"
            );
        }
        SyncProtocol::Legacy if remote_mode.eq_ignore_ascii_case("mvcc") => {
            bail!(
                "legacy seeded scenario expects a non-MVCC remote database; got journal_mode={remote_mode}"
            );
        }
        _ => {}
    }

    let initial_generation = if args.protocol == SyncProtocol::Mvcc {
        Some(query_remote_generation(config).await?)
    } else {
        None
    };
    let mut state = SeededScenarioState::new(table.clone(), args.replicas, args.seed);
    let mut replicas = Vec::with_capacity(args.replicas);

    if args.protocol == SyncProtocol::Legacy {
        initialize_seeded_table_remote(config, &table).await?;
        state.record_note("initialized legacy remote seed table");
    }

    let primary_bootstrap = args.protocol == SyncProtocol::Legacy;
    let primary = build_local_replica(
        "replica-0",
        dir.path().join("replica-0.db"),
        config,
        primary_bootstrap,
        args.protocol,
    )
    .await?;
    let primary_conn = primary
        .db
        .connect()
        .await
        .context("failed to connect primary seeded replica")?;
    ensure_protocol_mode(&primary_conn, args.protocol).await?;
    if args.protocol == SyncProtocol::Mvcc {
        initialize_seeded_table(&primary_conn, &table).await?;
    }
    drop(primary_conn);
    if args.protocol == SyncProtocol::Mvcc {
        assert_local_cdc_advanced(&primary, &mut state, 0, "initial seeded setup").await?;
        primary
            .db
            .push()
            .await
            .context("initial seeded push failed")?;
        assert_seeded_push_cdc_high_water(config, &primary, &state, 0, "initial seeded push")
            .await?;
    }
    let _bootstrap_generation = if args.protocol == SyncProtocol::Mvcc
        && env_flag_enabled("SYNC_EXAMPLE_FORCE_BOOTSTRAP_ROLLOVER")
    {
        let initial_generation = initial_generation.expect("MVCC initial generation must be set");
        let generation = force_remote_generation_rollover_with_push_pressure(
            config,
            &primary,
            0,
            &mut state,
            "seeded-bootstrap",
            initial_generation,
        )
        .await
        .context("failed to checkpoint remote state before seeded replica bootstrap")?;
        println!("seeded bootstrap checkpoint: generation {initial_generation} -> {generation}",);
        Some(generation)
    } else {
        initial_generation
    };
    if args.protocol == SyncProtocol::Mvcc && env_flag_enabled("SYNC_EXAMPLE_PROBE_BOOTSTRAP") {
        print_pull_updates_probe(config, "", "seeded bootstrap remote").await;
    }
    replicas.push(primary);

    for idx in 1..args.replicas {
        let replica = build_local_replica(
            format!("replica-{idx}"),
            dir.path().join(format!("replica-{idx}.db")),
            config,
            true,
            args.protocol,
        )
        .await?;
        let conn = replica
            .db
            .connect()
            .await
            .with_context(|| format!("failed to connect seeded replica {idx}"))?;
        ensure_protocol_mode(&conn, args.protocol).await?;
        drop(conn);
        print_bootstrap_replica_state(&replica, &table).await?;
        replicas.push(replica);
    }

    let bootstrap = wait_for_cluster_convergence(
        config,
        &mut replicas,
        &table,
        "seeded bootstrap",
        Some(&state),
    )
    .await?;
    print_compact_snapshot("seeded bootstrap", &bootstrap);
    state.record_note("bootstrap converged");

    let mut rng = ScenarioRng::new(args.seed);
    for step in 0..args.steps {
        let description = run_seeded_step(
            config,
            dir.path(),
            &mut replicas,
            &mut state,
            &mut rng,
            step,
        )
        .await
        .with_context(|| format!("seeded scenario failed at step {step}"))?;
        state.record_step(step, &description);
        println!("seeded step {step:03}: {description}");
        assert_cluster_integrity(
            config,
            &mut replicas,
            &table,
            &format!("seeded step {step:03} after {description}"),
        )
        .await
        .with_context(|| {
            format!("seeded integrity check failed after step {step}: {description}")
        })?;

        if (step + 1) % args.check_every == 0 {
            let snapshot = wait_for_cluster_convergence(
                config,
                &mut replicas,
                &table,
                &format!("seeded check {}", step + 1),
                Some(&state),
            )
            .await?;
            state.record_note(format!("seeded check {} converged", step + 1));
            print_compact_snapshot(&format!("seeded check {}", step + 1), &snapshot);
        }
    }

    let pre_rollover_snapshot = wait_for_cluster_convergence(
        config,
        &mut replicas,
        &table,
        "seeded pre-targeted",
        Some(&state),
    )
    .await?;
    print_snapshot("seeded pre-targeted snapshot", &pre_rollover_snapshot);

    if args.protocol == SyncProtocol::Mvcc {
        run_targeted_mvcc_logical_coverage_suite(config, dir.path(), &mut replicas, &mut state)
            .await?;
    } else {
        println!("skipping MVCC-only targeted logical coverage for legacy sync protocol");
    }

    let pre_rollover_snapshot = wait_for_cluster_convergence(
        config,
        &mut replicas,
        &table,
        "seeded pre-rollover",
        Some(&state),
    )
    .await?;
    print_snapshot("seeded pre-rollover snapshot", &pre_rollover_snapshot);

    if args.protocol == SyncProtocol::Mvcc {
        run_generation_rollover_logical_recovery_check(config, &mut replicas, &mut state).await?;
    } else {
        println!(
            "skipping MVCC generation rollover logical recovery check for legacy sync protocol"
        );
    }

    let final_snapshot =
        wait_for_cluster_convergence(config, &mut replicas, &table, "seeded final", Some(&state))
            .await?;
    print_snapshot("seeded final snapshot", &final_snapshot);

    run_bootstrap_check(
        config,
        dir.path(),
        &table,
        state.bootstrap_checks,
        &mut replicas,
        Some(&state),
    )
    .await?;

    println!(
        "seeded {:?} sync scenario completed successfully",
        args.protocol
    );
    Ok(())
}

async fn run_scripted_bug_files_text_pk_secondary_index(
    config: &Config,
    root_dir: &Path,
    protocol: SyncProtocol,
) -> Result<()> {
    let replica = build_local_replica(
        "bug-files-text-pk-secondary-index",
        root_dir.join("bug-files-text-pk-secondary-index.db"),
        config,
        false,
        protocol,
    )
    .await?;
    let conn = replica
        .db
        .connect()
        .await
        .context("BUG.md check failed to connect sync-enabled local replica")?;
    ensure_protocol_mode(&conn, protocol).await?;
    query_rows(&conn, "PRAGMA foreign_keys = ON")
        .await
        .context("BUG.md check failed to enable foreign keys")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            filename TEXT NOT NULL,
            file_type TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            metadata_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (parent_id) REFERENCES files(id) ON DELETE SET NULL
        )",
        (),
    )
    .await
    .context("BUG.md check failed to create files table")?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_parent_id ON files (parent_id)",
        (),
    )
    .await
    .context("BUG.md check failed to create files parent index")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS file_blobs (
            file_id TEXT NOT NULL,
            data BLOB NOT NULL,
            PRIMARY KEY (file_id),
            FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
        )",
        (),
    )
    .await
    .context("BUG.md check failed to create file_blobs table")?;
    conn.execute(
        "INSERT INTO files (
            id,
            parent_id,
            filename,
            file_type,
            mime_type,
            metadata_json,
            created_at,
            updated_at
        ) VALUES (
            '019de3f4-e018-7b33-a58b-6fb14640da4c',
            NULL,
            'interactive-sync.txt',
            'text',
            'text/plain',
            '{\"source\": \"interactive-test\"}',
            '2026-05-01 14:32:49',
            '2026-05-01 14:32:49'
        )",
        (),
    )
    .await
    .context("BUG.md check failed to insert first files row")?;
    let rows = query_rows(
        &conn,
        "SELECT id, parent_id, filename FROM files ORDER BY id",
    )
    .await
    .context("BUG.md check failed to read files row")?;
    let expected = vec![vec![
        Value::Text("019de3f4-e018-7b33-a58b-6fb14640da4c".to_string()),
        Value::Null,
        Value::Text("interactive-sync.txt".to_string()),
    ]];
    if rows != expected {
        bail!("BUG.md check inserted unexpected files rows: {rows:?}");
    }
    println!("scripted check: BUG.md files TEXT PRIMARY KEY insert with secondary index passed");
    Ok(())
}

fn parse_args() -> Result<RunArgs> {
    let mut mode = RunMode::Scripted;
    let mut protocol = SyncProtocol::Mvcc;
    let mut seed = default_seed();
    let mut steps = SEEDED_DEFAULT_STEPS;
    let mut replicas = SEEDED_DEFAULT_REPLICAS;
    let mut check_every = SEEDED_DEFAULT_CHECK_EVERY;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                let value = args
                    .next()
                    .context("expected value after --mode (scripted|seeded)")?;
                mode = match value.as_str() {
                    "scripted" => RunMode::Scripted,
                    "seeded" => RunMode::Seeded,
                    _ => bail!("unsupported mode '{value}', expected scripted or seeded"),
                };
            }
            "--protocol" => {
                let value = args
                    .next()
                    .context("expected value after --protocol (mvcc|legacy)")?;
                protocol = match value.as_str() {
                    "mvcc" => SyncProtocol::Mvcc,
                    "legacy" | "old" | "non-mvcc" => SyncProtocol::Legacy,
                    _ => bail!("unsupported protocol '{value}', expected mvcc or legacy"),
                };
            }
            "--seed" => {
                let value = args.next().context("expected value after --seed")?;
                seed = value.parse().context("invalid --seed value")?;
            }
            "--steps" => {
                let value = args.next().context("expected value after --steps")?;
                steps = value.parse().context("invalid --steps value")?;
            }
            "--replicas" => {
                let value = args.next().context("expected value after --replicas")?;
                replicas = value.parse().context("invalid --replicas value")?;
            }
            "--check-every" => {
                let value = args.next().context("expected value after --check-every")?;
                check_every = value.parse().context("invalid --check-every value")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument '{other}'"),
        }
    }

    if replicas == 0 {
        bail!("--replicas must be at least 1");
    }
    if check_every == 0 {
        bail!("--check-every must be at least 1");
    }

    Ok(RunArgs {
        mode,
        protocol,
        seed,
        steps,
        replicas,
        check_every,
    })
}

fn print_usage() {
    println!(
        "usage: cargo run --manifest-path sync/engine/examples/Cargo.toml --bin sync -- [--mode scripted|seeded] [--protocol mvcc|legacy] [--seed N] [--steps N] [--replicas N] [--check-every N]"
    );
}

fn default_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos() as u64
}

impl ScenarioRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn index(&mut self, upper: usize) -> usize {
        if upper <= 1 {
            0
        } else {
            (self.next_u64() as usize) % upper
        }
    }

    fn bool(&mut self) -> bool {
        self.next_u64() & 1 == 0
    }
}

impl SeededScenarioState {
    fn new(table: String, replicas: usize, seed: u64) -> Self {
        let mut next_local_ids = Vec::with_capacity(replicas);
        let mut local_live_ids = Vec::with_capacity(replicas);
        for idx in 0..replicas {
            next_local_ids.push(10_000 + idx as i64 * 10_000);
            local_live_ids.push(Vec::new());
        }
        Self {
            seed,
            table,
            available_columns: Vec::new(),
            indexed_columns: Vec::new(),
            next_column_idx: 0,
            next_local_ids,
            local_live_ids,
            local_cdc_high_water: vec![None; replicas],
            next_remote_id: 1_000_000,
            remote_live_ids: Vec::new(),
            bootstrap_checks: 0,
            recent_events: Vec::new(),
        }
    }

    fn record_step(&mut self, step: usize, detail: impl Into<String>) {
        self.push_recent(format!("step {step:03}: {}", detail.into()));
    }

    fn record_note(&mut self, detail: impl Into<String>) {
        self.push_recent(format!("note: {}", detail.into()));
    }

    fn push_recent(&mut self, event: String) {
        if self.recent_events.len() == SEEDED_RECENT_EVENT_LIMIT {
            self.recent_events.remove(0);
        }
        self.recent_events.push(event);
    }

    fn next_pending_column(&self) -> Option<ExtraColumnSpec> {
        SEEDED_EXTRA_COLUMNS.get(self.next_column_idx).copied()
    }

    fn record_added_column(&mut self, column: &'static str) {
        if !self.available_columns.contains(&column) {
            self.available_columns.push(column);
        }
        self.next_column_idx = self.available_columns.len();
    }

    fn next_unindexed_column(&self) -> Option<&'static str> {
        self.available_columns
            .iter()
            .copied()
            .find(|column| !self.indexed_columns.contains(column))
    }

    fn record_indexed_column(&mut self, column: &'static str) {
        if !self.indexed_columns.contains(&column) {
            self.indexed_columns.push(column);
        }
    }

    fn next_local_insert_id(&mut self, replica_idx: usize) -> i64 {
        let id = self.next_local_ids[replica_idx];
        self.next_local_ids[replica_idx] += 1;
        self.local_live_ids[replica_idx].push(id);
        id
    }

    fn next_remote_insert_id(&mut self) -> i64 {
        let id = self.next_remote_id;
        self.next_remote_id += 1;
        self.remote_live_ids.push(id);
        id
    }
}

async fn initialize_seeded_table(conn: &Connection, table: &str) -> Result<()> {
    conn.execute(
        &format!(
            "CREATE TABLE {table} (\
                id INTEGER PRIMARY KEY, \
                owner TEXT NOT NULL, \
                payload TEXT NOT NULL, \
                rev INTEGER NOT NULL DEFAULT 0\
            )"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to create seeded table {table}"))?;
    conn.execute(
        &format!("CREATE INDEX \"{table} owner rev idx\" ON {table}(owner, rev)"),
        (),
    )
    .await
    .with_context(|| format!("failed to create seeded index for {table}"))?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev) VALUES \
                (1, 'seed-a', 'alpha', 1), \
                (2, 'seed-b', 'beta', 1)"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to seed {table}"))?;
    Ok(())
}

async fn initialize_seeded_table_remote(config: &Config, table: &str) -> Result<()> {
    execute_remote_sql(
        config,
        &format!(
            "CREATE TABLE {} (\
                id INTEGER PRIMARY KEY, \
                owner TEXT NOT NULL, \
                payload TEXT NOT NULL, \
                rev INTEGER NOT NULL DEFAULT 0\
            )",
            sql_ident(table)
        ),
    )
    .await
    .with_context(|| format!("failed to create remote seeded table {table}"))?;
    execute_remote_sql(
        config,
        &format!(
            "CREATE INDEX \"{} owner rev idx\" ON {}(owner, rev)",
            table,
            sql_ident(table)
        ),
    )
    .await
    .with_context(|| format!("failed to create remote seeded index for {table}"))?;
    execute_remote_sql(
        config,
        &format!(
            "INSERT INTO {} (id, owner, payload, rev) VALUES \
                (1, 'seed-a', 'alpha', 1), \
                (2, 'seed-b', 'beta', 1)",
            sql_ident(table)
        ),
    )
    .await
    .with_context(|| format!("failed to seed remote table {table}"))?;
    Ok(())
}

async fn run_seeded_step(
    config: &Config,
    root_dir: &std::path::Path,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
    rng: &mut ScenarioRng,
    step: usize,
) -> Result<String> {
    let op = rng.index(14);
    match op {
        0 => {
            let replica_idx = rng.index(replicas.len());
            let id = state.next_local_insert_id(replica_idx);
            local_execute_sql(
                &replicas[replica_idx],
                &build_insert_sql(
                    &state.table,
                    id,
                    &replicas[replica_idx].name,
                    &format!("local-payload-{step}"),
                    &state.available_columns,
                ),
            )
            .await?;
            assert_local_cdc_advanced(
                &replicas[replica_idx],
                state,
                replica_idx,
                &format!("{} insert id={id}", replicas[replica_idx].name),
            )
            .await?;
            Ok(format!("{} insert id={id}", replicas[replica_idx].name))
        }
        1 => {
            let replica_idx = rng.index(replicas.len());
            let (id, action) =
                if let Some(id) = pick_live_id(&state.local_live_ids[replica_idx], rng) {
                    local_execute_sql(
                        &replicas[replica_idx],
                        &build_update_sql(
                            &state.table,
                            id,
                            &replicas[replica_idx].name,
                            step,
                            &state.available_columns,
                        ),
                    )
                    .await?;
                    (id, "update")
                } else {
                    let id = state.next_local_insert_id(replica_idx);
                    local_execute_sql(
                        &replicas[replica_idx],
                        &build_insert_sql(
                            &state.table,
                            id,
                            &replicas[replica_idx].name,
                            &format!("local-bootstrap-{step}"),
                            &state.available_columns,
                        ),
                    )
                    .await?;
                    (id, "bootstrap insert")
                };
            assert_local_cdc_advanced(
                &replicas[replica_idx],
                state,
                replica_idx,
                &format!("{} {action} id={id}", replicas[replica_idx].name),
            )
            .await?;
            Ok(format!("{} {action} id={id}", replicas[replica_idx].name))
        }
        2 => {
            let replica_idx = rng.index(replicas.len());
            if let Some(id) = pick_live_id(&state.local_live_ids[replica_idx], rng) {
                local_execute_sql(
                    &replicas[replica_idx],
                    &format!("DELETE FROM {} WHERE id = {id}", state.table),
                )
                .await?;
                assert_local_cdc_advanced(
                    &replicas[replica_idx],
                    state,
                    replica_idx,
                    &format!("{} delete id={id}", replicas[replica_idx].name),
                )
                .await?;
                remove_live_id(&mut state.local_live_ids[replica_idx], id);
                Ok(format!("{} delete id={id}", replicas[replica_idx].name))
            } else {
                Ok(format!(
                    "{} delete skipped (no local rows)",
                    replicas[replica_idx].name
                ))
            }
        }
        3 => {
            let id = state.next_remote_insert_id();
            execute_remote_sql(
                config,
                &build_insert_sql(
                    &state.table,
                    id,
                    "remote-owner",
                    &format!("remote-payload-{step}"),
                    &state.available_columns,
                ),
            )
            .await?;
            Ok(format!("remote insert id={id}"))
        }
        4 => {
            let (id, action) = if let Some(id) = pick_live_id(&state.remote_live_ids, rng) {
                execute_remote_sql(
                    config,
                    &build_update_sql(
                        &state.table,
                        id,
                        "remote-owner",
                        step,
                        &state.available_columns,
                    ),
                )
                .await?;
                (id, "update")
            } else {
                let id = state.next_remote_insert_id();
                execute_remote_sql(
                    config,
                    &build_insert_sql(
                        &state.table,
                        id,
                        "remote-owner",
                        &format!("remote-bootstrap-{step}"),
                        &state.available_columns,
                    ),
                )
                .await?;
                (id, "bootstrap insert")
            };
            Ok(format!("remote {action} id={id}"))
        }
        5 => {
            if let Some(id) = pick_live_id(&state.remote_live_ids, rng) {
                execute_remote_sql(
                    config,
                    &format!("DELETE FROM {} WHERE id = {id}", state.table),
                )
                .await?;
                remove_live_id(&mut state.remote_live_ids, id);
                Ok(format!("remote delete id={id}"))
            } else {
                Ok("remote delete skipped (no remote rows)".to_string())
            }
        }
        6 => {
            let replica_idx = rng.index(replicas.len());
            replicas[replica_idx]
                .db
                .push()
                .await
                .with_context(|| format!("push failed for {}", replicas[replica_idx].name))?;
            assert_seeded_push_cdc_high_water(
                config,
                &replicas[replica_idx],
                state,
                replica_idx,
                &format!("{} push", replicas[replica_idx].name),
            )
            .await?;
            Ok(format!("{} push", replicas[replica_idx].name))
        }
        7 => {
            let replica_idx = rng.index(replicas.len());
            let label = format!("{} pull", replicas[replica_idx].name);
            let pulled = pull_replica_with_legacy_retry(config, &mut replicas[replica_idx], &label)
                .await
                .with_context(|| format!("pull failed for {}", replicas[replica_idx].name))?;
            Ok(format!(
                "{} pull pulled={pulled}",
                replicas[replica_idx].name
            ))
        }
        8 => {
            let replica_idx = rng.index(replicas.len());
            replicas[replica_idx]
                .db
                .checkpoint()
                .await
                .with_context(|| format!("checkpoint failed for {}", replicas[replica_idx].name))?;
            Ok(format!("{} checkpoint", replicas[replica_idx].name))
        }
        9 => {
            let replica_idx = rng.index(replicas.len());
            reopen_replica(config, &mut replicas[replica_idx]).await?;
            Ok(format!("{} reopen", replicas[replica_idx].name))
        }
        10 if state.next_pending_column().is_some() => {
            let actor = if rng.bool() {
                SchemaActor::Local(rng.index(replicas.len()))
            } else {
                SchemaActor::Remote
            };
            apply_seeded_schema_change(config, replicas, state, actor).await
        }
        11 if state.next_unindexed_column().is_some() => {
            let actor = if rng.bool() {
                SchemaActor::Local(rng.index(replicas.len()))
            } else {
                SchemaActor::Remote
            };
            apply_seeded_index_creation(config, replicas, state, actor).await
        }
        12 => {
            state.record_note("starting fresh bootstrap consistency check");
            run_bootstrap_check(
                config,
                root_dir,
                &state.table,
                state.bootstrap_checks,
                replicas,
                Some(state),
            )
            .await?;
            state.bootstrap_checks += 1;
            Ok("fresh bootstrap consistency check".to_string())
        }
        _ => {
            state.record_note(format!("starting seeded explicit converge {step}"));
            wait_for_cluster_convergence(
                config,
                replicas,
                &state.table,
                &format!("seeded explicit converge {step}"),
                Some(state),
            )
            .await?;
            Ok("explicit cluster convergence".to_string())
        }
    }
}

async fn apply_seeded_schema_change(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
    actor: SchemaActor,
) -> Result<String> {
    let column = state
        .next_pending_column()
        .context("no pending columns for schema change")?;
    let alter_sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        state.table, column.name, column.ddl
    );
    let backfill_sql = format!(
        "UPDATE {} SET {} = {}",
        state.table,
        column.name,
        extra_value_sql(column.name, "schema", 0)
    );
    match actor {
        SchemaActor::Local(replica_idx) => {
            local_execute_sql(&replicas[replica_idx], &alter_sql).await?;
            local_execute_sql(&replicas[replica_idx], &backfill_sql).await?;
            assert_local_cdc_advanced(
                &replicas[replica_idx],
                state,
                replica_idx,
                &format!("{} add column {}", replicas[replica_idx].name, column.name),
            )
            .await?;
            replicas[replica_idx]
                .db
                .push()
                .await
                .with_context(|| format!("push failed for {}", replicas[replica_idx].name))?;
            assert_seeded_push_cdc_high_water(
                config,
                &replicas[replica_idx],
                state,
                replica_idx,
                &format!("{} add column push", replicas[replica_idx].name),
            )
            .await?;
        }
        SchemaActor::Remote => {
            execute_remote_sql(config, &alter_sql).await?;
            execute_remote_sql(config, &backfill_sql).await?;
        }
    }
    state.record_added_column(column.name);
    let description = match actor {
        SchemaActor::Local(replica_idx) => {
            format!("{} add column {}", replicas[replica_idx].name, column.name)
        }
        SchemaActor::Remote => format!("remote add column {}", column.name),
    };
    state.record_note(format!("starting {description}"));
    wait_for_cluster_convergence(
        config,
        replicas,
        &state.table,
        &format!("schema change {}", column.name),
        Some(state),
    )
    .await?;
    Ok(description)
}

async fn apply_seeded_index_creation(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
    actor: SchemaActor,
) -> Result<String> {
    let column = state
        .next_unindexed_column()
        .context("no unindexed columns available")?;
    let sql = format!(
        "CREATE INDEX \"{} {} idx\" ON {}({})",
        state.table, column, state.table, column
    );
    match actor {
        SchemaActor::Local(replica_idx) => {
            local_execute_sql(&replicas[replica_idx], &sql).await?;
            assert_local_cdc_advanced(
                &replicas[replica_idx],
                state,
                replica_idx,
                &format!("{} create index on {}", replicas[replica_idx].name, column),
            )
            .await?;
            replicas[replica_idx]
                .db
                .push()
                .await
                .with_context(|| format!("push failed for {}", replicas[replica_idx].name))?;
            assert_seeded_push_cdc_high_water(
                config,
                &replicas[replica_idx],
                state,
                replica_idx,
                &format!("{} create index push", replicas[replica_idx].name),
            )
            .await?;
        }
        SchemaActor::Remote => execute_remote_sql(config, &sql).await?,
    }
    state.record_indexed_column(column);
    let description = match actor {
        SchemaActor::Local(replica_idx) => {
            format!(
                "{} create quoted index on {}",
                replicas[replica_idx].name, column
            )
        }
        SchemaActor::Remote => format!("remote create quoted index on {}", column),
    };
    state.record_note(format!("starting {description}"));
    wait_for_cluster_convergence(
        config,
        replicas,
        &state.table,
        &format!("create index {}", column),
        Some(state),
    )
    .await?;
    Ok(description)
}

async fn run_bootstrap_check(
    config: &Config,
    root_dir: &std::path::Path,
    table: &str,
    bootstrap_checks: usize,
    replicas: &mut [LocalReplica],
    diagnostics: Option<&SeededScenarioState>,
) -> Result<()> {
    let path = root_dir.join(format!("bootstrap-check-{bootstrap_checks}.db"));
    let mut replica = build_local_replica(
        format!("bootstrap-check-{bootstrap_checks}"),
        path,
        config,
        true,
        replicas
            .first()
            .map(|replica| replica.protocol)
            .unwrap_or(SyncProtocol::Mvcc),
    )
    .await?;
    let conn = replica
        .db
        .connect()
        .await
        .context("failed to connect bootstrap-check replica")?;
    ensure_protocol_mode(&conn, replica.protocol).await?;
    drop(conn);
    wait_for_cluster_convergence(
        config,
        std::slice::from_mut(&mut replica),
        table,
        "bootstrap check",
        diagnostics,
    )
    .await?;
    assert_pull_idempotence(
        config,
        std::slice::from_mut(&mut replica),
        table,
        "bootstrap check",
        diagnostics,
    )
    .await?;
    for existing in replicas.iter_mut() {
        update_revision_monotonic(existing, "bootstrap comparison").await?;
    }
    Ok(())
}

async fn run_targeted_mvcc_logical_coverage_suite(
    config: &Config,
    root_dir: &Path,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
) -> Result<()> {
    state.record_note("starting targeted MVCC logical coverage suite");

    run_mvcc_legacy_incremental_pull_rejection_check(config, replicas, state).await?;
    run_targeted_mvcc_push_coverage_suite(config, replicas, state).await?;
    assert_scripted_current_generation_logical_pull_after_remote_drop(config).await?;
    run_orphan_index_checkpoint_probe(config, root_dir, "seeded-targeted").await?;
    let previous_generation = query_remote_generation(config).await?;
    let next_generation = force_remote_generation_rollover_with_push_pressure(
        config,
        replicas
            .first()
            .context("expected at least one replica for orphan-index generation rollover")?,
        0,
        state,
        "orphan-index-probe",
        previous_generation,
    )
    .await?;
    state.record_note(format!(
        "orphan-index probe forced generation rollover from {previous_generation} to {next_generation}"
    ));
    assert_no_remote_orphan_indexes(config, "seeded-targeted after orphan-index rollover").await?;
    run_replace_base_local_preservation_check(config, root_dir, state).await?;
    run_incremental_logical_stream_check(config, replicas, state).await?;
    run_quoted_identifier_logical_stream_check(config, replicas, state).await?;
    run_mixed_transaction_logical_stream_check(config, replicas, state, SchemaActor::Remote)
        .await?;
    run_mixed_transaction_logical_stream_check(config, replicas, state, SchemaActor::Local(0))
        .await?;

    let snapshot = wait_for_cluster_convergence(
        config,
        replicas,
        &state.table,
        SEEDED_TARGETED_LABEL,
        Some(state),
    )
    .await?;
    print_compact_snapshot(SEEDED_TARGETED_LABEL, &snapshot);

    Ok(())
}

async fn run_orphan_index_checkpoint_probe(
    config: &Config,
    root_dir: &Path,
    label: &str,
) -> Result<()> {
    println!("[orphan-index-probe] starting {ORPHAN_INDEX_PROBE_ROUNDS} rounds for {label}");
    assert_no_remote_orphan_indexes(config, &format!("{label} before orphan-index probe")).await?;

    let local = build_local_replica(
        format!("orphan-index-probe-{label}"),
        root_dir.join(format!("orphan-index-probe-{label}.db")),
        config,
        false,
        SyncProtocol::Mvcc,
    )
    .await?;
    let conn = local
        .db
        .connect()
        .await
        .context("failed to connect orphan-index probe replica")?;
    ensure_mvcc_mode(&conn).await?;

    for round in 0..ORPHAN_INDEX_PROBE_ROUNDS {
        let table = unique_name(&format!("mvcc_sync_orphan_probe_{round}"));
        let index = format!("{table}_owner_rev_idx");
        execute_remote_sql(
            config,
            &format!("DROP TABLE IF EXISTS {}", sql_ident(&table)),
        )
        .await
        .with_context(|| format!("failed to clean remote orphan-index probe table {table}"))?;

        local_execute_sql(
            &local,
            &format!(
                "CREATE TABLE {} (\
                    id INTEGER PRIMARY KEY,\
                    owner TEXT NOT NULL,\
                    payload TEXT NOT NULL,\
                    rev INTEGER NOT NULL\
                )",
                sql_ident(&table)
            ),
        )
        .await?;
        local_execute_sql(
            &local,
            &format!(
                "CREATE INDEX {} ON {} (owner, rev)",
                sql_ident(&index),
                sql_ident(&table)
            ),
        )
        .await?;
        local_execute_sql(
            &local,
            &format!(
                "INSERT INTO {} (id, owner, payload, rev) VALUES ({}, 'probe', 'created-via-sync', 1)",
                sql_ident(&table),
                round + 1
            ),
        )
        .await?;
        local
            .db
            .push()
            .await
            .with_context(|| format!("failed to push orphan-index probe round {round}"))?;

        let schema_rows = query_remote_sql(
            config,
            &format!(
                "SELECT type, name, tbl_name, rootpage, sql \
                 FROM sqlite_schema \
                 WHERE name = {} OR tbl_name = {} \
                 ORDER BY type, name",
                sql_text(&index),
                sql_text(&table)
            ),
        )
        .await
        .with_context(|| format!("failed to inspect remote schema after probe push for {table}"))?;
        println!("[orphan-index-probe] round {round} schema after sync push: {schema_rows:?}");
        if !schema_rows.iter().any(|row| {
            matches!(
                row.as_slice(),
                [Value::Text(kind), Value::Text(name), ..]
                    if kind == "index" && name == &index
            )
        }) {
            bail!(
                "orphan-index probe round {round} did not create expected remote index {index}; rows={schema_rows:?}"
            );
        }

        checkpoint_remote_and_assert_no_orphan_indexes(
            config,
            &format!("{label} round {round} after sync-created index"),
        )
        .await?;
        execute_remote_sql(
            config,
            &format!("DROP TABLE IF EXISTS {}", sql_ident(&table)),
        )
        .await
        .with_context(|| format!("failed to drop remote orphan-index probe table {table}"))?;
        checkpoint_remote_and_assert_no_orphan_indexes(
            config,
            &format!("{label} round {round} after remote drop table"),
        )
        .await?;

        let remaining = query_remote_sql(
            config,
            &format!(
                "SELECT type, name, tbl_name, rootpage, sql \
                 FROM sqlite_schema \
                 WHERE name = {} OR tbl_name = {} \
                 ORDER BY type, name",
                sql_text(&index),
                sql_text(&table)
            ),
        )
        .await
        .with_context(|| format!("failed to inspect remote schema after dropping {table}"))?;
        if !remaining.is_empty() {
            bail!(
                "orphan-index probe round {round} left sqlite_schema rows after DROP TABLE {table}: {remaining:?}"
            );
        }
    }

    Ok(())
}

async fn checkpoint_remote_and_assert_no_orphan_indexes(
    config: &Config,
    label: &str,
) -> Result<()> {
    match query_remote_sql(config, "PRAGMA wal_checkpoint(TRUNCATE)").await {
        Ok(rows) => println!("[orphan-index-probe] {label} checkpoint rows: {rows:?}"),
        Err(error) => {
            println!(
                "[orphan-index-probe] {label} checkpoint pragma unavailable; continuing with schema assertion: {error:#}"
            );
        }
    }
    assert_no_remote_orphan_indexes(config, label).await
}

async fn assert_no_remote_orphan_indexes(config: &Config, label: &str) -> Result<()> {
    let rows = query_remote_sql(
        config,
        "SELECT idx.rowid, idx.name, idx.tbl_name, idx.rootpage, idx.sql \
         FROM sqlite_schema AS idx \
         LEFT JOIN sqlite_schema AS tbl \
           ON tbl.type = 'table' AND tbl.name = idx.tbl_name \
         WHERE idx.type = 'index' \
           AND idx.sql IS NOT NULL \
           AND tbl.name IS NULL \
         ORDER BY idx.rowid",
    )
    .await
    .with_context(|| format!("failed to query remote orphan indexes during {label}"))?;
    if !rows.is_empty() {
        bail!("remote orphan sqlite_schema indexes during {label}: {rows:?}");
    }
    Ok(())
}

async fn run_targeted_mvcc_push_coverage_suite(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
) -> Result<()> {
    let replica_idx = 0;
    let replica = replicas
        .get(replica_idx)
        .context("expected at least one replica for targeted MVCC push coverage")?;
    let replica_name = replica.name.clone();
    state.record_note(format!(
        "starting targeted MVCC push coverage on {replica_name}"
    ));

    let insert_id = state.next_local_insert_id(replica_idx);
    local_execute_sql(
        &replicas[replica_idx],
        &build_insert_sql(
            &state.table,
            insert_id,
            &replica_name,
            "targeted-push-insert",
            &state.available_columns,
        ),
    )
    .await?;
    assert_local_cdc_advanced(
        &replicas[replica_idx],
        state,
        replica_idx,
        &format!("{replica_name} targeted push insert id={insert_id}"),
    )
    .await?;
    replicas[replica_idx]
        .db
        .push()
        .await
        .with_context(|| format!("targeted insert push failed for {replica_name}"))?;
    assert_seeded_push_cdc_high_water(
        config,
        &replicas[replica_idx],
        state,
        replica_idx,
        &format!("{replica_name} targeted insert push"),
    )
    .await?;
    assert_remote_row_payload(
        config,
        &state.table,
        insert_id,
        &replica_name,
        "targeted-push-insert",
        1,
        "targeted insert push",
    )
    .await?;

    let client_id = replica_client_unique_id(&replicas[replica_idx])?;
    let pushed_before = remote_sync_change_id_for_client(config, &client_id)
        .await?
        .context("expected remote high-water after targeted insert push")?;
    replicas[replica_idx]
        .db
        .push()
        .await
        .with_context(|| format!("targeted idempotent push failed for {replica_name}"))?;
    let pushed_after = remote_sync_change_id_for_client(config, &client_id)
        .await?
        .context("expected remote high-water after targeted idempotent push")?;
    if pushed_after != pushed_before {
        bail!(
            "idempotent targeted push advanced remote high-water unexpectedly: client_id={client_id} before={pushed_before} after={pushed_after}"
        );
    }

    let update_step = 70_001;
    local_execute_sql(
        &replicas[replica_idx],
        &build_update_sql(
            &state.table,
            insert_id,
            &replica_name,
            update_step,
            &state.available_columns,
        ),
    )
    .await?;
    assert_local_cdc_advanced(
        &replicas[replica_idx],
        state,
        replica_idx,
        &format!("{replica_name} targeted push update id={insert_id}"),
    )
    .await?;
    replicas[replica_idx]
        .db
        .push()
        .await
        .with_context(|| format!("targeted update push failed for {replica_name}"))?;
    assert_seeded_push_cdc_high_water(
        config,
        &replicas[replica_idx],
        state,
        replica_idx,
        &format!("{replica_name} targeted update push"),
    )
    .await?;
    assert_remote_row_payload(
        config,
        &state.table,
        insert_id,
        &replica_name,
        &format!("{replica_name}-updated-{update_step}"),
        2,
        "targeted update push",
    )
    .await?;

    local_execute_sql(
        &replicas[replica_idx],
        &format!("DELETE FROM {} WHERE id = {insert_id}", state.table),
    )
    .await?;
    assert_local_cdc_advanced(
        &replicas[replica_idx],
        state,
        replica_idx,
        &format!("{replica_name} targeted push delete id={insert_id}"),
    )
    .await?;
    replicas[replica_idx]
        .db
        .push()
        .await
        .with_context(|| format!("targeted delete push failed for {replica_name}"))?;
    assert_seeded_push_cdc_high_water(
        config,
        &replicas[replica_idx],
        state,
        replica_idx,
        &format!("{replica_name} targeted delete push"),
    )
    .await?;
    assert_remote_row_absent(config, &state.table, insert_id, "targeted delete push").await?;
    remove_live_id(&mut state.local_live_ids[replica_idx], insert_id);

    let ddl_table = unique_name("mvcc_sync_push_probe");
    execute_remote_sql(
        config,
        &format!("DROP TABLE IF EXISTS {}", sql_ident(&ddl_table)),
    )
    .await?;
    local_execute_sql(
        &replicas[replica_idx],
        &format!(
            "CREATE TABLE {}(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            sql_ident(&ddl_table)
        ),
    )
    .await?;
    local_execute_sql(
        &replicas[replica_idx],
        &format!(
            "INSERT INTO {}(id, payload) VALUES (1, 'targeted-ddl-push')",
            sql_ident(&ddl_table)
        ),
    )
    .await?;
    local_execute_sql(
        &replicas[replica_idx],
        &format!(
            "CREATE INDEX {} ON {}(payload)",
            sql_ident(&format!("{ddl_table} payload idx")),
            sql_ident(&ddl_table),
        ),
    )
    .await?;
    assert_local_cdc_advanced(
        &replicas[replica_idx],
        state,
        replica_idx,
        &format!("{replica_name} targeted DDL push table={ddl_table}"),
    )
    .await?;
    replicas[replica_idx]
        .db
        .push()
        .await
        .with_context(|| format!("targeted DDL push failed for {replica_name}"))?;
    assert_seeded_push_cdc_high_water(
        config,
        &replicas[replica_idx],
        state,
        replica_idx,
        &format!("{replica_name} targeted DDL push"),
    )
    .await?;
    assert_remote_push_probe_table(config, &ddl_table).await?;

    wait_for_cluster_convergence(
        config,
        replicas,
        &state.table,
        "targeted MVCC push coverage",
        Some(state),
    )
    .await?;
    state.record_note(format!(
        "{replica_name} targeted MVCC push coverage completed"
    ));
    Ok(())
}

async fn run_mvcc_legacy_incremental_pull_rejection_check(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
) -> Result<()> {
    let client_revision = current_replica_revision(
        replicas
            .last()
            .context("expected at least one replica for legacy MVCC pull check")?,
    )
    .await?;
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder().build()?;
    let request_body = ProbePullUpdatesReqProtoBody {
        encoding: ProbePageUpdatesEncodingReq::Raw as i32,
        logical_updates: false,
        server_revision: String::new(),
        client_revision: client_revision.clone(),
        long_poll_timeout_ms: 0,
        server_pages_selector: Vec::new(),
        server_query_selector: String::new(),
        client_pages: Vec::new(),
    }
    .encode_to_vec();
    let mut request = client
        .post(format!("{base_url}/pull-updates"))
        .header("content-type", "application/protobuf")
        .header("accept-encoding", "application/protobuf")
        .body(request_body);
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }
    if let Some(key) = &config.remote_encryption_key {
        request = request.header("x-turso-encryption-key", key);
    }

    let response = request
        .send()
        .await
        .context("failed to probe legacy MVCC pull rejection")?;
    if response.status().is_success() {
        let body = response
            .bytes()
            .await
            .context("failed to read unexpected legacy MVCC pull response")?;
        let stream_kind = read_probe_stream_kind(&body)?;
        bail!(
            "MVCC incremental pull without logical_updates unexpectedly succeeded: client_revision={} stream_kind={:?} bytes={}",
            client_revision,
            stream_kind,
            body.len(),
        );
    }
    state.record_note(format!(
        "legacy MVCC incremental pull rejected for revision {}",
        client_revision
    ));
    Ok(())
}

async fn assert_scripted_mvcc_incremental_page_pull_is_rejected(
    config: &Config,
    replica: &LocalReplica,
) -> Result<()> {
    let client_revision = ensure_replica_revision_initialized(config, replica).await?;
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder().build()?;
    let request_body = ProbePullUpdatesReqProtoBody {
        encoding: ProbePageUpdatesEncodingReq::Raw as i32,
        logical_updates: false,
        server_revision: String::new(),
        client_revision: client_revision.clone(),
        long_poll_timeout_ms: 0,
        server_pages_selector: Vec::new(),
        server_query_selector: String::new(),
        client_pages: Vec::new(),
    }
    .encode_to_vec();
    let mut request = client
        .post(format!("{base_url}/pull-updates"))
        .header("content-type", "application/protobuf")
        .header("accept-encoding", "application/protobuf")
        .body(request_body);
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }
    if let Some(key) = &config.remote_encryption_key {
        request = request.header("x-turso-encryption-key", key);
    }

    let response = request
        .send()
        .await
        .context("failed to probe scripted MVCC incremental page pull rejection")?;
    if response.status().is_success() {
        let body = response
            .bytes()
            .await
            .context("failed to read unexpected scripted MVCC page-pull response")?;
        let stream_kind = read_probe_stream_kind(&body)?;
        bail!(
            "scripted check expected MVCC incremental page pull to be rejected, but it succeeded: replica={} client_revision={} stream_kind={:?} bytes={}",
            replica.name,
            client_revision,
            stream_kind,
            body.len(),
        );
    }

    println!(
        "scripted check: MVCC incremental page pull is currently rejected for replica={} revision={}",
        replica.name, client_revision
    );
    if config.remote_encryption_key.is_some() {
        println!(
            "scripted note: encrypted MVCC remotes still do not provide a working end-to-end page fallback path here; this run only verifies the current rejection explicitly"
        );
    }
    Ok(())
}

async fn assert_scripted_current_generation_logical_pull_after_remote_drop(
    config: &Config,
) -> Result<()> {
    if env_flag_enabled("SYNC_EXAMPLE_SKIP_CURRENT_DROP_PROBE") {
        println!("scripted note: skipping current-generation logical drop probe by env request");
        return Ok(());
    }
    if config.remote_encryption_key.is_some() {
        println!(
            "scripted note: skipping current-generation logical drop probe because encrypted remotes do not use logical MVCC pull"
        );
        return Ok(());
    }

    let table = unique_name("mvcc_sync_current_drop_probe");
    execute_remote_sql(
        config,
        &format!("DROP TABLE IF EXISTS {}", sql_ident(&table)),
    )
    .await?;
    execute_remote_sql(
        config,
        &format!(
            "CREATE TABLE {}(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            sql_ident(&table),
        ),
    )
    .await?;

    let created_probe = probe_pull_updates_stream(config, "")
        .await
        .with_context(|| {
            format!(
                "failed to read post-create revision for current-generation drop probe table {}",
                table
            )
        })?;
    let client_revision = created_probe.server_revision.clone();

    execute_remote_sql(
        config,
        &format!(
            "INSERT INTO {}(id, payload) VALUES (1, 'current-drop-a'), (2, 'current-drop-b')",
            sql_ident(&table),
        ),
    )
    .await?;
    execute_remote_sql(config, &format!("DROP TABLE {}", sql_ident(&table))).await?;

    let probe = probe_pull_updates_stream(config, &client_revision)
        .await
        .with_context(|| {
            format!(
                "failed to probe current-generation logical drop from revision {} for {}",
                client_revision, table
            )
        })?;

    let client_mvcc_revision: MvccRevision =
        serde_json::from_str(&client_revision).with_context(|| {
            format!("invalid client revision for current-drop probe: {client_revision}")
        })?;
    let server_mvcc_revision: MvccRevision = serde_json::from_str(&probe.server_revision)
        .with_context(|| {
            format!(
                "invalid server revision for current-drop probe: {}",
                probe.server_revision
            )
        })?;
    if server_mvcc_revision.generation != client_mvcc_revision.generation {
        println!(
            "scripted note: current-generation logical drop probe crossed generation {} -> {}; server returned {:?}/{:?}",
            client_mvcc_revision.generation,
            server_mvcc_revision.generation,
            probe.stream_kind,
            probe.apply_mode
        );
        return Ok(());
    }

    if !probe_is_logical(&probe) {
        bail!(
            "current-generation drop scripted check expected logical stream, got stream_kind={:?}; client_revision={} server_revision={} apply_mode={:?}",
            probe.stream_kind,
            client_revision,
            probe.server_revision,
            probe.apply_mode,
        );
    }
    if probe.apply_mode != Some(PullUpdatesApplyMode::Incremental) {
        bail!(
            "current-generation drop scripted check expected apply_mode=incremental, got {:?}; client_revision={} server_revision={} trailing_messages={}",
            probe.apply_mode,
            client_revision,
            probe.server_revision,
            probe.trailing_messages,
        );
    }
    if !probe_has_logical_payload(&probe) {
        bail!(
            "current-generation drop scripted check expected logical messages in response; client_revision={} server_revision={}",
            client_revision,
            probe.server_revision,
        );
    }
    assert_probe_has_row_upsert(&probe, &table, 1)?;
    assert_probe_has_row_upsert(&probe, &table, 2)?;
    assert_probe_has_schema_op(
        &probe,
        ProbeLogicalSchemaAction::Drop,
        ProbeLogicalSchemaKind::Table,
        &table,
    )?;

    println!(
        "scripted check: current-generation logical drop replay succeeded for stale revision {} -> {}",
        client_revision, probe.server_revision
    );
    Ok(())
}

async fn run_replace_base_local_preservation_check(
    config: &Config,
    root_dir: &Path,
    state: &mut SeededScenarioState,
) -> Result<()> {
    if config.remote_encryption_key.is_some() {
        state.record_note(
            "skipping replace-base local preservation check because encrypted remotes do not use logical MVCC pull",
        );
        return Ok(());
    }

    let replica_name = "replace-base-preservation-replica".to_string();
    let replica_path = root_dir.join("replace-base-preservation.db");
    let mut replica = build_local_replica(
        replica_name.clone(),
        replica_path,
        config,
        true,
        SyncProtocol::Mvcc,
    )
    .await
    .context("failed to build isolated replace-base preservation replica")?;
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect isolated {replica_name}"))?;
    ensure_mvcc_mode(&conn).await?;
    drop(conn);

    let current_revision_string = current_replica_revision(&replica)
        .await
        .context("failed to read current replica revision for replace-base preservation check")?;
    let current_revision: RichMvccRevision = serde_json::from_str(&current_revision_string)
        .with_context(|| {
            format!(
                "invalid current replica revision for replace-base preservation check: {}",
                current_revision_string
            )
        })?;
    let unavailable_generation = current_revision
        .generation
        .checked_add(1_000_000)
        .with_context(|| {
            format!(
                "failed to synthesize unavailable retained generation from {}",
                current_revision.generation
            )
        })?;
    if unavailable_generation >= MVCC_GENERATION_ID_MAX_EXCLUSIVE {
        state.record_note(format!(
            "skipping synthetic replace-base fallback probe because current generation {} is too close to max generation {}",
            current_revision.generation, MVCC_GENERATION_ID_MAX_EXCLUSIVE,
        ));
        return Ok(());
    }
    let unavailable_revision = json!({
        "generation": unavailable_generation,
        "log_offset": MVCC_LOG_HDR_SIZE,
    })
    .to_string();

    let replace_probe = probe_pull_updates_header(config, &unavailable_revision)
        .await
        .with_context(|| {
            format!(
                "failed to probe replace-base fallback for unavailable MVCC revision {}",
                unavailable_revision
            )
        })?;
    if replace_probe.stream_kind != Some(PullUpdatesStreamKind::Pages) {
        bail!(
            "expected replace-base fallback to use page stream, got {:?}; client_revision={} server_revision={} apply_mode={:?}",
            replace_probe.stream_kind,
            unavailable_revision,
            replace_probe.server_revision,
            replace_probe.apply_mode,
        );
    }
    if replace_probe.apply_mode != Some(PullUpdatesApplyMode::ReplaceBase) {
        bail!(
            "expected replace-base fallback apply_mode=replace_base, got {:?}; client_revision={} server_revision={}",
            replace_probe.apply_mode,
            unavailable_revision,
            replace_probe.server_revision,
        );
    }
    if replace_probe.db_size == 0 {
        bail!(
            "replace-base fallback returned an empty database size; client_revision={} server_revision={}",
            unavailable_revision, replace_probe.server_revision,
        );
    }

    let noop_probe = probe_pull_updates_stream(config, &replace_probe.server_revision).await?;
    if noop_probe.apply_mode != Some(PullUpdatesApplyMode::Incremental)
        || noop_probe.trailing_messages != 0
    {
        bail!(
            "replace-base server revision was not a clean no-op on the next probe: revision={} apply_mode={:?} trailing_messages={}",
            replace_probe.server_revision,
            noop_probe.apply_mode,
            noop_probe.trailing_messages,
        );
    }

    let local_insert_id = 9_100_000;
    let local_update_id = 1;
    let local_delete_id = 2;
    let cdc_before_local_changes = local_pushable_cdc_max_change_id(&replica)
        .await
        .context("failed to read replace-base local CDC high-water before local edits")?;
    local_execute_sql(
        &replica,
        &build_insert_sql(
            &state.table,
            local_insert_id,
            "replace-base-local",
            "replace-base-local-insert",
            &state.available_columns,
        ),
    )
    .await?;
    local_execute_sql(
        &replica,
        &format!(
            "UPDATE {} SET payload = 'replace-base-local-update', rev = rev + 1 WHERE id = {local_update_id}",
            state.table
        ),
    )
    .await?;
    local_execute_sql(
        &replica,
        &format!("DELETE FROM {} WHERE id = {local_delete_id}", state.table),
    )
    .await?;
    let cdc_after_local_changes = local_pushable_cdc_max_change_id(&replica)
        .await
        .context("failed to read replace-base local CDC high-water after local edits")?;
    let sync_floor_after_local_changes = local_sync_change_id_for_replica(&replica)
        .await
        .context("failed to read replace-base local sync high-water after local edits")?;
    let replayable_after_local_changes = local_replayable_cdc_count(&replica)
        .await
        .context("failed to count replace-base replayable local CDC changes")?;
    if cdc_after_local_changes <= cdc_before_local_changes {
        let diagnostics = local_cdc_debug_summary(&replica, &state.table)
            .await
            .unwrap_or_else(|err| format!("<failed to gather diagnostics: {err:#}>"));
        bail!(
            "replace-base check did not create pending local CDC changes before injected failure: before={cdc_before_local_changes:?} after={cdc_after_local_changes:?} diagnostics={diagnostics}"
        );
    }
    if replayable_after_local_changes == 0 {
        let diagnostics = local_cdc_debug_summary(&replica, &state.table)
            .await
            .unwrap_or_else(|err| format!("<failed to gather diagnostics: {err:#}>"));
        bail!(
            "replace-base check has no replayable local CDC changes before injected failure: sync_floor={sync_floor_after_local_changes:?} cdc_before={cdc_before_local_changes:?} cdc_after={cdc_after_local_changes:?} diagnostics={diagnostics}"
        );
    }

    overwrite_replica_synced_revision(&replica, &unavailable_revision)
        .context("failed to force synthetic stale revision before replace-base local apply")?;
    reopen_replica(config, &mut replica)
        .await
        .context("failed to reopen replica after forcing synthetic stale revision")?;
    env::set_var(REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER_ENV, "0");
    let failed_pull = replica.db.pull().await;
    env::remove_var(REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER_ENV);
    let err = failed_pull
        .err()
        .with_context(|| {
            format!(
                "replace-base injected local replay failure unexpectedly succeeded: sync_floor_before={sync_floor_after_local_changes:?} replayable_before={replayable_after_local_changes} cdc_before={cdc_before_local_changes:?} cdc_after={cdc_after_local_changes:?}"
            )
        })?;
    if !format!("{err:#}").contains("injected replace-base local replay failure") {
        bail!("replace-base injected failure returned unexpected error: {err:#}");
    }
    let revision_after_failure = current_replica_revision(&replica)
        .await
        .context("failed to read revision after injected replace-base failure")?;
    if revision_after_failure != unavailable_revision {
        bail!(
            "replace-base injected failure advanced synced revision unexpectedly: before={} after={}",
            unavailable_revision,
            revision_after_failure
        );
    }
    if replica_replace_base_marker_path(&replica).exists() {
        bail!(
            "replace-base recovery marker still exists after injected failure restore: {}",
            replica_replace_base_marker_path(&replica).display()
        );
    }

    reopen_replica(config, &mut replica)
        .await
        .context("failed to reopen replica after injected replace-base failure")?;
    assert_replace_base_local_rows(
        &replica,
        &state.table,
        local_insert_id,
        local_update_id,
        local_delete_id,
        "after injected replace-base failure restore",
    )
    .await?;

    state.record_note(format!(
        "verified replace-base injected replay failure restore plus local insert/update/delete preservation from synthetic revision {}",
        unavailable_revision
    ));
    Ok(())
}

async fn assert_replace_base_local_rows(
    replica: &LocalReplica,
    table: &str,
    local_insert_id: i64,
    local_update_id: i64,
    local_delete_id: i64,
    label: &str,
) -> Result<()> {
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {} {label}", replica.name))?;
    let rows = query_rows(
        &conn,
        &format!(
            "SELECT id, owner, payload, rev FROM {} WHERE id IN ({local_insert_id}, {local_update_id}, {local_delete_id}) ORDER BY id",
            sql_ident(table),
        ),
    )
    .await?;
    let expected = vec![
        vec![
            Value::Integer(local_update_id),
            Value::Text("seed-a".to_string()),
            Value::Text("replace-base-local-update".to_string()),
            Value::Integer(2),
        ],
        vec![
            Value::Integer(local_insert_id),
            Value::Text("replace-base-local".to_string()),
            Value::Text("replace-base-local-insert".to_string()),
            Value::Integer(1),
        ],
    ];
    if rows != expected {
        bail!("replace-base local rows mismatch {label}: expected={expected:?} actual={rows:?}");
    }
    Ok(())
}

async fn ensure_replica_revision_initialized(
    config: &Config,
    replica: &LocalReplica,
) -> Result<String> {
    match current_replica_revision(replica).await {
        Ok(revision) => Ok(revision),
        Err(err) if err.to_string().contains("has no synced revision yet") => {
            let probe = probe_pull_updates_stream(config, "").await.with_context(|| {
                format!(
                    "failed to probe current server revision for scripted MVCC page-pull check on {}",
                    replica.name
                )
            })?;
            Ok(probe.server_revision)
        }
        Err(err) => Err(err),
    }
}

async fn run_incremental_logical_stream_check(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
) -> Result<()> {
    let replica_idx = replicas
        .len()
        .checked_sub(1)
        .context("expected at least one replica for incremental logical stream check")?;
    let replica_name = replicas[replica_idx].name.clone();
    let client_revision = current_replica_revision(&replicas[replica_idx]).await?;
    let remote_id = state.next_remote_insert_id();

    execute_remote_sql(
        config,
        &build_insert_sql(
            &state.table,
            remote_id,
            "remote-incremental",
            "logical-current-generation",
            &state.available_columns,
        ),
    )
    .await?;

    let probe = probe_pull_updates_stream(config, &client_revision).await?;
    if !probe_is_logical(&probe) {
        bail!(
            "expected logical pull-updates stream for incremental MVCC check, got {:?}; replica={} client_revision={} server_revision={}",
            probe.stream_kind,
            replica_name,
            client_revision,
            probe.server_revision,
        );
    }
    if !probe_has_logical_payload(&probe) {
        bail!(
            "incremental logical pull-updates probe returned no transactions; replica={} client_revision={} server_revision={}",
            replica_name,
            client_revision,
            probe.server_revision,
        );
    }
    assert_probe_has_row_upsert(&probe, &state.table, remote_id)?;

    state.record_note(format!(
        "incremental logical probe succeeded for {} with remote id {}",
        replica_name, remote_id
    ));
    Ok(())
}

async fn run_quoted_identifier_logical_stream_check(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
) -> Result<()> {
    let replica = replicas
        .last()
        .context("expected at least one replica for quoted identifier logical stream check")?;
    let client_revision = current_replica_revision(replica).await?;
    let quoted_table = format!("{}_quoted {}", state.table, state.seed);
    execute_remote_batch_sql(
        config,
        &[
            format!(
                "CREATE TABLE {}({} INTEGER PRIMARY KEY, {} TEXT NOT NULL)",
                sql_ident(&quoted_table),
                sql_ident("id col"),
                sql_ident("payload text"),
            ),
            format!(
                "INSERT INTO {}({}, {}) VALUES (1, 'quoted-payload')",
                sql_ident(&quoted_table),
                sql_ident("id col"),
                sql_ident("payload text"),
            ),
        ],
    )
    .await?;

    let probe = probe_pull_updates_stream(config, &client_revision).await?;
    if !probe_is_logical(&probe) {
        bail!(
            "expected logical pull-updates stream for quoted identifier check, got {:?}; client_revision={} server_revision={}",
            probe.stream_kind,
            client_revision,
            probe.server_revision,
        );
    }
    assert_probe_has_table_schema_op(&probe, &quoted_table)?;
    assert_probe_has_row_upsert(&probe, &quoted_table, 1)?;
    execute_remote_sql(
        config,
        &format!("DROP TABLE IF EXISTS {}", sql_ident(&quoted_table)),
    )
    .await?;
    state.record_note(format!(
        "quoted identifier logical probe succeeded for {}",
        quoted_table
    ));
    Ok(())
}

async fn run_mixed_transaction_logical_stream_check(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
    actor: SchemaActor,
) -> Result<()> {
    let observer_revision = current_replica_revision(
        replicas
            .last()
            .context("expected at least one replica for mixed logical stream check")?,
    )
    .await?;
    let index_name = format!(
        "{} {} mixed idx {}",
        state.table,
        actor.label(),
        state.next_remote_id + state.next_local_ids.iter().sum::<i64>()
    );
    let payload = format!("{}-mixed-payload", actor.label());
    let update_sql = format!(
        "UPDATE {} SET payload = {}, rev = rev + 1 WHERE id = 1",
        state.table,
        sql_text(&payload),
    );
    let index_sql = format!(
        "CREATE INDEX \"{}\" ON {}(payload)",
        index_name, state.table
    );

    let insert_id = match actor {
        SchemaActor::Local(replica_idx) => {
            let replica = &replicas[replica_idx];
            let insert_id = state.next_local_insert_id(replica_idx);
            let insert_sql = build_insert_sql(
                &state.table,
                insert_id,
                &replica.name,
                &format!("{}-mixed-insert", replica.name),
                &state.available_columns,
            );
            assert_single_local_integrity(
                replica,
                &state.table,
                &format!("{} before local mixed DDL/data transaction", replica.name),
            )
            .await?;
            let conn = replica
                .db
                .connect()
                .await
                .with_context(|| format!("failed to connect {}", replica.name))?;
            conn.execute_batch(format!(
                "BEGIN IMMEDIATE; {update_sql}; {index_sql}; {insert_sql}; COMMIT;"
            ))
            .await
            .with_context(|| {
                format!(
                    "local mixed DDL/data transaction failed on {}",
                    replica.name
                )
            })?;
            assert_single_local_integrity(
                replica,
                &state.table,
                &format!("{} after local mixed DDL/data transaction", replica.name),
            )
            .await?;
            assert_local_cdc_advanced(
                replica,
                state,
                replica_idx,
                &format!("{} mixed DDL/data transaction", replica.name),
            )
            .await?;
            replica
                .db
                .push()
                .await
                .with_context(|| format!("push failed for {}", replica.name))?;
            assert_single_local_integrity(
                replica,
                &state.table,
                &format!("{} after local mixed DDL/data push", replica.name),
            )
            .await?;
            assert_seeded_push_cdc_high_water(
                config,
                replica,
                state,
                replica_idx,
                &format!("{} mixed DDL/data push", replica.name),
            )
            .await?;
            state.record_note(format!(
                "{} mixed DDL/data logical transaction inserted id {}",
                replica.name, insert_id
            ));
            insert_id
        }
        SchemaActor::Remote => {
            let insert_id = state.next_remote_insert_id();
            let insert_sql = build_insert_sql(
                &state.table,
                insert_id,
                "remote-mixed",
                "remote-mixed-insert",
                &state.available_columns,
            );
            execute_remote_batch_sql(
                config,
                &[
                    "BEGIN IMMEDIATE".to_string(),
                    update_sql,
                    index_sql,
                    insert_sql,
                    "COMMIT".to_string(),
                ],
            )
            .await?;
            state.record_note(format!(
                "remote mixed DDL/data logical transaction inserted id {}",
                insert_id
            ));
            insert_id
        }
    };

    let probe = probe_pull_updates_stream(config, &observer_revision).await?;
    if let SchemaActor::Local(replica_idx) = actor {
        assert_single_local_integrity(
            &replicas[replica_idx],
            &state.table,
            &format!(
                "{} after local mixed DDL/data pull-updates probe",
                replicas[replica_idx].name
            ),
        )
        .await?;
    }
    if !probe_is_logical(&probe) {
        bail!(
            "expected logical pull-updates stream for mixed transaction check, got {:?}; client_revision={} server_revision={}",
            probe.stream_kind,
            observer_revision,
            probe.server_revision,
        );
    }
    assert_probe_has_schema_op(
        &probe,
        ProbeLogicalSchemaAction::Create,
        ProbeLogicalSchemaKind::Index,
        &index_name,
    )?;
    assert_probe_has_row_upsert(&probe, &state.table, insert_id)?;

    wait_for_cluster_convergence(
        config,
        replicas,
        &state.table,
        &format!("{} mixed txn logical check", actor.label()),
        Some(state),
    )
    .await?;
    Ok(())
}

async fn assert_single_local_integrity(
    replica: &LocalReplica,
    table: &str,
    label: &str,
) -> Result<()> {
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {} for integrity_check", replica.name))?;
    let local_integrity = query_rows(&conn, "PRAGMA integrity_check").await?;
    if local_integrity != vec![vec![Value::Text("ok".to_string())]] {
        eprintln!(
            "local integrity_check failed for {} at {} during {label}",
            replica.name,
            replica.path.display()
        );
        dump_local_sync_debug_tables(&conn, &replica.name, table).await?;
        pause_after_integrity_failure_if_requested().await;
        bail!(
            "local integrity_check failed for {} during {label}: {local_integrity:?}",
            replica.name
        );
    }
    Ok(())
}

async fn run_generation_rollover_logical_recovery_check(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
) -> Result<()> {
    let replica_idx = replicas
        .len()
        .checked_sub(1)
        .context("expected at least one replica for generation rollover check")?;
    let replica_name = replicas[replica_idx].name.clone();

    state.record_note(format!(
        "starting generation rollover logical recovery check on {replica_name}"
    ));

    let previous_generation = query_remote_generation(config).await?;
    let stored_revision = current_replica_revision(&replicas[replica_idx]).await?;
    let parsed_revision: RichMvccRevision = serde_json::from_str(&stored_revision)
        .with_context(|| format!("invalid stored revision for {replica_name}"))?;
    if parsed_revision.wal_fragment_no.is_none() {
        bail!(
            "replica {replica_name} revision is missing wal_fragment_no before rollover: {stored_revision}"
        );
    }
    println!(
        "generation rollover baseline: replica={} generation={} log_offset={} wal_fragment_no={:?}",
        replica_name,
        parsed_revision.generation,
        parsed_revision.log_offset,
        parsed_revision.wal_fragment_no,
    );

    let before_checkpoint_id = state.next_remote_insert_id();
    execute_remote_sql(
        config,
        &build_insert_sql(
            &state.table,
            before_checkpoint_id,
            "remote-rollover-before",
            "logical-before-checkpoint",
            &state.available_columns,
        ),
    )
    .await?;

    let next_generation = force_remote_generation_rollover_with_push_pressure(
        config,
        &replicas[0],
        0,
        state,
        "logical-rollover",
        previous_generation,
    )
    .await?;
    state.record_note(format!(
        "remote generation advanced from {} to {}",
        previous_generation, next_generation
    ));

    let after_checkpoint_id = state.next_remote_insert_id();
    execute_remote_sql(
        config,
        &build_insert_sql(
            &state.table,
            after_checkpoint_id,
            "remote-rollover-after",
            "logical-after-checkpoint",
            &state.available_columns,
        ),
    )
    .await?;

    let stripped_revision = strip_wal_fragment_from_revision(&stored_revision)?;
    let probe = probe_pull_updates_stream(config, &stripped_revision).await?;
    let server_revision: RichMvccRevision = serde_json::from_str(&probe.server_revision)
        .context("invalid server revision returned from pull-updates probe")?;
    if !probe_is_logical(&probe) {
        bail!(
            "expected logical pull-updates stream after generation rollover, got {:?}; client_revision={} server_revision={}",
            probe.stream_kind,
            stripped_revision,
            probe.server_revision,
        );
    }
    if !probe_has_logical_payload(&probe) {
        bail!(
            "logical pull-updates probe returned no transactions after generation rollover; client_revision={} server_revision={}",
            stripped_revision,
            probe.server_revision,
        );
    }
    assert_probe_has_row_upsert(&probe, &state.table, before_checkpoint_id)?;
    assert_probe_has_row_upsert(&probe, &state.table, after_checkpoint_id)?;
    if server_revision.generation >= parsed_revision.generation {
        bail!(
            "pull-updates probe did not advance to a newer internal generation: previous_internal_generation={}, server_internal_generation={} ({})",
            parsed_revision.generation,
            server_revision.generation,
            probe.server_revision,
        );
    }

    overwrite_replica_synced_revision(&replicas[replica_idx], &stripped_revision)?;
    reopen_replica(config, &mut replicas[replica_idx]).await?;
    let recovered_snapshot = wait_for_cluster_convergence(
        config,
        std::slice::from_mut(&mut replicas[replica_idx]),
        &state.table,
        "generation rollover logical recovery",
        Some(state),
    )
    .await?;
    print_compact_snapshot("generation rollover recovery", &recovered_snapshot);
    state.record_note(format!(
        "{} recovered across generation {} -> {} via logical pull-updates",
        replica_name, previous_generation, next_generation
    ));

    Ok(())
}

async fn wait_for_cluster_convergence(
    config: &Config,
    replicas: &mut [LocalReplica],
    table: &str,
    label: &str,
    diagnostics: Option<&SeededScenarioState>,
) -> Result<TableSnapshot> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        let remote = fetch_remote_snapshot(config, table).await?;
        let mut mismatches = Vec::new();
        for replica in replicas.iter() {
            let conn = replica
                .db
                .connect()
                .await
                .with_context(|| format!("failed to connect {}", replica.name))?;
            let snapshot = fetch_local_snapshot(&conn, table).await?;
            if snapshot != remote {
                mismatches.push((replica.name.clone(), snapshot));
            }
        }
        if mismatches.is_empty() {
            assert_cluster_integrity(config, replicas, table, label).await?;
            assert_pull_idempotence(config, replicas, table, label, diagnostics).await?;
            for replica in replicas.iter_mut() {
                update_revision_monotonic(replica, label).await?;
            }
            println!("cluster converged for {label} after {attempts} attempts");
            return Ok(remote);
        }
        if Instant::now() >= deadline {
            let mut details = Vec::new();
            for (name, snapshot) in mismatches {
                let revision =
                    if let Some(replica) = replicas.iter().find(|replica| replica.name == name) {
                        match replica.db.stats().await {
                            Ok(stats) => {
                                if let Some(revision) = stats.revision.clone() {
                                    print_pull_updates_probe(
                                        config,
                                        &revision,
                                        &format!("{label} {}", replica.name),
                                    )
                                    .await;
                                } else {
                                    eprintln!(
                                        "pull-updates probe for {} skipped: no stored revision",
                                        replica.name
                                    );
                                }
                                format!("{:?}", stats.revision)
                            }
                            Err(err) => format!("<stats err: {err:#}>"),
                        }
                    } else {
                        "<stats unavailable>".to_string()
                    };
                details.push(format!(
                    "{name}={} revision={} diff={}",
                    summarize_snapshot(&snapshot),
                    revision,
                    summarize_snapshot_diff(&remote, &snapshot),
                ));
            }
            bail!(
                "cluster failed to converge for {label} within {:?}\nremote={}\n{}",
                CONVERGENCE_TIMEOUT,
                summarize_snapshot(&remote),
                details.join("\n"),
            );
        }
        sync_all_replicas(config, replicas, table, label).await?;
        sleep(Duration::from_millis(250)).await;
    }
}

async fn sync_all_replicas(
    config: &Config,
    replicas: &mut [LocalReplica],
    table: &str,
    label: &str,
) -> Result<()> {
    for replica in replicas.iter_mut() {
        replica
            .db
            .push()
            .await
            .with_context(|| format!("push failed for {} during {label}", replica.name))?;
        assert_single_local_integrity(
            replica,
            table,
            &format!("after {} push during {label}", replica.name),
        )
        .await?;
    }
    for replica in replicas.iter_mut() {
        let _ = pull_replica_with_legacy_retry(config, replica, label)
            .await
            .with_context(|| format!("pull failed for {} during {label}", replica.name))?;
        assert_single_local_integrity(
            replica,
            table,
            &format!("after {} pull during {label}", replica.name),
        )
        .await?;
    }
    Ok(())
}

async fn pull_replica_with_legacy_retry(
    config: &Config,
    replica: &mut LocalReplica,
    label: &str,
) -> Result<bool> {
    const MAX_ATTEMPTS: usize = 3;
    for attempt in 1..=MAX_ATTEMPTS {
        match replica.db.pull().await {
            Ok(pulled) => return Ok(pulled),
            Err(err)
                if replica.protocol == SyncProtocol::Legacy
                    && attempt < MAX_ATTEMPTS
                    && is_schema_changed_error(&err) =>
            {
                eprintln!(
                    "legacy pull hit schema-cache invalidation during {label} for {}; reopening and retrying (attempt {attempt}/{MAX_ATTEMPTS}): {err:#}",
                    replica.name
                );
                reopen_replica(config, replica).await?;
            }
            Err(err) if attempt < MAX_ATTEMPTS && is_busy_error(&err) => {
                eprintln!(
                    "pull hit transient busy during {label} for {}; reopening and retrying (attempt {attempt}/{MAX_ATTEMPTS}): {err:#}",
                    replica.name
                );
                reopen_replica(config, replica).await?;
            }
            Err(err) => return Err(err.into()),
        }
    }
    unreachable!("pull retry loop returns on success or final error")
}

fn is_schema_changed_error(err: &TursoError) -> bool {
    format!("{err:#}").contains("Database schema changed")
}

fn is_busy_error(err: &TursoError) -> bool {
    format!("{err:#}").contains("Database is busy")
}

async fn assert_cluster_integrity(
    config: &Config,
    replicas: &mut [LocalReplica],
    table: &str,
    label: &str,
) -> Result<()> {
    let remote_integrity = query_remote_sql(config, "PRAGMA integrity_check").await?;
    if remote_integrity != vec![vec![Value::Text("ok".to_string())]] {
        dump_remote_integrity_diagnostics(config, table, label, &remote_integrity).await;
        bail!("remote integrity_check failed during {label}: {remote_integrity:?}");
    }
    for replica in replicas.iter_mut() {
        let conn =
            replica.db.connect().await.with_context(|| {
                format!("failed to connect {} for integrity_check", replica.name)
            })?;
        let local_integrity = query_rows(&conn, "PRAGMA integrity_check").await?;
        if local_integrity != vec![vec![Value::Text("ok".to_string())]] {
            eprintln!(
                "local integrity_check failed for {} at {} during {label}",
                replica.name,
                replica.path.display()
            );
            dump_local_sync_debug_tables(&conn, &replica.name, table).await?;
            pause_after_integrity_failure_if_requested().await;
            bail!(
                "local integrity_check failed for {} during {label}: {local_integrity:?}",
                replica.name
            );
        }
        let mode = query_rows(&conn, "PRAGMA journal_mode").await?;
        match replica.protocol {
            SyncProtocol::Mvcc if mode != vec![vec![Value::Text("mvcc".to_string())]] => {
                bail!(
                    "replica {} left mvcc mode during {label}: {mode:?}",
                    replica.name
                );
            }
            SyncProtocol::Legacy if mode == vec![vec![Value::Text("mvcc".to_string())]] => {
                bail!(
                    "legacy replica {} entered mvcc mode during {label}: {mode:?}",
                    replica.name
                );
            }
            _ => {}
        }
        let _ = fetch_local_snapshot(&conn, table).await?;
    }
    Ok(())
}

async fn pause_after_integrity_failure_if_requested() {
    if env_flag_enabled("SYNC_EXAMPLE_PAUSE_ON_INTEGRITY_FAILURE") {
        eprintln!(
            "pausing after integrity failure; inspect the local dir before killing the process"
        );
        tokio::time::sleep(std::time::Duration::from_secs(600)).await;
    }
}

async fn dump_remote_integrity_diagnostics(
    config: &Config,
    table: &str,
    label: &str,
    remote_integrity: &[Vec<Value>],
) {
    eprintln!("remote integrity_check diagnostics for {label}: {remote_integrity:?}");
    let duplicate_roots_sql = "\
        SELECT rootpage, COUNT(*), group_concat(type || ':' || name || ':' || tbl_name, '|') \
        FROM sqlite_schema \
        WHERE rootpage > 0 \
        GROUP BY rootpage \
        HAVING COUNT(*) > 1 \
        ORDER BY rootpage";
    match query_remote_sql(config, duplicate_roots_sql).await {
        Ok(rows) => eprintln!("remote duplicate sqlite_schema rootpages: {rows:?}"),
        Err(err) => eprintln!("failed to query remote duplicate rootpages: {err:#}"),
    }

    let duplicate_root_rows_sql = "\
        SELECT rowid, type, name, tbl_name, rootpage, sql \
        FROM sqlite_schema \
        WHERE rootpage IN ( \
            SELECT rootpage \
            FROM sqlite_schema \
            WHERE rootpage > 0 \
            GROUP BY rootpage \
            HAVING COUNT(*) > 1 \
        ) \
        ORDER BY rootpage, rowid";
    match query_remote_sql(config, duplicate_root_rows_sql).await {
        Ok(rows) => eprintln!("remote duplicate-root sqlite_schema rows: {rows:?}"),
        Err(err) => eprintln!("failed to query remote duplicate-root rows: {err:#}"),
    }

    match query_remote_sql(config, &debug_schema_sql(table)).await {
        Ok(rows) => eprintln!("remote current test table schema rows: {rows:?}"),
        Err(err) => eprintln!("failed to query remote current test table schema rows: {err:#}"),
    }

    match query_remote_sql(
        config,
        &format!("SELECT COUNT(*) FROM {}", sql_ident(table)),
    )
    .await
    {
        Ok(rows) => eprintln!("remote current test table row count: {rows:?}"),
        Err(err) => eprintln!("failed to query remote current test table row count: {err:#}"),
    }

    match query_remote_sql(config, &fetch_indexes_sql(table)).await {
        Ok(rows) => {
            for row in rows {
                let Some(Value::Text(index_name)) = row.first() else {
                    eprintln!("remote index count skipped for unexpected index row: {row:?}");
                    continue;
                };
                let sql = format!(
                    "SELECT COUNT(*) FROM {} INDEXED BY {}",
                    sql_ident(table),
                    sql_ident(index_name)
                );
                match query_remote_sql(config, &sql).await {
                    Ok(count) => {
                        eprintln!("remote index entry count for {index_name}: {count:?}")
                    }
                    Err(err) => {
                        eprintln!("failed to query remote index count for {index_name}: {err:#}")
                    }
                }
            }
        }
        Err(err) => {
            eprintln!("failed to query remote indexes for cardinality diagnostics: {err:#}")
        }
    }
}

async fn assert_pull_idempotence(
    config: &Config,
    replicas: &mut [LocalReplica],
    table: &str,
    label: &str,
    diagnostics: Option<&SeededScenarioState>,
) -> Result<()> {
    for idx in 0..replicas.len() {
        let before_conn = replicas[idx].db.connect().await.with_context(|| {
            format!(
                "failed to connect {} before idempotence pull",
                replicas[idx].name
            )
        })?;
        let before = fetch_local_snapshot(&before_conn, table).await?;
        drop(before_conn);
        let pulled = pull_replica_with_legacy_retry(config, &mut replicas[idx], label)
            .await
            .with_context(|| format!("idempotence pull failed for {}", replicas[idx].name))?;
        let after_conn = replicas[idx].db.connect().await.with_context(|| {
            format!(
                "failed to connect {} after idempotence pull",
                replicas[idx].name
            )
        })?;
        let after = fetch_local_snapshot(&after_conn, table).await?;
        drop(after_conn);
        if before != after {
            print_idempotence_diagnostics(
                config,
                replicas,
                idx,
                table,
                label,
                pulled,
                &before,
                &after,
                diagnostics,
            )
            .await;
            bail!(
                "idempotence violated during {label}: extra pull changed snapshot for {} (pulled={pulled})",
                replicas[idx].name
            );
        }
    }
    Ok(())
}

async fn update_revision_monotonic(replica: &mut LocalReplica, label: &str) -> Result<()> {
    let stats = replica
        .db
        .stats()
        .await
        .with_context(|| format!("failed to read stats for {}", replica.name))?;
    let Some(revision) = stats.revision.as_deref() else {
        return Ok(());
    };
    if replica.protocol == SyncProtocol::Mvcc {
        let parsed: MvccRevision = serde_json::from_str(revision)
            .with_context(|| format!("invalid revision for {}", replica.name))?;
        if let Some(previous) = replica.last_revision.as_deref() {
            let previous: MvccRevision = serde_json::from_str(previous)
                .with_context(|| format!("invalid previous revision for {}", replica.name))?;
            if mvcc_revision_is_older_than(parsed, previous) {
                bail!(
                    "revision moved backwards for {} during {label}: {:?} -> {:?}",
                    replica.name,
                    previous,
                    parsed
                );
            }
        }
    }
    replica.last_revision = Some(revision.to_string());
    Ok(())
}

fn mvcc_revision_is_older_than(current: MvccRevision, previous: MvccRevision) -> bool {
    if current.generation > previous.generation {
        return true;
    }
    if current.generation < previous.generation {
        return false;
    }
    current.log_offset < previous.log_offset
}

async fn print_idempotence_diagnostics(
    config: &Config,
    replicas: &mut [LocalReplica],
    failing_idx: usize,
    table: &str,
    label: &str,
    pulled: bool,
    before: &TableSnapshot,
    after: &TableSnapshot,
    diagnostics: Option<&SeededScenarioState>,
) {
    eprintln!("==== MVCC sync idempotence diagnostics ====");
    eprintln!("label: {label}");
    eprintln!("table: {table}");
    eprintln!(
        "failing replica: {} pulled={pulled}",
        replicas[failing_idx].name
    );
    if let Some(diagnostics) = diagnostics {
        eprintln!(
            "seeded state: seed={} available_columns={:?} indexed_columns={:?}",
            diagnostics.seed, diagnostics.available_columns, diagnostics.indexed_columns
        );
        eprintln!(
            "seeded live ids: remote={:?} locals={:?}",
            diagnostics.remote_live_ids, diagnostics.local_live_ids
        );
        if !diagnostics.recent_events.is_empty() {
            eprintln!("recent seeded events:");
            for event in &diagnostics.recent_events {
                eprintln!("  {event}");
            }
        }
    }

    eprintln!("before snapshot summary: {}", summarize_snapshot(before));
    print_snapshot_stderr("before snapshot", before);
    eprintln!("after snapshot summary: {}", summarize_snapshot(after));
    print_snapshot_stderr("after snapshot", after);

    match fetch_remote_snapshot(config, table).await {
        Ok(remote) => {
            eprintln!("remote snapshot summary: {}", summarize_snapshot(&remote));
            print_snapshot_stderr("remote snapshot", &remote);
        }
        Err(err) => eprintln!("failed to fetch remote snapshot during diagnostics: {err:#}"),
    }

    match query_remote_sql(config, &debug_schema_sql(table)).await {
        Ok(rows) => eprintln!("remote sqlite_schema rows: {rows:?}"),
        Err(err) => eprintln!("failed to fetch remote sqlite_schema rows: {err:#}"),
    }

    for idx in 0..replicas.len() {
        if let Err(err) =
            print_replica_idempotence_summary(&mut replicas[idx], table, idx == failing_idx).await
        {
            eprintln!(
                "failed to gather diagnostics for {}: {err:#}",
                replicas[idx].name
            );
        }
    }

    eprintln!("==== end MVCC sync idempotence diagnostics ====");
}

async fn print_replica_idempotence_summary(
    replica: &mut LocalReplica,
    table: &str,
    verbose: bool,
) -> Result<()> {
    let stats = replica
        .db
        .stats()
        .await
        .with_context(|| format!("failed to read stats for {}", replica.name))?;
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {} for diagnostics", replica.name))?;
    let snapshot = fetch_local_snapshot(&conn, table).await?;
    eprintln!(
        "replica {}: revision={:?} wal={} sent={} recv={} snapshot={}",
        replica.name,
        stats.revision,
        stats.main_wal_size,
        stats.network_sent_bytes,
        stats.network_received_bytes,
        summarize_snapshot(&snapshot),
    );
    if verbose {
        print_snapshot_stderr(&format!("{} current snapshot", replica.name), &snapshot);
        let schema_rows = query_rows(&conn, &debug_schema_sql(table)).await?;
        eprintln!("{} sqlite_schema rows: {schema_rows:?}", replica.name);
        dump_local_sync_debug_tables(&conn, &replica.name, table).await?;
    }
    Ok(())
}

async fn print_bootstrap_replica_state(replica: &LocalReplica, table: &str) -> Result<()> {
    let stats = replica
        .db
        .stats()
        .await
        .with_context(|| format!("failed to read stats for {}", replica.name))?;
    let metadata_path = replica_metadata_path(replica);
    let metadata = fs::read_to_string(&metadata_path)
        .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    let conn = replica.db.connect().await.with_context(|| {
        format!(
            "failed to connect {} for bootstrap diagnostics",
            replica.name
        )
    })?;
    let table_exists = local_table_exists(&conn, table).await?;
    let row_count = if table_exists {
        fetch_single_int_local(&conn, &format!("SELECT COUNT(*) FROM {table}"))
            .await?
            .unwrap_or(0)
    } else {
        0
    };
    println!(
        "bootstrap replica state: name={} revision={:?} table_exists={} row_count={} metadata={}",
        replica.name,
        stats.revision,
        table_exists,
        row_count,
        metadata.trim(),
    );
    Ok(())
}

async fn dump_local_sync_debug_tables(
    conn: &Connection,
    replica_name: &str,
    table: &str,
) -> Result<()> {
    match query_rows(conn, &debug_schema_sql(table)).await {
        Ok(rows) => eprintln!("{replica_name} local current test table schema rows: {rows:?}"),
        Err(err) => eprintln!(
            "failed to query {replica_name} local current test table schema rows: {err:#}"
        ),
    }

    match query_rows(conn, &format!("SELECT COUNT(*) FROM {}", sql_ident(table))).await {
        Ok(rows) => eprintln!("{replica_name} local current test table row count: {rows:?}"),
        Err(err) => {
            eprintln!("failed to query {replica_name} local current test table row count: {err:#}")
        }
    }

    match query_rows(conn, &fetch_indexes_sql(table)).await {
        Ok(rows) => {
            for row in rows {
                let Some(Value::Text(index_name)) = row.first() else {
                    eprintln!(
                        "{replica_name} local index count skipped for unexpected index row: {row:?}"
                    );
                    continue;
                };
                let sql = format!(
                    "SELECT COUNT(*) FROM {} INDEXED BY {}",
                    sql_ident(table),
                    sql_ident(index_name)
                );
                match query_rows(conn, &sql).await {
                    Ok(count) => eprintln!(
                        "{replica_name} local index entry count for {index_name}: {count:?}"
                    ),
                    Err(err) => eprintln!(
                        "failed to query {replica_name} local index count for {index_name}: {err:#}"
                    ),
                }
            }
        }
        Err(err) => {
            eprintln!("failed to query {replica_name} local indexes for diagnostics: {err:#}")
        }
    }

    if local_table_exists(conn, "turso_sync_last_change_id").await? {
        let rows = query_rows(
            conn,
            "SELECT * FROM turso_sync_last_change_id ORDER BY 1, 2, 3",
        )
        .await?;
        eprintln!("{replica_name} turso_sync_last_change_id rows: {rows:?}");
    } else {
        eprintln!("{replica_name} turso_sync_last_change_id rows: <missing>");
    }

    if local_table_exists(conn, "turso_cdc").await? {
        let recent = query_rows(
            conn,
            &format!(
                "SELECT change_id, change_txn_id, change_type, table_name, id \
                 FROM turso_cdc WHERE table_name = {} ORDER BY change_id DESC LIMIT 12",
                sql_text(table)
            ),
        )
        .await?;
        eprintln!("{replica_name} turso_cdc recent rows for {table}: {recent:?}");
    } else {
        eprintln!("{replica_name} turso_cdc rows: <missing>");
    }

    Ok(())
}

async fn local_execute_sql(replica: &LocalReplica, sql: &str) -> Result<()> {
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {}", replica.name))?;
    conn.execute(sql, ())
        .await
        .with_context(|| format!("local SQL failed on {}: {sql}", replica.name))?;
    Ok(())
}

async fn assert_local_cdc_advanced(
    replica: &LocalReplica,
    state: &mut SeededScenarioState,
    replica_idx: usize,
    label: &str,
) -> Result<()> {
    let current = match local_pushable_cdc_max_change_id(replica)
        .await
        .with_context(|| format!("failed to read local pushable CDC high-water for {label}"))?
    {
        Some(current) => current,
        None => {
            let diagnostics = local_cdc_debug_summary(replica, &state.table)
                .await
                .unwrap_or_else(|err| format!("<failed to gather diagnostics: {err:#}>"));
            bail!(
                "local CDC did not capture any pushable rows for {label}: diagnostics={diagnostics}"
            );
        }
    };
    if let Some(previous) = state.local_cdc_high_water[replica_idx] {
        if current <= previous {
            let sync_floor = local_sync_change_id_for_replica(replica).await?;
            if sync_floor.is_some_and(|floor| current > floor) {
                state.local_cdc_high_water[replica_idx] = Some(current);
                return Ok(());
            }
            let diagnostics = local_cdc_debug_summary(replica, &state.table)
                .await
                .unwrap_or_else(|err| format!("<failed to gather diagnostics: {err:#}>"));
            bail!(
                "local pushable CDC high-water did not advance for {label}: previous={previous} current={current} sync_floor={sync_floor:?} diagnostics={diagnostics}"
            );
        }
    }
    state.local_cdc_high_water[replica_idx] = Some(current);
    Ok(())
}

async fn assert_seeded_push_cdc_high_water(
    config: &Config,
    replica: &LocalReplica,
    _state: &SeededScenarioState,
    _replica_idx: usize,
    label: &str,
) -> Result<()> {
    // Pull/reopen paths can replace the local CDC stream with a new epoch whose
    // change ids are lower than the previous local-write high-water.
    let Some(expected) = local_pushable_cdc_max_change_id(replica)
        .await
        .with_context(|| format!("failed to read local pushable CDC high-water for {label}"))?
    else {
        return Ok(());
    };
    let client_id = replica_client_unique_id(replica)?;
    let pushed = remote_sync_change_id_for_client(config, &client_id)
        .await
        .with_context(|| {
            format!("failed to read remote push high-water for {label}: client_id={client_id}")
        })?;
    let Some(pushed) = pushed else {
        let rows = remote_sync_last_change_id_preview(config).await;
        bail!(
            "push high-water is missing for {label}: client_id={client_id} expected_at_least={expected} remote_rows={rows}"
        );
    };
    if pushed < expected {
        let rows = remote_sync_last_change_id_preview(config).await;
        bail!(
            "push high-water did not catch up to local pushable CDC for {label}: client_id={client_id} pushed={pushed} expected_at_least={expected} remote_rows={rows}"
        );
    }
    Ok(())
}

async fn assert_remote_row_payload(
    config: &Config,
    table: &str,
    id: i64,
    owner: &str,
    payload: &str,
    rev: i64,
    label: &str,
) -> Result<()> {
    let rows = query_remote_sql(
        config,
        &format!(
            "SELECT owner, payload, rev FROM {} WHERE id = {id}",
            sql_ident(table)
        ),
    )
    .await
    .with_context(|| format!("failed to read remote row for {label}: table={table} id={id}"))?;
    let expected = vec![vec![
        Value::Text(owner.to_string()),
        Value::Text(payload.to_string()),
        Value::Integer(rev),
    ]];
    if rows != expected {
        bail!(
            "remote row mismatch after {label}: table={table} id={id} expected={expected:?} actual={rows:?}"
        );
    }
    Ok(())
}

async fn assert_remote_row_absent(
    config: &Config,
    table: &str,
    id: i64,
    label: &str,
) -> Result<()> {
    let rows = query_remote_sql(
        config,
        &format!("SELECT COUNT(*) FROM {} WHERE id = {id}", sql_ident(table)),
    )
    .await
    .with_context(|| format!("failed to count remote row for {label}: table={table} id={id}"))?;
    if rows != vec![vec![Value::Integer(0)]] {
        bail!("remote row still exists after {label}: table={table} id={id} rows={rows:?}");
    }
    Ok(())
}

async fn assert_remote_push_probe_table(config: &Config, table: &str) -> Result<()> {
    let rows = query_remote_sql(
        config,
        &format!("SELECT payload FROM {} WHERE id = 1", sql_ident(table)),
    )
    .await
    .with_context(|| format!("failed to read remote push probe table {table}"))?;
    if rows != vec![vec![Value::Text("targeted-ddl-push".to_string())]] {
        bail!("remote push probe table row mismatch for {table}: {rows:?}");
    }
    let index_name = format!("{table} payload idx");
    let index_sql = fetch_single_text_remote(
        config,
        &format!(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = {}",
            sql_text(&index_name)
        ),
    )
    .await?;
    if index_sql.is_none() {
        bail!("remote push probe index missing: table={table} index={index_name}");
    }
    Ok(())
}

async fn local_pushable_cdc_max_change_id(replica: &LocalReplica) -> Result<Option<i64>> {
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {}", replica.name))?;
    if !local_table_exists(&conn, "turso_cdc").await? {
        return Ok(None);
    }
    fetch_single_int_local(
        &conn,
        "SELECT MAX(change_id) FROM turso_cdc \
         WHERE change_type != 2 AND table_name != 'turso_sync_last_change_id'",
    )
    .await
}

async fn local_sync_change_id_for_replica(replica: &LocalReplica) -> Result<Option<i64>> {
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {}", replica.name))?;
    if !local_table_exists(&conn, "turso_sync_last_change_id").await? {
        return Ok(None);
    }
    let client_id = replica_client_unique_id(replica)?;
    fetch_single_int_local(
        &conn,
        &format!(
            "SELECT change_id FROM turso_sync_last_change_id WHERE client_id = {}",
            sql_text(&client_id)
        ),
    )
    .await
}

async fn local_replayable_cdc_count(replica: &LocalReplica) -> Result<i64> {
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {}", replica.name))?;
    if !local_table_exists(&conn, "turso_cdc").await? {
        return Ok(0);
    }
    let floor = local_sync_change_id_for_replica(replica)
        .await?
        .unwrap_or(-1);
    Ok(fetch_single_int_local(
        &conn,
        &format!(
            "SELECT COUNT(*) FROM turso_cdc \
             WHERE change_id > {floor} \
             AND change_type != 2 \
             AND table_name != 'turso_sync_last_change_id'"
        ),
    )
    .await?
    .unwrap_or(0))
}

async fn local_cdc_debug_summary(replica: &LocalReplica, table: &str) -> Result<String> {
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {}", replica.name))?;
    let table_rows = if local_table_exists(&conn, table).await? {
        fetch_single_int_local(&conn, &format!("SELECT COUNT(*) FROM {}", sql_ident(table))).await?
    } else {
        None
    };
    let cdc_rows = if local_table_exists(&conn, "turso_cdc").await? {
        query_rows(
            &conn,
            &format!(
                "SELECT change_id, change_type, table_name, id FROM turso_cdc \
                 WHERE table_name = {} OR table_name IS NULL \
                 ORDER BY change_id DESC LIMIT 8",
                sql_text(table)
            ),
        )
        .await?
    } else {
        Vec::new()
    };
    Ok(format!(
        "replica={} table_rows={table_rows:?} recent_cdc={cdc_rows:?}",
        replica.name
    ))
}

async fn reopen_replica(config: &Config, replica: &mut LocalReplica) -> Result<()> {
    let protocol = replica.protocol;
    let reopened = reopen_replica_handle(config, replica, protocol).await?;
    let last_revision = replica.last_revision.clone();
    *replica = reopened;
    replica.last_revision = last_revision;
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect reopened {}", replica.name))?;
    ensure_protocol_mode(&conn, protocol).await?;
    Ok(())
}

async fn reopen_replica_handle(
    config: &Config,
    replica: &LocalReplica,
    protocol: SyncProtocol,
) -> Result<LocalReplica> {
    const MAX_ATTEMPTS: usize = 5;
    for attempt in 1..=MAX_ATTEMPTS {
        match build_local_replica(
            replica.name.clone(),
            replica.path.clone(),
            config,
            false,
            protocol,
        )
        .await
        {
            Ok(reopened) => return Ok(reopened),
            Err(err)
                if protocol == SyncProtocol::Legacy
                    && attempt < MAX_ATTEMPTS
                    && format!("{err:#}")
                        .contains("CDC pragma execution kept seeing schema updates") =>
            {
                eprintln!(
                    "legacy reopen hit CDC schema-update race for {}; retrying (attempt {attempt}/{MAX_ATTEMPTS}): {err:#}",
                    replica.name
                );
                sleep(Duration::from_millis(100)).await;
            }
            Err(err) => {
                return Err(err).with_context(|| format!("failed to reopen {}", replica.name));
            }
        }
    }
    unreachable!("reopen retry loop returns on success or final error")
}

async fn current_replica_revision(replica: &LocalReplica) -> Result<String> {
    let stats = replica
        .db
        .stats()
        .await
        .with_context(|| format!("failed to read stats for {}", replica.name))?;
    stats
        .revision
        .with_context(|| format!("replica {} has no synced revision yet", replica.name))
}

fn strip_wal_fragment_from_revision(revision: &str) -> Result<String> {
    let mut value: JsonValue =
        serde_json::from_str(revision).context("failed to parse MVCC revision JSON")?;
    let object = value
        .as_object_mut()
        .context("MVCC revision must be a JSON object")?;
    object.remove("wal_fragment_no");
    serde_json::to_string(&value).context("failed to serialize MVCC revision JSON")
}

fn replica_metadata_path(replica: &LocalReplica) -> PathBuf {
    let mut path = OsString::from(replica.path.as_os_str());
    path.push("-info");
    PathBuf::from(path)
}

fn replica_replace_base_marker_path(replica: &LocalReplica) -> PathBuf {
    let mut path = OsString::from(replica.path.as_os_str());
    path.push("-replace-base-apply");
    PathBuf::from(path)
}

fn replica_client_unique_id(replica: &LocalReplica) -> Result<String> {
    let metadata_path = replica_metadata_path(replica);
    let raw = fs::read(&metadata_path)
        .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    let metadata: JsonValue = serde_json::from_slice(&raw)
        .with_context(|| format!("invalid JSON in {}", metadata_path.display()))?;
    metadata
        .get("client_unique_id")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .with_context(|| {
            format!(
                "missing client_unique_id in replica metadata {}",
                metadata_path.display()
            )
        })
}

fn overwrite_replica_synced_revision(replica: &LocalReplica, revision: &str) -> Result<()> {
    let metadata_path = replica_metadata_path(replica);
    let raw = fs::read(&metadata_path)
        .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    let mut metadata: JsonValue = serde_json::from_slice(&raw)
        .with_context(|| format!("invalid JSON in {}", metadata_path.display()))?;
    let synced_revision = metadata
        .get_mut("synced_revision")
        .context("metadata is missing synced_revision")?;
    *synced_revision = json!({
        "type": "v1",
        "revision": revision,
    });
    let rewritten = serde_json::to_vec_pretty(&metadata)
        .with_context(|| format!("failed to encode {}", metadata_path.display()))?;
    fs::write(&metadata_path, rewritten)
        .with_context(|| format!("failed to write {}", metadata_path.display()))?;
    Ok(())
}

fn build_insert_sql(table: &str, id: i64, owner: &str, payload: &str, columns: &[&str]) -> String {
    let mut names = vec![
        "id".to_string(),
        "owner".to_string(),
        "payload".to_string(),
        "rev".to_string(),
    ];
    let mut values = vec![
        id.to_string(),
        sql_text(owner),
        sql_text(payload),
        "1".to_string(),
    ];
    for column in columns {
        names.push((*column).to_string());
        values.push(extra_value_sql(column, owner, id));
    }
    format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        names.join(", "),
        values.join(", ")
    )
}

fn build_update_sql(table: &str, id: i64, owner: &str, step: usize, columns: &[&str]) -> String {
    let mut assignments = vec![
        format!("payload = {}", sql_text(&format!("{owner}-updated-{step}"))),
        "rev = rev + 1".to_string(),
    ];
    for column in columns {
        assignments.push(format!(
            "{column} = {}",
            extra_value_sql(column, owner, id + step as i64)
        ));
    }
    format!(
        "UPDATE {table} SET {} WHERE id = {id}",
        assignments.join(", ")
    )
}

fn extra_value_sql(column: &str, owner: &str, n: i64) -> String {
    match column {
        "note" => sql_text(&format!("{owner}-note-{n}")),
        "bucket" => ((n % 17) + 1).to_string(),
        "tag" => sql_text(&format!("tag-{}", n % 7)),
        "status" => ((n % 9) + 1).to_string(),
        "rb_local" => sql_text(&format!("{owner}-rb-{n}")),
        other => panic!("unexpected extra column {other}"),
    }
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn pick_live_id(ids: &[i64], rng: &mut ScenarioRng) -> Option<i64> {
    if ids.is_empty() {
        None
    } else {
        Some(ids[rng.index(ids.len())])
    }
}

fn remove_live_id(ids: &mut Vec<i64>, id: i64) {
    if let Some(idx) = ids.iter().position(|candidate| *candidate == id) {
        ids.swap_remove(idx);
    }
}

fn load_config() -> Result<Config> {
    let remote_url = env::var("TURSO_REMOTE_URL")
        .or_else(|_| env::var("TURSO_LIVE_SYNC_REMOTE_URL"))
        .context("set TURSO_REMOTE_URL to the libsql/http remote database URL")?;
    let remote_encryption_key = env::var("TURSO_REMOTE_ENCRYPTION_KEY")
        .ok()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty());
    let remote_encryption_cipher = if remote_encryption_key.is_none() {
        None
    } else if let Some(cipher) = env::var("TURSO_REMOTE_ENCRYPTION_CIPHER").ok() {
        Some(
            cipher
                .parse()
                .map_err(|err: String| anyhow!("invalid TURSO_REMOTE_ENCRYPTION_CIPHER: {err}"))?,
        )
    } else {
        Some(RemoteEncryptionCipher::Aes256Gcm)
    };
    Ok(Config {
        remote_url,
        auth_token: load_auth_token(),
        remote_encryption_key,
        remote_encryption_cipher,
    })
}

fn load_auth_token() -> Option<String> {
    if let Ok(token) = env::var("TURSO_AUTH_TOKEN") {
        let trimmed = token.trim().to_string();
        if trimmed.is_empty() {
            eprintln!("TURSO_AUTH_TOKEN is empty");
        }
        return Some(trimmed);
    }
    eprintln!("TURSO_AUTH_TOKEN is empty");
    None
}

async fn build_local_replica(
    name: impl Into<String>,
    path: PathBuf,
    config: &Config,
    bootstrap_if_empty: bool,
    protocol: SyncProtocol,
) -> Result<LocalReplica> {
    let name = name.into();
    let db = build_sync_db(path.clone(), config, bootstrap_if_empty, protocol).await?;
    Ok(LocalReplica {
        name,
        path,
        db,
        protocol,
        last_revision: None,
    })
}

async fn build_sync_db(
    path: PathBuf,
    config: &Config,
    bootstrap_if_empty: bool,
    _protocol: SyncProtocol,
) -> Result<Database> {
    let path_str = path
        .to_str()
        .context("local database path contains invalid UTF-8")?;
    let builder = Builder::new_remote(path_str)
        .with_remote_url(&config.remote_url)
        .bootstrap_if_empty(bootstrap_if_empty);
    let builder = if let Some(token) = &config.auth_token {
        builder.with_auth_token(token)
    } else {
        builder
    };
    let builder = if let (Some(key), Some(cipher)) = (
        config.remote_encryption_key.as_ref(),
        config.remote_encryption_cipher,
    ) {
        builder.with_remote_encryption(key, cipher)
    } else {
        builder
    };
    let db = builder
        .build()
        .await
        .with_context(|| format!("failed to build sync database at {}", path.display()))?;
    Ok(db)
}

async fn ensure_mvcc_mode(conn: &Connection) -> Result<()> {
    let rows = query_rows(conn, "PRAGMA journal_mode = 'mvcc'").await?;
    if rows != vec![vec![Value::Text("mvcc".to_string())]] {
        bail!("failed to switch local replica into mvcc mode: {rows:?}");
    }
    Ok(())
}

async fn ensure_protocol_mode(conn: &Connection, protocol: SyncProtocol) -> Result<()> {
    match protocol {
        SyncProtocol::Mvcc => ensure_mvcc_mode(conn).await,
        SyncProtocol::Legacy => {
            let rows = query_rows(conn, "PRAGMA journal_mode").await?;
            if rows == vec![vec![Value::Text("mvcc".to_string())]] {
                bail!("legacy sync replica unexpectedly opened in mvcc mode");
            }
            Ok(())
        }
    }
}

async fn initialize_table(conn: &Connection, table: &str) -> Result<()> {
    conn.execute(
        &format!(
            "CREATE TABLE {table} (\
                id INTEGER PRIMARY KEY, \
                owner TEXT NOT NULL, \
                payload TEXT NOT NULL, \
                rev INTEGER NOT NULL DEFAULT 0\
            )"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to create {table}"))?;
    conn.execute(
        &format!("CREATE INDEX {table}_owner_rev_idx ON {table}(owner, rev)"),
        (),
    )
    .await
    .with_context(|| format!("failed to create index for {table}"))?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev) VALUES \
                (1, 'seed-a', 'alpha', 1), \
                (2, 'seed-a', 'beta', 1)"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to seed {table}"))?;
    Ok(())
}

async fn phase_one_local(db: &Database, table: &str) -> Result<()> {
    let conn = db
        .connect()
        .await
        .context("phase 1 failed to connect locally")?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev) VALUES \
                (3, 'local-a', 'gamma', 1), \
                (4, 'local-a', 'delta', 1), \
                (5, 'local-a', 'epsilon', 1), \
                (6, 'local-a', 'zeta', 1)"
        ),
        (),
    )
    .await
    .context("phase 1 insert burst failed")?;
    conn.execute(
        &format!("UPDATE {table} SET payload = 'beta-from-local', rev = rev + 1 WHERE id = 2"),
        (),
    )
    .await
    .context("phase 1 update failed")?;
    db.push().await.context("phase 1 push failed")
}

async fn phase_two_remote(config: &Config, table: &str) -> Result<()> {
    execute_remote_sql(config, &format!("ALTER TABLE {table} ADD COLUMN note TEXT")).await?;
    execute_remote_sql(
        config,
        &format!("CREATE INDEX {table}_note_idx ON {table}(note)"),
    )
    .await?;
    execute_remote_sql(
        config,
        &format!(
            "UPDATE {table} SET note = CASE id \
                WHEN 1 THEN 'remote-backfill-1' \
                WHEN 2 THEN 'remote-backfill-2' \
                ELSE note END \
             WHERE id IN (1, 2)"
        ),
    )
    .await?;
    execute_remote_sql(
        config,
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev, note) VALUES \
                (7, 'remote-b', 'eta', 1, 'from-remote'), \
                (8, 'remote-b', 'theta', 1, 'from-remote')"
        ),
    )
    .await?;
    Ok(())
}

async fn phase_three_local(db: &Database, table: &str) -> Result<()> {
    let conn = db
        .connect()
        .await
        .context("phase 3 failed to connect locally")?;
    conn.execute(
        &format!("ALTER TABLE {table} ADD COLUMN bucket INTEGER NOT NULL DEFAULT 0"),
        (),
    )
    .await
    .context("phase 3 failed to add bucket column")?;
    conn.execute(
        &format!("CREATE INDEX {table}_bucket_idx ON {table}(bucket)"),
        (),
    )
    .await
    .context("phase 3 failed to create bucket index")?;
    conn.execute(
        &format!(
            "UPDATE {table} SET \
                bucket = CASE WHEN id % 2 = 0 THEN 2 ELSE 1 END, \
                note = COALESCE(note, 'local-backfill'), \
                rev = rev + 1"
        ),
        (),
    )
    .await
    .context("phase 3 update failed")?;
    conn.execute(&format!("DELETE FROM {table} WHERE id = 1"), ())
        .await
        .context("phase 3 delete failed")?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES \
                (9, 'local-c', 'iota', 1, 'from-local', 9), \
                (10, 'local-c', 'kappa', 1, 'from-local', 10)"
        ),
        (),
    )
    .await
    .context("phase 3 insert failed")?;
    db.push().await.context("phase 3 push failed")
}

async fn phase_four_remote(config: &Config, table: &str) -> Result<()> {
    execute_remote_sql(
        config,
        &format!(
            "UPDATE {table} SET \
                bucket = CASE WHEN bucket = 0 THEN 7 ELSE bucket + 10 END, \
                note = COALESCE(note, 'remote-note'), \
                rev = rev + 1 \
             WHERE id IN (2, 3, 4, 7, 8)"
        ),
    )
    .await?;
    execute_remote_sql(
        config,
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES \
                (11, 'remote-d', 'lambda', 1, 'remote-final', 11), \
                (12, 'remote-d', 'mu', 1, 'remote-final', 12)"
        ),
    )
    .await?;
    Ok(())
}

async fn checkpoint_drill(local: &LocalReplica, table: &str) -> Result<()> {
    let conn = local
        .db
        .connect()
        .await
        .context("checkpoint drill failed to connect locally")?;
    for id in 100..140 {
        conn.execute(
            &format!(
                "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES (?, ?, ?, ?, ?, ?)"
            ),
            (
                id,
                "checkpoint-local",
                format!("payload-{id}"),
                1_i64,
                format!("checkpoint-burst-{id}"),
                (id % 5) as i64,
            ),
        )
        .await
        .with_context(|| format!("checkpoint drill failed to insert item {id}"))?;
    }

    let stats = local
        .db
        .stats()
        .await
        .context("failed to read pre-checkpoint stats")?;
    println!("checkpoint: before wal={}", stats.main_wal_size);

    if stats.main_wal_size != 0 {
        bail!("checkpoint drill expected empty WAL before mvcc checkpoint");
    }

    local
        .db
        .checkpoint()
        .await
        .context("checkpoint API failed")?;
    let verify_conn = local
        .db
        .connect()
        .await
        .context("failed to reconnect locally after checkpoint")?;
    let burst_rows = query_rows(
        &verify_conn,
        &format!("SELECT COUNT(*) FROM {table} WHERE id BETWEEN 100 AND 139"),
    )
    .await?;
    if burst_rows != vec![vec![Value::Integer(40)]] {
        bail!("checkpoint drill lost local rows after checkpoint: {burst_rows:?}");
    }
    Ok(())
}

async fn print_local_sync_diagnostics(local: &LocalReplica, label: &str) {
    match local.db.stats().await {
        Ok(stats) => eprintln!(
            "local sync diagnostics for {label}: revision={:?} wal={} sent={} recv={} last_pull={:?} last_push={:?}",
            stats.revision,
            stats.main_wal_size,
            stats.network_sent_bytes,
            stats.network_received_bytes,
            stats.last_pull_unix_time,
            stats.last_push_unix_time
        ),
        Err(err) => eprintln!("local sync diagnostics failed for {label}: {err:#}"),
    }
}

fn spawn_sync_worker(
    local: LocalReplica,
    stop: watch::Receiver<bool>,
    table: String,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut stop = stop;
        let mut tick = 0usize;
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let push_ok = match local.db.push().await {
                Ok(()) => true,
                Err(err) => {
                    eprintln!("sync worker push failed: {err:#}");
                    false
                }
            };
            let pulled = match local.db.pull().await {
                Ok(pulled) => Some(pulled),
                Err(err) => {
                    eprintln!("sync worker pull failed: {err:#}");
                    print_local_sync_diagnostics(&local, "sync worker pull failure").await;
                    None
                }
            };
            if tick % 3 == 0 {
                if let Err(err) = local.db.checkpoint().await {
                    eprintln!("sync worker checkpoint failed: {err:#}");
                }
            }
            if tick % 8 == 0 {
                let stats = local.db.stats().await.context("sync worker stats failed")?;
                println!(
                    "sync: push_ok={} pulled={} wal={} sent={} recv={}",
                    push_ok,
                    pulled.unwrap_or(false),
                    stats.main_wal_size,
                    stats.network_sent_bytes,
                    stats.network_received_bytes
                );
            }
            tick += 1;
            tokio::select! {
                _ = sleep(SYNC_INTERVAL) => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(());
                    }
                }
            }
            let _ = &table;
        }
    })
}

fn spawn_reader_worker(
    local: LocalReplica,
    stop: watch::Receiver<bool>,
    table: String,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut stop = stop;
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            match read_local_snapshot_once(&local, &table).await {
                Ok(_) => {}
                Err(err) if is_transient_local_contention_error(&err) => {
                    eprintln!("reader worker transient read contention: {err:#}");
                }
                Err(err) => return Err(err),
            }
            tokio::select! {
                _ = sleep(READER_INTERVAL) => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    })
}

async fn read_local_snapshot_once(local: &LocalReplica, table: &str) -> Result<TableSnapshot> {
    let conn = local
        .db
        .connect()
        .await
        .context("reader worker connect failed")?;
    fetch_local_snapshot(&conn, table)
        .await
        .context("reader worker query failed")
}

fn is_transient_local_contention_error(err: &anyhow::Error) -> bool {
    if err.chain().any(|cause| {
        cause.downcast_ref::<TursoError>().is_some_and(|cause| {
            matches!(
                cause,
                TursoError::Busy(_) | TursoError::BusySnapshot(_) | TursoError::Interrupt(_)
            )
        })
    }) {
        return true;
    }

    err.chain().any(|cause| {
        let message = cause.to_string().to_ascii_lowercase();
        message.contains("busy")
            || message.contains("database is locked")
            || message.contains("database table is locked")
    })
}

async fn local_writer_execute_with_retry(
    conn: &Connection,
    sql: &str,
    label: impl AsRef<str>,
) -> Result<()> {
    let label = label.as_ref();
    let deadline = Instant::now() + LOCAL_WRITER_RETRY_TIMEOUT;
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        match conn.execute(sql, ()).await {
            Ok(_) => {
                if attempts > 1 {
                    eprintln!(
                        "local writer {label} succeeded after {attempts} attempts due to transient contention"
                    );
                }
                return Ok(());
            }
            Err(err) => {
                let err = anyhow!(err).context(format!("local writer {label} failed"));
                if !is_transient_local_contention_error(&err) || Instant::now() >= deadline {
                    return Err(err);
                }
                tokio::select! {
                    _ = sleep(LOCAL_WRITER_RETRY_INTERVAL) => {}
                }
            }
        }
    }
}

fn spawn_local_writer_worker(
    local: LocalReplica,
    stop: watch::Receiver<bool>,
    table: String,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut stop = stop;
        let mut tick = 0_i64;
        loop {
            if *stop.borrow() {
                return Ok(());
            }

            let conn = local
                .db
                .connect()
                .await
                .context("local writer connect failed")?;
            let id = 10_000 + tick;
            local_writer_execute_with_retry(
                &conn,
                &format!(
                    "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES ({id}, 'bg-local', {}, 1, {}, {})",
                    sql_text(&format!("local-payload-{tick}")),
                    sql_text(&format!("local-note-{tick}")),
                    (tick % 7) + 1,
                ),
                format!("insert id={id}"),
            )
            .await
            .with_context(|| format!("local writer insert failed for id={id}"))?;

            if tick > 0 && tick % 2 == 0 {
                let update_id = 10_000 + (tick - 1);
                local_writer_execute_with_retry(
                    &conn,
                    &format!(
                        "UPDATE {table} SET payload = {}, note = {}, rev = rev + 1, bucket = bucket + 100 WHERE id = {update_id}",
                        sql_text(&format!("local-updated-{tick}")),
                        sql_text(&format!("local-updated-note-{tick}")),
                    ),
                    format!("update id={update_id}"),
                )
                .await
                .with_context(|| format!("local writer update failed for id={update_id}"))?;
            }

            if tick > 2 && tick % 5 == 0 {
                let delete_id = 10_000 + (tick - 2);
                local_writer_execute_with_retry(
                    &conn,
                    &format!("DELETE FROM {table} WHERE id = {delete_id}"),
                    format!("delete id={delete_id}"),
                )
                .await
                .with_context(|| format!("local writer delete failed for id={delete_id}"))?;
            }

            tick += 1;
            tokio::select! {
                _ = sleep(LOCAL_WRITER_INTERVAL) => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    })
}

fn spawn_remote_writer_worker(
    config: Config,
    stop: watch::Receiver<bool>,
    table: String,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut stop = stop;
        let mut tick = 0_i64;
        loop {
            if *stop.borrow() {
                return Ok(());
            }

            let id = 20_000 + tick;
            execute_remote_sql(
                &config,
                &format!(
                    "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES ({id}, 'bg-remote', 'remote-payload-{tick}', 1, 'remote-note-{tick}', {})",
                    (tick % 11) + 1
                ),
            )
            .await
            .with_context(|| format!("remote writer insert failed for id={id}"))?;

            if tick > 0 && tick % 2 == 1 {
                let update_id = 20_000 + (tick - 1);
                execute_remote_sql(
                    &config,
                    &format!(
                        "UPDATE {table} SET payload = 'remote-updated-{tick}', note = 'remote-updated-note-{tick}', rev = rev + 1, bucket = bucket + 200 WHERE id = {update_id}"
                    ),
                )
                .await
                .with_context(|| format!("remote writer update failed for id={update_id}"))?;
            }

            if tick > 3 && tick % 6 == 0 {
                let delete_id = 20_000 + (tick - 3);
                execute_remote_sql(
                    &config,
                    &format!("DELETE FROM {table} WHERE id = {delete_id}"),
                )
                .await
                .with_context(|| format!("remote writer delete failed for id={delete_id}"))?;
            }

            tick += 1;
            tokio::select! {
                _ = sleep(REMOTE_WRITER_INTERVAL) => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    })
}

async fn drain_local(local: &LocalReplica) -> Result<()> {
    for _ in 0..3 {
        local.db.push().await.context("final drain push failed")?;
        let _ = local.db.pull().await.context("final drain pull failed")?;
    }
    local
        .db
        .checkpoint()
        .await
        .context("final drain checkpoint failed")?;
    Ok(())
}

async fn wait_for_local_remote_convergence(
    config: &Config,
    local: &LocalReplica,
    table: &str,
    label: &str,
    sync_mode: ConvergenceSyncMode,
) -> Result<TableSnapshot> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        let remote = fetch_remote_snapshot(config, table).await?;
        let conn = local
            .db
            .connect()
            .await
            .context("failed to connect local replica during convergence check")?;
        let local_snapshot = fetch_local_snapshot(&conn, table).await?;
        if local_snapshot == remote {
            println!("local/remote converged for {label} after {attempts} attempts");
            return Ok(remote);
        }
        if Instant::now() >= deadline {
            bail!(
                "local/remote failed to converge for {label} within {:?}\nlocal={}\nremote={}",
                CONVERGENCE_TIMEOUT,
                summarize_snapshot(&local_snapshot),
                summarize_snapshot(&remote),
            );
        }
        if matches!(sync_mode, ConvergenceSyncMode::PushAndPull) {
            if let Err(err) = local.db.push().await {
                eprintln!("convergence retry push failed for {label}: {err:#}");
            }
        }
        if matches!(
            sync_mode,
            ConvergenceSyncMode::PushAndPull | ConvergenceSyncMode::PullOnly
        ) {
            match local.db.pull().await {
                Ok(pulled) => eprintln!("convergence retry pull for {label}: pulled={pulled}"),
                Err(err) => {
                    eprintln!("convergence retry pull failed for {label}: {err:#}");
                    print_local_sync_diagnostics(local, label).await;
                }
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn fetch_local_snapshot(conn: &Connection, table: &str) -> Result<TableSnapshot> {
    let schema_sql = fetch_single_text_local(
        conn,
        &format!("SELECT sql FROM sqlite_schema WHERE name = '{table}'"),
    )
    .await?
    .unwrap_or_default();
    let columns = fetch_columns_local(conn, table).await?;
    let selected = select_columns(&columns, ITEM_COLUMNS);
    if selected.is_empty() {
        return Ok(TableSnapshot {
            schema_sql,
            indexes: fetch_indexes_local(conn, table).await?,
            columns: selected,
            rows: Vec::new(),
        });
    }
    let rows = query_rows(
        conn,
        &format!("SELECT {} FROM {table} ORDER BY id", selected.join(", ")),
    )
    .await?;
    Ok(TableSnapshot {
        schema_sql,
        indexes: fetch_indexes_local(conn, table).await?,
        columns: selected,
        rows,
    })
}

async fn fetch_remote_snapshot(config: &Config, table: &str) -> Result<TableSnapshot> {
    let schema_sql = fetch_single_text_remote(
        config,
        &format!("SELECT sql FROM sqlite_schema WHERE name = '{table}'"),
    )
    .await?
    .unwrap_or_default();
    let columns = fetch_columns_remote(config, table).await?;
    let selected = select_columns(&columns, ITEM_COLUMNS);
    if selected.is_empty() {
        return Ok(TableSnapshot {
            schema_sql,
            indexes: fetch_indexes_remote(config, table).await?,
            columns: selected,
            rows: Vec::new(),
        });
    }
    let rows = query_remote_sql(
        config,
        &format!("SELECT {} FROM {table} ORDER BY id", selected.join(", ")),
    )
    .await?;
    Ok(TableSnapshot {
        schema_sql,
        indexes: fetch_indexes_remote(config, table).await?,
        columns: selected,
        rows,
    })
}

fn select_columns(available: &[String], preferred: &[&str]) -> Vec<String> {
    preferred
        .iter()
        .filter(|name| available.iter().any(|col| col == **name))
        .map(|name| (*name).to_string())
        .collect()
}

async fn fetch_columns_local(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let rows = query_rows(conn, &format!("PRAGMA table_info('{table}')")).await?;
    extract_column_names(&rows)
}

async fn fetch_columns_remote(config: &Config, table: &str) -> Result<Vec<String>> {
    let rows = query_remote_sql(config, &format!("PRAGMA table_info('{table}')")).await?;
    extract_column_names(&rows)
}

async fn fetch_indexes_local(conn: &Connection, table: &str) -> Result<Vec<IndexSnapshot>> {
    let rows = query_rows(conn, &fetch_indexes_sql(table)).await?;
    extract_indexes(&rows)
}

async fn fetch_indexes_remote(config: &Config, table: &str) -> Result<Vec<IndexSnapshot>> {
    let rows = query_remote_sql(config, &fetch_indexes_sql(table)).await?;
    extract_indexes(&rows)
}

fn fetch_indexes_sql(table: &str) -> String {
    format!(
        "SELECT name, sql FROM sqlite_schema \
         WHERE type = 'index' AND tbl_name = {} AND sql IS NOT NULL \
         ORDER BY name",
        sql_text(table)
    )
}

fn extract_indexes(rows: &[Vec<Value>]) -> Result<Vec<IndexSnapshot>> {
    let mut indexes = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(Value::Text(name)) = row.first() else {
            bail!("unexpected index row name: {row:?}");
        };
        let Some(Value::Text(sql)) = row.get(1) else {
            bail!("unexpected index row sql: {row:?}");
        };
        indexes.push(IndexSnapshot {
            name: name.to_string(),
            sql: normalize_schema_sql(sql),
        });
    }
    Ok(indexes)
}

fn normalize_schema_sql(sql: &str) -> String {
    let mut normalized = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(" IF NOT EXISTS ", " ");
    while normalized.contains(" (") {
        normalized = normalized.replace(" (", "(");
    }
    normalized
}

fn extract_column_names(rows: &[Vec<Value>]) -> Result<Vec<String>> {
    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(Value::Text(name)) = row.get(1) else {
            bail!("unexpected table_info row: {row:?}");
        };
        columns.push(name.to_string());
    }
    Ok(columns)
}

async fn fetch_single_text_local(conn: &Connection, sql: &str) -> Result<Option<String>> {
    let rows = query_rows(conn, sql).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let Some(Value::Text(text)) = row.first() else {
        return Ok(None);
    };
    Ok(Some(text.to_string()))
}

async fn fetch_single_int_local(conn: &Connection, sql: &str) -> Result<Option<i64>> {
    let rows = query_rows(conn, sql).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let Some(Value::Integer(value)) = row.first() else {
        return Ok(None);
    };
    Ok(Some(*value))
}

async fn local_table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(fetch_single_text_local(
        conn,
        &format!("SELECT name FROM sqlite_schema WHERE name = '{table}'"),
    )
    .await?
    .is_some())
}

async fn fetch_single_text_remote(config: &Config, sql: &str) -> Result<Option<String>> {
    let rows = query_remote_sql(config, sql).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let Some(Value::Text(text)) = row.first() else {
        return Ok(None);
    };
    Ok(Some(text.to_string()))
}

async fn remote_sync_change_id_for_client(config: &Config, client_id: &str) -> Result<Option<i64>> {
    let rows = query_remote_sql(
        config,
        &format!(
            "SELECT change_id FROM turso_sync_last_change_id WHERE client_id = {}",
            sql_text(client_id)
        ),
    )
    .await?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => match row.first() {
            Some(Value::Integer(value)) => Ok(Some(*value)),
            other => bail!(
                "unexpected remote turso_sync_last_change_id change_id for client_id={client_id}: {other:?}"
            ),
        },
        other => bail!(
            "expected one remote turso_sync_last_change_id row for client_id={client_id}, got {other:?}"
        ),
    }
}

async fn remote_sync_last_change_id_preview(config: &Config) -> String {
    match query_remote_sql(
        config,
        "SELECT client_id, pull_gen, change_id FROM turso_sync_last_change_id ORDER BY client_id LIMIT 8",
    )
    .await
    {
        Ok(rows) => format!("{rows:?}"),
        Err(error) => format!("<unavailable: {error:#}>"),
    }
}

async fn query_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<Value>>> {
    let mut rows = conn
        .query(sql, ())
        .await
        .with_context(|| format!("query failed: {sql}"))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().await.context("failed to fetch row")? {
        let mut values = Vec::with_capacity(row.column_count());
        for idx in 0..row.column_count() {
            values.push(row.get_value(idx)?);
        }
        result.push(values);
    }
    Ok(result)
}

async fn query_remote_journal_mode(config: &Config) -> Result<String> {
    let rows = query_remote_sql(config, "PRAGMA journal_mode").await?;
    let Some(Value::Text(mode)) = rows.first().and_then(|row| row.first()) else {
        bail!("unexpected remote journal_mode response: {rows:?}");
    };
    Ok(mode.to_string())
}

async fn maybe_cleanup_remote_test_tables(config: &Config) -> Result<()> {
    if !env_flag_enabled("SYNC_EXAMPLE_CLEANUP_REMOTE") {
        return Ok(());
    }

    match timeout(REMOTE_CLEANUP_TIMEOUT, cleanup_remote_test_tables(config)).await {
        Ok(result) => result,
        Err(_) => {
            eprintln!(
                "[sync-debug] remote cleanup timed out after {:?}; continuing with unique fuzzer table",
                REMOTE_CLEANUP_TIMEOUT
            );
            Ok(())
        }
    }
}

fn env_flag_enabled(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

async fn cleanup_remote_test_tables(config: &Config) -> Result<()> {
    let rows = query_remote_sql(
        config,
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' \
           AND name NOT LIKE 'sqlite_%' \
           AND (name LIKE 'mvcc_sync_items_%' \
             OR name LIKE 'mvcc_sync_example_%' \
             OR name LIKE 'mvcc_sync_fuzz_%' \
             OR name LIKE 'legacy_sync_fuzz_%') \
         ORDER BY name",
    )
    .await?;
    let mut dropped = 0usize;
    for row in rows {
        let Some(Value::Text(name)) = row.first() else {
            continue;
        };
        query_remote_sql(config, &format!("DROP TABLE IF EXISTS {}", sql_ident(name)))
            .await
            .with_context(|| format!("failed to drop remote test table {name}"))?;
        dropped += 1;
    }
    if dropped > 0 {
        println!("cleaned up {dropped} old remote test tables");
    }
    Ok(())
}

async fn execute_remote_sql(config: &Config, sql: &str) -> Result<()> {
    let _ = query_remote_sql(config, sql).await?;
    Ok(())
}

async fn execute_remote_batch_sql(config: &Config, statements: &[String]) -> Result<()> {
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder().timeout(REMOTE_SQL_TIMEOUT).build()?;
    let requests = statements
        .iter()
        .map(|sql| {
            json!({
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "want_rows": false,
                }
            })
        })
        .collect::<Vec<_>>();
    let mut request = client
        .post(format!("{base_url}/v2/pipeline"))
        .json(&json!({ "requests": requests }));
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }
    if let Some(key) = &config.remote_encryption_key {
        request = request.header("x-turso-encryption-key", key);
    }

    let response = timeout(REMOTE_SQL_TIMEOUT, request.send())
        .await
        .with_context(|| {
            format!(
                "timed out after {REMOTE_SQL_TIMEOUT:?} executing remote SQL batch with {} statements",
                statements.len()
            )
        })?
        .context("failed to execute remote SQL batch")?
        .error_for_status()
        .context("remote SQL batch request failed")?;
    let value: serde_json::Value = response
        .json()
        .await
        .context("invalid remote SQL batch response")?;
    ensure_remote_pipeline_ok(&value)?;
    Ok(())
}

async fn query_remote_generation(config: &Config) -> Result<u64> {
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder().timeout(REMOTE_SQL_TIMEOUT).build()?;
    let mut request = client.get(format!("{base_url}/info"));
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }
    if let Some(key) = &config.remote_encryption_key {
        request = request.header("x-turso-encryption-key", key);
    }
    let response = timeout(REMOTE_SQL_TIMEOUT, request.send())
        .await
        .with_context(|| format!("timed out after {REMOTE_SQL_TIMEOUT:?} querying remote /info"))?
        .context("failed to query remote /info")?
        .error_for_status()
        .context("remote /info request failed")?;
    let info: DbInfo = response.json().await.context("invalid /info response")?;
    Ok(info.current_generation)
}

async fn force_remote_generation_rollover_with_push_pressure(
    config: &Config,
    writer: &LocalReplica,
    writer_idx: usize,
    state: &mut SeededScenarioState,
    label: &str,
    previous_generation: u64,
) -> Result<u64> {
    let deadline = Instant::now() + GENERATION_ROLLOVER_TIMEOUT;
    for attempt in 0..GENERATION_PRESSURE_ATTEMPTS {
        for batch in 0..GENERATION_PRESSURE_BATCHES {
            let id = state.next_local_insert_id(writer_idx);
            local_execute_sql(
                writer,
                &build_insert_sql(
                    &state.table,
                    id,
                    &writer.name,
                    &generation_pressure_payload(label, attempt, batch),
                    &state.available_columns,
                ),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to write generation pressure row attempt={} batch={} id={id}",
                    attempt, batch
                )
            })?;
        }
        assert_local_cdc_advanced(
            writer,
            state,
            writer_idx,
            &format!("{label} generation pressure attempt {attempt}"),
        )
        .await?;
        writer
            .db
            .push()
            .await
            .with_context(|| format!("failed to push generation pressure for {}", writer.name))?;
        assert_seeded_push_cdc_high_water(
            config,
            writer,
            state,
            writer_idx,
            &format!("{label} generation pressure push {attempt}"),
        )
        .await?;

        for _ in 0..GENERATION_PRESSURE_POLLS_PER_ATTEMPT {
            let current_generation = query_remote_generation(config).await?;
            if current_generation != previous_generation {
                println!(
                    "generation rollover via push pressure: label={} previous_generation={} current_generation={} attempt={}",
                    label, previous_generation, current_generation, attempt
                );
                return Ok(current_generation);
            }
            if Instant::now() >= deadline {
                bail!(
                    "remote generation did not advance after push pressure within {:?}; label={} previous_generation={} attempts={}",
                    GENERATION_ROLLOVER_TIMEOUT,
                    label,
                    previous_generation,
                    attempt + 1,
                );
            }
            sleep(GENERATION_ROLLOVER_POLL_INTERVAL).await;
        }
    }
    bail!(
        "remote generation did not advance after push pressure; label={} previous_generation={} attempts={}",
        label,
        previous_generation,
        GENERATION_PRESSURE_ATTEMPTS,
    );
}

fn generation_pressure_payload(label: &str, attempt: usize, batch: usize) -> String {
    format!("{label}-pressure-{attempt}-{batch}-").repeat(32)
}

async fn probe_pull_updates_stream(
    config: &Config,
    client_revision: &str,
) -> Result<PullUpdatesProbe> {
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder()
        .timeout(PULL_UPDATES_PROBE_TIMEOUT)
        .build()?;
    let logical_updates = !client_revision.is_empty();
    let request_body = ProbePullUpdatesReqProtoBody {
        encoding: ProbePageUpdatesEncodingReq::Raw as i32,
        logical_updates,
        server_revision: String::new(),
        client_revision: client_revision.to_string(),
        long_poll_timeout_ms: 0,
        server_pages_selector: Vec::new(),
        server_query_selector: String::new(),
        client_pages: Vec::new(),
    }
    .encode_to_vec();
    let mut request = client
        .post(format!("{base_url}/pull-updates"))
        .header("content-type", "application/protobuf")
        .header("accept-encoding", "application/protobuf")
        .body(request_body);
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }
    if let Some(key) = &config.remote_encryption_key {
        request = request.header("x-turso-encryption-key", key);
    }

    let response = timeout(PULL_UPDATES_PROBE_TIMEOUT, request.send())
        .await
        .with_context(|| {
            format!(
                "timed out after {PULL_UPDATES_PROBE_TIMEOUT:?} probing pull-updates stream: client_revision={client_revision}"
            )
        })?
        .context("failed to probe pull-updates stream")?
        .error_for_status()
        .context("pull-updates probe failed")?;
    let body = timeout(PULL_UPDATES_PROBE_TIMEOUT, response.bytes())
        .await
        .with_context(|| {
            format!(
                "timed out after {PULL_UPDATES_PROBE_TIMEOUT:?} reading pull-updates probe response: client_revision={client_revision}"
            )
        })?
        .context("failed to read pull-updates probe response")?;
    let stream_kind = read_probe_stream_kind(&body)?;
    let (header_len, header_prefix) =
        read_length_delimited_prefix(&body)?.context("pull-updates probe returned no header")?;
    let header_end = header_len + header_prefix;
    let header = ProbePullUpdatesRespProtoBody::decode_length_delimited(&body[..header_end])
        .context("invalid pull-updates probe header")?;
    let apply_mode = ProbePullUpdatesApplyMode::try_from(header.apply_mode)
        .ok()
        .map(|mode| match mode {
            ProbePullUpdatesApplyMode::Incremental => PullUpdatesApplyMode::Incremental,
            ProbePullUpdatesApplyMode::ReplaceBase => PullUpdatesApplyMode::ReplaceBase,
        });
    let mut trailing_messages = 0usize;
    let raw_logical_bytes = if stream_kind == Some(PullUpdatesStreamKind::MvccLogicalLog) {
        body.len().saturating_sub(header_end)
    } else {
        0
    };
    if raw_logical_bytes > 0 {
        trailing_messages = 1;
    }
    let mut offset = header_end;
    while offset < body.len() && stream_kind != Some(PullUpdatesStreamKind::MvccLogicalLog) {
        let Some((message_len, prefix_len)) = read_length_delimited_prefix(&body[offset..])? else {
            bail!("truncated pull-updates probe message at offset {offset}");
        };
        offset += prefix_len + message_len;
        if offset > body.len() {
            bail!("pull-updates probe message exceeded body length");
        }
        trailing_messages += 1;
    }
    Ok(PullUpdatesProbe {
        server_revision: header.server_revision,
        db_size: header.db_size,
        stream_kind,
        apply_mode,
        trailing_messages,
        raw_logical_bytes,
    })
}

async fn probe_pull_updates_header(
    config: &Config,
    client_revision: &str,
) -> Result<PullUpdatesProbe> {
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder()
        .timeout(PULL_UPDATES_PROBE_TIMEOUT)
        .build()?;
    let logical_updates = !client_revision.is_empty();
    let request_body = ProbePullUpdatesReqProtoBody {
        encoding: ProbePageUpdatesEncodingReq::Raw as i32,
        logical_updates,
        server_revision: String::new(),
        client_revision: client_revision.to_string(),
        long_poll_timeout_ms: 0,
        server_pages_selector: Vec::new(),
        server_query_selector: String::new(),
        client_pages: Vec::new(),
    }
    .encode_to_vec();
    let mut request = client
        .post(format!("{base_url}/pull-updates"))
        .header("content-type", "application/protobuf")
        .header("accept-encoding", "application/protobuf")
        .body(request_body);
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }
    if let Some(key) = &config.remote_encryption_key {
        request = request.header("x-turso-encryption-key", key);
    }

    let mut response = timeout(PULL_UPDATES_PROBE_TIMEOUT, request.send())
        .await
        .with_context(|| {
            format!(
                "timed out after {PULL_UPDATES_PROBE_TIMEOUT:?} probing pull-updates header: client_revision={client_revision}"
            )
        })?
        .context("failed to probe pull-updates header")?
        .error_for_status()
        .context("pull-updates header probe failed")?;
    let mut body = Vec::new();
    let header_end = loop {
        if let Some((header_len, header_prefix)) = read_length_delimited_prefix(&body)? {
            let header_end = header_len + header_prefix;
            if body.len() >= header_end {
                break header_end;
            }
        }
        let Some(chunk) = timeout(PULL_UPDATES_PROBE_TIMEOUT, response.chunk())
            .await
            .with_context(|| {
                format!(
                    "timed out after {PULL_UPDATES_PROBE_TIMEOUT:?} reading pull-updates header: client_revision={client_revision}"
                )
            })?
            .context("failed to read pull-updates header")?
        else {
            bail!("pull-updates header probe ended before response header");
        };
        body.extend_from_slice(&chunk);
    };
    let header = ProbePullUpdatesRespProtoBody::decode_length_delimited(&body[..header_end])
        .context("invalid pull-updates probe header")?;
    let stream_kind = ProbePullUpdatesStreamKind::try_from(header.stream_kind)
        .ok()
        .map(|kind| match kind {
            ProbePullUpdatesStreamKind::Pages => PullUpdatesStreamKind::Pages,
            ProbePullUpdatesStreamKind::MvccLogicalLog => PullUpdatesStreamKind::MvccLogicalLog,
        });
    let apply_mode = ProbePullUpdatesApplyMode::try_from(header.apply_mode)
        .ok()
        .map(|mode| match mode {
            ProbePullUpdatesApplyMode::Incremental => PullUpdatesApplyMode::Incremental,
            ProbePullUpdatesApplyMode::ReplaceBase => PullUpdatesApplyMode::ReplaceBase,
        });
    Ok(PullUpdatesProbe {
        server_revision: header.server_revision,
        db_size: header.db_size,
        stream_kind,
        apply_mode,
        trailing_messages: 0,
        raw_logical_bytes: 0,
    })
}

async fn print_pull_updates_probe(config: &Config, client_revision: &str, label: &str) {
    match probe_pull_updates_stream(config, client_revision).await {
        Ok(probe) => eprintln!(
            "pull-updates probe for {label}: client_revision={} server_revision={} db_size={} stream_kind={:?} apply_mode={:?} trailing_messages={} raw_logical_bytes={}",
            client_revision,
            probe.server_revision,
            probe.db_size,
            probe.stream_kind,
            probe.apply_mode,
            probe.trailing_messages,
            probe.raw_logical_bytes,
        ),
        Err(err) => eprintln!(
            "pull-updates probe for {label} failed: client_revision={} err={:#}",
            client_revision, err
        ),
    }
}

fn read_probe_stream_kind(body: &[u8]) -> Result<Option<PullUpdatesStreamKind>> {
    let (header_len, header_prefix) =
        read_length_delimited_prefix(body)?.context("pull-updates probe returned no header")?;
    let header_end = header_len + header_prefix;
    let header = ProbePullUpdatesRespProtoBody::decode_length_delimited(&body[..header_end])
        .context("invalid pull-updates probe header")?;
    Ok(ProbePullUpdatesStreamKind::try_from(header.stream_kind)
        .ok()
        .map(|kind| match kind {
            ProbePullUpdatesStreamKind::Pages => PullUpdatesStreamKind::Pages,
            ProbePullUpdatesStreamKind::MvccLogicalLog => PullUpdatesStreamKind::MvccLogicalLog,
        }))
}

fn probe_is_logical(probe: &PullUpdatesProbe) -> bool {
    probe.stream_kind == Some(PullUpdatesStreamKind::MvccLogicalLog)
}

fn probe_has_logical_payload(probe: &PullUpdatesProbe) -> bool {
    probe.raw_logical_bytes > 0 || probe.trailing_messages > 0
}

fn assert_probe_has_row_upsert(probe: &PullUpdatesProbe, table: &str, rowid: i64) -> Result<()> {
    if probe.stream_kind == Some(PullUpdatesStreamKind::MvccLogicalLog)
        && probe.raw_logical_bytes > 0
    {
        return Ok(());
    }
    bail!(
        "raw MVCC logical-log probe missing bytes for expected upsert table={} rowid={}; server_revision={} stream_kind={:?}",
        table,
        rowid,
        probe.server_revision,
        probe.stream_kind,
    )
}

fn assert_probe_has_schema_op(
    probe: &PullUpdatesProbe,
    _action: ProbeLogicalSchemaAction,
    _kind: ProbeLogicalSchemaKind,
    name: &str,
) -> Result<()> {
    if probe.stream_kind == Some(PullUpdatesStreamKind::MvccLogicalLog)
        && probe.raw_logical_bytes > 0
    {
        return Ok(());
    }
    bail!(
        "raw MVCC logical-log probe missing bytes for expected schema op name={}; server_revision={} stream_kind={:?}",
        name,
        probe.server_revision,
        probe.stream_kind,
    )
}

fn assert_probe_has_table_schema_op(probe: &PullUpdatesProbe, name: &str) -> Result<()> {
    if probe.stream_kind == Some(PullUpdatesStreamKind::MvccLogicalLog)
        && probe.raw_logical_bytes > 0
    {
        return Ok(());
    }
    bail!(
        "raw MVCC logical-log probe missing bytes for expected table schema op name={}; server_revision={} stream_kind={:?}",
        name,
        probe.server_revision,
        probe.stream_kind,
    )
}

fn read_length_delimited_prefix(buf: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut value = 0u64;
    for idx in 0..9 {
        let Some(byte) = buf.get(idx).copied() else {
            return Ok(None);
        };
        value |= ((byte & 0x7f) as u64) << (idx * 7);
        if (byte & 0x80) == 0 {
            return Ok(Some((value as usize, idx + 1)));
        }
    }
    bail!("invalid length-delimited varint prefix")
}

async fn query_remote_sql(config: &Config, sql: &str) -> Result<Vec<Vec<Value>>> {
    let sql_for_log = compact_sql_for_log(sql);
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder().timeout(REMOTE_SQL_TIMEOUT).build()?;
    let mut request = client.post(format!("{base_url}/v2/pipeline")).json(&json!({
        "requests": [{
            "type": "execute",
            "stmt": { "sql": sql }
        }]
    }));
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }
    if let Some(key) = &config.remote_encryption_key {
        request = request.header("x-turso-encryption-key", key);
    }

    let response = timeout(REMOTE_SQL_TIMEOUT, request.send())
        .await
        .with_context(|| {
            format!("timed out after {REMOTE_SQL_TIMEOUT:?} querying remote SQL: {sql_for_log}")
        })?
        .with_context(|| format!("failed to query remote SQL: {sql}"))?
        .error_for_status()
        .with_context(|| format!("remote SQL query failed: {sql}"))?;
    let value: serde_json::Value = response.json().await?;
    ensure_remote_pipeline_ok(&value)?;
    let rows = parse_remote_rows(&value)?;
    Ok(rows)
}

fn compact_sql_for_log(sql: &str) -> String {
    let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    const LIMIT: usize = 220;
    if compact.len() > LIMIT {
        format!("{}...", &compact[..LIMIT])
    } else {
        compact
    }
}

fn normalize_base_url(input: &str) -> Result<String> {
    let input = input.trim();
    let base = if let Some(rest) = input.strip_prefix("libsql://") {
        format!("https://{rest}")
    } else {
        input.to_string()
    };
    if !(base.starts_with("https://") || base.starts_with("http://")) {
        bail!("unsupported remote URL scheme: {input}");
    }
    Ok(base.trim_end_matches('/').to_string())
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    format!("{prefix}_{nanos}")
}

fn debug_schema_sql(table: &str) -> String {
    format!(
        "SELECT rowid, type, name, tbl_name, rootpage, sql \
         FROM sqlite_schema \
         WHERE name = '{table}' OR tbl_name = '{table}' \
         ORDER BY rowid"
    )
}

fn print_snapshot(label: &str, snapshot: &TableSnapshot) {
    println!("{label}:");
    println!(
        "  columns={:?} rows={} digest={} preview={:?}",
        snapshot.columns,
        snapshot.rows.len(),
        snapshot_digest(snapshot),
        snapshot_row_preview(snapshot, 3),
    );
    for row in snapshot.rows.iter().take(20) {
        println!("    {row:?}");
    }
    if snapshot.rows.len() > 20 {
        println!("    ... {} more rows omitted", snapshot.rows.len() - 20);
    }
}

fn print_snapshot_stderr(label: &str, snapshot: &TableSnapshot) {
    eprintln!("{label}:");
    eprintln!("  schema={}", snapshot.schema_sql);
    eprintln!("  indexes={:?}", snapshot.indexes);
    eprintln!(
        "  columns={:?} rows={} digest={} preview={:?}",
        snapshot.columns,
        snapshot.rows.len(),
        snapshot_digest(snapshot),
        snapshot_row_preview(snapshot, 5),
    );
    for row in snapshot.rows.iter().take(20) {
        eprintln!("    {row:?}");
    }
    if snapshot.rows.len() > 20 {
        eprintln!("    ... {} more rows omitted", snapshot.rows.len() - 20);
    }
}

fn print_compact_snapshot(label: &str, snapshot: &TableSnapshot) {
    println!(
        "{label}: columns={:?} rows={}",
        snapshot.columns,
        snapshot.rows.len()
    );
}

fn summarize_snapshot(snapshot: &TableSnapshot) -> String {
    format!(
        "schema={} indexes={:?} columns={:?} rows={} digest={} preview={:?}",
        snapshot.schema_sql,
        snapshot.indexes,
        snapshot.columns,
        snapshot.rows.len(),
        snapshot_digest(snapshot),
        snapshot_row_preview(snapshot, 5),
    )
}

fn summarize_snapshot_diff(expected: &TableSnapshot, actual: &TableSnapshot) -> String {
    let expected_ids = snapshot_int_ids(expected);
    let actual_ids = snapshot_int_ids(actual);
    if expected_ids.is_empty() && actual_ids.is_empty() {
        return "row id diff unavailable".to_string();
    }
    let only_expected = expected_ids
        .iter()
        .filter(|id| !actual_ids.contains(id))
        .take(12)
        .copied()
        .collect::<Vec<_>>();
    let only_actual = actual_ids
        .iter()
        .filter(|id| !expected_ids.contains(id))
        .take(12)
        .copied()
        .collect::<Vec<_>>();
    let changed = expected
        .rows
        .iter()
        .filter_map(|expected_row| {
            let id = match expected_row.first() {
                Some(Value::Integer(value)) => *value,
                _ => return None,
            };
            let actual_row = actual.rows.iter().find(|row| match row.first() {
                Some(Value::Integer(actual_id)) => *actual_id == id,
                _ => false,
            })?;
            if expected_row == actual_row {
                return None;
            }
            Some(format!(
                "id={id} remote={expected_row:?} local={actual_row:?}"
            ))
        })
        .take(8)
        .collect::<Vec<_>>();
    format!("only_remote={only_expected:?} only_local={only_actual:?} changed={changed:?}")
}

fn snapshot_int_ids(snapshot: &TableSnapshot) -> Vec<i64> {
    snapshot
        .rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Integer(value)) => Some(*value),
            _ => None,
        })
        .collect()
}

fn snapshot_digest(snapshot: &TableSnapshot) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    snapshot.schema_sql.hash(&mut hasher);
    snapshot.indexes.hash(&mut hasher);
    snapshot.columns.hash(&mut hasher);
    for row in &snapshot.rows {
        format!("{row:?}").hash(&mut hasher);
    }
    hasher.finish()
}

fn snapshot_row_preview(snapshot: &TableSnapshot, limit: usize) -> Vec<String> {
    snapshot
        .rows
        .iter()
        .take(limit)
        .map(|row| {
            let id = row
                .first()
                .map(|value| format!("{value:?}"))
                .unwrap_or_default();
            let tail = row
                .iter()
                .skip(1)
                .take(2)
                .map(|value| match value {
                    Value::Text(text) => {
                        let text = if text.len() > 48 {
                            format!("{}...", &text[..48])
                        } else {
                            text.clone()
                        };
                        format!("Text({text:?})")
                    }
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join(", ");
            if tail.is_empty() {
                id
            } else {
                format!("{id} [{tail}]")
            }
        })
        .collect()
}

fn parse_remote_rows(value: &serde_json::Value) -> Result<Vec<Vec<Value>>> {
    let rows = value["results"][0]["response"]["result"]["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("unexpected remote SQL response: {value}"))?;
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let values = if let Some(values) = row.as_array() {
            values
        } else {
            row["values"]
                .as_array()
                .ok_or_else(|| anyhow!("unexpected remote row format: {row}"))?
        };
        let mut parsed_row = Vec::with_capacity(values.len());
        for cell in values {
            parsed_row.push(parse_remote_value(cell)?);
        }
        parsed.push(parsed_row);
    }
    Ok(parsed)
}

fn ensure_remote_pipeline_ok(value: &serde_json::Value) -> Result<()> {
    let results = value["results"]
        .as_array()
        .ok_or_else(|| anyhow!("unexpected remote SQL response: {value}"))?;
    for (idx, result) in results.iter().enumerate() {
        if result["type"] != "ok" {
            bail!("remote SQL step {idx} failed: {value}");
        }
    }
    Ok(())
}

fn parse_remote_value(value: &serde_json::Value) -> Result<Value> {
    if let Some(cell_type) = value["type"].as_str() {
        return match cell_type {
            "null" => Ok(Value::Null),
            "text" => Ok(Value::Text(
                value["value"]
                    .as_str()
                    .ok_or_else(|| anyhow!("unexpected remote text cell: {value}"))?
                    .to_string(),
            )),
            "integer" => {
                let parsed = if let Some(text) = value["value"].as_str() {
                    text.parse()?
                } else if let Some(integer) = value["value"].as_i64() {
                    integer
                } else {
                    bail!("unexpected remote integer cell: {value}");
                };
                Ok(Value::Integer(parsed))
            }
            "float" => {
                Ok(Value::Real(value["value"].as_f64().ok_or_else(|| {
                    anyhow!("unexpected remote float cell: {value}")
                })?))
            }
            "blob" => Ok(Value::Blob(
                value["base64"]
                    .as_str()
                    .or_else(|| value["value"].as_str())
                    .unwrap_or_default()
                    .as_bytes()
                    .to_vec(),
            )),
            _ => bail!("unexpected typed remote value: {value}"),
        };
    }
    if !value["null"].is_null() {
        return Ok(Value::Null);
    }
    if let Some(text) = value["text"].as_str() {
        return Ok(Value::Text(text.to_string()));
    }
    if let Some(integer) = value["integer"].as_str() {
        return Ok(Value::Integer(integer.parse()?));
    }
    if let Some(float) = value["float"].as_f64() {
        return Ok(Value::Real(float));
    }
    if let Some(blob) = value["blob"].as_str() {
        return Ok(Value::Blob(blob.as_bytes().to_vec()));
    }
    bail!("unexpected remote value: {value}")
}
