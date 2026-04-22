use std::sync::Arc;

use crate::error::LimboError;
use crate::schema::TypeDef;
use crate::storage::pager::AutoVacuumMode;
use crate::storage::sqlite3_ondisk::{CacheSize, DatabaseHeader, TextEncoding};
use crate::util::IOExt;
use crate::vdbe::execute::InsnFunctionStepResult;
use crate::Result;
use crate::{Connection, Database, DatabaseOpts, EncryptionOpts, OpenFlags};
use turso_macros::{turso_assert, turso_assert_eq};

/// A representation of a row from `sqlite_schema`.
///
/// Carries `rootpage` so we can distinguish storage-backed tables/indexes
/// (rootpage != 0) from virtual tables, custom index-method indexes, views,
/// and triggers (rootpage = 0).
#[derive(Debug)]
pub(crate) struct SchemaEntry {
    pub entry_type: SchemaEntryType,
    pub name: String,
    /// `sqlite_schema.tbl_name`: for indexes and triggers, this is the table
    /// the object belongs to; for tables and views it usually matches `name`.
    pub tbl_name: String,
    pub rootpage: i64,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaEntryType {
    Table,
    Index,
    Trigger,
    View,
}

impl SchemaEntryType {
    pub fn from_str(s: &str) -> crate::Result<Self> {
        match s {
            "table" => Ok(Self::Table),
            "index" => Ok(Self::Index),
            "trigger" => Ok(Self::Trigger),
            "view" => Ok(Self::View),
            other => Err(crate::error::LimboError::Corrupt(format!(
                "unexpected sqlite_schema type: {other}"
            ))),
        }
    }
}

impl SchemaEntry {
    /// Parse from a sqlite_schema row: (type, name, tbl_name, rootpage, sql)
    pub fn from_row(row: &crate::vdbe::Row) -> crate::Result<Self> {
        let entry_type = SchemaEntryType::from_str(row.get::<&str>(0)?)?;
        Ok(Self {
            entry_type,
            name: row.get::<&str>(1)?.to_string(),
            tbl_name: row.get::<&str>(2)?.to_string(),
            rootpage: row.get::<i64>(3)?,
            sql: row.get::<&str>(4)?.to_string(),
        })
    }

    /// Whether this entry represents a storage-backed object (table or index
    /// with rootpage != 0). MVCC can use negative rootpage values before
    /// checkpointing, so this checks `!= 0` rather than `> 0`.
    pub fn is_storage_backed(&self) -> bool {
        self.rootpage != 0
    }

    pub fn is_sqlite_sequence(&self) -> bool {
        self.name == "sqlite_sequence"
    }
}

/// Split rowid-ordered schema entries into the replay phases used by VACUUM.
/// Returns indices into `entries` for `(tables_to_create, tables_to_copy,
/// indexes_to_create, post_data_entries)`.
pub(crate) fn classify_schema_entries(
    entries: &[SchemaEntry],
) -> (Vec<usize>, Vec<usize>, Vec<usize>, Vec<usize>) {
    let mut tables_to_create: Vec<usize> = Vec::new();
    let mut tables_to_copy: Vec<usize> = Vec::new();
    let mut indexes_to_create: Vec<usize> = Vec::new();
    let mut post_data_entries: Vec<usize> = Vec::new();

    for (idx, entry) in entries.iter().enumerate() {
        match entry.entry_type {
            SchemaEntryType::Table if entry.is_storage_backed() => {
                // Skip sqlite_sequence in the schema creation phase. When we
                // create an AUTOINCREMENT table, Turso automatically creates
                // sqlite_sequence if it doesn't exist (see translate/schema.rs).
                // Since entries are ordered by rowid, an AUTOINCREMENT table may
                // appear before sqlite_sequence. If we create that table first
                // (which auto-creates sqlite_sequence), then later try to run
                // "CREATE TABLE sqlite_sequence(name,seq)", it fails with
                // "table already exists".
                if !entry.is_sqlite_sequence() {
                    tables_to_create.push(idx);
                }
                // All storage-backed tables get their data copied, including
                // sqlite_stat1 and other internal storage-backed tables.
                // sqlite_sequence data copy is handled specially by the caller
                // (only if the target materialized it).
                tables_to_copy.push(idx);
            }
            SchemaEntryType::Index if entry.is_storage_backed() => {
                // Storage-backed index (rootpage != 0). Includes both normal
                // user-defined indexes and backing_btree indexes for custom index
                // methods. The caller filters out backing_btree indexes since
                // those are recreated by the parent index method in the post-data
                // phase.
                indexes_to_create.push(idx);
            }
            SchemaEntryType::Trigger | SchemaEntryType::View => {
                // Triggers and views are replayed after data copy to avoid
                // triggers firing during the copy phase.
                post_data_entries.push(idx);
            }
            SchemaEntryType::Table => {
                // Virtual tables (rootpage = 0, type = table) land here.
                // Replayed after data copy alongside triggers and views.
                turso_assert!(
                    !entry.is_storage_backed(),
                    "unexpected storage-backed table (rootpage = 0): {entry.name}"
                );
                post_data_entries.push(idx);
            }
            SchemaEntryType::Index => {
                // Custom index-method indexes (FTS, vector, etc.) have rootpage = 0
                // in sqlite_schema because their storage is managed by the index
                // method, not a B-tree.
                // Replayed after data copy.
                post_data_entries.push(idx);
            }
        }
    }

    (
        tables_to_create,
        tables_to_copy,
        indexes_to_create,
        post_data_entries,
    )
}

/// Target database feature flags needed for schema replay during a vacuum build.
pub(crate) fn vacuum_target_opts_from_source(source_db: &Database) -> DatabaseOpts {
    DatabaseOpts::new()
        .with_views(source_db.experimental_views_enabled())
        .with_index_method(source_db.experimental_index_method_enabled())
        .with_custom_types(source_db.experimental_custom_types_enabled())
        .with_encryption(source_db.experimental_encryption_enabled())
        .with_autovacuum(source_db.experimental_autovacuum_enabled())
        .with_attach(source_db.experimental_attach_enabled())
        .with_generated_columns(source_db.experimental_generated_columns_enabled())
}

pub(crate) fn reject_unsupported_vacuum_auto_vacuum_mode(mode: AutoVacuumMode) -> Result<()> {
    if matches!(mode, AutoVacuumMode::Incremental) {
        return Err(LimboError::InternalError(
            "Incremental auto-vacuum is not supported".to_string(),
        ));
    }
    Ok(())
}

/// Database header metadata that the target build must finalize before commit.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VacuumDbHeaderMeta {
    schema_cookie: u32,
    default_page_cache_size: CacheSize,
    text_encoding: TextEncoding,
    user_version: i32,
    application_id: i32,
}

impl VacuumDbHeaderMeta {
    pub(crate) fn from_source_header(source: &DatabaseHeader) -> Self {
        Self {
            schema_cookie: source.schema_cookie.get().wrapping_add(1),
            default_page_cache_size: source.default_page_cache_size,
            text_encoding: source.text_encoding,
            user_version: source.user_version.get(),
            application_id: source.application_id.get(),
        }
    }

    fn apply_to(self, header: &mut DatabaseHeader) {
        header.schema_cookie = self.schema_cookie.into();
        header.default_page_cache_size = self.default_page_cache_size;
        header.text_encoding = self.text_encoding;
        header.user_version = self.user_version.into();
        header.application_id = self.application_id.into();
    }
}

/// File-backed internal temp database used by in-place `VACUUM`.
///
/// The temp directory is dropped after the connection and database handles so
/// host files can be closed before the directory cleanup runs.
pub(crate) struct VacuumTempDb {
    pub conn: Arc<Connection>,
    _db: Arc<Database>,
    #[cfg(test)]
    path: String,
    #[cfg(not(target_family = "wasm"))]
    _temp_dir: tempfile::TempDir,
}

#[cfg(not(target_family = "wasm"))]
fn vacuum_temp_db_encryption(
    source_conn: &Arc<Connection>,
) -> Result<(Option<EncryptionOpts>, Option<crate::EncryptionKey>)> {
    let Some(cipher_mode) = source_conn.get_encryption_cipher_mode() else {
        return Ok((None, None));
    };
    let encryption_key = source_conn.encryption_key.read().clone().ok_or_else(|| {
        LimboError::InternalError(
            "encrypted in-place VACUUM temp database requires source encryption key".to_string(),
        )
    })?;
    let encryption_opts = EncryptionOpts {
        cipher: cipher_mode.to_string(),
        hexkey: hex::encode(encryption_key.as_slice()),
    };
    Ok((Some(encryption_opts), Some(encryption_key)))
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn open_vacuum_temp_db(
    source_conn: &Arc<Connection>,
    source_db: &Arc<Database>,
    page_size: u32,
    reserved_space: u8,
) -> Result<VacuumTempDb> {
    // todo: let users specify the temp dir path
    let temp_dir = tempfile::tempdir().map_err(|e| crate::error::io_error(e, "tempdir"))?;
    let source_db_name = std::path::Path::new(&source_db.path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tursodb_vacuum_temp.db");
    // all temp files we will prefix with `etilqs_` as an homage to SQLite's lore
    let path = temp_dir.path().join(format!("etilqs_{source_db_name}"));
    let path = path
        .to_str()
        .ok_or_else(|| LimboError::InternalError("vacuum temp path is not valid UTF-8".into()))?
        .to_string();
    #[cfg(test)]
    let test_path = path.clone();

    let (encryption_opts, encryption_key) = vacuum_temp_db_encryption(source_conn)?;
    let db = Database::open_file_with_flags(
        source_db.io.clone(),
        &path,
        OpenFlags::Create,
        vacuum_target_opts_from_source(source_db),
        encryption_opts,
    )?;
    let conn = db.connect_with_encryption(encryption_key)?;
    conn.reset_page_size(page_size)?;
    conn.set_reserved_bytes(reserved_space)?;
    conn.wal_auto_checkpoint_disable();

    Ok(VacuumTempDb {
        conn,
        _db: db,
        #[cfg(test)]
        path: test_path,
        _temp_dir: temp_dir,
    })
}

#[cfg(target_family = "wasm")]
pub(crate) fn open_vacuum_temp_db(
    _source_conn: &Arc<Connection>,
    _source_db: &Arc<Database>,
    _page_size: u32,
    _reserved_space: u8,
) -> Result<VacuumTempDb> {
    Err(LimboError::InternalError(
        "in-place VACUUM requires a file-backed internal temp database".to_string(),
    ))
}

fn finalize_vacuum_target_header(
    target_conn: &Arc<Connection>,
    header_meta: &VacuumDbHeaderMeta,
) -> Result<crate::IOResult<()>> {
    if let Some(mv_store) = target_conn.mv_store_for_db(crate::MAIN_DB_ID) {
        let tx_id = target_conn.get_mv_tx_id_for_db(crate::MAIN_DB_ID);
        return mv_store
            .with_header_mut(|header| header_meta.apply_to(header), tx_id.as_ref())
            .map(crate::IOResult::Done);
    }
    let pager = target_conn.pager.load();
    pager.with_header_mut(|header| header_meta.apply_to(header))
}

/// Finish a VACUUM INTO output database with durable on-disk state.
///
/// Target build runs with `synchronous=OFF` for speed, so its commit alone is
/// not the durability boundary. Before reporting success, VACUUM INTO forces a
/// final TRUNCATE checkpoint under `SyncMode::Full` so the destination's main
/// database file is self-contained and synced.
pub(crate) fn finalize_vacuum_into_output(target: &VacuumTargetBuildContext) -> Result<()> {
    target.target_conn.set_sync_mode(crate::SyncMode::Full);
    target
        .target_conn
        .checkpoint(crate::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })?;
    Ok(())
}

/// Copy functions, vtab modules, and index methods from one connection to
/// another so that schema replay on the target sees the same symbols as
/// the source.
pub(crate) fn mirror_symbols(source: &Connection, target: &Connection) {
    let source_syms = source.syms.read();
    let mut target_syms = target.syms.write();
    target_syms.functions.extend(
        source_syms
            .functions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );
    target_syms.vtab_modules.extend(
        source_syms
            .vtab_modules
            .iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );
    target_syms.index_methods.extend(
        source_syms
            .index_methods
            .iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );
}

/// Capture non-builtin custom type definitions from the source schema.
pub(crate) fn capture_custom_types(
    source: &Connection,
    db_id: usize,
) -> Vec<(String, Arc<TypeDef>)> {
    source.with_schema(db_id, |schema| {
        schema
            .type_registry
            .iter()
            .filter(|(_, td)| !td.is_builtin)
            .map(|(name, td)| (name.clone(), td.clone()))
            .collect()
    })
}

/// Configuration for building a compacted vacuum target database.
/// Callers must mirror source symbols (functions, vtab modules, index methods)
/// directly into the target connection before starting the state machine.
pub(crate) struct VacuumTargetBuildConfig {
    pub source_conn: Arc<Connection>,
    /// Escaped schema name for safe SQL interpolation (e.g. `"main"`).
    pub escaped_schema_name: String,
    /// Database index for schema lookups on the source connection.
    pub source_db_id: usize,
    /// Database header metadata to write before committing the target database.
    pub header_meta: VacuumDbHeaderMeta,
    /// Pre-captured source custom type definitions for STRICT table replay.
    pub source_custom_types: Vec<(String, Arc<TypeDef>)>,
    /// Whether the target database should be placed in MVCC journal mode.
    pub target_mvcc_enabled: bool,
    /// Auto-vacuum mode to use while rebuilding the target image.
    pub target_auto_vacuum_mode: AutoVacuumMode,
    /// Whether to copy the source MVCC metadata table into the target image.
    pub copy_mvcc_metadata_table: bool,
}

/// Context for the vacuum target build. Holds the target connection and all
/// intermediate state needed across async yields.
pub(crate) struct VacuumTargetBuildContext {
    target_conn: Arc<Connection>,
    phase: VacuumTargetBuildPhase,
    /// Typed schema entries collected from sqlite_schema, ordered by rowid.
    schema_entries: Vec<SchemaEntry>,
    /// Storage-backed tables to CREATE (excludes sqlite_sequence).
    tables_to_create: Vec<usize>,
    /// Storage-backed tables whose data to copy.
    tables_to_copy: Vec<usize>,
    /// User-defined secondary indexes to CREATE
    indexes_to_create: Vec<usize>,
    /// Triggers, views, and rootpage = 0 objects
    post_data_entries: Vec<usize>,
}

impl VacuumTargetBuildContext {
    pub fn new(target_conn: Arc<Connection>) -> Self {
        Self {
            target_conn,
            phase: VacuumTargetBuildPhase::Init,
            schema_entries: Vec::new(),
            tables_to_create: Vec::new(),
            tables_to_copy: Vec::new(),
            indexes_to_create: Vec::new(),
            post_data_entries: Vec::new(),
        }
    }

    pub(crate) fn cleanup_after_error(&mut self) -> Result<()> {
        self.phase = VacuumTargetBuildPhase::Done;
        if !self.target_conn.get_auto_commit() {
            // Target-build connections are disposable. On error we only need to
            // unwind connection-local transaction state before the caller drops
            // the target database handle.
            let pager = self.target_conn.pager.load();
            self.target_conn.rollback_manual_txn_cleanup(&pager, true);
        }
        Ok(())
    }
}

/// Phases for the vacuum target build state machine.
#[derive(Default)]
pub(crate) enum VacuumTargetBuildPhase {
    /// Mirror symbols/types, set perf flags, begin target tx, prepare schema query.
    #[default]
    Init,
    /// Step through schema query to collect rows
    CollectSchemaRows { schema_stmt: Box<crate::Statement> },
    /// Prepare CREATE TABLE statement on the target (idx into tables_to_create)
    PrepareCreateTable { idx: usize },
    /// Step through CREATE TABLE statement on the target (async)
    StepCreateTable {
        target_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Start copying a table's data
    StartCopyTable { table_idx: usize },
    /// Select rows from source table and insert into the target.
    CopyRows {
        select_stmt: Box<crate::Statement>,
        target_insert_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Step through INSERT statement on the target.
    StepTargetInsert {
        select_stmt: Box<crate::Statement>,
        target_insert_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Prepare CREATE INDEX statement on the target (idx into indexes_to_create)
    PrepareCreateIndex { idx: usize },
    /// Step through CREATE INDEX statement on the target.
    StepCreateIndex {
        target_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Prepare post-data schema objects (triggers, views, rootpage = 0 entries)
    PreparePostData { idx: usize },
    /// Step through post-data CREATE statement on the target.
    StepPostData {
        target_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Finalize database header metadata before committing the target database.
    FinalizeTargetHeader,
    /// Operation complete
    Done,
}

/// Build a compacted vacuum target database.
///
/// This is an async state machine implementation that yields on I/O operations.
/// The caller creates a target database with matching
/// page_size, reserved bytes, source feature flags, and schema-replay symbols.
///
/// It:
/// 1. Mirrors source symbols and custom types to the target
/// 2. Queries sqlite_schema for all schema objects including rootpage, ordered by rowid
/// 3. Creates storage-backed tables (rootpage != 0) in the target, excluding
///    sqlite_sequence (auto-created when AUTOINCREMENT tables are created)
/// 4. Copies data for all storage-backed tables, including sqlite_stat1 and other
///    internal storage-backed tables
/// 5. Creates user-defined secondary indexes after data copy for performance
///    (backing-btree indexes for custom index methods are excluded here)
/// 6. Creates triggers, views, and rootpage = 0 objects last (after data copy).
///    Custom index methods (FTS, vector) recreate and backfill their backing
///    indexes from the copied table data in this phase.
/// 7. Finalizes target database header metadata, then commits the target transaction.
pub(crate) fn vacuum_target_build_step(
    config: &VacuumTargetBuildConfig,
    state: &mut VacuumTargetBuildContext,
) -> Result<crate::IOResult<()>> {
    loop {
        let current_phase = std::mem::take(&mut state.phase);

        match current_phase {
            VacuumTargetBuildPhase::Init => {
                // Mirror source custom type definitions into the target schema
                // so that STRICT tables with custom type columns can resolve
                // those types during CREATE TABLE replay.
                if !config.source_custom_types.is_empty() {
                    state.target_conn.with_schema_mut(|target_schema| {
                        for (name, td) in &config.source_custom_types {
                            target_schema.type_registry.insert(name.clone(), td.clone());
                        }
                    });
                }

                // Enable MVCC on target if source has it enabled
                if config.target_mvcc_enabled {
                    state.target_conn.execute("PRAGMA journal_mode = 'mvcc'")?;
                }

                // Performance optimizations for the target database (matches SQLite vacuum.c):
                // 1. Disable fsync - the target is a new output/temp database, if crash occurs we just delete it
                // 2. Disable foreign key checks - source data is already consistent
                // 3. Defer auto-checkpoint until the caller performs the final durable checkpoint
                // These match SQLite's vacuum.c optimizations (PAGER_SYNCHRONOUS_OFF, ~SQLITE_ForeignKeys)
                state.target_conn.set_sync_mode(crate::SyncMode::Off);
                state.target_conn.set_foreign_keys_enabled(false);
                state.target_conn.set_check_constraints_ignored(true);
                state.target_conn.wal_auto_checkpoint_disable();

                // Wrap all operations in a single write transaction so helper
                // statements share one durable target build and cleanup can
                // roll it back reliably on error.
                state.target_conn.execute("BEGIN IMMEDIATE")?;
                state
                    .target_conn
                    .pager
                    .load()
                    .persist_auto_vacuum_mode(config.target_auto_vacuum_mode)?;

                // Query sqlite_schema with rootpage, ordered by rowid.
                // Decide whether schema replay should include the internal MVCC metadata table.
                // VACUUM INTO excludes it because an MVCC destination bootstraps that table
                // itself. In-place VACUUM includes it because the temp image is the future
                // physical source database and must preserve the table as-is.
                let escaped_schema_name = &config.escaped_schema_name;
                let schema_sql = if config.copy_mvcc_metadata_table {
                    format!(
                        "SELECT type, name, tbl_name, rootpage, sql \
                         FROM \"{escaped_schema_name}\".sqlite_schema \
                         WHERE sql IS NOT NULL ORDER BY rowid"
                    )
                } else {
                    format!(
                        "SELECT type, name, tbl_name, rootpage, sql \
                         FROM \"{escaped_schema_name}\".sqlite_schema \
                         WHERE sql IS NOT NULL AND name <> '{}' ORDER BY rowid",
                        crate::mvcc::database::MVCC_META_TABLE_NAME
                    )
                };
                let schema_stmt = config.source_conn.prepare_internal(schema_sql.as_str())?;

                state.phase = VacuumTargetBuildPhase::CollectSchemaRows {
                    schema_stmt: Box::new(schema_stmt),
                };
                continue;
            }

            VacuumTargetBuildPhase::CollectSchemaRows { mut schema_stmt } => {
                match schema_stmt.step()? {
                    crate::StepResult::Row => {
                        let row = schema_stmt
                            .row()
                            .expect("StepResult::Row but row() returned None");
                        state.schema_entries.push(SchemaEntry::from_row(row)?);
                        state.phase = VacuumTargetBuildPhase::CollectSchemaRows { schema_stmt };
                        continue;
                    }
                    crate::StepResult::Done => {
                        // Classify schema entries into replay phases using rootpage.
                        let (tables_create, tables_copy, indexes_create, post_data) =
                            classify_schema_entries(&state.schema_entries);
                        state.tables_to_create = tables_create;
                        state.tables_to_copy = tables_copy;
                        // Backing-btree indexes are implementation details of custom index
                        // methods. i.e. when custom indexes are created, they are created automatically
                        // The user-visible custom-index CREATE in post_data_entries
                        // recreates and backfills those backing indexes from the copied rows.
                        // for now, we will skip them
                        state.indexes_to_create = indexes_create
                            .into_iter()
                            .filter(|entry_ordinal| {
                                let entry = &state.schema_entries[*entry_ordinal];
                                !config
                                    .source_conn
                                    .with_schema(config.source_db_id, |schema| {
                                        schema
                                            .get_index(&entry.tbl_name, &entry.name)
                                            .is_some_and(|idx| idx.is_backing_btree_index())
                                    })
                            })
                            .collect();
                        state.post_data_entries = post_data;

                        state.phase = VacuumTargetBuildPhase::PrepareCreateTable { idx: 0 };
                        continue;
                    }
                    crate::StepResult::IO => {
                        let io = schema_stmt
                            .take_io_completions()
                            .expect("StepResult::IO returned but no completions available");
                        state.phase = VacuumTargetBuildPhase::CollectSchemaRows { schema_stmt };
                        return Ok(crate::IOResult::IO(io));
                    }
                    crate::StepResult::Busy | crate::StepResult::Interrupt => {
                        return Err(LimboError::Busy);
                    }
                }
            }

            // Phase 1: Create storage-backed tables (rootpage != 0, type=table),
            // excluding sqlite_sequence (auto-created by AUTOINCREMENT tables).
            VacuumTargetBuildPhase::PrepareCreateTable { idx } => {
                let entries_len = state.tables_to_create.len();
                if idx >= entries_len {
                    // Done creating tables, start copying data
                    state.phase = VacuumTargetBuildPhase::StartCopyTable { table_idx: 0 };
                    continue;
                }

                let entry_ordinal = state.tables_to_create[idx];
                let entry = &state.schema_entries[entry_ordinal];
                let sql_str = &entry.sql;

                // System tables (sqlite_stat1, __turso_internal_types, etc.) have
                // reserved name prefixes that translate_create_table rejects for
                // user SQL. Temporarily mark the target connection as nested during
                // prepare() so the reserved-name check is bypassed at compile
                // time. The guard is only for prepare: keeping it during step()
                // would make this CREATE TABLE look nested, so its Transaction
                // opcode would skip write setup.
                let is_system = crate::schema::is_system_table(&entry.name);
                if is_system {
                    state.target_conn.start_nested();
                }
                let target_stmt = state.target_conn.prepare(sql_str);
                if is_system {
                    state.target_conn.end_nested();
                }
                let target_stmt = target_stmt?;
                state.phase = VacuumTargetBuildPhase::StepCreateTable {
                    target_schema_stmt: Box::new(target_stmt),
                    idx,
                };
                continue;
            }

            VacuumTargetBuildPhase::StepCreateTable {
                mut target_schema_stmt,
                idx,
            } => match target_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE TABLE statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.phase = VacuumTargetBuildPhase::PrepareCreateTable { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = target_schema_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.phase = VacuumTargetBuildPhase::StepCreateTable {
                        target_schema_stmt,
                        idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            // Phase 2: Copy data for all storage-backed tables.
            // Column lists are derived from BTreeTable.columns in the schema,
            // not PRAGMA table_info, because table_info omits generated columns
            // while SELECT * includes them - causing a column count mismatch.
            VacuumTargetBuildPhase::StartCopyTable { table_idx } => {
                let tables_len = state.tables_to_copy.len();
                if table_idx >= tables_len {
                    // Done copying all tables, proceed to deferred indexes.
                    state.phase = VacuumTargetBuildPhase::PrepareCreateIndex { idx: 0 };
                    continue;
                }

                let entry_ordinal = state.tables_to_copy[table_idx];
                let entry = &state.schema_entries[entry_ordinal];
                let table_name = &entry.name;

                // sqlite_sequence: only copy data if the target has it
                // (auto-created when an AUTOINCREMENT table was created in
                // phase 1). If not present, skip — no AUTOINCREMENT tables
                // means no counters to preserve. The explicit copy is needed
                // because inserting rows with the `rowid` pseudo-column does
                // not update sqlite_sequence counters automatically.
                if entry.is_sqlite_sequence() {
                    let target_has_sequence = state
                        .target_conn
                        .schema
                        .read()
                        .get_btree_table(crate::schema::SQLITE_SEQUENCE_TABLE_NAME)
                        .is_some();
                    if !target_has_sequence {
                        state.phase = VacuumTargetBuildPhase::StartCopyTable {
                            table_idx: table_idx + 1,
                        };
                        continue;
                    }
                }

                let escaped_table_name = table_name.replace('"', "\"\"");
                // Derive copy-column list from BTreeTable.columns in schema,
                // filtering out virtual generated columns so the SELECT and
                // INSERT arities stay aligned.
                let source_btree_table = config
                    .source_conn
                    .with_schema(config.source_db_id, |schema| {
                        schema.get_btree_table(table_name)
                    });

                // sqlite_sequence may already have rows from the AUTOINCREMENT
                // tracking that ran during the table data copy. Use INSERT OR
                // REPLACE so the source counter values overwrite any stale
                // auto-generated ones (matches SQLite vacuum.c behavior).
                // todo: sqlite disables AUTOINCREMENT during vacuum, but we don't have such a way yet
                let (select_sql, insert_sql) = build_copy_sql(
                    &config.escaped_schema_name,
                    &escaped_table_name,
                    source_btree_table.as_deref(),
                    entry.is_sqlite_sequence(),
                )?;

                // SELECT from source, INSERT into the target.
                let select_stmt = config.source_conn.prepare_internal(&select_sql)?;

                // System tables need nested mode during prepare() to bypass
                // "may not be modified" checks. Can't use prepare_internal()
                // because the nested guard must not persist into step() - the
                // Transaction opcode needs to run for page-level write setup.
                let is_system = crate::schema::is_system_table(table_name);
                if is_system {
                    state.target_conn.start_nested();
                }
                let target_insert_stmt = state.target_conn.prepare(&insert_sql);
                if is_system {
                    state.target_conn.end_nested();
                }
                let target_insert_stmt = target_insert_stmt?;

                state.phase = VacuumTargetBuildPhase::CopyRows {
                    select_stmt: Box::new(select_stmt),
                    target_insert_stmt: Box::new(target_insert_stmt),
                    table_idx,
                };
                continue;
            }

            VacuumTargetBuildPhase::CopyRows {
                mut select_stmt,
                mut target_insert_stmt,
                table_idx,
            } => match select_stmt.step()? {
                crate::StepResult::Row => {
                    let row = select_stmt
                        .row()
                        .expect("StepResult::Row but row() returned None");

                    target_insert_stmt.reset()?;
                    target_insert_stmt.clear_bindings();
                    for (i, value) in row.get_values().cloned().enumerate() {
                        let index =
                            std::num::NonZero::new(i + 1).expect("i + 1 is always non-zero");
                        target_insert_stmt.bind_at(index, value);
                    }

                    state.phase = VacuumTargetBuildPhase::StepTargetInsert {
                        select_stmt,
                        target_insert_stmt,
                        table_idx,
                    };
                    continue;
                }
                crate::StepResult::Done => {
                    // Move to next table
                    state.phase = VacuumTargetBuildPhase::StartCopyTable {
                        table_idx: table_idx + 1,
                    };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = select_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.phase = VacuumTargetBuildPhase::CopyRows {
                        select_stmt,
                        target_insert_stmt,
                        table_idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            VacuumTargetBuildPhase::StepTargetInsert {
                select_stmt,
                mut target_insert_stmt,
                table_idx,
            } => match target_insert_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("INSERT statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    // Go back to get next row from source
                    state.phase = VacuumTargetBuildPhase::CopyRows {
                        select_stmt,
                        target_insert_stmt,
                        table_idx,
                    };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = target_insert_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.phase = VacuumTargetBuildPhase::StepTargetInsert {
                        select_stmt,
                        target_insert_stmt,
                        table_idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            // Phase 3: Create user-defined secondary indexes.
            VacuumTargetBuildPhase::PrepareCreateIndex { idx } => {
                let entries_len = state.indexes_to_create.len();
                if idx >= entries_len {
                    // Done creating indexes, move to post-data objects
                    state.phase = VacuumTargetBuildPhase::PreparePostData { idx: 0 };
                    continue;
                }

                let entry_ordinal = state.indexes_to_create[idx];
                let entry = &state.schema_entries[entry_ordinal];
                // Backing-btree indexes for custom index methods were filtered
                // out when indexes_to_create was built. The remaining CREATE
                // INDEX statements are user-visible and can use ordinary prepare.
                let target_stmt = state.target_conn.prepare(&entry.sql)?;
                state.phase = VacuumTargetBuildPhase::StepCreateIndex {
                    target_schema_stmt: Box::new(target_stmt),
                    idx,
                };
                continue;
            }

            VacuumTargetBuildPhase::StepCreateIndex {
                mut target_schema_stmt,
                idx,
            } => match target_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE INDEX statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.phase = VacuumTargetBuildPhase::PrepareCreateIndex { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = target_schema_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.phase = VacuumTargetBuildPhase::StepCreateIndex {
                        target_schema_stmt,
                        idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            // Phase 4: Create triggers, views, and rootpage=0 schema objects.
            VacuumTargetBuildPhase::PreparePostData { idx } => {
                let entries_len = state.post_data_entries.len();
                if idx >= entries_len {
                    state.phase = VacuumTargetBuildPhase::FinalizeTargetHeader;
                    continue;
                }

                let entry_ordinal = state.post_data_entries[idx];
                let entry = &state.schema_entries[entry_ordinal];
                let target_stmt = state.target_conn.prepare(&entry.sql)?;
                state.phase = VacuumTargetBuildPhase::StepPostData {
                    target_schema_stmt: Box::new(target_stmt),
                    idx,
                };
                continue;
            }

            VacuumTargetBuildPhase::StepPostData {
                mut target_schema_stmt,
                idx,
            } => match target_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.phase = VacuumTargetBuildPhase::PreparePostData { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = target_schema_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.phase = VacuumTargetBuildPhase::StepPostData {
                        target_schema_stmt,
                        idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            VacuumTargetBuildPhase::FinalizeTargetHeader => {
                match finalize_vacuum_target_header(&state.target_conn, &config.header_meta)? {
                    crate::IOResult::Done(()) => {}
                    crate::IOResult::IO(io) => {
                        state.phase = VacuumTargetBuildPhase::FinalizeTargetHeader;
                        return Ok(crate::IOResult::IO(io));
                    }
                }

                state.phase = VacuumTargetBuildPhase::Done;
                continue;
            }

            VacuumTargetBuildPhase::Done => {
                // Commit the target transaction started in Init phase.
                state.target_conn.execute("COMMIT")?;
                return Ok(crate::IOResult::Done(()));
            }
        }
    }
}

// Build the SELECT and INSERT SQL strings for copying a table's data.
//
// Uses the in-memory `BTreeTable` column metadata from the schema to derive
// the copy-column list. Virtual generated columns are excluded from both
// SELECT and INSERT since they are computed, not stored. This keeps both
// lists tied to the same stored-column model.
//
// For e.g. given a schema:
// CREATE TABLE employees (
//       id INTEGER PRIMARY KEY,
//       name TEXT,
//       salary INTEGER,
//       bonus INTEGER GENERATED ALWAYS AS (salary * 0.1) VIRTUAL
//   )
//
//   Output:
//   - select_sql: SELECT rowid, "name", "salary" FROM "main"."employees"
//   - insert_sql: INSERT INTO "main"."employees" (rowid, "name", "salary") VALUES (?, ?, ?)
pub(crate) fn build_copy_sql(
    escaped_schema_name: &str,
    escaped_table_name: &str,
    source_btree_table: Option<&BTreeTable>,
    or_replace: bool,
) -> Result<(String, String)> {
    let Some(btree) = source_btree_table else {
        // Storage-backed tables must have schema metadata. If we get here,
        // the schema is inconsistent - somewhere it has gone terribly wrong
        return Err(LimboError::Corrupt(format!(
            "no schema metadata for storage-backed table \"{escaped_table_name}\""
        )));
    };

    // Collect non-virtual-generated columns with their quoted names.
    let mut data_columns: Vec<String> = Vec::new();
    let mut rowid_alias_col_idx: Option<usize> = None;
    for (i, col) in btree.columns().iter().enumerate() {
        if col.is_virtual_generated() {
            continue;
        }
        if col.is_rowid_alias() {
            rowid_alias_col_idx = Some(i);
        }
        let Some(name) = col.name.as_deref() else {
            return Err(LimboError::Corrupt(format!(
                "missing column name for table \"{escaped_table_name}\""
            )));
        };
        let escaped = name.replace('"', "\"\"");
        data_columns.push(format!("\"{escaped}\""));
    }

    if data_columns.is_empty() {
        return Err(LimboError::Corrupt(
            "found a table without any columns".to_string(),
        ));
    }

    // Determine rowid handling: for has_rowid tables, we need to preserve the
    // rowid. Find an alias name (rowid, _rowid_, or oid) that doesn't conflict
    // with an actual column name.
    let rowid_alias = if btree.has_rowid {
        ["rowid", "_rowid_", "oid"]
            .iter()
            .copied()
            .find(|alias| btree.get_column(alias).is_none())
    } else {
        None
    };

    // Build the column lists. If there's a rowid alias column (INTEGER PRIMARY KEY),
    // we exclude it from the data columns and use the rowid alias instead, since
    // the rowid alias column IS the rowid.
    //
    // Track bind_count explicitly instead of parsing the joined string - column
    // names can contain commas inside quotes which would miscount.
    let (select_cols, insert_cols, bind_count) = if let Some(alias) = rowid_alias {
        if let Some(alias_idx) = rowid_alias_col_idx {
            // Remove the rowid alias column from data_columns (it IS the rowid)
            let mut filtered: Vec<&str> = Vec::new();
            let mut col_physical_idx = 0;
            for (i, col) in btree.columns().iter().enumerate() {
                if col.is_virtual_generated() {
                    continue;
                }
                if i != alias_idx {
                    filtered.push(&data_columns[col_physical_idx]);
                }
                col_physical_idx += 1;
            }
            if filtered.is_empty() {
                // Table only has the rowid alias column
                (alias.to_string(), alias.to_string(), 1)
            } else {
                let count = filtered.len() + 1; // +1 for rowid alias
                let cols = filtered.join(", ");
                (
                    format!("{alias}, {cols}"),
                    format!("{alias}, {cols}"),
                    count,
                )
            }
        } else {
            // has_rowid but no explicit alias column - prepend the chosen rowid
            // pseudo-column to the stored column list.
            let count = data_columns.len() + 1; // +1 for rowid alias
            let cols = data_columns.join(", ");
            (
                format!("{alias}, {cols}"),
                format!("{alias}, {cols}"),
                count,
            )
        }
    } else {
        // Either WITHOUT ROWID, or a rowid table where all three pseudo-names
        // are shadowed by real columns. In the shadowed case SQL cannot name
        // the hidden rowid, and SQLite does not require rowid stability for
        // tables without an INTEGER PRIMARY KEY during VACUUM.
        let count = data_columns.len();
        let cols = data_columns.join(", ");
        (cols.clone(), cols, count)
    };

    // The first placeholder is just "?"; each later placeholder adds ", ?".
    // Reserve 3 bytes per placeholder, then subtract the 2-byte separator that
    // the first placeholder does not need.
    let mut placeholders = String::with_capacity(bind_count.saturating_mul(3).saturating_sub(2));
    for i in 0..bind_count {
        if i > 0 {
            placeholders.push_str(", ");
        }
        placeholders.push('?');
    }

    let select_sql =
        format!("SELECT {select_cols} FROM \"{escaped_schema_name}\".\"{escaped_table_name}\"");
    let insert_sql =
        format!("INSERT INTO \"{escaped_table_name}\" ({insert_cols}) VALUES ({placeholders})");

    Ok((select_sql, insert_sql))
}
