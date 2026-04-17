use std::sync::Arc;

use crate::error::LimboError;
use crate::schema::TypeDef;
use crate::types::IOResult;
use crate::Result;
use crate::{Connection, Database};
use turso_macros::turso_assert;
use turso_parser::identifier::Identifier;

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
                // (only if destination materialized it).
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

/// Configuration for the VACUUM INTO engine. Provided by the caller (opcode
/// handler) after reading source metadata and setting up the destination DB.
pub(crate) struct VacuumIntoConfig {
    /// Source connection - used for `prepare_internal` and `with_schema` during
    /// schema collection and data copy.
    pub source_conn: Arc<Connection>,
    /// Escaped schema name for safe SQL interpolation (e.g. `"main"`).
    pub escaped_schema_name: String,
    /// Database index for schema lookups on the source connection.
    pub database_id: usize,
    /// Source `user_version` pragma value to copy to destination.
    pub source_user_version: i32,
    /// Source `application_id` pragma value to copy to destination.
    pub source_application_id: i32,
    /// Pre-captured source custom type definitions for STRICT table replay.
    pub source_custom_types: Vec<(String, Arc<TypeDef>)>,
    /// Whether the source database has MVCC enabled.
    pub source_mvcc_enabled: bool,
}

pub(crate) struct VacuumInto {
    config: VacuumIntoConfig,
    state: VacuumIntoState,
}

impl VacuumInto {
    pub fn new(
        config: VacuumIntoConfig,
        dest_db: Arc<Database>,
        dest_conn: Arc<Connection>,
    ) -> Self {
        Self {
            config,
            state: VacuumIntoState::new(dest_db, dest_conn),
        }
    }

    pub fn step(&mut self) -> Result<IOResult<()>> {
        vacuum_into_step(&self.config, &mut self.state)
    }
}

/// State for the VACUUM INTO engine. Holds the destination connection and all
/// intermediate state needed across async yields.
struct VacuumIntoState {
    /// Keep the destination database alive while VACUUM INTO is in progress.
    #[allow(dead_code)]
    dest_db: Arc<Database>,
    dest_conn: Arc<Connection>,
    sub_state: VacuumIntoSubState,
    /// Typed schema entries collected from sqlite_schema, ordered by rowid.
    schema_entries: Vec<SchemaEntry>,
    /// Storage-backed tables to CREATE (excludes sqlite_sequence).
    tables_to_create: Vec<usize>,
    /// Storage-backed tables whose data to copy.
    tables_to_copy: Vec<usize>,
    /// User-defined secondary indexes to CREATE.
    indexes_to_create: Vec<usize>,
    /// Triggers, views, custom indexes, and rootpage = 0 objects.
    post_data_entries: Vec<usize>,
}

impl VacuumIntoState {
    fn new(dest_db: Arc<Database>, dest_conn: Arc<Connection>) -> Self {
        Self {
            dest_db,
            dest_conn,
            sub_state: VacuumIntoSubState::Init,
            schema_entries: Vec::new(),
            tables_to_create: Vec::new(),
            tables_to_copy: Vec::new(),
            indexes_to_create: Vec::new(),
            post_data_entries: Vec::new(),
        }
    }
}

/// Sub-states for the VACUUM INTO engine state machine.
#[derive(Default)]
enum VacuumIntoSubState {
    /// Mirror symbols/types, set perf flags, begin dest tx, prepare schema query.
    #[default]
    Init,
    /// Step through schema query to collect rows
    CollectSchemaRows { schema_stmt: Box<crate::Statement> },
    /// Prepare CREATE TABLE statement on destination (idx into tables_to_create)
    PrepareCreateTable { idx: usize },
    /// Step through CREATE TABLE statement on destination (async)
    StepCreateTable {
        dest_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Start copying a table's data
    StartCopyTable { table_idx: usize },
    /// Select rows from source table and insert into destination
    CopyRows {
        select_stmt: Box<crate::Statement>,
        dest_insert_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Step through INSERT statement on destination
    StepDestInsert {
        select_stmt: Box<crate::Statement>,
        dest_insert_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Copy meta values (user_version, application_id) from source to destination
    CopyMetaValues,
    /// Prepare CREATE INDEX statement on destination (idx into indexes_to_create)
    PrepareCreateIndex { idx: usize },
    /// Step through CREATE INDEX statement on destination
    StepCreateIndex {
        dest_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Prepare post-data schema objects (triggers, views, rootpage = 0 entries)
    PreparePostData { idx: usize },
    /// Step through post-data CREATE statement on destination
    StepPostData {
        dest_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Operation complete
    Done,
}

/// VACUUM INTO - create a compacted copy of the database at the specified path.
///
/// This is an async state machine implementation that yields on I/O operations.
/// The caller creates a new database at the destination path with matching
/// page_size, reserved bytes, source feature flags, and schema-replay symbols.
///
/// It:
/// 1. Mirrors source symbols and custom types to destination
/// 2. Queries sqlite_schema for all schema objects including rootpage, ordered by rowid
/// 3. Creates storage-backed tables (rootpage != 0) in destination, excluding
///    sqlite_sequence (auto-created when AUTOINCREMENT tables are created)
/// 4. Copies data for all storage-backed tables, including sqlite_stat1 and other
///    internal storage-backed tables
/// 5. Creates user-defined secondary indexes after data copy for performance
///    (backing-btree indexes for custom index methods are excluded here)
/// 6. Copies meta values (user_version, application_id) from source to destination
/// 7. Creates triggers, views, and rootpage = 0 objects last (after data copy).
///    Custom index methods (FTS, vector) recreate and backfill their backing
///    indexes from the copied table data in this phase.
fn vacuum_into_step(
    config: &VacuumIntoConfig,
    state: &mut VacuumIntoState,
) -> Result<IOResult<()>> {
    loop {
        let current_sub_state = std::mem::take(&mut state.sub_state);

        match current_sub_state {
            VacuumIntoSubState::Init => {
                // Mirror source symbols needed for schema replay (functions, vtab
                // modules, index methods). We skip vtabs - those are live instances
                // tied to the source connection and not needed for compiling schema SQL.
                {
                    let source_syms = config.source_conn.syms.read();
                    let mut dest_syms = state.dest_conn.syms.write();
                    dest_syms.functions.extend(
                        source_syms
                            .functions
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone())),
                    );
                    dest_syms.vtab_modules.extend(
                        source_syms
                            .vtab_modules
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone())),
                    );
                    dest_syms.index_methods.extend(
                        source_syms
                            .index_methods
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone())),
                    );
                }

                // Mirror source custom type definitions into destination schema
                // so that STRICT tables with custom type columns can resolve
                // those types during CREATE TABLE replay.
                if !config.source_custom_types.is_empty() {
                    state.dest_conn.with_schema_mut(|dest_schema| {
                        for (name, td) in &config.source_custom_types {
                            dest_schema
                                .type_registry
                                //TODO Identifier wtf
                                .insert(Identifier::from(name.as_str()), td.clone());
                        }
                    });
                }

                // Enable MVCC on destination if source has it enabled
                // Must be done before any schema operations to ensure the log file is created
                if config.source_mvcc_enabled {
                    state.dest_conn.execute("PRAGMA journal_mode = 'mvcc'")?;
                }

                // Performance optimizations for destination database (matches SQLite vacuum.c):
                // 1. Disable fsync - destination is a new file, if crash occurs we just delete it
                // 2. Disable foreign key checks - source data is already consistent
                // These match SQLite's vacuum.c optimizations (PAGER_SYNCHRONOUS_OFF, ~SQLITE_ForeignKeys)
                state.dest_conn.set_sync_mode(crate::SyncMode::Off);
                state.dest_conn.set_foreign_keys_enabled(false);

                // Wrap all operations in a single transaction for atomicity.
                state.dest_conn.execute("BEGIN")?;

                // Query sqlite_schema with rootpage, ordered by rowid.
                // Exclude the MVCC metadata table - it is an internal artifact.
                let escaped_schema_name = &config.escaped_schema_name;
                let schema_sql = format!(
                    "SELECT type, name, tbl_name, rootpage, sql \
                     FROM \"{escaped_schema_name}\".sqlite_schema \
                     WHERE sql IS NOT NULL AND name <> '{}' ORDER BY rowid",
                    crate::mvcc::database::MVCC_META_TABLE_NAME
                );
                let schema_stmt = config.source_conn.prepare_internal(schema_sql.as_str())?;

                state.sub_state = VacuumIntoSubState::CollectSchemaRows {
                    schema_stmt: Box::new(schema_stmt),
                };
                continue;
            }

            VacuumIntoSubState::CollectSchemaRows { mut schema_stmt } => {
                match schema_stmt.step()? {
                    crate::StepResult::Row => {
                        let row = schema_stmt
                            .row()
                            .expect("StepResult::Row but row() returned None");
                        state.schema_entries.push(SchemaEntry::from_row(row)?);
                        state.sub_state = VacuumIntoSubState::CollectSchemaRows { schema_stmt };
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
                                    .with_schema(config.database_id, |schema| {
                                        schema
                                            .get_index(
                                                &Identifier::from(entry.tbl_name.as_str()),
                                                &Identifier::from(entry.name.as_str()),
                                            )
                                            .is_some_and(|idx| idx.is_backing_btree_index())
                                    })
                            })
                            .collect();
                        state.post_data_entries = post_data;

                        state.sub_state = VacuumIntoSubState::PrepareCreateTable { idx: 0 };
                        continue;
                    }
                    crate::StepResult::IO => {
                        let io = schema_stmt
                            .take_io_completions()
                            .expect("StepResult::IO returned but no completions available");
                        state.sub_state = VacuumIntoSubState::CollectSchemaRows { schema_stmt };
                        return Ok(IOResult::IO(io));
                    }
                    crate::StepResult::Busy | crate::StepResult::Interrupt => {
                        return Err(LimboError::Busy);
                    }
                }
            }

            // Phase 1: Create storage-backed tables (rootpage != 0, type=table),
            // excluding sqlite_sequence (auto-created by AUTOINCREMENT tables).
            VacuumIntoSubState::PrepareCreateTable { idx } => {
                let entries_len = state.tables_to_create.len();
                if idx >= entries_len {
                    // Done creating tables, start copying data
                    state.sub_state = VacuumIntoSubState::StartCopyTable { table_idx: 0 };
                    continue;
                }

                let entry_ordinal = state.tables_to_create[idx];
                let entry = &state.schema_entries[entry_ordinal];
                let sql_str = &entry.sql;

                // System tables (sqlite_stat1, __turso_internal_types, etc.) have
                // reserved name prefixes that translate_create_table rejects for
                // user SQL. Temporarily mark the dest connection as nested during
                // prepare() so the reserved-name check is bypassed at compile
                // time. The guard is only for prepare: keeping it during step()
                // would make this CREATE TABLE look nested, so its Transaction
                // opcode would skip write setup.
                let is_system = crate::schema::is_system_table(&entry.name);
                if is_system {
                    state.dest_conn.start_nested();
                }
                let dest_stmt = state.dest_conn.prepare(sql_str);
                if is_system {
                    state.dest_conn.end_nested();
                }
                let dest_stmt = dest_stmt?;
                state.sub_state = VacuumIntoSubState::StepCreateTable {
                    dest_schema_stmt: Box::new(dest_stmt),
                    idx,
                };
                continue;
            }

            VacuumIntoSubState::StepCreateTable {
                mut dest_schema_stmt,
                idx,
            } => match dest_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE TABLE statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.sub_state = VacuumIntoSubState::PrepareCreateTable { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = dest_schema_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.sub_state = VacuumIntoSubState::StepCreateTable {
                        dest_schema_stmt,
                        idx,
                    };
                    return Ok(IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            // Phase 2: Copy data for all storage-backed tables.
            // Column lists are derived from BTreeTable.columns in the schema,
            // not PRAGMA table_info, because table_info omits generated columns
            // while SELECT * includes them - causing a column count mismatch.
            VacuumIntoSubState::StartCopyTable { table_idx } => {
                let tables_len = state.tables_to_copy.len();
                if table_idx >= tables_len {
                    // Done copying all tables, proceed to meta values
                    state.sub_state = VacuumIntoSubState::CopyMetaValues;
                    continue;
                }

                let entry_ordinal = state.tables_to_copy[table_idx];
                let entry = &state.schema_entries[entry_ordinal];
                let table_name = &entry.name;

                // sqlite_sequence: only copy data if the destination has it
                // (auto-created when an AUTOINCREMENT table was created in
                // phase 1). If not present, skip — no AUTOINCREMENT tables
                // means no counters to preserve. The explicit copy is needed
                // because inserting rows with the `rowid` pseudo-column does
                // not update sqlite_sequence counters automatically.
                if entry.is_sqlite_sequence() {
                    let dest_has_sequence = state
                        .dest_conn
                        .schema
                        .read()
                        .get_btree_table(&Identifier::from(
                            crate::schema::SQLITE_SEQUENCE_TABLE_NAME,
                        ))
                        .is_some();
                    if !dest_has_sequence {
                        state.sub_state = VacuumIntoSubState::StartCopyTable {
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
                    .with_schema(config.database_id, |schema| {
                        schema.get_btree_table(&Identifier::from(table_name.as_str()))
                    });

                let (select_sql, insert_sql) = build_copy_sql(
                    &config.escaped_schema_name,
                    &escaped_table_name,
                    source_btree_table.as_deref(),
                )?;

                // SELECT from source, INSERT into destination.
                let select_stmt = config.source_conn.prepare_internal(&select_sql)?;

                // System tables need nested mode during prepare() to bypass
                // "may not be modified" checks. Can't use prepare_internal()
                // because the nested guard must not persist into step() - the
                // Transaction opcode needs to run for page-level write setup.
                let is_system = crate::schema::is_system_table(table_name);
                if is_system {
                    state.dest_conn.start_nested();
                }
                let dest_insert_stmt = state.dest_conn.prepare(&insert_sql);
                if is_system {
                    state.dest_conn.end_nested();
                }
                let dest_insert_stmt = dest_insert_stmt?;

                state.sub_state = VacuumIntoSubState::CopyRows {
                    select_stmt: Box::new(select_stmt),
                    dest_insert_stmt: Box::new(dest_insert_stmt),
                    table_idx,
                };
                continue;
            }

            VacuumIntoSubState::CopyRows {
                mut select_stmt,
                mut dest_insert_stmt,
                table_idx,
            } => match select_stmt.step()? {
                crate::StepResult::Row => {
                    let row = select_stmt
                        .row()
                        .expect("StepResult::Row but row() returned None");

                    dest_insert_stmt.reset()?;
                    dest_insert_stmt.clear_bindings();
                    for (i, value) in row.get_values().cloned().enumerate() {
                        let index =
                            std::num::NonZero::new(i + 1).expect("i + 1 is always non-zero");
                        dest_insert_stmt.bind_at(index, value);
                    }

                    state.sub_state = VacuumIntoSubState::StepDestInsert {
                        select_stmt,
                        dest_insert_stmt,
                        table_idx,
                    };
                    continue;
                }
                crate::StepResult::Done => {
                    // Move to next table
                    state.sub_state = VacuumIntoSubState::StartCopyTable {
                        table_idx: table_idx + 1,
                    };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = select_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.sub_state = VacuumIntoSubState::CopyRows {
                        select_stmt,
                        dest_insert_stmt,
                        table_idx,
                    };
                    return Ok(IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            VacuumIntoSubState::StepDestInsert {
                select_stmt,
                mut dest_insert_stmt,
                table_idx,
            } => match dest_insert_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("INSERT statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    // Go back to get next row from source
                    state.sub_state = VacuumIntoSubState::CopyRows {
                        select_stmt,
                        dest_insert_stmt,
                        table_idx,
                    };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = dest_insert_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.sub_state = VacuumIntoSubState::StepDestInsert {
                        select_stmt,
                        dest_insert_stmt,
                        table_idx,
                    };
                    return Ok(IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            VacuumIntoSubState::CopyMetaValues => {
                // Copy meta values to destination database
                // Use pragma_update to set user_version and application_id
                // Note: schema_version is not copied - VACUUM INTO creates a new file so
                // there's no cache to invalidate. The destination will have its own
                // schema_version based on the schema operations performed.
                state
                    .dest_conn
                    .pragma_update("user_version", config.source_user_version.to_string())?;
                state
                    .dest_conn
                    .pragma_update("application_id", config.source_application_id.to_string())?;

                // Phase 3: Create user-defined secondary indexes after data copy
                // for performance (avoids maintaining indexes during bulk insert).
                state.sub_state = VacuumIntoSubState::PrepareCreateIndex { idx: 0 };
                continue;
            }

            // Phase 3: Create user-defined secondary indexes.
            VacuumIntoSubState::PrepareCreateIndex { idx } => {
                let entries_len = state.indexes_to_create.len();
                if idx >= entries_len {
                    // Done creating indexes, move to post-data objects
                    state.sub_state = VacuumIntoSubState::PreparePostData { idx: 0 };
                    continue;
                }

                let entry_ordinal = state.indexes_to_create[idx];
                let entry = &state.schema_entries[entry_ordinal];
                // Backing-btree indexes for custom index methods were filtered
                // out when indexes_to_create was built. The remaining CREATE
                // INDEX statements are user-visible and can use ordinary prepare.
                let dest_stmt = state.dest_conn.prepare(&entry.sql)?;
                state.sub_state = VacuumIntoSubState::StepCreateIndex {
                    dest_schema_stmt: Box::new(dest_stmt),
                    idx,
                };
                continue;
            }

            VacuumIntoSubState::StepCreateIndex {
                mut dest_schema_stmt,
                idx,
            } => match dest_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE INDEX statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.sub_state = VacuumIntoSubState::PrepareCreateIndex { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = dest_schema_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.sub_state = VacuumIntoSubState::StepCreateIndex {
                        dest_schema_stmt,
                        idx,
                    };
                    return Ok(IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            // Phase 4: Create triggers, views, and rootpage=0 schema objects.
            VacuumIntoSubState::PreparePostData { idx } => {
                let entries_len = state.post_data_entries.len();
                if idx >= entries_len {
                    state.sub_state = VacuumIntoSubState::Done;
                    continue;
                }

                let entry_ordinal = state.post_data_entries[idx];
                let entry = &state.schema_entries[entry_ordinal];
                let dest_stmt = state.dest_conn.prepare(&entry.sql)?;
                state.sub_state = VacuumIntoSubState::StepPostData {
                    dest_schema_stmt: Box::new(dest_stmt),
                    idx,
                };
                continue;
            }

            VacuumIntoSubState::StepPostData {
                mut dest_schema_stmt,
                idx,
            } => match dest_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.sub_state = VacuumIntoSubState::PreparePostData { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO => {
                    let io = dest_schema_stmt
                        .take_io_completions()
                        .expect("StepResult::IO returned but no completions available");
                    state.sub_state = VacuumIntoSubState::StepPostData {
                        dest_schema_stmt,
                        idx,
                    };
                    return Ok(IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy);
                }
            },

            VacuumIntoSubState::Done => {
                // Commit the destination transaction started in Init state.
                state.dest_conn.execute("COMMIT")?;
                return Ok(IOResult::Done(()));
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
    source_btree_table: Option<&crate::schema::BTreeTable>,
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
    for (i, col) in btree.columns.iter().enumerate() {
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
            for (i, col) in btree.columns.iter().enumerate() {
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
