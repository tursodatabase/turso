//! Generative stress test for multi-process WAL.
//!
//! Runs a seeded sequence of operations across multiple virtual processes,
//! checking invariants at every step. Fully deterministic: given a seed,
//! the exact same sequence replays every time.
//!
//! The shadow state tracks what every process should see. After every write
//! commits, ALL processes must see the exact shadow state — this is a local
//! database, not an eventually-consistent network system.
//!
//! Run with: `SEED=42 cargo test -p turso_core --lib mp_generative -- --test-threads=1`

use std::collections::BTreeMap;
use std::sync::Arc;

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

use crate::io::{OpenFlags, PlatformIO, IO};
use crate::numeric::Numeric;
use crate::storage::database::DatabaseFile;
use crate::storage::multi_process_wal::MultiProcessWal;
use crate::storage::wal::CheckpointMode;
use crate::types::Value;
use crate::util::IOExt;
use crate::{Connection, Database, DatabaseOpts, LimboError, LockingMode};

// ---------------------------------------------------------------------------
// Shadow state — the single source of truth
// ---------------------------------------------------------------------------

/// A row in the shadow state: (id, val).
/// `val` tracks the current TEXT value for the row so UPDATEs can be verified.
#[derive(Clone, Debug, PartialEq)]
struct ShadowRow {
    id: i64,
    val: Option<String>,
}

/// A table in the shadow state.
#[derive(Clone, Debug)]
struct ShadowTable {
    rows: Vec<ShadowRow>,
    next_id: i64,
}

/// Global shadow state — represents the committed state of the database.
/// After every successful write, ALL processes must see this exact state.
#[derive(Clone, Debug)]
struct ShadowState {
    tables: BTreeMap<String, ShadowTable>,
    /// Monotonically increasing counter for unique table names.
    name_counter: u64,
}

impl ShadowState {
    fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
            name_counter: 0,
        }
    }

    fn next_name(&mut self, prefix: &str) -> String {
        self.name_counter += 1;
        format!("{prefix}{}", self.name_counter)
    }
}

// ---------------------------------------------------------------------------
// Virtual process
// ---------------------------------------------------------------------------

struct VirtualProcess {
    id: usize,
    db: Option<Arc<Database>>,
    conn: Option<Arc<Connection>>,
    /// Whether this process is alive (not crashed).
    alive: bool,
    /// Whether this process is currently inside a BEGIN..COMMIT/ROLLBACK.
    in_transaction: bool,
    /// Pending shadow changes accumulated during a transaction.
    /// On COMMIT these merge into the global shadow; on ROLLBACK they're discarded.
    pending_inserts: Vec<(String, ShadowRow)>,
    pending_updates: Vec<(String, i64, String)>,
    pending_deletes: Vec<(String, i64)>,
    /// Snapshot of committed tables at BEGIN time. The process sees this
    /// frozen state + its own pending changes for the duration of the tx.
    /// Set on BEGIN, cleared on COMMIT/ROLLBACK/crash.
    snapshot_tables: Option<BTreeMap<String, ShadowTable>>,
}

impl VirtualProcess {
    fn conn(&self) -> &Arc<Connection> {
        self.conn.as_ref().expect("process not alive")
    }
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Op {
    Insert,
    Update,
    Delete,
    Select,
    Checkpoint,
    CreateTable,
    DropTable,
    Crash,
    Restart,
    IntegrityCheck,
    BeginTx,
    Commit,
    Rollback,
    CheckpointThenVerify,
    InsertConflict,
    SqlCheckpoint,
    GracefulClose,
    CrashAndRestart,
    ConstrainedCheckpoint,
    /// Full checkpoint followed by immediate insert on the same process.
    /// Forces try_restart_log_before_write which changes the WAL salt and
    /// resets max_frame to 0.
    CheckpointThenWrite,
    /// CREATE INDEX on a random existing table — forces multi-page schema
    /// changes (new B-tree root + sqlite_master update).
    CreateIndex,
    Noop,
}

impl Op {
    fn weighted_list() -> Vec<(Op, u32)> {
        vec![
            (Op::Insert, 25),
            (Op::Update, 10),
            (Op::Delete, 8),
            (Op::Select, 25),
            (Op::Checkpoint, 10),
            (Op::CreateTable, 3),
            (Op::DropTable, 2),
            (Op::Crash, 2),
            (Op::Restart, 3),
            (Op::IntegrityCheck, 5),
            (Op::BeginTx, 5),
            (Op::Commit, 4),
            (Op::Rollback, 2),
            (Op::CheckpointThenVerify, 5),
            (Op::InsertConflict, 5),
            (Op::SqlCheckpoint, 5),
            (Op::GracefulClose, 2),
            (Op::CrashAndRestart, 3),
            (Op::ConstrainedCheckpoint, 5),
            (Op::CheckpointThenWrite, 4),
            (Op::CreateIndex, 3),
            (Op::Noop, 5),
        ]
    }

    fn pick(rng: &mut ChaCha8Rng) -> Op {
        let weights = Self::weighted_list();
        let total: u32 = weights.iter().map(|(_, w)| w).sum();
        let mut roll = rng.random_range(0..total);
        for (op, w) in &weights {
            if roll < *w {
                return *op;
            }
            roll -= w;
        }
        Op::Noop
    }
}

// ---------------------------------------------------------------------------
// Simulator
// ---------------------------------------------------------------------------

struct MpSimulator {
    rng: ChaCha8Rng,
    seed: u64,
    locking_mode: LockingMode,
    processes: Vec<VirtualProcess>,
    _dir: tempfile::TempDir,
    db_path: String,
    shadow: ShadowState,
    steps: usize,
    max_wal_frame_seen: u64,
    /// In SharedReads mode, tracks which process holds the permanent write
    /// lock. None means no process currently owns it (e.g. after a crash).
    /// In SharedWrites mode, this is always None (any process can write).
    writer_owner: Option<usize>,
    /// Whether to print per-step trace output (set via VERBOSE=1 env var).
    verbose: bool,
}

/// Open a database bypassing the process-wide registry so each call
/// creates a truly independent Database instance with its own fds and
/// TSHM, simulating a separate OS process.
fn open_bypass_registry(db_path: &str, mode: LockingMode) -> crate::Result<Arc<Database>> {
    let wal_path = format!("{db_path}-wal");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let mut flags = OpenFlags::default();
    if mode.is_shared() {
        flags |= OpenFlags::SharedLock;
    }
    let file = io.open_file(db_path, flags, true)?;
    let db_file = Arc::new(DatabaseFile::new(file));
    let opts = DatabaseOpts::new().with_locking_mode(mode);
    let mut state = crate::OpenDbAsyncState::new();
    loop {
        match Database::open_with_flags_bypass_registry_async(
            &mut state,
            io.clone(),
            db_path,
            Some(&wal_path),
            db_file.clone(),
            flags,
            opts,
            None,
        )? {
            crate::IOResult::Done(db) => return Ok(db),
            crate::IOResult::IO(io_completion) => {
                io_completion.wait(&*io)?;
            }
        }
    }
}

impl MpSimulator {
    fn new(seed: u64, num_processes: usize, steps: usize, mode: LockingMode) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create WAL-mode database.
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "wal").unwrap();
        }

        let db_path_str = db_path.to_str().unwrap().to_string();
        let mut processes = Vec::new();
        let mut writer_owner = None;
        for i in 0..num_processes {
            let db = open_bypass_registry(&db_path_str, mode).unwrap();
            let conn = db.connect().unwrap();
            if mode == LockingMode::SharedReads && writer_owner.is_none() {
                writer_owner = Some(i);
            }
            processes.push(VirtualProcess {
                id: i,
                db: Some(db),
                conn: Some(conn),
                alive: true,
                in_transaction: false,
                pending_inserts: Vec::new(),
                pending_updates: Vec::new(),
                pending_deletes: Vec::new(),
                snapshot_tables: None,
            });
        }

        let verbose = std::env::var("VERBOSE").is_ok_and(|v| v == "1");

        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            seed,
            locking_mode: mode,
            processes,
            _dir: dir,
            db_path: db_path_str,
            shadow: ShadowState::new(),
            steps,
            max_wal_frame_seen: 0,
            writer_owner,
            verbose,
        }
    }

    fn run(mut self) {
        // Create at least one table so operations have something to work with.
        let creator = self.writer_owner.unwrap_or(0);
        self.do_create_table(creator, 0);

        for step in 0..self.steps {
            let op = Op::pick(&mut self.rng);
            let alive_ids: Vec<usize> = self
                .processes
                .iter()
                .filter(|p| p.alive)
                .map(|p| p.id)
                .collect();

            if alive_ids.is_empty() {
                self.do_restart(0, step);
                continue;
            }

            let proc_idx = alive_ids[self.rng.random_range(0..alive_ids.len())];

            if self.verbose {
                eprintln!("step {step}: proc {proc_idx} op={op:?}");
            }
            match op {
                Op::Insert => self.do_insert(proc_idx, step),
                Op::Update => self.do_update(proc_idx, step),
                Op::Delete => self.do_delete(proc_idx, step),
                Op::Select => self.do_select(proc_idx, step),
                Op::Checkpoint => self.do_checkpoint(proc_idx, step),
                Op::CreateTable => self.do_create_table(proc_idx, step),
                Op::DropTable => self.do_drop_table(proc_idx, step),
                Op::Crash => self.do_crash(proc_idx, step),
                Op::Restart => {
                    let dead_ids: Vec<usize> = self
                        .processes
                        .iter()
                        .filter(|p| !p.alive)
                        .map(|p| p.id)
                        .collect();
                    if !dead_ids.is_empty() {
                        let idx = dead_ids[self.rng.random_range(0..dead_ids.len())];
                        self.do_restart(idx, step);
                    }
                }
                Op::IntegrityCheck => self.do_integrity_check(proc_idx, step),
                Op::BeginTx => self.do_begin_tx(proc_idx, step),
                Op::Commit => self.do_commit(proc_idx, step),
                Op::Rollback => self.do_rollback(proc_idx, step),
                Op::CheckpointThenVerify => {
                    self.do_checkpoint(proc_idx, step);
                    self.verify_all_processes(&format!("step {step} (post-checkpoint verify)"));
                }
                Op::InsertConflict => self.do_insert_conflict(proc_idx, step),
                Op::SqlCheckpoint => self.do_sql_checkpoint(proc_idx, step),
                Op::GracefulClose => {
                    self.do_graceful_close(proc_idx, step);
                    // Immediately restart a dead process so the pool
                    // stays populated.
                    let dead_ids: Vec<usize> = self
                        .processes
                        .iter()
                        .filter(|p| !p.alive)
                        .map(|p| p.id)
                        .collect();
                    if !dead_ids.is_empty() {
                        let idx = dead_ids[self.rng.random_range(0..dead_ids.len())];
                        self.do_restart(idx, step);
                    }
                }
                Op::CrashAndRestart => {
                    // Rapid crash + restart of the same process. Stresses
                    // TSHM state transitions and lock takeover paths.
                    self.do_crash(proc_idx, step);
                    self.do_restart(proc_idx, step);
                }
                Op::ConstrainedCheckpoint => self.do_constrained_checkpoint(proc_idx, step),
                Op::CheckpointThenWrite => {
                    self.do_checkpoint_then_write(proc_idx, step);
                }
                Op::CreateIndex => self.do_create_index(proc_idx, step),
                Op::Noop => {}
            }
        }

        // Commit any open transactions before final verification.
        // If COMMIT fails (Busy), fall back to ROLLBACK so the process
        // can participate in final verification.
        for i in 0..self.processes.len() {
            if self.processes[i].alive && self.processes[i].in_transaction {
                self.do_commit(i, self.steps);
                if self.processes[i].in_transaction {
                    self.do_rollback(i, self.steps);
                }
            }
        }

        // Final: verify every alive process sees the exact shadow state.
        self.verify_all_processes("final");
    }

    // --- Shadow verification ---

    /// Verify that a process sees the expected shadow state.
    /// Checks actual row IDs and values — catches data corruption and lost updates.
    fn verify_shadow(
        &self,
        conn: &Arc<Connection>,
        proc_idx: usize,
        context: &str,
        expected: &BTreeMap<String, ShadowTable>,
    ) {
        // Forward check: every shadow table exists in DB with correct rows.
        for (table_name, table) in expected {
            let sql = format!("SELECT id, val FROM {table_name} ORDER BY id");
            let mut stmt = match conn.prepare(&sql) {
                Ok(s) => s,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("no such table") {
                        panic!(
                            "{context}: process {proc_idx} can't find table {table_name} \
                             that exists in shadow state (seed={})",
                            self.seed
                        );
                    }
                    panic!(
                        "{context}: process {proc_idx} failed to prepare SELECT on \
                         {table_name}: {e} (seed={})",
                        self.seed
                    );
                }
            };
            let rows = stmt.run_collect_rows().unwrap_or_else(|e| {
                panic!(
                    "{context}: process {proc_idx} failed to run SELECT on \
                     {table_name}: {e} (seed={})",
                    self.seed
                );
            });
            let actual: Vec<(i64, Option<String>)> = rows
                .iter()
                .map(|r| {
                    let id = match r.first() {
                        Some(Value::Numeric(Numeric::Integer(v))) => *v,
                        other => panic!(
                            "{context}: unexpected id type in {table_name}: {other:?} (seed={})",
                            self.seed
                        ),
                    };
                    let val = match r.get(1) {
                        Some(Value::Text(s)) => Some(s.to_string()),
                        Some(Value::Null) | None => None,
                        other => panic!(
                            "{context}: unexpected val type in {table_name}: {other:?} (seed={})",
                            self.seed
                        ),
                    };
                    (id, val)
                })
                .collect();

            let mut expected: Vec<(i64, Option<String>)> =
                table.rows.iter().map(|r| (r.id, r.val.clone())).collect();
            expected.sort_by_key(|(id, _)| *id);

            if actual != expected {
                panic!(
                    "{context}: process {proc_idx} data mismatch in {table_name}: \
                     actual={actual:?}, expected={expected:?} (seed={})",
                    self.seed
                );
            }
        }

        // Reverse check: DB has no tables that the shadow doesn't know about.
        let sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
        let mut stmt = conn.prepare(sql).unwrap_or_else(|e| {
            panic!(
                "{context}: process {proc_idx} failed to query sqlite_master: {e} (seed={})",
                self.seed
            );
        });
        let rows = stmt.run_collect_rows().unwrap();
        let db_tables: Vec<String> = rows
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Text(s)) => Some(s.to_string()),
                _ => None,
            })
            .collect();
        let shadow_tables: Vec<String> = expected.keys().cloned().collect();
        if db_tables != shadow_tables {
            panic!(
                "{context}: process {proc_idx} table set mismatch: \
                 db={db_tables:?}, shadow={shadow_tables:?} (seed={})",
                self.seed
            );
        }
    }

    /// Verify all alive, non-transacting processes see the committed state.
    /// Uncommitted frames are NOT visible to other processes (no commit marker
    /// on disk), so we verify against `self.shadow.tables` (committed only).
    fn verify_all_processes(&self, context: &str) {
        for p in &self.processes {
            if !p.alive {
                continue;
            }
            // Skip processes in a transaction — they see their own snapshot.
            if p.in_transaction {
                continue;
            }
            self.verify_shadow(p.conn(), p.id, context, &self.shadow.tables);
        }
    }

    /// After a successful write by `writer_idx`, verify:
    /// 1. The writer itself can read back the correct state (page cache consistency).
    /// 2. Another alive, non-transacting process sees the committed state
    ///    (uncommitted frames are NOT visible cross-process — no commit marker).
    fn verify_cross_process_after_write(&self, writer_idx: usize, step: usize) {
        // Self-verify: only for non-transacting writers. Within an explicit
        // transaction, begin_write_tx may rescan the WAL, upgrading the read
        // snapshot to include other processes' commits. The shadow's
        // snapshot_tables (captured at BEGIN) can't track this, so
        // writer_view() may not match reality. Autocommit writes always
        // have an accurate shadow.
        if !self.processes[writer_idx].in_transaction {
            let writer = &self.processes[writer_idx];
            let context = format!("step {step} (self-verify after write by proc {writer_idx})");
            self.verify_shadow(writer.conn(), writer_idx, &context, &self.shadow.tables);
        }

        // Cross-verify: another non-transacting process sees committed state only.
        let verifier = self
            .processes
            .iter()
            .find(|p| p.alive && p.id != writer_idx && !p.in_transaction);
        if let Some(p) = verifier {
            let context = format!("step {step} (cross-verify after write by proc {writer_idx})");
            self.verify_shadow(p.conn(), p.id, &context, &self.shadow.tables);
        }
    }

    /// Returns true if any alive process has an open transaction (BEGIN without
    /// COMMIT/ROLLBACK). Used to skip checkpoints and graceful closes: all
    /// virtual processes share the same OS PID, so TSHM
    /// min_reader_frame_excluding(my_pid) returns None and checkpoints have
    /// no reader constraint. In real multi-process operation, different PIDs
    /// would prevent the checkpoint from backfilling past the reader's slot.
    /// An open transaction holds a persistent read lock, so ANY open tx
    /// (even read-only) must block checkpoints that could truncate the WAL.
    fn any_open_tx(&self) -> bool {
        self.processes.iter().any(|p| p.alive && p.in_transaction)
    }

    // --- Write operation helper ---

    /// Attempt a write operation. Returns Ok(true) if it succeeded,
    /// Ok(false) if it was skipped or failed with an expected error,
    /// or panics if the single-writer invariant is violated.
    fn try_write(
        &mut self,
        proc_idx: usize,
        step: usize,
        op_name: &str,
        sql: &str,
    ) -> Result<bool, ()> {
        let conn = self.processes[proc_idx].conn().clone();
        match conn.execute(sql) {
            Ok(_) => {
                if self.locking_mode == LockingMode::SharedReads {
                    if let Some(owner) = self.writer_owner {
                        assert_eq!(
                            owner, proc_idx,
                            "step {step}: {op_name} succeeded on process {proc_idx}, \
                             but writer_owner is process {owner}! \
                             Two processes wrote simultaneously in SharedReads mode. \
                             (seed={})",
                            self.seed
                        );
                    } else {
                        self.writer_owner = Some(proc_idx);
                    }
                }
                Ok(true)
            }
            Err(LimboError::Busy) | Err(LimboError::BusySnapshot) => Ok(false),
            Err(e) => {
                let msg = e.to_string();
                let msg_lower = msg.to_lowercase();
                if msg_lower.contains("no such table") || msg_lower.contains("no such index") {
                    Ok(false)
                } else {
                    panic!(
                        "step {step}: {op_name} failed on process {proc_idx}: {e} (seed={})",
                        self.seed
                    );
                }
            }
        }
    }

    // --- Operation implementations ---

    fn do_insert(&mut self, proc_idx: usize, step: usize) {
        if self.shadow.tables.is_empty() {
            return;
        }

        let table_names: Vec<String> = self.shadow.tables.keys().cloned().collect();
        let table_name = table_names[self.rng.random_range(0..table_names.len())].clone();
        let id = self.shadow.tables.get(&table_name).unwrap().next_id;

        // 1 in 10 inserts use a large value to exercise overflow pages.
        let large_val = if self.rng.random_range(0..10u32) == 0 {
            let len = self.rng.random_range(2000..6000usize);
            let c = (b'a' + self.rng.random_range(0..26u8)) as char;
            Some(std::iter::repeat_n(c, len).collect::<String>())
        } else {
            None
        };
        let sql = if let Some(ref val) = large_val {
            format!("INSERT INTO {table_name}(id, val) VALUES ({id}, '{val}')")
        } else {
            format!("INSERT INTO {table_name}(id) VALUES ({id})")
        };
        if let Ok(true) = self.try_write(proc_idx, step, "INSERT", &sql) {
            // Always bump next_id globally to avoid PK conflicts, even if
            // a transaction is later rolled back (gaps are harmless).
            self.shadow.tables.get_mut(&table_name).unwrap().next_id += 1;
            let row = ShadowRow { id, val: large_val };

            if self.processes[proc_idx].in_transaction {
                if self.verbose {
                    eprintln!("  -> pending_insert({table_name}, id={id})");
                }
                self.processes[proc_idx]
                    .pending_inserts
                    .push((table_name, row));
            } else {
                if self.verbose {
                    eprintln!("  -> shadow_insert({table_name}, id={id})");
                }
                self.shadow
                    .tables
                    .get_mut(&table_name)
                    .unwrap()
                    .rows
                    .push(row);
                self.verify_cross_process_after_write(proc_idx, step);
            }
        }
    }

    fn do_update(&mut self, proc_idx: usize, step: usize) {
        if self.shadow.tables.is_empty() {
            return;
        }

        // When in a transaction, pick rows from the snapshot (what the process
        // actually sees). Using global shadow would target rows committed after
        // BEGIN, which are invisible to the in-transaction process — the UPDATE
        // would silently affect 0 rows but we'd record it in pending_updates,
        // corrupting the shadow on COMMIT.
        let pick_tables = if self.processes[proc_idx].in_transaction {
            self.processes[proc_idx].snapshot_tables.clone().unwrap()
        } else {
            self.shadow.tables.clone()
        };

        let table_names: Vec<String> = pick_tables.keys().cloned().collect();
        if table_names.is_empty() {
            return;
        }
        let table_name = table_names[self.rng.random_range(0..table_names.len())].clone();
        let table = pick_tables.get(&table_name).unwrap();
        if table.rows.is_empty() {
            return;
        }

        let row_idx = self.rng.random_range(0..table.rows.len());
        let id = table.rows[row_idx].id;
        let new_val = format!("v{}", self.rng.random_range(0..10000u32));

        // When in a transaction, verify the target row still exists in global
        // shadow. If another process deleted it since BEGIN, skip — the shadow
        // model can't accurately track page-level last-write-wins semantics.
        if self.processes[proc_idx].in_transaction {
            let exists_in_global = self
                .shadow
                .tables
                .get(&table_name)
                .is_some_and(|t| t.rows.iter().any(|r| r.id == id));
            if !exists_in_global {
                return;
            }
        }

        let sql = format!("UPDATE {table_name} SET val = '{new_val}' WHERE id = {id}");
        if let Ok(true) = self.try_write(proc_idx, step, "UPDATE", &sql) {
            if self.processes[proc_idx].in_transaction {
                self.processes[proc_idx]
                    .pending_updates
                    .push((table_name, id, new_val));
            } else {
                // Update the shadow val for this row.
                let table = self.shadow.tables.get_mut(&table_name).unwrap();
                if let Some(row) = table.rows.iter_mut().find(|r| r.id == id) {
                    row.val = Some(new_val);
                }
                self.verify_cross_process_after_write(proc_idx, step);
            }
        }
    }

    fn do_delete(&mut self, proc_idx: usize, step: usize) {
        if self.shadow.tables.is_empty() {
            return;
        }

        // When in a transaction, pick rows from the snapshot (same rationale
        // as do_update — avoid targeting rows invisible in the snapshot).
        let pick_tables = if self.processes[proc_idx].in_transaction {
            self.processes[proc_idx].snapshot_tables.clone().unwrap()
        } else {
            self.shadow.tables.clone()
        };

        let table_names: Vec<String> = pick_tables.keys().cloned().collect();
        if table_names.is_empty() {
            return;
        }
        let table_name = table_names[self.rng.random_range(0..table_names.len())].clone();
        let table = pick_tables.get(&table_name).unwrap();
        if table.rows.is_empty() {
            return;
        }

        let row_idx = self.rng.random_range(0..table.rows.len());
        let id = table.rows[row_idx].id;

        // When in a transaction, verify the target row still exists in global
        // shadow (same rationale as do_update).
        if self.processes[proc_idx].in_transaction {
            let exists_in_global = self
                .shadow
                .tables
                .get(&table_name)
                .is_some_and(|t| t.rows.iter().any(|r| r.id == id));
            if !exists_in_global {
                return;
            }
        }

        let sql = format!("DELETE FROM {table_name} WHERE id = {id}");
        if let Ok(true) = self.try_write(proc_idx, step, "DELETE", &sql) {
            if self.processes[proc_idx].in_transaction {
                self.processes[proc_idx]
                    .pending_deletes
                    .push((table_name, id));
            } else {
                let table = self.shadow.tables.get_mut(&table_name).unwrap();
                table.rows.retain(|r| r.id != id);
                self.verify_cross_process_after_write(proc_idx, step);
            }
        }
    }

    fn do_select(&self, proc_idx: usize, step: usize) {
        if self.shadow.tables.is_empty() {
            return;
        }
        // Don't verify processes in a transaction — they see a stale snapshot.
        if self.processes[proc_idx].in_transaction {
            return;
        }
        let context = format!("step {step}");
        self.verify_shadow(
            self.processes[proc_idx].conn(),
            proc_idx,
            &context,
            &self.shadow.tables,
        );
    }

    fn do_checkpoint(&mut self, proc_idx: usize, step: usize) {
        // In SharedReads mode, only the writer should checkpoint.
        if self.locking_mode == LockingMode::SharedReads && self.writer_owner != Some(proc_idx) {
            return;
        }
        // Don't checkpoint while in a transaction.
        if self.processes[proc_idx].in_transaction {
            return;
        }
        // Skip if another process has pending writes (see any_pending_write_tx doc).
        if self.any_open_tx() {
            return;
        }

        let conn = self.processes[proc_idx].conn().clone();
        // Sometimes use upper_bound_inclusive for partial checkpoints.
        let upper_bound = if self.rng.random_range(0..4u32) == 0 {
            Some(self.rng.random_range(1..100u64))
        } else {
            None
        };
        let all_modes = [
            CheckpointMode::Passive {
                upper_bound_inclusive: upper_bound,
            },
            CheckpointMode::Full,
            CheckpointMode::Restart,
            CheckpointMode::Truncate {
                upper_bound_inclusive: upper_bound,
            },
        ];
        let mode = all_modes[self.rng.random_range(0..all_modes.len())];

        let pager = conn.pager.load();
        match pager
            .io
            .block(|| pager.checkpoint(mode, crate::SyncMode::Full, true))
        {
            Ok(_) => {}
            Err(LimboError::Busy) => {}
            Err(e) => {
                panic!(
                    "step {step}: checkpoint({mode:?}) failed unexpectedly on process \
                     {proc_idx}: {e} (seed={})",
                    self.seed
                );
            }
        }

        // Check WAL isn't growing without bound.
        if let Ok(state) = pager.wal_state() {
            self.max_wal_frame_seen = self.max_wal_frame_seen.max(state.max_frame);
            assert!(
                state.max_frame < 10_000,
                "step {step}: WAL grew excessively (max_frame={}, seed={})",
                state.max_frame,
                self.seed
            );
        }
    }

    /// Checkpoint with a synthetic external reader constraint.
    /// Directly sets external_reader_max_frame on the inner WalFile to simulate
    /// another process holding a reader at a specific frame. This exercises
    /// determine_max_safe_checkpoint_frame and try_restart_log_before_write,
    /// which are structurally unreachable in a single-process fuzzer via TSHM
    /// (all virtual processes share the same PID).
    fn do_constrained_checkpoint(&mut self, proc_idx: usize, step: usize) {
        if self.locking_mode == LockingMode::SharedReads && self.writer_owner != Some(proc_idx) {
            return;
        }
        if self.processes[proc_idx].in_transaction {
            return;
        }
        if self.any_open_tx() {
            return;
        }

        let conn = self.processes[proc_idx].conn().clone();
        let pager = conn.pager.load();
        let Some(wal) = pager.wal.as_ref() else {
            return;
        };

        // Get current WAL state to pick a meaningful constraint frame.
        let Ok(wal_state) = pager.wal_state() else {
            return;
        };
        if wal_state.max_frame < 2 {
            return;
        }

        // Downcast to MultiProcessWal to access the inner WalFile.
        #[cfg(debug_assertions)]
        let mp_wal = wal.as_any().downcast_ref::<MultiProcessWal>();
        #[cfg(not(debug_assertions))]
        let mp_wal: Option<&MultiProcessWal> = None;

        let Some(mp_wal) = mp_wal else {
            return;
        };

        // Pick a constraint frame somewhere in the WAL.
        let constraint = self.rng.random_range(1..wal_state.max_frame);
        mp_wal.inner().set_external_reader_max_frame(constraint);

        // Run a Passive checkpoint — it must respect the constraint and not
        // backfill past the external reader's frame.
        let mode = CheckpointMode::Passive {
            upper_bound_inclusive: None,
        };
        match pager
            .io
            .block(|| pager.checkpoint(mode, crate::SyncMode::Full, true))
        {
            Ok(_) => {}
            Err(LimboError::Busy) => {}
            Err(e) => {
                panic!(
                    "step {step}: constrained checkpoint failed on process \
                     {proc_idx}: {e} (seed={})",
                    self.seed
                );
            }
        }

        // Clear the synthetic constraint.
        mp_wal.inner().set_external_reader_max_frame(0);

        // Verify all processes still see correct data.
        self.verify_all_processes(&format!(
            "step {step} (after constrained checkpoint, limit={constraint})"
        ));
    }

    /// Full checkpoint + immediate insert on the same process.
    /// This deterministically exercises try_restart_log_before_write:
    /// checkpoint(Full) makes nbackfills == max_frame, then the next
    /// begin_write_tx calls restart_log() which changes the WAL salt
    /// and resets max_frame to 0. The subsequent insert uses the new salt.
    /// Other processes must detect the salt change via TSHM and rescan.
    fn do_checkpoint_then_write(&mut self, proc_idx: usize, step: usize) {
        if self.locking_mode == LockingMode::SharedReads && self.writer_owner != Some(proc_idx) {
            return;
        }
        if self.processes[proc_idx].in_transaction {
            return;
        }
        if self.any_open_tx() {
            return;
        }
        if self.shadow.tables.is_empty() {
            return;
        }

        // Full checkpoint: backfill everything.
        let conn = self.processes[proc_idx].conn().clone();
        let pager = conn.pager.load();
        match pager
            .io
            .block(|| pager.checkpoint(CheckpointMode::Full, crate::SyncMode::Full, true))
        {
            Ok(_) => {}
            Err(LimboError::Busy) => return,
            Err(e) => {
                panic!(
                    "step {step}: CheckpointThenWrite checkpoint failed on process \
                     {proc_idx}: {e} (seed={})",
                    self.seed
                );
            }
        }

        // Immediately insert — this should trigger try_restart_log_before_write
        // inside begin_write_tx, changing the WAL salt.
        let table_names: Vec<String> = self.shadow.tables.keys().cloned().collect();
        let table_name = table_names[self.rng.random_range(0..table_names.len())].clone();
        let id = self.shadow.tables.get(&table_name).unwrap().next_id;
        let sql = format!("INSERT INTO {table_name}(id) VALUES ({id})");

        if let Ok(true) = self.try_write(proc_idx, step, "INSERT (post-restart)", &sql) {
            if self.verbose {
                eprintln!("  -> shadow_insert_checkpoint_then_write({table_name}, id={id})");
            }
            let table = self.shadow.tables.get_mut(&table_name).unwrap();
            table.next_id += 1;
            table.rows.push(ShadowRow { id, val: None });
            // Cross-verify: other processes must detect the WAL restart
            // and see the new row.
            self.verify_cross_process_after_write(proc_idx, step);
        }
    }

    /// CREATE INDEX on a random existing table. Forces multi-page schema
    /// changes: creates a new B-tree root page and updates sqlite_master,
    /// which stresses partial checkpoint behavior when schema pages are
    /// split across checkpoint boundaries.
    fn do_create_index(&mut self, proc_idx: usize, step: usize) {
        if self.processes[proc_idx].in_transaction {
            return;
        }
        if self.shadow.tables.is_empty() {
            return;
        }

        let table_names: Vec<String> = self.shadow.tables.keys().cloned().collect();
        let table_name = table_names[self.rng.random_range(0..table_names.len())].clone();
        let idx_name = format!("idx_{}", self.shadow.name_counter);
        self.shadow.name_counter += 1;
        let sql = format!("CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}(val)");

        if let Ok(true) = self.try_write(proc_idx, step, "CREATE INDEX", &sql) {
            self.verify_cross_process_after_write(proc_idx, step);
        }
    }

    fn do_create_table(&mut self, proc_idx: usize, step: usize) {
        // Don't do DDL inside a transaction — the shadow model doesn't
        // track pending DDL changes.
        if self.processes[proc_idx].in_transaction {
            return;
        }
        let name = self.shadow.next_name("t");
        let sql = format!("CREATE TABLE {name}(id INTEGER PRIMARY KEY, val TEXT)");
        if let Ok(true) = self.try_write(proc_idx, step, "CREATE TABLE", &sql) {
            self.shadow.tables.insert(
                name,
                ShadowTable {
                    rows: Vec::new(),
                    next_id: 1,
                },
            );
            self.verify_cross_process_after_write(proc_idx, step);
        }
    }

    fn do_drop_table(&mut self, proc_idx: usize, step: usize) {
        if self.shadow.tables.len() <= 1 {
            return;
        }
        // Don't do DDL inside a transaction — the shadow model doesn't
        // track pending DDL changes.
        if self.processes[proc_idx].in_transaction {
            return;
        }
        let table_names: Vec<String> = self.shadow.tables.keys().cloned().collect();
        let name = table_names[self.rng.random_range(0..table_names.len())].clone();
        let sql = format!("DROP TABLE {name}");
        if let Ok(true) = self.try_write(proc_idx, step, "DROP TABLE", &sql) {
            self.shadow.tables.remove(&name);
            self.verify_cross_process_after_write(proc_idx, step);
        }
    }

    /// Insert a duplicate PK to exercise the savepoint rollback path.
    /// The INSERT must fail with a constraint violation, and the database
    /// must remain consistent (shadow state unchanged).
    fn do_insert_conflict(&mut self, proc_idx: usize, step: usize) {
        if self.shadow.tables.is_empty() {
            return;
        }
        // In SharedReads mode, only the writer can attempt writes.
        if self.locking_mode == LockingMode::SharedReads && self.writer_owner != Some(proc_idx) {
            return;
        }
        // Skip conflict testing for in-transaction processes. Within an
        // explicit transaction, begin_write_tx may rescan the WAL and
        // upgrade the read snapshot to include other processes' commits,
        // making the snapshot_tables (captured at BEGIN time) stale.
        // The shadow model can't track this snapshot upgrade, so
        // writer_view() may not match what the process actually sees.
        // Conflict testing for autocommit processes (the common case)
        // is unaffected and still exercises the savepoint rollback path.
        if self.processes[proc_idx].in_transaction {
            return;
        }

        let table_names: Vec<String> = self.shadow.tables.keys().cloned().collect();
        if table_names.is_empty() {
            return;
        }
        let table_name = table_names[self.rng.random_range(0..table_names.len())].clone();
        let table = self.shadow.tables.get(&table_name).unwrap();
        if table.rows.is_empty() {
            return;
        }
        // Pick an existing row's ID to cause a PK conflict.
        let existing_id = table.rows[self.rng.random_range(0..table.rows.len())].id;
        let sql = format!("INSERT INTO {table_name}(id) VALUES ({existing_id})");

        let conn = self.processes[proc_idx].conn().clone();
        match conn.execute(&sql) {
            Err(LimboError::Busy) | Err(LimboError::BusySnapshot) => {
                // Another process holds the write lock — expected in SharedWrites.
            }
            Err(e) => {
                let msg = e.to_string();
                // "no such table" can happen if another process dropped the
                // table after our snapshot was captured and the schema was
                // reloaded during write lock acquisition.
                if !msg.contains("UNIQUE constraint") && !msg.contains("no such table") {
                    panic!(
                        "step {step}: INSERT conflict expected UNIQUE constraint error, \
                         got: {e} (seed={})",
                        self.seed
                    );
                }
            }
            Ok(_) => {
                panic!(
                    "step {step}: INSERT with duplicate PK {existing_id} into {table_name} \
                     should have failed but succeeded (seed={})",
                    self.seed
                );
            }
        }
        // Shadow state must be unchanged — verify the writing process sees correct data.
        if !self.processes[proc_idx].in_transaction {
            self.verify_shadow(
                &conn,
                proc_idx,
                &format!("step {step} (after conflict)"),
                &self.shadow.tables,
            );
        }
    }

    /// Checkpoint via PRAGMA wal_checkpoint(MODE) SQL — exercises the real
    /// user-facing code path through the bytecode interpreter.
    fn do_sql_checkpoint(&mut self, proc_idx: usize, step: usize) {
        // In SharedReads mode, only the writer should checkpoint.
        if self.locking_mode == LockingMode::SharedReads && self.writer_owner != Some(proc_idx) {
            return;
        }
        if self.processes[proc_idx].in_transaction {
            return;
        }
        if self.any_open_tx() {
            return;
        }

        let modes = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"];
        let mode_str = modes[self.rng.random_range(0..modes.len())];
        let sql = format!("PRAGMA wal_checkpoint({mode_str})");

        let conn = self.processes[proc_idx].conn().clone();
        match conn.execute(&sql) {
            Ok(_) => {}
            Err(LimboError::Busy) => {}
            Err(e) => {
                panic!(
                    "step {step}: PRAGMA wal_checkpoint({mode_str}) failed on process \
                     {proc_idx}: {e} (seed={})",
                    self.seed
                );
            }
        }
    }

    /// Graceful close: calls Connection::close() which triggers
    /// checkpoint_shutdown when it's the last connection. This exercises
    /// a different code path from crash (which just drops).
    fn do_graceful_close(&mut self, proc_idx: usize, step: usize) {
        if !self.processes[proc_idx].alive {
            return;
        }
        // Don't close during a transaction — would need rollback first.
        if self.processes[proc_idx].in_transaction {
            return;
        }
        // Don't close while another process has an open transaction.
        // Close triggers checkpoint_shutdown which can truncate the WAL,
        // invalidating other processes' read snapshots. In real multi-process
        // operation, TSHM reader slots prevent this; we simulate that here.
        if self.any_open_tx() {
            return;
        }

        let was_writer_owner = self.writer_owner == Some(proc_idx);
        if was_writer_owner {
            self.writer_owner = None;
        }

        let conn = self.processes[proc_idx].conn.take().unwrap();
        match conn.close() {
            Ok(_) => {}
            Err(e) => {
                // checkpoint_shutdown can fail with Busy if other processes
                // are active — this is expected in multi-process scenarios.
                let msg = e.to_string();
                if !msg.contains("Busy") && !msg.to_lowercase().contains("busy") {
                    panic!(
                        "step {step}: close() failed on process {proc_idx}: {e} (seed={})",
                        self.seed
                    );
                }
            }
        }
        drop(conn);
        self.processes[proc_idx].db = None;
        self.processes[proc_idx].alive = false;

        // Verify remaining alive processes still see correct data.
        self.verify_all_processes(&format!(
            "step {step} (after graceful close of proc {proc_idx})"
        ));
    }

    fn do_crash(&mut self, proc_idx: usize, step: usize) {
        let process = &mut self.processes[proc_idx];
        if !process.alive {
            return;
        }

        tracing::debug!("step {step}: crashing process {proc_idx}");

        let was_writer_owner = self.writer_owner == Some(proc_idx);
        if was_writer_owner {
            self.writer_owner = None;
        }

        // Discard any pending transaction state — crash = implicit rollback.
        self.processes[proc_idx].pending_inserts.clear();
        self.processes[proc_idx].pending_updates.clear();
        self.processes[proc_idx].pending_deletes.clear();
        self.processes[proc_idx].in_transaction = false;
        self.processes[proc_idx].snapshot_tables = None;

        self.processes[proc_idx].conn = None;
        self.processes[proc_idx].db = None;
        self.processes[proc_idx].alive = false;

        // After the writer owner crashes, verify another alive process
        // can take over writing.
        if self.locking_mode == LockingMode::SharedReads && was_writer_owner {
            self.verify_writer_takeover(proc_idx, step);
        }
    }

    fn verify_writer_takeover(&mut self, crashed_proc: usize, step: usize) {
        // Only use a non-transacting process for takeover verification.
        // If the candidate is in a transaction, the INSERT would go into that
        // transaction's uncommitted state, not be committed immediately.
        let candidate = self
            .processes
            .iter()
            .find(|p| p.alive && !p.in_transaction)
            .map(|p| p.id);
        let Some(alive_id) = candidate else {
            // All alive processes are in transactions — can't verify takeover
            // with an autocommit write. The actual takeover will be tested
            // when a fresh process restarts later.
            return;
        };
        let Some(table_name) = self.shadow.tables.keys().next().cloned() else {
            return;
        };
        let id = self.shadow.tables.get(&table_name).unwrap().next_id;
        let sql = format!("INSERT INTO {table_name}(id) VALUES ({id})");

        match self.try_write(alive_id, step, "INSERT (takeover)", &sql) {
            Ok(true) => {
                if self.verbose {
                    eprintln!("  -> shadow_insert_takeover({table_name}, id={id})");
                }
                let table = self.shadow.tables.get_mut(&table_name).unwrap();
                table.next_id += 1;
                table.rows.push(ShadowRow { id, val: None });
            }
            Ok(false) => {
                panic!(
                    "step {step}: writer takeover failed! Process {alive_id} could not write \
                     after writer owner {crashed_proc} crashed. (seed={})",
                    self.seed
                );
            }
            Err(()) => unreachable!(),
        }
    }

    fn do_restart(&mut self, proc_idx: usize, step: usize) {
        if self.processes[proc_idx].alive {
            return;
        }

        tracing::debug!("step {step}: restarting process {proc_idx}");
        let db = open_bypass_registry(&self.db_path, self.locking_mode).unwrap_or_else(|e| {
            panic!(
                "step {step}: failed to restart process {proc_idx}: {e} (seed={})",
                self.seed
            );
        });
        let conn = db.connect().unwrap();

        // A freshly opened process sees committed data only
        // (uncommitted frames have no commit marker on disk).
        self.verify_shadow(
            &conn,
            proc_idx,
            &format!("step {step} (restart)"),
            &self.shadow.tables,
        );

        let process = &mut self.processes[proc_idx];
        process.db = Some(db);
        process.conn = Some(conn);
        process.alive = true;
        process.in_transaction = false;

        if self.locking_mode == LockingMode::SharedReads && self.writer_owner.is_none() {
            self.writer_owner = Some(proc_idx);
        }
    }

    fn do_integrity_check(&self, proc_idx: usize, step: usize) {
        // Skip integrity check during transaction — it may conflict.
        if self.processes[proc_idx].in_transaction {
            return;
        }
        let conn = self.processes[proc_idx].conn();
        let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap_or_else(|e| {
            panic!(
                "step {step}: integrity_check prepare failed on process {proc_idx}: {e} \
                 (seed={})",
                self.seed
            );
        });
        let rows = stmt.run_collect_rows().unwrap_or_else(|e| {
            panic!(
                "step {step}: integrity_check failed on process {proc_idx}: {e} (seed={})",
                self.seed
            );
        });
        match &rows[0][0] {
            Value::Text(s) => {
                if s.as_ref() != "ok" {
                    panic!(
                        "step {step}: integrity_check failed on process {proc_idx} (seed={}): {s}",
                        self.seed
                    );
                }
            }
            other => panic!(
                "step {step}: unexpected integrity result: {other:?} (seed={})",
                self.seed
            ),
        }
    }

    // --- Transaction operations ---

    fn do_begin_tx(&mut self, proc_idx: usize, step: usize) {
        if self.processes[proc_idx].in_transaction {
            return;
        }
        let conn = self.processes[proc_idx].conn().clone();
        match conn.execute("BEGIN") {
            Ok(_) => {
                self.processes[proc_idx].in_transaction = true;
                // Capture the committed state as this process's snapshot.
                self.processes[proc_idx].snapshot_tables = Some(self.shadow.tables.clone());
            }
            Err(LimboError::Busy) => {}
            Err(e) => {
                panic!(
                    "step {step}: BEGIN failed on process {proc_idx}: {e} (seed={})",
                    self.seed
                );
            }
        }
    }

    fn do_commit(&mut self, proc_idx: usize, step: usize) {
        if !self.processes[proc_idx].in_transaction {
            return;
        }
        let conn = self.processes[proc_idx].conn().clone();
        match conn.execute("COMMIT") {
            Ok(_) => {
                // Merge pending changes into global shadow state.
                let pending_inserts = std::mem::take(&mut self.processes[proc_idx].pending_inserts);
                let pending_deletes = std::mem::take(&mut self.processes[proc_idx].pending_deletes);
                let pending_updates = std::mem::take(&mut self.processes[proc_idx].pending_updates);
                self.processes[proc_idx].in_transaction = false;
                self.processes[proc_idx].snapshot_tables = None;

                for (table_name, row) in pending_inserts {
                    if self.verbose {
                        eprintln!("  -> commit_merge_insert({table_name}, id={})", row.id);
                    }
                    if let Some(table) = self.shadow.tables.get_mut(&table_name) {
                        table.rows.push(row);
                    }
                    // If table was dropped during the tx, the insert was
                    // already lost — skip silently.
                }
                for (table_name, id) in pending_deletes {
                    if let Some(table) = self.shadow.tables.get_mut(&table_name) {
                        table.rows.retain(|r| r.id != id);
                    }
                }
                for (table_name, id, new_val) in pending_updates {
                    if let Some(table) = self.shadow.tables.get_mut(&table_name) {
                        if let Some(row) = table.rows.iter_mut().find(|r| r.id == id) {
                            row.val = Some(new_val);
                        }
                    }
                }

                // The committing process is now non-transacting — verify
                // it sees the updated committed state.
                self.verify_cross_process_after_write(proc_idx, step);
            }
            Err(LimboError::Busy) | Err(LimboError::BusySnapshot) => {
                // COMMIT failed due to contention or stale snapshot —
                // transaction stays open, caller may retry or rollback.
            }
            Err(e) => {
                panic!(
                    "step {step}: COMMIT failed on process {proc_idx}: {e} (seed={})",
                    self.seed
                );
            }
        }
    }

    fn do_rollback(&mut self, proc_idx: usize, step: usize) {
        if !self.processes[proc_idx].in_transaction {
            return;
        }
        let conn = self.processes[proc_idx].conn().clone();
        match conn.execute("ROLLBACK") {
            Ok(_) => {
                // Discard all pending changes.
                self.processes[proc_idx].pending_inserts.clear();
                self.processes[proc_idx].pending_updates.clear();
                self.processes[proc_idx].pending_deletes.clear();
                self.processes[proc_idx].in_transaction = false;
                self.processes[proc_idx].snapshot_tables = None;
            }
            Err(e) => {
                panic!(
                    "step {step}: ROLLBACK failed on process {proc_idx}: {e} (seed={})",
                    self.seed
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------

fn pick_seed() -> u64 {
    match std::env::var("SEED").ok().and_then(|s| s.parse().ok()) {
        Some(seed) => seed,
        None => {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        }
    }
}

#[test]
fn test_mp_generative() {
    let seed = pick_seed();
    eprintln!("test_mp_generative: seed={seed}");
    MpSimulator::new(seed, 4, 10_000, LockingMode::SharedWrites).run();
}

#[test]
fn test_mp_generative_shared_reads() {
    let seed = pick_seed();
    eprintln!("test_mp_generative_shared_reads: seed={seed}");
    MpSimulator::new(seed, 4, 10_000, LockingMode::SharedReads).run();
}

/// More processes = more contention on TSHM reader slots, write locks,
/// and page cache. Tests scenarios with higher fan-out.
#[test]
fn test_mp_generative_many_processes() {
    let seed = pick_seed();
    eprintln!("test_mp_generative_many_processes: seed={seed}");
    MpSimulator::new(seed, 8, 10_000, LockingMode::SharedWrites).run();
}
