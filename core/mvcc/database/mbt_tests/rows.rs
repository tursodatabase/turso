//! Driver for traces of `MvccPassiveCheckpoint.qnt`: one table
//! `t(k INTEGER PRIMARY KEY, v INTEGER)`, where spec key `kN` is rowid N and a
//! row value is the spec commit timestamp of its write.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use super::itf::{Itf, Step};
use super::*;

struct Writer {
    conn: Arc<Connection>,
    stop: Arc<StopAt>,
    commit: crate::Statement,
    tx_id: u64,
    logged: bool,
}

enum ReaderState {
    Idle,
    /// The spec reader pinned a WAL mark. The engine pins and begins in one
    /// step, so the engine reader starts at ReaderBegin.
    Pinned {
        mark: i64,
    },
    Active {
        key: i64,
    },
}

struct Reader {
    conn: Arc<Connection>,
    state: ReaderState,
}

pub(super) struct RowDriver {
    db: MvccTestDbNoConn,
    io: Arc<dyn IO>,
    mv: Arc<Store>,
    table_id: MVTableId,
    ck: Checkpointer,
    free_writers: Vec<(Arc<Connection>, Arc<StopAt>)>,
    /// Writers between WritePrepare and WriteFinish, by spec commit timestamp.
    writers: HashMap<i64, Writer>,
    readers: HashMap<String, Reader>,
    /// Engine commit timestamp of each spec commit timestamp. Spec timestamp 0
    /// maps to the durable boundary after setup.
    engine_ts: BTreeMap<i64, u64>,
    /// Engine WAL frame of each spec frame, counted across WAL restarts.
    /// Index 0 is the empty WAL.
    frames: Vec<u64>,
    /// Frames in the WAL generations before each checkpoint sequence.
    frames_before_seq: BTreeMap<u32, u64>,
    /// Checkpoint sequence and max frame of the WAL at the last look.
    last_wal: (u32, u64),
}

impl Driver for RowDriver {
    fn run_step(&mut self, _index: usize, step: &Step) -> StepOutcome {
        match step.action.as_str() {
            "WritePrepare" => self.write_prepare(step)?,
            "WriteLogAppend" => self.write_log_append(step)?,
            "WriteFinish" => self.write_finish(step)?,
            "ReaderPin" => self.reader_pin(step)?,
            "ReaderBegin" => self.reader_begin(step)?,
            "ReaderReadStart" => self.reader_read_start(step)?,
            "ReaderReadEnd" => self.reader_read_end(step)?,
            "ReaderEnd" => self.reader_end(step)?,
            "CheckpointStart" => self.checkpoint_start(step)?,
            "CheckpointCollect" => self.checkpoint_collect(step)?,
            "CheckpointCommit" => self.checkpoint_commit(step)?,
            "CheckpointPublish" => self
                .ck
                .step_until(|state| state == CheckpointState::CheckpointWal),
            "CheckpointBackfill" => self.checkpoint_backfill(step)?,
            "CheckpointTruncateLog" => self
                .ck
                .step_until(|state| state == CheckpointState::TruncateWal),
            "CheckpointStamp" => self.ck.finish(),
            "Gc" => self.gc(step)?,
            other => return refuse(format!("no engine operation for {other}")),
        }
        self.check(step)?;
        if step.action.starts_with("Write") || step.action.starts_with("Checkpoint") {
            self.check_recovery(step)?;
        }
        Ok(())
    }
}

impl RowDriver {
    pub(super) fn new() -> Self {
        let db = MvccTestDbNoConn::new_with_random_db_passive();
        let setup = db.connect();
        setup
            .execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        setup
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        setup.execute("PRAGMA mvcc_gc_threshold = -1").unwrap();
        setup.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let root = get_rows(
            &setup,
            "SELECT rootpage FROM sqlite_schema WHERE name = 't'",
        )[0][0]
            .as_int()
            .unwrap();
        setup.close().unwrap();

        let mv = db.get_mvcc_store();
        let table_id = mv.get_table_id_from_root_page(root);
        let ck = Checkpointer::new(&db, mv.clone());
        let io = ck.io.clone();
        let mut driver = Self {
            db,
            io,
            mv,
            table_id,
            ck,
            free_writers: Vec::new(),
            writers: HashMap::new(),
            readers: HashMap::new(),
            engine_ts: BTreeMap::new(),
            frames: Vec::new(),
            frames_before_seq: BTreeMap::new(),
            last_wal: (0, 0),
        };
        driver
            .engine_ts
            .insert(0, driver.mv.durable_txid_max.load(Ordering::SeqCst));
        let (seq, max_frame) = driver.wal_position();
        driver.frames_before_seq.insert(seq, 0);
        driver.last_wal = (seq, max_frame);
        driver.frames.push(max_frame);
        driver
    }

    fn write_prepare(&mut self, step: &Step) -> StepOutcome {
        let key = spec_key(step.pick("k"));
        let is_delete = step.pick("isDelete").bool();
        let ts = step.var("clock").int();
        let old = latest_in(step.var("writes"), key, ts);
        let (conn, stop) = self
            .free_writers
            .pop()
            .unwrap_or_else(|| connect_with_stop(&self.db));
        conn.execute("BEGIN CONCURRENT").unwrap();
        let sql = if is_delete {
            format!("DELETE FROM t WHERE k = {key}")
        } else if old != 0 {
            format!("UPDATE t SET v = {ts} WHERE k = {key}")
        } else {
            format!("INSERT INTO t VALUES ({key}, {ts})")
        };
        conn.execute(&sql).unwrap();
        let tx_id = conn.get_mv_tx_id().unwrap();
        stop.set(
            CommitYieldPoint::CommitValidation.point(),
            Some(commit_yield_key(tx_id)),
        );
        let mut commit = conn.prepare("COMMIT").unwrap();
        assert!(
            step_statement(&mut commit, &stop, &self.io),
            "COMMIT did not stop after it took its timestamp"
        );
        let end_ts = match self.mv.txs.get(&tx_id).unwrap().value().state.load() {
            TransactionState::Preparing(end_ts) => end_ts,
            other => panic!("a stopped COMMIT must be Preparing, got {other:?}"),
        };
        self.engine_ts.insert(ts, end_ts);
        self.writers.insert(
            ts,
            Writer {
                conn,
                stop,
                commit,
                tx_id,
                logged: false,
            },
        );
        Ok(())
    }

    fn write_log_append(&mut self, step: &Step) -> StepOutcome {
        let ts = step.pick("i").field("ts").int();
        assert!(
            !self.writers.values().any(|w| w.logged),
            "two writers between log append and finish: the engine holds pager_commit_lock, \
             and LogAppendOneAtATime of the spec rules this out"
        );
        let writer = self.writers.get_mut(&ts).unwrap();
        writer.stop.set(
            CommitYieldPoint::LogRecordMarkedWritten.point(),
            Some(commit_yield_key(writer.tx_id)),
        );
        assert!(
            step_statement(&mut writer.commit, &writer.stop, &self.io),
            "COMMIT did not stop after its log record was written"
        );
        writer.logged = true;
        Ok(())
    }

    fn write_finish(&mut self, step: &Step) -> StepOutcome {
        let ts = step.pick("i").field("ts").int();
        let mut writer = self.writers.remove(&ts).unwrap();
        writer.stop.clear();
        assert!(
            !step_statement(&mut writer.commit, &writer.stop, &self.io),
            "COMMIT must finish"
        );
        drop(writer.commit);
        self.free_writers.push((writer.conn, writer.stop));
        Ok(())
    }

    fn reader_pin(&mut self, step: &Step) -> StepOutcome {
        let name = step.pick("r").str().to_string();
        let mark = step.var("rd").get_str(&name).field("mark").int();
        let db = &self.db;
        let reader = self.readers.entry(name).or_insert_with(|| Reader {
            conn: db.connect(),
            state: ReaderState::Idle,
        });
        reader.state = ReaderState::Pinned { mark };
        Ok(())
    }

    fn reader_begin(&mut self, step: &Step) -> StepOutcome {
        let wal_len = step.var("wal").items().len() as i64;
        let reader = self.readers.get_mut(step.pick("r").str()).unwrap();
        let ReaderState::Pinned { mark } = reader.state else {
            panic!("ReaderBegin of a reader that is not pinned");
        };
        if mark != wal_len {
            return refuse("a checkpoint frame came between the pin and the begin of a reader");
        }
        reader.conn.execute("BEGIN").unwrap();
        get_rows(&reader.conn, "SELECT count(*) FROM t");
        reader.state = ReaderState::Active { key: 0 };
        Ok(())
    }

    fn reader_read_start(&mut self, step: &Step) -> StepOutcome {
        let key = spec_key(step.pick("k"));
        let reader = self.readers.get_mut(step.pick("r").str()).unwrap();
        reader.state = ReaderState::Active { key };
        Ok(())
    }

    fn reader_read_end(&mut self, step: &Step) -> StepOutcome {
        let name = step.pick("r").str();
        let reader = &self.readers[name];
        let ReaderState::Active { key } = reader.state else {
            panic!("ReaderReadEnd of a reader that is not active");
        };
        let begin = step.var("rd").get_str(name).field("b").int();
        let want = latest_in(step.var("writes"), key, begin);
        let rows = get_rows(&reader.conn, &format!("SELECT v FROM t WHERE k = {key}"));
        let got = rows.first().map_or(0, |row| row[0].as_int().unwrap());
        if got != want {
            return fail(
                Failure::WrongRead,
                format!(
                    "reader {name} read k{key} at begin {begin}: got {got}, spec oracle {want}"
                ),
            );
        }
        Ok(())
    }

    fn reader_end(&mut self, step: &Step) -> StepOutcome {
        let reader = self.readers.get_mut(step.pick("r").str()).unwrap();
        if let ReaderState::Active { .. } = reader.state {
            reader.conn.execute("ROLLBACK").unwrap();
        }
        reader.state = ReaderState::Idle;
        Ok(())
    }

    fn checkpoint_start(&mut self, step: &Step) -> StepOutcome {
        self.ck.start(1);
        let spec_snap = step.var("ck").field("snap").int();
        let snap = self.ck.sm().snapshot_ts_for_test();
        if let Some(commit) = self.commit_on_other_side(spec_snap, snap) {
            return fail(
                Failure::Snapshot,
                format!("spec snapshot {spec_snap}, engine {snap}: {commit}"),
            );
        }
        Ok(())
    }

    /// Visits the dirty keys of the engine up to the spec key, one per step.
    /// The engine visits only dirty keys, so a spec key that is not dirty is
    /// a no-op.
    fn checkpoint_collect(&mut self, step: &Step) -> StepOutcome {
        self.collect_up_to(spec_key(step.pick("k")));
        self.check_collected(step)
    }

    fn collect_up_to(&mut self, key: i64) {
        let target = self.row_id(key);
        let table_start = self.row_id(i64::MIN);
        loop {
            let ck = self.ck.sm();
            if ck.state_for_test() != CheckpointState::CollectTableRows {
                return;
            }
            let lower = match ck.last_collected_table_key_for_test() {
                Some(last) if last.table_id == self.table_id => Bound::Excluded(last.clone()),
                _ => Bound::Included(table_start.clone()),
            };
            let dirty_up_to_target = self
                .mv
                .checkpoint_dirty_table_keys
                .range((lower, Bound::Included(target.clone())))
                .next()
                .is_some();
            if !dirty_up_to_target {
                return;
            }
            let before = ck.last_collected_table_key_for_test().cloned();
            let ck = self.ck.sm.as_mut().unwrap();
            match ck.step(&()).unwrap() {
                TransitionResult::Io(io) => io.wait(self.io.as_ref()).unwrap(),
                TransitionResult::Continue => {}
                TransitionResult::Done(_) => panic!("checkpoint finished during collection"),
            }
            if ck.state_for_test() != CheckpointState::CollectTableRows {
                return;
            }
            let after = ck.last_collected_table_key_for_test().cloned();
            assert_ne!(before, after, "a collect step must visit a key");
            assert!(
                after.as_ref().is_some_and(|key| *key <= target),
                "the checkpoint visited {after:?}, after the spec key {target:?}"
            );
        }
    }

    /// The engine write set equals the spec `ck.wset` on the keys that the
    /// spec visited. A spec value of 0 is a delete, which the engine leaves
    /// out when the row was never in the B-tree.
    fn check_collected(&self, step: &Step) -> StepOutcome {
        let ck = step.var("ck");
        let todo: Vec<i64> = ck.field("todo").items().iter().map(spec_key).collect();
        let spec: BTreeMap<i64, bool> = ck
            .field("wset")
            .entries()
            .iter()
            .map(|(key, value)| (spec_key(key), value.int() == 0))
            .collect();
        let engine: BTreeMap<i64, bool> = self
            .ck
            .sm()
            .collected_table_rows_for_test()
            .into_iter()
            .filter(|(row_id, _)| row_id.table_id == self.table_id)
            .map(|(row_id, is_delete)| (row_id.row_id.to_int_or_panic(), is_delete))
            .collect();
        for key in spec.keys().chain(engine.keys()) {
            if todo.contains(key) {
                continue;
            }
            let same = match (spec.get(key), engine.get(key)) {
                (Some(spec_delete), Some(engine_delete)) => spec_delete == engine_delete,
                (Some(true), None) => true,
                _ => false,
            };
            if !same {
                return fail(
                    Failure::Collect,
                    format!(
                        "k{key}: spec deletes {:?}, engine deletes {:?}",
                        spec.get(key),
                        engine.get(key)
                    ),
                );
            }
        }
        Ok(())
    }

    fn checkpoint_commit(&mut self, step: &Step) -> StepOutcome {
        let spec_wrote = step.var("ck").field("frame").int() != 0;
        let wrote = self.ck.commit();
        if wrote != spec_wrote {
            return fail(
                Failure::Frame,
                format!("spec wrote a frame: {spec_wrote}, engine: {wrote}"),
            );
        }
        if wrote {
            let max_frame = self.max_frame();
            self.frames.push(max_frame);
        }
        Ok(())
    }

    fn checkpoint_backfill(&mut self, step: &Step) -> StepOutcome {
        let wal_len = step.var("wal").items().len() as i64;
        let pinned_below = self
            .readers
            .values()
            .any(|reader| matches!(reader.state, ReaderState::Pinned { mark } if mark < wal_len));
        if pinned_below {
            return refuse("a backfill ran while a reader pinned a mark but did not begin");
        }
        self.ck
            .step_until(|state| state == CheckpointState::TruncateLogicalLog);
        Ok(())
    }

    /// One pass of gc_incremental over the chain of one key.
    fn gc(&mut self, step: &Step) -> StepOutcome {
        let row_id = self.row_id(spec_key(step.pick("k")));
        let lwm = self.mv.sample_gc_lwm();
        let ckpt_max = self.mv.durable_txid_max.load(Ordering::SeqCst);
        let floor = self
            .mv
            .compute_min_reader_mark()
            .min(*self.mv.backfill_floor.read());
        if self.mv.rootpage_gc_protected(&row_id.table_id, floor) {
            return Ok(());
        }
        if let Some(entry) = self.mv.rows.get(&row_id) {
            let mut versions = entry.value().write();
            self.mv
                .gc_chain_now(&mut versions, lwm, ckpt_max, floor, true);
        }
        Ok(())
    }

    fn check(&mut self, step: &Step) -> StepOutcome {
        let spec_durable = step.var("durable").int();
        let durable = self.mv.durable_txid_max.load(Ordering::SeqCst);
        if let Some(commit) = self.commit_on_other_side(spec_durable, durable) {
            return fail(
                Failure::Durable,
                format!("spec durable {spec_durable}, engine {durable}: {commit}"),
            );
        }

        let wal_len = step.var("wal").items().len();
        let max_frame = self.max_frame();
        if self.frames.get(wal_len) != Some(&max_frame) {
            return fail(
                Failure::Frame,
                format!(
                    "spec has {wal_len} frames, engine max frame {max_frame}, frames {:?}",
                    self.frames
                ),
            );
        }
        let backfilled = {
            let pager = self.ck.conn.pager.load();
            self.counted_frame(self.last_wal.0, pager.wal_backfill_frame().unwrap())
        };
        let floor = *self.mv.backfill_floor.read();
        let floor = self.counted_frame(floor.checkpoint_seq, floor.frame);
        for (name, spec, engine) in [
            ("nBackfill", step.var("nBackfill").int(), backfilled),
            ("backfillFloor", step.var("backfillFloor").int(), floor),
        ] {
            if self.frames.get(spec as usize) != Some(&engine) {
                return fail(
                    Failure::Backfill,
                    format!(
                        "spec {name} {spec}, engine frame {engine}, frames {:?}",
                        self.frames
                    ),
                );
            }
        }

        let spec_log_empty = step.var("llog").items().is_empty();
        let log_empty = self.mv.storage.logical_log_offset() == 0;
        if spec_log_empty != log_empty {
            return fail(
                Failure::LogicalLog,
                format!("spec log empty: {spec_log_empty}, engine log empty: {log_empty}"),
            );
        }
        Ok(())
    }

    /// RecoveryCorrect: a crash now recovers the latest logged value of
    /// every key.
    fn check_recovery(&self, step: &Step) -> StepOutcome {
        let logged = step.var("logged");
        let mut want: BTreeMap<i64, i64> = BTreeMap::new();
        for write in logged.items() {
            want.entry(spec_key(write.field("k"))).or_insert(0);
        }
        let want: BTreeMap<i64, i64> = want
            .into_keys()
            .map(|key| (key, latest_in(logged, key, i64::MAX)))
            .filter(|(_, value)| *value != 0)
            .collect();
        let path = self.db.path.as_ref().unwrap();
        let got = match crash_probe(path, |conn| query_rows(conn, "SELECT k, v FROM t")) {
            Recovery::Read(rows) => rows
                .iter()
                .map(|row| (row[0].as_int().unwrap(), row[1].as_int().unwrap()))
                .collect::<BTreeMap<_, _>>(),
            Recovery::Error(err) => return fail(Failure::RecoveryError, err),
            Recovery::Panic(msg) => return fail(Failure::RecoveryPanic, msg),
        };
        if got != want {
            return fail(
                Failure::RecoveryRows,
                format!("recovered {got:?}, spec logged {want:?}"),
            );
        }
        Ok(())
    }

    /// A spec commit that is at or below `spec_ts` while its engine commit
    /// is above `engine_ts`, or the other way. Engine and spec timestamps
    /// differ, so the test compares only their order with the commits.
    fn commit_on_other_side(&self, spec_ts: i64, engine_ts: u64) -> Option<String> {
        self.engine_ts
            .iter()
            .find(|(&spec, &engine)| (spec <= spec_ts) != (engine <= engine_ts))
            .map(|(spec, engine)| format!("commit {spec} (engine {engine}) is on the other side"))
    }

    /// The max frame of the WAL, counted across WAL restarts. A restart
    /// starts a new checkpoint sequence after every frame was backfilled.
    fn max_frame(&mut self) -> u64 {
        let (seq, max_frame) = self.wal_position();
        let (last_seq, last_max) = self.last_wal;
        if seq != last_seq {
            assert!(seq > last_seq, "the WAL checkpoint sequence went back");
            let before = self.frames_before_seq[&last_seq] + last_max;
            self.frames_before_seq.insert(seq, before);
        }
        self.last_wal = (seq, max_frame);
        self.counted_frame(seq, max_frame)
    }

    fn counted_frame(&self, seq: u32, frame: u64) -> u64 {
        self.frames_before_seq[&seq] + frame
    }

    fn wal_position(&self) -> (u32, u64) {
        let pager = self.ck.conn.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        (wal.get_checkpoint_seq(), wal.get_max_frame_in_wal())
    }

    fn row_id(&self, key: i64) -> RowID {
        RowID::new(self.table_id, RowKey::Int(key))
    }
}

/// Spec key `kN` is rowid N.
fn spec_key(key: &Itf) -> i64 {
    key.str()
        .strip_prefix('k')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("spec key {key:?} is not kN"))
}

/// The value of key `key` after the writes with a timestamp below `ts`, as
/// LatestIn of the spec. 0 means no row.
fn latest_in(writes: &Itf, key: i64, ts: i64) -> i64 {
    writes
        .items()
        .iter()
        .filter(|w| spec_key(w.field("k")) == key && w.field("ts").int() < ts)
        .max_by_key(|w| w.field("ts").int())
        .map_or(0, |w| w.field("v").int())
}

#[test]
fn hand_written_row_trace_conforms() {
    let mut driver = RowDriver::new();
    let replayed = replay(
        "rows-hand",
        include_str!("traces/rows-hand.itf.json"),
        Expect::Conforms,
        &mut driver,
    );
    assert_eq!(replayed, Replay::Done);
}

/// Traces of the mbt module from `make traces`. rows-196 is refused: a
/// backfill runs between the pin and the begin of a reader.
const GENERATED_ROW_TRACES: [(&str, &str); 7] = [
    ("rows-23", include_str!("traces/rows-23.itf.json")),
    ("rows-66", include_str!("traces/rows-66.itf.json")),
    ("rows-83", include_str!("traces/rows-83.itf.json")),
    ("rows-102", include_str!("traces/rows-102.itf.json")),
    ("rows-104", include_str!("traces/rows-104.itf.json")),
    ("rows-186", include_str!("traces/rows-186.itf.json")),
    ("rows-196", include_str!("traces/rows-196.itf.json")),
];

#[test]
fn generated_row_traces_conform() {
    let mut replayed = 0;
    for (name, trace) in GENERATED_ROW_TRACES {
        if replay(name, trace, Expect::Conforms, &mut RowDriver::new()) == Replay::Done {
            replayed += 1;
        }
    }
    assert!(
        replayed >= 6,
        "only {replayed} traces replayed: a change to the driver or the engine refuses more traces"
    );
}

/// Replays every trace in the folder `MBT_TRACES`, for new traces from
/// `make traces`. Run it with `cargo test mbt_tests -- --ignored`.
#[test]
#[ignore]
fn row_traces_from_folder() {
    let folder = std::env::var("MBT_TRACES").expect("set MBT_TRACES to a folder of traces");
    let mut paths: Vec<_> = std::fs::read_dir(folder)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.to_string_lossy().ends_with(".itf.json"))
        .collect();
    paths.sort();
    let (mut refused, mut failed) = (0, 0);
    for path in &paths {
        let trace = std::fs::read_to_string(path).unwrap();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let result = std::panic::catch_unwind(|| {
            replay(&name, &trace, Expect::Conforms, &mut RowDriver::new())
        });
        match result {
            Ok(Replay::Done) => {}
            Ok(Replay::Refused) => refused += 1,
            Err(_) => failed += 1,
        }
    }
    println!(
        "{} traces: {} replayed, {refused} refused, {failed} failed",
        paths.len(),
        paths.len() - refused - failed
    );
    assert_eq!(failed, 0);
}
