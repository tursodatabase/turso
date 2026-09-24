use crate::io::{File, FileId, FileSyncType};
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::{
    Buffer, Clock, Completion, Database, MemoryIO, MonotonicInstant, OpenFlags, Result,
    SqliteDialect, Statement, StepResult, SyncMode, WallClockInstant, IO,
};
use std::sync::Arc;

#[test]
fn full_waiter_is_synced_when_group_leader_uses_sync_off() {
    let (_db, io, [mut leader, mut waiter]) =
        prepare_group_commit("group-commit-sync-off-leader.db", SyncMode::Off);
    let before = io.syncs.load(Ordering::Acquire);
    step_until_done(&mut leader);
    step_until_done(&mut waiter);
    assert!(
        io.syncs.load(Ordering::Acquire) > before,
        "FULL waiter returned success with zero sync calls"
    );
}

fn prepare_group_commit(
    path: &str,
    leader_sync: SyncMode,
) -> (Arc<Database>, Arc<SyncCountingIo>, [Statement; 2]) {
    let io = Arc::new(SyncCountingIo::default());
    let db = Database::open_file(io.clone(), path, Arc::new(SqliteDialect)).unwrap();
    let leader = db.connect().unwrap();
    leader.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    leader
        .execute("CREATE TABLE t(pk INTEGER PRIMARY KEY)")
        .unwrap();
    leader
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    let waiter = db.connect().unwrap();
    leader.set_sync_mode(leader_sync);
    waiter.set_sync_mode(SyncMode::Full);
    for (conn, pk) in [(&leader, 1), (&waiter, 2)] {
        conn.execute("BEGIN CONCURRENT").unwrap();
        conn.execute(format!("INSERT INTO t VALUES ({pk})"))
            .unwrap();
    }
    let store = db.get_mv_store().clone().unwrap();
    let coordinator = &store.commit_coordinator;
    assert!(coordinator.pager_commit_lock.write());
    let mut commits = [
        leader.prepare("COMMIT").unwrap(),
        waiter.prepare("COMMIT").unwrap(),
    ];
    for commit in &mut commits {
        assert!(matches!(commit.step().unwrap(), StepResult::IO));
    }
    coordinator.unlock_pager_commit_lock();
    (db, io, commits)
}

fn step_until_done(stmt: &mut Statement) {
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::Done => return,
            StepResult::IO | StepResult::Yield => continue,
            other => panic!("COMMIT ended with {other:?}"),
        }
    }
    panic!("statement never finished")
}

#[derive(Default)]
struct SyncCountingIo {
    inner: MemoryIO,
    syncs: Arc<AtomicUsize>,
}

impl Clock for SyncCountingIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for SyncCountingIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        Ok(Arc::new(SyncCountingFile {
            inner: self.inner.open_file(path, flags, direct)?,
            syncs: self.syncs.clone(),
        }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}

struct SyncCountingFile {
    inner: Arc<dyn File>,
    syncs: Arc<AtomicUsize>,
}

impl File for SyncCountingFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.syncs.fetch_add(1, Ordering::AcqRel);
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}
