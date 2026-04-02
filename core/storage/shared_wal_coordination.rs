//! Cross-process shared WAL coordination backed by the `.tshm` file.
//!
//! The mmap stores three kinds of state:
//!
//! 1. An authoritative WAL snapshot header (`max_frame`, checksums, salts,
//!    checkpoint counters).
//! 2. Cross-process ownership state for the single writer, the single
//!    checkpointer, and every active reader slot.
//! 3. A shared page-to-frame index so readers can resolve WAL pages without a
//!    process-local WAL scan.
//!
//! The design intentionally splits responsibilities between shared memory and
//! process-local bookkeeping:
//!
//! - Shared memory is the source of truth across processes.
//! - Process-local registries prevent same-process re-opens from reclaiming or
//!   double-using slots that are still owned by sibling connections.
//! - The shared frame index is append-only within a WAL generation and is only
//!   published after each entry is fully written, so other processes never
//!   observe half-written mappings.
//!
//! Stale-owner reclamation is best-effort and must only trade performance for
//! conservatism, never correctness: if the authority cannot prove a slot is
//! dead, it must leave that slot in place.

use crate::io::{
    Completion, File, FileSyncType, OpenFlags, SharedWalLockKind, SharedWalMappedRegion, IO,
};
use crate::storage::slot_bitmap::AtomicSlotBitmap;
use crate::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use crate::{turso_assert, CompletionError, LimboError, Result};
use std::collections::HashMap;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::{Arc, LazyLock, Mutex, RwLock};

const SHARED_WAL_COORDINATION_MAGIC: [u8; 8] = *b"TSHMWAL\0";
const SHARED_WAL_COORDINATION_VERSION: u32 = 9;
const SHARED_WAL_BACKFILL_PROOF_VERSION: u32 = 1;
const UNUSED_READER_FRAME: u64 = u64::MAX;
const UNOWNED_LOCK: u64 = 0;
const SHARED_WAL_COORDINATION_MAP_ALIGNMENT: usize = 4096;
const PROCESS_LIFETIME_LOCK_OFFSET: u64 = 0;
const WRITER_LOCK_OFFSET: u64 = 1;
const CHECKPOINT_LOCK_OFFSET: u64 = 2;
const READER_LOCK_START_OFFSET: u64 = 3;
const FRAME_INDEX_BLOCK_CAPACITY: u32 = 4096;
const FRAME_INDEX_BLOCK_HASH_SLOTS: u32 = FRAME_INDEX_BLOCK_CAPACITY * 2;
const MAX_FRAME_INDEX_BLOCKS: u32 = 64;
const INITIAL_FRAME_INDEX_BLOCKS: u32 = 1;
const MAX_FRAME_INDEX_CAPACITY: u32 = FRAME_INDEX_BLOCK_CAPACITY * MAX_FRAME_INDEX_BLOCKS;

/// Monotonic counter for generating unique per-connection instance IDs within
/// a process. Combined with the PID to form a `SharedOwnerRecord`.
static NEXT_SHARED_OWNER_INSTANCE_ID: AtomicU32 = AtomicU32::new(1);

/// Global registry of open tshm mappings, keyed by canonical file path.
/// Defensive dedup registry for tshm mappings within a single process.
///
/// In production, `DATABASE_MANAGER` already ensures one `Database` (and
/// therefore one `Arc<MappedSharedWalCoordination>`) per file per process,
/// so `open_count` is always 1. This registry exists as a safety net for
/// test code that may bypass `DATABASE_MANAGER` and open the same file
/// multiple times. The shared Arcs it provides (`frame_index_publish_lock`,
/// `process_local_ownership`) ensure those test-only re-opens still
/// coordinate correctly within the process.
static PROCESS_LOCAL_COORDINATION_OPENS: LazyLock<
    Mutex<HashMap<PathBuf, ProcessLocalCoordinationEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
struct ProcessLocalCoordinationEntry {
    open_count: usize,
    frame_index_publish_lock: Arc<Mutex<()>>,
    ownership: Arc<Mutex<ProcessLocalOwnershipState>>,
}

/// Process-private mirror of which locks THIS process currently holds.
///
/// Multiple `Connection`s to the same `Database` share a single
/// `MappedSharedWalCoordination` (and therefore a single `LocalLockState`).
/// The per-slot counts (not booleans) track how many connections within this
/// process hold each slot, so `register_reader` can skip slots already
/// occupied by a sibling connection, and `min_active_reader_frame` can
/// recognize "our own slot" without probing the cross-process lock.
#[derive(Debug)]
struct LocalLockState {
    writer_lock_held: bool,
    checkpoint_lock_held: bool,
    reader_locks: Vec<usize>,
}

/// Per-connection ownership bookkeeping within a single process.
///
/// Multiple connections share the same `MappedSharedWalCoordination` and the
/// same `SharedOwnerRecord`, so the shared-memory owner field alone cannot
/// distinguish which connection holds a slot. This struct fills that gap.
///
/// On non-OFD platforms (macOS) it is also the only way to distinguish
/// "same-process sibling connection" from "another process" during stale
/// reclamation, because POSIX fcntl locks are per-process, not per-fd.
#[derive(Debug)]
struct ProcessLocalOwnershipState {
    writer_owner: Option<SharedOwnerRecord>,
    checkpoint_owner: Option<SharedOwnerRecord>,
    reader_owners: Vec<Option<SharedOwnerRecord>>,
    shared_snapshot_readers: HashMap<u64, SharedReadMarkRegistration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SharedReadMarkRegistration {
    slot: SharedReaderSlot,
    ref_count: usize,
}

impl ProcessLocalOwnershipState {
    fn new(reader_slot_count: u32) -> Self {
        Self {
            writer_owner: None,
            checkpoint_owner: None,
            reader_owners: vec![None; reader_slot_count as usize],
            shared_snapshot_readers: HashMap::default(),
        }
    }

    fn try_acquire_writer(&mut self, owner: SharedOwnerRecord) -> bool {
        if self.writer_owner.is_some() {
            return false;
        }
        self.writer_owner = Some(owner);
        true
    }

    fn release_writer(&mut self, owner: SharedOwnerRecord) {
        turso_assert!(
            self.writer_owner == Some(owner),
            "process-local writer released by non-owner"
        );
        self.writer_owner = None;
    }

    fn writer_active(&self) -> bool {
        self.writer_owner.is_some()
    }

    fn try_acquire_checkpoint(&mut self, owner: SharedOwnerRecord) -> bool {
        if self.checkpoint_owner.is_some() {
            return false;
        }
        self.checkpoint_owner = Some(owner);
        true
    }

    fn release_checkpoint(&mut self, owner: SharedOwnerRecord) {
        turso_assert!(
            self.checkpoint_owner == Some(owner),
            "process-local checkpoint released by non-owner"
        );
        self.checkpoint_owner = None;
    }

    fn checkpoint_active(&self) -> bool {
        self.checkpoint_owner.is_some()
    }

    fn try_register_reader(&mut self, slot_index: u32, owner: SharedOwnerRecord) -> bool {
        let slot = &mut self.reader_owners[slot_index as usize];
        if slot.is_some() {
            return false;
        }
        *slot = Some(owner);
        true
    }

    fn unregister_reader(&mut self, slot_index: u32, owner: SharedOwnerRecord) {
        let slot = &mut self.reader_owners[slot_index as usize];
        turso_assert!(
            *slot == Some(owner),
            "process-local reader slot released by non-owner"
        );
        *slot = None;
    }

    fn reader_owner(&self, slot_index: u32) -> Option<SharedOwnerRecord> {
        self.reader_owners[slot_index as usize]
    }

    fn shared_snapshot_reader(&self, max_frame: u64) -> Option<SharedReadMarkRegistration> {
        self.shared_snapshot_readers.get(&max_frame).copied()
    }

    fn retain_shared_snapshot_reader(&mut self, max_frame: u64) -> Option<SharedReaderSlot> {
        let registration = self.shared_snapshot_readers.get_mut(&max_frame)?;
        registration.ref_count += 1;
        Some(registration.slot)
    }

    fn publish_shared_snapshot_reader(&mut self, slot: SharedReaderSlot) {
        let previous = self.shared_snapshot_readers.insert(
            slot.max_frame,
            SharedReadMarkRegistration { slot, ref_count: 1 },
        );
        turso_assert!(
            previous.is_none(),
            "process-local shared snapshot publication replaced a live registration",
            { "max_frame": slot.max_frame }
        );
    }

    fn release_shared_snapshot_reader(&mut self, slot: SharedReaderSlot) -> bool {
        let registration = self
            .shared_snapshot_readers
            .get_mut(&slot.max_frame)
            .expect("shared snapshot registration missing for release");
        turso_assert!(
            registration.slot == slot,
            "shared snapshot released with mismatched reader slot",
            {
                "published_slot_index": registration.slot.slot_index,
                "released_slot_index": slot.slot_index,
                "max_frame": slot.max_frame
            }
        );
        turso_assert!(
            registration.ref_count > 0,
            "shared snapshot refcount underflow"
        );
        registration.ref_count -= 1;
        if registration.ref_count != 0 {
            return false;
        }
        self.shared_snapshot_readers.remove(&slot.max_frame);
        true
    }
}

/// Packed (PID, instance_id) pair identifying a specific connection across
/// processes. Stored in shared memory owner slots so that stale-owner
/// reclamation can `kill(pid, 0)` to check liveness on non-OFD platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SharedOwnerRecord(u64);

impl SharedOwnerRecord {
    pub(crate) const UNOWNED: Self = Self(UNOWNED_LOCK);

    pub(crate) const fn from_raw(raw: u64) -> Option<Self> {
        if raw == UNOWNED_LOCK {
            None
        } else {
            Some(Self(raw))
        }
    }

    pub(crate) fn current_process(instance_id: u32) -> Self {
        Self::new(std::process::id(), instance_id)
    }

    pub(crate) fn new(pid: u32, instance_id: u32) -> Self {
        let raw = ((pid as u64) << 32) | instance_id as u64;
        turso_assert!(raw != UNOWNED_LOCK, "shared owner record must be non-zero");
        Self(raw)
    }

    pub(crate) const fn raw(self) -> u64 {
        self.0
    }

    pub(crate) const fn pid(self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub(crate) const fn instance_id(self) -> u32 {
        self.0 as u32
    }
}

fn next_shared_owner_instance_id() -> u32 {
    loop {
        let current = NEXT_SHARED_OWNER_INSTANCE_ID.load(Ordering::Relaxed);
        let next = if current == u32::MAX { 1 } else { current + 1 };
        if NEXT_SHARED_OWNER_INSTANCE_ID
            .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return current;
        }
    }
}

/// Check whether a process is still running via `kill(pid, 0)`.
/// Signal 0 is a no-op probe: the kernel checks permissions and existence
/// without delivering a signal. Returns true if the process exists (or we
/// lack permission to signal it — EPERM means it's alive but owned by
/// another user). False-negatives are impossible; false-positives can occur
/// if the PID has been recycled by an unrelated process.
fn pid_is_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    let rc = unsafe { libc::kill(pid as i32, 0) };
    if rc == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Serializable snapshot of the authoritative shared WAL coordination state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SharedWalCoordinationHeader {
    pub max_frame: u64,
    pub nbackfills: u64,
    pub transaction_count: u64,
    pub visibility_generation: u64,
    pub checkpoint_seq: u32,
    pub checkpoint_epoch: u32,
    pub page_size: u32,
    pub salt_1: u32,
    pub salt_2: u32,
    pub checksum_1: u32,
    pub checksum_2: u32,
    pub reader_slot_count: u32,
}

impl SharedWalCoordinationHeader {
    pub(crate) const BYTE_LEN: usize = 76;

    pub(crate) fn encode(self) -> [u8; Self::BYTE_LEN] {
        let mut bytes = [0u8; Self::BYTE_LEN];
        bytes[0..8].copy_from_slice(&SHARED_WAL_COORDINATION_MAGIC);
        bytes[8..12].copy_from_slice(&SHARED_WAL_COORDINATION_VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.reader_slot_count.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.max_frame.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.nbackfills.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.transaction_count.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.visibility_generation.to_le_bytes());
        bytes[48..52].copy_from_slice(&self.checkpoint_seq.to_le_bytes());
        bytes[52..56].copy_from_slice(&self.checkpoint_epoch.to_le_bytes());
        bytes[56..60].copy_from_slice(&self.page_size.to_le_bytes());
        bytes[60..64].copy_from_slice(&self.salt_1.to_le_bytes());
        bytes[64..68].copy_from_slice(&self.salt_2.to_le_bytes());
        bytes[68..72].copy_from_slice(&self.checksum_1.to_le_bytes());
        bytes[72..76].copy_from_slice(&self.checksum_2.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::BYTE_LEN {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination header must be {} bytes, got {}",
                Self::BYTE_LEN,
                bytes.len()
            )));
        }
        if bytes[0..8] != SHARED_WAL_COORDINATION_MAGIC {
            return Err(LimboError::Corrupt(
                "shared WAL coordination header magic mismatch".into(),
            ));
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != SHARED_WAL_COORDINATION_VERSION {
            return Err(LimboError::Corrupt(format!(
                "unsupported shared WAL coordination header version: {version}"
            )));
        }
        Ok(Self {
            reader_slot_count: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            max_frame: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            nbackfills: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            transaction_count: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            visibility_generation: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            checkpoint_seq: u32::from_le_bytes(bytes[48..52].try_into().unwrap()),
            checkpoint_epoch: u32::from_le_bytes(bytes[52..56].try_into().unwrap()),
            page_size: u32::from_le_bytes(bytes[56..60].try_into().unwrap()),
            salt_1: u32::from_le_bytes(bytes[60..64].try_into().unwrap()),
            salt_2: u32::from_le_bytes(bytes[64..68].try_into().unwrap()),
            checksum_1: u32::from_le_bytes(bytes[68..72].try_into().unwrap()),
            checksum_2: u32::from_le_bytes(bytes[72..76].try_into().unwrap()),
        })
    }
}

/// A registered reader slot in the shared coordination table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SharedReaderSlot {
    pub slot_index: u32,
    pub max_frame: u64,
    pub owner: SharedOwnerRecord,
}

/// Process-local atomic counters and slot bitmaps for WAL coordination.
///
/// Holds WAL snapshot metadata, reader/writer ownership
pub(crate) struct SharedWalCoordinationState {
    max_frame: AtomicU64,
    nbackfills: AtomicU64,
    transaction_count: AtomicU64,
    visibility_generation: AtomicU64,
    checkpoint_seq: AtomicU32,
    checkpoint_epoch: AtomicU32,
    page_size: AtomicU32,
    salt_1: AtomicU32,
    salt_2: AtomicU32,
    checksum_1: AtomicU32,
    checksum_2: AtomicU32,
    writer_owner: AtomicU64,
    checkpoint_owner: AtomicU64,
    reader_frames: Box<[AtomicU64]>,
    reader_owners: Box<[AtomicU64]>,
    reader_slots: AtomicSlotBitmap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SharedWalBackfillProof {
    nbackfills: u64,
    max_frame: u64,
    checkpoint_seq: u32,
    page_size: u32,
    salt_1: u32,
    salt_2: u32,
    checksum_1: u32,
    checksum_2: u32,
    db_size_pages: u32,
    db_header_crc32c: u32,
}

impl SharedWalBackfillProof {
    const CRC_INPUT_LEN: usize = 52;

    fn from_snapshot_and_db(
        snapshot: SharedWalCoordinationHeader,
        db_size_pages: u32,
        db_header_crc32c: u32,
    ) -> Self {
        Self {
            nbackfills: snapshot.nbackfills,
            max_frame: snapshot.max_frame,
            checkpoint_seq: snapshot.checkpoint_seq,
            page_size: snapshot.page_size,
            salt_1: snapshot.salt_1,
            salt_2: snapshot.salt_2,
            checksum_1: snapshot.checksum_1,
            checksum_2: snapshot.checksum_2,
            db_size_pages,
            db_header_crc32c,
        }
    }

    fn crc32c(self) -> u32 {
        let mut bytes = [0u8; Self::CRC_INPUT_LEN];
        bytes[0..4].copy_from_slice(&SHARED_WAL_BACKFILL_PROOF_VERSION.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.nbackfills.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.max_frame.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.checkpoint_seq.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.page_size.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.salt_1.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.salt_2.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.checksum_1.to_le_bytes());
        bytes[40..44].copy_from_slice(&self.checksum_2.to_le_bytes());
        bytes[44..48].copy_from_slice(&self.db_size_pages.to_le_bytes());
        bytes[48..52].copy_from_slice(&self.db_header_crc32c.to_le_bytes());
        crc32c::crc32c(&bytes)
    }

    fn is_structurally_valid(self) -> bool {
        self.nbackfills != 0 && self.nbackfills <= self.max_frame && self.page_size != 0
    }
}

#[repr(C)]
struct SharedWalCoordinationMapHeader {
    magic: [u8; 8],
    version: u32,
    reader_slot_count: u32,
    reader_bitmap_word_count: u32,
    frame_index_block_capacity: u32,
    frame_index_block_hash_slots: u32,
    frame_index_max_blocks: u32,
    frame_index_blocks: AtomicU32,
    frame_index_capacity: u32,
    frame_index_len: AtomicU32,
    /// Set once the reserved shared frame index space is exhausted.
    ///
    /// Normal operation grows the index a block at a time. This overflow bit
    /// only exists as a last-resort guard once we have consumed the full mapped
    /// region.
    frame_index_overflowed: AtomicU32,
    max_frame: AtomicU64,
    nbackfills: AtomicU64,
    transaction_count: AtomicU64,
    visibility_generation: AtomicU64,
    checkpoint_seq: AtomicU32,
    checkpoint_epoch: AtomicU32,
    page_size: AtomicU32,
    salt_1: AtomicU32,
    salt_2: AtomicU32,
    checksum_1: AtomicU32,
    checksum_2: AtomicU32,
    backfill_proof_version: AtomicU32,
    backfill_proof_nbackfills: AtomicU64,
    backfill_proof_max_frame: AtomicU64,
    backfill_proof_checkpoint_seq: AtomicU32,
    backfill_proof_page_size: AtomicU32,
    backfill_proof_salt_1: AtomicU32,
    backfill_proof_salt_2: AtomicU32,
    backfill_proof_checksum_1: AtomicU32,
    backfill_proof_checksum_2: AtomicU32,
    backfill_proof_db_size_pages: AtomicU32,
    backfill_proof_db_header_crc32c: AtomicU32,
    backfill_proof_crc32c: AtomicU32,
    writer_owner: AtomicU64,
    checkpoint_owner: AtomicU64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SharedWalFrameIndexEntry {
    page_id: u64,
    frame_id: u64,
}

/// How the tshm file was opened, determined at open time by probing byte 0.
///
/// - `Exclusive`: no other process had the file open. The opener is free to
///   reinitialize or repair any shared state.
/// - `MultiProcess`: at least one other process already has the file open.
///   The opener must not clobber state that the peer may be relying on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SharedWalCoordinationOpenMode {
    Exclusive,
    MultiProcess,
}

/// Which locking primitive is used for cross-process slot ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedWalOwnershipMode {
    /// `F_OFD_SETLK`: per file-description, survives `dup()`, independent
    /// across separate `open()` calls. Stale detection: probe the lock.
    LinuxOfd,
    /// `F_SETLK`: per-process, shared across all fds to the same file.
    /// Stale detection: `kill(pid, 0)` on the owner's PID.
    ProcessScopedFcntl,
}

/// mmap-backed shared WAL coordination region (the `.tshm` file).
///
/// This is the cross-process source of truth for WAL snapshot metadata,
/// reader/writer/checkpoint ownership and the shared page-to-frame index.
/// One instance exists per `Database` open (one per
/// process in normal operation). All `Connection`s to the same `Database`
/// share a single `Arc<MappedSharedWalCoordination>`.
///
/// ## Ownership model
///
/// Two ownership backends exist, selected at compile time:
///
/// - **Linux (OFD)**: `F_OFD_SETLK` byte-range locks on the tshm file.
///   Per file-description, so two opens in the same process get independent
///   locks. Stale-owner detection probes the lock: if it can be acquired,
///   the owner is dead.
///
/// - **macOS / other Unix (process-scoped fcntl)**: Classical `F_SETLK`
///   locks (per-process, not per-fd). Stale-owner detection falls back to
///   `kill(pid, 0)` liveness checks on the `SharedOwnerRecord` PID.
///
/// ## Lock byte layout on the tshm file
///
/// | Offset    | Purpose
/// |-----------|---------------
/// | 0         | Process-lifetime shared/exclusive lock (determines Exclusive vs MultiProcess open)
/// | 1         | Writer lock
/// | 2         | Checkpoint lock
/// | 3..3+N    | Reader slot locks (one byte per slot)
pub(crate) struct MappedSharedWalCoordination {
    file: Arc<dyn File>,
    _base_mapping: Box<dyn SharedWalMappedRegion>,
    base_ptr: NonNull<u8>,
    base_len: usize,
    owner_record: SharedOwnerRecord,
    ownership_mode: SharedWalOwnershipMode,
    frame_index_blocks: RwLock<Vec<FrameIndexBlockMapping>>,
    frame_index_publish_lock: Arc<Mutex<()>>,
    process_local_ownership: Arc<Mutex<ProcessLocalOwnershipState>>,
    local_lock_state: Mutex<LocalLockState>,
    open_mode: SharedWalCoordinationOpenMode,
    sanitized_backfill_proof_on_open: bool,
    primary_process_mapping: bool,
    registry_path: Option<PathBuf>,
}

struct FrameIndexBlockMapping {
    _mapping: Box<dyn SharedWalMappedRegion>,
    entries_ptr: NonNull<SharedWalFrameIndexEntry>,
    hash_ptr: NonNull<u16>,
    byte_len: usize,
}

unsafe impl Send for MappedSharedWalCoordination {}
unsafe impl Sync for MappedSharedWalCoordination {}

impl std::fmt::Debug for MappedSharedWalCoordination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mapped_blocks = self
            .frame_index_blocks
            .read()
            .expect("shared WAL frame index lock poisoned")
            .len();
        f.debug_struct("MappedSharedWalCoordination")
            .field("base_len", &self.base_len)
            .field("owner_record", &self.owner_record)
            .field("ownership_mode", &self.ownership_mode)
            .field("mapped_blocks", &mapped_blocks)
            .field("open_mode", &self.open_mode)
            .field("primary_process_mapping", &self.primary_process_mapping)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl Drop for MappedSharedWalCoordination {
    fn drop(&mut self) {
        self.release_owned_locks_on_drop();
        if let Some(path) = self.registry_path.as_ref() {
            let mut opens = PROCESS_LOCAL_COORDINATION_OPENS
                .lock()
                .expect("process-local coordination registry poisoned");
            let count = opens
                .get_mut(path)
                .expect("shared WAL coordination registry entry missing");
            turso_assert!(
                count.open_count > 0,
                "shared WAL coordination registry count underflow"
            );
            count.open_count -= 1;
            if count.open_count == 0 {
                opens.remove(path);
            }
        }
        self.frame_index_blocks
            .get_mut()
            .expect("shared WAL frame index lock poisoned")
            .clear();
        let _ = self
            .file
            .shared_wal_unlock_byte(PROCESS_LIFETIME_LOCK_OFFSET, self.lock_kind());
    }
}

impl std::fmt::Debug for SharedWalCoordinationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedWalCoordinationState")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl SharedWalCoordinationState {
    pub(crate) fn new(reader_slot_count: u32) -> Self {
        turso_assert!(
            reader_slot_count >= 64 && reader_slot_count % 64 == 0,
            "reader_slot_count must be a non-zero multiple of 64"
        );
        let reader_frames = (0..reader_slot_count)
            .map(|_| AtomicU64::new(UNUSED_READER_FRAME))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let reader_owners = (0..reader_slot_count)
            .map(|_| AtomicU64::new(UNOWNED_LOCK))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            max_frame: AtomicU64::new(0),
            nbackfills: AtomicU64::new(0),
            transaction_count: AtomicU64::new(0),
            visibility_generation: AtomicU64::new(0),
            checkpoint_seq: AtomicU32::new(0),
            checkpoint_epoch: AtomicU32::new(0),
            page_size: AtomicU32::new(0),
            salt_1: AtomicU32::new(0),
            salt_2: AtomicU32::new(0),
            checksum_1: AtomicU32::new(0),
            checksum_2: AtomicU32::new(0),
            writer_owner: AtomicU64::new(UNOWNED_LOCK),
            checkpoint_owner: AtomicU64::new(UNOWNED_LOCK),
            reader_frames,
            reader_owners,
            reader_slots: AtomicSlotBitmap::new(reader_slot_count),
        }
    }

    pub(crate) fn snapshot(&self) -> SharedWalCoordinationHeader {
        SharedWalCoordinationHeader {
            max_frame: self.max_frame.load(Ordering::Acquire),
            nbackfills: self.nbackfills.load(Ordering::Acquire),
            transaction_count: self.transaction_count.load(Ordering::Acquire),
            visibility_generation: self.visibility_generation.load(Ordering::Acquire),
            checkpoint_seq: self.checkpoint_seq.load(Ordering::Acquire),
            checkpoint_epoch: self.checkpoint_epoch.load(Ordering::Acquire),
            page_size: self.page_size.load(Ordering::Acquire),
            salt_1: self.salt_1.load(Ordering::Acquire),
            salt_2: self.salt_2.load(Ordering::Acquire),
            checksum_1: self.checksum_1.load(Ordering::Acquire),
            checksum_2: self.checksum_2.load(Ordering::Acquire),
            reader_slot_count: self.reader_frames.len() as u32,
        }
    }

    pub(crate) fn install_snapshot(&self, snapshot: SharedWalCoordinationHeader) {
        self.max_frame.store(snapshot.max_frame, Ordering::Release);
        self.nbackfills
            .store(snapshot.nbackfills, Ordering::Release);
        self.transaction_count
            .store(snapshot.transaction_count, Ordering::Release);
        self.visibility_generation
            .store(snapshot.visibility_generation, Ordering::Release);
        self.checkpoint_seq
            .store(snapshot.checkpoint_seq, Ordering::Release);
        self.checkpoint_epoch
            .store(snapshot.checkpoint_epoch, Ordering::Release);
        self.page_size.store(snapshot.page_size, Ordering::Release);
        self.salt_1.store(snapshot.salt_1, Ordering::Release);
        self.salt_2.store(snapshot.salt_2, Ordering::Release);
        self.checksum_1
            .store(snapshot.checksum_1, Ordering::Release);
        self.checksum_2
            .store(snapshot.checksum_2, Ordering::Release);
    }

    pub(crate) fn publish_commit(
        &self,
        max_frame: u64,
        checksum_1: u32,
        checksum_2: u32,
        transaction_count: u64,
    ) {
        self.max_frame.store(max_frame, Ordering::Release);
        self.checksum_1.store(checksum_1, Ordering::Release);
        self.checksum_2.store(checksum_2, Ordering::Release);
        self.transaction_count
            .store(transaction_count, Ordering::Release);
        self.visibility_generation.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn publish_backfill(&self, nbackfills: u64) {
        self.nbackfills.store(nbackfills, Ordering::Release);
    }

    pub(crate) fn install_header_fields(&self, page_size: u32, salt_1: u32, salt_2: u32) {
        self.page_size.store(page_size, Ordering::Release);
        self.salt_1.store(salt_1, Ordering::Release);
        self.salt_2.store(salt_2, Ordering::Release);
    }

    pub(crate) fn bump_checkpoint_seq(&self) -> u32 {
        self.checkpoint_seq.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(crate) fn checkpoint_epoch(&self) -> u32 {
        self.checkpoint_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn bump_checkpoint_epoch(&self) -> u32 {
        self.checkpoint_epoch.fetch_add(1, Ordering::AcqRel)
    }

    pub(crate) fn try_acquire_writer(&self, owner: SharedOwnerRecord) -> bool {
        self.writer_owner
            .compare_exchange(
                UNOWNED_LOCK,
                owner.raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn release_writer(&self, owner: SharedOwnerRecord) {
        turso_assert!(
            self.writer_owner
                .compare_exchange(
                    owner.raw(),
                    UNOWNED_LOCK,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok(),
            "writer released by non-owner"
        );
    }

    pub(crate) fn writer_owner(&self) -> Option<SharedOwnerRecord> {
        SharedOwnerRecord::from_raw(self.writer_owner.load(Ordering::Acquire))
    }

    pub(crate) fn try_acquire_checkpoint(&self, owner: SharedOwnerRecord) -> bool {
        self.checkpoint_owner
            .compare_exchange(
                UNOWNED_LOCK,
                owner.raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn release_checkpoint(&self, owner: SharedOwnerRecord) {
        turso_assert!(
            self.checkpoint_owner
                .compare_exchange(
                    owner.raw(),
                    UNOWNED_LOCK,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok(),
            "checkpoint released by non-owner"
        );
    }

    pub(crate) fn checkpoint_owner(&self) -> Option<SharedOwnerRecord> {
        SharedOwnerRecord::from_raw(self.checkpoint_owner.load(Ordering::Acquire))
    }

    pub(crate) fn register_reader(
        &self,
        owner: SharedOwnerRecord,
        max_frame: u64,
    ) -> Option<SharedReaderSlot> {
        let slot_index = self.reader_slots.alloc_one()?;
        self.reader_owners[slot_index as usize].store(owner.raw(), Ordering::Release);
        self.reader_frames[slot_index as usize].store(max_frame, Ordering::Release);
        Some(SharedReaderSlot {
            slot_index,
            max_frame,
            owner,
        })
    }

    pub(crate) fn update_reader(&self, slot: SharedReaderSlot, max_frame: u64) -> SharedReaderSlot {
        turso_assert!(
            self.reader_owners[slot.slot_index as usize].load(Ordering::Acquire)
                == slot.owner.raw(),
            "reader slot updated by non-owner"
        );
        self.reader_frames[slot.slot_index as usize].store(max_frame, Ordering::Release);
        SharedReaderSlot {
            slot_index: slot.slot_index,
            max_frame,
            owner: slot.owner,
        }
    }

    pub(crate) fn unregister_reader(&self, slot: SharedReaderSlot) {
        turso_assert!(
            self.reader_owners[slot.slot_index as usize].load(Ordering::Acquire)
                == slot.owner.raw(),
            "reader slot released by non-owner"
        );
        self.reader_frames[slot.slot_index as usize].store(UNUSED_READER_FRAME, Ordering::Release);
        self.reader_owners[slot.slot_index as usize].store(UNOWNED_LOCK, Ordering::Release);
        self.reader_slots.free_one(slot.slot_index);
    }

    pub(crate) fn reader_owner(&self, slot_index: u32) -> Option<SharedOwnerRecord> {
        SharedOwnerRecord::from_raw(self.reader_owners[slot_index as usize].load(Ordering::Acquire))
    }

    pub(crate) fn min_active_reader_frame(&self) -> Option<u64> {
        self.reader_frames
            .iter()
            .map(|frame| frame.load(Ordering::Acquire))
            .filter(|&frame| frame != UNUSED_READER_FRAME)
            .min()
    }
}

impl MappedSharedWalCoordination {
    const fn default_ownership_mode() -> SharedWalOwnershipMode {
        if cfg!(target_os = "linux") {
            SharedWalOwnershipMode::LinuxOfd
        } else {
            SharedWalOwnershipMode::ProcessScopedFcntl
        }
    }

    fn uses_linux_ofd_locking(&self) -> bool {
        self.ownership_mode == SharedWalOwnershipMode::LinuxOfd
    }

    const fn lock_kind_for_mode(mode: SharedWalOwnershipMode) -> SharedWalLockKind {
        match mode {
            SharedWalOwnershipMode::LinuxOfd => SharedWalLockKind::LinuxOfd,
            SharedWalOwnershipMode::ProcessScopedFcntl => SharedWalLockKind::ProcessScopedFcntl,
        }
    }

    const fn lock_kind(&self) -> SharedWalLockKind {
        Self::lock_kind_for_mode(self.ownership_mode)
    }

    /// Register this mapping in `PROCESS_LOCAL_COORDINATION_OPENS`.
    ///
    /// Returns shared Arcs for locks and ownership state so that multiple
    /// mappings of the same file (only possible in tests — production uses
    /// `DATABASE_MANAGER` which ensures one `Database` per file) coordinate
    /// correctly. Also returns whether this is the first mapping for this path
    /// (`primary_process_mapping`), which controls invalidation-on-close.
    fn register_process_mapping(
        path: &Path,
        reader_slot_count: u32,
    ) -> (
        PathBuf,
        bool,
        Arc<Mutex<()>>,
        Arc<Mutex<ProcessLocalOwnershipState>>,
    ) {
        let canonical_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let mut opens = PROCESS_LOCAL_COORDINATION_OPENS
            .lock()
            .expect("process-local coordination registry poisoned");
        let entry =
            opens
                .entry(canonical_path.clone())
                .or_insert_with(|| ProcessLocalCoordinationEntry {
                    open_count: 0,
                    frame_index_publish_lock: Arc::new(Mutex::new(())),
                    ownership: Arc::new(Mutex::new(ProcessLocalOwnershipState::new(
                        reader_slot_count,
                    ))),
                });
        turso_assert!(
            entry
                .ownership
                .lock()
                .expect("process-local ownership registry poisoned")
                .reader_owners
                .len()
                == reader_slot_count as usize,
            "process-local coordination slot count mismatch"
        );
        let primary_process_mapping = entry.open_count == 0;
        entry.open_count += 1;
        (
            canonical_path,
            primary_process_mapping,
            entry.frame_index_publish_lock.clone(),
            entry.ownership.clone(),
        )
    }

    fn process_already_has_mapping(path: &Path) -> bool {
        let canonical_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let opens = PROCESS_LOCAL_COORDINATION_OPENS
            .lock()
            .expect("process-local coordination registry poisoned");
        opens
            .get(&canonical_path)
            .is_some_and(|entry| entry.open_count > 0)
    }

    /// Open (or create) the `.tshm` coordination file at `path`.
    ///
    /// On first open within a host, the file is created, sized, and
    /// initialized. Subsequent opens in other processes mmap the existing
    /// file. The open-mode (Exclusive vs MultiProcess) is determined by
    /// attempting an exclusive lock on byte 0: if it succeeds no other
    /// process has the file open, so we are exclusive.
    pub(crate) fn create_or_open(
        io: &Arc<dyn IO>,
        path: &Path,
        reader_slot_count: u32,
    ) -> Result<Self> {
        Self::create_or_open_with_mode(io, path, reader_slot_count, Self::default_ownership_mode())
    }

    pub(crate) fn open_existing(
        io: &Arc<dyn IO>,
        path: &Path,
        reader_slot_count: u32,
    ) -> Result<Option<Self>> {
        Self::open_existing_with_mode(io, path, reader_slot_count, Self::default_ownership_mode())
    }

    #[cfg(test)]
    fn create_or_open_process_scoped_for_tests(
        io: &Arc<dyn IO>,
        path: &Path,
        reader_slot_count: u32,
    ) -> Result<Self> {
        Self::create_or_open_with_mode(
            io,
            path,
            reader_slot_count,
            SharedWalOwnershipMode::ProcessScopedFcntl,
        )
    }

    #[cfg(test)]
    pub(crate) fn mark_frame_index_overflowed_for_tests(&self) {
        self.header()
            .frame_index_overflowed
            .store(1, Ordering::Release);
    }

    fn create_or_open_with_mode(
        io: &Arc<dyn IO>,
        path: &Path,
        reader_slot_count: u32,
        ownership_mode: SharedWalOwnershipMode,
    ) -> Result<Self> {
        turso_assert!(
            reader_slot_count >= 64 && reader_slot_count % 64 == 0,
            "reader_slot_count must be a non-zero multiple of 64"
        );

        let base_len = Self::base_mapped_len(reader_slot_count);
        let initial_file_len =
            Self::file_len_for_blocks(reader_slot_count, INITIAL_FRAME_INDEX_BLOCKS);
        let file = io.open_shared_wal_file(path.to_str().ok_or_else(|| {
            LimboError::InternalError("shared WAL coordination path is not valid UTF-8".into())
        })?)?;
        let lock_kind = Self::lock_kind_for_mode(ownership_mode);
        let open_mode = if Self::process_already_has_mapping(path) {
            file.shared_wal_lock_byte(PROCESS_LIFETIME_LOCK_OFFSET, false, lock_kind)?;
            SharedWalCoordinationOpenMode::MultiProcess
        } else {
            match file.shared_wal_try_lock_byte(PROCESS_LIFETIME_LOCK_OFFSET, true, lock_kind)? {
                true => {
                    file.shared_wal_unlock_byte(PROCESS_LIFETIME_LOCK_OFFSET, lock_kind)?;
                    file.shared_wal_lock_byte(PROCESS_LIFETIME_LOCK_OFFSET, false, lock_kind)?;
                    SharedWalCoordinationOpenMode::Exclusive
                }
                false => {
                    file.shared_wal_lock_byte(PROCESS_LIFETIME_LOCK_OFFSET, false, lock_kind)?;
                    SharedWalCoordinationOpenMode::MultiProcess
                }
            }
        };
        let metadata_len = file.size()? as usize;
        let initialize = metadata_len == 0
            || (open_mode == SharedWalCoordinationOpenMode::Exclusive && metadata_len < base_len);
        if open_mode == SharedWalCoordinationOpenMode::MultiProcess && metadata_len < base_len {
            file.shared_wal_unlock_byte(PROCESS_LIFETIME_LOCK_OFFSET, lock_kind)?;
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination file is smaller than the coordination header: got {metadata_len}, minimum {base_len}"
            )));
        }
        if initialize {
            file.shared_wal_set_len(initial_file_len as u64)?;
        }

        let base_mapping = file.shared_wal_map(0, base_len)?;
        let base_ptr = base_mapping.ptr();

        let mut region = Self {
            file,
            _base_mapping: base_mapping,
            base_ptr,
            base_len,
            owner_record: SharedOwnerRecord::current_process(next_shared_owner_instance_id()),
            ownership_mode,
            frame_index_blocks: RwLock::new(Vec::new()),
            frame_index_publish_lock: Arc::new(Mutex::new(())),
            process_local_ownership: Arc::new(Mutex::new(ProcessLocalOwnershipState::new(
                reader_slot_count,
            ))),
            local_lock_state: Mutex::new(LocalLockState {
                writer_lock_held: false,
                checkpoint_lock_held: false,
                reader_locks: vec![0; reader_slot_count as usize],
            }),
            open_mode,
            sanitized_backfill_proof_on_open: false,
            primary_process_mapping: false,
            registry_path: None,
        };
        if initialize {
            region.initialize(reader_slot_count);
        } else if let Err(err) = region.validate_existing(reader_slot_count, metadata_len) {
            if open_mode == SharedWalCoordinationOpenMode::Exclusive {
                region.file.shared_wal_set_len(initial_file_len as u64)?;
                region.initialize(reader_slot_count);
            } else {
                return Err(err);
            }
        }
        if open_mode == SharedWalCoordinationOpenMode::Exclusive {
            region.sanitized_backfill_proof_on_open =
                region.sanitize_backfill_proof_for_exclusive_open();
        }
        let mapped_blocks = region.header().frame_index_blocks.load(Ordering::Acquire);
        region.ensure_mapped_frame_index_blocks(mapped_blocks)?;
        let (
            registry_path,
            primary_process_mapping,
            frame_index_publish_lock,
            process_local_ownership,
        ) = Self::register_process_mapping(path, reader_slot_count);
        region.frame_index_publish_lock = frame_index_publish_lock;
        region.process_local_ownership = process_local_ownership;
        region.primary_process_mapping = primary_process_mapping;
        region.registry_path = Some(registry_path);
        Ok(region)
    }

    fn open_existing_with_mode(
        io: &Arc<dyn IO>,
        path: &Path,
        reader_slot_count: u32,
        ownership_mode: SharedWalOwnershipMode,
    ) -> Result<Option<Self>> {
        turso_assert!(
            reader_slot_count >= 64 && reader_slot_count % 64 == 0,
            "reader_slot_count must be a non-zero multiple of 64"
        );

        let base_len = Self::base_mapped_len(reader_slot_count);
        let file = match io.open_file(
            path.to_str().ok_or_else(|| {
                LimboError::InternalError("shared WAL coordination path is not valid UTF-8".into())
            })?,
            OpenFlags::NoLock,
            false,
        ) {
            Ok(file) => file,
            Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                _,
            ))) => return Ok(None),
            Err(err) => return Err(err),
        };
        let lock_kind = Self::lock_kind_for_mode(ownership_mode);
        let open_mode = if Self::process_already_has_mapping(path) {
            file.shared_wal_lock_byte(PROCESS_LIFETIME_LOCK_OFFSET, false, lock_kind)?;
            SharedWalCoordinationOpenMode::MultiProcess
        } else {
            match file.shared_wal_try_lock_byte(PROCESS_LIFETIME_LOCK_OFFSET, true, lock_kind)? {
                true => {
                    file.shared_wal_unlock_byte(PROCESS_LIFETIME_LOCK_OFFSET, lock_kind)?;
                    file.shared_wal_lock_byte(PROCESS_LIFETIME_LOCK_OFFSET, false, lock_kind)?;
                    SharedWalCoordinationOpenMode::Exclusive
                }
                false => {
                    file.shared_wal_lock_byte(PROCESS_LIFETIME_LOCK_OFFSET, false, lock_kind)?;
                    SharedWalCoordinationOpenMode::MultiProcess
                }
            }
        };
        let metadata_len = file.size()? as usize;
        if metadata_len < base_len {
            file.shared_wal_unlock_byte(PROCESS_LIFETIME_LOCK_OFFSET, lock_kind)?;
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination file is smaller than the coordination header: got {metadata_len}, minimum {base_len}"
            )));
        }

        let base_mapping = file.shared_wal_map(0, base_len)?;
        let base_ptr = base_mapping.ptr();
        let mut region = Self {
            file,
            _base_mapping: base_mapping,
            base_ptr,
            base_len,
            owner_record: SharedOwnerRecord::current_process(next_shared_owner_instance_id()),
            ownership_mode,
            frame_index_blocks: RwLock::new(Vec::new()),
            frame_index_publish_lock: Arc::new(Mutex::new(())),
            process_local_ownership: Arc::new(Mutex::new(ProcessLocalOwnershipState::new(
                reader_slot_count,
            ))),
            local_lock_state: Mutex::new(LocalLockState {
                writer_lock_held: false,
                checkpoint_lock_held: false,
                reader_locks: vec![0; reader_slot_count as usize],
            }),
            open_mode,
            sanitized_backfill_proof_on_open: false,
            primary_process_mapping: false,
            registry_path: None,
        };
        if let Err(err) = region.validate_existing(reader_slot_count, metadata_len) {
            region
                .file
                .shared_wal_unlock_byte(PROCESS_LIFETIME_LOCK_OFFSET, lock_kind)?;
            return Err(err);
        }
        let mapped_blocks = region.header().frame_index_blocks.load(Ordering::Acquire);
        region.ensure_mapped_frame_index_blocks(mapped_blocks)?;
        let (
            registry_path,
            primary_process_mapping,
            frame_index_publish_lock,
            process_local_ownership,
        ) = Self::register_process_mapping(path, reader_slot_count);
        region.frame_index_publish_lock = frame_index_publish_lock;
        region.process_local_ownership = process_local_ownership;
        region.primary_process_mapping = primary_process_mapping;
        region.registry_path = Some(registry_path);
        Ok(Some(region))
    }

    fn process_lock_offset_for_reader(slot_index: u32) -> u64 {
        READER_LOCK_START_OFFSET + slot_index as u64
    }

    pub(crate) fn is_last_process_mapping(&self) -> bool {
        if !matches!(
            self.file.shared_wal_try_lock_byte(
                PROCESS_LIFETIME_LOCK_OFFSET,
                true,
                self.lock_kind(),
            ),
            Ok(true)
        ) {
            return false;
        }
        let _ = self
            .file
            .shared_wal_unlock_byte(PROCESS_LIFETIME_LOCK_OFFSET, self.lock_kind());
        true
    }

    fn release_owned_locks_on_drop(&mut self) {
        let (writer_lock_held, checkpoint_lock_held, reader_locks) = {
            let local = self
                .local_lock_state
                .get_mut()
                .expect("shared WAL local lock state poisoned");
            let writer_lock_held = local.writer_lock_held;
            let checkpoint_lock_held = local.checkpoint_lock_held;
            let reader_locks = std::mem::take(&mut local.reader_locks);
            local.writer_lock_held = false;
            local.checkpoint_lock_held = false;
            (writer_lock_held, checkpoint_lock_held, reader_locks)
        };

        if writer_lock_held {
            if self.uses_linux_ofd_locking() {
                self.header()
                    .writer_owner
                    .store(UNOWNED_LOCK, Ordering::Release);
                let _ = self
                    .file
                    .shared_wal_unlock_byte(WRITER_LOCK_OFFSET, self.lock_kind());
            } else {
                let _ = self
                    .file
                    .shared_wal_unlock_byte(WRITER_LOCK_OFFSET, self.lock_kind());
                let _ = self.header().writer_owner.compare_exchange(
                    self.owner_record.raw(),
                    UNOWNED_LOCK,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                self.with_process_local_ownership(|entry| {
                    if entry.writer_owner == Some(self.owner_record) {
                        entry.writer_owner = None;
                    }
                });
            }
        }

        if checkpoint_lock_held {
            if self.uses_linux_ofd_locking() {
                self.header()
                    .checkpoint_owner
                    .store(UNOWNED_LOCK, Ordering::Release);
                let _ = self
                    .file
                    .shared_wal_unlock_byte(CHECKPOINT_LOCK_OFFSET, self.lock_kind());
            } else {
                let _ = self
                    .file
                    .shared_wal_unlock_byte(CHECKPOINT_LOCK_OFFSET, self.lock_kind());
                let _ = self.header().checkpoint_owner.compare_exchange(
                    self.owner_record.raw(),
                    UNOWNED_LOCK,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                self.with_process_local_ownership(|entry| {
                    if entry.checkpoint_owner == Some(self.owner_record) {
                        entry.checkpoint_owner = None;
                    }
                });
            }
        }

        for (slot_index, held_count) in reader_locks.into_iter().enumerate() {
            if held_count == 0 {
                continue;
            }
            let slot_index_u32 = slot_index as u32;
            let _ = self.file.shared_wal_unlock_byte(
                Self::process_lock_offset_for_reader(slot_index_u32),
                self.lock_kind(),
            );
            self.reader_frames()[slot_index].store(UNUSED_READER_FRAME, Ordering::Release);
            if self.uses_linux_ofd_locking() {
                self.reader_owners()[slot_index].store(UNOWNED_LOCK, Ordering::Release);
            } else {
                let _ = self.reader_owners()[slot_index].compare_exchange(
                    self.owner_record.raw(),
                    UNOWNED_LOCK,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                self.with_process_local_ownership(|entry| {
                    if entry.reader_owner(slot_index_u32) == Some(self.owner_record) {
                        entry.reader_owners[slot_index] = None;
                    }
                });
            }
            let word_idx = slot_index >> 6;
            let bit = slot_index & 63;
            self.reader_bitmap_words()[word_idx].fetch_or(1u64 << bit, Ordering::Release);
        }
    }

    pub(crate) fn snapshot(&self) -> SharedWalCoordinationHeader {
        let header = self.header();
        SharedWalCoordinationHeader {
            max_frame: header.max_frame.load(Ordering::Acquire),
            nbackfills: header.nbackfills.load(Ordering::Acquire),
            transaction_count: header.transaction_count.load(Ordering::Acquire),
            visibility_generation: header.visibility_generation.load(Ordering::Acquire),
            checkpoint_seq: header.checkpoint_seq.load(Ordering::Acquire),
            checkpoint_epoch: header.checkpoint_epoch.load(Ordering::Acquire),
            page_size: header.page_size.load(Ordering::Acquire),
            salt_1: header.salt_1.load(Ordering::Acquire),
            salt_2: header.salt_2.load(Ordering::Acquire),
            checksum_1: header.checksum_1.load(Ordering::Acquire),
            checksum_2: header.checksum_2.load(Ordering::Acquire),
            reader_slot_count: header.reader_slot_count,
        }
    }

    pub(crate) const fn open_mode(&self) -> SharedWalCoordinationOpenMode {
        self.open_mode
    }

    pub(crate) const fn sanitized_backfill_proof_on_open(&self) -> bool {
        self.sanitized_backfill_proof_on_open
    }

    pub(crate) const fn owner_record(&self) -> SharedOwnerRecord {
        self.owner_record
    }

    pub(crate) const fn is_primary_process_mapping(&self) -> bool {
        self.primary_process_mapping
    }

    pub(crate) fn install_snapshot(&self, snapshot: SharedWalCoordinationHeader) {
        self.clear_backfill_proof();
        // Snapshots define the authoritative visible WAL range. If the shared
        // frame index still carries entries from an older generation past that
        // range, trim them before publishing the new header so later frame
        // appends cannot observe a stale tail.
        self.rollback_frames(snapshot.max_frame);

        let header = self.header();
        header
            .max_frame
            .store(snapshot.max_frame, Ordering::Release);
        header
            .nbackfills
            .store(snapshot.nbackfills, Ordering::Release);
        header
            .transaction_count
            .store(snapshot.transaction_count, Ordering::Release);
        header
            .visibility_generation
            .store(snapshot.visibility_generation, Ordering::Release);
        header
            .checkpoint_seq
            .store(snapshot.checkpoint_seq, Ordering::Release);
        header
            .checkpoint_epoch
            .store(snapshot.checkpoint_epoch, Ordering::Release);
        header
            .page_size
            .store(snapshot.page_size, Ordering::Release);
        header.salt_1.store(snapshot.salt_1, Ordering::Release);
        header.salt_2.store(snapshot.salt_2, Ordering::Release);
        header
            .checksum_1
            .store(snapshot.checksum_1, Ordering::Release);
        header
            .checksum_2
            .store(snapshot.checksum_2, Ordering::Release);
    }

    /// Repair transient runtime state when a process determines that it can
    /// reconcile the tshm with a local WAL disk scan.
    ///
    /// Clears writer/checkpoint owners and reclaims stale reader slots, but
    /// leaves the durable frame index intact.
    ///
    /// For reader slots, we must NOT blindly clear slots owned by
    /// live processes: doing so would cause those processes to panic with
    /// "reader slot released by non-owner" when they try to end their read
    /// transactions, corrupting the shared WAL state.
    ///
    /// - **OFD (Linux)**: probe each slot's OFD byte-range lock. If the lock
    ///   can be acquired, the owner is dead and the slot is safe to reclaim.
    /// - **Process-scoped fcntl (macOS)**: check `kill(pid, 0)` on the slot's
    ///   `SharedOwnerRecord` PID. Same semantics — dead owner ⇒ reclaim.
    pub(crate) fn repair_transient_state_for_exclusive_open(&self) {
        let header = self.header();
        header.writer_owner.store(UNOWNED_LOCK, Ordering::Release);
        header
            .checkpoint_owner
            .store(UNOWNED_LOCK, Ordering::Release);

        // For reader slots, we must check OFD locks before
        // clearing: another live process may hold a slot. Blindly clearing
        // would cause that process to panic with "reader slot released by
        // non-owner" and corrupt the shared WAL state.
        if self.uses_linux_ofd_locking() {
            for slot_index in 0..header.reader_slot_count {
                let offset = Self::process_lock_offset_for_reader(slot_index);
                match self
                    .file
                    .shared_wal_try_lock_byte(offset, true, self.lock_kind())
                {
                    Ok(true) => {
                        // Lock acquired → slot is stale (owner is dead), safe to clear.
                        self.reader_frames()[slot_index as usize]
                            .store(UNUSED_READER_FRAME, Ordering::Release);
                        self.reader_owners()[slot_index as usize]
                            .store(UNOWNED_LOCK, Ordering::Release);
                        let word_idx = (slot_index >> 6) as usize;
                        let bit = slot_index & 63;
                        self.reader_bitmap_words()[word_idx]
                            .fetch_or(1u64 << bit, Ordering::Release);
                        self.file
                            .shared_wal_unlock_byte(offset, self.lock_kind())
                            .expect("failed to release reader slot lock during reset");
                    }
                    Ok(false) | Err(_) => {
                        // Lock held by another live process → leave this slot alone.
                    }
                }
            }
        } else {
            // Non-OFD path (macOS, etc.): use PID liveness checks to avoid
            // clearing reader slots owned by live processes.
            for slot_index in 0..header.reader_slot_count {
                let owner = self.reader_owner(slot_index);
                match owner {
                    None => {
                        // Already unowned — ensure bitmap marks it available.
                        self.reader_frames()[slot_index as usize]
                            .store(UNUSED_READER_FRAME, Ordering::Release);
                        let word_idx = (slot_index >> 6) as usize;
                        let bit = slot_index & 63;
                        self.reader_bitmap_words()[word_idx]
                            .fetch_or(1u64 << bit, Ordering::Release);
                    }
                    Some(owner_rec) => {
                        if pid_is_alive(owner_rec.pid()) {
                            // Owner is alive — leave this slot alone.
                            continue;
                        }
                        // Owner is dead — safe to reclaim.
                        self.try_reclaim_dead_reader_owner(slot_index, owner_rec);
                    }
                }
            }
        }
    }

    /// Discard the durable shared frame index so the caller can rebuild it
    /// from a local WAL scan. The caller must decide separately when that is
    /// safe; this is intentionally distinct from transient-state repair.
    pub(crate) fn discard_durable_frame_index_for_exclusive_rebuild(&self) {
        let _publish_guard = self
            .frame_index_publish_lock
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let header = self.header();
        header.frame_index_len.store(0, Ordering::Release);
        header.frame_index_overflowed.store(0, Ordering::Release);
    }

    pub(crate) fn frame_index_overflowed(&self) -> bool {
        self.header().frame_index_overflowed.load(Ordering::Acquire) != 0
    }

    pub(crate) fn publish_commit(
        &self,
        max_frame: u64,
        checksum_1: u32,
        checksum_2: u32,
        transaction_count: u64,
    ) {
        self.clear_backfill_proof();
        let header = self.header();
        // Use fetch_max to ensure we never lower max_frame. In multi-process
        // mode, another process may have committed frames after ours, advancing
        // max_frame beyond our local value. Overwriting with a smaller value
        // would make those later frames invisible to checkpoints, causing data loss.
        header.max_frame.fetch_max(max_frame, Ordering::AcqRel);
        // Only update checksums if we are the latest writer (our max_frame is the current max).
        // Otherwise, a later writer's checksums are authoritative.
        if header.max_frame.load(Ordering::Acquire) == max_frame {
            header.checksum_1.store(checksum_1, Ordering::Release);
            header.checksum_2.store(checksum_2, Ordering::Release);
        }
        header
            .transaction_count
            .store(transaction_count, Ordering::Release);
        header.visibility_generation.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn publish_backfill(&self, nbackfills: u64) {
        if nbackfills == 0 {
            self.clear_backfill_proof();
        }
        self.header()
            .nbackfills
            .store(nbackfills, Ordering::Release);
    }

    pub(crate) fn clear_backfill_proof(&self) {
        let header = self.header();
        header.backfill_proof_version.store(0, Ordering::Release);
        header.backfill_proof_nbackfills.store(0, Ordering::Release);
        header.backfill_proof_max_frame.store(0, Ordering::Release);
        header
            .backfill_proof_checkpoint_seq
            .store(0, Ordering::Release);
        header.backfill_proof_page_size.store(0, Ordering::Release);
        header.backfill_proof_salt_1.store(0, Ordering::Release);
        header.backfill_proof_salt_2.store(0, Ordering::Release);
        header.backfill_proof_checksum_1.store(0, Ordering::Release);
        header.backfill_proof_checksum_2.store(0, Ordering::Release);
        header
            .backfill_proof_db_size_pages
            .store(0, Ordering::Release);
        header
            .backfill_proof_db_header_crc32c
            .store(0, Ordering::Release);
        header.backfill_proof_crc32c.store(0, Ordering::Release);
    }

    pub(crate) fn install_backfill_proof(
        &self,
        snapshot: SharedWalCoordinationHeader,
        db_size_pages: u32,
        db_header_crc32c: u32,
    ) {
        turso_assert!(
            snapshot.nbackfills != 0,
            "backfill proof requires positive nbackfills"
        );
        let proof =
            SharedWalBackfillProof::from_snapshot_and_db(snapshot, db_size_pages, db_header_crc32c);
        let header = self.header();
        header.backfill_proof_version.store(0, Ordering::Release);
        header
            .backfill_proof_nbackfills
            .store(proof.nbackfills, Ordering::Release);
        header
            .backfill_proof_max_frame
            .store(proof.max_frame, Ordering::Release);
        header
            .backfill_proof_checkpoint_seq
            .store(proof.checkpoint_seq, Ordering::Release);
        header
            .backfill_proof_page_size
            .store(proof.page_size, Ordering::Release);
        header
            .backfill_proof_salt_1
            .store(proof.salt_1, Ordering::Release);
        header
            .backfill_proof_salt_2
            .store(proof.salt_2, Ordering::Release);
        header
            .backfill_proof_checksum_1
            .store(proof.checksum_1, Ordering::Release);
        header
            .backfill_proof_checksum_2
            .store(proof.checksum_2, Ordering::Release);
        header
            .backfill_proof_db_size_pages
            .store(proof.db_size_pages, Ordering::Release);
        header
            .backfill_proof_db_header_crc32c
            .store(proof.db_header_crc32c, Ordering::Release);
        header
            .backfill_proof_crc32c
            .store(proof.crc32c(), Ordering::Release);
        header
            .backfill_proof_version
            .store(SHARED_WAL_BACKFILL_PROOF_VERSION, Ordering::Release);
    }

    pub(crate) fn validate_backfill_proof(
        &self,
        snapshot: SharedWalCoordinationHeader,
        db_size_pages: u32,
        db_header_crc32c: u32,
    ) -> bool {
        let header = self.header();
        let version = header.backfill_proof_version.load(Ordering::Acquire);
        if version != SHARED_WAL_BACKFILL_PROOF_VERSION {
            return false;
        }
        let proof = SharedWalBackfillProof {
            nbackfills: header.backfill_proof_nbackfills.load(Ordering::Acquire),
            max_frame: header.backfill_proof_max_frame.load(Ordering::Acquire),
            checkpoint_seq: header.backfill_proof_checkpoint_seq.load(Ordering::Acquire),
            page_size: header.backfill_proof_page_size.load(Ordering::Acquire),
            salt_1: header.backfill_proof_salt_1.load(Ordering::Acquire),
            salt_2: header.backfill_proof_salt_2.load(Ordering::Acquire),
            checksum_1: header.backfill_proof_checksum_1.load(Ordering::Acquire),
            checksum_2: header.backfill_proof_checksum_2.load(Ordering::Acquire),
            db_size_pages: header.backfill_proof_db_size_pages.load(Ordering::Acquire),
            db_header_crc32c: header
                .backfill_proof_db_header_crc32c
                .load(Ordering::Acquire),
        };
        if !proof.is_structurally_valid() {
            return false;
        }
        let stored_crc = header.backfill_proof_crc32c.load(Ordering::Acquire);
        if proof.crc32c() != stored_crc {
            return false;
        }
        proof
            == SharedWalBackfillProof::from_snapshot_and_db(
                snapshot,
                db_size_pages,
                db_header_crc32c,
            )
    }

    pub(crate) fn sync(&self, io: &Arc<dyn IO>, sync_type: FileSyncType) -> Result<()> {
        let c = self.file.sync(Completion::new_sync(|_| {}), sync_type)?;
        io.wait_for_completion(c)?;
        Ok(())
    }

    fn sanitize_backfill_proof_for_exclusive_open(&self) -> bool {
        let header = self.header();
        let version = header.backfill_proof_version.load(Ordering::Acquire);
        if version == 0 {
            return false;
        }
        if version != SHARED_WAL_BACKFILL_PROOF_VERSION {
            self.clear_backfill_proof();
            return true;
        }
        let proof = SharedWalBackfillProof {
            nbackfills: header.backfill_proof_nbackfills.load(Ordering::Acquire),
            max_frame: header.backfill_proof_max_frame.load(Ordering::Acquire),
            checkpoint_seq: header.backfill_proof_checkpoint_seq.load(Ordering::Acquire),
            page_size: header.backfill_proof_page_size.load(Ordering::Acquire),
            salt_1: header.backfill_proof_salt_1.load(Ordering::Acquire),
            salt_2: header.backfill_proof_salt_2.load(Ordering::Acquire),
            checksum_1: header.backfill_proof_checksum_1.load(Ordering::Acquire),
            checksum_2: header.backfill_proof_checksum_2.load(Ordering::Acquire),
            db_size_pages: header.backfill_proof_db_size_pages.load(Ordering::Acquire),
            db_header_crc32c: header
                .backfill_proof_db_header_crc32c
                .load(Ordering::Acquire),
        };
        let stored_crc = header.backfill_proof_crc32c.load(Ordering::Acquire);
        if !proof.is_structurally_valid() || proof.crc32c() != stored_crc {
            self.clear_backfill_proof();
            return true;
        }
        false
    }

    pub(crate) fn install_header_fields(&self, page_size: u32, salt_1: u32, salt_2: u32) {
        let header = self.header();
        header.page_size.store(page_size, Ordering::Release);
        header.salt_1.store(salt_1, Ordering::Release);
        header.salt_2.store(salt_2, Ordering::Release);
    }

    pub(crate) fn bump_checkpoint_seq(&self) -> u32 {
        self.header().checkpoint_seq.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(crate) fn checkpoint_epoch(&self) -> u32 {
        self.header().checkpoint_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn bump_checkpoint_epoch(&self) -> u32 {
        self.header()
            .checkpoint_epoch
            .fetch_add(1, Ordering::AcqRel)
    }

    fn with_local_lock_state<T>(&self, f: impl FnOnce(&mut LocalLockState) -> T) -> T {
        let mut entry = self
            .local_lock_state
            .lock()
            .expect("shared WAL local lock state poisoned");
        f(&mut entry)
    }

    fn with_process_local_ownership<T>(
        &self,
        f: impl FnOnce(&mut ProcessLocalOwnershipState) -> T,
    ) -> T {
        let mut entry = self
            .process_local_ownership
            .lock()
            .expect("process-local ownership registry poisoned");
        f(&mut entry)
    }

    fn try_acquire_shared_owner_slot(slot: &AtomicU64, owner: SharedOwnerRecord) -> bool {
        let desired = owner.raw();
        loop {
            let current = slot.load(Ordering::Acquire);
            if current == UNOWNED_LOCK {
                return slot
                    .compare_exchange(UNOWNED_LOCK, desired, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
            }
            if current == desired {
                return false;
            }
            let Some(current_owner) = SharedOwnerRecord::from_raw(current) else {
                continue;
            };
            if pid_is_alive(current_owner.pid()) {
                return false;
            }
            if slot
                .compare_exchange(current, desired, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn shared_owner_slot_active(slot: &AtomicU64) -> bool {
        loop {
            let current = slot.load(Ordering::Acquire);
            let Some(owner) = SharedOwnerRecord::from_raw(current) else {
                return false;
            };
            if pid_is_alive(owner.pid()) {
                return true;
            }
            if slot
                .compare_exchange(current, UNOWNED_LOCK, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return false;
            }
        }
    }

    fn release_shared_owner_slot(slot: &AtomicU64, owner: SharedOwnerRecord, _what: &str) {
        turso_assert!(
            slot.compare_exchange(
                owner.raw(),
                UNOWNED_LOCK,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok(),
            "shared owner slot released by non-owner"
        );
    }

    /// Try to reclaim a reader slot whose owner PID is dead (non-OFD path).
    /// Uses `kill(pid, 0)` to check liveness, then CAS to atomically clear
    /// the owner. Returns true if the slot was successfully reclaimed.
    fn try_reclaim_dead_reader_owner(&self, slot_index: u32, owner: SharedOwnerRecord) -> bool {
        if self.with_process_local_ownership(|entry| entry.reader_owner(slot_index).is_some()) {
            return false;
        }
        if pid_is_alive(owner.pid()) {
            return false;
        }
        if self.reader_owners()[slot_index as usize]
            .compare_exchange(
                owner.raw(),
                UNOWNED_LOCK,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        self.reader_frames()[slot_index as usize].store(UNUSED_READER_FRAME, Ordering::Release);
        let word_idx = (slot_index >> 6) as usize;
        let bit = slot_index & 63;
        self.reader_bitmap_words()[word_idx].fetch_or(1u64 << bit, Ordering::Release);
        true
    }

    fn try_acquire_supplemental_byte_lock(&self, offset: u64) -> bool {
        matches!(
            self.file
                .shared_wal_try_lock_byte(offset, true, self.lock_kind()),
            Ok(true)
        )
    }

    fn release_supplemental_byte_lock(&self, offset: u64) {
        self.file
            .shared_wal_unlock_byte(offset, self.lock_kind())
            .expect("failed to release shared WAL supplemental byte lock");
    }

    pub(crate) fn try_acquire_writer(&self, owner: SharedOwnerRecord) -> bool {
        let mut local = self
            .local_lock_state
            .lock()
            .expect("shared WAL local lock state poisoned");
        if local.writer_lock_held {
            return false;
        }
        if self.uses_linux_ofd_locking() {
            if !matches!(
                self.file
                    .shared_wal_try_lock_byte(WRITER_LOCK_OFFSET, true, self.lock_kind(),),
                Ok(true)
            ) {
                return false;
            }
        } else {
            if !self.with_process_local_ownership(|entry| entry.try_acquire_writer(owner)) {
                return false;
            }
            if !Self::try_acquire_shared_owner_slot(&self.header().writer_owner, owner) {
                self.with_process_local_ownership(|entry| entry.release_writer(owner));
                return false;
            }
            if !self.try_acquire_supplemental_byte_lock(WRITER_LOCK_OFFSET) {
                Self::release_shared_owner_slot(&self.header().writer_owner, owner, "writer");
                self.with_process_local_ownership(|entry| entry.release_writer(owner));
                return false;
            }
        }
        local.writer_lock_held = true;
        drop(local);
        if self.uses_linux_ofd_locking() {
            self.header()
                .writer_owner
                .store(owner.raw(), Ordering::Release);
        }
        true
    }

    pub(crate) fn release_writer(&self, owner: SharedOwnerRecord) {
        let mut local = self
            .local_lock_state
            .lock()
            .expect("shared WAL local lock state poisoned");
        turso_assert!(local.writer_lock_held, "writer registry count underflow");
        if self.uses_linux_ofd_locking() {
            let observed_owner = self.header().writer_owner.load(Ordering::Acquire);
            if observed_owner != owner.raw() {
                tracing::debug!(
                    observed_owner,
                    owner = owner.raw(),
                    "releasing shared WAL writer lock with stale owner field"
                );
            }
            self.header()
                .writer_owner
                .store(UNOWNED_LOCK, Ordering::Release);
            self.file
                .shared_wal_unlock_byte(WRITER_LOCK_OFFSET, self.lock_kind())
                .expect("failed to release shared WAL writer lock");
        } else {
            self.release_supplemental_byte_lock(WRITER_LOCK_OFFSET);
            Self::release_shared_owner_slot(&self.header().writer_owner, owner, "writer");
            self.with_process_local_ownership(|entry| entry.release_writer(owner));
        }
        local.writer_lock_held = false;
    }

    fn byte_lock_is_held(&self, offset: u64, local_held: bool) -> bool {
        if local_held {
            return true;
        }
        match self
            .file
            .shared_wal_try_lock_byte(offset, true, self.lock_kind())
        {
            Ok(true) => {
                self.file
                    .shared_wal_unlock_byte(offset, self.lock_kind())
                    .expect("failed to release probed shared WAL byte lock");
                false
            }
            Ok(false) => true,
            Err(err) => {
                tracing::debug!(offset, ?err, "failed probing shared WAL byte lock state");
                true
            }
        }
    }

    pub(crate) fn writer_or_checkpoint_lock_active(&self) -> bool {
        let (writer_held, checkpoint_held) = self
            .with_local_lock_state(|entry| (entry.writer_lock_held, entry.checkpoint_lock_held));
        if self.uses_linux_ofd_locking() {
            return self.byte_lock_is_held(WRITER_LOCK_OFFSET, writer_held)
                || self.byte_lock_is_held(CHECKPOINT_LOCK_OFFSET, checkpoint_held);
        }
        if self.with_process_local_ownership(|entry| {
            entry.writer_active() || entry.checkpoint_active()
        }) {
            return true;
        }
        Self::shared_owner_slot_active(&self.header().writer_owner)
            || Self::shared_owner_slot_active(&self.header().checkpoint_owner)
    }

    pub(crate) fn checkpoint_lock_active(&self) -> bool {
        let checkpoint_held = self.with_local_lock_state(|entry| entry.checkpoint_lock_held);
        if self.uses_linux_ofd_locking() {
            return self.byte_lock_is_held(CHECKPOINT_LOCK_OFFSET, checkpoint_held);
        }
        if self.with_process_local_ownership(|entry| entry.checkpoint_active()) {
            return true;
        }
        Self::shared_owner_slot_active(&self.header().checkpoint_owner)
    }

    pub(crate) fn writer_owner(&self) -> Option<SharedOwnerRecord> {
        SharedOwnerRecord::from_raw(self.header().writer_owner.load(Ordering::Acquire))
    }

    pub(crate) fn try_acquire_checkpoint(&self, owner: SharedOwnerRecord) -> bool {
        let mut local = self
            .local_lock_state
            .lock()
            .expect("shared WAL local lock state poisoned");
        if local.checkpoint_lock_held {
            return false;
        }
        if self.uses_linux_ofd_locking() {
            if !matches!(
                self.file
                    .shared_wal_try_lock_byte(CHECKPOINT_LOCK_OFFSET, true, self.lock_kind(),),
                Ok(true)
            ) {
                return false;
            }
        } else {
            if !self.with_process_local_ownership(|entry| entry.try_acquire_checkpoint(owner)) {
                return false;
            }
            if !Self::try_acquire_shared_owner_slot(&self.header().checkpoint_owner, owner) {
                self.with_process_local_ownership(|entry| entry.release_checkpoint(owner));
                return false;
            }
            if !self.try_acquire_supplemental_byte_lock(CHECKPOINT_LOCK_OFFSET) {
                Self::release_shared_owner_slot(
                    &self.header().checkpoint_owner,
                    owner,
                    "checkpoint",
                );
                self.with_process_local_ownership(|entry| entry.release_checkpoint(owner));
                return false;
            }
        }
        local.checkpoint_lock_held = true;
        drop(local);
        if self.uses_linux_ofd_locking() {
            self.header()
                .checkpoint_owner
                .store(owner.raw(), Ordering::Release);
        }
        true
    }

    pub(crate) fn release_checkpoint(&self, owner: SharedOwnerRecord) {
        let mut local = self
            .local_lock_state
            .lock()
            .expect("shared WAL local lock state poisoned");
        turso_assert!(
            local.checkpoint_lock_held,
            "checkpoint registry count underflow"
        );
        if self.uses_linux_ofd_locking() {
            let observed_owner = self.header().checkpoint_owner.load(Ordering::Acquire);
            if observed_owner != owner.raw() {
                tracing::debug!(
                    observed_owner,
                    owner = owner.raw(),
                    "releasing shared WAL checkpoint lock with stale owner field"
                );
            }
            self.header()
                .checkpoint_owner
                .store(UNOWNED_LOCK, Ordering::Release);
            self.file
                .shared_wal_unlock_byte(CHECKPOINT_LOCK_OFFSET, self.lock_kind())
                .expect("failed to release shared WAL checkpoint lock");
        } else {
            self.release_supplemental_byte_lock(CHECKPOINT_LOCK_OFFSET);
            Self::release_shared_owner_slot(&self.header().checkpoint_owner, owner, "checkpoint");
            self.with_process_local_ownership(|entry| entry.release_checkpoint(owner));
        }
        local.checkpoint_lock_held = false;
    }

    pub(crate) fn checkpoint_owner(&self) -> Option<SharedOwnerRecord> {
        SharedOwnerRecord::from_raw(self.header().checkpoint_owner.load(Ordering::Acquire))
    }

    fn try_reclaim_stale_reader_slot(&self, slot_index: u32) -> bool {
        if self.uses_linux_ofd_locking() {
            let should_probe = self.with_local_lock_state(|entry| {
                turso_assert!(
                    (slot_index as usize) < entry.reader_locks.len(),
                    "reader slot registry index out of range"
                );
                entry.reader_locks[slot_index as usize] == 0
            });
            if !should_probe {
                return false;
            }
            let offset = Self::process_lock_offset_for_reader(slot_index);
            if !matches!(
                self.file
                    .shared_wal_try_lock_byte(offset, true, self.lock_kind()),
                Ok(true)
            ) {
                return false;
            }
            self.reader_frames()[slot_index as usize].store(UNUSED_READER_FRAME, Ordering::Release);
            self.reader_owners()[slot_index as usize].store(UNOWNED_LOCK, Ordering::Release);
            let word_idx = (slot_index >> 6) as usize;
            let bit = slot_index & 63;
            self.reader_bitmap_words()[word_idx].fetch_or(1u64 << bit, Ordering::Release);
            self.file
                .shared_wal_unlock_byte(offset, self.lock_kind())
                .expect("failed to release reclaimed reader slot lock");
            return true;
        }
        let expected_owner = self.reader_owner(slot_index);
        let Some(owner) = expected_owner else {
            return false;
        };
        self.try_reclaim_dead_reader_owner(slot_index, owner)
    }

    fn reclaim_stale_reader_slots(&self) {
        for slot_index in 0..self.header().reader_slot_count {
            let word_idx = (slot_index >> 6) as usize;
            let bit = slot_index & 63;
            let mask = 1u64 << bit;
            if self.reader_bitmap_words()[word_idx].load(Ordering::Acquire) & mask == 0 {
                self.try_reclaim_stale_reader_slot(slot_index);
            }
        }
    }

    /// Claim a shared reader slot for a read transaction.
    ///
    /// Scans the bitmap for a free slot, atomically clears its bit, then
    /// acquires the platform-specific lock (OFD byte lock on Linux, supplemental
    /// fcntl lock on macOS). On success, stores `owner` and `max_frame` in
    /// shared memory so checkpoints can see where this reader is pinned.
    ///
    /// If all slots are taken, one retry is attempted after running stale-slot
    /// reclamation (which probes whether slot owners are still alive).
    ///
    /// Returns `None` if no slot could be acquired (all occupied by live processes).
    pub(crate) fn register_reader(
        &self,
        owner: SharedOwnerRecord,
        max_frame: u64,
    ) -> Option<SharedReaderSlot> {
        for attempt in 0..2 {
            let bitmap = self.reader_bitmap_words();
            let mut local = self
                .local_lock_state
                .lock()
                .expect("shared WAL local lock state poisoned");
            let mut process_local = self
                .process_local_ownership
                .lock()
                .expect("process-local ownership registry poisoned");
            for slot_index in 0..self.header().reader_slot_count {
                if local.reader_locks[slot_index as usize] > 0 {
                    continue;
                }
                if !self.uses_linux_ofd_locking()
                    && process_local.reader_owner(slot_index).is_some()
                {
                    continue;
                }
                let word_idx = (slot_index >> 6) as usize;
                let bit = slot_index & 63;
                let mask = 1u64 << bit;
                let word = &bitmap[word_idx];
                let mut current = word.load(Ordering::Acquire);
                while current & mask != 0 {
                    let desired = current & !mask;
                    match word.compare_exchange_weak(
                        current,
                        desired,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            if self.uses_linux_ofd_locking() {
                                let offset = Self::process_lock_offset_for_reader(slot_index);
                                match self.file.shared_wal_try_lock_byte(
                                    offset,
                                    true,
                                    self.lock_kind(),
                                ) {
                                    Ok(true) => {
                                        local.reader_locks[slot_index as usize] += 1;
                                        self.reader_owners()[slot_index as usize]
                                            .store(owner.raw(), Ordering::Release);
                                        self.reader_frames()[slot_index as usize]
                                            .store(max_frame, Ordering::Release);
                                        drop(process_local);
                                        drop(local);
                                        return Some(SharedReaderSlot {
                                            slot_index,
                                            max_frame,
                                            owner,
                                        });
                                    }
                                    Ok(false) | Err(_) => {
                                        word.fetch_or(mask, Ordering::Release);
                                        break;
                                    }
                                }
                            } else {
                                if !process_local.try_register_reader(slot_index, owner) {
                                    word.fetch_or(mask, Ordering::Release);
                                    break;
                                }
                                if self.reader_owners()[slot_index as usize]
                                    .compare_exchange(
                                        UNOWNED_LOCK,
                                        owner.raw(),
                                        Ordering::AcqRel,
                                        Ordering::Acquire,
                                    )
                                    .is_err()
                                {
                                    process_local.unregister_reader(slot_index, owner);
                                    word.fetch_or(mask, Ordering::Release);
                                    break;
                                }
                                let offset = Self::process_lock_offset_for_reader(slot_index);
                                if !self.try_acquire_supplemental_byte_lock(offset) {
                                    Self::release_shared_owner_slot(
                                        &self.reader_owners()[slot_index as usize],
                                        owner,
                                        "reader slot",
                                    );
                                    process_local.unregister_reader(slot_index, owner);
                                    word.fetch_or(mask, Ordering::Release);
                                    break;
                                }
                                local.reader_locks[slot_index as usize] += 1;
                                self.reader_frames()[slot_index as usize]
                                    .store(max_frame, Ordering::Release);
                                drop(process_local);
                                drop(local);
                                return Some(SharedReaderSlot {
                                    slot_index,
                                    max_frame,
                                    owner,
                                });
                            }
                        }
                        Err(actual) => current = actual,
                    }
                }
            }
            drop(process_local);
            drop(local);
            if attempt == 0 {
                self.reclaim_stale_reader_slots();
            }
        }
        None
    }

    pub(crate) fn update_reader(&self, slot: SharedReaderSlot, max_frame: u64) -> SharedReaderSlot {
        turso_assert!(
            self.reader_owners()[slot.slot_index as usize].load(Ordering::Acquire)
                == slot.owner.raw(),
            "reader slot updated by non-owner"
        );
        self.reader_frames()[slot.slot_index as usize].store(max_frame, Ordering::Release);
        SharedReaderSlot {
            slot_index: slot.slot_index,
            max_frame,
            owner: slot.owner,
        }
    }

    pub(crate) fn register_reader_for_snapshot(
        &self,
        owner: SharedOwnerRecord,
        max_frame: u64,
    ) -> Option<SharedReaderSlot> {
        {
            let mut process_local = self
                .process_local_ownership
                .lock()
                .expect("process-local ownership registry poisoned");
            if let Some(slot) = process_local.retain_shared_snapshot_reader(max_frame) {
                return Some(slot);
            }
        }

        let slot = self.register_reader(owner, max_frame)?;
        let mut process_local = self
            .process_local_ownership
            .lock()
            .expect("process-local ownership registry poisoned");
        if let Some(existing_slot) = process_local.shared_snapshot_reader(max_frame) {
            let retained = process_local.retain_shared_snapshot_reader(max_frame);
            turso_assert!(
                retained == Some(existing_slot.slot),
                "shared snapshot retention should return the published slot"
            );
            drop(process_local);
            self.unregister_reader(slot);
            return Some(existing_slot.slot);
        }
        process_local.publish_shared_snapshot_reader(slot);
        Some(slot)
    }

    /// Release a reader slot previously acquired by `register_reader`.
    ///
    /// Asserts that the current shared-memory owner matches `slot.owner` —
    /// a mismatch means another process reclaimed this slot while we still
    /// thought we held it, which is a correctness bug in the coordination
    /// layer (see `repair_transient_state_for_exclusive_open`).
    pub(crate) fn unregister_reader(&self, slot: SharedReaderSlot) {
        let mut local = self
            .local_lock_state
            .lock()
            .expect("shared WAL local lock state poisoned");
        turso_assert!(
            local.reader_locks[slot.slot_index as usize] > 0,
            "reader registry count underflow"
        );
        let current_owner = self.reader_owners()[slot.slot_index as usize].load(Ordering::Acquire);
        turso_assert!(
            current_owner == slot.owner.raw(),
            "reader slot released by non-owner",
            { "slot_index": slot.slot_index, "expected_owner": slot.owner.raw(), "current_owner": current_owner, "local_reader_count": local.reader_locks[slot.slot_index as usize] }
        );
        if self.uses_linux_ofd_locking() {
            self.file
                .shared_wal_unlock_byte(
                    Self::process_lock_offset_for_reader(slot.slot_index),
                    self.lock_kind(),
                )
                .expect("failed to release shared WAL reader slot lock");
        } else {
            self.with_process_local_ownership(|entry| {
                entry.unregister_reader(slot.slot_index, slot.owner)
            });
            self.release_supplemental_byte_lock(Self::process_lock_offset_for_reader(
                slot.slot_index,
            ));
            Self::release_shared_owner_slot(
                &self.reader_owners()[slot.slot_index as usize],
                slot.owner,
                "reader slot",
            );
        }
        local.reader_locks[slot.slot_index as usize] -= 1;
        self.reader_frames()[slot.slot_index as usize]
            .store(UNUSED_READER_FRAME, Ordering::Release);
        if self.uses_linux_ofd_locking() {
            self.reader_owners()[slot.slot_index as usize].store(UNOWNED_LOCK, Ordering::Release);
        }
        let word_idx = (slot.slot_index >> 6) as usize;
        let bit = slot.slot_index & 63;
        self.reader_bitmap_words()[word_idx].fetch_or(1u64 << bit, Ordering::Release);
    }

    pub(crate) fn unregister_reader_for_snapshot(&self, slot: SharedReaderSlot) {
        let release_slot = {
            let mut process_local = self
                .process_local_ownership
                .lock()
                .expect("process-local ownership registry poisoned");
            process_local.release_shared_snapshot_reader(slot)
        };
        if release_slot {
            self.unregister_reader(slot);
        }
    }

    pub(crate) fn reader_owner(&self, slot_index: u32) -> Option<SharedOwnerRecord> {
        SharedOwnerRecord::from_raw(
            self.reader_owners()[slot_index as usize].load(Ordering::Acquire),
        )
    }

    /// Return the smallest `max_frame` across all live reader slots, or `None`
    /// if no readers are active.
    ///
    /// Checkpoints use this to determine the safe backfill boundary: frames
    /// above the minimum active reader's mark cannot be checkpointed because
    /// that reader may still need to read the old page from the DB file.
    ///
    /// **Side-effect**: for each slot whose owner is detected as dead (OFD
    /// lock can be acquired, or PID is no longer alive), the slot is reclaimed
    /// inline and excluded from the result.
    pub(crate) fn min_active_reader_frame(&self) -> Option<u64> {
        self.reader_frames()
            .iter()
            .enumerate()
            .filter_map(|(slot_index, frame)| {
                if !self.uses_linux_ofd_locking() {
                    let owner = self.reader_owner(slot_index as u32)?;
                    let frame = frame.load(Ordering::Acquire);
                    if frame == UNUSED_READER_FRAME {
                        return None;
                    }
                    let local_owner = self.with_process_local_ownership(|entry| {
                        entry.reader_owner(slot_index as u32)
                    });
                    if local_owner.is_some() {
                        return Some(frame);
                    }
                    if pid_is_alive(owner.pid()) {
                        return Some(frame);
                    }
                    let _ = self.try_reclaim_dead_reader_owner(slot_index as u32, owner);
                    return None;
                }
                let frame = frame.load(Ordering::Acquire);
                if frame == UNUSED_READER_FRAME {
                    return None;
                }
                let local_lock_held =
                    self.with_local_lock_state(|entry| entry.reader_locks[slot_index] > 0);
                if local_lock_held {
                    return Some(frame);
                }
                let offset = Self::process_lock_offset_for_reader(slot_index as u32);
                match self
                    .file
                    .shared_wal_try_lock_byte(offset, true, self.lock_kind())
                {
                    Ok(true) => {
                        self.reader_frames()[slot_index]
                            .store(UNUSED_READER_FRAME, Ordering::Release);
                        self.reader_owners()[slot_index].store(UNOWNED_LOCK, Ordering::Release);
                        let word_idx = slot_index >> 6;
                        let bit = slot_index & 63;
                        self.reader_bitmap_words()[word_idx]
                            .fetch_or(1u64 << bit, Ordering::Release);
                        self.file
                            .shared_wal_unlock_byte(offset, self.lock_kind())
                            .expect("failed to release reclaimed reader slot lock");
                        None
                    }
                    Ok(false) => Some(frame),
                    Err(err) => panic!("failed probing shared WAL reader slot lock: {err}"),
                }
            })
            .min()
    }

    /// Append a (page_id, frame_id) entry to the shared frame index.
    ///
    /// Called by the writer after each WAL frame is written. The frame index
    /// is an append-only log of page→frame mappings that grows in fixed-size
    /// blocks. Readers use it to find the latest WAL frame for a given page
    /// without scanning the WAL file.
    ///
    /// The entry is written behind `frame_index_publish_lock`, and the
    /// `frame_index_len` counter is bumped with Release ordering only after
    /// the payload is fully written, so readers never observe a half-written slot.
    ///
    /// When frame_id == 1 and max_frame == 0 (WAL just restarted), the index
    /// is reset to empty before appending.
    #[track_caller]
    pub(crate) fn record_frame(&self, page_id: u64, frame_id: u64) {
        let _publish_guard = self
            .frame_index_publish_lock
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let header = self.header();
        if frame_id == 1 && header.max_frame.load(Ordering::Acquire) == 0 {
            header.frame_index_len.store(0, Ordering::Release);
            header.frame_index_overflowed.store(0, Ordering::Release);
        }
        let slot = loop {
            let len = header.frame_index_len.load(Ordering::Acquire);
            if len >= header.frame_index_capacity {
                // The shared index has exhausted every reserved block.
                header.frame_index_overflowed.store(1, Ordering::Release);
                return;
            }
            let blocks = header.frame_index_blocks.load(Ordering::Acquire);
            let current_capacity = blocks
                .checked_mul(header.frame_index_block_capacity)
                .expect("shared WAL frame index capacity overflow");
            if len >= current_capacity {
                if blocks >= header.frame_index_max_blocks {
                    header.frame_index_overflowed.store(1, Ordering::Release);
                    return;
                }
                if !self.try_grow_frame_index_blocks(blocks + 1) {
                    header.frame_index_overflowed.store(1, Ordering::Release);
                    return;
                }
                continue;
            }
            break len;
        };
        let required_blocks = (slot / FRAME_INDEX_BLOCK_CAPACITY) + 1;
        self.ensure_mapped_frame_index_blocks(required_blocks)
            .expect("shared WAL frame index block missing");
        let mappings = self
            .frame_index_blocks
            .read()
            .expect("shared WAL frame index block lock poisoned");
        if slot > 0 {
            let previous = Self::frame_index_entry(&mappings, slot - 1);
            assert!(
                frame_id > previous.frame_id,
                "shared WAL frame ids must increase monotonically: new_frame_id={}, previous_frame_id={}, slot={}, shared_max_frame={}",
                frame_id,
                previous.frame_id,
                slot,
                header.max_frame.load(Ordering::Acquire)
            );
        }
        let entry = Self::frame_index_entry_ptr(&mappings, slot);
        let block_index = slot / FRAME_INDEX_BLOCK_CAPACITY;
        let local_index = slot % FRAME_INDEX_BLOCK_CAPACITY;
        if local_index == 0 {
            Self::clear_frame_index_block_hash(&mappings[block_index as usize]);
        }
        unsafe {
            std::ptr::addr_of_mut!((*entry).page_id).write(page_id);
            std::ptr::addr_of_mut!((*entry).frame_id).write(frame_id);
        }
        Self::insert_frame_index_block_hash(&mappings[block_index as usize], local_index, page_id);
        // Publish the new entry only after its payload is fully written, so
        // readers that synchronize via frame_index_len never observe an
        // uninitialized slot.
        turso_assert!(
            header
                .frame_index_len
                .compare_exchange(slot, slot + 1, Ordering::Release, Ordering::Acquire)
                .is_ok(),
            "shared WAL frame index length changed while publishing an entry"
        );
    }

    /// Truncate the shared frame index to only contain entries with
    /// `frame_id <= max_frame`. Called during WAL restart (max_frame=0 to
    /// clear the entire index) and by `install_snapshot` to trim stale
    /// entries from a previous WAL generation.
    pub(crate) fn rollback_frames(&self, max_frame: u64) {
        let _publish_guard = self
            .frame_index_publish_lock
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let header = self.header();
        let len = header
            .frame_index_len
            .load(Ordering::Acquire)
            .min(header.frame_index_capacity);
        if len == 0 {
            return;
        }
        let old_blocks = len.div_ceil(FRAME_INDEX_BLOCK_CAPACITY);
        self.ensure_mapped_frame_index_blocks(old_blocks)
            .expect("shared WAL frame index block missing");
        let mappings = self
            .frame_index_blocks
            .read()
            .expect("shared WAL frame index block lock poisoned");
        let mut new_len = len;
        while new_len > 0 {
            let last = Self::frame_index_entry(&mappings, new_len - 1);
            if last.frame_id <= max_frame {
                break;
            }
            new_len -= 1;
        }
        if new_len == len {
            return;
        }
        header.frame_index_len.store(new_len, Ordering::Release);
        if new_len == 0 {
            return;
        }
        let retained_entries = new_len % FRAME_INDEX_BLOCK_CAPACITY;
        if retained_entries != 0 {
            let retained_block = (new_len - 1) / FRAME_INDEX_BLOCK_CAPACITY;
            Self::rebuild_frame_index_block_hash(
                &mappings[retained_block as usize],
                retained_entries,
            );
        }
    }

    /// Look up the latest WAL frame containing `page_id` within the visible
    /// range. Returns `None` if the page has no entry in the frame index
    /// (caller should read from the DB file instead).
    ///
    /// `min_frame..=max_frame` is the connection's WAL window (derived from
    /// the snapshot taken at `begin_read_tx`). `frame_watermark` optionally
    /// narrows the upper bound for MVCC snapshot reads.
    ///
    /// Uses per-block hash tables for O(1) lookup within each block, scanning
    /// blocks in reverse order so the most recent entry wins.
    pub(crate) fn find_frame(
        &self,
        page_id: u64,
        min_frame: u64,
        max_frame: u64,
        frame_watermark: Option<u64>,
    ) -> Option<u64> {
        let upper_frame = frame_watermark.unwrap_or(max_frame);
        if upper_frame < min_frame {
            return None;
        }
        let range = frame_watermark
            .map(|watermark| 0..=watermark)
            .unwrap_or(min_frame..=max_frame);
        let header = self.header();
        let len = header
            .frame_index_len
            .load(Ordering::Acquire)
            .min(header.frame_index_capacity);
        if len == 0 {
            return None;
        }
        let required_blocks = len.div_ceil(FRAME_INDEX_BLOCK_CAPACITY);
        self.ensure_mapped_frame_index_blocks(required_blocks)
            .expect("shared WAL frame index block missing");
        let mappings = self
            .frame_index_blocks
            .read()
            .expect("shared WAL frame index block lock poisoned");
        let visible_slots = Self::visible_frame_index_slots(&mappings, len, upper_frame);
        if visible_slots == 0 {
            return None;
        }
        let last_block = (visible_slots - 1) / FRAME_INDEX_BLOCK_CAPACITY;
        for block_index in (0..=last_block).rev() {
            let block_start_slot = block_index * FRAME_INDEX_BLOCK_CAPACITY;
            let visible_entries = visible_slots
                .saturating_sub(block_start_slot)
                .min(FRAME_INDEX_BLOCK_CAPACITY);
            if let Some(local_entry) =
                Self::find_frame_in_block(&mappings[block_index as usize], page_id, visible_entries)
            {
                let slot = block_start_slot + local_entry;
                let frame_id = Self::frame_index_entry(&mappings, slot).frame_id;
                if range.contains(&frame_id) {
                    return Some(frame_id);
                }
            }
        }
        None
    }

    /// Return the latest (page_id, frame_id) for every distinct page that has
    /// at least one frame index entry in `min_frame..=max_frame`.
    ///
    /// Used by checkpoint to determine which pages need to be copied from the
    /// WAL to the DB file. For each page, only the highest-numbered frame is
    /// returned (that frame contains the most recent version of the page).
    pub(crate) fn iter_latest_frames(&self, min_frame: u64, max_frame: u64) -> Vec<(u64, u64)> {
        let header = self.header();
        let len = header
            .frame_index_len
            .load(Ordering::Acquire)
            .min(header.frame_index_capacity);
        if len == 0 {
            return Vec::new();
        }
        let required_blocks = len.div_ceil(FRAME_INDEX_BLOCK_CAPACITY);
        self.ensure_mapped_frame_index_blocks(required_blocks)
            .expect("shared WAL frame index block missing");
        let mappings = self
            .frame_index_blocks
            .read()
            .expect("shared WAL frame index block lock poisoned");
        let visible_slots = Self::visible_frame_index_slots(&mappings, len, max_frame);
        if visible_slots == 0 {
            return Vec::new();
        }
        let mut seen_pages = std::collections::BTreeSet::new();
        let mut entries = Vec::new();
        let last_block = (visible_slots - 1) / FRAME_INDEX_BLOCK_CAPACITY;
        for block_index in (0..=last_block).rev() {
            let block_start_slot = block_index * FRAME_INDEX_BLOCK_CAPACITY;
            let visible_entries = visible_slots
                .saturating_sub(block_start_slot)
                .min(FRAME_INDEX_BLOCK_CAPACITY);
            let latest_in_block =
                Self::latest_entries_in_block(&mappings[block_index as usize], visible_entries);
            for (page_id, local_index) in latest_in_block {
                if !seen_pages.insert(page_id) {
                    continue;
                }
                let slot = block_start_slot + local_index;
                let frame_id = Self::frame_index_entry(&mappings, slot).frame_id;
                if (min_frame..=max_frame).contains(&frame_id) {
                    entries.push((page_id, frame_id));
                }
            }
        }
        entries.sort_unstable_by_key(|&(page_id, _)| page_id);
        entries
    }

    fn base_mapped_len(reader_slot_count: u32) -> usize {
        let reader_bitmap_words = (reader_slot_count / 64) as usize;
        let raw_len = size_of::<SharedWalCoordinationMapHeader>()
            + reader_bitmap_words * size_of::<AtomicU64>()
            + reader_slot_count as usize * size_of::<AtomicU64>()
            + reader_slot_count as usize * size_of::<AtomicU64>()
            + reader_bitmap_words * size_of::<AtomicU64>()
            + reader_slot_count as usize * size_of::<AtomicU64>();
        raw_len.div_ceil(SHARED_WAL_COORDINATION_MAP_ALIGNMENT)
            * SHARED_WAL_COORDINATION_MAP_ALIGNMENT
    }

    fn frame_index_block_entry_bytes() -> usize {
        FRAME_INDEX_BLOCK_CAPACITY as usize * size_of::<SharedWalFrameIndexEntry>()
    }

    fn frame_index_block_hash_bytes() -> usize {
        FRAME_INDEX_BLOCK_HASH_SLOTS as usize * size_of::<u16>()
    }

    fn frame_index_block_byte_len() -> usize {
        Self::frame_index_block_entry_bytes() + Self::frame_index_block_hash_bytes()
    }

    fn file_len_for_blocks(reader_slot_count: u32, blocks: u32) -> usize {
        Self::base_mapped_len(reader_slot_count)
            + blocks as usize * Self::frame_index_block_byte_len()
    }

    fn header(&self) -> &SharedWalCoordinationMapHeader {
        unsafe {
            &*self
                .base_ptr
                .as_ptr()
                .cast::<SharedWalCoordinationMapHeader>()
        }
    }

    fn reader_bitmap_words(&self) -> &[AtomicU64] {
        let header = self.header();
        let ptr = unsafe {
            self.base_ptr
                .as_ptr()
                .add(size_of::<SharedWalCoordinationMapHeader>())
                .cast::<AtomicU64>()
        };
        unsafe { std::slice::from_raw_parts(ptr, header.reader_bitmap_word_count as usize) }
    }

    fn reader_frames(&self) -> &[AtomicU64] {
        let header = self.header();
        let ptr = unsafe {
            self.base_ptr
                .as_ptr()
                .add(size_of::<SharedWalCoordinationMapHeader>())
                .add(header.reader_bitmap_word_count as usize * size_of::<AtomicU64>())
                .cast::<AtomicU64>()
        };
        unsafe { std::slice::from_raw_parts(ptr, header.reader_slot_count as usize) }
    }

    fn reader_owners(&self) -> &[AtomicU64] {
        let header = self.header();
        let ptr = unsafe {
            self.base_ptr
                .as_ptr()
                .add(size_of::<SharedWalCoordinationMapHeader>())
                .add(header.reader_bitmap_word_count as usize * size_of::<AtomicU64>())
                .add(header.reader_slot_count as usize * size_of::<AtomicU64>())
                .cast::<AtomicU64>()
        };
        unsafe { std::slice::from_raw_parts(ptr, header.reader_slot_count as usize) }
    }

    fn map_frame_index_block(&self, block_index: u32) -> Result<FrameIndexBlockMapping> {
        let offset = (Self::base_mapped_len(self.header().reader_slot_count)
            + block_index as usize * Self::frame_index_block_byte_len())
            as u64;
        let byte_len = Self::frame_index_block_byte_len();
        let mapping = self.file.shared_wal_map(offset, byte_len)?;
        let ptr = mapping.ptr();
        let entries_ptr = ptr.cast::<SharedWalFrameIndexEntry>();
        let hash_ptr = NonNull::new(unsafe {
            ptr.as_ptr()
                .add(Self::frame_index_block_entry_bytes())
                .cast::<u16>()
        })
        .expect("mmap returned null");
        Ok(FrameIndexBlockMapping {
            _mapping: mapping,
            entries_ptr,
            hash_ptr,
            byte_len,
        })
    }

    fn ensure_mapped_frame_index_blocks(&self, target_blocks: u32) -> Result<()> {
        let mut mappings = self
            .frame_index_blocks
            .write()
            .expect("shared WAL frame index block lock poisoned");
        while mappings.len() < target_blocks as usize {
            let block_index = mappings.len() as u32;
            mappings.push(self.map_frame_index_block(block_index)?);
        }
        Ok(())
    }

    fn try_grow_frame_index_blocks(&self, target_blocks: u32) -> bool {
        let target_len = Self::file_len_for_blocks(self.header().reader_slot_count, target_blocks);
        if self.file.shared_wal_set_len(target_len as u64).is_err() {
            return false;
        }
        if self
            .ensure_mapped_frame_index_blocks(target_blocks)
            .is_err()
        {
            return false;
        }
        self.header()
            .frame_index_blocks
            .store(target_blocks, Ordering::Release);
        true
    }

    fn frame_index_entry(
        mappings: &[FrameIndexBlockMapping],
        slot: u32,
    ) -> SharedWalFrameIndexEntry {
        unsafe { *Self::frame_index_entry_ptr(mappings, slot) }
    }

    fn frame_index_entry_ptr(
        mappings: &[FrameIndexBlockMapping],
        slot: u32,
    ) -> *mut SharedWalFrameIndexEntry {
        let block_index = (slot / FRAME_INDEX_BLOCK_CAPACITY) as usize;
        let entry_index = (slot % FRAME_INDEX_BLOCK_CAPACITY) as usize;
        unsafe { mappings[block_index].entries_ptr.as_ptr().add(entry_index) }
    }

    #[allow(clippy::mut_from_ref)]
    fn frame_index_block_hash_slice(mapping: &FrameIndexBlockMapping) -> &mut [u16] {
        unsafe {
            std::slice::from_raw_parts_mut(
                mapping.hash_ptr.as_ptr(),
                FRAME_INDEX_BLOCK_HASH_SLOTS as usize,
            )
        }
    }

    fn clear_frame_index_block_hash(mapping: &FrameIndexBlockMapping) {
        Self::frame_index_block_hash_slice(mapping).fill(0);
    }

    /// sqlite wal.c:
    /// ** To look for page P in the hash table, first compute a hash iKey on
    /// ** P as follows:
    /// ** iKey = (P * 383) % HASHTABLE_NSLOT
    fn hash_page_id(page_id: u64) -> usize {
        page_id
            .wrapping_mul(383)
            .rem_euclid(FRAME_INDEX_BLOCK_HASH_SLOTS as u64) as usize
    }

    fn insert_frame_index_block_hash(
        mapping: &FrameIndexBlockMapping,
        local_index: u32,
        page_id: u64,
    ) {
        turso_assert!(
            local_index < FRAME_INDEX_BLOCK_CAPACITY,
            "frame index block local index out of range"
        );
        let hash = Self::frame_index_block_hash_slice(mapping);
        let mut slot = Self::hash_page_id(page_id);
        let value = (local_index + 1) as u16;
        for _ in 0..FRAME_INDEX_BLOCK_HASH_SLOTS {
            if hash[slot] == 0 {
                hash[slot] = value;
                return;
            }
            slot = (slot + 1) % FRAME_INDEX_BLOCK_HASH_SLOTS as usize;
        }
        panic!("shared WAL frame index block hash table is full");
    }

    fn rebuild_frame_index_block_hash(mapping: &FrameIndexBlockMapping, visible_entries: u32) {
        turso_assert!(
            visible_entries <= FRAME_INDEX_BLOCK_CAPACITY,
            "visible block entries out of range"
        );
        Self::clear_frame_index_block_hash(mapping);
        for local_index in 0..visible_entries {
            let entry = unsafe { *mapping.entries_ptr.as_ptr().add(local_index as usize) };
            Self::insert_frame_index_block_hash(mapping, local_index, entry.page_id);
        }
    }

    fn find_frame_in_block(
        mapping: &FrameIndexBlockMapping,
        page_id: u64,
        visible_entries: u32,
    ) -> Option<u32> {
        if visible_entries == 0 {
            return None;
        }
        let hash = unsafe {
            std::slice::from_raw_parts(
                mapping.hash_ptr.as_ptr(),
                FRAME_INDEX_BLOCK_HASH_SLOTS as usize,
            )
        };
        let mut slot = Self::hash_page_id(page_id);
        let mut latest = None;
        for _ in 0..FRAME_INDEX_BLOCK_HASH_SLOTS {
            let local_plus_one = hash[slot];
            if local_plus_one == 0 {
                break;
            }
            let local_index = local_plus_one as u32 - 1;
            if local_index >= visible_entries {
                break;
            }
            let entry = unsafe { *mapping.entries_ptr.as_ptr().add(local_index as usize) };
            if entry.page_id == page_id {
                latest = Some(local_index);
            }
            slot = (slot + 1) % FRAME_INDEX_BLOCK_HASH_SLOTS as usize;
        }
        latest
    }

    fn latest_entries_in_block(
        mapping: &FrameIndexBlockMapping,
        visible_entries: u32,
    ) -> HashMap<u64, u32> {
        let hash = unsafe {
            std::slice::from_raw_parts(
                mapping.hash_ptr.as_ptr(),
                FRAME_INDEX_BLOCK_HASH_SLOTS as usize,
            )
        };
        let mut latest_entries: HashMap<u64, u32> = HashMap::new();
        for &local_plus_one in hash {
            if local_plus_one == 0 {
                continue;
            }
            let local_index = local_plus_one as u32 - 1;
            if local_index >= visible_entries {
                continue;
            }
            let entry = unsafe { *mapping.entries_ptr.as_ptr().add(local_index as usize) };
            latest_entries
                .entry(entry.page_id)
                .and_modify(|latest| *latest = (*latest).max(local_index))
                .or_insert(local_index);
        }
        latest_entries
    }

    /// Binary-search the frame index to find how many entries have
    /// `frame_id <= max_frame`. Since frame IDs are monotonically increasing,
    /// this gives the number of slots visible to a reader whose snapshot
    /// caps out at `max_frame`.
    fn visible_frame_index_slots(
        mappings: &[FrameIndexBlockMapping],
        len: u32,
        max_frame: u64,
    ) -> u32 {
        let mut low = 0;
        let mut high = len;
        while low < high {
            let mid = low + (high - low) / 2;
            let frame_id = Self::frame_index_entry(mappings, mid).frame_id;
            if frame_id <= max_frame {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }

    fn initialize(&self, reader_slot_count: u32) {
        let header = self
            .base_ptr
            .as_ptr()
            .cast::<SharedWalCoordinationMapHeader>();
        unsafe {
            std::ptr::addr_of_mut!((*header).magic).write(SHARED_WAL_COORDINATION_MAGIC);
            std::ptr::addr_of_mut!((*header).version).write(SHARED_WAL_COORDINATION_VERSION);
            std::ptr::addr_of_mut!((*header).reader_slot_count).write(reader_slot_count);
            std::ptr::addr_of_mut!((*header).reader_bitmap_word_count)
                .write(reader_slot_count / 64);
            std::ptr::addr_of_mut!((*header).frame_index_block_capacity)
                .write(FRAME_INDEX_BLOCK_CAPACITY);
            std::ptr::addr_of_mut!((*header).frame_index_block_hash_slots)
                .write(FRAME_INDEX_BLOCK_HASH_SLOTS);
            std::ptr::addr_of_mut!((*header).frame_index_max_blocks).write(MAX_FRAME_INDEX_BLOCKS);
            std::ptr::addr_of_mut!((*header).frame_index_blocks)
                .write(AtomicU32::new(INITIAL_FRAME_INDEX_BLOCKS));
            std::ptr::addr_of_mut!((*header).frame_index_capacity).write(MAX_FRAME_INDEX_CAPACITY);
            std::ptr::addr_of_mut!((*header).frame_index_len).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).frame_index_overflowed).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_version).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_nbackfills).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_max_frame).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_checkpoint_seq)
                .write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_page_size).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_salt_1).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_salt_2).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_checksum_1).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_checksum_2).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_db_size_pages).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_db_header_crc32c)
                .write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).backfill_proof_crc32c).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*header).writer_owner).write(AtomicU64::new(UNOWNED_LOCK));
            std::ptr::addr_of_mut!((*header).checkpoint_owner).write(AtomicU64::new(UNOWNED_LOCK));
        }
        // A freshly truncated file-backed mapping is already zero-filled. Avoid
        // touching the full reserved region here, because large sparse `.tshm`
        // files can SIGBUS on tmpfs/quota-constrained test environments if we
        // eagerly fault in every page up front.
        for word in self.reader_bitmap_words() {
            word.store(u64::MAX, Ordering::Release);
        }
        for frame in self.reader_frames() {
            frame.store(UNUSED_READER_FRAME, Ordering::Release);
        }
        for owner in self.reader_owners() {
            owner.store(UNOWNED_LOCK, Ordering::Release);
        }
    }

    fn validate_existing(
        &self,
        expected_reader_slot_count: u32,
        metadata_len: usize,
    ) -> Result<()> {
        let header = self.header();
        if header.magic != SHARED_WAL_COORDINATION_MAGIC {
            return Err(LimboError::Corrupt(
                "shared WAL coordination map magic mismatch".into(),
            ));
        }
        if header.version != SHARED_WAL_COORDINATION_VERSION {
            return Err(LimboError::Corrupt(format!(
                "unsupported shared WAL coordination map version: {}",
                header.version
            )));
        }
        if header.reader_slot_count != expected_reader_slot_count {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination map slot count mismatch: got {}, expected {}",
                header.reader_slot_count, expected_reader_slot_count
            )));
        }
        if header.frame_index_block_capacity != FRAME_INDEX_BLOCK_CAPACITY {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination map frame index block capacity mismatch: got {}, expected {}",
                header.frame_index_block_capacity, FRAME_INDEX_BLOCK_CAPACITY
            )));
        }
        if header.frame_index_block_hash_slots != FRAME_INDEX_BLOCK_HASH_SLOTS {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination map frame index hash slot count mismatch: got {}, expected {}",
                header.frame_index_block_hash_slots, FRAME_INDEX_BLOCK_HASH_SLOTS
            )));
        }
        if header.frame_index_max_blocks != MAX_FRAME_INDEX_BLOCKS {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination map frame index max blocks mismatch: got {}, expected {}",
                header.frame_index_max_blocks, MAX_FRAME_INDEX_BLOCKS
            )));
        }
        let blocks = header.frame_index_blocks.load(Ordering::Acquire);
        if !(INITIAL_FRAME_INDEX_BLOCKS..=MAX_FRAME_INDEX_BLOCKS).contains(&blocks) {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination map frame index block count out of range: {blocks}"
            )));
        }
        if header.frame_index_capacity != MAX_FRAME_INDEX_CAPACITY {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination map frame index capacity mismatch: got {}, expected {}",
                header.frame_index_capacity, MAX_FRAME_INDEX_CAPACITY
            )));
        }
        if header.frame_index_len.load(Ordering::Acquire) > header.frame_index_capacity {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination map frame index length exceeds capacity: len={}, capacity={}",
                header.frame_index_len.load(Ordering::Acquire),
                header.frame_index_capacity
            )));
        }
        let current_capacity = blocks
            .checked_mul(header.frame_index_block_capacity)
            .expect("shared WAL frame index capacity overflow");
        if header.frame_index_len.load(Ordering::Acquire) > current_capacity {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination map frame index length exceeds active block capacity: len={}, capacity={}",
                header.frame_index_len.load(Ordering::Acquire),
                current_capacity
            )));
        }
        let expected_file_len = Self::file_len_for_blocks(expected_reader_slot_count, blocks);
        if metadata_len != expected_file_len {
            return Err(LimboError::Corrupt(format!(
                "shared WAL coordination file has unexpected size: got {metadata_len}, expected {expected_file_len}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{OpenFlags, PlatformIO, IO};
    use crate::storage::database::DatabaseFile;
    use crate::storage::wal::{CheckpointMode, CheckpointResult};
    use crate::types::Value;
    use crate::util::IOExt;
    use crate::{Connection, Database, DatabaseOpts, IOResult, OpenDbAsyncState, Result, SyncMode};
    use std::sync::Arc;

    fn colliding_page_ids(count: usize) -> Vec<u64> {
        turso_assert!(count > 0, "must request at least one colliding page id");
        let target_bucket = MappedSharedWalCoordination::hash_page_id(1);
        let mut page_ids = Vec::with_capacity(count);
        let mut candidate = 1u64;
        while page_ids.len() < count {
            if MappedSharedWalCoordination::hash_page_id(candidate) == target_bucket {
                page_ids.push(candidate);
            }
            candidate += 1;
        }
        page_ids
    }

    fn test_shared_wal_io() -> Arc<dyn IO> {
        Arc::new(PlatformIO::new().unwrap())
    }

    fn create_mapping(path: &Path) -> MappedSharedWalCoordination {
        MappedSharedWalCoordination::create_or_open(&test_shared_wal_io(), path, 64).unwrap()
    }

    fn create_process_scoped_mapping(path: &Path) -> MappedSharedWalCoordination {
        MappedSharedWalCoordination::create_or_open_process_scoped_for_tests(
            &test_shared_wal_io(),
            path,
            64,
        )
        .unwrap()
    }

    fn exited_child_pid() -> u32 {
        let child = unsafe { libc::fork() };
        assert!(child >= 0, "fork failed");
        if child == 0 {
            unsafe { libc::_exit(0) };
        }
        let mut status: libc::c_int = 0;
        let waited = unsafe { libc::waitpid(child, &mut status, 0) };
        assert_eq!(waited, child, "waitpid failed");
        assert!(libc::WIFEXITED(status), "child did not exit cleanly");
        child as u32
    }

    #[test]
    fn shared_wal_coordination_header_round_trips() {
        let header = SharedWalCoordinationHeader {
            max_frame: 11,
            nbackfills: 7,
            transaction_count: 13,
            visibility_generation: 17,
            checkpoint_seq: 19,
            checkpoint_epoch: 23,
            page_size: 4096,
            salt_1: 29,
            salt_2: 31,
            checksum_1: 37,
            checksum_2: 41,
            reader_slot_count: 64,
        };

        let encoded = header.encode();
        let decoded = SharedWalCoordinationHeader::decode(&encoded).unwrap();

        assert_eq!(decoded.max_frame, header.max_frame);
        assert_eq!(decoded.nbackfills, header.nbackfills);
        assert_eq!(decoded.transaction_count, header.transaction_count);
        assert_eq!(decoded.visibility_generation, header.visibility_generation);
        assert_eq!(decoded.checkpoint_seq, header.checkpoint_seq);
        assert_eq!(decoded.checkpoint_epoch, header.checkpoint_epoch);
        assert_eq!(decoded.page_size, header.page_size);
        assert_eq!(decoded.salt_1, header.salt_1);
        assert_eq!(decoded.salt_2, header.salt_2);
        assert_eq!(decoded.checksum_1, header.checksum_1);
        assert_eq!(decoded.checksum_2, header.checksum_2);
        assert_eq!(decoded.reader_slot_count, header.reader_slot_count);
    }

    #[test]
    fn shared_wal_coordination_header_rejects_invalid_magic() {
        let mut encoded = [0u8; SharedWalCoordinationHeader::BYTE_LEN];
        encoded[0..8].copy_from_slice(b"badmagic");

        let err = SharedWalCoordinationHeader::decode(&encoded).unwrap_err();
        assert!(matches!(err, LimboError::Corrupt(_)));
    }

    #[test]
    fn shared_owner_record_round_trips_pid_and_instance() {
        let owner = SharedOwnerRecord::new(17, 23);

        assert_eq!(owner.pid(), 17);
        assert_eq!(owner.instance_id(), 23);
        assert_eq!(SharedOwnerRecord::from_raw(owner.raw()), Some(owner));
        assert_eq!(SharedOwnerRecord::from_raw(UNOWNED_LOCK), None);
    }

    #[test]
    fn process_local_ownership_state_tracks_same_process_exclusion() {
        let owner_a = SharedOwnerRecord::new(11, 1);
        let owner_b = SharedOwnerRecord::new(11, 2);
        let mut state = ProcessLocalOwnershipState::new(64);

        assert!(state.try_acquire_writer(owner_a));
        assert!(!state.try_acquire_writer(owner_b));
        state.release_writer(owner_a);
        assert!(state.try_acquire_writer(owner_b));
        state.release_writer(owner_b);

        assert!(state.try_acquire_checkpoint(owner_a));
        assert!(!state.try_acquire_checkpoint(owner_b));
        state.release_checkpoint(owner_a);
        assert!(state.try_register_reader(7, owner_a));
        assert!(!state.try_register_reader(7, owner_b));
        assert_eq!(state.reader_owner(7), Some(owner_a));
        state.unregister_reader(7, owner_a);
        assert_eq!(state.reader_owner(7), None);
    }

    #[test]
    fn shared_owner_slot_active_clears_dead_owner() {
        let dead_owner = SharedOwnerRecord::new(exited_child_pid(), 99);
        let slot = AtomicU64::new(dead_owner.raw());

        assert!(!MappedSharedWalCoordination::shared_owner_slot_active(
            &slot
        ));
        assert_eq!(slot.load(Ordering::Acquire), UNOWNED_LOCK);
    }

    #[test]
    fn mapped_shared_wal_coordination_reclaims_dead_writer_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let dead_owner = SharedOwnerRecord::new(exited_child_pid(), 41);

        mapped
            .header()
            .writer_owner
            .store(dead_owner.raw(), Ordering::Release);

        assert!(MappedSharedWalCoordination::try_acquire_shared_owner_slot(
            &mapped.header().writer_owner,
            mapped.owner_record(),
        ));
        assert_eq!(mapped.writer_owner(), Some(mapped.owner_record()));
        MappedSharedWalCoordination::release_shared_owner_slot(
            &mapped.header().writer_owner,
            mapped.owner_record(),
            "writer",
        );
        assert_eq!(mapped.writer_owner(), None);
    }

    #[test]
    fn mapped_shared_wal_coordination_reclaims_dead_checkpoint_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let dead_owner = SharedOwnerRecord::new(exited_child_pid(), 42);

        mapped
            .header()
            .checkpoint_owner
            .store(dead_owner.raw(), Ordering::Release);

        assert!(MappedSharedWalCoordination::try_acquire_shared_owner_slot(
            &mapped.header().checkpoint_owner,
            mapped.owner_record(),
        ));
        assert_eq!(mapped.checkpoint_owner(), Some(mapped.owner_record()));
        MappedSharedWalCoordination::release_shared_owner_slot(
            &mapped.header().checkpoint_owner,
            mapped.owner_record(),
            "checkpoint",
        );
        assert_eq!(mapped.checkpoint_owner(), None);
    }

    #[test]
    fn mapped_shared_wal_coordination_reclaims_dead_reader_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let dead_owner = SharedOwnerRecord::new(exited_child_pid(), 43);

        mapped.reader_bitmap_words()[0].fetch_and(!1u64, Ordering::Release);
        mapped.reader_frames()[0].store(17, Ordering::Release);
        mapped.reader_owners()[0].store(dead_owner.raw(), Ordering::Release);

        assert!(mapped.try_reclaim_dead_reader_owner(0, dead_owner));
        assert_eq!(mapped.reader_owner(0), None);
        assert_eq!(mapped.min_active_reader_frame(), None);
    }

    #[test]
    fn process_scoped_mapping_drop_releases_same_process_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");

        let mapped = create_process_scoped_mapping(&path);
        let owner = mapped.owner_record();
        assert!(mapped.try_acquire_writer(owner));
        assert!(mapped.try_acquire_checkpoint(owner));
        let reader = mapped.register_reader(owner, 9).unwrap();
        drop(mapped);

        let reopened = create_process_scoped_mapping(&path);
        assert!(reopened.try_acquire_writer(reopened.owner_record()));
        reopened.release_writer(reopened.owner_record());
        assert!(reopened.try_acquire_checkpoint(reopened.owner_record()));
        reopened.release_checkpoint(reopened.owner_record());
        let reader2 = reopened
            .register_reader(reopened.owner_record(), 5)
            .unwrap();
        reopened.unregister_reader(reader2);

        let probe = create_process_scoped_mapping(&path);
        assert_eq!(probe.writer_owner(), None);
        assert_eq!(probe.checkpoint_owner(), None);
        assert_eq!(probe.reader_owner(reader.slot_index), None);
    }

    #[test]
    fn process_scoped_mapping_reopens_after_stale_owner_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let stale_owner = SharedOwnerRecord::new(exited_child_pid(), 77);

        {
            let mapped = create_process_scoped_mapping(&path);
            mapped
                .header()
                .writer_owner
                .store(stale_owner.raw(), Ordering::Release);
            mapped
                .header()
                .checkpoint_owner
                .store(stale_owner.raw(), Ordering::Release);
            mapped.reader_bitmap_words()[0].fetch_and(!1u64, Ordering::Release);
            mapped.reader_frames()[0].store(17, Ordering::Release);
            mapped.reader_owners()[0].store(stale_owner.raw(), Ordering::Release);
        }

        let reopened = create_process_scoped_mapping(&path);
        assert!(reopened.try_acquire_writer(reopened.owner_record()));
        reopened.release_writer(reopened.owner_record());
        assert!(reopened.try_acquire_checkpoint(reopened.owner_record()));
        reopened.release_checkpoint(reopened.owner_record());
        let reader = reopened
            .register_reader(reopened.owner_record(), 5)
            .unwrap();
        reopened.unregister_reader(reader);
    }

    #[test]
    fn shared_wal_coordination_tracks_writer_and_checkpoint_owners() {
        let state = SharedWalCoordinationState::new(64);
        let writer = SharedOwnerRecord::new(11, 1);
        let checkpoint = SharedOwnerRecord::new(21, 2);

        assert!(state.try_acquire_writer(writer));
        assert_eq!(state.writer_owner(), Some(writer));
        assert!(!state.try_acquire_writer(SharedOwnerRecord::new(12, 1)));
        state.release_writer(writer);
        assert_eq!(state.writer_owner(), None);

        assert!(state.try_acquire_checkpoint(checkpoint));
        assert_eq!(state.checkpoint_owner(), Some(checkpoint));
        assert!(!state.try_acquire_checkpoint(SharedOwnerRecord::new(22, 2)));
        state.release_checkpoint(checkpoint);
        assert_eq!(state.checkpoint_owner(), None);
    }

    #[test]
    fn shared_wal_coordination_tracks_reader_registrations() {
        let state = SharedWalCoordinationState::new(64);
        let owner_a = SharedOwnerRecord::new(31, 1);
        let owner_b = SharedOwnerRecord::new(31, 2);

        let reader_a = state.register_reader(owner_a, 9).unwrap();
        let reader_b = state.register_reader(owner_b, 5).unwrap();
        assert_eq!(state.reader_owner(reader_a.slot_index), Some(owner_a));
        assert_eq!(state.reader_owner(reader_b.slot_index), Some(owner_b));
        assert_eq!(state.min_active_reader_frame(), Some(5));

        let reader_a = state.update_reader(reader_a, 3);
        assert_eq!(reader_a.max_frame, 3);
        assert_eq!(state.min_active_reader_frame(), Some(3));

        state.unregister_reader(reader_b);
        assert_eq!(state.reader_owner(reader_b.slot_index), None);
        assert_eq!(state.min_active_reader_frame(), Some(3));

        state.unregister_reader(reader_a);
        assert_eq!(state.reader_owner(reader_a.slot_index), None);
        assert_eq!(state.min_active_reader_frame(), None);
    }

    #[test]
    fn shared_wal_coordination_publishes_snapshot_fields() {
        let state = SharedWalCoordinationState::new(64);

        state.install_header_fields(4096, 17, 23);
        state.publish_commit(14, 31, 37, 9);
        state.publish_backfill(8);
        assert_eq!(state.bump_checkpoint_seq(), 1);
        assert_eq!(state.bump_checkpoint_epoch(), 0);

        let snapshot = state.snapshot();
        assert_eq!(snapshot.max_frame, 14);
        assert_eq!(snapshot.nbackfills, 8);
        assert_eq!(snapshot.transaction_count, 9);
        assert_eq!(snapshot.visibility_generation, 1);
        assert_eq!(snapshot.checkpoint_seq, 1);
        assert_eq!(snapshot.checkpoint_epoch, 1);
        assert_eq!(snapshot.page_size, 4096);
        assert_eq!(snapshot.salt_1, 17);
        assert_eq!(snapshot.salt_2, 23);
        assert_eq!(snapshot.checksum_1, 31);
        assert_eq!(snapshot.checksum_2, 37);
        assert_eq!(snapshot.reader_slot_count, 64);
    }

    #[test]
    fn mapped_shared_wal_coordination_persists_file_after_last_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let expected_len = MappedSharedWalCoordination::file_len_for_blocks(64, 1) as u64;

        {
            let mapped = create_mapping(&path);
            assert_eq!(mapped.open_mode(), SharedWalCoordinationOpenMode::Exclusive);
            mapped.install_header_fields(4096, 17, 23);
            mapped.publish_commit(14, 31, 37, 9);
            mapped.publish_backfill(8);
            assert_eq!(mapped.bump_checkpoint_seq(), 1);
            assert_eq!(mapped.bump_checkpoint_epoch(), 0);
        }

        assert_eq!(std::fs::metadata(&path).unwrap().len(), expected_len);

        let reopened = create_mapping(&path);
        assert_eq!(
            reopened.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );
        let snapshot = reopened.snapshot();
        assert_eq!(snapshot.max_frame, 14);
        assert_eq!(snapshot.nbackfills, 8);
        assert_eq!(snapshot.transaction_count, 9);
        assert_eq!(snapshot.visibility_generation, 1);
        assert_eq!(snapshot.checkpoint_seq, 1);
        assert_eq!(snapshot.checkpoint_epoch, 1);
        assert_eq!(snapshot.page_size, 4096);
        assert_eq!(snapshot.salt_1, 17);
        assert_eq!(snapshot.salt_2, 23);
        assert_eq!(snapshot.checksum_1, 31);
        assert_eq!(snapshot.checksum_2, 37);
    }

    #[test]
    fn mapped_shared_wal_coordination_repair_reclaims_dead_owners_without_clearing_frame_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);

        mapped.record_frame(7, 2);
        mapped.record_frame(9, 4);
        mapped
            .header()
            .writer_owner
            .store(SharedOwnerRecord::new(u32::MAX, 1).raw(), Ordering::Release);
        mapped
            .header()
            .checkpoint_owner
            .store(SharedOwnerRecord::new(u32::MAX, 2).raw(), Ordering::Release);
        mapped.reader_bitmap_words()[0].fetch_and(!1u64, Ordering::Release);
        mapped.reader_frames()[0].store(4, Ordering::Release);
        mapped.reader_owners()[0]
            .store(SharedOwnerRecord::new(u32::MAX, 3).raw(), Ordering::Release);

        mapped.repair_transient_state_for_exclusive_open();

        assert_eq!(mapped.writer_owner(), None);
        assert_eq!(mapped.checkpoint_owner(), None);
        assert_eq!(mapped.reader_owner(0), None);
        assert_eq!(mapped.min_active_reader_frame(), None);
        assert_eq!(mapped.find_frame(7, 0, 4, None), Some(2));
        assert_eq!(mapped.find_frame(9, 0, 4, None), Some(4));
    }

    #[test]
    fn mapped_shared_wal_coordination_repair_preserves_live_reader_slots_and_frame_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped_a = create_mapping(&path);
        let mapped_b = create_mapping(&path);

        mapped_a.record_frame(7, 2);
        mapped_a.record_frame(9, 4);
        let reader = mapped_b
            .register_reader(mapped_b.owner_record(), 4)
            .unwrap();

        mapped_a.repair_transient_state_for_exclusive_open();

        assert_eq!(mapped_a.reader_owner(reader.slot_index), Some(reader.owner));
        assert_eq!(mapped_a.min_active_reader_frame(), Some(4));
        assert_eq!(mapped_a.find_frame(7, 0, 4, None), Some(2));
        assert_eq!(mapped_a.find_frame(9, 0, 4, None), Some(4));

        mapped_b.unregister_reader(reader);
        assert_eq!(mapped_a.min_active_reader_frame(), None);
        assert_eq!(mapped_a.find_frame(7, 0, 4, None), Some(2));
        assert_eq!(mapped_a.find_frame(9, 0, 4, None), Some(4));
    }

    #[test]
    fn mapped_shared_wal_coordination_rebuilds_undersized_file_on_exclusive_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        std::fs::write(&path, [0u8; 32]).unwrap();

        let reopened = create_mapping(&path);
        assert_eq!(
            reopened.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );
        assert_eq!(
            reopened.file.size().unwrap() as usize,
            MappedSharedWalCoordination::file_len_for_blocks(64, 1)
        );
        assert_eq!(reopened.snapshot().max_frame, 0);
    }

    #[test]
    fn mapped_shared_wal_coordination_shares_lock_and_reader_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped_a = create_mapping(&path);
        assert_eq!(
            mapped_a.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );
        let mapped_b = create_mapping(&path);
        assert_ne!(
            mapped_a.owner_record().instance_id(),
            mapped_b.owner_record().instance_id()
        );

        assert!(mapped_a.try_acquire_writer(mapped_a.owner_record()));
        assert_eq!(mapped_a.writer_owner(), Some(mapped_a.owner_record()));
        assert!(!mapped_b.try_acquire_writer(mapped_b.owner_record()));
        mapped_a.release_writer(mapped_a.owner_record());
        assert!(mapped_b.try_acquire_writer(mapped_b.owner_record()));
        assert_eq!(mapped_a.writer_owner(), Some(mapped_b.owner_record()));
        mapped_b.release_writer(mapped_b.owner_record());
        assert_eq!(mapped_a.writer_owner(), None);

        let reader = mapped_a
            .register_reader(mapped_a.owner_record(), 9)
            .unwrap();
        let reader_slot = reader.slot_index;
        assert_eq!(
            mapped_b.reader_owner(reader_slot),
            Some(mapped_a.owner_record())
        );
        assert_eq!(mapped_b.min_active_reader_frame(), Some(9));
        let reader = mapped_b.update_reader(reader, 5);
        assert_eq!(mapped_a.min_active_reader_frame(), Some(5));
        mapped_a.unregister_reader(reader);
        assert_eq!(mapped_a.reader_owner(reader_slot), None);
        assert_eq!(mapped_a.min_active_reader_frame(), None);

        assert_eq!(mapped_a.bump_checkpoint_epoch(), 0);
        assert_eq!(mapped_b.checkpoint_epoch(), 1);
    }

    #[test]
    fn mapped_shared_wal_coordination_prevents_checkpoint_lock_reuse_across_mappings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped_a = create_mapping(&path);
        let mapped_b = create_mapping(&path);

        assert!(mapped_a.try_acquire_checkpoint(mapped_a.owner_record()));
        assert_eq!(mapped_a.checkpoint_owner(), Some(mapped_a.owner_record()));
        assert!(!mapped_b.try_acquire_checkpoint(mapped_b.owner_record()));
        mapped_a.release_checkpoint(mapped_a.owner_record());
        assert_eq!(mapped_b.checkpoint_owner(), None);

        assert!(mapped_b.try_acquire_checkpoint(mapped_b.owner_record()));
        assert_eq!(mapped_a.checkpoint_owner(), Some(mapped_b.owner_record()));
        mapped_b.release_checkpoint(mapped_b.owner_record());
        assert_eq!(mapped_a.checkpoint_owner(), None);
    }

    #[test]
    fn mapped_shared_wal_coordination_second_process_local_mapping_is_multiprocess() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");

        let mapped_a = create_mapping(&path);
        assert_eq!(
            mapped_a.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );
        assert!(mapped_a.is_primary_process_mapping());

        let mapped_b = create_mapping(&path);
        assert_eq!(
            mapped_b.open_mode(),
            SharedWalCoordinationOpenMode::MultiProcess
        );
        assert!(!mapped_b.is_primary_process_mapping());
    }

    #[test]
    fn mapped_shared_wal_coordination_persists_backfill_proof_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 14,
            nbackfills: 8,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        {
            let mapped = create_mapping(&path);
            mapped.install_snapshot(snapshot);
            mapped.install_backfill_proof(snapshot, 11, 0xAABB_CCDD);
            assert!(mapped.validate_backfill_proof(snapshot, 11, 0xAABB_CCDD));
        }

        let reopened = create_mapping(&path);
        assert!(reopened.validate_backfill_proof(snapshot, 11, 0xAABB_CCDD));
    }

    #[test]
    fn mapped_shared_wal_coordination_publish_commit_clears_backfill_proof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 14,
            nbackfills: 8,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        mapped.install_snapshot(snapshot);
        mapped.install_backfill_proof(snapshot, 11, 0xAABB_CCDD);
        assert!(mapped.validate_backfill_proof(snapshot, 11, 0xAABB_CCDD));

        mapped.publish_commit(15, 41, 43, 10);

        assert!(!mapped.validate_backfill_proof(snapshot, 11, 0xAABB_CCDD));
        assert_eq!(
            mapped
                .header()
                .backfill_proof_version
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_install_snapshot_clears_backfill_proof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 14,
            nbackfills: 8,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        mapped.install_snapshot(snapshot);
        mapped.install_backfill_proof(snapshot, 11, 0xAABB_CCDD);
        assert!(mapped.validate_backfill_proof(snapshot, 11, 0xAABB_CCDD));

        mapped.install_snapshot(SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            checkpoint_seq: 6,
            salt_1: 19,
            salt_2: 29,
            ..snapshot
        });

        assert!(!mapped.validate_backfill_proof(snapshot, 11, 0xAABB_CCDD));
        assert_eq!(
            mapped
                .header()
                .backfill_proof_version
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_rejects_corrupt_backfill_proof_crc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 14,
            nbackfills: 8,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        mapped.install_snapshot(snapshot);
        mapped.install_backfill_proof(snapshot, 11, 0xAABB_CCDD);
        mapped
            .header()
            .backfill_proof_crc32c
            .store(0xDEAD_BEEF, Ordering::Release);

        assert!(!mapped.validate_backfill_proof(snapshot, 11, 0xAABB_CCDD));
    }

    #[test]
    fn mapped_shared_wal_coordination_rejects_structurally_impossible_backfill_proof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 14,
            nbackfills: 8,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        mapped.install_snapshot(snapshot);
        mapped.install_backfill_proof(snapshot, 11, 0xAABB_CCDD);
        mapped
            .header()
            .backfill_proof_max_frame
            .store(3, Ordering::Release);
        mapped.header().backfill_proof_crc32c.store(
            SharedWalBackfillProof {
                max_frame: 3,
                ..SharedWalBackfillProof::from_snapshot_and_db(snapshot, 11, 0xAABB_CCDD)
            }
            .crc32c(),
            Ordering::Release,
        );

        assert!(
            !mapped.validate_backfill_proof(snapshot, 11, 0xAABB_CCDD),
            "proof with nbackfills beyond max_frame must be rejected even if CRC matches"
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_exclusive_reopen_clears_corrupt_backfill_proof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 14,
            nbackfills: 8,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        {
            let mapped = create_mapping(&path);
            mapped.install_snapshot(snapshot);
            mapped.install_backfill_proof(snapshot, 11, 0xAABB_CCDD);
            mapped
                .header()
                .backfill_proof_crc32c
                .store(0xDEAD_BEEF, Ordering::Release);
        }

        let reopened = create_mapping(&path);
        assert_eq!(
            reopened
                .header()
                .backfill_proof_version
                .load(Ordering::Acquire),
            0,
            "exclusive reopen should clear corrupt backfill proof state instead of rejecting the map"
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_exclusive_reopen_clears_unsupported_backfill_proof_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 14,
            nbackfills: 8,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        {
            let mapped = create_mapping(&path);
            mapped.install_snapshot(snapshot);
            mapped.install_backfill_proof(snapshot, 11, 0xAABB_CCDD);
            mapped
                .header()
                .backfill_proof_version
                .store(SHARED_WAL_BACKFILL_PROOF_VERSION + 1, Ordering::Release);
        }

        let reopened = create_mapping(&path);
        assert_eq!(
            reopened
                .header()
                .backfill_proof_version
                .load(Ordering::Acquire),
            0,
            "exclusive reopen should clear unsupported proof versions instead of discarding the whole map"
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_exclusive_reopen_clears_impossible_backfill_proof_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 14,
            nbackfills: 8,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        {
            let mapped = create_mapping(&path);
            mapped.install_snapshot(snapshot);
            mapped.install_backfill_proof(snapshot, 11, 0xAABB_CCDD);
            mapped
                .header()
                .backfill_proof_max_frame
                .store(3, Ordering::Release);
            mapped.header().backfill_proof_crc32c.store(
                SharedWalBackfillProof {
                    max_frame: 3,
                    ..SharedWalBackfillProof::from_snapshot_and_db(snapshot, 11, 0xAABB_CCDD)
                }
                .crc32c(),
                Ordering::Release,
            );
        }

        let reopened = create_mapping(&path);
        assert_eq!(
            reopened
                .header()
                .backfill_proof_version
                .load(Ordering::Acquire),
            0,
            "exclusive reopen should clear structurally impossible proof payloads"
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_prevents_reentrant_lock_reuse_within_same_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);

        assert!(mapped.try_acquire_writer(mapped.owner_record()));
        assert!(!mapped.try_acquire_writer(mapped.owner_record()));
        mapped.release_writer(mapped.owner_record());

        let reader_a = mapped.register_reader(mapped.owner_record(), 9).unwrap();
        let reader_b = mapped.register_reader(mapped.owner_record(), 5).unwrap();
        assert_ne!(reader_a.slot_index, reader_b.slot_index);
        assert_eq!(mapped.min_active_reader_frame(), Some(5));

        mapped.unregister_reader(reader_a);
        mapped.unregister_reader(reader_b);
        assert_eq!(mapped.min_active_reader_frame(), None);
    }

    #[test]
    fn mapped_shared_wal_coordination_ignores_stale_writer_owner_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);

        mapped
            .header()
            .writer_owner
            .store(SharedOwnerRecord::new(99, 7).raw(), Ordering::Release);
        assert!(mapped.try_acquire_writer(mapped.owner_record()));
        mapped.release_writer(mapped.owner_record());
    }

    #[test]
    fn mapped_shared_wal_coordination_reclaims_stale_reader_slots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);

        mapped.reader_bitmap_words()[0].fetch_and(!1u64, Ordering::Release);
        mapped.reader_frames()[0].store(17, Ordering::Release);
        mapped.reader_owners()[0].store(SharedOwnerRecord::new(42, 9).raw(), Ordering::Release);

        assert_eq!(mapped.min_active_reader_frame(), None);
        assert_eq!(mapped.reader_owner(0), None);

        let reader = mapped.register_reader(mapped.owner_record(), 23).unwrap();
        assert_eq!(reader.slot_index, 0);
        assert_eq!(
            mapped.reader_owner(reader.slot_index),
            Some(mapped.owner_record())
        );
        assert_eq!(mapped.min_active_reader_frame(), Some(23));
        mapped.unregister_reader(reader);
        assert_eq!(mapped.min_active_reader_frame(), None);
    }

    #[test]
    fn mapped_shared_wal_coordination_tracks_frame_index_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);

        mapped.record_frame(7, 2);
        mapped.record_frame(9, 4);
        mapped.record_frame(7, 5);

        assert_eq!(mapped.find_frame(7, 0, 5, None), Some(5));
        assert_eq!(mapped.find_frame(7, 0, 5, Some(4)), Some(2));
        assert_eq!(mapped.iter_latest_frames(0, 5), vec![(7, 5), (9, 4)]);

        mapped.rollback_frames(4);

        assert_eq!(mapped.find_frame(7, 0, 5, None), Some(2));
        assert_eq!(mapped.iter_latest_frames(0, 5), vec![(7, 2), (9, 4)]);
    }

    #[test]
    fn mapped_shared_wal_coordination_finds_simple_kv_page_after_seed_frame_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);

        for (frame_id, page_id) in [1, 702, 703, 1377, 1378, 559, 560, 1, 1066, 1067, 1377]
            .into_iter()
            .enumerate()
            .map(|(idx, page_id)| ((idx + 1) as u64, page_id as u64))
        {
            mapped.record_frame(page_id, frame_id);
        }

        assert_eq!(mapped.find_frame(1066, 0, 11, None), Some(9));
        assert_eq!(mapped.find_frame(1067, 0, 11, None), Some(10));
        assert_eq!(mapped.find_frame(1377, 0, 11, None), Some(11));
    }

    #[test]
    fn mapped_shared_wal_coordination_grows_frame_index_across_block_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let boundary = FRAME_INDEX_BLOCK_CAPACITY as u64;

        mapped.record_frame(7, 2);
        for frame_id in 3..=boundary + 1 {
            mapped.record_frame(100 + (frame_id % 17), frame_id);
        }
        mapped.record_frame(7, boundary + 2);

        let header = mapped.header();
        assert_eq!(
            header.frame_index_blocks.load(Ordering::Acquire),
            INITIAL_FRAME_INDEX_BLOCKS + 1
        );
        assert_eq!(
            mapped.find_frame(7, 0, boundary + 2, None),
            Some(boundary + 2)
        );
        assert_eq!(
            mapped.find_frame(7, 0, boundary + 2, Some(boundary + 1)),
            Some(2)
        );
        assert!(!mapped.frame_index_overflowed());
    }

    #[test]
    fn mapped_shared_wal_coordination_iterates_latest_frames_across_full_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let boundary = FRAME_INDEX_BLOCK_CAPACITY as u64;

        for frame_id in 1..=boundary {
            let page_id = match frame_id % 3 {
                1 => 7,
                2 => 9,
                _ => 11,
            };
            mapped.record_frame(page_id, frame_id);
        }
        mapped.record_frame(9, boundary + 1);
        mapped.record_frame(13, boundary + 2);

        assert_eq!(
            mapped.iter_latest_frames(0, boundary + 2),
            vec![
                (7, boundary),
                (9, boundary + 1),
                (11, boundary - 1),
                (13, boundary + 2),
            ]
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_marks_overflow_once_reserved_space_is_full() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let header = mapped.header();
        assert!(mapped.try_grow_frame_index_blocks(header.frame_index_max_blocks));
        header
            .frame_index_len
            .store(header.frame_index_capacity, Ordering::Release);

        mapped.record_frame(7, 2);
        assert_eq!(
            header.frame_index_len.load(Ordering::Acquire),
            header.frame_index_capacity
        );
        assert!(mapped.frame_index_overflowed());
        assert_eq!(mapped.find_frame(7, 1, u64::MAX, None), None);
        mapped.rollback_frames(1);
    }

    #[test]
    fn mapped_shared_wal_coordination_rebuilds_block_hash_after_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);

        mapped.record_frame(7, 1);
        mapped.record_frame(9, 2);
        mapped.record_frame(7, 3);
        mapped.record_frame(11, 4);

        mapped.rollback_frames(2);
        assert_eq!(mapped.find_frame(7, 0, 2, None), Some(1));
        assert_eq!(mapped.find_frame(11, 0, 2, None), None);

        mapped.record_frame(15, 3);
        mapped.record_frame(7, 4);
        assert_eq!(mapped.find_frame(7, 0, 4, None), Some(4));
        assert_eq!(mapped.find_frame(15, 0, 4, None), Some(3));
    }

    #[test]
    fn mapped_shared_wal_coordination_clears_stale_frame_index_when_wal_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let header = mapped.header();

        mapped.record_frame(7, 2);
        mapped.record_frame(9, 4);
        header.max_frame.store(0, Ordering::Release);

        mapped.record_frame(11, 1);

        assert_eq!(header.frame_index_len.load(Ordering::Acquire), 1);
        assert_eq!(mapped.find_frame(11, 0, 1, None), Some(1));
        assert_eq!(mapped.find_frame(7, 0, 1, None), None);
    }

    #[test]
    fn mapped_shared_wal_coordination_install_snapshot_trims_stale_frame_index_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let mut snapshot = mapped.snapshot();

        mapped.record_frame(7, 2);
        mapped.record_frame(9, 4);
        mapped.record_frame(11, 6);

        snapshot.max_frame = 2;
        mapped.install_snapshot(snapshot);

        assert_eq!(mapped.header().frame_index_len.load(Ordering::Acquire), 1);
        assert_eq!(mapped.find_frame(7, 0, 2, None), Some(2));
        assert_eq!(mapped.find_frame(9, 0, 2, None), None);

        mapped.record_frame(13, 3);
        assert_eq!(mapped.find_frame(13, 0, 3, None), Some(3));
    }

    #[test]
    fn mapped_shared_wal_coordination_handles_hash_collisions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let colliding = colliding_page_ids(3);

        mapped.record_frame(colliding[0], 2);
        mapped.record_frame(colliding[1], 4);
        mapped.record_frame(colliding[0], 6);
        mapped.record_frame(colliding[2], 8);
        mapped.record_frame(colliding[1], 10);

        assert_eq!(mapped.find_frame(colliding[0], 0, 10, None), Some(6));
        assert_eq!(mapped.find_frame(colliding[1], 0, 10, None), Some(10));
        assert_eq!(mapped.find_frame(colliding[2], 0, 10, None), Some(8));
        assert_eq!(mapped.find_frame(colliding[1], 0, 10, Some(9)), Some(4));
        assert_eq!(
            mapped.iter_latest_frames(0, 10),
            vec![(colliding[0], 6), (colliding[1], 10), (colliding[2], 8),]
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_reuses_block_hash_slots_after_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let colliding = colliding_page_ids(3);

        mapped.record_frame(colliding[0], 1);
        mapped.record_frame(colliding[1], 2);

        mapped.rollback_frames(1);
        mapped.record_frame(colliding[2], 2);

        assert_eq!(mapped.find_frame(colliding[0], 0, 2, None), Some(1));
        assert_eq!(mapped.find_frame(colliding[1], 0, 2, None), None);
        assert_eq!(mapped.find_frame(colliding[2], 0, 2, None), Some(2));
        assert_eq!(
            mapped.iter_latest_frames(0, 2),
            vec![(colliding[0], 1), (colliding[2], 2)]
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_keeps_initial_file_small() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");

        let mapped = create_mapping(&path);
        let metadata_len = mapped.file.size().unwrap() as usize;

        assert_eq!(
            metadata_len,
            MappedSharedWalCoordination::file_len_for_blocks(64, 1)
        );
        assert!(metadata_len < 128 * 1024);
    }

    #[test]
    fn mapped_shared_wal_coordination_respects_sparse_frame_watermarks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);

        mapped.record_frame(7, 2);
        mapped.record_frame(9, 4);
        mapped.record_frame(11, 8);
        mapped.record_frame(7, 13);
        mapped.record_frame(9, 21);

        assert_eq!(mapped.find_frame(7, 0, 21, None), Some(13));
        assert_eq!(mapped.find_frame(7, 0, 21, Some(12)), Some(2));
        assert_eq!(mapped.find_frame(9, 0, 21, Some(20)), Some(4));
        assert_eq!(mapped.find_frame(9, 5, 20, None), None);
        assert_eq!(mapped.find_frame(11, 0, 21, Some(7)), None);
        assert_eq!(
            mapped.iter_latest_frames(0, 13),
            vec![(7, 13), (9, 4), (11, 8)]
        );
    }

    #[test]
    fn mapped_shared_wal_coordination_rolls_back_across_block_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.tshm");
        let mapped = create_mapping(&path);
        let boundary = FRAME_INDEX_BLOCK_CAPACITY as u64;

        mapped.record_frame(7, 2);
        for frame_id in 3..=boundary + 1 {
            mapped.record_frame(100 + (frame_id % 17), frame_id);
        }
        mapped.record_frame(7, boundary + 2);
        mapped.record_frame(19, boundary + 3);

        assert_eq!(
            mapped.find_frame(7, 0, boundary + 3, None),
            Some(boundary + 2)
        );

        mapped.rollback_frames(boundary + 1);
        assert_eq!(mapped.find_frame(7, 0, boundary + 3, None), Some(2));
        assert_eq!(mapped.find_frame(19, 0, boundary + 3, None), None);

        mapped.record_frame(23, boundary + 2);
        mapped.record_frame(7, boundary + 3);
        assert_eq!(
            mapped.find_frame(23, 0, boundary + 3, None),
            Some(boundary + 2)
        );
        assert_eq!(
            mapped.find_frame(7, 0, boundary + 3, None),
            Some(boundary + 3)
        );
    }

    fn try_open_isolated_database(
        db_path: &std::path::Path,
    ) -> Result<(Arc<Database>, Arc<Connection>)> {
        let db_path_str = db_path.to_str().unwrap();
        let wal_path = format!("{db_path_str}-wal");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let file = io
            .open_file(db_path_str, OpenFlags::default(), true)
            .unwrap();
        let db_file = Arc::new(DatabaseFile::new(file));
        let mut state = OpenDbAsyncState::new();
        let db = loop {
            match Database::open_with_flags_bypass_registry_async(
                &mut state,
                io.clone(),
                db_path_str,
                Some(&wal_path),
                db_file.clone(),
                OpenFlags::default(),
                DatabaseOpts::new().with_multiprocess_wal(true),
                None,
                None,
            )? {
                IOResult::Done(db) => break db,
                IOResult::IO(io_completion) => io_completion.wait(&*io)?,
            }
        };
        let conn = db.connect()?;
        Ok((db, conn))
    }

    fn open_mp_database() -> (Arc<Database>, Arc<Connection>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "wal").unwrap();
        }
        let (db, conn) = try_open_isolated_database(&db_path).unwrap();
        (db, conn, dir)
    }

    fn open_second_process(dir: &std::path::Path) -> (Arc<Database>, Arc<Connection>) {
        try_open_isolated_database(&dir.join("test.db")).unwrap()
    }

    fn exec(conn: &Arc<Connection>, sql: &str) {
        conn.execute(sql).unwrap();
    }

    fn query_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
        try_query_i64(conn, sql).unwrap()
    }

    fn try_query_i64(conn: &Arc<Connection>, sql: &str) -> Result<i64> {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut value = 0i64;
        stmt.run_with_row_callback(|row| {
            value = row.get(0).unwrap();
            Ok(())
        })?;
        Ok(value)
    }

    fn query_rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.run_collect_rows().unwrap()
    }

    fn run_checkpoint(conn: &Arc<Connection>, mode: CheckpointMode) -> CheckpointResult {
        let pager = conn.pager.load();
        pager
            .io
            .block(|| pager.checkpoint(mode, SyncMode::Full, true))
            .unwrap()
    }

    fn assert_integrity_ok(conn: &Arc<Connection>) {
        let rows = query_rows(conn, "PRAGMA integrity_check");
        assert!(!rows.is_empty(), "integrity_check returned no rows");
        match &rows[0][0] {
            Value::Text(s) => assert_eq!(s.as_ref(), "ok", "integrity_check failed: {s}"),
            other => panic!("unexpected integrity_check result: {other:?}"),
        }
    }

    #[test]
    fn test_mp_snapshot_isolation() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );
        exec(&conn_writer, "INSERT INTO t VALUES (1,'a'), (2,'b')");
        let (_db_reader, conn_reader) = open_second_process(dir.path());

        exec(&conn_reader, "BEGIN");
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 2);

        exec(&conn_writer, "INSERT INTO t VALUES (3,'c'), (4,'d')");

        assert_eq!(
            query_i64(&conn_reader, "SELECT count(*) FROM t"),
            2,
            "reader should keep its original snapshot while the transaction stays open"
        );

        exec(&conn_reader, "COMMIT");
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 4);

        assert_integrity_ok(&conn_writer);
    }

    #[test]
    fn test_mp_checkpoint_respects_active_reader() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );

        exec(&conn_writer, "BEGIN");
        for i in 0..100 {
            exec(&conn_writer, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
        }
        exec(&conn_writer, "COMMIT");
        let (_db_reader, conn_reader) = open_second_process(dir.path());

        exec(&conn_reader, "BEGIN");
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 100);

        exec(&conn_writer, "BEGIN");
        for i in 100..200 {
            exec(&conn_writer, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
        }
        exec(&conn_writer, "COMMIT");

        let first_checkpoint = run_checkpoint(
            &conn_writer,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            !first_checkpoint.everything_backfilled(),
            "passive checkpoint should stop at the active reader snapshot"
        );

        exec(&conn_reader, "COMMIT");

        let second_checkpoint = run_checkpoint(
            &conn_writer,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            second_checkpoint.everything_backfilled(),
            "checkpoint should fully backfill after the reader releases its snapshot"
        );
        assert_eq!(query_i64(&conn_writer, "SELECT count(*) FROM t"), 200);
        assert_integrity_ok(&conn_writer);
    }

    #[test]
    fn test_mp_reader_crash_recovery_releases_checkpoint_barrier() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );
        exec(&conn_writer, "INSERT INTO t VALUES (1,'a'), (2,'b')");

        let (_db_reader, conn_reader) = open_second_process(dir.path());
        exec(&conn_reader, "BEGIN");
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 2);

        exec(&conn_writer, "INSERT INTO t VALUES (3,'c')");

        let checkpoint_with_reader = run_checkpoint(
            &conn_writer,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            !checkpoint_with_reader.everything_backfilled(),
            "active reader snapshot should limit passive checkpoint progress"
        );

        drop(conn_reader);
        drop(_db_reader);

        let checkpoint_after_crash = run_checkpoint(
            &conn_writer,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            checkpoint_after_crash.everything_backfilled(),
            "dropped reader process should no longer block checkpoint progress"
        );

        assert_eq!(query_i64(&conn_writer, "SELECT count(*) FROM t"), 3);
        assert_integrity_ok(&conn_writer);
    }

    #[test]
    fn test_mp_writer_crash_recovery_only_exposes_committed_rows() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );
        exec(&conn_writer, "BEGIN");
        for i in 0..10 {
            exec(&conn_writer, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
        }
        exec(&conn_writer, "COMMIT");
        assert_eq!(query_i64(&conn_writer, "SELECT count(*) FROM t"), 10);

        exec(&conn_writer, "BEGIN");
        for i in 10..20 {
            exec(&conn_writer, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
        }

        drop(conn_writer);
        drop(_db_writer);

        let (_db_recovered, conn_recovered) = open_second_process(dir.path());
        assert_eq!(
            query_i64(&conn_recovered, "SELECT count(*) FROM t"),
            10,
            "recovery should ignore uncommitted rows from the crashed writer"
        );
        assert_integrity_ok(&conn_recovered);
    }

    #[test]
    fn test_mp_multiple_readers_hold_distinct_snapshots() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );

        exec(&conn_writer, "BEGIN");
        for i in 0..10 {
            exec(&conn_writer, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
        }
        exec(&conn_writer, "COMMIT");

        let (_db_reader1, conn_reader1) = open_second_process(dir.path());
        exec(&conn_reader1, "BEGIN");
        assert_eq!(query_i64(&conn_reader1, "SELECT count(*) FROM t"), 10);

        exec(&conn_writer, "BEGIN");
        for i in 10..20 {
            exec(&conn_writer, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
        }
        exec(&conn_writer, "COMMIT");

        let (_db_reader2, conn_reader2) = open_second_process(dir.path());
        exec(&conn_reader2, "BEGIN");
        assert_eq!(query_i64(&conn_reader2, "SELECT count(*) FROM t"), 20);

        exec(&conn_writer, "BEGIN");
        for i in 20..30 {
            exec(&conn_writer, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
        }
        exec(&conn_writer, "COMMIT");

        let (_db_reader3, conn_reader3) = open_second_process(dir.path());
        exec(&conn_reader3, "BEGIN");
        assert_eq!(query_i64(&conn_reader3, "SELECT count(*) FROM t"), 30);

        assert_eq!(query_i64(&conn_reader1, "SELECT count(*) FROM t"), 10);
        assert_eq!(query_i64(&conn_reader2, "SELECT count(*) FROM t"), 20);
        assert_eq!(query_i64(&conn_reader3, "SELECT count(*) FROM t"), 30);

        let checkpoint = run_checkpoint(
            &conn_writer,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            !checkpoint.everything_backfilled(),
            "oldest reader snapshot should limit passive checkpoint progress"
        );

        exec(&conn_reader1, "COMMIT");
        exec(&conn_reader2, "COMMIT");
        exec(&conn_reader3, "COMMIT");

        let final_checkpoint = run_checkpoint(
            &conn_writer,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            final_checkpoint.everything_backfilled(),
            "checkpoint should complete once all reader snapshots are released"
        );

        assert_eq!(query_i64(&conn_writer, "SELECT count(*) FROM t"), 30);
        assert_integrity_ok(&conn_writer);
    }

    #[test]
    fn test_mp_stale_frame_cache_after_repeated_truncate() {
        let (_db0, conn0, dir) = open_mp_database();

        exec(&conn0, "CREATE TABLE t1(id INTEGER PRIMARY KEY)");
        exec(&conn0, "CREATE TABLE t2(id INTEGER PRIMARY KEY)");
        run_checkpoint(
            &conn0,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        let (_db1, conn1) = open_second_process(dir.path());
        let (_db2, conn2) = open_second_process(dir.path());

        exec(&conn1, "INSERT INTO t1 VALUES (1)");
        assert_eq!(query_i64(&conn2, "SELECT count(*) FROM t1"), 1);

        run_checkpoint(
            &conn0,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        exec(&conn1, "INSERT INTO t2 VALUES (100)");
        assert_eq!(query_i64(&conn2, "SELECT count(*) FROM t2"), 1);
        assert_integrity_ok(&conn2);
    }

    #[test]
    fn test_mp_restart_checkpoint_on_empty_wal() {
        let (_db1, conn1, dir) = open_mp_database();

        exec(&conn1, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)");
        exec(&conn1, "INSERT INTO t1 VALUES (1)");

        run_checkpoint(
            &conn1,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        let (_db2, conn2) = open_second_process(dir.path());
        run_checkpoint(&conn2, CheckpointMode::Restart);
        exec(&conn2, "INSERT INTO t1 VALUES (2)");

        assert_eq!(query_i64(&conn2, "SELECT count(*) FROM t1"), 2);
        assert_integrity_ok(&conn2);
    }

    #[test]
    fn test_mp_uncommitted_frames_not_visible_cross_process() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(&conn_writer, "CREATE TABLE t(id INTEGER PRIMARY KEY)");
        exec(&conn_writer, "INSERT INTO t VALUES (1)");

        let (_db_reader, conn_reader) = open_second_process(dir.path());
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 1);

        exec(&conn_writer, "BEGIN");
        exec(&conn_writer, "INSERT INTO t VALUES (2)");

        assert_eq!(
            query_i64(&conn_reader, "SELECT count(*) FROM t"),
            1,
            "uncommitted frames must stay invisible to other processes"
        );

        exec(&conn_writer, "COMMIT");
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 2);
    }

    #[test]
    fn test_mp_rollback_wal_index_cross_process() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );
        exec(&conn_writer, "INSERT INTO t VALUES (1, 'baseline')");

        let (_db_reader, conn_reader) = open_second_process(dir.path());
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 1);

        exec(&conn_writer, "BEGIN");
        exec(&conn_writer, "INSERT INTO t VALUES (2, 'will_rollback')");
        exec(&conn_writer, "INSERT INTO t VALUES (3, 'will_rollback')");
        exec(&conn_writer, "ROLLBACK");

        assert_eq!(query_i64(&conn_writer, "SELECT count(*) FROM t"), 1);
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 1);

        let rows = query_rows(&conn_reader, "SELECT val FROM t WHERE id = 1");
        match &rows[0][0] {
            Value::Text(s) => assert_eq!(s.as_ref(), "baseline"),
            other => panic!("unexpected val: {other:?}"),
        }
    }

    #[test]
    fn test_mp_truncate_then_cross_process_write() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );
        exec(&conn_writer, "INSERT INTO t VALUES (1, 'before_truncate')");

        let (_db_reader, conn_reader) = open_second_process(dir.path());
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 1);

        run_checkpoint(
            &conn_writer,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        exec(&conn_reader, "INSERT INTO t VALUES (2, 'after_truncate')");

        assert_eq!(query_i64(&conn_writer, "SELECT count(*) FROM t"), 2);
        assert_eq!(query_i64(&conn_reader, "SELECT count(*) FROM t"), 2);
        assert_integrity_ok(&conn_writer);
        assert_integrity_ok(&conn_reader);
    }

    #[test]
    fn test_mp_rapid_open_close_cycles() {
        let (_db_writer, conn_writer, dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );
        for i in 1..=10 {
            exec(
                &conn_writer,
                &format!("INSERT INTO t VALUES ({i}, 'row{i}')"),
            );
        }

        for cycle in 0..50 {
            let (db, conn) = open_second_process(dir.path());
            let count = query_i64(&conn, "SELECT count(*) FROM t");
            assert_eq!(count, 10, "cycle {cycle}: expected 10 rows, got {count}");
            drop(conn);
            drop(db);
        }

        assert_eq!(query_i64(&conn_writer, "SELECT count(*) FROM t"), 10);
        assert_integrity_ok(&conn_writer);

        exec(&conn_writer, "INSERT INTO t VALUES (11, 'after_churn')");
        assert_eq!(query_i64(&conn_writer, "SELECT count(*) FROM t"), 11);
    }

    #[test]
    fn test_mp_dbfile_readers_do_not_consume_shared_reader_slots() {
        let (db, conn_writer, _dir) = open_mp_database();

        exec(
            &conn_writer,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
        );
        exec(&conn_writer, "INSERT INTO t VALUES (1, 'baseline')");
        run_checkpoint(
            &conn_writer,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        let mut readers = Vec::new();
        for slot in 0..128 {
            let conn = db.connect().unwrap();
            exec(&conn, "BEGIN");
            assert_eq!(
                query_i64(&conn, "SELECT count(*) FROM t"),
                1,
                "db-file reader {slot} should not fail due to shared-reader slot pressure"
            );
            readers.push(conn);
        }

        let authority = db.shared_wal_coordination().unwrap().unwrap();
        assert_eq!(
            authority.min_active_reader_frame(),
            None,
            "db-file-only readers should not publish shared WAL reader slots"
        );

        for conn in readers {
            exec(&conn, "COMMIT");
        }
    }
}
