use super::{LogRecord, TxID};
use crate::io::clock::MonotonicInstant;
use crate::storage::wal::TursoRwLock;
use crate::sync::Arc;
use crate::sync::Mutex;
#[cfg(test)]
use crate::sync::atomic::{AtomicUsize, Ordering};
use rustc_hash::FxHashSet as HashSet;
use std::collections::VecDeque;
use std::time::Duration;

pub(crate) const MIN_BATCH: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct CoalesceWindow(Duration);

impl CoalesceWindow {
    pub(crate) const NONE: Self = Self(Duration::ZERO);

    pub(crate) fn from_micros(micros: u64) -> Self {
        Self(Duration::from_micros(micros))
    }

    pub(crate) fn as_micros(self) -> u64 {
        self.0.as_micros() as u64
    }

    pub(crate) fn verdict(
        self,
        head_arrived_at: MonotonicInstant,
        queued: usize,
        now: MonotonicInstant,
    ) -> CoalesceVerdict {
        if queued >= MIN_BATCH {
            return CoalesceVerdict::Go;
        }
        if self == Self::NONE {
            return CoalesceVerdict::Go;
        }
        let Some(until) = head_arrived_at.checked_add(self.0) else {
            // Overflow would make the hold unbounded. Fail open and lead.
            return CoalesceVerdict::Go;
        };
        if now >= until {
            CoalesceVerdict::Go
        } else {
            CoalesceVerdict::Hold { until }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoalesceVerdict {
    Go,
    Hold { until: MonotonicInstant },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupCommitMode {
    Off,
    On { coalesce: CoalesceWindow },
}

impl GroupCommitMode {
    /// `Off` drains immediately so records enqueued before someone disabled
    /// group commit still finish.
    fn window(self) -> CoalesceWindow {
        match self {
            GroupCommitMode::Off => CoalesceWindow::NONE,
            GroupCommitMode::On { coalesce } => coalesce,
        }
    }

    fn is_on(self) -> bool {
        matches!(self, GroupCommitMode::On { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupCommitOff;

#[derive(Debug)]
pub(crate) struct QueuedCommit {
    pub ticket: u64,
    pub tx_id: TxID,
    pub log_record: LogRecord,
    /// The window for the whole batch is derived from the front record's
    /// `arrived_at`; nothing else stores a deadline, so `requeue` and
    /// `drop_pending` need no window code.
    pub arrived_at: MonotonicInstant,
}

#[derive(Debug)]
pub(crate) struct GroupBatch {
    pub rest: VecDeque<QueuedCommit>,
    pub writing: QueuedCommit,
    pub advanced_through: Option<u64>,
}

impl GroupBatch {
    pub fn from_lead(writing: QueuedCommit, rest: VecDeque<QueuedCommit>) -> Self {
        Self {
            writing,
            rest,
            advanced_through: None,
        }
    }
}

#[derive(Debug)]
struct GroupState {
    mode: GroupCommitMode,
    next_ticket: u64,
    durable_through: u64,
    written_through: u64,
    pending: VecDeque<QueuedCommit>,
    /// Tickets whose in-flight `log_tx` was discarded before the offset was
    /// advanced. The waiter rebuilds its log record instead of hanging.
    retry: HashSet<u64>,
    /// Tx currently inside `log_tx`, before the offset advanced.
    issued: Option<TxID>,
    /// Waiters that dropped after `log_tx`. The leader finishes or rolls them
    /// back. They must not roll back themselves.
    abandoned: HashSet<TxID>,
}

#[derive(Debug)]
pub(crate) enum GroupWork {
    Lead {
        writing: QueuedCommit,
        rest: VecDeque<QueuedCommit>,
    },
    SyncPrefix,
    Coalescing {
        until: MonotonicInstant,
    },
    None,
}

#[derive(Debug)]
pub(crate) struct CommitCoordinator {
    pub pager_commit_lock: Arc<TursoRwLock>,
    group: Mutex<GroupState>,
    #[cfg(test)]
    last_group_size: AtomicUsize,
}

impl CommitCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            pager_commit_lock: Arc::new(TursoRwLock::new()),
            group: Mutex::new(GroupState {
                mode: GroupCommitMode::Off,
                next_ticket: 0,
                durable_through: 0,
                written_through: 0,
                pending: VecDeque::new(),
                retry: HashSet::default(),
                issued: None,
                abandoned: HashSet::default(),
            }),
            #[cfg(test)]
            last_group_size: AtomicUsize::new(0),
        }
    }

    pub(crate) fn group_commit_enabled(&self) -> bool {
        self.group.lock().mode.is_on()
    }

    pub(crate) fn set_group_commit_enabled(&self, enabled: bool) {
        let mut group = self.group.lock();
        if enabled {
            if matches!(group.mode, GroupCommitMode::Off) {
                group.mode = GroupCommitMode::On {
                    coalesce: CoalesceWindow::NONE,
                };
            }
        } else {
            group.mode = GroupCommitMode::Off;
        }
    }

    pub(crate) fn set_coalesce(&self, window: CoalesceWindow) -> Result<(), GroupCommitOff> {
        let mut group = self.group.lock();
        match &mut group.mode {
            GroupCommitMode::Off => Err(GroupCommitOff),
            GroupCommitMode::On { coalesce } => {
                *coalesce = window;
                Ok(())
            }
        }
    }

    pub(crate) fn coalesce(&self) -> Result<CoalesceWindow, GroupCommitOff> {
        match self.group.lock().mode {
            GroupCommitMode::Off => Err(GroupCommitOff),
            GroupCommitMode::On { coalesce } => Ok(coalesce),
        }
    }

    pub(crate) fn enqueue(&self, tx_id: TxID, log_record: LogRecord, now: MonotonicInstant) -> u64 {
        let mut group = self.group.lock();
        group.next_ticket += 1;
        let ticket = group.next_ticket;
        group.pending.push_back(QueuedCommit {
            ticket,
            tx_id,
            log_record,
            arrived_at: now,
        });
        ticket
    }

    #[cfg(test)]
    pub(crate) fn take_pending(&self) -> VecDeque<QueuedCommit> {
        let mut group = self.group.lock();
        let batch = std::mem::take(&mut group.pending);
        if !batch.is_empty() {
            self.last_group_size.store(batch.len(), Ordering::Release);
        }
        batch
    }

    pub(crate) fn take_work(&self, now: MonotonicInstant) -> GroupWork {
        let mut group = self.group.lock();
        if !group.retry.is_empty() {
            return if group.written_through > group.durable_through {
                GroupWork::SyncPrefix
            } else {
                GroupWork::None
            };
        }
        let Some(front) = group.pending.front() else {
            return if group.written_through > group.durable_through {
                GroupWork::SyncPrefix
            } else {
                GroupWork::None
            };
        };
        // Bytes already written still need an fsync; waiting would delay durability.
        let unsynced_prefix = group.written_through > group.durable_through;
        let verdict = group
            .mode
            .window()
            .verdict(front.arrived_at, group.pending.len(), now);
        if let CoalesceVerdict::Hold { until } = verdict {
            if !unsynced_prefix {
                return GroupWork::Coalescing { until };
            }
        }
        let writing = group.pending.pop_front().expect("front observed");
        let rest = std::mem::take(&mut group.pending);
        #[cfg(test)]
        {
            self.last_group_size
                .store(rest.len() + 1, Ordering::Release);
        }
        GroupWork::Lead { writing, rest }
    }

    pub(crate) fn requeue(&self, records: impl DoubleEndedIterator<Item = QueuedCommit>) {
        let mut group = self.group.lock();
        for entry in records.rev() {
            group.pending.push_front(entry);
        }
    }

    pub(crate) fn mark_durable(&self, ticket: u64) {
        let mut group = self.group.lock();
        let ticket = dense_prefix_cap(ticket, group.written_through, &group.retry);
        group.durable_through = group.durable_through.max(ticket);
    }

    pub(crate) fn durable_through(&self) -> u64 {
        self.group.lock().durable_through
    }

    pub(crate) fn note_written(&self, ticket: u64) {
        let mut group = self.group.lock();
        if group.retry.iter().any(|&hole| hole <= ticket) {
            return;
        }
        group.written_through = group.written_through.max(ticket);
    }

    pub(crate) fn written_through(&self) -> u64 {
        self.group.lock().written_through
    }

    /// Removes `ticket` from the queue. Returns whether it was still waiting.
    pub(crate) fn drop_pending(&self, ticket: u64) -> bool {
        let mut group = self.group.lock();
        group.retry.remove(&ticket);
        if let Some(index) = group
            .pending
            .iter()
            .position(|queued| queued.ticket == ticket)
        {
            group.pending.remove(index);
            true
        } else {
            false
        }
    }

    pub(crate) fn request_retry(&self, ticket: u64) {
        let mut group = self.group.lock();
        group.retry.insert(ticket);
        if group.written_through >= ticket {
            group.written_through = ticket.saturating_sub(1);
        }
        if group.durable_through >= ticket {
            group.durable_through = ticket.saturating_sub(1);
        }
    }

    pub(crate) fn take_retry(&self, ticket: u64) -> bool {
        self.group.lock().retry.remove(&ticket)
    }

    pub(crate) fn note_write_issued(&self, tx_id: TxID) {
        self.group.lock().issued = Some(tx_id);
    }

    pub(crate) fn clear_issued(&self) {
        self.group.lock().issued = None;
    }

    pub(crate) fn abandon_if_issued(&self, tx_id: TxID) -> bool {
        let mut group = self.group.lock();
        if group.issued == Some(tx_id) {
            group.abandoned.insert(tx_id);
            true
        } else {
            false
        }
    }

    pub(crate) fn is_abandoned(&self, tx_id: TxID) -> bool {
        self.group.lock().abandoned.contains(&tx_id)
    }

    pub(crate) fn take_abandoned(&self, tx_id: TxID) -> bool {
        self.group.lock().abandoned.remove(&tx_id)
    }

    #[cfg(test)]
    pub(crate) fn last_group_size(&self) -> usize {
        self.last_group_size.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.group.lock().pending.len()
    }
}

fn dense_prefix_cap(ticket: u64, written_through: u64, retry: &HashSet<u64>) -> u64 {
    let mut cap = ticket.min(written_through);
    if let Some(hole) = retry.iter().copied().filter(|&t| t <= cap).min() {
        cap = hole.saturating_sub(1);
    }
    cap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::DynAllocator;

    fn empty_record(end_ts: u64) -> LogRecord {
        LogRecord::empty(end_ts, DynAllocator::default())
    }

    fn t0() -> MonotonicInstant {
        MonotonicInstant::from_nanos(0)
    }

    fn us(micros: u64) -> Duration {
        Duration::from_micros(micros)
    }

    fn enabled_with_window(micros: u64) -> CommitCoordinator {
        let coordinator = CommitCoordinator::new();
        coordinator.set_group_commit_enabled(true);
        coordinator
            .set_coalesce(CoalesceWindow::from_micros(micros))
            .unwrap();
        coordinator
    }

    #[test]
    fn window_micros_round_trip() {
        assert_eq!(CoalesceWindow::NONE.as_micros(), 0);
        assert_eq!(CoalesceWindow::from_micros(250).as_micros(), 250);
        assert_eq!(CoalesceWindow::from_micros(0).as_micros(), 0);
    }

    #[test]
    fn verdict_goes_immediately_when_window_is_none() {
        assert_eq!(
            CoalesceWindow::NONE.verdict(t0(), 1, t0()),
            CoalesceVerdict::Go
        );
    }

    #[test]
    fn verdict_holds_a_lone_record_until_the_deadline() {
        let until = t0() + us(200);
        assert_eq!(
            CoalesceWindow::from_micros(200).verdict(t0(), 1, t0() + us(50)),
            CoalesceVerdict::Hold { until }
        );
    }

    #[test]
    fn verdict_goes_at_the_deadline_boundary() {
        assert_eq!(
            CoalesceWindow::from_micros(200).verdict(t0(), 1, t0() + us(200)),
            CoalesceVerdict::Go
        );
    }

    #[test]
    fn verdict_goes_once_min_batch_is_reached() {
        assert_eq!(
            CoalesceWindow::from_micros(200).verdict(t0(), MIN_BATCH, t0()),
            CoalesceVerdict::Go
        );
    }

    #[test]
    fn verdict_goes_when_deadline_overflows() {
        let head = MonotonicInstant::from_nanos(u128::MAX);
        assert_eq!(
            CoalesceWindow::from_micros(1).verdict(head, 1, t0()),
            CoalesceVerdict::Go
        );
    }

    #[test]
    fn take_work_reports_coalescing_deadline_for_lone_record() {
        let coordinator = enabled_with_window(200);
        coordinator.enqueue(1, empty_record(10), t0());
        match coordinator.take_work(t0() + us(50)) {
            GroupWork::Coalescing { until } => assert_eq!(until, t0() + us(200)),
            other => panic!("expected Coalescing, got {other:?}"),
        }
        assert_eq!(coordinator.pending_len(), 1);
        match coordinator.take_work(t0() + us(200)) {
            GroupWork::Lead { rest, .. } => assert!(rest.is_empty()),
            other => panic!("expected Lead, got {other:?}"),
        }
    }

    #[test]
    fn second_record_closes_the_batch_before_the_deadline() {
        let coordinator = enabled_with_window(200);
        let first = coordinator.enqueue(1, empty_record(10), t0());
        coordinator.enqueue(2, empty_record(20), t0() + us(60));
        match coordinator.take_work(t0() + us(61)) {
            GroupWork::Lead { writing, rest } => {
                assert_eq!(writing.ticket, first);
                assert_eq!(rest.len(), 1);
            }
            other => panic!("expected Lead, got {other:?}"),
        }
    }

    #[test]
    fn requeued_records_keep_their_arrival_time() {
        let coordinator = enabled_with_window(200);
        coordinator.enqueue(1, empty_record(10), t0());
        coordinator.enqueue(2, empty_record(20), t0());
        let both = coordinator.take_pending();
        coordinator.requeue(both.into_iter());
        assert!(matches!(
            coordinator.take_work(t0() + us(10)),
            GroupWork::Lead { ref rest, .. } if rest.len() == 1
        ));

        coordinator.enqueue(3, empty_record(30), t0());
        let one = coordinator.take_pending();
        coordinator.requeue(one.into_iter());
        match coordinator.take_work(t0() + us(10)) {
            GroupWork::Coalescing { until } => assert_eq!(until, t0() + us(200)),
            other => panic!("expected remainder of original window, got {other:?}"),
        }
    }

    #[test]
    fn dropping_the_head_moves_the_window_to_the_survivor() {
        let coordinator = enabled_with_window(200);
        let first = coordinator.enqueue(1, empty_record(10), t0());
        coordinator.enqueue(2, empty_record(20), t0() + us(100));
        assert!(coordinator.drop_pending(first));
        match coordinator.take_work(t0() + us(150)) {
            GroupWork::Coalescing { until } => assert_eq!(until, t0() + us(300)),
            other => panic!("expected survivor's window, got {other:?}"),
        }
    }

    #[test]
    fn coalesce_ignored_while_a_retry_hole_is_open() {
        let coordinator = enabled_with_window(200);
        let ticket = coordinator.enqueue(1, empty_record(10), t0());
        coordinator.request_retry(ticket);
        assert!(
            !matches!(coordinator.take_work(t0()), GroupWork::Coalescing { .. }),
            "retry holes short-circuit before coalesce"
        );
    }

    #[test]
    fn disabling_group_commit_resets_the_window() {
        let coordinator = enabled_with_window(200);
        coordinator.set_group_commit_enabled(true);
        assert_eq!(
            coordinator.coalesce().unwrap(),
            CoalesceWindow::from_micros(200)
        );
        coordinator.set_group_commit_enabled(false);
        assert_eq!(coordinator.coalesce(), Err(GroupCommitOff));
        coordinator.set_group_commit_enabled(true);
        assert_eq!(coordinator.coalesce(), Ok(CoalesceWindow::NONE));
    }

    #[test]
    fn take_work_drains_immediately_when_mode_is_off() {
        let coordinator = enabled_with_window(200);
        coordinator.enqueue(1, empty_record(10), t0());
        coordinator.set_group_commit_enabled(false);
        assert!(matches!(
            coordinator.take_work(t0()),
            GroupWork::Lead { .. }
        ));
    }

    #[test]
    fn take_work_never_coalesces_with_the_default_window() {
        let coordinator = CommitCoordinator::new();
        coordinator.set_group_commit_enabled(true);
        coordinator.enqueue(1, empty_record(10), t0());
        assert!(matches!(
            coordinator.take_work(t0()),
            GroupWork::Lead { .. }
        ));
    }

    #[test]
    fn take_work_never_coalesces_an_unsynced_prefix() {
        let coordinator = enabled_with_window(200);
        coordinator.note_written(1);
        assert!(
            matches!(coordinator.take_work(t0()), GroupWork::SyncPrefix),
            "empty pending with written > durable must SyncPrefix, not wait"
        );
        coordinator.enqueue(1, empty_record(10), t0());
        assert!(
            matches!(coordinator.take_work(t0()), GroupWork::Lead { .. }),
            "pending work with written > durable must lead, not wait"
        );
    }
}
