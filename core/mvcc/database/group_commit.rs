use super::{LogRecord, TxID};
use crate::io::Completion;
use crate::storage::wal::TursoRwLock;
#[cfg(test)]
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Arc;
use crate::sync::Mutex;
use rustc_hash::FxHashSet as HashSet;
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug)]
pub(crate) struct QueuedCommit {
    pub ticket: u64,
    pub tx_id: TxID,
    pub log_record: LogRecord,
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
    next_ticket: u64,
    durable_through: u64,
    written_through: u64,
    pending: VecDeque<QueuedCommit>,
    /// Tickets whose in-flight `log_tx` was discarded before the offset was
    /// advanced. The waiter rebuilds its log record instead of hanging.
    retry: HashSet<u64>,
    /// Tx whose `log_tx` the leader issued, until the offset advanced or the
    /// leader gave up the write.
    issued: Option<TxID>,
    /// Waiters that dropped after their `log_tx` was issued. The leader
    /// finishes or rolls them back. They must not roll back themselves.
    abandoned: HashSet<TxID>,
    /// Txs whose records the leader took and has not issued or put back.
    taken: HashSet<TxID>,
    /// Taken txs whose commit dropped before their `log_tx` was issued. The
    /// leader skips their records.
    withdrawn: HashSet<TxID>,
    /// Waiters asleep until the leader makes their ticket durable, asks
    /// them to retry, or releases the commit lock.
    parked: BTreeMap<u64, Completion>,
}

pub(crate) enum GroupWork {
    Lead {
        writing: QueuedCommit,
        rest: VecDeque<QueuedCommit>,
    },
    SyncPrefix,
    None,
}

#[derive(Debug)]
pub(crate) struct CommitCoordinator {
    pub pager_commit_lock: Arc<TursoRwLock>,
    group_commit_enabled: AtomicBool,
    group: Mutex<GroupState>,
    #[cfg(test)]
    last_group_size: AtomicUsize,
    #[cfg(test)]
    park_calls: AtomicUsize,
}

impl CommitCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            pager_commit_lock: Arc::new(TursoRwLock::new()),
            group_commit_enabled: AtomicBool::new(true),
            group: Mutex::new(GroupState {
                next_ticket: 0,
                durable_through: 0,
                written_through: 0,
                pending: VecDeque::new(),
                retry: HashSet::default(),
                issued: None,
                abandoned: HashSet::default(),
                taken: HashSet::default(),
                withdrawn: HashSet::default(),
                parked: BTreeMap::new(),
            }),
            #[cfg(test)]
            last_group_size: AtomicUsize::new(0),
            #[cfg(test)]
            park_calls: AtomicUsize::new(0),
        }
    }

    /// The completion a waiter sleeps on while another transaction holds the
    /// commit lock. Returns a plain yield when the waiter can already make
    /// progress, so the lock check and the registration happen under one
    /// lock and a release cannot slip in between them.
    pub(crate) fn park(&self, ticket: u64) -> Completion {
        #[cfg(test)]
        self.park_calls.fetch_add(1, Ordering::Relaxed);
        let mut group = self.group.lock();
        let can_progress = group.durable_through >= ticket
            || group.retry.contains(&ticket)
            || !self.pager_commit_lock.is_write_locked();
        if can_progress {
            return Completion::new_yield();
        }
        group
            .parked
            .entry(ticket)
            .or_insert_with(Completion::new_wait)
            .clone()
    }

    pub(crate) fn unlock_pager_commit_lock(&self) {
        self.pager_commit_lock.unlock();
        let woken = {
            let mut group = self.group.lock();
            take_parked_through(&mut group, u64::MAX)
        };
        wake(woken);
    }

    pub(crate) fn group_commit_enabled(&self) -> bool {
        self.group_commit_enabled.load(Ordering::Acquire)
    }

    pub(crate) fn set_group_commit_enabled(&self, enabled: bool) {
        self.group_commit_enabled.store(enabled, Ordering::Release);
    }

    pub(crate) fn enqueue(&self, tx_id: TxID, log_record: LogRecord) -> u64 {
        let mut group = self.group.lock();
        group.next_ticket += 1;
        let ticket = group.next_ticket;
        group.pending.push_back(QueuedCommit {
            ticket,
            tx_id,
            log_record,
        });
        ticket
    }

    #[cfg(test)]
    pub(crate) fn take_pending(&self) -> VecDeque<QueuedCommit> {
        let mut group = self.group.lock();
        let batch = std::mem::take(&mut group.pending);
        #[cfg(test)]
        if !batch.is_empty() {
            self.last_group_size.store(batch.len(), Ordering::Release);
        }
        batch
    }

    pub(crate) fn take_work(&self) -> GroupWork {
        let mut group = self.group.lock();
        if !group.retry.is_empty() {
            return if group.written_through > group.durable_through {
                GroupWork::SyncPrefix
            } else {
                GroupWork::None
            };
        }
        match group.pending.pop_front() {
            Some(writing) => {
                let rest = std::mem::take(&mut group.pending);
                group.taken.insert(writing.tx_id);
                group.taken.extend(rest.iter().map(|queued| queued.tx_id));
                #[cfg(test)]
                {
                    self.last_group_size
                        .store(rest.len() + 1, Ordering::Release);
                }
                GroupWork::Lead { writing, rest }
            }
            None if group.written_through > group.durable_through => GroupWork::SyncPrefix,
            None => GroupWork::None,
        }
    }

    pub(crate) fn requeue(&self, records: impl DoubleEndedIterator<Item = QueuedCommit>) {
        let mut group = self.group.lock();
        for entry in records.rev() {
            group.taken.remove(&entry.tx_id);
            if !group.withdrawn.remove(&entry.tx_id) {
                group.pending.push_front(entry);
            }
        }
    }

    pub(crate) fn mark_durable(&self, ticket: u64) {
        let woken = {
            let mut group = self.group.lock();
            let ticket = dense_prefix_cap(ticket, group.written_through, &group.retry);
            group.durable_through = group.durable_through.max(ticket);
            let durable_through = group.durable_through;
            take_parked_through(&mut group, durable_through)
        };
        wake(woken);
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

    pub(crate) fn take_retry(&self, ticket: u64) -> bool {
        self.group.lock().retry.remove(&ticket)
    }

    /// Marks the write of `tx_id` as issued, unless its commit already left
    /// the group. Returns whether the leader must write the record.
    pub(crate) fn try_issue(&self, tx_id: TxID) -> bool {
        let mut group = self.group.lock();
        group.taken.remove(&tx_id);
        if group.withdrawn.remove(&tx_id) {
            return false;
        }
        group.issued = Some(tx_id);
        true
    }

    /// Ends the issued write after the offset advanced. Returns whether its
    /// waiter was abandoned, so the leader must finish it.
    pub(crate) fn finish_issue(&self, tx_id: TxID) -> bool {
        let mut group = self.group.lock();
        group.issued = None;
        group.abandoned.remove(&tx_id)
    }

    /// Gives up the issued write of another tx. Returns whether its waiter
    /// was abandoned, so the leader must roll it back. Otherwise the waiter
    /// must retry.
    pub(crate) fn release_issued(&self, writing: &QueuedCommit) -> bool {
        let woken = {
            let mut group = self.group.lock();
            group.issued = None;
            if group.abandoned.remove(&writing.tx_id) {
                return true;
            }
            request_retry(&mut group, writing.ticket)
        };
        wake(woken);
        false
    }

    pub(crate) fn clear_issued(&self) {
        self.group.lock().issued = None;
    }

    /// Removes a dropped commit from the group. Returns whether its write is
    /// issued, so it is abandoned to the leader and must not roll back.
    pub(crate) fn leave(&self, tx_id: TxID, ticket: Option<u64>) -> bool {
        let mut group = self.group.lock();
        if group.issued == Some(tx_id) {
            group.abandoned.insert(tx_id);
            return true;
        }
        if let Some(ticket) = ticket {
            group.retry.remove(&ticket);
            group.parked.remove(&ticket);
        }
        group.pending.retain(|queued| queued.tx_id != tx_id);
        if group.taken.remove(&tx_id) {
            group.withdrawn.insert(tx_id);
        }
        false
    }

    pub(crate) fn is_abandoned(&self, tx_id: TxID) -> bool {
        self.group.lock().abandoned.contains(&tx_id)
    }

    #[cfg(test)]
    pub(crate) fn last_group_size(&self) -> usize {
        self.last_group_size.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn parked_tickets(&self) -> Vec<u64> {
        self.group.lock().parked.keys().copied().collect()
    }

    #[cfg(test)]
    pub(crate) fn park_calls(&self) -> usize {
        self.park_calls.load(Ordering::Relaxed)
    }
}

fn request_retry(group: &mut GroupState, ticket: u64) -> Option<Completion> {
    group.retry.insert(ticket);
    if group.written_through >= ticket {
        group.written_through = ticket.saturating_sub(1);
    }
    if group.durable_through >= ticket {
        group.durable_through = ticket.saturating_sub(1);
    }
    group.parked.remove(&ticket)
}

fn take_parked_through(group: &mut GroupState, through: u64) -> Vec<Completion> {
    let keep = group.parked.split_off(&through.saturating_add(1));
    std::mem::replace(&mut group.parked, keep)
        .into_values()
        .collect()
}

fn wake(parked: impl IntoIterator<Item = Completion>) {
    for completion in parked {
        completion.complete(0);
    }
}

fn dense_prefix_cap(ticket: u64, written_through: u64, retry: &HashSet<u64>) -> u64 {
    let mut cap = ticket.min(written_through);
    if let Some(hole) = retry.iter().copied().filter(|&t| t <= cap).min() {
        cap = hole.saturating_sub(1);
    }
    cap
}
