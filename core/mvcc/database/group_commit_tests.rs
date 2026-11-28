use crate::mvcc::database::{CommitCoordinator, GroupWork, LogRecord};
use crate::SyncMode;

fn empty_record(end_ts: u64) -> LogRecord {
    LogRecord::empty(end_ts, crate::alloc::DynAllocator::default())
}

#[test]
fn requeued_records_go_back_in_ticket_order() {
    let coordinator = CommitCoordinator::new();
    let first = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    let second = coordinator.enqueue(2, empty_record(20), SyncMode::Full);

    let batch = coordinator.take_pending();
    assert_eq!(
        batch.iter().map(|queued| queued.ticket).collect::<Vec<_>>(),
        vec![first, second]
    );

    let latecomer = coordinator.enqueue(3, empty_record(30), SyncMode::Full);
    coordinator.requeue(batch.into_iter());
    assert_eq!(
        coordinator
            .take_pending()
            .iter()
            .map(|queued| queued.ticket)
            .collect::<Vec<_>>(),
        vec![first, second, latecomer]
    );
}

#[test]
fn drop_pending_only_removes_queued_records() {
    let coordinator = CommitCoordinator::new();

    let queued = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    assert!(coordinator.drop_pending(queued));

    let claimed = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let _batch = coordinator.take_pending();
    assert!(
        !coordinator.drop_pending(claimed),
        "a record the leader already took is not in the queue"
    );
}

#[test]
fn durability_watermark_only_moves_forward() {
    let coordinator = CommitCoordinator::new();
    coordinator.note_written(7);
    coordinator.mark_durable(7);
    coordinator.mark_durable(3);
    assert_eq!(coordinator.durable_through(), 7);
}

#[test]
fn failed_leader_does_not_publish_unsynced_prefix() {
    let coordinator = CommitCoordinator::new();
    let first = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    let second = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let mut batch = coordinator.take_pending();
    let writing = batch.pop_front().unwrap();
    assert_eq!(writing.ticket, first);
    coordinator.note_written(first);
    coordinator.requeue(batch.into_iter());

    assert_eq!(
        coordinator.durable_through(),
        0,
        "owning log bytes is not the same as fsyncing them"
    );
    assert_eq!(coordinator.written_through(), first);

    coordinator.mark_durable(second);
    assert_eq!(
        coordinator.durable_through(),
        first,
        "durable cannot pass a ticket that was not owned"
    );
}

#[test]
fn failed_mid_batch_leader_does_not_cover_retry_hole() {
    let coordinator = CommitCoordinator::new();
    let t2 = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let t3 = coordinator.enqueue(3, empty_record(30), SyncMode::Full);
    let t4 = coordinator.enqueue(4, empty_record(40), SyncMode::Full);
    assert_eq!((t2, t3, t4), (1, 2, 3));

    let mut batch = coordinator.take_pending();
    let _owned = batch.pop_front().unwrap();
    let _retry = batch.pop_front().unwrap();
    let later = batch.pop_front().unwrap();
    coordinator.note_written(t2);
    coordinator.request_retry(t3);
    coordinator.requeue(std::iter::once(later));
    coordinator.note_written(t4);
    coordinator.mark_durable(coordinator.written_through());

    assert!(
        coordinator.durable_through() < t3,
        "T3 is a retry hole; durable must not cover it (durable={}, t3={t3})",
        coordinator.durable_through()
    );
    assert!(
        !matches!(
            coordinator.take_work(),
            GroupWork::Lead { writing, .. } if writing.ticket >= t3
        ),
        "later tickets cannot be taken while an earlier ticket is a retry hole"
    );
    assert!(
        coordinator.take_retry(t3),
        "T3 must still be retrying, not acked via the watermark"
    );
}

/// The owner drops after `take_work` and before `request_retry`. The retry
/// set must stay empty, or every later ticket parks.
#[test]
fn dropped_claimed_ticket_is_not_a_retry_hole() {
    let coordinator = CommitCoordinator::new();
    let claimed = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    let later = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let GroupWork::Lead { writing, rest } = coordinator.take_work() else {
        panic!("expected the first commit to lead");
    };
    assert_eq!(writing.ticket, claimed);

    assert!(
        !coordinator.drop_pending(claimed),
        "the leader already took the ticket out of the queue"
    );
    coordinator.request_retry(claimed);
    coordinator.requeue(rest.into_iter());

    match coordinator.take_work() {
        GroupWork::Lead { writing, .. } => assert_eq!(writing.ticket, later),
        GroupWork::SyncPrefix => panic!("sync without a written record"),
        GroupWork::None => {
            panic!("later commit stayed parked after the owner dropped the claimed ticket")
        }
    }
}

#[test]
fn live_owner_retry_still_blocks_later_ticket() {
    let coordinator = CommitCoordinator::new();
    let claimed = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    let later = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let GroupWork::Lead { writing, rest } = coordinator.take_work() else {
        panic!("expected the first commit to lead");
    };
    assert_eq!(writing.ticket, claimed);
    coordinator.request_retry(claimed);
    coordinator.requeue(rest.into_iter());

    assert!(
        matches!(coordinator.take_work(), GroupWork::None),
        "a live owner still has to take the retry before later commits run"
    );
    assert!(coordinator.take_retry(claimed));
    match coordinator.take_work() {
        GroupWork::Lead { writing, .. } => assert_eq!(writing.ticket, later),
        GroupWork::SyncPrefix => panic!("sync without a written record"),
        GroupWork::None => panic!("later commit stayed parked after the retry was taken"),
    }
}

#[test]
fn drop_after_retry_does_not_leave_a_hole() {
    let coordinator = CommitCoordinator::new();
    let claimed = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    let later = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let GroupWork::Lead { writing, rest } = coordinator.take_work() else {
        panic!("expected the first commit to lead");
    };
    assert_eq!(writing.ticket, claimed);
    coordinator.request_retry(claimed);
    assert!(!coordinator.drop_pending(claimed));
    coordinator.requeue(rest.into_iter());

    match coordinator.take_work() {
        GroupWork::Lead { writing, .. } => assert_eq!(writing.ticket, later),
        GroupWork::SyncPrefix => panic!("sync without a written record"),
        GroupWork::None => panic!("drop after retry left a hole"),
    }
}
