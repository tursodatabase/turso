use super::*;
#[cfg(not(all(target_os = "windows", feature = "experimental_win_iocp")))]
use crate::io::PlatformIO;
use crate::io::IO;
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
    #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
    {
        Arc::new(crate::WindowsIOCP::new().unwrap())
    }

    #[cfg(not(all(target_os = "windows", feature = "experimental_win_iocp")))]
    {
        Arc::new(PlatformIO::new().unwrap())
    }
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
    #[cfg(unix)]
    {
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

    #[cfg(windows)]
    {
        let mut child = std::process::Command::new("cmd")
            .args(["/C", "exit", "0"])
            .spawn()
            .expect("spawn exited child");
        let pid = child.id();
        let status = child.wait().expect("wait exited child");
        assert!(status.success(), "child did not exit cleanly");
        pid
    }
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
fn process_scoped_mapping_ignores_stale_same_pid_writer_owner_field() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("coordination.tshm");
    let mapped = create_process_scoped_mapping(&path);
    let stale_owner = SharedOwnerRecord::new(std::process::id(), 7);
    assert_ne!(stale_owner, mapped.owner_record());

    mapped
        .header()
        .writer_owner
        .store(stale_owner.raw(), Ordering::Release);

    assert!(mapped.try_acquire_writer(mapped.owner_record()));
    assert_eq!(mapped.writer_owner(), Some(mapped.owner_record()));
    mapped.release_writer(mapped.owner_record());
    assert_eq!(mapped.writer_owner(), None);
}

#[test]
fn process_scoped_mapping_reclaims_stale_same_pid_reader_slot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("coordination.tshm");
    let mapped = create_process_scoped_mapping(&path);
    let stale_owner = SharedOwnerRecord::new(std::process::id(), 9);
    assert_ne!(stale_owner, mapped.owner_record());

    mapped.reader_bitmap_words()[0].fetch_and(!1u64, Ordering::Release);
    mapped.reader_frames()[0].store(17, Ordering::Release);
    mapped.reader_owners()[0].store(stale_owner.raw(), Ordering::Release);

    assert_eq!(mapped.min_active_reader_frame(), None);
    assert_eq!(mapped.reader_owner(0), None);

    let reader = mapped.register_reader(mapped.owner_record(), 23).unwrap();
    assert_eq!(reader.slot_index, 0);
    mapped.unregister_reader(reader);
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
    mapped.reader_owners()[0].store(SharedOwnerRecord::new(u32::MAX, 3).raw(), Ordering::Release);

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
fn mapped_shared_wal_coordination_last_process_probe_reacquires_shared_lifetime_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("coordination.tshm");
    let mapped_a = create_mapping(&path);

    assert!(
        mapped_a.is_last_process_mapping(),
        "single mapping should identify itself as the last live process mapping"
    );

    let mapped_b = create_mapping(&path);
    if mapped_a.uses_linux_ofd_locking() {
        assert_eq!(
            mapped_b.open_mode(),
            SharedWalCoordinationOpenMode::MultiProcess,
            "lifetime lock probe must leave the original mapping holding the shared lifetime lock"
        );
    } else {
        assert_eq!(
            mapped_b.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive,
            "process-scoped fcntl locks are per-process, so a same-process duplicate open cannot observe the lifetime shared lock"
        );
    }
}

#[test]
fn mapped_shared_wal_coordination_snapshot_waits_for_stable_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("coordination.tshm");
    let mapped = Arc::new(create_mapping(&path));
    let expected = SharedWalCoordinationHeader {
        max_frame: 14,
        nbackfills: 8,
        transaction_count: 9,
        visibility_generation: 10,
        checkpoint_seq: 5,
        checkpoint_epoch: 7,
        page_size: 4096,
        salt_1: 17,
        salt_2: 23,
        checksum_1: 31,
        checksum_2: 37,
        reader_slot_count: 64,
    };

    let header = mapped.header();
    let seq = header.snapshot_seq.load(Ordering::Acquire);
    assert_eq!(
        seq & 1,
        0,
        "fresh mapping should start with a stable snapshot sequence"
    );
    assert!(
        header
            .snapshot_seq
            .compare_exchange(seq, seq + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok(),
        "test must acquire the snapshot write sequence"
    );

    let reader = mapped.clone();
    let handle = std::thread::spawn(move || reader.snapshot());
    std::thread::sleep(std::time::Duration::from_millis(10));
    assert!(
        !handle.is_finished(),
        "snapshot readers must wait while a writer holds the sequence lock"
    );

    header
        .max_frame
        .store(expected.max_frame, Ordering::Release);
    header
        .nbackfills
        .store(expected.nbackfills, Ordering::Release);
    header
        .transaction_count
        .store(expected.transaction_count, Ordering::Release);
    header
        .visibility_generation
        .store(expected.visibility_generation, Ordering::Release);
    header
        .checkpoint_seq
        .store(expected.checkpoint_seq, Ordering::Release);
    header
        .checkpoint_epoch
        .store(expected.checkpoint_epoch, Ordering::Release);
    header
        .page_size
        .store(expected.page_size, Ordering::Release);
    header.salt_1.store(expected.salt_1, Ordering::Release);
    header.salt_2.store(expected.salt_2, Ordering::Release);
    header
        .checksum_1
        .store(expected.checksum_1, Ordering::Release);
    header
        .checksum_2
        .store(expected.checksum_2, Ordering::Release);
    header.snapshot_seq.store(seq + 2, Ordering::Release);

    assert_eq!(handle.join().unwrap(), expected);
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
    let stale_owner = SharedOwnerRecord::new(exited_child_pid(), 7);

    mapped
        .header()
        .writer_owner
        .store(stale_owner.raw(), Ordering::Release);
    assert!(mapped.try_acquire_writer(mapped.owner_record()));
    mapped.release_writer(mapped.owner_record());
}

#[test]
fn mapped_shared_wal_coordination_ignores_stale_checkpoint_owner_field() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("coordination.tshm");
    let mapped = create_mapping(&path);
    let stale_owner = SharedOwnerRecord::new(exited_child_pid(), 8);

    mapped
        .header()
        .checkpoint_owner
        .store(stale_owner.raw(), Ordering::Release);
    assert!(mapped.try_acquire_checkpoint(mapped.owner_record()));
    mapped.release_checkpoint(mapped.owner_record());
}

#[test]
fn mapped_shared_wal_coordination_publish_commit_keeps_monotonic_transaction_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("coordination.tshm");
    let mapped = create_mapping(&path);

    mapped.publish_commit(12, 31, 37, 9);
    mapped.publish_commit(11, 41, 43, 8);

    let snapshot = mapped.snapshot();
    assert_eq!(snapshot.max_frame, 12);
    assert_eq!(snapshot.transaction_count, 9);
    assert_eq!(snapshot.checksum_1, 31);
    assert_eq!(snapshot.checksum_2, 37);
}

#[test]
fn mapped_shared_wal_coordination_reclaims_stale_reader_slots() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("coordination.tshm");
    let mapped = create_mapping(&path);
    let stale_owner = SharedOwnerRecord::new(exited_child_pid(), 9);

    mapped.reader_bitmap_words()[0].fetch_and(!1u64, Ordering::Release);
    mapped.reader_frames()[0].store(17, Ordering::Release);
    mapped.reader_owners()[0].store(stale_owner.raw(), Ordering::Release);

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
