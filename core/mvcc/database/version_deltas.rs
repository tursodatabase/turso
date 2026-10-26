//! Old row versions stored as deltas.
//!
//! A commit that updates a row ends the old version of the row and begins a new
//! version. The two records usually differ in a few columns. Thus the commit can
//! replace the record of the old version with a delta (see [`record_delta`])
//! that rebuilds it from the record of the new version.
//!
//! The base of a delta version is the next newer version that began at the
//! timestamp that ended the delta version. A base always has a committed
//! `begin`, so a rollback never removes a base. GC can remove a base. Then it
//! also removes the payload of each delta version that it keeps (see
//! [`remove_payloads_without_base`]).

use super::{record_delta, PayloadKind, RowVersion, TxTimestampOrID};
use crate::alloc::ConcurrentAllocator;
use crate::turso_assert;
use smallvec::SmallVec;
use std::borrow::Cow;

/// The default most delta versions that one read applies to rebuild a record.
pub(super) const DEFAULT_MAX_DELTA_RUN: usize = 16;

/// A record shorter than this keeps its bytes, because its delta saves too little.
const MIN_RECORD_LEN_FOR_DELTA: usize = 64;

/// The record of the table row version at `index`. The record of a delta
/// version is rebuilt from the newer versions of the chain.
pub(crate) fn version_record(versions: &[RowVersion], index: usize) -> Cow<'_, [u8]> {
    if !versions[index].is_delta() {
        return Cow::Borrowed(stored_bytes(&versions[index]));
    }
    // The deltas from the version at `index` to its base, oldest first.
    let mut deltas: SmallVec<[&[u8]; DEFAULT_MAX_DELTA_RUN]> = SmallVec::new();
    let mut base = index;
    while versions[base].is_delta() {
        deltas.push(stored_bytes(&versions[base]));
        base = delta_base_index(versions, base);
    }
    let mut record = Vec::new();
    record_delta::apply(
        stored_bytes(&versions[base]),
        deltas.into_iter().rev(),
        &mut record,
    )
    .unwrap_or_else(|err| panic!("cannot rebuild the record of a delta version: {err}"));
    Cow::Owned(record)
}

/// Replaces the record of the version at `index` with a delta, when the version
/// ended at the commit that began a newer version of the row, and when the
/// delta is at most half of the record. Returns true if it did.
///
/// `max_run` is the most delta versions that a read of this chain may apply.
pub(super) fn store_as_delta<A: ConcurrentAllocator>(
    versions: &mut [RowVersion],
    index: usize,
    max_run: usize,
    alloc: A,
) -> bool {
    let mut delta = record_delta::DeltaBytes::new();
    {
        let version = &versions[index];
        if version.payload_kind != PayloadKind::Record || version.begin().is_none() {
            return false;
        }
        let Some(base) = base_index(versions, index) else {
            return false;
        };
        let record = stored_bytes(version);
        if record.len() < MIN_RECORD_LEN_FOR_DELTA
            || deltas_reaching(versions, index) + 1 + delta_hops(versions, base) > max_run
        {
            return false;
        }
        let base_record = version_record(versions, base);
        if !record_delta::encode(&base_record, record, record.len() / 2, &mut delta) {
            return false;
        }
    }
    let Ok(delta) = crate::with_mv_store_allocation_site!(
        RowPayload,
        crate::alloc::try_arc_slice_from_slice_in(&delta, alloc)
    ) else {
        return false;
    };
    #[cfg(debug_assertions)]
    let record = versions[index].data.clone();
    versions[index].data = Some(delta);
    versions[index].payload_kind = PayloadKind::Delta;
    #[cfg(debug_assertions)]
    debug_assert_eq!(
        version_record(versions, index).as_ref(),
        record.as_deref().expect("the version had a record")
    );
    true
}

/// Removes the payload of each delta version that GC keeps while it removes
/// the base of the delta. `kept[i]` tells if GC keeps `versions[i]`.
///
/// No reader can see such a version: it ended when its base began, and GC
/// removes only versions that ended at or before `lwm`. GC keeps the version
/// only for its timestamps and flags, which tell the checkpoint what to write.
pub(super) fn remove_payloads_without_base(versions: &mut [RowVersion], kept: &[bool], lwm: u64) {
    // From newest to oldest, so that a delta whose base lost its payload also
    // loses its own payload.
    for index in (0..versions.len()).rev() {
        if !kept[index] || !versions[index].is_delta() {
            continue;
        }
        let base = delta_base_index(versions, index);
        if kept[base] && versions[base].payload_kind != PayloadKind::Removed {
            continue;
        }
        turso_assert!(
            matches!(versions[index].end(), Some(TxTimestampOrID::Timestamp(end)) if end <= lwm),
            "GC removes the payload only of a version that no reader can see"
        );
        versions[index].data = None;
        versions[index].payload_kind = PayloadKind::Removed;
    }
}

/// Asserts that each delta version of the chain still has its base.
pub(super) fn assert_deltas_have_bases(versions: &[RowVersion]) {
    for index in 0..versions.len() {
        if versions[index].is_delta() {
            turso_assert!(
                base_index(versions, index).is_some(),
                "a delta version lost its base"
            );
        }
    }
}

/// The stored bytes of a table row version: its record, or its delta.
fn stored_bytes(version: &RowVersion) -> &[u8] {
    turso_assert!(
        version.payload_kind != PayloadKind::Removed,
        "GC removed the payload of this version because no reader can see it"
    );
    version
        .data
        .as_deref()
        .expect("a table row version holds its record or a delta")
}

/// The index of the next newer version that began when the version at `index`
/// ended. A delta version rebuilds its record from that version.
fn base_index(versions: &[RowVersion], index: usize) -> Option<usize> {
    let end = versions[index].end();
    if !matches!(end, Some(TxTimestampOrID::Timestamp(_))) {
        return None;
    }
    versions[index + 1..]
        .iter()
        .position(|version| version.begin() == end)
        .map(|offset| index + 1 + offset)
}

fn delta_base_index(versions: &[RowVersion], index: usize) -> usize {
    base_index(versions, index).expect("the base of a delta version is in its chain")
}

/// The number of delta versions that a read applies to rebuild the record of
/// the version at `index`.
fn delta_hops(versions: &[RowVersion], mut index: usize) -> usize {
    let mut hops = 0;
    while versions[index].is_delta() {
        index = delta_base_index(versions, index);
        hops += 1;
    }
    hops
}

/// The number of older delta versions whose rebuild goes through the version
/// at `index`.
fn deltas_reaching(versions: &[RowVersion], mut index: usize) -> usize {
    let mut count = 0;
    loop {
        let begin = versions[index].begin();
        if !matches!(begin, Some(TxTimestampOrID::Timestamp(_))) {
            return count;
        }
        let Some(older) = versions[..index]
            .iter()
            .rposition(|version| version.end() == begin)
        else {
            return count;
        };
        if !versions[older].is_delta() || base_index(versions, older) != Some(index) {
            return count;
        }
        count += 1;
        index = older;
    }
}
