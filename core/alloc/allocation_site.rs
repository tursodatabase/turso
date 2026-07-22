use std::cell::Cell;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AllocationSite {
    MvStore(MvStoreAllocationSite),
    MvccCheckpoint(MvccCheckpointAllocationSite),
    Schema(SchemaAllocationSite),
    ValueBlob(ValueBlobAllocationSite),
    Vector(VectorAllocationSite),
    NoFaultInjection,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValueBlobAllocationSite {
    Concat,
    FromSlice,
    JsonbConstruction,
    JsonbCopy,
    Hash128,
    RecordDecode,
    CloneFrom,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MvStoreAllocationSite {
    RootpageMappingInsert,
    TxInsert,
    FinalizedTxStateInsert,
    TableRowsEntry,
    IndexRowsEntry,
    IndexKeyEntry,
    RowVersionReserve,
    RowPayload,
    SchemaRowPayload,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SchemaAllocationSite {
    MakeMut,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MvccCheckpointAllocationSite {
    CheckpointWriteSet,
    CheckpointIndexWriteSet,
    CheckpointMetadataPayload,
    CheckpointSequenceCompactions,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum VectorAllocationSite {
    Parse,
    Convert,
    Concat,
    Slice,
    Serialize,
    SparseConstruction,
    Float8Construction,
    IndexPayloadCopy,
}

impl From<MvStoreAllocationSite> for AllocationSite {
    fn from(site: MvStoreAllocationSite) -> Self {
        Self::MvStore(site)
    }
}

impl From<MvccCheckpointAllocationSite> for AllocationSite {
    fn from(site: MvccCheckpointAllocationSite) -> Self {
        Self::MvccCheckpoint(site)
    }
}

impl From<SchemaAllocationSite> for AllocationSite {
    fn from(site: SchemaAllocationSite) -> Self {
        Self::Schema(site)
    }
}

impl From<VectorAllocationSite> for AllocationSite {
    fn from(site: VectorAllocationSite) -> Self {
        Self::Vector(site)
    }
}

impl From<ValueBlobAllocationSite> for AllocationSite {
    fn from(site: ValueBlobAllocationSite) -> Self {
        Self::ValueBlob(site)
    }
}

thread_local! {
    static CURRENT_ALLOCATION_SITE: Cell<Option<AllocationSite>> = const { Cell::new(None) };
}

pub struct AllocationSiteGuard {
    previous: Option<AllocationSite>,
}

impl Drop for AllocationSiteGuard {
    fn drop(&mut self) {
        CURRENT_ALLOCATION_SITE.with(|slot| slot.set(self.previous));
    }
}

pub fn enter_allocation_site(site: impl Into<AllocationSite>) -> AllocationSiteGuard {
    let site = site.into();
    let previous = CURRENT_ALLOCATION_SITE.with(|slot| {
        let previous = slot.get();
        let site = if matches!(previous, Some(AllocationSite::NoFaultInjection)) {
            AllocationSite::NoFaultInjection
        } else {
            site
        };
        slot.set(Some(site));
        previous
    });
    AllocationSiteGuard { previous }
}

pub fn current_allocation_site() -> Option<AllocationSite> {
    CURRENT_ALLOCATION_SITE.with(Cell::get)
}

#[macro_export]
macro_rules! without_allocation_faults {
    ($expr:expr) => {{
        #[cfg(feature = "allocation_metric")]
        let _turso_allocation_site_guard =
            $crate::alloc::enter_allocation_site($crate::alloc::AllocationSite::NoFaultInjection);
        $expr
    }};
}

#[macro_export]
macro_rules! with_mv_store_allocation_site {
    ($site:ident, $expr:expr) => {{
        #[cfg(feature = "allocation_metric")]
        let _turso_allocation_site_guard =
            $crate::alloc::enter_allocation_site($crate::alloc::MvStoreAllocationSite::$site);
        $expr
    }};
}

#[macro_export]
macro_rules! with_value_blob_allocation_site {
    ($site:ident, $expr:expr) => {{
        #[cfg(feature = "allocation_metric")]
        let _turso_allocation_site_guard =
            $crate::alloc::enter_allocation_site($crate::alloc::ValueBlobAllocationSite::$site);
        $expr
    }};
}

#[cfg(test)]
mod tests {
    use super::{
        current_allocation_site, enter_allocation_site, AllocationSite, MvStoreAllocationSite,
    };

    #[test]
    fn allocation_site_guard_restores_previous_site() {
        assert_eq!(current_allocation_site(), None);
        {
            let _outer = enter_allocation_site(MvStoreAllocationSite::RootpageMappingInsert);
            assert_eq!(
                current_allocation_site(),
                Some(AllocationSite::MvStore(
                    MvStoreAllocationSite::RootpageMappingInsert
                ))
            );

            {
                let _inner = enter_allocation_site(AllocationSite::NoFaultInjection);
                assert_eq!(
                    current_allocation_site(),
                    Some(AllocationSite::NoFaultInjection)
                );
            }

            assert_eq!(
                current_allocation_site(),
                Some(AllocationSite::MvStore(
                    MvStoreAllocationSite::RootpageMappingInsert
                ))
            );
        }
        assert_eq!(current_allocation_site(), None);
    }

    #[test]
    fn no_fault_injection_site_dominates_nested_sites() {
        let _outer = enter_allocation_site(AllocationSite::NoFaultInjection);
        assert_eq!(
            current_allocation_site(),
            Some(AllocationSite::NoFaultInjection)
        );
        {
            let _inner = enter_allocation_site(MvStoreAllocationSite::RowVersionReserve);
            assert_eq!(
                current_allocation_site(),
                Some(AllocationSite::NoFaultInjection)
            );
        }
        assert_eq!(
            current_allocation_site(),
            Some(AllocationSite::NoFaultInjection)
        );
    }
}
