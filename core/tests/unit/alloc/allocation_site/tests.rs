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
