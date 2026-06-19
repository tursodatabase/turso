use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use turso_core::alloc::{
    AllocError, AllocationSite, ApiAllocator, Global, Layout, MvStoreAllocationSite,
    SetAllocatorError, TursoAllocBackend,
};

#[derive(Debug, Clone, Copy)]
pub struct AllocationFaultConfig {
    pub probability: f64,
}

impl Default for AllocationFaultConfig {
    fn default() -> Self {
        Self { probability: 0.0 }
    }
}

impl AllocationFaultConfig {
    pub fn is_enabled(self) -> bool {
        self.probability > 0.0
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AllocationFaultContext {
    pub(crate) step: u64,
    pub(crate) fiber_idx: u64,
    pub(crate) execution_id: u64,
}

thread_local! {
    static CURRENT_CONTEXT: Cell<Option<AllocationFaultContext>> = const { Cell::new(None) };
    static ALLOCATION_OCCURRENCE: Cell<u64> = const { Cell::new(0) };
}

pub(crate) struct AllocationFaultContextGuard {
    previous_context: Option<AllocationFaultContext>,
    previous_occurrence: u64,
}

impl Drop for AllocationFaultContextGuard {
    fn drop(&mut self) {
        CURRENT_CONTEXT.with(|slot| slot.set(self.previous_context));
        ALLOCATION_OCCURRENCE.with(|slot| slot.set(self.previous_occurrence));
    }
}

pub(crate) struct SimulatorAllocationFaultInjector {
    enabled: AtomicBool,
    seed: AtomicU64,
    threshold: AtomicU64,
    injected_faults: AtomicU64,
}

static ALLOCATION_FAULT_INJECTOR: SimulatorAllocationFaultInjector =
    SimulatorAllocationFaultInjector {
        enabled: AtomicBool::new(false),
        seed: AtomicU64::new(0),
        threshold: AtomicU64::new(0),
        injected_faults: AtomicU64::new(0),
    };
static ALLOCATION_FAULT_BACKEND_INSTALLED: AtomicBool = AtomicBool::new(false);

pub(crate) fn install_global_allocation_fault_backend(
    config: AllocationFaultConfig,
    seed: u64,
) -> anyhow::Result<Option<&'static SimulatorAllocationFaultInjector>> {
    if !config.is_enabled() {
        return Ok(None);
    }
    if !(0.0..=1.0).contains(&config.probability) || config.probability.is_nan() {
        anyhow::bail!(
            "allocation fault probability must be between 0.0 and 1.0, got {}",
            config.probability
        );
    }

    ALLOCATION_FAULT_INJECTOR.configure(config, seed);
    if !ALLOCATION_FAULT_BACKEND_INSTALLED.swap(true, Ordering::AcqRel) {
        if let Err(err) = unsafe { turso_core::alloc::set_allocator(&ALLOCATION_FAULT_INJECTOR) } {
            ALLOCATION_FAULT_BACKEND_INSTALLED.store(false, Ordering::Release);
            return Err(match err {
                SetAllocatorError::AlreadyInitialized => anyhow::anyhow!(
                    "allocation fault injection requires installing Whopper's allocator before \
                     another Turso allocator backend is installed"
                ),
            });
        }
    }

    Ok(Some(&ALLOCATION_FAULT_INJECTOR))
}

impl SimulatorAllocationFaultInjector {
    fn configure(&self, config: AllocationFaultConfig, seed: u64) {
        self.seed.store(seed, Ordering::Relaxed);
        self.threshold
            .store(probability_threshold(config.probability), Ordering::Relaxed);
        self.injected_faults.store(0, Ordering::Relaxed);
        self.enabled.store(true, Ordering::Release);
    }

    pub(crate) fn enter_context(
        &'static self,
        context: AllocationFaultContext,
    ) -> AllocationFaultContextGuard {
        let previous_context = CURRENT_CONTEXT.with(|slot| slot.replace(Some(context)));
        let previous_occurrence = ALLOCATION_OCCURRENCE.with(|slot| slot.replace(0));
        AllocationFaultContextGuard {
            previous_context,
            previous_occurrence,
        }
    }

    pub(crate) fn injected_faults(&self) -> u64 {
        self.injected_faults.load(Ordering::Relaxed)
    }

    fn should_fail(&self, layout: Layout) -> bool {
        if !self.enabled.load(Ordering::Acquire) {
            return false;
        }

        let Some(site) = turso_core::alloc::current_allocation_site() else {
            return false;
        };
        if matches!(site, AllocationSite::NoFaultInjection) {
            return false;
        }
        let Some(context) = CURRENT_CONTEXT.with(Cell::get) else {
            return false;
        };

        let occurrence = ALLOCATION_OCCURRENCE.with(|slot| {
            let occurrence = slot.get();
            slot.set(occurrence.wrapping_add(1));
            occurrence
        });
        let hash = allocation_hash(
            self.seed.load(Ordering::Relaxed),
            context,
            site,
            occurrence,
            layout,
        );
        if hash <= self.threshold.load(Ordering::Relaxed) {
            self.injected_faults.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }
}

unsafe impl TursoAllocBackend for SimulatorAllocationFaultInjector {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if self.should_fail(layout) {
            return Err(AllocError);
        }
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            Global.deallocate(ptr, layout);
        }
    }
}

fn probability_threshold(probability: f64) -> u64 {
    if probability <= 0.0 {
        return 0;
    }
    if probability >= 1.0 {
        return u64::MAX;
    }
    (probability * u64::MAX as f64) as u64
}

fn allocation_hash(
    seed: u64,
    context: AllocationFaultContext,
    site: AllocationSite,
    occurrence: u64,
    layout: Layout,
) -> u64 {
    splitmix64(
        seed ^ context.step.rotate_left(7)
            ^ context.fiber_idx.rotate_left(17)
            ^ context.execution_id.rotate_left(29)
            ^ allocation_site_id(site).rotate_left(41)
            ^ occurrence.rotate_left(53)
            ^ (layout.size() as u64)
            ^ (layout.align() as u64).rotate_left(11),
    )
}

fn allocation_site_id(site: AllocationSite) -> u64 {
    match site {
        AllocationSite::NoFaultInjection => 0,
        AllocationSite::MvStore(site) => match site {
            MvStoreAllocationSite::RootpageMappingInsert => 1,
            MvStoreAllocationSite::TxInsert => 2,
            MvStoreAllocationSite::FinalizedTxStateInsert => 3,
            MvStoreAllocationSite::TableRowsEntry => 4,
            MvStoreAllocationSite::IndexRowsEntry => 5,
            MvStoreAllocationSite::IndexKeyEntry => 6,
            MvStoreAllocationSite::RowVersionReserve => 7,
        },
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probability_threshold_handles_edges() {
        assert_eq!(probability_threshold(0.0), 0);
        assert_eq!(probability_threshold(1.0), u64::MAX);
    }

    #[test]
    fn allocation_hash_changes_by_occurrence() {
        let context = AllocationFaultContext {
            step: 1,
            fiber_idx: 2,
            execution_id: 3,
        };
        let site = AllocationSite::MvStore(MvStoreAllocationSite::TableRowsEntry);
        let layout = Layout::from_size_align(16, 8).unwrap();

        assert_ne!(
            allocation_hash(4, context, site, 0, layout),
            allocation_hash(4, context, site, 1, layout)
        );
    }
}
