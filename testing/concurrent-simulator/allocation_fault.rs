use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use turso_core::alloc::{
    AllocError, AllocationSite, ApiAllocator, Global, Layout, MvStoreAllocationSite,
    MvccCheckpointAllocationSite, SchemaAllocationSite, SetAllocatorError, TursoAllocBackend,
    ValueBlobAllocationSite, VectorAllocationSite,
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
            MvStoreAllocationSite::RowPayload => 12,
            MvStoreAllocationSite::SchemaRowPayload => 13,
        },
        AllocationSite::MvccCheckpoint(site) => match site {
            MvccCheckpointAllocationSite::CheckpointWriteSet => 8,
            MvccCheckpointAllocationSite::CheckpointIndexWriteSet => 9,
            MvccCheckpointAllocationSite::CheckpointMetadataPayload => 10,
            MvccCheckpointAllocationSite::CheckpointSequenceCompactions => 11,
        },
        AllocationSite::Schema(site) => match site {
            SchemaAllocationSite::MakeMut => 14,
        },
        AllocationSite::ValueBlob(site) => match site {
            ValueBlobAllocationSite::Concat => 23,
            ValueBlobAllocationSite::RecordDecode => 24,
            ValueBlobAllocationSite::FromSlice => 25,
            ValueBlobAllocationSite::JsonbCopy => 26,
            ValueBlobAllocationSite::Hash128 => 27,
            ValueBlobAllocationSite::JsonbConstruction => 28,
            ValueBlobAllocationSite::CloneFrom => 29,
            ValueBlobAllocationSite::RecordBuild => 30,
            ValueBlobAllocationSite::RecordCopy => 31,
            ValueBlobAllocationSite::AggAccumulate => 32,
        },
        AllocationSite::Vector(site) => match site {
            VectorAllocationSite::Parse => 15,
            VectorAllocationSite::Convert => 16,
            VectorAllocationSite::Concat => 17,
            VectorAllocationSite::Slice => 18,
            VectorAllocationSite::Serialize => 19,
            VectorAllocationSite::SparseConstruction => 20,
            VectorAllocationSite::Float8Construction => 21,
            VectorAllocationSite::IndexPayloadCopy => 22,
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

    #[test]
    fn vector_allocation_sites_have_distinct_ids() {
        let sites = [
            VectorAllocationSite::Parse,
            VectorAllocationSite::Convert,
            VectorAllocationSite::Concat,
            VectorAllocationSite::Slice,
            VectorAllocationSite::Serialize,
            VectorAllocationSite::SparseConstruction,
            VectorAllocationSite::Float8Construction,
            VectorAllocationSite::IndexPayloadCopy,
        ];
        let ids = sites.map(|site| allocation_site_id(AllocationSite::Vector(site)));
        for (index, id) in ids.iter().enumerate() {
            assert!(!ids[index + 1..].contains(id), "duplicate site id {id}");
        }
    }

    #[test]
    fn value_blob_allocation_sites_have_distinct_ids() {
        let sites = [
            ValueBlobAllocationSite::Concat,
            ValueBlobAllocationSite::RecordDecode,
            ValueBlobAllocationSite::FromSlice,
            ValueBlobAllocationSite::JsonbConstruction,
            ValueBlobAllocationSite::JsonbCopy,
            ValueBlobAllocationSite::Hash128,
        ];
        let ids = sites.map(|site| allocation_site_id(AllocationSite::ValueBlob(site)));
        for (index, id) in ids.iter().enumerate() {
            assert!(!ids[index + 1..].contains(id), "duplicate site id {id}");
        }
    }

    #[test]
    fn vector_allocation_site_is_fault_injectable() {
        static INJECTOR: SimulatorAllocationFaultInjector = SimulatorAllocationFaultInjector {
            enabled: AtomicBool::new(true),
            seed: AtomicU64::new(7),
            threshold: AtomicU64::new(u64::MAX),
            injected_faults: AtomicU64::new(0),
        };

        let _context = INJECTOR.enter_context(AllocationFaultContext {
            step: 1,
            fiber_idx: 2,
            execution_id: 3,
        });
        let _site = turso_core::alloc::enter_allocation_site(VectorAllocationSite::Serialize);
        let layout = Layout::from_size_align(16, 8).unwrap();

        assert!(INJECTOR.allocate(layout).is_err());
        assert_eq!(INJECTOR.injected_faults(), 1);
    }

    #[test]
    fn value_blob_concat_site_is_fault_injectable() {
        static INJECTOR: SimulatorAllocationFaultInjector = SimulatorAllocationFaultInjector {
            enabled: AtomicBool::new(true),
            seed: AtomicU64::new(11),
            threshold: AtomicU64::new(u64::MAX),
            injected_faults: AtomicU64::new(0),
        };

        let _context = INJECTOR.enter_context(AllocationFaultContext {
            step: 4,
            fiber_idx: 5,
            execution_id: 6,
        });
        let _site = turso_core::alloc::enter_allocation_site(ValueBlobAllocationSite::Concat);
        let layout = Layout::from_size_align(16, 8).unwrap();

        assert!(INJECTOR.allocate(layout).is_err());
        assert_eq!(INJECTOR.injected_faults(), 1);
    }

    #[cfg(nightly)]
    #[test]
    fn production_allocation_failures_propagate_and_leave_state_reusable() {
        let injector =
            install_global_allocation_fault_backend(AllocationFaultConfig { probability: 1.0 }, 29)
                .unwrap()
                .unwrap();

        let lhs = turso_core::Value::from_slice(&[1, 2]).unwrap();
        let rhs = turso_core::Value::from_slice(&[3, 4]).unwrap();
        let blob = turso_core::Value::from_slice(&[1, 2, 3, 4]).unwrap();
        let vector_bytes = 1.0f32.to_le_bytes();
        let json = turso_core::Value::build_text(r#"{"key":"value"}"#);
        let json_cache = turso_core::json::JsonCacheCell::new();
        turso_core::json::jsonb(&json, &json_cache).unwrap();

        let before = injector.injected_faults();
        {
            let _context = injector.enter_context(AllocationFaultContext {
                step: 10,
                fiber_idx: 11,
                execution_id: 12,
            });
            assert!(lhs.exec_concat(&rhs).is_err());
        }
        assert_eq!(injector.injected_faults(), before + 1);
        assert_eq!(
            lhs.exec_concat(&rhs).unwrap().to_blob(),
            Some([1, 2, 3, 4].as_slice())
        );

        let before = injector.injected_faults();
        {
            let _context = injector.enter_context(AllocationFaultContext {
                step: 13,
                fiber_idx: 14,
                execution_id: 15,
            });
            assert!(blob.exec_cast("BLOB").is_err());
        }
        assert_eq!(injector.injected_faults(), before + 1);
        assert_eq!(blob.exec_cast("BLOB").unwrap().to_blob(), blob.to_blob());

        let before = injector.injected_faults();
        {
            let _context = injector.enter_context(AllocationFaultContext {
                step: 16,
                fiber_idx: 17,
                execution_id: 18,
            });
            let vector =
                turso_core::vector::vector_types::Vector::from_slice(&vector_bytes).unwrap();
            assert!(turso_core::vector::operations::serialize::vector_serialize(vector).is_err());
        }
        assert_eq!(injector.injected_faults(), before + 1);
        let vector = turso_core::vector::vector_types::Vector::from_slice(&vector_bytes).unwrap();
        assert_eq!(
            turso_core::vector::operations::serialize::vector_serialize(vector)
                .unwrap()
                .to_blob(),
            Some(vector_bytes.as_slice())
        );

        let before = injector.injected_faults();
        {
            let _context = injector.enter_context(AllocationFaultContext {
                step: 19,
                fiber_idx: 20,
                execution_id: 21,
            });
            assert!(matches!(
                turso_core::json::jsonb(&json, &json_cache),
                Err(turso_core::LimboError::OutOfMemory)
            ));
        }
        assert_eq!(injector.injected_faults(), before + 1);
        assert!(turso_core::json::jsonb(&json, &json_cache).is_ok());

        let insert_key = turso_core::Value::build_text(r#"{"insert":"value"}"#);
        let insert_cache = turso_core::json::JsonCacheCell::new();
        let prepared_json =
            turso_core::json::convert_dbtype_to_jsonb(&insert_key, turso_core::json::Conv::Strict)
                .unwrap();
        let before = injector.injected_faults();
        {
            let _context = injector.enter_context(AllocationFaultContext {
                step: 22,
                fiber_idx: 23,
                execution_id: 24,
            });
            assert!(matches!(
                insert_cache.get_or_insert_with(&insert_key, |_| Ok(prepared_json)),
                Err(turso_core::LimboError::OutOfMemory)
            ));
        }
        assert_eq!(injector.injected_faults(), before + 1);

        let prepared_json =
            turso_core::json::convert_dbtype_to_jsonb(&insert_key, turso_core::json::Conv::Strict)
                .unwrap();
        let insert_closure_called = Cell::new(false);
        assert!(
            insert_cache
                .get_or_insert_with(&insert_key, |_| {
                    insert_closure_called.set(true);
                    Ok(prepared_json)
                })
                .is_ok()
        );
        assert!(insert_closure_called.get());

        let before = injector.injected_faults();
        {
            let _context = injector.enter_context(AllocationFaultContext {
                step: 25,
                fiber_idx: 26,
                execution_id: 27,
            });
            assert!(turso_core::json::is_json_valid(&json).is_err());
        }
        assert_eq!(injector.injected_faults(), before + 1);
        assert_eq!(
            turso_core::json::is_json_valid(&json).unwrap().as_int(),
            Some(1)
        );
    }
}
