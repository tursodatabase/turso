use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::alloc::{alloc, dealloc};
use std::ptr::NonNull;
use std::sync::Mutex;
use turso_core::alloc::{AllocError, Layout, SetAllocatorError, TursoAllocBackend, set_allocator};

pub(crate) struct AllocationFaultAllocator {
    probability: f64,
    rng: Mutex<ChaCha8Rng>,
}

impl AllocationFaultAllocator {
    pub(crate) fn new(seed: u64, probability: f64) -> Self {
        Self {
            probability,
            rng: Mutex::new(ChaCha8Rng::seed_from_u64(seed)),
        }
    }

    fn should_fail(&self) -> bool {
        let mut rng = self.rng.lock().unwrap();
        rng.random_bool(self.probability)
    }
}

unsafe impl TursoAllocBackend for AllocationFaultAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if self.should_fail() {
            return Err(AllocError);
        }
        if layout.size() == 0 {
            return Ok(NonNull::slice_from_raw_parts(NonNull::<u8>::dangling(), 0));
        }
        let ptr = unsafe { alloc(layout) };
        NonNull::new(ptr)
            .map(|ptr| NonNull::slice_from_raw_parts(ptr, layout.size()))
            .ok_or(AllocError)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() != 0 {
            unsafe {
                dealloc(ptr.as_ptr(), layout);
            }
        }
    }
}

pub(crate) fn validate_probability(probability: f64) -> anyhow::Result<bool> {
    if probability == 0.0 {
        return Ok(false);
    }
    if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
        anyhow::bail!("allocation fault probability must be between 0.0 and 1.0");
    }
    Ok(true)
}

pub(crate) fn install(seed: u64, probability: f64) -> anyhow::Result<bool> {
    if !validate_probability(probability)? {
        return Ok(false);
    }

    let allocator = Box::leak(Box::new(AllocationFaultAllocator::new(seed, probability)));
    unsafe {
        set_allocator(allocator).map_err(|err| match err {
            SetAllocatorError::AlreadyInitialized => anyhow::anyhow!(
                "Turso allocator was already initialized before Whopper installed allocation faults"
            ),
        })?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocator_fails_when_probability_is_one() {
        let allocator = AllocationFaultAllocator::new(1, 1.0);
        let layout = Layout::from_size_align(8, 8).unwrap();
        assert!(allocator.allocate(layout).is_err());
    }

    #[test]
    fn allocator_delegates_when_probability_is_zero() {
        let allocator = AllocationFaultAllocator::new(1, 0.0);
        let layout = Layout::from_size_align(8, 8).unwrap();
        let ptr = allocator.allocate(layout).expect("allocation succeeds");
        unsafe {
            allocator.deallocate(ptr.cast(), layout);
        }
    }
}
