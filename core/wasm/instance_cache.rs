use super::{WasmBudget, WasmInstanceApi};
use crate::LimboError;

/// Metrics snapshot from the WASM instance cache.
#[derive(Debug, Default, Clone)]
pub struct WasmCacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub instances_created: u64,
}

struct Entry {
    name: String,
    instance: Box<dyn WasmInstanceApi>,
    memory_bytes: usize,
    last_used: u64,
}

/// Per-ProgramState LRU cache for WASM instances, coordinated with a shared `WasmBudget`.
pub struct WasmInstanceCache {
    entries: Vec<Entry>,
    counter: u64,
    budget: WasmBudget,
    // Accumulated metrics
    hits: u64,
    misses: u64,
    evictions: u64,
    instances_created: u64,
}

impl WasmInstanceCache {
    pub fn new(budget: WasmBudget) -> Self {
        Self {
            entries: Vec::new(),
            counter: 0,
            budget,
            hits: 0,
            misses: 0,
            evictions: 0,
            instances_created: 0,
        }
    }

    /// Look up or create a WASM instance by name. Returns a mutable reference to the instance.
    ///
    /// On cache hit, bumps the LRU counter and returns the existing instance.
    /// On miss, calls `create` to build a new instance, registers its memory with the budget
    /// (evicting LRU entries from this local cache if needed), and inserts it.
    pub fn get_or_create(
        &mut self,
        name: &str,
        create: impl FnOnce() -> Result<Box<dyn WasmInstanceApi>, LimboError>,
    ) -> Result<&mut dyn WasmInstanceApi, LimboError> {
        self.counter += 1;
        let age = self.counter;

        // Check for existing entry
        if let Some(pos) = self.entries.iter().position(|e| e.name == name) {
            self.entries[pos].last_used = age;
            self.hits += 1;
            return Ok(self.entries[pos].instance.as_mut());
        }

        // Cache miss — create a new instance
        self.misses += 1;
        let instance = create()?;
        let memory_bytes = instance.memory_size();
        self.instances_created += 1;

        // Evict LRU entries until there's room in the budget
        while !self.budget.try_reserve(memory_bytes as i64) {
            if self.entries.is_empty() {
                return Err(LimboError::InternalError(format!(
                    "WASM instance cache budget exhausted: need {} bytes, capacity {} bytes, used {} bytes",
                    memory_bytes,
                    self.budget.capacity(),
                    self.budget.used(),
                )));
            }
            self.evict_lru();
        }

        self.entries.push(Entry {
            name: name.to_string(),
            instance,
            memory_bytes,
            last_used: age,
        });

        let last = self.entries.last_mut().unwrap();
        Ok(last.instance.as_mut())
    }

    /// Evict the least recently used entry from this local cache.
    fn evict_lru(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        let lru_pos = self
            .entries
            .iter()
            .enumerate()
            .min_by_key(|(_, e)| e.last_used)
            .map(|(i, _)| i)
            .unwrap();
        let evicted = self.entries.swap_remove(lru_pos);
        self.budget.release(evicted.memory_bytes as i64);
        self.evictions += 1;
    }

    /// Drop all cached instances and release all budget.
    pub fn clear(&mut self) {
        for entry in self.entries.drain(..) {
            self.budget.release(entry.memory_bytes as i64);
        }
    }

    /// Read accumulated metrics (non-destructive).
    pub fn metrics(&self) -> WasmCacheMetrics {
        WasmCacheMetrics {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            instances_created: self.instances_created,
        }
    }
}

impl Drop for WasmInstanceCache {
    fn drop(&mut self) {
        self.clear();
    }
}

// SAFETY: WasmInstanceApi is Send, and the cache is only accessed from one ProgramState at a time.
unsafe impl Send for WasmInstanceCache {}
unsafe impl Sync for WasmInstanceCache {}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mock WASM instance with configurable memory size.
    #[derive(Debug)]
    struct MockWasmInstance {
        mem_size: usize,
    }

    impl MockWasmInstance {
        fn new(mem_size: usize) -> Self {
            Self { mem_size }
        }
    }

    impl WasmInstanceApi for MockWasmInstance {
        fn write_memory(&mut self, _offset: usize, _bytes: &[u8]) -> Result<(), LimboError> {
            Ok(())
        }
        fn read_memory(&self, _offset: usize, _len: usize) -> Result<&[u8], LimboError> {
            Ok(&[])
        }
        fn memory_size(&self) -> usize {
            self.mem_size
        }
        fn malloc(&mut self, _size: i32) -> Result<i32, LimboError> {
            Ok(0)
        }
        fn call_raw(&mut self, _argc: i32, _argv_ptr: i32) -> Result<i64, LimboError> {
            Ok(0)
        }
    }

    #[test]
    fn test_cache_hit() {
        let budget = WasmBudget::new(1_000_000);
        let mut cache = WasmInstanceCache::new(budget);

        // First access: miss + create
        let _ = cache
            .get_or_create("func_a", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();

        // Second access: hit
        let _ = cache
            .get_or_create("func_a", || {
                panic!("should not be called on hit");
            })
            .unwrap();

        let m = cache.metrics();
        assert_eq!(m.hits, 1);
        assert_eq!(m.misses, 1);
        assert_eq!(m.instances_created, 1);
        assert_eq!(m.evictions, 0);
    }

    #[test]
    fn test_lru_eviction() {
        // Budget fits exactly 1 instance of 1000 bytes
        let budget = WasmBudget::new(1000);
        let mut cache = WasmInstanceCache::new(budget.clone());

        // Insert first
        let _ = cache
            .get_or_create("func_a", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();

        // Insert second — must evict first
        let _ = cache
            .get_or_create("func_b", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();

        let m = cache.metrics();
        assert_eq!(m.misses, 2);
        assert_eq!(m.instances_created, 2);
        assert_eq!(m.evictions, 1);
        assert_eq!(budget.used(), 1000);
    }

    #[test]
    fn test_zero_capacity() {
        let budget = WasmBudget::new(0);
        let mut cache = WasmInstanceCache::new(budget);

        let result = cache.get_or_create("func_a", || Ok(Box::new(MockWasmInstance::new(1000))));
        assert!(result.is_err());

        let m = cache.metrics();
        assert_eq!(m.misses, 1);
        assert_eq!(m.instances_created, 1);
        assert_eq!(m.evictions, 0);
    }

    #[test]
    fn test_lru_order() {
        // Budget fits 3 instances of 1000 bytes
        let budget = WasmBudget::new(3000);
        let mut cache = WasmInstanceCache::new(budget);

        let _ = cache
            .get_or_create("A", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();
        let _ = cache
            .get_or_create("B", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();
        let _ = cache
            .get_or_create("C", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();

        // Touch A to make it recently used
        let _ = cache.get_or_create("A", || panic!("should hit")).unwrap();

        // Insert D — should evict B (oldest untouched), not A
        let _ = cache
            .get_or_create("D", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();

        let m = cache.metrics();
        assert_eq!(m.evictions, 1);

        // A should still be in cache (hit)
        let _ = cache
            .get_or_create("A", || panic!("A should be cached"))
            .unwrap();
        // B should be evicted (miss)
        let mut b_created = false;
        let _ = cache
            .get_or_create("B", || {
                b_created = true;
                Ok(Box::new(MockWasmInstance::new(1000)))
            })
            .unwrap();
        assert!(b_created, "B should have been evicted and recreated");
    }

    #[test]
    fn test_shared_budget() {
        let budget = WasmBudget::new(1000);
        let mut cache1 = WasmInstanceCache::new(budget.clone());
        let mut cache2 = WasmInstanceCache::new(budget.clone());

        // Fill from cache1
        let _ = cache1
            .get_or_create("A", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();

        // Cache2 can't allocate — budget exhausted and cache2 has nothing to evict
        let result = cache2.get_or_create("B", || Ok(Box::new(MockWasmInstance::new(1000))));
        assert!(result.is_err());

        // Clear cache1 frees the budget
        cache1.clear();
        assert_eq!(budget.used(), 0);

        // Now cache2 can allocate
        let _ = cache2
            .get_or_create("B", || Ok(Box::new(MockWasmInstance::new(1000))))
            .unwrap();
        assert_eq!(budget.used(), 1000);
    }

    #[test]
    fn test_clear_releases_budget() {
        let budget = WasmBudget::new(10_000);
        let mut cache = WasmInstanceCache::new(budget.clone());

        let _ = cache
            .get_or_create("A", || Ok(Box::new(MockWasmInstance::new(3000))))
            .unwrap();
        let _ = cache
            .get_or_create("B", || Ok(Box::new(MockWasmInstance::new(4000))))
            .unwrap();
        assert_eq!(budget.used(), 7000);

        cache.clear();
        assert_eq!(budget.used(), 0);
    }
}
