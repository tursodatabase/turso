use std::{
    fmt,
    hash::{Hash, Hasher},
    mem::size_of,
};

use lru::LruCache;
use rustc_hash::FxHasher;

use crate::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    LimboError, Result,
};

const MAX_SHARDS: usize = 16;
const TARGET_BYTES_PER_SHARD: usize = 1024 * 1024;
const ENTRY_ALLOCATION_OVERHEAD: usize = 64;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum SharedPageVersion {
    Database {
        checkpoint_epoch: u32,
    },
    Wal {
        frame_id: u64,
        checkpoint_epoch: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct SharedPageKey {
    namespace: u64,
    generation: u64,
    page_id: usize,
    page_size: u32,
    version: SharedPageVersion,
}

impl SharedPageKey {
    fn weight(self, value_len: usize) -> usize {
        value_len
            .saturating_add(size_of::<Self>())
            .saturating_add(size_of::<Arc<[u8]>>())
            .saturating_add(ENTRY_ALLOCATION_OVERHEAD)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SharedPageCacheStats {
    /// Configured byte capacity.
    pub capacity_bytes: u64,
    /// Estimated bytes held by entries.
    pub resident_bytes: u64,
    /// Number of resident entries.
    pub entries: u64,
    /// Successful lookups.
    pub hits: u64,
    /// Unsuccessful lookups.
    pub misses: u64,
    /// First insertions for a key.
    pub insertions: u64,
    /// Replacements of an existing key.
    pub replacements: u64,
    /// Entries removed to honor capacity.
    pub evictions: u64,
    /// Entries rejected because they exceed a shard's capacity.
    pub rejected: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SharedPageCacheLookup {
    /// The exact requested page version was present.
    Hit,
    /// The exact requested page version was absent.
    Miss,
}

/// Receives aggregate cache lookup outcomes from the Pager read path.
///
/// Implementations must not block or allocate high-cardinality data.
pub trait SharedPageCacheObserver: Send + Sync {
    fn record_lookup(&self, outcome: SharedPageCacheLookup);
}

struct CacheShard {
    capacity_bytes: usize,
    resident_bytes: usize,
    entries: LruCache<SharedPageKey, Arc<[u8]>>,
    hits: u64,
    misses: u64,
    insertions: u64,
    replacements: u64,
    evictions: u64,
    rejected: u64,
}

impl CacheShard {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            resident_bytes: 0,
            entries: LruCache::unbounded(),
            hits: 0,
            misses: 0,
            insertions: 0,
            replacements: 0,
            evictions: 0,
            rejected: 0,
        }
    }

    fn get(&mut self, key: &SharedPageKey) -> Option<Arc<[u8]>> {
        let value = self.entries.get(key).cloned();
        if value.is_some() {
            self.hits = self.hits.saturating_add(1);
        } else {
            self.misses = self.misses.saturating_add(1);
        }
        value
    }

    fn insert(&mut self, key: SharedPageKey, value: Arc<[u8]>) {
        let weight = key.weight(value.len());
        if weight > self.capacity_bytes {
            self.rejected = self.rejected.saturating_add(1);
            return;
        }

        if let Some(previous) = self.entries.pop(&key) {
            self.resident_bytes = self
                .resident_bytes
                .saturating_sub(key.weight(previous.len()));
            self.replacements = self.replacements.saturating_add(1);
        } else {
            self.insertions = self.insertions.saturating_add(1);
        }

        self.resident_bytes = self.resident_bytes.saturating_add(weight);
        self.entries.put(key, value);

        while self.resident_bytes > self.capacity_bytes {
            let Some((evicted_key, evicted_value)) = self.entries.pop_lru() else {
                break;
            };
            self.resident_bytes = self
                .resident_bytes
                .saturating_sub(evicted_key.weight(evicted_value.len()));
            self.evictions = self.evictions.saturating_add(1);
        }
    }

    fn add_stats(&self, stats: &mut SharedPageCacheStats) {
        stats.resident_bytes = stats
            .resident_bytes
            .saturating_add(self.resident_bytes as u64);
        stats.entries = stats.entries.saturating_add(self.entries.len() as u64);
        stats.hits = stats.hits.saturating_add(self.hits);
        stats.misses = stats.misses.saturating_add(self.misses);
        stats.insertions = stats.insertions.saturating_add(self.insertions);
        stats.replacements = stats.replacements.saturating_add(self.replacements);
        stats.evictions = stats.evictions.saturating_add(self.evictions);
        stats.rejected = stats.rejected.saturating_add(self.rejected);
    }
}

/// A byte-accounted process cache for immutable, clean database page images.
///
/// Cached bytes are copied into a private Pager buffer on lookup. Mutable page,
/// transaction, dirty, spill, rollback, and pin state remain connection-local.
/// Reads bypass this cache when their WAL cannot provide a safe version epoch
/// or the database has encryption enabled.
pub struct SharedPageCache {
    capacity_bytes: usize,
    shards: Box<[Mutex<CacheShard>]>,
    next_namespace: AtomicU64,
    observer: Option<Arc<dyn SharedPageCacheObserver>>,
}

impl fmt::Debug for SharedPageCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedPageCache")
            .field("stats", &self.stats())
            .finish()
    }
}

impl SharedPageCache {
    /// Create a shared cache with the given total byte capacity.
    pub fn new(capacity_bytes: usize) -> Self {
        Self::with_optional_observer(capacity_bytes, None)
    }

    /// Create a shared cache with a lookup observer.
    pub fn with_observer(
        capacity_bytes: usize,
        observer: Arc<dyn SharedPageCacheObserver>,
    ) -> Self {
        Self::with_optional_observer(capacity_bytes, Some(observer))
    }

    fn with_optional_observer(
        capacity_bytes: usize,
        observer: Option<Arc<dyn SharedPageCacheObserver>>,
    ) -> Self {
        let shard_count = if capacity_bytes == 0 {
            1
        } else {
            capacity_bytes
                .div_ceil(TARGET_BYTES_PER_SHARD)
                .clamp(1, MAX_SHARDS)
        };
        let base_capacity = capacity_bytes / shard_count;
        let remainder = capacity_bytes % shard_count;
        let shards = (0..shard_count)
            .map(|index| {
                let capacity = base_capacity + usize::from(index < remainder);
                Mutex::new(CacheShard::new(capacity))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            capacity_bytes,
            shards,
            next_namespace: AtomicU64::new(1),
            observer,
        }
    }

    /// Return aggregate statistics across all cache shards.
    pub fn stats(&self) -> SharedPageCacheStats {
        let mut stats = SharedPageCacheStats {
            capacity_bytes: self.capacity_bytes as u64,
            ..SharedPageCacheStats::default()
        };
        for shard in &self.shards {
            shard.lock().add_stats(&mut stats);
        }
        stats
    }

    pub(crate) fn new_namespace(
        self: &Arc<Self>,
        generation: Arc<SharedPageCacheGeneration>,
    ) -> Result<SharedPageCacheNamespace> {
        let id = self
            .next_namespace
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .map_err(|_| {
                LimboError::InternalError(
                    "shared page cache exhausted its namespace identifiers".to_string(),
                )
            })?;
        Ok(SharedPageCacheNamespace {
            cache: self.clone(),
            id,
            generation,
        })
    }

    fn shard_index(&self, key: &SharedPageKey) -> usize {
        let mut hasher = FxHasher::default();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn get(&self, key: &SharedPageKey) -> Option<Arc<[u8]>> {
        self.shards[self.shard_index(key)].lock().get(key)
    }

    fn record_lookup(&self, hit: bool) {
        if let Some(observer) = self.observer.as_ref() {
            observer.record_lookup(if hit {
                SharedPageCacheLookup::Hit
            } else {
                SharedPageCacheLookup::Miss
            });
        }
    }

    fn insert(&self, key: SharedPageKey, value: Arc<[u8]>) {
        self.shards[self.shard_index(&key)]
            .lock()
            .insert(key, value);
    }
}

#[derive(Debug, Default)]
pub(crate) struct SharedPageCacheGeneration {
    value: RwLock<u64>,
}

impl SharedPageCacheGeneration {
    pub(crate) fn advance(&self) -> Result<()> {
        let mut value = self.value.write();
        *value = value.checked_add(1).ok_or_else(|| {
            LimboError::InternalError(
                "shared page cache exhausted its database generations".to_string(),
            )
        })?;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct SharedPageCacheNamespace {
    cache: Arc<SharedPageCache>,
    id: u64,
    generation: Arc<SharedPageCacheGeneration>,
}

pub(crate) enum SharedPageCacheEntry {
    Hit(Arc<[u8]>),
    Miss(SharedPageCachePublisher),
}

impl fmt::Debug for SharedPageCacheNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedPageCacheNamespace")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl SharedPageCacheNamespace {
    pub(crate) fn lookup(
        &self,
        page_id: usize,
        page_size: u32,
        version: SharedPageVersion,
    ) -> SharedPageCacheEntry {
        let generation = self.generation.value.read();
        let key = SharedPageKey {
            namespace: self.id,
            generation: *generation,
            page_id,
            page_size,
            version,
        };
        let value = self.cache.get(&key);
        let entry = match value {
            Some(value) => SharedPageCacheEntry::Hit(value),
            None => SharedPageCacheEntry::Miss(SharedPageCachePublisher {
                cache: self.cache.clone(),
                key,
            }),
        };
        drop(generation);
        self.cache
            .record_lookup(matches!(entry, SharedPageCacheEntry::Hit(_)));
        entry
    }

    #[cfg(test)]
    pub(crate) fn publisher(
        &self,
        page_id: usize,
        page_size: u32,
        version: SharedPageVersion,
    ) -> SharedPageCachePublisher {
        let generation = *self.generation.value.read();
        SharedPageCachePublisher {
            cache: self.cache.clone(),
            key: SharedPageKey {
                namespace: self.id,
                generation,
                page_id,
                page_size,
                version,
            },
        }
    }
}

pub(crate) struct SharedPageCachePublisher {
    cache: Arc<SharedPageCache>,
    key: SharedPageKey,
}

impl SharedPageCachePublisher {
    pub(crate) fn publish(&self, bytes: &[u8]) {
        if bytes.len() != self.key.page_size as usize {
            return;
        }
        self.cache.insert(self.key, Arc::<[u8]>::from(bytes));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{
        AtomicBool as StdAtomicBool, AtomicU64 as StdAtomicU64, Ordering as StdOrdering,
    };

    fn version(frame_id: u64) -> SharedPageVersion {
        SharedPageVersion::Wal {
            frame_id,
            checkpoint_epoch: 7,
        }
    }

    fn namespace(cache: &Arc<SharedPageCache>) -> SharedPageCacheNamespace {
        cache
            .new_namespace(Arc::new(SharedPageCacheGeneration::default()))
            .unwrap()
    }

    fn get(
        namespace: &SharedPageCacheNamespace,
        page_id: usize,
        page_size: u32,
        version: SharedPageVersion,
    ) -> Option<Arc<[u8]>> {
        match namespace.lookup(page_id, page_size, version) {
            SharedPageCacheEntry::Hit(value) => Some(value),
            SharedPageCacheEntry::Miss(_) => None,
        }
    }

    #[test]
    fn namespace_and_version_isolate_entries() {
        let cache = Arc::new(SharedPageCache::new(64 * 1024));
        let first = namespace(&cache);
        let second = namespace(&cache);
        first
            .publisher(3, 4096, version(11))
            .publish(&vec![1; 4096]);

        assert_eq!(get(&first, 3, 4096, version(11)).unwrap()[0], 1);
        assert!(get(&first, 3, 8192, version(11)).is_none());
        assert!(get(&first, 3, 4096, version(12)).is_none());
        assert!(get(&second, 3, 4096, version(11)).is_none());
    }

    #[test]
    fn database_generation_invalidates_existing_namespaces() {
        let cache = Arc::new(SharedPageCache::new(64 * 1024));
        let generation = Arc::new(SharedPageCacheGeneration::default());
        let namespace = cache.new_namespace(generation.clone()).unwrap();
        namespace
            .publisher(3, 4096, version(11))
            .publish(&vec![1; 4096]);

        assert!(get(&namespace, 3, 4096, version(11)).is_some());
        generation.advance().unwrap();
        assert!(get(&namespace, 3, 4096, version(11)).is_none());
    }

    #[test]
    fn byte_capacity_evicts_and_never_exceeds_bound() {
        let per_entry = SharedPageKey {
            namespace: 1,
            generation: 0,
            page_id: 1,
            page_size: 4096,
            version: version(1),
        }
        .weight(4096);
        let cache = Arc::new(SharedPageCache::new(per_entry * 2));
        let namespace = namespace(&cache);

        for page_id in 1..=3 {
            namespace
                .publisher(page_id, 4096, version(page_id as u64))
                .publish(&vec![page_id as u8; 4096]);
        }

        let stats = cache.stats();
        assert!(stats.resident_bytes <= stats.capacity_bytes);
        assert!(stats.entries <= 2);
        assert!(stats.evictions >= 1);
    }

    #[test]
    fn oversized_and_wrong_sized_values_are_not_cached() {
        let cache = Arc::new(SharedPageCache::new(1024));
        let namespace = namespace(&cache);
        namespace
            .publisher(1, 4096, version(1))
            .publish(&vec![0; 4096]);
        namespace.publisher(2, 4096, version(1)).publish(&[0; 128]);

        assert!(get(&namespace, 1, 4096, version(1)).is_none());
        assert!(get(&namespace, 2, 4096, version(1)).is_none());
        assert_eq!(cache.stats().rejected, 1);
    }

    #[derive(Default)]
    struct LookupObserver {
        hits: StdAtomicU64,
        misses: StdAtomicU64,
    }

    impl SharedPageCacheObserver for LookupObserver {
        fn record_lookup(&self, outcome: SharedPageCacheLookup) {
            match outcome {
                SharedPageCacheLookup::Hit => {
                    self.hits.fetch_add(1, StdOrdering::Relaxed);
                }
                SharedPageCacheLookup::Miss => {
                    self.misses.fetch_add(1, StdOrdering::Relaxed);
                }
            }
        }
    }

    #[test]
    fn observer_receives_aggregate_lookup_outcomes() {
        let observer = Arc::new(LookupObserver::default());
        let cache = Arc::new(SharedPageCache::with_observer(64 * 1024, observer.clone()));
        let namespace = namespace(&cache);

        assert!(get(&namespace, 1, 4096, version(1)).is_none());
        namespace.publisher(1, 4096, version(1)).publish(&[7; 4096]);
        assert!(get(&namespace, 1, 4096, version(1)).is_some());

        assert_eq!(observer.hits.load(StdOrdering::Relaxed), 1);
        assert_eq!(observer.misses.load(StdOrdering::Relaxed), 1);
    }

    struct ReentrantObserver {
        generation: Arc<SharedPageCacheGeneration>,
        acquired_generation: StdAtomicBool,
    }

    impl SharedPageCacheObserver for ReentrantObserver {
        fn record_lookup(&self, _outcome: SharedPageCacheLookup) {
            self.acquired_generation.store(
                self.generation.value.try_write().is_some(),
                StdOrdering::Relaxed,
            );
        }
    }

    #[test]
    fn observer_runs_without_internal_locks() {
        let generation = Arc::new(SharedPageCacheGeneration::default());
        let observer = Arc::new(ReentrantObserver {
            generation: generation.clone(),
            acquired_generation: StdAtomicBool::new(false),
        });
        let cache = Arc::new(SharedPageCache::with_observer(64 * 1024, observer.clone()));
        let namespace = cache.new_namespace(generation).unwrap();

        assert!(get(&namespace, 1, 4096, version(1)).is_none());
        assert!(observer.acquired_generation.load(StdOrdering::Relaxed));
    }

    struct AdvancingObserver {
        generation: Arc<SharedPageCacheGeneration>,
        advanced: StdAtomicBool,
    }

    impl SharedPageCacheObserver for AdvancingObserver {
        fn record_lookup(&self, _outcome: SharedPageCacheLookup) {
            if !self.advanced.swap(true, StdOrdering::Relaxed) {
                self.generation.advance().unwrap();
            }
        }
    }

    #[test]
    fn miss_publisher_keeps_the_lookup_generation() {
        let generation = Arc::new(SharedPageCacheGeneration::default());
        let observer = Arc::new(AdvancingObserver {
            generation: generation.clone(),
            advanced: StdAtomicBool::new(false),
        });
        let cache = Arc::new(SharedPageCache::with_observer(64 * 1024, observer));
        let namespace = cache.new_namespace(generation).unwrap();

        let SharedPageCacheEntry::Miss(publisher) = namespace.lookup(1, 4096, version(1)) else {
            panic!("empty cache must miss");
        };
        publisher.publish(&[7; 4096]);

        assert!(get(&namespace, 1, 4096, version(1)).is_none());
    }
}
