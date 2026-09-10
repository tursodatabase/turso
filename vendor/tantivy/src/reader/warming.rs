use std::collections::HashSet;
use std::ops::Deref;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::{Executor, Inventory, Searcher, SearcherGeneration, TantivyError};

pub const GC_INTERVAL: Duration = Duration::from_secs(1);

/// `Warmer` can be used to maintain segment-level state e.g. caches.
///
/// They must be registered with the [`IndexReaderBuilder`](super::IndexReaderBuilder).
pub trait Warmer: Sync + Send {
    /// Perform any warming work using the provided [`Searcher`].
    fn warm(&self, searcher: &Searcher) -> crate::Result<()>;

    /// Discards internal state for any [`SearcherGeneration`] not provided.
    fn garbage_collect(&self, live_generations: &[&SearcherGeneration]);
}

/// Warming-related state with interior mutability.
#[derive(Clone)]
pub(crate) struct WarmingState(Arc<Mutex<WarmingStateInner>>);

impl WarmingState {
    pub fn new(
        num_warming_threads: usize,
        warmers: Vec<Weak<dyn Warmer>>,
        searcher_generation_inventory: Inventory<SearcherGeneration>,
    ) -> crate::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(WarmingStateInner {
            num_warming_threads,
            warmers,
            gc_thread: None,
            warmed_generation_ids: Default::default(),
            searcher_generation_inventory,
        }))))
    }

    /// Start tracking a new generation of [`Searcher`], and [`Warmer::warm`] it if there are active
    /// warmers.
    ///
    /// A background GC thread for [`Warmer::garbage_collect`] calls is uniquely created if there
    /// are active warmers.
    pub fn warm_new_searcher_generation(&self, searcher: &Searcher) -> crate::Result<()> {
        self.0
            .lock()
            .unwrap()
            .warm_new_searcher_generation(searcher, &self.0)
    }

    #[cfg(test)]
    fn gc_maybe(&self) -> bool {
        self.0.lock().unwrap().gc_maybe()
    }
}

struct WarmingStateInner {
    num_warming_threads: usize,
    warmers: Vec<Weak<dyn Warmer>>,
    gc_thread: Option<JoinHandle<()>>,
    // Contains all generations that have been warmed up.
    // This list is used to avoid triggers the individual Warmer GCs
    // if no warmed generation needs to be collected.
    warmed_generation_ids: HashSet<u64>,
    searcher_generation_inventory: Inventory<SearcherGeneration>,
}

impl WarmingStateInner {
    /// Start tracking provided searcher as an exemplar of a new generation.
    /// If there are active warmers, warm them with the provided searcher, and kick background GC
    /// thread if it has not yet been kicked. Otherwise, prune state for dropped searcher
    /// generations inline.
    fn warm_new_searcher_generation(
        &mut self,
        searcher: &Searcher,
        this: &Arc<Mutex<Self>>,
    ) -> crate::Result<()> {
        let warmers = self.pruned_warmers();
        // Avoid threads (warming as well as background GC) if there are no warmers
        if warmers.is_empty() {
            return Ok(());
        }
        self.start_gc_thread_maybe(this)?;
        self.warmed_generation_ids
            .insert(searcher.generation().generation_id());
        warming_executor(self.num_warming_threads.min(warmers.len()))?
            .map(|warmer| warmer.warm(searcher), warmers.into_iter())?;
        Ok(())
    }

    /// Attempt to upgrade the weak `Warmer` references, pruning those which cannot be upgraded.
    /// Return the strong references.
    fn pruned_warmers(&mut self) -> Vec<Arc<dyn Warmer>> {
        let strong_warmers = self
            .warmers
            .iter()
            .flat_map(|weak_warmer| weak_warmer.upgrade())
            .collect::<Vec<_>>();
        self.warmers = strong_warmers.iter().map(Arc::downgrade).collect();
        strong_warmers
    }

    /// [`Warmer::garbage_collect`] active warmers if some searcher generation is observed to have
    /// been dropped.
    fn gc_maybe(&mut self) -> bool {
        let live_generations = self.searcher_generation_inventory.list();
        let live_generation_ids: HashSet<u64> = live_generations
            .iter()
            .map(|searcher_generation| searcher_generation.generation_id())
            .collect();
        let gc_not_required = self
            .warmed_generation_ids
            .iter()
            .all(|warmed_up_generation| live_generation_ids.contains(warmed_up_generation));
        if gc_not_required {
            return false;
        }
        let live_generation_refs = live_generations
            .iter()
            .map(Deref::deref)
            .collect::<Vec<_>>();
        for warmer in self.pruned_warmers() {
            warmer.garbage_collect(&live_generation_refs);
        }
        self.warmed_generation_ids = live_generation_ids;
        true
    }

    /// Start GC thread if one has not already been started.
    fn start_gc_thread_maybe(&mut self, this: &Arc<Mutex<Self>>) -> crate::Result<bool> {
        if self.gc_thread.is_some() {
            return Ok(false);
        }
        let weak_inner = Arc::downgrade(this);
        let handle = std::thread::Builder::new()
            .name("tantivy-warm-gc".to_owned())
            .spawn(|| Self::gc_loop(weak_inner))
            .map_err(|_| {
                TantivyError::SystemError("Failed to spawn warmer GC thread".to_owned())
            })?;
        self.gc_thread = Some(handle);
        Ok(true)
    }

    /// Every [`GC_INTERVAL`] attempt to GC, with panics caught and logged using
    /// [`std::panic::catch_unwind`].
    fn gc_loop(inner: Weak<Mutex<WarmingStateInner>>) {
        for _ in crossbeam_channel::tick(GC_INTERVAL) {
            if let Some(inner) = inner.upgrade() {
                // rely on deterministic gc in tests
                #[cfg(not(test))]
                if let Err(err) = std::panic::catch_unwind(|| inner.lock().unwrap().gc_maybe()) {
                    error!("Panic in Warmer GC {err:?}");
                }
                // avoid unused var warning in tests
                #[cfg(test)]
                drop(inner);
            }
        }
    }
}

fn warming_executor(num_threads: usize) -> crate::Result<Executor> {
    if num_threads <= 1 {
        Ok(Executor::single_thread())
    } else {
        Executor::multi_thread(num_threads, "tantivy-warm-")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{self, AtomicUsize};
    use std::sync::{Arc, RwLock, Weak};

    use super::Warmer;
    use crate::core::searcher::SearcherGeneration;
    use crate::directory::RamDirectory;
    use crate::index::SegmentId;
    use crate::indexer::index_writer::MEMORY_BUDGET_NUM_BYTES_MIN;
    use crate::schema::{Schema, INDEXED};
    use crate::{Index, IndexSettings, ReloadPolicy, Searcher};

    #[derive(Default)]
    struct TestWarmer {
        active_segment_ids: RwLock<HashSet<SegmentId>>,
        warm_calls: AtomicUsize,
        gc_calls: AtomicUsize,
    }

    impl TestWarmer {
        fn live_segment_ids(&self) -> HashSet<SegmentId> {
            self.active_segment_ids.read().unwrap().clone()
        }

        fn warm_calls(&self) -> usize {
            self.warm_calls.load(atomic::Ordering::Acquire)
        }

        fn gc_calls(&self) -> usize {
            self.gc_calls.load(atomic::Ordering::Acquire)
        }

        fn verify(
            &self,
            expected_warm_calls: usize,
            expected_gc_calls: usize,
            expected_segment_ids: HashSet<SegmentId>,
        ) {
            assert_eq!(self.warm_calls(), expected_warm_calls);
            assert_eq!(self.gc_calls(), expected_gc_calls);
            assert_eq!(self.live_segment_ids(), expected_segment_ids);
        }
    }

    impl Warmer for TestWarmer {
        fn warm(&self, searcher: &crate::Searcher) -> crate::Result<()> {
            self.warm_calls.fetch_add(1, atomic::Ordering::SeqCst);
            for reader in searcher.segment_readers() {
                self.active_segment_ids
                    .write()
                    .unwrap()
                    .insert(reader.segment_id());
            }
            Ok(())
        }

        fn garbage_collect(&self, live_generations: &[&SearcherGeneration]) {
            self.gc_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let active_segment_ids = live_generations
                .iter()
                .flat_map(|searcher_generation| searcher_generation.segments().keys().copied())
                .collect();
            *self.active_segment_ids.write().unwrap() = active_segment_ids;
        }
    }

    fn segment_ids(searcher: &Searcher) -> HashSet<SegmentId> {
        searcher
            .segment_readers()
            .iter()
            .map(|reader| reader.segment_id())
            .collect()
    }

    fn test_warming(num_warming_threads: usize) -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field("pk", INDEXED);
        let schema = schema_builder.build();

        let directory = RamDirectory::create();
        let index = Index::create(directory, schema, IndexSettings::default())?;

        let num_writer_threads = 4;
        let mut writer = index
            .writer_with_num_threads(
                num_writer_threads,
                MEMORY_BUDGET_NUM_BYTES_MIN * num_writer_threads,
            )
            .unwrap();

        for i in 0u64..1000u64 {
            writer.add_document(doc!(field => i))?;
        }
        writer.commit()?;

        let warmer1 = Arc::new(TestWarmer::default());
        let warmer2 = Arc::new(TestWarmer::default());
        warmer1.verify(0, 0, HashSet::new());
        warmer2.verify(0, 0, HashSet::new());

        let num_searchers = 4;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .num_warming_threads(num_warming_threads)
            .warmers(vec![
                Arc::downgrade(&warmer1) as Weak<dyn Warmer>,
                Arc::downgrade(&warmer2) as Weak<dyn Warmer>,
            ])
            .try_into()?;

        let warming_state = &reader.inner.warming_state;

        let searcher = reader.searcher();
        assert!(
            !warming_state.gc_maybe(),
            "no GC after first searcher generation"
        );
        warmer1.verify(1, 0, segment_ids(&searcher));
        warmer2.verify(1, 0, segment_ids(&searcher));
        assert_eq!(searcher.num_docs(), 1000);

        for i in 1000u64..2000u64 {
            writer.add_document(doc!(field => i))?;
        }
        writer.commit()?;
        writer.wait_merging_threads()?;

        drop(warmer1);

        let old_searcher = searcher;

        reader.reload()?;

        assert!(!warming_state.gc_maybe(), "old searcher still around");

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2000);

        warmer2.verify(
            2,
            0,
            segment_ids(&old_searcher)
                .union(&segment_ids(&searcher))
                .copied()
                .collect(),
        );

        drop(old_searcher);
        for _ in 0..num_searchers {
            // make sure the old searcher is dropped by the pool too
            let _ = reader.searcher();
        }
        assert!(warming_state.gc_maybe(), "old searcher dropped");

        warmer2.verify(2, 1, segment_ids(&searcher));

        Ok(())
    }

    #[test]
    fn warming_single_thread() -> crate::Result<()> {
        test_warming(1)
    }

    #[test]
    fn warming_four_threads() -> crate::Result<()> {
        test_warming(4)
    }
}
