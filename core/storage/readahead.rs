//! Readahead: pull pages off storage before a scan asks for them.
//!
//! A table scan reads one page, waits for the disk, reads the next page, waits
//! again. Each wait is a full round trip. On local NVMe that is tens of
//! microseconds; on network storage it is milliseconds. The scan spends almost
//! all of its time idle, and the storage device spends almost all of its time
//! idle, because only one request is ever outstanding.
//!
//! Readahead fixes that by noticing a reader walking forward through the file
//! and fetching a run of pages ahead of it in one request.
//!
//! # Why we key on physical page numbers
//!
//! A b-tree scan is not obviously a sequential file read: it descends interior
//! pages, walks leaves, pops back up. But the pages it touches are laid out in
//! near-perfect file order, because pages are handed out in ascending order as
//! the table grows. Measured on the 1.2 GB TPC-H database (4 KiB pages), the
//! page-to-page steps of a full scan are:
//!
//! | b-tree                        | step +1 | step +2 | within +1..+4 |
//! |-------------------------------|---------|---------|---------------|
//! | ORDERS (41 322 pages)         |  99.19% |   0.27% |        99.46% |
//! | LINEITEM (195 198 pages)      |  87.51% |  11.87% |        99.46% |
//! | PARTSUPP (30 411 pages)       |     ~87 |     ~12 |          ~99  |
//! | sqlite_autoindex_LINEITEM_1   |   0.00% |   0.00% |         0.00% |
//!
//! LINEITEM's `+2` steps are the interior pages: one gets allocated after
//! roughly every eight leaves, so the leaf run steps over it. That is why the
//! test below is "did the reader move forward by at most [`MAX_FORWARD_GAP`]"
//! and not "did it move forward by exactly one".
//!
//! The last row is the reason the test has an upper bound at all. That index's
//! leaves sit nine or ten pages apart, interleaved with table pages. A policy
//! that prefetched a fixed window ahead of every read would pull nine useless
//! table pages for every useful index page: measured at 9x read amplification
//! and 90-100% of prefetched pages never read. With a gap limit of 4 the
//! stream never qualifies as sequential, so that scan prefetches nothing at
//! all and costs exactly what it costs today.
//!
//! # Window sizing
//!
//! Once a stream qualifies, the window grows 4x, then 2x, then clamps -- the
//! same ramp Linux uses in `get_next_ra_size()` (mm/readahead.c). Starting
//! small keeps a short scan from paying for pages it will never reach;
//! doubling gets a long scan to the cap within a handful of steps.
//!
//! Growth is worth having because the cost of a wrong guess is asymmetric: an
//! unused prefetched page costs one page of bandwidth, while a missing page
//! costs a whole round trip. But it has to be bounded, hence the clamp.
//!
//! # Keeping the pipe full
//!
//! Fetching a window only when we stall still leaves one stall per window. So,
//! as Linux does, the first page of each window is a marker: when the reader
//! reaches it, we queue the *next* window immediately, without waiting. In
//! steady state the reader always has a full window of lead and never stalls.
//! Simulated over the TPC-H scans above, that is the difference between ~15x
//! and ~185x fewer blocking reads.
//!
//! # What this deliberately does not do
//!
//! It does not use the query plan to decide *how much* to fetch. A full scan
//! of the index in the table above would tell us "every page of this b-tree
//! will be read", which is true, and prefetching on that basis would still be
//! 9x read amplification, because the pages are not where the guess says they
//! are. Physical layout is the thing that decides whether readahead pays, so
//! physical layout is what we measure.

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;

use crate::io::Completion;
use crate::storage::pager::PageRef;
use crate::turso_assert;

/// Largest forward jump that still counts as "this reader is walking forward".
///
/// Covers the interior pages a leaf run steps over (see the table above:
/// 99.46% of steps in the TPC-H table scans land within +4) while excluding
/// the strided index scan, whose steps are +9/+10.
const MAX_FORWARD_GAP: i64 = 4;

/// Forward steps a stream must take before we spend any I/O on it.
///
/// One step proves nothing: any two reads in a row are "forward" half the
/// time. Two consecutive forward steps is enough evidence to risk one small
/// window, and a scan reaches it immediately.
const TRIGGER_RUN: u32 = 2;

/// Size of the first window a stream earns, in pages.
const INITIAL_WINDOW: u32 = 4;

/// How many independent forward-walking readers we follow at once.
///
/// A join reads two tables in lockstep, an index-driven query reads an index
/// and a table, so one detector is not enough. Four covers the plans we see
/// without making the lookup a search problem; the coldest is recycled.
const STREAM_COUNT: usize = 4;

/// What the pager should do about a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadaheadAction {
    /// Nothing to fetch.
    None,
    /// Fetch `count` pages starting at `start`, best effort.
    Fetch { start: i64, count: u32 },
}

/// One forward-walking reader.
#[derive(Debug, Clone, Copy)]
struct Stream {
    /// Page this stream last saw. `None` marks a free slot.
    last_page: Option<i64>,
    /// Consecutive forward steps taken so far. A fresh stream has taken none:
    /// seeing one page tells us nothing about where the reader is going.
    run: u32,
    /// Size of the last window we asked for, in pages. 0 before the first.
    size: u32,
    /// Last page of the window we have already asked for.
    window_end: i64,
    /// Reaching this page queues the next window.
    marker: i64,
    /// Bumped on every touch so the coldest stream can be recycled.
    stamp: u64,
}

impl Stream {
    const fn empty() -> Self {
        Self {
            last_page: None,
            run: 0,
            size: 0,
            window_end: -1,
            marker: -1,
            stamp: 0,
        }
    }

    fn restart(&mut self, page: i64, stamp: u64) {
        self.last_page = Some(page);
        self.run = 0;
        self.size = 0;
        self.window_end = -1;
        self.marker = -1;
        self.stamp = stamp;
    }

    /// Record that pages `[start, start + count)` are on their way.
    ///
    /// The marker goes on the first page of the window: the moment the reader
    /// gets there we queue the window after it, which leaves the reader a full
    /// window of lead. This is what Linux does with `async_size == size`.
    fn arm(&mut self, start: i64, count: u32) {
        self.window_end = start + count as i64 - 1;
        self.marker = start;
    }
}

/// Next window size, following Linux's `get_next_ra_size()`.
fn grow(current: u32, max: u32) -> u32 {
    if current == 0 {
        return INITIAL_WINDOW.min(max);
    }
    if current < max / 16 {
        return (current * 4).min(max);
    }
    if current <= max / 2 {
        return (current * 2).min(max);
    }
    max
}

/// Tracks forward-walking readers and decides when to fetch ahead of them.
///
/// Pure policy: it never touches storage or the page cache, so the whole thing
/// is exercised by unit tests below.
#[derive(Debug)]
pub struct Readahead {
    streams: [Stream; STREAM_COUNT],
    clock: u64,
}

impl Default for Readahead {
    fn default() -> Self {
        Self::new()
    }
}

impl Readahead {
    pub const fn new() -> Self {
        Self {
            streams: [Stream::empty(); STREAM_COUNT],
            clock: 0,
        }
    }

    /// Forget every stream. Called when what we know about the file stops
    /// being true: end of a read transaction, page cache cleared, checkpoint.
    pub fn reset(&mut self) {
        self.streams = [Stream::empty(); STREAM_COUNT];
    }

    /// A cursor is about to scan a whole b-tree from one end.
    ///
    /// Skips the [`TRIGGER_RUN`] warmup for that b-tree's root, because the
    /// plan already told us a walk is starting -- the same reasoning that
    /// makes Linux treat a read at file offset 0 as sequential without waiting
    /// for evidence.
    ///
    /// It does *not* skip the gap test: whether the walk turns out to be
    /// physically sequential is a property of the layout, not of the plan, and
    /// getting that wrong is the 9x-amplification case in the module docs.
    pub fn hint_scan_start(&mut self, root_page: i64) {
        self.clock += 1;
        let slot = self.slot_for(root_page);
        let clock = self.clock;
        let stream = &mut self.streams[slot];
        stream.restart(root_page, clock);
        stream.run = TRIGGER_RUN;
    }

    /// Record a page read and say what, if anything, to fetch ahead of it.
    ///
    /// `cache_hit` is whether the read was served without going to storage.
    /// `max_window` is the ceiling on window size in pages; `None` disables
    /// readahead entirely.
    pub fn on_read(
        &mut self,
        page: i64,
        cache_hit: bool,
        max_window: Option<NonZeroU32>,
    ) -> ReadaheadAction {
        let Some(max_window) = max_window else {
            return ReadaheadAction::None;
        };
        let max_window = max_window.get();
        self.clock += 1;
        let clock = self.clock;

        let Some(slot) = self.continue_stream(page, clock) else {
            // Not a continuation of anything we are following: start over on
            // the coldest slot. One step is not evidence, so nothing is
            // fetched yet.
            let slot = self.slot_for(page);
            self.streams[slot].restart(page, clock);
            return ReadaheadAction::None;
        };

        let stream = &mut self.streams[slot];
        if stream.run < TRIGGER_RUN {
            return ReadaheadAction::None;
        }

        if !cache_hit {
            // We just went to storage anyway. Fetch a window from here so the
            // next pages are already on their way.
            stream.size = grow(stream.size, max_window);
            let count = stream.size;
            stream.arm(page + 1, count);
            return ReadaheadAction::Fetch {
                start: page + 1,
                count,
            };
        }

        // Served from memory. If the reader has reached the marker inside the
        // window we already asked for, queue the next one now rather than
        // waiting to stall at the end of it.
        if stream.window_end >= page && page >= stream.marker {
            stream.size = grow(stream.size, max_window);
            let count = stream.size;
            let start = stream.window_end + 1;
            stream.arm(start, count);
            return ReadaheadAction::Fetch { start, count };
        }

        ReadaheadAction::None
    }

    /// Advance whichever stream `page` continues, or `None` if it continues no
    /// stream. A page that repeats the stream's last page is a re-read (the
    /// pager retries reads after a cache spill) and neither advances nor
    /// breaks the run.
    fn continue_stream(&mut self, page: i64, clock: u64) -> Option<usize> {
        for slot in 0..STREAM_COUNT {
            let stream = &mut self.streams[slot];
            let Some(last) = stream.last_page else {
                continue;
            };
            if last == page {
                stream.stamp = clock;
                return Some(slot);
            }
            if page > last && page - last <= MAX_FORWARD_GAP {
                stream.last_page = Some(page);
                stream.run = stream.run.saturating_add(1);
                stream.stamp = clock;
                return Some(slot);
            }
        }
        None
    }

    /// Free slot if there is one, otherwise the least recently touched.
    fn slot_for(&self, page: i64) -> usize {
        let _ = page;
        let mut best = 0;
        for slot in 0..STREAM_COUNT {
            if self.streams[slot].last_page.is_none() {
                return slot;
            }
            if self.streams[slot].stamp < self.streams[best].stamp {
                best = slot;
            }
        }
        best
    }
}

/// A page fetched before anything asked for it.
struct Prefetched {
    page: PageRef,
    /// The read covering this page. One read usually covers a whole run, so
    /// several entries share a completion.
    read: Completion,
}

/// What a demand read found waiting for it.
pub enum PrefetchHit {
    /// The page is here and its bytes have landed. No I/O at all.
    Ready(PageRef),
    /// The page is here and its read is still in flight. Wait on the
    /// completion, which was issued earlier than a demand read would have
    /// been.
    InFlight(PageRef, Completion),
}

/// Pages fetched ahead of a reader, held aside until something asks for them.
///
/// Deliberately *not* the page cache. A guess that turns out wrong should cost
/// bandwidth and nothing else; if wrong guesses went straight into the cache
/// they would evict pages the query is still using, and readahead would make
/// things slower exactly when it was least useful. Pages move into the real
/// cache only when a read asks for them.
///
/// Bounded and evicted first-in-first-out. A reader consumes these in the
/// order they were fetched, so the oldest entry is the one most likely to have
/// been a bad guess.
pub struct PrefetchBuffer {
    pages: HashMap<i64, Prefetched>,
    /// Insertion order, for the FIFO bound. May name pages already taken.
    order: VecDeque<i64>,
    capacity: usize,
}

impl PrefetchBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            pages: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        self.trim();
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn contains(&self, page_idx: i64) -> bool {
        self.pages.contains_key(&page_idx)
    }

    /// Drop everything. Called whenever what these pages say about the
    /// database might have stopped being true.
    pub fn clear(&mut self) {
        self.pages.clear();
        self.order.clear();
    }

    pub fn insert(&mut self, page_idx: i64, page: PageRef, read: Completion) {
        if self.capacity == 0 {
            return;
        }
        if self
            .pages
            .insert(page_idx, Prefetched { page, read })
            .is_none()
        {
            self.order.push_back(page_idx);
        }
        self.trim();
    }

    /// Hand over a prefetched page, if we have a usable one.
    ///
    /// A page whose read already failed is dropped and reported as absent, so
    /// the caller falls back to an ordinary read and the failure never
    /// reaches the query. Readahead is only ever an optimization.
    pub fn take(&mut self, page_idx: i64) -> Option<PrefetchHit> {
        let entry = self.pages.get(&page_idx)?;
        let finished = entry.read.finished();
        let failed = finished && !entry.read.succeeded();
        if failed {
            self.pages.remove(&page_idx);
            return None;
        }
        let entry = self.pages.remove(&page_idx)?;
        if finished {
            turso_assert!(
                entry.page.is_loaded(),
                "a prefetched page whose read succeeded must be loaded",
                { "page_idx": page_idx }
            );
            Some(PrefetchHit::Ready(entry.page))
        } else {
            Some(PrefetchHit::InFlight(entry.page, entry.read))
        }
    }

    /// Forget one page, without disturbing the rest.
    pub fn remove(&mut self, page_idx: i64) {
        self.pages.remove(&page_idx);
    }

    fn trim(&mut self) {
        while self.pages.len() > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.pages.remove(&oldest);
        }
        // Keep the order queue from growing without bound when entries are
        // taken out by page number rather than evicted.
        if self.order.len() > self.capacity.saturating_mul(4) + 8 {
            self.order.retain(|idx| self.pages.contains_key(idx));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(n: u32) -> Option<NonZeroU32> {
        NonZeroU32::new(n)
    }

    /// Reading the same pages over and over must never fetch anything: the
    /// reader is not going anywhere.
    #[test]
    fn repeated_reads_of_one_page_fetch_nothing() {
        let mut ra = Readahead::new();
        for _ in 0..50 {
            assert_eq!(ra.on_read(7, false, window(32)), ReadaheadAction::None);
        }
    }

    /// A single forward step is not evidence of a scan.
    #[test]
    fn one_forward_step_fetches_nothing() {
        let mut ra = Readahead::new();
        assert_eq!(ra.on_read(10, false, window(32)), ReadaheadAction::None);
        assert_eq!(ra.on_read(11, false, window(32)), ReadaheadAction::None);
    }

    /// Two forward steps earn the first, deliberately small, window.
    #[test]
    fn two_forward_steps_earn_the_first_window() {
        let mut ra = Readahead::new();
        ra.on_read(10, false, window(32));
        ra.on_read(11, false, window(32));
        assert_eq!(
            ra.on_read(12, false, window(32)),
            ReadaheadAction::Fetch {
                start: 13,
                count: INITIAL_WINDOW
            }
        );
    }

    /// Windows ramp 4x then 2x then stop at the ceiling, and never exceed it.
    #[test]
    fn window_grows_then_clamps_at_the_ceiling() {
        assert_eq!(grow(0, 128), 4);
        assert_eq!(grow(4, 128), 16); // 4x while below max/16
        assert_eq!(grow(16, 128), 32); // 2x while at or below max/2
        assert_eq!(grow(32, 128), 64);
        assert_eq!(grow(64, 128), 128);
        assert_eq!(grow(128, 128), 128); // clamped
        for max in 1..=256u32 {
            let mut size = 0;
            for _ in 0..20 {
                size = grow(size, max);
                assert!(size <= max, "window {size} exceeded ceiling {max}");
                assert!(size > 0);
            }
        }
    }

    /// A one-page ceiling still works and never returns a zero-page fetch.
    #[test]
    fn ceiling_of_one_page_is_honored() {
        let mut ra = Readahead::new();
        ra.on_read(10, false, window(1));
        ra.on_read(11, false, window(1));
        assert_eq!(
            ra.on_read(12, false, window(1)),
            ReadaheadAction::Fetch {
                start: 13,
                count: 1
            }
        );
    }

    /// Stepping over an interior page keeps the run going -- this is the
    /// LINEITEM `+2` case, 12% of that scan's steps.
    #[test]
    fn small_forward_gaps_keep_the_run_alive() {
        let mut ra = Readahead::new();
        ra.on_read(10, false, window(32));
        ra.on_read(12, false, window(32));
        assert!(matches!(
            ra.on_read(14, false, window(32)),
            ReadaheadAction::Fetch { .. }
        ));
    }

    /// A stride wider than the gap limit never qualifies. This is the
    /// index-scan case that naive readahead turns into 9x read amplification.
    #[test]
    fn strided_reads_never_prefetch() {
        let mut ra = Readahead::new();
        let mut page = 100;
        for _ in 0..200 {
            assert_eq!(
                ra.on_read(page, false, window(64)),
                ReadaheadAction::None,
                "a stride of 9 pages must never qualify as sequential"
            );
            page += 9;
        }
    }

    /// Random access pays nothing.
    #[test]
    fn random_reads_never_prefetch() {
        let mut ra = Readahead::new();
        let mut seed = 12345u64;
        for _ in 0..1000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let page = ((seed >> 33) % 100_000) as i64;
            assert_eq!(ra.on_read(page, false, window(32)), ReadaheadAction::None);
        }
    }

    /// Walking backwards is not something this fetches for.
    #[test]
    fn backwards_scan_never_prefetches() {
        let mut ra = Readahead::new();
        for page in (10..500).rev() {
            assert_eq!(ra.on_read(page, false, window(32)), ReadaheadAction::None);
        }
    }

    /// Breaking the pattern drops the window back to the smallest size, so a
    /// reader that wanders off does not keep paying for a big window.
    #[test]
    fn breaking_the_pattern_resets_the_window() {
        let mut ra = Readahead::new();
        for page in 10..40 {
            ra.on_read(page, false, window(64));
        }
        // Jump far away, then walk forward again from there.
        ra.on_read(9000, false, window(64));
        ra.on_read(9001, false, window(64));
        assert_eq!(
            ra.on_read(9002, false, window(64)),
            ReadaheadAction::Fetch {
                start: 9003,
                count: INITIAL_WINDOW
            }
        );
    }

    /// Reaching the marker inside a window queues the next one without a
    /// stall, and the windows tile the file without gaps or overlap.
    #[test]
    fn crossing_the_marker_queues_the_next_window_back_to_back() {
        let mut ra = Readahead::new();
        ra.on_read(10, false, window(32));
        ra.on_read(11, false, window(32));
        let ReadaheadAction::Fetch { start, count } = ra.on_read(12, false, window(32)) else {
            panic!("expected a window");
        };
        assert_eq!((start, count), (13, 4));
        // Page 13 is the marker: served from cache, but it queues 17..
        let ReadaheadAction::Fetch { start, count } = ra.on_read(13, true, window(32)) else {
            panic!("expected the next window to be queued on the marker");
        };
        assert_eq!(start, 17, "windows must tile without a gap");
        assert_eq!(count, 8, "window should have grown");
        // Pages inside the window that are not the marker queue nothing.
        assert_eq!(ra.on_read(14, true, window(32)), ReadaheadAction::None);
        assert_eq!(ra.on_read(15, true, window(32)), ReadaheadAction::None);
    }

    /// Two cursors walking two tables at once both get followed.
    #[test]
    fn interleaved_scans_are_tracked_separately() {
        let mut ra = Readahead::new();
        let mut a = 1000;
        let mut b = 50_000;
        let mut fetched_a = 0;
        let mut fetched_b = 0;
        for _ in 0..40 {
            if let ReadaheadAction::Fetch { start, .. } = ra.on_read(a, false, window(32)) {
                assert!(start > a && start < 40_000);
                fetched_a += 1;
            }
            if let ReadaheadAction::Fetch { start, .. } = ra.on_read(b, false, window(32)) {
                assert!(start > b);
                fetched_b += 1;
            }
            a += 1;
            b += 1;
        }
        assert!(fetched_a > 5, "stream A should have been followed");
        assert!(fetched_b > 5, "stream B should have been followed");
    }

    /// More concurrent readers than slots: the busiest ones keep their state
    /// and nothing panics or mixes streams up.
    #[test]
    fn more_readers_than_slots_recycles_the_coldest() {
        let mut ra = Readahead::new();
        let mut heads: Vec<i64> = (0..STREAM_COUNT as i64 + 3).map(|i| i * 100_000).collect();
        for _ in 0..50 {
            for head in heads.iter_mut() {
                if let ReadaheadAction::Fetch { start, count } =
                    ra.on_read(*head, false, window(32))
                {
                    assert!(start > *head);
                    assert!(count > 0);
                }
                *head += 1;
            }
        }
    }

    /// The scan hint skips the warmup but not the sequentiality test.
    #[test]
    fn scan_hint_skips_warmup() {
        let mut ra = Readahead::new();
        ra.hint_scan_start(10);
        assert_eq!(
            ra.on_read(11, false, window(32)),
            ReadaheadAction::Fetch {
                start: 12,
                count: INITIAL_WINDOW
            },
            "a hinted scan should not need to prove itself first"
        );
    }

    /// A hinted scan whose pages turn out to be scattered still prefetches
    /// nothing. The plan says "read this whole b-tree"; the layout says the
    /// pages are not next to each other, and the layout wins.
    #[test]
    fn scan_hint_does_not_override_a_scattered_layout() {
        let mut ra = Readahead::new();
        ra.hint_scan_start(10);
        let mut page = 10;
        for _ in 0..100 {
            page += 9;
            assert_eq!(ra.on_read(page, false, window(32)), ReadaheadAction::None);
        }
    }

    /// Zero window means completely off, whatever the access pattern.
    #[test]
    fn disabled_readahead_never_fetches() {
        let mut ra = Readahead::new();
        ra.hint_scan_start(10);
        for page in 10..200 {
            assert_eq!(ra.on_read(page, false, None), ReadaheadAction::None);
        }
    }

    /// Reset forgets everything, so a reader has to earn its window again.
    #[test]
    fn reset_forgets_every_stream() {
        let mut ra = Readahead::new();
        for page in 10..40 {
            ra.on_read(page, false, window(32));
        }
        ra.reset();
        assert_eq!(ra.on_read(40, false, window(32)), ReadaheadAction::None);
        assert_eq!(ra.on_read(41, false, window(32)), ReadaheadAction::None);
        assert!(matches!(
            ra.on_read(42, false, window(32)),
            ReadaheadAction::Fetch { .. }
        ));
    }

    /// The pager retries a read after a cache spill. Seeing the same page
    /// twice must not count as progress or as a broken pattern.
    #[test]
    fn a_repeated_read_neither_advances_nor_breaks_the_run() {
        let mut ra = Readahead::new();
        ra.on_read(10, false, window(32));
        ra.on_read(10, false, window(32));
        ra.on_read(11, false, window(32));
        ra.on_read(11, false, window(32));
        assert_eq!(
            ra.on_read(12, false, window(32)),
            ReadaheadAction::Fetch {
                start: 13,
                count: INITIAL_WINDOW
            }
        );
    }

    /// Over a long sequential run every page is covered exactly once: windows
    /// must never leave a gap (a stall we could have avoided) or overlap
    /// (bandwidth we paid for twice).
    #[test]
    fn windows_tile_a_long_scan_exactly_once() {
        let mut ra = Readahead::new();
        let max = 32;
        let mut covered: Vec<i64> = Vec::new();
        let mut requested = std::collections::HashSet::new();
        for page in 1..5000i64 {
            // Anything already asked for reads from memory.
            let hit = requested.contains(&page);
            if let ReadaheadAction::Fetch { start, count } = ra.on_read(page, hit, window(max)) {
                assert!(count <= max);
                for p in start..start + count as i64 {
                    assert!(
                        requested.insert(p),
                        "page {p} was asked for twice: windows overlap"
                    );
                    covered.push(p);
                }
            }
        }
        covered.sort_unstable();
        for pair in covered.windows(2) {
            assert_eq!(
                pair[1] - pair[0],
                1,
                "windows left a gap between {} and {}",
                pair[0],
                pair[1]
            );
        }
        assert!(
            covered.len() > 4000,
            "a 5000-page sequential scan should have been almost entirely covered, got {}",
            covered.len()
        );
    }
}
