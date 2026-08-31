//! Scan readahead policy: decide when a B-tree scan should read pages
//! before the cursor gets to them, and how many.
//!
//! The shape is borrowed from Linux file readahead:
//!
//! * Random access never prefetches. Readahead starts only when access is
//!   known to be sequential: either the scan was started with a rewind
//!   (a declared full scan, the analog of Linux starting readahead for a
//!   read at file offset 0), or the cursor has moved across enough
//!   consecutive leaf pages.
//! * The distance we stay ahead of the cursor starts small and doubles
//!   each time the scan consumes a full window, so short scans waste at
//!   most a few pages while long scans ramp up to the cap.
//! * The cap comes from `PRAGMA prefetch_pages`; 0 (the default) turns
//!   readahead off entirely.
//!
//! Unlike a filesystem we never guess future offsets: the cursor's parent
//! interior page lists exactly which leaf pages the scan visits next, so a
//! prefetched page is wrong only if the scan stops early. This module is
//! pure policy; the B-tree cursor resolves child indexes to page numbers
//! and the pager submits the reads.

/// Upper bound for `PRAGMA prefetch_pages`. Bounds how much IO a scan can
/// have in flight; 4096 pages is 16 MiB at the default page size.
pub(crate) const MAX_PREFETCH_PAGES: usize = 4096;

/// Window size for the first batch of a newly activated scan.
const INITIAL_WINDOW: usize = 4;

/// Number of consecutive forward leaf transitions after which access
/// counts as sequential. A rewind pre-satisfies this.
const ACTIVATION_STREAK: usize = 2;

/// Per-cursor readahead state.
pub(crate) struct ScanReadahead {
    /// Max pages this scan may keep ahead of the cursor. 0 = off.
    budget: usize,
    /// Stack level where this btree's leaf pages live, once we've seen one.
    leaf_level: Option<usize>,
    /// Consecutive forward leaf transitions since the last jump.
    streak: usize,
    /// How many pages ahead we currently try to stay.
    window: usize,
    /// Leaf transitions consumed since the window last grew.
    consumed_since_growth: usize,
    /// How far we have prefetched into the current parent interior page.
    frontier: Option<Frontier>,
}

struct Frontier {
    /// Page id of the interior page the prefetched children belong to.
    interior_page_id: usize,
    /// First child index not prefetched yet. Child index `cell_count`
    /// means the interior page's rightmost pointer.
    next_child_idx: usize,
    /// Whether the interior page that follows the current one was already
    /// prefetched, so switching parents does not stall the scan.
    next_interior_prefetched: bool,
}

/// One batch of prefetches for the cursor to submit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrefetchPlan {
    /// First child index of the current interior page to prefetch.
    /// Index `cell_count` means the rightmost pointer.
    pub start_child_idx: usize,
    /// How many children to prefetch, starting at `start_child_idx`.
    pub count: usize,
    /// Also prefetch the next interior page at this level (found through
    /// the grandparent): this plan reaches the end of the current parent.
    pub prefetch_next_interior: bool,
}

impl ScanReadahead {
    pub fn new() -> Self {
        Self {
            budget: 0,
            leaf_level: None,
            streak: 0,
            window: INITIAL_WINDOW,
            consumed_since_growth: 0,
            frontier: None,
        }
    }

    /// The cursor repositioned (seek, rewind, move to root): forget the
    /// access pattern. `budget` is the current `PRAGMA prefetch_pages`
    /// value; it is re-read here so the pragma takes effect on the next
    /// scan without touching in-flight state.
    pub fn on_jump(&mut self, budget: usize) {
        self.budget = budget;
        self.streak = 0;
        self.window = INITIAL_WINDOW.min(budget.max(1));
        self.consumed_since_growth = 0;
        self.frontier = None;
    }

    /// A full scan was requested (cursor rewind). Sequential access is
    /// certain, so readahead starts at the first leaf instead of waiting
    /// for a streak.
    pub fn on_scan_start(&mut self) {
        self.streak = ACTIVATION_STREAK;
    }

    /// Remember at which stack level the leaves of this btree live.
    pub fn note_leaf_level(&mut self, level: usize) {
        self.leaf_level = Some(level);
    }

    /// Turn readahead off until the next jump. Used when a speculative
    /// read fails; the real read will surface any real problem.
    pub fn disable_for_scan(&mut self) {
        self.budget = 0;
    }

    /// The cursor is about to descend from the interior page at stack
    /// level `level` (page id `interior_page_id`, `cell_count` cells) into
    /// its child at `child_idx` (`cell_count` = the rightmost pointer).
    /// Returns the batch to prefetch, if any.
    pub fn on_descend(
        &mut self,
        level: usize,
        interior_page_id: usize,
        child_idx: usize,
        cell_count: usize,
    ) -> Option<PrefetchPlan> {
        if self.budget == 0 {
            return None;
        }
        // Only descents into leaves count: higher-level descents happen
        // once per hundreds of leaves and would pollute the streak/window
        // accounting without measurably helping.
        let leaf_level = self.leaf_level?;
        if level + 1 != leaf_level {
            return None;
        }

        self.streak += 1;
        if self.streak < ACTIVATION_STREAK {
            return None;
        }

        // The window doubles each time the scan consumes a full window of
        // leaves, up to the budget.
        self.consumed_since_growth += 1;
        if self.consumed_since_growth >= self.window {
            self.window = (self.window * 2).min(self.budget);
            self.consumed_since_growth = 0;
        }

        // Child indexes run 0..=cell_count; index cell_count is the
        // rightmost pointer.
        let last_child_idx = cell_count;
        let mut frontier = match self.frontier.take() {
            Some(f) if f.interior_page_id == interior_page_id => f,
            _ => Frontier {
                interior_page_id,
                next_child_idx: child_idx + 1,
                next_interior_prefetched: false,
            },
        };
        // Never lag behind the cursor (e.g. after it skipped over cells).
        frontier.next_child_idx = frontier.next_child_idx.max(child_idx + 1);

        let ahead = frontier.next_child_idx - (child_idx + 1);
        let want = self.window.saturating_sub(ahead);
        let available = (last_child_idx + 1).saturating_sub(frontier.next_child_idx);
        let count = want.min(available);

        let mut plan = None;
        if count > 0 {
            plan = Some(PrefetchPlan {
                start_child_idx: frontier.next_child_idx,
                count,
                prefetch_next_interior: false,
            });
            frontier.next_child_idx += count;
        }
        // The window extends past this parent: also get the next interior
        // page moving so the scan does not stall when switching parents.
        if want > available && !frontier.next_interior_prefetched {
            frontier.next_interior_prefetched = true;
            match plan.as_mut() {
                Some(p) => p.prefetch_next_interior = true,
                None => {
                    plan = Some(PrefetchPlan {
                        start_child_idx: frontier.next_child_idx,
                        count: 0,
                        prefetch_next_interior: true,
                    })
                }
            }
        }
        self.frontier = Some(frontier);
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active_readahead(budget: usize) -> ScanReadahead {
        let mut ra = ScanReadahead::new();
        ra.on_jump(budget);
        ra.on_scan_start();
        ra.note_leaf_level(2);
        ra
    }

    #[test]
    fn budget_zero_never_prefetches() {
        let mut ra = ScanReadahead::new();
        ra.on_jump(0);
        ra.on_scan_start();
        ra.note_leaf_level(1);
        for child in 0..100 {
            assert_eq!(ra.on_descend(0, 7, child, 200), None);
        }
    }

    #[test]
    fn no_prefetch_before_leaf_level_is_known() {
        let mut ra = ScanReadahead::new();
        ra.on_jump(64);
        ra.on_scan_start();
        assert_eq!(ra.on_descend(0, 7, 0, 200), None);
    }

    #[test]
    fn descents_above_the_leaf_parent_level_are_ignored() {
        let mut ra = active_readahead(64);
        // leaf_level is 2, so only level-1 interiors trigger prefetch.
        assert_eq!(ra.on_descend(0, 7, 0, 200), None);
        assert!(ra.on_descend(1, 8, 0, 200).is_some());
    }

    #[test]
    fn rewind_prefetches_on_the_first_leaf_descend() {
        let mut ra = active_readahead(64);
        let plan = ra.on_descend(1, 7, 0, 200).unwrap();
        assert_eq!(plan.start_child_idx, 1);
        assert_eq!(plan.count, INITIAL_WINDOW);
        assert!(!plan.prefetch_next_interior);
    }

    #[test]
    fn random_access_needs_a_streak_before_prefetching() {
        let mut ra = ScanReadahead::new();
        ra.on_jump(64);
        ra.note_leaf_level(2);
        // First leaf transition after a seek: still counts as random.
        assert_eq!(ra.on_descend(1, 7, 3, 200), None);
        // Second consecutive transition: sequential, start prefetching.
        assert!(ra.on_descend(1, 7, 4, 200).is_some());
    }

    #[test]
    fn a_jump_resets_the_streak() {
        let mut ra = ScanReadahead::new();
        ra.on_jump(64);
        ra.note_leaf_level(2);
        assert_eq!(ra.on_descend(1, 7, 3, 200), None);
        ra.on_jump(64);
        ra.note_leaf_level(2);
        assert_eq!(ra.on_descend(1, 7, 9, 200), None);
    }

    #[test]
    fn window_doubles_as_the_scan_consumes_it_and_respects_the_budget() {
        let budget = 32;
        let mut ra = active_readahead(budget);
        let mut max_ahead = 0usize;
        let mut issued = 0usize;
        for child in 0..200usize {
            if let Some(plan) = ra.on_descend(1, 7, child, 400) {
                issued += plan.count;
                let ahead = plan.start_child_idx + plan.count - (child + 1);
                max_ahead = max_ahead.max(ahead);
                assert!(
                    ahead <= budget,
                    "scan got {ahead} pages ahead with budget {budget}"
                );
            }
        }
        // The scan must actually ramp up to the full budget.
        assert_eq!(max_ahead, budget);
        // Everything issued is consumed by a 200-leaf scan except the final
        // in-flight window.
        assert!(issued <= 200 + budget);
    }

    #[test]
    fn total_prefetch_never_exceeds_consumption_plus_one_window() {
        // A scan aborted after N leaves must not have prefetched more than
        // N + budget pages: this is the "do not fetch too much" bound.
        let budget = 16;
        for scan_len in [1usize, 2, 3, 5, 10, 50] {
            let mut ra = active_readahead(budget);
            let mut issued = 0usize;
            for child in 0..scan_len {
                if let Some(plan) = ra.on_descend(1, 7, child, 400) {
                    issued += plan.count;
                }
            }
            assert!(
                issued <= scan_len + budget,
                "scan of {scan_len} leaves prefetched {issued} pages"
            );
        }
    }

    #[test]
    fn prefetch_covers_every_upcoming_child_exactly_once() {
        let cell_count = 50;
        let mut ra = active_readahead(8);
        let mut seen = vec![0usize; cell_count + 1];
        for child in 0..cell_count {
            if let Some(plan) = ra.on_descend(1, 7, child, cell_count) {
                for idx in plan.start_child_idx..plan.start_child_idx + plan.count {
                    seen[idx] += 1;
                }
            }
        }
        // Children 0 (never upcoming) is skipped, everything else fetched
        // exactly once: no duplicate and no missed IO.
        for (idx, count) in seen.iter().enumerate().skip(1) {
            assert_eq!(*count, 1, "child {idx} prefetched {count} times");
        }
    }

    #[test]
    fn reaching_the_end_of_a_parent_prefetches_the_next_interior_once() {
        let cell_count = 5;
        let mut ra = active_readahead(64);
        let mut next_interior_plans = 0;
        for child in 0..=cell_count {
            if let Some(plan) = ra.on_descend(1, 7, child, cell_count) {
                if plan.prefetch_next_interior {
                    next_interior_plans += 1;
                }
            }
        }
        assert_eq!(next_interior_plans, 1);
    }

    #[test]
    fn moving_to_a_new_parent_restarts_the_frontier() {
        let mut ra = active_readahead(8);
        // Consume parent 7 fully.
        for child in 0..=5 {
            ra.on_descend(1, 7, child, 5);
        }
        // First descend in parent 9 prefetches its upcoming children.
        let plan = ra.on_descend(1, 9, 0, 5).unwrap();
        assert_eq!(plan.start_child_idx, 1);
        assert!(plan.count > 0);
    }

    #[test]
    fn disable_for_scan_stops_prefetching_until_the_next_jump() {
        let mut ra = active_readahead(64);
        assert!(ra.on_descend(1, 7, 0, 200).is_some());
        ra.disable_for_scan();
        assert_eq!(ra.on_descend(1, 7, 1, 200), None);
        ra.on_jump(64);
        ra.on_scan_start();
        ra.note_leaf_level(2);
        assert!(ra.on_descend(1, 7, 5, 200).is_some());
    }
}
