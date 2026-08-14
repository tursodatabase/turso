//! Filepath: src/tree/range/find.rs
//!
//! Types shared by forward scan functions.
//!
//! The scan algorithm implementations (`find_initial`, `find_next`, `find_retry`, etc.)
//! now live as methods on [`super::forward_ctx::ForwardScanCtx`].

// ============================================================================
//  LeafBatchResult
// ============================================================================

/// Result of processing entries within a single leaf.
///
/// Uses `#[repr(u8)]` for smaller size (1 byte vs default enum size) and
/// faster dispatch via direct byte comparison. This enum is returned from
/// hot-path batch processing functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LeafBatchResult {
    /// All entries in leaf processed, need to advance to next leaf
    LeafExhausted = 0,

    /// Encountered a layer pointer, need to descend
    LayerEncountered = 1,

    /// Version changed during processing, need retry
    VersionChanged = 2,

    /// Visitor returned false, stop iteration
    Stopped = 3,

    /// End bound exceeded, stop iteration
    EndBoundExceeded = 4,
}
