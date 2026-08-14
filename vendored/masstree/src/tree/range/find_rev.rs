// ============================================================================
//  Reverse Scan — Shared Types
// ============================================================================
//
// The bulk of reverse scan logic now lives in `reverse_ctx.rs`.
// This module retains only:
//  - `LeafBatchResultBack` enum (used by both `reverse_ctx` and batch strategies)
//  - Unit tests for cursor/helper operations

/// Result of processing a single leaf node during reverse batch iteration.
///
/// Each variant maps to a specific action the caller's state machine must take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LeafBatchResultBack {
    /// All entries in leaf processed, need to retreat to previous leaf
    LeafExhausted = 0,

    /// Encountered a layer pointer, need to descend
    LayerEncountered = 1,

    /// Version changed during processing, need retry
    VersionChanged = 2,

    /// Visitor returned false, stop iteration
    Stopped = 3,

    /// Start bound exceeded, stop iteration
    StartBoundExceeded = 4,
}

// ============================================================================
//  Tests
// ============================================================================
