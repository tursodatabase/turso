//! Filepath: `src/tree/range/iterator/iter_flags.rs`
//!
//! Packed boolean flags for forward and reverse scan contexts.

// ============================================================================
//  ForwardFlags — flags for ForwardScanCtx
// ============================================================================

/// Packed boolean flags for forward scan context state.
///
/// Uses a u8 bitfield to store 5 boolean flags efficiently.
/// Lives inside `ForwardScanCtx`. The `SINGLE_LAYER_MODE` flag is the
/// canonical source for both forward and reverse paths.
#[derive(Clone, Copy, Debug, Default)]
pub struct ForwardFlags(u8);

#[allow(dead_code, reason = "API Completeness")]
impl ForwardFlags {
    const EXHAUSTED: u8 = 1 << 0;
    const INITIALIZED: u8 = 1 << 1;
    const EMIT_EQUAL: u8 = 1 << 2;
    const NEEDS_DUPLICATE_CHECK: u8 = 1 << 3;
    const SINGLE_LAYER_MODE: u8 = 1 << 4;

    /// Create new flags with all bits cleared.
    #[inline(always)]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Create flags with initial values for forward iteration.
    #[inline(always)]
    pub const fn with_values(emit_equal: bool, single_layer_mode: bool) -> Self {
        let mut bits: u8 = 0;

        if emit_equal {
            bits |= Self::EMIT_EQUAL;
        }

        if single_layer_mode {
            bits |= Self::SINGLE_LAYER_MODE;
        }

        Self(bits)
    }

    // ========================================================================
    //  Getters
    // ========================================================================

    #[inline(always)]
    pub const fn exhausted(self) -> bool {
        self.0 & Self::EXHAUSTED != 0
    }

    #[inline(always)]
    pub const fn initialized(self) -> bool {
        self.0 & Self::INITIALIZED != 0
    }

    #[inline(always)]
    pub const fn emit_equal(self) -> bool {
        self.0 & Self::EMIT_EQUAL != 0
    }

    #[inline(always)]
    pub const fn needs_duplicate_check(self) -> bool {
        self.0 & Self::NEEDS_DUPLICATE_CHECK != 0
    }

    #[inline(always)]
    pub const fn single_layer_mode(self) -> bool {
        self.0 & Self::SINGLE_LAYER_MODE != 0
    }

    /// TURSO PATCH: `EMIT_EQUAL` doubles as "the Included start key has not
    /// been emitted yet". Clear it on the first forward emission so the
    /// duplicate check treats later cursor-equal slots as duplicates again
    /// (mirrors the reverse side's `on_pre_emit`).
    #[inline(always)]
    pub const fn clear_emit_equal(&mut self) {
        self.0 &= !Self::EMIT_EQUAL;
    }

    // ========================================================================
    //  Setters
    // ========================================================================

    #[inline(always)]
    pub const fn set_exhausted(&mut self, value: bool) {
        if value {
            self.0 |= Self::EXHAUSTED;
        } else {
            self.0 &= !Self::EXHAUSTED;
        }
    }

    #[inline(always)]
    pub const fn set_initialized(&mut self, value: bool) {
        if value {
            self.0 |= Self::INITIALIZED;
        } else {
            self.0 &= !Self::INITIALIZED;
        }
    }

    // ========================================================================
    //  Convenience methods
    // ========================================================================

    /// Mark as exhausted.
    #[inline(always)]
    pub const fn mark_exhausted(&mut self) {
        self.0 |= Self::EXHAUSTED;
    }

    /// Mark as initialized.
    #[inline(always)]
    pub const fn mark_initialized(&mut self) {
        self.0 |= Self::INITIALIZED;
    }

    /// Clear `needs_duplicate_check` flag.
    #[inline(always)]
    pub const fn clear_duplicate_check(&mut self) {
        self.0 &= !Self::NEEDS_DUPLICATE_CHECK;
    }

    /// Set `needs_duplicate_check` flag.
    #[inline(always)]
    pub const fn require_duplicate_check(&mut self) {
        self.0 |= Self::NEEDS_DUPLICATE_CHECK;
    }

    /// Disable single-layer mode (fall back to multi-layer).
    #[inline(always)]
    pub const fn disable_single_layer_mode(&mut self) {
        self.0 &= !Self::SINGLE_LAYER_MODE;
    }
}

// ============================================================================
//  ReverseFlags — extracted reverse-only flags for ReverseScanCtx
// ============================================================================

/// Packed boolean flags for reverse scan context state.
///
/// Uses a u8 bitfield to store 4 boolean flags efficiently.
/// Lives inside `ReverseScanCtx`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReverseFlags(u8);

#[allow(dead_code, reason = "API Completeness")]
impl ReverseFlags {
    const EXHAUSTED: u8 = 1 << 0;
    const INITIALIZED: u8 = 1 << 1;
    const EMIT_EQUAL: u8 = 1 << 2;
    const NEEDS_DUPLICATE_CHECK: u8 = 1 << 3;
    const UPPER_BOUND: u8 = 1 << 4;

    /// Create new flags with all bits cleared.
    #[inline(always)]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Create flags with initial values for reverse iteration.
    #[inline(always)]
    pub const fn with_values(emit_equal: bool) -> Self {
        let mut bits: u8 = 0;

        if emit_equal {
            bits |= Self::EMIT_EQUAL;
        }

        Self(bits)
    }

    // ========================================================================
    //  Getters
    // ========================================================================

    #[inline(always)]
    pub const fn exhausted(self) -> bool {
        self.0 & Self::EXHAUSTED != 0
    }

    #[inline(always)]
    pub const fn initialized(self) -> bool {
        self.0 & Self::INITIALIZED != 0
    }

    #[inline(always)]
    pub const fn emit_equal(self) -> bool {
        self.0 & Self::EMIT_EQUAL != 0
    }

    #[inline(always)]
    pub const fn needs_duplicate_check(self) -> bool {
        self.0 & Self::NEEDS_DUPLICATE_CHECK != 0
    }

    /// Whether we're at an upper bound position (scanning from maximum).
    ///
    /// When true:
    /// - `lower_reverse()` returns `size - 1` (start from last slot)
    /// - `is_duplicate_reverse()` returns false (no filtering needed)
    ///
    /// Set true on sublayer descent via `shift_clear_reverse()`,
    /// cleared by `clear_upper_bound()` after successful emission.
    #[inline(always)]
    pub const fn upper_bound(self) -> bool {
        self.0 & Self::UPPER_BOUND != 0
    }

    // ========================================================================
    //  Setters
    // ========================================================================

    #[inline(always)]
    pub const fn set_exhausted(&mut self, value: bool) {
        if value {
            self.0 |= Self::EXHAUSTED;
        } else {
            self.0 &= !Self::EXHAUSTED;
        }
    }

    #[inline(always)]
    pub const fn set_initialized(&mut self, value: bool) {
        if value {
            self.0 |= Self::INITIALIZED;
        } else {
            self.0 &= !Self::INITIALIZED;
        }
    }

    // ========================================================================
    //  Convenience methods
    // ========================================================================

    /// Mark as exhausted.
    #[inline(always)]
    pub const fn mark_exhausted(&mut self) {
        self.0 |= Self::EXHAUSTED;
    }

    /// Mark as initialized.
    #[inline(always)]
    pub const fn mark_initialized(&mut self) {
        self.0 |= Self::INITIALIZED;
    }

    /// Clear `needs_duplicate_check` flag.
    #[inline(always)]
    pub const fn clear_duplicate_check(&mut self) {
        self.0 &= !Self::NEEDS_DUPLICATE_CHECK;
    }

    /// Set `needs_duplicate_check` flag.
    #[inline(always)]
    pub const fn require_duplicate_check(&mut self) {
        self.0 |= Self::NEEDS_DUPLICATE_CHECK;
    }

    /// Set `upper_bound` flag (scanning from maximum position).
    #[inline(always)]
    pub const fn set_upper_bound(&mut self) {
        self.0 |= Self::UPPER_BOUND;
    }

    /// Clear `upper_bound` flag after successful key emission.
    #[inline(always)]
    pub const fn clear_upper_bound(&mut self) {
        self.0 &= !Self::UPPER_BOUND;
    }
}
