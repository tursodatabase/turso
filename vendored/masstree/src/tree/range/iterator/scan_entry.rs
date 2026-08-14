//! Filepath: `src/tree/range/iterator/scan_entry.rs`
//!
//! Entry type returned by range iterators.
///
/// Contains an owned copy of the key and the value output.
///
/// # Type Parameters
///
/// - `O`: The output type (e.g., `ValuePtr<V>` for `MassTree15<V>`, `V` for `MassTree<V>`/`MassTree15Inline<V>`)
///
/// # Example
///
/// ```ignore
/// for entry in tree.iter(&guard) {
///     let key: &[u8] = &entry.key;
///     let value: &V = entry.value.as_ref(); // For ValuePtr<V>
/// }
///
#[derive(Debug, Clone)]
pub struct ScanEntry<O> {
    /// The key as owned bytes.
    pub key: Vec<u8>,

    /// The value output.
    ///
    /// For `MassTree15<V>`: `ValuePtr<V>` (pointer valid under guard)
    /// For `MassTree<V>` / `MassTree15Inline<V>`: `V` (copy)
    pub value: O,
}

impl<O> ScanEntry<O> {
    /// Create a new scan entry.
    #[inline(always)]
    pub const fn new(key: Vec<u8>, value: O) -> Self {
        Self { key, value }
    }

    /// Get the key bytes.
    #[inline(always)]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Get a reference to the value.
    #[inline(always)]
    pub const fn value(&self) -> &O {
        &self.value
    }

    /// Consume the entry and return (key, value).
    #[inline(always)]
    pub fn into_parts(self) -> (Vec<u8>, O) {
        (self.key, self.value)
    }
}
