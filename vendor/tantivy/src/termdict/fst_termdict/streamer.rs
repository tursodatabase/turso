use std::io;

use tantivy_fst::automaton::AlwaysMatch;
use tantivy_fst::map::{Stream, StreamBuilder};
use tantivy_fst::{Automaton, IntoStreamer, Streamer};

use super::TermDictionary;
use crate::postings::TermInfo;
use crate::termdict::TermOrdinal;

/// `TermStreamerBuilder` is a helper object used to define
/// a range of terms that should be streamed.
pub struct TermStreamerBuilder<'a, A = AlwaysMatch>
where A: Automaton
{
    fst_map: &'a TermDictionary,
    stream_builder: StreamBuilder<'a, A>,
}

impl<'a, A> TermStreamerBuilder<'a, A>
where A: Automaton
{
    pub(crate) fn new(fst_map: &'a TermDictionary, stream_builder: StreamBuilder<'a, A>) -> Self {
        TermStreamerBuilder {
            fst_map,
            stream_builder,
        }
    }

    /// Limit the range to terms greater or equal to the bound
    pub fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.ge(bound);
        self
    }

    /// Limit the range to terms strictly greater than the bound
    pub fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.gt(bound);
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.le(bound);
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.lt(bound);
        self
    }

    /// Iterate over the range backwards.
    pub fn backward(mut self) -> Self {
        self.stream_builder = self.stream_builder.backward();
        self
    }

    /// Creates the stream corresponding to the range
    /// of terms defined using the `TermStreamerBuilder`.
    pub fn into_stream(self) -> io::Result<TermStreamer<'a, A>> {
        Ok(TermStreamer {
            fst_map: self.fst_map,
            stream: self.stream_builder.into_stream(),
            term_ord: 0u64,
            current_key: Vec::with_capacity(100),
            current_value: TermInfo::default(),
        })
    }
}

/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub struct TermStreamer<'a, A = AlwaysMatch>
where A: Automaton
{
    pub(crate) fst_map: &'a TermDictionary,
    pub(crate) stream: Stream<'a, A>,
    term_ord: TermOrdinal,
    current_key: Vec<u8>,
    current_value: TermInfo,
}

impl<A> TermStreamer<'_, A>
where A: Automaton
{
    /// Advance position the stream on the next item.
    /// Before the first call to `.advance()`, the stream
    /// is an uninitialized state.
    pub fn advance(&mut self) -> bool {
        if let Some((term, term_ord)) = self.stream.next() {
            self.current_key.clear();
            self.current_key.extend_from_slice(term);
            self.term_ord = term_ord;
            self.current_value = self.fst_map.term_info_from_ord(term_ord);
            true
        } else {
            false
        }
    }

    /// Returns the `TermOrdinal` of the given term.
    ///
    /// May panic if the called as `.advance()` as never
    /// been called before.
    pub fn term_ord(&self) -> TermOrdinal {
        self.term_ord
    }

    /// Accesses the current key.
    ///
    /// `.key()` should return the key that was returned
    /// by the `.next()` method.
    ///
    /// If the end of the stream as been reached, and `.next()`
    /// has been called and returned `None`, `.key()` remains
    /// the value of the last key encountered.
    ///
    /// Before any call to `.next()`, `.key()` returns an empty array.
    pub fn key(&self) -> &[u8] {
        &self.current_key
    }

    /// Accesses the current value.
    ///
    /// Calling `.value()` after the end of the stream will return the
    /// last `.value()` encountered.
    ///
    /// # Panics
    ///
    /// Calling `.value()` before the first call to `.advance()` returns
    /// `V::default()`.
    pub fn value(&self) -> &TermInfo {
        &self.current_value
    }

    /// Return the next `(key, value)` pair.
    #[expect(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(&[u8], &TermInfo)> {
        if self.advance() {
            Some((self.key(), self.value()))
        } else {
            None
        }
    }
}
