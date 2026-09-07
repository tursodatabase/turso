use common::read_u32_vint;
use stacker::{ExpUnrolledLinkedList, MemoryArena};

use crate::postings::FieldSerializer;
use crate::DocId;

const POSITION_END: u32 = 0;

#[derive(Default)]
pub(crate) struct BufferLender {
    buffer_u8: Vec<u8>,
    buffer_u32: Vec<u32>,
}

impl BufferLender {
    pub fn lend_u8(&mut self) -> &mut Vec<u8> {
        self.buffer_u8.clear();
        &mut self.buffer_u8
    }
    pub fn lend_all(&mut self) -> (&mut Vec<u8>, &mut Vec<u32>) {
        self.buffer_u8.clear();
        self.buffer_u32.clear();
        (&mut self.buffer_u8, &mut self.buffer_u32)
    }
}

pub struct VInt32Reader<'a> {
    data: &'a [u8],
}

impl<'a> VInt32Reader<'a> {
    fn new(data: &'a [u8]) -> VInt32Reader<'a> {
        VInt32Reader { data }
    }
}

impl Iterator for VInt32Reader<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.data.is_empty() {
            None
        } else {
            Some(read_u32_vint(&mut self.data))
        }
    }
}

/// `Recorder` is in charge of recording relevant information about
/// the presence of a term in a document.
///
/// Depending on the [`TextOptions`](crate::schema::TextOptions) associated
/// with the field, the recorder may record:
///   * the document frequency
///   * the document id
///   * the term frequency
///   * the term positions
pub(crate) trait Recorder: Copy + Default + Send + Sync + 'static {
    /// Returns the current document
    fn current_doc(&self) -> u32;
    /// Starts recording information about a new document
    /// This method shall only be called if the term is within the document.
    fn new_doc(&mut self, doc: DocId, arena: &mut MemoryArena);
    /// Record the position of a term. For each document,
    /// this method will be called `term_freq` times.
    fn record_position(&mut self, position: u32, arena: &mut MemoryArena);
    /// Close the document. It will help record the term frequency.
    fn close_doc(&mut self, arena: &mut MemoryArena);
    /// Pushes the postings information to the serializer.
    fn serialize(
        &self,
        arena: &MemoryArena,
        serializer: &mut FieldSerializer<'_>,
        buffer_lender: &mut BufferLender,
    );
    /// Returns the number of document containing this term.
    ///
    /// Returns `None` if not available.
    fn term_doc_freq(&self) -> Option<u32>;

    #[inline]
    fn has_term_freq(&self) -> bool {
        true
    }
}

/// Only records the doc ids
#[derive(Clone, Copy, Default)]
pub struct DocIdRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
}

impl Recorder for DocIdRecorder {
    #[inline]
    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    #[inline]
    fn new_doc(&mut self, doc: DocId, arena: &mut MemoryArena) {
        let delta = doc - self.current_doc;
        self.current_doc = doc;
        self.stack.writer(arena).write_u32_vint(delta);
    }

    #[inline]
    fn record_position(&mut self, _position: u32, _arena: &mut MemoryArena) {}

    #[inline]
    fn close_doc(&mut self, _arena: &mut MemoryArena) {}

    fn serialize(
        &self,
        arena: &MemoryArena,
        serializer: &mut FieldSerializer<'_>,
        buffer_lender: &mut BufferLender,
    ) {
        let buffer = buffer_lender.lend_u8();
        // TODO avoid reading twice.
        self.stack.read_to_end(arena, buffer);
        let iter = get_sum_reader(VInt32Reader::new(&buffer[..]));
        for doc_id in iter {
            serializer.write_doc(doc_id, 0u32, &[][..]);
        }
    }

    fn term_doc_freq(&self) -> Option<u32> {
        None
    }

    fn has_term_freq(&self) -> bool {
        false
    }
}

/// Takes an Iterator of delta encoded elements and returns an iterator
/// that yields the sum of the elements.
fn get_sum_reader(iter: impl Iterator<Item = u32>) -> impl Iterator<Item = u32> {
    iter.scan(0, |state, delta| {
        *state += delta;
        Some(*state)
    })
}

/// Recorder encoding document ids, and term frequencies
#[derive(Clone, Copy, Default)]
pub struct TermFrequencyRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    current_tf: u32,
    term_doc_freq: u32,
}

impl Recorder for TermFrequencyRecorder {
    #[inline]
    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    #[inline]
    fn new_doc(&mut self, doc: DocId, arena: &mut MemoryArena) {
        let delta = doc - self.current_doc;
        self.term_doc_freq += 1;
        self.current_doc = doc;
        self.stack.writer(arena).write_u32_vint(delta);
    }

    #[inline]
    fn record_position(&mut self, _position: u32, _arena: &mut MemoryArena) {
        self.current_tf += 1;
    }

    #[inline]
    fn close_doc(&mut self, arena: &mut MemoryArena) {
        debug_assert!(self.current_tf > 0);
        self.stack.writer(arena).write_u32_vint(self.current_tf);
        self.current_tf = 0;
    }

    fn serialize(
        &self,
        arena: &MemoryArena,
        serializer: &mut FieldSerializer<'_>,
        buffer_lender: &mut BufferLender,
    ) {
        let buffer = buffer_lender.lend_u8();
        self.stack.read_to_end(arena, buffer);
        let mut u32_it = VInt32Reader::new(&buffer[..]);
        let mut prev_doc = 0;
        while let Some(delta_doc_id) = u32_it.next() {
            let doc_id = prev_doc + delta_doc_id;
            prev_doc = doc_id;
            let term_freq = u32_it.next().unwrap_or(self.current_tf);
            serializer.write_doc(doc_id, term_freq, &[][..]);
        }
    }

    fn term_doc_freq(&self) -> Option<u32> {
        Some(self.term_doc_freq)
    }
}

/// Recorder encoding term frequencies as well as positions.
#[derive(Clone, Copy, Default)]
pub struct TfAndPositionRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    term_doc_freq: u32,
}

impl Recorder for TfAndPositionRecorder {
    #[inline]
    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    #[inline]
    fn new_doc(&mut self, doc: DocId, arena: &mut MemoryArena) {
        let delta = doc - self.current_doc;
        self.current_doc = doc;
        self.term_doc_freq += 1u32;
        self.stack.writer(arena).write_u32_vint(delta);
    }

    #[inline]
    fn record_position(&mut self, position: u32, arena: &mut MemoryArena) {
        self.stack
            .writer(arena)
            .write_u32_vint(position.wrapping_add(1u32));
    }

    #[inline]
    fn close_doc(&mut self, arena: &mut MemoryArena) {
        self.stack.writer(arena).write_u32_vint(POSITION_END);
    }

    fn serialize(
        &self,
        arena: &MemoryArena,
        serializer: &mut FieldSerializer<'_>,
        buffer_lender: &mut BufferLender,
    ) {
        let (buffer_u8, buffer_positions) = buffer_lender.lend_all();
        self.stack.read_to_end(arena, buffer_u8);
        let mut u32_it = VInt32Reader::new(&buffer_u8[..]);
        let mut prev_doc = 0;
        while let Some(delta_doc_id) = u32_it.next() {
            let doc_id = prev_doc + delta_doc_id;
            prev_doc = doc_id;
            let mut prev_position_plus_one = 1u32;
            buffer_positions.clear();
            loop {
                match u32_it.next() {
                    Some(POSITION_END) | None => {
                        break;
                    }
                    Some(position_plus_one) => {
                        let delta_position = position_plus_one - prev_position_plus_one;
                        buffer_positions.push(delta_position);
                        prev_position_plus_one = position_plus_one;
                    }
                }
            }
            serializer.write_doc(doc_id, buffer_positions.len() as u32, buffer_positions);
        }
    }

    fn term_doc_freq(&self) -> Option<u32> {
        Some(self.term_doc_freq)
    }
}

#[cfg(test)]
mod tests {

    use common::write_u32_vint;

    use super::{BufferLender, VInt32Reader};

    #[test]
    fn test_buffer_lender() {
        let mut buffer_lender = BufferLender::default();
        {
            let buf = buffer_lender.lend_u8();
            assert!(buf.is_empty());
            buf.push(1u8);
        }
        {
            let buf = buffer_lender.lend_u8();
            assert!(buf.is_empty());
            buf.push(1u8);
        }
        {
            let (_, buf) = buffer_lender.lend_all();
            assert!(buf.is_empty());
            buf.push(1u32);
        }
        {
            let (_, buf) = buffer_lender.lend_all();
            assert!(buf.is_empty());
            buf.push(1u32);
        }
    }

    #[test]
    fn test_vint_u32() {
        let mut buffer = vec![];
        let vals = [0, 1, 324_234_234, u32::MAX];
        for &i in &vals {
            assert!(write_u32_vint(i, &mut buffer).is_ok());
        }
        assert_eq!(buffer.len(), 1 + 1 + 5 + 5);
        let res: Vec<u32> = VInt32Reader::new(&buffer[..]).collect();
        assert_eq!(&res[..], &vals[..]);
    }
}
