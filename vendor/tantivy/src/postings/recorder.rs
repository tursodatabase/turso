use common::read_u32_vint;
use stacker::{ExpUnrolledLinkedList, MemoryArena};

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
    /// Decodes recorded documents independently of their output sink.
    fn recorded_docs<'a>(
        &self,
        arena: &MemoryArena,
        buffer_lender: &'a mut BufferLender,
    ) -> RecordedDocs<'a>;
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

    fn recorded_docs<'a>(
        &self,
        arena: &MemoryArena,
        buffer_lender: &'a mut BufferLender,
    ) -> RecordedDocs<'a> {
        let buffer = buffer_lender.lend_u8();
        self.stack.read_to_end(arena, buffer);
        RecordedDocs {
            reader: VInt32Reader::new(buffer),
            previous_doc: 0,
            encoding: RecordedEncoding::DocIds,
        }
    }

    fn term_doc_freq(&self) -> Option<u32> {
        None
    }

    fn has_term_freq(&self) -> bool {
        false
    }
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

    fn recorded_docs<'a>(
        &self,
        arena: &MemoryArena,
        buffer_lender: &'a mut BufferLender,
    ) -> RecordedDocs<'a> {
        let buffer = buffer_lender.lend_u8();
        self.stack.read_to_end(arena, buffer);
        RecordedDocs {
            reader: VInt32Reader::new(buffer),
            previous_doc: 0,
            encoding: RecordedEncoding::Frequencies(self.current_tf),
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

    fn recorded_docs<'a>(
        &self,
        arena: &MemoryArena,
        buffer_lender: &'a mut BufferLender,
    ) -> RecordedDocs<'a> {
        let (buffer_u8, buffer_positions) = buffer_lender.lend_all();
        self.stack.read_to_end(arena, buffer_u8);
        RecordedDocs {
            reader: VInt32Reader::new(buffer_u8),
            previous_doc: 0,
            encoding: RecordedEncoding::Positions(buffer_positions),
        }
    }

    fn term_doc_freq(&self) -> Option<u32> {
        Some(self.term_doc_freq)
    }
}

pub(crate) struct RecordedDocs<'a> {
    reader: VInt32Reader<'a>,
    previous_doc: DocId,
    encoding: RecordedEncoding<'a>,
}

enum RecordedEncoding<'a> {
    DocIds,
    Frequencies(u32),
    Positions(&'a mut Vec<u32>),
}

impl RecordedDocs<'_> {
    pub fn next_doc(&mut self) -> Option<(DocId, u32, &[u32])> {
        self.previous_doc += self.reader.next()?;
        let (frequency, positions) = match &mut self.encoding {
            RecordedEncoding::DocIds => (0, &[][..]),
            RecordedEncoding::Frequencies(last) => (self.reader.next().unwrap_or(*last), &[][..]),
            RecordedEncoding::Positions(positions) => {
                positions.clear();
                let mut previous = 1;
                while let Some(position) = self.reader.next() {
                    if position == POSITION_END {
                        break;
                    }
                    positions.push(position - previous);
                    previous = position;
                }
                (positions.len() as u32, positions.as_slice())
            }
        };
        Some((self.previous_doc, frequency, positions))
    }
}

#[cfg(test)]
mod tests {

    use common::write_u32_vint;

    use super::{
        BufferLender, DocIdRecorder, Recorder, TermFrequencyRecorder, TfAndPositionRecorder,
        VInt32Reader,
    };

    #[test]
    fn recorded_docs_preserve_frequencies_positions_and_open_last_document() {
        assert_eq!(
            recorded::<DocIdRecorder>(),
            vec![(0, 0, vec![]), (3, 0, vec![]), (127, 0, vec![])]
        );
        assert_eq!(
            recorded::<TermFrequencyRecorder>(),
            vec![(0, 2, vec![]), (3, 1, vec![]), (127, 3, vec![])]
        );
        assert_eq!(
            recorded::<TfAndPositionRecorder>(),
            vec![(0, 2, vec![0, 2]), (3, 1, vec![1]), (127, 3, vec![0, 7, 8])]
        );
    }

    fn recorded<R: Recorder>() -> Vec<(u32, u32, Vec<u32>)> {
        let mut arena = stacker::MemoryArena::default();
        let mut recorder = R::default();
        for (doc, positions) in [(0, &[0, 2][..]), (3, &[1][..]), (127, &[0, 7, 15][..])] {
            if doc != 0 {
                recorder.close_doc(&mut arena);
            }
            recorder.new_doc(doc, &mut arena);
            for position in positions {
                recorder.record_position(*position, &mut arena);
            }
        }
        let mut buffers = BufferLender::default();
        let mut docs = recorder.recorded_docs(&arena, &mut buffers);
        let mut result = Vec::new();
        while let Some((doc, freq, positions)) = docs.next_doc() {
            result.push((doc, freq, positions.to_vec()));
        }
        assert!(docs.next_doc().is_none());
        result
    }

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
