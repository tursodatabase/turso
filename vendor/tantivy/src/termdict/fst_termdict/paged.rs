use std::io;
use std::ops::Range;

use common::{BinarySerializable, HasLen};
use tantivy_fst::asynchronous::{Fst, RangeReader, Stream};
use tantivy_fst::Automaton;

use super::term_info_store::PagedTermInfoStore;
use super::termdict::FST_VERSION;
use crate::directory::{FileSlice, OwnedBytes};
use crate::postings::TermInfo;
use crate::termdict::DictionaryType;

/// A format-compatible FST dictionary read through injected asynchronous I/O.
/// Neither the FST nor the term-info arrays are retained in memory. Reads are
/// bounded by one FST node (4,619 bytes) or one 256-term information block.
#[derive(Clone)]
pub struct PagedTermDictionary {
    fst: Fst<FstFile>,
    infos: PagedTermInfoStore,
}

impl PagedTermDictionary {
    /// Opens an FST term dictionary, including its outer type discriminator.
    pub async fn open(file: FileSlice) -> io::Result<Self> {
        if file.len() < 16 {
            return Err(invalid());
        }
        let footer_start = file.len() - 16;
        let mut footer = file.slice(footer_start..).read_bytes_async().await?;
        let info_len = usize::try_from(u64::deserialize(&mut footer)?).map_err(|_| invalid())?;
        let version = u32::deserialize(&mut footer)?;
        let kind = u32::deserialize(&mut footer)?;
        if version != FST_VERSION || kind != DictionaryType::Fst as u32 || info_len > footer_start {
            return Err(invalid());
        }
        drop(footer);
        let info_start = footer_start - info_len;
        let fst = Fst::open(FstFile(file.slice(..info_start))).await?;
        let infos = PagedTermInfoStore::open(file.slice(info_start..footer_start)).await?;
        if fst.len() != infos.num_terms() {
            return Err(invalid());
        }
        Ok(Self { fst, infos })
    }

    /// Number of terms, without I/O.
    pub fn num_terms(&self) -> usize {
        self.fst.len()
    }

    /// Looks up a term without materializing the dictionary.
    pub async fn get(&self, key: &[u8]) -> io::Result<Option<TermInfo>> {
        match self.fst.get(key).await? {
            Some(ordinal) => self.infos.get(ordinal).await.map(Some),
            None => Ok(None),
        }
    }

    /// Looks up the information for a lexicographic term ordinal.
    pub async fn term_info_from_ord(&self, ordinal: u64) -> io::Result<TermInfo> {
        self.infos.get(ordinal).await
    }

    /// Streams all terms without reading dictionary bytes at construction.
    pub fn stream(&self) -> PagedTermStreamer<'_> {
        self.search(tantivy_fst::automaton::AlwaysMatch)
    }

    /// Streams matching terms in byte order with suspendible advancement.
    pub fn search<A: Automaton>(&self, automaton: A) -> PagedTermStreamer<'_, A> {
        PagedTermStreamer {
            stream: self.fst.search(automaton),
            infos: &self.infos,
            pending: None,
            key: Vec::new(),
            value: TermInfo::default(),
            ordinal: 0,
        }
    }
}

/// A stream that retains its pending term across term-information read errors
/// and cancelled `next` futures. Retained keys/automaton state grow with term
/// length, not with the number of dictionary entries.
pub struct PagedTermStreamer<'a, A: Automaton = tantivy_fst::automaton::AlwaysMatch> {
    stream: Stream<'a, FstFile, A>,
    infos: &'a PagedTermInfoStore,
    pending: Option<u64>,
    key: Vec<u8>,
    value: TermInfo,
    ordinal: u64,
}

impl<A: Automaton> PagedTermStreamer<'_, A> {
    /// Advances after both the key and its term information are available.
    pub async fn next(&mut self) -> io::Result<Option<(&[u8], &TermInfo)>> {
        if self.advance().await? {
            Ok(Some((self.key(), self.value())))
        } else {
            Ok(None)
        }
    }

    /// Current key, available without I/O after successful advancement.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Current term information, available without I/O.
    pub fn value(&self) -> &TermInfo {
        &self.value
    }

    /// Current lexicographic ordinal, available without I/O.
    pub fn term_ord(&self) -> u64 {
        self.ordinal
    }

    /// Advances without losing a pending term on cancellation or read failure.
    pub async fn advance(&mut self) -> io::Result<bool> {
        let ordinal = match self.pending {
            Some(ordinal) => ordinal,
            None => {
                let Some((key, ordinal)) = self.stream.next().await? else {
                    return Ok(false);
                };
                self.key.clear();
                self.key.extend_from_slice(key);
                self.pending = Some(ordinal);
                ordinal
            }
        };
        self.value = self.infos.get(ordinal).await?;
        self.ordinal = ordinal;
        self.pending = None;
        Ok(true)
    }
}

#[derive(Clone)]
struct FstFile(FileSlice);

impl RangeReader for FstFile {
    type Bytes = OwnedBytes;

    fn len(&self) -> usize {
        self.0.len()
    }

    async fn read(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.0.slice(range).read_bytes_async().await
    }
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "Invalid paged FST dictionary")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    use crate::directory::ReadQueue;
    use crate::termdict::{
        AsyncTermMerger, AsyncTermStreamer, TermDictionary, TermDictionaryBuilder,
    };
    use tantivy_fst::automaton::AlwaysMatch;

    #[test]
    fn paged_dictionary_matches_resident_with_delayed_reads() {
        let bytes = dictionary_bytes(1_025);
        let resident = TermDictionary::open(FileSlice::from(bytes.clone())).unwrap();
        let queue = ReadQueue::default();
        let file = FileSlice::new(queue.file("dict".into(), bytes.len()));
        let paged = drive(PagedTermDictionary::open(file), &queue, &bytes).unwrap();
        assert_eq!(paged.num_terms(), 1_025);
        for i in 0..1_025u64 {
            let key = format!("key-{i:08}");
            let expected = resident.get(&key).unwrap().unwrap();
            assert_eq!(
                drive(paged.get(key.as_bytes()), &queue, &bytes).unwrap(),
                Some(expected.clone())
            );
            assert_eq!(
                drive(paged.term_info_from_ord(i), &queue, &bytes).unwrap(),
                expected
            );
        }
        assert_eq!(drive(paged.get(b"missing"), &queue, &bytes).unwrap(), None);
        assert!(drive(paged.term_info_from_ord(1_025), &queue, &bytes).is_err());
        let mut stream = paged.search(AlwaysMatch);
        let mut expected = resident.stream().unwrap();
        while let Some((key, value)) = drive(stream.next(), &queue, &bytes).unwrap() {
            let (expected_key, expected_value) = expected.next().unwrap();
            assert_eq!(key, expected_key);
            assert_eq!(value, expected_value);
        }
        assert!(expected.next().is_none());
    }

    #[test]
    fn async_merger_resumes_at_every_read_boundary() {
        let bytes = dictionary_bytes(7);
        let resident = TermDictionary::open(FileSlice::from(bytes.clone())).unwrap();
        let empty = TermDictionary::empty();
        let queue = ReadQueue::default();
        let file = FileSlice::new(queue.file("dict".into(), bytes.len()));
        let paged = drive(PagedTermDictionary::open(file), &queue, &bytes).unwrap();
        let make_merger = || {
            AsyncTermMerger::new(vec![
                AsyncTermStreamer::Paged(paged.search(AlwaysMatch)),
                AsyncTermStreamer::Resident(
                    resident.range().ge(b"key-00000003").into_stream().unwrap(),
                ),
                AsyncTermStreamer::Resident(empty.stream().unwrap()),
                AsyncTermStreamer::Paged(paged.search(AlwaysMatch)),
            ])
        };
        let reads = check_merger(make_merger(), &queue, &bytes, &resident, None);
        assert!(reads > 20);
        for read in 0..reads {
            for cancel in [false, true] {
                check_merger(
                    make_merger(),
                    &queue,
                    &bytes,
                    &resident,
                    Some((read, cancel)),
                );
            }
        }
        let mut no_inputs = AsyncTermMerger::new(Vec::new());
        assert!(!drive(no_inputs.advance(), &queue, &bytes).unwrap());
        assert!(!drive(no_inputs.advance(), &queue, &bytes).unwrap());
    }

    fn check_merger(
        mut merger: AsyncTermMerger<'_>,
        queue: &ReadQueue,
        bytes: &[u8],
        resident: &TermDictionary,
        mut fault: Option<(usize, bool)>,
    ) -> usize {
        let mut reads = 0;
        let mut ordinal = 0;
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            let result = {
                let mut next = Box::pin(merger.advance());
                assert_send(&next);
                loop {
                    if let Poll::Ready(value) = next.as_mut().poll(&mut cx) {
                        break Some(value.unwrap());
                    }
                    let request = queue.pop().unwrap();
                    assert!(next.as_mut().poll(&mut cx).is_pending());
                    assert!(next.as_mut().poll(&mut cx).is_pending());
                    assert!(queue.pop().is_none());
                    reads += 1;
                    if fault.is_some_and(|(target, _)| target == reads - 1) {
                        let (_, cancel) = fault.take().unwrap();
                        if cancel {
                            drop(next);
                            assert!(request.is_cancelled());
                        } else {
                            request.complete(Err(io::Error::other("merge read failure")));
                            let Poll::Ready(Err(error)) = next.as_mut().poll(&mut cx) else {
                                panic!("merge read failure not propagated")
                            };
                            assert_eq!(error.to_string(), "merge read failure");
                        }
                        break None;
                    }
                    let range = request.range();
                    assert!(range.len() <= 4_619);
                    request.complete(Ok(OwnedBytes::new(bytes[range].to_vec())));
                }
            };
            match result {
                None => continue,
                Some(false) => break,
                Some(true) => {
                    let key = format!("key-{ordinal:08}");
                    assert_eq!(merger.key(), key.as_bytes());
                    let segments = if ordinal < 3 {
                        vec![0, 3]
                    } else {
                        vec![0, 1, 3]
                    };
                    assert_eq!(
                        merger.matching_segments().collect::<Vec<_>>(),
                        segments
                            .iter()
                            .map(|index| (*index, ordinal))
                            .collect::<Vec<_>>()
                    );
                    let info = resident.get(&key).unwrap().unwrap();
                    assert_eq!(
                        merger
                            .current_segment_ords_and_term_infos()
                            .collect::<Vec<_>>(),
                        segments
                            .into_iter()
                            .map(|index| (index, info.clone()))
                            .collect::<Vec<_>>()
                    );
                    ordinal += 1;
                }
            }
        }
        assert_eq!(ordinal, 7);
        assert!(fault.is_none());
        assert!(queue.pop().is_none());
        assert!(!drive(merger.advance(), queue, bytes).unwrap());
        reads
    }

    fn assert_send<T: Send>(_: &T) {}

    #[test]
    fn pending_term_info_survives_failure_and_cancellation() {
        let bytes = dictionary_bytes(2);
        let footer = bytes.len() - 16;
        let info_len = u64::from_le_bytes(bytes[footer..footer + 8].try_into().unwrap()) as usize;
        let metadata_start = bytes.len() - info_len;
        let queue = ReadQueue::default();
        let file = FileSlice::new(queue.file("dict".into(), bytes.len()));
        let paged = drive(PagedTermDictionary::open(file), &queue, &bytes).unwrap();
        let mut stream = paged.search(AlwaysMatch);
        let mut cx = Context::from_waker(Waker::noop());
        // Finish dictionary traversal, then fail the separate term-info read.
        {
            let mut next = Box::pin(stream.next());
            loop {
                assert!(next.as_mut().poll(&mut cx).is_pending());
                let request = queue.pop().unwrap();
                let range = request.range();
                if range.start == metadata_start {
                    request.complete(Err(io::Error::other("term info failure")));
                    let Poll::Ready(Err(error)) = next.as_mut().poll(&mut cx) else {
                        panic!("missing error")
                    };
                    assert_eq!(error.to_string(), "term info failure");
                    break;
                }
                request.complete(Ok(OwnedBytes::new(bytes[range].to_vec())));
            }
        }
        assert_eq!(stream.pending, Some(0));
        {
            let mut next = Box::pin(stream.next());
            assert!(next.as_mut().poll(&mut cx).is_pending());
        }
        assert!(queue.pop().is_none());
        let (key, _) = drive(stream.next(), &queue, &bytes).unwrap().unwrap();
        assert_eq!(key, b"key-00000000");
        let (key, _) = drive(stream.next(), &queue, &bytes).unwrap().unwrap();
        assert_eq!(key, b"key-00000001");
        assert!(drive(stream.next(), &queue, &bytes).unwrap().is_none());
    }

    #[test]
    fn paged_dictionary_empty_and_corrupt_metadata() {
        let bytes = dictionary_bytes(0);
        let queue = ReadQueue::default();
        let file = FileSlice::new(queue.file("dict".into(), bytes.len()));
        let paged = drive(PagedTermDictionary::open(file), &queue, &bytes).unwrap();
        assert_eq!(paged.num_terms(), 0);
        assert!(drive(paged.get(b"x"), &queue, &bytes).unwrap().is_none());
        for len in 0..16 {
            assert!(drive(
                PagedTermDictionary::open(FileSlice::from(vec![0; len])),
                &queue,
                &[]
            )
            .is_err());
        }
        let mut bytes = dictionary_bytes(1);
        let footer = bytes.len() - 16;
        bytes[footer..footer + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(drive(
            PagedTermDictionary::open(FileSlice::from(bytes)),
            &queue,
            &[]
        )
        .is_err());
    }

    #[test]
    fn paged_term_info_rejects_bad_widths_and_offsets() {
        let original = dictionary_bytes(2);
        let footer = original.len() - 16;
        let info_len =
            u64::from_le_bytes(original[footer..footer + 8].try_into().unwrap()) as usize;
        let metadata_start = original.len() - info_len;
        let queue = ReadQueue::default();
        for corruption in 0..3 {
            let mut bytes = original.clone();
            match corruption {
                0 => bytes[metadata_start..metadata_start + 8]
                    .copy_from_slice(&u64::MAX.to_le_bytes()),
                1 => bytes[metadata_start + 36] = 255,
                2 => bytes[metadata_start + 12..metadata_start + 20]
                    .copy_from_slice(&u64::MAX.to_le_bytes()),
                _ => unreachable!(),
            }
            let paged = drive(
                PagedTermDictionary::open(FileSlice::from(bytes)),
                &queue,
                &[],
            )
            .unwrap();
            assert_eq!(
                drive(paged.term_info_from_ord(1), &queue, &[])
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn maximum_width_term_info_block_stays_small() {
        let mut builder = TermDictionaryBuilder::create(Vec::new()).unwrap();
        for i in 0..256u64 {
            let start = (i << 48) as usize;
            builder
                .insert(
                    format!("key-{i:08}"),
                    &TermInfo {
                        doc_freq: u32::MAX,
                        postings_range: start..start + 1,
                        positions_range: start..start + 1,
                    },
                )
                .unwrap();
        }
        let bytes = builder.finish().unwrap();
        let queue = ReadQueue::default();
        let file = FileSlice::new(queue.file("dict".into(), bytes.len()));
        let paged = drive(PagedTermDictionary::open(file), &queue, &bytes).unwrap();
        for i in [1, 254, 255] {
            let info = drive(paged.term_info_from_ord(i), &queue, &bytes).unwrap();
            assert_eq!(info.doc_freq, u32::MAX);
            assert_eq!(info.postings_range.start, (i << 48) as usize);
            assert_eq!(info.positions_range.start, (i << 48) as usize);
        }
    }

    fn dictionary_bytes(count: u64) -> Vec<u8> {
        let mut builder = TermDictionaryBuilder::create(Vec::new()).unwrap();
        for i in 0..count {
            builder
                .insert(
                    format!("key-{i:08}"),
                    &TermInfo {
                        doc_freq: i as u32,
                        postings_range: (i * i) as usize..((i + 1) * (i + 1)) as usize,
                        positions_range: (i * i * 2) as usize..((i + 1) * (i + 1) * 2) as usize,
                    },
                )
                .unwrap();
        }
        builder.finish().unwrap()
    }

    fn drive<F: Future + Send>(future: F, queue: &ReadQueue, bytes: &[u8]) -> F::Output {
        let mut future = Box::pin(future);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
            let request = queue.pop().expect("future must yield a range request");
            assert!(future.as_mut().poll(&mut cx).is_pending());
            assert!(future.as_mut().poll(&mut cx).is_pending());
            assert!(queue.pop().is_none());
            let range = request.range();
            assert!(range.len() <= 4_619);
            request.complete(Ok(OwnedBytes::new(bytes[range].to_vec())));
        }
    }
}
