//! Runtime-free traversal of immutable FSTs through injected range reads.
//!
//! Only one encoded node is retained during traversal. Stream state grows with
//! key depth, not dictionary size. The reader owns scheduling, snapshot lifetime
//! and cache policy; this module does not run an executor or block on I/O.

use std::future::Future;
use std::io;
use std::ops::{Deref, Range};

use crate::raw::Node;
use crate::Automaton;

/// An immutable, snapshot-pinned file. Implementations must return exactly the
/// requested bytes, and release a cancelled request when its future is dropped.
pub trait RangeReader: Send + Sync {
    /// Retained storage for one contiguous range (not necessarily a copy).
    type Bytes: Deref<Target = [u8]> + Send;

    /// File length, available without I/O.
    fn len(&self) -> usize;

    /// Whether the file is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reads a range without performing synchronous storage I/O.
    fn read(&self, range: Range<usize>) -> impl Future<Output = io::Result<Self::Bytes>> + Send;
}

/// An FST whose bytes remain in the injected file instead of a resident slice.
#[derive(Clone)]
pub struct Fst<R> {
    reader: R,
    version: u64,
    root: usize,
    len: usize,
}

impl<R: RangeReader> Fst<R> {
    /// Opens header/footer metadata with two sixteen-byte reads.
    pub async fn open(reader: R) -> io::Result<Self> {
        let size = reader.len();
        if size < 32 {
            return Err(invalid());
        }
        let header = reader.read(0..16).await?;
        if header.len() != 16 {
            return Err(short_read());
        }
        let version = u64::from_le_bytes(header[..8].try_into().unwrap());
        if !(1..=2).contains(&version) {
            return Err(invalid());
        }
        drop(header);
        let footer = reader.read(size - 16..size).await?;
        if footer.len() != 16 {
            return Err(short_read());
        }
        let len = usize::try_from(u64::from_le_bytes(footer[..8].try_into().unwrap()))
            .map_err(|_| invalid())?;
        let root = usize::try_from(u64::from_le_bytes(footer[8..].try_into().unwrap()))
            .map_err(|_| invalid())?;
        if (root == 0 && size != 32) || (root != 0 && root.checked_add(17) != Some(size)) {
            return Err(invalid());
        }
        Ok(Self {
            reader,
            version,
            root,
            len,
        })
    }

    /// Number of keys, available without I/O.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether this FST has no keys.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Looks up one key, retaining at most one encoded node window.
    pub async fn get(&self, key: &[u8]) -> io::Result<Option<u64>> {
        let mut addr = self.root;
        let mut output = 0u64;
        for byte in key {
            let window = self.read_node(addr).await?;
            let node = window.node()?;
            let Some(index) = node.find_input(*byte) else {
                return Ok(None);
            };
            let transition = node.transition(index);
            output = output
                .checked_add(transition.out.value())
                .ok_or_else(invalid)?;
            addr = transition.addr;
        }
        let window = self.read_node(addr).await?;
        let node = window.node()?;
        if node.is_final() {
            Ok(Some(
                output
                    .checked_add(node.final_output().value())
                    .ok_or_else(invalid)?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Streams matching keys in byte order, without retaining dictionary bytes.
    pub fn search<A: Automaton>(&self, automaton: A) -> Stream<'_, R, A> {
        let state = automaton.start();
        Stream {
            fst: self,
            automaton,
            stack: vec![Frame {
                addr: self.root,
                next: 0,
                emitted: false,
                output: 0,
                state,
            }],
            key: Vec::new(),
        }
    }

    async fn read_node(&self, addr: usize) -> io::Result<Window<R::Bytes>> {
        if addr == 0 {
            return Ok(Window {
                bytes: None,
                addr: 0,
                base: 0,
                version: self.version,
            });
        }
        if !(16..self.reader.len() - 16).contains(&addr) {
            return Err(invalid());
        }
        let end = addr + 1;
        let base = end.saturating_sub(Node::MAX_ENCODED_SIZE).max(16);
        let bytes = self.reader.read(base..end).await?;
        if bytes.len() != end - base {
            return Err(short_read());
        }
        Ok(Window {
            bytes: Some(bytes),
            addr,
            base,
            version: self.version,
        })
    }
}

/// Fallible, suspendible depth-first traversal. Only addresses, accumulated
/// outputs and automaton states are kept for ancestors, not their node bytes.
pub struct Stream<'a, R, A: Automaton> {
    fst: &'a Fst<R>,
    automaton: A,
    stack: Vec<Frame<A::State>>,
    key: Vec<u8>,
}

impl<R: RangeReader, A: Automaton> Stream<'_, R, A> {
    /// Advances to the next match. Pending reads do not advance the current
    /// frame. Dropping this future (or receiving an I/O error) leaves the stream
    /// positioned before the next result, so resuming does not skip a key.
    pub async fn next(&mut self) -> io::Result<Option<(&[u8], u64)>> {
        while let Some(frame) = self.stack.last_mut() {
            let window = self.fst.read_node(frame.addr).await?;
            let node = window.node()?;
            if !frame.emitted {
                if node.is_final() && self.automaton.is_match(&frame.state) {
                    let output = frame
                        .output
                        .checked_add(node.final_output().value())
                        .ok_or_else(invalid)?;
                    frame.emitted = true;
                    return Ok(Some((&self.key, output)));
                }
                frame.emitted = true;
            }
            if frame.next == node.len() || !self.automaton.can_match(&frame.state) {
                self.stack.pop();
                self.key.pop();
                continue;
            }
            let transition = node.transition(frame.next);
            let output = frame
                .output
                .checked_add(transition.out.value())
                .ok_or_else(invalid)?;
            let state = self.automaton.accept(&frame.state, transition.inp);
            frame.next += 1;
            self.key.push(transition.inp);
            self.stack.push(Frame {
                addr: transition.addr,
                next: 0,
                emitted: false,
                output,
                state,
            });
        }
        Ok(None)
    }
}

struct Frame<S> {
    addr: usize,
    next: usize,
    emitted: bool,
    output: u64,
    state: S,
}

struct Window<B> {
    bytes: Option<B>,
    addr: usize,
    base: usize,
    version: u64,
}

impl<B: Deref<Target = [u8]>> Window<B> {
    fn node(&self) -> io::Result<Node<'_>> {
        Node::from_range(
            self.version,
            self.addr,
            self.base,
            self.bytes.as_deref().unwrap_or(&[]),
        )
    }
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "Invalid range-backed FST")
}

fn short_read() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "Short FST range read")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::future::poll_fn;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::task::{Context, Poll, Wake, Waker};

    use crate::automaton::AlwaysMatch;
    #[cfg(feature = "regex")]
    use crate::IntoStreamer;
    use crate::{MapBuilder, Streamer};

    #[test]
    fn delayed_lookup_and_stream_match_resident_fst() {
        let mut builder = MapBuilder::memory();
        builder.insert(b"", 7).unwrap();
        for i in 0..20_000u64 {
            builder.insert(format!("key-{i:08}"), i * i + 10).unwrap();
        }
        let bytes = builder.into_inner().unwrap();
        assert!(bytes.len() > Node::MAX_ENCODED_SIZE * 8);
        let resident = crate::Map::from_bytes(bytes.clone()).unwrap();
        let reader = Reader::new(bytes);
        let fst = reader.drive(Fst::open(reader.clone())).unwrap();
        assert_eq!(fst.len(), resident.len());
        for key in [
            b"".as_slice(),
            b"key-00000000",
            b"key-00010423",
            b"key-00019999",
            b"absent",
            b"key",
        ] {
            assert_eq!(reader.drive(fst.get(key)).unwrap(), resident.get(key));
        }
        let mut stream = fst.search(AlwaysMatch);
        let mut expected = resident.stream();
        while let Some((key, value)) = reader.drive(stream.next()).unwrap() {
            assert_eq!(Some((key, value)), expected.next());
        }
        assert_eq!(expected.next(), None);
        assert_eq!(reader.drive(stream.next()).unwrap(), None);
        assert!(reader
            .ranges
            .lock()
            .unwrap()
            .iter()
            .all(|range| range.len() <= Node::MAX_ENCODED_SIZE));
        assert!(reader.requests.lock().unwrap().is_empty());
        assert_eq!(reader.live.load(Ordering::Relaxed), 0);
        assert!(reader.peak.load(Ordering::Relaxed) <= Node::MAX_ENCODED_SIZE);
    }

    #[cfg(feature = "regex")]
    #[test]
    fn automaton_stream_matches_resident_fst() {
        let bytes = crate::Map::from_iter([
            ("", 10),
            ("abc", 11),
            ("abcd", 12),
            ("ax", 13),
            ("bc", 15),
            ("zz", 30),
        ])
        .unwrap()
        .as_fst()
        .to_vec();
        let resident = crate::Map::from_bytes(bytes.clone()).unwrap();
        let reader = Reader::new(bytes);
        let fst = reader.drive(Fst::open(reader.clone())).unwrap();
        let automaton = crate::Regex::new("a.*").unwrap();
        let mut expected = resident.search(&automaton).into_stream();
        let mut stream = fst.search(&automaton);
        while let Some(result) = reader.drive(stream.next()).unwrap() {
            assert_eq!(Some(result), expected.next());
        }
        assert_eq!(expected.next(), None);
    }

    #[test]
    fn stream_cancellation_and_read_errors_do_not_skip_results() {
        let bytes = crate::Map::from_iter([("abc", 2), ("abcd", 3), ("z", 5)])
            .unwrap()
            .as_fst()
            .to_vec();
        let reader = Reader::new(bytes);
        let fst = reader.drive(Fst::open(reader.clone())).unwrap();
        let mut stream = fst.search(AlwaysMatch);
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        {
            let mut next = Box::pin(stream.next());
            assert!(next.as_mut().poll(&mut cx).is_pending());
            reader.complete(None);
            assert!(next.as_mut().poll(&mut cx).is_pending());
        }
        let cancelled = reader.requests.lock().unwrap().pop_front().unwrap();
        assert!(cancelled.response.upgrade().is_none());
        {
            let mut next = Box::pin(stream.next());
            assert!(next.as_mut().poll(&mut cx).is_pending());
            reader.complete(Some(io::Error::other("injected read failure")));
            let Poll::Ready(Err(error)) = next.as_mut().poll(&mut cx) else {
                panic!("error not propagated")
            };
            assert_eq!(error.to_string(), "injected read failure");
        }
        assert_eq!(
            reader.drive(stream.next()).unwrap(),
            Some((b"abc".as_slice(), 2))
        );
        assert_eq!(
            reader.drive(stream.next()).unwrap(),
            Some((b"abcd".as_slice(), 3))
        );
        assert_eq!(
            reader.drive(stream.next()).unwrap(),
            Some((b"z".as_slice(), 5))
        );
        assert_eq!(reader.drive(stream.next()).unwrap(), None);
    }

    #[test]
    fn empty_and_empty_key_fsts() {
        for entries in [vec![], vec![("", 0)], vec![("", 19)]] {
            let bytes = crate::Map::from_iter(entries.clone())
                .unwrap()
                .as_fst()
                .to_vec();
            let reader = Reader::new(bytes);
            let fst = reader.drive(Fst::open(reader.clone())).unwrap();
            assert_eq!(fst.len(), entries.len());
            assert_eq!(
                reader.drive(fst.get(b"")).unwrap(),
                entries.first().map(|entry| entry.1)
            );
            assert_eq!(reader.drive(fst.get(b"x")).unwrap(), None);
        }
    }

    #[test]
    fn corrupt_metadata_and_short_reads_propagate() {
        let mut bytes = crate::Map::from_iter([("a", 1)]).unwrap().as_fst().to_vec();
        let end = bytes.len();
        bytes[end - 8..].copy_from_slice(&u64::MAX.to_le_bytes());
        let reader = Reader::new(bytes);
        assert_eq!(
            reader
                .drive(Fst::open(reader.clone()))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );
        let reader = Reader::new(vec![0; 31]);
        assert_eq!(
            reader
                .drive(Fst::open(reader.clone()))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(reader.ranges.lock().unwrap().is_empty());
        let reader = Reader::new(vec![0; 32]);
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        let mut open = Box::pin(Fst::open(reader.clone()));
        assert!(open.as_mut().poll(&mut cx).is_pending());
        let request = reader.requests.lock().unwrap().pop_front().unwrap();
        let response = request.response.upgrade().unwrap();
        response.lock().unwrap().result = Some(Ok(vec![0; 15]));
        let Poll::Ready(Err(error)) = open.as_mut().poll(&mut cx) else {
            panic!("short read not propagated")
        };
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[derive(Clone)]
    struct Reader {
        bytes: Arc<Vec<u8>>,
        requests: Arc<Mutex<VecDeque<Request>>>,
        ranges: Arc<Mutex<Vec<Range<usize>>>>,
        live: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    struct Request {
        range: Range<usize>,
        response: Weak<Mutex<Response>>,
    }

    #[derive(Default)]
    struct Response {
        result: Option<io::Result<Vec<u8>>>,
        waker: Option<Waker>,
    }

    impl Reader {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                bytes: Arc::new(bytes),
                requests: Arc::default(),
                ranges: Arc::default(),
                live: Arc::default(),
                peak: Arc::default(),
            }
        }

        fn drive<F: Future + Send>(&self, future: F) -> F::Output {
            let waker = Waker::from(Arc::new(Noop));
            let mut cx = Context::from_waker(&waker);
            let mut future = Box::pin(future);
            loop {
                if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                    return value;
                }
                let reads = self.ranges.lock().unwrap().len();
                assert!(future.as_mut().poll(&mut cx).is_pending());
                assert!(future.as_mut().poll(&mut cx).is_pending());
                assert_eq!(reads, self.ranges.lock().unwrap().len());
                self.complete(None);
            }
        }

        fn complete(&self, error: Option<io::Error>) {
            let request = self.requests.lock().unwrap().pop_front().unwrap();
            let response = request.response.upgrade().unwrap();
            let mut response = response.lock().unwrap();
            response.result = Some(match error {
                Some(error) => Err(error),
                None => Ok(self.bytes[request.range].to_vec()),
            });
            response.waker.take().unwrap().wake();
        }
    }

    impl RangeReader for Reader {
        type Bytes = TrackedBytes;

        fn len(&self) -> usize {
            self.bytes.len()
        }

        async fn read(&self, range: Range<usize>) -> io::Result<TrackedBytes> {
            self.ranges.lock().unwrap().push(range.clone());
            let response = Arc::new(Mutex::new(Response::default()));
            self.requests.lock().unwrap().push_back(Request {
                range,
                response: Arc::downgrade(&response),
            });
            let data = poll_fn(|cx| {
                let mut response = response.lock().unwrap();
                match response.result.take() {
                    Some(result) => Poll::Ready(result),
                    None => {
                        response.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            })
            .await?;
            let live = self.live.fetch_add(data.len(), Ordering::Relaxed) + data.len();
            self.peak.fetch_max(live, Ordering::Relaxed);
            Ok(TrackedBytes {
                data,
                live: self.live.clone(),
            })
        }
    }

    struct TrackedBytes {
        data: Vec<u8>,
        live: Arc<AtomicUsize>,
    }

    impl Deref for TrackedBytes {
        type Target = [u8];
        fn deref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for TrackedBytes {
        fn drop(&mut self) {
            self.live.fetch_sub(self.data.len(), Ordering::Relaxed);
        }
    }

    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
}
