use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;

use super::{AsyncTermStreamer, TermOrdinal};
use crate::postings::TermInfo;

/// Suspendible union of sorted dictionary streams. Each input retains only its
/// current key and traversal state, not a materialized list of terms.
pub struct AsyncTermMerger<'a> {
    streams: Vec<AsyncTermStreamer<'a>>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
    state: State,
    key: Vec<u8>,
    matches: Vec<(usize, TermOrdinal, TermInfo)>,
}

enum State {
    Initialize(usize),
    StartTerm,
    Collect,
    Advance(usize),
}

impl<'a> AsyncTermMerger<'a> {
    /// Creates a merger without reading any input. Initialization suspends in
    /// `advance`, whose progress survives cancellation and I/O failures.
    pub fn new(streams: Vec<AsyncTermStreamer<'a>>) -> Self {
        Self {
            streams,
            heap: BinaryHeap::new(),
            state: State::Initialize(0),
            key: Vec::new(),
            matches: Vec::new(),
        }
    }

    /// Advances to the next unique term, sorted by byte order. Read errors and
    /// dropped futures leave this operation resumable without losing matches.
    pub async fn advance(&mut self) -> io::Result<bool> {
        loop {
            match self.state {
                State::Initialize(index) if index == self.streams.len() => {
                    self.state = State::StartTerm;
                }
                State::Initialize(index) => {
                    self.advance_stream(index).await?;
                    self.state = State::Initialize(index + 1);
                }
                State::StartTerm => {
                    let Some(Reverse((key, _))) = self.heap.peek() else {
                        return Ok(false);
                    };
                    self.key.clone_from(key);
                    self.matches.clear();
                    self.state = State::Collect;
                }
                State::Collect => {
                    if self
                        .heap
                        .peek()
                        .is_some_and(|Reverse((key, _))| key == &self.key)
                    {
                        let Reverse((_, index)) = self.heap.pop().unwrap();
                        let stream = &self.streams[index];
                        self.matches
                            .push((index, stream.term_ord(), stream.value().clone()));
                        // The heap entry is already consumed. Remember which
                        // stream to advance before any read can suspend.
                        self.state = State::Advance(index);
                    } else {
                        self.matches.sort_unstable_by_key(|entry| entry.0);
                        self.state = State::StartTerm;
                        return Ok(true);
                    }
                }
                State::Advance(index) => {
                    self.advance_stream(index).await?;
                    self.state = State::Collect;
                }
            }
        }
    }

    async fn advance_stream(&mut self, index: usize) -> io::Result<()> {
        let stream = &mut self.streams[index];
        if stream.advance().await? {
            self.heap.push(Reverse((stream.key().to_vec(), index)));
        }
        Ok(())
    }

    /// Current key, valid after `advance` returned true.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Input segment and term ordinals, sorted by segment ordinal.
    pub fn matching_segments(&self) -> impl Iterator<Item = (usize, TermOrdinal)> + '_ {
        self.matches
            .iter()
            .map(|&(index, ordinal, _)| (index, ordinal))
    }

    /// Input term information, captured before advancing each input stream.
    /// Calling this never rereads a dictionary by ordinal.
    pub fn current_segment_ords_and_term_infos(
        &self,
    ) -> impl Iterator<Item = (usize, TermInfo)> + '_ {
        self.matches
            .iter()
            .map(|(index, _, info)| (*index, info.clone()))
    }
}
