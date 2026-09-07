use std::io::{self, Write};

use byteorder::{LittleEndian, WriteBytesExt};

use crate::error::Result;
use crate::raw::counting_writer::CountingWriter;
use crate::raw::error::Error;
use crate::raw::registry::{Registry, RegistryEntry};
use crate::raw::{CompiledAddr, FstType, Output, Transition, EMPTY_ADDRESS, NONE_ADDRESS, VERSION};
// use raw::registry_minimal::{Registry, RegistryEntry};
use crate::stream::{IntoStreamer, Streamer};

/// A builder for creating a finite state transducer.
///
/// This is not your average everyday builder. It has two important qualities
/// that make it a bit unique from what you might expect:
///
/// 1. All keys must be added in lexicographic order. Adding a key out of order
///    will result in an error. Additionally, adding a duplicate key with an
///    output value will also result in an error. That is, once a key is
///    associated with a value, that association can never be modified or
///    deleted.
/// 2. The representation of an fst is streamed to *any* `io::Write` as it is
///    built. For an in memory representation, this can be a `Vec<u8>`.
///
/// Point (2) is especially important because it means that an fst can be
/// constructed *without storing the entire fst in memory*. Namely, since it
/// works with any `io::Write`, it can be streamed directly to a file.
///
/// With that said, the builder does use memory, but **memory usage is bounded
/// to a constant size**. The amount of memory used trades off with the
/// compression ratio. Currently, the implementation hard codes this trade off
/// which can result in about 5-20MB of heap usage during construction. (N.B.
/// Guaranteeing a maximal compression ratio requires memory proportional to
/// the size of the fst, which defeats some of the benefit of streaming
/// it to disk. In practice, a small bounded amount of memory achieves
/// close-to-minimal compression ratios.)
///
/// The algorithmic complexity of fst construction is `O(n)` where `n` is the
/// number of elements added to the fst.
pub struct Builder<W> {
    /// The FST raw data is written directly to `wtr`.
    ///
    /// No internal buffering is done.
    wtr: CountingWriter<W>,
    /// The stack of unfinished nodes.
    ///
    /// An unfinished node is a node that could potentially have a new
    /// transition added to it when a new word is added to the dictionary.
    unfinished: UnfinishedNodes,
    /// A map of finished nodes.
    ///
    /// A finished node is one that has been compiled and written to `wtr`.
    /// After this point, the node is considered immutable and will never
    /// Achange.
    registry: Registry,
    /// The last word added.
    ///
    /// This is used to enforce the invariant that words are added in sorted
    /// order.
    last: Option<Vec<u8>>,
    /// The address of the last compiled node.
    ///
    /// This is used to optimize states with one transition that point
    /// to the previously compiled node. (The previously compiled node in
    /// this case actually corresponds to the next state for the transition,
    /// since states are compiled in reverse.)
    last_addr: CompiledAddr,
    /// The number of keys added.
    len: usize,
}

#[derive(Debug)]
struct UnfinishedNodes {
    stack: Vec<BuilderNodeUnfinished>,
}

#[derive(Debug)]
struct BuilderNodeUnfinished {
    node: BuilderNode,
    last: Option<LastTransition>,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct BuilderNode {
    pub is_final: bool,
    pub final_output: Output,
    pub trans: Vec<Transition>,
}

#[derive(Debug)]
struct LastTransition {
    inp: u8,
    out: Output,
}

impl Builder<Vec<u8>> {
    /// Create a builder that builds an fst in memory.
    #[inline]
    pub fn memory() -> Self {
        Builder::new(Vec::with_capacity(10 * (1 << 10))).unwrap()
    }
}

impl<W: io::Write> Builder<W> {
    /// Create a builder that builds an fst by writing it to `wtr` in a
    /// streaming fashion.
    pub fn new(wtr: W) -> Result<Builder<W>> {
        Builder::new_type(wtr, 0)
    }

    /// The same as `new`, except it sets the type of the fst to the type
    /// given.
    pub fn new_type(wtr: W, ty: FstType) -> Result<Builder<W>> {
        let mut wtr = CountingWriter::new(wtr);
        // Don't allow any nodes to have address 0-7. We use these to encode
        // the API version. We also use addresses `0` and `1` as special
        // sentinel values, so they should never correspond to a real node.
        wtr.write_u64::<LittleEndian>(VERSION)?;
        // Similarly for 8-15 for the fst type.
        wtr.write_u64::<LittleEndian>(ty)?;
        Ok(Builder {
            wtr,
            unfinished: UnfinishedNodes::new(),
            registry: Registry::new(10_000, 2),
            last: None,
            last_addr: NONE_ADDRESS,
            len: 0,
        })
    }

    /// Adds a byte string to this FST with a zero output value.
    pub fn add<B>(&mut self, bs: B) -> Result<()>
    where
        B: AsRef<[u8]>,
    {
        self.check_last_key(bs.as_ref(), false)?;
        self.insert_output(bs, None)
    }

    /// Insert a new key-value pair into the fst.
    ///
    /// Keys must be convertible to byte strings. Values must be a `u64`, which
    /// is a restriction of the current implementation of finite state
    /// transducers. (Values may one day be expanded to other types.)
    ///
    /// If a key is inserted that is less than or equal to any previous key
    /// added, then an error is returned. Similarly, if there was a problem
    /// writing to the underlying writer, an error is returned.
    pub fn insert<B>(&mut self, bs: B, val: u64) -> Result<()>
    where
        B: AsRef<[u8]>,
    {
        self.check_last_key(bs.as_ref(), true)?;
        self.insert_output(bs, Some(Output::new(val)))
    }

    /// Calls insert on each item in the iterator.
    ///
    /// If an error occurred while adding an element, processing is stopped
    /// and the error is returned.
    ///
    /// If a key is inserted that is less than or equal to any previous key
    /// added, then an error is returned. Similarly, if there was a problem
    /// writing to the underlying writer, an error is returned.
    pub fn extend_iter<T, I>(&mut self, iter: I) -> Result<()>
    where
        T: AsRef<[u8]>,
        I: IntoIterator<Item = (T, Output)>,
    {
        for (key, out) in iter {
            self.insert(key, out.value())?;
        }
        Ok(())
    }

    /// Calls insert on each item in the stream.
    ///
    /// Note that unlike `extend_iter`, this is not generic on the items in
    /// the stream.
    ///
    /// If a key is inserted that is less than or equal to any previous key
    /// added, then an error is returned. Similarly, if there was a problem
    /// writing to the underlying writer, an error is returned.
    pub fn extend_stream<'f, I, S>(&mut self, stream: I) -> Result<()>
    where
        I: for<'a> IntoStreamer<'a, Into = S, Item = (&'a [u8], Output)>,
        S: 'f + for<'a> Streamer<'a, Item = (&'a [u8], Output)>,
    {
        let mut stream = stream.into_stream();
        while let Some((key, out)) = stream.next() {
            self.insert(key, out.value())?;
        }
        Ok(())
    }

    /// Finishes the construction of the fst and flushes the underlying
    /// writer. After completion, the data written to `W` may be read using
    /// one of `Fst`'s constructor methods.
    pub fn finish(self) -> Result<()> {
        self.into_inner()?;
        Ok(())
    }

    /// Just like `finish`, except it returns the underlying writer after
    /// flushing it.
    pub fn into_inner(mut self) -> Result<W> {
        self.compile_from(0)?;
        let root_node = self.unfinished.pop_root();
        let root_addr = self.compile(&root_node)?;
        self.wtr.write_u64::<LittleEndian>(self.len as u64)?;
        self.wtr.write_u64::<LittleEndian>(root_addr as u64)?;
        self.wtr.flush()?;
        Ok(self.wtr.into_inner())
    }

    fn insert_output<B>(&mut self, bs: B, out: Option<Output>) -> Result<()>
    where
        B: AsRef<[u8]>,
    {
        let bs = bs.as_ref();
        if bs.is_empty() {
            self.len = 1; // must be first key, so length is always 1
            self.unfinished
                .set_root_output(out.unwrap_or_else(Output::zero));
            return Ok(());
        }
        let (prefix_len, out) = if let Some(out) = out {
            self.unfinished.find_common_prefix_and_set_output(bs, out)
        } else {
            (self.unfinished.find_common_prefix(bs), Output::zero())
        };
        if prefix_len == bs.len() {
            // If the prefix found consumes the entire set of bytes, then
            // the prefix *equals* the bytes given. This means it is a
            // duplicate value with no output. So we can give up here.
            //
            // If the below assert fails, then that means we let a duplicate
            // value through even when inserting outputs.
            assert!(out.is_zero());
            return Ok(());
        }
        self.len += 1;
        self.compile_from(prefix_len)?;
        self.unfinished.add_suffix(&bs[prefix_len..], out);
        Ok(())
    }

    fn compile_from(&mut self, istate: usize) -> Result<()> {
        let mut addr = NONE_ADDRESS;
        while istate + 1 < self.unfinished.len() {
            let node = if addr == NONE_ADDRESS {
                self.unfinished.pop_empty()
            } else {
                self.unfinished.pop_freeze(addr)
            };
            addr = self.compile(&node)?;
            assert_ne!(addr, NONE_ADDRESS);
        }
        self.unfinished.top_last_freeze(addr);
        Ok(())
    }

    #[inline]
    fn compile(&mut self, node: &BuilderNode) -> Result<CompiledAddr> {
        if node.is_final && node.trans.is_empty() && node.final_output.is_zero() {
            return Ok(EMPTY_ADDRESS);
        }
        let entry = self.registry.entry(&node);
        if let RegistryEntry::Found(ref addr) = entry {
            return Ok(*addr);
        }
        let start_addr = self.wtr.count() as CompiledAddr;
        node.compile_to(&mut self.wtr, self.last_addr, start_addr)?;
        self.last_addr = self.wtr.count() as CompiledAddr - 1;
        if let RegistryEntry::NotFound(cell) = entry {
            cell.insert(self.last_addr);
        }
        Ok(self.last_addr)
    }

    fn check_last_key(&mut self, bs: &[u8], check_dupe: bool) -> Result<()> {
        if let Some(ref mut last) = self.last {
            if check_dupe && bs == &**last {
                return Err(Error::DuplicateKey { got: bs.to_vec() }.into());
            }
            if bs < &**last {
                return Err(Error::OutOfOrder {
                    previous: last.to_vec(),
                    got: bs.to_vec(),
                }
                .into());
            }
            last.clear();
            for &b in bs {
                last.push(b);
            }
        } else {
            self.last = Some(bs.to_vec());
        }
        Ok(())
    }

    /// Gets a reference to the underlying writer.
    pub fn get_ref(&self) -> &W {
        self.wtr.get_ref()
    }

    /// Returns the number of bytes written to the underlying writer
    pub fn bytes_written(&self) -> u64 {
        self.wtr.count()
    }
}

impl UnfinishedNodes {
    fn new() -> UnfinishedNodes {
        let mut unfinished = UnfinishedNodes {
            stack: Vec::with_capacity(64),
        };
        unfinished.push_empty(false);
        unfinished
    }

    fn len(&self) -> usize {
        self.stack.len()
    }

    fn push_empty(&mut self, is_final: bool) {
        self.stack.push(BuilderNodeUnfinished {
            node: BuilderNode {
                is_final,
                ..BuilderNode::default()
            },
            last: None,
        });
    }

    fn pop_root(&mut self) -> BuilderNode {
        assert_eq!(self.stack.len(), 1);
        assert!(self.stack[0].last.is_none());
        self.stack.pop().unwrap().node
    }

    fn pop_freeze(&mut self, addr: CompiledAddr) -> BuilderNode {
        let mut unfinished = self.stack.pop().unwrap();
        unfinished.last_compiled(addr);
        unfinished.node
    }

    fn pop_empty(&mut self) -> BuilderNode {
        let unfinished = self.stack.pop().unwrap();
        assert!(unfinished.last.is_none());
        unfinished.node
    }

    fn set_root_output(&mut self, out: Output) {
        self.stack[0].node.is_final = true;
        self.stack[0].node.final_output = out;
    }

    fn top_last_freeze(&mut self, addr: CompiledAddr) {
        let last = self.stack.len().checked_sub(1).unwrap();
        self.stack[last].last_compiled(addr);
    }

    fn add_suffix(&mut self, bs: &[u8], out: Output) {
        if bs.is_empty() {
            return;
        }
        let last = self.stack.len().checked_sub(1).unwrap();
        assert!(self.stack[last].last.is_none());
        self.stack[last].last = Some(LastTransition { inp: bs[0], out });
        for &b in &bs[1..] {
            self.stack.push(BuilderNodeUnfinished {
                node: BuilderNode::default(),
                last: Some(LastTransition {
                    inp: b,
                    out: Output::zero(),
                }),
            });
        }
        self.push_empty(true);
    }

    fn find_common_prefix(&mut self, bs: &[u8]) -> usize {
        bs.iter()
            .zip(&self.stack)
            .take_while(|&(&b, ref node)| node.last.as_ref().map(|t| t.inp == b).unwrap_or(false))
            .count()
    }

    fn find_common_prefix_and_set_output(&mut self, bs: &[u8], mut out: Output) -> (usize, Output) {
        let mut i = 0;
        while i < bs.len() {
            let add_prefix = match self.stack[i].last.as_mut() {
                Some(ref mut t) if t.inp == bs[i] => {
                    i += 1;
                    let common_pre = t.out.prefix(out);
                    let add_prefix = t.out.sub(common_pre);
                    out = out.sub(common_pre);
                    t.out = common_pre;
                    add_prefix
                }
                _ => break,
            };
            if !add_prefix.is_zero() {
                self.stack[i].add_output_prefix(add_prefix);
            }
        }
        (i, out)
    }
}

impl BuilderNodeUnfinished {
    fn last_compiled(&mut self, addr: CompiledAddr) {
        if let Some(trans) = self.last.take() {
            self.node.trans.push(Transition {
                inp: trans.inp,
                out: trans.out,
                addr,
            });
        }
    }

    fn add_output_prefix(&mut self, prefix: Output) {
        if self.node.is_final {
            self.node.final_output = prefix.cat(self.node.final_output);
        }
        for t in &mut self.node.trans {
            t.out = prefix.cat(t.out);
        }
        if let Some(ref mut t) = self.last {
            t.out = prefix.cat(t.out);
        }
    }
}

impl Clone for BuilderNode {
    fn clone(&self) -> Self {
        BuilderNode {
            is_final: self.is_final,
            final_output: self.final_output,
            trans: self.trans.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.is_final = source.is_final;
        self.final_output = source.final_output;
        self.trans.clear();
        self.trans.extend(source.trans.iter());
    }
}

impl Default for BuilderNode {
    fn default() -> Self {
        BuilderNode {
            is_final: false,
            final_output: Output::zero(),
            trans: vec![],
        }
    }
}
