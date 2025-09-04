use crate::storage::buffer_pool::ArenaBuffer;
use crate::storage::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE;
use crate::{BufferPool, CompletionError, Result};
use bitflags::bitflags;
use cfg_block::cfg_block;
use intrusive_collections::linked_list::LinkedListOps;
use intrusive_collections::{intrusive_adapter, Adapter, PointerOps};
use intrusive_collections::{LinkedList, LinkedListLink};
use std::cell::RefCell;
use std::fmt;
use std::mem::offset_of;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::{fmt::Debug, pin::Pin};

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: u64, c: Completion) -> Result<()>;
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<()>;
    fn sync(&self, c: Completion) -> Result<()>;
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        if buffers.is_empty() {
            c.complete(0);
            return Ok(());
        }
        if buffers.len() == 1 {
            return self.pwrite(pos, buffers[0].clone(), c);
        }
        // naive default implementation can be overridden on backends where it makes sense to
        let mut pos = pos;
        let outstanding = Arc::new(AtomicUsize::new(buffers.len()));
        let total_written = Arc::new(AtomicUsize::new(0));

        for buf in buffers {
            let len = buf.len();
            let child_c = {
                let c_main = c.clone();
                let outstanding = outstanding.clone();
                let total_written = total_written.clone();
                let _cloned = buf.clone();
                Completion::new_write(move |n| {
                    if let Ok(n) = n {
                        // reference buffer in callback to ensure alive for async io
                        let _buf = _cloned.clone();
                        // accumulate bytes actually reported by the backend
                        total_written.fetch_add(n as usize, Ordering::Relaxed);
                        if outstanding.fetch_sub(1, Ordering::AcqRel) == 1 {
                            // last one finished
                            c_main.complete(total_written.load(Ordering::Acquire) as i32);
                        }
                    }
                })
            };
            if let Err(e) = self.pwrite(pos, buf.clone(), child_c) {
                // best-effort: mark as abort so caller won't wait forever
                // TODO: when we have `pwrite` and other I/O methods return CompletionError
                // instead of LimboError, store the error inside
                c.abort();
                return Err(e);
            }
            pos += len as u64;
        }
        Ok(())
    }
    fn size(&self) -> Result<u64>;
    fn truncate(&self, len: u64, c: Completion) -> Result<()>;
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct OpenFlags(i32);

bitflags! {
    impl OpenFlags: i32 {
        const None = 0b00000000;
        const Create = 0b0000001;
        const ReadOnly = 0b0000010;
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self::Create
    }
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    // remove_file is used in the sync-engine
    fn remove_file(&self, path: &str) -> Result<()>;

    fn run_once(&self) -> Result<()> {
        Ok(())
    }

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        while !c.finished() {
            self.run_once()?
        }
        if let Some(Some(err)) = c.inner.result.get().copied() {
            return Err(err.into());
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
    }

    fn register_fixed_buffer(&self, _ptr: NonNull<u8>, _len: usize) -> Result<u32> {
        Err(crate::LimboError::InternalError(
            "unsupported operation".to_string(),
        ))
    }
}

pub type ReadComplete = dyn FnOnce(Result<(Arc<Buffer>, i32), CompletionError>);
pub type WriteComplete = dyn FnOnce(Result<i32, CompletionError>);
pub type SyncComplete = dyn FnOnce(Result<i32, CompletionError>);
pub type TruncateComplete = dyn FnOnce(Result<i32, CompletionError>);

pub enum Operation {
    Read {
        file: Arc<dyn File>,
        offset: u64,
    },
    Write {
        file: Arc<dyn File>,
        buffer: Arc<Buffer>,
        offset: u64,
    },
    WriteV {
        file: Arc<dyn File>,
        buffers: Vec<Arc<Buffer>>,
        offset: u64,
    },
    Sync {
        file: Arc<dyn File>,
    },
    Truncate {
        file: Arc<dyn File>,
        len: u64,
    },
    Ready,
}

impl Debug for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { offset, .. } => f.debug_struct("Read").field("offset", offset).finish(),
            Self::Write { buffer, offset, .. } => f
                .debug_struct("Write")
                .field("buffer", buffer)
                .field("offset", offset)
                .finish(),
            Self::WriteV {
                buffers, offset, ..
            } => f
                .debug_struct("WriteV")
                .field("buffers", buffers)
                .field("offset", offset)
                .finish(),
            Self::Sync { .. } => f.debug_struct("Sync").finish(),
            Self::Truncate { len, .. } => f.debug_struct("Truncate").field("len", len).finish(),
            Self::Ready => write!(f, "Ready"),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct IOBuilder;

impl IOBuilder {
    pub fn pread(file: Arc<dyn File>, pos: u64, completion: Completion) -> CompletionBuilder {
        let mut builder = CompletionBuilder::default();
        let op = Operation::Read { file, offset: pos };
        builder.add(op, completion);
        builder
    }

    pub fn pwrite(
        file: Arc<dyn File>,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> CompletionBuilder {
        let mut builder = CompletionBuilder::default();
        let op = Operation::Write {
            file,
            buffer,
            offset: pos,
        };
        builder.add(op, completion);
        builder
    }

    pub fn pwritev(
        file: Arc<dyn File>,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> CompletionBuilder {
        let mut builder = CompletionBuilder::default();
        let op = Operation::WriteV {
            file,
            buffers,
            offset: pos,
        };
        builder.add(op, completion);
        builder
    }

    pub fn sync(file: Arc<dyn File>, completion: Completion) -> CompletionBuilder {
        let mut builder = CompletionBuilder::default();
        let op = Operation::Sync { file };
        builder.add(op, completion);
        builder
    }

    pub fn truncate(file: Arc<dyn File>, len: u64, completion: Completion) -> CompletionBuilder {
        let mut builder = CompletionBuilder::default();
        let op = Operation::Truncate { file, len };
        builder.add(op, completion);
        builder
    }

    /// Creates a completion that is always ready
    pub fn ready() -> CompletionBuilder {
        let mut builder = CompletionBuilder::default();
        let operation = Operation::Ready;
        let completion = Completion::new_ready();
        builder.add(operation, completion);
        builder
    }
}

#[must_use]
#[derive(Debug, Default)]
pub struct CompletionBuilder {
    op_list: LinkedList<CompletionBuilderAdapter>,
    completion_list: LinkedList<CompletionChainAdapter>,
}

impl CompletionBuilder {
    pub fn is_empty(&self) -> bool {
        self.completion_list.is_empty()
    }

    /// Asserts that the CompletionBuilder contains only a Single Completion and returns it
    pub fn get_single(&self) -> Completion {
        let cursor = self.completion_list.front();
        let c = cursor
            .clone_pointer()
            .expect("Completion Builder should contain 1 Completion");
        assert!(
            cursor.peek_next().is_null(),
            "Completion Builder should contain only 1 Completion",
        );
        c
    }

    pub fn add(&mut self, operation: Operation, completion: Completion) {
        self.op_list.push_back(Box::new(CompletionNodeBuilder {
            link: LinkedListLink::new(),
            operation,
        }));
        self.completion_list.push_back(completion);
    }

    pub fn append(&mut self, list: Self) {
        self.op_list.back_mut().splice_after(list.op_list);
        self.completion_list
            .back_mut()
            .splice_after(list.completion_list);
    }

    pub fn build(self) -> Result<CompletionChain> {
        {
            let front = self.op_list.front().get();
            assert!(
                front.is_some(),
                "CompletionBuilder must have at least one completion in it"
            );
        }

        let mut cursor = self.completion_list.front();
        // Completion list should have the same size of op_list
        for node in self.op_list {
            let c = cursor.clone_pointer().unwrap();
            match node.operation {
                Operation::Read { file, offset } => {
                    file.pread(offset, c)?;
                }
                Operation::Write {
                    file,
                    buffer,
                    offset,
                } => {
                    file.pwrite(offset, buffer, c)?;
                }
                Operation::WriteV {
                    file,
                    buffers,
                    offset,
                } => {
                    file.pwritev(offset, buffers, c)?;
                }
                Operation::Sync { file } => {
                    file.sync(c)?;
                }
                Operation::Truncate { file, len } => {
                    file.truncate(len, c)?;
                }
                Operation::Ready => {}
            };
            cursor.move_next();
        }
        assert!(cursor.is_null());
        Ok(CompletionChain {
            list: self.completion_list,
        })
    }

    /// Builds the CompletionBuilder and then waits for the CompletionChain to finish
    pub fn wait<I: IO + ?Sized>(self, io: &I) -> Result<()> {
        let chain = self.build()?;
        chain.wait(io)
    }
}

intrusive_adapter!(CompletionBuilderAdapter = Box<CompletionNodeBuilder>: CompletionNodeBuilder { link: LinkedListLink });

#[must_use]
#[derive(Debug)]
/// Completion Node that is stored in the Completion Builder intrusive linked list
pub struct CompletionNodeBuilder {
    link: LinkedListLink,
    operation: Operation,
}

#[must_use]
#[derive(Debug)]
/// Completion Node that is stored in the Completion Chain intrusive linked list
pub struct CompletionNodeChain {
    link: LinkedListLink,
    inner: CompletionInner,
}

impl Deref for CompletionNodeChain {
    type Target = CompletionInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[must_use]
#[derive(Debug, Clone)]
pub struct Completion {
    inner: Arc<CompletionNodeChain>,
}

impl Deref for Completion {
    type Target = CompletionInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Completion {
    pub fn new(completion_type: CompletionType) -> Self {
        Completion {
            inner: Arc::new(CompletionNodeChain {
                link: LinkedListLink::new(),
                inner: CompletionInner::new(completion_type),
            }),
        }
    }

    pub fn new_linked(completion_type: CompletionType) -> Self {
        Completion {
            inner: Arc::new(CompletionNodeChain {
                link: LinkedListLink::new(),
                inner: CompletionInner {
                    completion_type,
                    result: OnceLock::new(),
                    needs_link: true,
                },
            }),
        }
    }

    pub fn needs_link(&self) -> bool {
        self.inner.needs_link
    }

    pub fn new_write_linked<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
    {
        Self::new_linked(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_read<F>(buf: Arc<Buffer>, complete: F) -> Self
    where
        F: FnOnce(Result<(Arc<Buffer>, i32), CompletionError>) + 'static,
    {
        Self::new(CompletionType::Read(ReadCompletion::new(
            buf,
            Box::new(complete),
        )))
    }

    pub fn new_write<F>(complete: F) -> Self
    where
        F: FnOnce(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_writev<F>(complete: F) -> Self
    where
        F: FnOnce(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::WriteV(WriteVCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_sync<F>(complete: F) -> Self
    where
        F: FnOnce(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::Sync(SyncCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_trunc<F>(complete: F) -> Self
    where
        F: FnOnce(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::Truncate(TruncateCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_ready() -> Self {
        Self::new(CompletionType::Ready)
    }

    /// # Safety
    /// This operation is only safe when the list this Completion belongs to is Immutable
    /// because with that we know we cannot be traversing a list that is being appended to
    pub fn error(&self, err: CompletionError) {
        self.inner.error(err);

        let adapter = CompletionChainAdapter::new();

        unsafe {
            let mut current = adapter.link_ops.next(NonNull::from(&self.inner.link));
            // Iterate forwards
            while let Some(x) = current {
                let val = &*adapter.get_value(x);
                val.error(err);
                current = adapter.link_ops.next(x);
            }

            let mut current = adapter.link_ops.next(NonNull::from(&self.inner.link));
            // Iterate backwards
            while let Some(x) = current {
                let val = &*adapter.get_value(x);
                val.error(err);
                current = adapter.link_ops.prev(x);
            }
        }
    }

    pub fn abort(&self) {
        self.error(CompletionError::Aborted)
    }
}

// Adapter Code acquired by modifying the `instrusive_adapter` macro to use Completion instead of `Arc<CompletionNodeChain>`
#[derive(Debug, Copy, Clone, Default)]
struct CompletionPointerOps;

unsafe impl PointerOps for CompletionPointerOps {
    type Value = CompletionNodeChain;

    type Pointer = Completion;

    unsafe fn from_raw(&self, value: *const Self::Value) -> Self::Pointer {
        Completion {
            inner: Arc::from_raw(value),
        }
    }

    fn into_raw(&self, ptr: Self::Pointer) -> *const Self::Value {
        Arc::into_raw(ptr.inner)
    }
}

#[allow(explicit_outlives_requirements)]
struct CompletionChainAdapter {
    link_ops: <LinkedListLink as intrusive_collections::DefaultLinkOps>::Ops,
    pointer_ops: CompletionPointerOps,
}
unsafe impl Send for CompletionChainAdapter {}

unsafe impl Sync for CompletionChainAdapter {}

impl Copy for CompletionChainAdapter {}

impl Clone for CompletionChainAdapter {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl Default for CompletionChainAdapter {
    #[inline]
    fn default() -> Self {
        Self::NEW
    }
}

#[allow(dead_code)]
impl CompletionChainAdapter {
    pub const NEW: Self = CompletionChainAdapter {
        link_ops: <LinkedListLink as intrusive_collections::DefaultLinkOps>::NEW,
        pointer_ops: CompletionPointerOps,
    };
    #[inline]
    pub fn new() -> Self {
        Self::NEW
    }
}

#[allow(dead_code, unsafe_code)]
unsafe impl intrusive_collections::Adapter for CompletionChainAdapter {
    type LinkOps = <LinkedListLink as intrusive_collections::DefaultLinkOps>::Ops;
    type PointerOps = CompletionPointerOps;
    #[inline]
    unsafe fn get_value(
        &self,
        link: <Self::LinkOps as intrusive_collections::LinkOps>::LinkPtr,
    ) -> *const <Self::PointerOps as intrusive_collections::PointerOps>::Value {
        #[allow(clippy::cast_ptr_alignment)]
        {
            ((link.as_ptr()) as *const _ as *const u8).sub({
                {
                    offset_of!(CompletionNodeChain, link)
                }
            }) as *const CompletionNodeChain
        }
    }
    #[inline]
    unsafe fn get_link(
        &self,
        value: *const <Self::PointerOps as intrusive_collections::PointerOps>::Value,
    ) -> <Self::LinkOps as intrusive_collections::LinkOps>::LinkPtr {
        let ptr = (value as *const u8).add({
            {
                offset_of!(CompletionNodeChain, link)
            }
        });
        core::ptr::NonNull::new_unchecked(ptr as *mut _)
    }
    #[inline]
    fn link_ops(&self) -> &Self::LinkOps {
        &self.link_ops
    }
    #[inline]
    fn link_ops_mut(&mut self) -> &mut Self::LinkOps {
        &mut self.link_ops
    }
    #[inline]
    fn pointer_ops(&self) -> &Self::PointerOps {
        &self.pointer_ops
    }
}

#[derive(Debug, Default)]
/// Completion Chain that is used to track IO
pub struct CompletionChain {
    list: LinkedList<CompletionChainAdapter>,
}

impl CompletionChain {
    /// Checks if the entire Completion chain is completed or errored
    pub fn finished(&self) -> bool {
        self.list.back().get().unwrap().inner.finished()
    }

    pub fn is_completed(&self) -> bool {
        self.list.back().get().unwrap().inner.is_completed()
    }

    pub fn has_error(&self) -> bool {
        self.list.back().get().unwrap().inner.has_error()
    }

    pub fn get_error(&self) -> Option<CompletionError> {
        self.list.back().get().unwrap().inner.get_error()
    }

    pub fn error(&self, err: CompletionError) {
        let mut cursor = self.list.front();
        // Cancel all remaining completions
        while let Some(node) = cursor.get() {
            node.inner.error(err);
            cursor.move_next();
        }
    }

    pub fn abort(&self) {
        self.error(CompletionError::Aborted);
    }

    pub fn wait<I: IO + ?Sized>(&self, io: &I) -> Result<()> {
        let mut cursor = self.list.front();
        while let Some(c) = cursor.clone_pointer() {
            io.wait_for_completion(c)?;
            cursor.move_next();
        }
        Ok(())
    }
}

/// Abort completions if dropped
impl Drop for CompletionChain {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Debug)]
pub struct CompletionInner {
    completion_type: CompletionType,
    /// None means we completed successfully
    // Thread safe with OnceLock
    result: std::sync::OnceLock<Option<CompletionError>>,
    needs_link: bool,
}

impl Debug for CompletionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(..) => f.debug_tuple("Read").finish(),
            Self::Write(..) => f.debug_tuple("Write").finish(),
            Self::WriteV(..) => f.debug_tuple("WriteV").finish(),
            Self::Sync(..) => f.debug_tuple("Sync").finish(),
            Self::Truncate(..) => f.debug_tuple("Truncate").finish(),
            Self::Ready => f.debug_tuple("Ready").finish(),
        }
    }
}

impl CompletionInner {
    pub fn new(completion_type: CompletionType) -> Self {
        CompletionInner {
            completion_type,
            result: OnceLock::new(),
            needs_link: false,
        }
    }

    pub fn is_completed(&self) -> bool {
        if matches!(self.completion_type, CompletionType::Ready) {
            return true;
        }
        self.result.get().is_some_and(|val| val.is_none())
    }

    pub fn has_error(&self) -> bool {
        if matches!(self.completion_type, CompletionType::Ready) {
            return false;
        }
        self.result.get().is_some_and(|val| val.is_some())
    }

    pub fn get_error(&self) -> Option<CompletionError> {
        if matches!(self.completion_type, CompletionType::Ready) {
            return None;
        }
        self.result.get().and_then(|res| *res)
    }

    /// Checks if the Completion completed or errored
    pub fn finished(&self) -> bool {
        if matches!(self.completion_type, CompletionType::Ready) {
            return true;
        }
        self.result.get().is_some()
    }

    pub fn complete(&self, result: i32) {
        let result = Ok(result);
        match &self.completion_type {
            CompletionType::Read(r) => r.callback(result),
            CompletionType::Write(w) => w.callback(result),
            CompletionType::WriteV(w) => w.callback(result),
            CompletionType::Sync(s) => s.callback(result), // fix
            CompletionType::Truncate(t) => t.callback(result),
            CompletionType::Ready => {}
        };
        self.result.set(None).expect("result must be set only once");
    }

    pub fn error(&self, err: CompletionError) {
        let result = Err(err);
        match &self.completion_type {
            CompletionType::Read(r) => r.callback(result),
            CompletionType::Write(w) => w.callback(result),
            CompletionType::WriteV(w) => w.callback(result),
            CompletionType::Sync(s) => s.callback(result), // fix
            CompletionType::Truncate(t) => t.callback(result),
            CompletionType::Ready => {}
        };
        self.result
            .set(Some(err))
            .expect("result must be set only once");
    }

    pub fn abort(&self) {
        self.error(CompletionError::Aborted);
    }

    /// only call this method if you are sure that the completion is
    /// a ReadCompletion, panics otherwise
    pub fn as_read(&self) -> &ReadCompletion {
        match self.completion_type {
            CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        }
    }

    /// only call this method if you are sure that the completion is
    /// a WriteCompletion, panics otherwise
    pub fn as_write(&self) -> &WriteCompletion {
        match self.completion_type {
            CompletionType::Write(ref w) => w,
            _ => unreachable!(),
        }
    }
}

pub enum CompletionType {
    Read(ReadCompletion),
    Write(WriteCompletion),
    WriteV(WriteVCompletion),
    Sync(SyncCompletion),
    Truncate(TruncateCompletion),
    /// Completion that is always ready
    Ready,
}

pub struct ReadCompletion {
    pub buf: Arc<Buffer>,
    pub complete: Option<Box<ReadComplete>>,
    called: AtomicBool,
}

impl ReadCompletion {
    pub fn new(buf: Arc<Buffer>, complete: Box<ReadComplete>) -> Self {
        Self {
            buf,
            complete: Some(complete),
            called: AtomicBool::new(false),
        }
    }

    pub fn buf(&self) -> &Buffer {
        &self.buf
    }

    pub fn callback(&self, bytes_read: Result<i32, CompletionError>) {
        assert!(
            !self.called.swap(true, Ordering::SeqCst),
            "Completion callback called more than once"
        );
        let complete = unsafe {
            let ptr = &self.complete as *const Option<Box<ReadComplete>>
                as *mut Option<Box<ReadComplete>>;
            (*ptr).take().unwrap()
        };
        (complete)(bytes_read.map(|b| (self.buf.clone(), b)));
    }

    pub fn buf_arc(&self) -> Arc<Buffer> {
        self.buf.clone()
    }
}

pub struct WriteCompletion {
    pub complete: Option<Box<WriteComplete>>,
    called: AtomicBool,
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self {
            complete: Some(complete),
            called: AtomicBool::new(false),
        }
    }

    pub fn callback(&self, bytes_written: Result<i32, CompletionError>) {
        assert!(
            !self.called.swap(true, Ordering::SeqCst),
            "Completion callback called more than once"
        );
        let complete = unsafe {
            let ptr = &self.complete as *const Option<Box<WriteComplete>>
                as *mut Option<Box<WriteComplete>>;
            (*ptr).take().unwrap()
        };
        (complete)(bytes_written);
    }
}

pub struct WriteVCompletion {
    pub complete: Option<Box<WriteComplete>>,
    called: AtomicBool,
}

impl WriteVCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self {
            complete: Some(complete),
            called: AtomicBool::new(false),
        }
    }

    pub fn callback(&self, bytes_written: Result<i32, CompletionError>) {
        assert!(
            !self.called.swap(true, Ordering::SeqCst),
            "Completion callback called more than once"
        );
        let complete = unsafe {
            let ptr = &self.complete as *const Option<Box<WriteComplete>>
                as *mut Option<Box<WriteComplete>>;
            (*ptr).take().unwrap()
        };
        (complete)(bytes_written);
    }
}

pub struct SyncCompletion {
    pub complete: Option<Box<SyncComplete>>,
    called: AtomicBool,
}

impl SyncCompletion {
    pub fn new(complete: Box<SyncComplete>) -> Self {
        Self {
            complete: Some(complete),
            called: AtomicBool::new(false),
        }
    }

    pub fn callback(&self, res: Result<i32, CompletionError>) {
        assert!(
            !self.called.swap(true, Ordering::SeqCst),
            "Completion callback called more than once"
        );
        let complete = unsafe {
            let ptr = &self.complete as *const Option<Box<SyncComplete>>
                as *mut Option<Box<SyncComplete>>;
            (*ptr).take().unwrap()
        };
        (complete)(res);
    }
}

pub struct TruncateCompletion {
    pub complete: Option<Box<TruncateComplete>>,
    called: AtomicBool,
}

impl TruncateCompletion {
    pub fn new(complete: Box<TruncateComplete>) -> Self {
        Self {
            complete: Some(complete),
            called: AtomicBool::new(false),
        }
    }

    pub fn callback(&self, res: Result<i32, CompletionError>) {
        assert!(
            !self.called.swap(true, Ordering::SeqCst),
            "Completion callback called more than once"
        );
        let complete = unsafe {
            let ptr = &self.complete as *const Option<Box<TruncateComplete>>
                as *mut Option<Box<TruncateComplete>>;
            (*ptr).take().unwrap()
        };
        (complete)(res);
    }
}

pub type BufferData = Pin<Box<[u8]>>;

pub enum Buffer {
    Heap(BufferData),
    Pooled(ArenaBuffer),
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pooled(p) => write!(f, "Pooled(len={})", p.logical_len()),
            Self::Heap(buf) => write!(f, "{buf:?}: {}", buf.len()),
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let len = self.len();
        if let Self::Heap(buf) = self {
            TEMP_BUFFER_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                // take ownership of the buffer by swapping it with a dummy
                let buffer = std::mem::replace(buf, Pin::new(vec![].into_boxed_slice()));
                cache.return_buffer(buffer, len);
            });
        }
    }
}

impl Buffer {
    pub fn new(data: Vec<u8>) -> Self {
        tracing::trace!("buffer::new({:?})", data);
        Self::Heap(Pin::new(data.into_boxed_slice()))
    }

    /// Returns the index of the underlying `Arena` if it was registered with
    /// io_uring. Only for use with `UringIO` backend.
    pub fn fixed_id(&self) -> Option<u32> {
        match self {
            Self::Heap { .. } => None,
            Self::Pooled(buf) => buf.fixed_id(),
        }
    }

    pub fn new_pooled(buf: ArenaBuffer) -> Self {
        Self::Pooled(buf)
    }

    pub fn new_temporary(size: usize) -> Self {
        TEMP_BUFFER_CACHE.with(|cache| {
            if let Some(buffer) = cache.borrow_mut().get_buffer(size) {
                Self::Heap(buffer)
            } else {
                Self::Heap(Pin::new(vec![0; size].into_boxed_slice()))
            }
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Heap(buf) => buf.len(),
            Self::Pooled(buf) => buf.logical_len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Heap(buf) => {
                // SAFETY: The buffer is guaranteed to be valid for the lifetime of the slice
                unsafe { std::slice::from_raw_parts(buf.as_ptr(), buf.len()) }
            }
            Self::Pooled(buf) => buf,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Heap(buf) => buf.as_ptr(),
            Self::Pooled(buf) => buf.as_ptr(),
        }
    }
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        match self {
            Self::Heap(buf) => buf.as_ptr() as *mut u8,
            Self::Pooled(buf) => buf.as_ptr() as *mut u8,
        }
    }
}

thread_local! {
    /// thread local cache to re-use temporary buffers to prevent churn when pool overflows
    pub static TEMP_BUFFER_CACHE: RefCell<TempBufferCache> = RefCell::new(TempBufferCache::new());
}

/// A cache for temporary or any additional `Buffer` allocations beyond
/// what the `BufferPool` has room for, or for use before the pool is
/// fully initialized.
pub(crate) struct TempBufferCache {
    /// The `[Database::page_size]` at the time the cache is initiated.
    page_size: usize,
    /// Cache of buffers of size `self.page_size`.
    page_buffers: Vec<BufferData>,
    /// Cache of buffers of size `self.page_size` + WAL_FRAME_HEADER_SIZE.
    wal_frame_buffers: Vec<BufferData>,
    /// Maximum number of buffers that will live in each cache.
    max_cached: usize,
}

impl TempBufferCache {
    const DEFAULT_MAX_CACHE_SIZE: usize = 256;

    fn new() -> Self {
        Self {
            page_size: BufferPool::DEFAULT_PAGE_SIZE,
            page_buffers: Vec::with_capacity(8),
            wal_frame_buffers: Vec::with_capacity(8),
            max_cached: Self::DEFAULT_MAX_CACHE_SIZE,
        }
    }

    /// If the `[Database::page_size]` is set, any temporary buffers that might
    /// exist prior need to be cleared and new `page_size` needs to be saved.
    pub fn reinit_cache(&mut self, page_size: usize) {
        self.page_buffers.clear();
        self.wal_frame_buffers.clear();
        self.page_size = page_size;
    }

    fn get_buffer(&mut self, size: usize) -> Option<BufferData> {
        match size {
            sz if sz == self.page_size => self.page_buffers.pop(),
            sz if sz == (self.page_size + WAL_FRAME_HEADER_SIZE) => self.wal_frame_buffers.pop(),
            _ => None,
        }
    }

    fn return_buffer(&mut self, buff: BufferData, len: usize) {
        let sz = self.page_size;
        let cache = match len {
            n if n.eq(&sz) => &mut self.page_buffers,
            n if n.eq(&(sz + WAL_FRAME_HEADER_SIZE)) => &mut self.wal_frame_buffers,
            _ => return,
        };
        if self.max_cached > cache.len() {
            cache.push(buff);
        }
    }
}

cfg_block! {
    #[cfg(all(target_os = "linux", feature = "io_uring"))] {
        mod io_uring;
        #[cfg(feature = "fs")]
        pub use io_uring::UringIO;
    }

    #[cfg(target_family = "unix")] {
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(not(any(target_family = "unix", target_os = "android", target_os = "ios")))] {
        mod generic;
        pub use generic::GenericIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }
}

mod memory;
#[cfg(feature = "fs")]
mod vfs;
pub use memory::MemoryIO;
pub mod clock;
mod common;
pub use clock::Clock;

#[cfg(test)]
mod tests {
    use std::array;

    use crate::{io::CompletionChain, Completion};

    #[test]
    fn test_completion_chain_error() {
        let mut chain = CompletionChain::default();
        let completions: [Completion; 10] = array::from_fn(|_| Completion::new_write(|_| {}));
        for c in completions.iter().cloned() {
            chain.list.push_back(c);
        }

        let cancel_c = completions[4].clone();
        cancel_c.abort();

        for c in completions {
            assert!(c.has_error());
        }
    }
}
