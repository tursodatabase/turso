use lazy_static::lazy_static;
use limbo_ext::{
    register_extension, scalar, BufferRef, ExtResult, ResultCode, VTabCursor, VTabKind, VTabModule,
    VTabModuleDerive, Value,
};
#[cfg(not(target_family = "wasm"))]
use limbo_ext::{VfsDerive, VfsExtension, VfsFile};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::{LazyLock, Mutex};

register_extension! {
    vtabs: { KVStoreVTab },
    scalars: { test_scalar },
    vfs: { TestFS, AsyncTestVFS },
}

lazy_static! {
    static ref GLOBAL_STORE: Mutex<BTreeMap<i64, (String, String)>> = Mutex::new(BTreeMap::new());
}

#[derive(VTabModuleDerive, Default)]
pub struct KVStoreVTab;

/// the cursor holds a snapshot of (rowid, key, value) in memory.
pub struct KVStoreCursor {
    rows: Vec<(i64, String, String)>,
    index: Option<usize>,
}

impl VTabModule for KVStoreVTab {
    type VCursor = KVStoreCursor;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "kv_store";
    type Error = String;

    fn create_schema(_args: &[Value]) -> String {
        "CREATE TABLE x (key TEXT PRIMARY KEY, value TEXT);".to_string()
    }

    fn open(&self) -> Result<Self::VCursor, Self::Error> {
        Ok(KVStoreCursor {
            rows: Vec::new(),
            index: None,
        })
    }

    fn filter(cursor: &mut Self::VCursor, _args: &[Value]) -> ResultCode {
        let store = GLOBAL_STORE.lock().unwrap();
        cursor.rows = store
            .iter()
            .map(|(&rowid, (k, v))| (rowid, k.clone(), v.clone()))
            .collect();
        cursor.rows.sort_by_key(|(rowid, _, _)| *rowid);

        if cursor.rows.is_empty() {
            cursor.index = None;
            return ResultCode::EOF;
        } else {
            cursor.index = Some(0);
        }
        ResultCode::OK
    }

    fn insert(&mut self, values: &[Value]) -> Result<i64, Self::Error> {
        let key = values
            .first()
            .and_then(|v| v.to_text())
            .ok_or("Missing key")?
            .to_string();
        let val = values
            .get(1)
            .and_then(|v| v.to_text())
            .ok_or("Missing value")?
            .to_string();
        let rowid = hash_key(&key);
        {
            let mut store = GLOBAL_STORE.lock().unwrap();
            store.insert(rowid, (key, val));
        }
        Ok(rowid)
    }

    fn delete(&mut self, rowid: i64) -> Result<(), Self::Error> {
        let mut store = GLOBAL_STORE.lock().unwrap();
        store.remove(&rowid);
        Ok(())
    }

    fn update(&mut self, rowid: i64, values: &[Value]) -> Result<(), Self::Error> {
        {
            let mut store = GLOBAL_STORE.lock().unwrap();
            store.remove(&rowid);
        }
        let _ = self.insert(values)?;
        Ok(())
    }
    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.index.is_some_and(|s| s >= cursor.rows.len()) || cursor.index.is_none()
    }

    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        cursor.index = Some(cursor.index.unwrap_or(0) + 1);
        if cursor.index.is_some_and(|c| c >= cursor.rows.len()) {
            return ResultCode::EOF;
        }
        ResultCode::OK
    }

    fn column(cursor: &Self::VCursor, idx: u32) -> Result<Value, Self::Error> {
        if cursor.index.is_some_and(|c| c >= cursor.rows.len()) {
            return Err("cursor out of range".into());
        }
        let (_, ref key, ref val) = cursor.rows[cursor.index.unwrap_or(0)];
        match idx {
            0 => Ok(Value::from_text(key.clone())), // key
            1 => Ok(Value::from_text(val.clone())), // value
            _ => Err("Invalid column".into()),
        }
    }
}

fn hash_key(key: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as i64
}

impl VTabCursor for KVStoreCursor {
    type Error = String;

    fn rowid(&self) -> i64 {
        if self.index.is_some_and(|c| c < self.rows.len()) {
            self.rows[self.index.unwrap_or(0)].0
        } else {
            log::error!("rowid: -1");
            -1
        }
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        <KVStoreVTab as VTabModule>::column(self, idx)
    }

    fn eof(&self) -> bool {
        <KVStoreVTab as VTabModule>::eof(self)
    }

    fn next(&mut self) -> ResultCode {
        <KVStoreVTab as VTabModule>::next(self)
    }
}

pub struct TestFile {
    file: File,
}

#[cfg(target_family = "wasm")]
pub struct TestFS;

#[cfg(not(target_family = "wasm"))]
#[derive(VfsDerive, Default)]
pub struct TestFS;

// Test that we can have additional extension types in the same file
// and still register the vfs at comptime if linking staticly
#[scalar(name = "test_scalar")]
fn test_scalar(_args: limbo_ext::Value) -> limbo_ext::Value {
    limbo_ext::Value::from_integer(42)
}

#[cfg(not(target_family = "wasm"))]
impl VfsExtension for TestFS {
    const NAME: &'static str = "testvfs";
    type File = TestFile;
    fn open_file(&self, path: &str, flags: i32, _direct: bool) -> ExtResult<Self::File> {
        let _ = env_logger::try_init();
        log::debug!("opening file with testing VFS: {} flags: {}", path, flags);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(flags & 1 != 0)
            .open(path)
            .map_err(|_| ResultCode::Error)?;
        Ok(TestFile { file })
    }
}

#[cfg(not(target_family = "wasm"))]
impl VfsFile for TestFile {
    fn read(&mut self, mut buf: BufferRef, offset: i64, cb: Box<dyn FnOnce(i32) + Send>) {
        log::debug!("reading file with testing VFS: offset: {offset}");
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            cb(-1);
            return;
        }
        let res = self
            .file
            .read(&mut buf)
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
            .unwrap_or(-1);
        cb(res);
    }

    fn write(&mut self, buf: BufferRef, offset: i64, cb: Box<dyn FnOnce(i32) + Send>) {
        log::debug!(
            "writing to file with testing VFS: {} offset: {offset}",
            buf.len()
        );
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            cb(-1);
            return;
        }
        let res = self
            .file
            .write(&buf)
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
            .unwrap_or(-1);
        cb(res);
    }

    fn sync(&self, cb: Box<dyn FnOnce() + Send>) {
        log::debug!("syncing file with testing VFS");
        let _ = self.file.sync_all();
        cb();
    }

    fn size(&self) -> i64 {
        self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::{runtime::Runtime, task::JoinHandle, time::Duration};

lazy_static! {
    static ref RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    });
    static ref TASK_QUEUE: LazyLock<Mutex<TaskQueue>> =
        LazyLock::new(|| { Mutex::new(TaskQueue::new()) });
}

#[cfg(target_family = "wasm")]
pub struct AsyncTestVFS;

const PAGE_SIZE: usize = 4096;

// This is just essentially the MemoryIO in core/io/memory.rs
// with tokio + 'sleep's added to simulate async network IO
// to ensure that we can support something like a S3 backend
#[cfg(not(target_family = "wasm"))]
#[derive(Default, VfsDerive)]
pub struct AsyncTestVFS;

pub struct AsyncTestFile {
    storage: Arc<Mutex<BTreeMap<usize, Box<[u8; 4096]>>>>,
    size: Arc<AtomicUsize>,
}

struct TaskQueue {
    tasks: VecDeque<JoinHandle<()>>,
}

impl TaskQueue {
    fn new() -> Self {
        Self {
            tasks: VecDeque::new(),
        }
    }
}

#[cfg(not(target_family = "wasm"))]
impl VfsExtension for AsyncTestVFS {
    const NAME: &'static str = "async_testvfs";
    type File = AsyncTestFile;

    fn open_file(&self, _path: &str, _flags: i32, _direct: bool) -> ExtResult<Self::File> {
        let _ = env_logger::try_init();
        log::info!("Opening new file: async test vfs");
        Ok(AsyncTestFile {
            storage: Arc::new(Mutex::new(BTreeMap::new())),
            size: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn run_once(&self) -> ExtResult<()> {
        log::info!("run_once async test VFS");
        let mut task_queue = TASK_QUEUE.lock().unwrap();
        while let Some(task) = task_queue.tasks.pop_front() {
            let _ = RUNTIME.block_on(task);
        }
        Ok(())
    }
}

#[cfg(not(target_family = "wasm"))]
impl VfsFile for AsyncTestFile {
    fn read(&mut self, mut buf: BufferRef, offset: i64, cb: Box<dyn FnOnce(i32) + Send>) {
        let storage = Arc::clone(&self.storage);
        let offset = offset as usize;

        let task = RUNTIME.spawn(async move {
            log::debug!(
                "reading file with async testing VFS: {} offset: {offset}",
                buf.len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut storage = storage.lock().unwrap();
            let read_len = buf.len();
            {
                let read_buf = buf.as_mut_slice();
                let mut pos = offset;
                let mut remaining = read_len;
                let mut buf_offset = 0;

                while remaining > 0 {
                    let page_no = pos / PAGE_SIZE;
                    let page_offset = pos % PAGE_SIZE;
                    let bytes_to_read = remaining.min(PAGE_SIZE - page_offset);
                    let data = storage.entry(page_no).or_insert(Box::new([0; PAGE_SIZE]));
                    read_buf[buf_offset..buf_offset + bytes_to_read]
                        .copy_from_slice(&data[page_offset..page_offset + bytes_to_read]);
                    pos += bytes_to_read;
                    buf_offset += bytes_to_read;
                    remaining -= bytes_to_read;
                }
            }
            cb(read_len as i32);
        });

        TASK_QUEUE.lock().unwrap().tasks.push_back(task);
    }

    fn write(&mut self, buf: BufferRef, offset: i64, cb: Box<dyn FnOnce(i32) + Send>) {
        let storage = Arc::clone(&self.storage);
        let offset = offset as usize;
        let size = self.size.clone();
        let task = RUNTIME.spawn(async move {
            log::debug!(
                "writing to file with async testing VFS: {} offset: {offset}",
                buf.len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut storage = storage.lock().unwrap();
            let mut pos = offset;
            let mut remaining = buf.len();
            let mut buf_offset = 0;
            let data = &buf.as_slice();
            while remaining > 0 {
                let page_no = pos / PAGE_SIZE;
                let page_offset = pos % PAGE_SIZE;
                let bytes_to_write = remaining.min(PAGE_SIZE - page_offset);
                {
                    let page = storage.entry(page_no).or_insert(Box::new([0; PAGE_SIZE]));
                    page[page_offset..page_offset + bytes_to_write]
                        .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
                }
                pos += bytes_to_write;
                buf_offset += bytes_to_write;
                remaining -= bytes_to_write;
            }
            size.store(
                core::cmp::max(
                    offset + buf.len(),
                    size.load(std::sync::atomic::Ordering::Relaxed),
                ),
                std::sync::atomic::Ordering::Relaxed,
            );

            cb(buf.len() as i32);
        });

        TASK_QUEUE.lock().unwrap().tasks.push_back(task);
    }

    fn sync(&self, cb: Box<dyn FnOnce() + Send>) {
        let task = RUNTIME.spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            cb();
        });
        TASK_QUEUE.lock().unwrap().tasks.push_back(task);
    }

    fn size(&self) -> i64 {
        self.size.load(std::sync::atomic::Ordering::Relaxed) as i64
    }
}
