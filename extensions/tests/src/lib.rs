use lazy_static::lazy_static;
use limbo_ext::{
    register_extension, scalar, ExtResult, ResultCode, VTabCursor, VTabKind, VTabModule,
    VTabModuleDerive, Value,
};
#[cfg(not(target_family = "wasm"))]
use limbo_ext::{VfsDerive, VfsExtension, VfsFile};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

register_extension! {
    vtabs: { KVStoreVTab },
    scalars: { test_scalar },
    vfs: { TestFS },
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
        if let Some((_, ref key, ref val)) = cursor.rows.get(cursor.index.unwrap_or(0)) {
            match idx {
                0 => Ok(Value::from_text(key.clone())), // key
                1 => Ok(Value::from_text(val.clone())), // value
                _ => Err("Invalid column".into()),
            }
        } else {
            Err("cursor out of range".into())
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
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> ExtResult<i32> {
        log::debug!("reading file with testing VFS: bytes: {count} offset: {offset}");
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        self.file
            .read(&mut buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> ExtResult<i32> {
        log::debug!("writing to file with testing VFS: bytes: {count} offset: {offset}");
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        self.file
            .write(&buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn sync(&self) -> ExtResult<()> {
        log::debug!("syncing file with testing VFS");
        self.file.sync_all().map_err(|_| ResultCode::Error)
    }

    fn size(&self) -> i64 {
        self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
