use lazy_static::lazy_static;
use limbo_ext::{
    register_extension, scalar, ConstraintInfo, ConstraintOp, ConstraintUsage, ExtResult,
    IndexInfo, OrderByInfo, ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, VTable,
    Value,
};
#[cfg(not(target_family = "wasm"))]
use limbo_ext::{VfsDerive, VfsExtension, VfsFile};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

register_extension! {
    vtabs: { KVStoreVTabModule },
    scalars: { test_scalar },
    vfs: { TestFS },
}

lazy_static! {
    static ref GLOBAL_STORE: Mutex<BTreeMap<i64, (String, String)>> = Mutex::new(BTreeMap::new());
}

#[derive(VTabModuleDerive, Default)]
pub struct KVStoreVTabModule;

/// the cursor holds a snapshot of (rowid, key, value) in memory.
pub struct KVStoreCursor {
    rows: Vec<(i64, String, String)>,
    index: Option<usize>,
}

impl VTabModule for KVStoreVTabModule {
    type Table = KVStoreTable;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "kv_store";

    fn create(_args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let schema = "CREATE TABLE x (key TEXT PRIMARY KEY, value TEXT);".to_string();
        Ok((schema, KVStoreTable {}))
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

    fn filter(&mut self, args: &[Value], idx_str: Option<(&str, i32)>) -> ResultCode {
        match idx_str {
            Some(("key_eq", 1)) => {
                let key = args
                    .first()
                    .and_then(|v| v.to_text())
                    .map(|s| s.to_string());
                log::debug!("idx_str found: key_eq\n value: {:?}", key);
                if let Some(key) = key {
                    let rowid = hash_key(&key);
                    let store = GLOBAL_STORE.lock().unwrap();
                    if let Some((k, v)) = store.get(&rowid) {
                        self.rows.push((rowid, k.clone(), v.clone()));
                        self.index = Some(0);
                    } else {
                        self.rows.clear();
                        self.index = None;
                        return ResultCode::EOF;
                    }
                    return ResultCode::OK;
                }
                self.rows.clear();
                self.index = None;
                ResultCode::OK
            }
            _ => {
                let store = GLOBAL_STORE.lock().unwrap();
                self.rows = store
                    .iter()
                    .map(|(&rowid, (k, v))| (rowid, k.clone(), v.clone()))
                    .collect();
                self.rows.sort_by_key(|(rowid, _, _)| *rowid);
                if self.rows.is_empty() {
                    self.index = None;
                    ResultCode::EOF
                } else {
                    self.index = Some(0);
                    ResultCode::OK
                }
            }
        }
    }

    fn rowid(&self) -> i64 {
        if self.index.is_some_and(|c| c < self.rows.len()) {
            self.rows[self.index.unwrap_or(0)].0
        } else {
            log::error!("rowid: -1");
            -1
        }
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        if self.index.is_some_and(|c| c >= self.rows.len()) {
            return Err("cursor out of range".into());
        }
        if let Some((_, ref key, ref val)) = self.rows.get(self.index.unwrap_or(0)) {
            match idx {
                0 => Ok(Value::from_text(key.clone())), // key
                1 => Ok(Value::from_text(val.clone())), // value
                _ => Err("Invalid column".into()),
            }
        } else {
            Err("Invalid Column".into())
        }
    }

    fn eof(&self) -> bool {
        self.index.is_some_and(|s| s >= self.rows.len()) || self.index.is_none()
    }

    fn next(&mut self) -> ResultCode {
        self.index = Some(self.index.unwrap_or(0) + 1);
        if self.index.is_some_and(|c| c >= self.rows.len()) {
            return ResultCode::EOF;
        }
        ResultCode::OK
    }
}

pub struct KVStoreTable {}

impl VTable for KVStoreTable {
    type Cursor = KVStoreCursor;
    type Error = String;

    fn open(&self) -> Result<Self::Cursor, Self::Error> {
        let _ = env_logger::try_init();
        Ok(KVStoreCursor {
            rows: Vec::new(),
            index: None,
        })
    }

    fn best_index(constraints: &[ConstraintInfo], _order_by: &[OrderByInfo]) -> IndexInfo {
        // Look for: key = ?
        for constraint in constraints.iter() {
            if constraint.usable
                && constraint.op == ConstraintOp::Eq
                && constraint.column_index == 0
            {
                // this extension wouldn't support order by but for testing purposes,
                // we will consume it if we find an ASC order by clause on the value column
                let mut consumed = false;
                if let Some(order) = _order_by.first() {
                    if order.column_index == 1 && !order.desc {
                        consumed = true;
                    }
                }
                log::debug!("xBestIndex: constraint found for 'key = ?'");
                return IndexInfo {
                    idx_num: 1,
                    idx_str: Some("key_eq".to_string()),
                    order_by_consumed: consumed,
                    estimated_cost: 10.0,
                    estimated_rows: 4,
                    constraint_usages: vec![ConstraintUsage {
                        omit: true,
                        argv_index: Some(1),
                    }],
                };
            }
        }

        // fallback: full scan
        log::debug!("No usable constraints found, using full scan");
        IndexInfo {
            idx_num: -1,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1000.0,
            ..Default::default()
        }
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

    fn destroy(&mut self) -> Result<(), Self::Error> {
        println!("VDestroy called");
        Ok(())
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
