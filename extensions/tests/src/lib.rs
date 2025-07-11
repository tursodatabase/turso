use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use turso_ext::{
    register_extension, scalar, Connection, ConstraintInfo, ConstraintOp, ConstraintUsage,
    ExtResult, IndexInfo, OrderByInfo, ResultCode, StepResult, VTabCursor, VTabKind, VTabModule,
    VTabModuleDerive, VTable, Value,
};
#[cfg(not(target_family = "wasm"))]
use turso_ext::{CompletionCallbackFn, VfsDerive, VfsExtension, VfsFile};

register_extension! {
    vtabs: { KVStoreVTabModule, TableStatsVtabModule },
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
                log::debug!("idx_str found: key_eq\n value: {key:?}");
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

    fn open(&self, _conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error> {
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

type Callback = Box<dyn Fn() -> ExtResult<()>>;
type CallbackQueue = Arc<Mutex<Vec<Callback>>>;

pub struct TestFile {
    file: Arc<Mutex<File>>,
    callbacks: CallbackQueue,
}

#[cfg(target_family = "wasm")]
pub struct TestFS {
    callbacks: CallbackQueue,
}

#[cfg(not(target_family = "wasm"))]
#[derive(VfsDerive, Default)]
pub struct TestFS {
    callbacks: CallbackQueue,
}

unsafe impl Send for TestFS {}
unsafe impl Sync for TestFS {}

unsafe impl Send for TestFile {}
unsafe impl Sync for TestFile {}

// Test that we can have additional extension types in the same file
// and still register the vfs at comptime if linking staticly
#[scalar(name = "test_scalar")]
fn test_scalar(_args: turso_ext::Value) -> turso_ext::Value {
    turso_ext::Value::from_integer(42)
}

#[cfg(not(target_family = "wasm"))]
impl VfsExtension for TestFS {
    const NAME: &'static str = "testvfs";
    type File = TestFile;
    fn open_file(&self, path: &str, flags: i32, _direct: bool) -> ExtResult<Self::File> {
        let _ = env_logger::try_init();
        log::debug!("opening file with testing VFS: {path} flags: {flags}");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(flags & 1 != 0)
            .open(path)
            .map_err(|_| ResultCode::Error)?;
        Ok(TestFile {
            file: Arc::new(Mutex::new(file)),
            callbacks: self.callbacks.clone(),
        })
    }

    fn run_once(&self) -> ExtResult<()> {
        let mut callbacks = self.callbacks.lock().unwrap();
        let events = callbacks.drain(0..);
        for op in events {
            op()?
        }
        Ok(())
    }
}

#[cfg(not(target_family = "wasm"))]
impl VfsFile for TestFile {
    unsafe fn read(
        &mut self,
        buf: *mut u8,
        len: usize,
        offset: i64,
        callback: CompletionCallbackFn,
    ) {
        log::debug!("reading file with testing VFS: bytes: {len} offset: {offset}");
        assert!(!buf.is_null(), "Buf Pointer is null");

        let mut callbacks = self.callbacks.lock().unwrap();
        let clone_file = self.file.clone();
        callbacks.push(Box::new(move || -> ExtResult<()> {
            let mut file = clone_file.lock().unwrap();
            if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                return Err(ResultCode::Error);
            }
            let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf, len) };
            let res = file
                .read(buf)
                .map_err(|_| ResultCode::Error)
                .map(|n| n as i32)?;
            callback(res);
            Ok(())
        }));
    }

    unsafe fn write(
        &mut self,
        buf: *const u8,
        len: usize,
        offset: i64,
        callback: CompletionCallbackFn,
    ) {
        log::debug!("writing to file with testing VFS: bytes: {len} offset: {offset}");
        assert!(!buf.is_null(), "Buf Pointer is null");
        let mut callbacks = self.callbacks.lock().unwrap();
        let clone_file = self.file.clone();
        callbacks.push(Box::new(move || -> ExtResult<()> {
            let mut file = clone_file.lock().unwrap();
            if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                return Err(ResultCode::Error);
            }
            let buf: &[u8] = unsafe { std::slice::from_raw_parts(buf, len) };

            let res = file
                .write(&buf[..len])
                .map_err(|_| ResultCode::Error)
                .map(|n| n as i32)?;
            callback(res);

            Ok(())
        }));
    }

    fn sync(&self, callback: CompletionCallbackFn) {
        log::debug!("syncing file with testing VFS");
        let mut callbacks = self.callbacks.lock().unwrap();
        let clone_file = self.file.clone();
        callbacks.push(Box::new(move || -> ExtResult<()> {
            let file = clone_file.lock().unwrap();
            file.sync_all().map_err(|_| ResultCode::Error)?;
            callback(0);
            Ok(())
        }));
    }

    fn size(&self) -> i64 {
        self.file
            .lock()
            .unwrap()
            .metadata()
            .map(|m| m.len() as i64)
            .unwrap_or(-1)
    }
}

#[derive(VTabModuleDerive, Default)]
pub struct TableStatsVtabModule;

pub struct StatsCursor {
    pos: usize,
    rows: Vec<(String, i64)>,
    conn: Option<Arc<Connection>>,
}

pub struct StatsTable {}
impl VTabModule for TableStatsVtabModule {
    type Table = StatsTable;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "tablestats";

    fn create(_args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let schema = "CREATE TABLE x(name TEXT, rows INT);".to_string();
        Ok((schema, StatsTable {}))
    }
}

impl VTable for StatsTable {
    type Cursor = StatsCursor;
    type Error = String;

    fn open(&self, conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error> {
        Ok(StatsCursor {
            pos: 0,
            rows: Vec::new(),
            conn,
        })
    }
}

impl VTabCursor for StatsCursor {
    type Error = String;

    fn filter(&mut self, _args: &[Value], _idx_info: Option<(&str, i32)>) -> ResultCode {
        self.rows.clear();
        self.pos = 0;

        let Some(conn) = &self.conn else {
            log::error!("no connection present");
            return ResultCode::Error;
        };

        // discover application tables
        let mut master = conn
            .prepare(
                "SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%';",
            )
            .map_err(|_| ResultCode::Error)
            .unwrap();

        let mut tables = Vec::new();
        while let StepResult::Row = master.step() {
            let row = master.get_row();
            let tbl = if let Some(val) = row.first() {
                val.to_text().unwrap_or("").to_string()
            } else {
                "".to_string()
            };
            if tbl.is_empty() {
                continue;
            };
            if row
                .get(1)
                .is_some_and(|v| v.to_text().unwrap().contains("CREATE VIRTUAL TABLE"))
            {
                continue;
            }
            tables.push(tbl);
        }
        master.close();
        for tbl in tables {
            // count rows for each table
            if let Ok(mut count_stmt) = conn.prepare(&format!("SELECT COUNT(*) FROM {tbl};")) {
                let count = match count_stmt.step() {
                    StepResult::Row => count_stmt.get_row()[0].to_integer().unwrap_or(0),
                    _ => 0,
                };
                self.rows.push((tbl, count));
                count_stmt.close();
            }
        }
        if conn
            .execute(
                "insert into products (name, price) values(?, ?);",
                &[Value::from_text("xConnect".into()), Value::from_integer(42)],
            )
            .is_err()
        {
            log::error!("failed to insert into xConnect table");
        }
        let mut stmt = conn
            .prepare("select price from products where name = ? limit 1;")
            .map_err(|_| ResultCode::Error)
            .unwrap();
        stmt.bind_at(
            NonZeroUsize::new(1).expect("1 to be not zero"),
            Value::from_text("xConnect".into()),
        );
        while let StepResult::Row = stmt.step() {
            let row = stmt.get_row();
            if let Some(val) = row.first() {
                assert_eq!(val.to_integer(), Some(42));
            }
        }
        stmt.close();
        ResultCode::OK
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        self.rows
            .get(self.pos)
            .ok_or("row out of range".to_string())
            .and_then(|(name, cnt)| match idx {
                0 => Ok(Value::from_text(name.clone())),
                1 => Ok(Value::from_integer(*cnt)),
                _ => Err("bad column".into()),
            })
    }

    fn next(&mut self) -> ResultCode {
        self.pos += 1;
        if self.pos >= self.rows.len() {
            ResultCode::EOF
        } else {
            ResultCode::OK
        }
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }
    fn rowid(&self) -> i64 {
        self.pos as i64
    }
}
