#![deny(clippy::all)]

use std::cell::{RefCell, RefMut};
use std::num::NonZeroUsize;

use std::rc::Rc;
use std::sync::Arc;

use limbo_core::types::Text;
use limbo_core::{maybe_init_database_file, LimboError};
use napi::iterator::Generator;
use napi::JsObject;
use napi::{bindgen_prelude::ObjectFinalize, Env, JsUnknown};
use napi_derive::napi;

#[napi(object)]
pub struct OpenDatabaseOptions {
    pub readonly: bool,
    pub file_must_exist: bool,
    pub timeout: u32,
    // verbose => Callback,
}

#[napi(custom_finalize)]
#[derive(Clone)]
pub struct Database {
    #[napi(writable = false)]
    pub memory: bool,

    // TODO: implement each property
    // #[napi(writable = false)]
    // pub readonly: bool,
    // #[napi(writable = false)]
    // pub in_transaction: bool,
    // #[napi(writable = false)]
    // pub open: bool,
    #[napi(writable = false)]
    pub name: String,
    _db: Arc<limbo_core::Database>,
    conn: Rc<limbo_core::Connection>,
    io: Arc<dyn limbo_core::IO>,
}

impl ObjectFinalize for Database {
    // TODO: check if something more is required
    fn finalize(self, _env: Env) -> napi::Result<()> {
        self.conn.close().map_err(into_napi_error)?;
        Ok(())
    }
}

#[napi]
impl Database {
    #[napi(constructor)]
    pub fn new(path: String, _options: Option<OpenDatabaseOptions>) -> napi::Result<Self> {
        let memory = path == ":memory:";
        let io: Arc<dyn limbo_core::IO> = if memory {
            Arc::new(limbo_core::MemoryIO::new())
        } else {
            Arc::new(limbo_core::PlatformIO::new().map_err(into_napi_error)?)
        };
        let file = io
            .open_file(&path, limbo_core::OpenFlags::Create, false)
            .map_err(into_napi_error)?;
        maybe_init_database_file(&file, &io).map_err(into_napi_error)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        let db = limbo_core::Database::open(io.clone(), &path, db_file, false)
            .map_err(into_napi_error)?;
        let conn = db.connect().map_err(into_napi_error)?;

        Ok(Self {
            memory,
            _db: db,
            conn,
            name: path,
            io,
        })
    }

    #[napi]
    pub fn prepare(&self, sql: String) -> napi::Result<Statement> {
        let stmt = self.conn.prepare(&sql).map_err(into_napi_error)?;
        Ok(Statement::new(RefCell::new(stmt), self.clone()))
    }

    #[napi]
    pub fn transaction(&self) {
        todo!()
    }

    #[napi]
    pub fn pragma(&self) {
        todo!()
    }

    #[napi]
    pub fn backup(&self) {
        todo!()
    }

    #[napi]
    pub fn serialize(&self) {
        todo!()
    }

    #[napi]
    pub fn function(&self) {
        todo!()
    }

    #[napi]
    pub fn aggregate(&self) {
        todo!()
    }

    #[napi]
    pub fn table(&self) {
        todo!()
    }

    #[napi]
    pub fn load_extension(&self) {
        todo!()
    }

    #[napi]
    pub fn exec(&self) {
        todo!()
    }

    #[napi]
    pub fn close(&self) {
        todo!()
    }
}

// TODO: Add the (parent) 'database' property
#[napi]
pub struct Statement {
    // TODO: implement each property when core supports it
    // #[napi(able = false)]
    // pub reader: bool,
    // #[napi(writable = false)]
    // pub readonly: bool,
    // #[napi(writable = false)]
    // pub busy: bool,
    database: Database,
    inner: Rc<RefCell<limbo_core::Statement>>,
}

#[napi]
impl Statement {
    pub fn new(inner: RefCell<limbo_core::Statement>, database: Database) -> Self {
        Self {
            inner: Rc::new(inner),
            database,
        }
    }

    #[napi]
    pub fn get(&self, env: Env) -> napi::Result<JsUnknown> {
        let mut stmt = self.inner.borrow_mut();
        stmt.reset();
        let step = stmt.step().map_err(into_napi_error)?;
        match step {
            limbo_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                let mut obj = env.create_object()?;
                for (idx, value) in row.get_values().enumerate() {
                    let key = stmt.get_column_name(idx);
                    let js_value = to_js_value(&env, value);
                    obj.set_named_property(&key, js_value)?;
                }
                Ok(obj.into_unknown())
            }
            limbo_core::StepResult::Done => Ok(env.get_undefined()?.into_unknown()),
            limbo_core::StepResult::IO => todo!(),
            limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => Err(
                napi::Error::new(napi::Status::GenericFailure, format!("{:?}", step)),
            ),
        }
    }

    // TODO: Return Info object (https://github.com/WiseLibs/better-sqlite3/blob/master/docs/api.md#runbindparameters---object)
    // The original function is variadic, check if we can do the same
    #[napi]
    pub fn run(&self, env: Env, args: Option<Vec<JsUnknown>>) -> napi::Result<JsUnknown> {
        if let Some(args) = args {
            for (i, elem) in args.into_iter().enumerate() {
                let value = from_js_value(elem)?;
                self.inner
                    .borrow_mut()
                    .bind_at(NonZeroUsize::new(i + 1).unwrap(), value);
            }
        }

        let stmt = self.inner.borrow_mut();
        self.internal_all(env, stmt)
    }

    #[napi]
    pub fn iterate(&self, env: Env) -> IteratorStatement {
        IteratorStatement {
            stmt: Rc::clone(&self.inner),
            database: self.database.clone(),
            env,
        }
    }

    #[napi]
    pub fn all(&self, env: Env) -> napi::Result<JsUnknown> {
        let mut stmt = self.inner.borrow_mut();
        stmt.reset();

        self.internal_all(env, stmt)
    }

    fn internal_all(
        &self,
        env: Env,
        mut stmt: RefMut<'_, limbo_core::Statement>,
    ) -> napi::Result<JsUnknown> {
        let mut results = env.create_empty_array()?;
        let mut index = 0;
        loop {
            match stmt.step().map_err(into_napi_error)? {
                limbo_core::StepResult::Row => {
                    let row = stmt.row().unwrap();
                    let mut obj = env.create_object()?;
                    for (idx, value) in row.get_values().enumerate() {
                        let key = stmt.get_column_name(idx);
                        let js_value = to_js_value(&env, value);
                        obj.set_named_property(&key, js_value)?;
                    }
                    results.set_element(index, obj)?;
                    index += 1;
                }
                limbo_core::StepResult::Done => {
                    break;
                }
                limbo_core::StepResult::IO => {
                    self.database.io.run_once().map_err(into_napi_error)?;
                }
                limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                    return Err(napi::Error::new(
                        napi::Status::GenericFailure,
                        format!("{:?}", stmt.step()),
                    ));
                }
            }
        }

        Ok(results.into_unknown())
    }

    #[napi]
    pub fn pluck() {
        todo!()
    }
    #[napi]
    pub fn expand() {
        todo!()
    }
    #[napi]
    pub fn raw() {
        todo!()
    }
    #[napi]
    pub fn columns() {
        todo!()
    }
    #[napi]
    pub fn bind() {
        todo!()
    }
}

#[napi(iterator)]
pub struct IteratorStatement {
    stmt: Rc<RefCell<limbo_core::Statement>>,
    database: Database,
    env: Env,
}

impl Generator for IteratorStatement {
    type Yield = JsObject;

    type Next = ();

    type Return = ();

    fn next(&mut self, _: Option<Self::Next>) -> Option<Self::Yield> {
        let mut stmt = self.stmt.borrow_mut();

        match stmt.step().ok()? {
            limbo_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                let mut js_row = self.env.create_object().ok()?;

                for (idx, value) in row.get_values().enumerate() {
                    let key = stmt.get_column_name(idx);
                    let js_value = to_js_value(&self.env, value);
                    js_row.set_named_property(&key, js_value).ok()?;
                }

                Some(js_row)
            }
            limbo_core::StepResult::Done => None,
            limbo_core::StepResult::IO => {
                self.database.io.run_once().ok()?;
                None // clearly it's incorrect it should return to user
            }
            limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => None,
        }
    }
}

fn to_js_value(env: &napi::Env, value: &limbo_core::Value) -> napi::Result<JsUnknown> {
    match value {
        limbo_core::Value::Null => Ok(env.get_null()?.into_unknown()),
        limbo_core::Value::Integer(i) => Ok(env.create_int64(*i)?.into_unknown()),
        limbo_core::Value::Float(f) => Ok(env.create_double(*f)?.into_unknown()),
        limbo_core::Value::Text(s) => Ok(env.create_string(s.as_str())?.into_unknown()),
        limbo_core::Value::Blob(b) => Ok(env.create_buffer_copy(b.as_slice())?.into_unknown()),
    }
}

fn from_js_value(value: JsUnknown) -> napi::Result<limbo_core::Value> {
    match value.get_type()? {
        napi::ValueType::Undefined | napi::ValueType::Null | napi::ValueType::Unknown => {
            Ok(limbo_core::Value::Null)
        }
        napi::ValueType::Boolean => {
            let b = value.coerce_to_bool()?.get_value()?;
            Ok(limbo_core::Value::Integer(b as i64))
        }
        napi::ValueType::Number => {
            let num = value.coerce_to_number()?.get_double()?;
            if num.fract() == 0.0 {
                Ok(limbo_core::Value::Integer(num as i64))
            } else {
                Ok(limbo_core::Value::Float(num))
            }
        }
        napi::ValueType::String => {
            let s = value.coerce_to_string()?;
            Ok(limbo_core::Value::Text(Text::from_str(
                s.into_utf8()?.as_str()?,
            )))
        }
        napi::ValueType::Symbol
        | napi::ValueType::Object
        | napi::ValueType::Function
        | napi::ValueType::External => Err(napi::Error::new(
            napi::Status::GenericFailure,
            "Unsupported type",
        )),
    }
}

struct DatabaseFile {
    file: Arc<dyn limbo_core::File>,
}

unsafe impl Send for DatabaseFile {}
unsafe impl Sync for DatabaseFile {}

impl DatabaseFile {
    pub fn new(file: Arc<dyn limbo_core::File>) -> Self {
        Self { file }
    }
}

impl limbo_core::DatabaseStorage for DatabaseFile {
    fn read_page(&self, page_idx: usize, c: limbo_core::Completion) -> limbo_core::Result<()> {
        let r = match c {
            limbo_core::Completion::Read(ref r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(limbo_core::LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<std::cell::RefCell<limbo_core::Buffer>>,
        c: limbo_core::Completion,
    ) -> limbo_core::Result<()> {
        let size = buffer.borrow().len();
        let pos = (page_idx - 1) * size;
        self.file.pwrite(pos, buffer, c)?;
        Ok(())
    }

    fn sync(&self, c: limbo_core::Completion) -> limbo_core::Result<()> {
        self.file.sync(c)
    }
}

#[inline]
pub fn into_napi_error(limbo_error: LimboError) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, format!("{limbo_error}"))
}
