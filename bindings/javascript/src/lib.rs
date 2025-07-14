#![deny(clippy::all)]

use std::cell::{RefCell, RefMut};
use std::num::NonZeroUsize;

use std::rc::Rc;
use std::sync::Arc;

use napi::iterator::Generator;
use napi::{bindgen_prelude::ObjectFinalize, Env, JsUnknown};
use napi_derive::napi;
use turso_core::{LimboError, StepResult};

#[derive(Default)]
#[napi(object)]
pub struct OpenDatabaseOptions {
    pub readonly: Option<bool>,
    pub file_must_exist: Option<bool>,
    pub timeout: Option<u32>,
    // verbose => Callback,
}

impl OpenDatabaseOptions {
    fn readonly(&self) -> bool {
        self.readonly.unwrap_or(false)
    }
}

#[napi(object)]
pub struct PragmaOptions {
    pub simple: bool,
}

#[napi(custom_finalize)]
#[derive(Clone)]
pub struct Database {
    #[napi(writable = false)]
    pub memory: bool,

    #[napi(writable = false)]
    pub readonly: bool,
    // #[napi(writable = false)]
    // pub in_transaction: bool,
    #[napi(writable = false)]
    pub open: bool,
    #[napi(writable = false)]
    pub name: String,
    _db: Arc<turso_core::Database>,
    conn: Arc<turso_core::Connection>,
    _io: Arc<dyn turso_core::IO>,
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
    pub fn new(path: String, options: Option<OpenDatabaseOptions>) -> napi::Result<Self, String> {
        let memory = path == ":memory:";
        let io: Arc<dyn turso_core::IO> = if memory {
            Arc::new(turso_core::MemoryIO::new())
        } else {
            Arc::new(turso_core::PlatformIO::new().map_err(into_napi_sqlite_error)?)
        };
        let opts = options.unwrap_or_default();
        let flag = if opts.readonly() {
            turso_core::OpenFlags::ReadOnly
        } else {
            turso_core::OpenFlags::Create
        };
        let file = io
            .open_file(&path, flag, false)
            .map_err(|err| into_napi_error_with_message("SQLITE_CANTOPEN".to_owned(), err))?;

        let db_file = Arc::new(DatabaseFile::new(file));
        let db = turso_core::Database::open(io.clone(), &path, db_file, false, false)
            .map_err(into_napi_sqlite_error)?;
        let conn = db.connect().map_err(into_napi_sqlite_error)?;

        Ok(Self {
            readonly: opts.readonly(),
            memory,
            _db: db,
            conn,
            open: true,
            name: path,
            _io: io,
        })
    }

    #[napi]
    pub fn prepare(&self, sql: String) -> napi::Result<Statement> {
        let stmt = self.conn.prepare(&sql).map_err(into_napi_error)?;
        Ok(Statement::new(RefCell::new(stmt), self.clone(), sql))
    }

    #[napi]
    pub fn pragma(
        &self,
        env: Env,
        pragma_name: String,
        options: Option<PragmaOptions>,
    ) -> napi::Result<JsUnknown> {
        let sql = format!("PRAGMA {pragma_name}");
        let stmt = self.prepare(sql)?;
        match options {
            Some(PragmaOptions { simple: true, .. }) => {
                let mut stmt = stmt.inner.borrow_mut();
                loop {
                    match stmt.step().map_err(into_napi_error)? {
                        turso_core::StepResult::Row => {
                            let row: Vec<_> = stmt.row().unwrap().get_values().cloned().collect();
                            return to_js_value(&env, &row[0]);
                        }
                        turso_core::StepResult::Done => {
                            return Ok(env.get_undefined()?.into_unknown())
                        }
                        turso_core::StepResult::IO => {
                            stmt.run_once().map_err(into_napi_error)?;
                            continue;
                        }
                        step @ turso_core::StepResult::Interrupt
                        | step @ turso_core::StepResult::Busy => {
                            return Err(napi::Error::new(
                                napi::Status::GenericFailure,
                                format!("{step:?}"),
                            ))
                        }
                    }
                }
            }
            _ => stmt.run(env, None),
        }
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
    pub fn load_extension(&self, path: String) -> napi::Result<()> {
        let ext_path = turso_core::resolve_ext_path(path.as_str()).map_err(into_napi_error)?;
        self.conn
            .load_extension(ext_path)
            .map_err(into_napi_error)?;
        Ok(())
    }

    #[napi]
    pub fn exec(&self, sql: String) -> napi::Result<(), String> {
        let query_runner = self.conn.query_runner(sql.as_bytes());

        // Since exec doesn't return any values, we can just iterate over the results
        for output in query_runner {
            match output {
                Ok(Some(mut stmt)) => loop {
                    match stmt.step() {
                        Ok(StepResult::Row) => continue,
                        Ok(StepResult::IO) => stmt.run_once().map_err(into_napi_sqlite_error)?,
                        Ok(StepResult::Done) => break,
                        Ok(StepResult::Interrupt | StepResult::Busy) => {
                            return Err(napi::Error::new(
                                "SQLITE_ERROR".to_owned(),
                                "Statement execution interrupted or busy".to_string(),
                            ));
                        }
                        Err(err) => {
                            return Err(napi::Error::new(
                                "SQLITE_ERROR".to_owned(),
                                format!("Error executing SQL: {err}"),
                            ));
                        }
                    }
                },
                Ok(None) => continue,
                Err(err) => {
                    return Err(napi::Error::new(
                        "SQLITE_ERROR".to_owned(),
                        format!("Error executing SQL: {err}"),
                    ));
                }
            }
        }
        Ok(())
    }

    #[napi]
    pub fn close(&mut self) -> napi::Result<()> {
        if self.open {
            self.conn.close().map_err(into_napi_error)?;
            self.open = false;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum PresentationMode {
    Raw,
    Pluck,
    None,
}

#[napi]
#[derive(Clone)]
pub struct Statement {
    // TODO: implement each property when core supports it
    // #[napi(able = false)]
    // pub reader: bool,
    // #[napi(writable = false)]
    // pub readonly: bool,
    // #[napi(writable = false)]
    // pub busy: bool,
    #[napi(writable = false)]
    pub source: String,

    database: Database,
    presentation_mode: PresentationMode,
    binded: bool,
    inner: Rc<RefCell<turso_core::Statement>>,
}

#[napi]
impl Statement {
    pub fn new(inner: RefCell<turso_core::Statement>, database: Database, source: String) -> Self {
        Self {
            inner: Rc::new(inner),
            database,
            source,
            presentation_mode: PresentationMode::None,
            binded: false,
        }
    }

    #[napi]
    pub fn get(&self, env: Env, args: Option<Vec<JsUnknown>>) -> napi::Result<JsUnknown> {
        let mut stmt = self.check_and_bind(env, args)?;

        loop {
            let step = stmt.step().map_err(into_napi_error)?;
            match step {
                turso_core::StepResult::Row => {
                    let row = stmt.row().unwrap();

                    match self.presentation_mode {
                        PresentationMode::Raw => {
                            let mut raw_obj = env.create_array(row.len() as u32)?;
                            for (idx, value) in row.get_values().enumerate() {
                                let js_value = to_js_value(&env, value);

                                raw_obj.set(idx as u32, js_value)?;
                            }

                            return Ok(raw_obj.coerce_to_object()?.into_unknown());
                        }
                        PresentationMode::Pluck => {
                            let (_, value) =
                                row.get_values().enumerate().next().ok_or(napi::Error::new(
                                    napi::Status::GenericFailure,
                                    "Pluck mode requires at least one column in the result",
                                ))?;
                            let js_value = to_js_value(&env, value)?;

                            return Ok(js_value);
                        }
                        PresentationMode::None => {
                            let mut obj = env.create_object()?;

                            for (idx, value) in row.get_values().enumerate() {
                                let key = stmt.get_column_name(idx);
                                let js_value = to_js_value(&env, value);

                                obj.set_named_property(&key, js_value)?;
                            }

                            return Ok(obj.into_unknown());
                        }
                    }
                }
                turso_core::StepResult::Done => return Ok(env.get_undefined()?.into_unknown()),
                turso_core::StepResult::IO => {
                    stmt.run_once().map_err(into_napi_error)?;
                    continue;
                }
                turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                    return Err(napi::Error::new(
                        napi::Status::GenericFailure,
                        format!("{step:?}"),
                    ))
                }
            }
        }
    }

    // TODO: Return Info object (https://github.com/WiseLibs/better-sqlite3/blob/master/docs/api.md#runbindparameters---object)
    #[napi]
    pub fn run(&self, env: Env, args: Option<Vec<JsUnknown>>) -> napi::Result<JsUnknown> {
        let stmt = self.check_and_bind(env, args)?;

        self.internal_all(env, stmt)
    }

    #[napi]
    pub fn iterate(
        &self,
        env: Env,
        args: Option<Vec<JsUnknown>>,
    ) -> napi::Result<IteratorStatement> {
        if let Some(some_args) = args.as_ref() {
            if some_args.iter().len() != 0 {
                self.check_and_bind(env, args)?;
            }
        }

        Ok(IteratorStatement {
            stmt: Rc::clone(&self.inner),
            _database: self.database.clone(),
            env,
            presentation_mode: self.presentation_mode.clone(),
        })
    }

    #[napi]
    pub fn all(&self, env: Env, args: Option<Vec<JsUnknown>>) -> napi::Result<JsUnknown> {
        let stmt = self.check_and_bind(env, args)?;

        self.internal_all(env, stmt)
    }

    fn internal_all(
        &self,
        env: Env,
        mut stmt: RefMut<'_, turso_core::Statement>,
    ) -> napi::Result<JsUnknown> {
        let mut results = env.create_empty_array()?;
        let mut index = 0;
        loop {
            match stmt.step().map_err(into_napi_error)? {
                turso_core::StepResult::Row => {
                    let row = stmt.row().unwrap();

                    match self.presentation_mode {
                        PresentationMode::Raw => {
                            let mut raw_array = env.create_array(row.len() as u32)?;
                            for (idx, value) in row.get_values().enumerate() {
                                let js_value = to_js_value(&env, value)?;
                                raw_array.set(idx as u32, js_value)?;
                            }
                            results.set_element(index, raw_array.coerce_to_object()?)?;
                            index += 1;
                            continue;
                        }
                        PresentationMode::Pluck => {
                            let (_, value) =
                                row.get_values().enumerate().next().ok_or(napi::Error::new(
                                    napi::Status::GenericFailure,
                                    "Pluck mode requires at least one column in the result",
                                ))?;
                            let js_value = to_js_value(&env, value)?;
                            results.set_element(index, js_value)?;
                            index += 1;
                            continue;
                        }
                        PresentationMode::None => {
                            let mut obj = env.create_object()?;
                            for (idx, value) in row.get_values().enumerate() {
                                let key = stmt.get_column_name(idx);
                                let js_value = to_js_value(&env, value);
                                obj.set_named_property(&key, js_value)?;
                            }
                            results.set_element(index, obj)?;
                            index += 1;
                        }
                    }
                }
                turso_core::StepResult::Done => {
                    break;
                }
                turso_core::StepResult::IO => {
                    stmt.run_once().map_err(into_napi_error)?;
                }
                turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
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
    pub fn pluck(&mut self, pluck: Option<bool>) {
        if let Some(false) = pluck {
            self.presentation_mode = PresentationMode::None;
        }

        self.presentation_mode = PresentationMode::Pluck;
    }

    #[napi]
    pub fn expand() {
        todo!()
    }

    #[napi]
    pub fn raw(&mut self, raw: Option<bool>) {
        if let Some(false) = raw {
            self.presentation_mode = PresentationMode::None;
        }

        self.presentation_mode = PresentationMode::Raw;
    }

    #[napi]
    pub fn columns() {
        todo!()
    }

    #[napi]
    pub fn bind(&mut self, env: Env, args: Option<Vec<JsUnknown>>) -> napi::Result<Self, String> {
        self.check_and_bind(env, args)
            .map_err(with_sqlite_error_message)?;
        self.binded = true;

        Ok(self.clone())
    }

    /// Check if the Statement is already binded by the `bind()` method
    /// and bind values do variables. The expected type for args is `Option<Vec<JsUnknown>>`
    fn check_and_bind(
        &self,
        env: Env,
        args: Option<Vec<JsUnknown>>,
    ) -> napi::Result<RefMut<'_, turso_core::Statement>> {
        let mut stmt = self.inner.borrow_mut();
        stmt.reset();
        if let Some(args) = args {
            if self.binded {
                let err = napi::Error::new(
                    into_convertible_type_error_message("TypeError"),
                    "The bind() method can only be invoked once per statement object",
                );
                unsafe {
                    napi::JsTypeError::from(err).throw_into(env.raw());
                }

                return Err(napi::Error::from_status(napi::Status::PendingException));
            }

            for (i, elem) in args.into_iter().enumerate() {
                let value = from_js_value(elem)?;
                stmt.bind_at(NonZeroUsize::new(i + 1).unwrap(), value);
            }
        }

        Ok(stmt)
    }
}

#[napi(iterator)]
pub struct IteratorStatement {
    stmt: Rc<RefCell<turso_core::Statement>>,
    _database: Database,
    env: Env,
    presentation_mode: PresentationMode,
}

impl Generator for IteratorStatement {
    type Yield = JsUnknown;

    type Next = ();

    type Return = ();

    fn next(&mut self, _: Option<Self::Next>) -> Option<Self::Yield> {
        let mut stmt = self.stmt.borrow_mut();

        loop {
            match stmt.step().ok()? {
                turso_core::StepResult::Row => {
                    let row = stmt.row().unwrap();

                    match self.presentation_mode {
                        PresentationMode::Raw => {
                            let mut raw_array = self.env.create_array(row.len() as u32).ok()?;
                            for (idx, value) in row.get_values().enumerate() {
                                let js_value = to_js_value(&self.env, value);
                                raw_array.set(idx as u32, js_value).ok()?;
                            }

                            return Some(raw_array.coerce_to_object().ok()?.into_unknown());
                        }
                        PresentationMode::Pluck => {
                            let (_, value) = row.get_values().enumerate().next()?;
                            return to_js_value(&self.env, value).ok();
                        }
                        PresentationMode::None => {
                            let mut js_row = self.env.create_object().ok()?;
                            for (idx, value) in row.get_values().enumerate() {
                                let key = stmt.get_column_name(idx);
                                let js_value = to_js_value(&self.env, value);
                                js_row.set_named_property(&key, js_value).ok()?;
                            }

                            return Some(js_row.into_unknown());
                        }
                    }
                }
                turso_core::StepResult::Done => return None,
                turso_core::StepResult::IO => {
                    stmt.run_once().ok()?;
                    continue;
                }
                turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => return None,
            }
        }
    }
}

fn to_js_value(env: &napi::Env, value: &turso_core::Value) -> napi::Result<JsUnknown> {
    match value {
        turso_core::Value::Null => Ok(env.get_null()?.into_unknown()),
        turso_core::Value::Integer(i) => Ok(env.create_int64(*i)?.into_unknown()),
        turso_core::Value::Float(f) => Ok(env.create_double(*f)?.into_unknown()),
        turso_core::Value::Text(s) => Ok(env.create_string(s.as_str())?.into_unknown()),
        turso_core::Value::Blob(b) => Ok(env.create_buffer_copy(b.as_slice())?.into_unknown()),
    }
}

fn from_js_value(value: JsUnknown) -> napi::Result<turso_core::Value> {
    match value.get_type()? {
        napi::ValueType::Undefined | napi::ValueType::Null | napi::ValueType::Unknown => {
            Ok(turso_core::Value::Null)
        }
        napi::ValueType::Boolean => {
            let b = value.coerce_to_bool()?.get_value()?;
            Ok(turso_core::Value::Integer(b as i64))
        }
        napi::ValueType::Number => {
            let num = value.coerce_to_number()?.get_double()?;
            if num.fract() == 0.0 {
                Ok(turso_core::Value::Integer(num as i64))
            } else {
                Ok(turso_core::Value::Float(num))
            }
        }
        napi::ValueType::String => {
            let s = value.coerce_to_string()?;
            Ok(turso_core::Value::Text(s.into_utf8()?.as_str()?.into()))
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
    file: Arc<dyn turso_core::File>,
}

unsafe impl Send for DatabaseFile {}
unsafe impl Sync for DatabaseFile {}

impl DatabaseFile {
    pub fn new(file: Arc<dyn turso_core::File>) -> Self {
        Self { file }
    }
}

impl turso_core::DatabaseStorage for DatabaseFile {
    fn read_page(&self, page_idx: usize, c: turso_core::Completion) {
        let r = match c.completion_type {
            turso_core::CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            panic!("Not a DB");
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c);
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<std::cell::RefCell<turso_core::Buffer>>,
        c: turso_core::Completion,
    ) {
        let size = buffer.borrow().len();
        let pos = (page_idx - 1) * size;
        self.file.pwrite(pos, buffer, c);
    }

    fn sync(&self, c: turso_core::Completion) {
        self.file.sync(c);
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.file.size()
    }
}

#[inline]
fn into_napi_error(limbo_error: LimboError) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, format!("{limbo_error}"))
}

#[inline]
fn into_napi_sqlite_error(limbo_error: LimboError) -> napi::Error<String> {
    napi::Error::new(String::from("SQLITE_ERROR"), format!("{limbo_error}"))
}

#[inline]
fn into_napi_error_with_message(
    error_code: String,
    limbo_error: LimboError,
) -> napi::Error<String> {
    napi::Error::new(error_code, format!("{limbo_error}"))
}

#[inline]
fn with_sqlite_error_message(err: napi::Error) -> napi::Error<String> {
    napi::Error::new("SQLITE_ERROR".to_owned(), err.reason)
}

#[inline]
fn into_convertible_type_error_message(error_type: &str) -> String {
    "[TURSO_CONVERT_TYPE]".to_owned() + error_type
}
