use anyhow::anyhow;
use ariadne::Report;
use core::fmt;
use dsl_parser::{Statement, Test, TestKind, TestMode, WrappedRegex};
use limbo_core::{Database, IO};
use rusqlite::types::Value;
use rusqlite::{params, OpenFlags};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Debug)]
pub(crate) enum AssertKind {
    Eq,
    #[allow(dead_code)]
    Ne,
}

/// Copy of std assert_failed but returns instead
pub(crate) fn assert_failed<T, U>(
    kind: AssertKind,
    left: &T,
    right: &U,
    args: Option<fmt::Arguments<'_>>,
) -> anyhow::Error
where
    T: fmt::Debug + ?Sized,
    U: fmt::Debug + ?Sized,
{
    let op = match kind {
        AssertKind::Eq => "==",
        AssertKind::Ne => "!=",
    };

    match args {
        Some(args) => anyhow!(
            r#"assertion `left {op} right` failed: {args}
  left: {left:?}
 right: {right:?}"#
        ),
        None => anyhow!(
            r#"assertion `left {op} right` failed
  left: {left:?}
 right: {right:?}"#
        ),
    }
}

/// Copy of assert_eq! but returns instead
macro_rules! return_if_neq {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    let kind = $crate::testing::AssertKind::Eq;
                    // The reborrows below are intentional. Without them, the stack slot for the
                    // borrow is initialized even before the values are compared, leading to a
                    // noticeable slow down.
                    return Err($crate::testing::assert_failed(kind, &*left_val, &*right_val, $crate::option::Option::None));
                }
            }
        }
    };
    ($left:expr, $right:expr, $($arg:tt)+) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    let kind = $crate::testing::AssertKind::Eq;
                    // The reborrows below are intentional. Without them, the stack slot for the
                    // borrow is initialized even before the values are compared, leading to a
                    // noticeable slow down.
                    return Err($crate::testing::assert_failed(kind, &*left_val, &*right_val, Option::Some(format_args!($($arg)+))));
                }
            }
        }
    };
}

/// Copy of assert! but returns instead
macro_rules! return_if_false {
    ($val:expr $(,)?) => {
        if !($val) {
            let kind = $crate::testing::AssertKind::Eq;
            // The reborrows below are intentional. Without them, the stack slot for the
            // borrow is initialized even before the values are compared, leading to a
            // noticeable slow down.
            return Err($crate::testing::assert_failed(kind, &$val, &true, Option::None));
        }
    };
    ($val:expr, $($arg:tt)+) => {
        if !($val) {
            let kind = $crate::testing::AssertKind::Eq;
            // The reborrows below are intentional. Without them, the stack slot for the
            // borrow is initialized even before the values are compared, leading to a
            // noticeable slow down.
            return Err($crate::testing::assert_failed(kind, &$val, &true, Option::Some(format_args!($($arg)+))));
        }
    };
}

#[allow(dead_code)]
pub struct TempDatabase {
    pub path: PathBuf,
    pub io: Arc<dyn IO + Send>,
}
unsafe impl Send for TempDatabase {}

#[allow(clippy::arc_with_non_send_sync)]
impl TempDatabase {
    #[allow(dead_code)]
    pub fn new(db_name: &str) -> Self {
        let mut path = TempDir::new().unwrap().keep();
        path.push(db_name);
        let io: Arc<dyn IO + Send> = Arc::new(limbo_core::PlatformIO::new().unwrap());
        Self { path, io }
    }

    pub fn new_existent(db_path: &Path) -> Self {
        let io: Arc<dyn IO + Send> = Arc::new(limbo_core::PlatformIO::new().unwrap());
        Self {
            path: db_path.to_path_buf(),
            io,
        }
    }

    pub fn new_in_memory() -> Self {
        let io: Arc<dyn IO + Send> = Arc::new(limbo_core::MemoryIO::new());
        // No path for Memory IO
        Self {
            path: "memory".into(),
            io,
        }
    }

    pub fn connect_limbo(&self) -> Rc<limbo_core::Connection> {
        Self::connect_limbo_with_flags(&self, limbo_core::OpenFlags::default())
    }

    pub fn connect_limbo_with_flags(
        &self,
        flags: limbo_core::OpenFlags,
    ) -> Rc<limbo_core::Connection> {
        let db = Database::open_file_with_flags(
            self.io.clone(),
            self.path.to_str().unwrap(),
            flags,
            false,
        )
        .unwrap();

        let conn = db.connect().unwrap();
        conn
    }
}

#[derive(Debug)]
pub struct DslTest<'a> {
    pub inner: Test<'a>,
    is_random_db: bool,
}

#[derive(Debug)]
pub struct FileTest<'a> {
    pub file_name: &'a str,
    pub source: &'a str,
    pub tests: Vec<DslTest<'a>>,
    pub errors: Vec<Report<'a, (&'a str, std::ops::Range<usize>)>>,
}

impl<'a> DslTest<'a> {
    pub(crate) fn new(test: Test<'a>) -> Self {
        Self {
            inner: test,
            is_random_db: false,
        }
    }

    pub(crate) fn exec_sql(&self, db_path: Option<&Path>) -> anyhow::Result<()> {
        {
            let sqlite_conn = if let Some(db_path) = db_path {
                if !self.is_random_db {
                    rusqlite::Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                        .unwrap()
                } else {
                    // This can be a random db_path that was not created yet
                    rusqlite::Connection::open(db_path).unwrap()
                }
            } else {
                rusqlite::Connection::open_in_memory().unwrap()
            };
            self.exec_sql_sqlite(sqlite_conn, db_path)?;
        }

        {
            let db = if let Some(db_path) = db_path {
                TempDatabase::new_existent(db_path)
            } else {
                TempDatabase::new_in_memory()
            };
            let limbo_conn = if self.is_random_db || db_path.is_none() {
                db.connect_limbo()
            } else {
                db.connect_limbo_with_flags(
                    limbo_core::OpenFlags::default() | limbo_core::OpenFlags::ReadOnly,
                )
            };
            self.exec_sql_limbo(db, limbo_conn, db_path)?;
        }
        Ok(())
    }

    // TODO: for now, duplicate code here for testing
    fn exec_sql_sqlite(
        &self,
        sqlite_conn: rusqlite::Connection,
        db_path: Option<&Path>,
    ) -> anyhow::Result<()> {
        let db_path = db_path.as_ref().map(|db_path| db_path.to_string_lossy());
        let db_path = db_path.unwrap_or(std::borrow::Cow::Borrowed("memory"));

        let expected = match &self.inner.statement {
            Statement::Single(sql) => sqlite_exec_rows(&sqlite_conn, sql),

            Statement::Many(sql_queries) => {
                return_if_false!(!sql_queries.is_empty());
                sql_queries
                    .into_iter()
                    .map(|sql| sqlite_exec_rows(&sqlite_conn, &sql))
                    .reduce(|acc, val| match (acc, val) {
                        (Ok(mut acc), Ok(val)) => {
                            acc.extend(val);
                            Ok(acc)
                        }
                        (Err(e), _) | (_, Err(e)) => Err(e),
                    })
                    .unwrap()
            }
        };

        if matches!(self.inner.mode, TestMode::Error) {
            if !expected.is_err() {
                return Err(anyhow!("no error thrown in SQLite"));
            }
            return Ok(());
        }

        let expected = expected?;

        if let TestKind::Regex(WrappedRegex(regex)) = &self.inner.kind {
            for values in expected.iter() {
                for val in values {
                    match val {
                        Value::Text(s) => {
                            return_if_false!(
                                regex.is_match(s),
                                "regex `{}` did not match in {}
                                    query: {:#?}, 
                                    values: {:?}, 
                                    sqlite: {:?}, 
                                    db: {}",
                                regex,
                                s,
                                self.inner.statement,
                                values,
                                expected,
                                db_path,
                            );
                        }
                        _ => return Err(anyhow!("only expected Text Value for regex test")),
                    }
                }
            }
            return Ok(());
        }

        return_if_neq!(
            self.inner.values,
            expected,
            "
            query: {:?}, 
            values: {:?}, 
            sqlite: {:?}, 
            db: {}",
            self.inner.statement,
            self.inner.values,
            expected,
            db_path
        );
        Ok(())
    }

    fn exec_sql_limbo(
        &self,
        db: TempDatabase,
        limbo_conn: std::rc::Rc<limbo_core::Connection>,
        db_path: Option<&Path>,
    ) -> anyhow::Result<()> {
        let db_path = db_path.as_ref().map(|db_path| db_path.to_string_lossy());
        let db_path = db_path.unwrap_or(std::borrow::Cow::Borrowed("memory"));

        let expected = match &self.inner.statement {
            Statement::Single(sql) => limbo_exec_rows(&db, &limbo_conn, sql),

            Statement::Many(sql_queries) => {
                return_if_false!(!sql_queries.is_empty());
                sql_queries
                    .into_iter()
                    .map(|sql| limbo_exec_rows(&db, &limbo_conn, sql))
                    .reduce(|acc, val| match (acc, val) {
                        (Ok(mut acc), Ok(val)) => {
                            acc.extend(val);
                            Ok(acc)
                        }
                        (Err(e), _) | (_, Err(e)) => Err(e),
                    })
                    .unwrap()
            }
        };

        if matches!(self.inner.mode, TestMode::Error) {
            if !expected.is_err() {
                return Err(anyhow!("no error thrown in Limbo"));
            }
            return Ok(());
        }

        let expected = expected?;

        if let TestKind::Regex(WrappedRegex(regex)) = &self.inner.kind {
            for values in expected.iter() {
                for val in values {
                    match val {
                        Value::Text(s) => {
                            return_if_false!(
                                regex.is_match(s),
                                "regex `{}` did not match in {}
                                    query: {:#?}, 
                                    values: {:?}, 
                                    limbo: {:?}, 
                                    db: {}",
                                regex,
                                s,
                                self.inner.statement,
                                values,
                                expected,
                                db_path,
                            );
                        }
                        _ => return Err(anyhow!("only expected Text Value for regex test")),
                    }
                }
            }
            return Ok(());
        }

        return_if_neq!(
            self.inner.values,
            expected,
            "
            query: {:?}, 
            values: {:?}, 
            limbo: {:?}, 
            db: {}",
            self.inner.statement,
            self.inner.values,
            expected,
            db_path
        );
        Ok(())
    }
}

pub(crate) fn sqlite_exec_rows(
    conn: &rusqlite::Connection,
    query: &str,
) -> anyhow::Result<Vec<Vec<rusqlite::types::Value>>> {
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query(params![])?;
    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        let mut result = Vec::new();
        for i in 0.. {
            let column: rusqlite::types::Value = match row.get(i) {
                Ok(column) => column,
                Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
                Err(err) => panic!("unexpected rusqlite error: {}", err),
            };
            result.push(column);
        }
        results.push(result)
    }

    Ok(results)
}

pub(crate) fn limbo_exec_rows(
    db: &TempDatabase,
    conn: &Rc<limbo_core::Connection>,
    query: &str,
) -> anyhow::Result<Vec<Vec<rusqlite::types::Value>>> {
    let mut stmt = conn.prepare(query)?;
    let mut rows = Vec::new();
    'outer: loop {
        let row = loop {
            let result = stmt.step()?;
            match result {
                limbo_core::StepResult::Row => {
                    let row = stmt.row().unwrap();
                    break row;
                }
                limbo_core::StepResult::IO => {
                    db.io.run_once()?;
                    continue;
                }
                limbo_core::StepResult::Done => break 'outer,
                r => panic!("unexpected result {:?}: expecting single row", r),
            }
        };
        let row = row
            .get_values()
            .map(|x| match x {
                limbo_core::OwnedValue::Null => rusqlite::types::Value::Null,
                limbo_core::OwnedValue::Integer(x) => rusqlite::types::Value::Integer(*x),
                limbo_core::OwnedValue::Float(x) => rusqlite::types::Value::Real(*x),
                limbo_core::OwnedValue::Text(x) => {
                    rusqlite::types::Value::Text(x.as_str().to_string())
                }
                limbo_core::OwnedValue::Blob(x) => rusqlite::types::Value::Blob(x.to_vec()),
            })
            .collect();
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {}
