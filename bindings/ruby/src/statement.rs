use magnus::{data_type_builder, DataType, DataTypeFunctions, Error, Ruby, TypedData, Value};
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use turso_sdk_kit::rsapi::TursoStatement;

use crate::error::from_turso_error;
use crate::ERROR_CLASSES;

pub struct Statement {
    inner: RefCell<Option<Box<TursoStatement>>>,
    busy: AtomicBool,
}

impl DataTypeFunctions for Statement {
    fn free(self: Box<Self>) {
        if let Some(mut stmt) = self.inner.borrow_mut().take() {
            let _ = stmt.finalize(None);
        }
    }
}

unsafe impl TypedData for Statement {
    fn class(_ruby: &Ruby) -> magnus::RClass {
        let raw = crate::statement_class();
        magnus::RClass::from_value(unsafe { std::mem::transmute::<usize, Value>(raw) }).unwrap()
    }

    fn data_type() -> &'static DataType {
        static DATA_TYPE: DataType = data_type_builder!(Statement, "statement")
            .free_immediately()
            .build();
        &DATA_TYPE
    }
}

impl Statement {
    pub fn new(inner: Box<TursoStatement>) -> Self {
        Self {
            inner: RefCell::new(Some(inner)),
            busy: AtomicBool::new(false),
        }
    }

    fn with_guard<T>(
        &self,
        f: impl FnOnce(&mut TursoStatement) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if self
            .busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
            return Err(magnus::Error::new(
                classes.busy(),
                "statement is already in use",
            ));
        }

        let result = {
            let mut guard = self.inner.borrow_mut();
            let stmt = guard.as_deref_mut().ok_or_else(|| {
                let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
                magnus::Error::new(classes.base(), "statement has been finalized")
            })?;
            f(stmt)
        };

        self.busy.store(false, Ordering::Release);
        result
    }

    pub fn parameter_count(&self) -> Result<u32, Error> {
        self.with_guard(|stmt| Ok(stmt.parameters_count() as u32))
    }

    pub fn column_count(&self) -> Result<u32, Error> {
        self.with_guard(|stmt| Ok(stmt.column_count() as u32))
    }

    pub fn column_name(&self, index: u32) -> Result<String, Error> {
        self.with_guard(|stmt| {
            stmt.column_name(index as usize).map_err(|e| {
                let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
                from_turso_error(e, classes)
            })
        })
    }

    pub fn bind_positional(&self, args: magnus::RArray) -> Result<(), Error> {
        self.with_guard(|stmt| {
            let len = args.len();
            for idx in 0..len {
                let value: Value = args.entry(idx as isize)?;
                let turso_value =
                    crate::value::to_turso_value(unsafe { &Ruby::get_unchecked() }, value)?;
                stmt.bind_positional(idx + 1, turso_value).map_err(|e| {
                    let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
                    from_turso_error(e, classes)
                })?;
            }
            Ok(())
        })
    }

    pub fn step(&self) -> Result<u32, Error> {
        self.with_guard(|stmt| {
            stmt.step(None)
                .map(|status| match status {
                    turso_sdk_kit::rsapi::TursoStatusCode::Done => 0,
                    turso_sdk_kit::rsapi::TursoStatusCode::Row => 1,
                    turso_sdk_kit::rsapi::TursoStatusCode::Io => 2,
                })
                .map_err(|e| {
                    let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
                    from_turso_error(e, classes)
                })
        })
    }

    pub fn execute(&self) -> Result<i64, Error> {
        self.with_guard(|stmt| {
            stmt.execute(None)
                .map(|result| result.rows_changed as i64)
                .map_err(|e| {
                    let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
                    from_turso_error(e, classes)
                })
        })
    }

    pub fn row(&self) -> Result<magnus::RArray, Error> {
        self.with_guard(|stmt| {
            let ruby = unsafe { Ruby::get_unchecked() };
            let count = stmt.column_count();
            let ary = ruby.ary_new();
            for i in 0..count {
                let value = stmt.row_value(i).map_err(|e| {
                    let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
                    from_turso_error(e, classes)
                })?;
                let rv = crate::value::to_ruby_value(&ruby, &value)?;
                ary.push(rv)?;
            }
            Ok(ary)
        })
    }

    pub fn finalize(&self) -> Result<(), Error> {
        let mut guard = self.inner.borrow_mut();
        if let Some(mut stmt) = guard.take() {
            let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
            stmt.finalize(None)
                .map_err(|e| from_turso_error(e, classes))?;
        }
        Ok(())
    }

    pub fn reset(&self) -> Result<(), Error> {
        self.with_guard(|stmt| {
            stmt.reset().map_err(|e| {
                let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
                from_turso_error(e, classes)
            })
        })
    }

    pub fn n_change(&self) -> i64 {
        let guard = self.inner.borrow();
        guard.as_ref().map(|s| s.n_change()).unwrap_or(0)
    }

    pub fn column_decltype(&self, index: u32) -> Result<Option<String>, Error> {
        self.with_guard(|stmt| {
            Ok(stmt.column_decltype(index as usize))
        })
    }
}
