use rb_sys::rb_thread_call_without_gvl2;
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use crate::error::{from_turso_error, ErrorClasses};

trait Callable {
    fn call(self: Box<Self>) -> Result<Box<dyn Any + Send + 'static>, turso_sdk_kit::TursoError>;
}

impl<T, F> Callable for F
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, turso_sdk_kit::TursoError> + Send,
{
    fn call(self: Box<Self>) -> Result<Box<dyn Any + Send + 'static>, turso_sdk_kit::TursoError> {
        (*self)().map(|v| Box::new(v) as Box<dyn Any + Send + 'static>)
    }
}

pub fn run_blocking<F, T>(
    classes: &ErrorClasses,
    f: F,
) -> Result<T, magnus::Error>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, turso_sdk_kit::TursoError> + Send + 'static,
{
    let mut result: Option<Result<Box<dyn Any + Send + 'static>, turso_sdk_kit::TursoError>> = None;

    unsafe {
        struct Slot {
            closure: Option<Box<dyn Callable + Send>>,
            result: *mut Option<
                Result<Box<dyn Any + Send + 'static>, turso_sdk_kit::TursoError>,
            >,
        }

        let mut slot = Slot {
            closure: Some(Box::new(f)),
            result: &mut result,
        };

        extern "C" fn trampoline(arg: *mut std::os::raw::c_void) -> *mut std::os::raw::c_void {
            let slot = arg as *mut Slot;
            let slot_ref = &mut *slot;
            let closure = slot_ref.closure.take().unwrap();
            let res = catch_unwind(AssertUnwindSafe(|| closure.call())).unwrap_or_else(|_| {
                Err(turso_sdk_kit::TursoError::Error(
                    "panic in blocking operation".to_string(),
                ))
            });
            unsafe { *slot_ref.result = Some(res); }
            ptr::null_mut()
        }

        rb_thread_call_without_gvl2(
            Some(trampoline),
            &mut slot as *mut _ as *mut std::os::raw::c_void,
            None,
            ptr::null_mut(),
        );
    }

    result
        .unwrap()
        .map(|boxed| *boxed.downcast::<T>().expect("type mismatch in run_blocking"))
        .map_err(|e| from_turso_error(e, classes))
}
