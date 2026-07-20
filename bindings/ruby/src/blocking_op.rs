use rb_sys::rb_thread_call_without_gvl2;
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use crate::error::{from_turso_error, ErrorClasses};
use turso_sdk_kit::rsapi::TursoError;

#[allow(dead_code)]
trait Callable {
    fn call(self: Box<Self>) -> Result<Box<dyn Any + Send + 'static>, TursoError>;
}

impl<T, F> Callable for F
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, TursoError> + Send,
{
    fn call(self: Box<Self>) -> Result<Box<dyn Any + Send + 'static>, TursoError> {
        (*self)().map(|v| Box::new(v) as Box<dyn Any + Send + 'static>)
    }
}

#[allow(dead_code)]
pub fn run_blocking<F, T>(classes: &ErrorClasses, f: F) -> Result<T, magnus::Error>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, TursoError> + Send + 'static,
{
    let mut result: Option<Result<Box<dyn Any + Send + 'static>, TursoError>> = None;

    struct Slot {
        closure: Option<Box<dyn Callable + Send>>,
        result: *mut Option<Result<Box<dyn Any + Send + 'static>, TursoError>>,
    }

    let mut slot = Slot {
        closure: Some(Box::new(f)),
        result: &mut result,
    };

    extern "C" fn trampoline(arg: *mut std::os::raw::c_void) -> *mut std::os::raw::c_void {
        let slot = arg as *mut Slot;
        let slot_ref = unsafe { &mut *slot };
        let closure = slot_ref.closure.take().unwrap();
        let res = catch_unwind(AssertUnwindSafe(|| closure.call()))
            .unwrap_or_else(|_| Err(TursoError::Error("panic in blocking operation".to_string())));
        unsafe {
            *slot_ref.result = Some(res);
        }
        ptr::null_mut()
    }

    unsafe {
        rb_thread_call_without_gvl2(
            Some(trampoline),
            &mut slot as *mut _ as *mut std::os::raw::c_void,
            None,
            ptr::null_mut(),
        );
    }

    result
        .unwrap()
        .map(|boxed| {
            *boxed
                .downcast::<T>()
                .expect("type mismatch in run_blocking")
        })
        .map_err(|e| from_turso_error(e, classes))
}
