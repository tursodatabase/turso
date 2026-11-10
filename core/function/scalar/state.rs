use crate::native_ext::{ScalarFactory, ScalarStep};
use crate::Result;
use std::any::Any;

/// State for one scalar invocation, retained only while I/O is pending.
#[derive(Default)]
pub struct ScalarState {
    call: CallState,
}

#[derive(Default)]
enum CallState {
    #[default]
    None,
    Builtin(Box<dyn Any + Send + Sync>),
    Native(Box<dyn ScalarStep>),
}

impl ScalarState {
    /// Initialize once, then return the same state when execution resumes.
    pub fn get_or_try_init<T: Send + Sync + 'static>(
        &mut self,
        create: impl FnOnce() -> Result<T>,
    ) -> Result<&mut T> {
        if matches!(self.call, CallState::None) {
            self.call = CallState::Builtin(Box::new(create()?));
        }
        let CallState::Builtin(state) = &mut self.call else {
            unreachable!("builtin scalar requires builtin call state");
        };
        Ok(state
            .downcast_mut()
            .expect("scalar call state type changed while resuming"))
    }

    pub(crate) fn native_call(
        &mut self,
        factory: &dyn ScalarFactory,
    ) -> Result<&mut dyn ScalarStep> {
        if matches!(self.call, CallState::None) {
            self.call = CallState::Native(factory.create_call()?);
        }
        let CallState::Native(call) = &mut self.call else {
            unreachable!("native scalar requires native call state");
        };
        Ok(call.as_mut())
    }
}
