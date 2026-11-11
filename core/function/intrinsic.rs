pub(crate) mod constructors;
pub(crate) mod control;

use super::scalar::ScalarFunctionMetadata;
pub use crate::translate::expr::scalar::FunctionCompileContext;
use crate::Result;
use std::fmt::Debug;

pub trait IntrinsicFunction: Debug + Send + Sync {
    fn metadata(&self) -> &ScalarFunctionMetadata;
    fn is_internal(&self) -> bool {
        false
    }
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize>;
}
