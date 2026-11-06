use std::any::Any;
use std::sync::Arc;

use crate::Connection;

pub type SavepointMemo = Arc<dyn Any + Send + Sync>;

pub trait ConnectionHooks: Send + Sync {
    fn root_statement_ended(&self, conn: &Connection, failed: bool);
    fn transaction_rolled_back(&self, conn: &Connection);
    fn savepoint_created(&self, conn: &Connection) -> SavepointMemo;
    fn rolled_back_to_savepoint(&self, conn: &Connection, memo: &SavepointMemo);
}

/// The state that the owner of a connection gives it when the connection is created.
#[derive(Default)]
pub(crate) struct ConnectionContext {
    /// An arbitrary piece of context to store on the Connection. Can be used for session
    /// or transaction storage.
    pub(crate) value: Option<Arc<dyn Any + Send + Sync>>,
    pub(crate) hooks: Option<Arc<dyn ConnectionHooks>>,
}
