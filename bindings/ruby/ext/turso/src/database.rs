use magnus::{function, method, Error, Module, Object, Ruby};
use std::sync::Arc;
use turso_sdk_kit::rsapi::{TursoDatabase, TursoDatabaseConfig};

use crate::connection::RbConnection;
use crate::errors::map_turso_error;

#[magnus::wrap(class = "Turso::Database", free_immediately, size)]
pub struct RbDatabase {
    inner: Arc<TursoDatabase>,
}

impl RbDatabase {
    pub fn open(path: String) -> Result<Self, Error> {
        let config = TursoDatabaseConfig {
            path,
            experimental_features: None,
            async_io: false,
            io: None,
            db_file: None,
        };

        let db = TursoDatabase::new(config);
        db.open().map_err(map_turso_error)?;

        Ok(Self { inner: db })
    }

    pub fn connect(&self) -> Result<RbConnection, Error> {
        let conn = self.inner.connect().map_err(map_turso_error)?;
        Ok(RbConnection::new(conn))
    }

    pub fn close(&self) -> Result<(), Error> {
        // Nope,we don't do that here! but just adding for sake of adding
        Ok(())
    }
}

pub fn define_database(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let class = module.define_class("Database", ruby.class_object())?;
    class.define_singleton_method("open", function!(RbDatabase::open, 1))?;
    class.define_method("connect", method!(RbDatabase::connect, 0))?;
    class.define_method("close", method!(RbDatabase::close, 0))?;
    Ok(())
}
