use std::sync::Arc;

use flutter_rust_bridge::{frb, RustAutoOpaqueNom};
pub use turso_core::{Connection, Database};

use crate::{
    api::statement::LibsqlStatement,
    helpers::{
        params::Params,
        result::{ExecuteResult, QueryResult},
        wrapper::Wrapper,
    },
};

#[frb(opaque)]
pub struct LibsqlConnection {
    inner: RustAutoOpaqueNom<Wrapper<Arc<Connection>>>,
    database: RustAutoOpaqueNom<Wrapper<Arc<Database>>>,
}

impl LibsqlConnection {
    pub fn new(
        connection: Wrapper<Arc<Connection>>,
        database: Wrapper<Arc<Database>>,
    ) -> LibsqlConnection {
        LibsqlConnection {
            inner: RustAutoOpaqueNom::new(connection),
            database: RustAutoOpaqueNom::new(database),
        }
    }

    pub async fn query(&self, sql: String, params: Params) -> QueryResult {
        self.prepare(sql).await.query(params).await
    }

    pub async fn execute(&self, sql: String, params: Params) -> ExecuteResult {
        self.prepare(sql).await.execute(params).await
    }

    pub async fn prepare(&self, sql: String) -> LibsqlStatement {
        let statement = self.inner.try_write().unwrap().prepare(&sql).unwrap();
        LibsqlStatement::new(
            Wrapper { inner: statement },
            Wrapper {
                inner: self.inner.try_write().unwrap().to_owned(),
            },
        )
    }
}
