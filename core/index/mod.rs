use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use crate::{
    storage::btree::CursorTrait, types::IOResult, Connection, MvStore, Result, Statement,
    StepResult, Value,
};

#[derive(Debug, Clone)]
pub struct IndexConfiguration {
    pub table_name: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub settings: HashMap<String, Value>,
}

pub trait IndexModule: std::fmt::Debug + Send + Sync {
    fn init(&self, configuration: &IndexConfiguration) -> Result<Box<dyn IndexCursor>>;
}

pub const SCRATCH_BTREE_MODULE_NAME: &str = "btree";

#[derive(Debug)]
pub struct TestIndexModule;

pub enum TestIndexCursorCreateState {
    Init,
    Run { stmt: Statement },
}

pub struct TestIndexCursor {
    create_state: TestIndexCursorCreateState,
    configuration: IndexConfiguration,
}

impl IndexModule for TestIndexModule {
    fn init(&self, configuration: &IndexConfiguration) -> Result<Box<dyn IndexCursor>> {
        Ok(Box::new(TestIndexCursor {
            create_state: TestIndexCursorCreateState::Init,
            configuration: configuration.clone(),
        }))
    }
}

impl IndexCursor for TestIndexCursor {
    fn definition(&self) -> IndexDefinition {
        todo!()
    }

    fn create(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        loop {
            match &mut self.create_state {
                TestIndexCursorCreateState::Init => {
                    let sql = format!(
                        "CREATE INDEX {}_scratch ON {} USING {} ({})",
                        self.configuration.index_name,
                        self.configuration.table_name,
                        SCRATCH_BTREE_MODULE_NAME,
                        self.configuration.columns.join(", ")
                    );
                    let stmt = connection.prepare(&sql)?;
                    connection.start_nested();
                    self.create_state = TestIndexCursorCreateState::Run { stmt };
                }
                TestIndexCursorCreateState::Run { stmt } => {
                    // we need to properly track subprograms and propagate result to the root program to make this execution async
                    let result = stmt.run_ignore_rows();
                    connection.end_nested();
                    result?;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    fn destroy(&mut self, connection: &Arc<Connection>) -> Result<()> {
        todo!()
    }

    fn open_read(&mut self) -> Result<IOResult<()>> {
        todo!()
    }

    fn open_write(&mut self) -> Result<IOResult<()>> {
        Ok(IOResult::Done(()))
    }

    fn insert(&mut self, rowid: i64, values: &[&Value]) -> Result<IOResult<()>> {
        todo!()
    }

    fn delete(&mut self, rowid: i64) -> Result<IOResult<()>> {
        todo!()
    }

    fn query_start(&mut self, values: &[&Value]) -> Result<IOResult<()>> {
        todo!()
    }

    fn query_next(&mut self) -> Result<bool> {
        todo!()
    }

    fn query_column(&mut self, position: usize) -> Result<IOResult<&Value>> {
        todo!()
    }

    fn commit(&mut self) -> Result<()> {
        todo!()
    }

    fn rollback(&mut self) -> Result<()> {
        todo!()
    }
}

pub struct IndexDefinition {
    pub patterns: Vec<String>,
}

pub trait IndexCursor {
    fn definition(&self) -> IndexDefinition;

    fn create(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;
    fn destroy(&mut self, connection: &Arc<Connection>) -> Result<()>;

    fn open_read(&mut self) -> Result<IOResult<()>>;
    fn open_write(&mut self) -> Result<IOResult<()>>;

    fn insert(&mut self, rowid: i64, values: &[&Value]) -> Result<IOResult<()>>;
    fn delete(&mut self, rowid: i64) -> Result<IOResult<()>>;
    fn query_start(&mut self, values: &[&Value]) -> Result<IOResult<()>>;
    fn query_next(&mut self) -> Result<bool>;
    fn query_column(&mut self, position: usize) -> Result<IOResult<&Value>>;

    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
}

