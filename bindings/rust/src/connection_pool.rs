// Connection Pool for the Turso Rust Binding
//
// It is transparent, one ConnectionPool per Database
// if the ConnectionPool option is set the Database
// creates and holds the pool.
// If not set then Database behaves as before
//
// When connections are dropped (by going out of scope) they will be cleaned and
// returned to the pool.
//
// The pool will be filled lazily. If the ConnectionPool is enabled in the Database
// then it will try to get a Connection from the pool. If there is no Connection
// available then a new Connection will be created by the database. When that
// Connection goes out of scope, and is dropped, the connection gets cleaned and
// added to the pool.

use crate::Connection;
use std::sync::{Arc, Mutex};

const POOL_SIZE: usize = 10;

#[derive(Clone)]
pub(crate) struct ConnectionPool {
    pool: Arc<Mutex<Vec<Connection>>>,
}

impl ConnectionPool {
    pub(crate) fn new() -> Self {
        let pool = ConnectionPool {
            pool: Arc::new(Mutex::new(Vec::with_capacity(POOL_SIZE))),
        };
        pool
    }

    pub(crate) fn get(&self) -> Option<Connection> {
        let mut pool = self.pool.lock().unwrap();
        pool.pop()
    }

    pub(crate) fn add(&self, obj: Connection) {
        let mut pool = self.pool.lock().unwrap();
        pool.push(obj);
    }

    // probably only used for testing and potentially for tuning the size
    pub(crate) fn available_connections(&self) -> usize {
        self.pool.lock().unwrap().len()
    }
}
