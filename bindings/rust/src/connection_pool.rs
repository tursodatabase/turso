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
struct InnerConnectionPool {
    pool: Arc<Mutex<Vec<Connection>>>,
}

#[derive(Clone)]
pub(crate) struct ConnectionPool {
    inner_pool: Option<InnerConnectionPool>,
}

impl ConnectionPool {
    pub(crate) fn new(active_pool: bool) -> Self {
        match active_pool {
            true => {
                let inner_pool = InnerConnectionPool {
                    pool: Arc::new(Mutex::new(Vec::with_capacity(POOL_SIZE))),
                };
                ConnectionPool {
                    inner_pool: Some(inner_pool),
                }
            }
            false => ConnectionPool { inner_pool: None },
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        match &self.inner_pool {
            Some(_) => {
                return true;
            }
            None => return false,
        };
    }

    pub(crate) fn get(&self) -> Option<Connection> {
        match &self.inner_pool {
            Some(p) => {
                let mut pool = p.pool.lock().unwrap();
                return pool.pop();
            }
            None => return None,
        };
    }

    pub(crate) fn add(&self, obj: Connection) {
        if let Some(p) = &self.inner_pool {
            let mut pool = p.pool.lock().unwrap();
            //if &self.available_connections() >= &pool.capacity() {
            //    let _ = &pool.reserve(&self.available_connections() * 10);
            //}

            pool.push(obj);
        }
    }

    // probably only used for testing and potentially for tuning the size
    #[allow(dead_code)]
    pub(crate) fn available_connections(&self) -> usize {
        if let Some(p) = &self.inner_pool {
            p.pool.lock().unwrap().len()
        } else {
            0
        }
    }
}
