use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use limbo_core::Connection;

use crate::helpers::wrapper::Wrapper;

#[flutter_rust_bridge::frb(init)]
pub fn init_app() {
    flutter_rust_bridge::setup_default_user_utils();
}

lazy_static::lazy_static! {
   pub static ref DATABASE_REGISTRY: Arc<Mutex<HashMap<String, Arc<Wrapper<Connection>>>>> = Arc::new(Mutex::new(HashMap::new()));

}
