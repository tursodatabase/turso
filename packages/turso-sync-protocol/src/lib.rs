pub mod database_inner;
pub mod database_tape;
pub mod errors;
pub mod types;
pub mod wal_session;

pub type Result<T> = std::result::Result<T, errors::Error>;
