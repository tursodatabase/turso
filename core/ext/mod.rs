#[cfg(feature = "uuid")]
mod uuid;
#[cfg(feature = "uuid")]
pub use uuid::{exec_ts_from_uuid7, exec_uuid, exec_uuidblob, exec_uuidstr, UuidFunc};

#[derive(Debug, Clone, PartialEq)]
pub enum ExternalFunc {
    #[cfg(feature = "uuid")]
    Uuid(UuidFunc),
}

#[allow(unreachable_patterns)] // TODO: remove when more extension funcs added
impl std::fmt::Display for ExternalFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "uuid")]
            Self::Uuid(uuidfn) => write!(f, "{}", uuidfn),
            _ => write!(f, "unknown"),
        }
    }
}

#[allow(unreachable_patterns)]
impl ExternalFunc {
    pub fn resolve_function(name: &str, num_args: usize) -> Option<ExternalFunc> {
        match name {
            #[cfg(feature = "uuid")]
            name => UuidFunc::resolve_function(name, num_args),
            _ => None,
        }
    }
}

pub fn init(db: &mut crate::Database) {
    #[cfg(feature = "uuid")]
    uuid::init(db);
}
