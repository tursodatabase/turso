use magnus::TypedData;
use turso_sdk_kit::rsapi::TursoStatement;

pub struct Statement {
    _inner: Box<TursoStatement>,
}

unsafe impl TypedData for Statement {
    fn class_name() -> &'static str {
        "Turso::Statement"
    }

    fn data_type() -> magnus::DataType {
        magnus::DataType::new(Self::class_name())
    }
}

impl Statement {
    pub fn new(inner: Box<TursoStatement>) -> Self {
        Self { _inner: inner }
    }
}
