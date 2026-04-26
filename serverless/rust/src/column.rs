/// Column information.
#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    decl_type: Option<String>,
}

impl Column {
    pub fn new(name: String, decl_type: Option<String>) -> Self {
        Self { name, decl_type }
    }

    /// Return the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the declared type of the column.
    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type.as_deref()
    }
}
