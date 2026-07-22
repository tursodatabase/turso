/// Column information for a result set.
#[derive(Debug, Clone)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) decl_type: Option<String>,
}

impl Column {
    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the declared type of the column, when known.
    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type.as_deref()
    }
}
