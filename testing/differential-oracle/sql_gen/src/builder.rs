//! Builder API for SqlGen.

use std::marker::PhantomData;

use crate::SqlGen;
use crate::capabilities::{Capabilities, Full};
use crate::policy::Policy;
use crate::schema::Schema;

/// Builder for constructing an SqlGen.
pub struct SqlGenBuilder<C: Capabilities = Full> {
    schema: Option<Schema>,
    policy: Policy,
    _cap: PhantomData<C>,
}

impl SqlGenBuilder<Full> {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            schema: None,
            policy: Policy::default(),
            _cap: PhantomData,
        }
    }
}

impl Default for SqlGenBuilder<Full> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Capabilities> SqlGenBuilder<C> {
    /// Set the schema.
    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the policy.
    pub fn policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }

    /// Change the capability set.
    pub fn capabilities<C2: Capabilities>(self) -> SqlGenBuilder<C2> {
        SqlGenBuilder {
            schema: self.schema,
            policy: self.policy,
            _cap: PhantomData,
        }
    }

    /// Build the generator.
    pub fn build(self) -> Result<SqlGen<C>, &'static str> {
        let schema = self.schema.ok_or("schema is required")?;
        Ok(SqlGen::new(schema, self.policy))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, SchemaBuilder, Table};
    use crate::{DmlOnly, SelectOnly};

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build()
    }

    #[test]
    fn test_builder_basic() {
        let generator = SqlGenBuilder::new()
            .schema(test_schema())
            .policy(Policy::default())
            .build();

        assert!(generator.is_ok());
    }

    #[test]
    fn test_builder_without_schema() {
        let result: Result<SqlGen<Full>, _> =
            SqlGenBuilder::new().policy(Policy::default()).build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_with_capabilities() {
        let generator: SqlGen<SelectOnly> = SqlGenBuilder::new()
            .schema(test_schema())
            .capabilities::<SelectOnly>()
            .build()
            .unwrap();

        assert_eq!(generator.schema().tables.len(), 1);
    }

    #[test]
    fn test_builder_change_capabilities() {
        let builder = SqlGenBuilder::new()
            .schema(test_schema())
            .policy(Policy::default());

        // Change from Full to DmlOnly
        let generator: SqlGen<DmlOnly> = builder.capabilities::<DmlOnly>().build().unwrap();

        assert_eq!(generator.schema().tables.len(), 1);
    }

    #[test]
    fn test_builder_custom_policy() {
        let policy = Policy::default()
            .with_max_expr_depth(2)
            .with_null_probability(0.1);

        let generator = SqlGenBuilder::new()
            .schema(test_schema())
            .policy(policy)
            .build()
            .unwrap();

        assert_eq!(generator.policy().max_expr_depth, 2);
    }
}
