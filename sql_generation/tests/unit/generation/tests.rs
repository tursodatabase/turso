use crate::{
    generation::{GenerationContext, Opts},
    model::table::Table,
};

#[derive(Debug, Clone)]
pub struct TestContext {
    pub opts: Opts,
    pub tables: Vec<Table>,
}

impl Default for TestContext {
    fn default() -> Self {
        // Create a test context with generated columns disabled.
        // Predicate tests create random values for all columns, which doesn't work correctly
        // with generated columns since their values should be computed from expressions.
        let mut ctx = Self {
            opts: Default::default(),
            tables: Default::default(),
        };
        ctx.opts.table.generated_columns.enable = false;
        ctx
    }
}

impl GenerationContext for TestContext {
    fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    fn opts(&self) -> &Opts {
        &self.opts
    }
}
