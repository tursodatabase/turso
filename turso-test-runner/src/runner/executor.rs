use crate::backends::{BackendError, QueryResult, SqlBackend};
use crate::comparison::{compare, ComparisonResult};
use crate::parser::ast::{DatabaseConfig, TestCase};
use std::collections::HashMap;
use std::sync::Arc;

/// Executor for running individual tests
pub struct TestExecutor<B: SqlBackend> {
    backend: Arc<B>,
}

impl<B: SqlBackend> TestExecutor<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }

    /// Execute a single test case
    pub async fn execute(
        &self,
        db_config: &DatabaseConfig,
        test: &TestCase,
        setups: &HashMap<String, String>,
    ) -> Result<ExecutionResult, BackendError> {
        // Create database instance
        let mut db = self.backend.create_database(db_config).await?;

        // Run setups in order (using execute_setup which buffers for memory databases)
        for setup_name in &test.setups {
            if let Some(setup_sql) = setups.get(setup_name) {
                if let Err(e) = db.execute_setup(setup_sql).await {
                    let _ = db.close().await;
                    return Ok(ExecutionResult::SetupFailed {
                        setup_name: setup_name.clone(),
                        error: e.to_string(),
                    });
                }
            } else {
                let _ = db.close().await;
                return Ok(ExecutionResult::SetupNotFound {
                    setup_name: setup_name.clone(),
                });
            }
        }

        // Execute test SQL
        let result = db.execute(&test.sql).await?;

        // Close database
        let _ = db.close().await;

        // Compare result
        let comparison = compare(&result, &test.expectation);

        Ok(ExecutionResult::Completed {
            result,
            comparison,
        })
    }

    /// Execute SQL directly (for debugging/REPL)
    pub async fn execute_sql(
        &self,
        db_config: &DatabaseConfig,
        sql: &str,
    ) -> Result<QueryResult, BackendError> {
        let mut db = self.backend.create_database(db_config).await?;
        let result = db.execute(sql).await?;
        let _ = db.close().await;
        Ok(result)
    }
}

/// Result of executing a test
#[derive(Debug)]
pub enum ExecutionResult {
    /// Test completed (may have passed or failed comparison)
    Completed {
        result: QueryResult,
        comparison: ComparisonResult,
    },
    /// Setup block failed
    SetupFailed { setup_name: String, error: String },
    /// Setup block not found
    SetupNotFound { setup_name: String },
}

impl ExecutionResult {
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            ExecutionResult::Completed {
                comparison: ComparisonResult::Match,
                ..
            }
        )
    }

    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            ExecutionResult::Completed {
                comparison: ComparisonResult::Mismatch { .. },
                ..
            }
        )
    }

    pub fn is_error(&self) -> bool {
        matches!(
            self,
            ExecutionResult::SetupFailed { .. } | ExecutionResult::SetupNotFound { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_result_states() {
        let success = ExecutionResult::Completed {
            result: QueryResult::success(vec![]),
            comparison: ComparisonResult::Match,
        };
        assert!(success.is_success());
        assert!(!success.is_failure());
        assert!(!success.is_error());

        let failure = ExecutionResult::Completed {
            result: QueryResult::success(vec![]),
            comparison: ComparisonResult::mismatch("mismatch"),
        };
        assert!(!failure.is_success());
        assert!(failure.is_failure());
        assert!(!failure.is_error());

        let setup_error = ExecutionResult::SetupFailed {
            setup_name: "users".to_string(),
            error: "syntax error".to_string(),
        };
        assert!(!setup_error.is_success());
        assert!(!setup_error.is_failure());
        assert!(setup_error.is_error());
    }
}
