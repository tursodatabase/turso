use async_trait::async_trait;
use sqltest::cli::{BackendRun, Flavor};
use sqltest::{BackendError, FileResult};
use sqltest_pg::embedded::TursopgEmbeddedBackend;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    sqltest::cli::run(&PgFlavor).await
}

struct PgFlavor;

#[async_trait(?Send)]
impl Flavor for PgFlavor {
    fn bin_name(&self) -> &'static str {
        "sqltest-pg"
    }

    fn extra_backends(&self) -> &'static [&'static str] {
        &["tursopg-embedded"]
    }

    fn default_backend(&self) -> &'static str {
        "tursopg-embedded"
    }

    async fn run_backend(
        &self,
        name: &str,
        run: BackendRun<'_>,
    ) -> Option<Result<Vec<FileResult>, BackendError>> {
        if name != "tursopg-embedded" {
            return None;
        }
        let mut backend = TursopgEmbeddedBackend::new().with_timeout(run.timeout);
        if let Some(resolver) = run.resolver.clone() {
            backend = backend.with_default_db_resolver(resolver);
        }
        Some(run.run(backend).await)
    }
}
