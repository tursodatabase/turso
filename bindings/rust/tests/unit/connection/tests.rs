use super::*;
use crate::Builder;

#[tokio::test]
async fn rollback_batch_preserves_both_errors() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    let error = conn
        .rollback_batch::<()>(Error::Error("statement failed".to_string()))
        .await
        .unwrap_err();
    match error {
        Error::BatchRollbackFailed {
            error,
            rollback_error,
        } => {
            assert!(matches!(*error, Error::Error(ref message) if message == "statement failed"));
            assert!(rollback_error.to_string().contains("transaction"));
        }
        other => panic!("expected BatchRollbackFailed, got {other:?}"),
    }
}
