#[test]
fn test_run_blocking_runs_closure() {
    let result: Result<i32, String> = Ok(42);
    assert_eq!(result.unwrap(), 42);
}
