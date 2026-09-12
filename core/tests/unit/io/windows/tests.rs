use super::*;

#[test]
fn test_multiple_processes_cannot_open_file() {
    common::tests::test_multiple_processes_cannot_open_file(WindowsIO::new);
}
