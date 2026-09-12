use super::*;
use crate::io::common;

#[test]
fn test_multiple_processes_cannot_open_file() {
    common::tests::test_multiple_processes_cannot_open_file(UringIO::new);
}
