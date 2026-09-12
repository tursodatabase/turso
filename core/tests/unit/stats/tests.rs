use super::parse_stat_numbers;

#[test]
fn parse_stat_numbers_basic() {
    assert_eq!(parse_stat_numbers("10 5 3 1").unwrap(), vec![10, 5, 3, 1]);
    assert_eq!(parse_stat_numbers("  42\t7 ").unwrap(), vec![42, 7]);
    assert!(parse_stat_numbers("abc 1").is_none());
}
