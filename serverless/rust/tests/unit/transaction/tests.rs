use super::TransactionBehavior;

#[test]
fn behavior_maps_to_begin_statement() {
    assert_eq!(TransactionBehavior::Deferred.begin_sql(), "BEGIN DEFERRED");
    assert_eq!(
        TransactionBehavior::Immediate.begin_sql(),
        "BEGIN IMMEDIATE"
    );
    assert_eq!(
        TransactionBehavior::Exclusive.begin_sql(),
        "BEGIN EXCLUSIVE"
    );
    assert_eq!(
        TransactionBehavior::Concurrent.begin_sql(),
        "BEGIN CONCURRENT"
    );
}
