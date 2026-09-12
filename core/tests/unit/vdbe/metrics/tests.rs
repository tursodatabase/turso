use super::*;

#[test]
fn test_metrics_merge() {
    let mut m1 = StatementMetrics::new();
    m1.rows_read = 100;
    m1.vm_steps = 50;
    m1.btree_table_seeks = 3;
    m1.btree_index_seeks = 4;
    m1.btree_deferred_seeks = 2;
    m1.hash_join.spill_bytes_written = 42;

    let mut m2 = StatementMetrics::new();
    m2.rows_read = 200;
    m2.vm_steps = 75;
    m2.btree_table_seeks = 5;
    m2.btree_index_seeks = 6;
    m2.btree_deferred_seeks = 1;
    m2.hash_join.spill_bytes_written = 8;
    m2.hash_join.spill_max_partition_bytes = 1024;

    m1.merge(&m2);
    assert_eq!(m1.rows_read, 300);
    assert_eq!(m1.vm_steps, 125);
    assert_eq!(m1.btree_table_seeks, 8);
    assert_eq!(m1.btree_index_seeks, 10);
    assert_eq!(m1.btree_deferred_seeks, 3);
    assert_eq!(m1.hash_join.spill_bytes_written, 50);
    assert_eq!(m1.hash_join.spill_max_partition_bytes, 1024);
}

#[test]
fn test_connection_metrics_high_water() {
    let mut conn_metrics = ConnectionMetrics::new();

    let mut stmt1 = StatementMetrics::new();
    stmt1.vm_steps = 100;
    stmt1.rows_read = 50;
    conn_metrics.record_statement(&stmt1);

    let mut stmt2 = StatementMetrics::new();
    stmt2.vm_steps = 75;
    stmt2.rows_read = 100;
    conn_metrics.record_statement(&stmt2);

    assert_eq!(conn_metrics.max_vm_steps_per_statement, 100);
    assert_eq!(conn_metrics.max_rows_read_per_statement, 100);
    assert_eq!(conn_metrics.total_statements, 2);
    assert_eq!(conn_metrics.aggregate.vm_steps, 175);
    assert_eq!(conn_metrics.aggregate.rows_read, 150);
}
