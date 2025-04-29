#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(basic_tx_1, ["BEGIN IMMEDIATE", "END"], None);

    db_test!(basic_tx_2, ["BEGIN EXCLUSIVE", "END"], None);

    db_test!(basic_tx_3, ["BEGIN DEFERRED", "END"], None);
}
