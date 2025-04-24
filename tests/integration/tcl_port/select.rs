#[cfg(test)]
mod tests {
    use crate::common::exec_sql;

    #[test]
    fn select_const_1() {
        exec_sql("SELECT 1", vec![vec![1.into()]]);
    }
}
