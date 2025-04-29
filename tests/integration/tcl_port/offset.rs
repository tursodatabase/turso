#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(
        select_offset_0,
        "SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 0",
        [1]
    );

    db_test!(
        select_offset_1,
        "SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 1",
        [2]
    );

    db_test!(
        select_offset_negative,
        "SELECT id FROM users ORDER BY id LIMIT 1 OFFSET -1",
        [1]
    );

    db_test!(
        select_offset_0_groupby,
        "SELECT COUNT(*) FROM users GROUP BY STATE ORDER BY STATE LIMIT 5 OFFSET 0",
        [[168], [166], [162], [153], [166]]
    );

    db_test!(
        select_offset_1_groupby,
        "SELECT COUNT(*) FROM users GROUP BY STATE ORDER BY STATE LIMIT 5 OFFSET 1",
        [[166], [162], [153], [166], [170]]
    );

    db_test!(
        select_offset_subquery,
        "SELECT id, first_name, age FROM (SELECT id, first_name, age FROM users ORDER BY id ASC LIMIT 5 OFFSET 2) ORDER BY id DESC",
        [
            [7, "Aimee", 24],
            [6, "Nicholas", 89],
            [5, "Edward", 15],
            [4, "Jennifer", 33],
            [3, "Tommy", 18]
        ]
    );
}
