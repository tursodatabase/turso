#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(select_avg, "SELECT avg(age) FROM users", 50.396);

    db_test!(select_avg_text, "SELECT avg(first_name) FROM users", 0.0);

    db_test!(select_sum, "SELECT sum(age) FROM users", 503960);

    db_test!(select_sum_text, "SELECT sum(first_name) FROM users", 0.0);

    db_test!(select_total, "SELECT total(age) FROM users", 503960.0);

    db_test!(
        select_total_text,
        "SELECT total(first_name) FROM users WHERE id < 3",
        0.0
    );

    db_test!(
        select_limit,
        "SELECT typeof(id) FROM users LIMIT 1",
        "integer"
    );

    db_test!(select_count, "SELECT count(id) FROM users", 10000);

    db_test!(select_count_2, "SELECT count(*) FROM users", 10000);

    db_test!(
        select_count_constant_true,
        "SELECT count(*) FROM users WHERE true",
        10000
    );

    db_test!(
        select_count_constant_false,
        "SELECT count(*) FROM users WHERE false",
        0
    );

    db_test!(select_max, "SELECT max(age) FROM users", 100);

    db_test!(select_min, "SELECT min(age) FROM users", 1);

    db_test!(select_max_text, "SELECT max(first_name) FROM users", "Zoe");

    db_test!(
        select_mix_text,
        "SELECT min(first_name) FROM users",
        "Aaron"
    );

    db_test!(
        select_group_concat,
        "SELECT group_concat(name) FROM products",
        "hat,cap,shirt,sweater,sweatshirt,shorts,jeans,sneakers,boots,coat,accessories"
    );

    db_test!(
        select_group_concat_with_delimiter,
        "SELECT group_concat(name, ';') FROM products",
        "hat;cap;shirt;sweater;sweatshirt;shorts;jeans;sneakers;boots;coat;accessories"
    );

    db_test!(
        select_group_concat_with_column_delimiter,
        "SELECT group_concat(name, id) FROM products",
        "hat2cap3shirt4sweater5sweatshirt6shorts7jeans8sneakers9boots10coat11accessories"
    );

    db_test!(
        select_string_agg_with_delimiter,
        "SELECT string_agg(name, ',') FROM products",
        "hat,cap,shirt,sweater,sweatshirt,shorts,jeans,sneakers,boots,coat,accessories"
    );

    db_test!(
        select_string_agg_with_column_delimiter,
        "SELECT string_agg(name, id) FROM products",
        "hat2cap3shirt4sweater5sweatshirt6shorts7jeans8sneakers9boots10coat11accessories"
    );

    db_test!(
        select_agg_unary_negative,
        "SELECT -max(age) FROM users",
        -100
    );

    db_test!(
        select_agg_unary_positive,
        "SELECT +max(age) FROM users",
        100
    );

    db_test!(
        select_agg_binary_unary_negative,
        "SELECT min(age) + -max(age) FROM users",
        -99
    );

    db_test!(
        select_agg_binary_unary_positive,
        "SELECT min(age) + +max(age) FROM users",
        101
    );

    db_test!(
        select_non_agg_cols_should_be_not_null,
        "SELECT id, first_name, sum(age) FROM users LIMIT 1",
        [1, "Jamie", 503960]
    );

    db_test!(
        select_with_group_by_and_agg_1,
        "SELECT id, first_name, avg(age) FROM users group by last_name limit 1",
        [274, "Debra", 66.25]
    );

    db_test!(
        select_with_group_by_and_agg_2,
        "select first_name, last_name from users where state = 'AL' group by last_name limit 10",
        [
            ["Jay", "Acosta"],
            ["Daniel", "Adams"],
            ["Aaron", "Baker"],
            ["Sharon", "Becker"],
            ["Kim", "Berg"],
            ["Donald", "Bishop"],
            ["Brian", "Bradford"],
            ["Jesus", "Bradley"],
            ["John", "Brown"],
            ["Hunter", "Burke"]
        ]
    );

    db_test!(
        select_agg_json_array,
        "SELECT json_group_array(name) FROM products",
        "[\"hat\",\"cap\",\"shirt\",\"sweater\",\"sweatshirt\",\"shorts\",\"jeans\",\"sneakers\",\"boots\",\"coat\",\"accessories\"]"
    );

    db_test!(
        select_agg_json_array_object,
        "SELECT json_group_array(json_object('name', name)) FROM products",
        "[{\"name\":\"hat\"},{\"name\":\"cap\"},{\"name\":\"shirt\"},{\"name\":\"sweater\"},{\"name\":\"sweatshirt\"},{\"name\":\"shorts\"},{\"name\":\"jeans\"},{\"name\":\"sneakers\"},{\"name\":\"boots\"},{\"name\":\"coat\"},{\"name\":\"accessories\"}]"
    );
}
