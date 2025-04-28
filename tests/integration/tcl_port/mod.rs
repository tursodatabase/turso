mod agg_functions;
mod boolean;
mod changes;
mod coalesce;
mod compare;
mod concat;
mod default_value;
mod delete;
mod drop_table;
mod glob;
mod groupby;
mod select;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::LazyLock;

    pub const TEST_DBS: [&str; 2] = ["testing/testing.db", "testing/testing_norowidalias.db"];

    pub(crate) static WORKSPACE_ROOT: LazyLock<PathBuf> = LazyLock::new(get_workspace_root);

    // hack to get workspace root
    pub(crate) fn get_workspace_root() -> PathBuf {
        // ATTENTION: THIS IS NOT PORTABLE. IF THE FILE STRUCTURE CHANGES OF WHERE THE CARGO.toml FOR THIS
        // CRATE CHANGES THIS WILL BREAK. THE TESTS WILL FAIL.
        // To make this more portable we could colocate the databases inside this crate
        let root: PathBuf = std::env!("CARGO_MANIFEST_DIR").into();
        root.parent().unwrap().into()
    }

    #[macro_export]
    macro_rules! db_test {
        ([$($db_path:literal),*], $name:ident, $statement:literal, $expected:expr) => {
            #[test]
                fn $name() {
                $(
                    $crate::common::exec_sql(
                        $crate::tcl_port::tests::WORKSPACE_ROOT.join($db_path),
                        $statement,
                        ::limbo_tests_macros::sqlite_values!($expected),
                    );
                )*
            }
        };
        (memory, $name:ident, $statement:literal, $expected:expr) => {
            #[test]
            fn $name() {
                $crate::common::exec_sql_memory(
                    $statement,
                    ::limbo_tests_macros::sqlite_values!($expected),
                );
            }
        };
        ($name:ident, $statement:literal, $expected:expr) => {
            #[test]
            fn $name() {
                for db_path in $crate::tcl_port::tests::TEST_DBS {
                    $crate::common::exec_sql(
                        $crate::tcl_port::tests::WORKSPACE_ROOT.join(db_path),
                        $statement,
                        ::limbo_tests_macros::sqlite_values!($expected),
                    );
                }
            }
        };
        ($name:ident, [$($statement:literal),*], [$($expected:expr),*]) => {
            #[test]
            fn $name() {
                for db_path in $crate::tcl_port::tests::TEST_DBS {
                    let queries = vec![$($statement),*];
                    let expected_vals = ::limbo_tests_macros::sqlite_values!($($expected), *);

                    $crate::common::exec_many_sql(
                        $crate::tcl_port::tests::WORKSPACE_ROOT.join(db_path),
                        queries,
                        expected_vals,
                    );
                }
            }
        };
        (memory, $name:ident, [$($statement:literal),*], [$($expected:expr),*]) => {
            #[test]
            fn $name() {
                let queries = vec![$($statement),*];
                let expected_vals = ::limbo_tests_macros::sqlite_values!($($expected), *);

                $crate::common::exec_many_sql_memory(
                    queries,
                    expected_vals,
                );
            }
        };
        ($name:ident, $statement:literal) => {
            #[test]
            fn $name() {
                for db_path in $crate::tcl_port::tests::TEST_DBS {
                    $crate::common::exec_sql(
                        $crate::tcl_port::tests::WORKSPACE_ROOT.join(db_path),
                        $statement,
                        ::limbo_tests_macros::sqlite_values!(None),
                    );
                }
            }
        };
    }
}
