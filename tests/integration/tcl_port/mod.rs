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
mod insert;
mod join;
mod json;
mod like;
mod math;
mod offset;
mod orderby;
mod pragma;
mod scalar_functions;
mod scalar_functions_datetime;
mod scalar_functions_printf;
mod select;
mod subquery;

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
                    let tester = crate::common::SqlTester::single($statement, ::limbo_tests_macros::sqlite_values!($expected));
                $(
                    tester.exec_sql(Some($crate::tcl_port::tests::WORKSPACE_ROOT.join($db_path)));
                )*
            }
        };
        (memory, $name:ident, $statement:literal, $expected:expr) => {
            #[test]
            fn $name() {
                let tester = crate::common::SqlTester::single($statement, ::limbo_tests_macros::sqlite_values!($expected));
                tester.exec_sql(None);
            }
        };
        ($name:ident, $statement:literal, $expected:expr) => {
            #[test]
            fn $name() {
                let tester = crate::common::SqlTester::single($statement, ::limbo_tests_macros::sqlite_values!($expected));
                for db_path in $crate::tcl_port::tests::TEST_DBS {
                    tester.exec_sql(Some($crate::tcl_port::tests::WORKSPACE_ROOT.join(db_path)));
                }
            }
        };
        ($name:ident, [$($statement:literal),*], $expected:expr) => {
            #[test]
            fn $name() {
                for db_path in $crate::tcl_port::tests::TEST_DBS {
                    let queries = vec![$($statement),*];
                    let tester = crate::common::SqlTester::many(queries, ::limbo_tests_macros::sqlite_values!($expected));
                    tester.exec_sql(Some($crate::tcl_port::tests::WORKSPACE_ROOT.join(db_path)));
                }
            }
        };
        (memory, $name:ident, [$($statement:literal),*], $expected:expr) => {
            #[test]
            fn $name() {
                let queries = vec![$($statement),*];
                let tester = crate::common::SqlTester::many(queries, ::limbo_tests_macros::sqlite_values!($expected));
                tester.exec_sql(None);
            }
        };
        ($name:ident, $statement:literal) => {
            #[test]
            fn $name() {
                for db_path in $crate::tcl_port::tests::TEST_DBS {
                    let tester = crate::common::SqlTester::single($statement, ::limbo_tests_macros::sqlite_values!(None));
                    tester.exec_sql(Some($crate::tcl_port::tests::WORKSPACE_ROOT.join(db_path)));
                }
            }
        };
        (memory_expect_error, $name:ident, [$($statement:literal),*]) => {
            #[test]
            fn $name() {
                let queries = vec![$($statement),*];
                let tester = crate::common::SqlTester::memory_error(queries);
                tester.exec_sql(None);
            }
        };
        (regex, $name:ident, $statement:literal, $expected:expr) => {
            #[test]
            fn $name() {
                let tester = crate::common::SqlTester::regex($statement, ::regex::Regex::new($expected).unwrap());
                tester.exec_sql(None);
            }
        };
    }
}
