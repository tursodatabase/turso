mod select;

#[cfg(test)]
mod tests {
    use cargo_metadata::MetadataCommand;
    use std::path::PathBuf;
    use std::sync::LazyLock;

    pub const TEST_DBS: [&str; 2] = ["testing/testing.db", "testing/testing_norowidalias.db"];

    pub(crate) static WORKSPACE_ROOT: LazyLock<PathBuf> = LazyLock::new(get_workspace_root);

    pub(crate) fn get_workspace_root() -> PathBuf {
        let metadata = MetadataCommand::new().exec().unwrap();
        metadata.workspace_root.into_std_path_buf()
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
