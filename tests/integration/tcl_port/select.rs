#[cfg(test)]
mod tests {
    use cargo_metadata::{Error, MetadataCommand};
    use std::path::PathBuf;

    use crate::common::exec_sql;

    fn get_workspace_root() -> Result<PathBuf, Error> {
        let metadata = MetadataCommand::new().exec()?;
        Ok(metadata.workspace_root.into_std_path_buf())
    }

    #[test]
    fn select_const_1() {
        let root = get_workspace_root().unwrap();

        exec_sql(
            root.join("testing/testing.db"),
            "SELECT 1",
            vec![vec![1.into()]],
        );
    }
}
