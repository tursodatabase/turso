use crate::{ast, to_sql_string::ToSqlString};

impl ToSqlString for ast::CreateVirtualTable {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "CREATE VIRTUAL TABLE {}{} USING {}{};",
            if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.tbl_name.to_sql_string(context),
            self.module_name.0,
            self.args
                .as_ref()
                .map_or("".to_string(), |args| format!("({})", args.join(", ")))
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    to_sql_string_test!(
        test_create_virtual_table_fts5_basic,
        "CREATE VIRTUAL TABLE docs USING fts5(title, content);"
    );

    to_sql_string_test!(
        test_create_virtual_table_fts5_tokenizer,
        "CREATE VIRTUAL TABLE docs USING fts5(title, content, tokenize = 'porter');"
    );

    to_sql_string_test!(
        test_create_virtual_table_fts5_unindexed,
        "CREATE VIRTUAL TABLE docs USING fts5(title, content, metadata UNINDEXED);"
    );

    to_sql_string_test!(
        test_create_virtual_table_fts5_prefix,
        "CREATE VIRTUAL TABLE docs USING fts5(title, content, tokenize = 'unicode61', prefix = '2 4');"
    );

    to_sql_string_test!(
        test_create_virtual_table_fts5_contentless,
        "CREATE VIRTUAL TABLE docs USING fts5(title, content, content = '');"
    );

    to_sql_string_test!(
        test_create_virtual_table_fts5_external_content,
        "CREATE VIRTUAL TABLE docs_fts USING fts5(title, content, content = 'documents');"
    );

    to_sql_string_test!(
        test_create_virtual_table_rtree,
        "CREATE VIRTUAL TABLE geo USING rtree(id, min_x, max_x, min_y, max_y);"
    );

    to_sql_string_test!(
        test_create_virtual_table_rtree_aux,
        "CREATE VIRTUAL TABLE geo USING rtree(id, min_x, max_x, min_y, max_y, +name TEXT, +category INTEGER);"
    );

    to_sql_string_test!(
        test_create_virtual_table_if_not_exists,
        "CREATE VIRTUAL TABLE IF NOT EXISTS docs USING fts5(title, content);"
    );

    to_sql_string_test!(
        test_create_virtual_table_fts4,
        "CREATE VIRTUAL TABLE docs USING fts4(title, content, matchinfo = 'fts3');"
    );

    to_sql_string_test!(
        test_create_virtual_table_fts5_detail,
        "CREATE VIRTUAL TABLE docs USING fts5(title, body TEXT, detail = 'none');"
    );

    to_sql_string_test!(
        test_create_virtual_table_schema,
        "CREATE VIRTUAL TABLE main.docs USING fts5(title, content);"
    );
}
