// 通用 VACUUM 语句生成逻辑，供 limbo/sqlite 共享
pub fn gen_vacuum_stmt() -> Option<String> {
    Some("VACUUM;".to_string())
}
