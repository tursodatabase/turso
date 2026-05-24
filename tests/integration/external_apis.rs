use crate::common::TempDatabase;
use turso_core::LimboError;

#[turso_macros::test]
fn sql_extension_loading_is_disabled_by_default(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let err = conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));
    Ok(())
}

#[turso_macros::test]
fn sql_extension_loading_flag_is_per_connection(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let enabled_conn = tmp_db.connect_limbo();
    let disabled_conn = tmp_db.connect_limbo();

    enabled_conn.set_load_extension_enabled(true);
    let err = enabled_conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(matches!(err, LimboError::ExtensionError(_)));
    assert!(err.to_string().contains("Extension file not found"));

    let err = disabled_conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));

    enabled_conn.set_load_extension_enabled(false);
    let err = enabled_conn
        .execute("SELECT load_extension('definitely_missing_extension')")
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("runtime extension loading is disabled"));
    Ok(())
}

#[turso_macros::test]
fn direct_connection_extension_loading_bypasses_sql_flag(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let err = conn
        .load_extension("definitely_missing_extension")
        .unwrap_err();

    assert!(matches!(err, LimboError::ExtensionError(_)));
    assert!(!err
        .to_string()
        .contains("runtime extension loading is disabled"));
    Ok(())
}
