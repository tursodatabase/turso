use chrono::Utc;
use turso_core::Connection;
use turso_ext::{scalar, ExtensionApi, Value as ExtValue};

pub fn install(conn: &Connection) {
    conn.register_static_extension(register_pg_functions);
}

fn register_pg_functions(ext_api: &mut ExtensionApi) {
    unsafe {
        register_alias(ext_api, c"now".as_ptr());
        register_alias(ext_api, c"clock_timestamp".as_ptr());
        register_alias(ext_api, c"transaction_timestamp".as_ptr());
        register_alias(ext_api, c"statement_timestamp".as_ptr());
    }
}

unsafe fn register_alias(ext_api: &mut ExtensionApi, name: *const std::ffi::c_char) {
    (ext_api.register_scalar_function)(
        ext_api.ctx,
        name,
        -1,
        false,
        0,
        postgres_frontend_now,
        None,
        None,
    );
}

#[scalar(name = "now")]
fn postgres_frontend_now(_args: &[ExtValue]) -> ExtValue {
    let now = Utc::now();
    let formatted = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    ExtValue::from_text(formatted)
}
