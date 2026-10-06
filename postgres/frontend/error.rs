use turso_core::security::permission::{Action, Object, PermissionDenied, Reason};
use turso_core::LimboError;

/// Returns the message a PostgreSQL client sees for `error`.
pub fn error_message(error: &LimboError) -> String {
    match error {
        LimboError::PermissionDenied(denial) => permission_denied_message(denial),
        error => error.to_string(),
    }
}

fn permission_denied_message(denial: &PermissionDenied) -> String {
    if denial.reason == Reason::Owner {
        return format!("must be owner of {}", object_name(&denial.object));
    }
    match (denial.action, &denial.object) {
        (Action::CreateTemporary, _) => "permission denied to create temporary objects".into(),
        (Action::Create, Object::Role(_)) => "permission denied to create role".into(),
        (Action::Write, Object::Setting(name)) => {
            format!("permission denied to set parameter \"{name}\"")
        }
        (Action::Read, Object::Setting(name)) => format!("permission denied to examine \"{name}\""),
        (Action::Analyze, _) => "permission denied to run ANALYZE".into(),
        (Action::Vacuum, _) => "permission denied to run VACUUM".into(),
        (Action::Reindex, _) => "permission denied to run REINDEX".into(),
        (Action::Optimize, _) => "permission denied to run OPTIMIZE".into(),
        (Action::Attach, _) => "permission denied to attach a database".into(),
        (Action::Detach, _) => "permission denied to detach a database".into(),
        (Action::SetRole, Object::Role(name)) => {
            format!("permission denied to set role \"{name}\"")
        }
        (_, Object::File(_)) => "permission denied to COPY from a file".into(),
        (_, Object::Storage) => "permission denied to access database storage".into(),
        (_, object) => format!("permission denied for {}", object_name(object)),
    }
}

/// Names an object the way PostgreSQL does in its errors. Core's main
/// database is PostgreSQL's `public` schema.
fn object_name(object: &Object) -> String {
    match object {
        Object::Schema(name) if name == "main" => "schema public".to_string(),
        object => object.to_string(),
    }
}
