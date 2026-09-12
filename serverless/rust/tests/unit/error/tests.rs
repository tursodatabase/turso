use super::*;

fn proto_error(code: &str) -> ProtoError {
    ProtoError {
        message: "m".to_string(),
        code: Some(code.to_string()),
        extended_code: None,
    }
}

#[test]
fn busy_snapshot_maps_to_its_own_variant() {
    assert!(matches!(
        Error::from(proto_error("SQLITE_BUSY_SNAPSHOT")),
        Error::BusySnapshot(_)
    ));
    assert!(matches!(
        Error::from(proto_error("SQLITE_BUSY")),
        Error::Busy(_)
    ));
    assert!(matches!(
        Error::from(proto_error("SQLITE_BUSY_RECOVERY")),
        Error::Busy(_)
    ));
    assert!(matches!(
        Error::from(proto_error("SQLITE_CONSTRAINT_UNIQUE")),
        Error::Constraint(_)
    ));
}
