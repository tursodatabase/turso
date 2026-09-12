use super::*;

#[test]
fn register_and_retrieve() {
    let io = Arc::new(MemoryIO::new());
    register_io("ioreg::retrieve", io).unwrap();
    assert!(get_registered_io("ioreg::retrieve").is_some());
    assert!(get_registered_io("nonexistent").is_none());
    unregister_io("ioreg::retrieve");
}

#[test]
fn re_register_replaces() {
    let io1 = Arc::new(MemoryIO::new());
    let io2 = Arc::new(MemoryIO::new());
    register_io("ioreg::replace", io1).unwrap();
    register_io("ioreg::replace", io2).unwrap();
    let count = list_registered_io()
        .into_iter()
        .filter(|n| n == "ioreg::replace")
        .count();
    assert_eq!(count, 1, "should not duplicate entries");
    unregister_io("ioreg::replace");
}

#[test]
fn unregister_returns_false_for_missing() {
    assert!(!unregister_io("ioreg::never_registered"));
}

#[test]
fn unregister_removes() {
    let io = Arc::new(MemoryIO::new());
    register_io("ioreg::removable", io).unwrap();
    assert!(unregister_io("ioreg::removable"));
    assert!(get_registered_io("ioreg::removable").is_none());
}

#[test]
fn list_includes_registered() {
    let io = Arc::new(MemoryIO::new());
    register_io("ioreg::listed", io).unwrap();
    assert!(list_registered_io().contains(&"ioreg::listed".to_string()));
    unregister_io("ioreg::listed");
}

#[test]
fn empty_name_returns_error() {
    let result = register_io("", Arc::new(MemoryIO::new()));
    assert!(result.is_err());
}
