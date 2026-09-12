use super::*;

#[test]
fn closes_file_before_removing_temp_directory() {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().expect("platform IO must initialize"));
    let temp_file = TempFile::new(&io).expect("temporary file must open");
    let temp_dir = temp_file
        .temp_dir
        .as_ref()
        .expect("filesystem temporary file must retain its directory")
        .path()
        .to_owned();

    assert!(temp_dir.exists());
    drop(temp_file);
    assert!(!temp_dir.exists());
}
