use std::fs;
use std::path::PathBuf;

fn main() {
    // Enable page-poisoning by default ONLY in debug builds
    #[cfg(debug_assertions)]
    {
        // Only enable if the feature is not explicitly set (either enabled or disabled)
        if std::env::var("CARGO_FEATURE_PAGE_POISONING").is_err() {
            println!("cargo:rustc-cfg=feature=\"page-poisoning\"");
        }
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let built_file = out_dir.join("built.rs");

    built::write_built_file().expect("Failed to acquire build-time information");

    // So that we don't have to transform at runtime
    let sqlite_date = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    fs::write(
        &built_file,
        format!(
            "{}\npub const BUILT_TIME_SQLITE: &str = \"{}\";\n",
            fs::read_to_string(&built_file).unwrap(),
            sqlite_date
        ),
    )
    .expect("Failed to append to built file");
}
