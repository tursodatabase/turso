use cfg_aliases::cfg_aliases;

fn main() {
    cfg_aliases! {
        injected_yields: { any(feature = "test_helper", feature = "simulator") },
        host_shared_wal: { all(any(unix, target_os = "windows"), target_pointer_width = "64") },
    }
    println!("cargo::rerun-if-changed=build.rs");
}
