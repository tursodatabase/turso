use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=build.rs");
    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .output()
    {
        if output.status.success() {
            let directory =
                std::path::PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
            let head = directory.join("HEAD");
            println!("cargo:rerun-if-changed={}", head.display());
            println!(
                "cargo:rerun-if-changed={}",
                directory.join("index").display()
            );
            if let Ok(contents) = std::fs::read_to_string(head) {
                if let Some(reference) = contents.strip_prefix("ref: ") {
                    if let Ok(output) = Command::new("git")
                        .args(["rev-parse", "--git-path", reference.trim()])
                        .output()
                    {
                        if output.status.success() {
                            println!(
                                "cargo:rerun-if-changed={}",
                                String::from_utf8_lossy(&output.stdout).trim()
                            );
                        }
                    }
                }
            }
        }
    }
    for (name, program, args) in [
        ("WORKLOAD_GIT_REVISION", "git", vec!["rev-parse", "HEAD"]),
        ("WORKLOAD_RUSTC", "rustc", vec!["--version"]),
    ] {
        let value = Command::new(program)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_else(|| "unknown".into());
        println!("cargo:rustc-env={name}={value}");
    }
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .map_or(true, |o| !o.status.success() || !o.stdout.is_empty());
    println!("cargo:rustc-env=WORKLOAD_GIT_DIRTY={dirty}");
}
