use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

fn main() {
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());

    if profile == "debug" {
        println!("cargo::rerun-if-changed=build.rs");
    }

    // Tell cargo to rebuild when git HEAD changes, so sqlite_source_id() stays current.
    // We use `git rev-parse --git-dir` instead of hardcoding ".git" to support worktrees,
    // where the git directory lives elsewhere (e.g., ../.git/worktrees/my-worktree).
    // Silently ignored if git unavailable (e.g., building from tarball) - that's fine,
    // we'll just rebuild every time which is the safe default.
    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .output()
    {
        if output.status.success() {
            if let Ok(git_dir) = String::from_utf8(output.stdout) {
                let git_dir = git_dir.trim();
                // HEAD changes on checkout/switch
                println!("cargo::rerun-if-changed={git_dir}/HEAD");
                // The ref file (e.g., refs/heads/main) changes on commit
                if let Ok(head_content) = std::fs::read_to_string(format!("{git_dir}/HEAD")) {
                    if let Some(ref_path) = head_content.strip_prefix("ref: ") {
                        println!("cargo::rerun-if-changed={}/{}", git_dir, ref_path.trim());
                    }
                }
            }
        }
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let built_file = out_dir.join("built.rs");

    built::write_built_file().expect("Failed to acquire build-time information");

    // We shell out to git instead of using libgit2 (via the `built` crate's git2 feature)
    // because libgit2-sys adds ~18s to clean release builds. The git CLI is always available
    // in dev environments and CI. Falls back to None if git unavailable.
    let git_hash = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string());

    let git_hash_code = match git_hash {
        Some(hash) => format!("pub const GIT_COMMIT_HASH: Option<&str> = Some(\"{hash}\");"),
        None => "pub const GIT_COMMIT_HASH: Option<&str> = None;".to_string(),
    };

    // Pre-format the timestamp so sqlite_source_id() doesn't need chrono at runtime
    let sqlite_date = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    fs::write(
        &built_file,
        format!(
            "{}\npub const BUILT_TIME_SQLITE: &str = \"{}\";\n{}\n",
            fs::read_to_string(&built_file).unwrap(),
            sqlite_date,
            git_hash_code
        ),
    )
    .expect("Failed to append to built file");
}
