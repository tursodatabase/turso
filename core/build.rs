use chrono::{TimeZone, Utc};
use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

fn main() {
    // Ensure Cargo reruns when this script or the reproducibility seed changes.
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=SOURCE_DATE_EPOCH");

    // Tell cargo to rebuild when git HEAD changes, so sqlite_source_id() stays current.
    // We use `git rev-parse --git-dir` instead of hardcoding ".git" to support worktrees,
    // where the git directory lives elsewhere (e.g., ../.git/worktrees/my-worktree).
    // Silently ignored if git unavailable (e.g., building from tarball).
    // Resolve git dir dynamically to support worktrees.
    let git_dir = run_git(&["rev-parse", "--git-dir"]).map(PathBuf::from);
    if let Some(git_dir) = git_dir.as_ref() {
        // Common dir holds refs for worktrees; fall back to git_dir if unavailable.
        let git_common_dir = run_git(&["rev-parse", "--git-common-dir"]).map(PathBuf::from);
        let head_path = git_dir.join("HEAD");
        // HEAD changes on checkout/switch
        println!("cargo::rerun-if-changed={}", head_path.display());
        // The ref file (e.g., refs/heads/main) changes on commit
        if let Ok(head_content) = fs::read_to_string(&head_path) {
            if let Some(ref_path) = head_content.strip_prefix("ref: ") {
                let ref_base = git_common_dir.as_deref().unwrap_or(git_dir.as_path());
                let ref_path = ref_base.join(ref_path.trim());
                println!("cargo::rerun-if-changed={}", ref_path.display());
                if !ref_path.exists() {
                    let packed_refs = ref_base.join("packed-refs");
                    println!("cargo::rerun-if-changed={}", packed_refs.display());
                }
            }
        }
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    // Write to a temp file first, then only update built.rs if contents changed.
    let built_file = out_dir.join("built.rs");
    let temp_file = out_dir.join("built.rs.tmp");

    // We shell out to git instead of using libgit2 (via the `built` crate's git2 feature)
    // because libgit2-sys adds ~18s to clean release builds. The git CLI is always available
    // in dev environments and CI. Falls back to None if git unavailable.
    // Commit hash is used for sqlite_source_id() and to derive a stable timestamp.
    let git_hash = run_git(&["rev-parse", "HEAD"]);
    let git_commit_epoch = run_git(&["show", "-s", "--format=%ct", "HEAD"])
        .and_then(|epoch| epoch.parse::<i64>().ok());

    let git_hash_code = match git_hash {
        Some(hash) => format!("pub const GIT_COMMIT_HASH: Option<&str> = Some(\"{hash}\");"),
        None => "pub const GIT_COMMIT_HASH: Option<&str> = None;".to_string(),
    };

    // Honor reproducible-builds if set; otherwise seed it from git commit time.
    let source_date_epoch = env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|epoch| epoch.parse::<i64>().ok());

    if source_date_epoch.is_none() {
        if let Some(epoch) = git_commit_epoch {
            env::set_var("SOURCE_DATE_EPOCH", epoch.to_string());
        }
    }

    // Pre-format the timestamp so sqlite_source_id() doesn't need chrono at runtime.
    // Prefer SOURCE_DATE_EPOCH (reproducible builds), then git commit time, and fall back to now.
    let sqlite_date = source_date_epoch
        .or(git_commit_epoch)
        .and_then(|epoch| Utc.timestamp_opt(epoch, 0).single())
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();

    // Generate built metadata and append our extra constants.
    built::write_built_file_with_opts(&temp_file)
        .expect("Failed to acquire build-time information");
    let built_contents = fs::read_to_string(&temp_file).expect("Failed to read built metadata");
    let new_contents = format!(
        "{built_contents}\npub const BUILT_TIME_SQLITE: &str = \"{sqlite_date}\";\n{git_hash_code}\n"
    );

    // Avoid touching built.rs when content is unchanged to prevent rebuild loops.
    let existing_contents = fs::read_to_string(&built_file).ok();
    if existing_contents.as_deref() != Some(new_contents.as_str()) {
        fs::write(&built_file, new_contents).expect("Failed to write built file");
    }
    let _ = fs::remove_file(&temp_file);
}

fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
