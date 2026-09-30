use cfg_aliases::cfg_aliases;
use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

fn main() {
    cfg_aliases! {
        injected_yields: { any(feature = "test_helper", feature = "simulator") },
        host_shared_wal: { all(any(unix, target_os = "windows"), target_pointer_width = "64") },
    }

    // Ensure Cargo reruns when this script or the reproducibility seed changes.
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=SOURCE_DATE_EPOCH");

    // For reproducible builds: when SOURCE_DATE_EPOCH is set, skip git entirely
    // so the output is fully deterministic.
    let source_date_epoch = env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|epoch| epoch.parse::<i64>().ok());

    // Tell cargo to rebuild when git HEAD changes, so sqlite_source_id() stays current.
    // We use `git rev-parse --git-dir` instead of hardcoding ".git" to support worktrees,
    // where the git directory lives elsewhere (e.g., ../.git/worktrees/my-worktree).
    // Silently ignored if git unavailable (e.g., building from tarball).
    let (git_hash, git_commit_epoch) = if source_date_epoch.is_some() {
        (None, None)
    } else {
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

        // Falls back to None if the git cli is unavailable.
        // Commit hash is used for sqlite_source_id() and to derive a stable timestamp.
        let hash = run_git(&["rev-parse", "HEAD"]);
        let epoch = run_git(&["show", "-s", "--format=%ct", "HEAD"])
            .and_then(|epoch| epoch.parse::<i64>().ok());
        (hash, epoch)
    };

    let git_hash_code = match git_hash {
        Some(hash) => format!("pub const GIT_COMMIT_HASH: Option<&str> = Some(\"{hash}\");"),
        None => "pub const GIT_COMMIT_HASH: Option<&str> = None;".to_string(),
    };

    // Pre-format the timestamp so sqlite_source_id() doesn't need chrono at runtime.
    // Prefer SOURCE_DATE_EPOCH, then git commit time, and fall back to now.
    let sqlite_date = format_utc(source_date_epoch.or(git_commit_epoch).unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_secs() as i64
    }));

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let built_file = out_dir.join("built.rs");

    // Only the three constants actually used by turso_core (PKG_VERSION for
    // turso_version(), BUILT_TIME_SQLITE and GIT_COMMIT_HASH for sqlite_source_id()).
    let new_contents = format!(
        "pub const PKG_VERSION: &str = \"{}\";\npub const BUILT_TIME_SQLITE: &str = \"{sqlite_date}\";\n{git_hash_code}\n",
        env::var("CARGO_PKG_VERSION").unwrap(),
    );

    // Avoid touching built.rs when content is unchanged to prevent rebuild loops.
    let existing_contents = fs::read_to_string(&built_file).ok();
    if existing_contents.as_deref() != Some(new_contents.as_str()) {
        fs::write(&built_file, new_contents).expect("Failed to write built file");
    }
}

/// Format a Unix epoch as "YYYY-MM-DD HH:MM:SS" in UTC (civil date algorithm by Howard Hinnant).
fn format_utc(epoch: i64) -> String {
    let secs_per_day: i64 = 86400;
    let mut days = epoch.div_euclid(secs_per_day);
    let day_secs = epoch.rem_euclid(secs_per_day);
    let (h, m, s) = (day_secs / 3600, (day_secs % 3600) / 60, day_secs % 60);
    days += 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = (days - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{s:02}")
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
