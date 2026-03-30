use std::{
    collections::HashMap,
    env::current_dir,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Paths, model::interactions::InteractionPlan};

use super::cli::SimulatorCLI;

const READABLE_PLAN_PATH: &str = "plan.sql";
const SHRUNK_READABLE_PLAN_PATH: &str = "shrunk.sql";
const SEED_PATH: &str = "seed.txt";
const RUNS_PATH: &str = "runs.json";

/// A bug is a run that has been identified as buggy.
#[derive(Clone)]
pub struct Bug {
    /// The seed of the bug.
    pub seed: u64,

    /// The plan of the bug.
    /// TODO: currently plan is only saved to the .sql file, and that is not deserializable yet
    /// so we cannot always store an interaction plan here
    pub plan: Option<InteractionPlan>,

    /// The shrunk plan of the bug, if any.
    pub shrunk_plan: Option<InteractionPlan>,

    /// The runs of the bug.
    pub runs: Vec<BugRun>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BugRun {
    /// Commit hash of the current version of Limbo.
    pub hash: String,
    /// Timestamp of the run.
    #[serde(with = "chrono::serde::ts_seconds")]
    pub timestamp: DateTime<Utc>,
    /// Error message of the run.
    pub error: Option<String>,
    /// Options
    pub cli_options: SimulatorCLI,
    /// Whether the run was a shrunk run.
    pub shrunk: bool,
}

impl Bug {
    fn save_to_path(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        let bug_path = path.join(self.seed.to_string());
        std::fs::create_dir_all(&bug_path)
            .with_context(|| "should be able to create bug directory")?;

        let seed_path = bug_path.join(SEED_PATH);
        std::fs::write(&seed_path, self.seed.to_string())
            .with_context(|| "should be able to write seed file")?;

        if let Some(plan) = &self.plan {
            let readable_plan_path = bug_path.join(READABLE_PLAN_PATH);
            std::fs::write(&readable_plan_path, plan.to_string())
                .with_context(|| "should be able to write readable plan file")?;
        }

        if let Some(shrunk_plan) = &self.shrunk_plan {
            let readable_shrunk_plan_path = bug_path.join(SHRUNK_READABLE_PLAN_PATH);
            std::fs::write(&readable_shrunk_plan_path, shrunk_plan.to_string())
                .with_context(|| "should be able to write readable shrunk plan file")?;
        }

        let runs_path = bug_path.join(RUNS_PATH);
        std::fs::write(
            &runs_path,
            serde_json::to_string_pretty(&self.runs)
                .with_context(|| "should be able to serialize runs")?,
        )
        .with_context(|| "should be able to write runs file")?;

        Ok(())
    }

    pub fn last_cli_opts(&self) -> SimulatorCLI {
        self.runs.last().unwrap().cli_options.clone()
    }
}

/// Bug Base is a local database of buggy runs.
pub(crate) struct BugBase {
    /// Path to the bug base directory.
    path: PathBuf,
    /// The list of buggy runs, uniquely identified by their seed
    bugs: HashMap<u64, Option<Bug>>,
}

impl BugBase {
    /// Create a new bug base.
    fn new(path: PathBuf) -> anyhow::Result<Self> {
        let mut bugs = HashMap::new();

        // list all the bugs in the path as directories
        if let Ok(entries) = std::fs::read_dir(&path) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                    let seed = entry
                        .file_name()
                        .to_string_lossy()
                        .to_string()
                        .parse::<u64>()
                        .with_context(|| {
                            format!(
                                "failed to parse seed from directory name {}",
                                entry.file_name().to_string_lossy()
                            )
                        })?;
                    bugs.insert(seed, None);
                }
            }
        }

        Ok(Self { path, bugs })
    }

    /// Load the bug base from one of the potential paths.
    pub(crate) fn load() -> anyhow::Result<Self> {
        let potential_paths = [
            // limbo project directory
            BugBase::get_limbo_project_dir()?,
            // home directory
            dirs::home_dir().with_context(|| "should be able to get home directory")?,
            // current directory
            std::env::current_dir().with_context(|| "should be able to get current directory")?,
        ];

        for path in &potential_paths {
            let path = path.join(".bugbase");
            if path.exists() {
                return BugBase::new(path);
            }
        }

        for path in potential_paths {
            let path = path.join(".bugbase");
            if std::fs::create_dir_all(&path).is_ok() {
                tracing::info!("bug base created at {}", path.display());
                return BugBase::new(path);
            }
        }

        Err(anyhow!("failed to create bug base"))
    }

    fn load_bug(&self, seed: u64) -> anyhow::Result<Bug> {
        let path = self.path.join(seed.to_string()).join(RUNS_PATH);

        let runs = if !path.exists() {
            vec![]
        } else {
            std::fs::read_to_string(self.path.join(seed.to_string()).join(RUNS_PATH))
                .with_context(|| "should be able to read runs file")
                .and_then(|runs| serde_json::from_str(&runs).map_err(|e| anyhow!("{}", e)))?
        };

        let bug = Bug {
            seed,
            plan: None,
            shrunk_plan: None,
            runs,
        };
        Ok(bug)
    }

    pub fn load_bugs(&self) -> anyhow::Result<Vec<Bug>> {
        let seeds = self.bugs.keys().copied().collect::<Vec<_>>();

        seeds
            .iter()
            .map(|seed| self.load_bug(*seed))
            .collect::<Result<Vec<_>, _>>()
    }

    /// Add a new bug to the bug base.
    pub(crate) fn add_bug(
        &mut self,
        seed: u64,
        plan: InteractionPlan,
        error: Option<String>,
        cli_options: &SimulatorCLI,
    ) -> anyhow::Result<()> {
        let path = self.path.clone();

        tracing::debug!("adding bug with seed {}", seed);
        let bug = self.get_or_load_bug(seed)?;
        let bug = if let Some(bug) = bug {
            bug.runs.push(BugRun {
                hash: Self::get_current_commit_hash()?,
                timestamp: SystemTime::now().into(),
                error,
                cli_options: cli_options.clone(),
                shrunk: false,
            });
            bug.plan = Some(plan);
            bug
        } else {
            let bug = Bug {
                seed,
                plan: Some(plan),
                shrunk_plan: None,
                runs: vec![BugRun {
                    hash: Self::get_current_commit_hash()?,
                    timestamp: SystemTime::now().into(),
                    error,
                    cli_options: cli_options.clone(),
                    shrunk: false,
                }],
            };

            self.bugs.insert(seed, Some(bug));
            self.bugs.get_mut(&seed).unwrap().as_mut().unwrap()
        };

        // Save the bug to the bug base.
        bug.save_to_path(&path)
    }

    pub fn get_or_load_bug(&mut self, seed: u64) -> anyhow::Result<Option<&mut Bug>> {
        // Check if the bug exists and is loaded
        let needs_loading = match self.bugs.get(&seed) {
            Some(Some(_)) => false,  // Already loaded
            Some(None) => true,      // Exists but unloaded
            None => return Ok(None), // Doesn't exist
        };

        if needs_loading {
            let bug = self.load_bug(seed)?;
            self.bugs.insert(seed, Some(bug));
        }

        // Now get the mutable reference
        Ok(self.bugs.get_mut(&seed).and_then(|opt| opt.as_mut()))
    }

    pub(crate) fn save_shrunk(
        &mut self,
        seed: u64,
        cli_options: &SimulatorCLI,
        shrunk_plan: InteractionPlan,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let path = self.path.clone();
        let bug = self
            .get_or_load_bug(seed)?
            .expect("bug should have been loaded");
        bug.runs.push(BugRun {
            hash: Self::get_current_commit_hash()?,
            timestamp: SystemTime::now().into(),
            error,
            cli_options: cli_options.clone(),
            shrunk: true,
        });
        bug.shrunk_plan = Some(shrunk_plan);

        // Save the bug to the bug base.
        bug.save_to_path(path)
            .with_context(|| "should be able to save shrunk bug")?;
        Ok(())
    }

    pub(crate) fn list_bugs(&mut self) -> anyhow::Result<()> {
        let bugs = self.load_bugs()?;
        for bug in bugs {
            println!("seed: {}", bug.seed);
            println!("runs:");
            println!("  ------------------");
            for run in &bug.runs {
                println!("  - hash: {}", run.hash);
                println!("    timestamp: {}", run.timestamp);
                println!(
                    "    type: {}",
                    if run.cli_options.differential {
                        "differential"
                    } else if run.cli_options.doublecheck {
                        "doublecheck"
                    } else {
                        "default"
                    }
                );
                if let Some(error) = &run.error {
                    println!("    error: {error}");
                }
            }
            println!("  ------------------");
        }

        Ok(())
    }
}

impl BugBase {
    #[expect(dead_code)]
    /// Get the path to the bug base directory.
    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Get paths to all the files for a given seed.
    pub(crate) fn paths(&self, seed: u64) -> Paths {
        let base = self.path.join(format!("{seed}/"));
        Paths::new(&base)
    }
}

impl BugBase {
    pub(crate) fn get_current_commit_hash() -> anyhow::Result<String> {
        let git_dir = find_git_dir(current_dir()?).with_context(|| "should be a git repo")?;
        let hash =
            resolve_head(&git_dir).with_context(|| "should be able to get the commit hash")?;
        Ok(hash)
    }

    pub(crate) fn get_limbo_project_dir() -> anyhow::Result<PathBuf> {
        let git_dir = find_git_dir(current_dir()?).with_context(|| "should be a git repo")?;
        let workdir = git_dir
            .parent()
            .with_context(|| "work tree should be parent of .git")?;
        Ok(workdir.to_path_buf())
    }
}

fn find_git_dir(start_path: impl AsRef<Path>) -> Option<PathBuf> {
    let mut current = start_path.as_ref().to_path_buf();
    loop {
        let git_path = current.join(".git");
        if git_path.is_dir() {
            return Some(git_path);
        } else if git_path.is_file() {
            // Handle git worktrees - .git is a file containing "gitdir: <path>"
            if let Ok(contents) = read_to_string(&git_path) {
                if let Some(gitdir) = contents.strip_prefix("gitdir: ") {
                    return Some(PathBuf::from(gitdir));
                }
            }
        }
        if !current.pop() {
            return None;
        }
    }
}

fn resolve_head(git_dir: impl AsRef<Path>) -> anyhow::Result<String> {
    // HACK ignores stuff like packed-refs
    let head_path = git_dir.as_ref().join("HEAD");
    let head_contents = read_to_string(&head_path)?;
    if let Some(ref_path) = head_contents.strip_prefix("ref: ") {
        let ref_file = git_dir.as_ref().join(ref_path);
        read_to_string(&ref_file)
    } else {
        Ok(head_contents)
    }
}

fn read_to_string(path: impl AsRef<Path>) -> anyhow::Result<String> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents.trim().to_string())
}
