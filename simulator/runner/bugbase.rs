use std::{
    collections::HashMap,
    io::{self, Write},
    path::PathBuf,
    process::Command,
    time::SystemTime,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{InteractionPlan, Paths};

use super::cli::SimulatorCLI;

/// A bug is a run that has been identified as buggy.
#[derive(Clone)]
pub(crate) enum Bug {
    Unloaded { seed: u64 },
    Loaded(LoadedBug),
}

#[derive(Clone)]
pub struct LoadedBug {
    /// The seed of the bug.
    pub seed: u64,
    /// The plan of the bug.
    pub plan: InteractionPlan,
    /// The runs of the bug.
    pub runs: Vec<BugRun>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct BugRun {
    /// Commit hash of the current version of Limbo.
    pub(crate) hash: String,
    /// Timestamp of the run.
    #[serde(with = "chrono::serde::ts_seconds")]
    pub(crate) timestamp: DateTime<Utc>,
    /// Error message of the run.
    pub(crate) error: Option<String>,
    /// Options
    pub(crate) cli_options: SimulatorCLI,
}

impl Bug {
    /// Check if the bug is loaded.
    pub(crate) fn is_loaded(&self) -> bool {
        match self {
            Bug::Unloaded { .. } => false,
            Bug::Loaded { .. } => true,
        }
    }

    /// Get the seed of the bug.
    pub(crate) fn seed(&self) -> u64 {
        match self {
            Bug::Unloaded { seed } => *seed,
            Bug::Loaded(LoadedBug { seed, .. }) => *seed,
        }
    }
}

/// Bug Base is a local database of buggy runs.
pub(crate) struct BugBase {
    /// Path to the bug base directory.
    path: PathBuf,
    /// The list of buggy runs, uniquely identified by their seed
    bugs: HashMap<u64, Bug>,
}

impl BugBase {
    /// Create a new bug base.
    fn new(path: PathBuf) -> Result<Self, String> {
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
                        .or(Err(format!(
                            "failed to parse seed from directory name {}",
                            entry.file_name().to_string_lossy()
                        )))?;
                    bugs.insert(seed, Bug::Unloaded { seed });
                }
            }
        }

        Ok(Self { path, bugs })
    }

    /// Load the bug base from one of the potential paths.
    pub(crate) fn load() -> Result<Self, String> {
        let potential_paths = vec![
            // limbo project directory
            BugBase::get_limbo_project_dir()?,
            // home directory
            dirs::home_dir().ok_or("should be able to get home directory".to_string())?,
            // current directory
            std::env::current_dir()
                .or(Err("should be able to get current directory".to_string()))?,
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
                log::info!("bug base created at {}", path.display());
                return BugBase::new(path);
            }
        }

        Err("failed to create bug base".to_string())
    }

    /// Load the bug base from one of the potential paths.
    pub(crate) fn interactive_load() -> Result<Self, String> {
        let potential_paths = vec![
            // limbo project directory
            BugBase::get_limbo_project_dir()?,
            // home directory
            dirs::home_dir().ok_or("should be able to get home directory".to_string())?,
            // current directory
            std::env::current_dir()
                .or(Err("should be able to get current directory".to_string()))?,
        ];

        for path in potential_paths {
            let path = path.join(".bugbase");
            if path.exists() {
                return BugBase::new(path);
            }
        }

        println!("select bug base location:");
        println!("1. limbo project directory");
        println!("2. home directory");
        println!("3. current directory");
        print!("> ");
        io::stdout().flush().unwrap();
        let mut choice = String::new();
        io::stdin()
            .read_line(&mut choice)
            .expect("failed to read line");

        let choice = choice
            .trim()
            .parse::<u32>()
            .or(Err(format!("invalid choice {choice}")))?;
        let path = match choice {
            1 => BugBase::get_limbo_project_dir()?.join(".bugbase"),
            2 => {
                let home = std::env::var("HOME").or(Err("failed to get home directory"))?;
                PathBuf::from(home).join(".bugbase")
            }
            3 => PathBuf::from(".bugbase"),
            _ => return Err(format!("invalid choice {choice}")),
        };

        if path.exists() {
            unreachable!("bug base already exists at {}", path.display());
        } else {
            std::fs::create_dir_all(&path).or(Err("failed to create bug base"))?;
            log::info!("bug base created at {}", path.display());
            BugBase::new(path)
        }
    }

    /// Add a new bug to the bug base.
    pub(crate) fn add_bug(
        &mut self,
        seed: u64,
        plan: InteractionPlan,
        error: Option<String>,
        cli_options: &SimulatorCLI,
    ) -> Result<(), String> {
        log::debug!("adding bug with seed {}", seed);
        let bug = self.get_bug(seed);

        if bug.is_some() {
            let mut bug = self.load_bug(seed)?;
            bug.plan = plan.clone();
            bug.runs.push(BugRun {
                hash: Self::get_current_commit_hash()?,
                timestamp: SystemTime::now().into(),
                error,
                cli_options: cli_options.clone(),
            });
            self.bugs.insert(seed, Bug::Loaded(bug.clone()));
        } else {
            let bug = LoadedBug {
                seed,
                plan: plan.clone(),
                runs: vec![BugRun {
                    hash: Self::get_current_commit_hash()?,
                    timestamp: SystemTime::now().into(),
                    error,
                    cli_options: cli_options.clone(),
                }],
            };
            self.bugs.insert(seed, Bug::Loaded(bug.clone()));
        }
        // Save the bug to the bug base.
        self.save_bug(seed)
    }

    /// Get a bug from the bug base.
    pub(crate) fn get_bug(&self, seed: u64) -> Option<&Bug> {
        self.bugs.get(&seed)
    }

    /// Save a bug to the bug base.
    fn save_bug(&self, seed: u64) -> Result<(), String> {
        let bug = self.get_bug(seed);

        match bug {
            None | Some(Bug::Unloaded { .. }) => {
                unreachable!("save should only be called within add_bug");
            }
            Some(Bug::Loaded(bug)) => {
                let bug_path = self.path.join(seed.to_string());
                std::fs::create_dir_all(&bug_path)
                    .or(Err("should be able to create bug directory".to_string()))?;

                let seed_path = bug_path.join("seed.txt");
                std::fs::write(&seed_path, seed.to_string())
                    .or(Err("should be able to write seed file".to_string()))?;

                let plan_path = bug_path.join("plan.json");
                std::fs::write(
                    &plan_path,
                    serde_json::to_string_pretty(&bug.plan)
                        .or(Err("should be able to serialize plan".to_string()))?,
                )
                .or(Err("should be able to write plan file".to_string()))?;

                let readable_plan_path = bug_path.join("plan.sql");
                std::fs::write(&readable_plan_path, bug.plan.to_string())
                    .or(Err("should be able to write readable plan file".to_string()))?;

                let runs_path = bug_path.join("runs.json");
                std::fs::write(
                    &runs_path,
                    serde_json::to_string_pretty(&bug.runs)
                        .or(Err("should be able to serialize runs".to_string()))?,
                )
                .or(Err("should be able to write runs file".to_string()))?;
            }
        }

        Ok(())
    }

    pub(crate) fn load_bug(&mut self, seed: u64) -> Result<LoadedBug, String> {
        let seed_match = self.bugs.get(&seed);

        match seed_match {
            None => Err(format!("No bugs found for seed {}", seed)),
            Some(Bug::Unloaded { .. }) => {
                let plan =
                    std::fs::read_to_string(self.path.join(seed.to_string()).join("plan.json"))
                        .or(Err(format!(
                            "should be able to read plan file at {}",
                            self.path.join(seed.to_string()).join("plan.json").display()
                        )))?;
                let plan: InteractionPlan = serde_json::from_str(&plan)
                    .or(Err("should be able to deserialize plan".to_string()))?;

                let runs =
                    std::fs::read_to_string(self.path.join(seed.to_string()).join("runs.json"))
                        .or(Err("should be able to read runs file".to_string()))?;
                let runs: Vec<BugRun> = serde_json::from_str(&runs)
                    .or(Err("should be able to deserialize runs".to_string()))?;

                let bug = LoadedBug {
                    seed,
                    plan: plan.clone(),
                    runs,
                };

                self.bugs.insert(seed, Bug::Loaded(bug.clone()));
                log::debug!("Loaded bug with seed {}", seed);
                Ok(bug)
            }
            Some(Bug::Loaded(bug)) => {
                log::warn!(
                    "Bug with seed {} is already loaded, returning the existing plan",
                    seed
                );
                Ok(bug.clone())
            }
        }
    }

    pub(crate) fn mark_successful_run(
        &mut self,
        seed: u64,
        cli_options: &SimulatorCLI,
    ) -> Result<(), String> {
        let bug = self.get_bug(seed);
        match bug {
            None => {
                log::debug!("removing bug base entry for {}", seed);
                std::fs::remove_dir_all(self.path.join(seed.to_string()))
                    .or(Err("should be able to remove bug directory".to_string()))?;
            }
            Some(_) => {
                let mut bug = self.load_bug(seed)?;
                bug.runs.push(BugRun {
                    hash: Self::get_current_commit_hash()?,
                    timestamp: SystemTime::now().into(),
                    error: None,
                    cli_options: cli_options.clone(),
                });
                self.bugs.insert(seed, Bug::Loaded(bug.clone()));
                // Save the bug to the bug base.
                self.save_bug(seed)
                    .or(Err("should be able to save bug".to_string()))?;
                log::debug!("Updated bug with seed {}", seed);
            }
        }

        Ok(())
    }

    pub(crate) fn load_bugs(&mut self) -> Result<Vec<LoadedBug>, String> {
        let seeds = self.bugs.keys().map(|seed| *seed).collect::<Vec<_>>();

        seeds
            .iter()
            .map(|seed| self.load_bug(*seed))
            .collect::<Result<Vec<_>, _>>()
    }

    pub(crate) fn list_bugs(&mut self) -> Result<(), String> {
        let bugs = self.load_bugs()?;
        for bug in bugs {
            println!("seed: {}", bug.seed);
            println!("plan: {}", bug.plan.stats());
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
                    println!("    error: {}", error);
                }
            }
            println!("  ------------------");
        }

        Ok(())
    }
}

impl BugBase {
    /// Get the path to the bug base directory.
    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Get the path to the database file for a given seed.
    pub(crate) fn db_path(&self, seed: u64) -> PathBuf {
        self.path.join(format!("{}/test.db", seed))
    }

    /// Get paths to all the files for a given seed.
    pub(crate) fn paths(&self, seed: u64) -> Paths {
        let base = self.path.join(format!("{}/", seed));
        Paths::new(&base)
    }
}

impl BugBase {
    pub(crate) fn get_current_commit_hash() -> Result<String, String> {
        let output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .or(Err("should be able to get the commit hash".to_string()))?;
        let commit_hash = String::from_utf8(output.stdout)
            .or(Err("commit hash should be valid utf8".to_string()))?
            .trim()
            .to_string();
        Ok(commit_hash)
    }

    pub(crate) fn get_limbo_project_dir() -> Result<PathBuf, String> {
        Ok(PathBuf::from(
            String::from_utf8(
                Command::new("git")
                    .args(["rev-parse", "--git-dir"])
                    .output()
                    .or(Err("should be able to get the git path".to_string()))?
                    .stdout,
            )
            .or(Err("commit hash should be valid utf8".to_string()))?
            .trim()
            .strip_suffix(".git")
            .ok_or("should be able to strip .git suffix".to_string())?,
        ))
    }
}
