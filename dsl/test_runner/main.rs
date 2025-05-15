use std::fs;

use clap::Parser as _;
use dsl_test_runner::{runner, Args};
use walkdir::{DirEntry, WalkDir};

fn main() {
    let args = Args::parse();
    let path = args.path.unwrap_or(std::env::current_dir().unwrap());

    let walker = WalkDir::new(path).into_iter();
    // TODO: eventually ignore files from gitignore
    let sources = walker
        .filter_entry(|e| !is_hidden(e))
        .filter_map(|e| e.ok())
        .filter(|e| is_test_file(e))
        .map(|entry| {
            (
                entry.path().display().to_string(),
                fs::read_to_string(entry.path()).unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let mut runner = runner::Runner::new(&sources);
    if runner.has_errors() {
        runner.print_errors();
        return;
    }
    let failed = runner.run(args.databases);
    if failed {
        std::process::exit(1)
    }
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

fn is_test_file(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.ends_with(".test"))
        .unwrap_or(false)
}
