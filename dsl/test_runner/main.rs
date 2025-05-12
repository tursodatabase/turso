use std::fs;

use clap::Parser as _;
use runner::Args;
use walkdir::{DirEntry, WalkDir};

mod runner;

fn main() {
    let args = Args::parse();
    let path = args.file.unwrap_or(std::env::current_dir().unwrap());

    let walker = WalkDir::new(path).into_iter();
    let sources = walker
        .filter_entry(|e| !is_hidden(e))
        .filter_map(|e| e.ok())
        .filter(|e| is_test_file(e))
        .map(|entry| {
            println!("{}", entry.path().display());
            fs::read_to_string(entry.path()).unwrap()
        })
        .collect::<Vec<_>>();

    let runner = runner::Runner::new(&sources);
    if runner.has_errors() {
        runner.print_errors();
        return;
    }
    runner.run();
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
