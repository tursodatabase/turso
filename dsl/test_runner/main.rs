use std::fs;

use clap::Parser as _;
use dsl_test_runner::{runner, Args};
// use std::io::set_output_capture;
use walkdir::{DirEntry, WalkDir};

fn main() {
    // build_rayon_global_thread_pool();

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

// TODO: Re-enable set_output_capture when we have a better solution
// fn build_rayon_global_thread_pool() {
//     rayon::ThreadPoolBuilder::new()
//         .spawn_handler(|thread| {
//             let mut b = std::thread::Builder::new();
//             if let Some(name) = thread.name() {
//                 b = b.name(name.to_owned());
//             }
//             if let Some(stack_size) = thread.stack_size() {
//                 b = b.stack_size(stack_size);
//             }

//             // Get and clone the output capture of the current thread.
//             let output_capture = set_output_capture(None);
//             // Set the output capture of the new thread.
//             set_output_capture(output_capture.clone());

//             b.spawn(|| {
//                 set_output_capture(output_capture);
//                 thread.run()
//             })?;
//             Ok(())
//         })
//         .use_current_thread()
//         .build_global()
//         .unwrap();
// }
