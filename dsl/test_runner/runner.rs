use ariadne::{Color, Label, Report, ReportKind, Source};
use dsl_parser::{parser_dsl, Parser as _, TestKind};
use owo_colors::{OwoColorize, Style, Styled};
use rayon::prelude::*;
use std::{
    any::Any,
    borrow::Cow,
    // io::set_output_capture, // TODO: set_output_capture currently disabled as it is a nightly feature and messes up our build scripts
    panic::catch_unwind,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, Instant},
};

use crate::testing::{DslTest, FileTest};

static FAILED_RUN: OnceLock<bool> = OnceLock::new();

// TODO: later remove owo_dependency. Just lazy right now to mess with ansi escape codes
const OK: Styled<&'static str> = Style::new().bold().green().style("ok");

const FAILED: Styled<&'static str> = Style::new().bold().red().style("FAILED");

const IGNORED: Styled<&'static str> = Style::new().bold().yellow().style("ignored");

fn fold_err<T, E>(
    result: Result<Result<T, E>, Box<dyn Any + Send>>,
) -> Result<T, Box<dyn Any + Send>>
where
    E: Send + 'static,
{
    match result {
        Ok(Err(e)) => Err(Box::new(e)),
        Ok(Ok(v)) => Ok(v),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Clone)]
enum Status<'a> {
    Success,
    Ignore(&'a str),
    Failed {
        // TODO: maybe this should be a Vec<u8> instead
        stdout: String,
        error_msg: String,
    },
}

#[derive(Debug)]
pub struct FinishedTest<'a> {
    file_name: &'a str,
    name: Cow<'a, str>,
    time: Duration,
    status: Status<'a>,
}

impl FinishedTest<'_> {
    fn print_status(&self) {
        match &self.status {
            Status::Success => {
                println!(
                    "{} {}::{} ... {} {:?}",
                    "test".blue(),
                    self.file_name,
                    self.name,
                    OK,
                    self.time
                )
            }
            Status::Ignore(msg) => {
                println!(
                    "{} {}::{} ... {}, {} {:?}",
                    "test".blue(),
                    self.file_name,
                    self.name,
                    IGNORED,
                    msg.yellow(),
                    self.time
                )
            }
            Status::Failed { .. } => {
                println!(
                    "{} {}::{} ... {} {:?}",
                    "test".blue(),
                    self.file_name,
                    self.name,
                    FAILED,
                    self.time
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct Runner<'a> {
    inner: Vec<FileTest<'a>>,
}

impl<'src> Runner<'src> {
    /// Parse Sources and create Runner
    pub fn new(sources: &'src Vec<(String, String)>) -> Self {
        let tests = (0..sources.len())
            .into_iter()
            .map(|index| {
                let (file_name, source) = sources.get(index).unwrap();
                let (out, errs) = parser_dsl().parse(&source).into_output_errors();
                let errs = errs
                    .into_iter()
                    .map(|e| {
                        // Basic error message for now
                        Report::build(
                            ReportKind::Error,
                            (file_name.as_str(), e.span().into_range()),
                        )
                        .with_config(
                            ariadne::Config::new().with_index_type(ariadne::IndexType::Byte),
                        )
                        .with_message(e.to_string())
                        .with_label(
                            Label::new((file_name.as_str(), e.span().into_range()))
                                .with_message(e.reason().to_string())
                                .with_color(Color::Red),
                        )
                        .finish()
                    })
                    .collect::<Vec<_>>();
                FileTest {
                    file_name,
                    source,
                    tests: out
                        .unwrap_or_default()
                        .into_iter()
                        .map(DslTest::new)
                        .collect(),
                    errors: errs,
                }
            })
            .collect::<Vec<_>>();
        Self { inner: tests }
    }

    pub fn has_errors(&self) -> bool {
        for file_test in self.inner.iter() {
            if !file_test.errors.is_empty() {
                return true;
            }
        }
        false
    }

    pub fn print_errors(&self) {
        for file_test in self.inner.iter() {
            for error in file_test.errors.iter() {
                error
                    .print((file_test.file_name, Source::from(file_test.source)))
                    .unwrap()
            }
        }
    }

    pub fn run(&mut self, default_dbs: Vec<PathBuf>) -> bool {
        let (tests, failed, total_time) = self.run_inner(default_dbs);
        // TODO: in the future could avoid some computation if we know we did not fail.
        // But for now leave it here just to keep the code simpler
        if failed {
            println!("\nfailures:\n");
        }
        let mut failed_names = Vec::new();
        let mut success_count = 0usize;
        let mut ignore_count = 0usize;
        for test in tests {
            // TODO: eventually differentiate between cli output and Api output
            match test.status {
                Status::Failed { stdout, error_msg } => {
                    println!(
                        "---- {}::{} stdout ----",
                        test.file_name.blue(),
                        test.name.red()
                    );
                    println!("{}\n{}", stdout, error_msg);
                    failed_names.push(format!("{}::{}", test.file_name, test.name));
                }
                Status::Success => success_count += 1,
                Status::Ignore(..) => ignore_count += 1,
            }
        }
        let failed_count = failed_names.len();
        if !failed_names.is_empty() {
            println!("\nfailures:");
            let indent = "    ";
            let out = failed_names
                .into_iter()
                .fold(String::new(), |mut acc, name| {
                    acc.push_str(indent);
                    acc.push_str(&name);
                    acc.push('\n');
                    acc
                });
            println!("{out}");
        }
        let result = if failed { FAILED } else { OK };
        println!(
            "\ntest result: {}. {} passed; {} failed; {} ignored; finished in {:.2?}",
            result, success_count, failed_count, ignore_count, total_time
        );
        return failed;
    }

    // TODO: reenable stdout redirect
    fn run_inner(&mut self, default_dbs: Vec<PathBuf>) -> (Vec<FinishedTest>, bool, Duration) {
        assert!(!self.has_errors());
        let total_time = Instant::now();
        let finished_tests = self
            .inner
            .par_iter()
            .map(|file_test| {
                let tests = file_test.tests.par_iter().map(|test| {
                    // Buffer for capturing standard I/O
                    // let data = Arc::new(Mutex::new(Vec::new()));

                    // set_output_capture(Some(data.clone()));

                    let now = Instant::now();
                    let result = if test.inner.options.is_some() {
                        // Skip the test
                        Ok(())
                    } else {
                        fold_err(catch_unwind(|| match &test.inner.kind {
                            TestKind::Memory => test.exec_sql(None),
                            TestKind::Databases(dbs) => dbs
                                .iter()
                                .map(|db_path| test.exec_sql(Some(Path::new(db_path))))
                                .collect(),
                            _ => default_dbs
                                .iter()
                                .map(|db_path| test.exec_sql(Some(db_path)))
                                .collect(),
                        }))
                    };

                    // Release stdout
                    // set_output_capture(None);
                    let elapsed_time = now.elapsed();
                    let error_msg = match result {
                        Ok(()) => None,
                        Err(ref e) => {
                            // Panic Message or Error message. Got this code from libtest
                            let assert_msg = e
                                .downcast_ref::<String>()
                                .map(|e| &**e)
                                .or_else(|| e.downcast_ref::<&'static str>().copied())
                                .map(|s| s.to_string());
                            let error_msg =
                                e.downcast_ref::<anyhow::Error>().map(|err| err.to_string());
                            match (assert_msg, error_msg) {
                                (Some(msg), None) | (None, Some(msg)) => Some(msg),
                                _ => unreachable!(
                                    "Either a panic happens or an error happens not both or none"
                                ),
                            }
                        }
                    };
                    let status = if let Some(error_msg) = error_msg {
                        // let stdout = data.lock().unwrap_or_else(|e| e.into_inner()).to_vec();
                        FAILED_RUN.get_or_init(|| true);
                        Status::Failed {
                            stdout: String::new(),
                            error_msg,
                        }
                    } else {
                        if let Some(options) = &test.inner.options {
                            Status::Ignore(options)
                        } else {
                            Status::Success
                        }
                    };
                    let finished_test = FinishedTest {
                        file_name: file_test.file_name,
                        name: test.inner.ident.clone(),
                        status,
                        time: elapsed_time,
                    };
                    finished_test.print_status();

                    finished_test
                });
                tests
            })
            .flatten();
        (
            finished_tests.collect(),
            *FAILED_RUN.get_or_init(|| false),
            total_time.elapsed(),
        )
    }
}
