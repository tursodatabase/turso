use ariadne::{Color, Label, Report, ReportKind, Source};
use clap::Parser;
use dsl_parser::{parser_dsl, Parser as _};
use gag::BufferRedirect;
use owo_colors::{OwoColorize, Style, Styled};
use std::{
    any::Any,
    borrow::Cow,
    io::Read,
    panic::catch_unwind,
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::testing::{DslTest, FileTest};

// TODO: later remove owo_dependency. Just lazy right now to mess with ansi escape codes
const OK: Styled<&'static str> = Style::new().bold().green().style("ok");

const FAILED: Styled<&'static str> = Style::new().bold().red().style("FAILED");

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

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(name = "dsl_runner")]
#[command(version, about, long_about)]
pub struct Args {
    /// File path to run test
    #[arg(short, long)]
    pub path: Option<PathBuf>,
}

#[derive(Debug)]
enum Status {
    Success,
    // TODO: maybe this should be a Vec<u8> instead
    Failed {
        stdout: String,
        stderr: String,
        error_msg: String,
    },
}

#[derive(Debug)]
pub struct FinishedTest<'a> {
    file_name: &'a str,
    name: Cow<'a, str>,
    time: Duration,
    status: Status,
}

impl FinishedTest<'_> {
    fn print_status(&self) {
        let sub_millis = self.time.subsec_micros();
        let seconds = self.time.as_secs() % 60;
        let minutes = (self.time.as_secs() / 60) % 60;
        let time = format!("[{:0>2}:{:0>2}.{:0>4}]", minutes, seconds, sub_millis);
        if matches!(self.status, Status::Success) {
            println!(
                "{} {}::{} ... {} {}",
                "test".blue(),
                self.file_name,
                self.name,
                OK,
                time
            )
        } else {
            println!(
                "{} {}::{} ... {} {}",
                "test".blue(),
                self.file_name,
                self.name,
                FAILED,
                time
            )
        }
    }
}

#[derive(Debug)]
pub struct Runner<'a> {
    inner: Vec<FileTest<'a>>,
    failed: bool,
}

impl<'src> Runner<'src> {
    /// Parse Sources and create Runner
    pub fn new(sources: &'src Vec<(String, String)>) -> Self {
        let tests = (0..sources.len())
            .into_iter()
            .map(|index| {
                let (file_name, source) = sources.get(index).unwrap();
                let (out, errs) = parser_dsl().parse(&source).into_output_errors();
                let errs: Vec<Report<'_, ((), std::ops::Range<usize>)>> = errs
                    .into_iter()
                    .map(|e| {
                        // Basic error message for now
                        Report::build(ReportKind::Error, ((), e.span().into_range()))
                            .with_config(
                                ariadne::Config::new().with_index_type(ariadne::IndexType::Byte),
                            )
                            .with_message(e.to_string())
                            .with_label(
                                Label::new(((), e.span().into_range()))
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
        Self {
            inner: tests,
            failed: false,
        }
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
                error.print(Source::from(file_test.source)).unwrap()
            }
        }
    }

    pub fn run(&mut self, default_dbs: Option<Vec<PathBuf>>) -> bool {
        let tests = self.run_inner(default_dbs);
        self.failed
    }

    fn run_inner(&mut self, default_dbs: Option<Vec<PathBuf>>) -> Vec<FinishedTest> {
        assert!(!self.has_errors());
        let mut finished_tests = Vec::with_capacity(self.inner.capacity());
        let default_dbs = default_dbs;
        let mut failed = false;
        for file_test in self.inner.iter_mut() {
            println!("Running {} tests", file_test.tests.len());
            for test in file_test.tests.iter_mut() {
                let mut stdout_redirect = BufferRedirect::stdout().unwrap();
                let mut stderr_redirect = BufferRedirect::stderr().unwrap();
                let now = Instant::now();
                let result = fold_err(catch_unwind(|| {
                    if let Some(default_dbs) = default_dbs.as_ref() {
                        default_dbs
                            .iter()
                            .map(|db_path| test.exec_sql(Some(db_path)))
                            .collect()
                    } else {
                        test.exec_sql(None)
                    }
                }));
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
                    let mut stdout = String::new();
                    let mut stderr = String::new();
                    stdout_redirect.read_to_string(&mut stdout).unwrap();
                    stderr_redirect.read_to_string(&mut stderr).unwrap();
                    failed = true;
                    Status::Failed {
                        stdout,
                        stderr,
                        error_msg,
                    }
                } else {
                    Status::Success
                };
                let finished_test = FinishedTest {
                    file_name: file_test.file_name,
                    name: test.inner.ident.clone(),
                    status,
                    time: elapsed_time,
                };
                drop(stdout_redirect);
                drop(stderr_redirect);
                finished_test.print_status();

                finished_tests.push(finished_test);
            }
        }
        self.failed = failed;
        finished_tests
    }
}
