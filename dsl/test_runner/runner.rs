use ariadne::{Color, Label, Report, ReportKind, Source};
use dsl_parser::{parser_dsl, Parser as _, TestKind};
use owo_colors::{OwoColorize, Style, Styled};
use std::{
    any::Any,
    borrow::Cow,
    io::Read,
    panic::catch_unwind,
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::{
    buffer::BufferRedirect,
    testing::{DslTest, FileTest},
};

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

#[derive(Debug, Clone)]
enum Status {
    Success,
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
    status: Status,
}

impl FinishedTest<'_> {
    fn print_status(&self) {
        if matches!(self.status, Status::Success) {
            println!(
                "{} {}::{} ... {} {:?}",
                "test".blue(),
                self.file_name,
                self.name,
                OK,
                self.time
            )
        } else {
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
                error.print(Source::from(file_test.source)).unwrap()
            }
        }
    }

    pub fn run(&mut self, default_dbs: Vec<PathBuf>) {
        let (tests, failed, total_time) = self.run_inner(default_dbs);
        // TODO: in the future could avoid some computation if we know we did not fail.
        // But for now leave it here just to keep the code simpler
        if failed {
            println!("\nfailures:\n");
        }
        let mut failed_names = Vec::new();
        let mut success_count = 0usize;
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
                _ => success_count += 1,
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
        // TODO: when we support ignore tests, adjust count here
        println!(
            "\ntest result: {}. {} passed; {} failed; 0 ignored; finished in {:?}",
            result, success_count, failed_count, total_time
        );
    }

    fn run_inner(&mut self, default_dbs: Vec<PathBuf>) -> (Vec<FinishedTest>, bool, Duration) {
        assert!(!self.has_errors());
        let mut finished_tests = Vec::with_capacity(self.inner.capacity());
        let default_dbs = default_dbs;
        let mut failed = false;
        let total_time = Instant::now();
        for file_test in self.inner.iter() {
            println!("Running {} tests", file_test.tests.len());
            for test in file_test.tests.iter() {
                // TODO: this redirect is nice, but it forces us to write the output to a file
                // and then read it later
                // Consider using unstable io::set_output_capture that is used in libtest
                let mut stdout_redirect = BufferRedirect::stdout_stderr().unwrap();
                let now = Instant::now();
                let result = fold_err(catch_unwind(|| {
                    if matches!(test.inner.kind, TestKind::Memory) {
                        test.exec_sql(None)
                    } else {
                        default_dbs
                            .iter()
                            .map(|db_path| test.exec_sql(Some(db_path)))
                            .collect()
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
                    stdout_redirect.read_to_string(&mut stdout).unwrap();
                    failed = true;
                    Status::Failed { stdout, error_msg }
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
                finished_test.print_status();

                finished_tests.push(finished_test);
            }
        }
        (finished_tests, failed, total_time.elapsed())
    }
}
