use ariadne::{Color, Label, Report, ReportKind, Source};
use clap::Parser;
use dsl_parser::{parser_dsl, Parser as _};
use std::path::PathBuf;

use crate::testing::{DslTest, FileTest};

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
pub struct Runner<'a> {
    inner: Vec<FileTest<'a>>,
}

impl<'src> Runner<'src> {
    /// Parse Sources and create Runner
    pub fn new(sources: &'src Vec<String>) -> Self {
        let tests = (0..sources.len())
            .into_iter()
            .map(|index| {
                let source = sources.get(index).unwrap();
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

    pub fn run(&self) {}
}
