mod progress;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use hegel::TestCase;
use std::{
    fs,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
};
use turso_pg_dump_fuzzer::{
    generation::{self, workload, Feature, Profile},
    runner::{compare_files, Runner},
};

#[derive(Parser)]
struct Args {
    #[arg(long, global = true, help = "Disable progress output on stderr")]
    no_progress: bool,
    #[arg(long, env = "PG_DSN")]
    postgres_dsn: Option<String>,
    #[arg(long, env = "TURSOPG_BINARY", default_value = "target/debug/tursopg")]
    binary: PathBuf,
    #[arg(long, default_value = "postgres/conformance/pgdump/results/fuzz")]
    output: PathBuf,
    #[arg(long, default_value = "psql")]
    psql: PathBuf,
    #[arg(long, default_value = "pg_dump")]
    pg_dump: PathBuf,
    #[arg(long, value_enum)]
    profile: Option<Profile>,
    #[command(subcommand)]
    command: Mode,
}

#[derive(Subcommand)]
enum Mode {
    Fuzz {
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
        cases: u64,
        #[arg(long)]
        seed: Option<u64>,
        #[arg(
            long,
            help = "Stop at the first failure without shrinking or executing it again"
        )]
        fail_fast: bool,
        #[arg(
            long,
            value_enum,
            value_delimiter = ',',
            help = "Disable selected Compatible generators; comparison remains strict"
        )]
        disable_features: Vec<Feature>,
        #[arg(
            long,
            value_enum,
            value_delimiter = ',',
            help = "Enable Compatible generators disabled by default (including prerequisites)"
        )]
        enable_features: Vec<Feature>,
    },
    Replay {
        sql: PathBuf,
    },
    Compare {
        reference: PathBuf,
        actual: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    if cfg!(panic = "abort") && matches!(args.command, Mode::Fuzz { .. }) {
        anyhow::bail!("fuzzing requires panic unwinding; build with cargo build -p turso_pg_dump_fuzzer --profile pg-dump-fuzzer and use target/pg-dump-fuzzer/pg-dump-fuzzer");
    }
    if let Mode::Compare { reference, actual } = &args.command {
        let mut progress = progress::Progress::new(!args.no_progress);
        progress.begin("comparing dumps");
        progress.stage("comparing dump ASTs and COPY rows");
        let result = compare_files(reference, actual);
        progress.complete(result.as_ref().err());
        progress.finish();
        return result;
    }
    let profile = match (args.profile, &args.command) {
        (Some(profile), _) => profile,
        (None, Mode::Replay { sql }) => replay_profile(sql)?,
        _ => Profile::Compatible,
    };
    let disabled_features = match &args.command {
        Mode::Fuzz {
            disable_features,
            enable_features,
            ..
        } => {
            if profile != Profile::Compatible {
                if !disable_features.is_empty() || !enable_features.is_empty() {
                    anyhow::bail!("feature overrides apply only to --profile compatible");
                }
                Vec::new()
            } else {
                generation::disabled_features(enable_features, disable_features)?
            }
        }
        _ => Vec::new(),
    };
    let saved_features = match &args.command {
        Mode::Replay { sql } => match fs::read(sql.with_file_name("disabled-features.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        },
        _ => disabled_features
            .iter()
            .map(|feature| feature.to_possible_value().unwrap().get_name().to_owned())
            .collect(),
    };
    let runner = Runner {
        reference: args.postgres_dsn.context(
            "set PG_DSN or --postgres-dsn to a disposable server with CREATEDB permission",
        )?,
        binary: args.binary.canonicalize()?,
        output: args.output,
        psql: args.psql,
        pg_dump: args.pg_dump,
        privileges: matches!(profile, Profile::Compatible),
        disabled_features: saved_features,
    };
    if matches!(profile, Profile::Compatible)
        && url::Url::parse(&runner.reference)?.username() != "postgres"
    {
        anyhow::bail!("Compatible default privileges include their creating role: use PG_DSN with role postgres, or --profile tables");
    }
    match args.command {
        Mode::Fuzz {
            cases,
            seed,
            fail_fast,
            ..
        } => {
            let mut progress = progress::Progress::new(!args.no_progress);
            if !disabled_features.is_empty() {
                progress.stage(&format!(
                    "disabled generators: {}",
                    runner.disabled_features.join(", ")
                ));
            }
            let seed_label = seed.map_or_else(|| "automatic".to_owned(), |seed| seed.to_string());
            progress.stage(&format!(
                "generating SQL with Hegel: target {cases} cases, seed {seed_label}"
            ));
            let mut settings = hegel::Settings::new().test_cases(cases).seed(seed);
            if fail_fast {
                settings = settings.phases([hegel::Phase::Generate]);
            }
            let mut summary = turso_pg_dump_fuzzer::summary::Summary::default();
            let mut first_failure: Option<String> = None;
            let result = catch_unwind(AssertUnwindSafe(|| {
                hegel::Hegel::new(|tc: TestCase| {
                    let error = if let Some(error) = &first_failure {
                        error.clone()
                    } else {
                        let sql = tc.draw(workload(profile, disabled_features.clone()));
                        progress.begin("generated SQL; starting database check");
                        tc.note(&sql);
                        let result = runner.check_with_summary(
                            &sql,
                            &|stage| progress.stage(stage),
                            &mut summary,
                        );
                        progress.complete(result.as_ref().err());
                        match result {
                            Ok(()) => return,
                            Err(error) => format!("{error:#}"),
                        }
                    };
                    if fail_fast {
                        first_failure = Some(error.clone());
                    }
                    panic!("{error}");
                })
                .settings(settings)
                .run();
            }));
            progress.finish();
            if let Err(error) = summary.finish(&runner.output, seed, &runner.disabled_features) {
                eprintln!("Could not save generation summary: {error:#}");
                if result.is_ok() {
                    return Err(error);
                }
            }
            if let Err(panic) = result {
                resume_unwind(panic);
            }
            println!("Dump differential fuzzing passed");
            Ok(())
        }
        Mode::Replay { sql } => {
            let sql = fs::read_to_string(sql)?;
            let mut progress = progress::Progress::new(!args.no_progress);
            progress.begin("replaying SQL");
            let result = runner.check_with_progress(&sql, &|stage| progress.stage(stage));
            progress.complete(result.as_ref().err());
            progress.finish();
            result
        }
        Mode::Compare { .. } => unreachable!(),
    }
}

fn replay_profile(sql: &Path) -> Result<Profile> {
    let metadata = sql.with_file_name("profile.txt");
    match fs::read_to_string(metadata) {
        Ok(profile) => match profile.trim() {
            "tables" => Ok(Profile::Tables),
            "compatible" => Ok(Profile::Compatible),
            other => anyhow::bail!("unknown saved profile: {other}"),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Profile::Compatible),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators as gs;

    #[hegel::test(test_cases = 20)]
    fn replay_preserves_saved_profile(tc: TestCase) {
        let compatible = tc.draw(gs::booleans());
        let n = tc.draw(gs::integers::<u32>());
        let directory = tempfile::tempdir().unwrap();
        let sql = directory.path().join(format!("case {n}.sql"));
        fs::write(
            directory.path().join("profile.txt"),
            if compatible { "compatible" } else { "tables" },
        )
        .unwrap();
        assert_eq!(
            replay_profile(&sql).unwrap(),
            if compatible {
                Profile::Compatible
            } else {
                Profile::Tables
            }
        );
    }
}
