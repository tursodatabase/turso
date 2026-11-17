use crate::{
    dump_ast,
    summary::{Generated, Summary},
};
use anyhow::{bail, Context, Result};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use url::Url;

pub struct Runner {
    pub reference: String,
    pub binary: PathBuf,
    pub output: PathBuf,
    pub psql: PathBuf,
    pub pg_dump: PathBuf,
    pub privileges: bool,
    pub disabled_features: Vec<String>,
}

impl Runner {
    pub fn check(&self, sql: &str) -> Result<()> {
        self.check_with_progress(sql, &|_| {})
    }

    pub fn check_with_progress(&self, sql: &str, stage: &dyn Fn(&str)) -> Result<()> {
        self.check_with_summary(sql, stage, &mut Summary::default())
    }

    pub fn check_with_summary(
        &self,
        sql: &str,
        stage: &dyn Fn(&str),
        summary: &mut Summary,
    ) -> Result<()> {
        summary.attempts += 1;
        let generated = Generated::parse(sql);
        match &generated {
            Ok(generated) => summary.generated.add(generated),
            Err(_) => summary.unparsed_workloads += 1,
        }
        let result = self.check_case(sql, stage, summary, generated);
        if result.is_ok() {
            summary.passed += 1;
        } else {
            summary.failed += 1;
        }
        result
    }

    fn check_case(
        &self,
        sql: &str,
        stage: &dyn Fn(&str),
        summary: &mut Summary,
        generated: Result<Generated>,
    ) -> Result<()> {
        stage("saving input");
        fs::create_dir_all(&self.output)?;
        let case = tempfile::Builder::new()
            .prefix("case-")
            .tempdir_in(&self.output)?;
        let result = (|| {
            fs::write(case.path().join("input.sql"), sql)?;
            fs::write(
                case.path().join("profile.txt"),
                if self.privileges {
                    "compatible"
                } else {
                    "tables"
                },
            )?;
            fs::write(
                case.path().join("disabled-features.json"),
                serde_json::to_vec_pretty(&self.disabled_features)?,
            )?;
            let generated = generated?;
            self.check_in(case.path(), stage, summary, &generated)
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                let path = case.keep();
                if let Err(write_error) = fs::write(path.join("failure.txt"), format!("{error:#}"))
                {
                    bail!(
                        "artifacts: {}; {error:#}; could not write failure.txt: {write_error}",
                        path.display()
                    );
                }
                Err(error).with_context(|| format!("artifacts: {}", path.display()))
            }
        }
    }

    fn check_in(
        &self,
        directory: &Path,
        stage: &dyn Fn(&str),
        summary: &mut Summary,
        generated: &Generated,
    ) -> Result<()> {
        let mut reference =
            Url::parse(&self.reference).context("PG_DSN must be a PostgreSQL URL")?;
        if !matches!(reference.scheme(), "postgres" | "postgresql") {
            bail!("PG_DSN must be a PostgreSQL URL");
        }
        let database = format!(
            "dump_fuzz_{}_{}",
            std::process::id(),
            directory
                .file_name()
                .unwrap()
                .to_string_lossy()
                .replace('-', "_")
        );
        let query: Vec<_> = reference
            .query_pairs()
            .filter(|(key, _)| key != "dbname")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        reference.set_query(None);
        reference.query_pairs_mut().extend_pairs(query);
        reference.set_path(&format!("/{database}"));
        stage("creating PostgreSQL database");
        let create = self
            .psql(&self.reference)
            .args([
                "--command",
                &format!("CREATE DATABASE \"{database}\" TEMPLATE template0"),
            ])
            .run_logged(&directory.join("create.log"));
        let result = create.and_then(|()| {
            self.dump_pair(directory, reference.as_str(), stage, summary, generated)
        });
        stage("dropping PostgreSQL database");
        let cleanup = self
            .psql(&self.reference)
            .args([
                "--command",
                &format!("DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"),
            ])
            .run_logged(&directory.join("drop.log"));
        match (result, cleanup) {
            (Err(error), Err(cleanup)) => bail!("{error:#}; cleanup also failed: {cleanup:#}"),
            (Err(error), _) | (_, Err(error)) => Err(error),
            (Ok(()), Ok(())) => {
                stage("comparing dump ASTs and COPY rows");
                compare_files(
                    &directory.join("postgres.sql"),
                    &directory.join("turso.sql"),
                )
            }
        }
    }

    fn dump_pair(
        &self,
        directory: &Path,
        reference: &str,
        stage: &dyn Fn(&str),
        summary: &mut Summary,
        generated: &Generated,
    ) -> Result<()> {
        stage("starting tursopg");
        let log_path = directory.join("server.log");
        let log = File::create(&log_path)?;
        let mut server = Process(
            Command::new(&self.binary)
                .arg("--server")
                .arg("127.0.0.1:0")
                .arg(directory.join("turso.db"))
                .stdin(Stdio::null())
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()
                .context("start tursopg")?,
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        let port = loop {
            if let Some(status) = server.0.try_wait()? {
                bail!("tursopg exited during startup: {status}; see server.log");
            }
            let log = fs::read_to_string(&log_path)?;
            if let Some((_, rest)) = log.split_once("PostgreSQL server listening on 127.0.0.1:") {
                let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                let port: u16 = digits.parse().context("invalid server port")?;
                break port;
            }
            if Instant::now() >= deadline {
                bail!("tursopg startup timed out; see server.log");
            }
            thread::sleep(Duration::from_millis(20));
        };
        let turso = format!("postgresql://turso@127.0.0.1:{port}/main?sslmode=disable");
        for (name, dsn) in [("postgres", reference), ("turso", turso.as_str())] {
            stage(&format!("loading SQL into {name}"));
            let bootstrap = self
                .psql(dsn)
                .arg("--file")
                .arg(directory.join("input.sql"))
                .run_logged(&directory.join(format!("{name}-bootstrap.log")));
            summary.loaded(
                name,
                generated,
                bootstrap.is_ok(),
                &fs::read_to_string(directory.join(format!("{name}-bootstrap.log")))?,
            );
            bootstrap?;
            stage(&format!("dumping {name}"));
            let mut dump = Command::new(&self.pg_dump);
            if !self.privileges {
                dump.arg("--no-privileges");
            }
            dump.args([
                "--no-password",
                "--restrict-key=TursoPgdumpConformance",
                "--no-owner",
                "--dbname",
                dsn,
                "--file",
            ])
            .arg(directory.join(format!("{name}.sql")))
            .run_logged(&directory.join(format!("{name}-dump.log")))?;
            summary.dumped(
                name,
                &fs::read_to_string(directory.join(format!("{name}.sql")))?,
            )?;
        }
        Ok(())
    }

    fn psql(&self, dsn: &str) -> Command {
        let mut command = Command::new(&self.psql);
        command.args([
            "-X",
            "--no-password",
            "--set",
            "ON_ERROR_STOP=1",
            "--dbname",
            dsn,
        ]);
        command
    }
}

pub fn compare_files(reference: &Path, actual: &Path) -> Result<()> {
    let mut asts = Vec::new();
    for path in [reference, actual] {
        let ast = dump_ast(&fs::read_to_string(path)?)
            .with_context(|| format!("invalid dump: {}", path.display()))?;
        fs::write(
            path.with_extension("ast.json"),
            serde_json::to_string_pretty(&ast)?,
        )?;
        asts.push(ast);
    }
    if asts[0] != asts[1] {
        let first = asts[0]
            .iter()
            .zip(&asts[1])
            .position(|(a, b)| a != b)
            .unwrap_or_else(|| asts[0].len().min(asts[1].len()));
        bail!("dump ASTs differ at statement {first}; compare the .ast.json files");
    }
    Ok(())
}

trait LoggedCommand {
    fn run_logged(&mut self, log: &Path) -> Result<()>;
}

impl LoggedCommand for Command {
    fn run_logged(&mut self, log: &Path) -> Result<()> {
        let output = File::create(log)?;
        let mut process = Process(
            self.stdin(Stdio::null())
                .stdout(output.try_clone()?)
                .stderr(output)
                .env("PGCONNECT_TIMEOUT", "5")
                .spawn()
                .with_context(|| format!("start command for {}", log.display()))?,
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = process.0.try_wait()? {
                if !status.success() {
                    bail!("command exited with {status}; see {}", log.display());
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("command timed out; see {}", log.display());
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
