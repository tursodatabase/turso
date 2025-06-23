use clap::Args;
use clap_complete::{ArgValueCompleter, PathCompleter};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};
use turso_core::Connection;

#[derive(Debug, Clone, Args)]
pub struct ImportArgs {
    /// Use , and \n as column and row separators
    #[arg(long, default_value = "true")]
    csv: bool,
    /// "Verbose" - increase auxiliary output
    #[arg(short, default_value = "false")]
    verbose: bool,
    /// Skip the first N rows of input
    #[arg(long, default_value = "0")]
    skip: u64,
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    file: PathBuf,
    table: String,
}

pub struct ImportFile<'a> {
    conn: Arc<Connection>,
    writer: &'a mut dyn Write,
}

impl<'a> ImportFile<'a> {
    pub fn new(conn: Arc<Connection>, writer: &'a mut dyn Write) -> Self {
        Self { conn, writer }
    }

    pub fn import(&mut self, args: ImportArgs) {
        self.import_csv(args);
    }

    pub fn import_csv(&mut self, args: ImportArgs) {
        let file = match File::open(args.file) {
            Ok(file) => file,
            Err(e) => {
                let _ = self.writer.write_all(format!("{e:?}\n").as_bytes());
                return;
            }
        };

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        let mut success_rows = 0u64;
        let mut failed_rows = 0u64;

        for result in rdr.records().skip(args.skip as usize) {
            let record = result.unwrap();

            if !record.is_empty() {
                let mut values_string = String::new();

                for r in record.iter() {
                    values_string.push('\'');
                    // The string can have a single quote which needs to be escaped
                    values_string.push_str(&r.replace("'", "''"));
                    values_string.push_str("',");
                }

                // remove the last comma after last element
                values_string.pop();

                let insert_string =
                    format!("INSERT INTO {} VALUES ({});", args.table, values_string);

                match self.conn.query(insert_string) {
                    Ok(rows) => {
                        if let Some(mut rows) = rows {
                            while let Ok(x) = rows.step() {
                                match x {
                                    turso_core::StepResult::IO => {
                                        rows.run_once().unwrap();
                                    }
                                    turso_core::StepResult::Done => break,
                                    turso_core::StepResult::Interrupt => break,
                                    turso_core::StepResult::Busy => {
                                        let _ =
                                            self.writer.write_all("database is busy\n".as_bytes());
                                        break;
                                    }
                                    turso_core::StepResult::Row => todo!(),
                                }
                            }
                        }
                        success_rows += 1;
                    }
                    Err(_err) => {
                        failed_rows += 1;
                    }
                }
            }
        }

        if args.verbose {
            let _ = self.writer.write_all(
                format!(
                    "Added {} rows with {} errors using {} lines of input\n",
                    success_rows,
                    failed_rows,
                    success_rows + failed_rows
                )
                .as_bytes(),
            );
        }
    }
}
