use clap::Args;
use clap_complete::{ArgValueCompleter, PathCompleter};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};
use turso_core::{Connection, LimboError};

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
        // Check if the target table exists
        let table_check_query = format!(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='{}';",
            args.table
        );

        let mut table_exists = false;

        match self.conn.query(table_check_query) {
            Ok(rows) => {
                if let Some(mut rows) = rows {
                    let res = rows.run_with_row_callback(|_| {
                        table_exists = true;
                        Ok(())
                    });
                    if let Err(e) = res {
                        let _ = self.writer.write_all(
                            format!("Error checking table existence: {e:?}\n").as_bytes(),
                        );
                        return;
                    }
                }
            }
            Err(e) => {
                let _ = self
                    .writer
                    .write_all(format!("Error checking table existence: {e:?}\n").as_bytes());
                return;
            }
        }

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

        let mut records = rdr.records().skip(args.skip as usize).peekable();

        // If table doesn't exist, use first row as header to create table
        if !table_exists {
            if let Some(Ok(header)) = records.next() {
                let columns = header
                    .iter()
                    .map(normalize_ident)
                    .collect::<Vec<_>>()
                    .join(", ");
                let create_table = format!("CREATE TABLE {} ({});", args.table, columns);

                let rows = match self.conn.query(create_table) {
                    Ok(rows) => rows,
                    Err(e) => {
                        let _ = self
                            .writer
                            .write_all(format!("Error creating table: {e:?}\n").as_bytes());
                        return;
                    }
                };
                let Some(mut rows) = rows else {
                    let _ = self.writer.write_all(b"Error creating table\n");
                    return;
                };

                let res = rows.run_with_row_callback(|_| {
                    // Not expected for CREATE TABLE
                    panic!("Unexpected row for CREATE TABLE");
                });
                match res {
                    Ok(_) => {}
                    Err(LimboError::Busy | LimboError::Interrupt) => {
                        let _ = self
                            .writer
                            .write_all("Error creating table: interrupted / busy\n".as_bytes());
                        return;
                    }
                    Err(e) => {
                        let _ = self.writer.write_all(
                            format!("Error checking table existence: {e:?}\n").as_bytes(),
                        );
                        return;
                    }
                }
            } else {
                let _ = self.writer.write_all(b"Error: Empty input file\n");
                return;
            }
        }

        /// TODO: should this be in a single transaction (i.e. all or nothing)?
        const CSV_INSERT_BATCH_SIZE: usize = 1000;
        let mut batch = Vec::with_capacity(CSV_INSERT_BATCH_SIZE);
        for result in records {
            let record = match result {
                Ok(r) => r,
                Err(e) => {
                    failed_rows += 1;
                    let _ = self
                        .writer
                        .write_all(format!("Error reading row: {e:?}\n").as_bytes());
                    continue;
                }
            };

            if !record.is_empty() {
                let values: Vec<String> = record
                    .iter()
                    .map(|r| format!("'{}'", r.replace("'", "''")))
                    .collect();
                batch.push(values.join(","));

                if batch.len() >= CSV_INSERT_BATCH_SIZE {
                    println!("Inserting batch of {} rows", batch.len());
                    let insert_string =
                        format!("INSERT INTO {} VALUES ({});", args.table, batch.join("),("));

                    match self.conn.query(insert_string) {
                        Ok(rows) => {
                            if let Some(mut rows) = rows {
                                let res = rows.run_with_row_callback(|_| {
                                    panic!("Unexpected row for INSERT");
                                });
                                match res {
                                    Ok(_) => {
                                        success_rows += batch.len() as u64;
                                    }
                                    Err(LimboError::Interrupt) => {
                                        let _ = self.writer.write_all(b"interrupt\n");

                                        failed_rows += batch.len() as u64;
                                    }
                                    Err(LimboError::Busy) => {
                                        let _ = self.writer.write_all(b"database is busy\n");

                                        failed_rows += batch.len() as u64;
                                    }
                                    Err(e) => {
                                        let _ = self.writer.write_all(
                                            format!("Error executing query: {e:?}\n").as_bytes(),
                                        );
                                        failed_rows += batch.len() as u64;
                                    }
                                }
                            } else {
                                success_rows += batch.len() as u64;
                            }
                        }
                        Err(e) => {
                            let _ = self
                                .writer
                                .write_all(format!("Error executing query: {e:?}\n").as_bytes());
                            failed_rows += batch.len() as u64;
                        }
                    }
                    batch.clear();
                }
            }
        }

        // Insert remaining records
        if !batch.is_empty() {
            let insert_string =
                format!("INSERT INTO {} VALUES ({});", args.table, batch.join("),("));

            match self.conn.query(insert_string) {
                Ok(rows) => {
                    if let Some(mut rows) = rows {
                        let res = rows.run_with_row_callback(|_| {
                            panic!("Unexpected row for INSERT");
                        });
                        match res {
                            Ok(_) => {
                                success_rows += batch.len() as u64;
                            }
                            Err(LimboError::Interrupt) => {
                                let _ = self.writer.write_all(b"interrupt\n");

                                failed_rows += batch.len() as u64;
                            }
                            Err(LimboError::Busy) => {
                                let _ = self.writer.write_all(b"database is busy\n");

                                failed_rows += batch.len() as u64;
                            }
                            Err(e) => {
                                let _ = self.writer.write_all(
                                    format!("Error executing query: {e:?}\n").as_bytes(),
                                );
                                failed_rows += batch.len() as u64;
                            }
                        }
                    } else {
                        success_rows += batch.len() as u64;
                    }
                }
                Err(e) => {
                    let _ = self
                        .writer
                        .write_all(format!("Error executing query: {e:?}\n").as_bytes());
                    failed_rows += batch.len() as u64;
                }
            }
        }

        if args.verbose {
            let _ = self.writer.write_all(
                format!(
                    "Added {} rows with {} errors using {} lines of input",
                    success_rows,
                    failed_rows,
                    success_rows + failed_rows,
                )
                .as_bytes(),
            );
        }
    }
}

// https://sqlite.org/lang_keywords.html
const QUOTE_PAIRS: &[(char, char)] = &[('"', '"'), ('[', ']'), ('`', '`')];

pub fn normalize_ident(identifier: &str) -> String {
    let quote_pair = QUOTE_PAIRS
        .iter()
        .find(|&(start, end)| identifier.starts_with(*start) && identifier.ends_with(*end));

    if let Some(&(_, _)) = quote_pair {
        &identifier[1..identifier.len() - 1]
    } else {
        identifier
    }
    .to_lowercase()
}
