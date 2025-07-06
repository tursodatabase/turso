use crate::app::Opts;
use clap::ValueEnum;
use std::{
    fmt::{Display, Formatter},
    io::{self, Write},
    sync::Arc,
};

#[derive(Copy, Clone)]
pub enum DbLocation {
    Memory,
    Path,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug)]
pub enum Io {
    Syscall,
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring,
    External(String),
    Memory,
}

impl Display for Io {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Io::Memory => write!(f, "memory"),
            Io::Syscall => write!(f, "syscall"),
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            Io::IoUring => write!(f, "io_uring"),
            Io::External(str) => write!(f, "{}", str),
        }
    }
}

impl Default for Io {
    /// Custom Default impl with cfg! macro, to provide compile-time default to Clap based on platform
    /// The cfg! could be elided, but Clippy complains
    /// The default value can still be overridden with the Clap argument
    fn default() -> Self {
        match cfg!(all(target_os = "linux", feature = "io_uring")) {
            true => {
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                {
                    Io::Syscall // FIXME: make io_uring faster so it can be the default
                }
                #[cfg(any(
                    not(target_os = "linux"),
                    all(target_os = "linux", not(feature = "io_uring"))
                ))]
                {
                    Io::Syscall
                }
            }
            false => Io::Syscall,
        }
    }
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
pub enum OutputMode {
    List,
    Pretty,
}

impl std::fmt::Display for OutputMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_possible_value()
            .expect("no values are skipped")
            .get_name()
            .fmt(f)
    }
}

pub struct Settings {
    pub output_filename: String,
    pub db_file: String,
    pub null_value: String,
    pub output_mode: OutputMode,
    pub echo: bool,
    pub is_stdout: bool,
    pub io: Io,
    pub tracing_output: Option<String>,
    pub timer: bool,
    pub enable_mvcc: bool,
    pub enable_indexes: bool,
}

impl From<Opts> for Settings {
    fn from(opts: Opts) -> Self {
        Self {
            null_value: String::new(),
            output_mode: opts.output_mode,
            echo: false,
            is_stdout: opts.output.is_empty(),
            output_filename: opts.output,
            db_file: opts
                .database
                .as_ref()
                .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string()),
            io: match opts.vfs.as_ref().unwrap_or(&String::new()).as_str() {
                "memory" | ":memory:" => Io::Memory,
                "syscall" => Io::Syscall,
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                "io_uring" => Io::IoUring,
                "" => Io::default(),
                vfs => Io::External(vfs.to_string()),
            },
            tracing_output: opts.tracing_output,
            timer: false,
            enable_mvcc: opts.experimental_mvcc,
            enable_indexes: opts.experimental_indexes,
        }
    }
}

impl std::fmt::Display for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Settings:\nOutput mode: {}\nDB: {}\nOutput: {}\nNull value: {}\nCWD: {}\nEcho: {}",
            self.output_mode,
            self.db_file,
            match self.is_stdout {
                true => "STDOUT",
                false => &self.output_filename,
            },
            self.null_value,
            std::env::current_dir().unwrap().display(),
            match self.echo {
                true => "on",
                false => "off",
            }
        )
    }
}

pub fn get_writer(output: &str) -> Box<dyn Write> {
    match output {
        "" => Box::new(io::stdout()),
        _ => match std::fs::File::create(output) {
            Ok(file) => Box::new(file),
            Err(e) => {
                eprintln!("Error: {}", e);
                Box::new(io::stdout())
            }
        },
    }
}

pub fn get_io(db_location: DbLocation, io_choice: &str) -> anyhow::Result<Arc<dyn turso_core::IO>> {
    Ok(match db_location {
        DbLocation::Memory => Arc::new(turso_core::MemoryIO::new()),
        DbLocation::Path => {
            match io_choice {
                "memory" => Arc::new(turso_core::MemoryIO::new()),
                "syscall" => {
                    // We are building for Linux/macOS and syscall backend has been selected
                    #[cfg(target_family = "unix")]
                    {
                        Arc::new(turso_core::UnixIO::new()?)
                    }
                    // We are not building for Linux/macOS and syscall backend has been selected
                    #[cfg(not(target_family = "unix"))]
                    {
                        Arc::new(turso_core::PlatformIO::new()?)
                    }
                }
                // We are building for Linux and io_uring backend has been selected
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                "io_uring" => Arc::new(turso_core::UringIO::new()?),
                _ => Arc::new(turso_core::PlatformIO::new()?),
            }
        }
    })
}

pub const BEFORE_HELP_MSG: &str = r#"

Limbo SQL Shell Help
==============
Welcome to the Limbo SQL Shell! You can execute any standard SQL command here.
In addition to standard SQL commands, the following special commands are available:"#;
pub const AFTER_HELP_MSG: &str = r#"Usage Examples:
---------------
1. To quit the Limbo SQL Shell:
   .quit

2. To open a database file at path './employees.db':
   .open employees.db

3. To view the schema of a table named 'employees':
   .schema employees

4. To list all tables:
   .tables

5. To list all available SQL opcodes:
   .opcodes

6. To change the current output mode to 'pretty':
   .mode pretty

7. Send output to STDOUT if no file is specified:
   .output

8. To change the current working directory to '/tmp':
   .cd /tmp

9. Show the current values of settings:
   .show

10. To import csv file 'sample.csv' into 'csv_table' table:
   .import --csv sample.csv csv_table

11. To display the database contents as SQL:
   .dump

12. To load an extension library:
   .load /target/debug/liblimbo_regexp

13. To list all available VFS:
   .listvfs
14. To show names of indexes:
   .indexes ?TABLE?

Note:
- All SQL commands must end with a semicolon (;).
- Special commands start with a dot (.) and are not required to end with a semicolon."#;
