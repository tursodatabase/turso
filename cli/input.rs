use crate::app::Opts;
use clap::ValueEnum;
use std::{
    io::{self, Write},
    sync::Arc,
};

#[derive(Copy, Clone)]
pub enum DbLocation {
    Memory,
    Path,
}

#[derive(Copy, Clone, ValueEnum)]
pub enum Io {
    Syscall,
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring,
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
                    Io::IoUring
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
    Raw,
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
}

impl From<&Opts> for Settings {
    fn from(opts: &Opts) -> Self {
        Self {
            null_value: String::new(),
            output_mode: opts.output_mode,
            echo: false,
            is_stdout: opts.output.is_empty(),
            output_filename: opts.output.clone(),
            db_file: opts
                .database
                .as_ref()
                .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string()),
            io: opts.io,
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

pub fn get_io(db_location: DbLocation, io_choice: Io) -> anyhow::Result<Arc<dyn limbo_core::IO>> {
    Ok(match db_location {
        DbLocation::Memory => Arc::new(limbo_core::MemoryIO::new()?),
        DbLocation::Path => {
            match io_choice {
                Io::Syscall => {
                    // We are building for Linux/macOS and syscall backend has been selected
                    #[cfg(target_family = "unix")]
                    {
                        Arc::new(limbo_core::UnixIO::new()?)
                    }
                    // We are not building for Linux/macOS and syscall backend has been selected
                    #[cfg(not(target_family = "unix"))]
                    {
                        Arc::new(limbo_core::PlatformIO::new()?)
                    }
                }
                // We are building for Linux and io_uring backend has been selected
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                Io::IoUring => Arc::new(limbo_core::UringIO::new()?),
            }
        }
    })
}

pub const HELP_MSG: &str = r#"
Limbo SQL Shell Help
==============
Welcome to the Limbo SQL Shell! You can execute any standard SQL command here.
In addition to standard SQL commands, the following special commands are available:

Special Commands:
-----------------
.quit                      Stop interpreting input stream and exit.
.show                      Display current settings.
.open <database_file>      Open and connect to a database file.
.output <mode>             Change the output mode. Available modes are 'raw' and 'pretty'.
.schema <table_name>       Show the schema of the specified table.
.tables <pattern>          List names of tables matching LIKE pattern TABLE
.opcodes                   Display all the opcodes defined by the virtual machine
.cd <directory>            Change the current working directory.
.nullvalue <string>        Set the value to be displayed for null values.
.echo on|off               Toggle echo mode to repeat commands before execution.
.import --csv FILE TABLE   Import csv data from FILE into TABLE
.help                      Display this help message.

Usage Examples:
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

Note:
- All SQL commands must end with a semicolon (;).
- Special commands do not require a semicolon."#;
