use clap::{Args, ValueEnum};
use clap_complete::{ArgValueCompleter, CompletionCandidate, PathCompleter};

use crate::{input::OutputMode, opcodes_dictionary::OPCODE_DESCRIPTIONS};

#[derive(Debug, Clone, Args)]
pub struct ExitArgs {
    /// Exit code
    #[arg(default_value_t = 0)]
    pub code: i32,
}

#[derive(Debug, Clone, Args)]
pub struct OpenArgs {
    /// Path to open database
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    pub path: String,
    // TODO see how to have this completed with the output of List Vfs function
    // Currently not possible to pass arbitrary
    /// Name of VFS
    pub vfs_name: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct SchemaArgs {
    // TODO depends on PRAGMA table_list for completions
    /// Table name to visualize schema
    pub table_name: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct SetOutputArgs {
    /// File path to send output to
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct OutputModeArgs {
    #[arg(value_enum)]
    pub mode: OutputMode,
}

fn opcodes_completer(current: &std::ffi::OsStr) -> Vec<CompletionCandidate> {
    let mut completions = vec![];

    let Some(current) = current.to_str() else {
        return completions;
    };

    let current = current.to_lowercase();

    let opcodes = &OPCODE_DESCRIPTIONS;

    for op in opcodes {
        // TODO if someone know how to do prefix_match with case insensitve in Rust
        // without converting the String to lowercase first, please fix this.
        let op_name = op.name.to_ascii_lowercase();
        if op_name.starts_with(&current) {
            completions.push(CompletionCandidate::new(op.name).help(Some(op.description.into())));
        }
    }

    completions
}

#[derive(Debug, Clone, Args)]
pub struct OpcodesArgs {
    /// Opcode to display description
    #[arg(add = ArgValueCompleter::new(opcodes_completer))]
    pub opcode: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct CwdArgs {
    /// Target directory
    #[arg(add = ArgValueCompleter::new(PathCompleter::dir()))]
    pub directory: String,
}

#[derive(Debug, Clone, Args)]
pub struct NullValueArgs {
    pub value: String,
}

#[derive(Debug, Clone, Args)]
pub struct EchoArgs {
    #[arg(value_enum)]
    pub mode: EchoMode,
}

#[derive(Debug, ValueEnum, Clone)]
pub enum EchoMode {
    On,
    Off,
}

#[derive(Debug, Clone, Args)]
pub struct TablesArgs {
    pub pattern: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct LoadExtensionArgs {
    /// Path to extension file
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    pub path: String,
}
