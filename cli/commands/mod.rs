pub mod args;
pub mod import;

use args::{
    CwdArgs, EchoArgs, ExitArgs, LoadExtensionArgs, NullValueArgs, OpcodesArgs, OpenArgs,
    OutputModeArgs, SchemaArgs, SetOutputArgs, TablesArgs, TimerArgs,
};
use clap::Parser;
use import::ImportArgs;

use crate::input::{AFTER_HELP_MSG, BEFORE_HELP_MSG};

#[derive(Parser, Debug)]
#[command(
    multicall = true,
    arg_required_else_help(false),
    before_help(BEFORE_HELP_MSG),
    after_help(AFTER_HELP_MSG),
    // help_template(HELP_TEMPLATE)
)]
pub struct CommandParser {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, clap::Subcommand)]
#[command(disable_help_flag(false), disable_version_flag(true))]
pub enum Command {
    /// Exit this program with return-code CODE
    #[command(display_name = ".exit")]
    Exit(ExitArgs),
    /// Quit the shell
    #[command(display_name = ".quit")]
    Quit,
    /// Open a database file
    #[command(display_name = ".open")]
    Open(OpenArgs),
    /// Display schema for a table
    #[command(display_name = ".schema")]
    Schema(SchemaArgs),
    /// Set output file (or stdout if empty)
    #[command(name = "output", display_name = ".output")]
    SetOutput(SetOutputArgs),
    /// Set output display mode
    #[command(name = "mode", display_name = ".mode", arg_required_else_help(false))]
    OutputMode(OutputModeArgs),
    /// Show vdbe opcodes
    #[command(name = "opcodes", display_name = ".opcodes")]
    Opcodes(OpcodesArgs),
    /// Change the current working directory
    #[command(name = "cd", display_name = ".cd")]
    Cwd(CwdArgs),
    /// Display information about settings
    #[command(name = "show", display_name = ".show")]
    ShowInfo,
    /// Set the value of NULL to be printed in 'list' mode
    #[command(name = "nullvalue", display_name = ".nullvalue")]
    NullValue(NullValueArgs),
    /// Toggle 'echo' mode to repeat commands before execution
    #[command(display_name = ".echo")]
    Echo(EchoArgs),
    /// Display tables
    Tables(TablesArgs),
    /// Import data from FILE into TABLE
    #[command(name = "import", display_name = ".import")]
    Import(ImportArgs),
    /// Loads an extension library
    #[command(name = "load", display_name = ".load")]
    LoadExtension(LoadExtensionArgs),
    /// Dump the current database as a list of SQL statements
    Dump,
    /// List vfs modules available
    #[command(name = "vfslist", display_name = ".vfslist")]
    ListVfs,
    #[command(name = "timer", display_name = ".timer")]
    Timer(TimerArgs),
}

const _HELP_TEMPLATE: &str = "{before-help}{name}
{usage-heading} {usage}

{all-args}{after-help}
";

#[cfg(test)]
mod tests {
    use super::CommandParser;

    #[test]
    fn cli_assert() {
        use clap::CommandFactory;
        CommandParser::command().debug_assert();
    }
}
