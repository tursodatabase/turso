use clap::{command, Parser};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
#[command(name = "limbo-simulator")]
#[command(author, version, about, long_about = None)]
pub struct SimulatorCLI {
    #[clap(short, long, help = "set seed for reproducible runs", default_value = None)]
    pub seed: Option<u64>,
    #[clap(
        short,
        long,
        help = "enable doublechecking, run the simulator with the plan twice and check output equality"
    )]
    pub doublecheck: bool,
    #[clap(
        short = 'n',
        long,
        help = "change the maximum size of the randomly generated sequence of interactions",
        default_value_t = 5000
    )]
    pub maximum_tests: usize,
    #[clap(
        short = 'k',
        long,
        help = "change the minimum size of the randomly generated sequence of interactions",
        default_value_t = 1000
    )]
    pub minimum_tests: usize,
    #[clap(
        short = 't',
        long,
        help = "change the maximum time of the simulation(in seconds)",
        default_value_t = 60 * 60 // default to 1 hour
    )]
    pub maximum_time: usize,
    #[clap(short = 'l', long, help = "load plan from the bug base")]
    pub load: Option<String>,
    #[clap(
        short = 'w',
        long,
        help = "enable watch mode that reruns the simulation on file changes"
    )]
    pub watch: bool,
    #[clap(long, help = "run differential testing between sqlite and Limbo")]
    pub differential: bool,
    #[clap(subcommand)]
    pub subcommand: Option<SimulatorCommand>,
    #[clap(long, help = "disable BugBase", default_value_t = false)]
    pub disable_bugbase: bool,
    #[clap(long, help = "disable UPDATE Statement", default_value_t = false)]
    pub disable_update: bool,
    #[clap(long, help = "disable DELETE Statement", default_value_t = false)]
    pub disable_delete: bool,
    #[clap(long, help = "disable CREATE Statement", default_value_t = false)]
    pub disable_create: bool,
    #[clap(long, help = "disable CREATE INDEX Statement", default_value_t = false)]
    pub disable_create_index: bool,
    #[clap(long, help = "disable DROP Statement", default_value_t = false)]
    pub disable_drop: bool,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub enum SimulatorCommand {
    #[clap(about = "run the simulator in a loop")]
    Loop {
        #[clap(
            short = 'n',
            long,
            help = "number of iterations to run the simulator",
            default_value_t = 5
        )]
        n: usize,
        #[clap(
            short = 's',
            long,
            help = "short circuit the simulator, stop on the first failure",
            default_value_t = false
        )]
        short_circuit: bool,
    },
    #[clap(about = "list all the bugs in the base")]
    List,
    #[clap(about = "run the simulator against a specific bug")]
    Test {
        #[clap(
            short = 'b',
            long,
            help = "run the simulator with previous buggy runs for the specific filter"
        )]
        filter: String,
    },
}

impl SimulatorCLI {
    pub fn validate(&mut self) -> Result<(), String> {
        if self.minimum_tests < 1 {
            return Err("minimum size must be at least 1".to_string());
        }
        if self.maximum_tests < 1 {
            return Err("maximum size must be at least 1".to_string());
        }

        if self.minimum_tests > self.maximum_tests {
            tracing::warn!(
                "minimum size '{}' is greater than '{}' maximum size, setting both to '{}'",
                self.minimum_tests,
                self.maximum_tests,
                self.maximum_tests
            );
            self.minimum_tests = self.maximum_tests - 1;
        }

        if self.seed.is_some() && self.load.is_some() {
            return Err("Cannot set seed and load plan at the same time".to_string());
        }

        Ok(())
    }
}
