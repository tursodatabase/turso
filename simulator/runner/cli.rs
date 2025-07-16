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
    #[clap(
        long,
        help = "enable brute force shrink (warning: it might take a long time)"
    )]
    pub enable_brute_force_shrinking: bool,
    #[clap(subcommand)]
    pub subcommand: Option<SimulatorCommand>,
    #[clap(long, help = "disable BugBase", default_value_t = false)]
    pub disable_bugbase: bool,
    #[clap(long, help = "disable heuristic shrinking", default_value_t = false)]
    pub disable_heuristic_shrinking: bool,
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
    #[clap(
        long,
        help = "disable Insert-Values-Select Property",
        default_value_t = false
    )]
    pub disable_insert_values_select: bool,
    #[clap(
        long,
        help = "disable Double-Create-Failure Property",
        default_value_t = false
    )]
    pub disable_double_create_failure: bool,
    #[clap(long, help = "disable Select-Limit Property", default_value_t = false)]
    pub disable_select_limit: bool,
    #[clap(long, help = "disable Delete-Select Property", default_value_t = false)]
    pub disable_delete_select: bool,
    #[clap(long, help = "disable Drop-Select Property", default_value_t = false)]
    pub disable_drop_select: bool,
    #[clap(
        long,
        help = "disable Select-Select-Optimizer Property",
        default_value_t = false
    )]
    pub disable_select_optimizer: bool,
    #[clap(
        long,
        help = "disable Where-True-False-Null Property",
        default_value_t = false
    )]
    pub disable_where_true_false_null: bool,
    #[clap(
        long,
        help = "disable UNION ALL preserves cardinality Property",
        default_value_t = false
    )]
    pub disable_union_all_preserves_cardinality: bool,
    #[clap(long, help = "disable FsyncNoWait Property", default_value_t = true)]
    pub disable_fsync_no_wait: bool,
    #[clap(long, help = "enable FaultyQuery Property", default_value_t = false)]
    pub enable_faulty_query: bool,
    #[clap(long, help = "disable Reopen-Database fault", default_value_t = false)]
    pub disable_reopen_database: bool,
    #[clap(
        long = "latency-prob",
        help = "added IO latency probability",
        default_value_t = 1
    )]
    pub latency_probability: usize,
    #[clap(
        long,
        help = "Minimum tick time in microseconds for simulated time",
        default_value_t = 1
    )]
    pub min_tick: u64,
    #[clap(
        long,
        help = "Maximum tick time in microseconds for simulated time",
        default_value_t = 30
    )]
    pub max_tick: u64,
    #[clap(long, help = "Enable experimental MVCC feature")]
    pub experimental_mvcc: bool,
    #[clap(long, help = "Disable experimental indexing feature")]
    pub disable_experimental_indexes: bool,
    #[clap(
        long,
        help = "Use memory IO for complex simulations",
        default_value_t = false
    )]
    pub memory_io: bool,
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
    pub fn validate(&mut self) -> anyhow::Result<()> {
        if self.minimum_tests < 1 {
            anyhow::bail!("minimum size must be at least 1");
        }
        if self.maximum_tests < 1 {
            anyhow::bail!("maximum size must be at least 1");
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
            anyhow::bail!("Cannot set seed and load plan at the same time");
        }

        if self.latency_probability > 100 {
            anyhow::bail!(
                "latency probability must be a number between 0 and 100. Got `{}`",
                self.latency_probability
            );
        }

        if self.doublecheck && self.differential {
            anyhow::bail!("Cannot run doublecheck and differential testing at the same time");
        }

        Ok(())
    }
}
