# Limbo Simulator

Limbo simulator uses randomized deterministic simulations to test the Limbo database behaviors.

Each simulation begins with a random configurations:

- the database workload distribution(percentages of reads, writes, deletes...),
- database parameters(page size),
- number of reader or writers, etc.

Based on these parameters, we randomly generate **interaction plans**. Interaction plans consist of statements/queries, and assertions that will be executed in order. The building blocks of interaction plans are:

- Randomly generated SQL queries satisfying the workload distribution,
- Properties, which contain multiple matching queries with assertions indicating the expected result.

An example of a property is the following:

```sql
-- begin testing 'Select-Select-Optimizer'
-- ASSUME table marvelous_ideal exists;
SELECT ((devoted_ahmed = -9142609771.541502 AND loving_wicker = -1246708244.164486)) FROM marvelous_ideal WHERE TRUE;
SELECT * FROM marvelous_ideal WHERE (devoted_ahmed = -9142609771.541502 AND loving_wicker = -1246708244.164486);
-- ASSERT select queries should return the same amount of results;
-- end testing 'Select-Select-Optimizer'
```

The simulator starts from an initially empty database, adding random interactions based on the workload distribution. It can
add random queries unrelated to the properties without breaking the property invariants to reach more diverse states and respect the configured workload distribution.

The simulator executes the interaction plans in a loop, and checks the assertions. It can add random queries unrelated to the properties without
breaking the property invariants to reach more diverse states and respect the configured workload distribution.

The simulator code is broken into 4 main parts:

- **Simulator(main.rs)**: The main entry point of the simulator. It generates random configurations and interaction plans, and executes them.
- **Model(model.rs, model/table.rs, model/query.rs)**: A simpler model of the database, it contains atomic actions for insertion and selection, we use this model while deciding the next actions.
- **Generation(generation.rs, generation/table.rs, generation/query.rs, generation/plan.rs)**: Random generation functions for the database model and interaction plans.
- **Properties(properties.rs)**: Contains the properties that we want to test.

## Running the simulator

To run the simulator, you can use the following command:

```bash
RUST_LOG=limbo_sim=debug cargo run --bin limbo_sim
```

The simulator CLI has a few configuration options that you can explore via `--help` flag.

```txt
The Limbo deterministic simulator

Usage: limbo_sim [OPTIONS]

Options:
  -s, --seed <SEED>                  set seed for reproducible runs
  -d, --doublecheck                  enable doublechecking, run the simulator with the plan twice and check output equality
  -n, --maximum-size <MAXIMUM_SIZE>  change the maximum size of the randomly generated sequence of interactions [default: 5000]
  -k, --minimum-size <MINIMUM_SIZE>  change the minimum size of the randomly generated sequence of interactions [default: 1000]
  -t, --maximum-time <MAXIMUM_TIME>  change the maximum time of the simulation(in seconds) [default: 3600]
  -l, --load <LOAD>                  load plan from the bug base
  -w, --watch                        enable watch mode that reruns the simulation on file changes
      --differential                 run differential testing between sqlite and Limbo
  -h, --help                         Print help
  -V, --version                      Print version
```

## Adding new properties

The properties are defined in `simulator/generation/property.rs` in the `Property` enum. Each property is documented with
inline doc comments, an example is given below:

```rust
/// Insert-Select is a property in which the inserted row
/// must be in the resulting rows of a select query that has a
/// where clause that matches the inserted row.
/// The execution of the property is as follows
///     INSERT INTO <t> VALUES (...)
///     I_0
///     I_1
///     ...
///     I_n
///     SELECT * FROM <t> WHERE <predicate>
/// The interactions in the middle has the following constraints;
/// - There will be no errors in the middle interactions.
/// - The inserted row will not be deleted.
/// - The inserted row will not be updated.
/// - The table `t` will not be renamed, dropped, or altered.
InsertValuesSelect {
    /// The insert query
    insert: Insert,
    /// Selected row index
    row_index: usize,
    /// Additional interactions in the middle of the property
    queries: Vec<Query>,
    /// The select query
    select: Select,
},
```

If you would like to add a new property, you can add a new variant to the `Property` enum, and the corresponding
generation function in `simulator/generation/property.rs`. The generation function should return a `Property` instance, and
it should generate the necessary queries and assertions for the property.

## Automatic Compatibility Testing with SQLite

You can use the `--differential` flag to run the simulator in differential testing mode. This mode will run the same interaction plan on both Limbo and SQLite, and compare the results. It will also check for any panics or errors in either database.

## Resources

- [(reading) TigerBeetle Deterministic Simulation Testing](https://docs.tigerbeetle.com/about/vopr/)
- [(reading) sled simulation guide (jepsen-proof engineering)](https://sled.rs/simulation.html)
- [(video) "Testing Distributed Systems w/ Deterministic Simulation" by Will Wilson](https://www.youtube.com/watch?v=4fFDFbi3toc)
- [(video) FF meetup #4 - Deterministic simulation testing](https://www.youtube.com/live/29Vz5wkoUR8)
- [(code) Hiisi: a proof of concept libSQL written in Rust following TigerBeetle-style with deterministic simulation testing](https://github.com/penberg/hiisi)
