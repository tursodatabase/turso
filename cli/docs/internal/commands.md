# Cli Internal Docs

## Repl Custom Commands Arg Parser 

To distinguish between normal SQL queries and custom commands, we prefix a "." before our desired command. This signals to the to the REPL that you intend to use a custom command. 

To implement this we use CLAP with multicall. It is very important we use multicall, else this will not work

## Adding new Commands

To add new commands, we need to modify three places: 
- `commands/mod.rs` 
- `commands/args.rs` or create a new file under `commands` that will describe how you will use the Args for your command
- `app.rs` to handle the execution of the command

`commands/mod.rs`
```rust

   pub enum Command {
    ...
    /// Descriptive Message for your command
    Example(ExampleArgs),
   }
```

`commands/args.rs`
```rust 

   #[derive(Debug, Clone, Args)]
    pub struct ExampleArgs {
        /// Example arg
        pub example: String,
    }
```

`app.rs` 
```rust 

    pub fn handle_dot_command(&mut self, line: &str) {
        ...
        Ok(cmd) => match cmd.command {
            ...
            Command::Example(args) => {
                println!("{}", args.example);
            }
        }
    }

```

Every single option that is available to CLAP is available here. To facilitate the creation of more helpful help
messages, please use '///' in your args and command creation, so that CLAP can capture them in the codegen and create their descriptions.

