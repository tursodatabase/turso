# Limbo Fuzzing 

## Prerequisites

Ensure you have the following installed:

- Nightly Rust toolchain (required for `cargo-fuzz` unless using Nix)
- `cargo-fuzz` (install it using `cargo install cargo-fuzz`)
- Nix (if using a `flake.nix` setup)

## Using Nix

```sh
nix develop .#fuzz
```

This will set up the required environment with the nightly toolchain and
dependencies.

## Running the Fuzzer

If using Nix:

```sh
cargo fuzz run <fuzz_target>
```

If using `rustup` without Nix:

```sh
cargo +nightly fuzz run <fuzz_target>
```

This will compile the fuzz target and start fuzzing with `libFuzzer`.

## Example

Run the expression target with:

```sh
cargo fuzz run expression
```

