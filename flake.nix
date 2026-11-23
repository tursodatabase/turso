{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, flake-utils, rust-overlay, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        toolchain = (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
          extensions = [ "rust-analyzer" "rust-src" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        lib = pkgs.lib;

        # Custom SQLite package with debug enabled
        sqlite-debug = pkgs.sqlite.overrideAttrs (oldAttrs: rec {
          name = "sqlite-debug-${oldAttrs.version}";
          configureFlags = oldAttrs.configureFlags ++ [ "--enable-debug" ];
          dontStrip = true;
          separateDebugInfo = true;
        });

        cargoArtifacts = craneLib.buildDepsOnly {
          src = ./.;
          pname = "turso";
          nativeBuildInputs = with pkgs; [ python3 ];
          # .cargo/config.toml sets scripts/link-with-rust-lld.sh as the
          # linker, but the dummy source of buildDepsOnly has no scripts/
          # directory. In a Nix build, that script runs cc anyway.
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "cc";
        };

        commonArgs = {
          inherit cargoArtifacts;
          pname = "turso";
          src = ./.;
          nativeBuildInputs = with pkgs; [ python3 ];
          strictDeps = true;
          # The same linker as cargoArtifacts, so that Cargo reuses its build.
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "cc";
        };

        craneLib = ((crane.mkLib pkgs).overrideToolchain toolchain);
      in
      rec {
        formatter = pkgs.nixpkgs-fmt;
        checks = {
          doc = craneLib.cargoDoc commonArgs;
          fmt = craneLib.cargoFmt commonArgs;
          clippy = craneLib.cargoClippy (commonArgs // {
            # TODO: maybe add `-- --deny warnings`
            cargoClippyExtraArgs = "--all-targets";
          });
        };
        packages.turso_cli = craneLib.buildPackage (commonArgs // {
          cargoExtraArgs = "--package turso_cli";
        });
        packages.default = packages.turso_cli;
        devShells.default = with pkgs; mkShell {
          nativeBuildInputs = [
            clang
            sqlite-debug  # Use debug-enabled SQLite
            gnumake
            tcl
            python3
            nodejs
            toolchain
            uv
            rustPlatform.bindgenHook
          ] ++ lib.optionals pkgs.stdenv.isDarwin [
            apple-sdk
          ];
        };
        devShells.fuzz = with pkgs; mkShell {
          nativeBuildInputs = [
            (pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.minimal))
          ] ++ lib.optionals pkgs.stdenv.isDarwin [
            apple-sdk
          ];
        };
      }
    );
}
