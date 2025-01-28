{
  inputs = {
    nixpkgs = {
      url = "github:nixos/nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    nixpkgs,
    fenix,
    ...
  }: let
    systems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
    forAllSystems = nixpkgs.lib.genAttrs systems;
  in {
    devShells = forAllSystems (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};
        rust-stable = fenix.packages.${system}.stable;
        rust-toolchain = rust-stable.toolchain;
        rustfmt = rust-stable.rustfmt;
        rust-src = rust-stable.rust-src;
        rust-analyzer = rust-stable.rust-analyzer;
        wasmTarget = fenix.packages.${system}.targets.wasm32-wasi.latest.rust-std;
        extraDarwinInputs =
          if pkgs.stdenv.isDarwin
          then [pkgs.darwin.apple_sdk.frameworks.CoreFoundation]
          else [];
      in {
        default = with pkgs;
          mkShell {
            buildInputs =
              [
                clang
                libiconv
                sqlite
                gnumake
                rustup # not used to install the toolchain, but the makefile uses it
                rust-toolchain
                rust-src
                rust-analyzer
                rustfmt
                wasmTarget
                tcl
                python3
              ]
              ++ extraDarwinInputs;
          };
      }
    );
  };
}
