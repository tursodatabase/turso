all:
	echo Nothing to do...

ctags:
	ctags --options=ctags.rust --languages=Rust src/*.rs src/*/*.rs

docs:
	cargo doc
	in-dir ./target/doc fix-perms
	rscp ./target/doc/* gopher:~/www/burntsushi.net/rustdoc/

install:
	cargo build --manifest-path ./fst-bin/Cargo.toml --release
	cp fst-bin/target/release/fst $(CARGO_INSTALL_ROOT)/bin/
