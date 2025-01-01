java_run: lib
	javac limbo/Limbo.java && java -Djava.library.path=../../target/debug/ limbo.Limbo

.PHONY: lib

javah:
	javah limbo.HelloWorld

lib:
	cargo build
