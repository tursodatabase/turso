java_run: lib
	javac limbo/HelloWorld.java && java -Djava.library.path=../../target/debug/ limbo.HelloWorld

.PHONY: lib

javah:
	javah limbo.HelloWorld

lib:
	cargo build
