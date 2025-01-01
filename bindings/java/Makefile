java_run: lib
	javac HelloWorld.java && java -Djava.library.path=../../target/debug/ HelloWorld

.PHONY: lib

javah:
	javah HelloWorld

lib:
	cargo build
