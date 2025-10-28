ifeq ($(OS),Windows_NT)
	OS_TARGET = windows64
else 
    UNAME_S := $(shell uname -s)
	ifeq ($(UNAME_S),Linux)
		OS_TARGET = linux64
	endif
	ifeq ($(UNAME_S),Darwin)
		UNAME_P := $(shell uname -p)
		ifeq ($(UNAME_P),x86_64)
			OS_TARGET = macos64
		else
			OS_TARGET = macosarm64
		endif
	endif
endif

RELEASE_OPT ?= 

all:
	echo "make [build|test|benchmark|build-production]"

build-rust-windows64:
	cargo build --target-dir ./rs_compiled --target x86_64-pc-windows-msvc $(RELEASE_OPT)

build-rust-linux64:
	cargo build --target-dir ./rs_compiled --target x86_64-unknown-linux-gnu $(RELEASE_OPT)

build-rust-macos64:
	cargo build --target-dir ./rs_compiled --target x86_64-apple-darwin $(RELEASE_OPT)

build-rust-macosarm64:
	cargo build --target-dir ./rs_compiled --target aarch64-apple-darwin $(RELEASE_OPT)

build-rust: 
	echo "Building for $(OS_TARGET)"
	$(MAKE) build-rust-$(OS_TARGET)

build-dotnet:
	dotnet build ./src/Turso/Turso.csproj

build: build-rust build-dotnet

build-production:
	$(MAKE) build RELEASE_OPT=--release

test: build
	dotnet test ./src/Turso.Tests/Turso.Tests.csproj

benchmark: build-production
	dotnet run -c Release --project ./src/Benchmarks/Benchmarks.csproj

publish:
	dotnet pack --configuration Release ./src/Turso/Turso.csproj
	dotnet pack --configuration Release ./src/Turso.Raw/Turso.Raw.csproj
