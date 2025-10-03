all:
	echo "make [build|test|build-production]"

build:
	cargo build --target-dir ./rs_compiled/
	dotnet build ./src/Turso/Turso.csproj

build-production:
	cargo build --release --target-dir ./rs_compiled/
	dotnet build -c Release ./src/Turso/Turso.csproj

test: build
	dotnet test ./src/Turso.Tests/Turso.Tests.csproj