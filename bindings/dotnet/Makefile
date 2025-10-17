all:
	echo "make [build|test|benchmark|build-production]"

build:
	cargo build --target-dir ./rs_compiled/
	dotnet build ./src/Turso/Turso.csproj

build-production:
	cargo build --release --target-dir ./rs_compiled/
	dotnet build -c Release ./src/Turso/Turso.csproj

test: build
	dotnet test ./src/Turso.Tests/Turso.Tests.csproj

benchmark: build-production
	dotnet run -c Release --project ./src/Benchmarks/Benchmarks.csproj