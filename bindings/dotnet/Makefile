all:
	echo "make [build|test|benchmark|build-production]"

build-rust:
	cargo build --target-dir ./rs_compiled/

build-dotnet:
	dotnet build ./src/Turso/Turso.csproj

build-rust-production:
	cargo build --release --target-dir ./rs_compiled/

build-dotnet-production:
	dotnet build -c Release ./src/Turso/Turso.csproj


build: build-rust build-dotnet

build-production: build-rust-production build-dotnet-production

test: build
	dotnet test ./src/Turso.Tests/Turso.Tests.csproj

benchmark: build-production
	dotnet run -c Release --project ./src/Benchmarks/Benchmarks.csproj

publish:
	dotnet pack --configuration Release ./src/Turso/Turso.csproj
	dotnet pack --configuration Release ./src/Turso.Raw/Turso.Raw.csproj
