# SQLRight fuzzer for Turso/Limbo
# Build (from repo root):
#   docker build -f testing/sqlright/Dockerfile -t limbo-sqlright .
# Run:    docker run --rm -it limbo-sqlright
#
# The fuzzer output is written to /limbo/testing/sqlright/results/ inside the container.
# To persist results across runs, mount a volume:
#   docker run --rm -it -v sqlright-results:/limbo/testing/sqlright/results limbo-sqlright
#
# Options (passed as CMD arguments):
#   docker run --rm -it limbo-sqlright --cores 2 --oracle TLP
#   docker run --rm -it limbo-sqlright --resume

FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    bison \
    flex \
    libreadline-dev \
    zlib1g-dev \
    curl \
    git \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

# Install cargo-afl and build its runtime
RUN cargo install cargo-afl && cargo afl config --build --force

WORKDIR /limbo

# Copy the full source tree
COPY . .

# Remove bindings workspace members but create stubs for workspace deps
# (testing/stress depends on `turso` which points to bindings/rust)
# Match only indented member lines (start with spaces + quote), not dep lines
RUN VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1) \
    && sed -i '/^[[:space:]]*"bindings\//d' Cargo.toml \
    && mkdir -p bindings/rust/src bindings/javascript/src \
    && printf "[package]\nname = \"turso\"\nversion = \"$VERSION\"\nedition = \"2021\"\n" > bindings/rust/Cargo.toml \
    && printf "[package]\nname = \"turso_node\"\nversion = \"$VERSION\"\nedition = \"2021\"\n" > bindings/javascript/Cargo.toml \
    && touch bindings/rust/src/lib.rs bindings/javascript/src/lib.rs

# Build SQLRight (clone, patch, compile afl-fuzz)
RUN cd testing/sqlright && bash setup.sh

# Build instrumented tursodb
# Rebuild AFL runtime here to ensure it matches the exact rustc version
# -C link-arg=-lc: on aarch64, AFL's afl-compiler-rt.o is appended after -lc
# in the link line, so __stack_chk_guard goes unresolved; this adds -lc again at the end
RUN cargo afl config --build --force \
    && RUSTFLAGS="-C link-arg=-lc" cargo afl build --profile fuzzing --bin tursodb

# ---------------------------------------------------------------------------
# Runtime stage — smaller image without the full Rust toolchain
# ---------------------------------------------------------------------------
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    libreadline8 \
    zlib1g \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /limbo

# Copy built artifacts from builder
COPY --from=builder /limbo/testing/sqlright/build /limbo/testing/sqlright/build
COPY --from=builder /limbo/target/fuzzing/tursodb /limbo/target/fuzzing/tursodb

# Copy the run scripts and patches (for reference)
COPY testing/sqlright/run.sh \
     testing/sqlright/collect_coverage.sh \
     /limbo/testing/sqlright/
COPY testing/sqlright/patches /limbo/testing/sqlright/patches
COPY testing/sqlright/crash_reports /limbo/testing/sqlright/crash_reports

# AFL environment
ENV AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1
ENV AFL_SKIP_CPUFREQ=1
ENV AFL_MAP_SIZE=2097152
ENV AFL_OLD_FORKSERVER=1
ENV AFL_HANG_TMOUT=30000
ENV AFL_SKIP_CRASHES=1

WORKDIR /limbo/testing/sqlright

ENTRYPOINT ["./run.sh"]
CMD []
