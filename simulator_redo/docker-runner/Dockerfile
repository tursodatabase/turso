FROM lukemathwalker/cargo-chef:latest-rust-1.88.0 AS chef
RUN apt update \
    && apt install -y git libssl-dev pkg-config\
    && apt clean \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

#
# Cache dependencies
#

FROM chef AS planner
# cargo-chef requires all this crap to be present in the workspace
COPY Cargo.toml Cargo.lock ./
COPY core ./core/
COPY simulator ./simulator/
COPY simulator_redo ./simulator_redo/
COPY bindings ./bindings/
COPY extensions ./extensions/
COPY macros ./macros/
COPY sync ./sync
COPY parser ./parser/
COPY perf/encryption ./perf/encryption
COPY perf/throughput/rusqlite ./perf/throughput/rusqlite
COPY perf/throughput/turso ./perf/throughput/turso
COPY cli ./cli/
COPY sqlite3 ./sqlite3/
COPY stress ./stress/
COPY tests ./tests/
COPY packages ./packages/
COPY testing/sqlite_test_ext ./testing/sqlite_test_ext
COPY sql_generation ./sql_generation/
COPY sql_gen_prop ./sql_gen_prop/
COPY whopper ./whopper
COPY sdk-kit ./sdk-kit/
COPY sdk-kit-macros ./sdk-kit-macros/
RUN cargo chef prepare --bin sim_redo --recipe-path recipe.json

#
# Build the project.
#

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --bin sim_redo --recipe-path recipe.json
COPY --from=planner /app/core ./core/
COPY --from=planner /app/extensions ./extensions/
COPY --from=planner /app/macros ./macros/
COPY --from=planner /app/parser ./parser/
COPY --from=planner /app/perf/encryption ./perf/encryption
COPY --from=planner /app/perf/throughput/rusqlite ./perf/throughput/rusqlite
COPY --from=planner /app/perf/throughput/turso ./perf/throughput/turso
COPY --from=planner /app/simulator ./simulator/
COPY --from=planner /app/simulator_redo ./simulator_redo/
COPY --from=planner /app/packages ./packages/
COPY --from=planner /app/sql_generation ./sql_generation/
COPY --from=planner /app/sql_gen_prop ./sql_gen_prop/
COPY --from=planner /app/Cargo.toml /app/Cargo.lock ./

RUN cargo build --bin sim_redo

#
# The final image.
#

FROM debian:bookworm-slim AS runtime
# Install Bun (we want to use a fancy shell so we can e.g. post github issues when simulator fails)
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    ca-certificates \
    libssl3 \
    openssl \
    && curl -fsSL https://bun.sh/install | bash \
    && ln -s /root/.bun/bin/bun /usr/local/bin/bun \
    && rm -rf /var/lib/apt/lists/*

# Accept git hash as build argument
ARG GIT_HASH
ENV GIT_HASH=${GIT_HASH:-unknown}

WORKDIR /app
COPY --from=builder /app/target/debug/sim_redo /app/sim_redo
COPY simulator_redo/docker-runner/package.json simulator_redo/docker-runner/tsconfig.json ./
RUN bun install
COPY simulator_redo/docker-runner/*.ts ./

RUN chmod +x /app/docker-entrypoint.simulator.ts
RUN chmod +x /app/sim_redo

ENTRYPOINT ["bun", "/app/docker-entrypoint.simulator.ts"]
# Arguments can be passed at runtime
CMD []
