FROM rust:1-bookworm

RUN apt-get update \
    && apt-get install -y valgrind pkg-config libssl-dev rsync \
    && rm -rf /var/lib/apt/lists/*
