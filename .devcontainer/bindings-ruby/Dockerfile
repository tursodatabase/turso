FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

# Install libclang
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends libclang-dev\
    && apt-get clean && rm -rf /var/lib/apt/lists/*
