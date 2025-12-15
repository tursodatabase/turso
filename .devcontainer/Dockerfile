FROM node:20

ARG TZ
ARG CLAUDE_CODE_VERSION
ARG GIT_DELTA_VERSION
ARG ZSH_IN_DOCKER_VERSION
ARG GO_VERSION
ARG RUST_VERSION
ARG PYTHON_VERSION

ENV TZ="$TZ"

# Install basic development tools and iptables/ipset
RUN apt-get update && apt-get install -y --no-install-recommends \
  sqlite3 \
  less \
  git \
  procps \
  sudo \
  fzf \
  zsh \
  man-db \
  unzip \
  gnupg2 \
  gh \
  iptables \
  ipset \
  iproute2 \
  dnsutils \
  aggregate \
  jq \
  nano \
  vim \
  ca-certificates \
  build-essential \
  pkg-config \
  curl \
  python${PYTHON_VERSION}-dev \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

# Ensure default node user has access to /usr/local/share
RUN mkdir -p /usr/local/share/npm-global && \
  chown -R node:node /usr/local/share

ARG USERNAME=node

# Persist bash history.
RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
  && mkdir /commandhistory \
  && touch /commandhistory/.bash_history \
  && chown -R $USERNAME /commandhistory

# Set `DEVCONTAINER` environment variable to help with orientation
ENV DEVCONTAINER=true

# Create workspace and config directories and set permissions
RUN mkdir -p /workspace /home/node/.claude && \
  chown -R node:node /workspace /home/node/.claude

WORKDIR /workspace

RUN ARCH=$(dpkg --print-architecture) && \
  wget "https://github.com/dandavison/delta/releases/download/${GIT_DELTA_VERSION}/git-delta_${GIT_DELTA_VERSION}_${ARCH}.deb" && \
  sudo dpkg -i "git-delta_${GIT_DELTA_VERSION}_${ARCH}.deb" && \
  rm "git-delta_${GIT_DELTA_VERSION}_${ARCH}.deb"

# Install Go

ENV GOROOT=/usr/local/go
ENV GOPATH=/home/node/go
ENV PATH=$PATH:/usr/local/go/bin:/home/node/go/bin

RUN set -eux; \
    OS="$(uname -s)"; \
    ARCH="$(uname -m)"; \
    \
    case "$OS" in \
      Linux) \
        GOOS=linux ;; \
      Darwin) \
        GOOS=darwin ;; \
      *) \
        echo "Unsupported OS: $OS" && exit 1 ;; \
    esac; \
    \
    case "$ARCH" in \
      x86_64|amd64) \
        GOARCH=amd64 ;; \
      aarch64|arm64) \
        GOARCH=arm64 ;; \
      *) \
        echo "Unsupported arch: $ARCH" && exit 1 ;; \
    esac; \
    \
    curl -fsSL "https://go.dev/dl/go${GO_VERSION}.${GOOS}-${GOARCH}.tar.gz" \
      | sudo tar -C /usr/local -xz

# Set up non-root user
USER node

# Install Rust
ENV RUSTUP_HOME=/home/node/.rustup \
    CARGO_HOME=/home/node/.cargo
ENV PATH=$PATH:/home/node/.cargo/bin

RUN curl https://sh.rustup.rs -sSf | sh -s -- \
    -y \
    --default-toolchain ${RUST_VERSION} \
    --profile minimal \
 && rustup component add rustfmt clippy

# Install Python

ENV UV_HOME=/home/node/.uv
ENV PATH=$PATH:/home/node/.cargo/bin:/home/node/.local/bin

RUN curl -Ls https://astral.sh/uv/install.sh | sh
RUN uv python install ${PYTHON_VERSION}

# Check installation

RUN rustc --version \
 && cargo --version \
 && go version \
 && uv --version


# Install global packages
ENV NPM_CONFIG_PREFIX=/usr/local/share/npm-global
ENV PATH=$PATH:/usr/local/share/npm-global/bin

# Set the default shell to zsh rather than sh
ENV SHELL=/bin/zsh

# Set the default editor and visual
ENV EDITOR=vim
ENV VISUAL=vim

# Default powerline10k theme
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v${ZSH_IN_DOCKER_VERSION}/zsh-in-docker.sh)" -- \
  -p git \
  -p fzf \
  -a "source /usr/share/doc/fzf/examples/key-bindings.zsh" \
  -a "source /usr/share/doc/fzf/examples/completion.zsh" \
  -a "export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
  -x

# Install Claude
RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}

# Copy and set up firewall script
COPY init-firewall.sh /usr/local/bin/
USER root
RUN chmod +x /usr/local/bin/init-firewall.sh && \
  echo "node ALL=(root) NOPASSWD: /usr/local/bin/init-firewall.sh" > /etc/sudoers.d/node-firewall && \
  chmod 0440 /etc/sudoers.d/node-firewall
USER node
