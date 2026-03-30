# Turso Whopper - Concurrent Simulator

Deterministic concurrent simulator for Turso.

## Running Elle Locally (macOS)

### One-time setup

```bash
brew install leiningen openjdk graphviz

sudo ln -sfn $(brew --prefix openjdk)/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk

# Build elle-cli
git clone --depth 1 https://github.com/ligurio/elle-cli.git /tmp/elle-cli
cd /tmp/elle-cli && lein uberjar
```

### Running sim and elle analysis

Get the seed from CI logs and run the sim:

```shell
cargo build -p turso_whopper
SEED=14201626211019268779 ./target/debug/turso_whopper \
    --elle list-append \
    --elle-output elle-history.edn \
    --max-steps 100000 \
    --enable-mvcc
```

and then elle:

```shell
java -jar /tmp/elle-cli/target/elle-cli-0.1.9-standalone.jar \
    --model list-append \
    --consistency-models snapshot-isolation \
    --verbose \
    --directory elle-results \
    elle-history.edn
```