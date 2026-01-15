#!/bin/bash
set -e

# Run SQLancer against Limbo
# Usage: ./scripts/run-sqlancer.sh [--oracle ORACLE] [--timeout SECONDS] [--seed SEED]
#
# Options:
#   --oracle ORACLE    SQLancer oracle to use (NoREC, PQS, TLP). Default: NoREC
#   --timeout SECONDS  Timeout in seconds. Default: 60
#   --seed SEED        Random seed for reproducibility. If not set, uses random seed.
#   --clean            Remove and re-clone SQLancer directory
#
# This script sets up SQLancer with a dedicated Limbo provider that handles
# Limbo-specific SQL compatibility. The provider is patched into upstream
# SQLancer automatically.
#
# Prerequisites: Java 11+
# Maven will be downloaded automatically if not found

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIMBO_ROOT="$(dirname "$SCRIPT_DIR")"
SQLANCER_REPO="https://github.com/sqlancer/sqlancer.git"
SQLANCER_DIR="/tmp/sqlancer-limbo"
MAVEN_VERSION="3.9.6"
MAVEN_DIR="/tmp/apache-maven-$MAVEN_VERSION"
ORACLE="${ORACLE:-NoREC}"
TIMEOUT="${TIMEOUT:-60}"
SEED=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --oracle) ORACLE="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        --clean) CLEAN=1; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Check Java
if ! command -v java &> /dev/null; then
    echo "Error: Java is required but not found."
    echo "Please install Java 11 or later."
    exit 1
fi

# Setup Maven - download if not available
setup_maven() {
    if command -v mvn &> /dev/null; then
        MVN="mvn"
        return
    fi

    if [[ -x "$MAVEN_DIR/bin/mvn" ]]; then
        MVN="$MAVEN_DIR/bin/mvn"
        return
    fi

    echo "Maven not found, downloading..."
    MAVEN_URL="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"

    cd /tmp
    if command -v curl &> /dev/null; then
        curl -sL "$MAVEN_URL" | tar xz
    elif command -v wget &> /dev/null; then
        wget -qO- "$MAVEN_URL" | tar xz
    else
        echo "Error: curl or wget required to download Maven"
        exit 1
    fi

    MVN="$MAVEN_DIR/bin/mvn"
}

detect_platform() {
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Darwin)
            case "$arch" in
                arm64) echo "macos_arm64" ;;
                x86_64) echo "macos_x86" ;;
                *) echo ""; return 1 ;;
            esac
            ;;
        Linux)
            case "$arch" in
                x86_64) echo "linux_x86" ;;
                *) echo ""; return 1 ;;
            esac
            ;;
        MINGW*|MSYS*|CYGWIN*)
            echo "windows"
            ;;
        *)
            echo ""
            return 1
            ;;
    esac
}

setup_maven

PLATFORM=$(detect_platform)
if [[ -z "$PLATFORM" ]]; then
    echo "Error: Unsupported platform"
    exit 1
fi

echo "=== Setting up SQLancer for Limbo ==="
echo "Platform: $PLATFORM"

# Step 1: Build Limbo JDBC driver if needed
JAVA_BINDINGS_DIR="$LIMBO_ROOT/bindings/java"
LIMBO_VERSION=$(grep "^projectVersion=" "$JAVA_BINDINGS_DIR/gradle.properties" | cut -d= -f2)
LIMBO_JAR="$JAVA_BINDINGS_DIR/build/libs/turso-${LIMBO_VERSION}.jar"
NATIVE_LIB_DIR="$JAVA_BINDINGS_DIR/libs/$PLATFORM"
HASH_FILE="$JAVA_BINDINGS_DIR/.build-hash"

# Compute hash of Rust and Java source to detect changes
compute_source_hash() {
    {
        find "$LIMBO_ROOT/core" "$LIMBO_ROOT/bindings/java/rs_src" -name "*.rs" -type f 2>/dev/null
        find "$LIMBO_ROOT/bindings/java/src" -name "*.java" -type f 2>/dev/null
    } | sort | xargs cat 2>/dev/null | shasum -a 256 | cut -d' ' -f1
}

CURRENT_HASH=$(compute_source_hash)
STORED_HASH=""
if [[ -f "$HASH_FILE" ]]; then
    STORED_HASH=$(cat "$HASH_FILE")
fi

NEED_REBUILD=0
if [[ ! -f "$LIMBO_JAR" ]]; then
    NEED_REBUILD=1
elif [[ ! -d "$NATIVE_LIB_DIR" ]]; then
    NEED_REBUILD=1
elif [[ "$CURRENT_HASH" != "$STORED_HASH" ]]; then
    echo "Rust source changed, rebuilding JDBC driver..."
    NEED_REBUILD=1
fi

if [[ "$NEED_REBUILD" -eq 1 ]]; then
    echo "Building Limbo JDBC driver..."
    cd "$JAVA_BINDINGS_DIR"
    make "$PLATFORM"
    ./gradlew jar
    echo "$CURRENT_HASH" > "$HASH_FILE"
fi

# Step 2: Clone/update SQLancer
if [[ -n "$CLEAN" ]] && [[ -d "$SQLANCER_DIR" ]]; then
    echo "Cleaning SQLancer directory..."
    rm -rf "$SQLANCER_DIR"
fi

if [[ ! -d "$SQLANCER_DIR" ]]; then
    echo "Cloning SQLancer..."
    git clone --depth 1 "$SQLANCER_REPO" "$SQLANCER_DIR"
fi

cd "$SQLANCER_DIR"

# Step 3: Add/update Limbo provider
LIMBO_PROVIDER_DIR="src/sqlancer/limbo"
FIRST_SETUP=0
if [[ ! -d "$LIMBO_PROVIDER_DIR" ]]; then
    FIRST_SETUP=1
    mkdir -p "$LIMBO_PROVIDER_DIR"
fi

# Always copy latest LimboProvider.java
echo "Updating Limbo provider..."
cp "$LIMBO_ROOT/testing/sqlancer/patches/LimboProvider.java" "$LIMBO_PROVIDER_DIR/"

# Patch SQLite3Schema only on first setup
if [[ "$FIRST_SETUP" -eq 1 ]]; then
    # Patch SQLite3Schema to not query sqlite_temp_master (Limbo doesn't support it)
    echo "Patching SQLite3Schema for Limbo compatibility..."
    SCHEMA_FILE="src/sqlancer/sqlite3/schema/SQLite3Schema.java"

    # Apply patch or do manual replacement
    if patch -p1 < "$LIMBO_ROOT/testing/sqlancer/patches/SQLite3Schema.patch" 2>/dev/null; then
        echo "Applied SQLite3Schema patch successfully"
    else
        echo "Patch failed, applying manual fix..."
        # Manual fix: replace the two problematic queries
        # Line 277-278: table/view query
        sed -i.bak 's|"SELECT name, type as category, sql FROM sqlite_master UNION "|"SELECT name, type as category, sql FROM sqlite_master GROUP BY name;")) { // Limbo fix|g' "$SCHEMA_FILE"
        sed -i.bak '/sqlite_temp_master WHERE type=.table/d' "$SCHEMA_FILE"
        # Line 325: index query
        sed -i.bak "s|UNION SELECT name FROM sqlite_temp_master WHERE type='index'||g" "$SCHEMA_FILE"
    fi
fi

# Step 4: Patch pom.xml to use Limbo JAR if not already done
if ! grep -q "turso" pom.xml; then
    echo "Patching pom.xml for Limbo JDBC driver..."
    # Find sqlite-jdbc line and add Limbo dependency after it
    LINE=$(grep -n "sqlite-jdbc" pom.xml | cut -d: -f1)
    if [[ -n "$LINE" ]]; then
        END=$((LINE + 2))
        # Insert Limbo dependency after sqlite-jdbc block
        head -n "$END" pom.xml > pom.xml.new
        cat >> pom.xml.new << EOF
    <dependency>
      <groupId>tech.turso</groupId>
      <artifactId>turso</artifactId>
      <version>0.4.0</version>
      <scope>system</scope>
      <systemPath>$LIMBO_JAR</systemPath>
    </dependency>
EOF
        tail -n "+$((END + 1))" pom.xml >> pom.xml.new
        mv pom.xml.new pom.xml
    fi
fi

# Step 5: Build SQLancer
echo "Building SQLancer..."
"$MVN" package -DskipTests -q

# Step 6: Run SQLancer
echo ""
echo "=== Running SQLancer against Limbo ==="
echo "Oracle: $ORACLE"
echo "Timeout: ${TIMEOUT}s"
if [[ -n "$SEED" ]]; then
    echo "Seed: $SEED"
fi
echo ""

SQLANCER_JAR=$(ls target/sqlancer-*.jar | head -1)

# Build seed argument if provided
SEED_ARGS=""
if [[ -n "$SEED" ]]; then
    SEED_ARGS="--random-seed $SEED"
fi

# Run with both SQLancer and Limbo JARs on classpath
# Disable features Limbo doesn't support yet
java -Djava.library.path="$NATIVE_LIB_DIR" \
    -cp "$SQLANCER_JAR:$LIMBO_JAR" \
    sqlancer.Main \
    --timeout-seconds "$TIMEOUT" \
    --num-threads 1 \
    --print-progress-summary true \
    $SEED_ARGS \
    limbo \
    --oracle "$ORACLE" \
    --test-temp-tables false \
    --test-fts false \
    --test-rtree false \
    --test-check-constraints false \
    --test-nulls-first-last false \
    --test-generated-columns false \
    --test-foreign-keys false

EXIT_CODE=$?

# Print summary from logs
if [[ -d "$SQLANCER_DIR/logs/limbo" ]]; then
    echo ""
    echo "=== SQLancer Log Summary ==="
    for logfile in "$SQLANCER_DIR/logs/limbo"/*-cur.log; do
        if [[ -f "$logfile" ]]; then
            total=$(grep -c "^CREATE\|^INSERT\|^UPDATE\|^DELETE\|^SELECT\|^PRAGMA" "$logfile" 2>/dev/null || echo 0)
            creates=$(grep -c "^CREATE TABLE" "$logfile" 2>/dev/null || echo 0)
            inserts=$(grep -c "^INSERT" "$logfile" 2>/dev/null || echo 0)
            updates=$(grep -c "^UPDATE" "$logfile" 2>/dev/null || echo 0)
            deletes=$(grep -c "^DELETE" "$logfile" 2>/dev/null || echo 0)
            selects=$(grep -c "^SELECT" "$logfile" 2>/dev/null || echo 0)
            indexes=$(grep -c "^CREATE INDEX" "$logfile" 2>/dev/null || echo 0)
            views=$(grep -c "^CREATE VIEW" "$logfile" 2>/dev/null || echo 0)
            failed=$(grep -c " -- 0ms;$" "$logfile" 2>/dev/null || echo 0)

            echo "Log: $(basename "$logfile")"
            echo "  Total statements: $total"
            echo "  CREATE TABLE: $creates"
            echo "  CREATE INDEX: $indexes"
            echo "  CREATE VIEW: $views"
            echo "  INSERT: $inserts"
            echo "  UPDATE: $updates"
            echo "  DELETE: $deletes"
            echo "  SELECT: $selects"
            echo "  Failed (0ms): $failed"
        fi
    done
fi

exit $EXIT_CODE
