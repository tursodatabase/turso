# Java Examples

## Prerequisites

1. Java 8 or higher
2. Build the native library:
   ```bash
   cd /path/to/limbo
   cargo build --release -p turso-java
   ```

## Encryption

From the `sdks/java` directory:

```bash
cd sdks/java
./gradlew runEncryptionExample
```