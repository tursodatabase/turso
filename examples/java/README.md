# Java Examples

## Prerequisites

1. Java 8 or higher
2. Build the native library:
   ```bash
   cd /path/to/limbo
   cargo build --release -p turso-java
   ```

## Encryption

From the `bindings/java` directory:

```bash
cd bindings/java
./gradlew runEncryptionExample
```