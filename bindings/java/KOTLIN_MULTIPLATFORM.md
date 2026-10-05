# Kotlin Multiplatform and the Turso JDBC driver

This page describes what the Turso Java bindings actually support today, so that you can
decide whether they fit a Kotlin Multiplatform (KMP) project before you start.

**Short version:** `tech.turso:turso` is a plain **JVM** library. It works wherever a JVM
runs, including the `jvm()` target of a KMP project and Android application modules. It
cannot be used from `commonMain`, and there is no Kotlin/Native, JS or Wasm target for it.

## What the artifact is

`tech.turso:turso` is a [JDBC](https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html)
driver that wraps the Turso Rust core through JNI:

- Public API: `tech.turso.TursoDataSource`, `tech.turso.core.TursoConnection`,
  `tech.turso.core.TursoDB`, `tech.turso.core.TursoStatement`, `tech.turso.core.TursoConfig`
  (plus the `java.sql` / JDBC 4 types under `tech.turso.jdbc4`).
- Native code: `bindings/java/rs_src` — Rust, compiled to a shared library that is loaded
  at runtime.
- Published as a **single JVM jar** containing the desktop native libraries. There is no
  Kotlin Multiplatform Gradle plugin, no `commonMain`, and no `expect`/`actual` sources.

Because the Gradle module is a plain `java-library` (`bindings/java/build.gradle.kts`) with
`sourceCompatibility`/`targetCompatibility` set to Java 8, its output is ordinary JVM
bytecode. That is what makes it usable from KMP's `jvm()` target — see below.

## Compatibility matrix

The supported targets come from `Architecture.detect()` in
`bindings/java/src/main/java/tech/turso/core/TursoDB.java`, which maps the running OS and
architecture onto one of the native libraries packaged in the jar.

| Target | Status | Notes |
| --- | --- | --- |
| macOS `arm64` (Apple Silicon) | Supported | `MACOS_ARM64` |
| macOS `x86_64` | Supported | `MACOS_X86` |
| Linux `x86_64` / `amd64` | Supported | `LINUX_X86` |
| Windows (`x86_64`) | Supported | `WINDOWS` |
| Linux `arm64` / `aarch64` | **Not supported** | `detect()` throws `UnsupportedOperationException("ARM64 architecture is not supported on Linux yet")` |
| Android | **Not supported** | See [Android](#android) below |
| iOS (Kotlin/Native) | **Not supported** | No Kotlin/Native binding exists |
| Desktop via Kotlin/Native (Compose Desktop native) | **Not supported** | No Kotlin/Native binding exists |
| JS / Wasm | **Not supported** | No JS or Wasm target exists |

Any OS/architecture combination that is not in the table above resolves to
`Architecture.UNSUPPORTED`, whose library path is empty, and the driver throws
`InternalError: Unable to load necessary native library`.

### Android

`TursoDB`'s static initializer checks `System.getProperty("java.vm.vendor") == "The Android
Project"` and contains a `// TODO` for that branch, so an Android runtime follows the same
desktop code path. The jar's native libraries are desktop artifacts
(`lib_turso_java.dylib`, `.so`, `.dll`); none of them are built for an Android ABI
(`arm64-v8a`, `armeabi-v7a`, `x86_64`). The Makefile in `bindings/java` builds only
`macos_x86`, `macos_arm64`, `windows` and `linux_x86`.

So while the driver compiles against Android's classpath, it will not load on a device. Do
not plan on Android as a supported target until native libraries for Android ABIs are
published.

## Using it from a KMP project

A KMP project can depend on the driver from its **`jvm()` target** (and from an Android
application module, which also produces a JVM classpath), but **not from `commonMain`** —
`commonMain` has no access to `java.sql` or to JNI, and the artifact has no `commonMain`
metadata.

### Keep the database interface in `commonMain`

The recommended shape is to declare the database-facing interface once, in `commonMain`,
and let each target provide the implementation:

```kotlin
// commonMain - no java.sql, no JNI
interface Database {
    suspend fun <T> query(sql: String, params: List<Any?>, mapper: (Row) -> T): List<T>
    suspend fun execute(sql: String, params: List<Any?>): Int
}

/** Platform-neutral view of one result row. */
interface Row {
    fun getString(column: String): String?
    fun getInt(column: String): Int
}
```

Then implement it in the `jvm()` source set with the driver:

```kotlin
// jvmMain
class JvmDatabase(private val path: String) : Database {

    override suspend fun <T> query(
        sql: String,
        params: List<Any?>,
        mapper: (Row) -> T
    ): List<T> = withContext(Dispatchers.IO) {
        DriverManager.getConnection("jdbc:turso:$path").use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                params.forEachIndexed { index, value -> stmt.setObject(index + 1, value) }
                stmt.executeQuery().use { rs ->
                    buildList { while (rs.next()) add(mapper(JdbcRow(rs))) }
                }
            }
        }
    }

    override suspend fun execute(sql: String, params: List<Any?>): Int =
        withContext(Dispatchers.IO) {
            DriverManager.getConnection("jdbc:turso:$path").use { connection ->
                connection.prepareStatement(sql).use { stmt ->
                    params.forEachIndexed { index, value -> stmt.setObject(index + 1, value) }
                    stmt.executeUpdate()
                }
            }
        }
}

/** Wraps a ResultSet in the common Row interface. */
private class JdbcRow(private val rs: ResultSet) : Row {
    override fun getString(column: String): String? = rs.getString(column)
    override fun getInt(column: String): Int = rs.getInt(column)
}
```

This uses `DriverManager`, which is the form the driver's own tests use
(`bindings/java/src/test/java/tech/turso/JDBCTest.java`). If you need a pooled or
externally-configured data source, construct one instead:
`TursoDataSource(TursoConfig(Properties()), "jdbc:turso:$path")`. Note that
`TursoDataSource` takes `TursoConfig` and the URL as constructor arguments — there is no
`setUrl` setter — and `TursoConfig`'s only constructor takes a `Properties`.

Wrap writes in a transaction yourself; the driver does not manage that for you:

```kotlin
connection.autoCommit = false
try {
    // ... executeUpdate ...
    connection.commit()
} catch (e: Throwable) {
    connection.rollback()
    throw e
} finally {
    connection.autoCommit = previous
}
```

Add the dependency only where the JVM is available:

```kotlin
kotlin {
    jvm()

    sourceSets {
        commonMain.dependencies {
            // interfaces, models, coroutines - anything JVM-agnostic
        }
        jvmMain.dependencies {
            implementation("tech.turso:turso:<version>")
        }
    }
}
```

For targets with no supported binding (iOS, Kotlin/Native, JS/Wasm), either provide a
`Storage`-style no-op/failing implementation behind `expect`/`actual`, or keep those targets
out of the database feature until a binding exists. Do not put the JDBC dependency in
`commonMain` — it will fail to resolve for the non-JVM targets and the Kotlin/Native / JS
compilation will break.

### Dependency injection per target

Because the interface lives in `commonMain`, any KMP-compatible DI approach works — a simple
`commonMain` module object, or constructor injection wired up in each target's entry point
(`Activity` on Android, `main()` on desktop). The only target-specific part is *which
implementation* you pass in; that decision belongs next to the target's entry point, not in
`commonMain`.

## Build and publishing

### Maven Central

`tech.turso:turso` is **not published to Maven Central**. The README's statement that it has
not been published is still accurate, so a plain

```kotlin
repositories { mavenCentral() }
```

will not resolve it.

Until that changes, build it locally and consume it from `mavenLocal()`:

```bash
cd bindings/java

# pick the native target(s) you need: macos_x86 | macos_arm64 | windows | linux_x86
make linux_x86
make publish_local
```

Then resolve it from the local Maven cache:

```kotlin
repositories {
    mavenLocal()
    mavenCentral()   // for your other dependencies
}

dependencies {
    implementation("tech.turso:turso:<version>")
}
```

`mavenLocal()` only sees libraries you have built yourself. That is fine for local
development, but it means a clean machine — CI, a teammate's checkout, or a fresh Docker
image — has nothing to resolve, so a local build step must precede every build on that
machine.

### Versioning

The current version lives in `bindings/java/gradle.properties` as `projectVersion` (at the
time of writing, `0.8.0`), together with `projectGroup=tech.turso` and
`projectArtifactId=turso`. Always read the version from `gradle.properties` rather than
copying a coordinate from an older page or example, because those have drifted: the
`bindings/java/README.md` snippet and the example project both still show
`0.0.1-SNAPSHOT`.

There is no separate stable/preview channel published, so "stable vs preview" is not
something you can select with a repository or version suffix today. Use the version from
`gradle.properties`.

### How publication works when it happens

Publication is configured but manual:

- `bindings/java/build.gradle.kts` applies the `maven-publish` and `signing` plugins; the
  publication is defined in `bindings/java/gradle/publish.gradle.kts`.
- `publish.gradle.kts` sets GPG signing as **required** and reads the key and passphrase
  from the `MAVEN_SIGNING_KEY` and `MAVEN_SIGNING_PASSPHRASE` environment variables.
- `.github/workflows/java-publish.yml` runs `./gradlew clean publishToMavenCentral` and is
  triggered only by `workflow_dispatch` — it does not run on tag or push, so a release needs
  a maintainer to start it.

If you are downstream of this driver, treat the version in `gradle.properties` plus a local
`mavenLocal()` build as the supported way to consume it for now.

## What this page deliberately does not cover

The following are requested but are not implemented in the Java bindings, so this page does
not give guidance for them:

- **Embedded replicas and sync.** There is no sync or replica API in
  `bindings/java/src/main/java`. Sync and embedded replicas are features of other Turso
  bindings and of the Turso CLI, not of the JDBC driver.
- **Android native support.** No Android ABI libraries are built; see [Android](#android).
- **A full end-to-end KMP sample.** The snippets above are the minimum needed to wire a
  `jvm()` target to the driver. A sample covering CRUD, migrations, sync and conflict
  handling would need a Kotlin/Native binding for the non-JVM targets before it could be
  complete.

## Getting help

If a target you need is missing, or a platform you care about is mis-marked above, open an
issue and describe the target and the failure you hit. `bindings/java/README.md` is the
starting point for building and testing the driver locally.
