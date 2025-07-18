# Turso JDBC Driver

The Turso JDBC driver is a library for accessing and creating Turso database files using Java.

## Project Status

The project is actively developed. Feel free to open issues and contribute.

To view related works, visit this [issue](https://github.com/tursodatabase/turso/issues/615).

## How to use

Currently, we have not published to the maven central. Instead, you can locally build the jar and deploy it to
maven local to use it.

### Build jar and publish to maven local

```shell
$ cd bindings/java

# Please select the appropriate target platform, currently supports `macos_x86`, `macos_arm64`, `windows` and `linux_x86`
$ make macos_x86

# deploy to maven local
$ make publish_local
```

Now you can use the dependency as follows:

```kotlin
dependencies {
    implementation("tech.turso:turso:0.0.1-SNAPSHOT")
}
```

## Code style

- Favor composition over inheritance. For example, `JDBC4Connection` doesn't implement `TursoConnection`. Instead,
  it includes `TursoConnection` as a field. This approach allows us to preserve the characteristics of Turso using
  `TursoConnection` easily while maintaining interoperability with the Java world using `JDBC4Connection`.
