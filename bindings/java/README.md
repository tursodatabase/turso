# Limbo JDBC Driver

The Limbo JDBC driver is a library for accessing and creating Limbo database files using Java.

## Project Status

The project is actively developed. Feel free to open issues and contribute.

To view related works, visit this [issue](https://github.com/tursodatabase/limbo/issues/615).

## How to use

### Maven Central (WIP)

The Limbo JDBC driver is available on Maven Central. The library is automatically deployed to Maven Central when a new release is created. You can add it to your project using the following dependency:

#### Maven
```xml
<dependency>
    <groupId>tech.turso</groupId>
    <artifactId>limbo</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

#### Gradle (Kotlin DSL)
```kotlin
dependencies {
    implementation("tech.turso:limbo:0.0.1-SNAPSHOT")
}
```

#### Gradle (Groovy DSL)
```groovy
dependencies {
    implementation 'tech.turso:limbo:0.0.1-SNAPSHOT'
}
```

### Build jar and publish to maven local

If you want to build the library locally, you can use the following commands:

```shell
$ cd bindings/java 

# Please select the appropriate target platform, currently supports `macos_x86`, `macos_arm64`, `windows`
$ make macos_x86

# deploy to maven local 
$ make publish_local
```

## Code style

- Favor composition over inheritance. For example, `JDBC4Connection` doesn't implement `LimboConnection`. Instead,
  it includes `LimboConnection` as a field. This approach allows us to preserve the characteristics of Limbo using
  `LimboConnection` easily while maintaining interoperability with the Java world using `JDBC4Connection`. 
