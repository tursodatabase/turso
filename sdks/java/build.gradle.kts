import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    java
    application
    `java-library`
    `maven-publish`
    signing
    id("net.ltgt.errorprone") version "3.1.0"

    // If you're stuck on JRE 8, use id 'com.diffplug.spotless' version '6.13.0' or older.
    id("com.diffplug.spotless") version "6.13.0"
}

// Apply publishing configuration
apply(from = "gradle/publish.gradle.kts")

// Helper function to read properties with defaults
fun prop(key: String, default: String? = null): String? =
    findProperty(key)?.toString() ?: default

group = prop("projectGroup") ?: error("projectGroup must be set in gradle.properties")
version = prop("projectVersion") ?: error("projectVersion must be set in gradle.properties")

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withJavadocJar()
    withSourcesJar()
}

// TODO: Add javadoc to required class and methods. After that, let's remove this settings
tasks.withType<Javadoc> {
    options {
        (this as StandardJavadocDocletOptions).apply {
            addStringOption("Xdoclint:none", "-quiet")
        }
    }
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.slf4j:slf4j-api:1.7.32")

    errorprone("com.uber.nullaway:nullaway:0.10.26") // maximum version which supports java 8
    errorprone("com.google.errorprone:error_prone_core:2.10.0") // maximum version which supports java 8

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.0")
}

application {
    val tursoSystemLibraryPath = System.getenv("TURSO_LIBRARY_PATH")
    if (tursoSystemLibraryPath != null) {
        applicationDefaultJvmArgs = listOf(
            "-Djava.library.path=${System.getProperty("java.library.path")}:$tursoSystemLibraryPath"
        )
    }
}

tasks.jar {
    from("libs") {
        into("libs")
    }
}

sourceSets {
    test {
        resources {
            file("src/main/resource/turso-jdbc.properties")
        }
    }
}

tasks.test {
    useJUnitPlatform()
    // In order to find rust built file under resources, we need to set it as system path
    systemProperty(
        "java.library.path",
        "${System.getProperty("java.library.path")}:$projectDir/src/test/resources/turso/debug"
    )

    // For our fancy test logging
    testLogging {
        // set options for log level LIFECYCLE
        events(
            TestLogEvent.FAILED,
            TestLogEvent.PASSED,
            TestLogEvent.SKIPPED,
            TestLogEvent.STANDARD_OUT
        )
        exceptionFormat = TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true

        // set options for log level DEBUG and INFO
        debug {
            events(
                TestLogEvent.STARTED,
                TestLogEvent.FAILED,
                TestLogEvent.PASSED,
                TestLogEvent.SKIPPED,
                TestLogEvent.STANDARD_ERROR,
                TestLogEvent.STANDARD_OUT
            )
            exceptionFormat = TestExceptionFormat.FULL
        }
        info.events = debug.events
        info.exceptionFormat = debug.exceptionFormat

        afterSuite(KotlinClosure2<TestDescriptor, TestResult, Unit>({ desc, result ->
            if (desc.parent == null) { // will match the outermost suite
                val output = "Results: ${result.resultType} (${result.testCount} tests, ${result.successfulTestCount} passed, ${result.failedTestCount} failed, ${result.skippedTestCount} skipped)"
                val startItem = "|  "
                val endItem = "  |"
                val repeatLength = startItem.length + output.length + endItem.length
                println("\n" + "-".repeat(repeatLength) + "\n" + startItem + output + endItem + "\n" + "-".repeat(repeatLength))
            }
        }))
    }
}

tasks.withType<JavaCompile> {
    options.errorprone {
        // Let's select which checks to perform. NullAway is enough for now.
        disableAllChecks = true
        check("NullAway", CheckSeverity.ERROR)

        option("NullAway:AnnotatedPackages", "tech.turso")
        option(
            "NullAway:CustomNullableAnnotations",
            "tech.turso.annotations.Nullable,tech.turso.annotations.SkipNullableCheck"
        )
    }
    if (name.lowercase().contains("test")) {
        options.errorprone {
            disable("NullAway")
        }
    }
}

spotless {
    java {
        target("**/*.java")
        targetExclude(layout.buildDirectory.dir("**/*.java").get().asFile)
        targetExclude("example/**/*.java")
        targetExclude("src/main/java/examples/**/*.java")
        removeUnusedImports()
        googleJavaFormat("1.7") // or use eclipse().configFile("path/to/eclipse-format.xml")
    }
}

// Task to run the encryption example
tasks.register<JavaExec>("runEncryptionExample") {
    group = "examples"
    description = "Run the local database encryption example"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("examples.EncryptionExample")

    // Set the library path for native bindings
    // Check multiple possible locations for the native library
    val limboRoot = projectDir.parentFile.parentFile
    val tursoLibraryPath = System.getenv("TURSO_LIBRARY_PATH")
        ?: listOf(
            "${projectDir}/src/main/resources/libs/macos_arm64",
            "${projectDir}/src/main/resources/libs/linux_x64",
            "${projectDir}/build/resources/main/libs/macos_arm64",
            "${projectDir}/build/resources/main/libs/linux_x64",
            "${limboRoot}/target/debug",
            "${limboRoot}/target/release"
        ).joinToString(":")
    jvmArgs = listOf("-Djava.library.path=${System.getProperty("java.library.path")}:$tursoLibraryPath")
}
