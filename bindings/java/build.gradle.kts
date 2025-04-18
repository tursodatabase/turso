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

group = properties["projectGroup"]!!
version = properties["projectVersion"]!!

signing {
    val signingKey = System.getenv("MAVEN_SIGNING_KEY")
    val signingPassword = System.getenv("MAVEN_SIGNING_PASSPHRASE")

    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    } else {
        logger.warn("Signing key or passphrase not found. Artifact signing is disabled.")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8

    withJavadocJar()
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = properties["projectGroup"]!!.toString().replace("\"", "")
            artifactId = "limbo"
            version = properties["projectVersion"]!!.toString()

            pom {
                name.set("Limbo")
                description.set("Java bindings for Limbo database")
                url.set("https://github.com/tursodatabase/limbo")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }

                developers {
                    developer {
                        id.set("tursodatabase")
                        name.set("Turso Database Team")
                        email.set("support@turso.tech")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/tursodatabase/limbo.git")
                    developerConnection.set("scm:git:ssh://github.com:tursodatabase/limbo.git")
                    url.set("https://github.com/tursodatabase/limbo")
                }
            }
        }
    }

    repositories {
        maven {
            name = "OSSRH"
            val releasesRepoUrl = "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
            val snapshotsRepoUrl = "https://s01.oss.sonatype.org/content/repositories/snapshots/"
            url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)

            credentials {
                username = System.getenv("MAVEN_UPLOAD_USERNAME") ?: ""
                password = System.getenv("MAVEN_UPLOAD_PASSWORD") ?: ""
            }
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
    val limboSystemLibraryPath = System.getenv("LIMBO_LIBRARY_PATH")
    if (limboSystemLibraryPath != null) {
        applicationDefaultJvmArgs = listOf(
            "-Djava.library.path=${System.getProperty("java.library.path")}:$limboSystemLibraryPath"
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
            file("src/main/resource/limbo-jdbc.properties")
        }
    }
}

tasks.test {
    useJUnitPlatform()
    // In order to find rust built file under resources, we need to set it as system path
    systemProperty(
        "java.library.path",
        "${System.getProperty("java.library.path")}:$projectDir/src/test/resources/limbo/debug"
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
        removeUnusedImports()
        googleJavaFormat("1.7") // or use eclipse().configFile("path/to/eclipse-format.xml")
    }
}

// Task to publish to Maven Central
tasks.register("publishToMavenCentral") {
    group = "publishing"
    description = "Publishes all Maven publications to Maven Central"
    dependsOn(tasks.withType<PublishToMavenRepository>().matching {
        it.repository == publishing.repositories.getByName("OSSRH")
    })
}
