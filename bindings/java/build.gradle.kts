import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import java.security.MessageDigest

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

tasks.withType<Javadoc> {
    options {
        (this as StandardJavadocDocletOptions).apply {
            addStringOption("Xdoclint:none", "-quiet")
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = prop("projectGroup")!!
            artifactId = prop("projectArtifactId")!!
            version = prop("projectVersion")!!

            pom {
                name.set(prop("pomName"))
                description.set(prop("pomDescription"))
                url.set(prop("pomUrl"))

                licenses {
                    license {
                        name.set(prop("pomLicenseName"))
                        url.set(prop("pomLicenseUrl"))
                    }
                }

                developers {
                    developer {
                        id.set(prop("pomDeveloperId"))
                        name.set(prop("pomDeveloperName"))
                        email.set(prop("pomDeveloperEmail"))
                    }
                }

                scm {
                    connection.set(prop("pomScmConnection"))
                    developerConnection.set(prop("pomScmDeveloperConnection"))
                    url.set(prop("pomScmUrl"))
                }
            }
        }
    }
}

signing {
    // Only sign if signing credentials are available
    val signingKey = providers.environmentVariable("GPG_PRIVATE_KEY").orNull
    val signingPassword = providers.environmentVariable("GPG_PASSPHRASE").orNull

    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    }
}

// Helper task to generate checksums
val generateChecksums by tasks.registering {
    dependsOn("jar", "sourcesJar", "javadocJar", "generatePomFileForMavenJavaPublication")

    val checksumDir = layout.buildDirectory.dir("checksums")

    doLast {
        val files = listOf(
            tasks.jar.get().archiveFile.get().asFile,
            tasks.named("sourcesJar").get().outputs.files.singleFile,
            tasks.named("javadocJar").get().outputs.files.singleFile,
            layout.buildDirectory.file("publications/mavenJava/pom-default.xml").get().asFile
        )

        checksumDir.get().asFile.mkdirs()

        files.forEach { file ->
            if (file.exists()) {
                // MD5
                val md5 = MessageDigest.getInstance("MD5")
                    .digest(file.readBytes())
                    .joinToString("") { "%02x".format(it) }
                file("${file.absolutePath}.md5").writeText(md5)

                // SHA1
                val sha1 = MessageDigest.getInstance("SHA-1")
                    .digest(file.readBytes())
                    .joinToString("") { "%02x".format(it) }
                file("${file.absolutePath}.sha1").writeText(sha1)
            }
        }
    }
}

// Task to create a bundle zip for Maven Central Portal
val createMavenCentralBundle by tasks.registering(Zip::class) {
    group = "publishing"
    description = "Creates a bundle zip for Maven Central Portal upload"

    dependsOn("generatePomFileForMavenJavaPublication", "jar", "sourcesJar", "javadocJar", generateChecksums)

    val groupId = prop("projectGroup")!!.replace(".", "/")
    val artifactId = prop("projectArtifactId")!!
    val projectVer = version.toString()

    // Validate version is not SNAPSHOT for Maven Central
    doFirst {
        if (projectVer.contains("SNAPSHOT")) {
            throw GradleException(
                "Cannot publish SNAPSHOT version to Maven Central. " +
                "Please change projectVersion in gradle.properties to a release version (e.g., 0.0.1)"
            )
        }
    }

    archiveFileName.set("$artifactId-$projectVer-bundle.zip")
    destinationDirectory.set(layout.buildDirectory.dir("maven-central"))

    // Maven Central expects files in groupId/artifactId/version/ structure
    val basePath = "$groupId/$artifactId/$projectVer"

    // Main JAR + checksums + signature
    from(tasks.jar.get().archiveFile) {
        into(basePath)
        rename { "$artifactId-$projectVer.jar" }
    }
    from(tasks.jar.get().archiveFile.get().asFile.absolutePath + ".md5") {
        into(basePath)
        rename { "$artifactId-$projectVer.jar.md5" }
    }
    from(tasks.jar.get().archiveFile.get().asFile.absolutePath + ".sha1") {
        into(basePath)
        rename { "$artifactId-$projectVer.jar.sha1" }
    }

    // Sources JAR + checksums + signature
    from(tasks.named("sourcesJar").get().outputs.files) {
        into(basePath)
        rename { "$artifactId-$projectVer-sources.jar" }
    }
    from(tasks.named("sourcesJar").get().outputs.files.singleFile.absolutePath + ".md5") {
        into(basePath)
        rename { "$artifactId-$projectVer-sources.jar.md5" }
    }
    from(tasks.named("sourcesJar").get().outputs.files.singleFile.absolutePath + ".sha1") {
        into(basePath)
        rename { "$artifactId-$projectVer-sources.jar.sha1" }
    }

    // Javadoc JAR + checksums + signature
    from(tasks.named("javadocJar").get().outputs.files) {
        into(basePath)
        rename { "$artifactId-$projectVer-javadoc.jar" }
    }
    from(tasks.named("javadocJar").get().outputs.files.singleFile.absolutePath + ".md5") {
        into(basePath)
        rename { "$artifactId-$projectVer-javadoc.jar.md5" }
    }
    from(tasks.named("javadocJar").get().outputs.files.singleFile.absolutePath + ".sha1") {
        into(basePath)
        rename { "$artifactId-$projectVer-javadoc.jar.sha1" }
    }

    // POM + checksums + signature
    from(layout.buildDirectory.file("publications/mavenJava/pom-default.xml")) {
        into(basePath)
        rename { "$artifactId-$projectVer.pom" }
    }
    from(layout.buildDirectory.file("publications/mavenJava/pom-default.xml").get().asFile.absolutePath + ".md5") {
        into(basePath)
        rename { "$artifactId-$projectVer.pom.md5" }
    }
    from(layout.buildDirectory.file("publications/mavenJava/pom-default.xml").get().asFile.absolutePath + ".sha1") {
        into(basePath)
        rename { "$artifactId-$projectVer.pom.sha1" }
    }

    // Signature files (generated by signing plugin)
    from(layout.buildDirectory.dir("libs")) {
        into(basePath)
        include("*.jar.asc")
        rename { name ->
            when {
                name.contains("sources") -> "$artifactId-$projectVer-sources.jar.asc"
                name.contains("javadoc") -> "$artifactId-$projectVer-javadoc.jar.asc"
                else -> "$artifactId-$projectVer.jar.asc"
            }
        }
    }

    // POM signature
    from(layout.buildDirectory.dir("publications/mavenJava")) {
        into(basePath)
        include("pom-default.xml.asc")
        rename { "$artifactId-$projectVer.pom.asc" }
    }
}

// Task to upload bundle to Maven Central Portal
tasks.register("publishToMavenCentral") {
    group = "publishing"
    description = "Publishes artifacts to Maven Central Portal"

    // Run publish first to generate signatures, then create bundle
    dependsOn("publish")
    dependsOn(createMavenCentralBundle)

    // Make sure bundle creation happens after publish
    createMavenCentralBundle.get().mustRunAfter("publish")

    doLast {
        val username = providers.environmentVariable("MAVEN_CENTRAL_USERNAME").orNull
        val password = providers.environmentVariable("MAVEN_CENTRAL_PASSWORD").orNull
        val bundleFile = createMavenCentralBundle.get().archiveFile.get().asFile

        require(username != null) { "MAVEN_CENTRAL_USERNAME environment variable must be set" }
        require(password != null) { "MAVEN_CENTRAL_PASSWORD environment variable must be set" }
        require(bundleFile.exists()) { "Bundle file does not exist: ${bundleFile.absolutePath}" }

        logger.lifecycle("Uploading bundle to Maven Central Portal...")
        logger.lifecycle("Bundle: ${bundleFile.absolutePath}")
        logger.lifecycle("Size: ${bundleFile.length() / 1024} KB")

        // Use curl for uploading (simple and available on most systems)
        exec {
            commandLine(
                "curl",
                "-X", "POST",
                "-u", "$username:$password",
                "-F", "bundle=@${bundleFile.absolutePath}",
                "https://central.sonatype.com/api/v1/publisher/upload?name=${bundleFile.name}&publishingType=AUTOMATIC"
            )
        }

        logger.lifecycle("Upload completed. Check https://central.sonatype.com/publishing for status.")
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
        removeUnusedImports()
        googleJavaFormat("1.7") // or use eclipse().configFile("path/to/eclipse-format.xml")
    }
}
