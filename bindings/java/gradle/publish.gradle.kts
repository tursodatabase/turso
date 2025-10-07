import java.security.MessageDigest
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.plugins.signing.SigningExtension

// Helper function to read properties with defaults
fun prop(key: String, default: String? = null): String? =
    project.findProperty(key)?.toString() ?: default

// Maven Publishing Configuration
configure<PublishingExtension> {
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

// GPG Signing Configuration
configure<SigningExtension> {
    // Make signing required for publishing
    setRequired(true)

    // For CI/GitHub Actions: use in-memory keys
    val signingKey = providers.environmentVariable("GPG_PRIVATE_KEY").orNull
    val signingPassword = providers.environmentVariable("GPG_PASSPHRASE").orNull

    if (signingKey != null && signingPassword != null) {
        // CI mode: use in-memory keys
        useInMemoryPgpKeys(signingKey, signingPassword)
    } else {
        // Local mode: use GPG command from system
        useGpgCmd()
    }

    sign(the<PublishingExtension>().publications["mavenJava"])
}

// Helper task to generate checksums
val generateChecksums by tasks.registering {
    dependsOn("jar", "sourcesJar", "javadocJar", "generatePomFileForMavenJavaPublication")

    val checksumDir = layout.buildDirectory.dir("checksums")

    doLast {
        val files = listOf(
            tasks.named("jar").get().outputs.files.singleFile,
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

    dependsOn("generatePomFileForMavenJavaPublication", "jar", "sourcesJar", "javadocJar", "signMavenJavaPublication", generateChecksums)

    // Ensure signing happens before bundle creation
    mustRunAfter("signMavenJavaPublication")

    val groupId = prop("projectGroup")!!.replace(".", "/")
    val artifactId = prop("projectArtifactId")!!
    val projectVer = project.version.toString()

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
    from(tasks.named("jar").get().outputs.files) {
        into(basePath)
        rename { "$artifactId-$projectVer.jar" }
    }
    from(tasks.named("jar").get().outputs.files.singleFile.absolutePath + ".md5") {
        into(basePath)
        rename { "$artifactId-$projectVer.jar.md5" }
    }
    from(tasks.named("jar").get().outputs.files.singleFile.absolutePath + ".sha1") {
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

    // Signature files - get them from the signing task outputs
    doFirst {
        val signingTask = tasks.named("signMavenJavaPublication").get()
        logger.lifecycle("Signing task outputs: ${signingTask.outputs.files.files}")
    }

    // Include signature files generated by the signing plugin
    from(tasks.named("signMavenJavaPublication").get().outputs.files) {
        into(basePath)
        include("*.jar.asc", "pom-default.xml.asc")
        exclude("module.json.asc") // Exclude gradle module metadata signature
        rename { name ->
            // Only rename the POM signature file
            // JAR signatures are already correctly named by the signing plugin
            if (name == "pom-default.xml.asc") {
                "$artifactId-$projectVer.pom.asc"
            } else {
                name // Keep original name (already correct)
            }
        }
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
