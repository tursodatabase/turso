plugins {
    java
    application
}

group = "org.github.tursodatabase"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

application {
    mainClass.set("org.github.tursodatabase.Main")
}

tasks.test {
    useJUnitPlatform()
}
