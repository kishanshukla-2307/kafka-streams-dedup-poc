plugins {
    application
    java
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

repositories { mavenCentral() }

dependencies {
    val kafka = "3.7.0"
    implementation("org.apache.kafka:kafka-streams:$kafka")
    implementation("org.slf4j:slf4j-simple:2.0.13")
}

application {
    mainClass.set("com.example.dedup.DedupStreamApp")
}