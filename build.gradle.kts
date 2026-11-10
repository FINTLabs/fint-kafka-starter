import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    `java-library`
    `maven-publish`
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.ktlint)
    alias(libs.plugins.binary.compatibility.validator)
}

group = "no.novari"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    withSourcesJar()
}

// Build with JDK 25, publish for Java 21.
tasks.withType<JavaCompile>().configureEach {
    options.release = 21
}

kotlin {
    explicitApi()
    compilerOptions {
        jvmTarget = JvmTarget.JVM_21
        freeCompilerArgs.add("-Xjdk-release=21")
        // Spring Boot 4.1 manages Kotlin 2.3, so apps may run with a 2.3 stdlib.
        apiVersion = KotlinVersion.KOTLIN_2_3
    }
}

ktlint {
    version = libs.versions.ktlint.cli
}

dependencies {
    implementation(platform(libs.spring.boot.dependencies))

    constraints {
        implementation(libs.lz4.java) {
            because("kafka-clients 4.2.2 brings lz4-java 1.11.2, which has known vulnerabilities fixed in 1.11.4")
        }
        implementation(libs.snappy.java) {
            because(
                "kafka-clients 4.2.2 brings snappy-java 1.1.10.7, " +
                    "which has CVE-2026-93451, CVE-2026-108106 and CVE-2026-116590 fixed in 1.1.10.12",
            )
        }
    }

    api("org.springframework.boot:spring-boot-starter-kafka")
    api("org.springframework.boot:spring-boot-starter-jackson")
    implementation("tools.jackson.module:jackson-module-kotlin")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    repositories {
        maven {
            url = uri("https://repo.fintlabs.no/releases")
            credentials {
                username = System.getenv("REPOSILITE_USERNAME")
                password = System.getenv("REPOSILITE_PASSWORD")
            }
            authentication {
                create<BasicAuthentication>("basic")
            }
        }
    }
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            // Publish the versions resolved from the Spring Boot BOM, so consumers do not need the BOM.
            versionMapping {
                allVariants {
                    fromResolutionResult()
                }
            }
        }
    }
}
